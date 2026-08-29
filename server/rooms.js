/**
 * Registro e relay de salas.
 *
 * Salas são criadas explicitamente por alguém e vivem em memória. Cada uma
 * pertence a uma instância da Activity (o canal de voz), então canais
 * diferentes não enxergam as salas uns dos outros.
 *
 * Vários transmissores simultâneos por sala; N espectadores. Cada transmissor
 * recebe um "slot" numérico e carimba esse número no primeiro byte de todo
 * quadro, então o servidor repassa o buffer sem tocar nele e o espectador sabe
 * para qual decoder mandar.
 *
 * O servidor não decodifica nada. Ele guarda o decoderConfig de cada
 * transmissor e distingue keyframe de delta, porque quem começa a assistir
 * precisa de um keyframe: delta em decoder frio só dá erro.
 */
import crypto from 'node:crypto';

const MAX_BROADCASTERS = 4;
// Duas por pessoa: a tela e a câmera. O teto da sala continua valendo por cima,
// então duas pessoas com as duas fontes já lotam.
const MAX_POR_PESSOA = 2;

// Identidade de espectador na sinalização WebRTC. O transmissor precisa de um
// nome para endereçar cada conexão direta, e o id do usuário não serve: a mesma
// pessoa pode ter duas abas assistindo, e cada aba é uma conexão diferente.
let proximoPeerId = 1;

/** As fontes que uma transmissão pode ter. */
export const FONTES = new Set(['tela', 'camera']);
// Sala é objeto em memória criado por qualquer pessoa autenticada: sem teto,
// um laço de "criar sala" consome a RAM do processo.
const MAX_ROOMS_PER_INSTANCE = 20;

// A fila de um espectador é medida em TEMPO de mídia, não em bytes: 2 MB fixos
// eram ~6 segundos de atraso acumulado a 2,5 Mb/s antes de qualquer descarte —
// e delay acumulativo é exatamente isso. Tela ao vivo não pode acumular
// segundos de buffer, então o orçamento é uma fração de segundo da taxa da
// stream.
const MAX_FILA_SEGUNDOS = 0.3;

// O bootstrap NÃO é um piso permanente: a 100 kbps, 96 KB seriam ~7,9 s de
// mídia, que é justamente o atraso que este módulo existe para não ter. Ele
// vale só enquanto a medida não presta — ver AQUECIMENTO_MS — e some assim que
// existe taxa confiável, medida ou reportada.
const BOOTSTRAP_BYTES = 96 * 1024;

// Quanto tempo a medida leva para valer. Não é número escolhido a dedo: o
// estimador divide pelo tempo de vida da transmissão, então antes de 1 s o
// denominador é curto demais para uma amostra representativa e a taxa sai
// enviesada para baixo. Fora dessa janela o estimador é confiável.
const AQUECIMENTO_MS = 1000;

// Um keyframe é, por natureza, maior que a média da stream. Julgá-lo pela fila
// comum impediria a recuperação em bitrate baixo — sem keyframe o espectador
// não tem imagem nenhuma. A exceção é atômica (só com socket drenado) e tem
// teto próprio, em TEMPO como todo o resto: nunca `maxPayload`, que a 100 kbps
// seriam centenas de segundos de mídia numa fila só.
const TETO_KEYFRAME_SEGUNDOS = 2.0;

// Janela do estimador de taxa da transmissão.
const JANELA_TAXA_S = 5;

// Quantos descartes de admissão na janela marcam um espectador como degradando.
// Dois picos aparecem em rede limpa quando quadros encostam juntos no orçamento;
// três já formam uma rajada que justifica reduzir a origem.
const DESCARTES_PARA_DEGRADAR = 3;
// Em QUIC, uma rajada deste tamanho já prova que o orçamento físico foi
// ultrapassado. Esperar vários degraus intermediários cria backlog antes de o
// sinal nativo aparecer; o perfil de sobrevivência precisa valer nesta janela.
const DESCARTES_WEBTRANSPORT_EMERGENCIA = 8;
// O wire já comprovou um buraco de sequência, portanto reage antes da fila.
const PERDAS_WIRE_PARA_DEGRADAR = 2;
// Expiração nativa significa que um datagrama já envelheceu 250 ms dentro do
// QUIC: é evidência direta de que a oferta não cabe no enlace, não apenas um
// pico da fila JS. O proxy de ensaio cruza o enlace limitado na ida e na volta;
// portanto ~445 kb/s de vídeo (seis degraus), somados a áudio e FEC, ainda
// excedem 600 kb/s. Treze degraus levam imediatamente o vídeo ao piso de
// 60 kb/s; junto do áudio emergencial a 32 kb/s, o pior caso com FEC atravessa
// as duas pernas do relay sem transformar congestionamento em fila visual.
const PASSOS_PRESSAO_NATIVA = 13;
// Um contador nativo isolado também aparece ao abrir/fechar o gate de saúde e
// não prova congestionamento persistente. Três sinais em dois segundos ainda
// reagem em menos de uma janela sob 5% de perda, sem jogar uma sessão limpa no
// piso físico por uma única amostra espúria.
const PRESSAO_TRANSPORTE_PARA_EMERGENCIA = 3;
const JANELA_PRESSAO_TRANSPORTE_MS = 2000;

// Assimetria deliberada: descer é rápido e barato, subir é devagar e exige
// prova repetida de saúde. É a histerese que impede o laço de oscilar.
const DOWN_COOLDOWN_MS = 2000;
const UP_COOLDOWN_MS = 10 * 1000;
const JANELAS_LIMPAS_PARA_SUBIR = 2;

// Intervalo mínimo entre dois pedidos de keyframe para a mesma transmissão.
const KEYFRAME_ASK_EVERY_MS = 1000;
const KEYFRAME_RECOVERY_ASK_EVERY_MS = 350;
const CHUNKS_DEBOUNCE_MS = 100;
const MAX_NAME = 32;
const MAX_ROOM_NAME = 40;

// Sala vazia fecha, mas não no mesmo instante: recarregar a atividade
// desconecta e reconecta, e quem estivesse sozinho perderia a sala a cada F5.
// 12s cobre um reload com folga e some rápido o bastante para não deixar sala
// fantasma na lista.
const EMPTY_GRACE_MS = 12 * 1000;
// QUIC pode fechar uma sessão sob congestionamento sem a pessoa ter parado a
// captura. Slot, viewers e configs sobrevivem nesta janela para a nova sessão.
const BROADCASTER_RECONNECT_GRACE_MS = 8 * 1000;
// Quanto tempo a transmissão de alguém sobrevive à saída dessa pessoa da sala.
// Existe pelo mesmo motivo da carência acima: recarregar a atividade desconecta
// e reconecta, e sem ela um F5 derrubaria a transmissão de quem não saiu de
// lugar nenhum.
const SEM_PRESENCA_MS = 15 * 1000;
const SWEEP_EVERY_MS = 4 * 1000;
const QUALITY_DEBUG = process.env.WT_WIRE_DEBUG === '1';

// Freio de força bruta: sem isso uma senha curta cai em segundos, porque o
// endpoint responde tão rápido quanto a rede permite.
const MAX_ATTEMPTS = 5;
const ATTEMPT_WINDOW_MS = 60 * 1000;
const LOCKOUT_MS = 30 * 1000;

const SLOT_BYTE = 0;
const TYPE_BYTE = 1;
const KEYFRAME = 1;
const AUDIO = 3;

const rooms = new Map();

// Contadores do payload de midia que realmente atravessa o relay. Eles nao
// incluem os poucos bytes de cabecalho TCP/TLS/WebSocket, mas refletem a parte
// que cresce com bitrate e quantidade de espectadores.
const appTraffic = trafficCounter();

function trafficCounter() {
  return {
    startedAt: Date.now(),
    receivedBytes: 0,
    transmittedBytes: 0,
    droppedBytes: 0,
    buckets: new Map(),
    lastPrunedSecond: 0,
  };
}

function recordTraffic(counter, direction, bytes) {
  if (!counter || !Number.isFinite(bytes) || bytes <= 0) return;
  const second = Math.floor(Date.now() / 1000);
  let bucket = counter.buckets.get(second);
  if (!bucket) {
    bucket = { receivedBytes: 0, transmittedBytes: 0, droppedBytes: 0 };
    counter.buckets.set(second, bucket);
  }

  counter[direction] += bytes;
  bucket[direction] += bytes;

  // Um stream pode entregar centenas de chunks por segundo. A limpeza roda no
  // maximo uma vez por segundo por contador, nunca uma vez por chunk.
  if (counter.lastPrunedSecond !== second) {
    counter.lastPrunedSecond = second;
    for (const key of counter.buckets.keys()) {
      if (key < second - 60) counter.buckets.delete(key);
    }
  }
}

function trafficSnapshot(counter, windowSeconds = 5) {
  if (!counter) {
    return {
      receivedBytes: 0,
      transmittedBytes: 0,
      droppedBytes: 0,
      receivedBytesPerSecond: 0,
      transmittedBytesPerSecond: 0,
    };
  }

  const now = Date.now();
  const currentSecond = Math.floor(now / 1000);
  const firstSecond = currentSecond - windowSeconds + 1;
  let receivedBytes = 0;
  let transmittedBytes = 0;
  let droppedBytes = 0;

  for (const [second, bucket] of counter.buckets) {
    if (second < firstSecond) continue;
    receivedBytes += bucket.receivedBytes;
    transmittedBytes += bucket.transmittedBytes;
    droppedBytes += bucket.droppedBytes;
  }

  const actualWindow = Math.max(1, Math.min(windowSeconds, (now - counter.startedAt) / 1000));
  return {
    receivedBytes: counter.receivedBytes,
    transmittedBytes: counter.transmittedBytes,
    droppedBytes: counter.droppedBytes,
    receivedBytesPerSecond: receivedBytes / actualWindow,
    transmittedBytesPerSecond: transmittedBytes / actualWindow,
    droppedBytesPerSecond: droppedBytes / actualWindow,
  };
}

// ---------------------------------------------------- taxa da transmissão

/**
 * Estimador de taxa PRÓPRIO de cada transmissão, e não do socket.
 *
 * `entry.traffic` não serve para governar orçamento: ele nasce em
 * `attachBroadcaster`, e a aba de captura fica aberta muito antes de alguém
 * clicar em transmitir. Uma aba aberta há um minuto que começa a transmitir
 * agora dividiria meio segundo de mídia por cinco segundos de janela — taxa até
 * dez vezes menor que a real, orçamento dez vezes menor que o devido, e
 * descarte em massa no primeiro segundo de TODA transmissão.
 *
 * Zerar aqui, em cada `startStream`, elimina por construção quatro
 * contaminações: a transmissão anterior no mesmo segundo, o tráfego anterior ao
 * `start`, o posterior ao `stop` e a vida da conexão antes do stream.
 */
function estimadorTaxa(agora = Date.now()) {
  return { desde: agora, baldes: new Map() };
}

function registrarTaxa(estimador, bytes, agora = Date.now()) {
  if (!estimador || !Number.isFinite(bytes) || bytes <= 0) return;
  const segundo = Math.floor(agora / 1000);
  estimador.baldes.set(segundo, (estimador.baldes.get(segundo) ?? 0) + bytes);
  for (const chave of estimador.baldes.keys()) {
    if (chave < segundo - JANELA_TAXA_S) estimador.baldes.delete(chave);
  }
}

/**
 * Bytes por segundo desta transmissão, medidos desde que ela começou.
 *
 * O balde é de 1 segundo, então a fase do relógio dentro do segundo introduz
 * erro — declarado e limitado, não escondido: com a janela ancorada no início
 * do stream ele fica bem abaixo dos 20% que o contrato tolera, porque
 * numerador e denominador crescem juntos.
 */
function taxaMedida(estimador, agora) {
  if (!estimador) return 0;
  const segundo = Math.floor(agora / 1000);
  const primeiro = segundo - JANELA_TAXA_S + 1;
  let bytes = 0;
  for (const [chave, valor] of estimador.baldes) {
    if (chave >= primeiro) bytes += valor;
  }
  const janela = Math.min(JANELA_TAXA_S, Math.max(0, agora - estimador.desde) / 1000);
  return janela > 0 ? bytes / janela : 0;
}

/**
 * A taxa que governa o orçamento, em bytes por segundo — ou `null` quando não
 * há informação confiável nenhuma.
 *
 * Fria, a medida é enviesada para baixo por construção, então um `min` com ela
 * puxaria o orçamento junto e o snapshot do transmissor não serviria para nada
 * justamente quando é a única informação boa. Fria, portanto, o teto reportado
 * SUBSTITUI a medida; aquecida, ele apenas a limita — e é isso que faz uma
 * queda de qualidade valer no mesmo instante em que o transmissor a aplica, sem
 * esperar cinco segundos de média convergirem.
 */
function taxaDoOrcamento(entry, agora) {
  const teto = entry.qualidade ? entry.qualidade.bitrate / 8 : null;
  const aquecida = entry.startedAt !== null && agora - entry.startedAt >= AQUECIMENTO_MS;
  if (!aquecida) return teto;

  const medida = taxaMedida(entry.taxa, agora);
  return teto === null ? medida : Math.min(medida, teto);
}

/**
 * Estado de qualidade reportado pelo transmissor.
 *
 * O servidor ESPELHA em vez de contar. Um contador próprio aqui seria a escada
 * do cliente escrita duas vezes, livre para divergir dela — e a divergência
 * apareceria como dívida de `quality-up` que nunca fecha. `degraus` vem da
 * escada que o executa, então a recuperação é finita por construção.
 *
 * É entrada externa: validada na fronteira, uma vez, antes de virar estado de
 * domínio. Mensagem malformada é ignorada e o estado anterior permanece — um
 * transmissor adulterado não escreve no laço que governa a sala.
 */
export function setQuality(room, entry, snapshot) {
  if (!entry || !snapshot || typeof snapshot !== 'object') return false;
  const { degraus, bitrate, fps, piso } = snapshot;
  if (!Number.isInteger(degraus) || degraus < 0) return false;
  if (!Number.isFinite(bitrate) || bitrate <= 0) return false;
  if (!Number.isFinite(fps) || fps <= 0) return false;
  if (typeof piso !== 'boolean') return false;

  entry.degraus = degraus;
  entry.noPiso = piso;
  entry.qualidade = { bitrate, fps };
  const state = { type: 'quality-state', slot: entry.slot, degraus, bitrate, fps, piso };
  for (const viewer of room?.viewers ?? []) {
    if (viewer.__watching?.has(entry.slot)) sendJson(viewer, state);
  }
  return true;
}

// Uma pessoa pode ter duas transmissões ao mesmo tempo, então o uid sozinho não
// identifica mais uma delas. A chave composta mantém o acesso direto que o
// registro sempre teve, sem virar um Map de Maps.
const chaveDe = (uid, fonte) => `${uid}|${fonte}`;

/** As transmissões de uma pessoa, de uma fonte só quando `fonte` vem. */
export function broadcastersOf(room, userId, fonte = null) {
  return [...room.broadcasters.values()].filter(
    (e) => e.info.id === userId && (!fonte || e.fonte === fonte),
  );
}

const transmitindo = (room, userId) => broadcastersOf(room, userId).length > 0;

/**
 * A pessoa está na sala, e não só transmitindo para ela.
 *
 * A aba de captura não conta: ela tem conexão própria e continua de pé depois
 * que a atividade fecha, que é justamente o caso a detectar.
 */
function temViewer(room, userId) {
  for (const v of room.viewers) if (v.__info?.id === userId) return true;
  return false;
}

/**
 * A aba de captura, ligada desde que carrega e antes de qualquer transmissão.
 *
 * Existe porque a atividade precisa falar com ela justamente quando não há nada
 * no ar: mudar a qualidade, ou pedir a tela — que só nasce de um clique lá. A
 * conexão de transmissão não serve para isso, porque só é aberta depois que a
 * captura foi concedida.
 *
 * Não ocupa slot, não entra na contagem de pessoas e não segura a sala de pé:
 * uma aba esquecida aberta não pode manter viva uma sala que todo mundo já
 * deixou.
 */
export function attachControl(room, ws, userId) {
  ws.__controlOf = userId;
  room.controles.add(ws);
}

export function detachControl(room, ws) {
  room.controles.delete(ws);
}

/** Manda um recado para as abas de captura de uma pessoa. */
export function toControls(room, userId, obj) {
  let entregues = 0;
  for (const ws of room.controles) {
    if (ws.__controlOf !== userId) continue;
    if (sendJson(ws, obj)) entregues++;
  }
  return entregues;
}

// ------------------------------------------------------------------- senha

function hashPassword(password, salt = crypto.randomBytes(16)) {
  return { salt, hash: crypto.scryptSync(password, salt, 32) };
}

function passwordMatches(room, password) {
  if (!room.password) return true;
  const { hash } = hashPassword(password, room.password.salt);
  return crypto.timingSafeEqual(hash, room.password.hash);
}

/** Retorna null se pode tentar, ou os segundos que faltam para liberar. */
function lockoutRemaining(room) {
  if (!room.lockedUntil) return null;
  const left = room.lockedUntil - Date.now();
  if (left <= 0) {
    room.lockedUntil = 0;
    room.attempts = [];
    return null;
  }
  return Math.ceil(left / 1000);
}

/**
 * @returns {{ok:true}|{ok:false, reason:'senha'|'bloqueado', seconds?:number}}
 */
export function checkPassword(room, password) {
  const locked = lockoutRemaining(room);
  if (locked !== null) return { ok: false, reason: 'bloqueado', seconds: locked };

  if (passwordMatches(room, password ?? '')) {
    room.attempts = [];
    return { ok: true };
  }

  const now = Date.now();
  room.attempts = room.attempts.filter((t) => now - t < ATTEMPT_WINDOW_MS);
  room.attempts.push(now);

  if (room.attempts.length >= MAX_ATTEMPTS) {
    room.lockedUntil = now + LOCKOUT_MS;
    return { ok: false, reason: 'bloqueado', seconds: Math.ceil(LOCKOUT_MS / 1000) };
  }
  return { ok: false, reason: 'senha' };
}

/** Só o dono mexe na senha. Passar vazio remove. */
export function setPassword(room, userId, password) {
  if (room.ownerId !== userId) return 'Só quem criou a sala pode mudar a senha.';

  if (!password) {
    room.password = null;
  } else {
    room.password = hashPassword(String(password));
  }
  room.attempts = [];
  room.lockedUntil = 0;
  broadcastState(room);
  return null;
}

// ------------------------------------------------------------------ registro

export function createRoom({
  instance,
  name,
  ownerId,
  ownerName,
  password,
  guildId = null,
  guildName = null,
  channelId = null,
}) {
  const abertas = [...rooms.values()].filter((r) => r.instance === instance).length;
  if (abertas >= MAX_ROOMS_PER_INSTANCE) {
    return { error: 'Limite de salas abertas atingido. Feche uma antes de criar outra.' };
  }

  const escolhido = String(name ?? '')
    .replace(/\s+/g, ' ')
    .trim();
  // Nome é opcional: sem ele, um baseado em quem criou.
  const clean = (escolhido || `Sala de ${ownerName}`).slice(0, MAX_ROOM_NAME);

  const id = crypto.randomBytes(6).toString('base64url');

  const room = {
    id,
    instance,
    guildId,
    guildName,
    channelId,
    name: clean,
    ownerId,
    ownerName,
    password: password ? hashPassword(String(password)) : null,
    attempts: [],
    lockedUntil: 0,
    createdAt: Date.now(),
    emptySince: Date.now(),
    broadcasters: new Map(),
    slots: new Map(),
    viewers: new Set(),
    controles: new Set(),
    droppedChunks: 0,
    traffic: trafficCounter(),
  };

  rooms.set(id, room);
  return { room };
}

export const getRoom = (id) => rooms.get(id) ?? null;

/**
 * A sala fixa de uma call: id derivado do canal, criada na primeira entrada.
 *
 * Não tem dono nem senha — quem controla o acesso é a própria call, já que só
 * entra quem o Discord confirmou estar conectado ao canal.
 */
export function ensureCallRoom(instance, id, metadata = {}) {
  let room = rooms.get(id);
  if (room) {
    // A instância da Activity muda a cada relançamento no mesmo canal; o canal
    // é que é estável. Sem atualizar, a sala sumiria da lista após um relaunch.
    room.instance = instance;
    room.guildId = metadata.guildId ?? room.guildId ?? null;
    room.guildName = metadata.guildName ?? room.guildName ?? null;
    room.channelId = metadata.channelId ?? room.channelId ?? null;
    return room;
  }

  room = {
    id,
    instance,
    guildId: metadata.guildId ?? null,
    guildName: metadata.guildName ?? null,
    channelId: metadata.channelId ?? null,
    name: 'Sala da call',
    isCall: true,
    ownerId: null,
    ownerName: 'a call',
    password: null,
    attempts: [],
    lockedUntil: 0,
    createdAt: Date.now(),
    emptySince: Date.now(),
    broadcasters: new Map(),
    slots: new Map(),
    viewers: new Set(),
    controles: new Set(),
    droppedChunks: 0,
    traffic: trafficCounter(),
  };

  rooms.set(id, room);
  return room;
}

/**
 * Lista pública: nunca vaza hash de senha, só se ela existe.
 *
 * A sala automática da call fica de fora: dentro do Discord a atividade entra
 * nela direto, e no site ela nunca poderia ser aberta. Listá-la seria mostrar
 * uma porta que não abre.
 */
export function listRooms(instance) {
  return [...rooms.values()]
    .filter((r) => r.instance === instance && !r.isCall)
    .sort((a, b) => a.createdAt - b.createdAt)
    .map((r) => ({
      id: r.id,
      name: r.name,
      owner: r.ownerName,
      isCall: Boolean(r.isCall),
      locked: Boolean(r.password),
      people: countPeople(r),
      streams: [...r.broadcasters.values()].filter((e) => e.streaming).length,
    }));
}

function countPeople(room) {
  const ids = new Set();
  for (const v of room.viewers) if (v.__info) ids.add(v.__info.id);
  for (const e of room.broadcasters.values()) ids.add(e.info.id);
  return ids.size;
}

/**
 * Fecha salas vazias há tempo demais.
 *
 * A carência existe porque recarregar a atividade desconecta e reconecta: sem
 * ela, quem estivesse sozinho perderia a sala a cada F5.
 */
/**
 * Encerra a transmissão de quem já não está mais na sala.
 *
 * A aba de captura tem conexão própria: fechar a atividade não a alcança, e a
 * tela continua indo para quem ficou — sem a pessoa estar vendo, e sem nada na
 * frente dela dizendo que ainda está no ar. Isso é vazamento de tela, não
 * detalhe de interface, então quem decide é o servidor, que é o único lado que
 * enxerga as duas conexões.
 *
 * O `stop-request` faz a aba encerrar por conta própria e dizer o motivo. O
 * `detachBroadcaster` vem junto e não depende dela: uma aba travada, ou que
 * perdeu o socket, não pode continuar segurando a tela no ar.
 */
function derrubarAbandonadas(room, now) {
  for (const entry of room.broadcasters.values()) {
    if (temViewer(room, entry.info.id)) {
      entry.semDonoDesde = null;
      continue;
    }
    if (entry.semDonoDesde === null) {
      entry.semDonoDesde = now;
      continue;
    }
    if (now - entry.semDonoDesde <= SEM_PRESENCA_MS) continue;

    sendJson(entry.ws, {
      type: 'stop-request',
      motivo: 'Você saiu da atividade, então a transmissão parou.',
    });
    console.log(`[room ${room.id}] ${entry.info.name} saiu da sala — ${entry.fonte} encerrada`);
    detachBroadcaster(room, entry.ws);
  }
}

/**
 * Qualidade adaptativa: o feedback do relay para quem transmite.
 *
 * O servidor é o único lado que enxerga todos os espectadores ao mesmo tempo,
 * então é ele quem percebe que a sala está sofrendo. Ele decide QUANDO; o
 * transmissor, dono da escada, decide QUANTO e reporta de volta.
 *
 * Três estados de janela, e a diferença entre os dois últimos é o que impede a
 * recuperação de ser otimista demais:
 *
 * - suja: alguém pelo relay derrubou quadro demais — reduzir;
 * - limpa: há espectador pelo relay e nenhum sofrendo — prova de saúde;
 * - sem evidência: não há espectador pelo relay nenhum. Isso NÃO é prova de
 *   saúde, é ausência de prova, e por isso quebra a sequência de janelas
 *   limpas em vez de contar como uma delas.
 *
 * Com todo mundo em WebRTC o laço se suspende sozinho por essa definição, sem
 * caso especial: não há quem degrade nem prova de que está tudo bem.
 */
function avaliarQualidade(room, now) {
  for (const entry of room.broadcasters.values()) {
    if (!entry.streaming) continue;

    let relayOnly = 0;
    let degradando = false;
    let webTransportEmApuros = false;
    let maiorNumeroDeDescartes = 0;
    for (const v of room.viewers) {
      if (v.readyState !== v.OPEN) continue;
      if (!v.__watching?.has(entry.slot)) continue;
      // Quem recebe pela conexão direta não consome o relay: o que ele sofre
      // não é problema desta sala, e o que ficou na fila dele não conta.
      if (v.__rtc?.has(entry.slot)) continue;
      relayOnly++;
      const descartes = v.__descartes?.get(entry.slot) ?? 0;
      maiorNumeroDeDescartes = Math.max(maiorNumeroDeDescartes, descartes);
      if (descartes >= DESCARTES_PARA_DEGRADAR) degradando = true;
      if (v.transport === 'webtransport' && descartes >= DESCARTES_WEBTRANSPORT_EMERGENCIA) {
        webTransportEmApuros = true;
      }
    }

    if (QUALITY_DEBUG && relayOnly > 0 && (maiorNumeroDeDescartes > 0 || entry.degraus > 0)) {
      console.log(
        `[quality] room=${room.id} slot=${entry.slot} relay=${relayOnly} drops=${maiorNumeroDeDescartes} steps=${entry.degraus} action=${degradando ? 'down' : 'clean'}`,
      );
    }

    for (const v of room.viewers) v.__descartes?.delete(entry.slot);

    if (relayOnly === 0) {
      entry.janelasLimpas = 0;
      continue;
    }

    if (degradando) {
      reduzirQualidade(
        entry,
        now,
        webTransportEmApuros
          ? PASSOS_PRESSAO_NATIVA
          : passosPorSeveridade(maiorNumeroDeDescartes, DESCARTES_PARA_DEGRADAR),
        { urgente: webTransportEmApuros },
      );
      continue;
    }

    entry.janelasLimpas++;
    if (
      entry.degraus > 0 &&
      entry.janelasLimpas >= JANELAS_LIMPAS_PARA_SUBIR &&
      now - entry.ultimoAjuste >= UP_COOLDOWN_MS
    ) {
      sendJson(entry.ws, { type: 'quality-up' });
      entry.ultimoAjuste = now;
      entry.janelasLimpas = 0;
    }
  }
}

/**
 * Uma rajada grande não é quatro incidentes independentes: é uma prova mais
 * forte de que o teto atual não cabe. Traduzir a razão perda/limiar em poucos
 * degraus deixa o encoder chegar ao enlace em uma janela, sem remover o
 * cooldown que impede oscilação. O teto de quatro mantém a queda gradual e
 * limita qualquer sinal remoto isolado.
 */
function passosPorSeveridade(total, limiar) {
  if (!Number.isFinite(total) || !Number.isFinite(limiar) || limiar <= 0) return 1;
  return Math.max(1, Math.min(4, Math.ceil(total / limiar)));
}

function reduzirQualidade(entry, now = Date.now(), passos = 1, { urgente = false } = {}) {
  entry.janelasLimpas = 0;
  // No piso da escada o transmissor já não tem o que ceder: insistir só
  // geraria dívida de `quality-up` que a recuperação teria de pagar depois.
  if (entry.noPiso || (!urgente && now - entry.ultimoAjuste < DOWN_COOLDOWN_MS)) return false;
  const steps = Math.max(1, Math.min(PASSOS_PRESSAO_NATIVA, Math.ceil(Number(passos) || 1)));
  sendJson(entry.ws, { type: 'quality-down', steps });
  entry.ultimoAjuste = now;
  return true;
}

const sweeper = setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    derrubarAbandonadas(room, now);
    avaliarQualidade(room, now);

    const empty = room.viewers.size === 0 && room.broadcasters.size === 0;

    if (!empty) {
      room.emptySince = null;
      continue;
    }
    if (room.emptySince === null) {
      room.emptySince = now;
      continue;
    }
    if (now - room.emptySince > EMPTY_GRACE_MS) {
      // As abas de captura não seguram a sala de pé, mas continuam ligadas a
      // ela — e essa é a única conexão que sobrevive a este ponto, justamente
      // porque ficou de fora da conta de vazio. Sem fechar aqui, ela segue
      // aberta contra um objeto que ninguém mais alcança: não recebe mais nada,
      // e como não há `close`, a aba nem tenta reconectar.
      for (const ws of room.controles) {
        sendJson(ws, { type: 'room-gone' });
        ws.close();
      }
      room.controles.clear();

      rooms.delete(room.id);
      console.log(`[room ${room.id}] fechada por inatividade`);
    }
  }
}, SWEEP_EVERY_MS);
sweeper.unref?.();

// -------------------------------------------------------------------- envio

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) return false;
  ws.send(data);
  return true;
}

export function sendJson(ws, obj) {
  return send(ws, JSON.stringify(obj));
}

function toViewers(room, obj) {
  const msg = JSON.stringify(obj);
  for (const v of room.viewers) send(v, msg);
}

// -------------------------------------------------------------------- estado

// O avatar vai junto do nome: a lista de quem assiste mostra as fotos, e sem
// isto sobrava só a inicial colorida para quem tem foto no Discord.
function watchersOf(room, slot) {
  const byId = new Map();
  for (const v of room.viewers) {
    if (v.__watching?.has(slot) && v.__info) byId.set(v.__info.id, v.__info);
  }
  return [...byId.values()].map((info) => ({
    id: info.id,
    name: info.name,
    avatar: info.avatar ?? null,
  }));
}

function roomState(room) {
  // Uma pessoa pode ter a sala aberta em mais de uma aba; agrupamos por id
  // para não aparecer duplicada na lista.
  const byId = new Map();
  for (const v of room.viewers) {
    if (v.__info) byId.set(v.__info.id, v.__info);
  }

  const participants = [...byId.values()].map((info) => ({
    id: info.id,
    name: info.name,
    avatar: info.avatar ?? null,
    broadcasting: transmitindo(room, info.id),
  }));

  // Quem transmite pode ter fechado a aba da Activity: continua na lista,
  // senão o vídeo fica sem dono visível. O `vistos` importa agora que a mesma
  // pessoa pode ter duas transmissões — sem ele, apareceria duplicada.
  const vistos = new Set(byId.keys());
  for (const entry of room.broadcasters.values()) {
    if (vistos.has(entry.info.id)) continue;
    vistos.add(entry.info.id);
    participants.push({
      id: entry.info.id,
      name: entry.info.name,
      avatar: entry.info.avatar ?? null,
      broadcasting: true,
    });
  }

  participants.sort((a, b) => Number(b.broadcasting) - Number(a.broadcasting));

  // Quem tem aba de captura aberta. É o que permite à atividade saber se pode
  // falar com ela em vez de abrir outra — antes isso era deduzido do que estava
  // no ar, e uma aba ainda parada não aparecia em lugar nenhum.
  const abas = [...new Set([...room.controles].map((ws) => ws.__controlOf))];

  return {
    type: 'state',
    abas,
    room: { id: room.id, name: room.name, ownerId: room.ownerId, locked: Boolean(room.password) },
    broadcasting: room.broadcasters.size > 0,
    viewers: room.viewers.size,
    participants,
    streams: [...room.broadcasters.values()]
      .filter((e) => e.streaming)
      .map((e) => ({
        slot: e.slot,
        userId: e.info.id,
        fonte: e.fonte,
        watchers: watchersOf(room, e.slot),
      })),
  };
}

export function broadcastState(room) {
  const msg = JSON.stringify(roomState(room));
  for (const v of room.viewers) send(v, msg);
  for (const e of room.broadcasters.values()) send(e.ws, msg);
}

/**
 * Pede um ponto de partida novo ao transmissor, no máximo um por segundo.
 *
 * O limite não é economia: keyframe é o quadro mais caro que existe, e quem
 * pede é justamente quem já está com a conexão apertada. Sem o intervalo, dez
 * espectadores em apuros virariam dez keyframes seguidos — o remédio entupindo
 * o cano que ele deveria desentupir. Um serve todos, porque o transmissor manda
 * para a sala inteira. A única exceção é um gap já comprovado no viewer: ele
 * pode ultrapassar um pedido normal recente, mas continua coalescido por origem
 * a cada 350 ms — um pedido serve todos os viewers do mesmo slot.
 */
function requestKeyframe(entry, transicaoRtc = false, recuperacaoRelay = false) {
  const agora = Date.now();
  // Toda origem do pedido, inclusive a volta do RTC, passa pelo mesmo freio.
  // A primeira volta do RTC pode ultrapassar um pedido anterior do relay: o
  // decoder acabou de trocar de fonte e precisa de um ponto de partida novo.
  // A exceção é consumida uma vez por segundo; alternar rtc-ativo não abre uma
  // torneira de controles nem contorna o limite indefinidamente.
  if (
    entry.lastKeyframeAsk !== undefined &&
    agora - entry.lastKeyframeAsk < KEYFRAME_ASK_EVERY_MS
  ) {
    const rtcPodeUltrapassar =
      transicaoRtc &&
      (entry.lastRtcKeyframeAsk === undefined ||
        agora - entry.lastRtcKeyframeAsk >= KEYFRAME_ASK_EVERY_MS);
    const recoveryPodeUltrapassar =
      recuperacaoRelay &&
      (entry.lastRecoveryKeyframeAsk === undefined ||
        agora - entry.lastRecoveryKeyframeAsk >= KEYFRAME_RECOVERY_ASK_EVERY_MS);
    if (!rtcPodeUltrapassar && !recoveryPodeUltrapassar) return false;
  }
  if (transicaoRtc) entry.lastRtcKeyframeAsk = agora;
  if (recuperacaoRelay) entry.lastRecoveryKeyframeAsk = agora;
  entry.lastKeyframeAsk = agora;
  sendJson(entry.ws, { type: 'need-keyframe' });
  return true;
}

export function rename(room, ws, raw) {
  if (!ws.__info || typeof raw !== 'string') return;

  const name = raw.replace(/\s+/g, ' ').trim().slice(0, MAX_NAME);
  if (!name) return;

  ws.__info.name = name;
  // Todas as transmissões da pessoa, não "a" transmissão: quem divide tela e
  // câmera tem duas, e renomear só uma deixaria o grid com dois nomes.
  for (const entry of broadcastersOf(room, ws.__info.id)) entry.info.name = name;
  broadcastState(room);
}

// ---------------------------------------------------------------- transmissor

function freeSlot(room) {
  for (let i = 0; i < MAX_BROADCASTERS; i++) {
    if (!room.slots.has(i)) return i;
  }
  return null;
}

/** Retorna a entry criada, ou uma string com o motivo da recusa. */
export function attachBroadcaster(room, ws, info, fonte = 'tela') {
  const chave = chaveDe(info.id, fonte);
  const existing = room.broadcasters.get(chave);

  if (existing?.reconnectUntil && existing.reconnectUntil >= Date.now()) {
    clearTimeout(existing.reconnectTimer);
    existing.reconnectTimer = null;
    existing.reconnectUntil = null;
    existing.ws = ws;
    existing.connectedAt = Date.now();
    ws.__entry = existing;
    ws.__resumedBroadcaster = true;
    room.emptySince = null;
    sendJson(ws, { type: 'slot', slot: existing.slot });
    broadcastState(room);
    return existing;
  }

  // A recusa nomeia a fonte: "você já está transmitindo" era claro quando só
  // havia uma, mas com duas deixaria a pessoa sem saber qual delas repetiu.
  if (room.broadcasters.has(chave)) {
    return fonte === 'camera'
      ? 'Você já está transmitindo a câmera nesta sala.'
      : 'Você já está transmitindo a tela nesta sala.';
  }
  if (broadcastersOf(room, info.id).length >= MAX_POR_PESSOA) {
    return `Limite de ${MAX_POR_PESSOA} transmissões por pessoa atingido.`;
  }
  if (room.broadcasters.size >= MAX_BROADCASTERS) {
    return `Limite de ${MAX_BROADCASTERS} transmissões simultâneas atingido.`;
  }

  const slot = freeSlot(room);
  if (slot === null) return 'Sem espaço para mais transmissões.';

  const entry = {
    ws,
    info,
    fonte,
    chave,
    slot,
    streaming: false,
    // Desde quando quem transmite não está mais na sala. Null enquanto está.
    semDonoDesde: null,
    config: null,
    audioConfig: null,
    connectedAt: Date.now(),
    startedAt: null,
    traffic: trafficCounter(),
    droppedChunks: 0,
    // undefined = nunca dito. Vira true/false no primeiro espectador, e é o que
    // impede o servidor de repetir o mesmo recado a cada entrada e saída.
    chunksLigados: undefined,
    rtcChunksLastAt: undefined,
    chunksTimer: null,
    // Estimador de taxa da transmissão, trocado a cada `startStream`.
    taxa: estimadorTaxa(),
    // Espelho do estado reportado pelo transmissor. O servidor não conta.
    degraus: 0,
    noPiso: false,
    qualidade: null,
    janelasLimpas: 0,
    ultimoAjuste: Date.now(),
    ultimaPressaoTransporte: null,
    pressaoTransporteDesde: null,
    pressaoTransporteEventos: 0,
    reconnectTimer: null,
    reconnectUntil: null,
  };
  room.broadcasters.set(chave, entry);
  room.slots.set(slot, entry);
  ws.__entry = entry;
  room.emptySince = null;

  sendJson(ws, { type: 'slot', slot });
  broadcastState(room);
  return entry;
}

export function startStream(room, entry) {
  entry.streaming = true;
  entry.startedAt = Date.now();
  entry.config = null;
  entry.audioConfig = null;
  // Transmissão nova, dívida nenhuma: nem a qualidade herdada da anterior, nem
  // a taxa medida dela — inclusive quando as duas caem no mesmo segundo.
  entry.taxa = estimadorTaxa(entry.startedAt);
  entry.degraus = 0;
  entry.noPiso = false;
  entry.qualidade = null;
  entry.janelasLimpas = 0;
  entry.ultimoAjuste = entry.startedAt;
  entry.ultimaPressaoTransporte = null;
  entry.pressaoTransporteDesde = null;
  entry.pressaoTransporteEventos = 0;
  // Transmissão nova recomeça do zero: ninguém assiste até pedir.
  for (const v of room.viewers) {
    v.__primed?.delete(entry.slot);
    v.__watching?.delete(entry.slot);
    v.__descartes?.delete(entry.slot);
  }
  toViewers(room, {
    type: 'stream-start',
    slot: entry.slot,
    userId: entry.info.id,
    fonte: entry.fonte,
  });
  broadcastState(room);
}

/** Retoma uma stream preservada sem apagar watches nem reciclar o slot. */
export function resumeStream(room, entry) {
  if (!entry.streaming) {
    startStream(room, entry);
    return;
  }
  entry.connectedAt = Date.now();
  entry.ultimaPressaoTransporte = null;
  entry.pressaoTransporteDesde = null;
  entry.pressaoTransporteEventos = 0;
  broadcastState(room);
}

/**
 * Config do áudio, guardada e repassada igual à do vídeo.
 *
 * Quem começa a assistir no meio precisa dela para montar o decodificador — e,
 * ao contrário do vídeo, aqui não existe keyframe para servir de ponto de
 * partida: sem a config, nenhum pacote de som é aproveitável.
 */
export function setAudioConfig(room, entry, config) {
  entry.audioConfig = config;
  for (const v of room.viewers) {
    if (v.__watching?.has(entry.slot)) {
      sendJson(v, { type: 'audio-config', slot: entry.slot, config });
    }
  }
}

export function setConfig(room, entry, config) {
  entry.config = config;
  // Config nova significa decoder recriado; ele volta a precisar de keyframe.
  for (const v of room.viewers) v.__primed?.delete(entry.slot);
  for (const v of room.viewers) {
    if (v.__watching?.has(entry.slot)) sendJson(v, { type: 'config', slot: entry.slot, config });
  }
}

/** Um descarte na janela corrente do sweeper, contado por espectador e slot. */
function contarDescarte(v, slot) {
  if (!v.__descartes) v.__descartes = new Map();
  const total = (v.__descartes.get(slot) ?? 0) + 1;
  v.__descartes.set(slot, total);
  return total;
}

export function pushChunk(room, entry, chunk) {
  const bytes = Number(chunk?.byteLength ?? chunk?.length ?? 0);

  // A validação vem ANTES de qualquer contabilização: byte rejeitado não é
  // mídia. Contá-lo primeiro deixava um transmissor inflar a própria taxa — e
  // com ela o próprio orçamento — despejando buffers carimbados com slot
  // alheio, que o relay descarta mas registrava.
  if (chunk[SLOT_BYTE] !== entry.slot) return;

  const agora = Date.now();

  // A taxa é lida ANTES de o chunk entrar no estimador: o item em julgamento
  // não financia o próprio julgamento. Sem isto, um pico se autoriza sozinho.
  const taxa = taxaDoOrcamento(entry, agora);
  const orcamento = taxa === null ? BOOTSTRAP_BYTES : taxa * MAX_FILA_SEGUNDOS;
  const tetoKeyframe = taxa === null ? null : taxa * TETO_KEYFRAME_SEGUNDOS;

  recordTraffic(appTraffic, 'receivedBytes', bytes);
  recordTraffic(room.traffic, 'receivedBytes', bytes);
  recordTraffic(entry.traffic, 'receivedBytes', bytes);
  registrarTaxa(entry.taxa, bytes, agora);

  const tipo = chunk[TYPE_BYTE];
  const isKeyframe = tipo === KEYFRAME;
  const isAudio = tipo === AUDIO;

  // A pergunta não é "a fila dele JÁ passou do limite?", e sim "a fila FICA
  // passando do limite se eu mandar isto?". Com a primeira, um único quadro
  // grande entrava inteiro numa fila vazia e punha segundos de mídia no socket
  // — o teto nunca limitava o que estava entrando, só o que já estava lá.
  const cabe = (v, limite) => v.bufferedAmount + bytes <= limite;

  // No WebTransport, delta e audio seguem como datagramas por uma fila que ja
  // tem limites proprios de quantidade, bytes e idade. Reaplicar aqui o teto
  // de bufferedAmount do WebSocket descartava antes dessa fila cancelavel e
  // transformava pressao transitoria em uma longa espera por keyframe. O
  // keyframe continua confiavel e conserva a politica atomica logo abaixo.
  const cabeDatagrama = (v) => v.transport === 'webtransport' || cabe(v, orcamento);

  // Keyframe tem política própria e atômica: passa pelo teto comum de 2×, ou —
  // com o socket COMPLETAMENTE drenado — por um teto em tempo só dele. É o que
  // mantém a recuperação possível em bitrate baixo, onde um keyframe sozinho
  // vale mais que a fila inteira. Admitido o keyframe grande, a mídia seguinte
  // fica bloqueada até drenar pela regra ordinária, sem código a mais.
  const cabeKeyframe = (v) => {
    // O wire WT aposenta deltas antigos antes de admitir a âncora e reserva um
    // writer de recuperação. Julgar a fila anterior aqui cria um falso drop:
    // o próprio keyframe que esvaziaria a lane é recusado, passa a alimentar o
    // laço de qualidade e derruba uma sessão limpa degrau após degrau.
    if (v.transport === 'webtransport') {
      const limiteFisico = tetoKeyframe ?? BOOTSTRAP_BYTES * 2;
      return bytes <= limiteFisico;
    }
    if (cabe(v, orcamento * 2)) return true;
    if (tetoKeyframe === null || bytes > tetoKeyframe) return false;
    if (v.bufferedAmount === 0) return true;

    // O adapter WebTransport aposenta os deltas pendentes da lane ANTES de
    // admitir um keyframe. Rooms precisa deixar essa ancora chegar ao adapter:
    // julgar apenas o bufferedAmount anterior rejeitaria justamente o frame que
    // esvazia a fila. A excecao vale somente para decoder frio e conserva o teto
    // atomico de tempo; WebSocket continua sem ela porque nao consegue cancelar
    // bytes que ja estao no socket TCP.
    return false;
  };

  let sentCopies = 0;
  let droppedCopies = 0;

  for (const v of room.viewers) {
    if (v.readyState !== v.OPEN) continue;

    // Assistir é opt-in: quem não pediu esta tela não recebe os bytes dela.
    if (!v.__watching.has(entry.slot)) continue;

    // Já está recebendo esta tela pela conexão direta. O relay some do caminho
    // dele sem que nada seja desligado: se o WebRTC cair, o slot sai deste
    // conjunto e os bytes voltam a fluir no mesmo instante.
    if (v.__rtc?.has(entry.slot)) continue;

    // Áudio não depende de keyframe — cada pacote Opus se decodifica sozinho —,
    // então não passa pelo controle de "já recebeu ponto de partida".
    if (isAudio) {
      if (!cabeDatagrama(v)) {
        room.droppedChunks++;
        entry.droppedChunks++;
        droppedCopies++;
        contarDescarte(v, entry.slot);
        continue;
      }
      v.send(chunk);
      sentCopies++;
      v.__mediaBytesOut = (v.__mediaBytesOut ?? 0) + bytes;
      continue;
    }

    if (isKeyframe) {
      if (!cabeKeyframe(v)) {
        room.droppedChunks++;
        entry.droppedChunks++;
        droppedCopies++;
        contarDescarte(v, entry.slot);
        // Sem este keyframe ele continua sem ponto de partida. Pedir outro é o
        // que evita a espera pelo periódico, que é de segundos.
        requestKeyframe(entry);
        continue;
      }
      v.send(chunk);
      sentCopies++;
      v.__mediaBytesOut = (v.__mediaBytesOut ?? 0) + bytes;
      v.__primed.add(entry.slot);
      continue;
    }

    if (!v.__primed.has(entry.slot)) continue;

    if (!cabeDatagrama(v)) {
      room.droppedChunks++;
      entry.droppedChunks++;
      droppedCopies++;
      contarDescarte(v, entry.slot);

      // Um delta perdido quebra a cadeia de referência: daqui em diante o
      // decoder dele descarta tudo até chegar um keyframe. Continuar mandando
      // deltas seria despejar bytes indecifráveis numa conexão que já não vaza
      // — o buffer nunca drena, o descarte nunca para, e o vídeo fica parado
      // por segundos. Despreparar corta esse ciclo, e o pedido de keyframe traz
      // a imagem de volta em quadros em vez de em segundos.
      v.__primed.delete(entry.slot);
      requestKeyframe(entry);
      continue;
    }
    v.send(chunk);
    sentCopies++;
    v.__mediaBytesOut = (v.__mediaBytesOut ?? 0) + bytes;
  }

  const sentBytes = bytes * sentCopies;
  const droppedBytes = bytes * droppedCopies;
  for (const counter of [appTraffic, room.traffic, entry.traffic]) {
    recordTraffic(counter, 'transmittedBytes', sentBytes);
    recordTraffic(counter, 'droppedBytes', droppedBytes);
  }
}

export function stopStream(room, entry) {
  if (!entry.streaming) return;
  clearTimeout(entry.chunksTimer);
  entry.chunksTimer = null;
  entry.streaming = false;
  entry.startedAt = null;
  entry.config = null;
  entry.audioConfig = null;
  entry.degraus = 0;
  entry.noPiso = false;
  entry.qualidade = null;
  entry.janelasLimpas = 0;
  entry.ultimoAjuste = Date.now();
  entry.ultimaPressaoTransporte = null;
  entry.pressaoTransporteDesde = null;
  entry.pressaoTransporteEventos = 0;
  for (const v of room.viewers) {
    v.__primed?.delete(entry.slot);
    v.__watching?.delete(entry.slot);
    v.__rtc?.delete(entry.slot);
    v.__descartes?.delete(entry.slot);
  }
  entry.chunksLigados = undefined;
  toViewers(room, { type: 'stream-stop', slot: entry.slot });
}

export function detachBroadcaster(room, ws) {
  const entry = ws.__entry;
  if (!entry || room.broadcasters.get(entry.chave) !== entry || entry.ws !== ws) return;

  clearTimeout(entry.reconnectTimer);
  entry.reconnectTimer = null;
  entry.reconnectUntil = null;
  clearTimeout(entry.chunksTimer);
  entry.chunksTimer = null;
  stopStream(room, entry);
  room.broadcasters.delete(entry.chave);
  room.slots.delete(entry.slot);
  broadcastState(room);
}

/**
 * Conserva uma transmissão durante uma queda curta do transporte.
 * Retorna true quando entrou em carência; false quando a entrada foi removida.
 */
export function suspendBroadcaster(room, ws) {
  const entry = ws.__entry;
  if (
    !entry ||
    room.broadcasters.get(entry.chave) !== entry ||
    entry.ws !== ws ||
    !entry.streaming
  ) {
    detachBroadcaster(room, ws);
    return false;
  }

  clearTimeout(entry.reconnectTimer);
  entry.reconnectUntil = Date.now() + BROADCASTER_RECONNECT_GRACE_MS;
  entry.reconnectTimer = setTimeout(() => {
    if (entry.ws === ws && entry.reconnectUntil && entry.reconnectUntil <= Date.now()) {
      detachBroadcaster(room, ws);
    }
  }, BROADCASTER_RECONNECT_GRACE_MS);
  entry.reconnectTimer.unref?.();
  return true;
}

// --------------------------------------------------------------- espectador

export function watch(room, ws, slot) {
  const entry = room.slots.get(slot);
  if (!entry || !entry.streaming) return;
  // Repetir o pedido não muda nada, mas custaria um broadcast de estado para a
  // sala inteira — um cliente em laço faria o servidor inundar todo mundo.
  if (ws.__watching.has(slot)) return;

  ws.__watching.add(slot);
  ws.__primed.delete(slot);

  if (entry.config) sendJson(ws, { type: 'config', slot, config: entry.config });
  if (entry.audioConfig) {
    sendJson(ws, { type: 'audio-config', slot, config: entry.audioConfig });
  }
  if (entry.qualidade) {
    sendJson(ws, {
      type: 'quality-state',
      slot,
      degraus: entry.degraus,
      bitrate: entry.qualidade.bitrate,
      fps: entry.qualidade.fps,
      piso: entry.noPiso,
    });
  }
  requestKeyframe(entry);

  // Convida o transmissor a abrir uma conexão direta com este espectador. É só
  // um convite: enquanto ela não fecha — e ela pode nunca fechar — os quadros
  // continuam chegando pelo relay, que já começou acima.
  sendJson(entry.ws, { type: 'rtc-want', peer: ws.__peerId });

  atualizarChunks(room, entry);
  broadcastState(room);
}

/**
 * O espectador ficou sem ponto de partida e pede um novo.
 *
 * Quem descobre o buraco é o transporte dele: o relay entrega o que recebe e
 * não sabe que a cadeia de referência quebrou no meio do caminho. Sem esta
 * porta, um espectador que perdeu o keyframe ficava com a tela parada até o
 * próximo periódico — segundos, não quadros.
 *
 * É entrada externa, validada aqui na fronteira e em nenhum outro lugar:
 * - slot inteiro e dentro da faixa que a sala pode ter;
 * - transmissão existente e no ar;
 * - o pedinte assiste ESTE slot — senão qualquer espectador cobraria keyframe
 *   de uma transmissão que nem pediu, e o custo cairia na sala inteira;
 * - e não está pela conexão direta, onde o relay não é a origem da imagem.
 *
 * O pedido passa pelo mesmo freio de um por segundo do resto do módulo: quem
 * pede é justamente quem já está com a conexão apertada.
 */
export function pedirKeyframe(room, ws, slot) {
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_BROADCASTERS) return false;
  const entry = room.slots.get(slot);
  if (!entry || !entry.streaming) return false;
  if (!ws.__watching?.has(slot)) return false;
  if (ws.__rtc?.has(slot)) return false;

  // Ele já sabe que o decodificador está frio; o servidor precisa saber também,
  // senão continuaria despejando deltas indecifráveis até o keyframe chegar.
  ws.__primed?.delete(slot);
  // Buraco detectado pelo wire é descarte real do relay, mesmo quando a fila
  // JS já drenou. Sem alimentar a mesma janela de qualidade, perdas nativas de
  // datagrama pediam keyframe, mas o encoder continuava em 2,5 Mb/s contra um
  // caminho degradado e recriava o buraco indefinidamente.
  const perdasNaJanela = contarDescarte(ws, slot);
  // O wire enxerga perda nativa a cada poucos centésimos; esperar o sweeper de
  // quatro segundos fazia a escada ceder devagar demais para uma rampa real.
  // O mesmo cooldown global continua limitando a uma redução a cada 2 s.
  if (perdasNaJanela >= PERDAS_WIRE_PARA_DEGRADAR) {
    reduzirQualidade(
      entry,
      Date.now(),
      passosPorSeveridade(perdasNaJanela, PERDAS_WIRE_PARA_DEGRADAR),
    );
  }
  requestKeyframe(entry, false, true);
  return true;
}

/**
 * Reage ao sinal de congestionamento que só o QUIC nativo consegue observar.
 *
 * `expiredOutgoing` não é perda aleatória: o datagrama passou do próprio teto
 * de frescor antes de sair. Esperar o viewer detectar um buraco e o sweeper
 * acumular descartes adicionava segundos ao laço. Três eventos em uma janela
 * curta confirmam a pressão sem deixar um espúrio do preflight derrubar a
 * qualidade inteira; a reação preserva o cooldown global e só afeta
 * transmissões que este socket realmente assiste pelo relay.
 */
export function reportarPressaoTransporte(room, ws, diagnostic) {
  const reason = diagnostic?.reason;
  const expiracaoNativa = reason === 'datagram-native-expired';
  const perdaNativa = reason === 'datagram-native-lost';
  const writerSemCredito = reason === 'datagram-blocked' || reason === 'datagram-expired';
  if (!expiracaoNativa && !perdaNativa && !writerSemCredito) return 0;
  if (expiracaoNativa && !(Number(diagnostic.newlyExpired) > 0)) return 0;
  if (perdaNativa && !(Number(diagnostic.newlyLost) > 0)) return 0;
  const eventos = expiracaoNativa
    ? Number(diagnostic.newlyExpired)
    : perdaNativa
      ? Number(diagnostic.newlyLost)
      : 1;

  let afetadas = 0;
  const now = Date.now();
  for (const slot of ws.__watching ?? []) {
    if (ws.__rtc?.has(slot)) continue;
    const entry = room.slots.get(slot);
    if (!entry?.streaming) continue;
    if (
      Number.isFinite(entry.ultimaPressaoTransporte) &&
      now - entry.ultimaPressaoTransporte < DOWN_COOLDOWN_MS
    ) {
      continue;
    }
    if (
      !Number.isFinite(entry.pressaoTransporteDesde) ||
      now - entry.pressaoTransporteDesde > JANELA_PRESSAO_TRANSPORTE_MS
    ) {
      entry.pressaoTransporteDesde = now;
      entry.pressaoTransporteEventos = 0;
    }
    entry.pressaoTransporteEventos += eventos;
    if (entry.pressaoTransporteEventos < PRESSAO_TRANSPORTE_PARA_EMERGENCIA) continue;
    entry.pressaoTransporteDesde = null;
    entry.pressaoTransporteEventos = 0;
    // A pressão do writer/nativo não compete com o cooldown do sweeper. Se o
    // sweep acabou de pedir um degrau, o corte físico ainda precisa atravessar;
    // apenas sinais emergenciais entre si compartilham o freio de 2 s.
    if (reduzirQualidade(entry, now, PASSOS_PRESSAO_NATIVA, { urgente: true })) {
      entry.ultimaPressaoTransporte = now;
      afetadas++;
    }
  }
  return afetadas;
}

export function unwatch(room, ws, slot) {
  // Só avisa a sala se algo mudou de fato; ver a nota em watch().
  if (!ws.__watching.delete(slot)) return;
  ws.__primed.delete(slot);
  ws.__descartes?.delete(slot);
  encerrarPeer(room, ws, slot);
  broadcastState(room);
}

// ------------------------------------------------------------------- WebRTC

/**
 * Sinalização: o servidor só carrega envelope, nunca abre.
 *
 * Offer, answer e candidato ICE viajam opacos entre o transmissor e cada
 * espectador. O relay já é o canal de todos com todos e já está autenticado —
 * abrir um segundo canal só para isso seria uma porta a mais para guardar.
 */
function viewerPorPeer(room, peerId) {
  for (const v of room.viewers) {
    if (v.__peerId === peerId) return v;
  }
  return null;
}

/** Do espectador para o transmissor daquele slot. */
export function rtcParaBroadcaster(room, ws, slot, payload) {
  const entry = room.slots.get(slot);
  if (!entry || !entry.streaming) return;
  sendJson(entry.ws, { type: 'rtc', peer: ws.__peerId, payload });
}

/** Do transmissor para um espectador nomeado. */
export function rtcParaViewer(room, entry, peerId, payload) {
  const v = viewerPorPeer(room, peerId);
  if (!v) return;
  sendJson(v, { type: 'rtc', slot: entry.slot, payload });
}

/**
 * O espectador avisa que a conexão direta assumiu — ou que caiu.
 *
 * É ele quem decide, e não o servidor, porque é ele que sabe se está de fato
 * vendo quadros. Fechar o relay por causa de um `connectionState` otimista
 * deixaria a tela preta com a conexão "conectada".
 */
export function rtcAtivo(room, ws, slot, ativo) {
  const entry = room.slots.get(slot);
  if (!entry || !ws.__watching?.has(slot)) return;

  if (ativo) {
    if (ws.__rtc.has(slot)) return;
    ws.__rtc.add(slot);
    // O que ele sofreu no relay morre aqui junto com o relay dele: contar isso
    // contra a qualidade da sala seria cobrar de todo mundo por uma fila que
    // ninguém mais está consumindo.
    ws.__descartes?.delete(slot);
  } else {
    if (!ws.__rtc.delete(slot)) return;
    // Voltando ao relay, o decoder dele está frio: sem keyframe novo ele
    // descartaria tudo até o periódico, que é de segundos.
    ws.__primed.delete(slot);
    requestKeyframe(entry, true);
  }

  atualizarChunksRtc(room, entry);
}

/** Desfaz a conexão direta de um espectador com um slot, dos dois lados. */
function encerrarPeer(room, ws, slot) {
  const entry = room.slots.get(slot);
  ws.__rtc?.delete(slot);
  if (!entry) return;
  sendJson(entry.ws, { type: 'rtc-bye', peer: ws.__peerId });
  atualizarChunks(room, entry);
}

/**
 * Liga e desliga o fluxo do relay na origem.
 *
 * Quando todo mundo que assiste está na conexão direta, os quadros que sobem
 * para o servidor não têm para onde ir. Continuar codificando e enviando seria
 * gastar a subida de quem transmite — justamente o recurso mais escasso — para
 * alimentar um caminho que ninguém está usando. Vale o contrário também: basta
 * um espectador sem WebRTC para o relay voltar inteiro.
 */
function atualizarChunks(room, entry) {
  let precisa = 0;
  for (const v of room.viewers) {
    if (v.__watching?.has(entry.slot) && !v.__rtc?.has(entry.slot)) precisa++;
  }

  const ligado = precisa > 0;
  if (entry.chunksLigados === ligado) return;
  entry.chunksLigados = ligado;
  sendJson(entry.ws, { type: 'chunks', on: ligado });
  // Religar o relay sem ponto de partida entregaria só deltas indecifráveis.
  if (ligado) requestKeyframe(entry);
}

function atualizarChunksRtc(room, entry) {
  const agora = Date.now();
  const desde = agora - (entry.rtcChunksLastAt ?? -Infinity);
  if (!entry.chunksTimer && desde >= CHUNKS_DEBOUNCE_MS) {
    entry.rtcChunksLastAt = agora;
    atualizarChunks(room, entry);
    return;
  }

  if (entry.chunksTimer) return;
  entry.chunksTimer = setTimeout(
    () => {
      entry.chunksTimer = null;
      entry.rtcChunksLastAt = Date.now();
      atualizarChunks(room, entry);
    },
    Math.max(0, CHUNKS_DEBOUNCE_MS - desde),
  );
}

export function attachViewer(room, ws, info) {
  ws.__primed = new Set();
  ws.__watching = new Set();
  // Slots que já chegam por WebRTC. Enquanto o slot está aqui, o relay não
  // manda os bytes dele para este espectador — seria o mesmo vídeo duas vezes.
  ws.__rtc = new Set();
  // Descartes deste espectador na janela corrente do sweeper, por slot. É a
  // evidência de congestionamento, e é por espectador porque a decisão precisa
  // distinguir "um sofrendo" de "dois quase bem".
  ws.__descartes = new Map();
  ws.__peerId ??= `p${proximoPeerId++}`;
  ws.__info = info;
  ws.__connectedAt = ws.__connectedAt ?? Date.now();
  ws.__mediaBytesOut = ws.__mediaBytesOut ?? 0;
  room.viewers.add(ws);
  room.emptySince = null;

  sendJson(ws, roomState(room));

  // Anuncia o que está no ar, sem começar a mandar quadros: assistir é opt-in.
  for (const entry of room.broadcasters.values()) {
    if (!entry.streaming) continue;
    sendJson(ws, {
      type: 'stream-start',
      slot: entry.slot,
      userId: entry.info.id,
      fonte: entry.fonte,
    });
  }

  broadcastState(room);
}

export function detachViewer(room, ws) {
  // Sai antes de avisar: o recount de `atualizarChunks` não pode contar quem
  // acabou de fechar a aba como alguém que ainda precisa dos quadros.
  room.viewers.delete(ws);
  for (const slot of ws.__watching ?? []) encerrarPeer(room, ws, slot);
  broadcastState(room);
}

export function stats() {
  return [...rooms.values()].map((r) => ({
    id: r.id,
    name: r.name,
    locked: Boolean(r.password),
    broadcasting: r.broadcasters.size,
    viewers: r.viewers.size,
    droppedChunks: r.droppedChunks,
  }));
}

function average(values) {
  const valid = values.filter(Number.isFinite);
  return valid.length ? valid.reduce((sum, value) => sum + value, 0) / valid.length : null;
}

function usersOf(room) {
  const users = new Map();

  function add(info, role, ws, extra = {}) {
    if (!info?.id) return;
    let user = users.get(info.id);
    if (!user) {
      user = {
        id: info.id,
        name: info.name,
        avatar: info.avatar ?? null,
        roles: new Set(),
        connections: 0,
        connectedAt: Date.now(),
        pingSamples: [],
        watching: new Set(),
        mediaBytesOut: 0,
        bufferedBytes: 0,
      };
      users.set(info.id, user);
    }

    user.name = info.name || user.name;
    user.avatar = info.avatar ?? user.avatar;
    user.roles.add(role);
    user.connections++;
    user.connectedAt = Math.min(user.connectedAt, ws?.__connectedAt ?? Date.now());
    if (Number.isFinite(ws?.__rttMs)) user.pingSamples.push(ws.__rttMs);
    for (const slot of ws?.__watching ?? []) user.watching.add(slot);
    user.mediaBytesOut += ws?.__mediaBytesOut ?? 0;
    user.bufferedBytes += ws?.bufferedAmount ?? 0;
    if (extra.broadcasting) user.broadcasting = true;
  }

  for (const viewer of room.viewers) add(viewer.__info, 'viewer', viewer);
  for (const entry of room.broadcasters.values()) {
    add(entry.info, 'broadcaster', entry.ws, { broadcasting: entry.streaming });
  }

  return [...users.values()].map((user) => ({
    id: user.id,
    name: user.name,
    avatar: user.avatar,
    roles: [...user.roles],
    connections: user.connections,
    connectedAt: user.connectedAt,
    pingMs: average(user.pingSamples),
    watching: [...user.watching],
    broadcasting: Boolean(user.broadcasting),
    mediaBytesOut: user.mediaBytesOut,
    bufferedBytes: user.bufferedBytes,
  }));
}

/** Estado detalhado usado exclusivamente pela API administrativa protegida. */
export function adminStats() {
  const roomList = [...rooms.values()].map((room) => {
    const users = usersOf(room);
    const streams = [...room.broadcasters.values()]
      .filter((entry) => entry.streaming)
      .map((entry) => ({
        slot: entry.slot,
        userId: entry.info.id,
        userName: entry.info.name,
        startedAt: entry.startedAt,
        codec: entry.config?.codec ?? null,
        width: entry.config?.codedWidth ?? null,
        height: entry.config?.codedHeight ?? null,
        audioCodec: entry.audioConfig?.codec ?? null,
        watchers: watchersOf(room, entry.slot).length,
        droppedChunks: entry.droppedChunks,
        bufferedBytes: entry.ws?.bufferedAmount ?? 0,
        pingMs: Number.isFinite(entry.ws?.__rttMs) ? entry.ws.__rttMs : null,
        traffic: trafficSnapshot(entry.traffic),
      }));

    return {
      id: room.id,
      name: room.name,
      instance: room.instance,
      guildId: room.guildId ?? null,
      guildName: room.guildName ?? null,
      channelId: room.channelId ?? null,
      isCall: Boolean(room.isCall),
      locked: Boolean(room.password),
      createdAt: room.createdAt,
      connections: room.viewers.size + room.broadcasters.size,
      viewers: room.viewers.size,
      broadcasters: room.broadcasters.size,
      droppedChunks: room.droppedChunks,
      traffic: trafficSnapshot(room.traffic),
      users,
      streams,
    };
  });

  return { rooms: roomList, traffic: trafficSnapshot(appTraffic), startedAt: appTraffic.startedAt };
}
