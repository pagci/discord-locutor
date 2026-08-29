import dotenv from 'dotenv';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:http';
import express from 'express';
import { WebSocketServer } from 'ws';

import { signToken, verifyToken } from './tokens.js';
import * as R from './rooms.js';
import { systemSnapshot, startSampling } from './system.js';
import { buildAdminDashboard, mergeAdminDashboards } from './admin.js';
import { basePathFor, nodeFor, shardKey, stripNode } from '../shared/shard.js';
import { startWebTransport } from './webtransport.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const {
  DISCORD_CLIENT_ID,
  DISCORD_CLIENT_SECRET,
  DISCORD_BOT_TOKEN,
  DISCORD_ADMIN_ID = '',
  TURN_URL = '',
  TURN_USER = '',
  TURN_PASS = '',
  PUBLIC_ORIGIN: ORIGEM_CRUA = 'http://localhost:3001',
  SHARD_INDEX = '0',
  SHARD_NODES = '1',
  NODE_ORIGINS = '',
  PORT = 3001,
  NODE_ENV = 'development',
} = process.env;

// Uma barra sobrando no fim se propaga: o shareUrl vira "//share.html" e o
// redirect do OAuth vira "//auth/callback", que não bate com o endereço
// cadastrado no portal. O login falha sem explicar nada.
const PUBLIC_ORIGIN = ORIGEM_CRUA.replace(/[/]+$/, '');

// ------------------------------------------------------------------- sharding
//
// Este processo guarda as salas em memória e o relay só alcança os sockets dele
// mesmo. Rodar em mais de uma máquina exige que a call inteira — quem transmite
// e quem assiste — caia sempre na mesma; ver docs/sharding.md.
//
// O padrão é uma máquina só, e aí tudo abaixo é inerte: `nodeFor` devolve 0 para
// qualquer chave, nenhum pedido é recusado e nenhum prefixo aparece nas URLs. É
// o que roda na casa de quem só quer ver a tela de um amigo.
const NOS = Math.max(1, Math.trunc(Number(SHARD_NODES)) || 1);
const EU = Math.min(NOS - 1, Math.max(0, Math.trunc(Number(SHARD_INDEX)) || 0));
const FATIADO = NOS > 1;

// As origens públicas de cada máquina, na ordem dos índices. Servem para dizer a
// quem bateu na porta errada para onde ir, e para o shareUrl apontar para a
// máquina dona da sala.
const ORIGENS = String(NODE_ORIGINS)
  .split(/[\s,;]+/)
  .filter(Boolean)
  .map((o) => o.replace(/[/]+$/, ''));

if (FATIADO && ORIGENS.length !== NOS) {
  console.error(`ERRO: NODE_ORIGINS tem ${ORIGENS.length} endereco(s) para SHARD_NODES=${NOS}.`);
  console.error(
    '      Sem a lista completa nao ha como redirecionar quem chegar na maquina errada.',
  );
  process.exit(1);
}

/** A origem pública de uma máquina. Sem sharding, a deste processo. */
const origemDoNo = (i) => ORIGENS[i] ?? PUBLIC_ORIGIN;

/** O nó dono de uma chave de shard. */
const noDaChave = (key) => nodeFor(key, NOS);

/** A chave de shard de uma identidade ou de um token de sala. */
const chaveDe = (me) =>
  shardKey({ channel: me?.channel ?? me?.call ?? null, instance: me?.instance ?? null });

/**
 * Recusa o pedido quando a chave pertence a outra máquina.
 *
 * Devolve `true` quando recusou, para quem chama sair na hora. O cliente calcula
 * o mesmo nó sozinho e normalmente já bate no lugar certo — isto é a rede de
 * proteção para bundle velho, borda mal configurada e link antigo, casos em que
 * o sintoma sem esta checagem seria uma sala vazia, sem erro nenhum.
 */
function outraMaquina(key, res) {
  if (!FATIADO) return false;

  const dono = noDaChave(key);
  if (dono === EU) return false;

  res.setHeader('Cache-Control', 'no-store');
  res.status(409).json({
    error: 'wrong_node',
    node: dono,
    base: basePathFor(dono),
    origin: origemDoNo(dono),
  });
  return true;
}

const isProd = NODE_ENV === 'production';
// Mais de uma pessoa administra: separe os IDs por virgula. Um Set porque a
// unica pergunta feita aqui e "este ID esta na lista".
const ADMIN_IDS = new Set(
  String(DISCORD_ADMIN_ID)
    .split(/[\s,;]+/)
    .filter(Boolean),
);
const TEM_ADMIN = ADMIN_IDS.size > 0;
const ADMIN_COOKIE = 'discord_screen_admin';

// Falha no arranque, não no primeiro pedido: subir sem segredo significa
// assinar todos os tokens com o padrão público, e um servidor assim de pé é
// pior do que um servidor que não sobe.
if (isProd && !process.env.SESSION_SECRET) {
  console.error('ERRO: SESSION_SECRET obrigatorio em producao — sem ele os tokens sao forjaveis.');
  process.exit(1);
}

if (TEM_ADMIN && !process.env.SESSION_SECRET) {
  console.error('ERRO: SESSION_SECRET obrigatorio quando o painel admin esta ligado.');
  process.exit(1);
}

// O painel inteiro se apoia num cookie assinado com este segredo. Um segredo
// curto é adivinhável fora daqui, sem deixar rastro no servidor: quem acertar
// forja o cookie e entra como dono. O comando de configuração gera 64
// caracteres; este piso só barra quem editou o .env na mão e pôs qualquer coisa.
if (TEM_ADMIN && process.env.SESSION_SECRET.length < 32) {
  // Nomeia a variavel e desmente o engano que ela ja causou: quem acabou de
  // preencher o DISCORD_ADMIN_ID le "minimo 32" e conclui que o ID do Discord,
  // de 18 digitos, e que esta curto. Nao e — sao duas variaveis diferentes.
  console.error(
    `ERRO: SESSION_SECRET curto demais (tem ${process.env.SESSION_SECRET.length}, precisa de 32+).`,
  );
  console.error('      Nao e o DISCORD_ADMIN_ID: o ID do Discord tem 18 digitos e esta certo.');
  console.error(
    `      Gere um: node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"`,
  );
  process.exit(1);
}

for (const id of ADMIN_IDS) {
  if (/^[0-9]{15,21}$/.test(id)) continue;
  console.error(`ERRO: DISCORD_ADMIN_ID invalido: "${id}".`);
  console.error(
    '      Use o ID numerico da conta Discord (18 digitos). Varios: separe por virgula.',
  );
  process.exit(1);
}

// Sem painel, ninguém lê as métricas — então nem começa a medir.
if (TEM_ADMIN) startSampling();

const app = express();

// O proxy do Discord entrega as requisições da Activity sob o prefixo /.proxy.
// Se ele chega até aqui, toda rota vira 404 e o cliente espera para sempre por
// uma resposta que não vem — o sintoma é o "Está demorando…" do arranque, com
// o servidor de pé e os logs limpos.
//
// Nem sempre chega: depende de como a hospedagem e o mapeamento de URL do
// portal repassam o caminho. Tirar sempre custa uma comparação de string e faz
// o servidor funcionar nos dois casos, em vez de depender de qual borda está na
// frente. Fora da Activity nenhum caminho legítimo começa com /.proxy, então
// para quem abre o site direto isto é inerte.
//
// O mesmo já era feito no upgrade do WebSocket, mais abaixo; faltava no HTTP.
app.use((req, _res, next) => {
  // Fronteira de caminho, não de texto: /.proxyable é outra rota, não esta
  // com sufixo. Sem a barra, ela viraria /able em silêncio.
  if (req.url === '/.proxy' || req.url.startsWith('/.proxy/')) {
    req.url = req.url.slice('/.proxy'.length) || '/';
    // originalUrl junto: é dele que o serve-static monta o Location de um
    // redirecionamento, e sem atualizar ele mandaria a pessoa de volta ao
    // caminho prefixado — um salto a mais para chegar no mesmo lugar.
    req.originalUrl = req.url;
  }
  next();
});

// Depois do /.proxy vem o prefixo da máquina: /n0, /n1, … É por ele que a borda
// — as URL mappings do portal do Discord, ou um proxy próprio — entrega a call
// na máquina dona dela. Ver docs/sharding.md.
app.use((req, res, next) => {
  // A query fica de fora do corte: `stripNode` decide por caminho, e um "?"
  // logo depois do prefixo faria a fronteira falhar sem motivo.
  const corte = req.url.indexOf('?');
  const caminho = corte < 0 ? req.url : req.url.slice(0, corte);
  const busca = corte < 0 ? '' : req.url.slice(corte);

  const { index, path: limpo } = stripNode(caminho);
  if (index === null) return next();

  // Prefixo de outra máquina quer dizer que a borda roteou errado. Responder
  // isso em vez de seguir e cair no 404 do catch-all é o que separa "o
  // mapeamento está errado" de "a sala não existe" — e é a diferença entre
  // olhar o painel do Discord e procurar um bug que não existe no código.
  if (index !== EU) {
    res.setHeader('Cache-Control', 'no-store');
    return res.status(421).json({ error: 'misdirected', node: EU, base: basePathFor(EU) });
  }

  req.url = limpo + busca;
  req.originalUrl = req.url;
  next();
});

// CORS entre as máquinas do mesmo conjunto.
//
// Só existe por causa de quem abre o site fora do Discord. Ali não há proxy nem
// mapeamento de caminho, então o cliente fala com a máquina da sala pela origem
// absoluta dela — e um POST com Content-Type: application/json dispara
// verificação prévia. Sem estes headers o navegador barra o pedido antes de ele
// sair, e o sintoma é uma sala que nunca abre com o servidor intacto do outro
// lado. Dentro da Activity nada disto é usado: lá é tudo mesma origem.
//
// A lista permitida são as próprias máquinas e o domínio de entrada, nunca `*`.
// E sem `Allow-Credentials` de propósito: o acesso à sala viaja em token no
// corpo, não em cookie, então liberar cookie aqui só ampliaria a superfície sem
// habilitar nada. O painel admin, que é o único que usa cookie, não atravessa
// máquina nenhuma.
const WT_PUBLIC_ORIGIN = (() => {
  try {
    return new URL(process.env.WEBTRANSPORT_PUBLIC_URL).origin;
  } catch {
    return null;
  }
})();
const ORIGENS_OK = new Set([PUBLIC_ORIGIN, ...ORIGENS, WT_PUBLIC_ORIGIN].filter(Boolean));

function origemPermitida(origin, { required = false } = {}) {
  if (!origin) return !required;
  return ORIGENS_OK.has(String(origin));
}

app.use((req, res, next) => {
  // Numa máquina só não há origem cruzada para permitir, e o middleware sai
  // antes de tocar em qualquer coisa — inclusive no OPTIONS, que hoje cai no
  // catch-all e deve continuar caindo.
  if (!FATIADO) return next();

  const origem = req.headers.origin;

  if (origem && ORIGENS_OK.has(origem)) {
    res.setHeader('Access-Control-Allow-Origin', origem);
    // A resposta muda conforme a origem que pediu: sem isto um cache
    // intermediário serviria a permissão de uma para outra.
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Access-Control-Max-Age', '86400');
  }

  // A verificação prévia morre aqui: ela não tem corpo e não deve cair em rota
  // nenhuma.
  if (req.method === 'OPTIONS') return res.status(204).end();

  next();
});

app.use(express.json());

// Uma Activity roda dentro de um iframe em <id>.discordsays.com, que por sua
// vez está dentro do discord.com. Declarar essa cadeia é o que autoriza o
// navegador a desenhar a página ali.
//
// Havia aqui uma nota dizendo que, se a borda da hospedagem carimbasse
// "X-Frame-Options: SAMEORIGIN", não haveria nada a fazer deste lado. Estava
// errado, e o custo do engano foi um retângulo branco no Discord com log limpo
// e o mesmo endereço funcionando quando aberto direto.
//
// O frame-ancestors realmente não resolve sozinho: o proxy do Discord repassa o
// X-Frame-Options da origem sem repassar o nosso CSP, então quem decide é aquele
// header. Só que dá para desarmá-lo mandando o nosso — "ALLOWALL" não existe no
// padrão, e é justamente por isso que serve: diante de um valor que não
// reconhece, o navegador descarta o header inteiro. Isso só funciona onde a
// borda adiciona o dela apenas quando a origem não mandou nenhum; se ela
// sobrescrever, aí sim não há conserto daqui.
//
// Não é buraco de segurança: quem restringe o embutimento é o frame-ancestors
// acima, que tem precedência sobre o X-Frame-Options em qualquer navegador
// atual. O que se perde é uma proteção que este servidor nunca enviou.
//
// O Cloudflare-Frame-Options é o pedido explícito para a borda não injetar o
// dela. Fora de uma borda que o entenda é um header desconhecido, ignorado por
// navegador e por proxy.
app.use((_req, res, next) => {
  res.setHeader(
    'Content-Security-Policy',
    "frame-ancestors 'self' https://discord.com https://*.discord.com https://*.discordsays.com",
  );
  res.setHeader('X-Frame-Options', 'ALLOWALL');
  res.setHeader('Cloudflare-Frame-Options', 'allow');
  next();
});

// Página de captura (broadcaster). Servida como página normal, fora do proxy.
// Nomes fixos (share.html/js/css), então nunca cachear: senão uma correção
// fica presa no navegador de quem transmite, sem jeito óbvio de perceber.
// extensions: o portal do Discord pede as URLs de termos e privacidade, e
// "/termos" se lê melhor do que "/termos.html" — sem isto o catch-all lá
// embaixo devolveria a Activity para esses dois caminhos.
app.use(
  express.static(path.join(__dirname, 'public'), {
    extensions: ['html'],
    setHeaders: (res) => res.setHeader('Cache-Control', 'no-store'),
  }),
);

// Pipeline de transmissão compartilhado com a Activity. Ela o recebe pelo
// bundle do Vite; a página de captura importa daqui.
app.use(
  '/shared',
  express.static(path.join(__dirname, '..', 'shared'), {
    setHeaders: (res) => res.setHeader('Cache-Control', 'no-store'),
  }),
);

// ------------------------------------------------------------------ OAuth

/** Troca o code do OAuth pelo access_token. O secret nunca sai do servidor. */
app.post('/api/token', async (req, res) => {
  const { code, client_id } = req.body ?? {};
  if (!code) return res.status(400).json({ error: 'code obrigatorio' });

  // A metade que autoriza é a aplicação que abriu a atividade; a metade que
  // troca o código é este servidor. Se forem aplicações diferentes, o Discord
  // recusa — e o erro dele não diz qual das duas está errada.
  if (client_id && DISCORD_CLIENT_ID && client_id !== DISCORD_CLIENT_ID) {
    console.error(
      `[oauth] atividade e da aplicacao ${client_id}, mas o .env tem ${DISCORD_CLIENT_ID}`,
    );
    return res.status(409).json({
      error:
        `Esta atividade é da aplicação ${client_id}, mas o servidor está configurado ` +
        `com a ${DISCORD_CLIENT_ID}. As duas precisam ser a mesma.`,
    });
  }

  // Sem credencial não há troca possível, e o erro que o Discord devolve nesse
  // caso não deixa isso óbvio para ninguém. Dizer aqui poupa a caçada.
  if (!DISCORD_CLIENT_ID || !DISCORD_CLIENT_SECRET) {
    console.error('[oauth] DISCORD_CLIENT_ID ou DISCORD_CLIENT_SECRET ausente no .env');
    return res.status(500).json({
      error: 'O servidor está sem as credenciais do Discord. Rode: npm run configurar',
    });
  }

  try {
    const r = await fetch('https://discord.com/api/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: DISCORD_CLIENT_ID,
        client_secret: DISCORD_CLIENT_SECRET,
        grant_type: 'authorization_code',
        code,
      }),
    });

    const data = await r.json();
    if (!data.access_token) {
      console.error('[oauth] Discord recusou a troca:', data);
      // O motivo do Discord vai junto: "invalid_client" é secret errado,
      // "invalid_grant" é código já usado ou expirado. Sem isso, quem vê a
      // tela não tem como saber qual dos dois é.
      const motivo = data.error_description || data.error || 'motivo não informado';
      return res.status(401).json({ error: `O Discord recusou o login: ${motivo}` });
    }
    res.json({ access_token: data.access_token });
  } catch (err) {
    console.error('[oauth] erro:', err);
    res.status(500).json({ error: 'erro interno' });
  }
});

/**
 * Identidade da pessoa nesta instância da Activity.
 *
 * Separada das salas de propósito: cada operação de sala valida este token
 * assinado em vez de bater no Discord de novo, o que custaria uma ida à rede a
 * cada clique.
 */
app.post('/api/session', async (req, res) => {
  const { access_token, instance_id, guild_id, channel_id } = req.body ?? {};
  if (!access_token || !instance_id) {
    return res.status(400).json({ error: 'access_token e instance_id obrigatorios' });
  }

  // Validado antes da chave de shard, e não depois como estava: um channel_id
  // recusado pelo formato não entra no token, e a sala vira `atividade-<id>`.
  // Fatiar pelo valor cru mandaria a sessão para uma máquina e as chamadas de
  // sala seguintes para outra.
  const guildId = /^[0-9]{15,21}$/.test(String(guild_id ?? '')) ? String(guild_id) : null;
  const channelId = /^[0-9]{15,21}$/.test(String(channel_id ?? '')) ? String(channel_id) : null;

  // A checagem vem antes de falar com o Discord: recusar depois gastaria duas
  // idas à API para nada. O cliente já calculou este mesmo nó antes de chamar,
  // então na prática isto só dispara para bundle velho ou borda mal roteada.
  if (outraMaquina(shardKey({ channel: channelId, instance: instance_id }), res)) return;

  try {
    const me = await fetch('https://discord.com/api/users/@me', {
      headers: { Authorization: `Bearer ${access_token}` },
    }).then((r) => r.json());

    if (!me?.id) return res.status(401).json({ error: 'token invalido' });

    const [presenca, guildName] = await Promise.all([
      inVoiceChannel(guildId, channelId, me.id),
      resolveGuildName(guildId),
    ]);
    if (presenca === 'fora') {
      return res.status(403).json({ error: 'Entre na call antes de abrir a atividade.' });
    }

    // O canal entra no token assinado, não fica só na resposta: é o que permite
    // ao endpoint da sala da call confiar sem consultar o Discord de novo.
    const verificado = {
      ...(presenca === 'ok' ? { call: channelId } : {}),
      ...(guildId ? { guild: guildId } : {}),
      ...(guildName ? { guildName } : {}),
      ...(channelId ? { channel: channelId } : {}),
    };

    const identity = issueIdentity(
      instance_id,
      me.id,
      me.global_name || me.username,
      me.avatar ?? null,
      8 * 60 * 60,
      verificado,
    );

    // A sala vai junto da sessão em vez de custar uma segunda ida.
    //
    // O /api/rooms/call não faria nada que não pudesse ser feito aqui: ele
    // recebe a identidade que acabamos de assinar e deriva a sala do canal, que
    // já está nas mãos. Numa hospedagem distante cada ida e volta é fixa e cara
    // — medimos ~400ms por requisição, independente do que a rota faz —, então
    // a que dá para não fazer vale mais que qualquer micro-otimização dentro
    // dela.
    //
    // O /api/rooms/call continua existindo: é por onde entra quem já tem
    // identidade e voltou depois, sem refazer o login.
    const comoMe = {
      uid: me.id,
      name: me.global_name || me.username,
      av: me.avatar ?? null,
      instance: instance_id,
      ...verificado,
    };
    const salaDela = R.ensureCallRoom(comoMe.instance, salaDaCall(comoMe), {
      guildId: comoMe.guild ?? null,
      guildName: comoMe.guildName ?? null,
      channelId: comoMe.channel ?? comoMe.call ?? null,
    });

    res.json({
      ...identity,
      call: presenca === 'ok' ? channelId : null,
      guild: guildId,
      guildName,
      channel: channelId,
      sala: issueRoomTokens(salaDela.id, comoMe),
    });
  } catch (err) {
    console.error('[session] erro:', err);
    res.status(500).json({ error: 'erro interno' });
  }
});

/**
 * Identidade de convidado: entra sem conta.
 *
 * O login do Discord é uma melhoria opcional, não um pedágio — exigir conta só
 * para assistir uma tela afastaria justamente quem recebeu um link.
 *
 * Validade longa de propósito: o id do convidado é o que amarra a posse das
 * salas que ele criou, e perder isso no meio do uso seria pior que o risco de
 * um token de convidado antigo, que não dá acesso a nada além do lobby público.
 */
/**
 * Identidade de teste com instância à escolha. Fora do ar em produção: poder
 * escolher a instância permitiria espiar as salas de qualquer canal de voz.
 */
app.post('/api/session-dev', (req, res) => {
  if (isProd) return res.status(404).end();
  const { instance_id = 'dev', name = 'Dev', call = null } = req.body ?? {};
  res.json(
    issueIdentity(instance_id, `dev-${name}`, name, null, 8 * 60 * 60, call ? { call } : {}),
  );
});

app.post('/api/session-guest', (req, res) => {
  const raw = String(req.body?.name ?? '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 32);
  const name = raw || `Convidado ${Math.floor(Math.random() * 9000 + 1000)}`;
  const uid = `guest-${crypto.randomBytes(8).toString('base64url')}`;
  res.json(issueIdentity(WEB_INSTANCE, uid, name, null, 30 * 24 * 60 * 60));
});

function issueIdentity(instance, uid, name, avatar, ttl = 8 * 60 * 60, extra = {}) {
  return {
    user: { id: uid, name, avatar },
    instance,
    identity: signToken({ instance, uid, name, av: avatar, scope: 'identity', ...extra }, ttl),
  };
}

const guildCache = new Map();

/**
 * O nome de um servidor, pelo bot.
 *
 * Sem ampliar escopo de OAuth: quando o bot não está no servidor, o painel
 * continua funcional e mostra o ID no lugar do nome.
 *
 * Cache de uma hora. O painel se atualiza de 2 em 2 segundos, e sem cache isso
 * viraria uma chamada por servidor a cada volta.
 */
async function resolveGuildName(guildId) {
  if (!DISCORD_BOT_TOKEN || !guildId) return null;

  const cached = guildCache.get(guildId);
  if (cached && cached.expiresAt > Date.now()) return cached.name;

  let name = null;
  try {
    const response = await fetch(`https://discord.com/api/v10/guilds/${guildId}`, {
      headers: { Authorization: `Bot ${DISCORD_BOT_TOKEN}` },
      signal: AbortSignal.timeout(5000),
    });
    const guild = response.ok ? await response.json() : null;
    if (typeof guild?.name === 'string') name = guild.name;
  } catch {
    name = null;
  }

  guildCache.set(guildId, {
    name,
    expiresAt: Date.now() + (name ? 60 * 60 * 1000 : 10 * 60 * 1000),
  });
  return name;
}

/**
 * Confirma pelo Discord que a pessoa está mesmo naquela call.
 *
 * Sem isto o escopo por canal é obscuridade, não segurança: o `instance_id`
 * vem do cliente, e um cliente adulterado pode alegar qualquer canal. Aqui
 * quem responde é o Discord, com o token do bot.
 *
 * @returns {'ok'|'fora'|'indisponivel'}
 */
async function inVoiceChannel(guildId, channelId, userId) {
  if (!DISCORD_BOT_TOKEN || !guildId || !channelId) return 'indisponivel';

  try {
    const r = await fetch(`https://discord.com/api/v10/guilds/${guildId}/voice-states/${userId}`, {
      headers: { Authorization: `Bot ${DISCORD_BOT_TOKEN}` },
    });

    if (r.status === 404) {
      // Dois 404 bem diferentes chegam aqui, e tratá-los igual trancava a
      // atividade para fora.
      //
      // "Unknown Guild" (10004) quer dizer que o BOT não está neste servidor —
      // o caso de quem instalou a atividade na própria conta, sem adicionar bot
      // nenhum. Isso não diz nada sobre a pessoa estar em call: é falta de
      // visibilidade nossa, não ausência dela. Antes virava "fora", o que
      // devolvia 403 e a atividade abria em "Não foi possível entrar".
      //
      // Qualquer outro 404 é o que o nome sugere: não há estado de voz para
      // essa pessoa neste servidor, então ela está fora da call.
      const erro = await r.json().catch(() => ({}));
      if (erro?.code === 10004) {
        console.warn('[voz] o bot nao esta neste servidor — escopo cai para a instancia');
        return 'indisponivel';
      }
      return 'fora';
    }
    if (!r.ok) {
      console.warn(`[voz] Discord respondeu ${r.status} — verificação ignorada`);
      return 'indisponivel';
    }

    const state = await r.json();
    return state?.channel_id === channelId ? 'ok' : 'fora';
  } catch (err) {
    // Falha de rede não pode trancar todo mundo para fora.
    console.warn('[voz] falhou:', err.message);
    return 'indisponivel';
  }
}

/**
 * Espelho do avatar do Discord.
 *
 * O CSP da Activity bloqueia cdn.discordapp.com, e o proxy do Discord só
 * repassa domínios mapeados no portal do desenvolvedor. Servindo pelo nosso
 * próprio /api, a mesma URL funciona dentro e fora da Activity, sem depender
 * de configuração que ninguém lembra de fazer.
 *
 * O id e o hash são validados no formato exato do Discord: sem isso a rota
 * viraria um proxy aberto, com o servidor buscando qualquer URL que pedissem.
 */
const AVATAR_ID = /^[0-9]{15,21}$/;
const AVATAR_HASH = /^(a_)?[0-9a-f]{32}$/;

// Cache em memória: o hash muda quando a pessoa troca a foto, então a chave
// nunca fica velha. Sem ele, montar a grade numa sala cheia vira uma ida ao
// CDN do Discord por avatar, e a espera aparece como a sala demorando a abrir.
// Teto pequeno de propósito — são poucos KB por imagem e uma sala tem dezenas
// de pessoas, não milhares.
const AVATAR_CACHE = new Map();
const AVATAR_CACHE_MAX = 200;

app.get('/api/avatar/:id/:hash', async (req, res) => {
  const { id, hash } = req.params;
  if (!AVATAR_ID.test(id) || !AVATAR_HASH.test(hash)) return res.status(400).end();

  // Tipo fixo, não o que o upstream disser: pedimos .png e é png que sai.
  res.setHeader('Content-Type', 'image/png');
  // O hash muda quando a pessoa troca a foto, então a URL é imutável.
  res.setHeader('Cache-Control', 'public, max-age=86400, immutable');

  const chave = `${id}/${hash}`;
  const guardado = AVATAR_CACHE.get(chave);
  if (guardado) return res.end(guardado);

  try {
    const upstream = await fetch(`https://cdn.discordapp.com/avatars/${id}/${hash}.png?size=128`, {
      // O CDN fora do ar não pode virar uma sala que não abre.
      signal: AbortSignal.timeout(5000),
    });
    if (!upstream.ok) return res.status(404).end();

    const imagem = Buffer.from(await upstream.arrayBuffer());
    // Descarta o mais antigo primeiro; a ordem de inserção do Map basta.
    if (AVATAR_CACHE.size >= AVATAR_CACHE_MAX) {
      AVATAR_CACHE.delete(AVATAR_CACHE.keys().next().value);
    }
    AVATAR_CACHE.set(chave, imagem);

    res.end(imagem);
  } catch {
    res.status(502).end();
  }
});

/** Valida o token de identidade que acompanha toda operação de sala. */
function identityOf(req, res) {
  const payload = verifyToken(req.body?.identity);
  if (!payload || payload.scope !== 'identity') {
    res.status(401).json({ error: 'identidade invalida ou expirada' });
    return null;
  }
  return payload;
}

/**
 * Tokens de acesso a uma sala, emitidos depois de passar pela senha.
 *
 * Sem prazo de validade: quem entrou fica. A sala fecha ao esvaziar e o id é
 * aleatório, então o token morre junto com ela.
 *
 * O shareUrl aponta para a origem da máquina dona da sala, não para a deste
 * processo. É o que faz a aba de captura carregar já do lugar certo — e é por
 * isso que `server/public/share.js` não precisa saber que existe sharding: ele
 * monta o endereço do socket a partir do `location.host`, que a essa altura já
 * é o da máquina certa.
 */
function issueRoomTokens(roomId, me) {
  const base = {
    room: roomId,
    uid: me.uid,
    name: me.name,
    av: me.av ?? null,
    guild: me.guild ?? null,
    channel: me.channel ?? me.call ?? null,
  };
  const dono = noDaChave(chaveDe(me));

  return {
    roomId,
    // Quem manda nesta sala, dito por quem sabe. O cliente calculava isso
    // sozinho, e quando a config atrasava ele errava — e o WebSocket, ao
    // contrário do HTTP, não tinha como se corrigir depois. Vindo na resposta,
    // não há o que atrasar nem o que adivinhar.
    //
    // Só quando há mais de uma máquina: numa só, mandar `node` faria o cliente
    // prefixar as URLs com /n0, que exigiria mapeamento no portal do Discord
    // para uma divisão que não existe.
    ...(FATIADO ? { node: dono } : {}),
    viewerToken: signToken({ ...base, role: 'viewer' }),
    shareUrl: `${origemDoNo(dono)}/share.html?t=${encodeURIComponent(
      signToken({ ...base, role: 'broadcaster' }),
    )}`,
  };
}

// ---------------------------------------------------------------------- salas

/**
 * Listar não exige login: dá para ver o lobby antes de entrar.
 *
 * Criar e entrar continuam exigindo identidade — sem isso não haveria dono de
 * sala nem nome de participante.
 */
app.post('/api/rooms/list', (req, res) => {
  const me = verifyToken(req.body?.identity);
  const identificado = me?.scope === 'identity';
  const instance = identificado ? me.instance : WEB_INSTANCE;

  // Todas as salas de um canal moram na mesma máquina, então a lista de uma
  // instância nunca está partida — mas quem perguntar no lugar errado receberia
  // uma lista vazia em vez de um erro, que é o pior tipo de resposta errada.
  if (outraMaquina(identificado ? chaveDe(me) : WEB_INSTANCE, res)) return;

  res.json({ rooms: R.listRooms(instance) });
});

app.post('/api/rooms/create', (req, res) => {
  const me = identityOf(req, res);
  if (!me) return;
  if (outraMaquina(chaveDe(me), res)) return;

  const { room, error } = R.createRoom({
    instance: me.instance,
    name: req.body?.name,
    ownerId: me.uid,
    ownerName: me.name,
    password: req.body?.password || null,
    guildId: me.guild ?? null,
    guildName: me.guildName ?? null,
    channelId: me.channel ?? null,
  });
  if (error) return res.status(400).json({ error });

  console.log(`[room ${room.id}] criada por ${me.name}: "${room.name}"`);
  res.json(issueRoomTokens(room.id, me));
});

/**
 * A sala desta call. É a única sala que existe dentro do Discord: a atividade
 * abre nela direto, sem lista, porque escolher entre uma opção só não é escolha.
 *
 * Com o token do bot configurado, a porta é a presença no canal de voz,
 * confirmada pelo próprio Discord. Sem ele, a porta é a instância da atividade
 * — o mesmo escopo que a lista de salas sempre usou, então nada se afrouxa, e a
 * atividade continua funcionando para quem não quer criar um bot.
 */
const salaDaCall = (me) => (me.call ? `call-${me.call}` : `atividade-${me.instance}`);

app.post('/api/rooms/call', (req, res) => {
  const me = identityOf(req, res);
  if (!me) return;
  if (outraMaquina(chaveDe(me), res)) return;

  const room = R.ensureCallRoom(me.instance, salaDaCall(me), {
    guildId: me.guild ?? null,
    guildName: me.guildName ?? null,
    channelId: me.channel ?? me.call ?? null,
  });
  res.json(issueRoomTokens(room.id, me));
});

app.post('/api/rooms/join', (req, res) => {
  const me = identityOf(req, res);
  if (!me) return;
  if (outraMaquina(chaveDe(me), res)) return;

  const room = R.getRoom(req.body?.roomId);
  if (!room) return res.status(404).json({ error: 'Sala não existe mais.' });

  // A sala da call é avaliada antes da instância: quem manda nela é a presença
  // no canal, confirmada pelo Discord. Checar instância aqui recusaria por
  // motivo errado, já que o id dela vem do canal e não da instância.
  if (room.isCall) {
    if (room.id !== salaDaCall(me)) {
      return res.status(403).json({ error: 'Entre na call para acessar esta sala.' });
    }
    return res.json(issueRoomTokens(room.id, me));
  }

  // Salas comuns: as de um canal de voz não aparecem nem abrem em outro.
  if (room.instance !== me.instance) {
    return res.status(404).json({ error: 'Sala não existe mais.' });
  }

  const check = R.checkPassword(room, req.body?.password);
  if (!check.ok) {
    return res.status(check.reason === 'bloqueado' ? 429 : 403).json({
      error:
        check.reason === 'bloqueado'
          ? `Muitas tentativas. Tente de novo em ${check.seconds}s.`
          : 'Senha incorreta.',
      reason: check.reason,
    });
  }

  res.json(issueRoomTokens(room.id, me));
});

/**
 * Abre no site uma sala em que já se entrou pela atividade.
 *
 * O join normal não serve: a sala da call é recusada a quem não está no canal
 * de voz, e uma sessão do site nunca está — é justamente isso que faz dela uma
 * sala do Discord. Mas quem já entrou saiu de lá com um viewerToken assinado, e
 * ele prova que a porta já se abriu uma vez para aquela pessoa.
 *
 * O token vale como ingresso, não como identidade emprestada: os tokens
 * devolvidos sao reemitidos a partir do que está assinado dentro dele, entao
 * ninguém troca de nome no caminho. E vale enquanto a sala existir — ela morre
 * ao esvaziar, e o ingresso morre junto.
 */
app.post('/api/rooms/open', (req, res) => {
  const ingresso = verifyToken(req.body?.token);
  if (!ingresso?.room || ingresso.role !== 'viewer') {
    return res.status(401).json({ error: 'Link inválido ou expirado.' });
  }

  // A chave sai do próprio ingresso, que leva o canal assinado dentro dele. Uma
  // sala do site não tem canal e cai no lobby, que é onde ela mora mesmo.
  if (outraMaquina(chaveDe(ingresso), res)) return;

  const room = R.getRoom(ingresso.room);
  if (!room) return res.status(404).json({ error: 'Sala não existe mais.' });

  res.json({ ...issueRoomTokens(room.id, ingresso), name: room.name });
});

app.post('/api/rooms/password', (req, res) => {
  const me = identityOf(req, res);
  if (!me) return;
  if (outraMaquina(chaveDe(me), res)) return;

  const room = R.getRoom(req.body?.roomId);
  if (!room || room.instance !== me.instance) {
    return res.status(404).json({ error: 'Sala não existe mais.' });
  }

  const error = R.setPassword(room, me.uid, req.body?.password || null);
  if (error) return res.status(403).json({ error });

  res.json({ ok: true, locked: Boolean(room.password) });
});

// ------------------------------------------------- login web (fora do Discord)

// Quem entra pelo site não tem canal de voz, então todas essas pessoas
// compartilham um lobby só.
const WEB_INSTANCE = 'web';
const REDIRECT_URI = `${PUBLIC_ORIGIN}/auth/callback`;

function discordAuthorizeUrl(state = null) {
  const url = new URL('https://discord.com/oauth2/authorize');
  url.searchParams.set('client_id', DISCORD_CLIENT_ID);
  url.searchParams.set('redirect_uri', REDIRECT_URI);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', 'identify');
  if (state) url.searchParams.set('state', state);
  return url;
}

app.get('/auth/login', (_req, res) => {
  const url = discordAuthorizeUrl();
  res.redirect(url.toString());
});

app.get('/admin/auth/login', (_req, res) => {
  if (!TEM_ADMIN) return res.redirect('/admin?error=not_configured');
  if (!DISCORD_CLIENT_ID || !DISCORD_CLIENT_SECRET) {
    return res.redirect('/admin?error=discord_not_configured');
  }

  const state = signToken(
    { scope: 'oauth-state', target: 'admin', nonce: crypto.randomBytes(12).toString('base64url') },
    10 * 60,
  );
  res.redirect(discordAuthorizeUrl(state).toString());
});

app.get('/auth/callback', async (req, res) => {
  const { code, state } = req.query;
  const oauthState = verifyToken(typeof state === 'string' ? state : '');
  const adminFlow = oauthState?.scope === 'oauth-state' && oauthState.target === 'admin';
  if (!code) return res.redirect(adminFlow ? '/admin?error=sem_codigo' : '/?erro=sem_codigo');

  try {
    const token = await fetch('https://discord.com/api/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: DISCORD_CLIENT_ID,
        client_secret: DISCORD_CLIENT_SECRET,
        grant_type: 'authorization_code',
        redirect_uri: REDIRECT_URI,
        code: String(code),
      }),
    }).then((r) => r.json());

    if (!token.access_token) {
      return res.redirect(adminFlow ? '/admin?error=troca_falhou' : '/?erro=troca_falhou');
    }

    const me = await fetch('https://discord.com/api/users/@me', {
      headers: { Authorization: `Bearer ${token.access_token}` },
    }).then((r) => r.json());

    if (!me?.id) {
      return res.redirect(adminFlow ? '/admin?error=perfil_falhou' : '/?erro=perfil_falhou');
    }

    if (adminFlow) {
      if (!ADMIN_IDS.has(me.id)) return res.redirect('/admin?error=forbidden');

      const adminSession = signToken(
        {
          scope: 'admin',
          uid: me.id,
          name: me.global_name || me.username,
          av: me.avatar ?? null,
        },
        8 * 60 * 60,
      );
      const secure = PUBLIC_ORIGIN.startsWith('https://') ? '; Secure' : '';
      res.setHeader(
        'Set-Cookie',
        `${ADMIN_COOKIE}=${adminSession}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${8 * 60 * 60}${secure}`,
      );
      return res.redirect('/admin');
    }

    const identity = issueIdentity(
      WEB_INSTANCE,
      me.id,
      me.global_name || me.username,
      me.avatar ?? null,
    );

    // No fragmento, não na query: o fragmento não é enviado ao servidor nem
    // aparece em log de proxy. O cliente lê e limpa da barra de endereço.
    res.redirect(`/#identity=${encodeURIComponent(identity.identity)}`);
  } catch (err) {
    console.error('[auth] erro:', err);
    res.redirect(adminFlow ? '/admin?error=interno' : '/?erro=interno');
  }
});

app.get('/api/health', (_req, res) => res.json({ ok: true }));

let webtransport = null;
app.get('/api/transports', (_req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({
    websocket: true,
    node: EU,
    shards: NOS,
    webtransport: webtransport?.capability() ?? null,
  });
});

/**
 * Servidores ICE para a conexão direta entre quem transmite e quem assiste.
 *
 * O STUN público resolve a maioria das casas: ele só conta ao navegador qual é
 * o endereço externo dele, e a partir daí os dois lados se acham sozinhos. Quem
 * está atrás de NAT simétrico — operadora com CGNAT, rede corporativa — não se
 * acha de jeito nenhum, e para esses só um TURN resolve, porque ele encaminha o
 * vídeo de fato. Custa banda, então é opcional e vem por variável de ambiente.
 *
 * Sem TURN configurado ninguém fica sem transmissão: quem não conseguir fechar
 * a conexão direta continua vendo pelo relay, que nunca foi desligado.
 */
app.get('/api/ice', (_req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];

  if (TURN_URL) {
    const turn = { urls: TURN_URL };
    if (TURN_USER) turn.username = TURN_USER;
    if (TURN_PASS) turn.credential = TURN_PASS;
    iceServers.push(turn);
  }

  // Credencial de TURN é de curta duração e o cliente busca uma vez por sessão;
  // guardar em cache entregaria uma senha vencida na próxima transmissão.
  res.setHeader('Cache-Control', 'no-store');
  res.json({ iceServers });
});

function cookieOf(req, name) {
  for (const item of String(req.headers.cookie ?? '').split(';')) {
    const separator = item.indexOf('=');
    if (separator < 0 || item.slice(0, separator).trim() !== name) continue;
    try {
      return decodeURIComponent(item.slice(separator + 1).trim());
    } catch {
      return null;
    }
  }
  return null;
}

function adminOf(req) {
  const session = verifyToken(cookieOf(req, ADMIN_COOKIE));
  if (!session || session.scope !== 'admin' || !ADMIN_IDS.has(session.uid)) return null;
  return session;
}

function requireAdmin(req, res, next) {
  const admin = adminOf(req);
  if (!admin) {
    res.setHeader('Cache-Control', 'no-store');
    return res.status(401).json({ error: 'admin_required', configured: TEM_ADMIN });
  }
  req.admin = admin;
  res.setHeader('Cache-Control', 'no-store');
  next();
}

app.get('/api/admin/me', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!TEM_ADMIN) return res.status(503).json({ configured: false, error: 'not_configured' });
  const admin = adminOf(req);
  if (!admin) return res.status(401).json({ configured: true, error: 'admin_required' });
  res.json({
    configured: true,
    user: { id: admin.uid, name: admin.name, avatar: admin.av ?? null },
  });
});

app.post('/api/admin/logout', (_req, res) => {
  const secure = PUBLIC_ORIGIN.startsWith('https://') ? '; Secure' : '';
  res.setHeader(
    'Set-Cookie',
    `${ADMIN_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${secure}`,
  );
  res.setHeader('Cache-Control', 'no-store');
  res.json({ ok: true });
});

/** O painel desta máquina, sem perguntar a ninguém. */
const painelLocal = () =>
  buildAdminDashboard({
    roomState: R.adminStats(),
    sockets: wss.clients,
    system: systemSnapshot(),
    configuration: {
      environment: NODE_ENV,
      port: Number(PORT),
      publicOrigin: PUBLIC_ORIGIN,
      clientId: DISCORD_CLIENT_ID || null,
      botConfigured: Boolean(DISCORD_BOT_TOKEN),
      adminIds: [...ADMIN_IDS],
      sessionSecretConfigured: Boolean(process.env.SESSION_SECRET),
      node: EU,
      nodes: NOS,
    },
  });

/**
 * O painel desta máquina, para as outras máquinas do conjunto.
 *
 * Existe porque o cookie do painel é gravado no domínio de entrada e não é
 * enviado para os subdomínios — o navegador não consegue perguntar de máquina
 * em máquina sem um login em cada uma. Então quem pergunta é o servidor, e a
 * porta de entrada dele é esta, autenticada pelo segredo que as máquinas já
 * compartilham.
 *
 * Ela nunca pergunta a ninguém: é o que impede um leque virar recursão.
 */
app.get('/api/admin/local', (req, res) => {
  const token = verifyToken(String(req.headers.authorization ?? '').replace(/^Bearer /, ''));
  if (!FATIADO || token?.scope !== 'interno') return res.status(404).end();

  res.setHeader('Cache-Control', 'no-store');
  res.json(painelLocal());
});

// O leque custa uma ida a cada máquina. Dois admins com o painel aberto
// dobrariam isso sem necessidade — e o valor de um segundo atrás responde a
// mesma pergunta.
let painelCache = { quando: 0, valor: null };
const CACHE_PAINEL_MS = 1000;

/**
 * Pergunta o painel a uma máquina irmã. Nunca lança: máquina que não responde
 * vira uma linha `offline` no resultado, em vez de derrubar o painel inteiro.
 */
async function painelDe(index) {
  try {
    const r = await fetch(`${origemDoNo(index)}/n${index}/api/admin/local`, {
      headers: { Authorization: `Bearer ${signToken({ scope: 'interno' }, 60)}` },
      signal: AbortSignal.timeout(2500),
    });
    if (!r.ok) return { index, origin: origemDoNo(index), online: false, dashboard: null };
    return { index, origin: origemDoNo(index), online: true, dashboard: await r.json() };
  } catch {
    return { index, origin: origemDoNo(index), online: false, dashboard: null };
  }
}

app.get('/api/admin/metrics', requireAdmin, async (_req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  // Uma máquina só: nada a juntar, nada a perguntar.
  if (!FATIADO) return res.json(painelLocal());

  if (Date.now() - painelCache.quando < CACHE_PAINEL_MS && painelCache.valor) {
    return res.json(painelCache.valor);
  }

  // A própria primeiro: é dela a configuração e o `system` que sobem ao topo,
  // e ela é a única que com certeza responde.
  const eu = { index: EU, origin: origemDoNo(EU), online: true, dashboard: painelLocal() };
  const outras = await Promise.all(
    Array.from({ length: NOS }, (_, i) => i)
      .filter((i) => i !== EU)
      .map(painelDe),
  );

  // Sem ordenar: a junção usa a primeira da lista como base, e tem de ser a
  // que atendeu — é dela a configuração e o `system` que sobem ao topo. A
  // ordem por índice é reposta dentro da junção, só na quebra por máquina.
  const juntado = mergeAdminDashboards([eu, ...outras]);
  painelCache = { quando: Date.now(), valor: juntado };
  res.json(juntado);
});

/**
 * O que o cliente precisa saber e só o servidor sabe, em tempo de execução.
 *
 * `clientId` vinha embutido no bundle (VITE_DISCORD_CLIENT_ID). Isso obrigava a
 * rebuildar a cada troca de credencial, e esquecer o build não dava erro nenhum:
 * a Activity abria normalmente e só quebrava na hora do login, longe da causa.
 * O Client ID é público por natureza — aparece em toda URL de OAuth —, então
 * servi-lo aqui não expõe nada. O secret continua sem sair do servidor.
 *
 * `asset` é o carimbo do build atual, para a Activity perceber que está rodando
 * com um HTML velho. O index.html vai com no-store, mas o cliente do Discord
 * pode entregar uma cópia antiga assim mesmo.
 *
 * Já foi o nome do bundle, que levava hash de conteúdo. Não leva mais — o nome
 * virou fixo justamente para o HTML velho conseguir carregar o JS novo (ver
 * client/vite.config.js), e com isso o nome deixou de distinguir uma versão da
 * outra. Quem distingue agora é a meta `build`, carimbada no HTML pelo build.
 * O que se compara também mudou de objeto: o JS é sempre o atual, então o que
 * pode estar velho é o HTML, e é dele que sai o carimbo dos dois lados.
 */
app.get('/api/config', (_req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  let asset = null;
  try {
    const html = fs.readFileSync(path.join(clientDist, 'index.html'), 'utf8');
    asset = html.match(/<meta name="build" content="([A-Za-z0-9_-]+)"/)?.[1] ?? null;
  } catch {
    // Ainda sem build; em desenvolvimento quem serve o cliente é o Vite.
  }

  // || e nao ??: uma variavel vazia no .env chega como string vazia, e o
  // contrato aqui e "null significa nao configurado".
  //
  // `nodes` só serve a quem abre o site fora do Discord: lá não há proxy nem
  // mapeamento de caminho, então o cliente precisa da origem absoluta da máquina
  // para onde vai. Dentro da Activity o prefixo basta. Este endpoint não tem
  // estado, então qualquer máquina responde — é por isso que ele fica de fora do
  // roteamento por chave.
  res.json({
    clientId: DISCORD_CLIENT_ID || null,
    asset,
    shards: NOS,
    nodes: FATIADO ? ORIGENS : [],
  });
});

// Activity buildada (produção). Em dev o Vite serve o client na 5173.
const clientDist = path.join(__dirname, '..', 'client', 'dist');

app.use(
  express.static(clientDist, {
    // Nada aqui leva hash de conteúdo no nome. Já levou, e o `immutable` que
    // acompanhava era correto enquanto o nome mudava a cada build; agora o
    // nome é fixo (client/vite.config.js) e cachear para sempre prenderia a
    // Activity num build antigo — exatamente o defeito que o nome fixo veio
    // resolver, só que pior, porque duraria um ano.
    //
    // É a mesma política de server/public e de /shared, e pelo mesmo motivo:
    // nome fixo pede resposta fresca.
    setHeaders: (res) => res.setHeader('Cache-Control', 'no-store'),
  }),
);

// Ponte para o HTML da era do hash.
//
// O nome fixo resolve o problema de todo deploy futuro, mas não o do deploy que
// o introduziu: nesse, o cliente do Discord ainda tem guardado o index.html
// anterior, que pede assets/index-<hash>.js — apagado pelo build. Sem esta
// ponte ele toma 404, a Activity fica branca, e a espera dura o que o cache do
// Discord quiser, sem que ninguém do lado de cá possa apressar.
//
// Servir o bundle atual sob o nome antigo encerra a espera na hora: o HTML
// velho carrega o JS novo, que é exatamente a garantia que o nome fixo passou a
// dar dali em diante. Fica valendo também para qualquer cliente que reapareça
// meses depois com um HTML daquela era.
//
// Só os dois nomes que o Vite gerava, e só quando o arquivo de destino existe —
// em desenvolvimento não há build, e aí isto cai no next() como qualquer outro
// caminho que não resolve.
app.get(/^\/assets\/index-[A-Za-z0-9_-]+\.(js|css)$/, (req, res, next) => {
  const alvo = req.path.endsWith('.css') ? 'index.css' : 'app.js';
  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(path.join(clientDist, 'assets', alvo), (err) => err && next());
});

app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api')) return next();

  // Um arquivo precisa falhar como arquivo. Este catch-all existe para o
  // roteamento da aplicação — caminhos que a Activity resolve sozinha —, e sem
  // esta linha ele também respondia aos pedidos de asset que o static não
  // achou: 200 com text/html no lugar de um módulo JS. O navegador recusa
  // executar por MIME, nada roda, e a página fica em branco sem uma linha no
  // log, porque do lado de cá foi tudo 200. Um 404 honesto aparece no console
  // em três segundos.
  if (path.extname(req.path)) return next();

  res.setHeader('Cache-Control', 'no-store');
  res.sendFile(path.join(clientDist, 'index.html'), (err) => err && next());
});

// -------------------------------------------------------------- WebSocket

const server = createServer(app);
// maxPayload: o relay repassa o buffer intacto para todos os espectadores, então
// um quadro gigante de um transmissor adulterado sairia multiplicado por N. Um
// keyframe 1080p a 5 Mbps não passa de algumas centenas de KB.
// perMessageDeflate desligado: mídia H.264/Opus já vem entropicamente comprimida —
// zlib não tira um byte dela, só gasta CPU e adiciona latência quadro a quadro.
const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 4 * 1024 * 1024,
  perMessageDeflate: false,
});

server.on('upgrade', (req, socket, head) => {
  // O proxy do Discord entrega o caminho com o prefixo /.proxy/, e a borda do
  // sharding acrescenta o /nK antes dele chegar aqui.
  const url = new URL(req.url, 'http://localhost');
  const semProxy = url.pathname.replace(/^\/\.proxy/, '');
  const { index: no, path: pathname } = stripNode(semProxy);

  if (pathname !== '/ws') {
    socket.destroy();
    return;
  }

  if (!origemPermitida(req.headers.origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  if (no !== null && no !== EU) {
    socket.write('HTTP/1.1 421 Misdirected Request\r\n\r\n');
    socket.destroy();
    return;
  }

  const payload = verifyToken(url.searchParams.get('t'));
  // scope 'identity' não dá acesso a sala nenhuma: só os tokens de sala servem.
  if (!payload || !payload.room) {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
    return;
  }

  // O token de sala leva o canal assinado dentro dele, então dá para recusar
  // antes de aceitar a conexão. Sem isto, um socket entraria numa máquina que
  // não tem a sala e a pessoa ficaria olhando uma tela preta: o relay estaria
  // funcionando perfeitamente, do outro lado.
  if (FATIADO && noDaChave(chaveDe(payload)) !== EU) {
    socket.write('HTTP/1.1 409 Conflict\r\n\r\n');
    socket.destroy();
    return;
  }

  // A fonte não vai assinada, como `q` e `fps` também não vão: ela não
  // concede nada. Quem tem o token já pode transmitir nesta sala — a fonte só
  // rotula o stream e escolhe qual das duas vagas da pessoa é ocupada, e o teto
  // por pessoa é imposto no registro, não aqui.
  const pedida = url.searchParams.get('fonte');
  const fonte = R.FONTES.has(pedida) ? pedida : 'tela';
  // A aba de captura abre esta conexão ao carregar, antes de qualquer captura.
  const controle = url.searchParams.get('modo') === 'controle';

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req, payload, fonte, controle);
  });
});

function dispatchConnection(ws, auth, fonte, controle) {
  ws.__connectedAt = Date.now();
  ws.__rttMs = null;
  ws.__pingSentAt = null;
  const room = R.getRoom(auth.room);

  // A sala pode ter fechado entre a emissão do token e a conexão.
  if (!room) {
    R.sendJson(ws, { type: 'room-gone' });
    ws.close();
    return;
  }

  if (auth.role === 'broadcaster' && controle) {
    handleControl(ws, room, auth);
  } else if (auth.role === 'broadcaster') {
    handleBroadcaster(ws, room, { id: auth.uid, name: auth.name, avatar: auth.av ?? null }, fonte);
  } else {
    handleViewer(ws, room, auth);
  }
}

wss.on('connection', (ws, _req, auth, fonte, controle) => {
  dispatchConnection(ws, auth, fonte, controle);
});

webtransport = await startWebTransport({
  env: process.env,
  production: isProd,
  node: EU,
  sharded: FATIADO,
  allowedOrigins: ORIGENS_OK,
  sources: R.FONTES,
  verifyToken,
  nodeForToken: (auth) => noDaChave(chaveDe(auth)),
  roomExists: (roomId) => Boolean(R.getRoom(roomId)),
  onConnection: dispatchConnection,
  onState: ({ state, host, port, reason }) => {
    const address = host && port ? ` UDP ${host}:${port}` : '';
    const detail = reason ? ` (${reason})` : '';
    console.info(`[webtransport] ${state}${address}${detail}`);
  },
  onError: (error) => console.warn(`[webtransport] ${error?.message ?? error}`),
});
server.on('close', () => webtransport.stop());

/**
 * A aba de captura, sem mídia nenhuma: só recebe recados.
 *
 * Ela não transmite por aqui — quando começa, abre uma conexão de transmissão
 * separada, uma por fonte. Esta serve para a atividade alcançá-la enquanto
 * ainda não há nada no ar, que é justamente quando o `broadcastersOf` não
 * encontraria ninguém.
 */
function handleControl(ws, room, auth) {
  R.attachControl(room, ws, auth.uid);
  console.log(`[room ${room.id}] aba de captura de ${auth.name} conectada`);

  R.broadcastState(room);

  const sair = () => {
    R.detachControl(room, ws);
    R.broadcastState(room);
  };
  ws.on('close', sair);
  ws.on('error', sair);
}

function handleBroadcaster(ws, room, info, fonte) {
  const entry = R.attachBroadcaster(room, ws, info, fonte);

  if (typeof entry === 'string') {
    R.sendJson(ws, { type: 'error', message: entry });
    ws.close();
    return;
  }

  // WebTransport rejects media at the header boundary until the authenticated
  // broadcaster owns a concrete slot. Viewers/control sockets never receive
  // this capability, so valid framing cannot become unauthorized lane state.
  ws.authorizeMediaSlot?.(entry.slot);

  console.log(
    `[room ${room.id}] broadcaster conectado: ${info.name} · ${fonte} (slot ${entry.slot})`,
  );

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      R.pushChunk(room, entry, data);
      return;
    }

    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (msg.type === 'start') {
      R.startStream(room, entry);
      console.log(`[room ${room.id}] stream iniciada por ${info.name}`);
    } else if (msg.type === 'resume') {
      R.resumeStream(room, entry);
      console.log(`[room ${room.id}] stream retomada por ${info.name}`);
    } else if (msg.type === 'config' && msg.config) {
      R.setConfig(room, entry, msg.config);
      console.log(`[room ${room.id}] codec de ${info.name}: ${msg.config.codec}`);
    } else if (msg.type === 'audio-config' && msg.config) {
      R.setAudioConfig(room, entry, msg.config);
      console.log(`[room ${room.id}] audio de ${info.name}: ${msg.config.codec}`);
    } else if (msg.type === 'quality') {
      // O transmissor é dono da escada de qualidade; o servidor espelha o que
      // ele reporta em vez de manter uma cópia própria que possa divergir.
      // `setQuality` valida a forma na fronteira e ignora o que vier torto.
      R.setQuality(room, entry, msg);
    } else if (msg.type === 'rtc' && typeof msg.peer === 'string' && msg.payload) {
      R.rtcParaViewer(room, entry, msg.peer, msg.payload);
    } else if (msg.type === 'stop') {
      R.stopStream(room, entry);
      console.log(`[room ${room.id}] stream parada por ${info.name}`);
    }
  });

  ws.on('close', () => {
    const aguardando = R.suspendBroadcaster(room, ws);
    console.log(
      `[room ${room.id}] broadcaster ${aguardando ? 'reconectando' : 'saiu'}: ${info.name}`,
    );
  });
}

function handleViewer(ws, room, auth) {
  R.attachViewer(room, ws, { id: auth.uid, name: auth.name, avatar: auth.av ?? null });

  ws.on('media-drop', (slot) => R.pedirKeyframe(room, ws, slot));
  ws.on('transport-diagnostic', (event) => R.reportarPressaoTransporte(room, ws, event));

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;

    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    // Nome exibido escolhido pela pessoa. Nada é persistido: vale enquanto a
    // conexão durar, e some quando ela reabre a atividade.
    if (msg.type === 'rename') {
      R.rename(room, ws, msg.name);
      return;
    }

    if (msg.type === 'watch' && Number.isInteger(msg.slot)) {
      R.watch(room, ws, msg.slot);
      return;
    }

    if (msg.type === 'unwatch' && Number.isInteger(msg.slot)) {
      R.unwatch(room, ws, msg.slot);
      return;
    }

    // O transporte dele detectou buraco de mídia e o decodificador ficou sem
    // referência. Quem valida quem pode pedir o quê é o registro da sala.
    if (msg.type === 'need-keyframe') {
      R.pedirKeyframe(room, ws, msg.slot);
      return;
    }

    // Envelope de sinalização a caminho de quem transmite. O servidor não abre:
    // offer, answer e candidato só fazem sentido para as duas pontas.
    if (msg.type === 'rtc' && Number.isInteger(msg.slot) && msg.payload) {
      R.rtcParaBroadcaster(room, ws, msg.slot, msg.payload);
      return;
    }

    // A conexão direta assumiu (ou caiu). Só quem assiste sabe dizer, porque só
    // ele vê quadro chegando — e é isso que liga e desliga o relay para ele.
    if (msg.type === 'rtc-ativo' && Number.isInteger(msg.slot)) {
      R.rtcAtivo(room, ws, msg.slot, Boolean(msg.on));
      return;
    }

    // Encerrar a própria transmissão de dentro da Activity, sem ter que achar
    // a aba de captura. Cada um só encerra a sua.
    // Ligar a outra fonte sem abrir uma segunda aba: quem já está transmitindo
    // tem uma aba conectada, e é ela que consegue capturar. A atividade só
    // pede; a aba decide o que dá para fazer sem gesto (câmera dá, tela não).
    if (msg.type === 'start-broadcast' && R.FONTES.has(msg.fonte)) {
      // Vai para a aba, e não para as conexões de transmissão: é ela quem tem o
      // gesto do usuário e a permissão, e ela existe mesmo com nada no ar.
      const n = R.toControls(room, auth.uid, {
        type: 'start-request',
        fonte: msg.fonte,
        opcoes: msg.opcoes,
      });
      if (n) console.log(`[room ${room.id}] ${auth.name} pediu ${msg.fonte} à própria aba`);
      return;
    }

    // Configuração trocada na engrenagem. Chega à aba na hora, sem esperar o
    // próximo início: era o que fazia o resumo dela envelhecer em silêncio.
    if (msg.type === 'config-broadcast' && msg.opcoes) {
      R.toControls(room, auth.uid, { type: 'config-request', opcoes: msg.opcoes });
      return;
    }

    if (msg.type === 'stop-broadcast') {
      // Sem fonte, para tudo o que a pessoa estiver transmitindo. É o que o
      // botão da barra sempre fez, e continua valendo para quem só tem uma.
      const fonte = R.FONTES.has(msg.fonte) ? msg.fonte : null;
      const alvos = R.broadcastersOf(room, auth.uid, fonte);

      for (const entry of alvos) R.sendJson(entry.ws, { type: 'stop-request' });
      if (alvos.length) {
        console.log(
          `[room ${room.id}] parada pedida por ${auth.name}: ${alvos.map((e) => e.fonte).join(', ')}`,
        );
      }
    }
  });

  ws.on('close', () => R.detachViewer(room, ws));
  ws.on('error', () => R.detachViewer(room, ws));
}

// Derruba sockets mortos — sem isso o contador de viewers fica mentindo.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.__alive === false) {
      ws.terminate();
      continue;
    }
    ws.__alive = false;
    ws.__pingSentAt = Date.now();
    ws.ping();
  }
}, 15_000);

wss.on('connection', (ws) => {
  ws.__alive = true;
  ws.on('pong', () => {
    ws.__alive = true;
    if (ws.__pingSentAt) {
      const measured = Date.now() - ws.__pingSentAt;
      // Suaviza os saltos sem esconder uma conexao que ficou lenta.
      ws.__rttMs = Number.isFinite(ws.__rttMs) ? ws.__rttMs * 0.7 + measured * 0.3 : measured;
      ws.__pingSentAt = null;
    }
  });
});

// unref para o intervalo nao segurar o processo de pe sozinho: quem mantem o
// programa vivo e a porta escutando, e quando ela fecha nao ha mais socket
// para vigiar.
heartbeat.unref?.();

wss.on('close', () => clearInterval(heartbeat));

// Porta ocupada e o tropeco mais comum aqui: basta um "npm start" esquecido
// numa janela. Sem isto, o Node cospe um stack trace de vinte linhas que nao
// diz nem qual e o problema nem o que fazer.
server.on('error', (err) => {
  if (err.code !== 'EADDRINUSE') throw err;

  console.error('');
  console.error(`  A porta ${PORT} já está sendo usada.`);
  console.error('  Quase sempre é outra janela deste mesmo programa aberta.');
  console.error('  Feche a outra janela e tente de novo.');
  console.error('');
  console.error('  Se precisar rodar os dois, mude PORT no arquivo .env.');
  console.error('');
  process.exit(1);
});

/** Data da modificação mais recente dentro de um caminho. */
function maisRecente(alvo) {
  const s = fs.statSync(alvo);
  if (!s.isDirectory()) return s.mtimeMs;
  return fs
    .readdirSync(alvo)
    .reduce((maior, nome) => Math.max(maior, maisRecente(path.join(alvo, nome))), 0);
}

/**
 * Avisa quando o que está no ar foi montado antes da última mudança no código.
 *
 * Quem roda "npm run dev" vê as mudanças no localhost:5173, mas o Discord entra
 * por esta porta — que serve o último build. A mudança parece não ter
 * acontecido, e não há nada na tela que explique por quê.
 */
function avisarBuildVelho() {
  const raiz = path.join(__dirname, '..');
  try {
    const build = fs.statSync(path.join(clientDist, 'index.html')).mtimeMs;
    const fonte = Math.max(
      maisRecente(path.join(raiz, 'client', 'src')),
      maisRecente(path.join(raiz, 'client', 'index.html')),
      maisRecente(path.join(raiz, 'shared')),
    );
    if (fonte <= build) return;

    console.log('');
    console.log('  Aviso: o site no ar foi montado antes da sua última mudança no código.');
    console.log('  Pelo Discord as pessoas ainda veem a versão antiga.');
    console.log('  Rode "npm start" para montar de novo — "npm run dev" atualiza só o 5173.');
  } catch {
    // Ainda sem build; o proprio arranque ja diz o que fazer.
  }
}

server.listen(PORT, () => {
  const local = `http://localhost:${PORT}`;

  console.log('');
  console.log(`  Sala de Tela no ar em  ${local}`);
  console.log(`  Abra esse endereço no navegador para usar fora do Discord.`);
  console.log('');

  if (DISCORD_CLIENT_ID) {
    console.log(`  Discord: ligado · aplicação ${DISCORD_CLIENT_ID}`);
    console.log(`  Endereço público: ${PUBLIC_ORIGIN}`);
    console.log(`  Redirect que precisa estar no portal: ${PUBLIC_ORIGIN}/auth/callback`);
  } else {
    console.log('  Discord: desligado (só navegador).');
    console.log('  Para usar dentro do Discord, rode: npm run configurar');
  }

  if (TEM_ADMIN) {
    console.log(`  Painel administrativo: ${local}/admin`);
    if (PUBLIC_ORIGIN !== local) console.log(`  Painel publico: ${PUBLIC_ORIGIN}/admin`);
  } else {
    console.log('  Painel administrativo: desligado (defina DISCORD_ADMIN_ID no .env).');
  }

  // Erro fácil de cometer e difícil de diagnosticar: com PUBLIC_ORIGIN
  // apontando para o proxy, a página de captura abre dentro do sandbox do
  // Discord e getDisplayMedia volta a ser bloqueado.
  if (PUBLIC_ORIGIN.includes('discordsays.com')) {
    console.error('');
    console.error('  ERRO: o endereço público aponta para o proxy do Discord.');
    console.error('  A tela de captura precisa abrir fora do Discord, senão a');
    console.error('  captura é bloqueada. Rode: npm run tunel');
  }

  avisarBuildVelho();

  if (DISCORD_CLIENT_ID && PUBLIC_ORIGIN.startsWith('http://localhost')) {
    console.log('');
    console.log('  Aviso: o Discord não alcança localhost. Rode: npm run tunel');
  }

  console.log('');
});

/**
 * Publicado para o teste, que importa o servidor no proprio processo em vez de
 * gerar outro: so assim a cobertura enxerga as linhas que rodaram. Com PORT=0
 * o sistema escolhe uma porta livre, e o endereco real sai de
 * `server.address()` — nada aqui precisa saber que esta sob teste.
 */
export { app, server, wss };
