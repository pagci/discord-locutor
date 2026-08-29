/**
 * Player WebCodecs.
 *
 * Dentro da Activity não existe WebRTC, mas WebCodecs não é bloqueado por
 * Permissions Policy — então dá para decodificar quadro a quadro e desenhar
 * num canvas, sem passar por container nem por MediaSource.
 *
 * O canvas mantém SEMPRE o tamanho nativo do vídeo no buffer interno
 * (canvas.width/height). Isso dá a ele uma proporção intrínseca, e o CSS
 * apenas o limita com max-width/max-height — o navegador então reduz
 * preservando a proporção, por construção.
 *
 * Dimensionar o buffer pelo tamanho de exibição, como cheguei a tentar, faz a
 * proporção do vídeo passar a depender do formato do container e distorce a
 * imagem durante o redimensionamento.
 *
 * Os quadros NÃO são desenhados assim que chegam. Ver a nota no alvo: sem
 * essa espera, a irregularidade da rede vira micro-travada mesmo quando não se
 * perde um quadro sequer.
 */

/**
 * Quanto tempo cada quadro espera antes de aparecer.
 *
 * Este é o remédio para a travadinha que acontece com a transmissão inteira
 * chegando: os quadros são capturados a cada 33 ms cravados, mas chegam a cada
 * 28, 41, 30, 37… O caminho de rede não é regular — TCP entrega em rajada, o
 * relay reparte entre vários espectadores, e o agendador do sistema atrasa uns
 * milissegundos aqui e ali. Desenhando na chegada, essa irregularidade toda vai
 * direto para a tela, e é exatamente ela que se vê como solavanco.
 *
 * O valor deixou de ser fixo. O alvo acompanha a irregularidade MEDIDA da
 * entrega — o estimador de jitter de inter-chegada do RFC 3550, o mesmo
 * princípio dos buffers de recepção de todo stack RTP: rede lisa paga só o
 * piso; rede em rajada ganha folga sozinha, sem que ninguém configure. É a
 * diferença entre atraso e solavanco: o primeiro é constante e nem se percebe;
 * o segundo é variaçao e é o que se vê.
 */
const ALVO_PISO = 40;
const ALVO_TETO = 180;

/** Margem além do alvo em que a fila deixa de ser espera e vira atraso. */
const DRENA_ACIMA_DO_ALVO = 150;

/**
 * Teto da fila. Além disso a espera deixou de ser buffer e virou atraso.
 *
 * Acontece quando a origem manda mais rápido do que o combinado, ou quando o
 * relógio das duas máquinas anda em velocidades diferentes. Preferir descartar
 * é o mesmo princípio do encoder: atraso acumulado nunca mais sai sozinho.
 */
const FILA_MAX = 12;

/** De quanto em quanto tempo a espera é reavaliada, e sobre qual janela. */
const AJUSTE_MS = 1000;

/** Correção máxima por ajuste: acima disso a mudança de ritmo se vê. */
const PASSO_MAX_MS = 20;

/**
 * WebCodecs pode continuar `configured` e aceitar `decode()` depois de perder
 * uma referencia, sem chamar nem `output` nem `error`. Enquanto midia continua
 * chegando, 0,5 s sem saida e travamento, nao buffering de baixa latencia. O
 * teto de playout e 180 ms; esperar mais de oito vezes isso fazia a recuperacao chegar
 * tarde demais para uma lane QUIC de baixa latencia.
 */
const DECODER_SEM_SAIDA_MS = 500;

export function createPlayer(canvas, { onError, onTamanho, onNeedKeyframe } = {}) {
  const ctx = canvas.getContext('2d', { alpha: false, desynchronized: true });

  let decoder = null;
  let needKeyframe = true;
  let rawConfigAtual = null;
  let recuperacaoAgendada = false;
  let geracao = 0;
  let lastLagMs = 0;
  let framesDrawn = 0;
  let ultimaSaidaDecoder = 0;
  let entradasDesdeSaida = 0;

  // Quadros decodificados esperando a hora de aparecer, em ordem de exibição.
  const fila = [];
  // Instante local que corresponde ao timestamp zero da origem. É o que traduz
  // "capturado em tal momento" para "desenhar em tal momento".
  let base = null;
  let rafId = null;
  // Folga com que os quadros da janela atual chegaram: a menor delas é o que
  // sobra de margem antes de um quadro perder a própria hora, e o intervalo
  // entre a menor e a maior é a irregularidade que estamos combatendo.
  let folgaMin = Infinity;
  let folgaMax = -Infinity;
  let janelaAte = 0;
  let irregularidade = null;
  // Estimador de jitter de inter-chegada (RFC 3550): a espera-alvo acompanha a
  // irregularidade real da entrega em vez de ser um número escolhido a dedo.
  let jEstimado = 0;
  let chegadaAnterior = null;
  let tsAnteriorMs = null;
  // Último timestamp de captura visto. Serve para detectar a origem recomeçando:
  // o tempo andando para trás invalida a referência.
  let ultimoTs = -Infinity;
  // Quem espera precisa saber quando a espera acabou: entre pedir para assistir
  // e o primeiro quadro cabe um keyframe inteiro de atraso, e o canvas preto
  // desse intervalo é idêntico a um travamento.
  let virgem = true;

  /** A espera do momento: o piso mais duas vezes o jitter medido, com teto. */
  const alvo = () => Math.min(ALVO_TETO, Math.max(ALVO_PISO, ALVO_PISO + 2 * jEstimado));

  function recuperarDecoder(quebrado) {
    needKeyframe = true;
    if (recuperacaoAgendada || !rawConfigAtual) return;
    recuperacaoAgendada = true;
    const config = rawConfigAtual;
    const geracaoEsperada = geracao;
    queueMicrotask(() => {
      recuperacaoAgendada = false;
      if (decoder !== quebrado || geracao !== geracaoEsperada || rawConfigAtual !== config) return;
      if (start(config)) onNeedKeyframe?.();
    });
  }

  function start(rawConfig) {
    stop();
    rawConfigAtual = rawConfig;

    if (!window.VideoDecoder) {
      onError?.('Este navegador não tem WebCodecs — não é possível assistir.');
      return false;
    }

    const config = deserialize(rawConfig);

    const novoDecoder = new VideoDecoder({
      output: draw,
      error: (err) => {
        // Erro de decodificação normalmente é fluxo fora de sincronia:
        // pedir um keyframe recupera sem derrubar a sessão.
        console.warn('[decoder]', err.message);
        if (decoder === novoDecoder) recuperarDecoder(novoDecoder);
      },
    });
    decoder = novoDecoder;

    try {
      decoder.configure(config);
    } catch {
      onError?.(`Codec não suportado por este navegador: ${config.codec}`);
      decoder = null;
      rawConfigAtual = null;
      return false;
    }

    needKeyframe = true;
    ultimaSaidaDecoder = performance.now();
    entradasDesdeSaida = 0;
    return true;
  }

  /** Quadro empacotado: [1B slot][1B tipo][8B timestamp][8B envio][payload] */
  function push(buffer) {
    if (!decoder || decoder.state !== 'configured') return;

    const view = new DataView(buffer);
    const isKeyframe = view.getUint8(1) === 1;

    // Decoder frio só aceita keyframe; deltas antes disso viram erro.
    if (needKeyframe && !isKeyframe) return;

    // Existe um estado real do VideoDecoder em que decode() continua aceitando
    // chunks, mas nenhuma saida ou erro volta. Detectar pelo fluxo de entrada
    // evita confundir uma tela parada, sem midia, com um decoder travado.
    if (
      !needKeyframe &&
      entradasDesdeSaida > 0 &&
      performance.now() - ultimaSaidaDecoder >= DECODER_SEM_SAIDA_MS
    ) {
      recuperarDecoder(decoder);
      return;
    }

    const timestamp = view.getFloat64(2);
    const sentAt = view.getFloat64(10);
    lastLagMs = Date.now() - sentAt;

    // Origem nova: o tempo de captura andou para trás, então a transmissão
    // recomeçou. Nada do que estava em voo se traduz na régua nova — nem a fila,
    // nem a referência, nem o jitter medido —, e o salto negativo entraria no
    // estimador como uma amostra gigante, inflando a espera por dezenas de
    // quadros. A linha de playout inteira é descartada aqui, antes de decodificar
    // o primeiro quadro da origem nova.
    const tsMs = timestamp / 1000;
    if (tsAnteriorMs !== null && tsMs < tsAnteriorMs) reiniciarOrigem();

    // Amostra nova para o estimador de jitter: a distância entre a diferença
    // das chegadas e a diferença dos instantes de captura. Suavizada por 1/16,
    // como no RFC 3550 — responde a rajadas sem saltar com uma só.
    const chegada = performance.now();
    if (chegadaAnterior !== null) {
      const d = chegada - chegadaAnterior - (tsMs - tsAnteriorMs);
      jEstimado += (Math.abs(d) - jEstimado) / 16;
    }
    chegadaAnterior = chegada;
    tsAnteriorMs = tsMs;

    try {
      entradasDesdeSaida++;
      decoder.decode(
        new EncodedVideoChunk({
          type: isKeyframe ? 'key' : 'delta',
          timestamp,
          data: new Uint8Array(buffer, 18),
        }),
      );
      needKeyframe = false;
    } catch (err) {
      console.warn('[decode]', err.message);
      recuperarDecoder(decoder);
    }
  }

  /**
   * Um quadro decodificado entra na fila com a hora marcada para aparecer.
   *
   * A hora vem do timestamp da captura, e não do relógio de chegada: é assim
   * que o intervalo entre dois quadros na tela volta a ser o intervalo com que
   * eles foram capturados, independente de como a rede os entregou.
   */
  function draw(frame) {
    const agora = performance.now();
    ultimaSaidaDecoder = agora;
    entradasDesdeSaida = 0;
    const tsMs = (frame.timestamp ?? 0) / 1000;

    // Origem nova, ou timestamp que andou para trás (transmissão reiniciada):
    // não há o que traduzir a partir da referência antiga.
    if (base === null || tsMs < ultimoTs) reancorar(agora, tsMs);
    ultimoTs = tsMs;

    let exibirEm = base + tsMs;
    let folga = exibirEm - agora;

    // Chegou depois da própria hora — a rede engasgou e a referência ficou
    // otimista demais. Reancorar aqui custa um solavanco só, contra um quadro
    // atrasado a cada quadro se a referência ficasse como está.
    if (folga < -alvo()) {
      esvaziar();
      reancorar(agora, tsMs);
      pintar(frame);
      return;
    }

    // Um keyframe confiável pode atravessar congestionamento depois de ficar
    // velho e ser seguido imediatamente por um delta atual. Se mantivermos a
    // âncora do keyframe atrasado, esse salto de timestamp vira espera futura de
    // vários segundos. Acima da margem de drenagem, frescor vence continuidade.
    if (folga > alvo() + DRENA_ACIMA_DO_ALVO) {
      esvaziar();
      reancorar(agora, tsMs);
      exibirEm = base + tsMs;
      folga = exibirEm - agora;
    }

    medir(agora, folga);

    fila.push({ frame, tsMs, exibirEm });

    // Fila estourada: o mais velho é o que menos importa, e segurá-lo é atraso.
    while (fila.length > FILA_MAX) fila.shift().frame.close();

    // Fila virou atraso, não espera: passou do alvo mais a margem, esvazia até
    // o mais novo DE UMA VEZ e reancora — sangrar quadro a quadro manteria o
    // atraso de pé por segundos.
    const profundidade = fila.length > 1 ? fila[fila.length - 1].exibirEm - fila[0].exibirEm : 0;
    if (profundidade > alvo() + DRENA_ACIMA_DO_ALVO) {
      const ultimo = fila[fila.length - 1];
      while (fila.length > 1) fila.shift().frame.close();
      reancorar(agora, ultimo.tsMs);
      ultimo.exibirEm = base + ultimo.tsMs;
    }

    agendar();
  }

  /**
   * Descarta a linha de playout inteira porque a origem mudou.
   *
   * Reancorar sozinho não bastava: os quadros da origem velha continuavam na
   * fila, ordenados por uma régua que não existe mais, e seriam desenhados
   * depois do primeiro quadro novo — ou o bloqueariam. Fechá-los é obrigatório
   * de qualquer forma, porque `VideoFrame` segura memória de GPU.
   */
  function reiniciarOrigem() {
    esvaziar();
    base = null;
    folgaMin = Infinity;
    folgaMax = -Infinity;
    janelaAte = 0;
    ultimoTs = -Infinity;
    irregularidade = null;
    jEstimado = 0;
    chegadaAnterior = null;
    tsAnteriorMs = null;
  }

  /** Marca a referência de tempo a partir deste quadro. */
  function reancorar(agora, tsMs) {
    base = agora + alvo() - tsMs;
    folgaMin = Infinity;
    folgaMax = -Infinity;
    janelaAte = agora + AJUSTE_MS;
  }

  /**
   * Acompanha a folga e reajusta a espera de tempos em tempos.
   *
   * A referência de tempo envelhece: o relógio de quem transmite e o de quem
   * assiste nunca andam exatamente na mesma velocidade, e o desvio empurra a
   * fila para o vazio ou para o excesso. Corrigir pela MENOR folga da janela é
   * o que mantém a margem justa — a menor folga é a que quase perdeu a hora, e
   * é ela que decide se vai haver travada ou não.
   */
  function medir(agora, folga) {
    if (folga < folgaMin) folgaMin = folga;
    if (folga > folgaMax) folgaMax = folga;

    if (agora < janelaAte) return;

    // A distância entre o quadro mais folgado e o mais apertado da janela é,
    // literalmente, a irregularidade da entrega. É o número do diagnóstico.
    if (folgaMin !== Infinity) irregularidade = Math.round(folgaMax - folgaMin);

    const erro = folgaMin - alvo();
    if (folgaMin !== Infinity && Math.abs(erro) > 5) {
      base -= Math.max(-PASSO_MAX_MS, Math.min(PASSO_MAX_MS, erro));
      for (const item of fila) item.exibirEm = base + item.tsMs;
    }

    folgaMin = Infinity;
    folgaMax = -Infinity;
    janelaAte = agora + AJUSTE_MS;
  }

  /**
   * Desenha o quadro cuja hora chegou, alinhado ao refresh da tela.
   *
   * Se mais de um venceu no mesmo intervalo, só o último vai para a tela: os
   * anteriores já são passado, e desenhá-los seria gastar GPU para exibir uma
   * imagem que some no mesmo quadro do monitor.
   */
  function passo() {
    rafId = null;
    const agora = performance.now();

    let escolhido = null;
    while (fila.length && fila[0].exibirEm <= agora) {
      escolhido?.frame.close();
      escolhido = fila.shift();
    }

    if (escolhido) pintar(escolhido.frame);
    if (fila.length) agendar();
  }

  function agendar() {
    rafId ??= requestAnimationFrame(passo);
  }

  function esvaziar() {
    while (fila.length) fila.shift().frame.close();
    if (rafId !== null) cancelAnimationFrame(rafId);
    rafId = null;
  }

  function pintar(frame) {
    // Buffer no tamanho nativo do vídeo: é isso que define a proporção
    // intrínseca do elemento, e é o que impede o CSS de distorcer.
    let mudou = false;
    if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
      canvas.width = frame.displayWidth;
      canvas.height = frame.displayHeight;
      mudou = true;
    }

    ctx.drawImage(frame, 0, 0, canvas.width, canvas.height);

    // VideoFrame segura memória de GPU; sem close() a aba trava em segundos.
    frame.close();
    framesDrawn++;

    // Avisa no primeiro quadro e sempre que a resolução muda: quem desenha o
    // palco precisa das duas coisas — tirar o "conectando" e refazer a forma.
    if (virgem || mudou) {
      virgem = false;
      onTamanho?.();
    }
  }

  function stop() {
    geracao++;
    rawConfigAtual = null;
    recuperacaoAgendada = false;
    if (decoder && decoder.state !== 'closed') {
      try {
        decoder.close();
      } catch {
        // Fechar o que já se fechou sozinho lança; não há nada a desfazer.
      }
    }
    decoder = null;
    needKeyframe = true;
    lastLagMs = 0;
    ultimaSaidaDecoder = 0;
    esvaziar();
    base = null;
    jEstimado = 0;
    chegadaAnterior = null;
    tsAnteriorMs = null;
    ultimoTs = -Infinity;
    irregularidade = null;
    if (canvas.width && canvas.height) {
      ctx.fillStyle = '#000';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
    }
  }

  /** Atraso aproximado em ms. Exato na mesma máquina; entre máquinas, sujeito a desvio de relógio. */
  const getLag = () => lastLagMs;

  /**
   * O quanto a entrega chegou irregular na última janela, em ms.
   *
   * Este é o número que separa "a rede não dá conta" de "a rede dá conta, mas
   * entrega em rajada". Perto de zero e travando, o problema é outro; alto, e a
   * espera adaptativa é o que está segurando a imagem no lugar.
   */
  const getJitter = () => irregularidade;

  /** Resolução nativa do vídeo e tamanho de exibição — para diagnóstico. */
  function getSizes() {
    const rect = canvas.getBoundingClientRect();
    return {
      video: `${canvas.width}×${canvas.height}`,
      box: `${Math.round(rect.width)}×${Math.round(rect.height)}`,
    };
  }

  function takeFrameCount() {
    const n = framesDrawn;
    framesDrawn = 0;
    return n;
  }

  return { start, push, stop, getLag, getJitter, takeFrameCount, getSizes };
}

function deserialize(c) {
  const out = {
    codec: c.codec,
    codedWidth: c.codedWidth,
    codedHeight: c.codedHeight,
    // Reduz o buffering interno do decoder — sem isso ele acumula alguns
    // quadros antes de emitir o primeiro.
    optimizeForLatency: true,
  };

  if (c.description) {
    const bin = atob(c.description);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    out.description = bytes;
  }

  return out;
}
