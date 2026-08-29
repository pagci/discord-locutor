/**
 * O relógio do player.
 *
 * O que se testa aqui não é decodificação — é ritmo. Os quadros são capturados
 * em intervalos cravados e chegam em intervalos irregulares; a função deste
 * módulo é devolver o intervalo original na hora de desenhar. Um erro nessa
 * conta não aparece como imagem errada, aparece como solavanco, e solavanco não
 * quebra teste nenhum a menos que alguém escreva estes.
 *
 * Sem navegador: o player só chama `getContext`, `drawImage` e `requestAnimationFrame`.
 * Um canvas de mentira e um relógio na mão cobrem tudo, e cobrem mais depressa.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPlayer } from './player.js';

const ESPERA_MIN = 40;
const ALVO_TETO = 180;
const PASSO_MAX_MS = 20;
const KEYFRAME = 1;
const DELTA = 2;

let agora = 0;
let pendentes = [];
let desenhados = [];
/** Cada desenho com o relógio em que saiu, para medir latência de exibição. */
let instantes = [];

/** Canvas de mentira: o player só olha getContext, width/height e o retângulo. */
function canvasFalso() {
  return {
    width: 0,
    height: 0,
    getContext: () => ({
      drawImage: (frame) => {
        desenhados.push(frame.timestamp / 1000);
        instantes.push({ ts: frame.timestamp / 1000, quando: agora });
      },
      fillRect: () => {},
      set fillStyle(_) {},
    }),
    getBoundingClientRect: () => ({ width: 0, height: 0 }),
  };
}

/** Avança o relógio e roda os callbacks de animação que venceram. */
function avancar(ms, passo = 16) {
  const alvo = agora + ms;
  while (agora < alvo) {
    agora = Math.min(alvo, agora + passo);
    const rodando = pendentes;
    pendentes = [];
    for (const cb of rodando) cb(agora);
  }
}

/** Um pacote no formato do relay: [slot][tipo][timestamp][relógio][payload] */
function pacote(tipoDoQuadro, timestampMs) {
  const buffer = new ArrayBuffer(20);
  const view = new DataView(buffer);
  view.setUint8(0, 0);
  view.setUint8(1, tipoDoQuadro);
  view.setFloat64(2, timestampMs * 1000);
  view.setFloat64(10, Date.now());
  return buffer;
}

beforeEach(() => {
  agora = 1000;
  pendentes = [];
  desenhados = [];
  instantes = [];

  vi.spyOn(performance, 'now').mockImplementation(() => agora);
  globalThis.requestAnimationFrame = (cb) => {
    pendentes.push(cb);
    return pendentes.length;
  };
  globalThis.cancelAnimationFrame = () => {};

  // Decodificador de mentira: entrega o quadro na hora, que é o pior caso para
  // o agendamento — nenhum atraso de decodificação para esconder erro de conta.
  globalThis.VideoDecoder = class {
    constructor({ output }) {
      this.output = output;
      this.state = 'unconfigured';
    }
    configure() {
      this.state = 'configured';
    }
    decode(chunk) {
      this.output({
        timestamp: chunk.timestamp,
        displayWidth: 1280,
        displayHeight: 720,
        close: vi.fn(),
      });
    }
    close() {
      this.state = 'closed';
    }
  };
  globalThis.EncodedVideoChunk = class {
    constructor(init) {
      Object.assign(this, init);
    }
  };
  globalThis.window = { VideoDecoder: globalThis.VideoDecoder };
});

afterEach(() => {
  vi.restoreAllMocks();
  delete globalThis.window;
});

function player() {
  const p = createPlayer(canvasFalso(), {});
  expect(p.start({ codec: 'vp8', codedWidth: 1280, codedHeight: 720 })).toBe(true);
  return p;
}

describe('ritmo de exibição', () => {
  it('recria o decoder e pede keyframe depois de um erro assíncrono', async () => {
    const pedidos = [];
    const instancias = [];
    globalThis.VideoDecoder = class {
      constructor({ output, error }) {
        this.output = output;
        this.error = error;
        this.state = 'unconfigured';
        instancias.push(this);
      }
      configure() {
        this.state = 'configured';
      }
      decode(chunk) {
        if (instancias.length === 1) {
          this.state = 'closed';
          this.error(new Error('quadro dependente perdido'));
          return;
        }
        this.output({
          timestamp: chunk.timestamp,
          displayWidth: 1280,
          displayHeight: 720,
          close: vi.fn(),
        });
      }
      close() {
        this.state = 'closed';
      }
    };
    globalThis.window.VideoDecoder = globalThis.VideoDecoder;

    const p = createPlayer(canvasFalso(), { onNeedKeyframe: () => pedidos.push('keyframe') });
    expect(p.start({ codec: 'vp8', codedWidth: 1280, codedHeight: 720 })).toBe(true);
    p.push(pacote(KEYFRAME, 0));
    await Promise.resolve();

    expect(instancias).toHaveLength(2);
    expect(pedidos).toEqual(['keyframe']);
    p.push(pacote(DELTA, 33));
    p.push(pacote(KEYFRAME, 66));
    avancar(ESPERA_MIN + 80);
    expect(desenhados).toEqual([66]);
  });

  it('recria o decoder quando recebe vídeo mas deixa de produzir quadros sem erro', async () => {
    const pedidos = [];
    const instancias = [];
    globalThis.VideoDecoder = class {
      constructor({ output }) {
        this.output = output;
        this.state = 'unconfigured';
        instancias.push(this);
      }
      configure() {
        this.state = 'configured';
      }
      decode(chunk) {
        // O primeiro decoder produz um quadro e depois entra no estado observado
        // no Chrome real: continua configured, aceita decode(), não chama error,
        // mas também nunca mais chama output().
        if (instancias.length === 1 && chunk.timestamp > 0) return;
        this.output({
          timestamp: chunk.timestamp,
          displayWidth: 1280,
          displayHeight: 720,
          close: vi.fn(),
        });
      }
      close() {
        this.state = 'closed';
      }
    };
    globalThis.window.VideoDecoder = globalThis.VideoDecoder;

    const p = createPlayer(canvasFalso(), { onNeedKeyframe: () => pedidos.push('keyframe') });
    expect(p.start({ codec: 'vp8', codedWidth: 1280, codedHeight: 720 })).toBe(true);
    p.push(pacote(KEYFRAME, 0));
    avancar(100);
    p.push(pacote(DELTA, 33));
    avancar(500);
    p.push(pacote(DELTA, 66));
    await Promise.resolve();

    expect(instancias).toHaveLength(2);
    expect(pedidos).toEqual(['keyframe']);
    p.push(pacote(KEYFRAME, 99));
    avancar(ESPERA_MIN + 80);
    expect(desenhados).toEqual([0, 99]);
  });

  it('reancora quando um keyframe atrasado faz o próximo quadro cair muito no futuro', () => {
    const p = player();

    p.push(pacote(KEYFRAME, 0));
    avancar(ESPERA_MIN + 80);
    expect(desenhados).toEqual([0]);

    // Depois de uma perda longa, um keyframe confiável antigo pode chegar antes
    // dos datagramas atuais. A diferença de timestamps não pode virar 40 s de
    // tela congelada: é dívida velha, não uma espera de playout útil.
    avancar(1000);
    const chegada = agora;
    p.push(pacote(DELTA, 42_000));
    avancar(ALVO_TETO + 80);

    expect(desenhados).toEqual([0, 42_000]);
    expect(instantes.at(-1).quando - chegada).toBeLessThanOrEqual(ALVO_TETO + 16);
  });

  it('não desenha o quadro na chegada — ele espera a vez', () => {
    const p = player();

    p.push(pacote(KEYFRAME, 0));
    avancar(ESPERA_MIN - 16);

    expect(desenhados).toHaveLength(0);
  });

  it('desenha depois da espera mínima, que na rede lisa é o piso', () => {
    const p = player();

    p.push(pacote(KEYFRAME, 0));
    avancar(ESPERA_MIN + 32);

    expect(desenhados).toEqual([0]);
  });

  it('devolve o intervalo da captura a quadros que chegaram irregulares', () => {
    const p = player();
    const chegadas = [0, 55, 60, 130, 133]; // rajada e buraco, como numa rede ruim
    const capturas = [0, 33, 66, 99, 132]; // cravados a 30 fps

    // Entrega tudo de uma vez respeitando a hora de chegada de cada um.
    let anterior = 0;
    capturas.forEach((ts, i) => {
      avancar(chegadas[i] - anterior);
      anterior = chegadas[i];
      p.push(pacote(i === 0 ? KEYFRAME : DELTA, ts));
    });

    // Roda até o último quadro ter a vez.
    avancar(ESPERA_MIN + 132 + 80);

    expect(desenhados).toEqual(capturas);
  });

  it('reancora e desenha na hora quando o quadro perdeu a própria hora', () => {
    const p = player();
    p.push(pacote(KEYFRAME, 0));
    avancar(ESPERA_MIN + 16);
    expect(desenhados).toEqual([0]);

    // A rede parou meio segundo: o próximo quadro chega muito depois da hora
    // que a referência antiga previa para ele.
    avancar(500);
    p.push(pacote(DELTA, 33));

    // Sem esperar mais nada: apareceu no mesmo instante.
    expect(desenhados).toEqual([0, 33]);
  });

  it('descarta o quadro mais velho quando a fila estoura, e fecha o que descartou', () => {
    const p = player();
    const fechados = [];
    globalThis.VideoDecoder.prototype.decode = function (chunk) {
      this.output({
        timestamp: chunk.timestamp,
        displayWidth: 1280,
        displayHeight: 720,
        close: () => fechados.push(chunk.timestamp / 1000),
      });
    };

    // Vinte quadros de uma vez, sem deixar o relógio andar: nenhum tem a vez
    // ainda, e a fila tem que se defender sozinha.
    for (let i = 0; i < 20; i++) p.push(pacote(i === 0 ? KEYFRAME : DELTA, i * 33));

    // VideoFrame segura memória de GPU: descartar sem fechar trava a aba.
    expect(fechados.length).toBeGreaterThan(0);
    expect(fechados[0]).toBe(0);
  });

  it('fecha os quadros que ficaram na fila quando a transmissão para', () => {
    const p = player();
    const fechados = [];
    globalThis.VideoDecoder.prototype.decode = function (chunk) {
      this.output({
        timestamp: chunk.timestamp,
        displayWidth: 1280,
        displayHeight: 720,
        close: () => fechados.push(chunk.timestamp / 1000),
      });
    };

    p.push(pacote(KEYFRAME, 0));
    p.push(pacote(DELTA, 33));
    p.stop();

    expect(fechados).toEqual([0, 33]);
  });
});

describe('irregularidade', () => {
  it('começa sem medida, porque ainda não houve janela', () => {
    expect(player().getJitter()).toBeNull();
  });

  it('mede a distancia entre o quadro mais folgado e o mais apertado', () => {
    const p = player();

    // 30 fps cravados na origem; na chegada, alternando 53 ms e 13 ms — mesma
    // media, entregue em rajada. Cada quadro impar chega 20 ms depois da hora
    // dele, e o par volta ao lugar: e esse vaivem que vira solavanco quando se
    // desenha na chegada, e e ele que este numero mede.
    for (let i = 0; i < 70; i++) {
      p.push(pacote(i === 0 ? KEYFRAME : DELTA, i * 33));
      const intervalo = i % 2 === 0 ? 53 : 13;
      avancar(intervalo, intervalo);
    }

    expect(p.getJitter()).toBeGreaterThanOrEqual(18);
    expect(p.getJitter()).toBeLessThanOrEqual(22);
  });
});

describe('alvo adaptativo', () => {
  it('segura um quadro atrasado quando a rede já vinha irregular', () => {
    const p = player();

    // Quinze ciclos de 5, 5, 89 ms (média 33): rajada constante e larga, que o
    // estimador de jitter aprende. O alvo cresce junto com ela.
    let ts = 0;
    p.push(pacote(KEYFRAME, ts));
    for (let ciclo = 0; ciclo < 15; ciclo++) {
      for (const intervalo of [5, 5, 89]) {
        avancar(intervalo, intervalo);
        ts += 33;
        p.push(pacote(DELTA, ts));
      }
    }

    // Um buraco de 170 ms. Para o alvo fixo antigo (80 ms) ele é imperdoável —
    // o quadro reancora e é desenhado na chegada. Para o alvo que acompanhou a
    // irregularidade (que aqui passa de 100 ms), ainda cabe: espera a vez.
    avancar(170, 170);
    const antes = desenhados.length;
    ts += 33;
    p.push(pacote(DELTA, ts));

    expect(desenhados).toHaveLength(antes);

    avancar(300);
    expect(desenhados).toHaveLength(antes + 1);
  });

  it('esvazia de uma vez a fila que virou atraso, em vez de sangrar quadro a quadro', () => {
    const p = player();
    const fechados = [];
    globalThis.VideoDecoder.prototype.decode = function (chunk) {
      this.output({
        timestamp: chunk.timestamp,
        displayWidth: 1280,
        displayHeight: 720,
        close: () => fechados.push(chunk.timestamp / 1000),
      });
    };

    // Vinte quadros sem o relógio andar: 630 ms de fila. Muito além de
    // qualquer alvo — o remédio é drenar de uma vez, não sangrar um quadro
    // por vez até o FILA_MAX (que fecharia só 8).
    for (let i = 0; i < 20; i++) p.push(pacote(i === 0 ? KEYFRAME : DELTA, i * 33));

    expect(fechados.length).toBeGreaterThan(8);

    avancar(400);
    // O último sobrevive sempre, e o que ficou na fila vem logo atrás dele.
    expect(desenhados.at(-1)).toBe(19 * 33);
    expect(desenhados.length).toBeLessThan(8);
  });

  it('não deixa o desvio de relógio acumular para sempre', () => {
    const p = player();

    // Captura a 33 ms, chegada a 33,6: desvio de ~18 ms por segundo. Sem
    // correção à altura, a folga estoura e o quadro passa a ser desenhado NA
    // CHEGADA — latência quase zero, a assinatura da reancoragem. Com
    // correção contínua, a folga estaciona perto do alvo e nenhum quadro sai
    // desenhado no ato.
    p.push(pacote(KEYFRAME, 0));
    const chegada0 = agora;
    for (let i = 1; i <= 600; i++) {
      avancar(33.6, 33.6);
      p.push(pacote(DELTA, i * 33));
    }
    avancar(500);

    const tardios = instantes.filter((d) => d.quando - chegada0 > 10_000);
    expect(tardios.length).toBeGreaterThan(0);
    for (const d of tardios) {
      const chegadaDoQuadro = chegada0 + (d.ts / 33) * 33.6;
      // Todo quadro saiu com espera real — nenhum desenhado na chegada.
      expect(d.quando - chegadaDoQuadro).toBeGreaterThanOrEqual(15);
    }
  });
});

/**
 * Os três limites do alvo adaptativo, medidos no INSTANTE de cada desenho.
 *
 * O bloco acima prova quais quadros aparecem; este prova QUANDO. São coisas
 * diferentes: uma espera errada não muda a lista de quadros desenhados, então
 * nenhuma asserção sobre a lista consegue flagrar um piso, um alvo ou um passo
 * de correção errados. O relógio anda de 1 em 1 ms aqui de propósito — com o
 * passo de 16 ms do rAF, cada medida carregaria até 15 ms de arredondamento e
 * as três afirmações virariam aproximações.
 */
describe('instantes do alvo adaptativo', () => {
  it('desenha exatamente no piso de 40 ms quando a rede é lisa', () => {
    const p = player();
    const chegada = agora;

    p.push(pacote(KEYFRAME, 0));
    avancar(200, 1);

    expect(instantes).toHaveLength(1);
    expect(instantes[0].quando - chegada).toBe(ESPERA_MIN);
  });

  it('espera mais que o piso quando o jitter medido levantou o alvo', () => {
    const p = player();

    // Quinze ciclos de 5, 5, 89 ms com captura a 33: a irregularidade é real e
    // o estimador do RFC 3550 a acumula.
    let ts = 0;
    p.push(pacote(KEYFRAME, ts));
    for (let ciclo = 0; ciclo < 15; ciclo++) {
      for (const intervalo of [5, 5, 89]) {
        avancar(intervalo, 1);
        ts += 33;
        p.push(pacote(DELTA, ts));
      }
    }

    // Um buraco força a reancoragem, e reancorar usa o alvo do momento. O
    // quadro seguinte, entregue no ritmo da captura, espera exatamente o alvo:
    // é assim que o número interno vira instante observável.
    avancar(300, 1);
    ts += 33;
    p.push(pacote(DELTA, ts));

    const chegada = agora;
    avancar(33, 1);
    ts += 33;
    p.push(pacote(DELTA, ts));
    const antes = instantes.length;
    avancar(400, 1);

    const desenho = instantes.slice(antes).find((d) => d.ts === ts);
    expect(desenho).toBeDefined();
    const espera = desenho.quando - (chegada + 33);
    expect(espera).toBeGreaterThan(ESPERA_MIN);
    expect(espera).toBeLessThanOrEqual(ALVO_TETO);
  });

  it('corrige no máximo 20 ms por janela, mesmo com erro bem maior', () => {
    const p = player();

    // Primeiro fecha uma janela lisa. Isso tira o keyframe de âncora da janela
    // que vai medir o erro — ele tem folga 40 e mascararia qualquer folga maior
    // se as duas fases fossem misturadas.
    p.push(pacote(KEYFRAME, 0));
    let ts = 0;
    for (let i = 0; i < 31; i++) {
      avancar(33, 1);
      ts += 33;
      p.push(pacote(DELTA, ts));
    }

    // Na janela seguinte, a hora de captura salta 60 ms à frente e depois segue
    // no mesmo ritmo da chegada. A folga passa de 40 para 100 ms: erro constante
    // e muito maior que o passo permitido, sem deriva posterior para confundir a
    // correção aplicada no fechamento da janela.
    const chegadaDe = new Map();
    ts += 60;
    for (let i = 0; i < 33; i++) {
      avancar(33, 1);
      ts += 33;
      chegadaDe.set(ts, agora);
      p.push(pacote(DELTA, ts));
    }
    avancar(300, 1);

    const esperas = instantes
      .filter((d) => chegadaDe.has(d.ts))
      .map((d) => d.quando - chegadaDe.get(d.ts));

    // Uma única janela de ajuste fechou nessa fase. Se o passo não tivesse teto,
    // a espera teria caído os 60 ms do erro de uma vez; observamos somente 20.
    expect(esperas.length).toBeGreaterThan(20);
    expect(Math.max(...esperas) - Math.min(...esperas)).toBe(PASSO_MAX_MS);
  });
});
