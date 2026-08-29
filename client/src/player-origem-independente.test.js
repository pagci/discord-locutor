/** I2 — C2.6: reset completo quando a origem volta o timestamp. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPlayer } from './player.js';

const KEYFRAME = 1;
const DELTA = 2;
let agora;
let rafs;
let cancelados;
let frames;
let desenhados;

function pacote(tipo, timestampMs) {
  const buffer = new ArrayBuffer(20);
  const view = new DataView(buffer);
  view.setUint8(0, 0);
  view.setUint8(1, tipo);
  view.setFloat64(2, timestampMs * 1000);
  view.setFloat64(10, Date.now());
  return buffer;
}

function avancar(ms, passo = 16) {
  const alvo = agora + ms;
  while (agora < alvo) {
    agora = Math.min(alvo, agora + passo);
    const fila = rafs;
    rafs = [];
    for (const cb of fila) cb(agora);
  }
}

function canvas() {
  return {
    width: 0,
    height: 0,
    getContext: () => ({
      drawImage: (frame) => desenhados.push(frame.timestamp / 1000),
      fillRect: () => {},
      set fillStyle(_) {},
    }),
    getBoundingClientRect: () => ({ width: 0, height: 0 }),
  };
}

beforeEach(() => {
  agora = 1_000;
  rafs = [];
  cancelados = [];
  frames = [];
  desenhados = [];

  vi.spyOn(performance, 'now').mockImplementation(() => agora);
  vi.stubGlobal('requestAnimationFrame', (cb) => {
    rafs.push(cb);
    return rafs.length;
  });
  vi.stubGlobal('cancelAnimationFrame', (id) => cancelados.push(id));

  class Decoder {
    constructor({ output }) {
      this.output = output;
      this.state = 'unconfigured';
    }
    configure() {
      this.state = 'configured';
    }
    decode(chunk) {
      const frame = {
        timestamp: chunk.timestamp,
        displayWidth: 1280,
        displayHeight: 720,
        closed: false,
        close() {
          this.closed = true;
        },
      };
      frames.push(frame);
      this.output(frame);
    }
    close() {
      this.state = 'closed';
    }
  }

  vi.stubGlobal('VideoDecoder', Decoder);
  vi.stubGlobal(
    'EncodedVideoChunk',
    class {
      constructor(init) {
        Object.assign(this, init);
      }
    },
  );
  vi.stubGlobal('window', { VideoDecoder: Decoder });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('I2 — origem de playout', () => {
  it('C2.6 fecha a origem velha, cancela o RAF e só desenha a origem nova no piso', () => {
    const player = createPlayer(canvas(), {});
    expect(player.start({ codec: 'vp8', codedWidth: 1280, codedHeight: 720 })).toBe(true);

    player.push(pacote(KEYFRAME, 1_000));
    player.push(pacote(DELTA, 1_033));
    player.push(pacote(DELTA, 1_066));
    const antigos = [...frames];
    expect(antigos.every((frame) => !frame.closed)).toBe(true);
    expect(desenhados).toEqual([]);

    // A captura reiniciou sem o lifecycle externo chamar stop/start.
    player.push(pacote(KEYFRAME, 0));
    const novo = frames.at(-1);

    expect(antigos.every((frame) => frame.closed)).toBe(true);
    expect(cancelados.length).toBeGreaterThan(0);
    expect(novo.closed).toBe(false);

    avancar(24);
    expect(desenhados).toEqual([]);
    avancar(48);

    expect(desenhados).toEqual([0]);
    expect(desenhados).not.toEqual(expect.arrayContaining([1_000, 1_033, 1_066]));
    expect(novo.closed).toBe(true);
  });
});
