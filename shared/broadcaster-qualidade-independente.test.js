// @vitest-environment jsdom
/** I4 — C4.9, C4.10, C4.12, C4.16 e C4.17. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createBroadcaster } from './broadcaster.js';

let sockets;
let encoders;

class TrackFalsa {
  constructor() {
    this.constraints = null;
    this.ouvintes = new Map();
  }
  getSettings() {
    return { width: 1280, height: 720, displaySurface: 'monitor' };
  }
  addEventListener(nome, fn) {
    this.ouvintes.set(nome, fn);
  }
  applyConstraints(constraints) {
    this.constraints = constraints;
    return Promise.resolve();
  }
  stop() {}
}

class StreamFalsa {
  constructor() {
    this.track = new TrackFalsa();
  }
  getVideoTracks() {
    return [this.track];
  }
  getAudioTracks() {
    return [];
  }
  getTracks() {
    return [this.track];
  }
}

class EncoderFalso {
  static async isConfigSupported(config) {
    return { supported: true, config };
  }
  constructor({ output, error }) {
    this.output = output;
    this.error = error;
    this.state = 'unconfigured';
    this.configuracoes = [];
    encoders.push(this);
  }
  configure(config) {
    this.state = 'configured';
    this.configuracoes.push({ ...config });
  }
  encode() {}
  close() {
    this.state = 'closed';
  }
}

class SocketFalso {
  static OPEN = 1;
  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.enviados = [];
    this.ouvintes = new Map();
    sockets.push(this);
  }
  addEventListener(nome, fn) {
    const lista = this.ouvintes.get(nome) ?? [];
    lista.push(fn);
    this.ouvintes.set(nome, lista);
  }
  disparar(nome, evento = {}) {
    for (const fn of this.ouvintes.get(nome) ?? []) fn(evento);
  }
  abrir() {
    this.readyState = SocketFalso.OPEN;
    this.disparar('open');
  }
  receber(msg) {
    this.disparar('message', { data: JSON.stringify(msg) });
  }
  send(data) {
    this.enviados.push(data);
  }
  close() {
    this.readyState = 3;
  }
  mensagens(tipo = null) {
    const todas = this.enviados
      .filter((data) => typeof data === 'string')
      .map((data) => JSON.parse(data));
    return tipo ? todas.filter((msg) => msg.type === tipo) : todas;
  }
}

class ProcessorFalso {
  constructor() {
    this.readable = {
      getReader: () => ({
        read: () => new Promise(() => {}),
        cancel: () => Promise.resolve(),
      }),
    };
  }
}

const respirar = async () => {
  for (let i = 0; i < 4; i++) await new Promise((resolve) => setTimeout(resolve, 0));
};

async function noAr({ bitrate = 2_500_000, fps = 30, onStats } = {}) {
  const stream = new StreamFalsa();
  const broadcaster = createBroadcaster({
    wsUrl: 'wss://exemplo.test/ws?t=abc',
    bitrate,
    fps,
    streamPronto: stream,
    onStats,
  });
  const iniciando = broadcaster.start();
  await respirar();
  const ws = sockets.at(-1);
  ws.abrir();
  await iniciando;
  return { broadcaster, ws, encoder: encoders.at(-1), stream };
}

function emitirBinario(encoder) {
  const bytes = new Uint8Array([1, 2, 3]);
  encoder.output(
    {
      type: 'key',
      timestamp: 1_000,
      byteLength: bytes.length,
      copyTo(destino) {
        destino.set(bytes);
      },
    },
    { decoderConfig: { codec: 'vp8', codedWidth: 1280, codedHeight: 720 } },
  );
}

beforeEach(() => {
  sockets = [];
  encoders = [];
  vi.stubGlobal('WebSocket', SocketFalso);
  vi.stubGlobal('VideoEncoder', EncoderFalso);
  vi.stubGlobal('VideoFrame', class {});
  vi.stubGlobal('MediaStreamTrackProcessor', ProcessorFalso);
  vi.stubGlobal('RTCPeerConnection', undefined);
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({ ok: true, json: async () => ({ iceServers: [] }) })),
  );
  window.VideoEncoder = EncoderFalso;
  window.VideoFrame = globalThis.VideoFrame;
  window.EncodedVideoChunk = class {};
  window.MediaStreamTrackProcessor = ProcessorFalso;
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  for (const ws of sockets) ws.close();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('I4 — teto manual e snapshots quality', () => {
  it('C4.9 preserva um degrau quando o teto manual sobe para 8 Mbps', async () => {
    const { broadcaster, ws, encoder } = await noAr();
    ws.receber({ type: 'quality-down' });
    broadcaster.setQuality({ bitrate: 8_000_000, fps: 30 });

    expect(broadcaster.getSettings()).toEqual({ bitrate: 6_000_000, fps: 30 });
    expect(encoder.configuracoes.at(-1)).toMatchObject({ bitrate: 6_000_000, framerate: 30 });
    expect(broadcaster.getSettings().bitrate).toBeLessThan(8_000_000);
  });

  it('C4.10 preserva um degrau quando o teto manual cai para 0,8 Mbps', async () => {
    const { broadcaster, ws } = await noAr();
    ws.receber({ type: 'quality-down' });
    broadcaster.setQuality({ bitrate: 800_000, fps: 30 });

    expect(broadcaster.getSettings()).toEqual({ bitrate: 600_000, fps: 30 });
  });

  it('C4.12 responde down/up e ordena start → quality → primeiro binário', async () => {
    const { ws, encoder } = await noAr();
    emitirBinario(encoder);

    const primeiroBinario = ws.enviados.findIndex((data) => typeof data !== 'string');
    const tiposAntesDoBinario = ws.enviados
      .slice(0, primeiroBinario)
      .filter((data) => typeof data === 'string')
      .map((data) => JSON.parse(data).type);
    expect(tiposAntesDoBinario.indexOf('start')).toBeGreaterThanOrEqual(0);
    expect(tiposAntesDoBinario.indexOf('quality')).toBeGreaterThan(
      tiposAntesDoBinario.indexOf('start'),
    );

    expect(ws.mensagens('quality').at(0)).toMatchObject({
      degraus: 0,
      bitrate: 2_500_000,
      fps: 30,
      piso: false,
    });

    ws.receber({ type: 'quality-down' });
    expect(ws.mensagens('quality').at(-1)).toMatchObject({
      degraus: 1,
      bitrate: 1_875_000,
      fps: 30,
      piso: false,
    });

    ws.receber({ type: 'quality-up' });
    expect(ws.mensagens('quality').at(-1)).toMatchObject({
      degraus: 0,
      bitrate: 2_500_000,
      fps: 30,
      piso: false,
    });
  });

  it('aplica de uma vez a severidade limitada enviada pelo relay', async () => {
    const { ws } = await noAr();

    ws.receber({ type: 'quality-down', steps: 4 });

    expect(ws.mensagens('quality').at(-1)).toMatchObject({
      degraus: 4,
      bitrate: 791_016,
      fps: 30,
      piso: false,
    });
  });

  it('aceita o corte emergencial até o piso de bitrate por pressão nativa', async () => {
    const { ws } = await noAr();

    ws.receber({ type: 'quality-down', steps: 13 });

    expect(ws.mensagens('quality').at(-1)).toMatchObject({
      degraus: 13,
      bitrate: 60_000,
      fps: 30,
      piso: false,
    });
  });

  it('C4.16 snapshot manual tira do piso e torna um novo down aplicável', async () => {
    const { broadcaster, ws } = await noAr();
    for (let i = 0; i < 20; i++) ws.receber({ type: 'quality-down' });
    const saturado = ws.mensagens('quality').at(-1);
    expect(saturado).toMatchObject({ piso: true });

    broadcaster.setQuality({ bitrate: 8_000_000, fps: 60 });
    const depoisDoTeto = ws.mensagens('quality').at(-1);
    expect(depoisDoTeto).toMatchObject({ piso: false });

    const antes = depoisDoTeto.degraus;
    ws.receber({ type: 'quality-down' });
    expect(ws.mensagens('quality').at(-1).degraus).toBeGreaterThan(antes);
  });

  it('C4.17 teto manual no piso emite piso:true e ignora down redundante', async () => {
    const { broadcaster, ws } = await noAr();
    ws.receber({ type: 'quality-down' });
    broadcaster.setQuality({ bitrate: 60_000, fps: 15 });
    const noPiso = ws.mensagens('quality').at(-1);
    expect(noPiso).toMatchObject({ bitrate: 60_000, fps: 15, piso: true });
    expect(broadcaster.getSettings()).toEqual({ bitrate: 60_000, fps: 15 });
  });
});
