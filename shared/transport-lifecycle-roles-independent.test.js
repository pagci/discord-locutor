// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import process from 'node:process';

const transportPath = join(process.cwd(), 'shared', 'transport.js');
export const fallbackUsed = !existsSync(transportPath);
const estado = {
  sockets: [],
  factoryCalls: [],
  broadcasterOptions: [],
  broadcasters: [],
  players: [],
};

class SocketPapel {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  constructor(url, { transport = 'websocket', viaFactory = false } = {}) {
    this.url = url;
    this.transport = transport;
    this.viaFactory = viaFactory;
    this.readyState = SocketPapel.CONNECTING;
    this.bufferedAmount = 0;
    this.binaryType = 'arraybuffer';
    this.sent = [];
    this.listeners = new Map();
    estado.sockets.push(this);
  }
  addEventListener(name, fn) {
    const list = this.listeners.get(name) ?? [];
    list.push(fn);
    this.listeners.set(name, list);
  }
  removeEventListener(name, fn) {
    this.listeners.set(
      name,
      (this.listeners.get(name) ?? []).filter((item) => item !== fn),
    );
  }
  emit(name, event = {}) {
    for (const fn of [...(this.listeners.get(name) ?? [])]) fn(event);
  }
  open() {
    this.readyState = SocketPapel.OPEN;
    this.emit('open');
  }
  message(value) {
    this.emit('message', { data: typeof value === 'string' ? value : JSON.stringify(value) });
  }
  send(data) {
    this.sent.push(data);
  }
  close() {
    if (this.readyState === SocketPapel.CLOSED) return;
    this.readyState = SocketPapel.CLOSED;
    this.emit('close');
  }
  failPostOpen() {
    this.readyState = SocketPapel.CLOSED;
    this.emit('close', { code: 1, reason: 'transport-lost' });
  }
}

class WebSocketPiso extends SocketPapel {
  constructor(url) {
    super(url, { transport: 'websocket', viaFactory: false });
  }
}

function instalarFactory(transport) {
  if (fallbackUsed) return;
  vi.doMock('./transport.js', () => ({
    createTransport: vi.fn((options) => {
      estado.factoryCalls.push(options);
      return new SocketPapel(options.wsUrl, { transport, viaFactory: true });
    }),
  }));
}

function limparEstado() {
  estado.sockets.length = 0;
  estado.factoryCalls.length = 0;
  estado.broadcasterOptions.length = 0;
  estado.broadcasters.length = 0;
  estado.players.length = 0;
}

const flush = async (rounds = 12) => {
  for (let i = 0; i < rounds; i++) await Promise.resolve();
};

async function esperar(predicate, name) {
  for (let i = 0; i < 40; i++) {
    if (predicate()) return;
    await flush(2);
    await vi.advanceTimersByTimeAsync(0);
  }
  throw new Error(`fixture nao alcancou ${name}`);
}

function mockDependenciasActivity() {
  vi.doMock('@discord/embedded-app-sdk', () => ({
    DiscordSDK: class {
      commands = {};
      ready = async () => {};
    },
  }));
  vi.doMock('../client/src/player.js', () => ({
    createPlayer: () => {
      const player = {
        start: vi.fn(() => true),
        stop: vi.fn(),
        push: vi.fn(),
        getLag: () => 0,
        getJitter: () => 0,
        takeFrameCount: () => 0,
        getSizes: () => ({ video: '1x1', box: '1x1' }),
      };
      estado.players.push(player);
      return player;
    },
  }));
  vi.doMock('../client/src/audio.js', () => ({
    createAudio: () => ({ stop: vi.fn(), push: vi.fn() }),
  }));
  vi.doMock('./rtc.js', () => ({
    iceServers: async () => [],
    criarPeer: () => null,
    suportaWebRTC: () => false,
    resumoPeer: async () => ({ rtt: null, relay: false }),
    MORTO: 'morto',
    PRAZO_CONEXAO_MS: 5000,
  }));
  vi.doMock('./shard.js', () => ({
    basePathFor: () => '',
    nodeFor: () => 0,
    shardKey: () => '',
  }));
  vi.doMock('./broadcaster.js', () => ({
    createBroadcaster: vi.fn((options) => {
      estado.broadcasterOptions.push(options);
      const broadcaster = {
        start: vi.fn(async () => ({ getTracks: () => [] })),
        stop: vi.fn(),
        setQuality: vi.fn(),
        isRunning: () => true,
      };
      estado.broadcasters.push(broadcaster);
      return broadcaster;
    }),
  }));
}

async function provarViewer(transport) {
  vi.resetModules();
  limparEstado();
  instalarFactory(transport);
  mockDependenciasActivity();
  document.documentElement.innerHTML = readFileSync('client/index.html', 'utf8');
  history.replaceState({}, '', '/?t=ingresso');
  const identity = `${btoa(JSON.stringify({ uid: 'u1', name: 'Pessoa' }))}.x`;
  localStorage.setItem('identity', identity);
  vi.stubGlobal('WebSocket', WebSocketPiso);
  vi.stubGlobal('fetch', async (url) => {
    const value = String(url);
    if (value.endsWith('/api/config')) return { ok: true, status: 200, json: async () => ({}) };
    if (value.endsWith('/api/rooms/list')) {
      return { ok: true, status: 200, json: async () => ({ rooms: [] }) };
    }
    if (value.endsWith('/api/rooms/open')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          name: 'Sala',
          roomId: 'r1',
          viewerToken: 'viewer-token',
          shareUrl: 'http://localhost/share?t=broadcaster-token',
        }),
      };
    }
    return { ok: true, status: 200, json: async () => ({}) };
  });
  await import('../client/src/main.js');
  await esperar(() => estado.sockets.length === 1, 'socket viewer');
  const primeiro = estado.sockets[0];
  const roomState = {
    type: 'state',
    viewers: 1,
    participants: [
      { id: 'u1', name: 'Pessoa' },
      { id: 'u2', name: 'Outra', broadcasting: true },
    ],
    abas: [],
    streams: [{ slot: 4, userId: 'u2', fonte: 'tela', watchers: [] }],
    room: { id: 'r1', name: 'Sala', ownerId: 'u1' },
  };
  primeiro.open();
  primeiro.message(roomState);
  primeiro.message({ type: 'stream-start', slot: 4, userId: 'u2', fonte: 'tela' });
  await esperar(
    () =>
      primeiro.sent.map(String).includes(JSON.stringify({ type: 'watch', slot: 4 })) ||
      Boolean(document.querySelector('.watch-prompt button')),
    'acao watch do viewer',
  );
  document.querySelector('.watch-prompt button')?.click();
  expect.soft(primeiro.sent.map(String)).toContain(JSON.stringify({ type: 'watch', slot: 4 }));

  primeiro.failPostOpen();
  await vi.advanceTimersByTimeAsync(999);
  expect.soft(estado.sockets).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  await esperar(() => estado.sockets.length === 2, 'reconnect viewer');
  const segundo = estado.sockets[1];
  segundo.open();
  segundo.message(roomState);
  await flush();
  const watchesDoSegundo = () =>
    segundo.sent.map(String).filter((data) => data === JSON.stringify({ type: 'watch', slot: 4 }));
  expect.soft(watchesDoSegundo()).toHaveLength(1);

  segundo.message({ type: 'stream-start', slot: 4, userId: 'u2', fonte: 'tela' });
  segundo.message(roomState);
  segundo.message(roomState);
  await flush();
  expect.soft(watchesDoSegundo()).toHaveLength(1);

  segundo.message({
    type: 'config',
    slot: 4,
    config: { codec: 'vp8', codedWidth: 640, codedHeight: 360 },
  });
  await flush();
  expect.soft(estado.players).toHaveLength(1);
  expect.soft(estado.players[0]?.start).toHaveBeenCalledOnce();
  expect.soft(segundo.url).toContain('viewer-token');
  expect.soft(primeiro.transport).toBe(transport);
  expect.soft(estado.factoryCalls).toHaveLength(2);
}

async function provarControle(transport) {
  vi.resetModules();
  limparEstado();
  instalarFactory(transport);
  vi.doMock('/shared/broadcaster.js?v=9', () => ({
    createBroadcaster: vi.fn((options) => {
      estado.broadcasterOptions.push(options);
      const broadcaster = {
        start: vi.fn(async () => ({ getTracks: () => [], getVideoTracks: () => [] })),
        stop: vi.fn(),
        setQuality: vi.fn(),
        changeScreen: vi.fn(),
        trocarSom: vi.fn(),
        isRunning: () => true,
        temSom: () => false,
        somBloqueado: () => false,
      };
      estado.broadcasters.push(broadcaster);
      return broadcaster;
    }),
    supportError: () => null,
    fonteIndisponivel: () => null,
    opcoesTela: () => ({ video: true, audio: false }),
  }));
  document.documentElement.innerHTML = readFileSync('server/public/share.html', 'utf8');
  const token = `${btoa(JSON.stringify({ uid: 'u1', name: 'Pessoa' }))}.x`;
  history.replaceState({}, '', `/share.html?t=${encodeURIComponent(token)}`);
  vi.stubGlobal('WebSocket', WebSocketPiso);
  vi.spyOn(HTMLMediaElement.prototype, 'play').mockResolvedValue(undefined);
  await import('/share.js');
  await esperar(() => estado.sockets.length === 1, 'socket controle');
  const primeiro = estado.sockets[0];
  primeiro.open();
  document.getElementById('tela-start').click();
  await flush();
  expect.soft(estado.broadcasters).toHaveLength(1);

  primeiro.failPostOpen();
  await vi.advanceTimersByTimeAsync(2999);
  expect.soft(estado.sockets).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  await esperar(() => estado.sockets.length === 2, 'reconnect controle');
  expect.soft(estado.broadcasters).toHaveLength(1);
  expect.soft(estado.sockets[1].url).toContain('modo=controle');
  expect.soft(primeiro.transport).toBe(transport);
  expect.soft(estado.factoryCalls).toHaveLength(2);
}

class TrackFalsa {
  constructor() {
    this.kind = 'video';
    this.stopped = false;
    this.listeners = new Map();
  }
  getSettings() {
    return { width: 1280, height: 720, displaySurface: 'monitor' };
  }
  addEventListener(name, fn) {
    this.listeners.set(name, fn);
  }
  applyConstraints() {
    return Promise.resolve();
  }
  stop() {
    this.stopped = true;
  }
}

class EncoderFalso {
  static isConfigSupported = async (config) => ({ supported: true, config });
  constructor() {
    this.state = 'unconfigured';
    this.encodeQueueSize = 0;
    estado.encoder = this;
  }
  configure() {
    this.state = 'configured';
  }
  encode() {}
  close() {
    this.state = 'closed';
  }
}

async function provarBroadcaster(transport) {
  vi.resetModules();
  limparEstado();
  instalarFactory(transport);
  vi.doUnmock('./broadcaster.js');
  const track = new TrackFalsa();
  const stream = {
    getVideoTracks: () => [track],
    getAudioTracks: () => [],
    getTracks: () => [track],
    removeTrack: vi.fn(),
  };
  vi.stubGlobal('WebSocket', WebSocketPiso);
  vi.stubGlobal('VideoEncoder', EncoderFalso);
  vi.stubGlobal('EncodedVideoChunk', class {});
  vi.stubGlobal(
    'MediaStreamTrackProcessor',
    class {
      readable = {
        getReader: () => ({
          read: () => new Promise(() => {}),
          cancel: vi.fn(async () => {}),
        }),
      };
    },
  );
  vi.stubGlobal('navigator', {
    mediaDevices: {
      getDisplayMedia: vi.fn(async () => stream),
      getSupportedConstraints: () => ({ restrictOwnAudio: true }),
    },
  });
  vi.stubGlobal('fetch', async () => ({ ok: true, json: async () => ({ iceServers: [] }) }));
  vi.stubGlobal('RTCPeerConnection', undefined);
  const { createBroadcaster } = await import('./broadcaster.js');
  const onEnd = vi.fn();
  const broadcaster = createBroadcaster({
    wsUrl: 'wss://example.test/ws?t=broadcaster',
    bitrate: 2_500_000,
    fps: 30,
    audio: false,
    onEnd,
  });
  const started = broadcaster.start();
  await flush();
  const primeiro = estado.sockets[0];
  primeiro.open();
  await started;
  primeiro.failPostOpen();
  primeiro.failPostOpen();
  await vi.advanceTimersByTimeAsync(100);
  const retomado = estado.sockets[1];
  retomado.open();
  await flush();
  expect.soft(onEnd).not.toHaveBeenCalled();
  expect.soft(track.stopped).toBe(false);
  expect.soft(estado.encoder.state).toBe('configured');
  expect.soft(estado.sockets).toHaveLength(2);
  expect
    .soft(retomado.sent.map((data) => JSON.parse(data)))
    .toEqual(expect.arrayContaining([expect.objectContaining({ type: 'resume' })]));
  expect.soft(primeiro.transport).toBe(transport);
  expect.soft(retomado.transport).toBe(transport);
  expect.soft(estado.factoryCalls).toHaveLength(2);
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  localStorage.clear();
  document.documentElement.innerHTML = '';
});

describe.each(['websocket', 'webtransport'])('lifecycle pos-OPEN em %s', (transport) => {
  it('viewer preserva watch e o reenvia uma vez na reconexao autenticada', async () => {
    vi.useFakeTimers();
    await provarViewer(transport);
  });

  it('controle religa uma vez apenas em 3000 ms e nao duplica broadcaster', async () => {
    vi.useFakeTimers();
    await provarControle(transport);
  });

  it('broadcaster reconecta sem encerrar capture/encoder/onEnd', async () => {
    vi.useFakeTimers();
    await provarBroadcaster(transport);
  });
});

describe('controle positivo herdado', () => {
  it('mantem o teste real de reconexao do broadcaster no gate comum', () => {
    const herdado = readFileSync('shared/broadcaster.test.js', 'utf8');
    expect(herdado).toContain("it('reconecta sem encerrar captura quando a conexão cai sozinha'");
    expect(herdado).toContain('expect(onEnd).not.toHaveBeenCalled()');
  });
});
