// @vitest-environment jsdom
/**
 * Regressões relatadas por quem assiste.
 *
 * Este arquivo mantém o HTML e o main.js reais. Só navegador/rede/decoder são
 * dublês, para os asserts observarem o contrato do viewer como uma pessoa o vê.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

const duplos = vi.hoisted(() => ({
  peers: [],
  players: [],
  sockets: [],
  transportOptions: [],
  videos: [],
  inbound: {
    packetsReceived: 10,
    bytesReceived: 10_000,
    framesDecoded: 10,
  },
  selection: {
    transport: 'websocket',
    attemptedWebTransport: false,
  },
  peerSummary: { rtt: 73, relay: false },
  icePromise: null,
}));

vi.mock('@discord/embedded-app-sdk', () => ({
  DiscordSDK: class {
    commands = {};
    ready = async () => {};
  },
}));

vi.mock('./player.js', () => ({
  createPlayer: (_canvas, options) => {
    const player = {
      options,
      start: vi.fn(() => true),
      stop: vi.fn(),
      push: vi.fn(),
      getLag: () => 31,
      getJitter: () => 4,
      takeFrameCount: () => 30,
      getSizes: () => ({ video: '1280x720', box: '1280x720' }),
    };
    duplos.players.push(player);
    return player;
  },
}));

vi.mock('./audio.js', () => ({
  createAudio: () => ({
    start: () => true,
    stop: vi.fn(),
    push: vi.fn(),
    setVolume: vi.fn(),
  }),
}));

vi.mock('../../shared/broadcaster.js', () => ({
  createBroadcaster: () => ({
    start: vi.fn(),
    stop: vi.fn(),
    setQuality: vi.fn(),
    isRunning: () => false,
  }),
}));

vi.mock('../../shared/shard.js', () => ({
  basePathFor: () => '',
  nodeFor: () => 0,
  shardKey: () => '',
}));

vi.mock('../../shared/rtc.js', () => ({
  iceServers: () => duplos.icePromise ?? Promise.resolve([]),
  politicaIceDaUrl: () => 'all',
  suportaWebRTC: () => true,
  MORTO: new Set(['failed', 'closed', 'disconnected']),
  PRAZO_CONEXAO_MS: 8_000,
  criarPeer: vi.fn((options) => {
    const peer = {
      options,
      connectionState: 'connected',
      localDescription: { type: 'answer', sdp: 'resposta' },
      setRemoteDescription: vi.fn(async () => {}),
      createAnswer: vi.fn(async () => ({ type: 'answer', sdp: 'resposta' })),
      setLocalDescription: vi.fn(async () => {}),
      addIceCandidate: vi.fn(async () => {}),
      getStats: vi.fn(
        async () =>
          new Map([
            [
              'video-inbound',
              {
                id: 'video-inbound',
                type: 'inbound-rtp',
                kind: 'video',
                ...duplos.inbound,
              },
            ],
          ]),
      ),
      close: vi.fn(),
    };
    duplos.peers.push(peer);
    return peer;
  }),
  resumoPeer: async () => duplos.peerSummary,
}));

vi.mock('../../shared/transport.js', () => ({
  createTransport: vi.fn((options) => {
    const listeners = new Map();
    const socket = {
      OPEN: 1,
      readyState: 1,
      binaryType: 'arraybuffer',
      sent: [],
      addEventListener(type, listener) {
        const entries = listeners.get(type) ?? [];
        entries.push(listener);
        listeners.set(type, entries);
      },
      emit(type, event = {}) {
        for (const listener of listeners.get(type) ?? []) listener(event);
      },
      receive(message) {
        this.emit('message', {
          data: typeof message === 'string' ? message : JSON.stringify(message),
        });
      },
      send(data) {
        this.sent.push(typeof data === 'string' ? JSON.parse(data) : data);
      },
      close() {
        this.readyState = 3;
        this.emit('close');
      },
    };

    duplos.sockets.push(socket);
    duplos.transportOptions.push(options);
    queueMicrotask(() => {
      options.onTransport?.(duplos.selection);
      socket.emit('open');
    });
    return socket;
  }),
}));

async function microtasks(turns = 30) {
  for (let i = 0; i < turns; i++) await Promise.resolve();
}

function isHidden(element) {
  for (let node = element; node; node = node.parentElement) {
    if (node.hidden || node.getAttribute?.('aria-hidden') === 'true') return true;
    if (node.style?.display === 'none' || node.style?.visibility === 'hidden') return true;
  }
  return false;
}

/** Texto que está disponível sem Ctrl+Shift+D nem hover. */
function visibleText() {
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const parts = [];
  let node;
  while ((node = walker.nextNode())) {
    const parent = node.parentElement;
    if (!parent || parent.closest('script, style') || isHidden(parent)) continue;
    const value = node.textContent.replace(/\s+/g, ' ').trim();
    if (value) parts.push(value);
  }
  return parts.join(' ');
}

async function advance(ms) {
  await vi.advanceTimersByTimeAsync(ms);
  await microtasks();
}

async function mountViewer(selection = duplos.selection) {
  duplos.selection = selection;

  await import('./main.js');
  await microtasks(50);

  const socket = duplos.sockets.at(-1);
  expect(socket, 'harness deve abrir o transporte da sala').toBeDefined();

  socket.receive({
    type: 'state',
    participants: [
      { id: 'u1', name: 'Viewer' },
      { id: 'u2', name: 'Caster', broadcasting: true },
    ],
    abas: [],
    streams: [{ slot: 0, userId: 'u2', watchers: [] }],
    room: { id: 'r1', name: 'Sala', ownerId: 'u2' },
  });
  socket.receive({
    type: 'config',
    slot: 0,
    config: { codec: 'vp8', codedWidth: 1280, codedHeight: 720 },
  });

  // O link de ingresso pede para assistir automaticamente assim que o primeiro
  // slot aparece. A config acima chega antes da microtask que cumpre o pedido.
  await microtasks(20);

  const player = duplos.players.at(-1);
  expect(player, 'harness deve criar o player do slot assistido').toBeDefined();
  player.options.onTamanho?.();
  await microtasks();

  return { socket, player };
}

async function startDirect(socket) {
  socket.receive({
    type: 'rtc',
    slot: 0,
    payload: { kind: 'offer', sdp: { type: 'offer', sdp: 'oferta' } },
  });
  await microtasks(30);

  const peer = duplos.peers.at(-1);
  expect(peer, 'harness deve concluir a resposta WebRTC').toBeDefined();
  // Enquanto o relay está ativo, noDe(stream) mantém o <video> fora do DOM.
  // A harness o captura na criação para disparar o primeiro quadro realista.
  const video = duplos.videos.at(-1);
  expect(video, 'harness deve criar o video direto do slot').toBeDefined();

  const track = new EventTarget();
  track.kind = 'video';
  Object.defineProperty(track, 'muted', { configurable: true, value: false });
  const remote = {
    getVideoTracks: () => [track],
    getTracks: () => [track],
  };
  peer.options.onTrack?.({ streams: [remote], track });

  // Mantido apenas para o contador de FPS do painel. O watchdog testado abaixo
  // deve ler inbound-rtp (ou track.mute), não este contador de apresentação:
  // uma tela realmente estática pode repetir o mesmo frame por vários segundos.
  Object.defineProperty(video, 'getVideoPlaybackQuality', {
    configurable: true,
    value: () => ({ totalVideoFrames: 1, droppedVideoFrames: 0 }),
  });
  video.dispatchEvent(new Event('loadeddata'));
  await microtasks();
  return { peer, track, video };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetModules();

  duplos.peers.length = 0;
  duplos.players.length = 0;
  duplos.sockets.length = 0;
  duplos.transportOptions.length = 0;
  duplos.videos.length = 0;
  duplos.selection = { transport: 'websocket', attemptedWebTransport: false };
  duplos.peerSummary = { rtt: 73, relay: false };
  duplos.icePromise = null;
  duplos.inbound = {
    packetsReceived: 10,
    bytesReceived: 10_000,
    framesDecoded: 10,
  };

  document.documentElement.innerHTML = readFileSync('client/index.html', 'utf8');
  const createElement = document.createElement.bind(document);
  vi.spyOn(document, 'createElement').mockImplementation((tagName, options) => {
    const element = createElement(tagName, options);
    if (String(tagName).toLowerCase() === 'video') duplos.videos.push(element);
    return element;
  });
  localStorage.clear();
  const identity = `${btoa(JSON.stringify({ uid: 'u1', name: 'Viewer' }))}.x`;
  localStorage.setItem('identity', identity);
  history.replaceState({}, '', '/?t=ingresso');

  vi.stubGlobal(
    'WebSocket',
    class {
      static OPEN = 1;
    },
  );
  vi.stubGlobal('fetch', async (url) => {
    const path = String(url);
    if (path.endsWith('/api/config')) return { ok: true, status: 200, json: async () => ({}) };
    if (path.endsWith('/api/rooms/list')) {
      return { ok: true, status: 200, json: async () => ({ rooms: [] }) };
    }
    if (path.endsWith('/api/rooms/open')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          name: 'Sala',
          roomId: 'r1',
          viewerToken: 'viewer-token',
          shareUrl: 'http://localhost/share?t=share-token',
        }),
      };
    }
    return { ok: true, status: 200, json: async () => ({}) };
  });
  vi.spyOn(HTMLMediaElement.prototype, 'play').mockImplementation(async () => {});
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('viewer — método ativo sempre visível', () => {
  it.each([
    {
      name: 'relay WebSocket',
      selection: { transport: 'websocket', attemptedWebTransport: false },
      expected: /WebSocket.{0,20}TCP\/TLS/i,
    },
    {
      name: 'relay WebTransport',
      selection: { transport: 'webtransport', attemptedWebTransport: true },
      expected: /WebTransport.{0,20}QUIC.{0,20}HTTP\/3/i,
    },
  ])('identifica $name sem abrir o painel oculto', async ({ selection, expected }) => {
    await mountViewer(selection);
    await advance(1_100);

    expect(document.getElementById('panel').hidden).toBe(true);
    expect(visibleText()).toMatch(expected);
  });

  it('distingue fallback para WebSocket de uma escolha WebSocket normal', async () => {
    await mountViewer({
      transport: 'websocket',
      attemptedWebTransport: true,
      fallbackReason: 'timeout',
    });
    await advance(1_100);

    const text = visibleText();
    expect(document.getElementById('panel').hidden).toBe(true);
    expect(text).toMatch(/WebSocket/i);
    expect(text).toMatch(
      /fallback|conting[eê]ncia|WebTransport.{0,60}(falh|indispon|tempo|timeout)/i,
    );
  });

  it('distingue negociação direta do WebRTC já ativo', async () => {
    const { socket } = await mountViewer();
    socket.receive({
      type: 'rtc',
      slot: 0,
      payload: { kind: 'offer', sdp: { type: 'offer', sdp: 'oferta' } },
    });
    await microtasks(30);
    await advance(1_100);

    expect(visibleText()).toMatch(/relay/i);
    expect(visibleText()).toMatch(/negoci|tentando.{0,30}diret|WebRTC.{0,30}conect/i);

    const video = duplos.videos.at(-1);
    expect(video).toBeDefined();
    video.dispatchEvent(new Event('loadeddata'));
    await advance(1_100);

    expect(visibleText()).toMatch(/WebRTC/i);
    expect(visibleText()).toMatch(/diret/i);
    expect(visibleText()).not.toMatch(/negociando direto/i);
  });

  it('preserva candidato relay que chega enquanto a lista ICE ainda está carregando', async () => {
    const { socket } = await mountViewer();
    let liberarIce;
    duplos.icePromise = new Promise((resolve) => {
      liberarIce = () => resolve([]);
    });

    socket.receive({
      type: 'rtc',
      slot: 0,
      payload: { kind: 'offer', sdp: { type: 'offer', sdp: 'oferta' } },
    });
    socket.receive({
      type: 'rtc',
      slot: 0,
      payload: { kind: 'ice', candidate: { candidate: 'relay-udp' } },
    });
    await microtasks();
    expect(duplos.peers).toHaveLength(0);

    liberarIce();
    await microtasks(30);

    expect(duplos.peers.at(-1).addIceCandidate).toHaveBeenCalledWith({
      candidate: 'relay-udp',
    });
  });

  it('mostra alvo, bitrate e FPS recebidos e só promove o gate após três amostras', async () => {
    const { socket } = await mountViewer();
    socket.receive({
      type: 'quality-state',
      slot: 0,
      degraus: 0,
      bitrate: 2_500_000,
      fps: 30,
      piso: false,
    });

    expect(visibleText()).toMatch(/Quality gate:\s*MEDINDO/i);
    expect(visibleText()).toMatch(/alvo 2\.50 Mb\/s.{0,20}30 fps/i);

    for (let i = 0; i < 4; i++) {
      const chunk = new ArrayBuffer(100_000);
      const view = new DataView(chunk);
      view.setUint8(0, 0);
      view.setUint8(1, 2);
      socket.emit('message', { data: chunk });
      await advance(1_000);
    }

    expect(visibleText()).toMatch(/Quality gate:\s*PASS/i);
    expect(visibleText()).toMatch(/chegada .{0,20}(?:kb|Mb)\/s.{0,20}30\.0 fps/i);
  });
});

describe('viewer — WebRTC que para de entregar', () => {
  it('volta ao relay após 5 s sem novo quadro mesmo com connectionState connected', async () => {
    const { socket, player } = await mountViewer();
    const { peer } = await startDirect(socket);

    expect(peer.connectionState).toBe('connected');
    expect(socket.sent).toContainEqual({ type: 'rtc-ativo', slot: 0, on: true });
    const startsBeforeStall = player.start.mock.calls.length;

    // A API continua dizendo "connected", mas o contador não avança. Cinco
    // segundos é o teto assumido por este teste para uma janela razoável.
    await advance(5_001);

    expect(peer.getStats.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(socket.sent).toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    expect(peer.close).toHaveBeenCalled();
    expect(player.start.mock.calls.length).toBeGreaterThan(startsBeforeStall);
  });

  it('reinicia os 5 s quando inbound-rtp volta a avançar', async () => {
    const { socket } = await mountViewer();
    const { peer } = await startDirect(socket);

    await advance(4_000);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });

    duplos.inbound = {
      packetsReceived: 11,
      bytesReceived: 11_200,
      framesDecoded: 11,
    };
    // Dá ao monitor uma nova amostra com progresso.
    await advance(1_000);

    await advance(4_999);
    expect(peer.connectionState).toBe('connected');
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });

    await advance(2);
    expect(peer.getStats.mock.calls.length).toBeGreaterThanOrEqual(3);
    expect(socket.sent).toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
  });
});

describe('viewer — semântica da latência WebRTC', () => {
  it('não apresenta currentRoundTripTime do ICE sob o rótulo de latência', async () => {
    const { socket } = await mountViewer();
    await startDirect(socket);
    await advance(1_100);

    const value = document.getElementById('pLag');
    expect(value.textContent).toContain('73 ms');
    const label = value.closest('.panel-row')?.querySelector('span')?.textContent ?? '';

    // Mostrar o número é opcional; se exibido, ele precisa dizer o que mede:
    // RTT/ida-e-volta ICE, não latência de playout de um único sentido.
    expect(label).toMatch(/RTT|ida.{0,8}volta/i);
    expect(label).not.toMatch(/lat[eê]ncia|playout/i);
  });
});
