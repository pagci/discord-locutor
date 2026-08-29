// @vitest-environment jsdom
/**
 * Findings da revisão final do viewer.
 *
 * HTML e main.js são reais. Os dublês controlam apenas navegador, transporte e
 * RTCPeerConnection para tornar promises atrasadas e corridas determinísticas.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

const duplos = vi.hoisted(() => ({
  peers: [],
  peerPlans: [],
  players: [],
  sockets: [],
  videos: [],
  transportOptions: [],
  selection: { transport: 'websocket', attemptedWebTransport: false },
  resumoImpl: async () => ({ rtt: null, relay: false }),
}));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function inboundStats({ packetsReceived = 10, bytesReceived = 10_000, framesDecoded = 10 } = {}) {
  return new Map([
    [
      'video-inbound',
      {
        id: 'video-inbound',
        type: 'inbound-rtp',
        kind: 'video',
        packetsReceived,
        bytesReceived,
        framesDecoded,
      },
    ],
  ]);
}

vi.mock('@discord/embedded-app-sdk', () => ({
  DiscordSDK: class {
    commands = {};
    ready = async () => {};
  },
}));

vi.mock('./player.js', () => ({
  createPlayer: (canvas, options) => {
    const index = duplos.players.length;
    const player = {
      canvas,
      video: duplos.videos.at(-1),
      options,
      lag: 31 + index * 10,
      start: vi.fn(() => true),
      stop: vi.fn(),
      push: vi.fn(),
      getLag() {
        return this.lag;
      },
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
    temSom: () => false,
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
  iceServers: async () => [],
  politicaIceDaUrl: () => 'all',
  suportaWebRTC: () => true,
  MORTO: new Set(['failed', 'closed', 'disconnected']),
  PRAZO_CONEXAO_MS: 8_000,
  criarPeer: vi.fn((options) => {
    const plan = duplos.peerPlans.shift() ?? {};
    const peer = {
      id: `peer-${duplos.peers.length + 1}`,
      options,
      connectionState: 'connected',
      inbound: { packetsReceived: 10, bytesReceived: 10_000, framesDecoded: 10 },
      localDescription: { type: 'answer', sdp: 'resposta' },
      setRemoteDescription: vi.fn(async () => {}),
      createAnswer: vi.fn(async () => ({ type: 'answer', sdp: 'resposta' })),
      setLocalDescription: vi.fn(async () => {}),
      addIceCandidate: vi.fn(async () => {}),
      close: vi.fn(),
    };
    peer.getStats = vi.fn(
      plan.getStats ? () => plan.getStats(peer) : async () => inboundStats(peer.inbound),
    );
    duplos.peers.push(peer);
    return peer;
  }),
  resumoPeer: vi.fn((peer) => duplos.resumoImpl(peer)),
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

async function advance(ms) {
  await vi.advanceTimersByTimeAsync(ms);
  await microtasks();
}

function tileFor(name, childSelector) {
  return [...document.querySelectorAll('.tile')].find(
    (tile) =>
      tile.querySelector('.tile-name')?.textContent.trim() === name &&
      (!childSelector || tile.querySelector(childSelector)),
  );
}

async function boot() {
  await import('./main.js');
  await microtasks(50);
  const socket = duplos.sockets.at(-1);
  expect(socket, 'harness deve abrir o transporte da sala').toBeDefined();
  return socket;
}

async function mountSingle() {
  const socket = await boot();
  socket.receive({
    type: 'state',
    participants: [
      { id: 'u1', name: 'Viewer' },
      { id: 'u2', name: 'Caster A', broadcasting: true },
    ],
    abas: [],
    streams: [
      {
        slot: 0,
        userId: 'u2',
        watchers: [],
        quality: { bitrate: 2_500_000, fps: 30 },
      },
    ],
    room: { id: 'r1', name: 'Sala', ownerId: 'u2' },
  });
  socket.receive({
    type: 'config',
    slot: 0,
    config: { codec: 'vp8', codedWidth: 1280, codedHeight: 720 },
  });
  socket.receive({ type: 'quality-state', slot: 0, bitrate: 2_500_000, fps: 30 });
  await microtasks(20);

  const player = duplos.players[0];
  expect(player, 'harness deve assistir automaticamente o slot A').toBeDefined();
  player.options.onTamanho?.();
  await microtasks();
  return { socket, player };
}

async function mountTwo() {
  const socket = await boot();
  socket.receive({
    type: 'state',
    participants: [
      { id: 'u1', name: 'Viewer' },
      { id: 'u2', name: 'Caster A', broadcasting: true },
      { id: 'u3', name: 'Caster B', broadcasting: true },
    ],
    abas: [],
    streams: [
      { slot: 0, userId: 'u2', watchers: [] },
      { slot: 1, userId: 'u3', watchers: [] },
    ],
    room: { id: 'r1', name: 'Sala', ownerId: 'u2' },
  });
  for (const slot of [0, 1]) {
    socket.receive({
      type: 'config',
      slot,
      config: { codec: 'vp8', codedWidth: 1280, codedHeight: 720 },
    });
  }
  await microtasks(20);

  const playerA = duplos.players[0];
  expect(playerA, 'harness deve assistir automaticamente o slot A').toBeDefined();
  playerA.options.onTamanho?.();

  const promptB = tileFor('Caster B', '.watch-prompt')?.querySelector('.watch-prompt button');
  expect(promptB, 'harness deve oferecer o slot B na lateral').toBeDefined();
  promptB.click();
  await microtasks();

  const playerB = duplos.players[1];
  expect(playerB, 'harness deve criar o relay do slot B').toBeDefined();
  playerB.options.onTamanho?.();
  await microtasks();
  return { socket, playerA, playerB };
}

async function assumeDirect(socket, slot, player) {
  socket.receive({
    type: 'rtc',
    slot,
    payload: { kind: 'offer', sdp: { type: 'offer', sdp: `oferta-${slot}` } },
  });
  await microtasks(30);

  const peer = duplos.peers.at(-1);
  expect(peer, `harness deve responder a oferta do slot ${slot}`).toBeDefined();
  const video = player.video;
  expect(video, `harness deve capturar o video do slot ${slot}`).toBeDefined();

  const track = new EventTarget();
  track.kind = 'video';
  Object.defineProperty(track, 'muted', { configurable: true, value: false });
  const remote = {
    getVideoTracks: () => [track],
    getAudioTracks: () => [],
    getTracks: () => [track],
  };
  peer.options.onTrack?.({ streams: [remote], track });
  Object.defineProperty(video, 'getVideoPlaybackQuality', {
    configurable: true,
    value: () => ({ totalVideoFrames: 1, droppedVideoFrames: 0 }),
  });
  video.dispatchEvent(new Event('loadeddata'));
  await microtasks();
  return { peer, track, video };
}

function expectRelayFallback(socket, peer, player, startsBefore) {
  expect(socket.sent).toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
  expect(peer.close).toHaveBeenCalled();
  expect(player.start.mock.calls.length).toBeGreaterThan(startsBefore);
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.resetModules();

  duplos.peers.length = 0;
  duplos.peerPlans.length = 0;
  duplos.players.length = 0;
  duplos.sockets.length = 0;
  duplos.videos.length = 0;
  duplos.transportOptions.length = 0;
  duplos.selection = { transport: 'websocket', attemptedWebTransport: false };
  duplos.resumoImpl = async () => ({ rtt: null, relay: false });

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
  // cheia=0 mantém a lateral montada para trocar o palco entre A e B.
  history.replaceState({}, '', '/?t=ingresso&cheia=0');

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

describe('watchdog WebRTC — getStats adversarial', () => {
  it('volta ao relay no deadline mesmo quando getStats nunca assenta', async () => {
    const pending = deferred();
    duplos.peerPlans.push({ getStats: () => pending.promise });
    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);
    const startsBefore = player.start.mock.calls.length;

    await advance(5_001);

    expect(peer.getStats).toHaveBeenCalled();
    expectRelayFallback(socket, peer, player, startsBefore);
  });

  it('volta ao relay no deadline quando todas as leituras de getStats rejeitam', async () => {
    duplos.peerPlans.push({
      getStats: async () => {
        throw new Error('stats indisponíveis');
      },
    });
    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);
    const startsBefore = player.start.mock.calls.length;

    await advance(5_001);

    expect(peer.getStats.mock.calls.length).toBeGreaterThanOrEqual(2);
    expectRelayFallback(socket, peer, player, startsBefore);
  });

  it('finally do peer antigo não libera uma leitura ainda ocupada da geração nova', async () => {
    const oldRead = deferred();
    const newRead = deferred();
    duplos.peerPlans.push({ getStats: () => oldRead.promise }, { getStats: () => newRead.promise });
    const { socket, player } = await mountSingle();
    const { peer: oldPeer } = await assumeDirect(socket, 0, player);

    await advance(1_000);
    expect(oldPeer.getStats).toHaveBeenCalledTimes(1);

    const { peer: newPeer } = await assumeDirect(socket, 0, player);
    await advance(1_000);
    expect(newPeer.getStats).toHaveBeenCalledTimes(1);

    // A leitura antiga termina quando a nova já está pendurada. O finally da
    // geração antiga não pode limpar o busy pertencente ao peer novo.
    oldRead.resolve(inboundStats());
    await microtasks();
    await advance(1_000);

    expect(oldPeer).not.toBe(newPeer);
    expect(newPeer.getStats).toHaveBeenCalledTimes(1);
  });

  it('aceita progresso da amostra iniciada imediatamente antes do deadline', async () => {
    const finalRead = deferred();
    const semProgresso = inboundStats({
      packetsReceived: 10,
      bytesReceived: 10_000,
      framesDecoded: 10,
    });
    const comProgresso = inboundStats({
      packetsReceived: 11,
      bytesReceived: 11_200,
      framesDecoded: 10,
    });
    let calls = 0;
    duplos.peerPlans.push({
      getStats: () => {
        calls += 1;
        if (calls === 5) return finalRead.promise;
        return Promise.resolve(calls < 5 ? semProgresso : comProgresso);
      },
    });

    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);
    const startsBefore = player.start.mock.calls.length;

    // t=1s..4s: amostras assentam sem progresso. Em t=5s nasce uma leitura
    // recente que já contém progresso de packets/bytes, mas leva 100 ms para
    // entregar o snapshot ao JavaScript.
    await advance(5_000);
    expect(peer.getStats).toHaveBeenCalledTimes(5);
    setTimeout(() => finalRead.resolve(comProgresso), 100);

    // O deadline nominal não pode vencer a amostra que começou há só 1 ms.
    await advance(1);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    expect(peer.close).not.toHaveBeenCalled();

    await advance(100);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    expect(peer.close).not.toHaveBeenCalled();

    // O progresso rearmou uma janela completa a partir de ~t=5.100.
    await advance(4_998);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    await advance(3);
    expectRelayFallback(socket, peer, player, startsBefore);
  });

  it('limita a um ciclo a tolerância da amostra final recente que nunca assenta', async () => {
    const finalRead = deferred();
    const semProgresso = inboundStats();
    let calls = 0;
    duplos.peerPlans.push({
      getStats: () => {
        calls += 1;
        return calls === 5 ? finalRead.promise : Promise.resolve(semProgresso);
      },
    });

    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);
    const startsBefore = player.start.mock.calls.length;

    await advance(5_000);
    expect(peer.getStats).toHaveBeenCalledTimes(5);

    // Diferente da promise pendurada desde o primeiro probe, esta começou no
    // último ciclo: ganha no máximo RTC_VIGIA_MS para assentar, não outra janela.
    await advance(1);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    expect(peer.close).not.toHaveBeenCalled();

    await advance(1_000);
    expectRelayFallback(socket, peer, player, startsBefore);
  });

  it('não renova infinitamente a graça com novos probes lentos sem progresso', async () => {
    const semProgresso = inboundStats({
      packetsReceived: 10,
      bytesReceived: 10_000,
      framesDecoded: 10,
    });
    let calls = 0;
    duplos.peerPlans.push({
      getStats: () => {
        calls += 1;
        if (calls < 5) return Promise.resolve(semProgresso);
        return new Promise((resolve) => {
          setTimeout(() => resolve(semProgresso), 100);
        });
      },
    });

    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);
    const startsBefore = player.start.mock.calls.length;

    // Probes 1..4 assentam sem progresso. O #5 começa em t=5.000, justifica a
    // primeira graça e assenta em t=5.100 ainda sem progresso.
    await advance(5_001);
    expect(peer.getStats).toHaveBeenCalledTimes(5);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
    expect(peer.close).not.toHaveBeenCalled();

    // O #6 nasce em t=6.000 e ainda está em voo em t=6.001. Ele não pode
    // renovar a graça já consumida pelo #5: sem progresso real, este é o hard bound.
    await advance(1_000);
    expect(peer.getStats).toHaveBeenCalledTimes(6);
    expectRelayFallback(socket, peer, player, startsBefore);
  });

  it('considera packets/bytes como progresso mesmo com framesDecoded parado', async () => {
    const { socket, player } = await mountSingle();
    const { peer } = await assumeDirect(socket, 0, player);

    await advance(4_000);
    peer.inbound = {
      packetsReceived: 11,
      bytesReceived: 11_200,
      framesDecoded: 10,
    };
    await advance(1_000);
    await advance(4_999);

    expect(peer.getStats.mock.calls.length).toBeGreaterThanOrEqual(5);
    expect(peer.inbound.framesDecoded).toBe(10);
    expect(socket.sent).not.toContainEqual({ type: 'rtc-ativo', slot: 0, on: false });
  });
});

describe('watchdog do relay — sessão aberta sem mídia', () => {
  it('pede keyframe em cadência até os bytes voltarem a avançar', async () => {
    const { socket } = await mountSingle();
    const pedidos = () =>
      socket.sent.filter((message) => message.type === 'need-keyframe' && message.slot === 0);

    await advance(999);
    expect(pedidos()).toHaveLength(0);

    await advance(1);
    expect(pedidos()).toHaveLength(1);

    await advance(1_000);
    expect(pedidos()).toHaveLength(2);

    socket.emit('message', { data: new Uint8Array([0, 1, 0]).buffer });
    await advance(999);
    expect(pedidos()).toHaveLength(2);

    await advance(1);
    expect(pedidos()).toHaveLength(3);
  });
});

describe('viewer com dois slots', () => {
  it('troca o badge imediatamente entre A WebRTC e B relay ao mudar o palco', async () => {
    const { socket, playerA } = await mountTwo();
    await assumeDirect(socket, 0, playerA);

    const badge = document.getElementById('viaBadge');
    expect(badge.textContent).toMatch(/WebRTC.{0,20}diret/i);

    const tileB = tileFor('Caster B', 'canvas');
    expect(tileB, 'slot B relay deve estar clicável na lateral').toBeDefined();
    tileB.click();

    expect(badge.textContent).toMatch(/relay.{0,30}WebSocket/i);
    expect(badge.textContent).not.toMatch(/WebRTC.{0,20}diret/i);

    const tileA = tileFor('Caster A', 'video');
    expect(tileA, 'slot A WebRTC deve estar clicável na lateral').toBeDefined();
    tileA.click();
    expect(badge.textContent).toMatch(/WebRTC.{0,20}diret/i);
  });

  it('ignora resumoPeer tardio de A depois que B relay vira o palco', async () => {
    const summaryA = deferred();
    duplos.resumoImpl = (peer) =>
      peer.id === 'peer-1' ? summaryA.promise : Promise.resolve({ rtt: null, relay: false });

    const { socket, playerA, playerB } = await mountTwo();
    await assumeDirect(socket, 0, playerA);

    // Dispara a leitura de RTT de A e a mantém pendente.
    await advance(1_000);
    const tileB = tileFor('Caster B', 'canvas');
    tileB.click();

    // A passagem seguinte do painel já representa B pelo relay.
    await advance(1_000);
    expect(document.getElementById('pLagLabel').textContent).toMatch(/Chegada da rede/i);
    expect(document.getElementById('pLag').textContent).toBe(`${playerB.lag} ms`);

    summaryA.resolve({ rtt: 87, relay: false });
    await microtasks();

    expect(document.getElementById('pLagLabel').textContent).toMatch(/Chegada da rede/i);
    expect(document.getElementById('pLag').textContent).toBe(`${playerB.lag} ms`);
    expect(document.getElementById('pLag').textContent).not.toContain('87 ms');
  });
});
