// @vitest-environment jsdom
/** I5 (Activity) — C5.4 no index.html e main.js reais. */
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

const duplos = vi.hoisted(() => ({ opcoesBroadcaster: [], sockets: [] }));

vi.mock('@discord/embedded-app-sdk', () => ({
  DiscordSDK: class {
    commands = {};
    ready = async () => {};
  },
}));

vi.mock('./player.js', () => ({
  createPlayer: () => ({
    start: () => true,
    stop: () => {},
    push: () => {},
    getLag: () => 0,
    getJitter: () => 0,
    takeFrameCount: () => 0,
    getSizes: () => ({ video: '1280×720', box: '1280×720' }),
  }),
}));

vi.mock('./audio.js', () => ({ createAudio: () => ({ stop: () => {}, push: () => {} }) }));

vi.mock('../../shared/rtc.js', () => ({
  iceServers: async () => [],
  criarPeer: () => null,
  suportaWebRTC: () => false,
  resumoPeer: async () => ({ rtt: null, relay: false }),
  MORTO: 'morto',
  PRAZO_CONEXAO_MS: 5_000,
}));

vi.mock('../../shared/shard.js', () => ({
  basePathFor: () => '',
  nodeFor: () => 0,
  shardKey: () => '',
}));

vi.mock('../../shared/broadcaster.js', () => ({
  createBroadcaster: vi.fn((opcoes) => {
    duplos.opcoesBroadcaster.push(opcoes);
    return {
      start: vi.fn(async () => ({ getTracks: () => [] })),
      stop: vi.fn(),
      setQuality: vi.fn(),
      isRunning: () => true,
    };
  }),
}));

class SocketFalso {
  static OPEN = 1;
  constructor() {
    this.readyState = 0;
    this.ouvintes = new Map();
    duplos.sockets.push(this);
  }
  addEventListener(nome, fn) {
    const lista = this.ouvintes.get(nome) ?? [];
    lista.push(fn);
    this.ouvintes.set(nome, lista);
  }
  enviarEvento(nome, evento = {}) {
    for (const fn of this.ouvintes.get(nome) ?? []) fn(evento);
  }
  receber(msg) {
    this.enviarEvento('message', { data: JSON.stringify(msg) });
  }
  send() {}
  close() {}
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

async function esperar(predicado, descricao) {
  for (let i = 0; i < 30; i++) {
    if (predicado()) return;
    await tick();
  }
  throw new Error(`harness não alcançou ${descricao}`);
}

beforeAll(async () => {
  document.documentElement.innerHTML = readFileSync('client/index.html', 'utf8');
  const identidade = `${btoa(JSON.stringify({ uid: 'u1', name: 'Pessoa' }))}.x`;
  localStorage.setItem('identity', identidade);
  history.replaceState({}, '', '/?t=ingresso');

  vi.stubGlobal('WebSocket', SocketFalso);
  vi.stubGlobal('fetch', async (url) => {
    const caminho = String(url);
    if (caminho.endsWith('/api/config')) {
      return { ok: true, status: 200, json: async () => ({}) };
    }
    if (caminho.endsWith('/api/rooms/list')) {
      return { ok: true, status: 200, json: async () => ({ rooms: [] }) };
    }
    if (caminho.endsWith('/api/rooms/open')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          name: 'Sala',
          roomId: 'r1',
          viewerToken: 'viewer',
          shareUrl: 'http://localhost/share?t=share-token',
        }),
      };
    }
    return { ok: true, status: 200, json: async () => ({}) };
  });
  Object.defineProperty(navigator, 'mediaDevices', {
    configurable: true,
    value: { getDisplayMedia: vi.fn(async () => ({ getTracks: () => [] })) },
  });
  window.VideoEncoder = class {};

  await import('./main.js');
  await esperar(() => !document.getElementById('share').hidden, 'a sala aberta');
  document.getElementById('share').click();
  await esperar(() => duplos.opcoesBroadcaster.length === 1, 'o broadcaster in-Activity');
});

describe('I5 — status persistente in-Activity', () => {
  it('C5.4 faz oculto → visível → persistente após renderBar → oculto', () => {
    const opcoes = duplos.opcoesBroadcaster[0];
    const status = () => document.querySelector('[data-quality-auto]');

    opcoes.onStats?.({ degraus: 0, bitrate: 2_500_000, fps: 30 });
    expect(status()).not.toBeNull();
    expect(status().hidden).toBe(true);

    opcoes.onStats?.({ degraus: 2, bitrate: 1_406_250, fps: 30 });
    expect(status().hidden).toBe(false);
    expect(status().textContent).toMatch(/rede|autom|reduz|congestion/i);
    expect(status().textContent).toMatch(/1[,.]4\s*Mb\/s/i);
    expect(status().textContent).toMatch(/30\s*fps/i);

    // Uma atualização normal de sala chama renderBar novamente. O aviso não
    // pode desaparecer como um toast.
    duplos.sockets.at(-1)?.receber({
      type: 'state',
      viewers: 1,
      participants: [{ id: 'u1', name: 'Pessoa' }],
      abas: [],
      streams: [],
      room: { id: 'r1', name: 'Sala', ownerId: 'u1' },
    });
    expect(status().hidden).toBe(false);
    expect(status().textContent).toMatch(/1[,.]4\s*Mb\/s/i);

    opcoes.onStats?.({ degraus: 0, bitrate: 2_500_000, fps: 30 });
    expect(status().hidden).toBe(true);
  });
});
