// @vitest-environment jsdom
/** I5 (externo) — C5.1, C5.2 e C5.3 contra o share.html real. */
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

const duplos = vi.hoisted(() => ({ opcoes: [], broadcasters: [] }));

vi.mock('/shared/broadcaster.js?v=9', () => ({
  createBroadcaster: vi.fn((opcoes) => {
    duplos.opcoes.push(opcoes);
    const stream = { getTracks: () => [], getVideoTracks: () => [] };
    const broadcaster = {
      start: vi.fn(async () => stream),
      stop: vi.fn(),
      setQuality: vi.fn(),
      changeScreen: vi.fn(),
      trocarSom: vi.fn(),
      isRunning: () => true,
      temSom: () => false,
      somBloqueado: () => false,
    };
    duplos.broadcasters.push(broadcaster);
    return broadcaster;
  }),
  supportError: () => null,
  fonteIndisponivel: () => null,
  opcoesTela: () => ({ video: true, audio: false }),
}));

class SocketControle {
  static OPEN = 1;
  constructor() {
    this.readyState = 0;
    this.ouvintes = new Map();
  }
  addEventListener(nome, fn) {
    const lista = this.ouvintes.get(nome) ?? [];
    lista.push(fn);
    this.ouvintes.set(nome, lista);
  }
  send() {}
  close() {}
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const token = `${btoa(JSON.stringify({ uid: 'u1', name: 'Pessoa' }))}.x`;

beforeAll(async () => {
  document.documentElement.innerHTML = readFileSync('server/public/share.html', 'utf8');
  history.replaceState({}, '', `/share.html?t=${encodeURIComponent(token)}`);
  vi.stubGlobal('WebSocket', SocketControle);
  vi.spyOn(HTMLMediaElement.prototype, 'play').mockResolvedValue(undefined);

  // Importa exatamente pelo caminho absoluto usado pelo HTML no navegador.
  await import('/share.js');
  document.getElementById('tela-start').click();
  await tick();
  expect(duplos.opcoes).toHaveLength(1);
});

function publicar(stats) {
  duplos.opcoes[0].onStats({
    viewers: 2,
    mbps: stats.bitrate / 1e6,
    seconds: 7,
    ...stats,
  });
  // Controle do harness: o callback real do painel foi executado.
  expect(document.getElementById('tela-viewers').textContent).toBe('2');
  return document.getElementById('tela-auto');
}

describe('I5 — status externo persistente', () => {
  it('C5.1 mantém oculto o status automático em degraus zero', () => {
    const el = publicar({ degraus: 0, bitrate: 2_500_000, fps: 30 });
    expect(el).not.toBeNull();
    expect(el.hidden).toBe(true);
  });

  it('C5.2 mostra motivo e par efetivo quando a rede reduziu a qualidade', () => {
    const el = publicar({ degraus: 2, bitrate: 1_406_250, fps: 30 });
    expect(el).not.toBeNull();
    expect(el.hidden).toBe(false);
    expect(el.textContent).toMatch(/rede|autom|reduz|congestion/i);
    expect(el.textContent).toMatch(/1[,.]4\s*Mb\/s/i);
    expect(el.textContent).toMatch(/30\s*fps/i);
  });

  it('C5.3 oculta novamente quando os degraus voltam a zero', () => {
    publicar({ degraus: 2, bitrate: 1_406_250, fps: 30 });
    const el = publicar({ degraus: 0, bitrate: 2_500_000, fps: 30 });
    expect(el).not.toBeNull();
    expect(el.hidden).toBe(true);
  });
});
