/**
 * Oráculos independentes da escolha por conexão (C2.11/C2.19/C2.20,
 * C3.2–C3.10 e C3.14).
 *
 * Antes da Sprint 02 existir, somente a ausência exata de `transport.js`
 * autoriza a fixture WS-only abaixo. Qualquer erro do módulo real propaga.
 */
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const caminhoProduto = fileURLToPath(new URL('./transport.js', import.meta.url));
const produtoExiste = existsSync(caminhoProduto);

class WsDoPiso {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instancias = [];

  constructor(url) {
    this.url = String(url);
    this.readyState = WsDoPiso.CONNECTING;
    this.bufferedAmount = 0;
    this.binaryType = 'arraybuffer';
    this.listeners = new Map();
    this.enviados = [];
    WsDoPiso.instancias.push(this);
    queueMicrotask(() => {
      if (this.readyState !== WsDoPiso.CONNECTING) return;
      this.readyState = WsDoPiso.OPEN;
      this.#emitir('open', {});
    });
  }

  addEventListener(type, fn) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add(fn);
  }

  removeEventListener(type, fn) {
    this.listeners.get(type)?.delete(fn);
  }

  send(data) {
    if (this.readyState !== WsDoPiso.OPEN)
      throw new DOMException('CONNECTING', 'InvalidStateError');
    this.enviados.push(data);
  }

  close() {
    if (this.readyState >= WsDoPiso.CLOSING) return;
    this.readyState = WsDoPiso.CLOSED;
    this.#emitir('close', { code: 1000, reason: '' });
  }

  #emitir(type, event) {
    for (const fn of this.listeners.get(type) ?? []) fn.call(this, event);
    this[`on${type}`]?.call(this, event);
  }
}

function criarPisoWs({ wsUrl, WebSocket = WsDoPiso }) {
  return new WebSocket(wsUrl);
}

const produto = produtoExiste ? await import('./transport.js') : { createTransport: criarPisoWs };
const { createTransport } = produto;
export const fallbackUsed = !produtoExiste;

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((ok, erro) => {
    resolve = ok;
    reject = erro;
  });
  return { promise, resolve, reject };
}

class WtPendente {
  static instancias = [];
  static aoCriar = null;

  constructor(url, options) {
    this.url = String(url);
    this.options = options;
    this.pronto = deferred();
    this.fechado = deferred();
    this.ready = this.pronto.promise;
    this.closed = this.fechado.promise;
    this.closeCalls = 0;
    this.controlWrites = [];
    this.incomingUnidirectionalStreams = new ReadableStream();
    WtPendente.instancias.push(this);
    WtPendente.aoCriar?.(this);
  }

  async createBidirectionalStream() {
    let controller;
    const readable = new ReadableStream({ start: (c) => (controller = c) });
    const writable = new WritableStream({
      write: (chunk) => {
        const bytes = new Uint8Array(chunk.buffer, chunk.byteOffset ?? 0, chunk.byteLength);
        const copia = bytes.slice();
        this.controlWrites.push(copia);
        controller.enqueue(copia);
      },
      close: () => controller.close(),
    });
    return { readable, writable };
  }

  close() {
    this.closeCalls++;
    this.fechado.resolve({ closeCode: 0, reason: 'closed-by-test' });
  }
}

const WS_URL = 'wss://host/.proxy/n1/ws?t=SECRET&fonte=tela&modo=controle&x=DROP#frag';
const GET_EXATO = 'https://host/.proxy/n1/api/transports';
const WT_BASE = 'https://wt-n1.example/wt';
const CONNECT_EXATO = `${WT_BASE}?t=SECRET&fonte=tela&modo=controle`;

function capability(overrides = {}) {
  return new Response(
    JSON.stringify({
      websocket: true,
      webtransport: { url: WT_BASE, version: 1 },
      node: 1,
      ...overrides,
    }),
    { status: 200, headers: { 'Content-Type': 'application/json' } },
  );
}

function opcoes(overrides = {}) {
  const diagnosticos = [];
  const fetch = vi.fn(async () => capability());
  const socket = createTransport({
    wsUrl: WS_URL,
    timeoutMs: 1500,
    WebSocket: WsDoPiso,
    WebTransport: WtPendente,
    fetch,
    onDiagnostic: (evento) => diagnosticos.push(evento),
    ...overrides,
  });
  return { socket, fetch, diagnosticos };
}

async function drenar(vezes = 20) {
  for (let i = 0; i < vezes; i++) await Promise.resolve();
}

beforeEach(() => {
  vi.useFakeTimers();
  WsDoPiso.instancias = [];
  WtPendente.instancias = [];
  WtPendente.aoCriar = null;
});

describe('seam independente', () => {
  it('declara exatamente quando a fixture WS-only foi usada', () => {
    expect(fallbackUsed).toBe(!produtoExiste);
    expect(createTransport).toEqual(expect.any(Function));
  });
});

describe('fallback oportunista por conexão', () => {
  it('sem WebTransport abre exatamente um WS no endereço original', async () => {
    const { socket } = opcoes({ WebTransport: undefined });
    await drenar();

    expect(WsDoPiso.instancias.map((ws) => ws.url)).toEqual([WS_URL]);
    expect(socket.readyState).toBe(WsDoPiso.OPEN);
  });

  it('timeout total fecha a tentativa WT e abre um único WS', async () => {
    const { fetch, diagnosticos } = opcoes();
    await drenar();
    await vi.advanceTimersByTimeAsync(1500);

    expect.soft(fetch).toHaveBeenCalledTimes(1);
    expect.soft(WtPendente.instancias).toHaveLength(1);
    expect.soft(WtPendente.instancias[0]?.closeCalls).toBe(1);
    expect.soft(WsDoPiso.instancias).toHaveLength(1);
    expect.soft(diagnosticos).toContainEqual(expect.objectContaining({ reason: 'timeout' }));
  });

  it('ready tardio não rouba o socket do WS nem emite segundo open', async () => {
    const { socket } = opcoes();
    const opens = vi.fn();
    socket.addEventListener?.('open', opens);
    await drenar();
    await vi.advanceTimersByTimeAsync(1500);

    const wt = WtPendente.instancias[0];
    wt?.pronto.resolve();
    await drenar();

    expect.soft(WsDoPiso.instancias).toHaveLength(1);
    expect.soft(wt?.closeCalls).toBe(1);
    expect.soft(opens).toHaveBeenCalledTimes(1);
    expect.soft(socket.transport).toBe('websocket');
  });

  it('WT valido conclui ready e handshake, emite um open e nao cria WS', async () => {
    WtPendente.aoCriar = (wt) => queueMicrotask(() => wt.pronto.resolve());
    const { socket, fetch } = opcoes();
    const opens = vi.fn();
    socket.addEventListener?.('open', opens);
    await drenar();

    expect.soft(fetch).toHaveBeenCalledTimes(1);
    expect.soft(WtPendente.instancias).toHaveLength(1);
    expect.soft(WtPendente.instancias[0]?.controlWrites ?? []).not.toHaveLength(0);
    expect.soft(WsDoPiso.instancias).toHaveLength(0);
    expect.soft(opens).toHaveBeenCalledTimes(1);
    expect.soft(socket.readyState).toBe(socket.OPEN ?? 1);
    expect.soft(socket.transport).toBe('webtransport');
  });

  it('AbortSignal durante fetch cancela sem abrir fallback; controle não abortado abre WS', async () => {
    const pendente = deferred();
    const abortado = new AbortController();
    opcoes({ signal: abortado.signal, fetch: vi.fn(() => pendente.promise) });
    abortado.abort();
    await drenar();

    expect.soft(WsDoPiso.instancias).toHaveLength(0);
    expect.soft(WtPendente.instancias).toHaveLength(0);

    const antesDoControle = WsDoPiso.instancias.length;
    opcoes({ WebTransport: undefined });
    await drenar();
    expect.soft(WsDoPiso.instancias.slice(antesDoControle)).toHaveLength(1);
  });

  it('close durante negociação impede recurso tardio e WS surpresa', async () => {
    const { socket } = opcoes();
    await drenar();
    socket.close();
    WtPendente.instancias[0]?.pronto.resolve();
    await drenar();
    await vi.advanceTimersByTimeAsync(2000);

    expect.soft(WtPendente.instancias[0]?.closeCalls).toBe(1);
    expect.soft(WsDoPiso.instancias).toHaveLength(0);
  });

  it('captura GET sem segredo e CONNECT apenas com t/fonte/modo', async () => {
    const capturadas = [];
    const fetch = vi.fn(async (url) => {
      capturadas.push(String(url));
      return capability();
    });
    WtPendente.aoCriar = (wt) => capturadas.push(wt.url);
    const abort = new AbortController();

    opcoes({ fetch, signal: abort.signal });
    await drenar();
    abort.abort();
    await drenar();

    expect.soft(WsDoPiso.instancias).toHaveLength(0);
    expect.soft(WtPendente.instancias[0]?.closeCalls).toBe(1);
    expect.soft(capturadas).toEqual([GET_EXATO, CONNECT_EXATO]);
    expect.soft(capturadas[0] ?? '').not.toContain('SECRET');
    expect.soft(capturadas[1] ?? '').not.toContain('x=');
    expect.soft(capturadas.every((url) => !url.includes('#'))).toBe(true);
  });

  it('capability de outro nó ignora WT e abre um único WS', async () => {
    const fetch = vi.fn(async () => capability({ node: 0 }));
    opcoes({ fetch });
    await drenar();

    expect.soft(fetch.mock.calls.map(([url]) => String(url))).toEqual([GET_EXATO]);
    expect.soft(WtPendente.instancias).toHaveLength(0);
    expect.soft(WsDoPiso.instancias).toHaveLength(1);
  });
});
