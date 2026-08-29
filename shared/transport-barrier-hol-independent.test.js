/**
 * Oráculos independentes do framing, barreira e lanes codec-safe (C4.1–C4.30).
 * A única API de produto usada é `createTransport`; o peer injetado observa
 * streams e callbacks, nunca estado interno do adapter.
 */
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const caminhoProduto = fileURLToPath(new URL('./transport.js', import.meta.url));
const produtoExiste = existsSync(caminhoProduto);

class WsLoopback {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instancias = [];

  constructor(url) {
    this.url = String(url);
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.binaryType = 'arraybuffer';
    this.listeners = new Map();
    WsLoopback.instancias.push(this);
    queueMicrotask(() => {
      this.readyState = 1;
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
    if (this.readyState !== 1) throw new DOMException('CONNECTING', 'InvalidStateError');
    const copia = typeof data === 'string' ? data : copiarBytes(data).buffer;
    queueMicrotask(() => this.#emitir('message', { data: copia }));
  }

  close() {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    this.#emitir('close', { code: 1000, reason: '' });
  }

  #emitir(type, event) {
    for (const fn of this.listeners.get(type) ?? []) fn.call(this, event);
    this[`on${type}`]?.call(this, event);
  }
}

function pisoWs({ wsUrl, WebSocket = WsLoopback }) {
  return new WebSocket(wsUrl);
}

const produto = produtoExiste ? await import('./transport.js') : { createTransport: pisoWs };
const { createTransport } = produto;
export const fallbackUsed = !produtoExiste;

function copiarBytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data.slice(0));
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength));
  }
  return new TextEncoder().encode(String(data));
}

function concatenar(partes) {
  const total = partes.reduce((n, parte) => n + parte.byteLength, 0);
  const saida = new Uint8Array(total);
  let offset = 0;
  for (const parte of partes) {
    saida.set(parte, offset);
    offset += parte.byteLength;
  }
  return saida;
}

function readableDe(bytes, fragmentar = false) {
  return new ReadableStream({
    start(controller) {
      if (!fragmentar || bytes.byteLength < 3) controller.enqueue(bytes);
      else {
        controller.enqueue(bytes.slice(0, 1));
        controller.enqueue(bytes.slice(1, 3));
        controller.enqueue(bytes.slice(3));
      }
      controller.close();
    },
  });
}

class WtLoopback {
  static instancias = [];
  static fragmentarControle = false;
  static coalescerControle = false;
  static segurarControle = false;
  static reordenarUnis = false;
  static espelharControle = true;

  constructor(url) {
    this.url = String(url);
    this.ready = Promise.resolve();
    this.fim = new Promise((resolve) => (this.resolverFim = resolve));
    this.closed = this.fim;
    this.closeCalls = 0;
    this.unis = [];
    this.unisPendentes = [];
    this.controlesPendentes = [];
    this.controlesEnviados = [];
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => (this.uniController = controller),
    });
    this.incomingBidirectionalStreams = new ReadableStream();
    WtLoopback.instancias.push(this);
  }

  async createBidirectionalStream() {
    let controller;
    let handshake = true;
    const readable = new ReadableStream({ start: (c) => (controller = c) });
    const writable = new WritableStream({
      write: async (chunk) => {
        const bytes = copiarBytes(chunk);
        if (handshake) {
          handshake = false;
          for (const parte of WtLoopback.fragmentarControle
            ? [bytes.slice(0, 1), bytes.slice(1)]
            : [bytes]) {
            controller.enqueue(parte);
          }
          return;
        }
        this.controlesEnviados.push(bytes);
        if (!WtLoopback.espelharControle) return;
        if (WtLoopback.segurarControle || WtLoopback.coalescerControle) {
          this.controlesPendentes.push(bytes);
          if (WtLoopback.coalescerControle && this.controlesPendentes.length >= 2) {
            controller.enqueue(concatenar(this.controlesPendentes.splice(0)));
          }
          return;
        }
        if (WtLoopback.fragmentarControle) {
          controller.enqueue(bytes.slice(0, 2));
          controller.enqueue(bytes.slice(2));
        } else controller.enqueue(bytes);
      },
      close: () => controller.close(),
    });
    this.liberarControle = () => {
      for (const bytes of this.controlesPendentes.splice(0)) controller.enqueue(bytes);
    };
    return { readable, writable };
  }

  async createUnidirectionalStream() {
    const indice = this.unis.length;
    const partes = [];
    const registro = { indice, partes, fechado: false };
    this.unis.push(registro);
    return new WritableStream({
      write: (chunk) => partes.push(copiarBytes(chunk)),
      close: () => {
        registro.fechado = true;
        const stream = readableDe(concatenar(partes), indice % 2 === 0);
        if (!WtLoopback.reordenarUnis) this.uniController.enqueue(stream);
        else {
          this.unisPendentes.push(stream);
          if (this.unisPendentes.length === 2) this.uniController.enqueue(this.unisPendentes[1]);
        }
      },
      abort: () => {},
    });
  }

  liberarPrimeiroUni() {
    if (this.unisPendentes[0]) this.uniController.enqueue(this.unisPendentes[0]);
  }

  close(info = {}) {
    this.closeCalls++;
    this.resolverFim(info);
    this.uniController.close();
  }
}

function capability() {
  return new Response(
    JSON.stringify({
      websocket: true,
      webtransport: { url: 'https://wt.example/wt', version: 1 },
      node: 0,
    }),
    { status: 200, headers: { 'Content-Type': 'application/json' } },
  );
}

function abrir() {
  const socket = createTransport({
    wsUrl: 'wss://host/ws?t=TOKEN',
    timeoutMs: 1500,
    WebSocket: WsLoopback,
    WebTransport: WtLoopback,
    fetch: vi.fn(async () => capability()),
  });
  const mensagens = [];
  socket.addEventListener?.('message', (event) => mensagens.push(event.data));
  return { socket, mensagens };
}

async function pronto(socket) {
  for (let i = 0; i < 20 && socket.readyState !== 1; i++) await Promise.resolve();
  expect(socket.readyState).toBe(1);
}

async function drenar(vezes = 20) {
  for (let i = 0; i < vezes; i++) await Promise.resolve();
}

function pacote(slot, tipo, timestamp, marcador) {
  const buffer = new ArrayBuffer(19);
  const view = new DataView(buffer);
  view.setUint8(0, slot);
  view.setUint8(1, tipo);
  view.setFloat64(2, timestamp);
  view.setFloat64(10, Date.now());
  view.setUint8(18, marcador);
  return buffer;
}

const comoBytes = (data) =>
  new Uint8Array(
    data instanceof ArrayBuffer ? data : data.buffer,
    data.byteOffset ?? 0,
    data.byteLength ?? data.buffer?.byteLength,
  );
const marcador = (data) => comoBytes(data)[18];
const binarios = (mensagens) => mensagens.filter((data) => typeof data !== 'string');
const controlesEnviados = (wt) =>
  wt.controlesEnviados.map((frame) => {
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    const tamanho = view.getUint32(8);
    return JSON.parse(new TextDecoder().decode(frame.slice(16, 16 + tamanho)));
  });

beforeEach(() => {
  WsLoopback.instancias = [];
  WtLoopback.instancias = [];
  WtLoopback.fragmentarControle = false;
  WtLoopback.coalescerControle = false;
  WtLoopback.segurarControle = false;
  WtLoopback.reordenarUnis = false;
  WtLoopback.espelharControle = true;
});

afterEach(() => vi.useRealTimers());

describe('controle e barreira', () => {
  it('fragmenta um frame e coalesce dois sem fundir mensagens', async () => {
    WtLoopback.fragmentarControle = true;
    WtLoopback.coalescerControle = true;
    const { socket, mensagens } = abrir();
    await pronto(socket);

    socket.send(JSON.stringify({ type: 'config', n: 1 }));
    socket.send(JSON.stringify({ type: 'quality', n: 2 }));
    await drenar();

    expect.soft(socket.transport).toBe('webtransport');
    expect.soft(mensagens.filter((data) => typeof data === 'string').map(JSON.parse)).toEqual([
      { type: 'config', n: 1 },
      { type: 'quality', n: 2 },
    ]);
  });

  it('mídia aguarda seu controle e é liberada quando a barreira chega', async () => {
    WtLoopback.segurarControle = true;
    const { socket, mensagens } = abrir();
    await pronto(socket);

    socket.send(JSON.stringify({ type: 'config', codec: 'vp8' }));
    socket.send(pacote(0, 1, 1_000, 7));
    await drenar();
    expect.soft(binarios(mensagens)).toHaveLength(0);

    WtLoopback.instancias[0]?.liberarControle();
    await drenar();
    expect.soft(binarios(mensagens).map(marcador)).toEqual([7]);
  });

  it('cada recipient usa sua própria seq de controle', async () => {
    WtLoopback.segurarControle = true;
    const a = abrir();
    const b = abrir();
    await Promise.all([pronto(a.socket), pronto(b.socket)]);

    a.socket.send(JSON.stringify({ type: 'config', n: 1 }));
    a.socket.send(JSON.stringify({ type: 'quality', n: 2 }));
    a.socket.send(pacote(0, 1, 1_000, 1));
    b.socket.send(JSON.stringify({ type: 'config', n: 1 }));
    b.socket.send(pacote(0, 1, 1_000, 2));
    await drenar();

    WtLoopback.instancias[1]?.liberarControle();
    await drenar();
    expect.soft(binarios(b.mensagens).map(marcador)).toEqual([2]);
    expect.soft(binarios(a.mensagens)).toHaveLength(0);

    WtLoopback.instancias[0]?.liberarControle();
    await drenar();
    expect.soft(binarios(a.mensagens).map(marcador)).toEqual([1]);
  });
});

describe('lanes codec-safe sem HOL', () => {
  it('áudio B ultrapassa A e o late-old A nunca chega', async () => {
    WtLoopback.reordenarUnis = true;
    const { socket, mensagens } = abrir();
    await pronto(socket);
    socket.send(pacote(0, 3, 1_000, 10));
    socket.send(pacote(0, 3, 2_000, 20));
    await drenar();

    WtLoopback.instancias[0]?.liberarPrimeiroUni();
    await drenar();
    expect.soft(socket.transport).toBe('webtransport');
    expect.soft(binarios(mensagens).map(marcador)).toEqual([20]);
  });

  it('keyframe B ultrapassa A e torna o vídeo antigo obsoleto', async () => {
    WtLoopback.reordenarUnis = true;
    const { socket, mensagens } = abrir();
    await pronto(socket);
    socket.send(pacote(0, 2, 1_000, 30));
    socket.send(pacote(0, 1, 2_000, 40));
    await drenar();
    WtLoopback.instancias[0]?.liberarPrimeiroUni();
    await drenar();

    expect.soft(binarios(mensagens).map(marcador)).toEqual([40]);
  });

  it('gap pede keyframe ao peer por slot em cadência e retoma sem mensagem sintética', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    WtLoopback.reordenarUnis = true;
    WtLoopback.espelharControle = false;
    const { socket, mensagens } = abrir();
    await pronto(socket);
    const wt = WtLoopback.instancias[0];
    socket.send(pacote(7, 2, 1_000, 50));
    socket.send(pacote(7, 2, 2_000, 60));
    await drenar();

    const pedidosInbound = () =>
      mensagens
        .filter((data) => typeof data === 'string')
        .map(JSON.parse)
        .filter((msg) => msg.type === 'need-keyframe');
    const controlesDoTipo = (type) => controlesEnviados(wt).filter((msg) => msg.type === type);
    expect.soft(binarios(mensagens)).toHaveLength(0);
    expect.soft(pedidosInbound()).toHaveLength(0);
    expect.soft(controlesEnviados(wt)).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(3001);
    await drenar();
    expect.soft(binarios(mensagens)).toHaveLength(0);
    expect.soft(controlesDoTipo('media-loss')).toEqual([{ type: 'media-loss', slot: 7 }]);
    expect.soft(controlesDoTipo('need-keyframe')).toEqual([{ type: 'need-keyframe', slot: 7 }]);
    expect.soft(pedidosInbound()).toHaveLength(0);

    WtLoopback.reordenarUnis = false;
    socket.send(pacote(7, 2, 2_500, 65));
    await drenar();
    await vi.advanceTimersByTimeAsync(3001);
    await drenar();
    expect.soft(controlesDoTipo('media-loss')).toEqual([
      { type: 'media-loss', slot: 7 },
      { type: 'media-loss', slot: 7 },
    ]);
    expect.soft(controlesDoTipo('need-keyframe')).toEqual([
      { type: 'need-keyframe', slot: 7 },
      { type: 'need-keyframe', slot: 7 },
    ]);
    expect.soft(pedidosInbound()).toHaveLength(0);

    socket.send(pacote(7, 1, 3_000, 70));
    socket.send(pacote(7, 2, 4_000, 80));
    await drenar();
    expect.soft(binarios(mensagens).map(marcador)).toEqual([70, 80]);

    await vi.advanceTimersByTimeAsync(3001);
    await drenar();
    expect.soft(controlesDoTipo('media-loss')).toHaveLength(2);
    expect.soft(controlesDoTipo('need-keyframe')).toHaveLength(2);
    expect.soft(pedidosInbound()).toHaveLength(0);
  });

  it('slot e classe diferentes não suprimem uma à outra', async () => {
    WtLoopback.reordenarUnis = true;
    const { socket, mensagens } = abrir();
    await pronto(socket);
    socket.send(pacote(0, 3, 1_000, 90));
    socket.send(pacote(1, 1, 500, 91));
    await drenar();

    expect.soft(binarios(mensagens).map(marcador)).toEqual([91]);
    WtLoopback.instancias[0]?.liberarPrimeiroUni();
    await drenar();
    expect.soft(binarios(mensagens).map(marcador)).toEqual([91, 90]);
  });
});

describe('ownership e contadores observáveis', () => {
  it('send copia imediatamente e bufferedAmount volta a zero uma vez', async () => {
    const { socket, mensagens } = abrir();
    await pronto(socket);
    const original = new Uint8Array(pacote(0, 1, 1_000, 111));
    socket.send(original);
    const durante = socket.bufferedAmount;
    original[18] = 222;
    await drenar();

    expect.soft(durante).toBeGreaterThan(0);
    expect.soft(binarios(mensagens).map(marcador)).toEqual([111]);
    expect.soft(socket.bufferedAmount).toBe(0);
  });
});
