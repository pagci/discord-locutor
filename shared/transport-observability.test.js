import { afterEach, describe, expect, it, vi } from 'vitest';

import { createTransport } from './transport.js';
import { nodeFor, shardKey } from './shard.js';

class EventSocket {
  static instances = [];
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  constructor(url) {
    this.url = url;
    this.readyState = EventSocket.CONNECTING;
    this.listeners = new Map();
    EventSocket.instances.push(this);
  }

  addEventListener(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add(listener);
  }

  send(data) {
    this.sent = data;
  }

  close(code = 1000, reason = '') {
    this.readyState = EventSocket.CLOSED;
    this.emit('close', { code, reason });
  }

  emit(type, event = {}) {
    if (type === 'open') this.readyState = EventSocket.OPEN;
    for (const listener of this.listeners.get(type) ?? []) listener.call(this, event);
  }
}

class EmitterSocket {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.listeners = new Map();
    EmitterSocket.instances.push(this);
  }

  on(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add(listener);
  }

  send(data) {
    this.sent = data;
  }

  close(code, reason) {
    this.emit('close', code, reason);
  }

  emit(type, ...args) {
    if (type === 'open') this.readyState = 1;
    for (const listener of this.listeners.get(type) ?? []) listener(...args);
  }
}

class RejectingTransport {
  static instances = [];

  constructor(url, options) {
    this.url = url;
    this.options = options;
    this.ready = Promise.reject(new Error('native handshake failed'));
    this.closed = new Promise(() => {});
    this.draining = new Promise(() => {});
    this.close = vi.fn();
    RejectingTransport.instances.push(this);
  }
}

const flush = async (times = 12) => {
  for (let index = 0; index < times; index++) await Promise.resolve();
};

function wireControlFrame(kind, sequence, text) {
  const payload = new TextEncoder().encode(text);
  const frame = new Uint8Array(16 + payload.byteLength);
  const view = new DataView(frame.buffer);
  view.setUint32(0, 0x44534c32);
  view.setUint8(4, 1);
  view.setUint8(5, kind);
  view.setUint32(8, payload.byteLength);
  view.setUint32(12, sequence);
  frame.set(payload, 16);
  return frame;
}

function joinFrames(frames) {
  const joined = new Uint8Array(frames.reduce((total, frame) => total + frame.byteLength, 0));
  let offset = 0;
  for (const frame of frames) {
    joined.set(frame, offset);
    offset += frame.byteLength;
  }
  return joined;
}

function coalescedControlTransport(payloads) {
  return class CoalescedControlTransport {
    static instances = [];

    constructor() {
      this.ready = Promise.resolve();
      this.closed = new Promise(() => {});
      this.draining = new Promise(() => {});
      this.close = vi.fn();
      this.incomingUnidirectionalStreams = new ReadableStream();
      this.constructor.instances.push(this);
    }

    async createBidirectionalStream() {
      let controller;
      let first = true;
      const readable = new ReadableStream({ start: (value) => (controller = value) });
      const writable = new WritableStream({
        write() {
          if (!first) return;
          first = false;
          const frames = [wireControlFrame(1, 0, 'discord-locutor-wt/1')];
          payloads.forEach((payload, index) => {
            frames.push(wireControlFrame(2, index + 1, payload));
          });
          controller.enqueue(joinFrames(frames));
        },
        close: () => controller.close(),
      });
      return { readable, writable };
    }
  };
}

function capability(body, { ok = true, status = 200 } = {}) {
  return vi.fn(async () => ({ ok, status, json: async () => body }));
}

afterEach(() => {
  EventSocket.instances = [];
  EmitterSocket.instances = [];
  RejectingTransport.instances = [];
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('observabilidade e ramos defensivos da factory', () => {
  it('aceita listener WT cross-host quando node, shard e /wt são autoritativos', async () => {
    const room = 'cross-host-room';
    let instance = 'cross-host-instance';
    while (nodeFor(shardKey({ instance }), 2) !== 0) instance += '-x';
    const body = globalThis.Buffer.from(JSON.stringify({ room, instance })).toString('base64url');
    const token = `${body}.signature`;
    const diagnostics = [];
    createTransport({
      wsUrl: `wss://app.example.invalid/n0/ws?t=${token}`,
      capabilityUrl: 'https://app.example.invalid/n0/api/transports',
      WebTransport: RejectingTransport,
      WebSocket: EventSocket,
      fetch: capability({
        node: 0,
        shards: 2,
        webtransport: { url: 'https://media.example.invalid/wt' },
      }),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush();

    expect(RejectingTransport.instances).toHaveLength(1);
    expect(RejectingTransport.instances[0].url).toContain('https://media.example.invalid/wt?t=');
    expect(diagnostics).not.toContainEqual(
      expect.objectContaining({ reason: 'wt-origin-invalid' }),
    );
  });

  it('seleciona WS sem tentativa quando WebTransport não existe', async () => {
    const diagnostics = [];
    const selections = [];
    const closes = [];
    const socket = createTransport({
      wsUrl: 'ws://host/ws?t=secret',
      WebTransport: undefined,
      WebSocket: EventSocket,
      fetch: vi.fn(),
      onDiagnostic: (event) => diagnostics.push(event),
      onTransport: (event) => selections.push(event),
    });
    socket.addEventListener('ignored', null);
    socket.addEventListener('close', (event) => closes.push(event));
    socket.removeEventListener('missing', vi.fn());
    expect(() => socket.send('too-soon')).toThrow(/OPEN/);

    await flush();
    EventSocket.instances[0].emit('open');
    EventSocket.instances[0].emit('open');

    expect(diagnostics).toContainEqual({ transport: 'webtransport', reason: 'unsupported' });
    expect(selections).toEqual([
      expect.objectContaining({
        transport: 'websocket',
        reason: 'selected',
        attemptedWebTransport: false,
        fallbackReason: 'unsupported',
      }),
    ]);
    socket.send('ready');
    expect(EventSocket.instances[0].sent).toBe('ready');
    EventSocket.instances[0].emit('close', { code: 1000, reason: 'done' });
    EventSocket.instances[0].emit('close', { code: 1000, reason: 'duplicate' });
    expect(closes).toHaveLength(1);
  });

  it('distingue fetch ausente e ausência total de fallback', async () => {
    const diagnostics = [];
    createTransport({
      wsUrl: 'wss://host/ws',
      WebTransport: class {},
      WebSocket: undefined,
      fetch: undefined,
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush();
    expect(diagnostics).toEqual([{ transport: 'webtransport', reason: 'capability-error' }]);
  });

  it('fecha de forma observável quando o construtor WS falha', async () => {
    const diagnostics = [];
    const closes = [];
    const socket = createTransport({
      wsUrl: 'wss://host/ws',
      WebTransport: undefined,
      WebSocket: class {
        constructor() {
          throw new Error('constructor failed');
        }
      },
      fetch: vi.fn(),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    socket.addEventListener('close', (event) => closes.push(event));
    await flush();

    expect(diagnostics).toContainEqual({ transport: 'websocket', reason: 'ws-connect-failed' });
    expect(closes).toHaveLength(1);
    socket.close();
  });

  it('send descarta em CLOSING/CLOSED e preserva InvalidStateError só em CONNECTING', async () => {
    const socket = createTransport({
      wsUrl: 'ws://host/ws',
      WebTransport: undefined,
      WebSocket: EventSocket,
      fetch: vi.fn(),
    });
    expect(() => socket.send('connecting')).toThrow(
      expect.objectContaining({ name: 'InvalidStateError' }),
    );
    await flush();
    EventSocket.instances[0].emit('open');
    socket.readyState = socket.CLOSING;
    expect(socket.send('closing')).toBeUndefined();
    socket.readyState = socket.CLOSED;
    expect(socket.send('closed')).toBeUndefined();
    expect(EventSocket.instances[0].sent).toBeUndefined();
  });

  it('adapta WebSocket EventEmitter, binário textual e unexpected-response sem status', async () => {
    const diagnostics = [];
    const messages = [];
    const socket = createTransport({
      wsUrl: 'ws://host/ws',
      WebTransport: undefined,
      WebSocket: EmitterSocket,
      fetch: vi.fn(),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    socket.addEventListener('message', (event) => messages.push(event.data));
    await flush();
    const native = EmitterSocket.instances[0];
    native.emit('open');
    native.emit('message', new Uint8Array([65]), false);
    const resume = vi.fn();
    native.emit('unexpected-response', {}, { resume });

    expect(messages).toEqual(['65']);
    expect(resume).toHaveBeenCalledOnce();
    expect(diagnostics).toContainEqual({
      transport: 'websocket',
      status: undefined,
      reason: 'ws-connect-failed',
    });
  });

  it.each([
    [{ ok: false, status: 503 }, 'capability-error'],
    [{ ok: true, body: { node: 0, webtransport: null } }, 'not-advertised'],
    [
      {
        ok: true,
        body: { node: 0, webtransport: { url: 'http://host/wt' } },
      },
      'webtransport-unavailable',
    ],
  ])('sanitiza falha de capability %#', async (scenario, expectedReason) => {
    const diagnostics = [];
    const fetch = scenario.ok
      ? capability(scenario.body)
      : capability({}, { ok: false, status: scenario.status });
    createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      capabilityUrl: 'https://host/api/transports',
      WebTransport: RejectingTransport,
      WebSocket: EventSocket,
      fetch,
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush();

    expect(diagnostics).toContainEqual(
      expect.objectContaining({ transport: 'webtransport', reason: expectedReason }),
    );
    expect(EventSocket.instances).toHaveLength(1);
  });

  it('preserva status 421 do path do nó sem vazar a URL', async () => {
    const diagnostics = [];
    createTransport({
      wsUrl: 'wss://host/n0/ws?t=secret',
      capabilityUrl: 'https://host/n0/api/transports',
      WebTransport: RejectingTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/n1/wt' } }),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush();
    expect(diagnostics).toContainEqual({
      transport: 'webtransport',
      status: 421,
      reason: 'wt-node-misdirected',
    });
  });

  it('classifica rejeição nativa pré-OPEN como handshake e seleciona um WS', async () => {
    const diagnostics = [];
    const selections = [];
    createTransport({
      wsUrl: 'wss://host/ws?t=secret&x=drop',
      capabilityUrl: 'https://host/api/transports',
      WebTransport: RejectingTransport,
      WebSocket: EventSocket,
      fetch: capability({
        node: 0,
        webtransport: { url: 'https://host/wt', hashes: ['AQID'] },
      }),
      onDiagnostic: (event) => diagnostics.push(event),
      onTransport: (event) => selections.push(event),
    });
    await flush();

    expect(RejectingTransport.instances[0].url).toBe('https://host/wt?t=secret');
    expect(RejectingTransport.instances[0].options).toEqual({
      allowPooling: false,
      serverCertificateHashes: [{ algorithm: 'sha-256', value: new Uint8Array([1, 2, 3]) }],
    });
    expect(diagnostics).toContainEqual({
      transport: 'webtransport',
      reason: 'handshake-error',
    });
    EventSocket.instances[0].emit('open');
    expect(selections[0]).toEqual(
      expect.objectContaining({ transport: 'websocket', fallbackReason: 'handshake-error' }),
    );
  });

  it('usa decodificação base64 do browser e eventos sem Event global', async () => {
    const originalBuffer = globalThis.Buffer;
    vi.stubGlobal('Buffer', undefined);
    vi.stubGlobal('Event', undefined);
    try {
      createTransport({
        wsUrl: 'wss://host/ws?t=secret',
        capabilityUrl: 'https://host/api/transports',
        WebTransport: RejectingTransport,
        WebSocket: EventSocket,
        fetch: capability({
          node: 0,
          webtransport: { url: 'https://host/wt', hashes: ['AQID'] },
        }),
      });
      await flush();
      expect(RejectingTransport.instances[0].options.serverCertificateHashes[0].value).toEqual(
        new Uint8Array([1, 2, 3]),
      );
      EventSocket.instances[0].emit('open');
    } finally {
      vi.stubGlobal('Buffer', originalBuffer);
    }
  });
});

describe('corridas de selecao do transporte', () => {
  it('ready tardio nao vence o WS escolhido enquanto ele ainda esta CONNECTING', async () => {
    vi.useFakeTimers();
    let releaseReady;
    const streamCalls = vi.fn();
    class LateTransport {
      static instances = [];

      constructor() {
        this.ready = new Promise((resolve) => (releaseReady = resolve));
        this.closed = new Promise(() => {});
        this.draining = new Promise(() => {});
        this.close = vi.fn();
        this.createBidirectionalStream = streamCalls;
        LateTransport.instances.push(this);
      }
    }

    const socket = createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      timeoutMs: 1,
      WebTransport: LateTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/wt' } }),
    });
    await flush();
    await vi.advanceTimersByTimeAsync(1);
    expect(EventSocket.instances).toHaveLength(1);
    expect(socket.transport).toBeNull();

    releaseReady();
    await flush();
    expect(streamCalls).not.toHaveBeenCalled();
    expect(LateTransport.instances[0].close).toHaveBeenCalledOnce();
    expect(socket.transport).toBeNull();

    EventSocket.instances[0].emit('open');
    expect(socket.transport).toBe('websocket');
  });

  it('handshake tardio nao reverte o fallback mesmo com WS ainda CONNECTING', async () => {
    vi.useFakeTimers();
    const selections = [];
    const messages = [];
    class LateHandshakeTransport {
      static instances = [];

      constructor() {
        this.ready = Promise.resolve();
        this.closed = new Promise(() => {});
        this.draining = new Promise(() => {});
        this.close = vi.fn();
        this.incomingUnidirectionalStreams = new ReadableStream();
        LateHandshakeTransport.instances.push(this);
      }

      async createBidirectionalStream() {
        let controller;
        let first = true;
        const readable = new ReadableStream({ start: (value) => (controller = value) });
        const writable = new WritableStream({
          write(chunk) {
            if (!first) return;
            first = false;
            setTimeout(() => controller.enqueue(new Uint8Array(chunk)), 60);
          },
          close: () => controller.close(),
        });
        return { readable, writable };
      }
    }

    const socket = createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      timeoutMs: 20,
      WebTransport: LateHandshakeTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/wt' } }),
      onTransport: (event) => selections.push(event),
    });
    socket.addEventListener('message', (event) => messages.push(event.data));
    await flush();
    await vi.advanceTimersByTimeAsync(20);
    expect(EventSocket.instances).toHaveLength(1);
    expect(socket.transport).toBeNull();

    await vi.advanceTimersByTimeAsync(40);
    await flush(40);
    expect(socket.transport).toBeNull();
    expect(messages).toEqual([]);
    expect(LateHandshakeTransport.instances[0].close).toHaveBeenCalled();

    EventSocket.instances[0].emit('open');
    expect(socket.transport).toBe('websocket');
    expect(selections.map((event) => event.transport)).toEqual(['websocket']);
  });

  it('controle pre-handshake do WT perdedor nunca alcanca o socket logico', async () => {
    vi.useFakeTimers();
    const messages = [];
    class PreHandshakeControlTransport {
      static instances = [];

      constructor() {
        this.ready = Promise.resolve();
        this.closed = new Promise(() => {});
        this.draining = new Promise(() => {});
        this.close = vi.fn();
        this.incomingUnidirectionalStreams = new ReadableStream();
        PreHandshakeControlTransport.instances.push(this);
      }

      async createBidirectionalStream() {
        let controller;
        let first = true;
        const readable = new ReadableStream({ start: (value) => (controller = value) });
        const writable = new WritableStream({
          write() {
            if (!first) return;
            first = false;
            const payload = new TextEncoder().encode(
              JSON.stringify({ type: 'state', secret: 'from-losing-wt' }),
            );
            const frame = new Uint8Array(16 + payload.byteLength);
            const view = new DataView(frame.buffer);
            view.setUint32(0, 0x44534c32);
            view.setUint8(4, 1);
            view.setUint8(5, 2);
            view.setUint32(8, payload.byteLength);
            view.setUint32(12, 1);
            frame.set(payload, 16);
            setTimeout(() => controller.enqueue(frame), 5);
          },
          close: () => controller.close(),
        });
        return { readable, writable };
      }
    }

    const socket = createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      timeoutMs: 20,
      WebTransport: PreHandshakeControlTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/wt' } }),
    });
    socket.addEventListener('message', (event) => messages.push(event.data));
    await flush();
    await vi.advanceTimersByTimeAsync(5);
    await flush(40);

    expect(messages).toEqual([]);
    expect(socket.transport).toBeNull();
    expect(EventSocket.instances).toHaveLength(1);
    expect(PreHandshakeControlTransport.instances[0].close).toHaveBeenCalled();

    EventSocket.instances[0].emit('open');
    expect(socket.transport).toBe('websocket');
    expect(messages).toEqual([]);
  });

  it('fecha control-overflow no 65º evento coalescido antes da seleção', async () => {
    const diagnostics = [];
    const messages = [];
    const OverflowControlTransport = coalescedControlTransport(
      Array.from({ length: 65 }, (_, index) => JSON.stringify({ sequence: index + 1 })),
    );

    const socket = createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      WebTransport: OverflowControlTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/wt' } }),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    socket.addEventListener('message', (event) => messages.push(event.data));
    await flush(80);

    expect(messages).toEqual([]);
    expect(socket.transport).toBeNull();
    expect(OverflowControlTransport.instances[0].close).toHaveBeenCalledOnce();
    expect(OverflowControlTransport.instances[0].close).toHaveBeenCalledWith({
      closeCode: 1011,
      reason: 'control-overflow',
    });
    expect(diagnostics).toContainEqual({
      transport: 'webtransport',
      reason: 'control-overflow',
    });
    expect(EventSocket.instances).toHaveLength(1);

    EventSocket.instances[0].emit('open');
    expect(socket.transport).toBe('websocket');
  });

  it('fecha control-overflow quando controles pré-seleção excedem 512 KiB', async () => {
    const payload = 'x'.repeat(256 * 1024);
    const OverflowBytesTransport = coalescedControlTransport([payload, payload, 'x']);
    const diagnostics = [];
    createTransport({
      wsUrl: 'wss://host/ws?t=secret',
      WebTransport: OverflowBytesTransport,
      WebSocket: EventSocket,
      fetch: capability({ node: 0, webtransport: { url: 'https://host/wt' } }),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush(80);

    expect(OverflowBytesTransport.instances[0].close).toHaveBeenCalledOnce();
    expect(OverflowBytesTransport.instances[0].close).toHaveBeenCalledWith({
      closeCode: 1011,
      reason: 'control-overflow',
    });
    expect(diagnostics).toContainEqual({
      transport: 'webtransport',
      reason: 'control-overflow',
    });
    expect(EventSocket.instances).toHaveLength(1);
  });
});
