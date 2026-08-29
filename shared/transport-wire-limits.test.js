import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  acceptServerWireSession,
  closeWireSession,
  openClientWireSession,
} from './transport-wire.js';

const flush = async (times = 80) => {
  for (let index = 0; index < times; index++) await Promise.resolve();
};
const SERVER_CLEANUP_LIMIT_MS = 100;

function packet(type, marker = 1, bytes = 19) {
  const buffer = new ArrayBuffer(bytes);
  const view = new DataView(buffer);
  view.setUint8(0, 0);
  view.setUint8(1, type);
  if (bytes >= 10) view.setFloat64(2, marker * 1000);
  new Uint8Array(buffer)[bytes - 1] = marker;
  return buffer;
}

function packetFor(slot, type, marker = 1, bytes = 19) {
  const buffer = packet(type, marker, bytes);
  new DataView(buffer).setUint8(0, slot);
  return buffer;
}

function mediaFrame({
  type = 3,
  sequence = 1,
  requiredControlSeq = 0,
  marker = 1,
  timestamp = marker * 1000,
  flags = 0,
} = {}) {
  const payload = new Uint8Array(packet(type, marker));
  new DataView(payload.buffer).setFloat64(2, timestamp);
  const frame = new Uint8Array(24 + payload.byteLength);
  const view = new DataView(frame.buffer);
  view.setUint32(0, 0x44534c32);
  view.setUint8(4, 1);
  view.setUint8(5, 3);
  view.setUint8(6, payload[0]);
  view.setUint8(7, type === 3 ? 1 : 0);
  view.setUint32(8, sequence);
  view.setUint32(12, requiredControlSeq);
  view.setUint32(16, payload.byteLength);
  view.setUint8(20, type);
  view.setUint8(21, flags);
  frame.set(payload, 24);
  return frame;
}

function controlFrame(sequence, text = '{}', kind = 2) {
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

function readable(bytes, close = true) {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      if (close) controller.close();
    },
  });
}

class Harness {
  constructor(createUnidirectionalStream) {
    this.createUnidirectionalStreamImpl = createUnidirectionalStream;
    this.createOptions = [];
    this.closed = new Promise((resolve) => (this.resolveClosed = resolve));
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => (this.uniController = controller),
    });
  }

  async createBidirectionalStream() {
    let controller;
    let handshake = true;
    const readable = new ReadableStream({ start: (value) => (controller = value) });
    this.controlController = controller;
    const writable = new WritableStream({
      write: (chunk) => {
        if (handshake) {
          handshake = false;
          controller.enqueue(new Uint8Array(chunk));
        }
      },
      close: () => controller.close(),
    });
    return { readable, writable };
  }

  createUnidirectionalStream(options) {
    this.createOptions.push(options);
    return this.createUnidirectionalStreamImpl?.(options);
  }

  close(info) {
    this.closeInfo = info;
    this.resolveClosed(info);
    try {
      this.uniController.close();
    } catch {
      // Tests may race explicit cleanup with an automatic close.
    }
  }
}

class NoHandshakeHarness extends Harness {
  async createBidirectionalStream() {
    let controller;
    const readable = new ReadableStream({ start: (value) => (controller = value) });
    this.controlController = controller;
    const writable = new WritableStream({
      write: () => undefined,
      close: () => controller.close(),
    });
    return { readable, writable };
  }
}

function serverControlStream() {
  let controller;
  const cancel = vi.fn();
  const writes = [];
  const writerClose = vi.fn();
  const readable = new ReadableStream({
    start: (value) => (controller = value),
    cancel,
  });
  const writable = new WritableStream({
    write: (chunk) => writes.push(new Uint8Array(chunk)),
    close: writerClose,
  });
  return { stream: { readable, writable }, controller, cancel, writes, writerClose };
}

function serverSession(stream) {
  let resolveClosed;
  let resolveOuter;
  const outerCancel = vi.fn((reason) => {
    resolveOuter?.({ done: true });
    return Promise.resolve(reason);
  });
  const outerRelease = vi.fn();
  const outerRead = stream
    ? vi.fn(async () => ({ value: stream, done: false }))
    : vi.fn(() => new Promise((resolve) => (resolveOuter = resolve)));
  const uniCancel = vi.fn();
  let uniController;
  const session = {
    closed: new Promise((resolve) => (resolveClosed = resolve)),
    incomingBidirectionalStreams: {
      getReader: () => ({
        closed: new Promise(() => {}),
        read: outerRead,
        cancel: outerCancel,
        releaseLock: outerRelease,
      }),
    },
    incomingUnidirectionalStreams: new ReadableStream({
      start: (controller) => (uniController = controller),
      cancel: uniCancel,
    }),
    close: vi.fn((info) => {
      session.closeInfo = info;
      resolveClosed(info);
    }),
  };
  return { session, outerCancel, outerRead, outerRelease, uniCancel, uniController };
}

function silentStream(cancels) {
  let finish;
  const pending = new Promise((resolve) => (finish = resolve));
  return {
    getReader: () => ({
      closed: Promise.resolve(),
      read: () => pending,
      cancel: vi.fn((reason) => {
        cancels.push(reason);
        finish({ done: true });
      }),
      releaseLock: vi.fn(),
    }),
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('deadline total do handshake no servidor WebTransport', () => {
  it('cancela a espera sem bidi, fecha uma vez e rejeita o bind', async () => {
    vi.useFakeTimers();
    const harness = serverSession();
    const opening = acceptServerWireSession(harness.session, {}, { handshakeTimeoutMs: 20 });
    const rejected = expect(opening).rejects.toThrow('handshake-timeout');

    await vi.advanceTimersByTimeAsync(20 + SERVER_CLEANUP_LIMIT_MS);
    await rejected;
    await flush();

    expect(harness.outerCancel).toHaveBeenCalledOnce();
    expect(harness.outerCancel).toHaveBeenCalledWith('handshake-timeout');
    expect(harness.outerRelease).toHaveBeenCalledOnce();
    expect(harness.session.close).toHaveBeenCalledOnce();
    expect(harness.session.closeInfo).toEqual({ closeCode: 1, reason: 'handshake-timeout' });
    expect(await harness.session.closed).toEqual({ closeCode: 1, reason: 'handshake-timeout' });
    expect(closeWireSession(harness.session, { closeCode: 1, reason: 'session-invalid' })).toBe(
      false,
    );
    expect(harness.session.close).toHaveBeenCalledOnce();
  });

  it('cancela controle sem HANDSHAKE, fecha uma vez e zera readers', async () => {
    vi.useFakeTimers();
    const control = serverControlStream();
    const harness = serverSession(control.stream);
    const opening = acceptServerWireSession(harness.session, {}, { handshakeTimeoutMs: 20 });
    const rejected = expect(opening).rejects.toThrow('handshake-timeout');
    await flush();

    await vi.advanceTimersByTimeAsync(20);
    await rejected;
    await flush();

    expect(harness.outerCancel).not.toHaveBeenCalled();
    expect(harness.outerRelease).toHaveBeenCalledOnce();
    expect(control.cancel).toHaveBeenCalledOnce();
    expect(control.cancel).toHaveBeenCalledWith('handshake-timeout');
    expect(control.writerClose).toHaveBeenCalledOnce();
    expect(harness.uniCancel).not.toHaveBeenCalled();
    expect(harness.session.close).toHaveBeenCalledOnce();
    expect(harness.session.closeInfo).toEqual({ closeCode: 1, reason: 'handshake-timeout' });
    expect(await harness.session.closed).toEqual({ closeCode: 1, reason: 'handshake-timeout' });
  });

  it('aceita HANDSHAKE válido dentro do mesmo deadline e desarma o timer', async () => {
    vi.useFakeTimers();
    const control = serverControlStream();
    const harness = serverSession(control.stream);
    const opening = acceptServerWireSession(harness.session, {}, { handshakeTimeoutMs: 20 });
    await flush();
    control.controller.enqueue(controlFrame(0, 'discord-locutor-wt/1', 1));

    const endpoint = await opening;
    await flush();
    await vi.advanceTimersByTimeAsync(40);

    expect(control.writes).toHaveLength(1);
    expect(harness.session.close).not.toHaveBeenCalled();
    expect(harness.outerRelease).toHaveBeenCalledOnce();
    endpoint.close();
    await flush();
    expect(harness.session.close).toHaveBeenCalledOnce();
  });
});

describe('autorizacao de midia antes do dispatcher WebTransport', () => {
  it.each([
    ['viewer', { auth: { role: 'viewer' }, control: false }],
    ['controle', { auth: { role: 'broadcaster' }, control: true }],
  ])('%s nao entrega binario nem arma gap de lane', async (_papel, userData) => {
    vi.useFakeTimers();
    const control = serverControlStream();
    const harness = serverSession(control.stream);
    harness.session.userData = userData;
    const messages = [];
    const onNeedKeyframe = vi.fn();
    const opening = acceptServerWireSession(harness.session, {
      onMessage: (data, binary) => messages.push({ data, binary }),
      onNeedKeyframe,
    });
    await flush();
    control.controller.enqueue(controlFrame(0, 'discord-locutor-wt/1', 1));
    const endpoint = await opening;

    harness.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 1, marker: 9, flags: 0 })),
    );
    harness.uniController.enqueue(
      readable(mediaFrame({ type: 2, sequence: 1, marker: 10, flags: 0 })),
    );
    await flush();
    await vi.advanceTimersByTimeAsync(3001);
    await flush();

    expect.soft(messages).toHaveLength(0);
    expect.soft(onNeedKeyframe).not.toHaveBeenCalled();
    expect.soft(harness.session.closeInfo).toBeUndefined();
    endpoint.close();
  });
});

describe('limites falsificáveis do wire WebTransport', () => {
  it('cancela o provisional mais antigo no 33º stream e todos vencem header em 750 ms', async () => {
    vi.useFakeTimers();
    const cancels = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session);

    for (let index = 0; index < 33; index++) session.uniController.enqueue(silentStream(cancels));
    await flush(160);
    expect(cancels).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(751);
    await flush();
    expect(cancels).toHaveLength(33);
    expect(session.closeInfo).toBeUndefined();
    const messages = [];
    endpoint.onMessage = (data) => messages.push(data);
    session.uniController.enqueue(readable(mediaFrame({ marker: 9 })));
    await flush();
    expect(messages).toHaveLength(1);
    endpoint.close();
  });

  it('aplica deadline de payload, FIN e barreira sem fechar na primeira mídia incompleta', async () => {
    vi.useFakeTimers();
    const session = new Harness();
    const endpoint = await openClientWireSession(session);

    const headerOnly = mediaFrame().slice(0, 24);
    session.uniController.enqueue(readable(headerOnly, false));
    await flush();
    await vi.advanceTimersByTimeAsync(1501);
    expect(session.closeInfo).toBeUndefined();

    session.uniController.enqueue(readable(mediaFrame({ marker: 2 }), false));
    await flush();
    await vi.advanceTimersByTimeAsync(501);
    expect(session.closeInfo).toBeUndefined();

    session.uniController.enqueue(readable(mediaFrame({ marker: 3, requiredControlSeq: 1 })));
    await flush();
    await vi.advanceTimersByTimeAsync(1501);
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'barrier-timeout' }));
    endpoint.close();
  });

  it('fecha no primeiro gap estrutural de controlSeq', async () => {
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    session.controlController.enqueue(controlFrame(2));
    await flush();
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'control-invalid' }));
    endpoint.close();
  });

  it('reseta oito frames inválidos e fecha media-framing-abuse no nono', async () => {
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    const invalid = mediaFrame();
    new DataView(invalid.buffer).setUint32(0, 0);
    for (let index = 0; index < 9; index++) session.uniController.enqueue(readable(invalid));
    await flush(200);
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'media-framing-abuse' }));
    endpoint.close();
  });

  it('um write de vídeo pendente não impede áudio de obter stream independente', async () => {
    let releaseVideo;
    const blocked = new Promise((resolve) => (releaseVideo = resolve));
    let creates = 0;
    const session = new Harness(() => {
      const index = creates++;
      return new WritableStream({
        write: index === 0 ? () => blocked : () => undefined,
      });
    });
    let buffered = 0;
    const endpoint = await openClientWireSession(session, {
      onBuffered: (amount) => (buffered = amount),
    });

    endpoint.send(packet(1, 1));
    endpoint.send(packet(3, 2));
    await flush();
    expect(creates).toBe(2);
    expect(buffered).toBeGreaterThan(0);

    releaseVideo();
    await flush();
    expect(buffered).toBe(0);
    endpoint.close();
  });

  it('dá prioridade nativa maior ao keyframe mais novo da lane', async () => {
    const session = new Harness(() => new WritableStream());
    const endpoint = await openClientWireSession(session);

    endpoint.send(packet(1, 10));
    endpoint.send(packet(1, 20));
    await flush();

    const priorities = session.createOptions.map((options) => options?.sendOrder);
    expect(priorities).toEqual([1, 2]);
    endpoint.close();
  });

  it('recupera após 65 drops locais priorizando keyframe sobre deltas bloqueados', async () => {
    let blocked = true;
    const pendingCreates = [];
    const diagnostics = [];
    const delivered = [];
    let buffered = 0;
    let session;
    const loopback = () =>
      new WritableStream({
        write: (chunk) => session.uniController.enqueue(readable(new Uint8Array(chunk))),
      });
    session = new Harness(() => {
      if (!blocked) return loopback();
      return new Promise((resolve) => pendingCreates.push(resolve));
    });
    const endpoint = await openClientWireSession(session, {
      onBuffered: (amount) => (buffered = amount),
      onDiagnostic: (event) => diagnostics.push(event),
      onMessage: (data, binary) => {
        if (binary) delivered.push(new Uint8Array(data).at(-1));
      },
    });

    for (let index = 0; index < 128; index++) endpoint.send(packet(2, index + 1, 1024));
    await flush();
    expect(pendingCreates.length).toBeGreaterThan(0);
    for (let index = 0; index < 65; index++) endpoint.send(packet(2, 129 + index, 1024));
    expect(diagnostics.filter((event) => event.detail === 'queue-full')).toHaveLength(65);
    expect(session.closeInfo).toBeUndefined();

    endpoint.send(packet(1, 201));
    await flush();
    expect(diagnostics.filter((event) => event.detail === 'queue-full')).toHaveLength(65);
    expect(buffered).toBe(19);

    blocked = false;
    for (const resolve of pendingCreates) resolve(loopback());
    await flush(1000);
    expect(buffered).toBe(0);

    // O peer confirma que a âncora foi entregue; só então um delta novo volta a
    // ser útil. Deltas gerados durante a travessia foram coalescidos de propósito.
    session.controlController.enqueue(
      controlFrame(1, JSON.stringify({ type: 'media-recovered', slot: 0 })),
    );
    await flush();
    endpoint.send(packet(2, 202));
    await flush();

    expect(delivered.slice(-2)).toEqual([201, 202]);
    expect(buffered).toBe(0);
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('queue-full não consome a sequência do próximo frame útil', async () => {
    let blocked = true;
    let release;
    const writeBlocked = new Promise((resolve) => (release = resolve));
    const frames = [];
    const diagnostics = [];
    const session = new Harness(
      () =>
        new WritableStream({
          write: (chunk) => {
            frames.push(new Uint8Array(chunk));
            return blocked ? writeBlocked : undefined;
          },
        }),
    );
    const endpoint = await openClientWireSession(session, {
      onDiagnostic: (event) => diagnostics.push(event),
    });

    for (let index = 0; index < 128; index++) endpoint.send(packetFor(0, 2, index + 1));
    await flush();
    endpoint.send(packetFor(7, 2, 200));
    expect(diagnostics).toContainEqual(
      expect.objectContaining({ reason: 'backpressure-drop', detail: 'queue-full' }),
    );

    blocked = false;
    release();
    await flush(1000);
    endpoint.send(packetFor(7, 1, 201));
    await flush();

    const slot7Sequences = frames
      .filter((frame) => frame[6] === 7)
      .map((frame) => new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(8));
    expect(slot7Sequences).toEqual([1]);
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('controle não é descartável: a 65ª mensagem fecha control-overflow', async () => {
    let release;
    const blocked = new Promise((resolve) => (release = resolve));
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    endpoint.writer.write = vi.fn(() => blocked);

    for (let index = 0; index < 65; index++) endpoint.send(`control-${index}`);
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'control-overflow' }));
    release();
  });
});

describe('epocas de midia no wire WebTransport', () => {
  it('stream-start descarta frame antigo tardio e aceita timestamp reiniciado', async () => {
    const messages = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session, {
      onMessage: (data, binary) => messages.push({ data, binary }),
    });

    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 1, marker: 1, timestamp: 5000 })),
    );
    await flush();
    session.controlController.enqueue(
      controlFrame(1, JSON.stringify({ type: 'stream-start', slot: 0 })),
    );
    await flush();
    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 2, marker: 2, timestamp: 6000 })),
    );
    session.uniController.enqueue(
      readable(
        mediaFrame({
          type: 1,
          sequence: 3,
          requiredControlSeq: 1,
          marker: 3,
          timestamp: 1000,
          flags: 1,
        }),
      ),
    );
    await flush(120);

    const markers = messages
      .filter((entry) => entry.binary)
      .map((entry) => new Uint8Array(entry.data)[18]);
    expect(markers).toEqual([1, 3]);
    endpoint.close();
  });

  it('controle de epoca descarta somente as lanes de saida no escopo indicado', async () => {
    const pending = [];
    const session = new Harness(() => new Promise((resolve) => pending.push(resolve)));
    let buffered = 0;
    const endpoint = await openClientWireSession(session, {
      onBuffered: (amount) => (buffered = amount),
    });

    endpoint.send(packetFor(0, 3, 1));
    endpoint.send(packetFor(1, 3, 2));
    expect(buffered).toBe(38);
    endpoint.send(JSON.stringify({ type: 'stream-start', slot: 0 }));
    await flush();
    expect(buffered).toBe(19);
    endpoint.send(JSON.stringify({ type: 'start' }));
    await flush();
    expect(buffered).toBe(0);

    for (const resolve of pending) resolve(new WritableStream());
    await flush();
    endpoint.close();
  });

  it('controle de nova epoca cancela reader antigo que ja possui header', async () => {
    const cancel = vi.fn();
    const header = mediaFrame({ type: 1, sequence: 1 }).slice(0, 24);
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    session.uniController.enqueue(
      new ReadableStream({
        start(controller) {
          controller.enqueue(header);
        },
        cancel,
      }),
    );
    await flush();
    session.controlController.enqueue(
      controlFrame(1, JSON.stringify({ type: 'stream-start', slot: 0 })),
    );
    await flush();

    expect(cancel).toHaveBeenCalledWith('media-epoch-obsolete');
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });
});

describe('bordas defensivas e recuperacao do wire WebTransport', () => {
  it('recusa controle como primeiro frame sem vazar callback antes do handshake', async () => {
    const messages = [];
    const session = new NoHandshakeHarness();
    const opening = openClientWireSession(session, {
      onMessage: (data) => messages.push(data),
    });
    const rejected = expect(opening).rejects.toThrow('transport-closed');
    await flush();
    session.controlController.enqueue(controlFrame(1, JSON.stringify({ type: 'state' })));
    await rejected;

    expect(messages).toEqual([]);
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'control-invalid' }));
  });

  it('recusa midia antes do handshake sem vazar callback', async () => {
    const messages = [];
    const cancel = vi.fn();
    const session = new NoHandshakeHarness();
    const opening = openClientWireSession(session, {
      onMessage: (data) => messages.push(data),
    });
    const rejected = expect(opening).rejects.toThrow('transport-closed');
    await flush();
    session.uniController.enqueue(
      new ReadableStream({
        start(controller) {
          controller.enqueue(mediaFrame());
          controller.close();
        },
        cancel,
      }),
    );
    await rejected;
    await flush();

    expect(messages).toEqual([]);
    expect(cancel).toHaveBeenCalledWith('handshake-required');
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'handshake-required' }));
  });

  it('handshake valido libera controle e midia posteriores', async () => {
    const messages = [];
    const session = new NoHandshakeHarness();
    const opening = openClientWireSession(session, {
      onMessage: (data, binary) => messages.push({ data, binary }),
    });
    await flush();
    session.controlController.enqueue(controlFrame(0, 'discord-locutor-wt/1', 1));
    const endpoint = await opening;
    session.controlController.enqueue(controlFrame(1, JSON.stringify({ type: 'state' })));
    session.uniController.enqueue(readable(mediaFrame({ marker: 7 })));
    await flush(80);

    expect(messages.map((entry) => entry.binary)).toEqual([false, true]);
    expect(new Uint8Array(messages[1].data)[18]).toBe(7);
    endpoint.close();
  });

  it('recusa inconsistencias internas de frame sem tratar o primeiro caso como abuso fatal', async () => {
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    const frames = [];

    const wrongSlot = mediaFrame();
    wrongSlot[24] = 1;
    frames.push(wrongSlot);
    const wrongPayloadType = mediaFrame({ type: 3 });
    wrongPayloadType[25] = 1;
    frames.push(wrongPayloadType);
    const unsupportedType = mediaFrame({ type: 3 });
    unsupportedType[20] = 4;
    unsupportedType[25] = 4;
    unsupportedType[7] = 0;
    frames.push(unsupportedType);
    const wrongLaneClass = mediaFrame({ type: 3 });
    wrongLaneClass[7] = 0;
    frames.push(wrongLaneClass);

    for (const frame of frames) session.uniController.enqueue(readable(frame));
    await flush(120);
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('recusa framing estrutural de controle no primeiro frame', async () => {
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    const invalid = controlFrame(1);
    new DataView(invalid.buffer).setUint32(0, 0);
    session.controlController.enqueue(invalid);
    await flush();
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'control-invalid' }));
    endpoint.close();
  });

  it('mantem no maximo 32 writers e entrega o credito liberado ao proximo item', async () => {
    let creates = 0;
    const releases = [];
    const session = new Harness(() => {
      creates++;
      let release;
      const write = new Promise((resolve) => (release = resolve));
      releases.push(release);
      return {
        getWriter: () => ({
          closed: Promise.resolve(),
          ready: Promise.resolve(),
          write: () => write,
          close: () => Promise.resolve(),
          abort: () => Promise.resolve(),
        }),
      };
    });
    const endpoint = await openClientWireSession(session);
    for (let slot = 0; slot < 33; slot++) endpoint.send(packetFor(slot, 3, slot + 1));
    await flush(120);
    expect(creates).toBe(32);

    releases[0]();
    await flush(120);
    expect(creates).toBe(33);
    for (const release of releases.slice(1)) release();
    await flush(120);
    endpoint.close();
  });

  it('32 creates presos reservam passagem para keyframe de recuperacao alem de 500 ms', async () => {
    vi.useFakeTimers();
    const pendingCreates = [];
    const sent = [];
    let creates = 0;
    let buffered = 0;
    const session = new Harness(() => {
      creates++;
      if (creates <= 32) {
        return new Promise((resolve) => pendingCreates.push(resolve));
      }
      return new WritableStream({
        write: (chunk) => sent.push(new Uint8Array(chunk)),
      });
    });
    const endpoint = await openClientWireSession(session, {
      onBuffered: (amount) => (buffered = amount),
    });

    for (let slot = 0; slot < 32; slot++) endpoint.send(packetFor(slot, 2, slot + 1));
    await flush(160);
    expect(creates).toBe(32);

    endpoint.send(packetFor(0, 1, 201));
    await flush(160);
    await vi.advanceTimersByTimeAsync(501);
    await flush(240);

    const recovery = sent.find((frame) => frame[6] === 0 && frame.at(-1) === 201);
    expect.soft(recovery).toBeDefined();
    expect.soft((recovery?.[21] ?? 0) & 1).toBe(1);
    expect.soft(session.closeInfo).toBeUndefined();

    for (const resolve of pendingCreates) resolve(new WritableStream());
    await flush(240);
    expect.soft(buffered).toBe(0);
    endpoint.close();
  });

  it('fecha ao exceder 64 itens bloqueados por uma barreira inexistente', async () => {
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    for (let sequence = 1; sequence <= 65; sequence++) {
      session.uniController.enqueue(
        readable(mediaFrame({ sequence, requiredControlSeq: 1, marker: sequence })),
      );
    }
    await flush(240);
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'barrier-overflow' }));
    endpoint.close();
  });

  it('keyframe marcado ultrapassa o gap e torna o antecessor tardio obsoleto', async () => {
    const markers = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session, {
      onMessage: (data, binary) => binary && markers.push(new Uint8Array(data)[18]),
    });
    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 2, marker: 2, flags: 1 })),
    );
    await flush();
    expect(markers).toEqual([2]);
    session.uniController.enqueue(readable(mediaFrame({ type: 1, sequence: 1, marker: 1 })));
    await flush();
    expect(markers).toEqual([2]);
    endpoint.close();
  });

  it('keyframes comuns reordenados continuam completos e na ordem', async () => {
    const markers = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session, {
      onMessage: (data, binary) => binary && markers.push(new Uint8Array(data)[18]),
    });

    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 2, marker: 2, flags: 0 })),
    );
    await flush();
    expect(markers).toEqual([]);

    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 1, marker: 1, flags: 0 })),
    );
    await flush();
    expect(markers).toEqual([1, 2]);
    endpoint.close();
  });

  it('gap duradouro reaproveita o keyframe comum que ja chegou em vez de descarta-lo', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const markers = [];
    const pedidos = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session, {
      onMessage: (data, binary) => binary && markers.push(new Uint8Array(data)[18]),
      onNeedKeyframe: (slot) => pedidos.push(slot),
    });

    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 1, marker: 1, flags: 0 })),
    );
    session.uniController.enqueue(
      readable(mediaFrame({ type: 2, sequence: 3, marker: 3, flags: 0 })),
    );
    session.uniController.enqueue(
      readable(mediaFrame({ type: 1, sequence: 4, marker: 4, flags: 0 })),
    );
    await flush();
    expect(markers).toEqual([1]);

    await vi.advanceTimersByTimeAsync(3001);
    await flush();
    expect(markers).toEqual([1, 4]);
    expect(pedidos).toEqual([]);
    endpoint.close();
  });

  it('fecha quando a promise closed da sessao rejeita depois do handshake', async () => {
    let rejectClosed;
    const session = new Harness();
    session.closed = new Promise((_, reject) => (rejectClosed = reject));
    const endpoint = await openClientWireSession(session);
    rejectClosed(new Error('native-closed-error'));
    await flush();
    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'transport-error' }));
    endpoint.close();
  });

  it('aplica o teto agregado de 8 MiB a payloads simultaneos', async () => {
    const declared = 4 * 1024 * 1024;
    const partial = new Uint8Array(24 + declared - 1);
    partial.set(mediaFrame().slice(0, 24));
    new DataView(partial.buffer).setUint32(16, declared);
    const cancels = [];
    const openPartial = (marker) =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(partial);
        },
        cancel(reason) {
          cancels.push({ marker, reason });
        },
      });
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    session.uniController.enqueue(openPartial(1));
    await flush();
    session.uniController.enqueue(openPartial(2));
    await flush(80);

    expect(cancels).toContainEqual({ marker: 2, reason: 'media-receive-cap' });
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('mantem erros de quota com nome estavel mesmo sem DOMException global', async () => {
    const original = globalThis.DOMException;
    vi.stubGlobal('DOMException', undefined);
    try {
      const session = new Harness();
      const endpoint = await openClientWireSession(session);
      expect(() => endpoint.send('x'.repeat(256 * 1024 + 1))).toThrow(
        expect.objectContaining({ name: 'QuotaExceededError' }),
      );
      expect(() => endpoint.send(new Uint8Array(4 * 1024 * 1024 + 1))).toThrow(
        expect.objectContaining({ name: 'QuotaExceededError' }),
      );
      endpoint.close();
    } finally {
      vi.stubGlobal('DOMException', original);
    }
  });

  it('limita 128 streams ativos com header e cancela o mais antigo no 129o', async () => {
    vi.useFakeTimers();
    const cancels = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    const openPartial = (sequence) =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(mediaFrame({ sequence }).slice(0, 24));
        },
        cancel() {
          cancels.push(sequence);
        },
      });

    for (let sequence = 1; sequence <= 128; sequence++) {
      session.uniController.enqueue(openPartial(sequence));
    }
    await flush(500);
    expect(cancels).toHaveLength(0);
    session.uniController.enqueue(openPartial(129));
    await flush(40);
    expect(cancels).toEqual([1]);
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('ignora chunk vazio, aceita o valido seguinte e reseta excessos isoladamente', async () => {
    const messages = [];
    const session = new Harness();
    const endpoint = await openClientWireSession(session, {
      onMessage: (data, binary) => binary && messages.push(data),
    });
    const valid = mediaFrame({ marker: 7 });
    session.uniController.enqueue(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array());
          controller.enqueue(valid);
          controller.close();
        },
      }),
    );

    const tooLarge = new Uint8Array(24 + 4 * 1024 * 1024 + 1);
    tooLarge.set(valid.slice(0, 24));
    new DataView(tooLarge.buffer).setUint32(16, 4 * 1024 * 1024);
    session.uniController.enqueue(readable(tooLarge));
    const extra = new Uint8Array(valid.byteLength + 1);
    extra.set(valid);
    session.uniController.enqueue(readable(extra));
    await flush(160);

    expect(messages).toHaveLength(1);
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('classifica falha de read, usa cancel fallback e mantem a sessao', async () => {
    const streamCancel = vi.fn();
    const reader = {
      closed: Promise.resolve(),
      read: vi.fn().mockRejectedValue(new Error('read-broken')),
      cancel: vi.fn(() => {
        throw new Error('reader-cancel-broken');
      }),
      releaseLock: vi.fn(),
    };
    const session = new Harness();
    const endpoint = await openClientWireSession(session);
    session.uniController.enqueue({ getReader: () => reader, cancel: streamCancel });
    session.uniController.enqueue({});
    await flush();

    expect(streamCancel).toHaveBeenCalledWith('read-broken');
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });

  it('falha do reader de streams fecha uma vez mesmo se o observador de erro falhar', async () => {
    const releaseLock = vi.fn();
    let rejectRead;
    const read = new Promise((_, reject) => (rejectRead = reject));
    const session = new Harness();
    session.incomingUnidirectionalStreams = {
      getReader: () => ({
        closed: Promise.resolve(),
        read: vi.fn(() => read),
        releaseLock,
      }),
    };
    const endpoint = await openClientWireSession(session, {
      onError: () => {
        throw new Error('observer-broken');
      },
    });
    rejectRead(new Error('accept-broken'));
    await flush();

    expect(session.closeInfo).toEqual(expect.objectContaining({ reason: 'media-accept-failed' }));
    expect(releaseLock).toHaveBeenCalledOnce();
    endpoint.close();
  });

  it('descarta stream criado depois do timeout mesmo quando getWriter falha', async () => {
    vi.useFakeTimers();
    let resolveCreate;
    const abort = vi.fn();
    const diagnostics = [];
    const session = new Harness(() => new Promise((resolve) => (resolveCreate = resolve)));
    const endpoint = await openClientWireSession(session, {
      onDiagnostic: (event) => diagnostics.push(event),
    });
    endpoint.send(packet(3, 1));
    await vi.advanceTimersByTimeAsync(501);
    resolveCreate({
      getWriter() {
        throw new Error('late-lock');
      },
      abort,
    });
    await flush(80);

    expect(diagnostics).toContainEqual(expect.objectContaining({ detail: 'media-create-timeout' }));
    expect(abort).toHaveBeenCalledOnce();
    endpoint.close();
  });

  it('erro de write e abort e apenas uma falha local recuperavel', async () => {
    const diagnostics = [];
    const session = new Harness(() => ({
      getWriter: () => ({
        closed: Promise.resolve(),
        ready: Promise.resolve(),
        write: vi.fn().mockRejectedValue(new Error('write-broken')),
        close: vi.fn(),
        abort: vi.fn().mockRejectedValue(new Error('abort-broken')),
      }),
    }));
    let buffered = 0;
    const endpoint = await openClientWireSession(session, {
      onBuffered: (amount) => (buffered = amount),
      onDiagnostic: (event) => diagnostics.push(event),
    });
    endpoint.send(packet(3, 1));
    await flush(80);

    expect(buffered).toBe(0);
    expect(diagnostics).toContainEqual(expect.objectContaining({ detail: 'write-broken' }));
    expect(session.closeInfo).toBeUndefined();
    endpoint.close();
  });
});
