import { afterEach, describe, expect, it, vi } from 'vitest';

import { openClientWireSession } from './transport-wire.js';

const bytes = (value) =>
  value instanceof Uint8Array
    ? new Uint8Array(value)
    : new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice();

const readable = (payload) =>
  new ReadableStream({
    start(controller) {
      controller.enqueue(payload);
      controller.close();
    },
  });

function packet(type, marker, size = 260) {
  const value = new Uint8Array(Math.max(19, size));
  value[0] = 0;
  value[1] = type;
  const view = new DataView(value.buffer);
  view.setFloat64(2, marker);
  view.setFloat64(10, Date.now());
  value[value.byteLength - 1] = marker;
  return value;
}

class LoopbackSession {
  constructor(maxDatagramSize = 96) {
    this.ready = Promise.resolve();
    this.closed = new Promise((resolve) => (this.resolveClosed = resolve));
    this.datagramWrites = [];
    this.lostOutgoing = 0;
    this.beforeReliableClose = null;
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => (this.uniController = controller),
    });
    this.datagramWritable = new WritableStream({
      write: (chunk) => {
        this.datagramWrites.push(bytes(chunk));
      },
    });
    this.datagrams = {
      maxDatagramSize,
      readable: new ReadableStream({
        start: (controller) => (this.datagramController = controller),
      }),
      writable: this.datagramWritable,
    };
  }

  async createBidirectionalStream() {
    const controlReadable = new ReadableStream({
      start: (controller) => (this.controlController = controller),
    });
    const controlWritable = new WritableStream({
      write: (chunk) => this.controlController.enqueue(bytes(chunk)),
    });
    return { readable: controlReadable, writable: controlWritable };
  }

  async createUnidirectionalStream() {
    const parts = [];
    return new WritableStream({
      write: (chunk) => parts.push(bytes(chunk)),
      close: async () => {
        const length = parts.reduce((total, part) => total + part.byteLength, 0);
        const joined = new Uint8Array(length);
        let offset = 0;
        for (const part of parts) {
          joined.set(part, offset);
          offset += part.byteLength;
        }
        await this.beforeReliableClose?.(joined);
        this.uniController.enqueue(readable(joined));
      },
    });
  }

  async getStats() {
    return { datagrams: { expiredOutgoing: 0, lostOutgoing: this.lostOutgoing } };
  }

  close(info = {}) {
    this.resolveClosed(info);
  }
}

async function flush(turns = 30) {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

afterEach(() => vi.useRealTimers());

describe('keyframe de recuperacao no transporte hibrido', () => {
  it('mantem uma unica ancora confiavel e deixa datagramas apenas para deltas', async () => {
    const received = [];
    const session = new LoopbackSession(1_200);
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onMessage: (value, binary) => binary && received.push(new Uint8Array(value).at(-1)),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush(80);
    expect(received).toEqual([10]);
    expect(session.datagramWrites).toHaveLength(0);

    endpoint.send(JSON.stringify({ type: 'need-keyframe', slot: 0 }));
    await flush(80);
    // No enlace oficial de 600 kbit/s, duplicar este keyframe por datagrama
    // exige mais bytes do que cabem no prazo de expiração nativo. A âncora é
    // pequena o bastante para um stream confiável e precisa atravessar uma vez.
    endpoint.send(packet(1, 20, 28_000));
    await flush(120);

    expect(received).toEqual([10, 20]);
    expect(session.datagramWrites).toHaveLength(0);
    expect(endpoint.stats.recoveryKeyframesSent).toBe(1);

    // O ACK fim a fim da âncora libera a lane para os deltas seguintes, que
    // continuam descartáveis e não voltam a criar dívida confiável.
    await flush(80);
    endpoint.send(packet(2, 30, 120));
    await flush(80);
    expect(session.datagramWrites.length).toBeGreaterThan(0);
    for (const frame of session.datagramWrites) {
      session.datagramController.enqueue(frame);
    }
    await flush(80);
    expect(received).toEqual([10, 20, 30]);
    endpoint.close();
  });
});

describe('FEC adaptativo dos deltas WebTransport', () => {
  it('reaproveita o keyframe confiável quando a janela curta confirma o delta perdido', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const received = [];
    const pedidos = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onMessage: (value, binary) => binary && received.push(new Uint8Array(value).at(-1)),
      onNeedKeyframe: (slot) => pedidos.push(slot),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush(80);

    const perdido = session.datagramWrites.length;
    endpoint.send(packet(2, 20, 40));
    await flush(80);
    expect(session.datagramWrites.length).toBeGreaterThan(perdido);

    const proximo = session.datagramWrites.length;
    endpoint.send(packet(2, 30, 40));
    await flush(80);
    for (const frame of session.datagramWrites.slice(proximo)) {
      session.datagramController.enqueue(frame);
    }
    endpoint.send(packet(1, 40, 40));
    await flush(80);
    expect(received).toEqual([10]);

    await vi.advanceTimersByTimeAsync(151);
    await flush(80);
    expect(received).toEqual([10, 40]);
    expect(pedidos).toEqual([]);

    const depoisDaRecuperacao = session.datagramWrites.length;
    endpoint.send(packet(2, 50, 120));
    await flush(80);
    expect(
      session.datagramWrites.slice(depoisDaRecuperacao).filter((frame) => (frame[21] & 1) !== 0),
    ).toHaveLength(1);
    expect(endpoint.stats.datagramFecActivations).toBe(1);
    endpoint.close();
  });

  it('arma FEC também quando o gap ainda não tem keyframe pendente', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const pedidos = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onNeedKeyframe: (slot) => pedidos.push(slot),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush(80);
    endpoint.send(packet(2, 20, 40));
    await flush(80);
    expect(session.datagramWrites.at(-1)[21] & 2).toBe(2);

    const proximo = session.datagramWrites.length;
    endpoint.send(packet(2, 30, 40));
    await flush(80);
    expect(session.datagramWrites.slice(proximo).every((frame) => (frame[21] & 2) === 0)).toBe(
      true,
    );
    for (const frame of session.datagramWrites.slice(proximo)) {
      session.datagramController.enqueue(frame);
    }

    await vi.advanceTimersByTimeAsync(151);
    await flush(80);
    expect(pedidos).toEqual([0]);

    await vi.advanceTimersByTimeAsync(348);
    await flush(80);
    expect(pedidos).toEqual([0]);
    await vi.advanceTimersByTimeAsync(1);
    await flush(80);
    expect(pedidos).toEqual([0, 0]);

    const depoisDoGap = session.datagramWrites.length;
    endpoint.send(packet(2, 40, 120));
    await flush(80);
    expect(
      session.datagramWrites.slice(depoisDoGap).filter((frame) => (frame[21] & 1) !== 0),
    ).toHaveLength(1);
    expect(endpoint.stats.datagramFecActivations).toBe(1);
    endpoint.close();
  });

  it('prioriza a paridade e não pede keyframe quando o dado seguinte expira com FEC', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const received = [];
    const drops = [];
    const session = new LoopbackSession();
    let failedData = false;
    const attemptedKinds = [];
    session.datagramWrites = [];
    session.datagramWritable = new WritableStream({
      write(chunk) {
        const frame = bytes(chunk);
        const parity = (frame[21] & 1) !== 0;
        attemptedKinds.push(parity ? 'parity' : 'data');
        const expired = !failedData && !parity;
        if (expired) failedData = true;
        session.datagramWritable.lastWriteStatus = expired
          ? { code: 'expired', message: 'prazo nativo' }
          : { code: 'success' };
        if (!expired) session.datagramWrites.push(frame);
      },
    });
    session.datagrams.writable = session.datagramWritable;
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onMessage: (value, binary) => binary && received.push(new Uint8Array(value).at(-1)),
      onMediaDrop: (slot, reason) => drops.push({ slot, reason }),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush(80);
    session.lostOutgoing = 1;
    await vi.advanceTimersByTimeAsync(251);
    await flush(80);
    expect(endpoint.stats.datagramFecActivations).toBe(1);

    // A pressão nativa desprime a lane. Reancore antes de isolar a perda do
    // delta seguinte; o que o contrato mede é não criar um segundo drop quando
    // a paridade consegue reparar exatamente um fragmento.
    endpoint.send(packet(1, 15, 40));
    await flush(80);
    drops.length = 0;

    endpoint.send(packet(2, 20, 40));
    await flush(80);
    expect(attemptedKinds.slice(-2)).toEqual(['parity', 'data']);
    expect(session.datagramWrites).toHaveLength(1);
    expect(session.datagramWrites[0][21] & 1).toBe(1);
    session.datagramController.enqueue(session.datagramWrites[0]);
    await flush(80);

    expect(received).toEqual([10, 15, 20]);
    expect(drops).toEqual([]);
    expect(endpoint.stats.datagramFramesRecovered).toBe(1);
    endpoint.close();
  });

  it('publica bloqueio nativo antes do drop comum do datagrama', async () => {
    const eventos = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onDiagnostic: ({ reason }) => eventos.push(`diagnostic:${reason}`),
      onMediaDrop: (_slot, reason) => eventos.push(`drop:${reason}`),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush();
    session.datagramWritable.lastWriteStatus = {
      code: 'blocked',
      message: 'congestion window exhausted',
    };
    endpoint.send(packet(2, 20, 40));
    await flush(80);

    const native = eventos.indexOf('diagnostic:datagram-blocked');
    const drop = eventos.indexOf('drop:datagram-blocked');
    expect(native).toBe(0);
    expect(drop).toBeGreaterThan(native);
    endpoint.close();
  });

  it('usa pressão nativa para armar FEC sem inventar um gap de mídia', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const eventos = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onDiagnostic: ({ reason }) => eventos.push(`diagnostic:${reason}`),
      onMediaDrop: (_slot, reason) => eventos.push(`drop:${reason}`),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush();
    session.lostOutgoing = 1;
    await vi.advanceTimersByTimeAsync(251);

    expect(eventos).toEqual(['diagnostic:datagram-native-lost']);
    expect(endpoint.stats.datagramFecActivations).toBe(1);
    endpoint.close();
  });

  it('recupera um fragmento perdido sem retransmissão nem entrega duplicada', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const received = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onMessage: (value, binary) => binary && received.push(new Uint8Array(value).at(-1)),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush();
    session.lostOutgoing = 1;
    await vi.advanceTimersByTimeAsync(251);

    // A perda nativa arma FEC para a lane; o keyframe confiável recupera a
    // cadeia e o delta seguinte ganha paridade.
    endpoint.send(packet(1, 20, 40));
    await flush(80);
    expect(received).toEqual([10, 20]);

    const before = session.datagramWrites.length;
    endpoint.send(packet(2, 30, 260));
    await flush(80);
    const frames = session.datagramWrites.slice(before);
    const headers = frames.map((frame) => {
      const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
      return { flags: view.getUint8(21), index: view.getUint16(22) };
    });
    expect(headers.filter(({ flags }) => (flags & 1) !== 0)).toHaveLength(1);

    // Some um fragmento de dados; a paridade e os demais chegam fora de ordem.
    const droppedIndex = headers.find(({ flags }) => (flags & 1) === 0)?.index;
    for (const [index, frame] of [...frames.entries()].reverse()) {
      const header = headers[index];
      if ((header.flags & 1) === 0 && header.index === droppedIndex) continue;
      session.datagramController.enqueue(frame);
    }
    await flush(80);

    expect(received).toEqual([10, 20, 30]);
    expect(endpoint.stats.datagramFramesRecovered).toBe(1);
    endpoint.close();
  });

  it('arma por lane após perda, mantém janela mínima e desarma após amostras limpas', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, { assumePeerDatagrams: true });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush();
    let before = session.datagramWrites.length;
    endpoint.send(packet(2, 11, 120));
    await flush(50);
    expect(session.datagramWrites.slice(before).some((frame) => (frame[21] & 1) !== 0)).toBe(false);
    for (const frame of session.datagramWrites.slice(before)) {
      session.datagramController.enqueue(frame);
    }
    await flush(50);

    session.lostOutgoing = 1;
    await vi.advanceTimersByTimeAsync(251);
    endpoint.send(packet(1, 20, 40));
    before = session.datagramWrites.length;
    endpoint.send(packet(2, 21, 120));
    await flush(50);
    expect(
      session.datagramWrites.slice(before).filter((frame) => (frame[21] & 1) !== 0),
    ).toHaveLength(1);
    expect(endpoint.stats.datagramFecActivations).toBe(1);

    // Um único evento não deixa overhead permanente: depois do período mínimo
    // e de oito polls limpos, o delta volta a usar somente seus fragmentos.
    await vi.advanceTimersByTimeAsync(32_250);
    before = session.datagramWrites.length;
    endpoint.send(packet(2, 22, 120));
    await flush(50);
    expect(session.datagramWrites.slice(before).some((frame) => (frame[21] & 1) !== 0)).toBe(false);
    expect(endpoint.stats.datagramFecDeactivations).toBe(1);
    endpoint.close();
  });

  it('arma FEC pelo feedback do receptor mesmo sem stats nativos e limita o overhead', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const session = new LoopbackSession();
    session.getStats = undefined;
    const endpoint = await openClientWireSession(session, { assumePeerDatagrams: true });
    await flush();

    endpoint.send(packet(1, 10, 40));
    await flush();
    endpoint.send(JSON.stringify({ type: 'need-keyframe', slot: 0 }));
    await flush();

    const recoveryStart = session.datagramWrites.length;
    endpoint.send(packet(1, 20, 40));
    await flush(50);
    for (const frame of session.datagramWrites.slice(recoveryStart)) {
      session.datagramController.enqueue(frame);
    }
    await flush(50);

    let before = session.datagramWrites.length;
    endpoint.send(packet(2, 21, 120));
    await flush(50);
    expect(
      session.datagramWrites.slice(before).filter((frame) => (frame[21] & 1) !== 0),
    ).toHaveLength(1);
    expect(endpoint.stats.datagramFecActivations).toBe(1);

    await vi.advanceTimersByTimeAsync(30_001);
    before = session.datagramWrites.length;
    endpoint.send(packet(2, 22, 120));
    await flush(50);
    expect(session.datagramWrites.slice(before).some((frame) => (frame[21] & 1) !== 0)).toBe(false);
    expect(endpoint.stats.datagramFecDeactivations).toBe(1);
    endpoint.close();
  });

  it('rejeita payload subdeclarado sem reter assembly até o timeout', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const diagnostics = [];
    const session = new LoopbackSession();
    const endpoint = await openClientWireSession(session, {
      assumePeerDatagrams: true,
      onDiagnostic: (event) => diagnostics.push(event),
    });
    await flush();

    endpoint.send(packet(1, 10, 40));
    endpoint.send(packet(2, 11, 120));
    await flush(50);
    const malformed = bytes(session.datagramWrites[0]);
    const view = new DataView(malformed.buffer, malformed.byteOffset, malformed.byteLength);
    view.setUint32(16, 1);
    view.setUint16(22, 0);
    view.setUint16(24, 1);
    session.datagramController.enqueue(malformed);
    await flush(30);

    expect(diagnostics.some(({ reason }) => reason === 'datagram-length-invalid')).toBe(true);
    await vi.advanceTimersByTimeAsync(351);
    expect(endpoint.stats.datagramAssembliesExpired).toBe(0);
    endpoint.close();
  });
});
