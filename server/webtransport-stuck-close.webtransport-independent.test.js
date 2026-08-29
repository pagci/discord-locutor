import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { spawn } from 'node:child_process';
import { createHash, X509Certificate } from 'node:crypto';
import { createSocket } from 'node:dgram';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

import { createTransport } from '../shared/transport.js';
import { acceptServerWireSession, closeWireSession } from '../shared/transport-wire.js';
import { signToken, verifyToken } from './tokens.js';
import { startWebTransport } from './webtransport.js';

const live = process.env.WEBTRANSPORT_LIVE === '1';
const liveSuite = live ? describe : describe.skip;
const HANDSHAKE_TIMEOUT_MS = 1500;
const CLEANUP_LIMIT_MS = 100;
const ALLOWED_ORIGIN = 'https://activity.example';
const LIVE_ROOM = 'stuck-close-independent-room';
const nativeSetTimeout = globalThis.setTimeout.bind(globalThis);
const nativeClearTimeout = globalThis.clearTimeout.bind(globalThis);

function prazo(promise, milliseconds, label) {
  let timer;
  return new Promise((resolve, reject) => {
    timer = nativeSetTimeout(() => reject(new Error(`${label} timeout`)), milliseconds);
    Promise.resolve(promise).then(
      (value) => {
        nativeClearTimeout(timer);
        resolve(value);
      },
      (error) => {
        nativeClearTimeout(timer);
        reject(error);
      },
    );
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

async function flush(times = 80) {
  for (let index = 0; index < times; index++) await Promise.resolve();
}

async function eventually(predicate, milliseconds, label) {
  const deadline = Date.now() + milliseconds;
  let lastError;
  while (Date.now() <= deadline) {
    try {
      const value = await predicate();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => nativeSetTimeout(resolve, 10));
  }
  throw lastError ?? new Error(`${label} timeout`);
}

function publicApi(proto) {
  const names = new Set();
  for (
    let current = proto;
    current && current !== Object.prototype;
    current = Object.getPrototypeOf(current)
  ) {
    for (const name of Object.getOwnPropertyNames(current)) names.add(name);
  }
  return [...names].sort();
}

async function udpPortIsFree(port) {
  const socket = createSocket('udp4');
  return new Promise((resolve) => {
    let finished = false;
    const finish = (free) => {
      if (finished) return;
      finished = true;
      try {
        socket.close(() => resolve(free));
      } catch {
        resolve(free);
      }
    };
    socket.once('error', () => finish(false));
    socket.bind(port, '127.0.0.1', () => finish(true));
  });
}

// This ledger belongs only to the deterministic adapter seam. It must never be
// presented as state.sessions/state.connections or as room lifecycle evidence.
function adapterOwnerLedger() {
  return {
    ownerIncrements: 0,
    ownerDecrements: 0,
    balance() {
      return this.ownerIncrements - this.ownerDecrements;
    },
  };
}

function stuckAdapterSession(ledger, label) {
  const read = deferred();
  const closed = deferred();
  const closing = deferred();
  let readSettled = false;
  const timeline = ['active'];
  const counters = {
    label,
    reads: 0,
    readTerminals: 0,
    cancels: 0,
    releases: 0,
    closes: 0,
    lateReadRejects: 0,
    bindSettled: false,
    winner: null,
  };
  const reader = {
    closed: new Promise(() => {}),
    read: vi.fn(() => {
      counters.reads++;
      ledger.ownerIncrements++;
      return read.promise;
    }),
    cancel: vi.fn((reason) => {
      counters.cancels++;
      if (!readSettled) {
        readSettled = true;
        counters.readTerminals++;
        ledger.ownerDecrements++;
        read.resolve({ value: undefined, done: true, reason });
      }
      return Promise.resolve();
    }),
    releaseLock: vi.fn(() => counters.releases++),
  };
  const session = {
    closed: closed.promise,
    ready: Promise.resolve(),
    incomingBidirectionalStreams: { getReader: () => reader },
    incomingUnidirectionalStreams: new ReadableStream({
      start: (controller) => controller.close(),
    }),
    close: vi.fn((info) => {
      counters.closes++;
      counters.closeInfo = info;
      if (!timeline.includes('closing')) {
        timeline.push('closing');
        closing.resolve();
      }
    }),
  };

  const start = (handshakeTimeoutMs = HANDSHAKE_TIMEOUT_MS) => {
    const opening = acceptServerWireSession(session, {}, { handshakeTimeoutMs });
    const observed = opening.then(
      (endpoint) => {
        counters.winner = 'connected';
        return { endpoint, error: null };
      },
      (error) => {
        counters.winner = error?.reason ?? error?.message;
        return { endpoint: null, error };
      },
    );
    const settled = observed.then((result) => {
      counters.bindSettled = true;
      timeline.push('settled');
      return result;
    });
    return { opening, settled };
  };

  return {
    session,
    reader,
    counters,
    timeline,
    closing: closing.promise,
    start,
    settleClosed: (value = { closeCode: 1, reason: 'listener-lost' }) => closed.resolve(value),
    rejectClosed: (error = new Error('late-closed-reject')) => closed.reject(error),
    rejectReadLate: (error = new Error('late-read-reject')) => {
      counters.lateReadRejects++;
      read.reject(error);
    },
  };
}

function noHandshakeAdapterSession() {
  const outerRead = deferred();
  const controlRead = deferred();
  const counters = {
    outerCancels: 0,
    outerReleases: 0,
    controlCancels: 0,
    controlReleases: 0,
    writerCloses: 0,
    writerAborts: 0,
    sessionCloses: 0,
  };
  const controlReader = {
    closed: new Promise(() => {}),
    read: () => controlRead.promise,
    cancel: (reason) => {
      counters.controlCancels++;
      controlRead.resolve({ done: true, reason });
      return Promise.resolve();
    },
    releaseLock: () => counters.controlReleases++,
  };
  const controlWriter = {
    closed: Promise.resolve(),
    ready: Promise.resolve(),
    write: () => Promise.resolve(),
    close: () => {
      counters.writerCloses++;
      return Promise.resolve();
    },
    abort: () => {
      counters.writerAborts++;
      return Promise.resolve();
    },
  };
  const stream = {
    readable: { getReader: () => controlReader },
    writable: { getWriter: () => controlWriter },
  };
  outerRead.resolve({ value: stream, done: false });
  const session = {
    closed: new Promise(() => {}),
    incomingBidirectionalStreams: {
      getReader: () => ({
        closed: new Promise(() => {}),
        read: () => outerRead.promise,
        cancel: () => {
          counters.outerCancels++;
          return Promise.resolve();
        },
        releaseLock: () => counters.outerReleases++,
      }),
    },
    incomingUnidirectionalStreams: new ReadableStream({
      start: (controller) => controller.close(),
    }),
    close: vi.fn(() => counters.sessionCloses++),
  };
  return { session, counters };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('I3.2 adapter deterministico com session.closed preso', () => {
  it('cumpre 1500 ms + cleanup <=100 ms e assenta o read sem bidi uma vez', async () => {
    vi.useFakeTimers();
    const ledger = adapterOwnerLedger();
    const fixture = stuckAdapterSession(ledger, 'no-bidi-default-deadline');
    const { settled } = fixture.start();

    await vi.advanceTimersByTimeAsync(HANDSHAKE_TIMEOUT_MS - 1);
    expect.soft(fixture.counters.bindSettled).toBe(false);
    expect.soft(fixture.timeline).toEqual(['active']);

    await vi.advanceTimersByTimeAsync(1);
    await fixture.closing;
    expect.soft(fixture.timeline).toEqual(['active', 'closing']);
    expect.soft(fixture.counters.bindSettled).toBe(false);

    await vi.advanceTimersByTimeAsync(CLEANUP_LIMIT_MS);
    const result = await settled;
    expect
      .soft(result.error)
      .toMatchObject({ message: 'handshake-timeout', reason: 'handshake-timeout' });
    expect.soft(fixture.timeline).toEqual(['active', 'closing', 'settled']);
    expect.soft(fixture.counters).toMatchObject({
      reads: 1,
      readTerminals: 1,
      cancels: 1,
      releases: 1,
      closes: 1,
      winner: 'handshake-timeout',
    });
    expect.soft(fixture.counters.closeInfo).toEqual({ closeCode: 1, reason: 'handshake-timeout' });
    expect.soft(ledger).toMatchObject({ ownerIncrements: 1, ownerDecrements: 1 });
    expect.soft(ledger.balance()).toBe(0);
    expect.soft(vi.getTimerCount()).toBe(0);
  });

  it('com bidi sem HANDSHAKE usa uma terminal de writer e assenta readers sem closed', async () => {
    vi.useFakeTimers();
    const fixture = noHandshakeAdapterSession();
    const opening = acceptServerWireSession(fixture.session, {}, { handshakeTimeoutMs: 20 });
    const observed = opening.then(
      () => null,
      (error) => error,
    );
    await flush();
    await vi.advanceTimersByTimeAsync(20);
    const error = await observed;
    await flush();

    expect.soft(error).toMatchObject({ message: 'handshake-timeout', reason: 'handshake-timeout' });
    expect.soft(fixture.counters).toEqual({
      outerCancels: 0,
      outerReleases: 1,
      controlCancels: 1,
      controlReleases: 1,
      writerCloses: 1,
      writerAborts: 0,
      sessionCloses: 1,
    });
    expect.soft(fixture.counters.writerCloses + fixture.counters.writerAborts).toBe(1);
    expect.soft(vi.getTimerCount()).toBe(0);
  });

  it('mantem closed, reject e close tardios inertes depois de settled', async () => {
    vi.useFakeTimers();
    const ledger = adapterOwnerLedger();
    const fixture = stuckAdapterSession(ledger, 'late-events');
    const { settled } = fixture.start(20);
    await vi.advanceTimersByTimeAsync(20);
    await fixture.closing;
    await vi.advanceTimersByTimeAsync(CLEANUP_LIMIT_MS);
    const result = await settled;
    const snapshot = {
      cancels: fixture.counters.cancels,
      releases: fixture.counters.releases,
      closes: fixture.counters.closes,
      ownerDecrements: ledger.ownerDecrements,
    };

    fixture.settleClosed();
    fixture.rejectReadLate();
    expect
      .soft(closeWireSession(fixture.session, { closeCode: 1, reason: 'late-close' }))
      .toBe(false);
    await flush();

    expect.soft(result.error?.reason).toBe('handshake-timeout');
    expect.soft(fixture.timeline).toEqual(['active', 'closing', 'settled']);
    expect.soft(snapshot).toEqual({ cancels: 1, releases: 1, closes: 1, ownerDecrements: 1 });
    expect
      .soft({
        cancels: fixture.counters.cancels,
        releases: fixture.counters.releases,
        closes: fixture.counters.closes,
        ownerDecrements: ledger.ownerDecrements,
      })
      .toEqual(snapshot);
    expect.soft(ledger.balance()).toBe(0);
    expect.soft(vi.getTimerCount()).toBe(0);
  });

  it('estressa 32 owners do adapter sem representar CONNECTs nem state do servidor', async () => {
    vi.useFakeTimers();
    const ledger = adapterOwnerLedger();
    const attackers = Array.from({ length: 32 }, (_, index) =>
      stuckAdapterSession(ledger, `adapter-attacker-${index}`),
    );
    const attempts = attackers.map((fixture) => fixture.start(25).settled);
    await vi.advanceTimersByTimeAsync(25 + CLEANUP_LIMIT_MS);
    const results = await Promise.all(attempts);

    expect.soft(results.every(({ error }) => error?.reason === 'handshake-timeout')).toBe(true);
    expect.soft(attackers.every(({ counters }) => counters.closes === 1)).toBe(true);
    expect.soft(attackers.every(({ counters }) => counters.cancels === 1)).toBe(true);
    expect.soft(attackers.every(({ counters }) => counters.releases === 1)).toBe(true);
    expect.soft(ledger).toMatchObject({ ownerIncrements: 32, ownerDecrements: 32 });
    expect.soft(ledger.balance()).toBe(0);
    expect.soft(vi.getTimerCount()).toBe(0);
  });

  it('canario filho sai 0 e imprime somente CANARY_OK sem handlers recuperadores', async () => {
    const wireUrl = pathToFileURL(join(process.cwd(), 'shared', 'transport-wire.js')).href;
    const script = `
      import assert from 'node:assert/strict';
      import { acceptServerWireSession } from ${JSON.stringify(wireUrl)};
      let resolveRead;
      const read = new Promise((resolve) => { resolveRead = resolve; });
      const counters = { close: 0, cancel: 0, release: 0, pending: 1 };
      const session = {
        closed: new Promise(() => {}),
        incomingBidirectionalStreams: { getReader: () => ({
          closed: new Promise(() => {}),
          read: () => read,
          cancel: () => { counters.cancel++; counters.pending--; resolveRead({ done: true }); },
          releaseLock: () => counters.release++,
        }) },
        incomingUnidirectionalStreams: new ReadableStream({ start: (c) => c.close() }),
        close: () => counters.close++,
      };
      const error = await acceptServerWireSession(session, {}, { handshakeTimeoutMs: 25 }).then(
        () => null,
        (reason) => reason,
      );
      assert.equal(error?.reason, 'handshake-timeout');
      assert.deepEqual(counters, { close: 1, cancel: 1, release: 1, pending: 0 });
      await new Promise((resolve) => setTimeout(resolve, 100));
      assert.deepEqual(counters, { close: 1, cancel: 1, release: 1, pending: 0 });
      process.stdout.write('CANARY_OK\\n');
    `;
    const child = spawn(process.execPath, ['--input-type=module', '--eval', script], {
      cwd: process.cwd(),
      env: { ...process.env, WEBTRANSPORT_LIVE: '0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => (stdout += chunk));
    child.stderr.on('data', (chunk) => (stderr += chunk));
    let killTimer;
    const outcome = await Promise.race([
      new Promise((resolve, reject) => {
        child.once('error', reject);
        child.once('exit', (code, signal) => resolve({ code, signal, timeout: false }));
      }),
      new Promise((resolve) => {
        killTimer = nativeSetTimeout(() => {
          child.kill();
          resolve({ code: null, signal: null, timeout: true });
        }, 5000);
      }),
    ]).finally(() => nativeClearTimeout(killTimer));

    expect.soft(outcome).toEqual({ code: 0, signal: null, timeout: false });
    expect.soft(stderr).toBe('');
    expect.soft(stdout).toMatch(/^CANARY_OK\r?\n$/);
    expect.soft(`${stdout}${stderr}`).not.toContain('ERR_INVALID_STATE');
  });
});

let addon;
let selfsigned;
let livePair;
let liveTemp;
let originalSetRequestCallback;
let previousSessionSecret;
const originPending = [];
const originRequests = [];
const liveObservers = new Set();
const nativeClients = new Set();
const settledNativeClients = new WeakSet();
const closedNativeClients = new WeakSet();

function requestPath(url) {
  const parsed = new URL(String(url), 'https://listener.invalid');
  return `${parsed.pathname}${parsed.search}`;
}

function registerOrigin(url, scenario) {
  const entry = { path: requestPath(url), scenario, origin: ALLOWED_ORIGIN };
  originPending.push(entry);
  return entry;
}

function removeOrigin(entry) {
  const index = originPending.indexOf(entry);
  if (index >= 0) originPending.splice(index, 1);
}

function installOriginSeam() {
  originalSetRequestCallback = addon.Http3Server.prototype.setRequestCallback;
  addon.Http3Server.prototype.setRequestCallback = function (callback) {
    if (typeof callback !== 'function') return originalSetRequestCallback.call(this, callback);
    return originalSetRequestCallback.call(this, async (args) => {
      const rawHeader = args?.header ?? {};
      const path = String(rawHeader[':path'] ?? rawHeader.path ?? '');
      const index = originPending.findIndex((entry) => entry.path === path);
      const entry = index < 0 ? null : originPending.splice(index, 1)[0];
      const request = {
        scenario: entry?.scenario ?? 'unplanned',
        path,
        rawOrigin: rawHeader.origin ?? rawHeader.Origin,
        injectedOrigin: entry?.origin,
      };
      originRequests.push(request);
      const header = { ...rawHeader };
      if (entry) header.origin = entry.origin;
      const result = await callback({ ...args, header });
      request.status = result?.status;
      request.reason = result?.header?.['x-discord-locutor-reason'];
      return result;
    });
  };
}

function instrumentNativeSession(session, observer) {
  const record = {
    session,
    closes: 0,
    closeInfo: [],
    readerGets: 0,
    reads: 0,
    readTerminals: 0,
    pendingReads: 0,
    cancels: 0,
    releases: 0,
    closedSettlements: 0,
    instrumentationErrors: [],
  };
  observer.sessionRecords.push(record);

  Promise.resolve(session.closed).then(
    () => record.closedSettlements++,
    () => record.closedSettlements++,
  );

  try {
    const nativeClose = session.close.bind(session);
    session.close = (...args) => {
      record.closes++;
      record.closeInfo.push(args[0]);
      const result = nativeClose(...args);
      observer.onSessionClose?.({ observer, record, args });
      return result;
    };
  } catch (error) {
    record.instrumentationErrors.push(`session.close: ${error?.message ?? error}`);
  }

  try {
    const incoming = session.incomingBidirectionalStreams;
    const nativeGetReader = incoming.getReader.bind(incoming);
    incoming.getReader = (...args) => {
      const reader = nativeGetReader(...args);
      record.readerGets++;
      try {
        const nativeRead = reader.read.bind(reader);
        reader.read = (...readArgs) => {
          record.reads++;
          record.pendingReads++;
          const result = nativeRead(...readArgs);
          Promise.resolve(result).then(
            () => {
              record.pendingReads--;
              record.readTerminals++;
            },
            () => {
              record.pendingReads--;
              record.readTerminals++;
            },
          );
          return result;
        };
        const nativeCancel = reader.cancel?.bind(reader);
        if (nativeCancel) {
          reader.cancel = (...cancelArgs) => {
            record.cancels++;
            return nativeCancel(...cancelArgs);
          };
        }
        const nativeRelease = reader.releaseLock?.bind(reader);
        if (nativeRelease) {
          reader.releaseLock = (...releaseArgs) => {
            record.releases++;
            return nativeRelease(...releaseArgs);
          };
        }
      } catch (error) {
        record.instrumentationErrors.push(`reader: ${error?.message ?? error}`);
      }
      return reader;
    };
  } catch (error) {
    record.instrumentationErrors.push(`incomingBidirectionalStreams: ${error?.message ?? error}`);
  }
  return record;
}

function instrumentServerState(state, observer) {
  const nativeSessionAdd = state.sessions.add.bind(state.sessions);
  const nativeSessionDelete = state.sessions.delete.bind(state.sessions);
  state.sessions.add = (session) => {
    observer.counts.sessionsAdd++;
    const record = instrumentNativeSession(session, observer);
    observer.recordsBySession.set(session, record);
    const result = nativeSessionAdd(session);
    observer.counts.maxSessions = Math.max(observer.counts.maxSessions, state.sessions.size);
    return result;
  };
  state.sessions.delete = (session) => {
    observer.counts.sessionsDelete++;
    return nativeSessionDelete(session);
  };

  const nativeConnectionAdd = state.connections.add.bind(state.connections);
  const nativeConnectionDelete = state.connections.delete.bind(state.connections);
  state.connections.add = (socket) => {
    observer.counts.connectionsAdd++;
    const result = nativeConnectionAdd(socket);
    observer.counts.maxConnections = Math.max(
      observer.counts.maxConnections,
      state.connections.size,
    );
    return result;
  };
  state.connections.delete = (socket) => {
    observer.counts.connectionsDelete++;
    return nativeConnectionDelete(socket);
  };

  const nativeStopServer = state.listener.stopServer.bind(state.listener);
  state.listener.stopServer = (...args) => {
    observer.counts.listenerStopServer++;
    return nativeStopServer(...args);
  };
}

async function startObservedListener(label) {
  const observer = {
    label,
    state: null,
    port: null,
    errors: [],
    lifecycles: [],
    sessionRecords: [],
    recordsBySession: new Map(),
    serverMessages: [],
    sockets: [],
    onSessionClose: null,
    onConnection: null,
    counts: {
      sessionsAdd: 0,
      sessionsDelete: 0,
      connectionsAdd: 0,
      connectionsDelete: 0,
      onConnection: 0,
      roomAttach: 0,
      roomDetach: 0,
      listenerStopServer: 0,
      maxSessions: 0,
      maxConnections: 0,
    },
  };
  const state = await startWebTransport({
    env: {
      WEBTRANSPORT_ENABLED: 'true',
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: livePair.certPath,
      WEBTRANSPORT_KEY_PATH: livePair.keyPath,
      WEBTRANSPORT_HOST: '127.0.0.1',
      WEBTRANSPORT_PORT: '0',
      WEBTRANSPORT_PUBLIC_URL: 'https://127.0.0.1:0/wt',
    },
    production: false,
    allowedOrigins: new Set([ALLOWED_ORIGIN]),
    verifyToken,
    sharded: false,
    node: 0,
    nodeForToken: () => 0,
    roomExists: (room) => room === LIVE_ROOM,
    sources: new Set(['tela', 'camera']),
    onState: (event) => observer.lifecycles.push(event),
    onError: (error) => observer.errors.push(error),
    onConnection: (socket, auth, source, control) => {
      observer.counts.onConnection++;
      observer.counts.roomAttach++;
      observer.sockets.push(socket);
      socket.once('close', () => observer.counts.roomDetach++);
      observer.onConnection?.({ socket, auth, source, control });
    },
  });
  observer.state = state;
  if (!state.listener) throw new Error(`${label}: listener was not created`);
  instrumentServerState(state, observer);
  await eventually(() => state.capability(), 10_000, `${label}: listener ready`);
  observer.port = state.listener.address()?.port;
  if (!observer.port) throw new Error(`${label}: listener has no UDP port`);
  liveObservers.add(observer);
  return observer;
}

function liveToken(uid) {
  return signToken({
    room: LIVE_ROOM,
    role: 'viewer',
    uid,
    name: uid,
    instance: `stuck-close-${uid}`,
  });
}

function directAuthenticatedClient(observer, uid, scenario = uid) {
  const url = new URL(observer.state.capability().url);
  url.searchParams.set('t', liveToken(uid));
  const originEntry = registerOrigin(url, scenario);
  let client;
  try {
    client = new addon.WebTransport(url, {
      allowPooling: false,
      serverCertificateHashes: [{ algorithm: 'sha-256', value: livePair.hash }],
    });
  } catch (error) {
    removeOrigin(originEntry);
    throw error;
  }
  nativeClients.add(client);
  const closed = Promise.resolve(client.closed).then(
    () => 'closed',
    () => 'rejected',
  );
  void closed.then(() => {
    settledNativeClients.add(client);
    nativeClients.delete(client);
  });
  return { client, closed, originEntry, url: url.toString() };
}

function closeNativeClientOnce(client, info) {
  if (!client || settledNativeClients.has(client) || closedNativeClients.has(client)) return false;
  closedNativeClients.add(client);
  try {
    client.close(info);
  } catch {
    // The native peer may have closed between the settled check and this call.
  }
  return true;
}

function factoryWebTransport(scenario) {
  return class ObservedWebTransport extends addon.WebTransport {
    constructor(url, options) {
      const originEntry = registerOrigin(url, scenario);
      try {
        super(url, options);
      } catch (error) {
        removeOrigin(originEntry);
        throw error;
      }
    }
  };
}

function stateInventory(observer) {
  return {
    sessions: observer.state.sessions.size,
    connections: observer.state.connections.size,
    originPending: originPending.length,
    listenerPort: observer.state.listener.address()?.port,
  };
}

async function waitForInventory(observer, baseline, label) {
  await eventually(
    () => {
      const current = stateInventory(observer);
      return (
        current.sessions === baseline.sessions &&
        current.connections === baseline.connections &&
        current.originPending === baseline.originPending &&
        current.listenerPort === baseline.listenerPort
      );
    },
    10_000,
    label,
  );
  return stateInventory(observer);
}

async function cleanupListener(observer) {
  if (!observer) return;
  try {
    if (!observer.state.stopped) observer.state.stop();
    await prazo(
      Promise.resolve(observer.state.listener.closed).catch(() => {}),
      10_000,
      `${observer.label}: listener closed`,
    );
    await eventually(
      () => observer.state.sessions.size === 0 && observer.state.connections.size === 0,
      5000,
      `${observer.label}: state drained`,
    );
  } finally {
    liveObservers.delete(observer);
  }
}

liveSuite('I3 live real: listener, state, CONNECT, factory, QUIC, UDP e TLS', () => {
  beforeAll(async () => {
    expect(process.env.WT_LIVE_NODE_MODULES).toBeUndefined();
    previousSessionSecret = process.env.SESSION_SECRET;
    process.env.SESSION_SECRET = 'stuck-close-independent-secret';
    const [wt, certModule] = await Promise.all([
      import('@fails-components/webtransport'),
      import('selfsigned'),
    ]);
    addon = wt;
    selfsigned = certModule.default ?? certModule;
    await addon.quicheLoaded;

    const notBeforeDate = new Date(Date.now() - 60_000);
    const notAfterDate = new Date(notBeforeDate);
    notAfterDate.setDate(notAfterDate.getDate() + 12);
    const pair = await selfsigned.generate([{ name: 'commonName', value: 'localhost' }], {
      keyType: 'ec',
      curve: 'P-256',
      algorithm: 'sha256',
      notBeforeDate,
      notAfterDate,
      extensions: [
        { name: 'basicConstraints', cA: false, critical: true },
        { name: 'keyUsage', digitalSignature: true, critical: true },
        { name: 'extKeyUsage', serverAuth: true },
        {
          name: 'subjectAltName',
          altNames: [
            { type: 2, value: 'localhost' },
            { type: 7, ip: '127.0.0.1' },
          ],
        },
      ],
    });
    liveTemp = await mkdtemp(join(tmpdir(), 'discord-locutor-stuck-live-'));
    const certPath = join(liveTemp, 'cert.pem');
    const keyPath = join(liveTemp, 'key.pem');
    await Promise.all([writeFile(certPath, pair.cert), writeFile(keyPath, pair.private)]);
    livePair = {
      ...pair,
      certPath,
      keyPath,
      hash: createHash('sha256').update(new X509Certificate(pair.cert).raw).digest(),
    };
    installOriginSeam();
  }, 30_000);

  afterAll(async () => {
    for (const client of nativeClients) {
      closeNativeClientOnce(client, { closeCode: 0, reason: 'test-cleanup' });
    }
    await Promise.all([...liveObservers].map((observer) => cleanupListener(observer)));
    if (originalSetRequestCallback)
      addon.Http3Server.prototype.setRequestCallback = originalSetRequestCallback;
    if (previousSessionSecret === undefined) delete process.env.SESSION_SECRET;
    else process.env.SESSION_SECRET = previousSessionSecret;
    if (liveTemp) await rm(liveTemp, { recursive: true, force: true });
  });

  it('fixa addon 1.6.7 e libera tarde o efeito real de onClose depois do cancel do adapter', async () => {
    const root = process.cwd();
    const mainPackage = JSON.parse(
      await readFile(
        join(root, 'node_modules', '@fails-components', 'webtransport', 'package.json'),
        'utf8',
      ),
    );
    const quichePackage = JSON.parse(
      await readFile(
        join(
          root,
          'node_modules',
          '@fails-components',
          'webtransport-transport-http3-quiche',
          'package.json',
        ),
        'utf8',
      ),
    );
    const serverApi = publicApi(addon.Http3Server.prototype);

    expect.soft(mainPackage.version).toBe('1.6.7');
    expect.soft(quichePackage.version).toBe('1.6.7');
    expect
      .soft(serverApi)
      .toEqual(
        expect.arrayContaining([
          'startServer',
          'stopServer',
          'sessionStream',
          'setRequestCallback',
        ]),
      );
    expect.soft(Object.keys(addon)).not.toContain('HttpWTSession');
    expect.soft(addon.setOnCloseEffectHook).toBeTypeOf('function');

    const observer = await startObservedListener('native-onclose-seam');
    const baseline = stateInventory(observer);
    const requestsBefore = originRequests.length;
    const retained = deferred();
    let retainedEffect;
    let retainedSession;
    let retainedSnapshot;
    let serverHookCalls = 0;
    let effectReleases = 0;
    const restoreHook = addon.setOnCloseEffectHook(({ session, effect }) => {
      if (!observer.state.sessions.has(session)) {
        effect();
        return;
      }
      serverHookCalls++;
      const record = observer.recordsBySession.get(session);
      retainedSession = session;
      retainedEffect = effect;
      retainedSnapshot = {
        pendingReads: record?.pendingReads,
        readTerminals: record?.readTerminals,
        cancels: record?.cancels,
        releases: record?.releases,
      };
      retained.resolve();
    });
    const attack = directAuthenticatedClient(
      observer,
      'native-onclose-seam',
      'native-onclose-seam',
    );
    try {
      await prazo(attack.client.ready, 10_000, 'native seam CONNECT ready');
      await eventually(
        () => {
          const record = observer.sessionRecords[0];
          return record?.reads === 1 && record.pendingReads === 1;
        },
        5000,
        'adapter aggregate read pending',
      );

      expect
        .soft(
          closeNativeClientOnce(attack.client, {
            closeCode: 0,
            reason: 'trigger-native-onclose',
          }),
        )
        .toBe(true);
      await prazo(retained.promise, 5000, 'native onClose effect retained');
      const record = observer.sessionRecords[0];

      expect.soft(retainedSession).toBe(record.session);
      expect.soft(retainedEffect).toBeTypeOf('function');
      expect.soft(serverHookCalls).toBe(1);
      expect.soft(retainedSnapshot).toEqual({
        pendingReads: 1,
        readTerminals: 0,
        cancels: 0,
        releases: 0,
      });

      await eventually(
        () =>
          observer.errors.some((error) => error?.reason === 'handshake-timeout') &&
          record.cancels === 1 &&
          record.releases === 1 &&
          record.pendingReads === 0,
        5000,
        'adapter cleanup before late native effect',
      );
      const beforeRelease = {
        closes: record.closes,
        closeInfo: record.closeInfo.map((info) => ({ ...info })),
        reads: record.reads,
        readTerminals: record.readTerminals,
        pendingReads: record.pendingReads,
        cancels: record.cancels,
        releases: record.releases,
        closedSettlements: record.closedSettlements,
        sessionsAdd: observer.counts.sessionsAdd,
        sessionsDelete: observer.counts.sessionsDelete,
        connectionsAdd: observer.counts.connectionsAdd,
        connectionsDelete: observer.counts.connectionsDelete,
        onConnection: observer.counts.onConnection,
        roomAttach: observer.counts.roomAttach,
        roomDetach: observer.counts.roomDetach,
        errors: observer.errors.length,
      };

      expect.soft(beforeRelease).toEqual({
        closes: 1,
        closeInfo: [{ closeCode: 1, reason: 'handshake-timeout' }],
        reads: 1,
        readTerminals: 1,
        pendingReads: 0,
        cancels: 1,
        releases: 1,
        closedSettlements: 1,
        sessionsAdd: 1,
        sessionsDelete: 1,
        connectionsAdd: 0,
        connectionsDelete: 0,
        onConnection: 0,
        roomAttach: 0,
        roomDetach: 0,
        errors: 1,
      });
      expect.soft(observer.errors[0]).toMatchObject({ reason: 'handshake-timeout' });
      expect.soft(stateInventory(observer)).toEqual(baseline);

      expect(() => retainedEffect()).not.toThrow();
      effectReleases++;
      expect(() => retainedEffect()).not.toThrow();
      effectReleases++;
      await new Promise((resolve) => nativeSetTimeout(resolve, 100));

      expect.soft(effectReleases).toBe(2);
      expect.soft(serverHookCalls).toBe(1);
      expect
        .soft({
          closes: record.closes,
          closeInfo: record.closeInfo.map((info) => ({ ...info })),
          reads: record.reads,
          readTerminals: record.readTerminals,
          pendingReads: record.pendingReads,
          cancels: record.cancels,
          releases: record.releases,
          closedSettlements: record.closedSettlements,
          sessionsAdd: observer.counts.sessionsAdd,
          sessionsDelete: observer.counts.sessionsDelete,
          connectionsAdd: observer.counts.connectionsAdd,
          connectionsDelete: observer.counts.connectionsDelete,
          onConnection: observer.counts.onConnection,
          roomAttach: observer.counts.roomAttach,
          roomDetach: observer.counts.roomDetach,
          errors: observer.errors.length,
        })
        .toEqual(beforeRelease);
      expect.soft(stateInventory(observer)).toEqual(baseline);
      const observedRequests = originRequests.slice(requestsBefore);
      expect.soft(observedRequests).toHaveLength(1);
      expect.soft(observedRequests[0]).toMatchObject({
        scenario: 'native-onclose-seam',
        status: 200,
        rawOrigin: undefined,
        injectedOrigin: ALLOWED_ORIGIN,
      });
    } finally {
      if (retainedEffect && effectReleases === 0) {
        try {
          retainedEffect();
        } catch {
          // Preserve the original test failure while restoring global state.
        }
      }
      restoreHook();
      removeOrigin(attack.originEntry);
      closeNativeClientOnce(attack.client, { closeCode: 0, reason: 'native-seam-finally' });
      await cleanupListener(observer);
    }
    expect.soft(await udpPortIsFree(observer.port)).toBe(true);
  }, 30_000);

  it('abre 32 CONNECTs autenticados reais, volta ao baseline e seleciona WT na factory no mesmo listener', async () => {
    const observer = await startObservedListener('attack-and-factory');
    const baseline = stateInventory(observer);
    const requestsBefore = originRequests.length;
    const attackers = Array.from({ length: 32 }, (_, index) =>
      directAuthenticatedClient(observer, `attacker-${index}`, `attacker-${index}`),
    );
    let logical;
    let wsCreated = 0;
    try {
      await prazo(
        Promise.all(attackers.map(({ client }) => client.ready)),
        10_000,
        '32 authenticated CONNECTs ready',
      );
      await eventually(
        () => observer.counts.sessionsAdd === 32,
        5000,
        '32 real sessions registered',
      );
      await prazo(
        Promise.all(
          attackers.map(({ client }) =>
            Promise.resolve(client.closed).then(
              () => 'closed',
              () => 'rejected',
            ),
          ),
        ),
        10_000,
        '32 no-bidi sessions closed',
      );
      const afterAttack = await waitForInventory(
        observer,
        baseline,
        'listener baseline after 32 CONNECTs',
      );
      const attackRecords = observer.sessionRecords.slice(0, 32);
      const attackTotals = attackRecords.reduce(
        (totals, record) => ({
          closes: totals.closes + record.closes,
          readerGets: totals.readerGets + record.readerGets,
          reads: totals.reads + record.reads,
          readTerminals: totals.readTerminals + record.readTerminals,
          cancels: totals.cancels + record.cancels,
          releases: totals.releases + record.releases,
          pendingReads: totals.pendingReads + record.pendingReads,
        }),
        {
          closes: 0,
          readerGets: 0,
          reads: 0,
          readTerminals: 0,
          cancels: 0,
          releases: 0,
          pendingReads: 0,
        },
      );

      expect.soft(afterAttack).toEqual(baseline);
      expect.soft(observer.counts.maxSessions).toBeGreaterThanOrEqual(32);
      expect.soft(observer.counts).toMatchObject({
        sessionsAdd: 32,
        sessionsDelete: 32,
        connectionsAdd: 0,
        connectionsDelete: 0,
        onConnection: 0,
        roomAttach: 0,
        roomDetach: 0,
      });
      expect.soft(attackRecords).toHaveLength(32);
      expect.soft(attackTotals).toEqual({
        closes: 32,
        readerGets: 32,
        reads: 32,
        readTerminals: 32,
        cancels: 32,
        releases: 32,
        pendingReads: 0,
      });
      expect
        .soft(attackRecords.every((record) => record.instrumentationErrors.length === 0))
        .toBe(true);
      expect
        .soft(observer.errors.filter((error) => error?.reason === 'handshake-timeout'))
        .toHaveLength(32);

      const serverMessages = [];
      const clientMessages = [];
      observer.onConnection = ({ socket }) => {
        socket.on('message', (data) => {
          serverMessages.push(String(data));
          socket.send(JSON.stringify({ type: 'factory-ack' }));
        });
      };
      class ForbiddenWebSocket {
        constructor() {
          wsCreated++;
          throw new Error('websocket fallback forbidden');
        }
      }
      const factoryToken = liveToken('factory-positive');
      const wsUrl = `ws://127.0.0.1/unused/ws?t=${encodeURIComponent(factoryToken)}`;
      logical = createTransport({
        wsUrl,
        capabilityUrl: 'https://capability.invalid/api/transports',
        timeoutMs: 3000,
        WebTransport: factoryWebTransport('factory-positive'),
        WebSocket: ForbiddenWebSocket,
        fetch: async () =>
          new Response(
            JSON.stringify({
              websocket: true,
              node: 0,
              shards: 1,
              webtransport: observer.state.capability(),
            }),
            { status: 200, headers: { 'Content-Type': 'application/json' } },
          ),
      });
      logical.addEventListener('error', () => {});
      logical.addEventListener('message', (event) => clientMessages.push(String(event.data)));
      if (logical.readyState !== logical.OPEN) {
        await prazo(
          new Promise((resolve, reject) => {
            logical.addEventListener('open', resolve, { once: true });
            logical.addEventListener('error', reject, { once: true });
          }),
          10_000,
          'factory WebTransport open',
        );
      }
      expect.soft(logical.transport).toBe('webtransport');
      expect.soft(wsCreated).toBe(0);
      logical.send(JSON.stringify({ type: 'factory-probe' }));
      await eventually(
        () => serverMessages.length === 1 && clientMessages.length === 1,
        5000,
        'factory bidirectional traffic',
      );
      expect.soft(serverMessages).toEqual([JSON.stringify({ type: 'factory-probe' })]);
      expect.soft(clientMessages).toEqual([JSON.stringify({ type: 'factory-ack' })]);
      expect.soft(observer.state.listener.address()?.port).toBe(baseline.listenerPort);

      logical.close(1000, 'factory-complete');
      await waitForInventory(observer, baseline, 'same listener baseline after factory client');
      expect.soft(observer.counts).toMatchObject({
        sessionsAdd: 33,
        sessionsDelete: 33,
        connectionsAdd: 1,
        connectionsDelete: 1,
        onConnection: 1,
        roomAttach: 1,
        roomDetach: 1,
      });
      expect.soft(wsCreated).toBe(0);
      const observedRequests = originRequests.slice(requestsBefore);
      expect.soft(observedRequests).toHaveLength(33);
      expect.soft(observedRequests.every((request) => request.status === 200)).toBe(true);
      expect.soft(observedRequests.every((request) => request.rawOrigin === undefined)).toBe(true);
      expect
        .soft(observedRequests.every((request) => request.injectedOrigin === ALLOWED_ORIGIN))
        .toBe(true);
    } finally {
      try {
        logical?.close(1000, 'factory-finally');
      } catch {
        // The logical socket may already be closed.
      }
      for (const { client, originEntry } of attackers) {
        removeOrigin(originEntry);
        closeNativeClientOnce(client, { closeCode: 0, reason: 'attacker-finally' });
      }
      await cleanupListener(observer);
    }
    expect.soft(await udpPortIsFree(observer.port)).toBe(true);
  }, 60_000);

  it('inicia stopServer real no callback de close enquanto o owner real ainda esta closing', async () => {
    const observer = await startObservedListener('listener-race');
    const baseline = stateInventory(observer);
    const requestsBefore = originRequests.length;
    let raceSnapshot;
    let stopRequests = 0;
    let listenerClosedSettled = false;
    void Promise.resolve(observer.state.listener.closed).then(
      () => (listenerClosedSettled = true),
      () => (listenerClosedSettled = true),
    );
    observer.onSessionClose = ({ record }) => {
      if (raceSnapshot) return;
      raceSnapshot = {
        sessions: observer.state.sessions.size,
        connections: observer.state.connections.size,
        closeCalls: record.closes,
        pendingReads: record.pendingReads,
        readTerminals: record.readTerminals,
        releases: record.releases,
        listenerClosedSettled,
      };
      stopRequests++;
      observer.state.stop();
    };
    const attack = directAuthenticatedClient(observer, 'listener-race', 'listener-race');
    try {
      await prazo(attack.client.ready, 10_000, 'listener race CONNECT ready');
      await prazo(observer.state.listener.closed, 10_000, 'listener race listener.closed');
      await eventually(
        () => observer.state.sessions.size === 0 && observer.state.connections.size === 0,
        5000,
        'listener race state drained',
      );
      let clientOutcome = await Promise.race([
        attack.closed,
        new Promise((resolve) => nativeSetTimeout(() => resolve('pending'), 500)),
      ]);
      if (clientOutcome === 'pending') {
        closeNativeClientOnce(attack.client, {
          closeCode: 0,
          reason: 'listener-race-client-cleanup',
        });
        clientOutcome = await Promise.race([
          attack.closed,
          new Promise((resolve) => nativeSetTimeout(() => resolve('pending-after-close'), 500)),
        ]);
      }
      const record = observer.sessionRecords[0];
      const stableCounts = {
        closes: record?.closes,
        reads: record?.reads,
        readTerminals: record?.readTerminals,
        pendingReads: record?.pendingReads,
        cancels: record?.cancels,
        releases: record?.releases,
        closedSettlements: record?.closedSettlements,
        sessionsAdd: observer.counts.sessionsAdd,
        sessionsDelete: observer.counts.sessionsDelete,
        connectionsAdd: observer.counts.connectionsAdd,
        connectionsDelete: observer.counts.connectionsDelete,
        onConnection: observer.counts.onConnection,
        roomAttach: observer.counts.roomAttach,
        roomDetach: observer.counts.roomDetach,
        listenerStopServer: observer.counts.listenerStopServer,
        errors: observer.errors.length,
      };
      expect
        .soft(closeWireSession(record?.session, { closeCode: 1, reason: 'late-listener-close' }))
        .toBe(false);
      await new Promise((resolve) => nativeSetTimeout(resolve, 100));

      expect
        .soft(
          ['closed', 'rejected', 'pending-after-close'],
          `diagnostico remoto client.closed=${clientOutcome}`,
        )
        .toContain(clientOutcome);
      expect.soft(listenerClosedSettled).toBe(true);
      expect.soft(raceSnapshot).toEqual({
        sessions: 1,
        connections: 0,
        closeCalls: 1,
        pendingReads: 1,
        readTerminals: 0,
        releases: 0,
        listenerClosedSettled: false,
      });
      expect.soft(stopRequests).toBe(1);
      expect.soft(observer.counts.listenerStopServer).toBe(1);
      expect.soft(observer.counts).toMatchObject({
        sessionsAdd: 1,
        sessionsDelete: 1,
        connectionsAdd: 0,
        connectionsDelete: 0,
        onConnection: 0,
        roomAttach: 0,
        roomDetach: 0,
      });
      expect.soft(record).toMatchObject({
        closes: 1,
        readerGets: 1,
        reads: 1,
        readTerminals: 1,
        pendingReads: 0,
        cancels: 1,
        releases: 1,
        closedSettlements: 1,
        instrumentationErrors: [],
      });
      expect.soft(observer.errors).toHaveLength(1);
      expect.soft(observer.errors[0]).toMatchObject({ reason: 'handshake-timeout' });
      expect
        .soft({
          closes: record.closes,
          reads: record.reads,
          readTerminals: record.readTerminals,
          pendingReads: record.pendingReads,
          cancels: record.cancels,
          releases: record.releases,
          closedSettlements: record.closedSettlements,
          sessionsAdd: observer.counts.sessionsAdd,
          sessionsDelete: observer.counts.sessionsDelete,
          connectionsAdd: observer.counts.connectionsAdd,
          connectionsDelete: observer.counts.connectionsDelete,
          onConnection: observer.counts.onConnection,
          roomAttach: observer.counts.roomAttach,
          roomDetach: observer.counts.roomDetach,
          listenerStopServer: observer.counts.listenerStopServer,
          errors: observer.errors.length,
        })
        .toEqual(stableCounts);
      expect.soft(stateInventory(observer)).toMatchObject({
        sessions: baseline.sessions,
        connections: baseline.connections,
        originPending: baseline.originPending,
      });
      const observedRequests = originRequests.slice(requestsBefore);
      expect.soft(observedRequests).toHaveLength(1);
      expect.soft(observedRequests[0]).toMatchObject({
        scenario: 'listener-race',
        status: 200,
        rawOrigin: undefined,
        injectedOrigin: ALLOWED_ORIGIN,
      });
    } finally {
      removeOrigin(attack.originEntry);
      closeNativeClientOnce(attack.client, {
        closeCode: 0,
        reason: 'listener-race-finally',
      });
      await cleanupListener(observer);
    }
    expect.soft(await udpPortIsFree(observer.port)).toBe(true);
  }, 30_000);

  it('cleanup global remove temporario somente depois dos listeners', () => {
    expect.soft(existsSync(liveTemp)).toBe(true);
    expect.soft(originPending).toHaveLength(0);
  });
});
