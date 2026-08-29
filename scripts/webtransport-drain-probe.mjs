import { createHash, X509Certificate } from 'node:crypto';
import { createSocket } from 'node:dgram';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';

import { signToken, verifyToken } from '../server/tokens.js';
import { startWebTransport } from '../server/webtransport.js';

const ALLOWED_ORIGIN = 'https://activity.example';
const LIVE_ROOM = 'drain-diagnostic-room';
const POLL_MS = 10;
const DRAIN_LIMIT_MS = 10_000;
const nativeSetTimeout = globalThis.setTimeout.bind(globalThis);
const nativeClearTimeout = globalThis.clearTimeout.bind(globalThis);

let addon;
let livePair;
let liveTemp;
let originalSetRequestCallback;
const originPending = [];
const originRequests = [];
const nativeClients = new Set();
const settledClients = new WeakSet();
const closedClients = new WeakSet();

function wait(milliseconds) {
  return new Promise((resolve) => nativeSetTimeout(resolve, milliseconds));
}

function withTimeout(promise, milliseconds, label) {
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
    await wait(POLL_MS);
  }
  throw lastError ?? new Error(`${label} timeout`);
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
      const header = { ...rawHeader };
      if (entry) header.origin = entry.origin;
      const request = {
        scenario: entry?.scenario ?? 'unplanned',
        path,
        rawOrigin: rawHeader.origin ?? rawHeader.Origin,
        injectedOrigin: entry?.origin,
      };
      originRequests.push(request);
      const response = await callback({ ...args, header });
      request.status = response?.status;
      return response;
    });
  };
}

function liveToken(uid) {
  return signToken({
    room: LIVE_ROOM,
    role: 'viewer',
    uid,
    name: uid,
    instance: `drain-diagnostic-${uid}`,
  });
}

function directAuthenticatedClient(observer, uid) {
  const url = new URL(observer.state.capability().url);
  url.searchParams.set('t', liveToken(uid));
  const originEntry = registerOrigin(url, uid);
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
    settledClients.add(client);
    nativeClients.delete(client);
  });
  return { client, closed, originEntry };
}

function closeClientOnce(client, info) {
  if (!client || settledClients.has(client) || closedClients.has(client)) return false;
  closedClients.add(client);
  try {
    client.close(info);
  } catch {
    // The native peer may settle between the check and close.
  }
  return true;
}

function stateInventory(observer) {
  return {
    sessions: observer.state.sessions.size,
    connections: observer.state.connections.size,
    originPending: originPending.length,
    listenerPort: observer.state.listener.address()?.port,
  };
}

function inventoriesEqual(left, right) {
  return (
    left.sessions === right.sessions &&
    left.connections === right.connections &&
    left.originPending === right.originPending &&
    left.listenerPort === right.listenerPort
  );
}

async function prepareAddonAndCertificate() {
  delete process.env.WT_LIVE_NODE_MODULES;
  const [wt, certModule] = await Promise.all([
    import('@fails-components/webtransport'),
    import('selfsigned'),
  ]);
  addon = wt;
  const selfsigned = certModule.default ?? certModule;
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
  liveTemp = await mkdtemp(join(tmpdir(), 'discord-locutor-drain-probe-'));
  const certPath = join(liveTemp, 'cert.pem');
  const keyPath = join(liveTemp, 'key.pem');
  await Promise.all([writeFile(certPath, pair.cert), writeFile(keyPath, pair.private)]);
  livePair = {
    certPath,
    keyPath,
    hash: createHash('sha256').update(new X509Certificate(pair.cert).raw).digest(),
  };
  installOriginSeam();
}

async function startObservedListener(label) {
  const observer = {
    label,
    state: null,
    port: null,
    errors: [],
    sessionsAdd: 0,
    sessionsDelete: 0,
    maxSessions: 0,
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
    onError: (error) => observer.errors.push(error),
    onConnection: () => {},
  });
  observer.state = state;
  if (!state.listener) throw new Error(`${label}: listener was not created`);
  const nativeAdd = state.sessions.add.bind(state.sessions);
  const nativeDelete = state.sessions.delete.bind(state.sessions);
  state.sessions.add = (session) => {
    observer.sessionsAdd++;
    const result = nativeAdd(session);
    observer.maxSessions = Math.max(observer.maxSessions, state.sessions.size);
    return result;
  };
  state.sessions.delete = (session) => {
    observer.sessionsDelete++;
    return nativeDelete(session);
  };
  await eventually(() => state.capability(), 10_000, `${label}: listener ready`);
  observer.port = state.listener.address()?.port;
  if (!observer.port) throw new Error(`${label}: listener has no UDP port`);
  return observer;
}

async function cleanupListener(observer) {
  if (!observer?.state) return;
  if (!observer.state.stopped) observer.state.stop();
  await withTimeout(
    Promise.resolve(observer.state.listener.closed).catch(() => {}),
    10_000,
    'listener closed',
  );
  await eventually(
    () => observer.state.sessions.size === 0 && observer.state.connections.size === 0,
    5000,
    'listener state drained',
  );
}

async function runProbe() {
  const previousSecret = process.env.SESSION_SECRET;
  process.env.SESSION_SECRET = 'drain-diagnostic-secret';
  let observer;
  let attackers = [];
  let terminal;
  try {
    await prepareAddonAndCertificate();
    observer = await startObservedListener('drain-diagnostic');
    const baseline = stateInventory(observer);
    attackers = Array.from({ length: 32 }, (_, index) =>
      directAuthenticatedClient(observer, `attacker-${index}`),
    );
    await withTimeout(
      Promise.all(attackers.map(({ client }) => client.ready)),
      10_000,
      '32 authenticated CONNECTs ready',
    );
    await eventually(() => observer.sessionsAdd === 32, 5000, '32 sessions registered');
    await withTimeout(
      Promise.all(attackers.map(({ closed }) => closed)),
      10_000,
      '32 no-bidi sessions closed',
    );

    const t0 = performance.now();
    const deadline = t0 + DRAIN_LIMIT_MS;
    let finalInventory = stateInventory(observer);
    while (!inventoriesEqual(finalInventory, baseline) && performance.now() <= deadline) {
      await wait(POLL_MS);
      finalInventory = stateInventory(observer);
    }
    const elapsed = performance.now() - t0;
    const censored = !inventoriesEqual(finalInventory, baseline);
    terminal = {
      kind: 'drain-probe',
      ok: true,
      censored,
      drainMs: censored ? undefined : elapsed,
      drainMsLowerBound: censored ? DRAIN_LIMIT_MS : elapsed,
      pollMs: POLL_MS,
      baseline,
      finalInventory,
      listenerPort: observer.port,
      sessionsAdd: observer.sessionsAdd,
      sessionsDelete: observer.sessionsDelete,
      maxSessions: observer.maxSessions,
      originRequests: originRequests.length,
      originStatuses: originRequests.map((request) => request.status),
      observerErrors: observer.errors.map(
        (error) => error?.reason ?? error?.message ?? String(error),
      ),
    };
  } catch (error) {
    terminal = {
      kind: 'drain-probe',
      ok: false,
      error: error?.stack ?? error?.message ?? String(error),
      inventory: observer ? stateInventory(observer) : null,
    };
  } finally {
    for (const { client, originEntry } of attackers) {
      removeOrigin(originEntry);
      closeClientOnce(client, { closeCode: 0, reason: 'drain-probe-cleanup' });
    }
    const cleanupErrors = [];
    try {
      await cleanupListener(observer);
    } catch (error) {
      cleanupErrors.push(error?.message ?? String(error));
    }
    if (originalSetRequestCallback && addon) {
      addon.Http3Server.prototype.setRequestCallback = originalSetRequestCallback;
    }
    try {
      if (liveTemp) await rm(liveTemp, { recursive: true, force: true });
    } catch (error) {
      cleanupErrors.push(error?.message ?? String(error));
    }
    if (previousSecret === undefined) delete process.env.SESSION_SECRET;
    else process.env.SESSION_SECRET = previousSecret;
    if (terminal) {
      terminal.cleanupErrors = cleanupErrors;
      terminal.udpPortFree = observer?.port ? await udpPortIsFree(observer.port) : null;
      terminal.ok = terminal.ok && cleanupErrors.length === 0 && terminal.udpPortFree !== false;
    }
  }
  process.stdout.write(`DRAIN_JSON ${JSON.stringify(terminal)}\n`);
  if (!terminal?.ok) process.exitCode = 1;
}

await runProbe();
