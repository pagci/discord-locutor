import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createHash, X509Certificate } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import WebSocket from 'ws';

const live = process.env.WEBTRANSPORT_LIVE === '1';
const suite = live ? describe : describe.skip;
const transportPath = join(process.cwd(), 'shared', 'transport.js');
export const fallbackUsed = !existsSync(transportPath);
const ORIGIN_PERMITIDA = 'https://activity.example';

class WsOnlyFloor {
  constructor({ wsUrl, WebSocket: Ws = WebSocket }) {
    return new Ws(wsUrl);
  }
}

async function carregarFactory() {
  if (!fallbackUsed) return import(pathToFileURL(transportPath).href);
  return { createTransport: (opts) => new WsOnlyFloor(opts) };
}

const prazo = (promise, ms, nome) =>
  Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${nome} excedeu ${ms} ms`)), ms)),
  ]);

let addon;
let selfsigned;
let temp;
let pair;
let http;
let wss;
let porta;
let createTransport;
let signToken;
let R;
let originalSetRequestCallback;
const sockets = [];
const origensPendentes = [];
const requestsOrigin = [];

function pathDe(url) {
  const parsed = new URL(String(url), 'https://listener.invalid');
  return `${parsed.pathname}${parsed.search}`;
}

function registrarOrigin(url, cenario) {
  origensPendentes.push({ path: pathDe(url), cenario });
}

function instalarSeamOrigin() {
  originalSetRequestCallback = addon.Http3Server.prototype.setRequestCallback;
  addon.Http3Server.prototype.setRequestCallback = function (callback) {
    if (typeof callback !== 'function') return originalSetRequestCallback.call(this, callback);
    return originalSetRequestCallback.call(this, async (args) => {
      const rawHeader = args?.header ?? {};
      const path = String(rawHeader[':path'] ?? rawHeader.path ?? '');
      const index = origensPendentes.findIndex((entrada) => entrada.path === path);
      const entrada = index < 0 ? null : origensPendentes.splice(index, 1)[0];
      const request = {
        cenario: entrada?.cenario ?? 'nao-planejado',
        path,
        rawOrigin: rawHeader.origin ?? rawHeader.Origin,
        injectedOrigin: entrada ? ORIGIN_PERMITIDA : undefined,
      };
      requestsOrigin.push(request);
      const header = { ...rawHeader };
      if (entrada) header.origin = ORIGIN_PERMITIDA;
      const result = await callback({ ...args, header });
      request.status = result?.status;
      return result;
    });
  };
}

function wtAllowlisted(cenario) {
  return class WebTransportRelayAllowlisted extends addon.WebTransport {
    constructor(url, options) {
      registrarOrigin(url, cenario);
      super(url, options);
    }
  };
}

function afirmarAddonSemOrigin(cenario) {
  const requests = requestsOrigin.filter((request) => request.cenario === cenario);
  expect.soft(requests).toHaveLength(1);
  expect.soft(requests.every(({ rawOrigin }) => rawOrigin === undefined)).toBe(true);
  expect
    .soft(requests.every(({ injectedOrigin }) => injectedOrigin === ORIGIN_PERMITIDA))
    .toBe(true);
}

async function prepararCertificado() {
  const base = process.env.WT_LIVE_NODE_MODULES ?? join(process.cwd(), 'node_modules');
  const [wt, certModule] = await Promise.all([
    import(
      pathToFileURL(join(base, '@fails-components', 'webtransport', 'lib', 'index.node.js')).href
    ),
    import(pathToFileURL(join(base, 'selfsigned', 'index.js')).href),
  ]);
  addon = wt;
  selfsigned = certModule.default ?? certModule;
  const notBeforeDate = new Date(Date.now() - 60_000);
  const notAfterDate = new Date(notBeforeDate);
  notAfterDate.setDate(notAfterDate.getDate() + 12);
  pair = await selfsigned.generate([{ name: 'commonName', value: 'localhost' }], {
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
  pair.hash = createHash('sha256').update(new X509Certificate(pair.cert).raw).digest();
  temp = await mkdtemp(join(tmpdir(), 'discord-locutor-relay-wt-'));
  pair.certPath = join(temp, 'cert.pem');
  pair.keyPath = join(temp, 'key.pem');
  await Promise.all([writeFile(pair.certPath, pair.cert), writeFile(pair.keyPath, pair.private)]);
}

function token(room, role, uid) {
  return signToken({ room: room.id, role, uid, name: uid, av: null });
}

function quadro(slot, tipo, marcador) {
  const bytes = Buffer.alloc(19);
  bytes[0] = slot;
  bytes[1] = tipo;
  bytes.writeDoubleBE(1, 2);
  bytes.writeDoubleBE(2, 10);
  bytes[18] = marcador;
  return bytes;
}

function observar(socket) {
  socket.json = [];
  socket.bin = [];
  socket.addEventListener('message', (event) => {
    const data = event.data ?? event;
    if (typeof data === 'string') socket.json.push(JSON.parse(data));
    else socket.bin.push(Buffer.from(data));
  });
  return socket;
}

async function aberto(socket) {
  if (socket.readyState === 1) return socket;
  await prazo(
    new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', reject, { once: true });
    }),
    5000,
    'socket open',
  );
  return socket;
}

async function ate(socket, predicado, nome) {
  const existente = socket.json.find(predicado);
  if (existente) return existente;
  return prazo(
    new Promise((resolve) => {
      const ouvir = (event) => {
        const data = event.data ?? event;
        if (typeof data !== 'string') return;
        const msg = JSON.parse(data);
        if (!predicado(msg)) return;
        socket.removeEventListener('message', ouvir);
        resolve(msg);
      };
      socket.addEventListener('message', ouvir);
    }),
    5000,
    nome,
  );
}

async function ateBinarios(socket, quantidade) {
  if (socket.bin.length >= quantidade) return;
  await prazo(
    new Promise((resolve) => {
      const ouvir = (event) => {
        const data = event.data ?? event;
        if (typeof data === 'string') return;
        if (socket.bin.length >= quantidade) {
          socket.removeEventListener('message', ouvir);
          resolve();
        }
      };
      socket.addEventListener('message', ouvir);
    }),
    5000,
    `${quantidade} binarios`,
  );
}

function wsReal(room, role, uid) {
  const socket = observar(
    new WebSocket(`ws://127.0.0.1:${porta}/ws?t=${encodeURIComponent(token(room, role, uid))}`),
  );
  sockets.push(socket);
  return socket;
}

function wtLogico(room, role, uid, extra = '') {
  const t = token(room, role, uid);
  const socket = observar(
    createTransport({
      wsUrl: `ws://127.0.0.1:${porta}/ws?t=${encodeURIComponent(t)}${extra}`,
      capabilityUrl: `http://127.0.0.1:${porta}/api/transports`,
      timeoutMs: 1500,
      WebTransport: wtAllowlisted(`relay-${role}-${uid}`),
      WebSocket,
      fetch,
    }),
  );
  sockets.push(socket);
  return socket;
}

suite('relay misto real', () => {
  beforeAll(async () => {
    await prepararCertificado();
    instalarSeamOrigin();
    Object.assign(process.env, {
      PORT: '0',
      NODE_ENV: 'test',
      PUBLIC_ORIGIN: ORIGIN_PERMITIDA,
      NODE_ORIGINS: ORIGIN_PERMITIDA,
      WEBTRANSPORT_ENABLED: 'true',
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: pair.certPath,
      WEBTRANSPORT_KEY_PATH: pair.keyPath,
      WEBTRANSPORT_HOST: '127.0.0.1',
      WEBTRANSPORT_PORT: '0',
      WEBTRANSPORT_PUBLIC_URL: 'https://127.0.0.1:0/wt',
    });
    ({ createTransport } = await carregarFactory());
    ({ signToken } = await import('./tokens.js'));
    R = await import('./rooms.js');
    ({ server: http, wss } = await import('./index.js'));
    if (!http.listening) await new Promise((resolve) => http.once('listening', resolve));
    porta = http.address().port;
  }, 30_000);

  afterAll(async () => {
    try {
      for (const socket of sockets) socket.terminate?.() ?? socket.close?.();
      wss?.close();
      if (http?.listening) await new Promise((resolve) => http.close(resolve));
    } finally {
      if (originalSetRequestCallback)
        addon.Http3Server.prototype.setRequestCallback = originalSetRequestCallback;
      if (temp) await rm(temp, { recursive: true, force: true });
    }
  });

  const novaSala = (sufixo) =>
    R.createRoom({
      instance: `mixed-${sufixo}`,
      name: 'Mista',
      ownerId: 'owner',
      ownerName: 'Owner',
    }).room;

  async function provarFluxo(transmissor, viewer) {
    await Promise.all([aberto(transmissor), aberto(viewer)]);
    const slot = (await ate(transmissor, (m) => m.type === 'slot', 'slot')).slot;
    transmissor.send(JSON.stringify({ type: 'start' }));
    await ate(viewer, (m) => m.type === 'stream-start', 'stream-start');
    transmissor.send(JSON.stringify({ type: 'config', config: { codec: 'vp8' } }));
    transmissor.send(quadro(slot, 1, 10));
    transmissor.send(quadro(slot, 3, 10));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect.soft(viewer.bin).toHaveLength(0);
    viewer.send(JSON.stringify({ type: 'watch', slot }));
    await ate(transmissor, (m) => m.type === 'need-keyframe', 'need-keyframe');
    transmissor.send(JSON.stringify({ type: 'config', config: { codec: 'vp8' } }));
    transmissor.send(JSON.stringify({ type: 'audio-config', config: { codec: 'opus' } }));
    transmissor.send(quadro(slot, 1, 11));
    transmissor.send(quadro(slot, 2, 12));
    transmissor.send(quadro(slot, 3, 13));
    await ateBinarios(viewer, 3);
    expect
      .soft(viewer.json)
      .toEqual(
        expect.arrayContaining([
          expect.objectContaining({ type: 'config', slot }),
          expect.objectContaining({ type: 'audio-config', slot }),
        ]),
      );
    const entregues = viewer.bin.slice(-3).map((b) => [b[0], b[1], b[18]]);
    expect.soft(entregues).toEqual(
      expect.arrayContaining([
        [slot, 1, 11],
        [slot, 2, 12],
        [slot, 3, 13],
      ]),
    );
    expect.soft(entregues.filter(([, type]) => type !== 3)).toEqual([
      [slot, 1, 11],
      [slot, 2, 12],
    ]);
  }

  it('WT broadcaster -> WS viewer preserva config/keyframe/delta/audio e opt-in', async () => {
    const room = novaSala('wt-ws');
    const viewer = wsReal(room, 'viewer', 'viewer-ws');
    const transmissor = wtLogico(room, 'broadcaster', 'caster-wt');
    await provarFluxo(transmissor, viewer);
    expect(transmissor.transport).toBe('webtransport');
    afirmarAddonSemOrigin('relay-broadcaster-caster-wt');
  });

  it('WS broadcaster -> WT viewer preserva config/keyframe/delta/audio e opt-in', async () => {
    const room = novaSala('ws-wt');
    const transmissor = wsReal(room, 'broadcaster', 'caster-ws');
    const viewer = wtLogico(room, 'viewer', 'viewer-wt');
    await provarFluxo(transmissor, viewer);
    expect(viewer.transport).toBe('webtransport');
    afirmarAddonSemOrigin('relay-viewer-viewer-wt');
    expect.soft(origensPendentes).toHaveLength(0);
  });
});

describe('inventario do seam misto', () => {
  it('declara fallback sem transformar ausencia do modulo em RED', () => {
    expect(typeof fallbackUsed).toBe('boolean');
  });
});
