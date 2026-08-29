import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createHash, X509Certificate } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createSocket } from 'node:dgram';
import WebSocket from 'ws';

const live = process.env.WEBTRANSPORT_LIVE === '1';
const suite = live ? describe : describe.skip;
const transportPath = join(process.cwd(), 'shared', 'transport.js');
export const fallbackUsed = !existsSync(transportPath);
const HOSTIL = 'https://hostil.example';
const PERMITIDA = 'https://activity.example';
const TOKEN_LITERAL = 'token-super-secreto-nao-logar';
const prazo = (promise, ms, nome) =>
  Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${nome} excedeu ${ms} ms`)), ms)),
  ]);

let addon;
let pair;
let temp;
let http;
let wss;
let porta;
let wtOrigin;
let signToken;
let createTransport;
let R;
let nodeFor;
let originalSetRequestCallback;
const abertos = [];
const logs = [];
const origensPendentes = [];
const requestsOrigin = [];

function pathDe(url) {
  const parsed = new URL(String(url), 'https://listener.invalid');
  return `${parsed.pathname}${parsed.search}`;
}

function registrarOrigin(url, cenario, origin) {
  origensPendentes.push({ path: pathDe(url), cenario, origin, inject: origin !== undefined });
}

function criarWtCenario(cenario, origin) {
  return class WebTransportComOriginDoCenario extends addon.WebTransport {
    constructor(url, options) {
      registrarOrigin(url, cenario, origin);
      super(url, options);
    }
  };
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
      const rawOrigin = rawHeader.origin ?? rawHeader.Origin;
      const request = {
        cenario: entrada?.cenario ?? 'nao-planejado',
        path: path.split('?')[0],
        rawOrigin,
        injectedOrigin: entrada?.inject ? entrada.origin : undefined,
      };
      requestsOrigin.push(request);
      const header = { ...rawHeader };
      if (entrada?.inject) header.origin = entrada.origin;
      const result = await callback({ ...args, header });
      request.status = result?.status;
      request.reason = result?.header?.['x-discord-locutor-reason'];
      return result;
    });
  };
}

function afirmarAddonSemOrigin(desde, cenarios) {
  const observadas = requestsOrigin.slice(desde);
  expect.soft(observadas.map(({ cenario }) => cenario)).toEqual(expect.arrayContaining(cenarios));
  expect.soft(observadas.every(({ rawOrigin }) => rawOrigin === undefined)).toBe(true);
}

async function conectarWtDireto(url, cenario, origin) {
  registrarOrigin(url, cenario, origin);
  const client = new addon.WebTransport(url, {
    allowPooling: false,
    serverCertificateHashes: [{ algorithm: 'sha-256', value: pair.hash }],
  });
  abertos.push(client);
  void client.closed.catch(() => {});
  await prazo(
    client.ready.then(
      () => 'open',
      () => 'rejected',
    ),
    4000,
    `CONNECT WT ${cenario}`,
  );
  try {
    client.close();
  } catch {
    // A rejeicao do CONNECT pode fechar o cliente nativo antes do cleanup.
  }
  return requestsOrigin.findLast((request) => request.cenario === cenario);
}

async function portaUdpLivre() {
  const socket = createSocket('udp4');
  await new Promise((resolve, reject) => {
    socket.once('error', reject);
    socket.bind(0, '127.0.0.1', resolve);
  });
  const { port } = socket.address();
  await new Promise((resolve) => socket.close(resolve));
  return port;
}

class WsComOriginHostil extends WebSocket {
  constructor(url) {
    super(url, { origin: HOSTIL });
  }
}

class WsComOriginPermitida extends WebSocket {
  constructor(url) {
    super(url, { origin: PERMITIDA });
  }
}

class WsOnlyFloor {
  constructor({ wsUrl, WebSocket: Ws = WebSocket }) {
    return new Ws(wsUrl);
  }
}

async function carregarFactory() {
  if (!fallbackUsed) return import(pathToFileURL(transportPath).href);
  return { createTransport: (opts) => new WsOnlyFloor(opts) };
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
  const selfsigned = certModule.default ?? certModule;
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
  temp = await mkdtemp(join(tmpdir(), 'discord-locutor-auth-wt-'));
  pair.certPath = join(temp, 'cert.pem');
  pair.keyPath = join(temp, 'key.pem');
  await Promise.all([writeFile(pair.certPath, pair.cert), writeFile(pair.keyPath, pair.private)]);
}

function conectarWs(path, { origin, esperarOpen = false } = {}) {
  const ws = new WebSocket(`ws://127.0.0.1:${porta}${path}`, origin ? { origin } : undefined);
  const mensagens = [];
  ws.on('message', (data, isBinary) => {
    if (!isBinary) mensagens.push(JSON.parse(String(data)));
  });
  abertos.push(ws);
  const resultado = new Promise((resolve) => {
    ws.once('open', () => resolve({ status: 101, reason: 'open', ws, mensagens }));
    ws.once('unexpected-response', (_req, res) => {
      res.resume();
      resolve({ status: res.statusCode, reason: res.statusMessage, ws, mensagens });
    });
    ws.once('error', (error) => {
      if (!esperarOpen) resolve({ status: 0, reason: error.message, ws, mensagens });
    });
  });
  return prazo(resultado, 4000, `handshake WS ${path}`);
}

async function ateJson(conexao, predicado, nome, desde = 0) {
  return prazo(
    (async () => {
      for (;;) {
        const achada = conexao.mensagens.slice(desde).find(predicado);
        if (achada) return achada;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    })(),
    4000,
    nome,
  );
}

async function abrirLogico(opcoes) {
  const diagnosticos = [];
  const { waitMs = 900, ...transportOptions } = opcoes;
  const socket = createTransport({
    WebTransport: addon.WebTransport,
    fetch,
    ...transportOptions,
    timeoutMs: opcoes.timeoutMs ?? 600,
    onDiagnostic: (evento) => diagnosticos.push(evento),
  });
  abertos.push(socket);
  socket.addEventListener?.('error', () => {});
  await new Promise((resolve) => setTimeout(resolve, waitMs));
  return { socket, diagnosticos };
}

function capabilityInjetada(hostname) {
  const url = new URL(`${wtOrigin}/wt`);
  url.hostname = hostname;
  return new Response(
    JSON.stringify({
      websocket: true,
      node: 1,
      webtransport: {
        url: url.toString(),
        version: 1,
        hashes: [pair.hash.toString('base64')],
      },
    }),
    { status: 200, headers: { 'Content-Type': 'application/json' } },
  );
}

suite('auth, shard e Origin antes do attach', () => {
  beforeAll(async () => {
    await prepararCertificado();
    instalarSeamOrigin();
    const wtPort = await portaUdpLivre();
    wtOrigin = `https://127.0.0.1:${wtPort}`;
    Object.assign(process.env, {
      PORT: '0',
      NODE_ENV: 'test',
      PUBLIC_ORIGIN: PERMITIDA,
      SHARD_INDEX: '1',
      SHARD_NODES: '2',
      NODE_ORIGINS: `${PERMITIDA},${wtOrigin}`,
      WEBTRANSPORT_ENABLED: 'true',
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: pair.certPath,
      WEBTRANSPORT_KEY_PATH: pair.keyPath,
      WEBTRANSPORT_HOST: '127.0.0.1',
      WEBTRANSPORT_PORT: String(wtPort),
      WEBTRANSPORT_PUBLIC_URL: `${wtOrigin}/wt`,
    });
    vi.spyOn(console, 'log').mockImplementation((...args) => logs.push(args.join(' ')));
    vi.spyOn(console, 'warn').mockImplementation((...args) => logs.push(args.join(' ')));
    vi.spyOn(console, 'error').mockImplementation((...args) => logs.push(args.join(' ')));
    ({ createTransport } = await carregarFactory());
    ({ signToken } = await import('./tokens.js'));
    R = await import('./rooms.js');
    ({ nodeFor } = await import('../shared/shard.js'));
    ({ server: http, wss } = await import('./index.js'));
    if (!http.listening) await new Promise((resolve) => http.once('listening', resolve));
    porta = http.address().port;
  }, 30_000);

  afterAll(async () => {
    try {
      for (const socket of abertos) socket.terminate?.() ?? socket.close?.();
      wss?.close();
      if (http?.listening) await new Promise((resolve) => http.close(resolve));
    } finally {
      if (originalSetRequestCallback)
        addon.Http3Server.prototype.setRequestCallback = originalSetRequestCallback;
      if (temp) await rm(temp, { recursive: true, force: true });
    }
  });

  function salaValida() {
    for (let i = 0; i < 30; i++) {
      const instance = `auth-${i}-${Date.now()}`;
      if (nodeFor(instance, 2) !== 1) continue;
      const room = R.createRoom({
        instance,
        name: 'Auth',
        ownerId: 'owner',
        ownerName: 'Owner',
      }).room;
      const t = signToken({
        room: room.id,
        role: 'viewer',
        uid: 'viewer',
        name: 'Viewer',
        instance,
      });
      return { room, t, instance };
    }
    throw new Error('fixture nao criou sala');
  }

  it('WS recusa o Origin hostil e aceita a origem exata, sem attach no negativo', async () => {
    const { room, t, instance } = salaValida();
    const observadorToken = signToken({
      room: room.id,
      role: 'viewer',
      uid: 'observer',
      name: 'Observer',
      instance,
    });
    const observador = await conectarWs(`/n1/ws?t=${encodeURIComponent(observadorToken)}`);
    expect.soft(observador.status).toBe(101);
    await ateJson(observador, (msg) => msg.type === 'state' && msg.viewers === 1, 'estado inicial');

    const antesHostil = observador.mensagens.length;
    const hostil = await conectarWs(`/n1/ws?t=${encodeURIComponent(t)}`, { origin: HOSTIL });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const efeitosHostis = observador.mensagens.slice(antesHostil);
    if (hostil.status === 101) {
      const antesRollback = observador.mensagens.length;
      hostil.ws.close();
      await ateJson(
        observador,
        (msg) => msg.type === 'state' && msg.viewers === 1,
        'rollback hostil',
        antesRollback,
      );
    }

    const antesPermitida = observador.mensagens.length;
    const permitida = await conectarWs(`/n1/ws?t=${encodeURIComponent(t)}`, { origin: PERMITIDA });
    await ateJson(
      observador,
      (msg) => msg.type === 'state' && msg.viewers === 2,
      'attach positivo allowlisted',
      antesPermitida,
    );
    expect.soft(hostil).toMatchObject({ status: 403, reason: expect.stringMatching(/forbidden/i) });
    expect.soft(efeitosHostis).toHaveLength(0);
    expect.soft(permitida.status).toBe(101);
    expect.soft(logs.join('\n')).not.toContain(t);
  });

  it('WS distingue path, token, node e shard antes do dispatcher', async () => {
    const { t } = salaValida();
    const identity = signToken({ scope: 'identity', uid: 'x', instance: 'x' });
    let instanceOutro = 'node-zero';
    while (nodeFor(instanceOutro, 2) !== 0) instanceOutro += '-x';
    const outroShard = signToken({
      room: 'inexistente',
      role: 'viewer',
      uid: 'x',
      instance: instanceOutro,
    });
    const [path, auth, node, shard] = await Promise.all([
      conectarWs(`/n1/outro?t=${encodeURIComponent(t)}`),
      conectarWs(`/n1/ws?t=${encodeURIComponent(identity)}`),
      conectarWs(`/n0/ws?t=${encodeURIComponent(t)}`),
      conectarWs(`/n1/ws?t=${encodeURIComponent(outroShard)}`),
    ]);
    expect.soft(path.status).not.toBe(101);
    expect.soft(auth.status).toBe(401);
    expect.soft(node.status).toBe(421);
    expect.soft(shard.status).toBe(409);
    expect.soft(logs.join('\n')).not.toContain(identity);
  });

  it('capability dirigida a n1 e CONNECT preservam apenas t/fonte/modo', async () => {
    const { t } = salaValida();
    const desdeOrigin = requestsOrigin.length;
    const capturadas = [];
    class WtCapturado extends addon.WebTransport {
      constructor(url, opts) {
        capturadas.push(String(url));
        registrarOrigin(url, 'capability-n1-allowlisted', PERMITIDA);
        super(url, opts);
      }
    }
    const capabilityUrl = `http://127.0.0.1:${porta}/.proxy/n1/api/transports`;
    const { socket } = await abrirLogico({
      wsUrl: `ws://127.0.0.1:${porta}/.proxy/n1/ws?t=${encodeURIComponent(t)}&fonte=camera&modo=controle&x=nao`,
      capabilityUrl,
      WebTransport: WtCapturado,
      WebSocket: WsComOriginPermitida,
    });
    expect.soft(socket.transport).toBe('webtransport');
    expect.soft(capturadas).toHaveLength(1);
    const conectada = new URL(capturadas[0] ?? 'https://invalid/');
    expect.soft(conectada.origin).toBe(wtOrigin);
    expect.soft(process.env.NODE_ORIGINS?.split(',')).toContain(conectada.origin);
    expect.soft(conectada.pathname).toBe('/wt');
    expect.soft([...conectada.searchParams.keys()].sort()).toEqual(['fonte', 'modo', 't']);
    expect.soft(conectada.searchParams.get('fonte')).toBe('camera');
    expect.soft(conectada.searchParams.get('modo')).toBe('controle');
    afirmarAddonSemOrigin(desdeOrigin, ['capability-n1-allowlisted']);
  });

  it('mesmo Origin hostil recusa WT e o fallback WS, inclusive quando WT falha', async () => {
    const { room, t, instance } = salaValida();
    const desdeOrigin = requestsOrigin.length;
    const observadorToken = signToken({
      room: room.id,
      role: 'viewer',
      uid: 'observer-wt',
      name: 'Observer WT',
      instance,
    });
    const observador = await conectarWs(`/n1/ws?t=${encodeURIComponent(observadorToken)}`);
    await ateJson(observador, (msg) => msg.type === 'state' && msg.viewers === 1, 'estado WT');
    const origensPositivas = [];
    class WtOriginAllowlisted extends addon.WebTransport {
      constructor(url, options) {
        origensPositivas.push(new URL(url).origin);
        registrarOrigin(url, 'wt-origin-allowlisted', PERMITIDA);
        super(url, options);
      }
    }
    const tokenPositivo = signToken({
      room: room.id,
      role: 'viewer',
      uid: 'viewer-wt-positive',
      name: 'Viewer WT positive',
      instance,
    });
    const antesPositivo = observador.mensagens.length;
    const positivo = await abrirLogico({
      wsUrl: `ws://127.0.0.1:${porta}/.proxy/n1/ws?t=${encodeURIComponent(tokenPositivo)}`,
      capabilityUrl: `http://127.0.0.1:${porta}/.proxy/n1/api/transports`,
      fetch: async () => capabilityInjetada('127.0.0.1'),
      WebTransport: WtOriginAllowlisted,
      WebSocket: WsComOriginPermitida,
      timeoutMs: 1500,
      waitMs: 1800,
    });
    await ateJson(
      observador,
      (msg) => msg.type === 'state' && msg.viewers === 2,
      'attach WT positivo',
      antesPositivo,
    );
    expect
      .soft(positivo.socket.transport, JSON.stringify(positivo.diagnosticos))
      .toBe('webtransport');
    const antesFecharPositivo = observador.mensagens.length;
    positivo.socket.close();
    await ateJson(
      observador,
      (msg) => msg.type === 'state' && msg.viewers === 1,
      'detach WT positivo',
      antesFecharPositivo,
    );

    const antesHostil = observador.mensagens.length;
    const { socket, diagnosticos } = await abrirLogico({
      wsUrl: `ws://127.0.0.1:${porta}/.proxy/n1/ws?t=${encodeURIComponent(t)}`,
      capabilityUrl: `http://127.0.0.1:${porta}/.proxy/n1/api/transports`,
      fetch: async () => capabilityInjetada('127.0.0.1'),
      WebTransport: criarWtCenario('wt-origin-hostil', HOSTIL),
      WebSocket: WsComOriginHostil,
      timeoutMs: 1500,
      waitMs: 1800,
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const efeitosHostis = observador.mensagens.slice(antesHostil);
    expect.soft(socket.readyState).toBe(socket.CLOSED ?? 3);
    expect.soft(efeitosHostis).toHaveLength(0);
    expect
      .soft(diagnosticos)
      .toEqual(expect.arrayContaining([expect.objectContaining({ reason: 'ws-origin-invalid' })]));
    expect.soft(requestsOrigin.slice(desdeOrigin)).toContainEqual(
      expect.objectContaining({
        cenario: 'wt-origin-hostil',
        status: 403,
        reason: 'wt-origin-invalid',
      }),
    );
    expect.soft(origensPositivas).toEqual([wtOrigin]);
    expect.soft(process.env.NODE_ORIGINS?.split(',')).toContain(origensPositivas[0]);
    afirmarAddonSemOrigin(desdeOrigin, ['wt-origin-hostil', 'wt-origin-allowlisted']);
    expect.soft(logs.join('\n')).not.toContain(t);
    expect.soft(logs.join('\n')).not.toContain(TOKEN_LITERAL);
  }, 10_000);

  it('WT distingue Origin ausente e path/token/node/shard/room invalidos antes do attach', async () => {
    const desdeOrigin = requestsOrigin.length;
    const resposta = await fetch(`http://127.0.0.1:${porta}/.proxy/n1/api/transports`);
    const tipo = resposta.headers.get('content-type') ?? '';
    const texto = await resposta.text();
    expect.soft(tipo).toContain('application/json');
    if (!tipo.includes('application/json')) return;
    const capability = JSON.parse(texto);
    const baseWt = new URL(capability.webtransport.url);
    let instanceOutro = 'wt-node-zero';
    while (nodeFor(instanceOutro, 2) !== 0) instanceOutro += '-x';
    const tokenOutroShard = signToken({
      room: 'inexistente',
      role: 'viewer',
      uid: 'x',
      instance: instanceOutro,
    });
    const sala = salaValida();
    const observadorToken = signToken({
      room: sala.room.id,
      role: 'viewer',
      uid: 'observer-negative-wt',
      name: 'Observer negative WT',
      instance: sala.instance,
    });
    const observador = await conectarWs(`/n1/ws?t=${encodeURIComponent(observadorToken)}`);
    await ateJson(
      observador,
      (msg) => msg.type === 'state' && msg.viewers === 1,
      'estado negativo WT',
    );
    const antesNegativos = observador.mensagens.length;
    let instanceRoomGone = 'wt-room-gone';
    while (nodeFor(instanceRoomGone, 2) !== 1) instanceRoomGone += '-x';
    const tokenRoomGone = signToken({
      room: `inexistente-${Date.now()}`,
      role: 'viewer',
      uid: 'room-gone',
      instance: instanceRoomGone,
    });
    const casos = [
      ['path-allowlisted', '/outro', '', PERMITIDA, 404, 'wt-path-invalid'],
      ['token-allowlisted', '/wt', TOKEN_LITERAL, PERMITIDA, 401, 'wt-auth-invalid'],
      ['node-allowlisted', '/n0/wt', sala.t, PERMITIDA, 421, 'wt-node-misdirected'],
      ['shard-allowlisted', '/n1/wt', tokenOutroShard, PERMITIDA, 409, 'wt-shard-mismatch'],
      ['room-allowlisted', '/n1/wt', tokenRoomGone, PERMITIDA, 404, 'wt-room-gone'],
      ['missing-origin', '/n1/wt', sala.t, undefined, 403, 'wt-origin-invalid'],
    ];
    const observados = [];
    for (const [cenario, path, t, origin, status, reason] of casos) {
      const url = new URL(baseWt);
      url.pathname = path;
      if (t) url.searchParams.set('t', t);
      const observado = await conectarWtDireto(url, cenario, origin);
      observados.push(observado);
      expect.soft(observado).toMatchObject({ status, reason });
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect
      .soft(observador.mensagens.slice(antesNegativos))
      .not.toContainEqual(expect.objectContaining({ type: 'state', viewers: 2 }));
    afirmarAddonSemOrigin(
      desdeOrigin,
      casos.map(([cenario]) => cenario),
    );
    expect.soft(origensPendentes).toHaveLength(0);
    expect.soft(JSON.stringify(observados)).not.toContain(TOKEN_LITERAL);
  });
});

describe('inventario do seam de auth', () => {
  it('declara fallback sem usar ausencia do modulo como RED', () => {
    expect(typeof fallbackUsed).toBe('boolean');
  });
});
