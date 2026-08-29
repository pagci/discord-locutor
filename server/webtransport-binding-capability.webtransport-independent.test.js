import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createHash, randomBytes, X509Certificate } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createSocket } from 'node:dgram';
import { createServer as createTcpServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import WebSocket from 'ws';

const live = process.env.WEBTRANSPORT_LIVE === '1';
const liveModules = process.env.WT_LIVE_NODE_MODULES;
const suite = live ? describe : describe.skip;
const transportPath = join(process.cwd(), 'shared', 'transport.js');
export const fallbackUsed = !existsSync(transportPath);
const nativeSetTimeout = globalThis.setTimeout.bind(globalThis);
const esperar = (ms) => new Promise((resolve) => nativeSetTimeout(resolve, ms));
const prazo = (promise, ms, nome) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      nativeSetTimeout(() => reject(new Error(`${nome} excedeu ${ms} ms`)), ms),
    ),
  ]);

let addon;
let selfsigned;
let temp;
let current;
let next;
let createTransport;
let signToken;
let R;
let http;
let wss;
let httpUrl;
let wtOrigin;
let captured;
let originalStart;
let originalUpdate;
let originalSetRequestCallback;
let preReadyQuery;
let readyAt;
let rolloverProbe;
let preReadyStop = false;
let listenerStopIssued = false;
let ignorarProbeCru = false;
let fakeTimersActive = false;
let rolloverToken;
const updates = [];
const sockets = [];
const origensPendentes = [];
const requestsOrigin = [];

function pathDe(url) {
  const parsed = new URL(String(url), 'https://listener.invalid');
  return `${parsed.pathname}${parsed.search}`;
}

function registrarOrigin(url, cenario, origin = wtOrigin) {
  const entrada = { path: pathDe(url), cenario, origin };
  origensPendentes.push(entrada);
  return entrada;
}

function removerOriginPendente(entrada) {
  const index = origensPendentes.indexOf(entrada);
  if (index >= 0) origensPendentes.splice(index, 1);
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
        injectedOrigin: entrada?.origin,
      };
      requestsOrigin.push(request);
      const header = { ...rawHeader };
      if (entrada) header.origin = entrada.origin;
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

function urlAutenticada(url) {
  const parsed = new URL(url);
  parsed.searchParams.set('t', rolloverToken);
  return parsed.toString();
}

function criarWtAllowlisted(cenario) {
  return class WebTransportAllowlisted extends addon.WebTransport {
    constructor(url, options) {
      registrarOrigin(url, cenario);
      super(url, options);
    }
  };
}

class WsOnlyFloor {
  constructor({ wsUrl, WebSocket: Ws = WebSocket }) {
    return new Ws(wsUrl);
  }
}

async function carregarFactory() {
  if (!fallbackUsed) return import(pathToFileURL(transportPath).href);
  return { createTransport: (options) => new WsOnlyFloor(options) };
}

async function portaTcpLivre() {
  const server = createTcpServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
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

const agora = () => performance.now();

async function lerCapability() {
  const resposta = await fetch(`${httpUrl}/api/transports`);
  const tipo = resposta.headers.get('content-type') ?? '';
  const texto = await resposta.text();
  return {
    status: resposta.status,
    tipo,
    body: tipo.includes('application/json') ? JSON.parse(texto) : null,
  };
}

function observar(socket) {
  socket.json = [];
  socket.bin = [];
  socket.addEventListener?.('message', (event) => {
    const data = event.data ?? event;
    if (typeof data === 'string') socket.json.push(JSON.parse(data));
    else socket.bin.push(Buffer.from(data));
  });
  sockets.push(socket);
  return socket;
}

async function aberto(socket) {
  if (socket.readyState === (socket.OPEN ?? 1)) return socket;
  await prazo(
    new Promise((resolve, reject) => {
      socket.addEventListener?.('open', resolve, { once: true });
      socket.addEventListener?.('error', reject, { once: true });
    }),
    6000,
    'socket logico open',
  );
  return socket;
}

async function ateJson(socket, predicado, nome) {
  const existente = socket.json.find(predicado);
  if (existente) return existente;
  return prazo(
    (async () => {
      for (;;) {
        const achada = socket.json.find(predicado);
        if (achada) return achada;
        await esperar(10);
      }
    })(),
    6000,
    nome,
  );
}

async function ateBinarios(socket, quantidade, nome, ms = 30_000) {
  await prazo(
    (async () => {
      while (socket.bin.length < quantidade) await esperar(10);
    })(),
    ms,
    nome,
  );
}

function quadro(slot, tipo, timestamp, payloadBytes, marcador) {
  const bytes = Buffer.alloc(18 + payloadBytes);
  bytes[0] = slot;
  bytes[1] = tipo;
  bytes.writeDoubleBE(timestamp, 2);
  bytes.writeDoubleBE(Date.now(), 10);
  bytes[bytes.length - 1] = marcador;
  return bytes;
}

function token(room, role, uid) {
  return signToken({ room: room.id, role, uid, name: uid, av: null });
}

function logico(room, role, uid, WebTransportClass = criarWtAllowlisted(`logico-${role}-${uid}`)) {
  const t = token(room, role, uid);
  return observar(
    createTransport({
      wsUrl: `${httpUrl.replace('http:', 'ws:')}/ws?t=${encodeURIComponent(t)}`,
      capabilityUrl: `${httpUrl}/api/transports`,
      timeoutMs: 1500,
      WebTransport: WebTransportClass,
      WebSocket,
      fetch,
    }),
  );
}

async function importarLive() {
  const base = liveModules ?? join(process.cwd(), 'node_modules');
  const wtPath = join(base, '@fails-components', 'webtransport', 'lib', 'index.node.js');
  const certPath = join(base, 'selfsigned', 'index.js');
  const [wt, cert] = await Promise.all([
    import(pathToFileURL(wtPath).href),
    import(pathToFileURL(certPath).href),
  ]);
  return { wt, selfsigned: cert.default ?? cert };
}

async function certificado(nome) {
  const notBeforeDate = new Date(Date.now() - 60_000);
  const notAfterDate = new Date(notBeforeDate);
  notAfterDate.setDate(notAfterDate.getDate() + 12);
  const pair = await selfsigned.generate([{ name: 'commonName', value: nome }], {
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
  const hash = createHash('sha256').update(new X509Certificate(pair.cert).raw).digest();
  const certPath = join(temp, `${nome}.crt`);
  const keyPath = join(temp, `${nome}.key`);
  await Promise.all([writeFile(certPath, pair.cert), writeFile(keyPath, pair.private)]);
  return { ...pair, hash, certPath, keyPath };
}

function criarCliente(url, hash, cenario) {
  const finalUrl = cenario ? urlAutenticada(url) : url;
  const entrada = cenario ? registrarOrigin(finalUrl, cenario) : null;
  const client = new addon.WebTransport(finalUrl, {
    allowPooling: false,
    serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
  });
  return { client, entrada };
}

async function abrirCliente(url, hash, cenario) {
  const { client, entrada } = criarCliente(url, hash, cenario);
  try {
    await prazo(client.ready, 10_000, 'handshake real');
    return client;
  } finally {
    removerOriginPendente(entrada);
  }
}

async function observarRolloverControlado(overlap) {
  const currentB64 = current.hash.toString('base64');
  const nextB64 = next.hash.toString('base64');
  const overlapAt = Date.now();
  const ambos = [currentB64, nextB64];
  const clienteCurrent = await abrirCliente(
    overlap.body.webtransport.url,
    current.hash,
    'rollover-current-inicial',
  );
  clienteCurrent.close();

  await vi.advanceTimersByTimeAsync(2999);
  const antesDaTroca = {
    ...(await lerCapability()),
    at: Date.now(),
    updates: updates.length,
  };
  const clienteCurrent2999 = await abrirCliente(
    antesDaTroca.body.webtransport.url,
    current.hash,
    'rollover-current-2999',
  );
  clienteCurrent2999.close();

  await vi.advanceTimersByTimeAsync(1);
  const bordaTroca = { ...(await lerCapability()), at: Date.now(), updates: updates.length };
  const limiteTroca = overlapAt + 60_000;
  while (updates.length === 0) {
    if (vi.getTimerCount() === 0) {
      await esperar(1);
      if (vi.getTimerCount() === 0)
        throw new Error('updateCert nao foi agendado depois da borda de 3000 ms');
    }
    await vi.advanceTimersToNextTimerAsync();
    if (Date.now() > limiteTroca)
      throw new Error('updateCert excedeu 60000 ms no relogio controlado');
  }
  const updateAt = updates[0].at;
  const durante = { ...(await lerCapability()), at: Date.now() };
  const clienteNextDurante = await abrirCliente(
    durante.body.webtransport.url,
    next.hash,
    'rollover-next-durante',
  );
  clienteNextDurante.close();

  await vi.advanceTimersByTimeAsync(2999);
  const fimDoOverlap = { ...(await lerCapability()), at: Date.now() };
  const clienteNext2999 = await abrirCliente(
    fimDoOverlap.body.webtransport.url,
    next.hash,
    'rollover-next-2999',
  );
  clienteNext2999.close();

  await vi.advanceTimersByTimeAsync(1);
  const bordaFinal = { ...(await lerCapability()), at: Date.now() };
  let final = bordaFinal;
  const limiteFinal = updateAt + 60_000;
  while (JSON.stringify(final.body?.webtransport?.hashes) !== JSON.stringify([nextB64])) {
    if (vi.getTimerCount() === 0) {
      await esperar(1);
      if (vi.getTimerCount() === 0)
        throw new Error('retirada do current nao foi agendada depois da borda de 3000 ms');
    }
    await vi.advanceTimersToNextTimerAsync();
    if (Date.now() > limiteFinal)
      throw new Error('retirada do current excedeu 60000 ms no relogio controlado');
    final = { ...(await lerCapability()), at: Date.now() };
  }
  const clienteNextFinal = await abrirCliente(
    final.body.webtransport.url,
    next.hash,
    'rollover-next-final',
  );
  clienteNextFinal.close();

  const currentFinalCriado = criarCliente(
    final.body.webtransport.url,
    current.hash,
    'rollover-current-final-rejeitado',
  );
  const clienteCurrentFinal = currentFinalCriado.client;
  void clienteCurrentFinal.closed.catch(() => {});
  const currentFinal = await Promise.race([
    clienteCurrentFinal.ready.then(
      () => 'aceito',
      () => 'rejeitado',
    ),
    esperar(3000).then(() => 'timeout'),
  ]);
  removerOriginPendente(currentFinalCriado.entrada);
  clienteCurrentFinal.close();
  return {
    overlap,
    overlapAt,
    antesDaTroca,
    bordaTroca,
    updateAt,
    durante,
    fimDoOverlap,
    bordaFinal,
    final,
    currentFinal,
    ambos,
  };
}

async function executarRolloverAntesDosTestes() {
  try {
    if (!captured || !preReadyQuery)
      throw new Error('servidor nao criou o listener WebTransport configurado');
    const publicacao = await prazo(
      preReadyQuery,
      5000,
      'capability WT nunca publicada depois de listener.ready',
    );
    const resultado = await observarRolloverControlado(publicacao.firstPublication);
    return { ok: true, publicacao, resultado };
  } catch (error) {
    return { ok: false, error };
  } finally {
    preReadyStop = true;
    if (fakeTimersActive) {
      vi.useRealTimers();
      fakeTimersActive = false;
    }
  }
}

suite('binding QUIC real e streams unidirecionais', () => {
  beforeAll(async () => {
    const carregado = await importarLive();
    addon = carregado.wt;
    selfsigned = carregado.selfsigned;
    temp = await mkdtemp(join(tmpdir(), 'discord-locutor-wt-cert-'));
    [current, next] = await Promise.all([certificado('current'), certificado('next')]);
    await prazo(addon.quicheLoaded, 10_000, 'binding quiche');
    const [httpPort, wtPort] = await Promise.all([portaTcpLivre(), portaUdpLivre()]);
    httpUrl = `http://127.0.0.1:${httpPort}`;
    wtOrigin = `https://127.0.0.1:${wtPort}`;

    originalStart = addon.Http3Server.prototype.startServer;
    originalUpdate = addon.Http3Server.prototype.updateCert;
    instalarSeamOrigin();
    addon.Http3Server.prototype.startServer = function (...args) {
      if (ignorarProbeCru) return originalStart.apply(this, args);
      captured = this;
      readyAt = null;
      this.ready.then(
        () => (readyAt = agora()),
        () => (readyAt = agora()),
      );
      preReadyQuery = (async () => {
        const observations = [];
        let unavailableBeforeReady = 0;
        while (!preReadyStop) {
          const requestedAt = agora();
          const readyAtRequest = readyAt;
          try {
            const snapshot = await lerCapability();
            const observation = {
              ...snapshot,
              requestedAt,
              completedAt: agora(),
              readyAtRequest,
            };
            observations.push(observation);
            if (snapshot.body?.webtransport)
              return { observations, unavailableBeforeReady, firstPublication: observation };
          } catch {
            if (readyAt === null) unavailableBeforeReady++;
          }
          await esperar(1);
        }
        return { observations, unavailableBeforeReady, firstPublication: null };
      })();
      return originalStart.apply(this, args);
    };
    addon.Http3Server.prototype.updateCert = function (...args) {
      updates.push({ at: Date.now(), args });
      return originalUpdate.apply(this, args);
    };

    Object.assign(process.env, {
      PORT: String(httpPort),
      NODE_ENV: 'test',
      NODE_ORIGINS: wtOrigin,
      WEBTRANSPORT_ENABLED: 'true',
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: current.certPath,
      WEBTRANSPORT_KEY_PATH: current.keyPath,
      WEBTRANSPORT_NEXT_CERT_PATH: next.certPath,
      WEBTRANSPORT_NEXT_KEY_PATH: next.keyPath,
      WEBTRANSPORT_HOST: '127.0.0.1',
      WEBTRANSPORT_PORT: String(wtPort),
      WEBTRANSPORT_PUBLIC_URL: `${wtOrigin}/wt`,
    });

    ({ createTransport } = await carregarFactory());
    ({ signToken } = await import('./tokens.js'));
    R = await import('./rooms.js');
    vi.useFakeTimers({
      toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'],
    });
    fakeTimersActive = true;
    ({ server: http, wss } = await import('./index.js'));
    if (!http.listening) await new Promise((resolve) => http.once('listening', resolve));
    const rolloverRoom = R.createRoom({
      instance: `binding-rollover-${Date.now()}`,
      name: 'Binding rollover',
      ownerId: 'owner-rollover',
      ownerName: 'Owner rollover',
    }).room;
    rolloverToken = token(rolloverRoom, 'viewer', 'viewer-rollover');
    rolloverProbe = await executarRolloverAntesDosTestes();
  }, 45_000);

  afterAll(async () => {
    preReadyStop = true;
    if (fakeTimersActive) {
      vi.useRealTimers();
      fakeTimersActive = false;
    }
    await Promise.all(
      sockets.map(async (socket) => {
        if (socket.readyState === (socket.CLOSED ?? 3)) return;
        const fechado = new Promise((resolve) => {
          if (typeof socket.once === 'function') socket.once('close', resolve);
          else socket.addEventListener?.('close', resolve, { once: true });
        });
        if (typeof socket.terminate === 'function') socket.terminate();
        else socket.close?.();
        await Promise.race([fechado, esperar(1000)]);
      }),
    );
    if (captured && !listenerStopIssued) {
      listenerStopIssued = true;
      captured.stopServer();
      await prazo(captured.closed, 5000, 'cleanup listener').catch(() => {});
    }
    if (originalStart) addon.Http3Server.prototype.startServer = originalStart;
    if (originalUpdate) addon.Http3Server.prototype.updateCert = originalUpdate;
    if (originalSetRequestCallback)
      addon.Http3Server.prototype.setRequestCallback = originalSetRequestCallback;
    if (wss)
      await prazo(new Promise((resolve) => wss.close(resolve)), 2000, 'cleanup wss').catch(
        () => {},
      );
    if (http?.listening) {
      http.closeAllConnections?.();
      await prazo(new Promise((resolve) => http.close(resolve)), 2000, 'cleanup http').catch(
        () => {},
      );
    }
    if (temp) await rm(temp, { recursive: true, force: true });
  });

  it('controle cru usa UDP/certificado reais, uni nos dois sentidos e fecha 1102 streams', async () => {
    const server = new addon.Http3Server({
      port: 0,
      host: '127.0.0.1',
      secret: randomBytes(32).toString('hex'),
      cert: current.cert,
      privKey: current.private,
    });
    const sessions = server.sessionStream('/independent').getReader();
    ignorarProbeCru = true;
    try {
      server.startServer();
    } finally {
      ignorarProbeCru = false;
    }
    await prazo(server.ready, 10_000, 'listener UDP');
    const address = server.address();
    expect(address?.port).toBeGreaterThan(0);

    const aceito = (async () => {
      const { value: session } = await sessions.read();
      await session.ready;
      const incoming = session.incomingUnidirectionalStreams.getReader();
      const concluidos = [];
      let ativos = 0;
      let criados = 0;
      const tarefas = [];
      for (let i = 0; i < 1102; i++) {
        const { value: stream } = await incoming.read();
        criados++;
        ativos++;
        tarefas.push(
          (async () => {
            const reader = stream.getReader();
            const bytes = [];
            for (;;) {
              const parte = await reader.read();
              if (parte.done) break;
              bytes.push(...parte.value);
            }
            concluidos.push(bytes[0]);
            ativos--;
          })(),
        );
      }
      const volta = await session.createUnidirectionalStream();
      const writer = volta.getWriter();
      await writer.write(new Uint8Array([201, 202]));
      await writer.close();
      await Promise.all(tarefas);
      return { concluidos, criados, ativos };
    })();

    const client = await abrirCliente(
      `https://127.0.0.1:${address.port}/independent`,
      current.hash,
    );
    const abertos = [];
    for (let i = 0; i < 1100; i++) {
      const stream = await client.createUnidirectionalStream();
      const writer = stream.getWriter();
      await writer.write(new Uint8Array([i % 199]));
      await writer.close();
      abertos.push(writer.closed.catch(() => {}));
    }
    const a = await client.createUnidirectionalStream();
    const wa = a.getWriter();
    await wa.write(new Uint8Array([200]));
    const b = await client.createUnidirectionalStream();
    const wb = b.getWriter();
    await wb.write(new Uint8Array([201]));
    await wb.close();
    await esperar(25);
    await wa.close();

    const volta = await client.incomingUnidirectionalStreams.getReader().read();
    const voltaBytes = await volta.value.getReader().read();
    const medido = await prazo(aceito, 60_000, 'stress unidirecional');
    expect(voltaBytes.value).toEqual(new Uint8Array([201, 202]));
    expect(medido.concluidos.indexOf(201)).toBeLessThan(medido.concluidos.indexOf(200));
    expect(medido.criados).toBe(1102);
    expect(medido.ativos).toBe(0);
    await Promise.all(abertos);

    client.close({ closeCode: 0, reason: 'independent complete' });
    server.stopServer();
    await prazo(server.closed, 10_000, 'listener closed');
  }, 120_000);

  it('mede pre-ready e as duas bordas de 3000 ms do rollover com certificado servido', async () => {
    const health = await fetch(`${httpUrl}/api/health`);
    expect.soft(health.status).toBe(200);
    expect.soft(captured).toBeTruthy();
    expect.soft(rolloverProbe?.ok, rolloverProbe?.error?.message).toBe(true);
    if (!rolloverProbe?.ok) return;

    const { publicacao, resultado } = rolloverProbe;
    expect.soft(readyAt).toEqual(expect.any(Number));
    const respostasConcluidasAntesDeReady = publicacao.observations.filter(
      (observation) => observation.completedAt < readyAt,
    );
    for (const observation of respostasConcluidasAntesDeReady)
      expect.soft(observation.body?.webtransport ?? null).toBeNull();
    expect.soft(publicacao.firstPublication.completedAt).toBeGreaterThanOrEqual(readyAt);

    const ambos = [current.hash.toString('base64'), next.hash.toString('base64')];
    expect.soft(resultado.overlap.body.webtransport.hashes).toEqual(ambos);
    expect.soft(resultado.overlap.body.webtransport.url).not.toMatch(/:0(?:\/|$)/);
    expect.soft(resultado.antesDaTroca.at - resultado.overlapAt).toBe(2999);
    expect.soft(resultado.antesDaTroca.updates).toBe(0);
    expect.soft(resultado.antesDaTroca.body.webtransport.hashes).toEqual(ambos);
    expect.soft(resultado.bordaTroca.at - resultado.overlapAt).toBe(3000);
    expect.soft(resultado.bordaTroca.body.webtransport.hashes).toEqual(ambos);
    expect.soft(resultado.updateAt - resultado.overlapAt).toBeGreaterThanOrEqual(3000);
    expect.soft(resultado.durante.body.webtransport.hashes).toEqual(ambos);
    expect.soft(resultado.fimDoOverlap.at - resultado.updateAt).toBe(2999);
    expect.soft(resultado.fimDoOverlap.body.webtransport.hashes).toEqual(ambos);
    expect.soft(resultado.bordaFinal.at - resultado.updateAt).toBe(3000);
    expect
      .soft([ambos, [next.hash.toString('base64')]])
      .toContainEqual(resultado.bordaFinal.body.webtransport.hashes);
    expect.soft(resultado.final.at - resultado.updateAt).toBeGreaterThanOrEqual(3000);
    expect.soft(resultado.final.body.webtransport.hashes).toEqual([next.hash.toString('base64')]);
    expect.soft(resultado.currentFinal).toBe('rejeitado');
    expect.soft(updates).toHaveLength(1);
    afirmarAddonSemOrigin(0, [
      'rollover-current-inicial',
      'rollover-current-2999',
      'rollover-next-durante',
      'rollover-next-2999',
      'rollover-next-final',
    ]);
  }, 20_000);

  it('atravessa createTransport e adapters reais por 10 s, com wire híbrido e saldo zero', async () => {
    const room = R.createRoom({
      instance: `binding-stress-${Date.now()}`,
      name: 'Binding stress',
      ownerId: 'owner',
      ownerName: 'Owner',
    }).room;
    let liberarA;
    const aLiberado = new Promise((resolve) => (liberarA = resolve));
    let concluirA;
    let falharA;
    const aSettled = new Promise((resolve, reject) => {
      concluirA = resolve;
      falharA = reject;
    });
    let writersAtivos = 0;
    class WtRealComAtrasoEmA extends addon.WebTransport {
      constructor(url, options) {
        registrarOrigin(url, 'logico-broadcaster-stress');
        super(url, options);
      }

      async createUnidirectionalStream() {
        const real = await super.createUnidirectionalStream();
        const writer = real.getWriter();
        const partes = [];
        let settled = false;
        writersAtivos++;
        const encerrar = () => {
          if (settled) return;
          settled = true;
          writersAtivos--;
        };
        const ehA = () => {
          const bytes = Buffer.concat(partes);
          return bytes[bytes.length - 1] === 200;
        };
        return new WritableStream({
          write(chunk) {
            partes.push(Buffer.from(chunk).subarray().slice());
          },
          async close() {
            const bytes = Buffer.concat(partes);
            const atrasoA = bytes[bytes.length - 1] === 200;
            try {
              if (atrasoA) await aLiberado;
              for (const parte of partes) await writer.write(parte);
              await writer.close();
              await writer.closed;
              if (atrasoA) concluirA({ kind: 'closed' });
            } catch (error) {
              if (atrasoA) falharA(error);
              throw error;
            } finally {
              encerrar();
            }
          },
          async abort(reason) {
            const atrasoA = ehA();
            try {
              await writer.abort(reason);
              if (atrasoA) concluirA({ kind: 'aborted' });
            } catch (error) {
              if (atrasoA) falharA(error);
              throw error;
            } finally {
              encerrar();
            }
          },
        });
      }
    }

    const viewer = logico(room, 'viewer', 'viewer-binding');
    const broadcaster = logico(room, 'broadcaster', 'caster-binding', WtRealComAtrasoEmA);
    await Promise.all([aberto(viewer), aberto(broadcaster)]);
    const slot = (await ateJson(broadcaster, (msg) => msg.type === 'slot', 'slot')).slot;
    broadcaster.send(JSON.stringify({ type: 'start' }));
    await ateJson(viewer, (msg) => msg.type === 'stream-start', 'stream-start');
    viewer.send(JSON.stringify({ type: 'watch', slot }));
    await ateJson(broadcaster, (msg) => msg.type === 'need-keyframe', 'need-keyframe');
    broadcaster.send(JSON.stringify({ type: 'config', config: { codec: 'vp8' } }));
    broadcaster.send(JSON.stringify({ type: 'audio-config', config: { codec: 'opus' } }));
    broadcaster.send(quadro(slot, 1, 1, 100, 190));
    await ateBinarios(viewer, 1, 'priming inicial');

    const antesBA = viewer.bin.length;
    broadcaster.send(quadro(slot, 2, 2, 100, 200));
    broadcaster.send(quadro(slot, 1, 3, 100, 201));
    try {
      await ateBinarios(viewer, antesBA + 1, 'keyframe B vence A', 3000);
      expect
        .soft(
          viewer.bin
            .slice(antesBA)
            .map((bytes) => bytes[bytes.length - 1])
            .at(-1),
        )
        .toBe(201);
    } finally {
      liberarA();
    }

    const inicio = Date.now();
    let pico = 0;
    for (let tick = 0; tick < 100; tick++) {
      for (let n = 0; n < 6; n++) {
        broadcaster.send(quadro(slot, 1, 10_000 + tick * 6 + n, 10_417, tick));
        pico = Math.max(pico, broadcaster.bufferedAmount);
      }
      for (let n = 0; n < 5; n++) {
        broadcaster.send(quadro(slot, 3, 20_000 + tick * 5 + n, 320, tick));
        pico = Math.max(pico, broadcaster.bufferedAmount);
      }
      await esperar(Math.max(0, inicio + (tick + 1) * 100 - Date.now()));
    }
    const duracao = Date.now() - inicio;
    const stressRecebido = () =>
      viewer.bin.filter((bytes) => {
        const timestamp = bytes.readDoubleBE(2);
        return (
          (bytes[1] === 1 && timestamp >= 10_000 && timestamp < 11_000) ||
          (bytes[1] === 3 && timestamp >= 20_000 && timestamp < 21_000)
        );
      });
    await prazo(
      (async () => {
        while (stressRecebido().filter((bytes) => bytes[1] === 1).length < 600) await esperar(10);
      })(),
      30_000,
      '600 keyframes confiáveis identificados no consumidor',
    );
    const recebidos = stressRecebido();
    const video = recebidos.filter((bytes) => bytes[1] === 1);
    const audio = recebidos.filter((bytes) => bytes[1] === 3);
    if (broadcaster.transport === 'webtransport' && writersAtivos > 0)
      await prazo(aSettled, 10_000, 'writer real de A nao assentou');
    await prazo(
      (async () => {
        while (
          writersAtivos !== 0 ||
          broadcaster.bufferedAmount !== 0 ||
          viewer.bufferedAmount !== 0
        )
          await esperar(10);
      })(),
      10_000,
      'quiescencia depois do stress e de A',
    );

    const antesRecuperacao = viewer.bin.length;
    broadcaster.send(quadro(slot, 1, 99_999, 100, 202));
    await ateBinarios(viewer, antesRecuperacao + 1, 'controle de recuperacao pos-stress');
    await prazo(
      (async () => {
        while (
          writersAtivos !== 0 ||
          broadcaster.bufferedAmount !== 0 ||
          viewer.bufferedAmount !== 0
        )
          await esperar(10);
      })(),
      10_000,
      'quiescencia final depois da recuperacao',
    );
    const marcadoresAteQuiescencia = viewer.bin
      .slice(antesBA)
      .map((bytes) => bytes[bytes.length - 1]);

    expect.soft(duracao).toBeGreaterThanOrEqual(9900);
    expect.soft(video).toHaveLength(600);
    expect.soft(audio.length).toBeGreaterThanOrEqual(475);
    expect
      .soft(video.reduce((bits, bytes) => bits + (bytes.length - 18) * 8, 0))
      .toBeGreaterThanOrEqual(50_000_000);
    expect
      .soft(audio.reduce((bits, bytes) => bits + (bytes.length - 18) * 8, 0))
      .toBeGreaterThanOrEqual(1_216_000);
    expect.soft(marcadoresAteQuiescencia).toContain(201);
    expect.soft(marcadoresAteQuiescencia).toContain(202);
    expect
      .soft(viewer.bin.slice(antesRecuperacao).map((bytes) => bytes[bytes.length - 1]))
      .toEqual([202]);
    expect.soft(pico).toBeLessThanOrEqual(8 * 1024 * 1024);
    expect.soft(broadcaster.bufferedAmount).toBe(0);
    expect.soft(viewer.bufferedAmount).toBe(0);
    expect.soft(broadcaster.wireStats?.datagramFragmentsSent).toBeGreaterThan(0);
    expect.soft(viewer.wireStats?.datagramFramesReassembled).toBeGreaterThan(0);
    expect.soft(broadcaster.transport).toBe('webtransport');
    expect.soft(viewer.transport).toBe('webtransport');
    afirmarAddonSemOrigin(0, ['logico-viewer-viewer-binding', 'logico-broadcaster-stress']);
    expect.soft(origensPendentes).toHaveLength(0);
  }, 90_000);

  it('mantem capability entre stopServer e closed e remove somente em listener-lost', async () => {
    expect.soft(captured).toBeTruthy();
    if (!captured) return;
    const antes = await lerCapability();
    expect.soft(antes.body?.webtransport?.hashes).toEqual([next.hash.toString('base64')]);
    let closedAt = null;
    const closedMarcado = captured.closed.then(
      () => (closedAt = agora()),
      () => (closedAt = agora()),
    );
    listenerStopIssued = true;
    captured.stopServer();
    expect.soft(closedAt).toBeNull();
    const duranteClose = [];
    const observarEnquantoPendente = (async () => {
      do {
        const requestAt = agora();
        const snapshot = await lerCapability();
        duranteClose.push({ ...snapshot, requestAt, completedAt: agora() });
      } while (closedAt === null);
    })();
    await prazo(
      Promise.all([observarEnquantoPendente, closedMarcado]),
      10_000,
      'listener-lost real',
    );
    expect.soft(duranteClose.length).toBeGreaterThan(0);
    for (const observation of duranteClose) {
      expect.soft(observation.requestAt).toBeLessThanOrEqual(closedAt);
      const hashesDurante = observation.body?.webtransport?.hashes ?? null;
      if (observation.completedAt < closedAt)
        expect.soft(hashesDurante).toEqual([next.hash.toString('base64')]);
      else expect.soft([[next.hash.toString('base64')], null]).toContainEqual(hashesDurante);
    }

    const depoisAt = agora();
    const perdido = await lerCapability();
    expect.soft(depoisAt).toBeGreaterThanOrEqual(closedAt);
    expect.soft(perdido.body?.webtransport ?? null).toBeNull();
  }, 15_000);
});

describe('inventario do path live', () => {
  it('na suite comum nao resolve addon por caminho externo', () => {
    if (!live) expect(process.env.WT_LIVE_NODE_MODULES).toBeUndefined();
    else expect(liveModules).toBeTruthy();
  });
});
