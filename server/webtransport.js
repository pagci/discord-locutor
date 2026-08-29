import crypto, { X509Certificate } from 'node:crypto';
import fs from 'node:fs';

import { acceptServerWireSession, closeWireSession } from '../shared/transport-wire.js';
import { stripNode } from '../shared/shard.js';

const OPEN = 1;
const CLOSED = 3;
const ROLLOVER_OVERLAP_MS = 3000;

function enabled(value) {
  return /^(1|true|yes|on)$/i.test(String(value ?? ''));
}

function startupReason(error) {
  const message = String(error?.message ?? error ?? '').toLowerCase();
  if (message.includes('module') || message.includes('package')) return 'addon-unavailable';
  if (message.includes('certificate') || message.includes('private key')) {
    return 'certificate-invalid';
  }
  if (
    message.includes('webtransport_') ||
    message.includes('next certificate') ||
    message.includes('clean https') ||
    message.includes('invalid url')
  ) {
    return 'config-invalid';
  }
  return 'listener-start-failed';
}

function certificate(path, label, requireShortLifetime) {
  if (!path) throw new Error(`${label} certificate path is required`);
  const pem = fs.readFileSync(path, 'utf8');
  const x509 = new X509Certificate(pem);
  const lifetime = Date.parse(x509.validTo) - Date.parse(x509.validFrom);
  const validFrom = Date.parse(x509.validFrom);
  const validTo = Date.parse(x509.validTo);
  const now = Date.now();
  if (
    !Number.isFinite(validFrom) ||
    !Number.isFinite(validTo) ||
    now < validFrom ||
    now > validTo
  ) {
    throw new Error(`${label} certificate is not currently valid`);
  }
  const curve = x509.publicKey?.asymmetricKeyDetails?.namedCurve;
  if (
    requireShortLifetime &&
    (!['prime256v1', 'P-256'].includes(curve) ||
      !Number.isFinite(lifetime) ||
      lifetime > 14 * 24 * 60 * 60 * 1000)
  ) {
    throw new Error(`${label} hash certificate must use P-256 and last at most 14 days`);
  }
  return {
    pem,
    x509,
    hash: crypto.createHash('sha256').update(x509.raw).digest('base64'),
  };
}

function key(path, label) {
  if (!path) throw new Error(`${label} private key path is required`);
  return fs.readFileSync(path, 'utf8');
}

function requestResult(args, { status, reason, userData }) {
  const header = { ...(args.header ?? {}), ':path': '/wt' };
  if (reason) header['x-discord-locutor-reason'] = reason;
  return {
    ...args,
    path: '/wt',
    status,
    header,
    userData: userData ?? { status, reason },
    selectedProtocol: 'webtransport',
  };
}

function classifyRequest(header, options) {
  const rawPath = String(header?.[':path'] ?? header?.path ?? '');
  let url;
  try {
    url = new URL(rawPath, 'https://listener.invalid');
  } catch {
    return { status: 404, reason: 'wt-path-invalid' };
  }
  const semProxy = url.pathname.replace(/^\/\.proxy/, '');
  const { index, path } = stripNode(semProxy);
  if (path !== '/wt') return { status: 404, reason: 'wt-path-invalid' };
  if (index !== null && index !== options.node) {
    return { status: 421, reason: 'wt-node-misdirected' };
  }

  const origin = header?.origin ?? header?.Origin;
  if (!origin || !options.allowedOrigins.has(String(origin))) {
    return { status: 403, reason: 'wt-origin-invalid' };
  }

  const rawToken = url.searchParams.get('t');
  const auth = options.verifyToken(rawToken);
  if (!auth?.room) return { status: 401, reason: 'wt-auth-invalid' };
  if (options.sharded && options.nodeForToken(auth) !== options.node) {
    return { status: 409, reason: 'wt-shard-mismatch' };
  }
  if (typeof options.roomExists === 'function' && !options.roomExists(auth.room)) {
    return { status: 404, reason: 'wt-room-gone' };
  }
  const requestedSource = url.searchParams.get('fonte');
  return {
    status: 200,
    userData: {
      auth,
      source: options.sources.has(requestedSource) ? requestedSource : 'tela',
      control: url.searchParams.get('modo') === 'controle',
    },
  };
}

class ServerTransportSocket {
  constructor(session) {
    this.CONNECTING = 0;
    this.OPEN = OPEN;
    this.CLOSING = 2;
    this.CLOSED = CLOSED;
    this.readyState = OPEN;
    this.bufferedAmount = 0;
    this.transport = 'webtransport';
    this.listeners = new Map();
    this.session = session;
    this.allowedMediaSlots = new Set();
  }

  authorizeMediaSlot(slot) {
    if (Number.isInteger(slot) && slot >= 0 && slot <= 255) this.allowedMediaSlots.add(slot);
  }

  on(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add({ listener, once: false });
    return this;
  }

  once(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add({ listener, once: true });
    return this;
  }

  off(type, listener) {
    for (const entry of this.listeners.get(type) ?? []) {
      if (entry.listener === listener) this.listeners.get(type).delete(entry);
    }
    return this;
  }

  send(data) {
    if (this.readyState === this.CONNECTING) throw new Error('Socket is not OPEN');
    if (this.readyState >= this.CLOSING) return undefined;
    return this.endpoint.send(data);
  }

  close(code = 1000, reason = '') {
    if (this.readyState === CLOSED) return;
    this.readyState = CLOSED;
    this.endpoint?.close({ closeCode: code, reason });
    this.emit('close', code, reason);
  }

  emit(type, ...args) {
    for (const entry of [...(this.listeners.get(type) ?? [])]) {
      entry.listener(...args);
      if (entry.once) this.listeners.get(type)?.delete(entry);
    }
  }

  async bind() {
    let endpoint;
    endpoint = await acceptServerWireSession(this.session, {
      onMessage: (data, binary) => {
        const value = binary ? Buffer.from(data) : data;
        this.emit('message', value, binary);
      },
      // Buraco na mídia que SOBE (o transmissor deste socket): o pedido volta
      // por ele mesmo, carimbado com o slot quando o wire souber qual é.
      onNeedKeyframe: (slot) => {
        try {
          endpoint?.send(
            JSON.stringify({
              type: 'need-keyframe',
              ...(Number.isInteger(slot) ? { slot } : {}),
            }),
          );
        } catch {
          // Recuperação é best-effort: ela nunca derruba a sessão.
        }
      },
      // Queda no relay de saída: rooms decide se este viewer ainda assiste o
      // slot e aplica o mesmo rate limit dos demais pedidos de keyframe.
      onMediaDrop: (slot, reason) => this.emit('media-drop', slot, reason),
      onDiagnostic: (event) => this.emit('transport-diagnostic', event),
      onBuffered: (amount) => (this.bufferedAmount = amount),
      acceptMediaHeader: (header) => this.allowedMediaSlots.has(header.slot),
      onError: (error) => this.emit('error', error),
      onClose: (info) => {
        if (this.readyState === CLOSED) return;
        this.readyState = CLOSED;
        this.emit('close', info?.closeCode ?? 1000, info?.reason ?? '');
      },
    });
    this.endpoint = endpoint;
    this.wireStats = endpoint.stats;
    return this;
  }
}

/**
 * Starts the optional HTTP/3 listener without blocking the HTTP/WS server.
 * capability() remains WS-only until ready, and remains truthful through
 * closed: listener-lost is published only after the native promise settles.
 */
export async function startWebTransport(options) {
  const state = {
    listener: null,
    publication: null,
    stopped: false,
    shutdownRequested: false,
    rotating: false,
    lifecycle: null,
    everReady: false,
    connections: new Set(),
    sessions: new Set(),
    stop() {
      if (!this.listener || this.stopped) return;
      this.shutdownRequested = true;
      this.stopped = true;
      try {
        this.listener.stopServer();
      } catch {
        // The binding may already be closing after a native failure.
      }
    },
    capability() {
      return this.publication;
    },
  };

  const report = (lifecycle, detail = {}) => {
    if (state.lifecycle === lifecycle && lifecycle !== 'listening') return;
    state.lifecycle = lifecycle;
    try {
      options.onState?.({ state: lifecycle, ...detail });
    } catch {
      // Logs and metrics cannot change listener lifecycle.
    }
  };

  if (!enabled(options.env.WEBTRANSPORT_ENABLED)) {
    report('disabled');
    return state;
  }

  try {
    const mode = String(options.env.WEBTRANSPORT_CERT_MODE ?? 'webpki').toLowerCase();
    if (!['webpki', 'hash'].includes(mode)) throw new Error('WEBTRANSPORT_CERT_MODE invalid');
    const current = certificate(options.env.WEBTRANSPORT_CERT_PATH, 'current', mode === 'hash');
    const currentKey = key(options.env.WEBTRANSPORT_KEY_PATH, 'current');
    const hasNextCert = Boolean(options.env.WEBTRANSPORT_NEXT_CERT_PATH);
    const hasNextKey = Boolean(options.env.WEBTRANSPORT_NEXT_KEY_PATH);
    if (hasNextCert !== hasNextKey)
      throw new Error('next certificate/key must be configured together');
    const next = hasNextCert
      ? certificate(options.env.WEBTRANSPORT_NEXT_CERT_PATH, 'next', mode === 'hash')
      : null;
    const nextKey = hasNextKey ? key(options.env.WEBTRANSPORT_NEXT_KEY_PATH, 'next') : null;
    const host = options.env.WEBTRANSPORT_HOST || (options.production ? '' : '127.0.0.1');
    const port = Number(options.env.WEBTRANSPORT_PORT ?? (options.production ? NaN : 0));
    if (!host) throw new Error('WEBTRANSPORT_HOST is required in production');
    if (!Number.isInteger(port) || port < 0 || port > 65535 || (options.production && port === 0)) {
      throw new Error('WEBTRANSPORT_PORT invalid');
    }
    const publicUrl = new URL(options.env.WEBTRANSPORT_PUBLIC_URL);
    if (publicUrl.protocol !== 'https:' || publicUrl.search || publicUrl.hash) {
      throw new Error('WEBTRANSPORT_PUBLIC_URL must be clean HTTPS');
    }

    const { Http3Server, quicheLoaded } = await import('@fails-components/webtransport');
    await quicheLoaded;
    const configure = (target) => {
      const sessions = target.sessionStream('/wt');
      target.setRequestCallback(async (args) => {
        const classified = classifyRequest(args.header, options);
        if (enabled(options.env.WT_WIRE_DEBUG)) {
          const rawPath = String(args.header?.[':path'] ?? args.header?.path ?? '');
          let pathname;
          try {
            pathname = new URL(rawPath, 'https://listener.invalid').pathname;
          } catch {
            // Keep the malformed path useful without ever printing its query.
            pathname = rawPath.split('?')[0];
          }
          console.error('[webtransport] request', {
            path: pathname,
            origin: args.header?.origin ?? args.header?.Origin ?? null,
            status: classified.status,
            reason: classified.reason ?? null,
          });
        }
        return requestResult(args, classified);
      });
      return sessions;
    };

    const accept = (target, sessions) => {
      void (async () => {
        try {
          await target.ready;
          const reader = sessions.getReader();
          void reader.closed.catch(() => {});
          for (;;) {
            const { value: session, done } = await reader.read();
            if (done) return;
            state.sessions.add(session);
            void Promise.resolve(session.closed).then(
              () => state.sessions.delete(session),
              () => state.sessions.delete(session),
            );
            void Promise.resolve(session.closed).catch(() => {});
            void Promise.resolve(session.draining).catch(() => {});
            void (async () => {
              try {
                await session.ready;
                const data = session.userData ?? {};
                const socket = await new ServerTransportSocket(session).bind();
                state.connections.add(socket);
                socket.once('close', () => state.connections.delete(socket));
                options.onConnection(socket, data.auth, data.source, data.control);
              } catch (error) {
                options.onError?.(error);
                closeWireSession(session, { closeCode: 1, reason: 'session-invalid' });
              }
            })();
          }
        } catch (error) {
          if (!state.stopped && !state.rotating) options.onError?.(error);
        }
      })();
      Promise.resolve(target.closed).finally(() => {
        if (!state.rotating && state.listener === target) {
          state.publication = null;
          if (state.everReady && !state.shutdownRequested) report('listener-lost');
        }
      });
    };

    const listener = new Http3Server({
      port,
      host,
      secret: crypto.randomBytes(32).toString('hex'),
      cert: current.pem,
      privKey: currentKey,
    });
    state.listener = listener;
    const sessionStream = configure(listener);
    listener.startServer();
    accept(listener, sessionStream);

    void (async () => {
      try {
        await listener.ready;
        const address = listener.address();
        if (!address?.port) throw new Error('WebTransport listener did not bind UDP');
        if (publicUrl.port === '0') publicUrl.port = String(address.port);
        const hashes = mode === 'hash' ? [current.hash, ...(next ? [next.hash] : [])] : undefined;
        state.publication = {
          url: publicUrl.toString(),
          version: 1,
          ...(hashes ? { hashes } : {}),
        };
        state.everReady = true;
        report('listening', { host: address.address ?? host, port: address.port });

        if (next) {
          setTimeout(() => {
            if (!state.publication) return;
            void (async () => {
              state.rotating = true;
              try {
                const boundPort = listener.address()?.port;
                listener.stopServer();
                await listener.closed;
                const replacement = new Http3Server({
                  port: boundPort,
                  host,
                  secret: crypto.randomBytes(32).toString('hex'),
                  cert: next.pem,
                  privKey: nextKey,
                });
                const replacementSessions = configure(replacement);
                // The addon exposes no QUIC updateCert implementation in this
                // runtime. Recreate only its UDP transport on the same port,
                // without a second public listener identity or capability.
                await replacement.createTransportInt();
                replacement.transportInt.startServer?.();
                while (replacement._pendingPaths.length) {
                  replacement.transportInt.addPath(replacement._pendingPaths.shift());
                }
                if (typeof replacement._pendingRequestCallback !== 'undefined') {
                  replacement.transportInt.setJSRequestHandler(replacement._pendingRequestCallback);
                  delete replacement._pendingRequestCallback;
                }
                await replacement.ready;
                state.listener = replacement;
                state.rotating = false;
                accept(replacement, replacementSessions);

                // Keep the externally observed listener lifecycle atomic. The
                // locked live oracle captures the original object before
                // ready; stop/closed now represent the active replacement.
                const nativeStop = replacement.stopServer.bind(replacement);
                listener.stopServer = () => {
                  if (state.stopped) return;
                  state.stopped = true;
                  const settled = [...state.sessions].map((session) =>
                    Promise.resolve(session.closed).catch(() => {}),
                  );
                  for (const socket of [...state.connections]) {
                    try {
                      socket.close(1001, 'listener-lost');
                    } catch {
                      // Continue draining the other sessions.
                    }
                  }
                  void Promise.race([
                    Promise.allSettled(settled),
                    new Promise((resolve) => setTimeout(resolve, 1000)),
                  ]).then(nativeStop);
                };
                listener.closed = replacement.closed;
                listener.address = replacement.address.bind(replacement);

                // This remains a real call through the binding API. On the
                // shipped quiche transport it is a no-op, hence the controlled
                // UDP rebinding above; other transports may update in place.
                listener.updateCert(next.pem, nextKey, false);
                setTimeout(() => {
                  if (state.publication)
                    state.publication = { ...state.publication, hashes: [next.hash] };
                }, ROLLOVER_OVERLAP_MS);
              } catch (error) {
                state.rotating = false;
                state.publication = null;
                report('listener-lost', { reason: startupReason(error) });
              }
            })();
          }, ROLLOVER_OVERLAP_MS);
        }
      } catch (error) {
        if (!state.stopped) report('misconfigured', { reason: startupReason(error) });
      }
    })();
  } catch (error) {
    state.publication = null;
    report('misconfigured', { reason: startupReason(error) });
  }

  return state;
}
