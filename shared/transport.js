import { closeWireSession, openClientWireSession, invalidState } from './transport-wire.js';
import { nodeFor, shardKey } from './shard.js';

const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;
const MAX_PENDING_CONTROL_ITEMS = 64;
const MAX_PENDING_CONTROL_BYTES = 512 * 1024;
const transportEncoder = new TextEncoder();

function eventOf(type, init = {}) {
  if (typeof Event === 'function') return Object.assign(new Event(type), init);
  return { type, ...init };
}

function bytesFromBase64(value) {
  if (typeof globalThis.Buffer !== 'undefined') {
    return new Uint8Array(globalThis.Buffer.from(value, 'base64'));
  }
  const text = atob(value);
  return Uint8Array.from(text, (character) => character.charCodeAt(0));
}

function expectedNode(url) {
  const match = url.pathname.match(/(?:^|\/)n(\d+)(?:\/|$)/);
  return match ? Number(match[1]) : null;
}

function tokenPayload(value) {
  const body = String(value ?? '').split('.')[0];
  if (!body || !String(value).includes('.')) return null;
  try {
    const normalized = body.replace(/-/g, '+').replace(/_/g, '/');
    const json =
      typeof globalThis.Buffer !== 'undefined'
        ? globalThis.Buffer.from(normalized, 'base64').toString('utf8')
        : decodeURIComponent(
            Array.from(
              atob(normalized),
              (character) => `%${character.charCodeAt(0).toString(16).padStart(2, '0')}`,
            ).join(''),
          );
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function preflightCapability(
  capability,
  advertised,
  wsUrl,
  _capabilityUrl,
  _explicitCapabilityUrl,
) {
  const ws = new URL(wsUrl);
  const base = new URL(advertised.url);
  const shards = Number(capability.shards);
  const authoritative = Number.isInteger(shards) && shards > 0;
  const explicitWsNode = expectedNode(ws);
  if (explicitWsNode !== null && Number(capability.node) !== explicitWsNode) {
    throw Object.assign(new Error('webtransport-unavailable'), { unavailable: true });
  }
  // HTTP capability discovery and the UDP/QUIC listener may deliberately use
  // different hosts. Ownership is established by the authoritative node and
  // shard metadata plus the clean /wt path, not by hostname equality.
  const semProxy = base.pathname.replace(/^\/\.proxy/, '');
  const match = semProxy.match(/^\/n(\d+)(\/.*)$/);
  const pathNode = match ? Number(match[1]) : null;
  const path = match ? match[2] : semProxy;
  if (path !== '/wt') throw Object.assign(new Error('wt-path-invalid'), { status: 404 });
  if (pathNode !== null && pathNode !== Number(capability.node)) {
    throw Object.assign(new Error('wt-node-misdirected'), { status: 421 });
  }
  if (authoritative) {
    const token = ws.searchParams.get('t');
    const claims = tokenPayload(token);
    if (!claims?.room) throw Object.assign(new Error('wt-auth-invalid'), { status: 401 });
    if (shards > 1 && nodeFor(shardKey(claims), shards) !== Number(capability.node)) {
      throw Object.assign(new Error('wt-shard-mismatch'), { status: 409 });
    }
  }
  return base;
}

function capabilityUrlFor(wsUrl) {
  const url = new URL(wsUrl);
  url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
  url.username = '';
  url.password = '';
  url.search = '';
  url.hash = '';
  url.pathname = url.pathname.replace(/\/ws$/, '/api/transports');
  return url.toString();
}

function connectUrlFor(base, wsUrl) {
  const url = new URL(base);
  url.search = '';
  url.hash = '';
  const source = new URL(wsUrl);
  for (const name of ['t', 'fonte', 'modo']) {
    if (source.searchParams.has(name)) url.searchParams.set(name, source.searchParams.get(name));
  }
  return url.toString();
}

function reasonFromStatus(status, transport) {
  if (transport === 'ws' && status === 403) return 'ws-origin-invalid';
  if (status === 404) return 'wt-path-invalid';
  if (status === 401) return 'wt-auth-invalid';
  if (status === 403) return 'wt-origin-invalid';
  if (status === 409) return 'wt-shard-mismatch';
  if (status === 421) return 'wt-node-misdirected';
  return `${transport}-connect-failed`;
}

function statusFromError(error) {
  const values = [error?.status, error?.statusCode, error?.code, error?.source?.status];
  for (const value of values) {
    const parsed = Number(value);
    if (parsed >= 400 && parsed <= 599) return parsed;
  }
  const match = String(error?.message ?? error ?? '').match(/\b(4\d\d|5\d\d)\b/);
  return match ? Number(match[1]) : undefined;
}

class LogicalSocket {
  constructor({ onDiagnostic, onTransport } = {}) {
    this.CONNECTING = CONNECTING;
    this.OPEN = OPEN;
    this.CLOSING = CLOSING;
    this.CLOSED = CLOSED;
    this._readyState = CONNECTING;
    this.bufferedAmount = 0;
    this.binaryType = 'arraybuffer';
    this.transport = null;
    this.listeners = new Map();
    this.onDiagnostic = onDiagnostic;
    this.onTransport = onTransport;
    this.fallbackReason = null;
    this.webtransportAttempted = false;
    this.wireStats = null;
  }

  get readyState() {
    if (this._readyState === OPEN && this.transport === 'websocket' && this.delegate) {
      return this.delegate.readyState;
    }
    return this._readyState;
  }

  set readyState(value) {
    this._readyState = value;
  }

  addEventListener(type, listener, options = {}) {
    if (typeof listener !== 'function') return;
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add({ listener, once: Boolean(options?.once) });
  }

  removeEventListener(type, listener) {
    const entries = this.listeners.get(type);
    if (!entries) return;
    for (const entry of entries) if (entry.listener === listener) entries.delete(entry);
  }

  send(data) {
    const state = this.readyState;
    if (state === CONNECTING) throw invalidState();
    if (state >= CLOSING) return undefined;
    if (!this.sender) throw invalidState();
    return this.sender(data);
  }

  close(code = 1000, reason = '') {
    if (this._readyState >= CLOSING) return;
    this._readyState = CLOSING;
    this.cancelNegotiation?.();
    this.closer?.({ closeCode: code, reason });
    this.#closed({ code, reason, wasClean: true });
  }

  diagnostic(event) {
    if (
      event?.transport === 'webtransport' &&
      event.reason !== 'selected' &&
      event.reason !== 'closed'
    ) {
      this.fallbackReason = event.reason;
    }
    try {
      this.onDiagnostic?.(event);
    } catch {
      // Diagnostics must never change transport selection.
    }
  }

  opened(transport, sender, closer) {
    if (this._readyState !== CONNECTING) return false;
    this.transport = transport;
    this.sender = sender;
    this.closer = closer;
    this._readyState = OPEN;
    const selection = {
      transport,
      reason: 'selected',
      attemptedWebTransport: this.webtransportAttempted,
      ...(transport === 'websocket' && this.fallbackReason
        ? { fallbackReason: this.fallbackReason }
        : {}),
    };
    this.diagnostic(selection);
    try {
      this.onTransport?.(selection);
    } catch {
      // Observability must never change the selected transport.
    }
    this.#emit('transport', eventOf('transport', selection));
    this.#emit('open', eventOf('open'));
    return true;
  }

  message(data) {
    if (this._readyState === CLOSED) return;
    this.#emit('message', eventOf('message', { data }));
  }

  error(error) {
    this.#emit(
      'error',
      eventOf('error', { error, message: String(error?.message ?? error ?? '') }),
    );
  }

  closed(info = {}) {
    if (this._readyState === CLOSED) return;
    if (this.transport) this.diagnostic({ transport: this.transport, reason: 'closed' });
    this.#closed({
      code: info.closeCode ?? info.code ?? 1000,
      reason: info.reason ?? '',
      wasClean: (info.closeCode ?? info.code ?? 0) === 0 || (info.closeCode ?? info.code) === 1000,
    });
  }

  #closed(event) {
    if (this._readyState === CLOSED) return;
    this._readyState = CLOSED;
    this.bufferedAmount = 0;
    this.#emit('close', eventOf('close', event));
  }

  #emit(type, event) {
    for (const entry of [...(this.listeners.get(type) ?? [])]) {
      entry.listener.call(this, event);
      if (entry.once) this.listeners.get(type)?.delete(entry);
    }
    this[`on${type}`]?.call(this, event);
  }
}

function attachWebSocket(logical, wsUrl, WebSocketImpl) {
  if (logical.readyState !== CONNECTING) return;
  let ws;
  try {
    ws = new WebSocketImpl(wsUrl);
  } catch (error) {
    logical.diagnostic({ transport: 'websocket', reason: 'ws-connect-failed' });
    logical.error(error);
    logical.closed({ code: 1006, reason: 'ws-connect-failed' });
    return;
  }
  ws.binaryType = 'arraybuffer';
  logical.delegate = ws;
  logical.cancelNegotiation = () => ws.close?.();

  const opened = () => {
    logical.opened(
      'websocket',
      (data) => ws.send(data),
      ({ closeCode, reason }) => ws.close(closeCode, reason),
    );
  };
  const messaged = (event, isBinary) => {
    const data = event?.data ?? event;
    logical.message(isBinary === false && typeof data !== 'string' ? String(data) : data);
  };
  const errored = (event) => logical.error(event?.error ?? event);
  const closed = (event) => logical.closed(event ?? {});
  if (typeof ws.addEventListener === 'function') {
    ws.addEventListener('open', opened);
    ws.addEventListener('message', messaged);
    ws.addEventListener('error', errored);
    ws.addEventListener('close', closed);
  } else {
    ws.on?.('open', opened);
    ws.on?.('message', messaged);
    ws.on?.('error', errored);
    ws.on?.('close', (code, reason) => closed({ code, reason: String(reason ?? '') }));
  }
  ws.on?.('unexpected-response', (_request, response) => {
    const status = Number(response?.statusCode) || undefined;
    response?.resume?.();
    logical.diagnostic({ transport: 'websocket', status, reason: reasonFromStatus(status, 'ws') });
    logical.closed({ code: 1006, reason: reasonFromStatus(status, 'ws') });
  });
}

function abortPromise(signal) {
  if (!signal) return null;
  if (signal.aborted) return Promise.reject(Object.assign(new Error('aborted'), { abort: true }));
  return new Promise((_, reject) => {
    signal.addEventListener(
      'abort',
      () => reject(Object.assign(new Error('aborted'), { abort: true })),
      { once: true },
    );
  });
}

/**
 * Create the one logical transport consumed by viewer, share control and
 * broadcaster roles. WebTransport is opportunistic per connection; WebSocket
 * remains the universal path and is opened at most once, only before OPEN.
 */
export function createTransport(options) {
  const logical = new LogicalSocket(options);
  const wsUrl = String(options?.wsUrl ?? '');
  const WebSocketImpl = 'WebSocket' in (options ?? {}) ? options.WebSocket : globalThis.WebSocket;
  const WebTransportImpl =
    'WebTransport' in (options ?? {}) ? options.WebTransport : globalThis.WebTransport;
  const fetchImpl = 'fetch' in (options ?? {}) ? options.fetch : globalThis.fetch;
  const timeoutMs = Math.max(1, Number(options?.timeoutMs) || 1500);
  const externalSignal = options?.signal;
  let wt = null;
  let wtClosed = false;
  let timer = null;
  let cancelled = false;
  let fallbackStarted = false;

  const closeWt = (info = { closeCode: 0, reason: 'negotiation-ended' }) => {
    if (!wt || wtClosed) return;
    wtClosed = true;
    closeWireSession(wt, info);
  };
  logical.cancelNegotiation = () => {
    cancelled = true;
    clearTimeout(timer);
    closeWt();
  };

  const openFallback = () => {
    if (cancelled || fallbackStarted || logical.readyState !== CONNECTING) return;
    fallbackStarted = true;
    if (WebSocketImpl) attachWebSocket(logical, wsUrl, WebSocketImpl);
    else logical.closed({ code: 1006, reason: logical.fallbackReason ?? 'ws-unavailable' });
  };

  queueMicrotask(async () => {
    if (!WebTransportImpl || !fetchImpl) {
      logical.diagnostic({
        transport: 'webtransport',
        reason: !WebTransportImpl ? 'unsupported' : 'capability-error',
      });
      openFallback();
      return;
    }

    const internalAbort = new AbortController();
    const timedOut = new Promise((_, reject) => {
      timer = setTimeout(() => {
        internalAbort.abort();
        reject(Object.assign(new Error('timeout'), { timeout: true }));
      }, timeoutMs);
    });
    const aborted = abortPromise(externalSignal);

    try {
      const attempt = (async () => {
        const capabilityUrl = options.capabilityUrl ?? capabilityUrlFor(wsUrl);
        const response = await fetchImpl(capabilityUrl, {
          signal: internalAbort.signal,
          cache: 'no-store',
        });
        if (!response?.ok) {
          throw Object.assign(new Error('capability-unavailable'), {
            status: response?.status,
            reason: 'capability-error',
          });
        }
        const capability = await response.json();
        const advertised = capability?.webtransport;
        if (!advertised?.url) {
          throw Object.assign(new Error('not-advertised'), {
            unavailable: true,
            reason: 'not-advertised',
          });
        }
        const base = preflightCapability(
          capability,
          advertised,
          wsUrl,
          capabilityUrl,
          Boolean(options.capabilityUrl),
        );
        if (base.protocol !== 'https:' || base.search || base.hash) {
          throw Object.assign(new Error('webtransport-capability-invalid'), { unavailable: true });
        }
        const hashes = Array.isArray(advertised.hashes)
          ? advertised.hashes.map((value) => ({
              algorithm: 'sha-256',
              value: bytesFromBase64(value),
            }))
          : undefined;
        const wtOptions = {};
        if (hashes?.length) {
          wtOptions.allowPooling = false;
          wtOptions.serverCertificateHashes = hashes;
        }
        logical.webtransportAttempted = true;
        wt = new WebTransportImpl(connectUrlFor(base, wsUrl), wtOptions);
        void Promise.resolve(wt.closed).catch(() => {});
        void Promise.resolve(wt.draining).catch(() => {});
        logical.cancelNegotiation = () => {
          cancelled = true;
          clearTimeout(timer);
          internalAbort.abort();
          closeWt();
        };
        await wt.ready;
        // Timeout/failure may already have committed this logical socket to
        // its single WS fallback even when that WS has not emitted `open` yet.
        // A native implementation is allowed to resolve `ready` after close;
        // that late resource must never win the still-CONNECTING race.
        if (wtClosed || fallbackStarted || cancelled || logical.readyState !== CONNECTING) {
          closeWt();
          return;
        }
        let endpointSelected = false;
        let pendingBuffered = 0;
        const pendingEvents = [];
        let pendingControlItems = 0;
        let pendingControlBytes = 0;
        let pendingOverflow = false;
        const winnerEvent = (type, value) => {
          if (endpointSelected) {
            if (type === 'message') logical.message(value);
            else if (type === 'error') logical.error(value);
            else logical.closed(value);
            return;
          }
          if (pendingOverflow) return;
          if (type === 'message' && typeof value === 'string') {
            const bytes = transportEncoder.encode(value).byteLength;
            if (
              pendingControlItems >= MAX_PENDING_CONTROL_ITEMS ||
              pendingControlBytes + bytes > MAX_PENDING_CONTROL_BYTES
            ) {
              pendingOverflow = true;
              pendingEvents.length = 0;
              return;
            }
            pendingControlItems++;
            pendingControlBytes += bytes;
          }
          pendingEvents.push({ type, value });
        };
        // O endpoint só existe depois do handshake, e o pedido de keyframe só
        // nasce de um buraco de mídia — sempre bem depois dele. A referência
        // vive fora do `await` porque o callback é registrado na criação.
        let wire = null;
        const endpoint = await openClientWireSession(wt, {
          onMessage: (data) => winnerEvent('message', data),
          // Falta de keyframe é um pedido para o PEER, não uma mensagem para a
          // interface. Injetá-lo como se tivesse chegado do servidor fazia a UI
          // reagir a um recado que ninguém mandou, e o transmissor — o único que
          // pode resolver — nunca ficava sabendo.
          onNeedKeyframe: (slot) => {
            try {
              wire?.send(
                JSON.stringify({
                  type: 'need-keyframe',
                  ...(Number.isInteger(slot) ? { slot } : {}),
                }),
              );
            } catch {
              // Recuperação é best-effort: ela nunca derruba o transporte.
            }
          },
          // Se o próprio uplink descartou um delta antes de entregá-lo ao QUIC,
          // o encoder local precisa produzir imediatamente um novo ponto de
          // partida. Isso é um evento local, não feedback fingindo vir do peer.
          onMediaDrop: () =>
            winnerEvent('message', JSON.stringify({ type: 'need-keyframe', local: true })),
          onBuffered: (amount) => {
            if (endpointSelected) logical.bufferedAmount = amount;
            else pendingBuffered = amount;
          },
          onDiagnostic: (event) => logical.diagnostic({ transport: 'webtransport', ...event }),
          onError: (error) => winnerEvent('error', error),
          onClose: (info) => winnerEvent('close', info),
        });
        wire = endpoint;
        logical.wireStats = endpoint.stats;
        if (pendingOverflow) {
          endpoint.close({ closeCode: 1011, reason: 'control-overflow' });
          throw Object.assign(new Error('control-overflow'), { reason: 'control-overflow' });
        }
        if (cancelled || fallbackStarted || wtClosed || logical.readyState !== CONNECTING) {
          endpoint.close();
          return;
        }
        clearTimeout(timer);
        logical.cancelNegotiation = () => endpoint.close();
        if (
          !logical.opened(
            'webtransport',
            (data) => endpoint.send(data),
            (info) => endpoint.close(info),
          )
        ) {
          endpoint.close();
          return;
        }
        endpointSelected = true;
        logical.bufferedAmount = pendingBuffered;
        for (const event of pendingEvents) winnerEvent(event.type, event.value);
      })();
      await Promise.race([attempt, timedOut, ...(aborted ? [aborted] : [])]);
    } catch (error) {
      clearTimeout(timer);
      closeWt();
      if (
        cancelled ||
        error?.abort ||
        externalSignal?.aborted ||
        logical.readyState !== CONNECTING
      ) {
        if (logical.readyState === CONNECTING) logical.closed({ code: 1000, reason: 'aborted' });
        return;
      }
      const status = statusFromError(error);
      const reason = error?.timeout
        ? 'timeout'
        : error?.reason
          ? error.reason
          : error?.unavailable
            ? 'webtransport-unavailable'
            : status
              ? reasonFromStatus(status, 'wt')
              : logical.webtransportAttempted
                ? 'handshake-error'
                : 'capability-error';
      logical.diagnostic({ transport: 'webtransport', ...(status ? { status } : {}), reason });
      openFallback();
    }
  });

  return logical;
}
