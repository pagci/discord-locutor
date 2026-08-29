const MAGIC = 0x44534c32;
const VERSION = 1;
const CONTROL_HEADER = 16;
const MEDIA_HEADER = 24;
const HANDSHAKE = 1;
const CONTROL = 2;
const MEDIA = 3;
const MEDIA_DATAGRAM = 4;
const HANDSHAKE_TEXT = 'discord-locutor-wt/1';
const CAPABILITIES_TYPE = 'wire-capabilities';
const DATAGRAM_MAX_AGE_MS = 250;

const MAX_SEQUENCE = 0xffffffff;
const MAX_CONTROL = 256 * 1024;
const MAX_MEDIA = 4 * 1024 * 1024;
const MAX_CONTROL_ITEMS = 64;
const MAX_CONTROL_QUEUE = 512 * 1024;
const MAX_MEDIA_ITEMS = 128;
const MAX_MEDIA_QUEUE = 8 * 1024 * 1024;
const MAX_MEDIA_WRITERS = 32;
const MAX_INCOMING_MEDIA = 128;
const MAX_PROVISIONAL = 32;
const MAX_BARRIER_ITEMS = 64;
const MAX_BARRIER_BYTES = 8 * 1024 * 1024;
const MAX_LANE_PENDING_ITEMS = 64;
const MAX_LANE_PENDING_BYTES = 4 * 1024 * 1024;
const DATAGRAM_HEADER = 28;
const MAX_DATAGRAM_FRAGMENTS = 256;
const MAX_DATAGRAM_ITEMS = 32;
const MAX_DATAGRAM_QUEUE = 2 * 1024 * 1024;
const MAX_DATAGRAM_ASSEMBLIES = 64;
const MAX_DATAGRAM_ASSEMBLY_BYTES = 8 * 1024 * 1024;

const HEADER_TIMEOUT_MS = 750;
const PAYLOAD_IDLE_MS = 1500;
const FIN_TIMEOUT_MS = 500;
const INCOMING_TOTAL_MS = 3000;
const CREATE_TIMEOUT_MS = 500;
const WRITE_TIMEOUT_MS = 3000;
const BARRIER_TIMEOUT_MS = 1500;
const MEDIA_GAP_MS = 3000;
// Um datagrama pode ultrapassar o stream confiavel que leva o keyframe anterior.
// O ensaio oficial adiciona 200 ms de atraso e um keyframe de ~28 KB ainda
// precisa ser serializado no enlace de 600 kb/s; 150 ms classificava essa
// reordenacao esperada como perda. O teto continua bem abaixo do watchdog de
// midia e reage no mesmo segundo quando o gap e real.
const DATAGRAM_GAP_MS = 350;
const DATAGRAM_HOT_GAP_MS = 150;
const DATAGRAM_ASSEMBLY_MS = 750;
// A ancora de recuperacao (~28 KB no ensaio real) cruza um enlace de 600 kbit/s
// depois de uma fila QUIC ja pressionada. Deltas vencem cedo para preservar
// frescor; a unica ancora que pode reabrir o decoder recebe uma janela limitada
// de dois segundos, ainda dentro do gate rigido e do timeout de repeticao.
const RECOVERY_DATAGRAM_ASSEMBLY_MS = 2000;
const DATAGRAM_STATS_MS = 250;
const RECENT_DATAGRAM_MS = 1500;
const MAX_RECENT_DATAGRAMS = 512;
// Paridade e uma ponte curta ate o proximo keyframe, nao um modo permanente:
// sob congestionamento, prolonga-la aumenta justamente a fila que expira.
// Cinco segundos cobrem a rajada real e a histerese abaixo exige oito amostras
// limpas antes de retirar a protecao.
const FEC_MIN_ACTIVE_MS = 5000;
const FEC_CLEAN_SAMPLES_TO_DISABLE = 8;
const MEDIA_FLAG_RECOVERY = 1;
const DATAGRAM_FLAG_PARITY = 1;
const DATAGRAM_FLAG_AFTER_KEYFRAME = 2;
const DATAGRAM_FLAG_RECOVERY = 4;
const FAILURE_WINDOW_MS = 10_000;
const SERVER_HANDSHAKE_TIMEOUT_MS = 1500;
const SERVER_CLEANUP_LIMIT_MS = 100;

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const locallyClosedSessions = new WeakSet();

export function closeWireSession(session, info = { closeCode: 0, reason: '' }) {
  if (!session || (typeof session !== 'object' && typeof session !== 'function')) return false;
  if (locallyClosedSessions.has(session)) return false;
  locallyClosedSessions.add(session);
  try {
    session.close?.(info);
  } catch {
    // A native peer may already have completed its close path.
  }
  return true;
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

function bytesOf(value) {
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(
      value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
    );
  }
  return encoder.encode(String(value));
}

function concatBytes(left, right) {
  if (!left.byteLength) return right;
  const joined = new Uint8Array(left.byteLength + right.byteLength);
  joined.set(left);
  joined.set(right, left.byteLength);
  return joined;
}

function after(promise, milliseconds, reason) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(reason)), Math.max(0, milliseconds));
    Promise.resolve(promise).then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function frameControl(kind, sequence, payload) {
  const body = bytesOf(payload);
  if (body.byteLength > MAX_CONTROL) throw new RangeError('control-message-too-large');
  const frame = new Uint8Array(CONTROL_HEADER + body.byteLength);
  const view = new DataView(frame.buffer);
  view.setUint32(0, MAGIC);
  view.setUint8(4, VERSION);
  view.setUint8(5, kind);
  view.setUint32(8, body.byteLength);
  view.setUint32(12, sequence);
  frame.set(body, CONTROL_HEADER);
  return frame;
}

function mediaLane(payload) {
  return `${payload[0] ?? 0}:${payload[1] === 3 ? 1 : 0}`;
}

function timestampOf(payload) {
  if (payload.byteLength < 10) return null;
  const timestamp = new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getFloat64(
    2,
  );
  return Number.isFinite(timestamp) ? timestamp : null;
}

function frameMedia(payload, sequence, requiredControlSeq, flags = 0) {
  const body = bytesOf(payload);
  if (body.byteLength > MAX_MEDIA) throw new RangeError('media-message-too-large');
  const slot = body[0] ?? 0;
  const type = body[1] ?? 0;
  const laneClass = type === 3 ? 1 : 0;
  const frame = new Uint8Array(MEDIA_HEADER + body.byteLength);
  const view = new DataView(frame.buffer);
  view.setUint32(0, MAGIC);
  view.setUint8(4, VERSION);
  view.setUint8(5, MEDIA);
  view.setUint8(6, slot);
  view.setUint8(7, laneClass);
  view.setUint32(8, sequence);
  view.setUint32(12, requiredControlSeq);
  view.setUint32(16, body.byteLength);
  view.setUint8(20, type);
  // Only recovery keyframes may overtake a missing sequence. Ordinary
  // keyframes stay ordered so benign QUIC stream reordering loses nothing.
  view.setUint8(21, flags);
  frame.set(body, MEDIA_HEADER);
  return frame;
}

function mediaHeader(bytes) {
  if (bytes.byteLength < MEDIA_HEADER) throw new Error('media-header-incomplete');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = view.getUint32(16);
  const sequence = view.getUint32(8);
  const requiredControlSeq = view.getUint32(12);
  const flags = view.getUint8(21);
  if (
    view.getUint32(0) !== MAGIC ||
    view.getUint8(4) !== VERSION ||
    view.getUint8(5) !== MEDIA ||
    length > MAX_MEDIA ||
    sequence < 1 ||
    sequence > MAX_SEQUENCE ||
    requiredControlSeq > MAX_SEQUENCE ||
    (flags & ~MEDIA_FLAG_RECOVERY) !== 0
  ) {
    throw new Error('media-frame-invalid');
  }
  return {
    length,
    total: MEDIA_HEADER + length,
    slot: view.getUint8(6),
    requiredControlSeq,
    flags,
  };
}

function parseMedia(bytes) {
  const header = mediaHeader(bytes);
  if (bytes.byteLength !== header.total) throw new Error('media-frame-invalid');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const payload = bytes.slice(MEDIA_HEADER);
  const type = view.getUint8(20);
  const laneClass = view.getUint8(7);
  if (
    payload[0] !== view.getUint8(6) ||
    payload[1] !== type ||
    ![1, 2, 3].includes(type) ||
    laneClass !== (type === 3 ? 1 : 0)
  ) {
    throw new Error('media-frame-invalid');
  }
  return {
    slot: view.getUint8(6),
    laneClass,
    sequence: view.getUint32(8),
    requiredControlSeq: view.getUint32(12),
    flags: view.getUint8(21),
    type,
    timestamp: timestampOf(payload),
    payload: payload.buffer,
  };
}

function frameMediaDatagrams(
  payload,
  sequence,
  requiredControlSeq,
  maxDatagramSize,
  withParity = false,
  afterKeyframe = false,
  recovery = false,
) {
  const body = bytesOf(payload);
  const fragmentCapacity = Math.floor(maxDatagramSize) - DATAGRAM_HEADER;
  if (fragmentCapacity < 1) throw new RangeError('datagram-unavailable');
  const fragmentCount = Math.ceil(body.byteLength / fragmentCapacity);
  if (fragmentCount < 1 || fragmentCount > MAX_DATAGRAM_FRAGMENTS) {
    throw new RangeError('datagram-frame-too-fragmented');
  }
  const slot = body[0] ?? 0;
  const type = body[1] ?? 0;
  const laneClass = type === 3 ? 1 : 0;
  const frames = [];
  const chunks = [];
  for (let fragmentIndex = 0; fragmentIndex < fragmentCount; fragmentIndex++) {
    const start = fragmentIndex * fragmentCapacity;
    const chunk = body.slice(start, Math.min(body.byteLength, start + fragmentCapacity));
    const frame = new Uint8Array(DATAGRAM_HEADER + chunk.byteLength);
    const view = new DataView(frame.buffer);
    view.setUint32(0, MAGIC);
    view.setUint8(4, VERSION);
    view.setUint8(5, MEDIA_DATAGRAM);
    view.setUint8(6, slot);
    view.setUint8(7, laneClass);
    view.setUint32(8, sequence);
    view.setUint32(12, requiredControlSeq);
    view.setUint32(16, body.byteLength);
    view.setUint8(20, type);
    view.setUint8(
      21,
      (afterKeyframe ? DATAGRAM_FLAG_AFTER_KEYFRAME : 0) | (recovery ? DATAGRAM_FLAG_RECOVERY : 0),
    );
    view.setUint16(22, fragmentIndex);
    view.setUint16(24, fragmentCount);
    view.setUint16(26, chunk.byteLength);
    frame.set(chunk, DATAGRAM_HEADER);
    // A paridade é a âncora do quadro sob pressão: envie-a antes dos dados.
    // Quando a janela nativa fecha no meio do quadro, deixá-la por último
    // fazia o único fragmento capaz de reparar a perda ser o primeiro recusado.
    // O reassembler aceita paridade-first e só publica após ter dados suficientes.
    frames.push(frame);
    chunks.push(chunk);
  }
  if (withParity && (type === 1 || type === 2)) {
    const parity = new Uint8Array(fragmentCapacity);
    for (const chunk of chunks) {
      for (let index = 0; index < chunk.byteLength; index++) parity[index] ^= chunk[index];
    }
    const frame = new Uint8Array(DATAGRAM_HEADER + parity.byteLength);
    const view = new DataView(frame.buffer);
    view.setUint32(0, MAGIC);
    view.setUint8(4, VERSION);
    view.setUint8(5, MEDIA_DATAGRAM);
    view.setUint8(6, slot);
    view.setUint8(7, laneClass);
    view.setUint32(8, sequence);
    view.setUint32(12, requiredControlSeq);
    view.setUint32(16, body.byteLength);
    view.setUint8(20, type);
    view.setUint8(
      21,
      DATAGRAM_FLAG_PARITY |
        (afterKeyframe ? DATAGRAM_FLAG_AFTER_KEYFRAME : 0) |
        (recovery ? DATAGRAM_FLAG_RECOVERY : 0),
    );
    view.setUint16(22, fragmentCount);
    view.setUint16(24, fragmentCount);
    view.setUint16(26, parity.byteLength);
    frame.set(parity, DATAGRAM_HEADER);
    frames.unshift(frame);
  }
  return frames;
}

function parseMediaDatagram(value) {
  const bytes = bytesOf(value);
  if (bytes.byteLength < DATAGRAM_HEADER) throw new Error('datagram-header-incomplete');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const sequence = view.getUint32(8);
  const requiredControlSeq = view.getUint32(12);
  const totalLength = view.getUint32(16);
  const type = view.getUint8(20);
  const flags = view.getUint8(21);
  const parity = (flags & DATAGRAM_FLAG_PARITY) !== 0;
  const afterKeyframe = (flags & DATAGRAM_FLAG_AFTER_KEYFRAME) !== 0;
  const recovery = (flags & DATAGRAM_FLAG_RECOVERY) !== 0;
  const fragmentIndex = view.getUint16(22);
  const fragmentCount = view.getUint16(24);
  const fragmentLength = view.getUint16(26);
  const slot = view.getUint8(6);
  const laneClass = view.getUint8(7);
  if (
    view.getUint32(0) !== MAGIC ||
    view.getUint8(4) !== VERSION ||
    view.getUint8(5) !== MEDIA_DATAGRAM ||
    sequence < 1 ||
    sequence > MAX_SEQUENCE ||
    requiredControlSeq > MAX_SEQUENCE ||
    totalLength < 1 ||
    totalLength > MAX_MEDIA ||
    (type !== 1 && type !== 2 && type !== 3) ||
    laneClass !== (type === 3 ? 1 : 0) ||
    (flags & ~(DATAGRAM_FLAG_PARITY | DATAGRAM_FLAG_AFTER_KEYFRAME | DATAGRAM_FLAG_RECOVERY)) !==
      0 ||
    (recovery ? type !== 1 : type === 1) ||
    (afterKeyframe && type !== 2) ||
    fragmentCount < 1 ||
    fragmentCount > MAX_DATAGRAM_FRAGMENTS ||
    fragmentCount > totalLength ||
    (parity ? type === 3 || fragmentIndex !== fragmentCount : fragmentIndex >= fragmentCount) ||
    fragmentLength < 1 ||
    bytes.byteLength !== DATAGRAM_HEADER + fragmentLength
  ) {
    throw new Error('datagram-frame-invalid');
  }
  const payload = bytes.slice(DATAGRAM_HEADER);
  return {
    slot,
    laneClass,
    sequence,
    requiredControlSeq,
    totalLength,
    type,
    fragmentIndex,
    fragmentCount,
    parity,
    afterKeyframe,
    recovery,
    payload,
  };
}

class ControlParser {
  constructor(onFrame) {
    this.buffer = new Uint8Array();
    this.onFrame = onFrame;
  }

  push(chunk) {
    this.buffer = concatBytes(this.buffer, bytesOf(chunk));
    for (;;) {
      if (this.buffer.byteLength < CONTROL_HEADER) return;
      const view = new DataView(this.buffer.buffer, this.buffer.byteOffset, this.buffer.byteLength);
      const length = view.getUint32(8);
      if (view.getUint32(0) !== MAGIC || view.getUint8(4) !== VERSION || length > MAX_CONTROL) {
        throw new Error('control-frame-invalid');
      }
      const total = CONTROL_HEADER + length;
      if (this.buffer.byteLength < total) return;
      const frame = this.buffer.slice(0, total);
      this.buffer = this.buffer.slice(total);
      const frameView = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
      this.onFrame({
        kind: frameView.getUint8(5),
        sequence: frameView.getUint32(12),
        payload: frame.slice(CONTROL_HEADER),
      });
    }
  }
}

function invalidState() {
  if (typeof DOMException === 'function')
    return new DOMException('Socket is not OPEN', 'InvalidStateError');
  const error = new Error('Socket is not OPEN');
  error.name = 'InvalidStateError';
  return error;
}

function quotaExceeded(reason) {
  if (typeof DOMException === 'function') return new DOMException(reason, 'QuotaExceededError');
  const error = new Error(reason);
  error.name = 'QuotaExceededError';
  return error;
}

function trimFailures(values, now = Date.now()) {
  while (values.length && now - values[0] > FAILURE_WINDOW_MS) values.shift();
  return values;
}

function parseControl(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function epochScope(message) {
  const type = message?.type;
  if (type === 'start' || type === 'stop') return { all: true, slot: null };
  if (type !== 'stream-start' && type !== 'stream-stop') return null;
  const slot = Number(message?.slot);
  return Number.isInteger(slot) && slot >= 0 && slot <= 255 ? { all: false, slot } : null;
}

/**
 * Controle que viaja do RECEPTOR de mídia para o EMISSOR.
 *
 * Ele não descreve nada do que sai por este lado, então não pode virar barreira
 * da mídia de saída. Um pedido de keyframe que barra os próprios quadros que
 * veio destravar não é uma barreira conservadora: é a sessão fechando por
 * `barrier-timeout` exatamente no instante em que a recuperação começaria.
 *
 * A lista é de EXCLUSÃO de propósito. Esquecer uma entrada aqui custa uma
 * barreira conservadora — correta, só um pouco mais lenta. Uma lista de
 * INCLUSÃO esquecida custaria uma corrida: mídia entregue antes do controle que
 * a descreve.
 */
const FEEDBACK_CONTROL = new Set(['need-keyframe', 'media-loss', 'media-recovered']);

const isFeedbackControl = (message) => FEEDBACK_CONTROL.has(message?.type);

/**
 * Private WebTransport framing adapter. Control is serialized once per socket;
 * media is concurrent across lanes and bounded independently from control.
 */
class WireEndpoint {
  constructor({
    session,
    stream,
    initiator,
    onMessage,
    onClose,
    onError,
    onBuffered,
    onNeedKeyframe,
    onMediaDrop,
    onDiagnostic,
    acceptMediaHeader,
    assumePeerDatagrams = false,
  }) {
    this.session = session;
    this.stream = stream;
    this.initiator = initiator;
    this.onMessage = onMessage;
    this.onClose = onClose;
    this.onError = onError;
    this.onBuffered = onBuffered;
    this.onNeedKeyframe = onNeedKeyframe;
    this.onMediaDrop = onMediaDrop;
    this.onDiagnostic = onDiagnostic;
    this.acceptMediaHeader = acceptMediaHeader;
    this.writer = stream.writable.getWriter();
    void this.writer.closed.catch(() => {});
    void this.writer.ready.catch(() => {});

    this.controlWrite = Promise.resolve();
    this.controlOut = 0;
    // A última sequência de controle que a mídia de saída realmente depende —
    // ver FEEDBACK_CONTROL. É ela, e não `controlOut`, que a barreira carrega.
    this.controlBarrier = 0;
    this.controlIn = 0;
    this.controlItems = 0;
    this.controlBytes = 0;
    this.mediaOut = new Map();
    this.outLanes = new Map();
    this.recoverySlots = new Set();
    this.mediaItems = new Set();
    this.mediaBytes = 0;
    this.mediaWriters = 0;
    this.mediaWaiters = [];
    this.datagramWriter = null;
    this.datagramWritable = null;
    this.datagramReader = null;
    this.maxDatagramSize = 0;
    this.datagramItems = new Set();
    this.datagramQueue = [];
    this.datagramBytes = 0;
    this.datagramBusy = false;
    this.datagramStatsBusy = false;
    this.datagramStatsTimer = null;
    this.lastNativeDatagramExpired = 0;
    this.lastNativeDatagramLost = 0;
    this.datagramAssemblies = new Map();
    this.datagramAssemblyBytes = 0;
    this.recentDatagrams = new Map();
    this.peerDatagrams = false;
    this.peerDatagramFec = false;
    this.peerDatagramRecovery = false;
    this.assumePeerDatagrams = Boolean(assumePeerDatagrams);
    this.incoming = new Set();
    this.incomingBytes = 0;
    this.provisional = [];
    this.lanes = new Map();
    this.pendingItems = 0;
    this.pendingBytes = 0;
    this.epochAllSeq = 0;
    this.epochSlotSeq = new Map();
    this.barrier = [];
    this.barrierBytes = 0;
    this.peerIncomplete = [];
    this.peerFraming = [];
    this.stats = {
      sent: 0,
      localDrops: 0,
      incoming: 0,
      resets: 0,
      delivered: 0,
      late: 0,
      keyframesSent: 0,
      recoveryKeyframesSent: 0,
      keyframesDelivered: 0,
      maxKeyframeBytes: 0,
      datagramFragmentsSent: 0,
      datagramFragmentsReceived: 0,
      datagramFramesReassembled: 0,
      datagramFramesRecovered: 0,
      datagramParitySent: 0,
      datagramFecActivations: 0,
      datagramFecDeactivations: 0,
      datagramAssembliesExpired: 0,
      nativeDatagramsExpired: 0,
      nativeDatagramsLost: 0,
    };
    this.closed = false;
    this.finished = false;
    this.controlReader = null;
    this.handshake = deferred();
    this.ready = this.handshake.promise;
    this.handshakeConfirmed = false;
    this.buffered = 0;
    try {
      const datagrams = session.datagrams;
      this.maxDatagramSize = Math.max(0, Math.floor(Number(datagrams?.maxDatagramSize) || 0));
      if ('outgoingMaxAge' in (datagrams ?? {})) datagrams.outgoingMaxAge = DATAGRAM_MAX_AGE_MS;
      const writable = datagrams?.createWritable?.() ?? datagrams?.writable;
      this.datagramWritable = writable ?? null;
      this.datagramWriter = writable?.getWriter?.() ?? null;
      void this.datagramWriter?.closed?.catch?.(() => {});
      void this.datagramWriter?.ready?.catch?.(() => {});
      this.peerDatagrams =
        this.assumePeerDatagrams && this.maxDatagramSize > DATAGRAM_HEADER && !!this.datagramWriter;
    } catch {
      this.datagramWriter = null;
      this.datagramWritable = null;
      this.maxDatagramSize = 0;
    }
  }

  start() {
    void this.#readControl();
    void this.#readUnidirectional();
    void this.#readDatagrams();
    Promise.resolve(this.session.closed).then(
      (info) => this.#finish(info),
      (error) => this.#fatal('transport-error', error),
    );
    this.#scheduleDatagramStats();
    if (this.initiator) void this.#queueControl(frameControl(HANDSHAKE, 0, HANDSHAKE_TEXT));
    return this;
  }

  send(data) {
    if (this.closed) return undefined;
    if (typeof data === 'string') return this.#sendControl(data);
    return this.#sendMedia(data);
  }

  close(info = { closeCode: 0, reason: '' }) {
    if (this.closed) return;
    this.closed = true;
    try {
      void this.writer.close().catch(() => {});
    } catch {
      // The native session close below is authoritative.
    }
    closeWireSession(this.session, info);
    this.#finish(info);
  }

  #publishBuffered() {
    const amount = this.controlBytes + this.mediaBytes + this.datagramBytes;
    if (amount === this.buffered) return;
    this.buffered = amount;
    this.onBuffered?.(amount);
  }

  #scheduleDatagramStats(delayMs = DATAGRAM_STATS_MS) {
    if (this.closed || typeof this.session?.getStats !== 'function') return;
    clearTimeout(this.datagramStatsTimer);
    this.datagramStatsTimer = setTimeout(() => void this.#pollDatagramStats(), delayMs);
  }

  #activateLaneFec(laneState) {
    if (!laneState.fecActive) this.stats.datagramFecActivations++;
    laneState.fecActive = true;
    laneState.fecMinUntil = Date.now() + FEC_MIN_ACTIVE_MS;
    laneState.fecCleanSamples = 0;
  }

  #laneFecActive(laneState) {
    if (!laneState?.fecActive) return false;
    if (typeof this.session?.getStats === 'function' || Date.now() < laneState.fecMinUntil) {
      return true;
    }
    laneState.fecActive = false;
    laneState.fecCleanSamples = 0;
    this.stats.datagramFecDeactivations++;
    return false;
  }

  async #pollDatagramStats() {
    if (this.closed || this.datagramStatsBusy || typeof this.session?.getStats !== 'function') {
      this.#scheduleDatagramStats();
      return;
    }
    this.datagramStatsBusy = true;
    try {
      const native = await this.session.getStats();
      if (this.closed) return;
      const expired = Math.max(0, Number(native?.datagrams?.expiredOutgoing) || 0);
      const lost = Math.max(0, Number(native?.datagrams?.lostOutgoing) || 0);
      const newlyExpired = Math.max(0, expired - this.lastNativeDatagramExpired);
      const newlyLost = Math.max(0, lost - this.lastNativeDatagramLost);
      this.lastNativeDatagramExpired = expired;
      this.lastNativeDatagramLost = lost;
      this.stats.nativeDatagramsExpired += newlyExpired;
      this.stats.nativeDatagramsLost += newlyLost;

      if (newlyExpired || newlyLost) {
        const reason = newlyExpired ? 'datagram-native-expired' : 'datagram-native-lost';
        // A pressão agregada do QUIC reduz a oferta e arma FEC, mas não prova
        // que um quadro ficou irrecuperável: ele pode ter sido reconstruído por
        // paridade. Só o resultado do write ou um gap observado pelo receptor
        // pode desprimar a lane e pedir keyframe.
        try {
          this.onDiagnostic?.({ reason, newlyExpired, newlyLost });
        } catch {
          // Native loss telemetry never changes session lifecycle.
        }
        for (const [lane, laneState] of this.outLanes) {
          if (!lane.endsWith(':0')) continue;
          this.#activateLaneFec(laneState);
        }
      } else {
        const now = Date.now();
        for (const [lane, laneState] of this.outLanes) {
          if (!lane.endsWith(':0') || !laneState.fecActive || now < laneState.fecMinUntil) continue;
          laneState.fecCleanSamples++;
          if (laneState.fecCleanSamples >= FEC_CLEAN_SAMPLES_TO_DISABLE) {
            laneState.fecActive = false;
            laneState.fecCleanSamples = 0;
            this.stats.datagramFecDeactivations++;
          }
        }
      }
    } catch {
      // Stats are advisory. A missing/temporary native sample cannot close media.
    } finally {
      this.datagramStatsBusy = false;
      this.#scheduleDatagramStats();
    }
  }

  #sendControl(text) {
    const payload = encoder.encode(text);
    if (payload.byteLength > MAX_CONTROL) throw quotaExceeded('control-message-too-large');
    if (
      this.controlItems >= MAX_CONTROL_ITEMS ||
      this.controlBytes + payload.byteLength > MAX_CONTROL_QUEUE
    ) {
      this.#fatal('control-overflow', new Error('control-overflow'));
      return undefined;
    }
    if (this.controlOut >= MAX_SEQUENCE) {
      this.#fatal('sequence-wrap', new Error('control-sequence-wrap'));
      return undefined;
    }
    const sequence = ++this.controlOut;
    const frame = frameControl(CONTROL, sequence, payload);
    const message = parseControl(text);
    if (!isFeedbackControl(message)) this.controlBarrier = sequence;
    this.#applyOutgoingEpoch(message);
    this.controlItems++;
    this.controlBytes += payload.byteLength;
    this.#publishBuffered();
    const settled = () => {
      this.controlItems = Math.max(0, this.controlItems - 1);
      this.controlBytes = Math.max(0, this.controlBytes - payload.byteLength);
      this.#publishBuffered();
    };
    void this.#queueControl(frame).then(settled, settled);
    return undefined;
  }

  #applyOutgoingEpoch(message) {
    const scope = epochScope(message);
    if (!scope) return;
    const matches = (lane) => scope.all || Number(lane.slice(0, lane.indexOf(':'))) === scope.slot;
    for (const item of [...this.mediaItems]) {
      if (matches(item.lane)) this.#obsolete(item);
    }
    for (const item of [...this.datagramItems]) {
      if (matches(item.lane) && !item.active) this.#settleDatagram(item);
    }
    for (const lane of [...this.outLanes.keys()]) {
      if (matches(lane)) this.outLanes.delete(lane);
    }
  }

  #queueControl(frame) {
    const write = this.controlWrite.then(() => {
      if (this.closed) throw new Error('transport-closed');
      return this.writer.write(frame);
    });
    this.controlWrite = write.catch((error) => {
      if (!this.closed) this.#fatal('control-write-failed', error);
    });
    return write;
  }

  #nextMediaSequence(lane) {
    const current = this.mediaOut.get(lane) ?? 0;
    if (current >= MAX_SEQUENCE) {
      this.#fatal('sequence-wrap', new Error('media-sequence-wrap'));
      return null;
    }
    const next = current + 1;
    this.mediaOut.set(lane, next);
    return next;
  }

  #sendMedia(data) {
    const payload = bytesOf(data);
    if (payload.byteLength > MAX_MEDIA) throw quotaExceeded('media-message-too-large');
    const lane = mediaLane(payload);
    const type = payload[1] ?? 0;
    const slot = payload[0] ?? 0;

    let laneState = this.outLanes.get(lane);
    if (!laneState) {
      laneState = {
        tail: null,
        lastType: null,
        needsRecovery: false,
        dropReported: false,
        fecActive: false,
        fecMinUntil: 0,
        fecCleanSamples: 0,
      };
      this.outLanes.set(lane, laneState);
    }

    // Um keyframe é o ponto de partida novo da lane: todo delta ainda na fila
    // referencia um passado que ele acaba de substituir. Aposentá-los ANTES da
    // admissão é o que faz a recuperação caber justamente quando a fila está
    // cheia — antes, o congestionamento descartava o próprio remédio e a lane
    // ficava presa a deltas que ninguém mais poderia decodificar.
    if (type === 1) {
      for (const previous of [...this.mediaItems]) {
        if (previous.lane === lane && previous.type === 2) {
          laneState.needsRecovery = true;
          this.#obsolete(previous);
        }
      }
      for (const previous of [...this.datagramItems]) {
        if (previous.lane === lane && previous.type === 2) {
          laneState.needsRecovery = true;
          if (!previous.active) this.#settleDatagram(previous);
        }
      }
    }

    const recovery = type === 1 && (laneState.needsRecovery || this.recoverySlots.has(slot));

    // A âncora precisa sobreviver ao enlace degradado. No perfil oficial de
    // 600 kbit/s, um keyframe de 28 KB duplicado + paridade ocupa cerca de
    // 85 KB, mais do que a rede consegue transmitir antes do prazo nativo de
    // 250 ms dos datagramas. Deltas e áudio continuam descartáveis; keyframe,
    // inclusive o urgente, usa um único stream confiável e coalescido.

    // Delta e áudio são sempre mídia de tempo real quando há datagramas.
    if ((type === 2 || type === 3) && this.peerDatagrams) {
      if (type === 2 && laneState.needsRecovery) return undefined;
      this.#sendDatagramMedia(payload, lane, type);
      return undefined;
    }

    if (
      this.mediaItems.size >= MAX_MEDIA_ITEMS ||
      this.mediaBytes + payload.byteLength > MAX_MEDIA_QUEUE
    ) {
      // Fila cheia não é buraco de sequência: nada foi emitido, então nada pode
      // ter sido numerado. A sequência é alocada só abaixo, na admissão — assim
      // o próximo quadro útil continua contíguo e o receptor nunca vê um vão que
      // a fila de saída inventou.
      this.#localFailure('queue-full');
      return undefined;
    }

    const sequence = this.#nextMediaSequence(lane);
    if (sequence === null) return undefined;

    const item = {
      lane,
      type,
      sequence,
      bytes: payload.byteLength,
      frame: frameMedia(payload, sequence, this.controlBarrier, recovery ? MEDIA_FLAG_RECOVERY : 0),
      enqueuedAt: Date.now(),
      writer: null,
      stream: null,
      obsolete: false,
      dropped: false,
      settled: false,
      recovery,
      writerAcquiredAt: null,
      queueTimer: null,
    };
    this.mediaItems.add(item);
    this.mediaBytes += item.bytes;
    if (type === 1) {
      this.stats.keyframesSent++;
      if (recovery) this.stats.recoveryKeyframesSent++;
      this.stats.maxKeyframeBytes = Math.max(this.stats.maxKeyframeBytes, payload.byteLength);
    }
    this.#publishBuffered();

    if (recovery) {
      laneState.needsRecovery = false;
      laneState.dropReported = false;
      this.recoverySlots.delete(slot);
    }
    const serialize = item.type === 3 || (item.type === 1 && laneState.lastType === 1);
    const blocker = serialize ? laneState.tail : null;
    const laneDone = deferred();
    item.releaseLane = () => {
      if (item.laneReleased) return;
      item.laneReleased = true;
      laneDone.resolve();
    };
    if (item.type === 1 || item.type === 3) laneState.tail = laneDone.promise;
    laneState.lastType = item.type;

    if (!recovery) {
      item.queueTimer = setTimeout(
        () => this.#dropMedia(item, 'media-create-timeout'),
        CREATE_TIMEOUT_MS,
      );
    }
    // A bounded writer pool protects native QUIC credits without a global
    // promise tail: one slow video writer still leaves independent slots for
    // audio, another lane, or a recovery keyframe.
    void this.#runMedia(item, blocker);
    return undefined;
  }

  #sendDatagramMedia(payload, lane, type, { recovery = false, duplicateData = false } = {}) {
    // Deltas codificados dependem dos anteriores. Trocar um delta ainda não
    // emitido por outro mais novo preservaria a sequência do wire, mas quebraria
    // a cadeia do codec. Mantemos a fila curta e expiráveis; se ela estourar,
    // paramos a lane e pedimos um keyframe em vez de enviar deltas indecifráveis.
    const fragmentCapacity = this.maxDatagramSize - DATAGRAM_HEADER;
    const fragmentCount = Math.ceil(payload.byteLength / fragmentCapacity);
    if (fragmentCapacity < 1 || fragmentCount < 1 || fragmentCount > MAX_DATAGRAM_FRAGMENTS) {
      if (type !== 3) this.#markLaneRecovery(lane, 'datagram-frame-too-fragmented');
      this.#localFailure('datagram-frame-too-fragmented');
      return false;
    }
    const dataBytes = payload.byteLength + fragmentCount * DATAGRAM_HEADER;
    const physicalBytes =
      dataBytes * (duplicateData ? 2 : 1) + (recovery ? this.maxDatagramSize : 0);
    if (
      this.datagramItems.size >= MAX_DATAGRAM_ITEMS ||
      this.datagramBytes + physicalBytes > MAX_DATAGRAM_QUEUE
    ) {
      if (type !== 3) this.#markLaneRecovery(lane, 'datagram-queue-full');
      this.#localFailure('datagram-queue-full');
      return false;
    }
    const item = {
      lane,
      type,
      bytes: physicalBytes,
      payload,
      recovery,
      duplicateData,
      requiredControlSeq: this.controlBarrier,
      enqueuedAt: Date.now(),
      active: false,
      settled: false,
    };
    this.datagramItems.add(item);
    this.datagramQueue.push(item);
    this.datagramBytes += item.bytes;
    this.#publishBuffered();
    void this.#pumpDatagrams();
    return true;
  }

  async #pumpDatagrams() {
    if (this.datagramBusy || this.closed || !this.datagramWriter) return;
    this.datagramBusy = true;
    try {
      while (!this.closed && this.datagramQueue.length) {
        const item = this.datagramQueue.shift();
        if (!item || item.settled) continue;
        item.active = true;
        try {
          if (Date.now() - item.enqueuedAt > DATAGRAM_MAX_AGE_MS) {
            if (item.type !== 3) this.#markLaneRecovery(item.lane, 'datagram-queue-expired');
            this.#localFailure('datagram-queue-expired');
            continue;
          }
          // A sequência nasce somente quando o quadro efetivamente começa a
          // entrar no QUIC. Itens substituídos na fila não criam buracos
          // artificiais que o receptor confundiria com perda de rede.
          const sequence = this.#nextMediaSequence(item.lane);
          if (sequence === null) continue;
          item.sequence = sequence;
          const laneState = this.outLanes.get(item.lane);
          const afterKeyframe = item.type === 2 && laneState?.lastType === 1;
          if (laneState) laneState.lastType = item.type;
          const withParity =
            this.peerDatagramFec &&
            (item.recovery || (item.type === 2 && this.#laneFecActive(laneState)));
          const fragments = frameMediaDatagrams(
            item.payload,
            sequence,
            item.requiredControlSeq,
            this.maxDatagramSize,
            withParity,
            afterKeyframe,
            item.recovery,
          );
          const writes = item.duplicateData
            ? [
                ...fragments.filter((fragment) => (fragment[21] & DATAGRAM_FLAG_PARITY) !== 0),
                ...fragments.filter((fragment) => (fragment[21] & DATAGRAM_FLAG_PARITY) === 0),
                ...fragments.filter((fragment) => (fragment[21] & DATAGRAM_FLAG_PARITY) === 0),
              ]
            : fragments;
          const fragmentFailures = [];
          const successfulData = new Set();
          let paritySucceeded = false;
          for (const fragment of writes) {
            await this.datagramWriter.write(fragment);
            const status = this.datagramWritable?.lastWriteStatus;
            if (status?.code && status.code !== 'success') {
              const reason = `datagram-${status.code}`;
              fragmentFailures.push({
                reason,
                message: status.message,
                parity: (fragment[21] & DATAGRAM_FLAG_PARITY) !== 0,
              });
              // Com FEC, o primeiro fragmento perdido não encerra o quadro: a
              // paridade prioritária já enviada consegue reconstruí-lo. Sem FEC,
              // continuar só gastaria rede num frame inevitavelmente incompleto.
              if (!withParity) break;
              continue;
            }
            this.stats.datagramFragmentsSent++;
            if ((fragment[21] & DATAGRAM_FLAG_PARITY) !== 0) {
              this.stats.datagramParitySent++;
              paritySucceeded = true;
            } else {
              const view = new DataView(fragment.buffer, fragment.byteOffset, fragment.byteLength);
              successfulData.add(view.getUint16(22));
            }
          }
          if (fragmentFailures.length) {
            for (const failure of fragmentFailures) {
              try {
                this.onDiagnostic?.({ reason: failure.reason });
              } catch {
                // Telemetria/controle adaptativo nunca pode quebrar o wire.
              }
              this.#localFailure(
                failure.message ? `${failure.reason}:${failure.message}` : failure.reason,
              );
            }
            const firstView = new DataView(
              fragments[0].buffer,
              fragments[0].byteOffset,
              fragments[0].byteLength,
            );
            const missingData = firstView.getUint16(24) - successfulData.size;
            const recoveredByFec =
              missingData === 0 || (withParity && missingData === 1 && paritySucceeded);
            if (!recoveredByFec) {
              const first = fragmentFailures[0];
              const error = new Error(
                first.message ? `${first.reason}:${first.message}` : first.reason,
              );
              error.datagramReason = first.reason;
              error.failurePublished = true;
              throw error;
            }
          }
          this.stats.sent++;
        } catch (error) {
          const reason = error?.datagramReason ?? 'datagram-write-failed';
          // O status do writer é o primeiro sinal confiável de que o QUIC já
          // não consegue admitir mídia fresca. Publique-o antes do pedido de
          // keyframe/drop comum: o relay pode reduzir a oferta no mesmo ciclo,
          // sem gastar o cooldown com uma reação menor e tardia.
          if (error?.datagramReason && !error.failurePublished) {
            try {
              this.onDiagnostic?.({ reason });
            } catch {
              // Telemetria/controle adaptativo nunca pode quebrar o wire.
            }
          }
          if (item.type !== 3) this.#markLaneRecovery(item.lane, reason);
          if (!error?.failurePublished) {
            this.#localFailure(error?.message ?? 'datagram-write-failed');
          }
        } finally {
          this.#settleDatagram(item);
        }
      }
    } finally {
      this.datagramBusy = false;
      if (!this.closed && this.datagramQueue.some((item) => !item.settled)) {
        void this.#pumpDatagrams();
      }
    }
  }

  #settleDatagram(item) {
    if (!item || item.settled) return;
    item.settled = true;
    this.datagramItems.delete(item);
    this.datagramQueue = this.datagramQueue.filter((entry) => entry !== item);
    this.datagramBytes = Math.max(0, this.datagramBytes - item.bytes);
    this.#publishBuffered();
  }

  async #runMedia(item, blocker) {
    try {
      const queueLeft = () =>
        CREATE_TIMEOUT_MS -
        (Date.now() - (item.recovery ? item.writerAcquiredAt : item.enqueuedAt));
      if (blocker) await blocker;
      // Item liquidado — obsoleto, descartado ou já escrito — não volta a
      // consumir nada. Ele não pega crédito de writer, não abre stream e não
      // ocupa a vaga que o keyframe da recuperação está esperando.
      if (this.closed || item.settled) return;
      const writerCredit = this.#acquireMediaWriter(item);
      if (writerCredit) await writerCredit;
      if (this.closed || item.settled) return;
      if (queueLeft() <= 0) throw new Error('media-create-timeout');
      // Inicie a abertura no mesmo turno em que o frame foi admitido. Além de
      // reduzir uma volta de microtask por chunk, isto dá ao item um dono nativo
      // que pode ser abortado se um keyframe o aposentar logo em seguida.
      // QUIC pode intercalar streams independentes. O número crescente faz o
      // keyframe mais fresco ganhar do keyframe velho que ainda retransmite,
      // sem sacrificar confiabilidade nem criar uma fila paralela no app.
      const create = Promise.resolve(
        this.session.createUnidirectionalStream({ sendOrder: item.sequence }),
      );
      try {
        item.stream = await after(create, queueLeft(), 'media-create-timeout');
      } catch (error) {
        void create
          .then((stream) => this.#disposeLateStream(stream, item.obsolete ? item.frame : null))
          .catch(() => {});
        throw error;
      }
      if (this.closed || item.settled) {
        await this.#disposeLateStream(item.stream, item.obsolete ? item.frame : null);
        return;
      }
      clearTimeout(item.queueTimer);
      item.queueTimer = null;

      item.writer = item.stream.getWriter();
      void item.writer.closed.catch(() => {});
      void item.writer.ready.catch(() => {});
      await after(
        Promise.resolve(item.writer.write(item.frame)).then(() => {
          const closing = item.writer.close();
          item.releaseLane?.();
          return closing;
        }),
        WRITE_TIMEOUT_MS,
        'media-write-timeout',
      );
      this.stats.sent++;
    } catch (error) {
      if (!this.closed && !item.obsolete && !item.dropped) {
        if (item.type !== 3) this.#markLaneRecovery(item.lane);
        this.#localFailure(error?.message ?? 'media-write-failed');
      }
      try {
        await item.writer?.abort(error);
      } catch {
        // This item has no other owner.
      }
    } finally {
      item.releaseLane?.();
      this.#releaseMediaWriter(item);
      this.#settleMedia(item);
    }
  }

  #acquireMediaWriter(item) {
    if (this.mediaWriters < MAX_MEDIA_WRITERS) {
      this.mediaWriters++;
      item.hasWriterSlot = true;
      item.writerAcquiredAt = Date.now();
      return null;
    }
    return new Promise((resolve) => this.mediaWaiters.push({ item, resolve }));
  }

  #releaseMediaWriter(item) {
    if (!item.hasWriterSlot) return;
    item.hasWriterSlot = false;
    this.mediaWriters = Math.max(0, this.mediaWriters - 1);
    while (this.mediaWaiters.length) {
      const preferred = this.mediaWaiters.findIndex(
        (entry) => entry.item.type === 1 && !entry.item.settled,
      );
      const next = this.mediaWaiters.splice(preferred >= 0 ? preferred : 0, 1)[0];
      if (next.item.settled || this.closed) {
        next.resolve();
        continue;
      }
      this.mediaWriters++;
      next.item.hasWriterSlot = true;
      next.item.writerAcquiredAt = Date.now();
      next.resolve();
      break;
    }
  }

  async #disposeLateStream(stream, obsoleteFrame = null) {
    let writer;
    try {
      writer = stream?.getWriter?.();
      // Um stream nativo pode nascer depois que o item já foi aposentado. O
      // wrapper só consegue associar o abort ao frame se tiver visto seus bytes;
      // escrevemos no sink já aberto e resetamos antes do FIN. Isso também evita
      // deixar um create nativo órfão, sem transformar o frame velho em entrega.
      if (writer && obsoleteFrame)
        await after(writer.write(obsoleteFrame), CREATE_TIMEOUT_MS, 'obsolete-write-timeout');
    } catch {
      // O reset no finally continua sendo a única obrigação do dono tardio.
    } finally {
      try {
        if (writer) await writer.abort?.(new Error('media-item-dropped'));
        else await stream?.abort?.(new Error('media-item-dropped'));
      } catch {
        // A late native stream can already be closed.
      }
    }
  }

  #obsolete(item) {
    if (item.settled || item.obsolete) return;
    item.obsolete = true;
    try {
      void item.writer?.abort?.(new Error('media-obsolete')).catch(() => {});
    } catch {
      // The receiver will reject late-old by sequence even if abort races.
    }
    this.#settleMedia(item);
  }

  #settleMedia(item) {
    if (item.settled) return;
    item.settled = true;
    clearTimeout(item.queueTimer);
    item.queueTimer = null;
    // A cauda da lane é recurso como o crédito de writer: quem já não vai
    // escrever não pode segurar quem vem depois. Sem isto, um item aposentado
    // enquanto espera um `createUnidirectionalStream` que nunca resolve
    // bloquearia a lane para sempre.
    item.releaseLane?.();
    this.mediaItems.delete(item);
    this.mediaBytes = Math.max(0, this.mediaBytes - item.bytes);
    this.#publishBuffered();
  }

  #dropMedia(item, detail) {
    if (item.settled || item.obsolete || this.closed) return;
    item.dropped = true;
    if (item.type !== 3) this.#markLaneRecovery(item.lane);
    this.#localFailure(detail);
    try {
      void item.writer?.abort?.(new Error(detail)).catch(() => {});
    } catch {
      // The eventual create/write continuation observes item.settled.
    }
    this.#settleMedia(item);
  }

  #markLaneRecovery(lane, reason = 'media-drop') {
    const laneState = this.outLanes.get(lane);
    if (!laneState) return;
    laneState.needsRecovery = true;
    // Deltas queued after an optimistic recovery admission still depend on that
    // keyframe. If the keyframe itself fails, retire those deltas before the
    // pump can spend bandwidth on an undecodable chain.
    for (const item of [...this.datagramItems]) {
      if (item.lane === lane && item.type === 2 && !item.active) this.#settleDatagram(item);
    }
    if (laneState.dropReported) return;
    laneState.dropReported = true;
    try {
      const slot = Number.parseInt(String(lane).split(':', 1)[0], 10);
      this.onMediaDrop?.(Number.isInteger(slot) ? slot : null, reason);
    } catch {
      // Pedir um novo ponto de partida nunca derruba a sessão.
    }
  }

  /**
   * Descarte LOCAL de mídia: perda de quadro, nunca perda de sessão.
   *
   * Fila cheia, create estourado, write que falhou — tudo isso é a saída daqui
   * sob pressão, e a mídia é descartável por natureza: o próximo keyframe
   * reconstrói a imagem inteira. Fechar a sessão por acumular esses descartes
   * invertia o remédio: a rajada de perda acontece exatamente quando um
   * keyframe novo resolveria tudo, e o transporte morria um instante antes.
   *
   * Fatalidade continua reservada ao que NÃO é recuperável deste lado:
   * `control-overflow` (controle não é descartável), abuso de framing/streams
   * do peer, barreira estourada e erro estrutural da sessão.
   */
  #localFailure(detail) {
    this.stats.localDrops++;
    try {
      this.onDiagnostic?.({ reason: 'backpressure-drop', detail: String(detail) });
    } catch {
      // Diagnostics never alter wire state.
    }
  }

  async #readControl() {
    const parser = new ControlParser((frame) => this.#acceptControl(frame));
    const reader = this.stream.readable.getReader();
    this.controlReader = reader;
    void reader.closed.catch(() => {});
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) throw new Error('control-stream-finished');
        parser.push(value);
      }
    } catch (error) {
      if (!this.closed) this.#fatal('control-invalid', error);
    } finally {
      if (this.controlReader === reader) this.controlReader = null;
      reader.releaseLock?.();
    }
  }

  #acceptControl(frame) {
    if (!this.handshakeConfirmed) {
      if (
        frame.kind !== HANDSHAKE ||
        frame.sequence !== 0 ||
        decoder.decode(frame.payload) !== HANDSHAKE_TEXT
      ) {
        throw new Error('handshake-invalid');
      }
      this.handshakeConfirmed = true;
      if (!this.initiator) void this.#queueControl(frameControl(HANDSHAKE, 0, HANDSHAKE_TEXT));
      if (this.datagramWriter && this.maxDatagramSize > DATAGRAM_HEADER) {
        this.#sendControl(
          JSON.stringify({
            type: CAPABILITIES_TYPE,
            mediaDatagrams: 1,
            mediaDatagramFec: 1,
            mediaRecoveryDatagram: 1,
          }),
        );
      }
      this.handshake.resolve(this);
      return;
    }
    if (frame.kind === HANDSHAKE) throw new Error('handshake-invalid');
    if (frame.kind !== CONTROL) throw new Error('control-kind-invalid');
    if (this.controlIn >= MAX_SEQUENCE || frame.sequence !== this.controlIn + 1) {
      throw new Error('control-sequence-invalid');
    }
    this.controlIn = frame.sequence;
    const text = decoder.decode(frame.payload);
    const message = parseControl(text);
    if (message?.type === CAPABILITIES_TYPE) {
      this.peerDatagrams =
        message.mediaDatagrams === 1 &&
        this.maxDatagramSize > DATAGRAM_HEADER &&
        !!this.datagramWriter;
      this.peerDatagramFec = message.mediaDatagramFec === 1;
      this.peerDatagramRecovery = message.mediaRecoveryDatagram === 1;
      this.#releaseBarrier();
      return;
    }
    if (message?.type === 'need-keyframe' && Number.isInteger(message.slot)) {
      const laneState = this.outLanes.get(`${message.slot}:0`);
      this.recoverySlots.add(message.slot);
      if (laneState) {
        laneState.needsRecovery = true;
        if (this.peerDatagramFec) this.#activateLaneFec(laneState);
      }
    }
    if (message?.type === 'media-recovered' && Number.isInteger(message.slot)) {
      const laneState = this.outLanes.get(`${message.slot}:0`);
      if (laneState) {
        laneState.needsRecovery = false;
        laneState.dropReported = false;
      }
      this.recoverySlots.delete(message.slot);
      this.#releaseBarrier();
      return;
    }
    if (
      message?.type === 'media-loss' &&
      Number.isInteger(message.slot) &&
      message.slot >= 0 &&
      message.slot <= 255
    ) {
      const laneState = this.outLanes.get(`${message.slot}:0`);
      if (laneState && this.peerDatagramFec) this.#activateLaneFec(laneState);
      this.#releaseBarrier();
      return;
    }
    this.#applyEpochControl(message, frame.sequence);
    this.onMessage?.(text, false);
    this.#releaseBarrier();
  }

  #applyEpochControl(message, sequence) {
    const scope = epochScope(message);
    if (!scope) return;
    const { all, slot } = scope;

    if (all) this.epochAllSeq = sequence;
    else this.epochSlotSeq.set(slot, sequence);

    const matches = (candidate) => all || candidate === slot;
    for (const [key, lane] of [...this.lanes]) {
      const laneSlot = Number(key.slice(0, key.indexOf(':')));
      if (matches(laneSlot)) this.#dropLane(key, lane);
    }
    for (const record of [...this.incoming]) {
      if (
        record.header &&
        matches(record.header.slot) &&
        record.header.requiredControlSeq < sequence
      ) {
        this.#cancelIncomingEpoch(record);
      }
    }
    for (const assembly of [...this.datagramAssemblies.values()]) {
      if (matches(assembly.slot) && assembly.requiredControlSeq < sequence) {
        this.#dropDatagramAssembly(assembly, null);
      }
    }
  }

  #epochFloor(slot) {
    return Math.max(this.epochAllSeq, this.epochSlotSeq.get(slot) ?? 0);
  }

  #cancelIncomingEpoch(record) {
    if (!record || record.done) return;
    this.#cleanupIncoming(record);
    try {
      void Promise.resolve(record.reader.cancel?.('media-epoch-obsolete')).catch(() => {});
    } catch {
      // Epoch cancellation is expected lifecycle, never peer abuse.
    }
  }

  async #readUnidirectional() {
    const streams = this.session.incomingUnidirectionalStreams;
    if (!streams?.getReader) return;
    const reader = streams.getReader();
    void reader.closed.catch(() => {});
    try {
      for (;;) {
        const { value: stream, done } = await reader.read();
        if (done) return;
        if (!this.handshakeConfirmed) {
          try {
            const mediaReader = stream?.getReader?.();
            if (mediaReader) {
              void Promise.resolve(mediaReader.cancel?.('handshake-required'))
                .catch(() => {})
                .finally(() => mediaReader.releaseLock?.());
            } else void Promise.resolve(stream?.cancel?.('handshake-required')).catch(() => {});
          } catch {
            // The structural close below remains authoritative.
          }
          this.#fatal('handshake-required', new Error('media-before-handshake'));
          return;
        }
        this.#startIncoming(stream);
      }
    } catch (error) {
      if (!this.closed) this.#fatal('media-accept-failed', error);
    } finally {
      reader.releaseLock?.();
    }
  }

  async #readDatagrams() {
    const readable = this.session.datagrams?.readable;
    if (!readable?.getReader) return;
    const reader = readable.getReader();
    this.datagramReader = reader;
    void reader.closed?.catch?.(() => {});
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        if (!this.handshakeConfirmed) {
          this.#fatal('handshake-required', new Error('datagram-before-handshake'));
          return;
        }
        this.#acceptDatagram(value);
      }
    } catch (error) {
      if (!this.closed) this.#fatal('datagram-accept-failed', error);
    } finally {
      if (this.datagramReader === reader) this.datagramReader = null;
      reader.releaseLock?.();
    }
  }

  #acceptDatagram(value) {
    this.stats.datagramFragmentsReceived++;
    let fragment;
    try {
      fragment = parseMediaDatagram(value);
    } catch (error) {
      this.#peerFailure('framing');
      try {
        this.onDiagnostic?.({ reason: 'datagram-invalid', detail: error?.message });
      } catch {
        // Diagnostics never alter wire state.
      }
      return;
    }
    const header = {
      slot: fragment.slot,
      laneClass: fragment.laneClass,
      requiredControlSeq: fragment.requiredControlSeq,
    };
    if (this.acceptMediaHeader && !this.acceptMediaHeader(header)) return;
    if (fragment.requiredControlSeq < this.#epochFloor(fragment.slot)) return;

    const now = Date.now();
    for (const [recentKey, expiresAt] of this.recentDatagrams) {
      if (expiresAt > now) break;
      this.recentDatagrams.delete(recentKey);
    }
    const key = `${fragment.slot}:${fragment.laneClass}:${fragment.requiredControlSeq}:${fragment.sequence}`;
    if ((this.recentDatagrams.get(key) ?? 0) > now) return;
    let assembly = this.datagramAssemblies.get(key);
    if (!assembly) {
      if (
        this.datagramAssemblies.size >= MAX_DATAGRAM_ASSEMBLIES ||
        this.datagramAssemblyBytes + fragment.payload.byteLength > MAX_DATAGRAM_ASSEMBLY_BYTES
      ) {
        try {
          this.onDiagnostic?.({ reason: 'datagram-assembly-cap', slot: fragment.slot });
        } catch {
          // Cap is a recoverable media drop.
        }
        return;
      }
      assembly = {
        key,
        slot: fragment.slot,
        laneClass: fragment.laneClass,
        sequence: fragment.sequence,
        requiredControlSeq: fragment.requiredControlSeq,
        totalLength: fragment.totalLength,
        type: fragment.type,
        afterKeyframe: fragment.afterKeyframe,
        recovery: fragment.recovery,
        fragmentCount: fragment.fragmentCount,
        fragments: new Map(),
        parity: null,
        receivedBytes: 0,
        retainedBytes: 0,
        timer: null,
      };
      assembly.timer = setTimeout(
        () => this.#dropDatagramAssembly(assembly, 'datagram-expired'),
        fragment.recovery ? RECOVERY_DATAGRAM_ASSEMBLY_MS : DATAGRAM_ASSEMBLY_MS,
      );
      this.datagramAssemblies.set(key, assembly);
    } else if (
      assembly.requiredControlSeq !== fragment.requiredControlSeq ||
      assembly.totalLength !== fragment.totalLength ||
      assembly.type !== fragment.type ||
      assembly.afterKeyframe !== fragment.afterKeyframe ||
      assembly.recovery !== fragment.recovery ||
      assembly.fragmentCount !== fragment.fragmentCount
    ) {
      this.#dropDatagramAssembly(assembly, 'datagram-metadata-conflict');
      this.#peerFailure('framing');
      return;
    }

    if (fragment.parity) {
      if (assembly.parity) return;
      if (this.datagramAssemblyBytes + fragment.payload.byteLength > MAX_DATAGRAM_ASSEMBLY_BYTES) {
        this.#dropDatagramAssembly(assembly, 'datagram-assembly-cap');
        return;
      }
      assembly.parity = fragment.payload;
      assembly.retainedBytes += fragment.payload.byteLength;
      this.datagramAssemblyBytes += fragment.payload.byteLength;
    } else {
      if (assembly.fragments.has(fragment.fragmentIndex)) return;
      if (assembly.receivedBytes + fragment.payload.byteLength > assembly.totalLength) {
        this.#dropDatagramAssembly(assembly, 'datagram-length-invalid');
        this.#peerFailure('framing');
        return;
      }
      if (this.datagramAssemblyBytes + fragment.payload.byteLength > MAX_DATAGRAM_ASSEMBLY_BYTES) {
        this.#dropDatagramAssembly(assembly, 'datagram-assembly-cap');
        return;
      }
      assembly.fragments.set(fragment.fragmentIndex, fragment.payload);
      assembly.receivedBytes += fragment.payload.byteLength;
      assembly.retainedBytes += fragment.payload.byteLength;
      this.datagramAssemblyBytes += fragment.payload.byteLength;
    }
    if (
      assembly.fragments.size !== assembly.fragmentCount &&
      !(assembly.parity && assembly.fragments.size === assembly.fragmentCount - 1)
    ) {
      return;
    }
    if (assembly.fragments.size === assembly.fragmentCount - 1) {
      let missing = -1;
      for (let index = 0; index < assembly.fragmentCount; index++) {
        if (!assembly.fragments.has(index)) {
          missing = index;
          break;
        }
      }
      const missingLength = assembly.totalLength - assembly.receivedBytes;
      if (missing < 0 || missingLength < 1 || missingLength > assembly.parity.byteLength) {
        this.#dropDatagramAssembly(assembly, 'datagram-fec-invalid');
        this.#peerFailure('framing');
        return;
      }
      const recovered = assembly.parity.slice(0, missingLength);
      for (const chunk of assembly.fragments.values()) {
        const overlap = Math.min(chunk.byteLength, recovered.byteLength);
        for (let index = 0; index < overlap; index++) recovered[index] ^= chunk[index];
      }
      assembly.fragments.set(missing, recovered);
      assembly.receivedBytes += recovered.byteLength;
      this.stats.datagramFramesRecovered++;
    }
    if (assembly.receivedBytes !== assembly.totalLength) {
      this.#dropDatagramAssembly(assembly, 'datagram-length-invalid');
      this.#peerFailure('framing');
      return;
    }

    const payload = new Uint8Array(assembly.totalLength);
    let offset = 0;
    for (let index = 0; index < assembly.fragmentCount; index++) {
      const chunk = assembly.fragments.get(index);
      if (!chunk) return;
      payload.set(chunk, offset);
      offset += chunk.byteLength;
    }
    this.#dropDatagramAssembly(assembly, null);
    this.recentDatagrams.set(key, now + RECENT_DATAGRAM_MS);
    while (this.recentDatagrams.size > MAX_RECENT_DATAGRAMS) {
      this.recentDatagrams.delete(this.recentDatagrams.keys().next().value);
    }
    if (
      payload[0] !== assembly.slot ||
      payload[1] !== assembly.type ||
      assembly.laneClass !== (assembly.type === 3 ? 1 : 0)
    ) {
      this.#peerFailure('framing');
      return;
    }
    this.stats.datagramFramesReassembled++;
    this.stats.incoming++;
    this.#acceptMedia({
      slot: assembly.slot,
      laneClass: assembly.laneClass,
      sequence: assembly.sequence,
      requiredControlSeq: assembly.requiredControlSeq,
      flags: assembly.recovery ? MEDIA_FLAG_RECOVERY : 0,
      type: assembly.type,
      timestamp: timestampOf(payload),
      payload: payload.buffer,
      datagram: true,
      afterKeyframe: assembly.afterKeyframe,
    });
  }

  #dropDatagramAssembly(assembly, reason) {
    if (!assembly || this.datagramAssemblies.get(assembly.key) !== assembly) return;
    clearTimeout(assembly.timer);
    this.datagramAssemblies.delete(assembly.key);
    this.datagramAssemblyBytes = Math.max(0, this.datagramAssemblyBytes - assembly.retainedBytes);
    if (reason) {
      if (reason === 'datagram-expired') this.stats.datagramAssembliesExpired++;
      try {
        this.onDiagnostic?.({ reason, slot: assembly.slot });
      } catch {
        // Expiration is a recoverable media drop.
      }
      if (reason === 'datagram-expired' && assembly.laneClass === 0) {
        const lane = this.#laneOf({ slot: assembly.slot, laneClass: assembly.laneClass });
        if (!lane.gapTimer) this.#requestLaneRecovery(lane);
      }
    }
  }

  #startIncoming(stream) {
    if (this.closed || !stream?.getReader) return;
    if (this.incoming.size >= MAX_INCOMING_MEDIA) {
      const oldest = this.provisional[0] ?? this.incoming.values().next().value;
      if (oldest) this.#resetIncoming(oldest, 'media-stream-cap', 'incomplete');
    }
    if (this.closed) return;

    const reader = stream.getReader();
    void reader.closed?.catch?.(() => {});
    const record = {
      stream,
      reader,
      bytes: new Uint8Array(),
      header: null,
      done: false,
      headerTimer: null,
      idleTimer: null,
      finTimer: null,
      totalTimer: null,
    };
    this.incoming.add(record);
    this.provisional.push(record);
    record.headerTimer = setTimeout(
      () => this.#resetIncoming(record, 'media-header-timeout', 'incomplete'),
      HEADER_TIMEOUT_MS,
    );
    record.totalTimer = setTimeout(
      () => this.#resetIncoming(record, 'media-total-timeout', 'incomplete'),
      INCOMING_TOTAL_MS,
    );

    if (this.provisional.length > MAX_PROVISIONAL) {
      this.#resetIncoming(this.provisional[0], 'media-provisional-cap', 'incomplete');
    }
    void this.#consumeIncoming(record);
  }

  async #consumeIncoming(record) {
    try {
      for (;;) {
        const { value, done } = await record.reader.read();
        if (record.done) return;
        if (done) {
          if (!record.header || record.bytes.byteLength !== record.header.total) {
            this.#resetIncoming(record, 'media-stream-incomplete', 'incomplete');
            return;
          }
          let frame;
          try {
            frame = parseMedia(record.bytes);
          } catch (error) {
            this.#resetIncoming(record, error.message, 'framing');
            return;
          }
          this.#cleanupIncoming(record);
          this.stats.incoming++;
          this.#acceptMedia(frame);
          return;
        }

        const chunk = bytesOf(value);
        if (!chunk.byteLength) continue;
        if (this.incomingBytes + chunk.byteLength > MAX_MEDIA_QUEUE) {
          this.#resetIncoming(record, 'media-receive-cap', 'incomplete');
          return;
        }
        this.incomingBytes += chunk.byteLength;
        record.bytes = concatBytes(record.bytes, chunk);
        if (record.bytes.byteLength > MEDIA_HEADER + MAX_MEDIA) {
          this.#resetIncoming(record, 'media-stream-too-large', 'framing');
          return;
        }

        if (!record.header && record.bytes.byteLength >= MEDIA_HEADER) {
          try {
            record.header = mediaHeader(record.bytes);
          } catch (error) {
            this.#resetIncoming(record, error.message, 'framing');
            return;
          }
          clearTimeout(record.headerTimer);
          record.headerTimer = null;
          this.provisional = this.provisional.filter((entry) => entry !== record);
          if (this.acceptMediaHeader && !this.acceptMediaHeader(record.header)) {
            // Papel/slot são decididos pelo servidor antes do payload. Um viewer
            // malicioso não ganha uma lane nem memória só por usar framing válido.
            this.#resetIncoming(record, 'media-unauthorized', null);
            return;
          }
          if (record.header.requiredControlSeq < this.#epochFloor(record.header.slot)) {
            this.#cancelIncomingEpoch(record);
            return;
          }
        }

        if (!record.header) continue;
        if (record.bytes.byteLength > record.header.total) {
          this.#resetIncoming(record, 'media-length-invalid', 'framing');
          return;
        }
        clearTimeout(record.idleTimer);
        clearTimeout(record.finTimer);
        if (record.bytes.byteLength === record.header.total) {
          record.finTimer = setTimeout(
            () => this.#resetIncoming(record, 'media-fin-timeout', 'incomplete'),
            FIN_TIMEOUT_MS,
          );
        } else {
          record.idleTimer = setTimeout(
            () => this.#resetIncoming(record, 'media-payload-timeout', 'incomplete'),
            PAYLOAD_IDLE_MS,
          );
        }
      }
    } catch (error) {
      if (!record.done && !this.closed) {
        this.#resetIncoming(record, error?.message ?? 'media-read-failed', 'incomplete');
      }
    } finally {
      record.reader.releaseLock?.();
    }
  }

  #resetIncoming(record, reason, category) {
    if (!record || record.done) return;
    this.#cleanupIncoming(record);
    this.stats.resets++;
    try {
      void Promise.resolve(record.reader.cancel?.(reason)).catch(() => {});
    } catch {
      try {
        void Promise.resolve(record.stream.cancel?.(reason)).catch(() => {});
      } catch {
        // Reset accounting is authoritative even if the fake/native stream raced closed.
      }
    }
    if (category) this.#peerFailure(category);
  }

  #cleanupIncoming(record) {
    if (record.done) return;
    record.done = true;
    clearTimeout(record.headerTimer);
    clearTimeout(record.idleTimer);
    clearTimeout(record.finTimer);
    clearTimeout(record.totalTimer);
    this.incoming.delete(record);
    this.provisional = this.provisional.filter((entry) => entry !== record);
    this.incomingBytes = Math.max(0, this.incomingBytes - record.bytes.byteLength);
    record.bytes = new Uint8Array();
  }

  #peerFailure(category) {
    const values = category === 'framing' ? this.peerFraming : this.peerIncomplete;
    const limit = category === 'framing' ? 9 : 65;
    const now = Date.now();
    trimFailures(values, now).push(now);
    if (values.length >= limit) {
      const reason = category === 'framing' ? 'media-framing-abuse' : 'media-stream-abuse';
      this.#fatal(reason, new Error(reason));
    }
  }

  #acceptMedia(frame) {
    if (this.closed) return;
    if (frame.requiredControlSeq < this.#epochFloor(frame.slot)) return;
    if (frame.requiredControlSeq > this.controlIn) {
      const item = { frame, timer: null, bytes: frame.payload.byteLength };
      if (
        this.barrier.length >= MAX_BARRIER_ITEMS ||
        this.barrierBytes + item.bytes > MAX_BARRIER_BYTES
      ) {
        this.#fatal('barrier-overflow', new Error('barrier-overflow'));
        return;
      }
      this.barrier.push(item);
      this.barrierBytes += item.bytes;
      item.timer = setTimeout(() => {
        if (!this.barrier.includes(item)) return;
        this.#fatal('barrier-timeout', new Error('barrier-timeout'));
      }, BARRIER_TIMEOUT_MS);
      return;
    }
    this.#acceptReadyMedia(frame);
  }

  #acceptReadyMedia(frame) {
    if (frame.datagram && !this.peerDatagrams) {
      this.#peerFailure('framing');
      try {
        this.onDiagnostic?.({ reason: 'datagram-not-negotiated', slot: frame.slot });
      } catch {
        // Diagnostics never alter wire state.
      }
      return;
    }
    this.#acceptLane(frame);
  }

  #releaseBarrier() {
    const ready = [];
    const waiting = [];
    for (const item of this.barrier) {
      if (item.frame.requiredControlSeq <= this.controlIn) ready.push(item);
      else waiting.push(item);
    }
    this.barrier = waiting;
    for (const item of ready) {
      clearTimeout(item.timer);
      this.barrierBytes = Math.max(0, this.barrierBytes - item.bytes);
      this.#acceptReadyMedia(item.frame);
    }
  }

  #clearBarrier() {
    for (const item of this.barrier) clearTimeout(item.timer);
    this.barrier = [];
    this.barrierBytes = 0;
  }

  #laneOf(frame) {
    const key = `${frame.slot}:${frame.laneClass}`;
    let lane = this.lanes.get(key);
    if (!lane) {
      lane = {
        // O slot fica NA lane, e não só na chave: quem pede o keyframe precisa
        // dizer de qual transmissão ele é. Sem isso o pedido chega ao peer sem
        // endereço, e ele não tem como saber qual decodificador ficou sem
        // ponto de partida.
        slot: frame.slot,
        delivered: 0,
        timestamp: null,
        primed: false,
        recovering: false,
        pending: new Map(),
        pendingBytes: 0,
        gapTimer: null,
        gapRepeatMs: MEDIA_GAP_MS,
      };
      this.lanes.set(key, lane);
    }
    return lane;
  }

  #dropLane(key, lane) {
    clearTimeout(lane.gapTimer);
    lane.gapTimer = null;
    this.#clearLanePending(lane);
    this.lanes.delete(key);
  }

  #deletePending(lane, sequence) {
    const frame = lane.pending.get(sequence);
    if (!frame) return null;
    lane.pending.delete(sequence);
    const bytes = frame.payload.byteLength;
    lane.pendingBytes = Math.max(0, lane.pendingBytes - bytes);
    this.pendingItems = Math.max(0, this.pendingItems - 1);
    this.pendingBytes = Math.max(0, this.pendingBytes - bytes);
    return frame;
  }

  #clearLanePending(lane, through = Infinity) {
    for (const sequence of [...lane.pending.keys()]) {
      if (sequence <= through) this.#deletePending(lane, sequence);
    }
  }

  #queuePending(lane, frame) {
    if (lane.pending.has(frame.sequence)) return false;
    const bytes = frame.payload.byteLength;
    if (
      lane.pending.size >= MAX_LANE_PENDING_ITEMS ||
      lane.pendingBytes + bytes > MAX_LANE_PENDING_BYTES ||
      this.pendingItems >= MAX_INCOMING_MEDIA ||
      this.pendingBytes + bytes > MAX_MEDIA_QUEUE
    ) {
      this.#clearLanePending(lane);
      lane.primed = false;
      lane.recovering = true;
      try {
        this.onDiagnostic?.({ reason: 'pending-cap', slot: lane.slot });
      } catch {
        // Diagnostics never alter wire state.
      }
      this.#armGap(lane);
      return false;
    }
    lane.pending.set(frame.sequence, frame);
    lane.pendingBytes += bytes;
    this.pendingItems++;
    this.pendingBytes += bytes;
    return true;
  }

  #isLate(lane, frame) {
    const late =
      frame.sequence <= lane.delivered ||
      (frame.timestamp !== null && lane.timestamp !== null && frame.timestamp < lane.timestamp);
    if (late) this.stats.late++;
    return late;
  }

  #deliver(lane, frame) {
    if (this.#isLate(lane, frame)) return false;
    lane.delivered = frame.sequence;
    if (frame.timestamp !== null) lane.timestamp = frame.timestamp;
    this.stats.delivered++;
    if (frame.type === 1) this.stats.keyframesDelivered++;
    this.onMessage?.(frame.payload, true);
    return true;
  }

  #acceptLane(frame) {
    const lane = this.#laneOf(frame);
    if (this.#isLate(lane, frame)) return;

    if (frame.laneClass === 1) {
      this.#deliver(lane, frame);
      return;
    }

    if (frame.type === 1 && (((frame.flags ?? 0) & MEDIA_FLAG_RECOVERY) !== 0 || lane.recovering)) {
      // Uma ancora explicitamente marcada como recuperacao pode ultrapassar o
      // buraco que veio corrigir. Keyframes comuns continuam ordenados: em rede
      // limpa, reordenacao de streams QUIC nao pode virar descarte artificial.
      if (!this.#deliver(lane, frame)) return;
      if (((frame.flags ?? 0) & MEDIA_FLAG_RECOVERY) !== 0) {
        this.#sendControl(JSON.stringify({ type: 'media-recovered', slot: frame.slot }));
      }
      lane.primed = true;
      lane.recovering = false;
      this.#clearLanePending(lane, frame.sequence);
      clearTimeout(lane.gapTimer);
      lane.gapTimer = null;
      this.#drainLane(lane);
      return;
    }

    if (frame.type === 1 && frame.sequence === lane.delivered + 1) {
      if (!this.#deliver(lane, frame)) return;
      lane.primed = true;
      this.#drainLane(lane);
      return;
    }

    if (frame.type === 2 && lane.primed && frame.sequence === lane.delivered + 1) {
      if (this.#deliver(lane, frame)) this.#drainLane(lane);
      return;
    }

    if (this.#queuePending(lane, frame)) {
      const delay = frame.datagram
        ? lane.primed && !frame.afterKeyframe
          ? DATAGRAM_HOT_GAP_MS
          : DATAGRAM_GAP_MS
        : MEDIA_GAP_MS;
      this.#armGap(lane, delay);
    }
  }

  /**
   * O buraco durou MEDIA_GAP_MS: o que está pendurado já não serve.
   *
   * O pedido de keyframe se repete a cada janela enquanto nenhum chegar, porque
   * um pedido pode se perder — no caminho, ou num transmissor que estava com o
   * relay desligado. Um pedido só, e a lane ficaria preta para sempre esperando
   * uma resposta que ninguém mais vai mandar.
   *
   * A cadência é o próprio timer: enquanto ele está armado não há segundo
   * pedido, então N frames fora de ordem custam um pedido por janela, e não um
   * por frame. Quem desarma é o keyframe que chega — ou o fim da lane.
   */
  #requestLaneRecovery(lane) {
    // Todo gap de datagrama é também feedback de perda para o emissor. Antes,
    // só o caso raro em que um keyframe já estava pendente armava a FEC; o caso
    // comum pedia outro keyframe sem proteger os deltas seguintes e repetia o
    // ciclo sob perda sustentada.
    this.#sendControl(JSON.stringify({ type: 'media-loss', slot: lane.slot }));
    const pendingKeyframe = [...lane.pending.values()]
      .filter((frame) => frame.type === 1)
      .sort((left, right) => right.sequence - left.sequence)[0];

    if (pendingKeyframe) {
      // O keyframe já atravessou o QUIC e só estava esperando uma sequência que
      // não virá. Quando o gap vence, ele é exatamente a âncora de recuperação:
      // descartar esse frame para pedir outro adicionava um ciclo completo de
      // keyframe + rede ao congelamento observado sob perda.
      // O emissor não enxerga de modo confiável a perda de datagramas na rede
      // pelos contadores nativos. Avise que houve perda mesmo quando este
      // keyframe já em voo basta para recuperar, para manter a FEC dos deltas
      // ativa sem pedir outro keyframe redundante.
      this.#clearLanePending(lane, pendingKeyframe.sequence);
      lane.primed = false;
      lane.recovering = true;
      if (this.#deliver(lane, pendingKeyframe)) {
        lane.primed = true;
        lane.recovering = false;
        this.#drainLane(lane);
        if (lane.pending.size > 0 && !lane.gapTimer) {
          const hasDatagram = [...lane.pending.values()].some((frame) => frame.datagram);
          this.#armGap(lane, hasDatagram ? DATAGRAM_GAP_MS : MEDIA_GAP_MS);
        }
        return;
      }
    }

    this.#clearLanePending(lane);
    lane.primed = false;
    lane.recovering = true;
    try {
      this.onNeedKeyframe?.(lane.slot);
    } catch {
      // Pedir keyframe é recuperação, nunca causa de queda da sessão.
    }
    this.#armGap(lane, lane.gapRepeatMs);
  }

  #armGap(lane, delayMs = MEDIA_GAP_MS) {
    if (lane.gapTimer) return;
    lane.gapRepeatMs = delayMs <= DATAGRAM_GAP_MS ? DATAGRAM_GAP_MS : MEDIA_GAP_MS;
    lane.gapTimer = setTimeout(() => {
      lane.gapTimer = null;
      this.#requestLaneRecovery(lane);
    }, delayMs);
  }

  #drainLane(lane) {
    for (;;) {
      const next = lane.pending.get(lane.delivered + 1);
      if (!next) {
        if (lane.pending.size === 0) {
          clearTimeout(lane.gapTimer);
          lane.gapTimer = null;
        }
        return;
      }
      if (next.type === 2 && !lane.primed) return;
      this.#deletePending(lane, next.sequence);
      if (!this.#deliver(lane, next)) return;
      if (next.type === 1) lane.primed = true;
    }
  }

  #fatal(reason, error) {
    if (this.closed) return;
    try {
      this.onError?.(error);
    } catch {
      // Error observers cannot change shutdown.
    }
    this.#clearBarrier();
    this.close({ closeCode: 1011, reason });
  }

  #finish(info) {
    if (this.finished) return;
    this.finished = true;
    this.closed = true;
    clearTimeout(this.datagramStatsTimer);
    this.datagramStatsTimer = null;
    this.#clearBarrier();
    const cancelReason = String(info?.reason || 'transport-closed');
    for (const reader of [this.controlReader, this.datagramReader]) {
      if (!reader) continue;
      try {
        void Promise.resolve(reader.cancel?.(cancelReason)).catch(() => {});
      } catch {
        // Reader cleanup is best-effort after the owning session is closed.
      }
    }
    for (const lane of this.lanes.values()) clearTimeout(lane.gapTimer);
    for (const record of [...this.incoming]) {
      this.#cleanupIncoming(record);
      try {
        void Promise.resolve(record.reader.cancel?.('transport-closed')).catch(() => {});
      } catch {
        // Native reader already closed.
      }
    }
    for (const item of [...this.mediaItems]) this.#obsolete(item);
    for (const item of [...this.datagramItems]) this.#settleDatagram(item);
    for (const assembly of [...this.datagramAssemblies.values()]) {
      this.#dropDatagramAssembly(assembly, null);
    }
    try {
      void Promise.resolve(this.datagramWriter?.close?.()).catch(() => {});
    } catch {
      // Session close remains authoritative.
    }
    this.controlItems = 0;
    this.controlBytes = 0;
    this.#publishBuffered();
    this.handshake.reject?.(new Error('transport-closed'));
    if (globalThis.process?.env?.WT_WIRE_DEBUG === '1') {
      console.info('[webtransport-wire]', JSON.stringify(this.stats));
    }
    this.onClose?.(info ?? { closeCode: 0, reason: '' });
  }
}

export async function openClientWireSession(session, callbacks = {}) {
  const stream = await session.createBidirectionalStream();
  const endpoint = new WireEndpoint({ session, stream, initiator: true, ...callbacks }).start();
  await endpoint.ready;
  return endpoint;
}

export async function acceptServerWireSession(session, callbacks = {}, options = {}) {
  const timeoutMs = Math.max(1, Number(options.handshakeTimeoutMs) || SERVER_HANDSHAKE_TIMEOUT_MS);
  const reader = session.incomingBidirectionalStreams.getReader();
  void reader.closed?.catch?.(() => {});
  let released = false;
  let endpoint = null;
  let timer;
  const release = () => {
    if (released) return;
    released = true;
    try {
      reader.releaseLock?.();
    } catch {
      // A native timeout may have released the reader while cancel settled.
    }
  };
  const timeoutError = Object.assign(new Error('handshake-timeout'), {
    reason: 'handshake-timeout',
  });
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => reject(timeoutError), timeoutMs);
  });
  try {
    const { value: stream, done } = await Promise.race([reader.read(), deadline]);
    release();
    if (done || !stream) throw new Error('control-stream-missing');
    const acceptMediaHeader =
      callbacks.acceptMediaHeader ??
      (() => {
        const data = session.userData;
        if (!data?.auth) return true;
        return data.auth.role === 'broadcaster' && data.control !== true;
      });
    endpoint = new WireEndpoint({
      session,
      stream,
      initiator: false,
      ...callbacks,
      acceptMediaHeader,
    }).start();
    await Promise.race([endpoint.ready, deadline]);
    clearTimeout(timer);
    return endpoint;
  } catch (error) {
    clearTimeout(timer);
    const reason = error === timeoutError ? 'handshake-timeout' : 'session-invalid';
    if (endpoint) {
      endpoint.close({ closeCode: 1, reason });
    } else {
      closeWireSession(session, { closeCode: 1, reason });
      // The adapter owns this aggregate read independently of session.closed.
      // The optional addon fork makes its later native controller-close effect
      // idempotent, so cancellation is always safe and never waits on a public
      // closed promise that may remain pending forever.
      let cancellation;
      try {
        cancellation = Promise.resolve(reader.cancel?.(reason)).catch(() => {});
      } catch {
        cancellation = Promise.resolve();
      }
      // Keep `closing` observable as a distinct owner state and give the
      // cancellation one bounded cleanup turn. A non-conforming cancel promise
      // cannot extend bind settlement beyond this fence.
      void cancellation;
      await new Promise((resolve) => setTimeout(resolve, SERVER_CLEANUP_LIMIT_MS));
    }
    throw error;
  } finally {
    clearTimeout(timer);
    release();
  }
}

export { invalidState };
