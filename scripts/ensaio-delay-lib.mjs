/**
 * Nucleo matematico do ensaio de delay em rede real.
 *
 * Este modulo nao abre browser nem altera rede. Ele concentra o contrato que
 * precisa permanecer fixo antes das medicoes: codec do relogio visual,
 * estatistica robusta e classificacao PASS/FAIL/INCONCLUSIVO.
 */

export const GRID_COLS = 16;
export const GRID_ROWS = 4;
export const CLOCK_MAGIC = 0xd1;

export const CRITERIOS = Object.freeze({
  amostrasValidasPct: 90,
  picoMediana1sMs: 2000,
  picoAmostraMs: 2500,
  margemP95SustentadoMs: 1000,
  inclinacaoMaxMsPorS: 10,
  crescimentoSustentadoMaxMs: 200,
  recuperacaoPrazoMs: 12_000,
  recuperacaoJanelaMs: 3000,
  recuperacaoMargemP95Ms: 100,
  recuperacaoMargemPosteriorMs: 200,
  jitterMinimoMs: 20,
  esperaMaxMs: 300,
  esperaCrescimentoMinMs: 20,
  esperaRecuperacaoMargemMs: 50,
  cpuPicoPct: 90,
  cpuPicoDuracaoMs: 2000,
  memoriaLivreMinBytes: 1024 ** 3,
  calibracaoSaltoMinMs: 350,
  calibracaoFinalMinMs: 1200,
  impairmentFinalMinMs: 900,
});

function bytesDoTimestamp(timestampMs) {
  const n = BigInt(Math.max(0, Math.round(Number(timestampMs))));
  const bytes = new Uint8Array(8);
  bytes[0] = CLOCK_MAGIC;
  for (let i = 0; i < 6; i++) {
    bytes[6 - i] = Number((n >> BigInt(i * 8)) & 0xffn);
  }
  bytes[7] = crc8(bytes.subarray(0, 7));
  return bytes;
}

export function crc8(bytes) {
  let crc = 0;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 0x80 ? ((crc << 1) ^ 0x07) & 0xff : (crc << 1) & 0xff;
    }
  }
  return crc;
}

export function encodeClockBits(timestampMs) {
  const bits = [];
  for (const byte of bytesDoTimestamp(timestampMs)) {
    for (let bit = 7; bit >= 0; bit--) bits.push((byte >> bit) & 1);
  }
  return bits;
}

export function decodeClockBits(bits) {
  if (!Array.isArray(bits) || bits.length !== GRID_COLS * GRID_ROWS) return null;
  const bytes = new Uint8Array(8);
  for (let i = 0; i < bits.length; i++) {
    if (bits[i] !== 0 && bits[i] !== 1) return null;
    bytes[Math.floor(i / 8)] |= bits[i] << (7 - (i % 8));
  }
  if (bytes[0] !== CLOCK_MAGIC || crc8(bytes.subarray(0, 7)) !== bytes[7]) return null;

  let timestamp = 0n;
  for (let i = 1; i <= 6; i++) timestamp = (timestamp << 8n) | BigInt(bytes[i]);
  const value = Number(timestamp);
  return Number.isSafeInteger(value) ? value : null;
}

const numeros = (values) => values.filter(Number.isFinite);

export function percentile(values, pct) {
  const sorted = numeros(values).sort((a, b) => a - b);
  if (!sorted.length) return null;
  if (sorted.length === 1) return sorted[0];
  const pos = (Math.min(100, Math.max(0, pct)) / 100) * (sorted.length - 1);
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (pos - lower);
}

export const median = (values) => percentile(values, 50);

export function hasStableGateTail(samples, { state = 'pass', consecutive = 5 } = {}) {
  return (
    Number.isInteger(consecutive) &&
    consecutive > 0 &&
    samples.length >= consecutive &&
    samples.slice(-consecutive).every((sample) => sample?.gate === state)
  );
}

export function isResolvedGateSample(sample) {
  return sample?.gate === 'pass' || sample?.gate === 'fail';
}

export function isExpectedPageReady(state, expectedOrigin) {
  return state?.ready === 'complete' && state?.origin === expectedOrigin;
}

export function evaluateCaptureSelection({
  runId,
  label,
  pickerSelectionName,
  displaySurface,
  markerDecoded,
}) {
  const labelMatchesRunId =
    typeof runId === 'string' &&
    runId.length > 0 &&
    typeof label === 'string' &&
    label.includes(runId);
  const browserSurface = displaySurface === 'browser';
  const pixelMarkerDecoded = markerDecoded === true;
  const pickerSelectionMatchesRunId =
    typeof runId === 'string' &&
    runId.length > 0 &&
    typeof pickerSelectionName === 'string' &&
    pickerSelectionName === `ENSAIO RELOGIO VISUAL ${runId}`;
  const sourceIdentityProven = labelMatchesRunId || pickerSelectionMatchesRunId;
  return {
    pass: sourceIdentityProven && browserSurface && pixelMarkerDecoded,
    labelMatchesRunId,
    pickerSelectionMatchesRunId,
    sourceIdentityProven,
    browserSurface,
    pixelMarkerDecoded,
  };
}

export function iqr(values) {
  const p25 = percentile(values, 25);
  const p75 = percentile(values, 75);
  return p25 === null || p75 === null ? null : p75 - p25;
}

export function theilSenSlope(points) {
  const valid = points.filter((p) => Number.isFinite(p?.x) && Number.isFinite(p?.y));
  const slopes = [];
  for (let i = 0; i < valid.length; i++) {
    for (let j = i + 1; j < valid.length; j++) {
      const dx = valid[j].x - valid[i].x;
      if (dx !== 0) slopes.push((valid[j].y - valid[i].y) / dx);
    }
  }
  return median(slopes);
}

function deFase(samples, phase) {
  return samples.filter((sample) => sample.phase === phase && Number.isFinite(sample.visualLagMs));
}

function dentro(samples, inicio, fim) {
  return samples.filter((sample) => sample.tMs >= inicio && sample.tMs <= fim);
}

function medianasMoveis(samples, field, windowMs) {
  const valid = samples.filter((sample) => Number.isFinite(sample[field]));
  return valid.map((sample) => ({
    tMs: sample.tMs,
    value: median(
      valid
        .filter((other) => Math.abs(other.tMs - sample.tMs) <= windowMs / 2)
        .map((other) => other[field]),
    ),
  }));
}

function janelaRecuperada(recovery, baselineP95, criterios) {
  if (!recovery.length) return null;
  const inicio = recovery[0].tMs;
  const limite = inicio + criterios.recuperacaoPrazoMs;
  for (const sample of recovery) {
    if (sample.tMs > limite) break;
    const window = dentro(recovery, sample.tMs, sample.tMs + criterios.recuperacaoJanelaMs);
    if (window.length < 4) continue;
    if (
      percentile(
        window.map((entry) => entry.visualLagMs),
        95,
      ) <=
      baselineP95 + criterios.recuperacaoMargemP95Ms
    ) {
      return sample.tMs;
    }
  }
  return null;
}

function temPicoDeCarga(samples, criterios) {
  const high = samples.filter(
    (sample) =>
      Number(sample.cpuPercent) >= criterios.cpuPicoPct ||
      (Number.isFinite(sample.freeMemoryBytes) &&
        sample.freeMemoryBytes < criterios.memoriaLivreMinBytes),
  );
  if (!high.length) return false;
  let start = high[0].tMs;
  let previous = high[0].tMs;
  for (const sample of high.slice(1)) {
    if (sample.tMs - previous > 1500) start = sample.tMs;
    if (sample.tMs - start >= criterios.cpuPicoDuracaoMs) return true;
    previous = sample.tMs;
  }
  return previous - start >= criterios.cpuPicoDuracaoMs;
}

function tunelInstavel(samples) {
  const probes = samples.filter(
    (sample) => sample.tunnelOk !== null && sample.tunnelOk !== undefined,
  );
  if (!probes.length) return false;
  const failures = probes.filter((sample) => sample.tunnelOk === false).length;
  const baseline = probes.filter(
    (sample) => sample.phase === 'baseline' && Number.isFinite(sample.tunnelRttMs),
  );
  const degraded = probes.filter(
    (sample) => sample.phase !== 'baseline' && Number.isFinite(sample.tunnelRttMs),
  );
  const baselineP95 = percentile(
    baseline.map((sample) => sample.tunnelRttMs),
    95,
  );
  const degradedP95 = percentile(
    degraded.map((sample) => sample.tunnelRttMs),
    95,
  );
  return (
    failures / probes.length > 0.02 ||
    (baselineP95 !== null && degradedP95 !== null && degradedP95 > baselineP95 + 500)
  );
}

function frameContinuity(samples) {
  const groups = new Map();
  for (const sample of samples) {
    if (!['baseline', 'sustain', 'recovery'].includes(sample.phase)) continue;
    const key = `${sample.phase}:${Math.floor(sample.tMs / 1000)}`;
    if (!groups.has(key)) groups.set(key, []);
    if (Number.isFinite(sample.frames)) groups.get(key).push(sample.frames);
  }
  const entries = [...groups.entries()];
  const measured = entries.filter(([, frames]) => frames.length);
  let consecutiveZero = 0;
  let maxConsecutiveZero = 0;
  let previousPhase = null;
  for (const [key, frames] of entries) {
    const phase = key.split(':')[0];
    if (phase !== previousPhase) consecutiveZero = 0;
    previousPhase = phase;
    if (frames.length && Math.max(...frames) <= 0) consecutiveZero++;
    else consecutiveZero = 0;
    maxConsecutiveZero = Math.max(maxConsecutiveZero, consecutiveZero);
  }
  return {
    observedPct: entries.length ? (measured.length / entries.length) * 100 : 0,
    maxConsecutiveZero,
  };
}

export function evaluateDelaySeries(samples, context = {}) {
  const criterios = { ...CRITERIOS, ...(context.criterios ?? {}) };
  const expected = samples.length;
  const valid = samples.filter((sample) => Number.isFinite(sample.visualLagMs));
  const validPct = expected ? (valid.length / expected) * 100 : 0;
  const baseline = deFase(samples, 'baseline');
  const sustain = deFase(samples, 'sustain');
  const recovery = deFase(samples, 'recovery');
  const baselineP95 = percentile(
    baseline.map((sample) => sample.visualLagMs),
    95,
  );

  const rollingPeak = Math.max(
    ...medianasMoveis(sustain, 'visualLagMs', 1000)
      .map((entry) => entry.value)
      .filter(Number.isFinite),
    -Infinity,
  );
  const samplePeak = Math.max(...sustain.map((sample) => sample.visualLagMs), -Infinity);
  const sustainP95 = percentile(
    sustain.map((sample) => sample.visualLagMs),
    95,
  );
  const sustainStart = sustain[0]?.tMs ?? 0;
  const sustainEnd = sustain.at(-1)?.tMs ?? 0;
  const tail = dentro(sustain, Math.max(sustainStart, sustainEnd - 20_000), sustainEnd);
  const slope = theilSenSlope(
    tail.map((sample) => ({ x: sample.tMs / 1000, y: sample.visualLagMs })),
  );
  const first5 = dentro(sustain, sustainStart, sustainStart + 5000);
  const last5 = dentro(sustain, Math.max(sustainStart, sustainEnd - 5000), sustainEnd);
  const sustainedGrowth =
    median(last5.map((sample) => sample.visualLagMs)) -
    median(first5.map((sample) => sample.visualLagMs));
  const recoveredAt =
    baselineP95 === null ? null : janelaRecuperada(recovery, baselineP95, criterios);
  const recoveryAfter =
    recoveredAt === null ? [] : recovery.filter((sample) => sample.tMs >= recoveredAt);
  const recoveryPostP95 = percentile(
    recoveryAfter.map((sample) => sample.visualLagMs),
    95,
  );

  const jitterBase = median(baseline.map((sample) => sample.jitterMs));
  const jitterDegraded = median(sustain.map((sample) => sample.jitterMs));
  const jitterRecovery = median(recovery.map((sample) => sample.jitterMs));
  const jitterDelta =
    jitterBase === null || jitterDegraded === null ? null : jitterDegraded - jitterBase;
  const wait = (sample) =>
    Number.isFinite(sample.visualLagMs) && Number.isFinite(sample.arrivalLagMs)
      ? sample.visualLagMs - sample.arrivalLagMs
      : null;
  const waitBase = median(baseline.map(wait));
  const waitDegraded = median(sustain.map(wait));
  const waitRecovery = median(recovery.map(wait));
  const waitDelta = waitBase === null || waitDegraded === null ? null : waitDegraded - waitBase;
  const frameState = frameContinuity(samples);

  const checks = {
    amostrasValidas: validPct >= criterios.amostrasValidasPct,
    framesObservados: frameState.observedPct >= 70,
    framesContinuos: frameState.maxConsecutiveZero < 2,
    baselineSuficiente: baseline.length >= 20,
    sustainSuficiente: sustain.length >= 20,
    picoMediana1s: rollingPeak <= criterios.picoMediana1sMs,
    picoAmostra: samplePeak < criterios.picoAmostraMs,
    p95Sustentado:
      baselineP95 !== null &&
      sustainP95 !== null &&
      sustainP95 <= baselineP95 + criterios.margemP95SustentadoMs,
    semInclinacaoAcumulativa: slope !== null && slope <= criterios.inclinacaoMaxMsPorS,
    semCrescimentoSustentado: sustainedGrowth <= criterios.crescimentoSustentadoMaxMs,
    recuperouNoPrazo: recoveredAt !== null,
    permaneceuRecuperado:
      baselineP95 !== null &&
      recoveryPostP95 !== null &&
      recoveryPostP95 <= baselineP95 + criterios.recuperacaoMargemPosteriorMs,
    jitterEvidenciado: jitterDelta !== null && jitterDelta >= criterios.jitterMinimoMs,
    esperaAdaptou:
      waitDelta !== null &&
      waitDelta >= criterios.esperaCrescimentoMinMs &&
      waitDegraded <= criterios.esperaMaxMs,
    jitterRecuperou:
      jitterBase !== null &&
      jitterRecovery !== null &&
      jitterRecovery <= jitterBase + criterios.jitterMinimoMs,
    esperaRecuperou:
      waitBase !== null &&
      waitRecovery !== null &&
      waitRecovery <= waitBase + criterios.esperaRecuperacaoMargemMs,
  };

  const delayChecks = Object.entries(checks).filter(
    ([name]) =>
      !['jitterEvidenciado', 'esperaAdaptou', 'jitterRecuperou', 'esperaRecuperou'].includes(name),
  );
  const jitterChecks = Object.entries(checks).filter(([name]) =>
    ['jitterEvidenciado', 'esperaAdaptou', 'jitterRecuperou', 'esperaRecuperou'].includes(name),
  );
  const delayPass = delayChecks.every(([, passed]) => passed);
  const jitterPass = jitterChecks.every(([, passed]) => passed);

  const confounds = [];
  if (context.instrumentOk === false) confounds.push('medidor');
  if (context.impairmentOk === false) confounds.push('impairment');
  if (context.qualityReady === false) confounds.push('qualidade-preexistente');
  if (
    context.transport === 'webtransport' &&
    (context.gateBefore !== 'verde' || context.gateAfter !== 'verde')
  ) {
    confounds.push('gate');
  }
  if (!delayPass && temPicoDeCarga(samples, criterios)) confounds.push('carga');
  if (
    !delayPass &&
    context.transport !== 'webrtc' &&
    String(context.path ?? '').includes('tunnel') &&
    tunelInstavel(samples)
  ) {
    confounds.push('tunel');
  }

  return {
    status: confounds.length ? 'INCONCLUSIVO' : delayPass ? 'PASS' : 'FAIL',
    reason: confounds.length
      ? confounds.join('+')
      : delayPass
        ? 'criterios atendidos'
        : 'criterios violados',
    delayPass,
    jitterStatus: jitterPass
      ? 'PASS'
      : checks.jitterEvidenciado
        ? 'FAIL'
        : 'INCONCLUSIVO/jitter-nao-evidenciado',
    checks,
    metrics: {
      validPct,
      baselineP95,
      rollingPeak,
      samplePeak,
      sustainP95,
      slopeMsPerS: slope,
      sustainedGrowth,
      recoveredInMs: recoveredAt === null ? null : recoveredAt - (recovery[0]?.tMs ?? recoveredAt),
      recoveryPostP95,
      jitterBase,
      jitterDegraded,
      jitterRecovery,
      jitterDelta,
      waitBase,
      waitDegraded,
      waitRecovery,
      waitDelta,
      visualLagMedian: median(valid.map((sample) => sample.visualLagMs)),
      visualLagIqr: iqr(valid.map((sample) => sample.visualLagMs)),
      frameWindowsObservedPct: frameState.observedPct,
      maxConsecutiveZeroFrameWindows: frameState.maxConsecutiveZero,
    },
    confounds,
  };
}

export function evaluateCalibration(
  samples,
  {
    finalMinimum = CRITERIOS.calibracaoFinalMinMs,
    stepMinimum = CRITERIOS.calibracaoSaltoMinMs,
  } = {},
) {
  const delays = [...new Set(samples.map((sample) => sample.configuredDelayMs))].sort(
    (a, b) => a - b,
  );
  const plateaus = delays.map((delay) => ({
    configuredDelayMs: delay,
    medianLagMs: median(
      samples
        .filter((sample) => sample.configuredDelayMs === delay)
        .map((sample) => sample.visualLagMs),
    ),
  }));
  const steps = plateaus
    .slice(1)
    .map((entry, index) => entry.medianLagMs - plateaus[index].medianLagMs);
  const finalDelta =
    plateaus.length >= 2 ? plateaus.at(-1).medianLagMs - plateaus[0].medianLagMs : null;
  const pass =
    plateaus.length >= 4 &&
    plateaus.every((entry) => Number.isFinite(entry.medianLagMs)) &&
    steps.every((step) => step >= stepMinimum) &&
    finalDelta >= finalMinimum;
  return { pass, plateaus, steps, finalDelta };
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function toCsv(records) {
  if (!records.length) return '';
  const keys = [...new Set(records.flatMap((record) => Object.keys(record)))];
  return [
    keys.join(','),
    ...records.map((record) => keys.map((key) => csvCell(record[key])).join(',')),
  ].join('\n');
}
