/**
 * Quality gate de chegada.
 *
 * O teto em bit/s não é usado como piso: vídeo de tela quase estático comprime
 * muito abaixo do teto sem perder qualidade. O gate exige fluxo vivo, cadência
 * de quadros e, quando o transporte expõe, perda/descarte dentro do limite.
 */
export const QUALITY_GATE_LIMITS = Object.freeze({
  minimumBitrateBps: 32_000,
  minimumFpsRatio: 0.6,
  minimumFpsAbsolute: 5,
  maximumPacketLossPct: 5,
  maximumDroppedFramesPct: 10,
  samplesToSettle: 3,
});

const finite = (value) => Number.isFinite(value) && value >= 0;

export function evaluateArrivalQuality(sample = {}, target = {}) {
  const bitrateBps = Number(sample.bitrateBps);
  const fps = Number(sample.fps);
  const targetFps = Number(target.fps);

  if (!finite(bitrateBps) || !finite(fps) || !finite(targetFps) || targetFps <= 0) {
    return { measurable: false, pass: false, reasons: ['aguardando-amostra'] };
  }

  const minimumFps = Math.max(
    QUALITY_GATE_LIMITS.minimumFpsAbsolute,
    targetFps * QUALITY_GATE_LIMITS.minimumFpsRatio,
  );
  const reasons = [];
  if (bitrateBps < QUALITY_GATE_LIMITS.minimumBitrateBps) reasons.push('sem-fluxo-de-bits');
  if (fps < minimumFps) reasons.push('fps-baixo');
  if (
    finite(sample.packetLossPct) &&
    sample.packetLossPct > QUALITY_GATE_LIMITS.maximumPacketLossPct
  )
    reasons.push('perda-alta');
  if (
    finite(sample.droppedFramesPct) &&
    sample.droppedFramesPct > QUALITY_GATE_LIMITS.maximumDroppedFramesPct
  )
    reasons.push('descarte-alto');

  return { measurable: true, pass: reasons.length === 0, reasons, minimumFps };
}

export function createArrivalQualityGate({
  samplesToSettle = QUALITY_GATE_LIMITS.samplesToSettle,
} = {}) {
  let state = 'measuring';
  let consecutivePasses = 0;
  let consecutiveFails = 0;
  let targetKey = null;

  function reset() {
    state = 'measuring';
    consecutivePasses = 0;
    consecutiveFails = 0;
  }

  function update(sample, target) {
    const nextTargetKey = `${Number(target?.bitrate) || 0}:${Number(target?.fps) || 0}`;
    if (targetKey !== null && targetKey !== nextTargetKey) reset();
    targetKey = nextTargetKey;

    const evaluation = evaluateArrivalQuality(sample, target);
    if (!evaluation.measurable) return { ...evaluation, state };

    if (evaluation.pass) {
      consecutivePasses++;
      consecutiveFails = 0;
      if (consecutivePasses >= samplesToSettle) state = 'pass';
    } else {
      consecutiveFails++;
      consecutivePasses = 0;
      if (consecutiveFails >= samplesToSettle) state = 'fail';
    }

    return { ...evaluation, state, consecutivePasses, consecutiveFails };
  }

  return { update, reset, getState: () => state };
}

export function formatBitrate(bitrateBps) {
  if (!finite(bitrateBps)) return '—';
  if (bitrateBps >= 1_000_000) return `${(bitrateBps / 1_000_000).toFixed(2)} Mb/s`;
  return `${Math.round(bitrateBps / 1000)} kb/s`;
}
