import { describe, expect, it } from 'vitest';
import {
  decodeClockBits,
  encodeClockBits,
  evaluateCalibration,
  evaluateCaptureSelection,
  evaluateDelaySeries,
  hasStableGateTail,
  isResolvedGateSample,
  isExpectedPageReady,
  toCsv,
} from '../scripts/ensaio-delay-lib.mjs';

function serie({ accumulating = false, load = false, tunnel = false } = {}) {
  const samples = [];
  for (let second = 0; second < 20; second++) {
    samples.push({
      tMs: second * 1000,
      phase: 'baseline',
      visualLagMs: 100,
      arrivalLagMs: 50,
      jitterMs: 5,
      frames: 30,
      cpuPercent: 20,
      freeMemoryBytes: 4 * 1024 ** 3,
      tunnelOk: tunnel ? true : undefined,
      tunnelRttMs: tunnel ? 40 : null,
    });
  }
  for (let second = 0; second < 25; second++) {
    samples.push({
      tMs: (25 + second) * 1000,
      phase: 'sustain',
      visualLagMs: accumulating ? 500 + second * 100 : 700,
      arrivalLagMs: accumulating ? 400 + second * 100 : 600,
      jitterMs: 30,
      frames: 20,
      cpuPercent: load ? 95 : 25,
      freeMemoryBytes: 4 * 1024 ** 3,
      tunnelOk: tunnel ? second > 2 : undefined,
      tunnelRttMs: tunnel ? 700 : null,
    });
  }
  for (let second = 0; second < 30; second++) {
    samples.push({
      tMs: (50 + second) * 1000,
      phase: 'recovery',
      visualLagMs: 120,
      arrivalLagMs: 60,
      jitterMs: 10,
      frames: 30,
      cpuPercent: 20,
      freeMemoryBytes: 4 * 1024 ** 3,
      tunnelOk: tunnel ? true : undefined,
      tunnelRttMs: tunnel ? 45 : null,
    });
  }
  return samples;
}

describe('relogio visual do ensaio', () => {
  it('preserva timestamp de 48 bits e recusa palavra corrompida', () => {
    const timestamp = Date.now();
    const bits = encodeClockBits(timestamp);
    expect(bits).toHaveLength(64);
    expect(decodeClockBits(bits)).toBe(timestamp);

    bits[17] ^= 1;
    expect(decodeClockBits(bits)).toBeNull();
  });

  it('exige plateaus monotônicos e delta final predefinido', () => {
    const samples = [0, 500, 1000, 1500].flatMap((delay) =>
      Array.from({ length: 8 }, (_, index) => ({
        configuredDelayMs: delay,
        visualLagMs: 100 + delay + (index % 2),
      })),
    );
    const result = evaluateCalibration(samples);
    expect(result.pass).toBe(true);
    expect(result.steps.every((step) => step >= 499)).toBe(true);
    expect(result.finalDelta).toBeGreaterThanOrEqual(1499);
  });

  it('permite limiares operacionais explicitos sem afrouxar o padrao', () => {
    const samples = [0, 100, 200, 300].flatMap((delay) =>
      Array.from({ length: 8 }, (_, index) => ({
        configuredDelayMs: delay,
        visualLagMs: 100 + delay + (index % 2),
      })),
    );

    expect(evaluateCalibration(samples).pass).toBe(false);
    expect(evaluateCalibration(samples, { stepMinimum: 99, finalMinimum: 299 }).pass).toBe(true);
  });
});

describe('condutor e identidade da captura', () => {
  it('separa aquecimento instavel de uma cauda PASS consecutiva', () => {
    const warmup = [
      { gate: 'fail' },
      { gate: 'fail' },
      ...Array.from({ length: 5 }, () => ({ gate: 'pass' })),
    ];
    expect(hasStableGateTail(warmup)).toBe(true);
    expect(hasStableGateTail(warmup.slice(0, -1))).toBe(false);
    expect(hasStableGateTail([{ gate: 'pass' }], { consecutive: 1 })).toBe(true);
    expect(hasStableGateTail([], { consecutive: 0 })).toBe(false);
  });

  it('preserva FALHOU anterior sem invalidar uma cauda recuperada em PASS', () => {
    const passFailPass = [
      { gate: 'pass' },
      { gate: 'fail' },
      ...Array.from({ length: 5 }, () => ({ gate: 'pass' })),
    ];
    expect(passFailPass.some((sample) => sample.gate === 'fail')).toBe(true);
    expect(hasStableGateTail(passFailPass)).toBe(true);
  });

  it('na janela oficial conta apenas estados resolvidos do quality gate', () => {
    expect(isResolvedGateSample({ gate: 'pass' })).toBe(true);
    expect(isResolvedGateSample({ gate: 'fail' })).toBe(true);
    expect(isResolvedGateSample({ gate: 'measuring' })).toBe(false);
    expect(isResolvedGateSample({ gate: '' })).toBe(false);
  });

  it('so considera a pagina pronta com origem esperada e readyState completo', () => {
    expect(
      isExpectedPageReady(
        { ready: 'complete', origin: 'http://localhost:3100' },
        'http://localhost:3100',
      ),
    ).toBe(true);
    expect(
      isExpectedPageReady({ ready: 'complete', origin: 'null' }, 'http://localhost:3100'),
    ).toBe(false);
    expect(
      isExpectedPageReady(
        { ready: 'loading', origin: 'http://localhost:3100' },
        'http://localhost:3100',
      ),
    ).toBe(false);
  });

  it('exige titulo/runId, superficie browser e marcador visual juntos', () => {
    const valid = {
      runId: 'DL-ABC123',
      label: 'Chrome Tab - ENSAIO RELOGIO VISUAL DL-ABC123',
      displaySurface: 'browser',
      markerDecoded: true,
    };
    expect(evaluateCaptureSelection(valid)).toEqual({
      pass: true,
      labelMatchesRunId: true,
      pickerSelectionMatchesRunId: false,
      sourceIdentityProven: true,
      browserSurface: true,
      pixelMarkerDecoded: true,
    });
    expect(evaluateCaptureSelection({ ...valid, label: 'screen:0:0' }).pass).toBe(false);
    expect(
      evaluateCaptureSelection({
        ...valid,
        label: 'web-contents-media-stream://opaque',
        pickerSelectionName: `ENSAIO RELOGIO VISUAL ${valid.runId}`,
      }).pass,
    ).toBe(true);
    expect(
      evaluateCaptureSelection({
        ...valid,
        label: 'web-contents-media-stream://opaque',
        pickerSelectionName: 'ENSAIO RELOGIO VISUAL outro-run',
      }).pass,
    ).toBe(false);
    expect(evaluateCaptureSelection({ ...valid, displaySurface: 'monitor' }).pass).toBe(false);
    expect(evaluateCaptureSelection({ ...valid, markerDecoded: false }).pass).toBe(false);
  });
});

describe('classificacao das series', () => {
  it('aprova lag limitado, jitter evidenciado e recuperacao', () => {
    const result = evaluateDelaySeries(serie(), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'websocket',
      path: 'localhost',
    });
    expect(result.status).toBe('PASS');
    expect(result.jitterStatus).toBe('PASS');
    expect(result.metrics.slopeMsPerS).toBe(0);
    expect(result.checks.recuperouNoPrazo).toBe(true);
  });

  it('falha quando o atraso acumula', () => {
    const result = evaluateDelaySeries(serie({ accumulating: true }), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'websocket',
      path: 'localhost',
    });
    expect(result.status).toBe('FAIL');
    expect(result.checks.semInclinacaoAcumulativa).toBe(false);
  });

  it('falha com duas janelas consecutivas sem frames', () => {
    const samples = serie();
    for (const sample of samples) {
      if (sample.phase === 'sustain' && sample.tMs >= 30_000 && sample.tMs < 32_000)
        sample.frames = 0;
    }
    const result = evaluateDelaySeries(samples, {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'websocket',
      path: 'localhost',
    });
    expect(result.status).toBe('FAIL');
    expect(result.checks.framesContinuos).toBe(false);
  });

  it('nao atribui falha coincidente com pico de carga ao player', () => {
    const result = evaluateDelaySeries(serie({ accumulating: true, load: true }), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'websocket',
      path: 'localhost',
    });
    expect(result.status).toBe('INCONCLUSIVO');
    expect(result.confounds).toContain('carga');
  });

  it('faz gate QUIC vermelho dominar mesmo quando os numeros passam', () => {
    const result = evaluateDelaySeries(serie(), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'webtransport',
      path: 'localhost',
      gateBefore: 'vermelho',
      gateAfter: 'verde',
    });
    expect(result.status).toBe('INCONCLUSIVO');
    expect(result.confounds).toContain('gate');
  });

  it('separa instabilidade do tunnel de falha do player', () => {
    const result = evaluateDelaySeries(serie({ accumulating: true, tunnel: true }), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'websocket',
      path: 'quick-tunnel',
    });
    expect(result.status).toBe('INCONCLUSIVO');
    expect(result.confounds).toContain('tunel');
  });

  it('nao usa o tunnel de sinalizacao como confound da midia WebRTC P2P', () => {
    const result = evaluateDelaySeries(serie({ accumulating: true, tunnel: true }), {
      instrumentOk: true,
      impairmentOk: true,
      transport: 'webrtc',
      path: 'quick-tunnel',
    });
    expect(result.status).toBe('FAIL');
    expect(result.confounds).not.toContain('tunel');
  });
});

it('CSV preserva virgulas, aspas e objetos', () => {
  const csv = toCsv([{ a: 'um,dois', b: { x: '"' } }]);
  expect(csv).toContain('"um,dois"');
  expect(csv).toContain('""""');
});
