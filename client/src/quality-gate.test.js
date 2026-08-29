import { describe, expect, it } from 'vitest';
import { createArrivalQualityGate, evaluateArrivalQuality, formatBitrate } from './quality-gate.js';

const target = { bitrate: 2_500_000, fps: 30 };

describe('quality gate de chegada', () => {
  it('não inventa veredito antes de conhecer alvo e amostra', () => {
    expect(evaluateArrivalQuality({}, {})).toMatchObject({ measurable: false, pass: false });
  });

  it('aceita bitrate baixo de cena estática quando bits e quadros continuam chegando', () => {
    expect(evaluateArrivalQuality({ bitrateBps: 180_000, fps: 29 }, target)).toMatchObject({
      measurable: true,
      pass: true,
    });
  });

  it('recusa fluxo sem bits ou abaixo de 60% do fps contratado', () => {
    expect(evaluateArrivalQuality({ bitrateBps: 0, fps: 30 }, target).reasons).toContain(
      'sem-fluxo-de-bits',
    );
    expect(evaluateArrivalQuality({ bitrateBps: 1_000_000, fps: 17 }, target).reasons).toContain(
      'fps-baixo',
    );
  });

  it('recusa perda de pacotes e descarte de quadros acima dos limites', () => {
    const result = evaluateArrivalQuality(
      { bitrateBps: 1_000_000, fps: 30, packetLossPct: 6, droppedFramesPct: 11 },
      target,
    );
    expect(result.reasons).toEqual(['perda-alta', 'descarte-alto']);
  });

  it('exige três amostras coerentes antes de declarar PASS ou FAIL', () => {
    const gate = createArrivalQualityGate();
    const good = { bitrateBps: 1_000_000, fps: 30 };
    expect(gate.update(good, target).state).toBe('measuring');
    expect(gate.update(good, target).state).toBe('measuring');
    expect(gate.update(good, target).state).toBe('pass');

    const bad = { bitrateBps: 0, fps: 0 };
    expect(gate.update(bad, target).state).toBe('pass');
    expect(gate.update(bad, target).state).toBe('pass');
    expect(gate.update(bad, target).state).toBe('fail');
  });

  it('volta a medir quando o alvo muda', () => {
    const gate = createArrivalQualityGate({ samplesToSettle: 2 });
    const sample = { bitrateBps: 1_000_000, fps: 30 };
    expect(gate.update(sample, target).state).toBe('measuring');
    expect(gate.update(sample, target).state).toBe('pass');
    expect(gate.update(sample, { bitrate: 800_000, fps: 24 }).state).toBe('measuring');
  });

  it('formata a taxa efetivamente recebida sem confundir bits com bytes', () => {
    expect(formatBitrate(2_345_678)).toBe('2.35 Mb/s');
    expect(formatBitrate(96_000)).toBe('96 kb/s');
  });
});
