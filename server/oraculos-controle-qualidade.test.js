/** I3 — C3.12, C4.13 e C4.14: streak, saturação e recuperação finita. */
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.useFakeTimers();
const R = await import('./rooms.js');

const SWEEP = 4_000;
const AUDIO = 3;
let sequencia = 0;

function socket({ buffered = 0 } = {}) {
  return {
    OPEN: 1,
    readyState: 1,
    bufferedAmount: buffered,
    enviados: [],
    send(data) {
      this.enviados.push(data);
    },
    mensagens(tipo = null) {
      const todas = this.enviados
        .filter((data) => typeof data === 'string')
        .map((data) => JSON.parse(data));
      return tipo ? todas.filter((msg) => msg.type === tipo) : todas;
    },
  };
}

function quadro(slot) {
  const out = Buffer.alloc(64);
  out[0] = slot;
  out[1] = AUDIO;
  return out;
}

function cena({ buffered = 3 * 1024 * 1024 } = {}) {
  const { room } = R.createRoom({
    instance: `controle-independente-${++sequencia}`,
    ownerId: 'dono',
    ownerName: 'Dono',
  });
  const origem = socket();
  const transmissorId = `t-${sequencia}`;
  const entry = R.attachBroadcaster(room, origem, { id: transmissorId, name: 'T' });
  R.startStream(room, entry);
  const viewer = socket({ buffered });
  // Mantém a presença do dono durante os cenários de muitas janelas.
  R.attachViewer(room, viewer, { id: transmissorId, name: 'T' });
  R.watch(room, viewer, entry.slot);
  return { room, origem, entry, viewer };
}

function degradar(contexto) {
  R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));
  R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));
  R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));
}

function reportar(contexto, { degraus, piso, bitrate = 120_000, fps = piso ? 15 : 30 }) {
  R.setQuality?.(contexto.room, contexto.entry, { degraus, bitrate, fps, piso });
}

function varrer(contexto, { suja = false } = {}) {
  if (suja) degradar(contexto);
  vi.advanceTimersByTime(SWEEP);
}

beforeEach(() => {
  vi.setSystemTime(100_000);
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

describe('I3 — controle relay-only', () => {
  it('C3.12 quebra a streak quando uma janela fica sem evidência', () => {
    const contexto = cena({ buffered: 0 });
    reportar(contexto, { degraus: 1, piso: false, bitrate: 1_875_000 });

    varrer(contexto); // limpa 1
    R.rtcAtivo(contexto.room, contexto.viewer, contexto.entry.slot, true);
    varrer(contexto); // sem relay-only: precisa quebrar a streak
    R.rtcAtivo(contexto.room, contexto.viewer, contexto.entry.slot, false);
    varrer(contexto); // limpa 1 novamente
    expect(contexto.origem.mensagens('quality-up')).toHaveLength(0);

    varrer(contexto); // limpa 2 consecutiva
    expect(contexto.origem.mensagens('quality-up')).toHaveLength(1);
  });

  it('C4.13 piso reportado suprime downs redundantes após downs reais', () => {
    const contexto = cena();
    varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);

    reportar(contexto, { degraus: 14, piso: true });
    for (let i = 0; i < 4; i++) varrer(contexto, { suja: true });

    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);
  });

  it('C4.14 limita a dívida e emite no máximo um up por degrau reportado', () => {
    const contexto = cena();
    let degraus = 0;

    // Quinze janelas ruins: a escada de 2,5 Mbps/30 fps satura em 14 degraus.
    for (let janela = 0; janela < 15; janela++) {
      const antes = contexto.origem.mensagens('quality-down').length;
      varrer(contexto, { suja: true });
      const depois = contexto.origem.mensagens('quality-down').length;
      if (depois > antes) {
        degraus = Math.min(14, degraus + 1);
        reportar(contexto, { degraus, piso: degraus === 14 });
      }
    }

    const dividaMaxima = degraus;
    expect(dividaMaxima).toBeGreaterThan(0);
    expect(contexto.origem.mensagens('quality-down').length).toBeLessThanOrEqual(14);

    contexto.viewer.bufferedAmount = 0;
    while (degraus > 0) {
      const antes = contexto.origem.mensagens('quality-up').length;
      // 3 sweeps = 12 s: duas janelas limpas e cooldown de 10 s.
      varrer(contexto);
      varrer(contexto);
      varrer(contexto);
      const depois = contexto.origem.mensagens('quality-up').length;
      expect(depois - antes).toBe(1);
      degraus -= 1;
      reportar(contexto, {
        degraus,
        piso: false,
        bitrate: degraus === 0 ? 2_500_000 : 120_000,
        fps: degraus === 0 ? 30 : 15,
      });
    }

    expect(contexto.origem.mensagens('quality-up')).toHaveLength(dividaMaxima);
    expect(contexto.origem.mensagens('quality-up').length).toBeLessThanOrEqual(dividaMaxima);

    // Já no teto, janelas limpas adicionais não criam crédito acima da escolha manual.
    for (let i = 0; i < 4; i++) varrer(contexto);
    expect(contexto.origem.mensagens('quality-up')).toHaveLength(dividaMaxima);
  });
});

describe('I4 — snapshot manual refletido pelo relay', () => {
  it('C4.16 piso:false após elevar o teto reabilita down na próxima janela suja', () => {
    const contexto = cena();
    varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);

    reportar(contexto, { degraus: 14, piso: true });
    varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);

    // Snapshot emitido pelo setQuality manual depois de elevar o teto.
    reportar(contexto, { degraus: 11, piso: false, bitrate: 337_881, fps: 60 });
    varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(2);
  });

  it('C4.17 piso:true após reduzir o teto suprime down redundante', () => {
    const contexto = cena();
    varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);

    // Snapshot emitido pelo setQuality manual ao encostar no piso atual.
    reportar(contexto, { degraus: 1, piso: true, bitrate: 120_000, fps: 15 });
    for (let i = 0; i < 3; i++) varrer(contexto, { suja: true });
    expect(contexto.origem.mensagens('quality-down')).toHaveLength(1);
  });
});
