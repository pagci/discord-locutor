/**
 * Qualidade adaptativa: o feedback do relay para quem transmite.
 *
 * O servidor é o único lado que enxerga todos os espectadores ao mesmo tempo,
 * então é ele quem decide quando a qualidade precisa ceder — e quando pode
 * voltar. A decisão roda no mesmo relógio que fecha salas vazias, e por isso o
 * relógio falso precisa estar de pé *antes* do import, como em
 * rooms-limpeza.test.js.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.useFakeTimers();
const R = await import('./rooms.js');

const SWEEP = 4 * 1000;
const AUDIO = 3;

let sequencia = 0;
const instancia = () => `qualidade-${++sequencia}`;

function socket({ buffered = 0 } = {}) {
  return {
    OPEN: 1,
    readyState: 1,
    bufferedAmount: buffered,
    recebidas: [],
    send(data) {
      if (typeof data === 'string') this.recebidas.push(JSON.parse(data));
    },
    tipos() {
      return this.recebidas.map((m) => m.type);
    },
    contar(tipo) {
      return this.recebidas.filter((m) => m.type === tipo).length;
    },
  };
}

const quadro = (slot) => {
  const b = Buffer.alloc(64);
  b[0] = slot;
  b[1] = AUDIO;
  return b;
};

/** Uma sala com transmissão no ar e um espectador entupido. */
function cena({ entupido = true } = {}) {
  const { room } = R.createRoom({ instance: instancia(), ownerId: 'd', ownerName: 'D' });
  const ws = socket();
  const entry = R.attachBroadcaster(room, ws, { id: 't', name: 'T' });
  R.startStream(room, entry);
  const lento = socket({ buffered: entupido ? 3 * 1024 * 1024 : 0 });
  R.attachViewer(room, lento, { id: 'v', name: 'V' });
  R.watch(room, lento, entry.slot);
  return { room, ws, entry, lento };
}

/** Três quadros derrubados no espectador — o bastante para marcar a janela. */
function degradar(contexto, total = 3) {
  const { room, entry } = contexto;
  for (let i = 0; i < total; i++) R.pushChunk(room, entry, quadro(entry.slot));
  return contexto;
}

beforeEach(() => {
  vi.setSystemTime(100_000);
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

describe('quality-down', () => {
  it('pede quando um espectador vive derrubando quadro', () => {
    const contexto = degradar(cena());

    vi.advanceTimersByTime(SWEEP);

    expect(contexto.ws.tipos()).toContain('quality-down');
  });

  it('traduz a severidade da rajada em até quatro degraus sem repetir mensagem', () => {
    const contexto = degradar(cena(), 12);

    vi.advanceTimersByTime(SWEEP);

    const downs = contexto.ws.recebidas.filter((m) => m.type === 'quality-down');
    expect(downs).toEqual([{ type: 'quality-down', steps: 4 }]);
  });

  it('mantém uma janela apenas limítrofe em um degrau', () => {
    const contexto = degradar(cena(), 3);

    vi.advanceTimersByTime(SWEEP);

    const downs = contexto.ws.recebidas.filter((m) => m.type === 'quality-down');
    expect(downs).toEqual([{ type: 'quality-down', steps: 1 }]);
  });

  it('não pede por um descarte eventual', () => {
    const contexto = cena();
    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));

    vi.advanceTimersByTime(SWEEP);

    expect(contexto.ws.tipos()).not.toContain('quality-down');
  });

  it('não confunde dois picos de admissão em rede limpa com congestionamento', () => {
    const contexto = cena();
    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));
    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot));

    vi.advanceTimersByTime(SWEEP);

    expect(contexto.ws.tipos()).not.toContain('quality-down');
  });

  it('conta pedidos de recuperação do wire como perda do relay', () => {
    const contexto = cena({ entupido: false });

    vi.advanceTimersByTime(2000);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);

    expect(contexto.ws.tipos()).toContain('quality-down');
  });

  it('prioriza gap comprovado sem abrir uma torneira de keyframes', () => {
    const contexto = cena({ entupido: false });
    contexto.ws.recebidas.length = 0;

    // O watch acabou de pedir uma âncora normal. Um gap real pode ultrapassá-la
    // uma vez; repetições simultâneas de vários viewers continuam coalescidas.
    vi.setSystemTime(100_100);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);
    expect(contexto.ws.contar('need-keyframe')).toBe(1);

    vi.setSystemTime(100_449);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);
    expect(contexto.ws.contar('need-keyframe')).toBe(1);

    vi.setSystemTime(100_450);
    R.pedirKeyframe(contexto.room, contexto.lento, contexto.entry.slot);
    expect(contexto.ws.contar('need-keyframe')).toBe(2);
  });

  it('exige três expirações nativas próximas antes de formar fila visual', () => {
    const contexto = cena({ entupido: false });

    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'datagram-native-expired',
        newlyExpired: 1,
        newlyLost: 0,
      }),
    ).toBe(0);
    const afetadas = R.reportarPressaoTransporte(contexto.room, contexto.lento, {
      reason: 'datagram-native-expired',
      newlyExpired: 2,
      newlyLost: 0,
    });

    expect(afetadas).toBe(1);
    expect(contexto.ws.recebidas.filter((m) => m.type === 'quality-down')).toEqual([
      { type: 'quality-down', steps: 13 },
    ]);
  });

  it('exige três perdas nativas próximas antes de formar fila visual', () => {
    const contexto = cena({ entupido: false });

    const afetadas = R.reportarPressaoTransporte(contexto.room, contexto.lento, {
      reason: 'datagram-native-lost',
      newlyExpired: 0,
      newlyLost: 3,
    });

    expect(afetadas).toBe(1);
    expect(contexto.ws.recebidas.filter((m) => m.type === 'quality-down')).toEqual([
      { type: 'quality-down', steps: 13 },
    ]);
  });

  it('reage a uma rajada de bloqueio do writer antes de acumular expirações nativas', () => {
    const contexto = cena({ entupido: false });

    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'datagram-blocked',
      }),
    ).toBe(0);
    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'datagram-blocked',
      }),
    ).toBe(0);
    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'datagram-blocked',
      }),
    ).toBe(1);
    expect(contexto.ws.recebidas.filter((m) => m.type === 'quality-down')).toEqual([
      { type: 'quality-down', steps: 13 },
    ]);
  });

  it('não trata bufferedAmount do WebTransport como perda de datagrama', () => {
    const contexto = cena();
    contexto.lento.transport = 'webtransport';
    degradar(contexto, 8);

    vi.advanceTimersByTime(SWEEP);

    // A fila datagrama do wire é cancelável e limitada por idade. A pressão
    // física chega pelos diagnósticos datagram-blocked/native-* acima; usar o
    // bufferedAmount genérico aqui recriaria o duplo controle do WebSocket.
    expect(contexto.ws.recebidas.filter((m) => m.type === 'quality-down')).toEqual([]);
  });

  it('ignora diagnóstico local e espectador que já migrou para WebRTC', () => {
    const contexto = cena({ entupido: false });

    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'backpressure-drop',
      }),
    ).toBe(0);
    R.rtcAtivo(contexto.room, contexto.lento, contexto.entry.slot, true);
    expect(
      R.reportarPressaoTransporte(contexto.room, contexto.lento, {
        reason: 'datagram-native-expired',
        newlyExpired: 4,
      }),
    ).toBe(0);
    expect(contexto.ws.tipos()).not.toContain('quality-down');
  });

  it('não conta quem já recebe pela conexão direta', () => {
    const contexto = degradar(cena());
    // O descarte para, porque o relay para — mas o que ficou na fila ainda
    // assim não pode contar contra a qualidade da sala.
    R.rtcAtivo(contexto.room, contexto.lento, contexto.entry.slot, true);

    vi.advanceTimersByTime(SWEEP);

    expect(contexto.ws.tipos()).not.toContain('quality-down');
  });
});

/**
 * O que o transmissor faz ao receber um `quality-down`: aplica o degrau e conta
 * de volta onde ficou. O servidor não mantém contador próprio — ele espelha
 * este relato —, então sem ele não há dívida a devolver e nenhum `quality-up`
 * sai. É o mesmo caminho que `shared/broadcaster.js` percorre de verdade.
 */
function relatar(contexto, degraus, { piso = false } = {}) {
  R.setQuality(contexto.room, contexto.entry, {
    degraus,
    bitrate: degraus === 0 ? 2_500_000 : 1_875_000,
    fps: 30,
    piso,
  });
}

describe('quality-up', () => {
  it('volta a qualidade depois de janelas limpas repetidas', () => {
    const contexto = degradar(cena());
    vi.advanceTimersByTime(SWEEP);
    expect(contexto.ws.tipos()).toContain('quality-down');
    relatar(contexto, 1);
    contexto.lento.bufferedAmount = 0;

    // Duas janelas limpas e o intervalo de espera do ajuste.
    vi.advanceTimersByTime(SWEEP * 3);

    expect(contexto.ws.contar('quality-up')).toBe(1);
  });

  it('não sobe sem o transmissor ter relatado o degrau', () => {
    // Falha fechando: transmissor que não reporta nunca recebe crédito de volta,
    // e o servidor jamais oferece qualidade acima do que a pessoa escolheu.
    const contexto = degradar(cena());
    vi.advanceTimersByTime(SWEEP);
    expect(contexto.ws.tipos()).toContain('quality-down');
    contexto.lento.bufferedAmount = 0;

    vi.advanceTimersByTime(SWEEP * 3);

    expect(contexto.ws.contar('quality-up')).toBe(0);
  });

  it('não oferece subir quem nunca desceu', () => {
    const contexto = cena({ entupido: false });

    vi.advanceTimersByTime(SWEEP * 4);

    expect(contexto.ws.tipos()).not.toContain('quality-up');
  });

  it('não repete o oferecimento depois de voltar ao normal', () => {
    const contexto = degradar(cena());
    vi.advanceTimersByTime(SWEEP);
    relatar(contexto, 1);
    contexto.lento.bufferedAmount = 0;
    vi.advanceTimersByTime(SWEEP * 3);
    expect(contexto.ws.contar('quality-up')).toBe(1);
    // De volta ao teto: não há mais degrau nenhum a devolver.
    relatar(contexto, 0);

    vi.advanceTimersByTime(SWEEP * 4);

    expect(contexto.ws.contar('quality-up')).toBe(1);
  });

  it('transmissão nova começa sem dívida nenhuma', () => {
    const contexto = degradar(cena());
    vi.advanceTimersByTime(SWEEP);
    expect(contexto.ws.tipos()).toContain('quality-down');

    R.stopStream(contexto.room, contexto.entry);
    R.startStream(contexto.room, contexto.entry);
    contexto.lento.bufferedAmount = 0;
    vi.advanceTimersByTime(SWEEP * 4);

    // A degradação era da transmissão anterior; a nova não tem por que subir.
    expect(contexto.ws.contar('quality-up')).toBe(0);
  });
});
