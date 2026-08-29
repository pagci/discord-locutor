/**
 * I1 — oráculos independentes do orçamento temporal (C1.7–C1.10, C1.13–C1.20).
 *
 * A asserção primária é sempre a fila observável do socket depois da decisão:
 * o dublê soma cada binário enviado ao `bufferedAmount`, como um WebSocket real.
 * Nenhum teste lê o orçamento ou o estimador internos.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.useFakeTimers();
const R = await import('./rooms.js');

const KEYFRAME = 1;
const DELTA = 2;
const AUDIO = 3;
let sequencia = 0;

function socket({ buffered = 0, acumula = false } = {}) {
  return {
    OPEN: 1,
    readyState: 1,
    bufferedAmount: buffered,
    enviados: [],
    send(data) {
      this.enviados.push(data);
      if (acumula && typeof data !== 'string') {
        this.bufferedAmount += Number(data?.byteLength ?? data?.length ?? 0);
      }
    },
    limpar() {
      this.enviados.length = 0;
    },
    binarios() {
      return this.enviados.filter((data) => typeof data !== 'string');
    },
    mensagens() {
      return this.enviados
        .filter((data) => typeof data === 'string')
        .map((data) => JSON.parse(data));
    },
  };
}

function quadro(slot, tipo, bytes) {
  const out = Buffer.alloc(Math.max(2, Math.round(bytes)));
  out[0] = slot;
  out[1] = tipo;
  return out;
}

function criarStream({ fase = 0, conectadoHa = 0, snapshot = null } = {}) {
  vi.setSystemTime(10_000 + fase);
  const { room } = R.createRoom({
    instance: `fila-independente-${++sequencia}`,
    ownerId: 'dono',
    ownerName: 'Dono',
  });
  const origem = socket();
  const transmissorId = `t-${sequencia}`;
  const entry = R.attachBroadcaster(room, origem, { id: transmissorId, name: 'T' });
  // Presença do próprio transmissor: cenários longos medem orçamento, não o
  // lifecycle de abandono que o sweeper encerraria em t=20 s.
  R.attachViewer(room, socket(), { id: transmissorId, name: 'T' });
  if (conectadoHa) vi.advanceTimersByTime(conectadoHa);
  R.startStream(room, entry);
  if (snapshot) reportarQualidade({ room, entry }, snapshot);
  origem.limpar();
  return { room, origem, entry };
}

/** Fronteira de domínio correspondente à mensagem `quality` validada em index.js. */
function reportarQualidade(contexto, state) {
  R.setQuality?.(contexto.room, contexto.entry, {
    degraus: 0,
    bitrate: state.bitrate,
    fps: state.fps ?? 30,
    piso: state.piso ?? false,
  });
}

function espectador(contexto, { buffered = 0, primed = true } = {}) {
  const viewer = socket({ buffered, acumula: true });
  R.attachViewer(contexto.room, viewer, { id: `v-${++sequencia}`, name: 'V' });
  R.watch(contexto.room, viewer, contexto.entry.slot);
  viewer.limpar();
  contexto.origem.limpar();
  if (primed) viewer.__primed.add(contexto.entry.slot);
  return viewer;
}

/** Produz taxa legítima e estável antes de instalar qualquer espectador. */
function aquecer(contexto, bytesPorSegundo, segundos = 1.2) {
  const passo = 100;
  const bytes = bytesPorSegundo / (1000 / passo);
  const total = Math.ceil((segundos * 1000) / passo);
  for (let i = 0; i < total; i++) {
    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot, KEYFRAME, bytes));
    vi.advanceTimersByTime(passo);
  }
  contexto.origem.limpar();
  return contexto;
}

function enviar(contexto, viewer, tipo, bytes) {
  const antes = viewer.bufferedAmount;
  R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot, tipo, bytes));
  return { antes, depois: viewer.bufferedAmount, enviados: viewer.binarios().length };
}

/** Mede o maior `bufferedAmount` que ainda admite um delta mínimo. */
function limiarObservado(contexto, maximo = 600_000) {
  let baixo = 0;
  let alto = maximo;
  while (baixo + 1 < alto) {
    const meio = Math.floor((baixo + alto) / 2);
    const viewer = espectador(contexto, { buffered: meio });
    const passou = enviar(contexto, viewer, DELTA, 2).enviados === 1;
    R.detachViewer(contexto.room, viewer);
    if (passou) baixo = meio;
    else alto = meio;
  }
  return baixo;
}

beforeEach(() => {
  vi.setSystemTime(10_000);
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

describe('I1 — orçamento temporal independente', () => {
  it('C1.7 limita 100 kbps aquecido em décimos de segundo, sem piso permanente', () => {
    const cabe = aquecer(criarStream(), 12_500);
    const viewerCabe = espectador(cabe);
    expect(enviar(cabe, viewerCabe, DELTA, 3_125)).toMatchObject({
      depois: 3_125,
      enviados: 1,
    });

    const estoura = aquecer(criarStream(), 12_500);
    const viewerEstoura = espectador(estoura);
    expect(enviar(estoura, viewerEstoura, DELTA, 4_375)).toMatchObject({
      depois: 0,
      enviados: 0,
    });
  });

  it('C1.8 expira o bootstrap de 96 KB após o aquecimento sem snapshot', () => {
    const contexto = criarStream();
    const viewer = espectador(contexto);

    expect(enviar(contexto, viewer, DELTA, 50_000)).toMatchObject({
      depois: 50_000,
      enviados: 1,
    });

    viewer.bufferedAmount = 0;
    viewer.limpar();
    vi.advanceTimersByTime(1_100);
    expect(enviar(contexto, viewer, DELTA, 50_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });
  });

  it('C1.9 ancora a taxa no start, mesmo após conexão ociosa de 60 s', () => {
    const contexto = aquecer(criarStream({ conectadoHa: 60_000 }), 1_000_000, 1);
    const viewer = espectador(contexto);

    expect(enviar(contexto, viewer, DELTA, 250_000)).toMatchObject({
      depois: 250_000,
      enviados: 1,
    });
  });

  it('C1.10 slot alheio não infla a taxa; mídia válida equivalente é o controle', () => {
    const invalido = aquecer(criarStream(), 1_000_000, 1);
    for (let i = 0; i < 20; i++) {
      R.pushChunk(
        invalido.room,
        invalido.entry,
        quadro(invalido.entry.slot + 1, KEYFRAME, 100_000),
      );
    }
    const viewerInvalido = espectador(invalido);
    expect(enviar(invalido, viewerInvalido, DELTA, 350_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });

    const valido = aquecer(criarStream(), 1_000_000, 1);
    for (let i = 0; i < 20; i++) {
      R.pushChunk(valido.room, valido.entry, quadro(valido.entry.slot, KEYFRAME, 100_000));
    }
    const viewerValido = espectador(valido);
    expect(enviar(valido, viewerValido, DELTA, 350_000)).toMatchObject({
      depois: 350_000,
      enviados: 1,
    });
  });

  it('C1.13 julga o delta pela fila resultante e não admite um item de 4 MB', () => {
    const grande = aquecer(criarStream(), 12_500);
    const viewerGrande = espectador(grande);
    expect(enviar(grande, viewerGrande, DELTA, 4 * 1024 * 1024)).toMatchObject({
      antes: 0,
      depois: 0,
      enviados: 0,
    });

    const pequeno = aquecer(criarStream(), 12_500);
    const viewerPequeno = espectador(pequeno);
    expect(enviar(pequeno, viewerPequeno, DELTA, 3_125)).toMatchObject({
      depois: 3_125,
      enviados: 1,
    });
  });

  it('C1.14 aplica a mesma admissão pela fila resultante ao áudio', () => {
    const grande = aquecer(criarStream(), 12_500);
    const viewerGrande = espectador(grande, { primed: false });
    expect(enviar(grande, viewerGrande, AUDIO, 4 * 1024 * 1024)).toMatchObject({
      antes: 0,
      depois: 0,
      enviados: 0,
    });

    const pequeno = aquecer(criarStream(), 12_500);
    const viewerPequeno = espectador(pequeno, { primed: false });
    expect(enviar(pequeno, viewerPequeno, AUDIO, 3_125)).toMatchObject({
      depois: 3_125,
      enviados: 1,
    });
  });

  it('C1.15 admite um keyframe atômico de 2 s, recusa 3 s e bloqueia o delta até drenar', () => {
    const permitido = aquecer(criarStream(), 12_500);
    const viewerPermitido = espectador(permitido, { primed: false });
    expect(enviar(permitido, viewerPermitido, KEYFRAME, 25_000)).toMatchObject({
      depois: 25_000,
      enviados: 1,
    });
    viewerPermitido.limpar();
    expect(enviar(permitido, viewerPermitido, DELTA, 1_000)).toMatchObject({
      depois: 25_000,
      enviados: 0,
    });

    const proibido = aquecer(criarStream(), 12_500);
    const viewerProibido = espectador(proibido, { primed: false });
    vi.advanceTimersByTime(1_100);
    proibido.origem.limpar();
    expect(enviar(proibido, viewerProibido, KEYFRAME, 37_500)).toMatchObject({
      depois: 0,
      enviados: 0,
    });
    expect(proibido.origem.mensagens().map((m) => m.type)).toContain('need-keyframe');
  });

  it('C1.16 usa a taxa anterior ao item; só mídia legitimamente acumulada eleva o teto', () => {
    const isolado = criarStream();
    vi.advanceTimersByTime(1_100);
    const viewerIsolado = espectador(isolado);
    expect(enviar(isolado, viewerIsolado, DELTA, 100_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });

    const aindaIsolado = aquecer(criarStream(), 12_500);
    const viewerAindaIsolado = espectador(aindaIsolado);
    expect(enviar(aindaIsolado, viewerAindaIsolado, DELTA, 100_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });

    const taxaLegitima = aquecer(criarStream(), 1_000_000);
    const viewerLegitimo = espectador(taxaLegitima);
    expect(enviar(taxaLegitima, viewerLegitimo, DELTA, 100_000)).toMatchObject({
      depois: 100_000,
      enviados: 1,
    });
  });

  it('C1.17 aplica step-down de 8 Mbps para 100 kbps imediatamente', () => {
    const contexto = aquecer(criarStream(), 1_000_000);
    const viewer = espectador(contexto);
    expect(enviar(contexto, viewer, DELTA, 250_000)).toMatchObject({
      depois: 250_000,
      enviados: 1,
    });

    viewer.bufferedAmount = 0;
    viewer.limpar();
    reportarQualidade(contexto, { bitrate: 100_000 });
    expect(enviar(contexto, viewer, DELTA, 250_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });
  });

  it('C1.18 mantém a recuperação por keyframe após um step-up conservador', () => {
    const contexto = aquecer(criarStream({ snapshot: { bitrate: 100_000 } }), 12_500);
    reportarQualidade(contexto, { bitrate: 5_000_000 });
    const viewer = espectador(contexto, { primed: false });

    expect(enviar(contexto, viewer, KEYFRAME, 25_000)).toMatchObject({
      depois: 25_000,
      enviados: 1,
    });
  });

  it('C1.19 isola fase de relógio e restart dentro do mesmo balde', () => {
    const limiteZero = limiarObservado(aquecer(criarStream({ fase: 0 }), 1_000_000, 1.2));
    const limiteFim = limiarObservado(aquecer(criarStream({ fase: 999 }), 1_000_000, 1.2));
    const diferenca = Math.abs(limiteZero - limiteFim) / Math.max(limiteZero, limiteFim);
    expect(diferenca).toBeLessThanOrEqual(0.2);

    const reinicio = criarStream({ fase: 100 });
    for (let i = 0; i < 4; i++) {
      R.pushChunk(reinicio.room, reinicio.entry, quadro(reinicio.entry.slot, KEYFRAME, 400_000));
      vi.advanceTimersByTime(100);
    }
    R.stopStream(reinicio.room, reinicio.entry);
    vi.advanceTimersByTime(50);
    R.startStream(reinicio.room, reinicio.entry);
    aquecer(reinicio, 1_000_000, 1.2);
    const limiteReinicio = limiarObservado(reinicio);
    const diferencaRestart =
      Math.abs(limiteReinicio - limiteZero) / Math.max(limiteReinicio, limiteZero);
    expect(diferencaRestart).toBeLessThanOrEqual(0.2);
  });

  it('C1.20 snapshot frio de 5 Mbps admite o primeiro keyframe de 300 KB na ordem correta', () => {
    const comSnapshot = criarStream({ snapshot: { bitrate: 5_000_000 } });
    const viewerCom = espectador(comSnapshot, { primed: false });
    expect(enviar(comSnapshot, viewerCom, KEYFRAME, 300_000)).toMatchObject({
      depois: 300_000,
      enviados: 1,
    });

    const semSnapshot = criarStream();
    const viewerSem = espectador(semSnapshot, { primed: false });
    expect(enviar(semSnapshot, viewerSem, KEYFRAME, 300_000)).toMatchObject({
      depois: 0,
      enviados: 0,
    });
  });
});

describe('I1 — cadencia de controle na alternancia WebRTC', () => {
  it('64 pares rtc-ativo nao amplificam keyframe/chunks nem fecham sockets', () => {
    const contexto = criarStream();
    const viewer = espectador(contexto);
    contexto.origem.limpar();

    const alternar = () => {
      for (let i = 0; i < 64; i++) {
        R.rtcAtivo(contexto.room, viewer, contexto.entry.slot, true);
        R.rtcAtivo(contexto.room, viewer, contexto.entry.slot, false);
      }
    };
    const afirmarTeto = () => {
      const controles = contexto.origem.mensagens();
      expect.soft(controles.filter((msg) => msg.type === 'need-keyframe')).toHaveLength(1);
      expect.soft(controles.filter((msg) => msg.type === 'chunks').length).toBeLessThanOrEqual(2);
      expect.soft(contexto.origem.readyState).toBe(contexto.origem.OPEN);
      expect.soft(viewer.readyState).toBe(viewer.OPEN);
    };

    alternar();
    afirmarTeto();

    vi.advanceTimersByTime(1001);
    contexto.origem.limpar();
    alternar();
    afirmarTeto();
  });
});

describe('I1 — autoridade do slot de midia', () => {
  it('broadcaster nao injeta quadro em slot diferente do atribuido', () => {
    const contexto = criarStream();
    const viewer = espectador(contexto, { primed: false });
    viewer.limpar();

    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot + 1, KEYFRAME, 1024));
    expect.soft(viewer.binarios()).toHaveLength(0);

    R.pushChunk(contexto.room, contexto.entry, quadro(contexto.entry.slot, KEYFRAME, 1024));
    expect.soft(viewer.binarios()).toHaveLength(1);
    expect.soft(contexto.origem.readyState).toBe(contexto.origem.OPEN);
    expect.soft(viewer.readyState).toBe(viewer.OPEN);
  });
});
