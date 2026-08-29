/** I6 — C6.1/C6.2: handshake e tráfego reais sem permessage-deflate. */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';

vi.spyOn(console, 'log').mockImplementation(() => {});
vi.spyOn(console, 'warn').mockImplementation(() => {});
vi.spyOn(console, 'error').mockImplementation(() => {});

const { server, wss } = await import('./index.js');
const { signToken } = await import('./tokens.js');
const R = await import('./rooms.js');
if (!server.listening) await new Promise((resolve) => server.once('listening', resolve));
const porta = server.address().port;
const abertos = [];

function token(room, role, uid) {
  return signToken({ room: room.id, uid, name: uid, av: null, role });
}

function conectar(room, role, uid) {
  const ws = new WebSocket(
    `ws://127.0.0.1:${porta}/ws?t=${encodeURIComponent(token(room, role, uid))}`,
    { perMessageDeflate: true },
  );
  ws.textos = [];
  ws.binarios = [];
  ws.on('message', (data, isBinary) => {
    if (isBinary) ws.binarios.push(Buffer.from(data));
    else ws.textos.push(JSON.parse(data.toString()));
  });
  abertos.push(ws);
  return new Promise((resolve, reject) => {
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

function esperar(ws, predicado, descricao) {
  const existente = ws.textos.find(predicado);
  if (existente) return Promise.resolve(existente);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      ws.off('message', ouvir);
      reject(new Error(`tempo esgotado esperando ${descricao}`));
    }, 3_000);
    function ouvir(data, isBinary) {
      if (isBinary) return;
      const msg = JSON.parse(data.toString());
      if (!predicado(msg)) return;
      clearTimeout(timeout);
      ws.off('message', ouvir);
      resolve(msg);
    }
    ws.on('message', ouvir);
  });
}

function esperarBinario(ws) {
  if (ws.binarios.length) return Promise.resolve(ws.binarios[0]);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      ws.off('message', ouvir);
      reject(new Error('tempo esgotado esperando mídia binária'));
    }, 3_000);
    function ouvir(data, isBinary) {
      if (!isBinary) return;
      clearTimeout(timeout);
      ws.off('message', ouvir);
      resolve(Buffer.from(data));
    }
    ws.on('message', ouvir);
  });
}

let transmissor;
let espectador;
let slot;

beforeAll(async () => {
  const { room } = R.createRoom({
    instance: 'deflate-independente',
    ownerId: 'dono',
    ownerName: 'Dono',
  });
  transmissor = await conectar(room, 'broadcaster', 'origem');
  slot = (await esperar(transmissor, (m) => m.type === 'slot', 'slot')).slot;
  transmissor.send(JSON.stringify({ type: 'start' }));

  espectador = await conectar(room, 'viewer', 'viewer');
  await esperar(espectador, (m) => m.type === 'stream-start', 'stream-start');
  espectador.send(JSON.stringify({ type: 'watch', slot }));
  await esperar(transmissor, (m) => m.type === 'need-keyframe', 'need-keyframe');
});

afterAll(async () => {
  for (const ws of abertos) ws.terminate();
  wss.close();
  await new Promise((resolve) => server.close(resolve));
});

describe('I6 — WebSocket real', () => {
  it('C6.1 recusa permessage-deflate mesmo quando o cliente oferece', () => {
    expect(espectador.extensions).not.toMatch(/permessage-deflate/i);
  });

  it('C6.2 preserva JSON e mídia binária no mesmo socket sem extensão', async () => {
    transmissor.send(JSON.stringify({ type: 'config', config: { codec: 'vp8' } }));
    const controle = await esperar(espectador, (m) => m.type === 'config', 'config JSON');
    expect(controle).toMatchObject({ type: 'config', slot, config: { codec: 'vp8' } });

    const frame = Buffer.alloc(64);
    frame[0] = slot;
    frame[1] = 1;
    transmissor.send(frame);
    expect(await esperarBinario(espectador)).toEqual(frame);
    expect(espectador.extensions).not.toMatch(/permessage-deflate/i);
  });
});
