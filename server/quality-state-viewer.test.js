import { describe, expect, it } from 'vitest';
import * as R from './rooms.js';

let sequence = 0;

function socket() {
  return {
    OPEN: 1,
    readyState: 1,
    bufferedAmount: 0,
    sent: [],
    send(data) {
      if (typeof data === 'string') this.sent.push(JSON.parse(data));
    },
    messages(type) {
      return this.sent.filter((message) => message.type === type);
    },
  };
}

function scene({ watch = true } = {}) {
  const { room } = R.createRoom({
    instance: `quality-state-${++sequence}`,
    ownerId: 'owner',
    ownerName: 'Owner',
  });
  const source = socket();
  const entry = R.attachBroadcaster(room, source, { id: 'source', name: 'Source' });
  R.startStream(room, entry);
  const viewer = socket();
  R.attachViewer(room, viewer, { id: 'viewer', name: 'Viewer' });
  if (watch) R.watch(room, viewer, entry.slot);
  viewer.sent.length = 0;
  return { room, entry, viewer };
}

describe('quality-state para o espectador', () => {
  it('envia o alvo atualizado somente para quem assiste ao slot', () => {
    const active = scene();
    const inactive = scene({ watch: false });

    expect(
      R.setQuality(active.room, active.entry, {
        degraus: 1,
        bitrate: 1_875_000,
        fps: 30,
        piso: false,
      }),
    ).toBe(true);
    R.setQuality(inactive.room, inactive.entry, {
      degraus: 0,
      bitrate: 2_500_000,
      fps: 30,
      piso: false,
    });

    expect(active.viewer.messages('quality-state')).toEqual([
      {
        type: 'quality-state',
        slot: active.entry.slot,
        degraus: 1,
        bitrate: 1_875_000,
        fps: 30,
        piso: false,
      },
    ]);
    expect(inactive.viewer.messages('quality-state')).toEqual([]);
  });

  it('entrega imediatamente o alvo já conhecido quando o espectador começa a assistir', () => {
    const context = scene({ watch: false });
    R.setQuality(context.room, context.entry, {
      degraus: 0,
      bitrate: 2_500_000,
      fps: 30,
      piso: false,
    });

    R.watch(context.room, context.viewer, context.entry.slot);

    expect(context.viewer.messages('quality-state').at(-1)).toMatchObject({
      bitrate: 2_500_000,
      fps: 30,
    });
  });

  it('não propaga snapshot inválido', () => {
    const context = scene();
    expect(
      R.setQuality(context.room, context.entry, {
        degraus: 0,
        bitrate: 0,
        fps: 30,
        piso: false,
      }),
    ).toBe(false);
    expect(context.viewer.messages('quality-state')).toEqual([]);
  });
});
