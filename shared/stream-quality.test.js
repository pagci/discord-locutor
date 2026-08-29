import { describe, expect, it } from 'vitest';
import {
  DEFAULT_STREAM_SETTINGS,
  normalizeStreamSettings,
  streamSettingsToQuery,
} from './stream-quality.js';

describe('preferências de resolução e FPS', () => {
  it('preserva uma combinação válida e serializa a resolução para a captura externa', () => {
    const settings = normalizeStreamSettings({ bitrate: 5_000_000, resolution: 720, fps: 60 });

    expect(settings).toEqual({ bitrate: 5_000_000, resolution: 720, fps: 60 });
    expect(streamSettingsToQuery(settings)).toEqual({ q: '5000000', res: '720', fps: '60' });
  });

  it('recusa valores arbitrários vindos do localStorage ou da URL', () => {
    expect(normalizeStreamSettings({ bitrate: -1, resolution: 9999, fps: 144 })).toEqual(
      DEFAULT_STREAM_SETTINGS,
    );
  });
});
