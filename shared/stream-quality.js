export const STREAM_RESOLUTIONS = [480, 720, 1080];
export const STREAM_FPS = [15, 30, 60];
export const DEFAULT_STREAM_SETTINGS = Object.freeze({
  bitrate: 2_500_000,
  resolution: 1080,
  fps: 30,
});

const tem = (lista, valor) => lista.includes(Number(valor));

/** Normaliza preferências persistidas ou vindas da URL antes de chegar ao encoder. */
export function normalizeStreamSettings(raw = {}) {
  const bitrate = Number(raw.bitrate);
  return {
    bitrate:
      Number.isFinite(bitrate) && bitrate >= 120_000 && bitrate <= 8_000_000
        ? Math.round(bitrate)
        : DEFAULT_STREAM_SETTINGS.bitrate,
    resolution: tem(STREAM_RESOLUTIONS, raw.resolution)
      ? Number(raw.resolution)
      : DEFAULT_STREAM_SETTINGS.resolution,
    fps: tem(STREAM_FPS, raw.fps) ? Number(raw.fps) : DEFAULT_STREAM_SETTINGS.fps,
  };
}

/** Formato curto usado para levar a escolha da Activity à página externa. */
export function streamSettingsToQuery(settings) {
  const normalized = normalizeStreamSettings(settings);
  return {
    q: String(normalized.bitrate),
    res: String(normalized.resolution),
    fps: String(normalized.fps),
  };
}
