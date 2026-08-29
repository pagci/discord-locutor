import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import selfsigned from 'selfsigned';

import { startWebTransport } from './webtransport.js';

let temp;
let shortEc;
let longEc;
let shortRsa;
let expiredEc;
let futureEc;

async function pair(
  name,
  { days = 12, keyType = 'ec', notBefore = '2026-08-20T00:00:00.000Z' } = {},
) {
  const notBeforeDate = new Date(notBefore);
  const notAfterDate = new Date(notBeforeDate);
  notAfterDate.setDate(notAfterDate.getDate() + days);
  const generated = await selfsigned.generate([{ name: 'commonName', value: 'localhost' }], {
    keyType,
    ...(keyType === 'ec' ? { curve: 'P-256' } : { keySize: 2048 }),
    algorithm: 'sha256',
    notBeforeDate,
    notAfterDate,
    extensions: [
      { name: 'basicConstraints', cA: true },
      {
        name: 'subjectAltName',
        altNames: [
          { type: 2, value: 'localhost' },
          { type: 7, ip: '127.0.0.1' },
        ],
      },
    ],
  });
  const certPath = join(temp, `${name}.crt`);
  const keyPath = join(temp, `${name}.key`);
  await Promise.all([writeFile(certPath, generated.cert), writeFile(keyPath, generated.private)]);
  return { certPath, keyPath };
}

function options(env = {}, production = false, onState = vi.fn()) {
  return {
    env: { WEBTRANSPORT_ENABLED: '1', ...env },
    production,
    node: 0,
    sharded: false,
    allowedOrigins: new Set(['https://app.example.invalid']),
    sources: new Set(['tela', 'camera']),
    verifyToken: () => null,
    nodeForToken: () => 0,
    onConnection: vi.fn(),
    onError: vi.fn(),
    onState,
  };
}

async function rejected(env, production = false) {
  const opts = options(env, production);
  const state = await startWebTransport(opts);
  expect(state.capability()).toBeNull();
  expect(opts.onState).toHaveBeenCalledTimes(1);
  expect(opts.onState).toHaveBeenCalledWith(expect.objectContaining({ state: 'misconfigured' }));
  expect(() => state.stop()).not.toThrow();
  return opts.onState.mock.calls[0][0];
}

beforeAll(async () => {
  temp = await mkdtemp(join(tmpdir(), 'discord-locutor-wt-config-'));
  [shortEc, longEc, shortRsa, expiredEc, futureEc] = await Promise.all([
    pair('short-ec'),
    pair('long-ec', { days: 30 }),
    pair('short-rsa', { keyType: 'rsa' }),
    pair('expired-ec', { days: 2, notBefore: '2026-08-01T00:00:00.000Z' }),
    pair('future-ec', { days: 2, notBefore: '2026-09-01T00:00:00.000Z' }),
  ]);
});

afterAll(async () => {
  await rm(temp, { recursive: true, force: true });
});

describe('configuração inerte do listener WebTransport', () => {
  it('disabled não lê arquivos, não abre listener e tolera observador defeituoso', async () => {
    const observed = vi.fn(() => {
      throw new Error('metrics offline');
    });
    const state = await startWebTransport(
      options({ WEBTRANSPORT_ENABLED: 'false' }, false, observed),
    );

    expect(state.listener).toBeNull();
    expect(state.capability()).toBeNull();
    expect(observed).toHaveBeenCalledWith({ state: 'disabled' });
  });

  it.each(['bad', '', 'ftp'])('recusa CERT_MODE inválido (%s)', async (mode) => {
    const event = await rejected({ WEBTRANSPORT_CERT_MODE: mode });
    expect(event.reason).toBe('config-invalid');
  });

  it('recusa certificado ausente ou ilegível sem importar o addon', async () => {
    const missing = await rejected({ WEBTRANSPORT_CERT_MODE: 'hash' });
    const unreadable = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: join(temp, 'missing.crt'),
    });
    expect(missing.reason).toBe('certificate-invalid');
    expect(unreadable.reason).toBe('listener-start-failed');
  });

  it('hash exige P-256 e validade total de no máximo 14 dias', async () => {
    const rsa = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: shortRsa.certPath,
      WEBTRANSPORT_KEY_PATH: shortRsa.keyPath,
    });
    const long = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: longEc.certPath,
      WEBTRANSPORT_KEY_PATH: longEc.keyPath,
    });
    expect(rsa.reason).toBe('certificate-invalid');
    expect(long.reason).toBe('certificate-invalid');
  });

  it('recusa certificado expirado ou ainda não válido no instante do arranque', async () => {
    for (const current of [expiredEc, futureEc]) {
      const event = await rejected({
        WEBTRANSPORT_CERT_MODE: 'hash',
        WEBTRANSPORT_CERT_PATH: current.certPath,
        WEBTRANSPORT_KEY_PATH: current.keyPath,
      });
      expect(event.reason).toBe('certificate-invalid');
    }
  });

  it('webpki aceita certificado longo e avança até validar a URL pública', async () => {
    const event = await rejected({
      WEBTRANSPORT_CERT_MODE: 'webpki',
      WEBTRANSPORT_CERT_PATH: longEc.certPath,
      WEBTRANSPORT_KEY_PATH: longEc.keyPath,
      WEBTRANSPORT_PUBLIC_URL: 'http://media.example.invalid/wt',
    });
    expect(event.reason).toBe('config-invalid');
  });

  it('webpki não impõe curva P-256 ao certificado RSA válido', async () => {
    const event = await rejected({
      WEBTRANSPORT_CERT_MODE: 'webpki',
      WEBTRANSPORT_CERT_PATH: shortRsa.certPath,
      WEBTRANSPORT_KEY_PATH: shortRsa.keyPath,
      WEBTRANSPORT_PUBLIC_URL: 'http://media.example.invalid/wt',
    });
    expect(event.reason).toBe('config-invalid');
  });

  it('exige chave current e par next completo', async () => {
    const noKey = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: shortEc.certPath,
    });
    const halfNext = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: shortEc.certPath,
      WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
      WEBTRANSPORT_NEXT_CERT_PATH: shortEc.certPath,
    });
    expect(noKey.reason).toBe('certificate-invalid');
    expect(halfNext.reason).toBe('certificate-invalid');
  });

  it.each([['-1'], ['1.5'], ['65536']])('recusa porta UDP inválida %s', async (port) => {
    const event = await rejected({
      WEBTRANSPORT_CERT_PATH: shortEc.certPath,
      WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
      WEBTRANSPORT_PORT: port,
      WEBTRANSPORT_PUBLIC_URL: 'https://media.example.invalid/wt',
    });
    expect(event.reason).toBe('config-invalid');
  });

  it('em produção exige host explícito e porta não zero', async () => {
    const noHost = await rejected(
      {
        WEBTRANSPORT_CERT_PATH: shortEc.certPath,
        WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
        WEBTRANSPORT_PORT: '4443',
        WEBTRANSPORT_PUBLIC_URL: 'https://media.example.invalid/wt',
      },
      true,
    );
    const zeroPort = await rejected(
      {
        WEBTRANSPORT_CERT_PATH: shortEc.certPath,
        WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
        WEBTRANSPORT_HOST: '127.0.0.1',
        WEBTRANSPORT_PORT: '0',
        WEBTRANSPORT_PUBLIC_URL: 'https://media.example.invalid/wt',
      },
      true,
    );
    expect(noHost.reason).toBe('config-invalid');
    expect(zeroPort.reason).toBe('config-invalid');
  });

  it.each([
    [undefined],
    ['http://media.example.invalid/wt'],
    ['https://media.example.invalid/wt?token=no'],
    ['https://media.example.invalid/wt#fragment'],
  ])('recusa URL pública ausente ou não limpa (%s)', async (publicUrl) => {
    const event = await rejected({
      WEBTRANSPORT_CERT_PATH: shortEc.certPath,
      WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
      WEBTRANSPORT_PUBLIC_URL: publicUrl,
    });
    expect(event.reason).toBe('config-invalid');
  });

  it('valida também o certificado next antes de tentar o addon', async () => {
    const event = await rejected({
      WEBTRANSPORT_CERT_MODE: 'hash',
      WEBTRANSPORT_CERT_PATH: shortEc.certPath,
      WEBTRANSPORT_KEY_PATH: shortEc.keyPath,
      WEBTRANSPORT_NEXT_CERT_PATH: shortRsa.certPath,
      WEBTRANSPORT_NEXT_KEY_PATH: shortRsa.keyPath,
      WEBTRANSPORT_PUBLIC_URL: 'https://media.example.invalid/wt',
    });
    expect(event.reason).toBe('certificate-invalid');
  });
});
