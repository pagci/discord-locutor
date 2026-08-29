import { describe, expect, it } from 'vitest';

const liveSuite = process.env.WEBTRANSPORT_LIVE === '1' ? describe : describe.skip;

describe('fork de backpressure dos datagramas WebTransport', () => {
  it.each(['blocked', 'tooBig'])(
    'expõe %s sem envenenar o writer reutilizável',
    async (nativeCode) => {
      let code = nativeCode;
      const internal = await import('../vendor/fails-components-webtransport/lib/session.js');
      const native = {
        writeDatagram: () => ({ code, message: `${code}-native` }),
      };
      const session = new internal.HttpWTSession({ object: native, parentobj: {} });
      const writable = session.datagrams.createWritable();
      const writer = writable.getWriter();

      await expect(writer.write(new Uint8Array([1]))).resolves.toBeUndefined();
      expect(writable.lastWriteStatus).toEqual({
        code: nativeCode,
        message: `${nativeCode}-native`,
      });

      code = 'success';
      await expect(writer.write(new Uint8Array([2]))).resolves.toBeUndefined();
      expect(writable.lastWriteStatus).toEqual({
        code: 'success',
        message: 'success-native',
      });
      await writer.close();
    },
  );
});

liveSuite('fork optional do lifecycle nativo WebTransport', () => {
  it('retém e libera o efeito real de onClose sem fechar controller duas vezes', async () => {
    const [addon, internal] = await Promise.all([
      import('@fails-components/webtransport'),
      import('../vendor/fails-components-webtransport/lib/session.js'),
    ]);
    expect(addon.setOnCloseEffectHook).toBe(internal.setOnCloseEffectHook);

    const session = new internal.HttpWTSession({ parentobj: {} });
    session.onReady({ protocol: 'h3' });
    const reader = session.incomingBidirectionalStreams.getReader();
    const pendingRead = reader.read();
    await reader.cancel('adapter-handshake-timeout');
    await expect(pendingRead).resolves.toEqual({ done: true, value: undefined });

    let retained;
    const restore = addon.setOnCloseEffectHook(({ session: observed, effect }) => {
      expect(observed).toBe(session);
      retained = effect;
    });
    try {
      expect(() =>
        session.onClose({ errorcode: 1, error: 'late-native-controller-close' }),
      ).not.toThrow();
      await expect(session.closed).resolves.toEqual({
        closeCode: 1,
        reason: 'late-native-controller-close',
      });
      expect(retained).toBeTypeOf('function');
      expect(() => retained()).not.toThrow();
      expect(() => retained()).not.toThrow();
    } finally {
      restore();
      reader.releaseLock();
    }
  });

  it('rejeita hook inválido sem alterar a seam pública', async () => {
    const { setOnCloseEffectHook } = await import('@fails-components/webtransport');
    expect(() => setOnCloseEffectHook('invalid')).toThrow(TypeError);
    const restore = setOnCloseEffectHook(null);
    expect(restore).toBeTypeOf('function');
    restore();
  });
});
