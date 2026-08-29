import { describe, expect, it } from 'vitest';
import { relayTransportLabel, rtcTransportLabel } from './transport-label.js';

describe('rótulo inequívoco do transporte', () => {
  it('identifica WebTransport como QUIC/HTTP/3', () => {
    expect(relayTransportLabel({ transport: 'webtransport' })).toBe(
      'relay WebTransport · QUIC/HTTP/3 híbrido · deltas datagrama + FEC adaptativa',
    );
  });

  it('identifica WebSocket como TCP/TLS e preserva o fallback', () => {
    expect(relayTransportLabel({ transport: 'websocket' })).toBe('relay WebSocket · TCP/TLS');
    expect(
      relayTransportLabel({
        transport: 'websocket',
        attemptedWebTransport: true,
        fallbackReason: 'timeout',
      }),
    ).toContain('TCP/TLS · fallback do WebTransport: timeout');
  });

  it('distingue WebRTC P2P de TURN e mostra o protocolo ICE selecionado', () => {
    expect(rtcTransportLabel({ relay: false, protocol: 'udp' })).toBe(
      'WebRTC direto P2P · SRTP/UDP',
    );
    expect(rtcTransportLabel({ relay: true, protocol: 'tcp' })).toBe('WebRTC via TURN · TCP');
  });
});
