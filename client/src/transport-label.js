export function relayTransportLabel({ transport, attemptedWebTransport, fallbackReason } = {}) {
  if (!transport) return 'relay negociando…';
  if (transport === 'webtransport')
    return 'relay WebTransport · QUIC/HTTP/3 híbrido · deltas datagrama + FEC adaptativa';
  if (attemptedWebTransport && fallbackReason)
    return `relay WebSocket · TCP/TLS · fallback do WebTransport: ${fallbackReason}`;
  return 'relay WebSocket · TCP/TLS';
}

export function rtcTransportLabel({ relay, protocol } = {}) {
  if (relay === true) return `WebRTC via TURN${protocol ? ` · ${protocol.toUpperCase()}` : ''}`;
  if (relay === false)
    return `WebRTC direto P2P${protocol ? ` · SRTP/${protocol.toUpperCase()}` : ' · SRTP'}`;
  return 'WebRTC direto · ICE/SRTP (verificando TURN…)';
}
