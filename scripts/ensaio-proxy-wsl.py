#!/usr/bin/env python3
"""Proxy TCP/UDP minimo para colocar o trafego real do ensaio sob tc netem.

O processo roda dentro do WSL. TCP encaminha HTTP/WebSocket; UDP encaminha
QUIC/WebTransport. Cada cliente UDP recebe um socket upstream conectado e
independente; isso preserva simultaneamente as sessoes QUIC da origem e do
espectador sem tentar interpretar connection IDs criptografados.
"""

import argparse
import asyncio
import signal


async def pump(reader, writer):
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def tcp_client(reader, writer, target_host, target_port):
    upstream_reader, upstream_writer = await asyncio.open_connection(target_host, target_port)
    await asyncio.gather(
        pump(reader, upstream_writer),
        pump(upstream_reader, writer),
        return_exceptions=True,
    )


class UdpUpstream(asyncio.DatagramProtocol):
    def __init__(self, client, downstream, on_lost):
        self.client = client
        self.downstream = downstream
        self.on_lost = on_lost

    def datagram_received(self, data, _addr):
        self.downstream.sendto(data, self.client)

    def connection_lost(self, _error):
        self.on_lost(self.client)


class UdpProxy(asyncio.DatagramProtocol):
    def __init__(self, target_host, target_port):
        self.target = (target_host, target_port)
        self.transport = None
        self.upstreams = {}
        self.pending = {}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        upstream = self.upstreams.get(addr)
        if upstream:
            upstream.sendto(data)
            return
        queue = self.pending.setdefault(addr, [])
        queue.append(data)
        if len(queue) == 1:
            asyncio.create_task(self.open_upstream(addr))

    async def open_upstream(self, client):
        try:
            loop = asyncio.get_running_loop()
            upstream, _ = await loop.create_datagram_endpoint(
                lambda: UdpUpstream(client, self.transport, self.drop_client),
                remote_addr=self.target,
            )
            self.upstreams[client] = upstream
            for data in self.pending.pop(client, []):
                upstream.sendto(data)
        except Exception:
            self.pending.pop(client, None)
            self.drop_client(client)
            raise

    def drop_client(self, client):
        upstream = self.upstreams.pop(client, None)
        if upstream:
            upstream.close()

    def connection_lost(self, _error):
        for upstream in list(self.upstreams.values()):
            upstream.close()
        self.upstreams.clear()
        self.pending.clear()


async def main(args):
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for name in ("SIGINT", "SIGTERM"):
        loop.add_signal_handler(getattr(signal, name), stop.set)

    server = await asyncio.start_server(
        lambda reader, writer: tcp_client(reader, writer, args.target, args.tcp_target),
        host=args.listen,
        port=args.tcp_listen,
    )
    udp_transport, _ = await loop.create_datagram_endpoint(
        lambda: UdpProxy(args.target, args.udp_target),
        local_addr=(args.listen, args.udp_listen),
    )
    print(
        f"PROXY_READY tcp={args.listen}:{args.tcp_listen}->{args.target}:{args.tcp_target} "
        f"udp={args.listen}:{args.udp_listen}->{args.target}:{args.udp_target}",
        flush=True,
    )
    try:
        await stop.wait()
    finally:
        udp_transport.close()
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--target", required=True)
    parser.add_argument("--tcp-listen", type=int, default=3200)
    parser.add_argument("--tcp-target", type=int, default=3100)
    parser.add_argument("--udp-listen", type=int, default=4444)
    parser.add_argument("--udp-target", type=int, default=4443)
    asyncio.run(main(parser.parse_args()))
