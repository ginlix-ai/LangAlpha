"""Where every request the browser makes goes first, so what it may reach is
decided by the address it would connect to rather than by the text of its URL.

Chrome's URL blocklist matches host text and stops only navigations: a page
reaches the same port through another spelling of loopback, the machine's own
address, a name that resolves to it or rebinds to it, or a no-cors fetch or
WebSocket from its own script. Here each request's host is resolved once,
every address it resolves to is checked, and the connection goes to an address
that was checked, never to a second lookup. Chrome sends everything here,
loopback included, which it would otherwise send direct.

Refused: the machine's own service ports at any address that is the machine,
and link-local, cloud metadata, the default gateway and the Docker host at any
port. The rest of loopback stays open, where the person's own apps run.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import struct
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from . import daemon
from .protocol import DOCKER_HOSTS, METADATA_HOSTS, PROXY_PORT, SERVICE_PORTS

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

_LOG = "proxy.log"
# Chrome opens sockets ahead of use and may leave one quiet for a while.
_IDLE_S = 60.0
_RESOLVE_TIMEOUT_S = 10.0
_DIAL_TIMEOUT_S = 10.0
_HEAD_LIMIT = 256 * 1024
_PLACES_TTL_S = 30.0
# What a start waits for the proxy to listen before letting the command go on.
_UP_S = 3.0


def _metadata_addresses() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    # The names among them resolve to link-local addresses, refused already.
    found = []
    for host in METADATA_HOSTS:
        try:
            found.append(ipaddress.ip_network(host))
        except ValueError:
            continue
    return tuple(found)


_ANYWHERE = (
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
    *_metadata_addresses(),
)
_THIS_NETWORK = ipaddress.ip_network("0.0.0.0/8")
_HOP_BY_HOP = frozenset({b"connection", b"keep-alive", b"proxy-connection", b"proxy-authorization"})

_FIB_TRIE = Path("/proc/net/fib_trie")
_IF_INET6 = Path("/proc/net/if_inet6")
_ROUTE = Path("/proc/net/route")
_IPV6_ROUTE = Path("/proc/net/ipv6_route")


def plain(address: IPAddress) -> IPAddress:
    """The address a connection to *address* reaches: an IPv4-mapped
    (``::ffff:a.b.c.d``) or IPv4-compatible (``::a.b.c.d``) one is that IPv4."""
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return address.ipv4_mapped
        packed = address.packed
        if packed[:12] == bytes(12) and int(address) > 1:
            return ipaddress.IPv4Address(packed[12:])
    return address


def _address(text: str) -> IPAddress:
    return plain(ipaddress.ip_address(text.split("%", 1)[0]))


@dataclass(frozen=True)
class Places:
    """The addresses that are this machine, and those of what it runs on."""

    own: frozenset[IPAddress] = frozenset()
    hosts: frozenset[IPAddress] = frozenset()


def refused(address: IPAddress, port: int, places: Places) -> bool:
    address = plain(address)
    if address in places.hosts or any(address in network for network in _ANYWHERE):
        return True
    this_machine = (
        address.is_loopback
        or address.is_unspecified
        or address in places.own
        or (address.version == 4 and address in _THIS_NETWORK)
    )
    return this_machine and port in SERVICE_PORTS


def _own_addresses() -> set[IPAddress]:
    found: set[IPAddress] = set()
    try:
        # A /32 marked LOCAL under the address it follows is one of ours.
        last = ""
        for line in _FIB_TRIE.read_text().splitlines():
            text = line.strip()
            if text.startswith("|--"):
                last = text[3:].strip()
            elif text.startswith("/32 host LOCAL") and last:
                found.add(_address(last))
    except (OSError, ValueError):
        pass
    try:
        for line in _IF_INET6.read_text().splitlines():
            found.add(plain(ipaddress.IPv6Address(bytes.fromhex(line.split()[0]))))
    except (OSError, ValueError, IndexError):
        pass
    try:
        for *_, sockaddr in socket.getaddrinfo(socket.gethostname(), None, type=socket.SOCK_STREAM):
            found.add(_address(sockaddr[0]))
    except (OSError, ValueError):
        pass
    # The address a connection out would leave from, where /proc says nothing.
    for family, probe in ((socket.AF_INET, "192.0.2.1"), (socket.AF_INET6, "2001:db8::1")):
        try:
            with socket.socket(family, socket.SOCK_DGRAM) as s:
                s.connect((probe, 9))
                found.add(_address(s.getsockname()[0]))
        except (OSError, ValueError):
            pass
    return found


def _host_addresses() -> set[IPAddress]:
    """The default gateway's addresses and the Docker host's."""
    found: set[IPAddress] = set()
    try:
        for line in _ROUTE.read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) > 2 and fields[1] == "00000000" and int(fields[2], 16):
                found.add(ipaddress.IPv4Address(struct.pack("<I", int(fields[2], 16))))
    except (OSError, ValueError):
        pass
    try:
        for line in _IPV6_ROUTE.read_text().splitlines():
            fields = line.split()
            if len(fields) > 4 and fields[0] == "0" * 32 and fields[1] == "00" and int(fields[4], 16):
                found.add(plain(ipaddress.IPv6Address(bytes.fromhex(fields[4]))))
    except (OSError, ValueError):
        pass
    for name in DOCKER_HOSTS:
        try:
            for *_, sockaddr in socket.getaddrinfo(name, None, type=socket.SOCK_STREAM):
                found.add(_address(sockaddr[0]))
        except (OSError, ValueError):
            pass
    return found


def read_places() -> Places:
    return Places(frozenset(_own_addresses()), frozenset(_host_addresses()))


@dataclass(frozen=True)
class Request:
    """Where a request asks to go, and for plain HTTP the head to send there."""

    host: str
    port: int
    head: bytes | None = None


def parse_head(head: bytes) -> Request | bytes:
    """The request a proxy head asks for, or the status to refuse it with.

    ``CONNECT host:port`` opens a tunnel (HTTPS and every WebSocket); anything
    else must name an ``http://`` URL, which is sent on in origin form asking
    the server to close after its answer, so no second request rides a
    connection made for another host.
    """
    lines = daemon.split_head(head)
    if lines is None:
        return b"400 Bad Request"
    parts = lines[0].split(b" ")
    if len(parts) != 3 or not parts[2].startswith(b"HTTP/1."):
        return b"400 Bad Request"
    method, target, version = parts
    try:
        text = target.decode("ascii")
    except UnicodeDecodeError:
        return b"400 Bad Request"
    if method == b"CONNECT":
        host, _, port = text.rpartition(":")
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        if not host or not port.isdigit() or not 0 < int(port) < 65536:
            return b"400 Bad Request"
        return Request(host, int(port))
    try:
        url = urlsplit(text)
        port = url.port or 80
    except ValueError:
        return b"400 Bad Request"
    if url.scheme.lower() != "http" or not url.hostname or url.username is not None:
        return b"400 Bad Request"
    kept = []
    upgrade = False
    for line in lines[1:]:
        name, colon, _ = line.partition(b":")
        if not colon or line[:1] in (b" ", b"\t"):
            return b"400 Bad Request"
        name = name.strip().lower()
        upgrade = upgrade or name == b"upgrade"
        if name not in _HOP_BY_HOP:
            kept.append(line)
    origin_form = (url.path or "/") + (f"?{url.query}" if url.query else "")
    connection = b"Connection: Upgrade" if upgrade else b"Connection: close"
    sent = [method + b" " + origin_form.encode() + b" " + version, *kept, connection]
    return Request(url.hostname, port, b"\r\n".join(sent) + b"\r\n\r\n")


def closing(head: bytes) -> bytes:
    """A response head that tells the browser not to reuse this connection."""
    lines = head[: -len(b"\r\n\r\n")].split(b"\r\n")
    kept = [lines[0]] + [
        line for line in lines[1:] if line.partition(b":")[0].strip().lower() not in _HOP_BY_HOP
    ]
    return b"\r\n".join([*kept, b"Connection: close"]) + b"\r\n\r\n"


def _status(head: bytes) -> int:
    try:
        return int(head.split(b" ", 2)[1])
    except (IndexError, ValueError):
        return 0


class Proxy:
    def __init__(self, places: Places) -> None:
        self.places = places
        self.read_at = time.monotonic()

    async def refresh(self) -> None:
        if time.monotonic() - self.read_at < _PLACES_TTL_S:
            return
        self.read_at = time.monotonic()
        self.places = await asyncio.get_running_loop().run_in_executor(None, read_places)

    async def addresses(self, host: str, port: int) -> list[str] | bytes:
        """The addresses to dial for *host*, or the status to refuse with.
        One refused address refuses the name, so a name cannot point at both."""
        try:
            infos = await asyncio.wait_for(
                asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM),
                _RESOLVE_TIMEOUT_S,
            )
        except (OSError, TimeoutError, UnicodeError):
            return b"502 Bad Gateway"
        found: list[str] = []
        for *_, sockaddr in infos:
            try:
                address = ipaddress.ip_address(sockaddr[0].split("%", 1)[0])
            except ValueError:
                return b"403 Forbidden"
            if refused(address, port, self.places):
                return b"403 Forbidden"
            if sockaddr[0] not in found:
                found.append(sockaddr[0])
        return found or b"502 Bad Gateway"

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), _IDLE_S)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, OSError):
            writer.close()
            return
        request = parse_head(head)
        if isinstance(request, bytes):
            await daemon.answer(writer, request)
            return
        await self.refresh()
        addresses = await self.addresses(request.host, request.port)
        if isinstance(addresses, bytes):
            await daemon.answer(writer, addresses)
            return
        upstream = await _dial(addresses, request.port)
        if upstream is None:
            await daemon.answer(writer, b"502 Bad Gateway")
            return
        up_reader, up_writer = upstream
        sending = daemon.pipe(reader, up_writer, half_close=True)
        if request.head is None:
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            answering = daemon.pipe(up_reader, writer, half_close=True)
        else:
            up_writer.write(request.head)
            answering = _answer_from(up_reader, writer)
        try:
            await _both_ways(sending, answering)
        finally:
            for w in (writer, up_writer):
                w.close()


async def _both_ways(sending: Coroutine[Any, Any, None], answering: Coroutine[Any, Any, None]) -> None:
    """Carry both directions until both end, the second given ``_IDLE_S`` once
    the first has: a request the browser gave up on, to a server that never
    answers, would otherwise hold both sockets as long as the proxy runs. A
    long poll is unbounded while the browser still holds its side open."""
    tasks = {asyncio.ensure_future(sending), asyncio.ensure_future(answering)}
    try:
        _done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if pending:
            await asyncio.wait(pending, timeout=_IDLE_S)
    finally:
        for task in tasks:
            task.cancel()


async def _dial(addresses: list[str], port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
    for address in addresses:
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(address, port, limit=_HEAD_LIMIT), _DIAL_TIMEOUT_S
            )
        except (OSError, TimeoutError):
            continue
    return None


async def _answer_from(up_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """The server's answer, its final head marked to close the connection."""
    try:
        while True:
            head = await up_reader.readuntil(b"\r\n\r\n")
            status = _status(head)
            if 100 <= status < 200 and status != 101:
                writer.write(head)
                continue
            writer.write(head if status == 101 else closing(head))
            break
    except asyncio.IncompleteReadError as e:
        writer.write(e.partial)
        writer.close()
        return
    except (asyncio.LimitOverrunError, OSError):
        writer.close()
        return
    await daemon.pipe(up_reader, writer, half_close=True)


def ensure() -> bool:
    """Whether the proxy is up, starting it and waiting a moment when it is not.
    A browser that starts without it reaches nothing, which is the safe way to fail."""
    return daemon.ensure("proxy", PROXY_PORT, _LOG, _UP_S)


def serve() -> int:
    listener = daemon.bind("127.0.0.1", PROXY_PORT, 256)
    if listener is None:
        return 0

    async def main() -> None:
        proxy = Proxy(await asyncio.get_running_loop().run_in_executor(None, read_places))

        async def tick(up: bool) -> None:
            await proxy.refresh()

        await daemon.serve_while_stream(listener, proxy.handle, _HEAD_LIMIT, tick)

    asyncio.run(main())
    return 0

