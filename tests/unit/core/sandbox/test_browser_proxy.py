"""The proxy every request of the computer's browser goes through is what keeps a
page off the machine's own services, so which address and port it refuses is the
contract, and that it decides on the address it then connects to."""

from __future__ import annotations

import asyncio
import ipaddress

import pytest

from ptc_agent.core.sandbox.browser_runtime import protocol, proxy

OWN = ipaddress.ip_address("172.17.0.5")
GATEWAY = ipaddress.ip_address("172.17.0.1")
DOCKER_HOST = ipaddress.ip_address("192.168.65.254")
PLACES = proxy.Places(own=frozenset({OWN}), hosts=frozenset({GATEWAY, DOCKER_HOST}))


@pytest.mark.parametrize(
    ("address", "port", "refused"),
    [
        # Loopback, in every spelling, on a service port.
        ("127.0.0.1", 9224, True),
        ("127.0.0.2", 9224, True),
        ("127.255.0.1", 2280, True),
        ("::1", 9223, True),
        ("::ffff:7f00:1", 9224, True),
        ("::ffff:127.0.0.2", 22220, True),
        ("::7f00:1", 49983, True),
        ("0.0.0.0", 9224, True),
        ("0.1.2.3", 2375, True),
        ("::", 2376, True),
        ("127.0.0.1", protocol.PROXY_PORT, True),
        ("127.0.0.1", 22222, True),
        # The machine's own address on a service port.
        (str(OWN), 9224, True),
        (f"::ffff:{OWN}", 2280, True),
        # Link-local and metadata on any port.
        ("169.254.169.254", 80, True),
        ("169.254.170.2", 80, True),
        ("::ffff:169.254.169.254", 80, True),
        ("fd00:ec2::254", 80, True),
        ("100.100.100.200", 80, True),
        ("fe80::1", 8080, True),
        # The gateway and the Docker host on any port.
        (str(GATEWAY), 80, True),
        (str(DOCKER_HOST), 3000, True),
        # The person's own apps, wherever they listen on this machine.
        ("127.0.0.1", 3000, False),
        ("::1", 8080, False),
        ("::ffff:127.0.0.1", 5173, False),
        ("0.0.0.0", 3000, False),
        (str(OWN), 3000, False),
        # The rest of the world, on any port.
        ("93.184.215.14", 443, False),
        ("93.184.215.14", 9224, False),
        ("2606:2800:21f:cb07:6820:80da:af6b:8b2c", 80, False),
        ("10.0.0.7", 2280, False),
    ],
)
def test_what_the_browser_may_reach(address: str, port: int, refused: bool):
    assert proxy.refused(ipaddress.ip_address(address), port, PLACES) is refused


def test_a_connect_names_where_the_tunnel_goes():
    assert proxy.parse_head(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n") == (
        proxy.Request("example.com", 443)
    )
    assert proxy.parse_head(b"CONNECT [::1]:9224 HTTP/1.1\r\n\r\n") == proxy.Request("::1", 9224)


def test_a_plain_request_goes_on_in_origin_form_and_asks_to_close():
    request = proxy.parse_head(
        b"GET http://127.0.0.1.nip.io:9224/a?b=1 HTTP/1.1\r\n"
        b"Host: 127.0.0.1.nip.io:9224\r\n"
        b"Proxy-Connection: keep-alive\r\n"
        b"Connection: keep-alive\r\n"
        b"Accept: */*\r\n\r\n"
    )
    assert isinstance(request, proxy.Request)
    assert (request.host, request.port) == ("127.0.0.1.nip.io", 9224)
    assert request.head == (
        b"GET /a?b=1 HTTP/1.1\r\nHost: 127.0.0.1.nip.io:9224\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    )


@pytest.mark.parametrize(
    "head",
    [
        b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
        b"GET https://example.com/ HTTP/1.1\r\n\r\n",
        b"GET http://user@example.com/ HTTP/1.1\r\n\r\n",
        b"GET http://example.com:99999/ HTTP/1.1\r\n\r\n",
        b"CONNECT example.com HTTP/1.1\r\n\r\n",
        b"GET http://example.com/ HTTP/1.1\r\nHost: a\rb\r\n\r\n",
        b"GET http://example.com/ HTTP/1.1\r\n folded\r\n\r\n",
        b"GET http://example.com/\r\n\r\n",
    ],
)
def test_anything_else_is_a_bad_request(head: bytes):
    assert proxy.parse_head(head) == b"400 Bad Request"


def test_an_answer_tells_the_browser_not_to_reuse_the_connection():
    head = b"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nKeep-Alive: timeout=5\r\nContent-Length: 2\r\n\r\n"
    assert proxy.closing(head) == b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n"


@pytest.mark.enable_socket  # resolves localhost, nothing beyond it
@pytest.mark.asyncio
async def test_a_name_refused_at_any_of_its_addresses_is_refused():
    p = proxy.Proxy(PLACES)
    assert await p.addresses("localhost", 9224) == b"403 Forbidden"
    assert await p.addresses("127.0.0.2", 2280) == b"403 Forbidden"
    found = await p.addresses("localhost", 3000)
    assert isinstance(found, list) and found
    assert all(ipaddress.ip_address(a).is_loopback for a in found)


@pytest.mark.enable_socket  # loopback listeners, nothing beyond them
@pytest.mark.asyncio
async def test_requests_through_the_proxy_reach_an_app_and_not_a_service(monkeypatch):
    """Over real sockets: a plain request and a tunnel reach an app on loopback,
    and the same app on a service port is answered 403 without being dialed."""
    dialed = []

    async def app(reader, writer):
        dialed.append(True)
        head = await reader.readuntil(b"\r\n\r\n")
        if head.startswith(b"GET"):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nhi")
        else:
            writer.write(b"echo:" + head)
        await writer.drain()
        writer.close()

    upstream = await asyncio.start_server(app, "127.0.0.1", 0)
    app_port = upstream.sockets[0].getsockname()[1]
    server = await asyncio.start_server(proxy.Proxy(PLACES).handle, "127.0.0.1", 0)
    proxy_port = server.sockets[0].getsockname()[1]

    async def ask(request: bytes) -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(request)
        await writer.drain()
        answer = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return answer

    async with upstream, server:
        answer = await ask(f"GET http://localhost:{app_port}/ HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        assert answer.startswith(b"HTTP/1.1 200 OK\r\n")
        assert b"Connection: close" in answer and b"keep-alive" not in answer
        assert answer.endswith(b"hi")

        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(f"CONNECT 127.0.0.1:{app_port} HTTP/1.1\r\n\r\n".encode())
        assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 200 Connection Established\r\n\r\n"
        writer.write(b"PING\r\n\r\n")
        assert await asyncio.wait_for(reader.read(), 5) == b"echo:PING\r\n\r\n"
        writer.close()

        dialed.clear()
        monkeypatch.setattr(proxy, "SERVICE_PORTS", (app_port,))
        for target in (f"127.0.0.2:{app_port}", f"[::ffff:7f00:1]:{app_port}", f"localhost:{app_port}"):
            assert (await ask(f"CONNECT {target} HTTP/1.1\r\n\r\n".encode())).startswith(
                b"HTTP/1.1 403 Forbidden\r\n"
            )
            assert (
                await ask(f"GET http://{target}/ HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            ).startswith(b"HTTP/1.1 403 Forbidden\r\n")
        assert dialed == []


@pytest.mark.enable_socket  # loopback listeners, nothing beyond them
@pytest.mark.asyncio
@pytest.mark.parametrize("tunnel", [False, True], ids=["plain", "connect"])
async def test_a_request_the_browser_gave_up_on_lets_go_of_a_silent_server(monkeypatch, tunnel):
    """A server that never answers would otherwise hold both sockets for as long
    as the proxy runs, once the browser has closed its side."""
    monkeypatch.setattr(proxy, "_IDLE_S", 0.1)
    browser_gone = asyncio.Event()
    test_done = asyncio.Event()

    async def silent(reader, writer):
        await reader.read()  # until the proxy passes on the browser's end
        browser_gone.set()
        await test_done.wait()  # and never answers or closes before then
        writer.close()

    upstream = await asyncio.start_server(silent, "127.0.0.1", 0)
    port = upstream.sockets[0].getsockname()[1]
    handled = []

    async def handle(reader, writer):
        await proxy.Proxy(PLACES).handle(reader, writer)
        handled.append(True)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with upstream, server:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
        if tunnel:
            writer.write(f"CONNECT 127.0.0.1:{port} HTTP/1.1\r\n\r\n".encode())
            await reader.readuntil(b"\r\n\r\n")
        else:
            writer.write(f"GET http://127.0.0.1:{port}/ HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        await writer.drain()
        writer.close()
        await asyncio.wait_for(browser_gone.wait(), 5)
        for _ in range(50):
            if handled:
                break
            await asyncio.sleep(0.02)
        test_done.set()
        assert handled == [True]


@pytest.mark.enable_socket  # its own name and a route lookup that sends nothing
def test_the_machine_s_own_places_are_read_from_its_tables(tmp_path, monkeypatch):
    fib = tmp_path / "fib_trie"
    fib.write_text(
        "Main:\n  +-- 0.0.0.0/0 3 0 5\n     |-- 0.0.0.0\n        /0 universe UNICAST\n"
        "     |-- 172.17.0.5\n        /32 host LOCAL\n     |-- 172.17.0.255\n        /32 link BROADCAST\n"
    )
    route = tmp_path / "route"
    route.write_text(
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\n"
        "eth0\t00000000\t010011AC\t0003\t0\t0\t0\t00000000\n"
        "eth0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\n"
    )
    monkeypatch.setattr(proxy, "_FIB_TRIE", fib)
    monkeypatch.setattr(proxy, "_ROUTE", route)
    monkeypatch.setattr(proxy, "_IF_INET6", tmp_path / "absent")
    monkeypatch.setattr(proxy, "_IPV6_ROUTE", tmp_path / "absent")
    monkeypatch.setattr(proxy, "DOCKER_HOSTS", ())
    places = proxy.read_places()
    assert OWN in places.own
    assert ipaddress.ip_address("172.17.0.255") not in places.own
    assert places.hosts == {GATEWAY}
