"""Unit tests for the SSRF egress guard transport."""

from __future__ import annotations

import asyncio
import socket
from unittest.mock import patch

import httpx
import pytest

from src.tools.web.inhouse.guard import (
    BlockedAddressError,
    GuardedAsyncTransport,
    UnresolvableHost,
    _ip_is_blocked,
    _resolve,
    fetch_capped,
)
from src.tools.web.inhouse import guard


class TestIpClassification:
    @pytest.mark.parametrize(
        "ip",
        ["127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "::1"],
    )
    def test_blocked(self, ip):
        assert _ip_is_blocked(ip) is True

    @pytest.mark.parametrize(
        "ip",
        [
            "100.100.100.200",
            "198.51.100.7",
            "224.0.0.1",
            "::ffff:10.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "::a9fe:a9fe",
            "fd00::1",
            "fec0::1",
        ],
        ids=[
            "shared-space-metadata",
            "documentation",
            "multicast",
            "v4-mapped-private",
            "nat64-metadata",
            "v4-compatible-metadata",
            "ula",
            "site-local",
        ],
    )
    def test_blocked_beyond_the_private_ranges(self, ip):
        assert _ip_is_blocked(ip) is True

    @pytest.mark.parametrize("ip", ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111", "::ffff:8.8.8.8"])
    def test_allowed(self, ip):
        assert _ip_is_blocked(ip) is False

    def test_resolve_raw_ip_is_identity(self):
        assert _resolve("203.0.113.5") == ["203.0.113.5"]


class TestTransportGuard:
    """The transport must reject blocked targets before delegating to the network."""

    def _request(self, url: str) -> httpx.Request:
        return httpx.Request("GET", url)

    @pytest.mark.asyncio
    async def test_literal_private_ip_blocked(self):
        transport = GuardedAsyncTransport()
        with pytest.raises(BlockedAddressError, match="private/reserved"):
            await transport.handle_async_request(self._request("http://169.254.169.254/latest/meta-data"))

    @pytest.mark.asyncio
    async def test_loopback_blocked(self):
        transport = GuardedAsyncTransport()
        with pytest.raises(BlockedAddressError):
            await transport.handle_async_request(self._request("http://127.0.0.1:8003/api/auth/validate"))

    @pytest.mark.asyncio
    async def test_localhost_blocked(self):
        transport = GuardedAsyncTransport()
        with pytest.raises(BlockedAddressError, match="Blocked host"):
            await transport.handle_async_request(self._request("http://localhost/x"))

    @pytest.mark.asyncio
    async def test_dns_rebinding_blocked(self):
        """A public hostname that resolves to a private IP is rejected."""
        transport = GuardedAsyncTransport()
        with patch("src.tools.web.inhouse.guard._resolve", return_value=["10.1.2.3"]):
            with pytest.raises(BlockedAddressError, match="private/reserved"):
                await transport.handle_async_request(self._request("http://evil.example/pdf"))

    @pytest.mark.asyncio
    async def test_public_host_delegates(self):
        """A public target is passed through to the real transport layer."""
        sentinel = httpx.Response(200, content=b"ok")
        with patch("src.tools.web.inhouse.guard._resolve", return_value=["93.184.216.34"]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", return_value=sentinel
        ) as delegated:
            transport = GuardedAsyncTransport()
            resp = await transport.handle_async_request(self._request("http://example.com/page"))
        assert resp is sentinel
        delegated.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("url", "address", "dialed"),
        [
            ("https://example.com/a.png?x=1", "93.184.216.34", "https://93.184.216.34/a.png?x=1"),
            ("http://example.com:8080/a", "2606:4700:4700::1111", "http://[2606:4700:4700::1111]:8080/a"),
        ],
    )
    async def test_the_checked_address_is_the_one_dialed(self, url, address, dialed):
        """Handed the hostname, the connection would resolve it again, and a
        second answer could name a private address."""
        with patch("src.tools.web.inhouse.guard._resolve", return_value=[address]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", return_value=httpx.Response(200)
        ) as delegated:
            await GuardedAsyncTransport().handle_async_request(self._request(url))

        (sent,) = delegated.await_args.args
        assert str(sent.url) == dialed
        assert sent.headers["host"] == httpx.URL(url).netloc.decode()
        assert sent.extensions.get("sni_hostname") == ("example.com" if url.startswith("https") else None)

    @pytest.mark.asyncio
    async def test_a_checked_address_that_refuses_the_connection_falls_to_the_next(self):
        """A dial by hostname tries each record in turn, so one dead record
        (round-robin DNS, an unroutable IPv6 first) does not fail the fetch."""
        dialed: list[str] = []

        async def connect(self, request):  # noqa: ANN001
            dialed.append(request.url.host)
            if len(dialed) == 1:
                raise httpx.ConnectError("refused", request=request)
            return httpx.Response(200, request=request)

        with patch("src.tools.web.inhouse.guard._resolve", return_value=["2606:4700:4700::1111", "93.184.216.34", "93.184.216.34"]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", connect
        ):
            response = await GuardedAsyncTransport().handle_async_request(self._request("https://example.com/a.png"))

        assert response.status_code == 200
        assert dialed == ["2606:4700:4700::1111", "93.184.216.34"]

    @pytest.mark.asyncio
    async def test_the_last_address_refusing_the_connection_fails_the_fetch(self):
        async def refuse(self, request):  # noqa: ANN001
            raise httpx.ConnectError("refused", request=request)

        with patch("src.tools.web.inhouse.guard._resolve", return_value=["93.184.216.34", "93.184.216.35"]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", refuse
        ):
            with pytest.raises(httpx.ConnectError):
                await GuardedAsyncTransport().handle_async_request(self._request("https://example.com/a.png"))

    @pytest.mark.asyncio
    async def test_a_host_that_does_not_resolve_says_so(self):
        with patch("src.tools.web.inhouse.guard._resolve", side_effect=socket.gaierror("no such host")):
            with pytest.raises(UnresolvableHost):
                await GuardedAsyncTransport().handle_async_request(self._request("http://nowhere.example/"))

    @pytest.mark.asyncio
    async def test_redirect_to_private_is_blocked(self):
        """A public page 302-ing to an internal address fails on the second hop."""

        async def fake_super(self, request):  # noqa: ANN001
            if request.headers["host"] == "example.com":
                return httpx.Response(302, headers={"location": "http://169.254.169.254/"}, request=request)
            return httpx.Response(200, content=b"secret", request=request)

        with patch("src.tools.web.inhouse.guard._resolve", side_effect=lambda h: ["93.184.216.34"] if h == "example.com" else [h]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", fake_super
        ):
            async with httpx.AsyncClient(transport=GuardedAsyncTransport(), follow_redirects=True) as client:
                with pytest.raises(BlockedAddressError, match="private/reserved"):
                    await client.get("http://example.com/report")


class TestConnectionReuse:
    @pytest.mark.enable_socket
    @pytest.mark.asyncio
    async def test_a_hostname_never_rides_a_connection_another_one_opened(self):
        """Dialed by address, a kept connection would carry the next hostname
        on that address over the first one's TLS session."""
        connections: list[str] = []

        async def serve(reader, writer):
            connections.append("opened")
            while line := await reader.readline():
                if line == b"\r\n":
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    await writer.drain()
            writer.close()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            with patch.object(guard, "_resolve", return_value=["127.0.0.1"]), patch.object(guard, "_ip_is_blocked", return_value=False):
                async with httpx.AsyncClient(transport=GuardedAsyncTransport()) as client:
                    for host in ("a.example.com", "b.example.com"):
                        assert (await client.get(f"http://{host}:{port}/")).text == "ok"
        finally:
            server.close()
            await server.wait_closed()

        assert connections == ["opened", "opened"]


class TestFetchCappedRedirects:
    @staticmethod
    def _serving(monkeypatch, respond):
        monkeypatch.setattr(guard, "GuardedAsyncTransport", lambda: httpx.MockTransport(respond))

    @pytest.mark.asyncio
    async def test_a_redirect_body_is_never_read(self, monkeypatch):
        """httpx reads and decodes a followed redirect's whole body before the
        cap could see it, so a large or compressed one must not be read."""
        read: list[str] = []

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                read.append("redirect")
                yield b"x" * 1024

        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/a":
                return httpx.Response(302, headers={"location": "/b", "content-encoding": "gzip"}, stream=Body())
            return httpx.Response(200, content=b"image")

        self._serving(monkeypatch, respond)

        assert await fetch_capped("https://example.com/a", lambda _body: 100) == b"image"
        assert read == []

    @pytest.mark.asyncio
    async def test_a_redirect_loop_ends_the_fetch(self, monkeypatch):
        self._serving(monkeypatch, lambda request: httpx.Response(302, headers={"location": "/a"}))

        with pytest.raises(httpx.TooManyRedirects):
            await fetch_capped("https://example.com/a", lambda _body: 100)


class TestFetchDeadline:
    @pytest.mark.asyncio
    async def test_a_fetch_that_outlasts_the_deadline_times_out(self, monkeypatch):
        """Each read may be quick while the whole answer never ends."""

        async def forever(_url, _cap):
            await asyncio.sleep(60)

        monkeypatch.setattr(guard, "_fetch_capped", forever)
        monkeypatch.setattr(guard, "FETCH_DEADLINE", 0.05)

        with pytest.raises(httpx.TimeoutException):
            await fetch_capped("https://example.com/a.png", lambda _body: 100)
