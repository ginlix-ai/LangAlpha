"""Unit tests for the SSRF egress guard transport."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest

from src.tools.web.inhouse.guard import (
    BlockedAddressError,
    GuardedAsyncTransport,
    _ip_is_blocked,
    _resolve,
)


class TestIpClassification:
    @pytest.mark.parametrize(
        "ip",
        ["127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "::1"],
    )
    def test_blocked(self, ip):
        assert _ip_is_blocked(ip) is True

    @pytest.mark.parametrize("ip", ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"])
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
    async def test_redirect_to_private_is_blocked(self):
        """A public page 302-ing to an internal address fails on the second hop."""

        async def fake_super(self, request):  # noqa: ANN001
            if request.url.host == "example.com":
                return httpx.Response(302, headers={"location": "http://169.254.169.254/"}, request=request)
            return httpx.Response(200, content=b"secret", request=request)

        with patch("src.tools.web.inhouse.guard._resolve", side_effect=lambda h: ["93.184.216.34"] if h == "example.com" else [h]), patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", fake_super
        ):
            async with httpx.AsyncClient(transport=GuardedAsyncTransport(), follow_redirects=True) as client:
                with pytest.raises(BlockedAddressError, match="private/reserved"):
                    await client.get("http://example.com/report")
