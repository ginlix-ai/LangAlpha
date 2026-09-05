"""PinnedSessionTransport — the SSRF pin and byte cap under an SDK session.

The SDK dials hostnames itself, so discovery's guarantees live entirely in
this transport: every request leaves rewritten to the validated IP (Host +
SNI restored), any request for a different host is refused before send, and
the response body errors past the cap instead of buffering unbounded.
"""

from __future__ import annotations

import pytest

import httpx2

from src.server.services.mcp_oauth.http import (
    DISCOVERY_MAX_BYTES,
    OAuthHopBlocked,
    PinnedSessionTransport,
    pinned_discovery_client,
)
from src.server.utils.egress_guard import PinnedTarget

TARGET = PinnedTarget(
    url="https://203.0.113.9/mcp",
    host="mcp.example.test",
    ip="203.0.113.9",
    authority="mcp.example.test",
)


class _Body(httpx2.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _Upstream:
    """Captures what actually leaves the transport and answers a canned body."""

    def __init__(self, monkeypatch, *, headers=None, chunks=None) -> None:
        self.request: httpx2.Request | None = None
        self.body = _Body(chunks if chunks is not None else [b"{}"])
        response_headers = headers or {}

        async def _handle(transport_self, request):
            self.request = request
            return httpx2.Response(
                200, headers=response_headers, stream=self.body, request=request
            )

        monkeypatch.setattr(
            httpx2.AsyncHTTPTransport, "handle_async_request", _handle
        )


async def _drain(stream) -> bytes:
    return b"".join([chunk async for chunk in stream])


@pytest.mark.asyncio
async def test_requests_leave_pinned_to_the_validated_ip(monkeypatch):
    """The pin travels with the request: IP in the URL, hostname preserved as
    Host + SNI so routing and certificate verification still see the real name."""
    upstream = _Upstream(monkeypatch)
    transport = PinnedSessionTransport(TARGET, max_bytes=1024)
    request = httpx2.Request("POST", "https://mcp.example.test/mcp")

    await transport.handle_async_request(request)

    sent = upstream.request
    assert sent is not None
    assert sent.url.host == "203.0.113.9"
    assert sent.url.scheme == "https"
    assert sent.url.path == "/mcp"
    assert sent.headers["Host"] == "mcp.example.test"
    assert sent.headers["Accept-Encoding"] == "identity"
    assert sent.extensions["sni_hostname"] == "mcp.example.test"


@pytest.mark.asyncio
async def test_a_request_for_another_host_never_leaves(monkeypatch):
    """A redirect the SDK follows, an SSE reconnect hint, anything that steers
    the session off its validated host — refused before a byte is sent."""
    upstream = _Upstream(monkeypatch)
    transport = PinnedSessionTransport(TARGET, max_bytes=1024)
    request = httpx2.Request("POST", "https://evil.example.net/mcp")

    with pytest.raises(OAuthHopBlocked) as e:
        await transport.handle_async_request(request)

    assert e.value.request_sent is False
    assert upstream.request is None


@pytest.mark.asyncio
async def test_a_body_past_the_cap_errors_instead_of_buffering(monkeypatch):
    _Upstream(monkeypatch, chunks=[b"12345", b"67890", b"X"])
    transport = PinnedSessionTransport(TARGET, max_bytes=10)
    request = httpx2.Request("POST", "https://mcp.example.test/mcp")

    response = await transport.handle_async_request(request)
    served: list[bytes] = []
    with pytest.raises(OAuthHopBlocked):
        async for chunk in response.stream:
            served.append(chunk)

    # Everything handed out before the trip stays within the cap.
    assert len(b"".join(served)) <= 10


@pytest.mark.asyncio
async def test_a_compressed_answer_is_refused_and_closed(monkeypatch):
    """identity was requested; a server that compresses anyway would make the
    byte count a lie (the cap must measure what costs memory)."""
    upstream = _Upstream(monkeypatch, headers={"content-encoding": "gzip"})
    transport = PinnedSessionTransport(TARGET, max_bytes=1024)
    request = httpx2.Request("POST", "https://mcp.example.test/mcp")

    with pytest.raises(OAuthHopBlocked):
        await transport.handle_async_request(request)

    assert upstream.body.closed is True


@pytest.mark.asyncio
async def test_a_bounded_identity_body_passes_through_unchanged(monkeypatch):
    _Upstream(
        monkeypatch,
        headers={"content-encoding": "identity"},
        chunks=[b'{"tools"', b": []}"],
    )
    transport = PinnedSessionTransport(TARGET, max_bytes=1024)
    request = httpx2.Request("POST", "https://mcp.example.test/mcp")

    response = await transport.handle_async_request(request)

    assert await _drain(response.stream) == b'{"tools": []}'


@pytest.mark.asyncio
async def test_the_discovery_client_is_wired_to_the_pin():
    async with pinned_discovery_client(
        TARGET, headers={"Authorization": "Bearer t"}
    ) as client:
        transport = client._transport
        assert isinstance(transport, PinnedSessionTransport)
        assert transport._max_bytes == DISCOVERY_MAX_BYTES
        assert client.follow_redirects is False
        assert client.headers["Authorization"] == "Bearer t"
