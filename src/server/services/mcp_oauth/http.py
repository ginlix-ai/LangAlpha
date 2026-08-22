"""SSRF-pinned HTTP for OAuth hops (httpx2 — the SDK's request objects).

Every host-side request to a user-supplied URL (server probe, PRM, AS
metadata, DCR, token) goes through :func:`pinned_request`: the hostname is
resolved once, every address is required to be globally routable, and the
request is sent to the validated IP with the Host header + SNI restored — so a
DNS answer swapped between validation and connect cannot re-target the
request. Redirects are refused outright (fail closed), and the response body
is read under a byte cap and wall-clock deadline (fail closed again — the
read timeout alone is an idle timeout a trickling server resets forever).
"""

from __future__ import annotations

import asyncio

import httpx2

from src.server.utils.egress_guard import (
    EgressBlockedError,
    PinnedTarget,
    pin_public_url,
)

# One ladder for every OAuth hop: these are interactive, user-facing calls.
DEFAULT_TIMEOUT = httpx2.Timeout(15.0, connect=5.0)

# OAuth hop responses (metadata, DCR, token) are KB-scale JSON — the cap is
# generous headroom, not a format budget.
HOP_MAX_BYTES = 1_048_576
HOP_DEADLINE_SECONDS = 30.0

USER_AGENT = "langalpha-mcp-connect/1"


class OAuthHopBlocked(Exception):
    """A hop failed SSRF validation, tried to redirect, or overran its bounds.

    ``request_sent`` separates the raise sites: the pin runs before any
    bytes leave, while a refused redirect or overrun answers a request the
    server has already seen. A grant is one-time under rotation, so that
    difference — not the exception type — decides whether the hop may be
    retried. It defaults to the pessimistic answer, so an unlabelled raise is
    treated as sent.
    """

    def __init__(self, message: str, *, request_sent: bool = True):
        self.request_sent = request_sent
        super().__init__(message)


def oauth_http_client() -> httpx2.AsyncClient:
    """Client for OAuth hops: no redirects, no env proxies, short timeouts."""
    return httpx2.AsyncClient(
        follow_redirects=False,
        trust_env=False,
        timeout=DEFAULT_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )


async def pinned_request(
    client: httpx2.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: dict[str, str] | None = None,
    content: bytes | None = None,
) -> httpx2.Response:
    """Send one SSRF-pinned request; refuse redirects and unbounded bodies."""
    try:
        target = await pin_public_url(url, require_https=True)
    except EgressBlockedError as e:
        # Provably nothing on the wire. The guard raises this for a permanent
        # policy rejection and for a transient resolution failure alike, so a
        # caller spending a one-time grant must be able to retry it.
        raise OAuthHopBlocked(str(e), request_sent=False) from e
    url_pinned, send_headers, extensions = target.pinned_kwargs(headers)
    try:
        async with asyncio.timeout(HOP_DEADLINE_SECONDS):
            async with client.stream(
                method,
                url_pinned,
                headers=send_headers,
                data=data,
                content=content,
                extensions=extensions,
            ) as upstream:
                chunks: list[bytes] = []
                seen = 0
                # Decoded bytes, so a compressed bomb is measured at its
                # expanded size — the number that actually costs memory.
                async for chunk in upstream.aiter_bytes():
                    seen += len(chunk)
                    if seen > HOP_MAX_BYTES:
                        raise OAuthHopBlocked(
                            f"{method} {url} answered more than "
                            f"{HOP_MAX_BYTES} bytes; refusing the hop"
                        )
                    chunks.append(chunk)
                # Rebuilt from the consumed stream. The wire-framing fields
                # describe an encoding the content no longer carries.
                stripped = [
                    (k, v)
                    for k, v in upstream.headers.raw
                    if k.decode("latin-1").lower()
                    not in ("content-encoding", "content-length", "transfer-encoding")
                ]
                response = httpx2.Response(
                    upstream.status_code,
                    headers=stripped,
                    content=b"".join(chunks),
                    request=upstream.request,
                )
    except TimeoutError:
        raise OAuthHopBlocked(
            f"{method} {url} exceeded the {HOP_DEADLINE_SECONDS:.0f}s hop deadline"
        )
    if response.is_redirect:
        raise OAuthHopBlocked(
            f"{method} {url} answered a redirect ({response.status_code}); "
            "redirects are refused on OAuth hops"
        )
    return response


async def pinned_send(
    client: httpx2.AsyncClient, request: httpx2.Request
) -> httpx2.Response:
    """Re-issue an SDK-built request through the pinned path."""
    body = request.read()
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    return await pinned_request(
        client,
        request.method,
        str(request.url),
        headers=headers,
        content=body or None,
    )


# tools/list is KB-to-low-MB JSON for real servers; the cap is headroom against
# a hostile pump, not a format budget. sanitize keeps 200KB/server post-parse.
DISCOVERY_MAX_BYTES = 8 * 1_048_576


class PinnedSessionTransport(httpx2.AsyncHTTPTransport):
    """Holds an SDK-driven session to its validated address and size.

    The SDK dials hostnames itself, re-resolving what a pre-connect check
    validated — the classic rebinding TOCTOU. Here the validation travels
    WITH the requests instead: every request in the session is rewritten to
    the pinned IP with the Host authority and SNI restored (certificate
    verification still runs against the real name), and any request for a
    different host is refused outright. Bodies are bounded where the SDK has
    no bound of its own: compression is refused (identity is requested, and a
    server that compresses anyway is refused), so the wire count IS the
    memory cost, and the stream errors past ``max_bytes``.
    """

    def __init__(self, target: PinnedTarget, *, max_bytes: int) -> None:
        super().__init__()
        self._target = target
        self._max_bytes = max_bytes

    async def handle_async_request(
        self, request: httpx2.Request
    ) -> httpx2.Response:
        if request.url.host != self._target.host:
            raise OAuthHopBlocked(
                f"refusing a hop to {request.url.host!r}: this session is "
                f"pinned to {self._target.host!r}",
                request_sent=False,
            )
        request.url = request.url.copy_with(host=self._target.ip)
        request.headers["Host"] = self._target.authority
        request.headers["Accept-Encoding"] = "identity"
        request.extensions["sni_hostname"] = self._target.host
        response = await super().handle_async_request(request)
        encoding = (response.headers.get("content-encoding") or "identity").lower()
        if encoding not in ("", "identity"):
            await response.aclose()
            raise OAuthHopBlocked(
                f"{self._target.host} answered content-encoding {encoding!r}; "
                "refusing a body the byte guard cannot measure"
            )
        response.stream = _CappedByteStream(
            response.stream, self._max_bytes, self._target.host
        )
        return response


class _CappedByteStream(httpx2.AsyncByteStream):
    def __init__(self, inner, max_bytes: int, host: str) -> None:
        self._inner = inner
        self._max_bytes = max_bytes
        self._host = host

    async def __aiter__(self):
        seen = 0
        async for chunk in self._inner:
            seen += len(chunk)
            if seen > self._max_bytes:
                raise OAuthHopBlocked(
                    f"{self._host} answered more than {self._max_bytes} "
                    "bytes; refusing the hop"
                )
            yield chunk

    async def aclose(self) -> None:
        await self._inner.aclose()


def pinned_stream_client(
    target: PinnedTarget,
    *,
    max_bytes: int,
    headers: dict[str, str] | None = None,
) -> httpx2.AsyncClient:
    """A pinned, byte-bounded client: no redirects, no env proxies (a proxy
    would re-resolve past the pin). The general form behind discovery and the
    plugin archive fetch — callers pick the byte budget for their payload."""
    return httpx2.AsyncClient(
        transport=PinnedSessionTransport(target, max_bytes=max_bytes),
        follow_redirects=False,
        trust_env=False,
        timeout=DEFAULT_TIMEOUT,
        headers={"User-Agent": USER_AGENT, **(headers or {})},
    )


def pinned_discovery_client(
    target: PinnedTarget, *, headers: dict[str, str] | None = None
) -> httpx2.AsyncClient:
    """SDK-compatible client for host-side discovery: the stream client at
    the discovery byte budget."""
    return pinned_stream_client(
        target, max_bytes=DISCOVERY_MAX_BYTES, headers=headers
    )
