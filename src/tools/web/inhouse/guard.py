"""SSRF egress guard for the in-house fetch layer.

The extractors and sitemap fetcher run in the backend process, so a request
they make originates from a host that can reach internal services and the
cloud metadata endpoint. ``_validate_url`` guards the *initial* URL, but httpx
follows redirects transparently — a public page can 3xx to ``127.0.0.1`` or
``169.254.169.254`` and slip past that one check.

:class:`GuardedAsyncTransport` re-validates the resolved address on *every*
hop httpx makes (initial request and each redirect), and connects to the
address it checked rather than the hostname, so a second DNS answer naming a
private address (rebinding) is never dialed.

Scope: httpx clients only. The scrapling engine tiers (curl_cffi tier 1 and
the browser tiers) do their own DNS and redirect handling below Python and are
not covered here — that residual needs network-level egress control.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from urllib.parse import urlsplit

import httpx

from src.server.utils.egress_guard import is_public_address


class BlockedAddressError(httpx.RequestError):
    """A request target resolved to a private/reserved address."""


class UnresolvableHost(BlockedAddressError):
    """A request target's hostname did not resolve."""


def _ip_is_blocked(ip: str) -> bool:
    """Whether ``ip`` is outside the public internet, judged as the server's
    own egress judges it: anything not globally reachable, including the
    shared address space (100.64.0.0/10, where some clouds serve their
    metadata), and an IPv6 address carrying an IPv4 one by that IPv4 address."""
    return not is_public_address(ipaddress.ip_address(ip))


def _resolve(host: str) -> list[str]:
    """Resolve a host to its addresses; a raw-IP host resolves to itself."""
    try:
        ipaddress.ip_address(host)
        return [host]
    except ValueError:
        return [info[4][0] for info in socket.getaddrinfo(host, None)]


class GuardedAsyncTransport(httpx.AsyncHTTPTransport):
    """httpx transport that blocks requests resolving to private/reserved IPs.

    httpx invokes the transport once per hop, so validating here covers the
    initial request and every redirect target uniformly.

    Keeps no connection open between requests: dialed by address, the pool
    knows a connection by that address alone, so a later request for another
    hostname on it would ride a TLS session whose certificate named the first.
    """

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("limits", httpx.Limits(max_keepalive_connections=0))
        super().__init__(**kwargs)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if not host or host == "localhost":
            raise BlockedAddressError(f"Blocked host: {host!r}", request=request)
        try:
            addresses = await asyncio.to_thread(_resolve, host)
        except socket.gaierror as exc:
            raise UnresolvableHost(f"DNS resolution failed for {host!r}", request=request) from exc
        for ip in addresses:
            if _ip_is_blocked(ip):
                raise BlockedAddressError(
                    f"Blocked private/reserved address {ip} for host {host!r}",
                    request=request,
                )
        # Dialed by the addresses just checked, in order, the next one only
        # when a connection fails, as a dial by hostname would: handed the
        # hostname, httpcore would resolve it again, and that answer could
        # name a private host. The Host header and the TLS name stay the
        # hostname's.
        extensions = dict(request.extensions)
        if request.url.scheme == "https":
            extensions["sni_hostname"] = host
        *earlier, last = dict.fromkeys(addresses)
        for ip in earlier:
            try:
                return await super().handle_async_request(self._pinned(request, ip, extensions))
            except (httpx.ConnectError, httpx.ConnectTimeout):
                continue
        return await super().handle_async_request(self._pinned(request, last, extensions))

    @staticmethod
    def _pinned(request: httpx.Request, ip: str, extensions: dict) -> httpx.Request:
        return httpx.Request(
            request.method,
            request.url.copy_with(host=ip),
            headers=request.headers,
            stream=request.stream,
            extensions=extensions,
        )


class UnexpectedStatus(Exception):
    """A capped fetch answered with a status other than 200, which it carries."""

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code)
        self.status_code = status_code


class OverCap(Exception):
    """A capped fetch passed its cap, which it carries."""

    def __init__(self, limit: int) -> None:
        super().__init__(limit)
        self.limit = limit


class UnexpectedEncoding(Exception):
    """A capped fetch was answered compressed, which it carries."""

    def __init__(self, encoding: str) -> None:
        super().__init__(encoding)
        self.encoding = encoding


#: The longest a capped fetch may take in all. httpx's timeout bounds each
#: read, so a server sending a byte at a time could hold one for hours.
FETCH_DEADLINE = 30.0

#: The most redirects a capped fetch follows, httpx's own default.
MAX_REDIRECTS = 20


def fetch_host(url: str) -> str:
    """The host of ``url``, for a log line: a full URL can carry a signed query."""
    try:
        return urlsplit(url).hostname or "?"
    except ValueError:
        return "?"


async def fetch_capped(url: str, cap: Callable[[bytes], int]) -> bytes:
    """The body at ``url``, fetched through :class:`GuardedAsyncTransport` and
    stopped as soon as it passes ``cap`` of what has arrived.

    Streamed, so the cap fires mid-transfer: a Content-Length header is only a
    claim until the bytes arrive. Asked uncompressed, and refused when
    compressed anyway, since a decoder expands a chunk with no bound before
    the cap could see it: a few kilobytes of gzip or zstd can inflate to
    gigabytes. Redirects are followed here rather than by httpx, which reads
    and decodes each redirect's whole body first; here it is never read.

    Raises:
        UnexpectedStatus: When the answer is not a 200.
        UnexpectedEncoding: When the answer is compressed anyway.
        OverCap: When the body passes the cap.
        BlockedAddressError, httpx.TimeoutException, httpx.RequestError: As
            httpx raises them, a timeout also past ``FETCH_DEADLINE`` in all.
    """
    try:
        async with asyncio.timeout(FETCH_DEADLINE):
            return await _fetch_capped(url, cap)
    except TimeoutError:
        raise httpx.ReadTimeout(f"no whole answer within {FETCH_DEADLINE:.0f}s") from None


async def _fetch_capped(url: str, cap: Callable[[bytes], int]) -> bytes:
    async with httpx.AsyncClient(
        transport=GuardedAsyncTransport(),
        timeout=FETCH_DEADLINE,
        headers={"Accept-Encoding": "identity"},
    ) as client:
        request = client.build_request("GET", url)
        for _ in range(MAX_REDIRECTS + 1):
            response = await client.send(request, stream=True)
            try:
                if response.next_request is not None:
                    request = response.next_request
                    continue
                if response.status_code != 200:
                    raise UnexpectedStatus(response.status_code)
                encoding = response.headers.get("content-encoding", "identity").strip().lower()
                if encoding not in ("", "identity"):
                    raise UnexpectedEncoding(encoding)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    limit = cap(body)
                    if len(body) > limit:
                        raise OverCap(limit)
                return bytes(body)
            finally:
                await response.aclose()
        raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects", request=request)
