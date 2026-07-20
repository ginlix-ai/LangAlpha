"""SSRF egress guard for the in-house fetch layer.

The extractors and sitemap fetcher run in the backend process, so a request
they make originates from a host that can reach internal services and the
cloud metadata endpoint. ``_validate_url`` guards the *initial* URL, but httpx
follows redirects transparently — a public page can 3xx to ``127.0.0.1`` or
``169.254.169.254`` and slip past that one check.

:class:`GuardedAsyncTransport` re-validates the resolved address on *every*
hop httpx makes (initial request and each redirect), which also closes DNS
rebinding for these paths since it checks the resolved IP, not the hostname.

Scope: httpx clients only. The scrapling engine tiers (curl_cffi tier 1 and
the browser tiers) do their own DNS and redirect handling below Python and are
not covered here — that residual needs network-level egress control.
"""

import asyncio
import ipaddress
import socket

import httpx


class BlockedAddressError(httpx.RequestError):
    """A request target resolved to a private/reserved address."""


def _ip_is_blocked(ip: str) -> bool:
    addr = ipaddress.ip_address(ip)
    return (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_reserved
        or addr.is_unspecified
        or addr.is_multicast
    )


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
    """

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if not host or host == "localhost":
            raise BlockedAddressError(f"Blocked host: {host!r}", request=request)
        try:
            addresses = await asyncio.to_thread(_resolve, host)
        except socket.gaierror as exc:
            raise BlockedAddressError(
                f"DNS resolution failed for {host!r}", request=request
            ) from exc
        for ip in addresses:
            if _ip_is_blocked(ip):
                raise BlockedAddressError(
                    f"Blocked private/reserved address {ip} for host {host!r}",
                    request=request,
                )
        return await super().handle_async_request(request)
