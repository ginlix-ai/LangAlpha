"""Resolution-time SSRF guard for host-originated egress to user-configured URLs.

``validate_remote_url`` (models/mcp_server.py) is the static write-time policy;
it documents DNS rebinding as an accepted residual because the sandbox is the
caller. Host-side egress (OAuth discovery/DCR/token hops, the egress relay's
vendor dial) has no such excuse: this module resolves the host itself, rejects
any non-global address, and pins the connection to a validated IP — the URL is
rewritten to the IP while TLS SNI and the Host header keep the original
hostname, so a rebinding resolver cannot swap the target between check and
connect.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

__all__ = [
    "EgressBlockedError",
    "PinnedTarget",
    "pin_public_url",
    "resolve_public_ips",
]


class EgressBlockedError(ValueError):
    """The target host failed egress policy (scheme, resolution, or IP range)."""


@dataclass(frozen=True)
class PinnedTarget:
    """A URL rewritten to a validated IP, plus what the transport must restore.

    ``url`` carries the IP in the netloc; ``host`` is the original hostname used
    as the TLS ``sni_hostname`` extension so certificate verification still runs
    against the real name. ``authority`` is what the caller must send as the
    ``Host`` header — the hostname plus a non-default port (bracketed for IPv6),
    since a server routing or validating the full authority rejects a bare host.
    """

    url: str
    host: str
    ip: str
    authority: str

    def pinned_kwargs(
        self, headers: Mapping[str, str] | None = None
    ) -> tuple[str, dict[str, str], dict[str, str]]:
        """The (url, headers, extensions) triple that keeps the pin intact.

        Every caller must apply all three together — sending the pinned URL
        without the restored Host/SNI reaches the right IP under the wrong
        name, and sending the original URL re-resolves the hostname.
        """
        sent = dict(headers or {})
        sent["Host"] = self.authority
        return self.url, sent, {"sni_hostname": self.host}


def _classify(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    # is_global covers private/loopback/link-local/reserved/multicast/
    # unspecified AND CGNAT — same predicate as the write-time validator.
    return ip.is_global


async def resolve_public_ips(
    host: str,
    *,
    port: int = 443,
    allow_non_global: bool = False,
) -> list[str]:
    """Resolve ``host`` and return its addresses, rejecting non-global ones.

    Every resolved address must pass — a name that maps to one public and one
    private address is an attack shape, not a configuration.
    """
    candidate = host.lower().rstrip(".").strip("[]")
    try:
        literal = ipaddress.ip_address(candidate)
    except ValueError:
        literal = None
    if literal is not None:
        if not allow_non_global and not _classify(literal):
            raise EgressBlockedError(
                f"egress to {host!r} is blocked: non-global address"
            )
        return [str(literal)]

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(
            candidate, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
        )
    except OSError as exc:
        raise EgressBlockedError(f"egress to {host!r} is blocked: DNS resolution failed") from exc
    ips: list[str] = []
    for _family, _type, _proto, _canon, sockaddr in infos:
        addr = ipaddress.ip_address(sockaddr[0])
        if not allow_non_global and not _classify(addr):
            raise EgressBlockedError(
                f"egress to {host!r} is blocked: resolves to a non-global address"
            )
        if str(addr) not in ips:
            ips.append(str(addr))
    if not ips:
        raise EgressBlockedError(f"egress to {host!r} is blocked: no addresses resolved")
    return ips


async def pin_public_url(
    url: str,
    *,
    allow_non_global: bool = False,
    require_https: bool = True,
) -> PinnedTarget:
    """Validate ``url`` and return it pinned to one validated resolved IP.

    Callers send the request with ``PinnedTarget.pinned_kwargs()``, which
    carries the pinned URL, the restored Host authority and the SNI extension.
    """
    try:
        parts = urlsplit(url)
    except ValueError as e:
        # The url is third-party in every caller (a manifest field, a
        # handshake icon, an upstream Location), so unparseable is an ordinary
        # input, not a fault. ``http://[bad`` raises here rather than
        # returning something to reject.
        raise EgressBlockedError(f"egress url is not parseable: {e}") from e
    if require_https and parts.scheme != "https":
        raise EgressBlockedError("egress requires https")
    if parts.scheme not in ("https", "http"):
        raise EgressBlockedError(f"egress scheme {parts.scheme!r} is not allowed")
    if parts.username or parts.password:
        raise EgressBlockedError("egress url must not contain userinfo credentials")
    host = parts.hostname
    if not host:
        raise EgressBlockedError("egress url must include a host")

    default_port = 443 if parts.scheme == "https" else 80
    try:
        port = parts.port or default_port
    except ValueError as e:
        # ``parts.port`` parses lazily, so a port that is out of range or not
        # a number survives urlsplit and raises on this read instead.
        raise EgressBlockedError(f"egress url has an unusable port: {e}") from e
    ips = await resolve_public_ips(host, port=port, allow_non_global=allow_non_global)
    ip = ips[0]

    ip_netloc = f"[{ip}]" if ":" in ip else ip
    if parts.port is not None:
        ip_netloc = f"{ip_netloc}:{parts.port}"
    pinned = urlunsplit((parts.scheme, ip_netloc, parts.path, parts.query, ""))
    # The Host authority keeps the original hostname (bracketed for IPv6) and
    # carries a non-default port; SNI/cert verification still use the bare host.
    host_netloc = f"[{host}]" if ":" in host else host
    authority = (
        f"{host_netloc}:{parts.port}"
        if parts.port is not None and parts.port != default_port
        else host_netloc
    )
    return PinnedTarget(url=pinned, host=host, ip=ip, authority=authority)
