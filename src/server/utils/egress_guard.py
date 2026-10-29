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
import functools
import ipaddress
import socket
from collections.abc import Mapping
from concurrent.futures import Executor
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

__all__ = [
    "EgressBlockedError",
    "EgressResolutionError",
    "MCP_PROTOCOL_HEADERS",
    "PinnedTarget",
    "RESERVED_HEADERS",
    "RESERVED_REQUEST_HEADERS",
    "pin_public_url",
    "resolve_public_ips",
    "resolve_public_ips_sync",
    "strip_configured_headers",
    "strip_reserved_headers",
]

# Headers no caller may supply: framing and hop-by-hop fields belong to the
# client that frames the request, and ``Host`` is the pin itself. A dict is
# case-sensitive where HTTP is not, so a lowercase ``host`` from a config row
# would ride alongside the pinned one and put two on the wire.
RESERVED_REQUEST_HEADERS = frozenset({
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "te",
    "upgrade",
    "expect",
    "content-encoding",
})


# Names the MCP protocol owns, on top of the framing ones. Whoever negotiated
# the session frames these, never configuration: a row spelling the version
# desyncs the wire header from the body ``_meta`` it has to match, and one
# spelling a session id forges a session the server never issued.
MCP_PROTOCOL_HEADERS = frozenset({
    "mcp-protocol-version",
    "mcp-method",
    "mcp-name",
    "mcp-session-id",
})

# What a user-configured header map may never carry. The host probe, the egress
# relay and the sandbox runtime each merge a row's headers into a request they
# framed themselves, so all three drop exactly this set. Split any of them and
# the same row probes green on one path and fails on another.
RESERVED_HEADERS = RESERVED_REQUEST_HEADERS | MCP_PROTOCOL_HEADERS


def strip_reserved_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """Drop every :data:`RESERVED_REQUEST_HEADERS` key, matched case-folded.

    The transport's own strip, for a map it is about to re-frame. A caller
    holding a *configured* map wants :func:`strip_configured_headers` instead:
    the protocol names this one keeps are ones the transport itself is
    entitled to send.
    """
    return {
        k: v
        for k, v in (headers or {}).items()
        if k.lower() not in RESERVED_REQUEST_HEADERS
    }


def strip_configured_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    """Drop every :data:`RESERVED_HEADERS` key, matched case-folded."""
    return {
        k: v
        for k, v in (headers or {}).items()
        if k.lower() not in RESERVED_HEADERS
    }


class EgressBlockedError(ValueError):
    """The target host failed egress policy (scheme, resolution, or IP range)."""


class EgressResolutionError(EgressBlockedError):
    """The host resolved to nothing. Unlike a range refusal this can be a
    resolver hiccup, so a caller that retries connection errors may retry it."""


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
        sent = strip_reserved_headers(headers)
        sent["Host"] = self.authority
        return self.url, sent, {"sni_hostname": self.host}


# IPv6 ranges that carry an IPv4 address in their last 32 bits, which is where
# a connection to them goes: the NAT64 well-known prefix (a NAT64 gateway on
# the server's network translates it to that IPv4 address there), the
# deprecated IPv4-compatible form, and the IPv4-mapped form a dual-stack socket
# dials as IPv4. ``is_global`` passes the first two whatever they carry.
_EMBEDS_IPV4 = (
    ipaddress.IPv6Network("64:ff9b::/96"),
    ipaddress.IPv6Network("::/96"),
    ipaddress.IPv6Network("::ffff:0:0/96"),
)

# Blocks that are not globally reachable but that ``is_global`` passes on
# Python 3.13 and 3.14 alike, found by checking every IANA special-purpose
# registry entry marked not globally reachable, and the multicast ranges,
# block by block against both. Python's lists trail the registries, so recheck
# on a new registry entry or Python. Local-use NAT64 is listed although both
# refuse it, since that refusal is a recent Python fix.
_NOT_GLOBAL = (
    ipaddress.IPv4Network("192.88.99.0/24"),  # 6to4 relay anycast, deprecated; 6a44 relay (RFC 6751)
    ipaddress.IPv4Network("224.0.0.0/4"),  # multicast
    ipaddress.IPv6Network("64:ff9b:1::/48"),  # local-use NAT64 (RFC 8215)
    ipaddress.IPv6Network("100:0:0:1::/64"),  # dummy prefix (RFC 9780)
    ipaddress.IPv6Network("5f00::/16"),  # SRv6 SIDs (RFC 9602)
    ipaddress.IPv6Network("fec0::/10"),  # site-local, deprecated but still routed where configured
    ipaddress.IPv6Network("ff00::/8"),  # multicast
)


def is_public_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    # is_global covers private/loopback/link-local/reserved/unspecified AND
    # CGNAT; the write-time validator stops there, this adds _NOT_GLOBAL.
    if any(ip in net for net in _NOT_GLOBAL):
        return False
    if ip.version == 6 and any(ip in net for net in _EMBEDS_IPV4):
        embedded = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        return ip.is_global and is_public_address(embedded)
    return ip.is_global


async def resolve_public_ips(
    host: str,
    *,
    port: int = 443,
    allow_non_global: bool = False,
    executor: Executor | None = None,
) -> list[str]:
    """Resolve ``host`` and return its addresses, rejecting non-global ones.

    The lookup blocks a thread of ``executor``, the loop's default when None.

    Every resolved address must pass — a name that maps to one public and one
    private address is an attack shape, not a configuration.
    """
    candidate = _candidate(host)
    literal = _literal_ips(host, candidate, allow_non_global)
    if literal is not None:
        return literal

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.run_in_executor(
            executor,
            functools.partial(
                socket.getaddrinfo,
                candidate,
                port,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            ),
        )
    # A name IDNA cannot encode (a label over 63 bytes, an empty one) fails
    # before any lookup, as UnicodeError, and resolves to nothing all the same.
    except (OSError, UnicodeError) as exc:
        raise EgressResolutionError(f"egress to {host!r} is blocked: DNS resolution failed") from exc
    return _checked_ips(host, infos, allow_non_global)


def resolve_public_ips_sync(
    host: str,
    *,
    port: int = 443,
    allow_non_global: bool = False,
) -> list[str]:
    """:func:`resolve_public_ips` for a caller already off the event loop (a
    synchronous HTTP transport), so one policy covers both client shapes."""
    candidate = _candidate(host)
    literal = _literal_ips(host, candidate, allow_non_global)
    if literal is not None:
        return literal
    try:
        infos = socket.getaddrinfo(
            candidate, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
        )
    except (OSError, UnicodeError) as exc:
        raise EgressResolutionError(f"egress to {host!r} is blocked: DNS resolution failed") from exc
    return _checked_ips(host, infos, allow_non_global)


def _candidate(host: str) -> str:
    return host.lower().rstrip(".").strip("[]")


def _literal_ips(host: str, candidate: str, allow_non_global: bool) -> list[str] | None:
    try:
        literal = ipaddress.ip_address(candidate)
    except ValueError:
        return None
    if not allow_non_global and not is_public_address(literal):
        raise EgressBlockedError(
            f"egress to {host!r} is blocked: non-global address"
        )
    return [str(literal)]


def _checked_ips(host: str, infos, allow_non_global: bool) -> list[str]:
    ips: list[str] = []
    for _family, _type, _proto, _canon, sockaddr in infos:
        addr = ipaddress.ip_address(sockaddr[0])
        if not allow_non_global and not is_public_address(addr):
            raise EgressBlockedError(
                f"egress to {host!r} is blocked: resolves to a non-global address"
            )
        if str(addr) not in ips:
            ips.append(str(addr))
    if not ips:
        raise EgressResolutionError(f"egress to {host!r} is blocked: no addresses resolved")
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
