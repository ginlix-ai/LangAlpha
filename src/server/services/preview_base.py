"""Where a deployment serves previews, and whether its settings can work.

The base is deployment configuration: each capability is a subdomain of it,
and it must sit on a registrable domain of its own, apart from the app's.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from tld import get_fld

if TYPE_CHECKING:
    from ptc_agent.config.core import SandboxConfig

__all__ = [
    "PreviewBase",
    "PreviewProxyConfigurationError",
    "check_preview_proxy_config",
    "parse_preview_base",
    "preview_base",
    "preview_proxy_requirements",
]

# ``<port>-<token>``, the token as a capability is minted: 32 lowercase base32
# characters, lowercase because a hostname is case-insensitive.
_LABEL = re.compile(r"^(\d{1,5})-([a-z2-7]{32})$")


class PreviewProxyConfigurationError(RuntimeError):
    """The deployment's preview settings cannot work; boot stops on this."""


@dataclass(frozen=True)
class PreviewBase:
    """Where previews are served: a scheme, a host, and the port if not default."""

    scheme: str
    host: str
    port: int | None = None

    def url(self, port: int, token: str) -> str:
        authority = f"{port}-{token}.{self.host}"
        if self.port is not None:
            authority = f"{authority}:{self.port}"
        return f"{self.scheme}://{authority}/"

    def claims(self, host: str | None) -> bool:
        """Whether a request to *host* belongs to the proxy, parsed or not."""
        name = _hostname(host)
        return name is not None and (
            name == self.host or name.endswith("." + self.host)
        )

    def parse(self, host: str | None) -> tuple[int, str] | None:
        """The port and token a capability host names; None for anything else."""
        name = _hostname(host)
        suffix = "." + self.host
        if name is None or not name.endswith(suffix):
            return None
        match = _LABEL.match(name[: -len(suffix)])
        if match is None:
            return None
        return int(match.group(1)), match.group(2)


def _hostname(host: str | None) -> str | None:
    """A Host header's name: lowercased, without its port or a trailing dot."""
    if not host:
        return None
    name = host.strip().lower()
    if name.startswith("["):
        return None
    head, sep, tail = name.rpartition(":")
    if sep and tail.isdigit():
        name = head
    return name.rstrip(".") or None


def parse_preview_base(value: str) -> PreviewBase | None:
    """``PREVIEW_PROXY_BASE_URL`` as a base, None when unset.

    Scheme, host and an optional port only: a capability is a hostname, so a
    path here would be dropped from every URL minted against it.
    """
    value = (value or "").strip()
    if not value:
        return None
    parts = urlsplit(value)
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or port == -1
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or parts.username
        or parts.password
    ):
        raise PreviewProxyConfigurationError(
            "PREVIEW_PROXY_BASE_URL must be a scheme and host with an optional "
            f"port, such as https://preview.example.com; got {value!r}"
        )
    host = parts.hostname.rstrip(".")
    if _is_ip(host):
        raise PreviewProxyConfigurationError(
            "PREVIEW_PROXY_BASE_URL must name a host, not an address: each "
            f"capability is a subdomain of it; got {value!r}"
        )
    if parts.scheme == "http" and not _is_local(host):
        raise PreviewProxyConfigurationError(
            "PREVIEW_PROXY_BASE_URL must be https unless it is local "
            "(localhost or *.localhost): the capability travels in the "
            f"hostname, which plain http shows to every hop; got {value!r}"
        )
    default = 443 if parts.scheme == "https" else 80
    return PreviewBase(
        scheme=parts.scheme,
        host=host,
        port=None if port in (None, default) else port,
    )


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


def _is_local(host: str) -> bool:
    """A name or address that only ever reaches this machine."""
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _site(host: str) -> str:
    """The registrable domain (eTLD+1) a browser scopes cookies to.

    A name under no listed suffix falls to the list's default rule, which
    makes its last label the suffix, as browsers do.
    """
    if _is_ip(host):
        return host
    return get_fld(host, fix_protocol=True, fail_silently=True) or ".".join(
        host.split(".")[-2:]
    )


def _within(host: str, domain: str) -> bool:
    return host == domain or host.endswith("." + domain)


def _shares_site(base: str, app: str) -> bool:
    """Whether cookies one host may set can reach the other.

    Each side can scope a cookie as wide as its own registrable domain, so
    both directions are checked. Usually that is one equality; it also holds
    when the base is itself listed as a public suffix under the app's domain.
    """
    return _within(base, _site(app)) or _within(app, _site(base))


@lru_cache(maxsize=4)
def _parsed(value: str) -> PreviewBase | None:
    return parse_preview_base(value)


def preview_base() -> PreviewBase | None:
    """This deployment's preview base, read from the environment."""
    from src.config import env

    return _parsed(env.PREVIEW_PROXY_BASE_URL)


def preview_proxy_requirements(sandbox: SandboxConfig) -> tuple[str, ...]:
    """The settings under which previews must go through the proxy, as config names them.

    A private E2B computer's ports answer only with a traffic header the
    proxy adds. An SSH machine's ports are forwarded over the server's own
    connection, so no browser can reach them without it. These read the
    settings, not the fleet: a private E2B computer stays private after
    ``public_traffic`` turns true, and an SSH computer outlives ``ssh``
    leaving ``offered_providers``.
    """
    reasons = []
    if not sandbox.e2b.public_traffic:
        reasons.append("sandbox.e2b.public_traffic is false")
    if "ssh" in sandbox.offered_providers:
        reasons.append("sandbox.offered_providers includes ssh")
    return tuple(reasons)


def check_preview_proxy_config(
    *, proxy_required_by: Iterable[str], app_urls: Iterable[str]
) -> None:
    """Refuse a deployment whose previews could not be served, or not safely.

    *proxy_required_by* names the settings, from
    :func:`preview_proxy_requirements`, under which a preview link without a
    base would be one no browser could open; any of them makes the base
    required, whatever the default provider is, since a computer is built
    from its own kind's settings on any deployment. A base that contains the
    app's own host would claim every request to the app as a preview. A base
    on the app's site would let preview code read cookies the app scopes to
    its domain and set ones the app then receives, so the base must be a
    registrable domain of its own; only local names, which share no cookies a
    deployment cares about, are exempt.
    """
    base = preview_base()
    if base is None:
        reasons = tuple(proxy_required_by)
        if reasons:
            raise PreviewProxyConfigurationError(
                f"{' and '.join(reasons)}, so previews must go through the "
                "preview proxy; set PREVIEW_PROXY_BASE_URL"
            )
        return
    for url in app_urls:
        # A CORS origin may be a wildcard such as https://*.example.com.
        host = (urlsplit(url or "").hostname or "").removeprefix("*.").rstrip(".")
        if not host:
            continue
        if base.claims(host):
            raise PreviewProxyConfigurationError(
                f"PREVIEW_PROXY_BASE_URL ({base.host}) contains the app's host "
                f"({host}); serve previews from a domain of their own"
            )
        if not (_is_local(base.host) or _is_local(host)) and _shares_site(
            base.host, host
        ):
            raise PreviewProxyConfigurationError(
                f"PREVIEW_PROXY_BASE_URL ({base.host}) is on the same site as "
                f"the app ({host}), so preview code could reach the app's "
                "cookies; serve previews from a registrable domain of their own"
            )
