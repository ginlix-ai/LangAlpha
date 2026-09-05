"""Sandbox-side reachability of the egress relay.

The relay base URL is minted into every sandbox credential file, but whether
the sandbox can dial it depends on where the sandbox runs: a local Docker
container reaches the host only via the Docker host gateway, and a remote
(Daytona) sandbox reaches nothing that isn't publicly routable. This module
keeps that topology awareness in one place so the rest of the relay stack
stays deployment-agnostic.
"""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

# Hostnames that are local by construction — no DNS needed to know a remote
# sandbox can never reach them.
_LOCAL_HOSTNAMES = {"localhost", "host.docker.internal"}
_LOOPBACK_HOSTNAMES = {"localhost", "0.0.0.0"}


def _hostname(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


def _is_local_host(host: str) -> bool:
    """The host is unreachable from outside this machine's network position."""
    if not host:
        return True
    if host in _LOCAL_HOSTNAMES or host.endswith(".localhost"):
        return True
    try:
        return not ipaddress.ip_address(host).is_global
    except ValueError:
        # A DNS name we can't classify without resolving — assume reachable
        # rather than crying wolf on every custom domain.
        return False


def effective_relay_base_url(provider: str) -> str:
    """The relay base a sandbox on `provider` should dial.

    An explicitly configured EGRESS_RELAY_BASE_URL is always honored verbatim.
    Only the implicit SERVER_BASE_URL fallback is adapted: for local Docker
    sandboxes a loopback host is rewritten to the Docker host gateway, so the
    OSS default stack works with zero relay configuration.
    """
    from src.config.env import EGRESS_RELAY_BASE_URL, SERVER_BASE_URL

    if EGRESS_RELAY_BASE_URL:
        return EGRESS_RELAY_BASE_URL
    parts = urlsplit(SERVER_BASE_URL)
    host = (parts.hostname or "").lower()
    if provider == "docker" and (
        host in _LOOPBACK_HOSTNAMES or host.startswith("127.")
    ):
        port = f":{parts.port}" if parts.port else ""
        return f"{parts.scheme or 'http'}://host.docker.internal{port}"
    return SERVER_BASE_URL


def relay_reachability_warning(provider: str, base_url: str) -> str | None:
    """A user-facing warning when sandboxes on `provider` cannot dial the relay.

    Only remote providers warrant one: their sandboxes sit outside this
    machine, so a loopback/private relay base can never work. Warn, never
    block — a self-hosted Daytona on a private network is legitimate.
    """
    if provider != "daytona":
        return None
    if not _is_local_host(_hostname(base_url)):
        return None
    return (
        f"Daytona sandboxes run remotely and cannot reach the egress relay at "
        f"'{base_url}'. OAuth connector tools will fail inside sandboxes until "
        "EGRESS_RELAY_BASE_URL points at a URL that is reachable from the "
        "sandbox network — your backend's public domain, or a tunnel "
        "(e.g. cloudflared) during local development."
    )
