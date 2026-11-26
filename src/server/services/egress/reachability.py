"""Sandbox-side reachability of the egress relay.

The relay base URL is minted into every sandbox credential file, but whether
the sandbox can dial it depends on where the sandbox runs: a local Docker
container reaches the host only via the Docker host gateway, and a remote
(Daytona or E2B) sandbox reaches nothing that isn't publicly routable. This module
keeps that topology awareness in one place so the rest of the relay stack
stays deployment-agnostic, and checks at boot that the addresses sandboxes dial
can reach this deployment.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from ptc_agent.core.sandbox.providers import (
    provider_class,
    provider_reaches_host_through_alias,
    provider_runs_remote,
)
from src.config import env
from src.config.urls import enforce, host_class

if TYPE_CHECKING:
    from ptc_agent.config.core import SandboxConfig


def effective_relay_base_url(provider: str) -> str:
    """The relay base a sandbox on `provider` should dial.

    An explicitly configured SERVER_SANDBOX_URL is always honored verbatim.
    Only the implicit SERVER_BASE_URL fallback is adapted: for local Docker
    sandboxes a loopback host is rewritten to the Docker host gateway, so the
    OSS default stack works with zero relay configuration. A computer on this
    very machine dials loopback as it is.
    """
    if env.SERVER_SANDBOX_URL:
        return env.SERVER_SANDBOX_URL
    if provider_reaches_host_through_alias(provider) and host_class(env.SERVER_BASE_URL) == "loopback":
        parts = urlsplit(env.SERVER_BASE_URL)
        port = f":{parts.port}" if parts.port else ""
        return f"{parts.scheme or 'http'}://host.docker.internal{port}"
    return env.SERVER_BASE_URL


def unreachable_from_sandbox(provider: str, url: str) -> str | None:
    """The class of *url*'s host when sandboxes on a remote *provider* cannot dial
    it, else None.

    They sit outside this machine, where a loopback, private or container
    address never reaches this deployment. A URL with no host is a defect
    src/config/urls.py reports, so it is left to that.
    """
    where = host_class(url)
    if where in ("none", "public") or not provider_runs_remote(provider):
        return None
    return where


def relay_reachability_warning(provider: str, base_url: str) -> str | None:
    """A user-facing warning when sandboxes on `provider` cannot dial the relay.

    Only remote providers warrant one: their sandboxes sit outside this
    machine, so a loopback/private relay base can never work. Warn, never
    block — a self-hosted Daytona on a private network is legitimate.
    """
    if unreachable_from_sandbox(provider, base_url) is None:
        return None
    return (
        f"{provider_class(provider).label} sandboxes run remotely and "
        f"cannot reach the egress relay at "
        f"'{base_url}'. OAuth connector tools will fail inside sandboxes until "
        "SERVER_SANDBOX_URL points at a URL that is reachable from the "
        "sandbox network: your backend's public domain, or a tunnel "
        "(e.g. cloudflared) during local development."
    )


def check_sandbox_urls(sandbox: SandboxConfig) -> None:
    """Refuse to start a managed deployment whose default provider's sandboxes
    cannot dial the addresses minted for them; anywhere else, warn.

    The data token file would otherwise hand a bearer token to whatever listens
    at the address. A container on its own network has a loopback of its own;
    one on the host network of a daemon on this machine shares this machine's.
    """
    provider = sandbox.provider
    daemon_here = os.environ.get("DOCKER_HOST", "unix://").startswith("unix://")
    shares_loopback = sandbox.docker.network_mode == "host" and daemon_here
    own_loopback = provider_reaches_host_through_alias(provider) and not shares_loopback
    problems = []
    for name, url in (
        ("SERVER_SANDBOX_URL", effective_relay_base_url(provider)),
        ("LANGALPHA_DATA_SANDBOX_URL", env.LANGALPHA_DATA_SANDBOX_URL),
    ):
        if where := unreachable_from_sandbox(provider, url):
            reason = "since they run outside this machine"
        elif own_loopback and (where := host_class(url)) == "loopback":
            reason = "since each has a loopback of its own"
        else:
            continue
        problems.append(
            f"{name} resolves to a {where} address, which {provider_class(provider).label} sandboxes "
            f"cannot reach, {reason}"
        )
    enforce(problems)
