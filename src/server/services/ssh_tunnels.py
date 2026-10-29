"""The tunnel gateway as the server sees it: where it is, and its keys.

A machine the server cannot dial, behind a home router or a firewall,
connects out to the gateway (``python -m src.server.ssh_gateway``) and keeps a
tunnel open there; the server's workers reach the machine through it.
``SSH_GATEWAY_ADDRESS`` turns tunnels on and is where the workers dial the
gateway. ``SSH_GATEWAY_PUBLIC_ADDRESS`` is where machines dial it, when that
differs, as a container name inside a deployment and a public name outside it
do. ``SSH_GATEWAY_LISTEN`` is what the gateway binds; ``0.0.0.0`` or ``::``
binds every address of both families. ``SSH_GATEWAY_DIAL_LISTEN``, optional,
gives the workers' login an address of its own, kept off the internet, which
``SSH_GATEWAY_ADDRESS`` then names; the machines' address refuses it.

The gateway's keys live in the database, so the gateway and every worker
agree on them without a shared file: whichever process asks first makes them.
"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import os
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlsplit

from ptc_agent.core.sandbox.providers._ssh_shell import TUNNEL_UNINSTALL_SCRIPT
from ptc_agent.core.sandbox.providers.ssh import (
    GatewayAccess,
    generate_client_key,
    set_gateway_loader,
)
from src.server.database.encryption import encryption_configured
from src.server.database.ssh_tunnels import ensure_gateway_key

DEFAULT_PORT = 2022
# Where under the login's home a machine keeps each tunnel, in a folder named
# by :func:`tunnel_name`: its key, the gateway's pinned key, the loop that keeps
# the tunnel open, and ``uninstall.sh``, which takes all of it away again.
TUNNEL_PARENT = ".langalpha"
UNINSTALL_SCRIPT = TUNNEL_UNINSTALL_SCRIPT


@dataclass(frozen=True)
class GatewayAddress:
    host: str
    port: int

    @property
    def display(self) -> str:
        """As OpenSSH writes it in known_hosts: bracketed with a port other than 22."""
        if self.port == 22:
            return self.host
        return f"[{self.host}]:{self.port}"


def parse_address(value: str, *, default_port: int = DEFAULT_PORT) -> GatewayAddress:
    """``host``, ``host:port`` or ``[v6]:port``.

    Raises:
        ValueError: There is no host, or the port is not one.
    """
    parts = urlsplit(f"//{value.strip()}")
    try:
        port = parts.port
    except ValueError:
        raise ValueError(f"{value!r} does not end in a port number") from None
    if not parts.hostname:
        raise ValueError(f"{value!r} names no host")
    return GatewayAddress(parts.hostname, port or default_port)


def _env_address(name: str) -> Optional[GatewayAddress]:
    value = os.getenv(name, "").strip()
    return parse_address(value) if value else None


def gateway_address() -> Optional[GatewayAddress]:
    """Where the workers dial the gateway; None when tunnels are off."""
    return _env_address("SSH_GATEWAY_ADDRESS")


def gateway_public_address() -> Optional[GatewayAddress]:
    """Where machines dial the gateway."""
    return _env_address("SSH_GATEWAY_PUBLIC_ADDRESS") or gateway_address()


def gateway_listen() -> GatewayAddress:
    return _env_address("SSH_GATEWAY_LISTEN") or GatewayAddress("0.0.0.0", DEFAULT_PORT)


def gateway_dial_listen() -> Optional[GatewayAddress]:
    """The workers' own address on the gateway; None when they share the machines'."""
    return _env_address("SSH_GATEWAY_DIAL_LISTEN")


def tunnels_enabled() -> bool:
    """Whether a machine that cannot be dialed may connect through a tunnel.

    The gateway's keys are kept encrypted, as the users' client keys are.
    """
    return gateway_address() is not None and encryption_configured()


def _new_key(comment: str) -> Tuple[str, str]:
    key = generate_client_key(comment)
    return key.public_key, key.private_key


async def gateway_host_key() -> Tuple[str, str]:
    """The gateway's host key as (public line, private text)."""
    return await ensure_gateway_key(
        "host", functools.partial(_new_key, "langalpha-gateway")
    )


def tunnel_name(host_key: str) -> str:
    """What a machine calls its tunnel to the gateway holding *host_key*: the
    folder under ``TUNNEL_PARENT``, the LaunchAgent and the systemd unit.

    One per deployment, so a login paired with two keeps both, and pairing
    again replaces only its own. The gateway's host key is what a machine
    knows a deployment by, and it lives in the database: restarts, redeploys
    and a new public address keep it. A key replaced there is a deployment the
    machines no longer trust anyway, having pinned the old one.
    """
    blob = base64.b64decode(host_key.split()[1])
    return "tunnel-" + hashlib.sha256(blob).hexdigest()[:8]


async def deployment_tunnel_name() -> str:
    """:func:`tunnel_name` for this deployment's gateway."""
    host_key, _ = await gateway_host_key()
    return tunnel_name(host_key)


async def gateway_dialer_key() -> Tuple[str, str]:
    """The key the workers log in to the gateway with, as (public line, private text)."""
    return await ensure_gateway_key(
        "dialer", functools.partial(_new_key, "langalpha-dialer")
    )


async def gateway_access() -> GatewayAccess:
    """The provider's gateway loader: the address to dial and the keys to use."""
    address = gateway_address()
    if address is None:
        raise RuntimeError("SSH_GATEWAY_ADDRESS is not set")
    (host_key, _), (_, dialer_key) = await asyncio.gather(
        gateway_host_key(), gateway_dialer_key()
    )
    return GatewayAccess(
        host=address.host, port=address.port, host_key=host_key, dialer_key=dialer_key
    )


def install_gateway_loader() -> None:
    """Give the provider its gateway loader in this worker, when tunnels are on.

    Every worker runs this from the lifespan, beside the client key loader.
    """
    set_gateway_loader(gateway_access if tunnels_enabled() else None)
