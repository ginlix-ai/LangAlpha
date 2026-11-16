"""The keys the host holds per user for built-in servers, one row per kind.

A built-in whose ``auth`` names a row here is reached through the egress relay
on a ``platform_mcp`` grant. The row pairs how the user's key is had with the
meter that prices the vendor's calls, so the relay, the meter and the charge
read one table and no generic module names a vendor.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.server.services.egress.firecrawl_meter import FirecrawlMeter
from src.server.services.firecrawl_accounts import (
    firecrawl_configured,
    firecrawl_key_for,
    provision_firecrawl_account,
    recheck_firecrawl_key,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ptc_agent.config.core import MCPServerConfig
    from src.server.services.egress.metering import Meter


@dataclass(frozen=True)
class PlatformAccount:
    #: The user's key; raises ``AccountUnavailable`` when none can be had,
    #: or ``RelayRejection`` when the user may not have one yet. Never
    #: creates the account: without one it asks for the vendor's terms.
    key_for: Callable[[str], Awaitable[str]]
    #: Creates the user's account unless they have one, run when they accept
    #: the vendor's terms; raises as ``key_for`` does.
    provision: Callable[[str], Awaitable[None]]
    #: Replaces the user's key if the vendor no longer accepts it, run in the
    #: background after a call answered with a tool error, which is how a
    #: vendor may report a revoked key; the next call sends the replacement.
    #: Never raises.
    recheck_key: Callable[[str], Awaitable[None]]
    #: Whether the deployment holds what ``key_for`` needs to give any user a
    #: key; read at startup.
    configured: Callable[[], bool]
    meter: Meter
    #: The vendor's Terms of Service, which the user accepts to have their
    #: account created (``services.vendor_terms``).
    vendor: str
    name: str
    terms_url: str


#: Keyed by ``MCPServerConfig.auth``.
PLATFORM_ACCOUNTS: Mapping[str, PlatformAccount] = {
    "firecrawl_account": PlatformAccount(
        key_for=firecrawl_key_for,
        provision=provision_firecrawl_account,
        recheck_key=recheck_firecrawl_key,
        configured=firecrawl_configured,
        meter=FirecrawlMeter(),
        vendor="firecrawl",
        name="Firecrawl",
        terms_url="https://www.firecrawl.dev/terms-of-service",
    ),
}


@dataclass(frozen=True)
class PlatformServer:
    """A platform grant's built-in as the operator's config declares it now,
    with the account its ``auth`` names."""

    config: MCPServerConfig
    account: PlatformAccount


def meter_for_vendor(vendor: str) -> Meter | None:
    """The meter whose ledger rows carry ``vendor``."""
    return next(
        (a.meter for a in PLATFORM_ACCOUNTS.values() if a.meter.vendor == vendor), None
    )


def disable_unreachable_platform_servers(config: Any) -> None:
    """Turn off, at startup, each server on a platform account that no call
    could reach: the egress relay is off (no ``EGRESS_RELAY_SECRET``), or the
    deployment lacks what its vendor's keys need (``PlatformAccount.configured``).

    The relay is the only path to such a server, and it sends the user's key
    from the account: the sandbox is given no vendor URL and no key. Left on,
    its tools are listed and every call fails, so it is turned off and said so
    instead.
    """
    from src.config.env import EGRESS_RELAY_SECRET

    servers = getattr(getattr(config, "mcp", None), "servers", None) or []
    for server in servers:
        account = PLATFORM_ACCOUNTS.get(server.auth) if server.enabled else None
        if account is None:
            continue
        if not EGRESS_RELAY_SECRET:
            reason = "it is reached only through the egress relay, and EGRESS_RELAY_SECRET is unset"
        elif not account.configured():
            reason = f"the keys or settings {account.name} needs are not configured"
        else:
            continue
        server.enabled = False
        logger.warning("MCP server %r is turned off: %s", server.name, reason)


def platform_accounts_configured() -> bool:
    """Whether the operator's config declares any server on a platform account."""
    from src.server.app import setup

    config = setup.agent_config
    return config is not None and any(
        s.enabled and s.auth in PLATFORM_ACCOUNTS for s in config.mcp.servers
    )
