"""Bringing this worker in line with the saved settings.

``refresh`` reads the whole table and applies it, so any worker reaches the same
state from the same rows however many saves it missed, and applying twice costs
nothing. Postgres stays the truth; this process holds only its environment and
the config built from it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import zlib
from collections.abc import Mapping
from dataclasses import dataclass

from ptc_agent.config.core import KindsInReach, kind_availability
from src.server.database.deployment_settings import (
    add_deployment_setting,
    read_deployment_settings,
    write_deployment_settings,
)
from src.server.services.setup_settings.catalog import (
    FMP_KEY,
    PROVIDER_SETTING,
    SANDBOX_KEYS,
    SANDBOX_NAMESPACES,
)
from src.server.services.setup_settings.environment import (
    effective_values,
    export,
    take_snapshot,
)

logger = logging.getLogger(__name__)

_lock = asyncio.Lock()
# Names already in this worker's environment whose apply has not finished. The
# environment no longer differs for them, so a refresh that raised partway is
# finished by the next one only through this.
_unapplied: set[str] = set()


@dataclass(frozen=True)
class ConfigReading:
    """How this server's config load reads what the config file leaves open."""

    autodetect: bool
    in_reach: KindsInReach | None = None


async def in_reach_of(rows: Mapping[str, str]) -> KindsInReach | None:
    """The owner's choice and what can run beside it, once they have chosen.

    Having used the step means having chosen, not having reached the last
    screen, which the ordinary model wizard also shows. A deployment that has
    not keeps its own list: its environment may hold keys it never meant to
    offer.
    """
    if PROVIDER_SETTING not in rows:
        return None
    return KindsInReach(chosen=rows[PROVIDER_SETTING], availability=await kind_availability())


def _platform_secrets_declared() -> bool:
    from ptc_agent.config.file_utils import load_agent_config

    return bool((load_agent_config().get("sandbox") or {}).get("platform_secrets"))


async def _mint_namespaces(rows: Mapping[str, str]) -> bool:
    """A platform-secret catalog needs a namespace for each cloud kind it runs
    on, unique to this deployment so two on one account never share a Secret. It
    must not change once used, hence a saved row rather than the derivation alone.
    """
    if not _platform_secrets_declared():
        return False
    values = effective_values(rows)
    minted = False
    for kind, var in SANDBOX_NAMESPACES.items():
        if values.get(var) or not values.get(SANDBOX_KEYS[kind]):
            continue
        place = f"{socket.gethostname()}-{os.getcwd()}".encode()
        await add_deployment_setting(var, f"langalpha-local-{zlib.crc32(place)}")
        minted = True
    return minted


async def _read_rows() -> dict[str, str]:
    rows = await read_deployment_settings()
    if await _mint_namespaces(rows):
        rows = await read_deployment_settings()
    return rows


async def _reset_market_data() -> None:
    """Providers chosen and clients built before the key changed hold the old one."""
    from src.data_client.fmp import replace_fmp_client
    from src.data_client.registry import reset_data_providers

    # The client first: a chain built between the two holds the old one, and
    # the reset drops that chain.
    await replace_fmp_client()
    await reset_data_providers()


def _current_config():
    from src.server.app import setup

    return setup.agent_config


async def _rebuild_config(config, rows: Mapping[str, str], changed: set[str]) -> None:
    """Build what the environment decides afresh, through the factories config
    load uses, and swap it in at once.

    A turn copies the config per use (``to_core_config``), so it sees all of a
    refresh or none of it, and what was built before keeps its copy: the
    sessions open now, and the cached clients dropped below.
    """
    from ptc_agent.config.file_utils import clear_config_cache, load_agent_config
    from ptc_agent.config.utils import (
        create_mcp_config,
        create_sandbox_config,
        default_search_api,
    )
    from src.server.services.egress.platform_accounts import (
        disable_unreachable_platform_servers,
    )

    if changed:
        # Config load expanded ``${VAR}`` once, so a changed key needs it again.
        clear_config_cache()
    config_data = load_agent_config()
    in_reach = await in_reach_of(rows)

    sandbox = create_sandbox_config(config_data, autodetect=True, in_reach=in_reach)
    search_api = default_search_api(config_data)
    mcp = config_mcp_before = config.mcp
    if changed and config_data.get("mcp"):
        # As at boot, a server no call could reach is turned off.
        staged = config.model_copy(update={"mcp": create_mcp_config(config_data["mcp"])})
        disable_unreachable_platform_servers(staged)
        mcp = staged.mcp
    config.sandbox, config.mcp, config.search_api = sandbox, mcp, search_api
    if mcp is not config_mcp_before:
        # Sessions borrow the registry frozen at boot, not this config.
        from ptc_agent.core.mcp_registry import follow_config

        await follow_config(config.to_core_config())

    if changed & set(SANDBOX_KEYS.values()):
        from src.server.services.computer_manager import ComputerManager

        manager = ComputerManager.current()
        if manager is not None:
            await manager.drop_cached_providers()


async def refresh() -> dict[str, str]:
    """Read the table and apply it to this worker. Returns the rows."""
    global _unapplied
    async with _lock:
        rows = await _read_rows()
        changed = export(rows) | _unapplied
        _unapplied = changed
        if FMP_KEY in changed:
            await _reset_market_data()
        config = _current_config()
        if config is not None:
            await _rebuild_config(config, rows, changed)
        _unapplied = set()
        return rows


async def load_at_boot() -> ConfigReading:
    """Before config load, so a saved key or choice reads like any other value,
    and the owner's choice reaches the loader. Never fatal: a deployment still
    boots on its ``.env``."""
    take_snapshot()
    try:
        async with _lock:
            rows = await _read_rows()
            if export(rows):
                from ptc_agent.config.file_utils import clear_config_cache

                # Reading the rows loaded the config, expanding ``${VAR}``
                # before these were in the environment.
                clear_config_cache()
            return ConfigReading(autodetect=True, in_reach=await in_reach_of(rows))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Saved deployment settings not loaded: %s", type(exc).__name__)
        return ConfigReading(autodetect=True)


async def save(values: Mapping[str, str | None]) -> dict[str, str]:
    """Write, apply here, then tell the other workers. Returns the rows."""
    from src.server.services.setup_settings.follow import publish_changed

    await write_deployment_settings(values)
    rows = await refresh()
    await publish_changed()
    return rows
