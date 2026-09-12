"""Direct MCP tools for a Flash turn, which has a workspace but no sandbox.

Flash runs on the user's one flash workspace and never opens a session, so
nothing on that path installs a composite or provisions relay grants. The
direct path needs neither: it reads the same resolve, the same host-side
discovery snapshot and the same binding plan the composite reads, syncs the
workspace's grants the same way the session binder does, and binds the
``direct`` and ``both`` sets. A ``both`` tool has no sandbox to be wrapped
in here, so on Flash it is simply a direct tool.

Grants are synced every turn rather than cached: the sync is one short
transaction that no-ops on an unchanged set, and it is what retires a grant
the moment the user disconnects, the same guarantee the sandbox path has.
"""

from __future__ import annotations

import logging
from typing import Any

from src.config.env import EGRESS_RELAY_SECRET
from src.server.database.egress_grants import sync_oauth_grants
from src.server.database.mcp_tool_schemas import get_user_tool_schemas
from src.server.services.egress.direct_tools import (
    FLASH_SANDBOX_ID,
    DirectMCPBinding,
    prepare_direct_mcp_tools,
)
from src.server.services.mcp_config import resolve_mcp_config
from src.server.services.mcp_discovery import ToolSnapshotIndex
from src.server.services.mcp_tool_split import build_direct_entries

logger = logging.getLogger(__name__)


def _direct_servers(resolved: Any) -> list[Any]:
    """The resolved servers Flash can bind: a binding plan, and a connection."""
    plans = resolved.binding_plans_by_name
    return [s for s in resolved.servers if s.name in plans and s.oauth_connection_id]


async def sync_flash_grants(
    base_config: Any, *, user_id: str, workspace_id: str
) -> None:
    """Bring the flash workspace's grants up to its current scope, now.

    Every turn syncs on its way in, which retires a grant before the *next*
    turn. A turn already running holds the grant it was bound with, so a scope
    change made mid-turn keeps reaching the vendor until that turn ends unless
    the change lands here as well. Nothing else converges this workspace, which
    is why the sync retries rather than accepting a superseded answer: there is
    no scheduled apply behind it to pick the retirement back up.
    """
    from src.server.services.egress.grant_resync import (
        GrantSyncSuperseded,
        sync_grants_until_current,
    )

    wrote = await sync_grants_until_current(
        base_config,
        user_id=user_id,
        workspace_id=workspace_id,
        connection_ids=lambda resolved: [
            s.oauth_connection_id for s in _direct_servers(resolved)
        ],
    )
    if not wrote:
        # Raised rather than logged because the caller's 200 is the claim that
        # the revocation happened, and here it would be false: no scheduled
        # apply stands behind this path, so an out-of-scope grant that survives
        # this call survives until the next turn binds. The toggle is safe to
        # repeat, and repeating it is what the caller should do.
        raise GrantSyncSuperseded(workspace_id)


async def bind_flash_direct_tools(
    base_config: Any, *, user_id: str | None, workspace_id: str
) -> DirectMCPBinding:
    empty = DirectMCPBinding(user_id=user_id)
    if not user_id or not EGRESS_RELAY_SECRET:
        logger.debug(
            "[DIRECT_MCP] flash: skipped user=%r secret=%s",
            user_id,
            bool(EGRESS_RELAY_SECRET),
        )
        return empty

    resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
    plans = resolved.binding_plans_by_name
    servers = _direct_servers(resolved)
    if not servers:
        logger.debug(
            "[DIRECT_MCP] flash: no directly bound server (servers=%s plans=%s)",
            [s.name for s in resolved.servers],
            list(plans),
        )
        # Still sync, with nothing: an empty set is what retires the grant a
        # previous turn left behind. Returning early here kept a thread that
        # already held the connector calling after the user took it out of
        # Flash's scope, which no other revocation path covers.
        await sync_oauth_grants(
            user_id=user_id,
            workspace_id=workspace_id,
            connection_ids=[],
            config_version=resolved.version,
        )
        return empty

    synced = await sync_oauth_grants(
        user_id=user_id,
        workspace_id=workspace_id,
        connection_ids=[s.oauth_connection_id for s in servers],
        config_version=resolved.version,
    )
    if synced is None:
        # A newer resolve owns the grants; this turn runs without direct
        # tools rather than on a set it cannot vouch for.
        logger.info("[DIRECT_MCP] flash resolve superseded; no direct tools this turn")
        return empty
    grants: dict[str, str] = {}
    for server in servers:
        grant_id = synced.grants.get(server.oauth_connection_id)
        if grant_id is None:
            # The connection vanished between resolve and here (disconnect
            # race). Fail closed and say so: this server's tools are simply
            # absent from the turn, and the PTC path warns about the same
            # miss, so a silent one here is the only place it does not show.
            logger.warning(
                "[DIRECT_MCP] flash: connection %s gone for server %s, left unbound",
                server.oauth_connection_id,
                server.name,
            )
            continue
        grants[server.name] = grant_id

    index = ToolSnapshotIndex(user_rows=await get_user_tool_schemas(user_id))
    # Flash has no sandbox, so the sandbox half of the split is dropped here.
    _, by_server = build_direct_entries(
        servers, index, denied=resolved.denied_tools_by_name, plans=plans
    )

    return await prepare_direct_mcp_tools(
        user_id=user_id,
        workspace_id=workspace_id,
        sandbox_id=FLASH_SANDBOX_ID,
        grants=grants,
        by_server=by_server,
    )
