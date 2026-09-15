"""Built-in MCP servers: process-global config with a per-user toggle.

A builtin is not a catalog row: it is this build's own configuration,
discovered once at startup and identical for every user. What IS per-user is
whether it is switched on, which is why these routes sit beside the catalog
rather than inside it, and why the catalog's wire shape stays untouched.

Endpoints (user-scoped):
- GET   /api/v1/mcp/builtin-servers
- GET   /api/v1/mcp/builtin-servers/{name}/tools
- PATCH /api/v1/mcp/builtin-servers/{name}/enabled
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from src.server.app.mcp_icons import icon_url
from src.server.database.account_disables import (
    list_account_disables,
    set_account_disable,
)
from src.server.database.mcp_servers import list_scope_markers_for_user
from src.server.models.mcp_server import (
    BuiltinServer,
    BuiltinServerList,
    EnabledInput,
)
from src.server.services.mcp_config import builtin_names
from src.server.services.plugins.bundled import component_owners
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])


@router.get("/builtin-servers")
@handle_api_exceptions("list builtin MCP servers", logger)
async def list_builtin_servers(
    user_id: CurrentUserId, all_scopes: bool = False
) -> BuiltinServerList:
    """The process-global builtins with this user's account-wide enabled state.

    A separate route from the catalog list on purpose: builtins are config,
    not rows, and the catalog wire shape stays untouched. ``all_scopes`` adds
    each builtin's per-workspace disable-markers for the "active in" checklist.
    """
    from src.server.app import setup

    if setup.agent_config is None:
        # Startup race: report an empty list rather than 500.
        return BuiltinServerList(servers=[])
    from ptc_agent.core.mcp_registry import get_global_registry

    # Provenance, not state: a server whose bundle is off keeps its own
    # enabled flag and travels with ``plugin_enabled=False``, the same way a
    # catalog row explains a plugin holding it down. Both kinds, one read.
    disables = await list_account_disables(user_id)
    owners = component_owners().servers
    registry = get_global_registry()
    connectors = registry.connectors if registry else {}
    marked: dict[str, list[str]] = {}
    if all_scopes:
        for m in await list_scope_markers_for_user(user_id):
            if m["source"] == "builtin":
                marked.setdefault(m["name"], []).append(m["workspace_id"])
    servers = []
    for s in setup.agent_config.mcp.servers:
        if not getattr(s, "enabled", True):
            continue
        connector = connectors.get(s.name)
        owner = owners.get(s.name)
        servers.append(
            BuiltinServer(
                name=s.name,
                description=s.description or "",
                transport=s.transport,
                enabled=s.name not in disables.servers,
                icon_url=await icon_url(
                    connector.server_info if connector else None
                ),
                plugin_name=owner,
                plugin_enabled=(
                    owner not in disables.bundles if owner is not None else None
                ),
                disabled_workspace_ids=sorted(marked.get(s.name, [])),
            )
        )
    return BuiltinServerList(servers=servers)


@router.patch("/builtin-servers/{name}/enabled")
@handle_api_exceptions("toggle builtin MCP server", logger)
async def set_builtin_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Account-wide toggle for a builtin. Applies to every workspace of the
    user, and no workspace marker can re-enable it. The DB layer fans the
    ``mcp_config_version`` bump out in the same transaction."""
    if name not in builtin_names():
        raise HTTPException(status_code=404, detail="Unknown builtin server")
    await set_account_disable(user_id, "server", name, disabled=not body.enabled)
    return {"name": name, "enabled": body.enabled}


@router.get("/builtin-servers/{name}/tools")
@handle_api_exceptions("list builtin MCP server tools", logger)
async def get_builtin_server_tools(name: str, user_id: CurrentUserId) -> dict:
    """The tools a builtin reported, read from the frozen process registry.

    A separate route from the catalog's tool snapshot because the two are
    discovered by different things at different times. A catalog server is the
    user's, and it is discovered when they add or refresh it, so its schemas
    live in their rows. A builtin is config: this process connected to it at
    startup and froze what it reported, which is the same answer for every
    user and is already in memory here.

    ``connected`` is the field that keeps this honest. A server whose startup
    connect failed is dropped from the registry and never retried, because the
    snapshot is frozen for the life of the process, so this worker has no
    answer for it while its siblings answer normally. Reporting that as an
    empty tool list would state as fact something only this process believes;
    the caller is told the difference and says so.
    """
    if name not in builtin_names():
        raise HTTPException(status_code=404, detail="Unknown builtin server")
    from ptc_agent.core.mcp_registry import get_global_registry

    registry = get_global_registry()
    connector = registry.connectors.get(name) if registry else None
    return {
        "server_name": name,
        "connected": connector is not None,
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            }
            for tool in (connector.tools if connector else [])
        ],
        # The catalog's field, always null here: a builtin is discovered once
        # per process rather than at a moment the user did something, so a
        # timestamp would date this worker's boot, not the server's schemas.
        "discovered_at": None,
    }
