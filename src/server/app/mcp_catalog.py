"""User-level MCP server API — the Plugins backing store.

An ``enabled`` row is live config: ``resolve_mcp_config`` inherits it into
every one of the user's workspaces. A disabled row is inert — a stored
definition that reaches no workspace until it is enabled. Every route here is
owner-scoped, so responses echo the stored env/header maps as written — vault
refs and the owner's own literals, never a resolved secret — because a PUT
replaces the whole row and the edit form has to round-trip them.
``env_refs``/``header_refs`` remain the display-only vault-name projection.

Endpoints (user-scoped):
- GET    /api/v1/mcp/servers
- POST   /api/v1/mcp/servers
- GET    /api/v1/mcp/servers/{name}
- GET    /api/v1/mcp/servers/{name}/tools
- PUT    /api/v1/mcp/servers/{name}
- PATCH  /api/v1/mcp/servers/{name}/enabled
- DELETE /api/v1/mcp/servers/{name}
- GET    /api/v1/mcp/builtin-servers
- PATCH  /api/v1/mcp/builtin-servers/{name}/enabled
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, HTTPException
from pydantic import ValidationError

from src.server.database.mcp_oauth import (
    ConnectionStatus,
    get_connection,
    list_connections,
)
from src.server.services.mcp_config import builtin_names
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_catalog_server,
    delete_catalog_server,
    get_catalog_server,
    list_catalog_servers,
    list_local_servers_for_user,
    list_scope_markers_for_user,
    list_user_builtin_disables,
    set_catalog_server_enabled,
    set_user_builtin_disable,
)
from src.server.database.mcp_tool_schemas import get_user_tool_schemas
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.models.mcp_server import (
    BuiltinServer,
    BuiltinServerList,
    CatalogServer,
    CatalogServerList,
    EnabledInput,
    McpServerInput,
    WorkspaceScopedServer,
    _format_validation_error,
    catalog_row_to_response,
    isolation_warnings,
    parse_mcp_servers_payload,
)
from src.server.services.mcp_catalog import apply_catalog_edit, detach_warning
from src.server.services.mcp_import import ImportScope, run_mcp_import
from src.server.services.vault_invalidation import USER_TIER, after_secret_change
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])


async def _oauth_status_by_server(user_id: str) -> dict[str, ConnectionStatus]:
    """server_name → connection status, for decorating catalog responses."""
    try:
        return {
            c["server_name"]: ConnectionStatus(c["status"])
            for c in await list_connections(user_id)
        }
    except Exception:
        logger.warning(
            "[mcp_catalog] OAuth connection lookup failed for %s", user_id,
            exc_info=True,
        )
        return {}


async def _tool_counts_by_server(
    user_id: str, rows: list[dict]
) -> dict[str, int]:
    """server_name → discovered tool count, hash-gated to the CURRENT config.

    Same acceptance rule as the workspace effective list (``ToolSnapshotIndex``
    owns it), so the number shown here always matches what workspaces serve.
    Pure decoration: any failure degrades to no counts, never a 500.
    """
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import ToolSnapshotIndex

    try:
        schema_rows = await get_user_tool_schemas(user_id)
    except Exception:
        logger.warning(
            "[mcp_catalog] tool-schema lookup failed for %s", user_id,
            exc_info=True,
        )
        return {}
    snapshots = ToolSnapshotIndex(user_rows=schema_rows)
    counts: dict[str, int] = {}
    for row in rows:
        try:
            snapshot = snapshots.ok(user_row_to_server_config(row))
        except Exception:  # noqa: BLE001 — malformed row: just omit the count
            continue
        if snapshot is not None:
            counts[row["name"]] = len(snapshot.get("tools") or [])
    return counts


async def _oauth_headers_warning(user_id: str, server: McpServerInput) -> str | None:
    """Warn when configured headers meet a live OAuth connection.

    The two are independently settable, but the OAuth path never sends the
    configured headers: the probe sends its own, host discovery and the relay
    send only the OAuth Authorization. Silence would read as pass-through.
    """
    if not server.headers:
        return None
    try:
        connection = await get_connection(user_id, server.name)
    except Exception:
        logger.warning(
            "[mcp_catalog] OAuth connection lookup failed for %s", user_id,
            exc_info=True,
        )
        return None
    if connection is None or connection.status == ConnectionStatus.REVOKED:
        return None
    return (
        "This server is OAuth-connected, so its configured headers are not "
        "sent: discovery and sandbox tool calls carry only the OAuth "
        "Authorization header. Disconnect OAuth to use headers instead."
    )


async def _write_warnings(user_id: str, server: McpServerInput) -> list[str] | None:
    """The write-time nudges for a catalog row: isolation, then dropped headers."""
    warnings = isolation_warnings(server)
    if headers_warning := await _oauth_headers_warning(user_id, server):
        warnings.append(headers_warning)
    return warnings or None


@router.get("/servers")
@handle_api_exceptions("list MCP catalog servers", logger)
async def list_servers(
    user_id: CurrentUserId, all_scopes: bool = False
) -> CatalogServerList:
    """The user's catalog; ``all_scopes`` adds the scope-management inventory:
    per-server tombstone workspaces (the "active in" deny-list) and every
    workspace-local server across the user's workspaces."""
    rows = await list_catalog_servers(user_id)
    oauth = await _oauth_status_by_server(user_id)
    tool_counts = await _tool_counts_by_server(user_id, rows)
    servers = [
        catalog_row_to_response(
            r,
            oauth_status=oauth.get(r["name"]),
            tool_count=tool_counts.get(r["name"]),
        )
        for r in rows
    ]
    workspace_servers: list[WorkspaceScopedServer] = []
    if all_scopes:
        markers = await list_scope_markers_for_user(user_id)
        tombstoned: dict[str, list[str]] = {}
        for m in markers:
            if m["source"] == "user":
                tombstoned.setdefault(m["name"], []).append(m["workspace_id"])
        for server in servers:
            server.disabled_workspace_ids = sorted(
                tombstoned.get(server.name, [])
            )
        catalog_names = {r["name"] for r in rows}
        for local in await list_local_servers_for_user(user_id, live_only=True):
            config = local.get("config") or {}
            workspace_servers.append(
                WorkspaceScopedServer(
                    name=local["name"],
                    workspace_id=local["workspace_id"],
                    transport=config.get("transport") or "stdio",
                    enabled=bool(local["enabled"]),
                    description=config.get("description") or "",
                    shadows_inherited=local["name"] in catalog_names,
                )
            )
    return CatalogServerList(
        servers=servers,
        max_servers=MAX_CATALOG_SERVERS_PER_USER,
        workspace_servers=workspace_servers,
    )


@router.post("/servers", status_code=201)
@handle_api_exceptions("create MCP catalog server", logger)
async def create_server(
    user_id: CurrentUserId, body: dict = Body(...)
) -> CatalogServer:
    try:
        server = McpServerInput(**body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))
    if server.name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{server.name!r} collides with a built-in server name",
        )
    try:
        row = await create_catalog_server(
            user_id, server.name, **server.to_catalog_fields()
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    response = catalog_row_to_response(row)
    # A brand-new name has no connection, but a recreate over a name whose
    # connection row outlived the old catalog entry does.
    response.warnings = await _write_warnings(user_id, server)
    return response


@router.get("/servers/{name}")
@handle_api_exceptions("get MCP catalog server", logger)
async def get_server(name: str, user_id: CurrentUserId) -> CatalogServer:
    row = await get_catalog_server(user_id, name)
    if not row:
        raise HTTPException(status_code=404, detail="MCP server not found")
    oauth = await _oauth_status_by_server(user_id)
    return catalog_row_to_response(row, oauth_status=oauth.get(name))


@router.get("/servers/{name}/tools")
@handle_api_exceptions("list MCP catalog server tools", logger)
async def get_server_tools(name: str, user_id: CurrentUserId) -> dict:
    """The discovered tool snapshot for one catalog server, hash-gated to its
    CURRENT config (``ToolSnapshotIndex`` owns the acceptance rule) — the
    detail view must never show tools a workspace would not serve. Rows are
    sanitized at cache-write time, so this is a plain projection."""
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import ToolSnapshotIndex

    row = await get_catalog_server(user_id, name)
    if not row:
        raise HTTPException(status_code=404, detail="MCP server not found")
    snapshot = None
    try:
        schema_rows = await get_user_tool_schemas(user_id)
        snapshot = ToolSnapshotIndex(user_rows=schema_rows).ok(
            user_row_to_server_config(row)
        )
    except Exception:
        logger.warning(
            "[mcp_catalog] tool-schema lookup failed for %s", user_id, exc_info=True
        )
    tools = (snapshot or {}).get("tools") or []
    return {
        "server_name": name,
        "tools": [
            {
                "name": t.get("name", ""),
                "description": t.get("description", ""),
                "input_schema": t.get("input_schema") or {},
            }
            for t in tools
        ],
        "discovered_at": (snapshot or {}).get("discovered_at"),
    }


@router.put("/servers/{name}")
@handle_api_exceptions("update MCP catalog server", logger)
async def update_server(
    name: str, user_id: CurrentUserId, body: dict = Body(...)
) -> CatalogServer:
    try:
        server = McpServerInput(**body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))
    # The path name is authoritative; a renamed body is rejected to avoid
    # silently creating a second row under a different key.
    if server.name != name:
        raise HTTPException(
            status_code=409, detail="name in body must match the path name"
        )
    # A hand edit forks the row off its plugin; the service owns that decision
    # along with the consent revoke and the rediscovery kick.
    edit = await apply_catalog_edit(
        user_id, name, server.to_catalog_fields(), detach_plugin=True
    )
    if edit is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
    response = catalog_row_to_response(edit.row)
    # After the revoke inside the edit, so one that just severed the connection
    # does not warn about headers it has now made effective.
    response.warnings = await _write_warnings(user_id, server)
    if plugin := edit.detached_from_plugin:
        response.warnings = (response.warnings or []) + [detach_warning(plugin)]
    return response


@router.post("/servers/import")
@handle_api_exceptions("import MCP catalog servers", logger)
async def import_servers(
    user_id: CurrentUserId, body: dict = Body(...)
) -> dict:
    """Parse a standard ``{"mcpServers": {...}}`` blob into the user catalog.

    Mirrors the workspace import (name coercion, transport mapping, literal
    credentials auto-extracted — here into the USER vault) with one deliberate
    difference: imported rows land ``enabled=false`` (inert templates), so an
    import never silently changes every workspace's toolset. The UI nudges the
    user to flip each one live.
    """
    parsed = parse_mcp_servers_payload(body)
    if not parsed:
        raise HTTPException(
            status_code=422,
            detail='No MCP servers found. Expected a JSON object like '
            '{"mcpServers": { "<name>": { ... } }}.',
        )

    async def create_secret(conn, secret) -> None:
        await create_user_secret(
            user_id, secret.name, secret.value, secret.description, conn=conn
        )

    async def persist(conn, server: McpServerInput) -> bool:
        # No ON CONFLICT arm here — a raced duplicate raises ValueError, so a
        # successful call always means "created".
        await create_catalog_server(
            user_id, server.name, conn=conn, **server.to_catalog_fields()
        )
        return True

    existing_names = {r["name"] for r in await list_catalog_servers(user_id)}
    report = await run_mcp_import(
        parsed,
        scope=ImportScope(
            reserved_names=builtin_names(),
            existing_names=existing_names,
            current_count=len(existing_names),
            cap=MAX_CATALOG_SERVERS_PER_USER,
            cap_message=(
                f"Plugins server cap "
                f"({MAX_CATALOG_SERVERS_PER_USER}) reached"
            ),
            exists_message="already exists in your Plugins",
            existing_secret_names=set(await get_user_secret_names(user_id)),
            create_secret=create_secret,
            persist=persist,
        ),
    )

    # The imported SERVERS land disabled (inert), so they need no fan-out; the
    # imported SECRETS do. One can complete a ``${vault:NAME}`` ref that an
    # already-enabled connector has been dangling on, and nothing else in this
    # path purges its snapshot, bumps the version, or pushes to a live sandbox.
    for name in dict.fromkeys(report.secrets_created):
        await after_secret_change(USER_TIER, user_id, name, user_id=user_id)

    return {
        "results": report.results,
        "created": report.created,
        "secrets_created": report.secrets_created,
        "config_version": 0,
    }


async def _relay_execution_warning(user_id: str, name: str) -> str | None:
    """OAuth-connected servers execute only via the egress relay — activation
    is the moment to tell the user their deployment can't actually run them."""
    from src.config.env import EGRESS_RELAY_SECRET
    from src.server.app import setup
    from src.server.services.egress.reachability import (
        effective_relay_base_url,
        relay_reachability_warning,
    )

    if setup.agent_config is None:
        return None
    if await get_connection(user_id, name) is None:
        return None
    if not EGRESS_RELAY_SECRET:
        return (
            "The egress relay is disabled (EGRESS_RELAY_SECRET is not set), so "
            "this server's tools cannot run in sandboxes. Set a strong "
            "EGRESS_RELAY_SECRET in the backend environment and restart."
        )
    provider = setup.agent_config.sandbox.provider
    return relay_reachability_warning(provider, effective_relay_base_url(provider))


@router.patch("/servers/{name}/enabled")
@handle_api_exceptions("toggle MCP catalog server", logger)
async def set_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Flip a user server live/inert. The DB layer bumps every workspace's
    ``mcp_config_version`` in the same transaction (next-acquire convergence)."""
    from src.server.services.mcp_oauth.lifecycle import revoke_live_grants

    found = await set_catalog_server_enabled(user_id, name, body.enabled)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    out: dict = {"name": name, "enabled": body.enabled}
    if body.enabled:
        warning = await _relay_execution_warning(user_id, name)
        if warning:
            out["warnings"] = [warning]
    else:
        await revoke_live_grants(user_id, [name])
    return out


@router.delete("/servers/{name}")
@handle_api_exceptions("delete MCP catalog server", logger)
async def delete_server(name: str, user_id: CurrentUserId) -> dict:
    from src.server.services.mcp_oauth.lifecycle import oauth_fence

    # The drop takes the OAuth fence: a catalog row has no FK to its connection,
    # so dropping it unfenced orphans a live token. oauth_fence carries the why.
    async with oauth_fence(user_id, [name]):
        found = await delete_catalog_server(user_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Builtins — process-global servers with a per-user account-wide toggle
# ---------------------------------------------------------------------------


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
    disabled = await list_user_builtin_disables(user_id)
    marked: dict[str, list[str]] = {}
    if all_scopes:
        for m in await list_scope_markers_for_user(user_id):
            if m["source"] == "builtin":
                marked.setdefault(m["name"], []).append(m["workspace_id"])
    return BuiltinServerList(
        servers=[
            BuiltinServer(
                name=s.name,
                description=s.description or "",
                transport=s.transport,
                enabled=s.name not in disabled,
                disabled_workspace_ids=sorted(marked.get(s.name, [])),
            )
            for s in setup.agent_config.mcp.servers
            if getattr(s, "enabled", True)
        ]
    )


@router.patch("/builtin-servers/{name}/enabled")
@handle_api_exceptions("toggle builtin MCP server", logger)
async def set_builtin_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Account-wide toggle for a builtin — applies to every workspace of the
    user, and no workspace marker can re-enable it. The DB layer fans the
    ``mcp_config_version`` bump out in the same transaction."""
    if name not in builtin_names():
        raise HTTPException(status_code=404, detail="Unknown builtin server")
    await set_user_builtin_disable(user_id, name, disabled=not body.enabled)
    return {"name": name, "enabled": body.enabled}
