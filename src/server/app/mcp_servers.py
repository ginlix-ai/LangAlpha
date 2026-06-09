"""Per-workspace MCP server API.

The effective-list endpoint calls the SAME ``resolve_mcp_config`` chokepoint the
sandbox-sync path uses and only decorates each server with live status drawn
from the discovery schema cache + the workspace vault. Mutations are DB-write
+ version-bump ONLY (plan §8): no sandbox push, no per-workspace lock, no live
mutation. The running session picks the change up on its next post-cooldown
acquire (≤30s).

Endpoints (all require_workspace_owner):
- GET    /api/v1/workspaces/{id}/mcp/servers
- POST   /api/v1/workspaces/{id}/mcp/servers
- PUT    /api/v1/workspaces/{id}/mcp/servers/{name}
- PATCH  /api/v1/workspaces/{id}/mcp/servers/{name}/enabled
- DELETE /api/v1/workspaces/{id}/mcp/servers/{name}
- POST   /api/v1/workspaces/{id}/mcp/servers/{name}/discover
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, HTTPException
from pydantic import ValidationError

from src.server.database.mcp_servers import (
    MAX_MCP_SERVERS_PER_WORKSPACE,
    delete_workspace_server,
    get_catalog_server,
    get_tool_schemas,
    list_workspace_servers,
    set_workspace_server_enabled,
    upsert_workspace_server,
)
from src.server.database.vault_secrets import get_workspace_secret_names
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.handlers.chat.mcp_config import resolve_mcp_config
from src.server.models.mcp_server import (
    EffectiveServer,
    EffectiveServerList,
    EnabledInput,
    McpServerInput,
    ToolSummary,
    collect_vault_refs,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import CurrentUserId, handle_api_exceptions, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["MCP Servers"])

# Re-running discovery for a freshly-discovered server is wasteful; skip it if
# the cached row at the current version is < this many seconds old and not
# pending (kept simple — no Redis).
_DISCOVER_DEBOUNCE_SECONDS = 15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_validation_error(exc: ValidationError) -> str:
    """Flatten a Pydantic ValidationError into a JSON-safe detail string."""
    parts = []
    for err in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in err.get("loc", ())) or "body"
        parts.append(f"{loc}: {err.get('msg', 'invalid')}")
    return "; ".join(parts) or "validation error"


def _builtin_names() -> set[str]:
    """Names of the process-global built-in MCP servers (from agent_config)."""
    from src.server.app import setup

    if setup.agent_config is None:
        return set()
    return {s.name for s in setup.agent_config.mcp.servers}


async def _require_owned_workspace(workspace_id: str, user_id: str) -> dict:
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


def _derive_status(
    *,
    origin: str,
    enabled: bool,
    env_refs: list[str],
    header_refs: list[str],
    secret_names: set[str],
    schema_row: dict[str, Any] | None,
) -> tuple[str, str]:
    """Derive the (status, error) pair for one effective server.

    - builtin disabled-marker rows never reach here (excluded from effective).
    - builtins are process-global ⇒ ``connected``.
    - a workspace server with a ``${vault:NAME}`` ref naming a secret missing
      from the workspace vault ⇒ ``needs_secret``.
    - else from the schema cache at the current version: ``ok`` ⇒ connected,
      ``error`` ⇒ error (with text), missing row ⇒ pending.
    """
    if origin == "builtin":
        return "connected", ""
    missing = [n for n in (*env_refs, *header_refs) if n not in secret_names]
    if missing:
        return "needs_secret", ""
    if schema_row is None:
        return "pending", ""
    status = schema_row.get("status")
    if status == "ok":
        return "connected", ""
    if status == "error":
        return "error", str(schema_row.get("error") or "discovery failed")
    return "pending", ""


def _tools_from_schema(schema_row: dict[str, Any] | None) -> list[ToolSummary]:
    if not schema_row:
        return []
    return [
        ToolSummary(
            name=str(t.get("name") or ""),
            description=str(t.get("description") or ""),
            input_schema=t.get("input_schema") or {},
        )
        for t in (schema_row.get("tools") or [])
    ]


def _sandbox_running(workspace: dict) -> bool:
    return workspace.get("status") == "running"


# ---------------------------------------------------------------------------
# GET — effective list
# ---------------------------------------------------------------------------


@router.get("/{workspace_id}/mcp/servers")
@handle_api_exceptions("list workspace MCP servers", logger)
async def list_servers(workspace_id: str, user_id: CurrentUserId) -> EffectiveServerList:
    workspace = await _require_owned_workspace(workspace_id, user_id)

    from src.server.app import setup

    base_config = setup.agent_config
    if base_config is None:
        # Startup race: report an empty effective set rather than 500.
        return EffectiveServerList(
            servers=[], sandbox_running=False,
            max_servers=MAX_MCP_SERVERS_PER_WORKSPACE, config_version=0,
        )

    resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
    secret_names = await get_workspace_secret_names(workspace_id)
    schema_rows = await get_tool_schemas(workspace_id, resolved.version)
    schema_by_name = {r["server_name"]: r for r in schema_rows}

    servers: list[EffectiveServer] = []
    for srv in resolved.servers:
        origin = "builtin" if srv.name in resolved.builtin_names else "workspace"
        env_refs = collect_vault_refs(dict(srv.env or {}))
        header_refs = collect_vault_refs(dict(srv.headers or {}))
        schema_row = schema_by_name.get(srv.name) if origin == "workspace" else None
        status, error = _derive_status(
            origin=origin,
            enabled=srv.enabled,
            env_refs=env_refs,
            header_refs=header_refs,
            secret_names=secret_names,
            schema_row=schema_row,
        )
        missing = sorted(
            {n for n in (*env_refs, *header_refs) if n not in secret_names}
        )
        tools = _tools_from_schema(schema_row)
        servers.append(
            EffectiveServer(
                name=srv.name,
                origin=origin,
                transport=srv.transport,
                enabled=srv.enabled,
                editable=(origin == "workspace"),
                deletable=(origin == "workspace"),
                status=status,
                error=error,
                tool_count=len(tools),
                tools=tools,
                missing_secrets=missing,
                env_refs=env_refs,
                header_refs=header_refs,
                description=srv.description or "",
                instruction=srv.instruction or "",
                tool_exposure_mode=srv.tool_exposure_mode,
                command=srv.command,
                args=list(srv.args or []),
                url=srv.url,
                config_version=resolved.version,
            )
        )

    return EffectiveServerList(
        servers=servers,
        sandbox_running=_sandbox_running(workspace),
        max_servers=MAX_MCP_SERVERS_PER_WORKSPACE,
        config_version=resolved.version,
    )


# ---------------------------------------------------------------------------
# POST — add (full def OR from_template)
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers", status_code=201)
@handle_api_exceptions("add workspace MCP server", logger)
async def add_server(
    workspace_id: str,
    user_id: CurrentUserId,
    body: dict = Body(...),
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if "from_template" in body:
        server = await _server_from_template(user_id, body)
    else:
        try:
            server = McpServerInput(**body)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=_format_validation_error(e))

    if server.name in _builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{server.name!r} collides with a built-in server name",
        )
    existing = {r["name"] for r in await list_workspace_servers(workspace_id)}
    if server.name in existing:
        raise HTTPException(
            status_code=409, detail=f"{server.name!r} already exists in this workspace"
        )

    try:
        row = await upsert_workspace_server(
            workspace_id,
            server.name,
            source="workspace",
            enabled=True,
            config=server.to_config_blob(),
        )
    except ValueError as e:
        # DB layer signals over-cap by raising ValueError under the advisory lock.
        raise HTTPException(status_code=409, detail=str(e))
    return {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}


async def _server_from_template(user_id: str, body: dict) -> McpServerInput:
    """Load a catalog template and re-validate it as a workspace server def."""
    if set(body) != {"from_template"}:
        raise HTTPException(
            status_code=422,
            detail="from_template must be the only field in the body",
        )
    template = await get_catalog_server(user_id, body["from_template"])
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    # Re-validate the stored template through the same input model. A template
    # that no longer passes the (possibly tightened) policy yields a 422.
    try:
        return McpServerInput(
            name=template["name"],
            transport=template["transport"],
            command=template.get("command"),
            args=template.get("args") or [],
            url=template.get("url"),
            env=template.get("env") or {},
            headers=template.get("headers") or {},
            description=template.get("description") or "",
            instruction=template.get("instruction") or "",
            tool_exposure_mode=template.get("tool_exposure_mode") or "summary",
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False))


# ---------------------------------------------------------------------------
# PUT — edit a workspace-source row
# ---------------------------------------------------------------------------


@router.put("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("edit workspace MCP server", logger)
async def edit_server(
    workspace_id: str, name: str, body: McpServerInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        raise HTTPException(status_code=409, detail="Cannot edit a built-in server")
    if body.name != name:
        raise HTTPException(
            status_code=409, detail="name in body must match the path name"
        )

    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    existing = rows.get(name)
    if existing is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
    if existing["source"] != "workspace":
        raise HTTPException(status_code=409, detail="Cannot edit a built-in server")

    row = await upsert_workspace_server(
        workspace_id,
        name,
        source="workspace",
        enabled=bool(existing["enabled"]),
        config=body.to_config_blob(),
    )
    return {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}


# ---------------------------------------------------------------------------
# PATCH — enabled toggle (handles builtin disable-marker semantics)
# ---------------------------------------------------------------------------


@router.patch("/{workspace_id}/mcp/servers/{name}/enabled")
@handle_api_exceptions("toggle workspace MCP server", logger)
async def set_enabled(
    workspace_id: str, name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        # Built-ins are toggled by an explicit (source='builtin', enabled=false)
        # disable-marker row; enabling = delete the marker.
        if body.enabled:
            await delete_workspace_server(workspace_id, name)
        else:
            await upsert_workspace_server(
                workspace_id, name, source="builtin", enabled=False, config=None
            )
        return {"name": name, "enabled": body.enabled}

    found = await set_workspace_server_enabled(workspace_id, name, body.enabled)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return {"name": name, "enabled": body.enabled}


# ---------------------------------------------------------------------------
# DELETE — remove a workspace row (409 on builtin)
# ---------------------------------------------------------------------------


@router.delete("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("delete workspace MCP server", logger)
async def delete_server(
    workspace_id: str, name: str, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        raise HTTPException(status_code=409, detail="Cannot delete a built-in server")

    found = await delete_workspace_server(workspace_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return {"ok": True}


# ---------------------------------------------------------------------------
# POST — on-demand discovery probe (debounced; no lock, no sandbox mutation)
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/{name}/discover")
@handle_api_exceptions("discover workspace MCP server", logger)
async def discover_server(
    workspace_id: str, name: str, user_id: CurrentUserId
) -> dict:
    workspace = await _require_owned_workspace(workspace_id, user_id)

    from src.server.app import setup
    from src.server.services.mcp_discovery import discover_and_cache

    base_config = setup.agent_config
    if base_config is None:
        raise HTTPException(status_code=503, detail="Agent config not ready")

    if name in _builtin_names():
        raise HTTPException(
            status_code=409, detail="Discovery is for user servers only"
        )

    resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
    server = next((s for s in resolved.servers if s.name == name), None)
    if server is None or name not in resolved.user_names:
        raise HTTPException(status_code=404, detail="MCP server not found")

    # Debounce: if the cached row at this version is fresh and not pending,
    # return it without re-running discovery.
    existing = {
        r["server_name"]: r
        for r in await get_tool_schemas(workspace_id, resolved.version)
    }
    cached = existing.get(name)
    if cached is not None and cached.get("status") != "pending":
        if _is_fresh(cached.get("discovered_at")):
            return {"server": _discovery_row_to_dict(cached)}

    sandbox = _get_live_sandbox(workspace_id, workspace)
    rows = await discover_and_cache(workspace_id, sandbox, [server], resolved.version)
    row = rows[0] if rows else None
    return {"server": _discovery_row_to_dict(row)}


def _get_live_sandbox(workspace_id: str, workspace: dict) -> Any | None:
    """Return the in-memory live sandbox if one is ready, else None.

    Reads the cached session directly (no lock, no acquire) so discovery never
    races the warm/Phase-2 machinery. A stopped/cold workspace ⇒ None, which
    ``discover_and_cache`` turns into ``pending`` rows.
    """
    if not _sandbox_running(workspace):
        return None
    try:
        wm = WorkspaceManager.get_instance()
        if not wm.has_ready_session(workspace_id):
            return None
        session = wm._sessions.get(workspace_id)
        return session.sandbox if session else None
    except Exception:
        logger.warning(
            "[mcp] could not resolve live sandbox for %s", workspace_id, exc_info=True
        )
        return None


def _is_fresh(discovered_at: Any) -> bool:
    """True if ``discovered_at`` (ISO string or datetime) is within the debounce."""
    if not discovered_at:
        return False
    if isinstance(discovered_at, str):
        try:
            dt = datetime.fromisoformat(discovered_at)
        except ValueError:
            return False
    elif isinstance(discovered_at, datetime):
        dt = discovered_at
    else:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - dt).total_seconds()
    return age < _DISCOVER_DEBOUNCE_SECONDS


def _discovery_row_to_dict(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {"status": "pending", "tools": [], "error": ""}
    return {
        "server_name": row.get("server_name"),
        "status": row.get("status"),
        "tools": row.get("tools") or [],
        "error": row.get("error") or "",
        "config_version": row.get("config_version"),
        "discovered_at": row.get("discovered_at"),
    }
