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
- POST   /api/v1/workspaces/{id}/mcp/servers/{name}/promote
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, HTTPException
from pydantic import ValidationError

from src.server.database.mcp_servers import (
    MAX_MCP_SERVERS_PER_WORKSPACE,
    create_catalog_server,
    delete_catalog_server,
    delete_workspace_server,
    get_catalog_server,
    get_workspace_servers_and_version,
    insert_workspace_server,
    list_workspace_servers,
    set_catalog_server_enabled,
    set_workspace_server_enabled,
    upsert_workspace_server,
)
from src.server.database.mcp_tool_schemas import get_tool_schemas, get_user_tool_schemas
from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.database.vault_secrets import (
    create_secret as create_secret_db,
    get_workspace_secret_names,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.mcp_catalog import (
    apply_catalog_edit,
    detach_warning,
    reject_reserved_brokerage_name,
    reject_reserved_catalog_name,
)
from src.server.services.mcp_config import (
    Origin,
    ResolvedServer,
    State,
    account_disabled_builtins,
    builtin_names,
    classify_server_name,
    reserved_catalog_names,
    resolve_mcp_config,
)
from src.server.services.mcp_discovery import ToolSnapshotIndex
from src.server.services.mcp_import import ImportScope, run_mcp_import
from src.server.services.vault_invalidation import refs_for_server
from src.server.models.mcp_server import (
    CatalogServer,
    EffectiveServer,
    EffectiveServerList,
    EnabledInput,
    McpServerInput,
    PromoteInput,
    ToolSummary,
    _format_validation_error,
    catalog_row_to_response,
    collect_vault_refs,
    isolation_warnings,
    parse_mcp_servers_payload,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import CurrentUserId, handle_api_exceptions, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["MCP Servers"])

# Re-running discovery for a freshly-discovered server is wasteful; skip it if
# the cached row at the current version is < this many seconds old and not
# pending (kept simple — no Redis).
_DISCOVER_DEBOUNCE_SECONDS = 15

# Mutation refusals, written once — the three endpoints reach the same states.
_NOT_FOUND = "MCP server not found"
_BUILTIN_EDIT = "Cannot edit a built-in server"
_BUILTIN_DELETE = "Cannot delete a built-in server"
_INHERITED_EDIT = (
    "This server is inherited from your Plugins — edit it there, or add a "
    "copy to this workspace to fork it."
)
_INHERITED_DELETE = (
    "This server is inherited from your Plugins — remove it there, or "
    "disable it for this workspace."
)
_INHERITED_DELETE_TOMBSTONE = (
    "This server is inherited from your Plugins — remove it there, or "
    "re-enable it for this workspace."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _require_owned_workspace(workspace_id: str, user_id: str) -> dict:
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


def _derive_status(
    *,
    origin: Origin,
    refs: set[str],
    secret_names: set[str],
    schema_row: dict[str, Any] | None,
) -> tuple[str, str, list[str]]:
    """Derive the (status, error, missing_secrets) triple for one effective server.

    - builtin disabled-marker rows never reach here (excluded from effective).
    - builtins are process-global ⇒ ``connected``.
    - a server with a ``${vault:NAME}`` ref that ``secret_names`` cannot satisfy
      ⇒ ``needs_secret``. ``secret_names`` must be the merged user+workspace set
      the sandbox actually resolves against, and ``refs`` the full resolve-time
      scan (env/headers/args/url — ``refs_for_server``), not just the env/header
      projections: the import path writes ``--flag=${vault:N}`` args, and a ref
      only in args fails at call time all the same.
    - else from the schema cache at the current version: ``ok`` ⇒ connected,
      ``error`` ⇒ error (with text), missing row ⇒ pending.
    """
    missing = sorted(refs - secret_names)
    if origin is Origin.BUILTIN:
        return "connected", "", missing
    if missing:
        return "needs_secret", "", missing
    if schema_row is None:
        return "pending", "", missing
    status = schema_row.get("status")
    if status == "ok":
        return "connected", "", missing
    if status == "error":
        return "error", str(schema_row.get("error") or "discovery failed"), missing
    return "pending", "", missing


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


# Statuses where the sandbox is on its way *up* toward running — a warm is in
# flight (our proactive MCP apply, or workspace entry, kicked one). The UI uses
# this to keep polling and show "Starting workspace…" through the
# stopped→starting→running gap, rather than freezing on a stale "stopped".
_WARMING_STATUSES = frozenset({"starting", "creating"})


def _sandbox_warming(workspace: dict) -> bool:
    return workspace.get("status") in _WARMING_STATUSES


# ---------------------------------------------------------------------------
# GET — effective list
# ---------------------------------------------------------------------------


def _effective_server(
    entry: ResolvedServer,
    *,
    status: str,
    config_version: int,
    error: str = "",
    tools: list[ToolSummary] | None = None,
    missing_secrets: list[str] | None = None,
    env_refs: list[str] | None = None,
    header_refs: list[str] | None = None,
) -> EffectiveServer:
    """Build one effective-list row; editable/deletable derive from origin."""
    tools = tools or []
    srv = entry.config
    origin = entry.origin
    return EffectiveServer(
        oauth_status=entry.oauth_status,
        disabled_scope=entry.disabled_scope,
        plugin_name=entry.plugin_name,
        name=srv.name,
        origin=origin,
        transport=srv.transport,
        enabled=entry.state is State.ACTIVE,
        editable=(origin is Origin.WORKSPACE),
        deletable=(origin is Origin.WORKSPACE),
        status=status,
        error=error,
        tool_count=len(tools),
        tools=tools,
        missing_secrets=missing_secrets or [],
        env_refs=env_refs or [],
        header_refs=header_refs or [],
        # Echo the stored reference maps (refs/literals, never resolved
        # secrets) so the edit form round-trips them; built-ins stay empty.
        env=dict(srv.env or {}) if origin is Origin.WORKSPACE else {},
        headers=dict(srv.headers or {}) if origin is Origin.WORKSPACE else {},
        description=srv.description or "",
        instruction=srv.instruction or "",
        tool_exposure_mode=srv.tool_exposure_mode or "summary",
        discovery_uses_secrets=bool(getattr(srv, "discovery_uses_secrets", False)),
        command=srv.command,
        args=list(srv.args or []),
        url=srv.url,
        config_version=config_version,
    )


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

    resolved, secret_names, schema_rows, user_secret_names, user_schema_rows = (
        await asyncio.gather(
            resolve_mcp_config(base_config, user_id, workspace_id),
            get_workspace_secret_names(workspace_id),
            get_tool_schemas(workspace_id),
            get_user_secret_names(user_id),
            get_user_tool_schemas(user_id),
        )
    )
    snapshots = ToolSnapshotIndex(
        workspace_rows=schema_rows, user_rows=user_schema_rows
    )
    # The sandbox vault merges user + workspace secrets (workspace wins), so a
    # ref resolvable from either tier is satisfied.
    merged_secret_names = set(secret_names) | set(user_secret_names)

    def _row_for(entry: ResolvedServer) -> EffectiveServer:
        srv = entry.config
        origin = entry.origin
        env_refs = collect_vault_refs(dict(srv.env or {}))
        header_refs = collect_vault_refs(dict(srv.headers or {}))
        if entry.state is State.ACTIVE:
            # No status gate: an ``error`` snapshot is how the row reports why
            # a server isn't serving tools. Built-ins never carry one.
            schema_row = (
                None if origin is Origin.BUILTIN else snapshots.snapshot(srv)
            )
            status, error, missing = _derive_status(
                origin=origin,
                refs=refs_for_server(srv),
                # Merged for BOTH tiers: the push is one namespace, so a
                # workspace server's ref lands whichever tier defines it.
                secret_names=merged_secret_names,
                schema_row=schema_row,
            )
            tools = _tools_from_schema(schema_row)
        else:
            status, error, missing, tools = "disabled", "", [], []
        row = _effective_server(
            entry,
            status=status,
            error=error,
            tools=tools,
            missing_secrets=missing,
            env_refs=env_refs,
            header_refs=header_refs,
            config_version=resolved.version,
        )
        if origin is Origin.WORKSPACE and srv.name in resolved.shadowed_inherited_names:
            row.shadows_inherited = True
        return row

    # One row per entry, in resolver order: the running set first, then the
    # rows carried purely so the UI keeps a re-enable toggle (disabled
    # built-ins, tombstoned inherited, disabled workspace servers). A SHADOWED
    # inherited server has no row of its own — its local fork carries the flag.
    servers = [
        _row_for(entry)
        for entry in resolved.entries
        if entry.state is not State.SHADOWED
    ]

    # Version the running session has actually applied (no I/O) — drives the
    # frontend's version-accurate "synced" state. None when no warm session.
    applied_version: int | None = None
    try:
        applied_version = WorkspaceManager.get_instance().get_applied_mcp_config_version(
            workspace_id, expected_sandbox_id=workspace.get("sandbox_id")
        )
    except Exception:
        logger.debug("[mcp] applied version lookup failed for %s", workspace_id)

    return EffectiveServerList(
        servers=servers,
        sandbox_running=_sandbox_running(workspace),
        sandbox_warming=_sandbox_warming(workspace),
        max_servers=MAX_MCP_SERVERS_PER_WORKSPACE,
        config_version=resolved.version,
        applied_config_version=applied_version,
    )


async def _insert_local_fork(
    workspace_id: str, server: McpServerInput
) -> dict | None:
    """Insert a ``source='workspace'`` row, replacing a tombstone squatter.

    Conflict-safe insert first (ON CONFLICT DO NOTHING): two concurrent
    creates of the same new name can't both win — the loser gets None, never
    a silent UPDATE. A tombstone marker (``source='user'``, from disabling an
    inherited server) squats the UNIQUE(workspace_id, name) slot — that one is
    replaced with the local fork; real workspace rows stay None (⇒ 409). The
    tombstone branch reads then upserts without a shared lock, so racing
    creates over a tombstone are last-write-wins (same user only — callers
    owner-check the workspace). Raises ValueError over cap.
    """
    row = await insert_workspace_server(
        workspace_id, server.name, config=server.to_config_blob()
    )
    if row is not None:
        return row
    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    existing = rows.get(server.name)
    if existing is not None and existing["source"] == "user":
        return await upsert_workspace_server(
            workspace_id,
            server.name,
            source="workspace",
            enabled=True,
            config=server.to_config_blob(),
        )
    return None


# ---------------------------------------------------------------------------
# POST — add
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers", status_code=201)
@handle_api_exceptions("add workspace MCP server", logger)
async def add_server(
    workspace_id: str,
    user_id: CurrentUserId,
    body: dict = Body(...),
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    try:
        server = McpServerInput(**body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))

    if server.name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{server.name!r} collides with a built-in server name",
        )
    # A workspace row shadows the inherited catalog row of the same name whether
    # it is enabled or not, so this name is spoken for here too.
    reject_reserved_brokerage_name(server.name)

    try:
        row = await _insert_local_fork(workspace_id, server)
    except ValueError as e:
        # DB layer signals over-cap by raising ValueError under the advisory lock.
        raise HTTPException(status_code=409, detail=str(e))
    if row is None:
        raise HTTPException(
            status_code=409, detail=f"{server.name!r} already exists in this workspace"
        )
    _schedule_proactive_apply(workspace_id, user_id)
    response = {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}
    if warnings := isolation_warnings(server):
        response["warnings"] = warnings
    return response


# ---------------------------------------------------------------------------
# POST — promote a workspace server UP into the user's template catalog
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/{name}/promote", status_code=201)
@handle_api_exceptions("promote workspace MCP server to template", logger)
async def promote_server(
    workspace_id: str,
    name: str,
    user_id: CurrentUserId,
    body: PromoteInput | None = None,
) -> CatalogServer:
    """Save a workspace server's definition as a reusable user-level template.

    Copies the workspace row's config into the user catalog (re-validated
    through the same input model). Only
    ``${vault:NAME}`` reference names travel — secret values are workspace-scoped
    and never copied, so the template surfaces ``missing_secrets`` when later
    added to another workspace. ``overwrite`` replaces an existing template of
    the same name; without it a name clash is a 409.
    """
    await _require_owned_workspace(workspace_id, user_id)
    overwrite = bool(body and body.overwrite)
    remove_source = bool(body and body.remove_source)

    if name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail="Built-in servers are global; only workspace servers can be "
            "saved as templates",
        )
    # Promoting mints a catalog row, so it owes the same reservation the create
    # and import doors owe: the name is what the Plugins page joins a shipped
    # brokerage on, and a template is free to point anywhere.
    reject_reserved_catalog_name(name)

    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    existing = rows.get(name)
    if existing is None or existing["source"] != "workspace":
        raise HTTPException(status_code=404, detail="MCP server not found")

    # Re-validate the stored config so a template is never minted from a row that
    # no longer passes the (possibly tightened) policy.
    try:
        server = McpServerInput(**(existing.get("config") or {}))
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))

    fields = server.to_catalog_fields()

    async def _finish(row: dict) -> CatalogServer:
        """Shared tail for both the overwrite and create arms."""
        if remove_source:
            if existing["enabled"] and not row["enabled"]:
                # A move keeps the server live. Promote mints inert templates
                # (enabled is not part of the copied fields), but this row was
                # running in the source workspace — landing it disabled would
                # silently switch the server off everywhere. The DB toggle
                # bumps every workspace's version in its own transaction.
                await set_catalog_server_enabled(user_id, server.name, True)
                row = {**row, "enabled": True}
            # Drop the local fork so it doesn't shadow the template it just
            # created. Ordered catalog-write-then-delete: a crash in between
            # leaves the ordinary shadow state, which the resolver already
            # renders and a later delete resolves.
            await delete_workspace_server(workspace_id, name)
            _schedule_proactive_apply(workspace_id, user_id)
        return catalog_row_to_response(row)

    if overwrite:
        # An overwrite is a catalog edit like the PUT, so it owes the same
        # policy: it can move a connected server off its consented endpoint (or
        # onto stdio, which has no relay path at all), and it forks a
        # plugin-owned template the same way a hand edit does. The write and
        # the revoke are not atomic — a refresh racing the gap is caught by the
        # consent re-check in refresh_user_tool_schemas.
        edit = await apply_catalog_edit(
            user_id, server.name, fields, detach_plugin=True
        )
        if edit is not None:
            response = await _finish(edit.row)
            # Same forking as the PUT, so it says the same thing: a detach the
            # user is not told about reads as one the plugin sanctioned.
            if plugin := edit.detached_from_plugin:
                response.warnings = (response.warnings or []) + [
                    detach_warning(plugin)
                ]
            return response
        # Nothing to overwrite (raced delete / never existed) ⇒ fall through.

    if await get_catalog_server(user_id, server.name) is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A template named {server.name!r} already exists. "
            "Pass overwrite to replace it.",
        )
    try:
        row = await create_catalog_server(user_id, server.name, **fields)
    except ValueError as e:
        # DB layer signals over-cap (or a raced duplicate) by raising ValueError.
        raise HTTPException(status_code=409, detail=str(e))
    return await _finish(row)


# ---------------------------------------------------------------------------
# POST — adopt a user-level server DOWN into this workspace (move, not copy)
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/{name}/adopt", status_code=201)
@handle_api_exceptions("move user MCP server into workspace", logger)
async def adopt_server(
    workspace_id: str, name: str, user_id: CurrentUserId
) -> dict:
    """Move a user-level (Plugins) server into this workspace only.

    The inverse of promote-with-remove_source: the catalog row becomes a
    workspace-local fork here, then the catalog row is deleted (which also
    clears the name's tombstones everywhere). OAuth-connected servers refuse
    the move — connections exist only at the user tier, so moving would sever
    the login. Plugin-owned servers refuse it too (see below). The fork lands
    enabled regardless of the catalog flag: scoping a server to one workspace
    is a statement of intent to use it here.
    """
    from src.server.database.mcp_oauth import ConnectionStatus, get_connection
    from src.server.services.mcp_oauth.lifecycle import oauth_fence

    await _require_owned_workspace(workspace_id, user_id)

    row = await get_catalog_server(user_id, name)
    if row is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    # Asked before the connection test below, which would otherwise answer a
    # connected brokerage with "disconnect it first, then move the server" --
    # true of the connection and useless here, because disconnecting does not
    # make this move possible. A brokerage row that moved down would land under
    # a name the workspace resolver skips, and the edit path that could rename
    # it refuses the same name, so it would be inert with no way back but
    # deleting it. The tier is the point: the connection lives at the user tier,
    # and every surface joins the row to the shipped vendor there.
    reject_reserved_brokerage_name(name)
    if row["plugin_id"] is not None:
        # Plugin-level disable acts through ONE predicate, on
        # list_enabled_user_servers, and that predicate only reaches the user
        # tier. A component moved down here would keep serving after its
        # plugin was disabled, with nothing left at the user tier to suppress:
        # this move is the one way out of the chokepoint the whole design
        # rests on. The manifest still declares the component too, so the next
        # plugin update re-creates the catalog row and the workspace fork
        # starts shadowing it. Refuse, the same way an OAuth connection does,
        # and leave detaching to the edit path that says so out loud.
        owner = row["plugin_name"] or "a plugin"
        raise HTTPException(
            status_code=409,
            detail=f"This server is installed by the plugin {owner!r}, which "
            "manages it at the account level. Edit the server to detach it "
            "from the plugin first, then move it. Uninstalling the plugin "
            "removes the server instead.",
        )
    connection = await get_connection(user_id, name)
    if connection is not None and connection.status is not ConnectionStatus.REVOKED:
        raise HTTPException(
            status_code=409,
            detail="This server has an OAuth connection, which only exists at "
            "the user level. Disconnect it first, then move the server.",
        )

    # Re-validate through the input model so the move can never mint a row
    # that no longer passes (possibly tightened) policy.
    try:
        server = McpServerInput(
            name=name,
            **{
                k: row[k]
                for k in (
                    "transport", "command", "args", "url", "env", "headers",
                    "description", "instruction", "tool_exposure_mode",
                    "discovery_uses_secrets",
                )
            },
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))

    try:
        ws_row = await _insert_local_fork(workspace_id, server)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if ws_row is None:
        raise HTTPException(
            status_code=409, detail=f"{name!r} already exists in this workspace"
        )

    # Fork first, catalog delete second: a crash in between leaves the shadow
    # state the resolver already renders. The delete also purges the name's
    # tombstones across every workspace and the user-tier discovery cache.
    # Only a REVOKED connection can exist here, but the drop still takes the
    # fence: a callback landing in the gap must not leave a live token behind a
    # server that no longer exists.
    async with oauth_fence(user_id, [name]):
        await delete_catalog_server(user_id, name)
    _schedule_proactive_apply(workspace_id, user_id)
    return {
        "name": ws_row["name"],
        "source": ws_row["source"],
        "enabled": ws_row["enabled"],
    }


# ---------------------------------------------------------------------------
# POST — bulk import a standard `mcpServers` JSON blob
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/import")
@handle_api_exceptions("import workspace MCP servers", logger)
async def import_servers(
    workspace_id: str,
    user_id: CurrentUserId,
    body: dict = Body(...),
) -> dict:
    """Parse a standard ``{"mcpServers": {...}}`` blob and create each server.

    Names are coerced to our identifier shape, transports are mapped, and inline
    literal secrets are auto-extracted into the workspace vault (rewritten to
    ``${vault:NAME}`` refs, deduped by value across the import). Per-server
    outcomes are reported so a partial import is legible. Like every mutation,
    this only writes DB rows + bumps the config version — the change applies on
    the next agent run (≤30s).
    """
    await _require_owned_workspace(workspace_id, user_id)

    parsed = parse_mcp_servers_payload(body)
    if not parsed:
        raise HTTPException(
            status_code=422,
            detail='No MCP servers found. Expected a JSON object like '
            '{"mcpServers": { "<name>": { ... } }}.',
        )

    existing_rows, _ = await get_workspace_servers_and_version(workspace_id)

    async def create_secret(conn, secret) -> None:
        await create_secret_db(
            workspace_id, secret.name, secret.value, secret.description, conn=conn
        )

    async def persist(conn, server: McpServerInput) -> bool:
        # ON CONFLICT DO NOTHING ⇒ None means the name is taken, not an error.
        return await insert_workspace_server(
            workspace_id, server.name, config=server.to_config_blob(), conn=conn
        ) is not None

    report = await run_mcp_import(
        parsed,
        scope=ImportScope(
            reserved_names=reserved_catalog_names(),
            existing_names={r["name"] for r in existing_rows},
            # Only the workspace's OWN servers count against the cap; builtin
            # markers and inherited tombstones are not servers.
            current_count=sum(
                1 for r in existing_rows if r["source"] == "workspace"
            ),
            cap=MAX_MCP_SERVERS_PER_WORKSPACE,
            cap_message=(
                f"workspace MCP server cap "
                f"({MAX_MCP_SERVERS_PER_WORKSPACE}) reached"
            ),
            exists_message="already exists in this workspace",
            existing_secret_names=set(await get_workspace_secret_names(workspace_id)),
            create_secret=create_secret,
            persist=persist,
        ),
    )

    # Imported secrets are usable immediately on a live sandbox (best-effort);
    # the server set itself applies on the next agent run.
    if report.secrets_created:
        await _push_vault_to_sandbox(workspace_id)

    _, version = await get_workspace_servers_and_version(workspace_id)
    if report.created > 0:
        _schedule_proactive_apply(workspace_id, user_id)
    return {
        "results": report.results,
        "created": report.created,
        "secrets_created": report.secrets_created,
        "config_version": version,
    }


async def _push_vault_to_sandbox(workspace_id: str) -> None:
    """Best-effort push of vault secrets to a running sandbox."""
    try:
        wm = WorkspaceManager.get_instance()
        await wm.push_vault_secrets(workspace_id)
    except Exception:
        logger.warning(
            "[mcp] failed to push imported vault secrets for %s",
            workspace_id,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# PUT — edit a workspace-source row
# ---------------------------------------------------------------------------


@router.put("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("edit workspace MCP server", logger)
async def edit_server(
    workspace_id: str, name: str, body: McpServerInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in builtin_names():
        raise HTTPException(status_code=409, detail=_BUILTIN_EDIT)
    reject_reserved_brokerage_name(name)
    if body.name != name:
        raise HTTPException(
            status_code=409, detail="name in body must match the path name"
        )

    ref = await classify_server_name(workspace_id, user_id, name)
    if ref is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    match ref.origin:
        case Origin.WORKSPACE:
            pass
        case Origin.USER:
            raise HTTPException(status_code=409, detail=_INHERITED_EDIT)
        case _:
            raise HTTPException(status_code=409, detail=_BUILTIN_EDIT)

    row = await upsert_workspace_server(
        workspace_id,
        name,
        source="workspace",
        enabled=ref.state is State.ACTIVE,
        config=body.to_config_blob(),
    )
    _schedule_proactive_apply(workspace_id, user_id)
    response = {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}
    if warnings := isolation_warnings(body):
        response["warnings"] = warnings
    return response


# ---------------------------------------------------------------------------
# PATCH — enabled toggle (handles builtin disable-marker semantics)
# ---------------------------------------------------------------------------


@router.patch("/{workspace_id}/mcp/servers/{name}/enabled")
@handle_api_exceptions("toggle workspace MCP server", logger)
async def set_enabled(
    workspace_id: str, name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in builtin_names():
        # Built-ins are toggled by an explicit (source='builtin', enabled=false)
        # disable-marker row; enabling = delete the marker.
        if body.enabled:
            if name in await account_disabled_builtins(user_id):
                # Deleting the marker would report success and change nothing:
                # the account-level subtraction outranks every workspace,
                # whether it came from this server's own switch or from the
                # bundle that ships it.
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "This server is disabled for your account; enable it "
                        "in Plugins first"
                    ),
                )
            await delete_workspace_server(workspace_id, name)
        else:
            await upsert_workspace_server(
                workspace_id, name, source="builtin", enabled=False, config=None
            )
        _schedule_proactive_apply(workspace_id, user_id)
        return {"name": name, "enabled": body.enabled}

    ref = await classify_server_name(workspace_id, user_id, name)
    if ref is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    match (ref.origin, ref.state):
        case (Origin.WORKSPACE, _):
            await set_workspace_server_enabled(workspace_id, name, body.enabled)
        case (Origin.USER, State.TOMBSTONED):
            # An existing tombstone for an inherited server; enabling = delete
            # it. (Disabling again is a no-op — it's already tombstoned.)
            if body.enabled:
                await delete_workspace_server(workspace_id, name)
        case (Origin.USER, _):
            # Inherited and not yet marked: disabling writes the per-workspace
            # tombstone; enabling is a no-op (it's already live via inheritance).
            if not body.enabled:
                await upsert_workspace_server(
                    workspace_id, name, source="user", enabled=False, config=None
                )
        case _:
            # A disable-marker whose built-in no longer exists: nothing to toggle.
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
    _schedule_proactive_apply(workspace_id, user_id)
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

    if name in builtin_names():
        raise HTTPException(status_code=409, detail=_BUILTIN_DELETE)

    ref = await classify_server_name(workspace_id, user_id, name)
    if ref is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    match (ref.origin, ref.state):
        case (Origin.WORKSPACE, _):
            pass
        case (Origin.USER, State.TOMBSTONED):
            # Deleting the tombstone here would silently re-enable the
            # inherited server — make that toggle explicit instead.
            raise HTTPException(
                status_code=409, detail=_INHERITED_DELETE_TOMBSTONE
            )
        case (Origin.USER, _):
            raise HTTPException(status_code=409, detail=_INHERITED_DELETE)
        case _:
            raise HTTPException(status_code=409, detail=_BUILTIN_DELETE)

    await delete_workspace_server(workspace_id, name)
    _schedule_proactive_apply(workspace_id, user_id)
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

    if name in builtin_names():
        raise HTTPException(
            status_code=409, detail="Discovery is for user servers only"
        )

    resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
    # A shadowed inherited server never wins this lookup: its local fork sorts
    # ahead of it in entry order, which is the row the probe should address.
    entry = next((e for e in resolved.entries if e.name == name), None)
    if (
        entry is None
        or entry.state is not State.ACTIVE
        or entry.origin is Origin.BUILTIN
    ):
        raise HTTPException(status_code=404, detail="MCP server not found")
    if entry.host_side_oauth:
        # OAuth servers are discovered host-side (on connect and via the
        # Plugins refresh) — never probed from the sandbox; reconnecting,
        # not probing, is the fix for a disconnected one.
        raise HTTPException(
            status_code=409,
            detail="OAuth servers are discovered host-side; manage the "
            "connection from Plugins instead.",
        )
    server = entry.config

    # Debounce: if the cached snapshot is for this server's CURRENT config and is
    # fresh + settled, return it without re-running discovery. A stale-hash
    # row (config changed) always falls through to a real probe.
    snapshots = ToolSnapshotIndex(
        workspace_rows=await get_tool_schemas(workspace_id)
    )
    cached = snapshots.snapshot(server, accept=_settled_and_fresh)
    if cached is not None:
        return {"server": _discovery_row_to_dict(cached)}

    sandbox = _get_live_sandbox(workspace_id, workspace)
    rows = await discover_and_cache(workspace_id, sandbox, [server])
    row = rows[0] if rows else None
    if row is not None and row.get("status") == "ok":
        # The probe wrote a fresh snapshot WITHOUT a version bump, so a live
        # session's composite/summary would short-circuit past it on the next
        # acquire. Refresh explicitly (background, best-effort) so the agent
        # sees the same tools the UI now shows.
        _schedule_session_mcp_refresh(workspace_id, user_id)
    return {"server": _discovery_row_to_dict(row)}


# Strong refs to in-flight proactive-apply tasks so they aren't GC'd mid-run.
_proactive_apply_tasks: set[asyncio.Task] = set()
_proactive_apply_pending: dict[str, asyncio.Task] = {}
_PROACTIVE_APPLY_SETTLE_S = 1.5


def _schedule_proactive_apply(workspace_id: str, user_id: str) -> None:
    """Front-load verifying + applying a just-saved MCP config.

    Fire-and-forget so it never blocks (or fails) the mutation response. It
    drives a background session acquire that brings the applied config up to the
    new version — warming (cold-starting) the sandbox if it isn't running yet —
    so the change is discovered and live before the user's next turn (no
    surprise). Best-effort: any failure falls back to the next-message apply.

    Mutations within the settle window coalesce into one apply: a newer
    mutation cancels a still-waiting sleeper, never an in-flight apply.
    """
    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return

    pending = _proactive_apply_pending.get(workspace_id)
    if pending is not None and not pending.done():
        pending.cancel()

    async def _settle_then_apply() -> None:
        await asyncio.sleep(_PROACTIVE_APPLY_SETTLE_S)
        # Past the settle window: deregister so newer mutations schedule a
        # fresh apply instead of cancelling this one mid-flight.
        if _proactive_apply_pending.get(workspace_id) is asyncio.current_task():
            _proactive_apply_pending.pop(workspace_id, None)
        await wm.proactively_apply_mcp_config(workspace_id, user_id)

    task = asyncio.create_task(_settle_then_apply())
    _proactive_apply_pending[workspace_id] = task
    _proactive_apply_tasks.add(task)

    def _cleanup(t: asyncio.Task) -> None:
        _proactive_apply_tasks.discard(t)
        if _proactive_apply_pending.get(workspace_id) is t:
            _proactive_apply_pending.pop(workspace_id, None)

    task.add_done_callback(_cleanup)


def _schedule_session_mcp_refresh(workspace_id: str, user_id: str) -> None:
    """Background composite rebuild after an out-of-band schema-cache update.

    Unlike ``_schedule_proactive_apply`` there is no version bump to apply, so
    this goes through ``refresh_session_mcp`` (which busts the session's cached
    version first). Undebounced: probes are explicit single user actions.
    """
    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return
    task = asyncio.create_task(wm.refresh_session_mcp(workspace_id, user_id))
    _proactive_apply_tasks.add(task)
    task.add_done_callback(_proactive_apply_tasks.discard)


def _get_live_sandbox(workspace_id: str, workspace: dict) -> Any | None:
    """Return the in-memory live sandbox if one is ready, else None.

    Reads the cached session directly (no lock, no acquire) so discovery never
    races the warm/Phase-2 machinery, but fenced against the row's binding: a
    handle for a replaced sandbox would have discovery probe the dead one and
    persist its schemas under this workspace. A stopped/cold workspace, or a
    superseded handle, ⇒ None, which ``discover_and_cache`` turns into
    ``pending`` rows.
    """
    if not _sandbox_running(workspace):
        return None
    try:
        session = WorkspaceManager.get_instance().get_session_if_ready(
            workspace_id, expected_sandbox_id=workspace.get("sandbox_id")
        )
        return session.sandbox if session else None
    except Exception:
        logger.warning(
            "[mcp] could not resolve live sandbox for %s", workspace_id, exc_info=True
        )
        return None


def _settled_and_fresh(row: dict[str, Any]) -> bool:
    """Debounce acceptance: a still-pending probe is never worth returning."""
    return row.get("status") != "pending" and _is_fresh(row.get("discovered_at"))


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


def _discovery_status(raw: Any) -> str:
    """Map a schema-cache status to the McpStatus enum the effective list emits.

    The cache stores ``ok``; the API surfaces ``connected`` so the discovery
    probe and the effective list agree. ``error`` / ``pending`` pass through.
    """
    return "connected" if raw == "ok" else (str(raw) if raw else "pending")


def _discovery_row_to_dict(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {"status": "pending", "tools": [], "error": ""}
    return {
        "server_name": row.get("server_name"),
        "status": _discovery_status(row.get("status")),
        "tools": row.get("tools") or [],
        "error": row.get("error") or "",
        "config_hash": row.get("config_hash"),
        "discovered_at": row.get("discovered_at"),
    }
