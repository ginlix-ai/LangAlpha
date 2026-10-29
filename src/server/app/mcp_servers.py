"""Per-workspace MCP server API.

Servers are installed per user and selected per workspace: a name means one
server in every workspace of its user. This router is the workspace's view of
that. The effective-list endpoint calls the SAME ``resolve_mcp_config``
chokepoint the sandbox-sync path uses and only decorates each server with live
status drawn from the discovery schema cache + the user's vault. Adding a
server here installs it on the account and switches it on in this workspace
only (a workspace created later starts with it off); editing one edits the
account row, so it changes every workspace where the server is on. Mutations
are DB-write + version-bump ONLY (plan §8): no sandbox push, no per-workspace
lock, no live mutation. The running session picks the change up on its next
post-cooldown acquire (≤30s).

Endpoints (all require_workspace_owner):
- GET    /api/v1/workspaces/{id}/mcp/servers
- POST   /api/v1/workspaces/{id}/mcp/servers
- POST   /api/v1/workspaces/{id}/mcp/servers/import
- PUT    /api/v1/workspaces/{id}/mcp/servers/{name}
- PATCH  /api/v1/workspaces/{id}/mcp/servers/{name}/enabled
- POST   /api/v1/workspaces/{id}/mcp/servers/{name}/discover
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial
from typing import Any

from fastapi import APIRouter, Body, HTTPException

from src.server.database.home_workspace import is_unbound
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_workspace_catalog_server,
    delete_workspace_server,
    get_catalog_server,
    get_workspace_servers_and_version,
    list_catalog_servers,
    runs_on_account,
    tombstone_user_server,
    untombstone_user_server,
    upsert_workspace_server,
)
from src.server.database.mcp_tool_schemas import get_tool_schemas, get_user_tool_schemas
from src.server.database.user_vault_secrets import (
    get_user_secret_names,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.brokerages import brokerage_names
from src.server.services.computer_manager import MachineTarget
from src.server.services.mcp_apply import (
    schedule_proactive_apply,
    schedule_session_mcp_refresh,
)
from src.server.services.mcp_catalog import detach_warning, refuse_missing_sign_in
from src.server.services.mcp_catalog_view import catalog_write_warnings
from src.server.services.mcp_config import (
    Origin,
    ResolvedServer,
    State,
    account_disabled_builtins,
    builtin_names,
    classify_server_name,
    resolve_mcp_config,
)
from src.server.services.mcp_discovery import ToolSnapshotIndex
from src.server.services.mcp_oauth.discovery import (
    REMOTE_TRANSPORTS,
    discover_catalog_server,
    schedule_catalog_discovery,
)
from src.server.services.mcp_oauth.lifecycle import TokenUnavailable
from src.server.services.mcp_import import catalog_import_scope, run_mcp_import
from src.server.services.mcp_saves import (
    NOT_FOUND,
    ServerEdit,
    WorkspaceAdd,
    parse_server,
)
from src.server.services.mcp_validation import check_servers
from src.server.services.vault_invalidation import (
    after_secrets_changed,
    refs_for_server,
)
from src.server.models.mcp_server import (
    EffectiveServer,
    EffectiveServerList,
    EnabledInput,
    McpServerInput,
    ParsedMcpServer,
    ToolSummary,
    collect_vault_refs,
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
      ⇒ ``needs_secret``. ``secret_names`` must be the user's vault, the one
      namespace the sandbox resolves against, and ``refs`` the full resolve-time
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
    """Build one effective-list row; editability derives from origin.

    A user row is editable from any workspace because the edit lands on the
    one account-level definition; a brokerage's row is managed by its own
    connect flow on Plugins.
    """
    tools = tools or []
    srv = entry.config
    origin = entry.origin
    user_row = origin is Origin.USER
    return EffectiveServer(
        oauth_status=entry.oauth_status,
        disabled_scope=entry.disabled_scope,
        plugin_name=entry.plugin_name,
        name=srv.name,
        origin=origin,
        transport=srv.transport,
        enabled=entry.state is State.ACTIVE,
        editable=user_row and srv.name not in brokerage_names(),
        status=status,
        error=error,
        tool_count=len(tools),
        tools=tools,
        missing_secrets=missing_secrets or [],
        env_refs=env_refs or [],
        header_refs=header_refs or [],
        # Echo the stored reference maps (refs/literals, never resolved
        # secrets) so the edit form round-trips them; built-ins stay empty.
        env=dict(srv.env or {}) if user_row else {},
        headers=dict(srv.headers or {}) if user_row else {},
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
            max_servers=MAX_CATALOG_SERVERS_PER_USER, config_version=0,
        )

    resolved, schema_rows, user_secret_names, user_schema_rows = (
        await asyncio.gather(
            resolve_mcp_config(base_config, user_id, workspace_id),
            get_tool_schemas(workspace_id),
            get_user_secret_names(user_id),
            get_user_tool_schemas(user_id),
        )
    )
    snapshots = ToolSnapshotIndex(
        workspace_rows=schema_rows, user_rows=user_schema_rows
    )
    secret_names = set(user_secret_names)

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
                secret_names=secret_names,
                schema_row=schema_row,
            )
            tools = _tools_from_schema(schema_row)
        else:
            status, error, missing, tools = "disabled", "", [], []
        return _effective_server(
            entry,
            status=status,
            error=error,
            tools=tools,
            missing_secrets=missing,
            env_refs=env_refs,
            header_refs=header_refs,
            config_version=resolved.version,
        )

    # One row per entry, in resolver order: the running set first, then the
    # rows carried purely so the UI keeps a re-enable toggle (disabled
    # built-ins, tombstoned inherited).
    servers = [_row_for(entry) for entry in resolved.entries]

    # Version the running session has actually applied (no I/O) — drives the
    # frontend's version-accurate "synced" state. None when no warm session.
    applied_version: int | None = None
    try:
        applied_version = WorkspaceManager.get_instance().get_applied_mcp_config_version(
            workspace_id,
            expected_sandbox_id=workspace.get("sandbox_id"),
            target=MachineTarget.of_workspace(workspace),
        )
    except Exception:
        logger.debug("[mcp] applied version lookup failed for %s", workspace_id)

    return EffectiveServerList(
        servers=servers,
        sandbox_running=_sandbox_running(workspace),
        sandbox_warming=_sandbox_warming(workspace),
        max_servers=MAX_CATALOG_SERVERS_PER_USER,
        config_version=resolved.version,
        applied_config_version=applied_version,
    )


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
    """Install a server on the user's account, switched on only here.

    The row is the same one Plugins lists; every other workspace of the user
    gets a tombstone in the same transaction, and a workspace created later
    starts with it off, so the server starts nowhere the user did not add it.
    It is written only once the server has passed its check, a command server
    on this workspace's computer.
    """
    save = WorkspaceAdd(user_id, parse_server(body), workspace_id)
    await save.refuse()
    await save.check()
    row = await save.write()
    schedule_catalog_discovery(user_id, row["name"], reason="create")
    await save.after_write()
    response = {"name": row["name"], "source": "user", "enabled": True}
    if warnings := await catalog_write_warnings(user_id, save.server):
        response["warnings"] = warnings
    return response


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

    Each server lands the way the add route lands one: on the user's account,
    switched on only in this workspace, and off in workspaces created later.
    Names are coerced to our identifier shape, transports are mapped, and
    inline literal secrets are auto-extracted into the user's vault (rewritten
    to ``${vault:NAME}`` refs, deduped by value across the import). Each entry
    is checked before it lands, a command server on this workspace's computer,
    and one that fails or needs a sign-in is reported rather than created.
    Per-server outcomes are reported so a partial import is legible.
    """
    await _require_owned_workspace(workspace_id, user_id)

    parsed = parse_mcp_servers_payload(body)
    if not parsed:
        raise HTTPException(
            status_code=422,
            detail='No MCP servers found. Expected a JSON object like '
            '{"mcpServers": { "<name>": { ... } }}.',
        )

    async def persist(
        conn, server: McpServerInput, entry: ParsedMcpServer
    ) -> bool:
        # A raced duplicate raises ValueError, so returning means "created".
        await create_workspace_catalog_server(
            user_id, workspace_id, server.name, conn=conn,
            **server.to_catalog_fields(),
        )
        return True

    report = await run_mcp_import(
        parsed,
        scope=await catalog_import_scope(
            user_id,
            existing_names={r["name"] for r in await list_catalog_servers(user_id)},
            persist=persist,
            exists_message="already exists in your Plugins",
            check=partial(check_servers, user_id, workspace_id=workspace_id),
        ),
    )

    # A new secret can complete a ref an already-running server was missing,
    # so it gets the same fan-out as one saved on the vault page, once for the
    # batch rather than one vault push per secret.
    await after_secrets_changed(user_id, report.secrets_created)
    for result in report.results:
        if result.get("status") == "created":
            schedule_catalog_discovery(user_id, result["name"], reason="import")

    _, version = await get_workspace_servers_and_version(workspace_id)
    if report.created > 0:
        schedule_proactive_apply(workspace_id, user_id)
    return {
        "results": report.results,
        "created": report.created,
        "secrets_created": report.secrets_created,
        "config_version": version,
    }


# ---------------------------------------------------------------------------
# PUT: edit the account-level server behind a workspace row
# ---------------------------------------------------------------------------


@router.put("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("edit workspace MCP server", logger)
async def edit_server(
    workspace_id: str, name: str, body: McpServerInput, user_id: CurrentUserId
) -> dict:
    """Edit a server from a workspace; the change reaches every workspace
    where the server is on.

    There is one definition per name, so this is the Plugins edit reached from
    here, through the same service: it is checked when it moves how the server
    is reached, a plugin-owned row detaches from its plugin, and an OAuth
    consent the edit moves off is revoked.
    """
    save = ServerEdit(user_id, name, body, workspace_id)
    ref = await save.refuse()
    edit = await save.write()
    await save.after_write()
    response = {
        "name": name,
        "source": "user",
        "enabled": ref.state is State.ACTIVE,
    }
    warnings = await catalog_write_warnings(user_id, body) or []
    if plugin := edit.detached_from_plugin:
        warnings.append(detach_warning(plugin))
    if warnings:
        response["warnings"] = warnings
    return response


# ---------------------------------------------------------------------------
# PATCH — enabled toggle (handles builtin disable-marker semantics)
# ---------------------------------------------------------------------------


_ACCOUNT_DISABLED = (
    "This server is disabled for your account; enable it in Plugins first"
)


def _refuse_account_disabled(catalog: dict[str, Any]) -> None:
    """Refuse an enable the account's switch, or its plugin's, outranks.

    Reporting it on here would claim a switch that did not move. A concurrent
    account toggle needs no lock: either order is one the user could have
    made on purpose.
    """
    if not runs_on_account(catalog):
        raise HTTPException(status_code=409, detail=_ACCOUNT_DISABLED)


@router.patch("/{workspace_id}/mcp/servers/{name}/enabled")
@handle_api_exceptions("toggle workspace MCP server", logger)
async def set_enabled(
    workspace_id: str, name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    workspace = await _require_owned_workspace(workspace_id, user_id)

    if name in builtin_names():
        # Built-ins are toggled by an explicit (source='builtin', enabled=false)
        # disable-marker row; enabling = delete the marker.
        if body.enabled:
            if name in await account_disabled_builtins(user_id):
                # Deleting the marker would report success and change nothing:
                # the account-level subtraction outranks every workspace,
                # whether it came from this server's own switch or from the
                # bundle that ships it.
                raise HTTPException(status_code=409, detail=_ACCOUNT_DISABLED)
            await delete_workspace_server(workspace_id, name)
        else:
            await upsert_workspace_server(
                workspace_id, name, source="builtin", enabled=False, config=None
            )
        await _apply_toggle_now(workspace, workspace_id, user_id)
        return {"name": name, "enabled": body.enabled}

    ref = await classify_server_name(workspace_id, user_id, name)
    if ref is None:
        raise HTTPException(status_code=404, detail=NOT_FOUND)
    match (ref.origin, ref.state):
        case (Origin.USER, State.TOMBSTONED):
            # An existing tombstone for an inherited server; enabling = delete
            # it. (Disabling again is a no-op: it's already tombstoned.) While
            # the account keeps the server off, dropping it would report a
            # switch that did not move and start the server here later.
            if body.enabled:
                catalog = await get_catalog_server(user_id, name)
                # The account's own switch answers first (the untombstone
                # below refuses it under its lock): signing in would not
                # turn on a server the account keeps off.
                if catalog is not None and runs_on_account(catalog):
                    await refuse_missing_sign_in(user_id, catalog)
                match await untombstone_user_server(
                    user_id, workspace_id, name, ref.row["workspace_mcp_server_id"]
                ):
                    case "account_off":
                        raise HTTPException(status_code=409, detail=_ACCOUNT_DISABLED)
                    case "gone":
                        # Deleted, or replaced by a server scoped off here.
                        raise HTTPException(status_code=404, detail=NOT_FOUND)
        case (Origin.USER, _):
            # Inherited and not yet marked: disabling writes the per-workspace
            # tombstone; enabling is a no-op (it's already live via inheritance).
            if body.enabled:
                _refuse_account_disabled(ref.row)
                await refuse_missing_sign_in(user_id, ref.row)
            elif not await tombstone_user_server(
                user_id, workspace_id, name, ref.row["user_mcp_server_id"]
            ):
                # Deleted since it was classified, or replaced by a server
                # this request never read.
                raise HTTPException(status_code=404, detail=NOT_FOUND)
        case _:
            # A disable-marker whose built-in no longer exists: nothing to toggle.
            raise HTTPException(status_code=404, detail=NOT_FOUND)
    await _apply_toggle_now(workspace, workspace_id, user_id)
    return {"name": name, "enabled": body.enabled}


async def _apply_toggle_now(
    workspace: dict, workspace_id: str, user_id: str
) -> None:
    """Retire the toggled-off grants now, and warm the change in if there is a sandbox.

    An unbound row (Home before its first turn, or a former one) is on no
    computer, so there is no sandbox to warm, and the acquire would only fail;
    the turn that binds Home applies the config then. Its grants are still
    synced: they are scoped to the row while it has no computer, and the order
    reconcile reads through any active grant of the user's.
    """
    await _sync_sandbox_grants_now(workspace_id, user_id)
    if not is_unbound(workspace):
        schedule_proactive_apply(workspace_id, user_id)


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
    entry = next((e for e in resolved.entries if e.name == name), None)
    if (
        entry is None
        or entry.state is not State.ACTIVE
        or entry.origin is Origin.BUILTIN
    ):
        raise HTTPException(status_code=404, detail="MCP server not found")
    server = entry.config
    if entry.origin is Origin.USER and server.transport in REMOTE_TRANSPORTS:
        # An inherited remote row is discovered from the host, whatever it
        # authenticates with: the OAuth bearer never enters a sandbox, and a
        # header row's snapshot has to land where Plugins reads it. Same
        # debounce as the sandbox path, against the user tier.
        cached = ToolSnapshotIndex(
            user_rows=await get_user_tool_schemas(user_id)
        ).snapshot(server, accept=_settled_and_fresh)
        if cached is not None:
            return {"server": _discovery_row_to_dict(cached)}
        try:
            row = await discover_catalog_server(user_id, name)
        except TokenUnavailable as e:
            # A connection the user has to repair: reconnecting, not probing,
            # is the fix, and the row's connection status already says so.
            raise HTTPException(
                status_code=409,
                detail=f"OAuth connection is {e.reason}; manage the connection "
                "from Plugins instead.",
            )
        if row is not None:
            if row.get("status") == "ok":
                schedule_session_mcp_refresh(workspace_id, user_id)
            return {"server": _discovery_row_to_dict(row)}
        # None: the row has no host-side path -- it is an ``sse`` row, or it
        # was deleted or turned stdio between the resolve and the re-read. The
        # checks below judge the entry as it resolved, and a row that is gone
        # is refused there or probed in the workspace.
    if entry.host_side_oauth:
        raise HTTPException(
            status_code=409,
            detail="OAuth servers are discovered host-side; manage the "
            "connection from Plugins instead.",
        )

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
        schedule_session_mcp_refresh(workspace_id, user_id)
    return {"server": _discovery_row_to_dict(row)}


async def _sync_sandbox_grants_now(workspace_id: str, user_id: str) -> None:
    """Retire a workspace's out-of-scope relay grants before replying.

    ``schedule_proactive_apply`` converges these too, but it sleeps first and
    swallows its own failures, so between the 200 and that task a turn already
    in flight still holds an active grant for a server the workspace no longer
    resolves. The relay authorizes against the grant row on every request, so
    retiring the row here is what actually stops the next call; the scheduled
    apply still runs, because it is what pushes the new credential file into
    the sandbox.

    The kept set is the one ``sync_egress_relay`` keeps, which is why it is
    read from the same ``grant_scope``: narrowing it to the directly bound
    servers would retire the grants the sandbox wrappers dial through.
    """
    from src.server.app import setup

    base_config = setup.agent_config
    if base_config is None:
        return
    from src.server.services.egress.grant_resync import sync_grants_until_current
    from src.server.services.egress.grant_scope import grant_refs, user_snapshots

    snapshots = await user_snapshots(user_id)
    # Not raised when the retries are exhausted: ``schedule_proactive_apply``
    # runs behind it and re-resolves, so the retirement has somewhere else to
    # land, and failing the toggle would be the harsher answer to a race that
    # fixes itself. A row on no computer has no apply behind it, but no turn
    # holds its grants either: the turn that binds it syncs the whole set.
    await sync_grants_until_current(
        base_config,
        user_id=user_id,
        workspace_id=workspace_id,
        refs=lambda resolved: grant_refs(
            resolved, user_id=user_id, snapshots=snapshots
        ),
    )


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
            workspace_id,
            expected_sandbox_id=workspace.get("sandbox_id"),
            target=MachineTarget.of_workspace(workspace),
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
