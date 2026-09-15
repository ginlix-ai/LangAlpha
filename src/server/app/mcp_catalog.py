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
- POST   /api/v1/mcp/servers/probe
- POST   /api/v1/mcp/servers/import
- GET    /api/v1/mcp/servers/{name}
- GET    /api/v1/mcp/servers/{name}/tools
- PUT    /api/v1/mcp/servers/{name}
- PATCH  /api/v1/mcp/servers/{name}/enabled
- PATCH  /api/v1/mcp/servers/{name}/binding
- DELETE /api/v1/mcp/servers/{name}

The rest of the ``/api/v1/mcp`` prefix is not the user catalog and lives with
what it is about: ``mcp_builtin`` (this build's own servers), ``mcp_brokerages``
(the shipped connectors) and ``mcp_icons`` (the marks servers declare).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import ValidationError

from src.server.app.mcp_icons import icon_url
from src.server.database.mcp_oauth import (
    SERVABLE,
    ConnectionStatus,
    get_connection,
    list_connections,
)
from src.server.services.mcp_config import reserved_catalog_names
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_catalog_server,
    delete_catalog_server,
    get_catalog_server,
    list_catalog_servers,
    list_local_servers_for_user,
    list_scope_markers_for_user,
    set_catalog_server_enabled,
    update_catalog_server,
)
from src.server.database.mcp_tool_schemas import get_user_tool_schemas
from src.server.database.pool import get_db_connection
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.models.mcp_server import (
    BindingInput,
    CatalogServer,
    CatalogServerList,
    EnabledInput,
    McpServerInput,
    ParsedMcpServer,
    ProbeInput,
    ProbeResult,
    WorkspaceScopedServer,
    _format_validation_error,
    catalog_row_to_response,
    isolation_warnings,
    parse_mcp_servers_payload,
)
from src.server.services.mcp_catalog import (
    apply_catalog_edit,
    detach_warning,
    reject_reserved_catalog_name,
)
from src.server.services.mcp_import import ImportScope, run_mcp_import
from src.server.services.mcp_oauth.discovery import (
    SELF_HEAL_INTERVAL_S,
    RejectedHeaderValue,
    resolve_header_refs,
    schedule_catalog_discovery,
    vault_ref_names,
)
from src.server.services.mcp_probe import (
    bounded_probe,
    effective_secrets_for_probe,
    missing_secrets_result,
    probe_result,
    rejected_header_result,
)
from src.server.services.vault_invalidation import USER_TIER, after_secret_change
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])

async def oauth_for_server(user_id: str, name: str) -> dict | None:
    """One row's connection, for a response that is only ever about one row.

    Shaped like a row from the map below rather than handed over as the record
    it arrives as, so :func:`decorated` has one thing to read. Same swallow as
    the map too: the connection is decoration here, and losing it must not fail
    the write that just succeeded.
    """
    try:
        conn = await get_connection(user_id, name)
    except Exception:
        logger.warning(
            "[mcp_catalog] OAuth connection lookup failed for %s/%s", user_id, name,
            exc_info=True,
        )
        return None
    if conn is None:
        return None
    return {
        "status": conn.status,
        "server_url": conn.server_url,
        "granted_capabilities": conn.granted_capabilities,
    }


async def _oauth_by_server(user_id: str) -> dict[str, dict]:
    """server_name → its connection summary, for decorating catalog responses."""
    try:
        return {c["server_name"]: c for c in await list_connections(user_id)}
    except Exception:
        logger.warning(
            "[mcp_catalog] OAuth connection lookup failed for %s", user_id,
            exc_info=True,
        )
        return {}


def decorated(row: dict, conn: dict | None, **extra) -> CatalogServer:
    """A catalog response carrying what its OAuth connection knows.

    One helper for every path that has a connection in hand, because they all
    answer the same two questions about it, and a caller that reached for only
    the status is how consent stayed invisible after connecting.

    A connection that can no longer be served reports its status but not its
    capabilities. The stored keys outlive a revoke, and every reader treats a
    non-null list as the grant currently in force -- so passing them through
    drew a disconnected broker still badged as able to place live orders.
    Withholding them reads as "nothing connected here", which is the truth.

    The choice itself still travels, under its own name. Reconnecting is the
    only way to change a selection, so the dialog that opens on a needs_reauth
    or revoked row has to start from what the user last chose; seeding it from
    the grant instead meant a repair after a token expiry re-proposed every
    group they had declined. Two fields because they are two questions, and the
    reader that wanted the wrong one is how this went wrong in both directions.
    """
    if conn is None:
        return catalog_row_to_response(row, **extra)
    from src.server.services.brokerage_capabilities import (
        effective_capabilities,
        vendor_for_url,
    )

    status = ConnectionStatus(conn["status"])
    stored = conn["granted_capabilities"]
    return catalog_row_to_response(
        row,
        oauth_status=status,
        # In force rather than as stored, since every badge reads this: a group
        # whose requirement was not granted is refused, so it is not drawn.
        granted_capabilities=(
            list(effective_capabilities(vendor_for_url(conn.get("server_url")), stored))
            if status in SERVABLE and stored is not None
            else None
        ),
        remembered_capabilities=stored,
        **extra,
    )


def _has_direct_tools(row: dict, conn: dict | None, snapshot: dict | None) -> bool:
    """Whether any tool on this row binds directly, and so reaches Flash.

    Flash has no sandbox, so a row is reachable from it only through a tool on
    the direct path. Answered from the snapshot the list already holds rather
    than by asking per row, which is the observer behind every server the user
    owns that the rows deliberately do not carry.

    Read the same way ``resolve_mcp_config`` plans the row: a servable
    connection says which vendor's rules apply and what was consented to,
    while a row without one is judged by its own address, its own headers and
    consent to nothing. A revoked connection is history rather than a claim on
    the row, so it falls to that header path. A connection that needs repair
    answers no on its own; the sync binds no grant for it, so nothing it lists
    could be called.

    Intersected with what the snapshot actually published, because a plan
    carries every name the vendor's curation grants whether or not this server
    published it, while ``build_direct_entries`` can only bind a schema it
    holds. Reading ``plan.direct`` alone would offer Flash on a connection
    whose scope toggle saves cleanly and then gives Flash nothing to call.
    """
    from src.server.models.mcp_server import probe_ok
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.egress import fold_tool_name, folded
    from src.server.services.tool_binding import inputs_from_row, resolve_plan

    status = ConnectionStatus(conn["status"]) if conn is not None else None
    if status is ConnectionStatus.REVOKED:
        conn = None
    elif status is not None and status not in SERVABLE:
        return False
    # The flag has to agree with the grant the sync would write, and a header
    # row's grant hangs on this verdict alone (``grant_scope._probe_ok``): the
    # no-downgrade upsert keeps the tools and the ok status a refused probe
    # never took away.
    if conn is None and not probe_ok(snapshot):
        return False
    published = (snapshot or {}).get("tools") or []
    names = [name for t in published if (name := t.get("name"))]
    vendor_url = conn.get("server_url") if conn is not None else row.get("url")
    plan = resolve_plan(
        vendor_for_url(vendor_url),
        (conn.get("granted_capabilities") or ()) if conn is not None else (),
        inputs_from_row(row),
        candidates=names,
    )
    direct = folded(plan.direct)
    return any(fold_tool_name(n) in direct for n in names)


async def _snapshots_by_server(
    user_id: str, rows: list[dict]
) -> dict[str, dict]:
    """server_name → its snapshot under the CURRENT config, whatever its status.

    Hash-gated the way the workspace effective list is (``ToolSnapshotIndex``
    owns the rule), so a stale config's row is never read. Every status rather
    than only ``ok``: the probe's failure is a fact the page has to show, and
    it lives on the same row as the tools would. Callers that want tools check
    ``ok_snapshot`` themselves. The whole snapshot rather than one derived
    number, because the tool count, the auth verdict and the server's own
    identity are facts from the same row.

    Pure decoration: any failure degrades to no snapshots, never a 500.
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
    index = ToolSnapshotIndex(user_rows=schema_rows)
    accepted: dict[str, dict] = {}
    for row in rows:
        try:
            snapshot = index.snapshot(user_row_to_server_config(row))
        except Exception:  # noqa: BLE001 — malformed row: just omit it
            continue
        if snapshot is not None:
            accepted[row["name"]] = snapshot
    return accepted


def _kicked_recently(row: dict) -> bool:
    """Whether this row's probe clock was stamped inside the self-heal window.

    ``claim_probe_kick`` is still the guard every worker agrees on; this is the
    cheap pre-check in front of it, so a polling list does not spawn a task per
    remote row only to be told no.
    """
    stamp = row.get("probe_kicked_at")
    if not stamp:
        return False
    try:
        kicked = datetime.fromisoformat(stamp)
    except (TypeError, ValueError):
        return False
    if kicked.tzinfo is None:
        kicked = kicked.replace(tzinfo=UTC)
    return (datetime.now(UTC) - kicked).total_seconds() < SELF_HEAL_INTERVAL_S


def _kick_unprobed(user_id: str, rows: list[dict], snapshots: dict[str, dict]) -> None:
    """Self-heal: a live remote row nothing has probed under its current config
    gets its probe now.

    Rows from before host-side discovery covered them, rows whose snapshot
    predates the verdict column, rows whose kick was lost to a restart, and
    rows whose one probe never reached the server all land here. Only
    ``unreachable`` is retried: every other verdict is the server's own answer,
    and only a config or vault change earns a new one.
    """
    from src.server.models.mcp_server import snapshot_probe

    for row in rows:
        # An inert template is never dialled in the background, so a kick from
        # here would only spend a task and a throttle stamp to be told no by
        # the discovery pass. Its first probe comes from the switch.
        if not row.get("enabled") or row.get("plugin_enabled") is False:
            continue
        # The host-side probe dials streamable HTTP, so an ``sse`` row has no
        # path from here at all and keeps its in-sandbox discovery.
        if row.get("transport") != "http":
            continue
        probe = snapshot_probe(snapshots.get(row["name"]))
        unprobed = (
            row["name"] not in snapshots
            or probe is None
            or probe.verdict == "unreachable"
        )
        if unprobed and not _kicked_recently(row):
            schedule_catalog_discovery(
                user_id, row["name"], reason="self-heal", throttle=True
            )


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
    workspace-local server across the user's workspaces.

    Not a pure read: a remote row with no verdict gets its probe kicked here
    (stamping ``probe_kicked_at``), throttled per row, so a listing is what
    heals a row whose probe was lost."""
    rows = await list_catalog_servers(user_id)
    oauth = await _oauth_by_server(user_id)
    snapshots = await _snapshots_by_server(user_id, rows)
    _kick_unprobed(user_id, rows, snapshots)
    servers = []
    for r in rows:
        snapshot = snapshots.get(r["name"])
        meta = (snapshot or {}).get("observed_meta") or {}
        servers.append(
            decorated(
                r,
                oauth.get(r["name"]),
                icon_url=await icon_url(meta.get("server_info")),
                has_direct_tools=_has_direct_tools(r, oauth.get(r["name"]), snapshot),
                snapshot=snapshot,
            )
        )
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
    reject_reserved_catalog_name(server.name)
    try:
        row = await create_catalog_server(
            user_id, server.name, **server.to_catalog_fields()
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    # The probe runs now rather than at the user's next turn: the row's tool
    # count and its auth verdict are what the page shows next. Only for a row
    # that landed switched on: the background pass refuses an inert one, so
    # the enable toggle is what schedules the first probe for the rest, and a
    # kick here would spend the throttle stamp and leave the page reporting a
    # check nothing is making.
    if row.get("enabled"):
        schedule_catalog_discovery(user_id, server.name, reason="create")
    response = catalog_row_to_response(row)
    # A brand-new name has no connection, but a recreate over a name whose
    # connection row outlived the old catalog entry does.
    response.warnings = await _write_warnings(user_id, server)
    return response


# How often a probe in flight looks for the client that asked for it.
DISCONNECT_POLL_S = 0.5


async def _unless_gone(request: Request, coro):
    """Run ``coro`` until it answers or the caller hangs up, whichever first.

    The form aborts a superseded probe, but a dropped connection does not
    cancel a handler on its own, so without this every edit toward a slow
    address left the last probe holding a gate slot for its whole budget.
    """
    task = asyncio.ensure_future(coro)
    try:
        while not task.done():
            if await request.is_disconnected():
                task.cancel()
                # Settle the cancellation here so the gate slot is free by
                # the time this returns, not at some later tick.
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                raise HTTPException(status_code=499, detail="Client disconnected")
            await asyncio.wait({task}, timeout=DISCONNECT_POLL_S)
        return task.result()
    finally:
        if not task.done():
            task.cancel()


@router.post("/servers/probe")
@handle_api_exceptions("probe MCP server", logger)
async def probe_server(
    body: ProbeInput, request: Request, user_id: CurrentUserId
) -> ProbeResult:
    """Ask a remote address what it offers, with the headers the form holds,
    before anything is saved.

    Declared before ``/servers/{name}`` so the literal path wins the match.
    Nothing is written: the form shows the verdict and the save that follows
    schedules the discovery that caches it.
    """
    secrets = await effective_secrets_for_probe(
        user_id, body.workspace_id, vault_ref_names(body.headers)
    )
    try:
        headers, missing = resolve_header_refs(body.headers, secrets)
    except RejectedHeaderValue:
        # The same answer httpx would have produced one layer down, minus the
        # round trip and minus the value quoted back in the error it raises.
        return rejected_header_result()
    if missing:
        return missing_secrets_result(missing)
    outcome = await _unless_gone(request, bounded_probe(body.url, headers))
    return probe_result(outcome, include_tools=True)


@router.get("/servers/{name}")
@handle_api_exceptions("get MCP catalog server", logger)
async def get_server(name: str, user_id: CurrentUserId) -> CatalogServer:
    row = await get_catalog_server(user_id, name)
    if not row:
        raise HTTPException(status_code=404, detail="MCP server not found")
    oauth = await _oauth_by_server(user_id)
    snapshot = (await _snapshots_by_server(user_id, [row])).get(name)
    return decorated(row, oauth.get(name), snapshot=snapshot)


@router.get("/servers/{name}/tools")
@handle_api_exceptions("list MCP catalog server tools", logger)
async def get_server_tools(name: str, user_id: CurrentUserId) -> dict:
    """The discovered tool snapshot for one catalog server, hash-gated to its
    CURRENT config (``ToolSnapshotIndex`` owns the acceptance rule), so the
    detail view can never show tools a stale config produced. Rows are
    sanitized at cache-write time, so this is a plain projection.

    Deliberately not gated on ``enabled``: a disabled or plugin-suppressed row
    still shows its last-known tools, which is what makes the detail view
    useful for deciding whether to turn it back on, and the panel badges the
    suppression beside them. Delivery is decided in one place,
    ``list_enabled_user_servers``, and a catalog reader is not it."""
    from src.server.services.brokerage_capabilities import (
        group_of_tool,
        is_always_denied,
        order_modes,
        vendor_for_url,
    )
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import ToolSnapshotIndex
    from src.server.services.tool_binding import inputs_from_row

    row = await get_catalog_server(user_id, name)
    if not row:
        raise HTTPException(status_code=404, detail="MCP server not found")
    binding_inputs = inputs_from_row(row)
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
    # The vendor is the row's address, not its name: the name is the user's to
    # choose, and joining on it drew somebody's own row wearing a broker's
    # curation (or missed the curation of a row that had been repointed).
    _vendor = vendor_for_url(row.get("url"))
    return {
        "server_name": name,
        # Which order modes this brokerage has at all, so the page can offer
        # one approval switch per mode without keeping its own vendor table.
        "order_modes": [m.value for m in order_modes(_vendor)],
        "tools": [
            {
                "name": t.get("name", ""),
                "description": t.get("description", ""),
                "input_schema": t.get("input_schema") or {},
                # Which consent toggle reaches this tool, null when none does.
                # Discovery is deliberately unfiltered -- it is what the vendor
                # offers, not what this connection may call -- so the group is
                # the join the detail view needs to say which of them are
                # actually reachable and which the user declined.
                "capability": group_of_tool(_vendor, t.get("name", "")),
                # A null capability alone does not say whether the tool can be
                # called: one we deliberately withheld is refused at every
                # grant, one we simply have not classified passes at every
                # grant. The client cannot tell them apart and drew both as
                # unreachable, so the distinction travels.
                "always_denied": is_always_denied(_vendor, t.get("name", "")),
                # Which path the tool takes to the model if consent lets it
                # through, which layer decided, and which paths it may take
                # at all. Independent of consent on purpose: the column shows
                # what a grant would put in force.
                **_binding_fields(_vendor, t.get("name", ""), binding_inputs),
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
    # The snapshot outlives an edit that leaves the discovery fingerprint
    # alone, and the page reads the row it gets back: without it the tool count
    # and the probe verdict blank out on every save that changed nothing about
    # discovery. An edit that DID move the fingerprint misses here, which is
    # the right answer -- the rediscovery the edit schedules owns the refill.
    response = catalog_row_to_response(
        edit.row, snapshot=(await _snapshots_by_server(user_id, [edit.row])).get(name)
    )
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

    landed_enabled: set[str] = set()

    async def persist(
        conn, server: McpServerInput, entry: ParsedMcpServer
    ) -> bool:
        # No ON CONFLICT arm here — a raced duplicate raises ValueError, so a
        # successful call always means "created".
        row = await create_catalog_server(
            user_id, server.name, conn=conn, **server.to_catalog_fields()
        )
        if row.get("enabled"):
            landed_enabled.add(server.name)
        return True

    existing_names = {r["name"] for r in await list_catalog_servers(user_id)}
    report = await run_mcp_import(
        parsed,
        scope=ImportScope(
            reserved_names=reserved_catalog_names(),
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
    # After the secrets landed, so a row whose header refs one of them probes
    # with the value rather than a missing-secret error. Only for a row that
    # landed switched on: the background pass refuses an inert one, so the
    # enable toggle is what schedules the first probe for the rest.
    for result in report.results:
        if result.get("status") == "created" and result["name"] in landed_enabled:
            schedule_catalog_discovery(user_id, result["name"], reason="import")

    return {
        "results": report.results,
        "created": report.created,
        "secrets_created": report.secrets_created,
        "config_version": 0,
    }


async def _relay_execution_warning(user_id: str, name: str) -> str | None:
    """A row the relay has to carry runs only through it, and activation is the
    moment to tell the user their deployment cannot actually run it.

    Two shapes need the warning: an OAuth connection, whose token only the
    relay spends, and an ``http`` row with a tool on the direct path, which the
    model reaches by dialing the relay too. The second is read off the row's
    own binding map rather than its probe verdict: over-warning a row that is
    failing its probe costs a sentence, while waiting for a verdict would keep
    the warning from the silent case it exists for, where the sync binds
    nothing and the tool is gone from both agents.
    """
    from src.config.env import EGRESS_RELAY_SECRET
    from src.server.app import setup
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.egress.reachability import (
        effective_relay_base_url,
        relay_reachability_warning,
    )
    from src.server.services.tool_binding import inputs_from_row, resolve_plan

    if setup.agent_config is None:
        return None
    connection = await get_connection(user_id, name)
    # A revoked connection is history, not a claim on the row (the rule the
    # resolver and the relay apply), so the row is judged by its own binding.
    if connection is None or connection.status is ConnectionStatus.REVOKED:
        row = await get_catalog_server(user_id, name)
        if row is None or row.get("transport") != "http":
            return None
        # Planned off the row's own address and consent to nothing, the way
        # every other reader plans a row that has no connection.
        plan = resolve_plan(vendor_for_url(row.get("url")), (), inputs_from_row(row))
        if not plan.direct:
            return None
    if not EGRESS_RELAY_SECRET:
        return (
            "The egress relay is disabled (EGRESS_RELAY_SECRET is not set), so "
            "this server's tools cannot run in sandboxes. Set a strong "
            "EGRESS_RELAY_SECRET in the backend environment and restart."
        )
    provider = setup.agent_config.sandbox.provider
    return relay_reachability_warning(provider, effective_relay_base_url(provider))


async def apply_catalog_enabled(
    user_id: str, name: str, enabled: bool
) -> tuple[dict | None, str | None]:
    """The one place a *switch* flips a catalog row. Returns the row and
    whatever the user is owed about it, or ``(None, None)`` if it is gone.

    Every switch routes through here so no caller can end up with half of what
    another does: the DB layer bumps every workspace's ``mcp_config_version``
    in the same transaction (next-acquire convergence), and disable also has to
    bite now rather than at next acquire, which ``revoke_live_grants`` carries
    the reasoning for.

    The column itself has other writers — promoting a workspace fork, and the
    disable an edit does before rewriting a row. They reach the DB toggle
    directly and mean to: neither is a user flipping a switch, so neither owes
    a relay warning, and the edit path is mid-transaction when it runs.
    """
    from src.server.services.mcp_oauth.lifecycle import revoke_live_grants

    row = await set_catalog_server_enabled(user_id, name, enabled)
    if row is None:
        return None, None
    if enabled:
        return row, await _relay_execution_warning(user_id, name)
    await revoke_live_grants(user_id, [name])
    return row, None


def _binding_fields(vendor: str | None, tool: str, inputs) -> dict:
    """The effective path, which layer chose it, which paths the row may pick
    from, and what the call does to an order, so the page offers exactly the
    options the write path accepts rather than keeping its own copy of the
    policy."""
    from src.server.services.tool_binding import order_payload, resolve_tool

    resolved = resolve_tool(vendor, tool, inputs)
    return {
        "binding": resolved.binding,
        "binding_source": resolved.source,
        "allowed": sorted(resolved.allowed),
        "approval": resolved.approval,
        "order": order_payload(resolved.order),
    }


@router.patch("/servers/{name}/binding")
@handle_api_exceptions("set MCP catalog server binding", logger)
async def set_binding(
    name: str, body: BindingInput, user_id: CurrentUserId
) -> CatalogServer:
    """Change how this row's tools reach the model.

    Not a PUT: the map is policy, not connection config, so it neither forks
    the row off its plugin nor revokes its OAuth connection. The grants in
    force are rewritten in the same breath as the row, the way a consent
    change is, because the relay reads the grant and the model is already
    running on the previous answer.

    The body names the tools it changes rather than carrying the map. Merging
    here, inside the lock that already serializes writers, is what keeps two
    tabs editing different tools of one row from overwriting each other.
    """
    from src.server.database.egress_grants import (
        apply_binding_to_active_header_grants,
        apply_consent_to_active_grants,
        lock_user_egress_state,
    )
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.mcp_discovery import MAX_TOOLS_PER_SERVER
    from src.server.services.tool_binding import (
        merge_overrides,
        order_approval_overrides,
        strip_disallowed_overrides,
        validate_overrides,
    )

    updates: dict = {}
    delta = body.tool_binding_set is not None or body.tool_binding_unset is not None
    if "binding_preset" in body.model_fields_set:
        updates["binding_preset"] = body.binding_preset
    if not delta and not updates and body.order_approval is None:
        raise HTTPException(status_code=422, detail="nothing to change")

    # Only a servable connection's address says which vendor's rules apply: a
    # revoked one may belong to the host the row used to point at.
    connection = await get_connection(user_id, name)

    # One transaction, and the row is read inside it under the user's egress
    # lock: the stored map is both what this request is judged against and
    # what healing rewrites, so a read taken before the lock lets a write that
    # landed in between be put back to the version this worker saw. The grant
    # is rewritten in the same breath as the row because the relay reads the
    # grant and the model is already running on the previous answer.
    async with get_db_connection() as db, db.transaction():
        await lock_user_egress_state(db, user_id)
        row = await get_catalog_server(user_id, name, conn=db)
        if not row:
            raise HTTPException(status_code=404, detail="MCP server not found")
        stored = row.get("tool_binding") or {}
        # Merged under the same lock as the map, and for the same reason: the
        # body names the modes it changes, so a page flipping live cannot put
        # another tab's paper answer back to what this worker last read. Only
        # the modes someone set are stored, so the rest follow their defaults.
        if body.order_approval is not None:
            updates["order_approval"] = {
                **order_approval_overrides(row.get("order_approval")),
                **{k: bool(v) for k, v in body.order_approval.items()},
            }
        # The relay dials streamable HTTP, so only an ``http`` row has an
        # address it can reach: a legacy ``sse`` row keeps its sandbox
        # discovery and never earns a grant, so no tool on it can take the
        # direct path however the request or its group is worded.
        relayable = row.get("transport") == "http"
        vendor = vendor_for_url(
            connection.server_url
            if connection is not None and connection.status in SERVABLE
            else row.get("url")
        )
        # Validate what this request asks for. A path the tool's group does
        # not allow is refused rather than stored and then overruled at
        # resolve time. The delta is exactly this request's doing, so nothing
        # a previous write left in the row is judged again here.
        requested = dict(body.tool_binding_set or {})
        if delta:
            reason = validate_overrides(
                vendor, requested, stored=stored, relayable=relayable
            )
            if reason:
                raise HTTPException(status_code=422, detail=reason)
        merged = (
            merge_overrides(stored, set_=requested, unset=body.tool_binding_unset)
            if delta
            else stored
        )
        # Store the map with anything the clamp overrules stripped, so an
        # entry that got in before the clamp did leaves on the next write
        # instead of sitting under the resolver's ``policy`` answer forever.
        healed = strip_disallowed_overrides(vendor, merged, relayable)
        # The stored map is what every later resolve expands and what the grant
        # rows carry, and an override for a tool the server never published is
        # kept rather than dropped, because a row can be pointed at a server
        # whose tool list arrives later. Bounding it here is what stops a run of
        # deltas from growing one no server could ever match. Judged after the
        # merge and only when this request grew the map, so a row already over
        # the line can still be edited down.
        if len(healed) > MAX_TOOLS_PER_SERVER and len(healed) > len(stored):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"a row may hold at most {MAX_TOOLS_PER_SERVER} tool bindings"
                ),
            )
        if delta or healed != stored:
            updates["tool_binding"] = healed

        updated = await update_catalog_server(user_id, name, updates=updates, conn=db)
        if updated is None:
            raise HTTPException(status_code=404, detail="MCP server not found")
        if connection is not None:
            await apply_consent_to_active_grants(connection.connection_id, conn=db)
        # And the row's own grants: a header grant is keyed by name and carries
        # no connection, so the rewrite above never reaches one.
        await apply_binding_to_active_header_grants(user_id, name, conn=db)
    oauth = await _oauth_by_server(user_id)
    # A binding change moves no discovery fingerprint, so the row's snapshot is
    # still its own; dropping it here blanked the tool count and the verdict on
    # every path toggle.
    snapshot = (await _snapshots_by_server(user_id, [updated])).get(name)
    return decorated(updated, oauth.get(name), snapshot=snapshot)


@router.patch("/servers/{name}/enabled")
@handle_api_exceptions("toggle MCP catalog server", logger)
async def set_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Flip a user server live/inert."""
    row, warning = await apply_catalog_enabled(user_id, name, body.enabled)
    if row is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
    if body.enabled and row.get("transport") == "http":
        # A row going live is about to be inherited by every workspace; the
        # snapshot under its current config, if any, is reused, and a missing
        # one is fetched now. Unthrottled, because this is usually the row's
        # FIRST probe: nothing dials an inert template, so the kick its create
        # or its import spent left a ``probe_kicked_at`` stamp and no verdict,
        # and a throttled kick would be refused by that stamp and leave the row
        # blank for the rest of the self-heal window. A toggle flipped twice
        # costs one extra dial at most, and only until a verdict lands: a
        # settled snapshot of any status stops this branch.
        snapshot = (await _snapshots_by_server(user_id, [row])).get(name)
        if snapshot is None:
            schedule_catalog_discovery(user_id, name, reason="enable")
    out: dict = {"name": name, "enabled": body.enabled}
    if warning:
        out["warnings"] = [warning]
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

