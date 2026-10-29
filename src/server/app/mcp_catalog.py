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
- PATCH  /api/v1/mcp/servers/{name}/new-workspaces
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
from functools import partial

from fastapi import APIRouter, Body, HTTPException, Request

from src.server.database.mcp_oauth import SERVABLE, get_connection
from src.server.database.mcp_servers import (
    MAX_CATALOG_SERVERS_PER_USER,
    create_catalog_server,
    delete_catalog_server,
    get_catalog_server,
    list_catalog_servers,
    list_scope_markers_for_user,
    set_catalog_server_new_workspace_default,
    update_catalog_server,
)
from src.server.database.mcp_tool_schemas import get_user_tool_schemas
from src.server.database.pool import get_db_connection
from src.server.models.mcp_server import (
    BindingInput,
    CatalogServer,
    CatalogServerList,
    EnabledInput,
    McpServerInput,
    ParsedMcpServer,
    ProbeInput,
    ProbeResult,
    catalog_row_to_response,
    parse_mcp_servers_payload,
)
from src.server.services.mcp_catalog import apply_catalog_enabled, detach_warning
from src.server.services.mcp_catalog_view import (
    catalog_write_warnings,
    decorate_row,
    decorate_rows,
    snapshot_of,
)
from src.server.services.mcp_import import catalog_import_scope, run_mcp_import
from src.server.services.mcp_oauth.discovery import schedule_catalog_discovery
from src.server.services.mcp_probe import probe_configured, probe_result
from src.server.services.mcp_saves import CatalogAdd, ServerEdit, parse_server
from src.server.services.mcp_validation import check_servers
from src.server.services.vault_invalidation import after_secrets_changed
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])

@router.get("/servers")
@handle_api_exceptions("list MCP catalog servers", logger)
async def list_servers(
    user_id: CurrentUserId, all_scopes: bool = False
) -> CatalogServerList:
    """The user's catalog; ``all_scopes`` adds the scope-management inventory:
    per-server tombstone workspaces (the "active in" deny-list).

    Not a pure read: a remote row with no verdict gets its probe kicked here
    (stamping ``probe_kicked_at``), throttled per row, so a listing is what
    heals a row whose probe was lost."""
    servers = await decorate_rows(user_id, await list_catalog_servers(user_id))
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
    return CatalogServerList(
        servers=servers,
        max_servers=MAX_CATALOG_SERVERS_PER_USER,
    )


@router.post("/servers", status_code=201)
@handle_api_exceptions("create MCP catalog server", logger)
async def create_server(
    user_id: CurrentUserId, body: dict = Body(...)
) -> CatalogServer:
    save = CatalogAdd(user_id, parse_server(body))
    await save.refuse()
    # Nothing is saved that has not answered: a failed check is the response
    # (422, or 409 when a sign-in is what saves it), and no row is left behind.
    await save.check()
    row = await save.write()
    server = save.server
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
    response.warnings = await catalog_write_warnings(user_id, server)
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
    Advisory: nothing is written, and the save runs its own check whatever
    this one said, so the form's verdict is a preview of that answer.
    """
    outcome = await _unless_gone(
        request, probe_configured(user_id, body.url, body.headers)
    )
    return probe_result(outcome, include_tools=True)


@router.get("/servers/{name}")
@handle_api_exceptions("get MCP catalog server", logger)
async def get_server(name: str, user_id: CurrentUserId) -> CatalogServer:
    row = await get_catalog_server(user_id, name)
    if not row:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return await decorate_row(user_id, row)


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
    save = ServerEdit(user_id, name, parse_server(body))
    await save.refuse()
    edit = await save.write()
    server = save.server
    # The snapshot outlives an edit that leaves the discovery fingerprint
    # alone, and the page reads the row it gets back: without it the tool count
    # and the probe verdict blank out on every save that changed nothing about
    # discovery. An edit that DID move the fingerprint misses here, which is
    # the right answer -- the rediscovery the edit schedules owns the refill.
    response = catalog_row_to_response(
        edit.row, snapshot=await snapshot_of(user_id, edit.row)
    )
    # After the revoke inside the edit, so one that just severed the connection
    # does not warn about headers it has now made effective.
    response.warnings = await catalog_write_warnings(user_id, server)
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
    credentials auto-extracted into the user's vault, every entry checked
    before it lands) with one deliberate difference: imported rows land
    ``enabled=false`` (inert templates), so an import never silently changes
    every workspace's toolset. The UI nudges the user to flip each one live.
    """
    parsed = parse_mcp_servers_payload(body)
    if not parsed:
        raise HTTPException(
            status_code=422,
            detail='No MCP servers found. Expected a JSON object like '
            '{"mcpServers": { "<name>": { ... } }}.',
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

    report = await run_mcp_import(
        parsed,
        scope=await catalog_import_scope(
            user_id,
            existing_names={r["name"] for r in await list_catalog_servers(user_id)},
            persist=persist,
            exists_message="already exists in your Plugins",
            check=partial(check_servers, user_id),
        ),
    )

    # The imported SERVERS land disabled (inert), so they need no fan-out; the
    # imported SECRETS do. One can complete a ``${vault:NAME}`` ref that an
    # already-enabled connector has been dangling on, and nothing else in this
    # path purges its snapshot, bumps the version, or pushes to a live sandbox.
    # One fan-out for the batch, not one vault push per secret.
    await after_secrets_changed(user_id, report.secrets_created)
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
    from src.server.database.order_attempts import refuse_unasked_attempts
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.mcp_discovery import MAX_TOOLS_PER_SERVER
    from src.server.services.tool_binding import (
        inputs_from_row,
        merge_overrides,
        order_approval_refusal,
        order_approval_to_store,
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
        # the modes someone set are stored, and live or staged only while they
        # ask, so the rest follow the level.
        if body.order_approval is not None:
            refusal = order_approval_refusal(
                body.order_approval, inputs_from_row(row).trading
            )
            if refusal:
                raise HTTPException(status_code=422, detail=refusal)
            updates["order_approval"] = order_approval_to_store(
                row.get("order_approval"), body.order_approval
            )
            if turned_on := [mode for mode, asks in body.order_approval.items() if asks]:
                await refuse_unasked_attempts(user_id, turned_on, server=name, conn=db)
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
    # A binding change moves no discovery fingerprint, so the row's snapshot is
    # still its own; dropping it here blanked the tool count and the verdict on
    # every path toggle.
    return await decorate_row(user_id, updated)


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
        if await snapshot_of(user_id, row) is None:
            schedule_catalog_discovery(user_id, name, reason="enable")
    out: dict = {"name": name, "enabled": body.enabled}
    if warning:
        out["warnings"] = [warning]
    return out


@router.patch("/servers/{name}/new-workspaces")
@handle_api_exceptions("set MCP server default for new workspaces", logger)
async def set_new_workspace_default(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Choose whether workspaces created from now on start with this server on.

    Each existing workspace keeps its own switch, so nothing re-resolves.
    """
    row = await set_catalog_server_new_workspace_default(user_id, name, body.enabled)
    if row is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return {
        "name": name,
        "enabled_in_new_workspaces": row["enabled_in_new_workspaces"],
    }


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

