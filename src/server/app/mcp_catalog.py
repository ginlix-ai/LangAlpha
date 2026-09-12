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
- PATCH  /api/v1/mcp/servers/{name}/binding
- DELETE /api/v1/mcp/servers/{name}
- GET    /api/v1/mcp/builtin-servers
- GET    /api/v1/mcp/builtin-servers/{name}/tools
- PATCH  /api/v1/mcp/builtin-servers/{name}/enabled
- GET    /api/v1/mcp/brokerages
- PATCH  /api/v1/mcp/brokerages/{name}/enabled
- GET    /api/v1/mcp/brokerages/{name}/icon
- GET    /api/v1/mcp/server-icons/{handle}
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, HTTPException, Response
from pydantic import ValidationError

from src.server.database.mcp_oauth import (
    SERVABLE,
    ConnectionStatus,
    get_connection,
    list_connections,
)
from src.server.services.brand_icons import (
    icon_response,
    icon_response_for_handle,
    publish_icon_source,
)
from src.server.services.mcp_identity import icon_source
from src.server.services.plugins.bundled import component_owners
from src.server.services.brokerages import (
    BROKERAGES,
    Brokerage,
    brokerage_by_name,
)
from src.server.services.mcp_config import builtin_names, reserved_catalog_names
from src.server.database.account_disables import (
    list_account_disables,
    set_account_disable,
)
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
    BrokerageList,
    BuiltinServer,
    BuiltinServerList,
    CatalogServer,
    CatalogServerList,
    EnabledInput,
    McpServerInput,
    WorkspaceScopedServer,
    _format_validation_error,
    brokerage_to_response,
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
from src.server.services.vault_invalidation import USER_TIER, after_secret_change
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])

async def _oauth_for_server(user_id: str, name: str) -> dict | None:
    """One row's connection, for a response that is only ever about one row.

    Shaped like a row from the map below rather than handed over as the record
    it arrives as, so :func:`_decorated` has one thing to read. Same swallow as
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


def _decorated(row: dict, conn: dict | None, **extra) -> CatalogServer:
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

    Intersected with what the snapshot actually published, because a plan
    carries every name the vendor's curation grants whether or not this server
    published it, while ``build_direct_entries`` can only bind a schema it
    holds. Reading ``plan.direct`` alone would offer Flash on a connection
    whose scope toggle saves cleanly and then gives Flash nothing to call.
    """
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.egress import folded_contains
    from src.server.services.tool_binding import inputs_from_row, resolve_plan

    if conn is None or ConnectionStatus(conn["status"]) not in SERVABLE:
        return False
    published = (snapshot or {}).get("tools") or []
    names = [name for t in published if (name := t.get("name"))]
    plan = resolve_plan(
        vendor_for_url(conn.get("server_url")),
        conn.get("granted_capabilities") or (),
        inputs_from_row(row),
        candidates=names,
    )
    return any(folded_contains(plan.direct, n) for n in names)


async def _snapshots_by_server(
    user_id: str, rows: list[dict]
) -> dict[str, dict]:
    """server_name → its accepted snapshot, hash-gated to the CURRENT config.

    Same acceptance rule as the workspace effective list (``ToolSnapshotIndex``
    owns it), so everything read off it here matches what workspaces serve. The
    whole snapshot rather than one derived number, because the tool count and
    the server's own identity are two facts from the same accepted row and a
    second reducer would re-litigate the acceptance rule to find them.

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
            snapshot = index.ok(user_row_to_server_config(row))
        except Exception:  # noqa: BLE001 — malformed row: just omit it
            continue
        if snapshot is not None:
            accepted[row["name"]] = snapshot
    return accepted


async def _icon_url(server_info: dict | None) -> str | None:
    """This origin's path to the mark a server's handshake named, if any."""
    source = icon_source(server_info)
    if source is None:
        return None
    handle = await publish_icon_source(source)
    return None if handle is None else f"/api/v1/mcp/server-icons/{handle}"


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
    oauth = await _oauth_by_server(user_id)
    snapshots = await _snapshots_by_server(user_id, rows)
    servers = []
    for r in rows:
        snapshot = snapshots.get(r["name"])
        meta = (snapshot or {}).get("observed_meta") or {}
        servers.append(
            _decorated(
                r,
                oauth.get(r["name"]),
                tool_count=(
                    len(snapshot.get("tools") or []) if snapshot is not None else None
                ),
                icon_url=await _icon_url(meta.get("server_info")),
                has_direct_tools=_has_direct_tools(r, oauth.get(r["name"]), snapshot),
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
    oauth = await _oauth_by_server(user_id)
    return _decorated(row, oauth.get(name))


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


async def _apply_catalog_enabled(
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
        # A stdio row has no address for the relay, so no tool on it can take
        # the direct path however the request or its group is worded.
        relayable = row.get("transport") != "stdio"
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
    oauth = await _oauth_by_server(user_id)
    return _decorated(updated, oauth.get(name))


@router.patch("/servers/{name}/enabled")
@handle_api_exceptions("toggle MCP catalog server", logger)
async def set_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    """Flip a user server live/inert."""
    row, warning = await _apply_catalog_enabled(user_id, name, body.enabled)
    if row is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
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
                icon_url=await _icon_url(
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
    """Account-wide toggle for a builtin — applies to every workspace of the
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


# ---------------------------------------------------------------------------
# Brokerages — shipped connectors, off until the user turns one on
# ---------------------------------------------------------------------------


@router.get("/brokerages")
@handle_api_exceptions("list brokerage connectors", logger)
async def list_brokerages(user_id: CurrentUserId) -> BrokerageList:
    """The brokerage connectors this build ships.

    Static and user-independent: whether one is configured is answered by the
    catalog list, which the page already holds and joins on ``name``. Behind
    the same auth as the rest of the router regardless: its only reader is the
    page, which is holding a token already, so there is nothing an exception
    here would buy.
    """
    return BrokerageList(brokerages=[brokerage_to_response(b) for b in BROKERAGES])


async def _create_brokerage_row(user_id: str, brokerage: Brokerage) -> None:
    """Bring a shipped brokerage into the user's catalog, inert.

    Inert and then toggled, never created live: the switch is the only thing
    that should decide a row's enabled state, and it is the one that already
    knows what each direction owes an OAuth connection.
    """
    if brokerage.name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{brokerage.name!r} collides with a built-in server name",
        )
    try:
        # Through the same validator every user-written row passes, so our own
        # definition cannot be the one payload that skips the URL policy. Its
        # ValidationError is a ValueError, so it answers here rather than
        # escaping the decorator as an untyped 500.
        server = McpServerInput(
            name=brokerage.name,
            transport="http",
            url=brokerage.url,
            description=brokerage.description,
        )
        await create_catalog_server(user_id, server.name, **server.to_catalog_fields())
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    logger.info(
        "[mcp_catalog] brokerage %s configured for user %s", brokerage.name, user_id
    )


@router.patch("/brokerages/{name}/enabled")
@handle_api_exceptions("toggle brokerage connector", logger)
async def set_brokerage_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> CatalogServer:
    """Turn a shipped brokerage on or off, creating its row the first time.

    One route for both, so the page never has to know whether a row exists yet
    — which also keeps it from being the thing that chooses the endpoint URL.

    An existing row is toggled and never rewritten. Once it is the user's, its
    URL is theirs to edit, and a row they built themselves under this name is
    still theirs; silently restoring our address on every enable would undo a
    deliberate edit at the moment they were only reaching for the switch.
    """
    brokerage = brokerage_by_name(name)
    if brokerage is None:
        raise HTTPException(status_code=404, detail="Unknown brokerage")

    existing = await get_catalog_server(user_id, name)
    # A plugin-owned row under a brokerage name is not the user's own edit, and
    # this route would adopt it and hand it the vendor's identity: the tab joins
    # by name, so it would be presented as this broker while Connect went to
    # whatever address the plugin chose. New installs cannot claim these names
    # any more; one installed before they were reserved still can, so refuse it
    # here rather than trusting that no such row exists. The row stays usable
    # on the Connectors tab, under the plugin that owns it.
    if existing and existing.get("plugin_id"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"{name!r} is a server installed by a plugin, so it cannot be "
                "managed as a brokerage connector. Open it on the Connectors tab."
            ),
        )

    if existing is None:
        if not body.enabled:
            raise HTTPException(
                status_code=404,
                detail=f"{name!r} is not configured, so there is nothing to disable",
            )
        await _create_brokerage_row(user_id, brokerage)

    # The same apply every other switch on this page goes through. These are the
    # rows that can place orders, so a weaker disable than the server beside them
    # is the last thing they should have.
    row, warning = await _apply_catalog_enabled(user_id, name, body.enabled)
    if row is None:
        # Deleted between the read and the write.
        raise HTTPException(status_code=404, detail="MCP server not found")

    # A recreate over a name whose OAuth connection outlived the old row is
    # already connected, so read the status rather than assuming none. One row,
    # so one lookup: listing every connection to decorate a single response is
    # a second round trip that answers the same question.
    response = _decorated(row, await _oauth_for_server(user_id, name))
    if warning:
        response.warnings = [warning]
    return response


@router.get("/server-icons/{handle}")
async def get_server_icon(handle: str) -> Response:
    """The mark an MCP server declared for itself, proxied.

    Unauthenticated for the same reason the brokerage route is: an ``<img>``
    cannot carry a bearer token, and there is nothing here to authenticate
    anyway. The handle is the whole access control. It is minted only while
    listing a user's own servers, so the set of resolvable sources is exactly
    the set some user's server declared, and a route that took the URL outright
    would fetch whatever any caller named.

    What comes back is bytes we fetched, never a redirect to the server's own
    address: resolving on the host means one fetch serves everyone, instead of
    every render of the page telling a third party who is looking.
    """
    return await icon_response_for_handle(handle)


@router.get("/brokerages/{name}/icon")
async def get_brokerage_icon(name: str) -> Response:
    """The broker's own logo, proxied from their site.

    Unauthenticated because it has nothing to authenticate: the only input is
    a name this build ships, so the answer is the same public logo for every
    caller and no user's configuration is read to produce it. Serving it under
    the user's bearer token was never an option anyway — an ``<img>`` cannot
    send one, and routing brand art through a fetch-to-blob just to carry a
    credential that guards nothing is machinery for its own sake.

    404 is a normal answer, not an error: a vendor may simply have no usable
    mark, and the row draws its monogram instead. It carries a cache header so
    a page full of rows does not re-ask on every render.
    """
    brokerage = brokerage_by_name(name)
    return await icon_response(brokerage.site if brokerage else None)
