"""Database CRUD for per-workspace and user-level MCP server configuration.

Two concerns live here:
- User-level servers (``user_mcp_servers``): CRUD by ``(user_id, name)``, the
  only place a server is defined. ``enabled`` rows are LIVE config inherited by
  every workspace of the user at resolve time; disabled rows are inert
  templates. Any write of an enabled row, its create included, fans out a
  version bump to ALL the user's workspaces in the same transaction, and
  convergence is next-acquire.
  ``enabled_in_new_workspaces`` is the exception: it reaches only workspaces
  that do not exist yet.
- Per-workspace rows (``workspace_mcp_servers``): selection only, a tombstone
  that switches an inherited server off in one workspace or a marker that
  switches a built-in off. EVERY write bumps ``workspaces.mcp_config_version``
  in the SAME transaction so sessions can detect drift on their next acquire,
  except the tombstones a workspace starts with: they commit with its INSERT,
  before any session could have resolved it.

The discovery schema cache for both tiers lives in ``mcp_tool_schemas``.

Secrets are never stored here — env/header values hold ``${vault:NAME}``
references resolved against the user's vault inside the sandbox.
"""

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.mcp_config_version import (
    bump_user_versions,
    bump_workspace_version,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_lock import lock_user_writes

logger = logging.getLogger(__name__)

# Hard cap on catalog templates per user.
#
# A catalog row costs a row and a line on the settings page until it is
# switched on. Enabling is what makes it live,
# and it is not a per-workspace act: ``list_enabled_user_servers`` inherits
# every enabled row into every one of the user's workspaces, where it pays for
# discovery and, on stdio, a subprocess. So this bounds what may be COLLECTED,
# and the ceiling on what runs is however many of them the user switches on.
MAX_CATALOG_SERVERS_PER_USER = 100

# Mutable catalog columns, split by how a value binds. Anything outside the
# union is rejected by ``update_catalog_server`` rather than silently dropped.
_CATALOG_JSONB_COLUMNS = frozenset({
    "args", "env", "headers", "tool_binding", "order_approval",
})
_CATALOG_SCALAR_COLUMNS = frozenset({
    "transport", "command", "url", "description", "instruction",
    "tool_exposure_mode", "discovery_uses_secrets",
})

# How the row's tools reach the model. Writable through the binding endpoint
# only and kept OUT of ``CATALOG_COLUMNS``: a PUT replaces the connection
# config whole, and a form that never showed these must not reset them.
_CATALOG_BINDING_COLUMNS = frozenset({
    "tool_binding", "binding_preset", "order_approval",
})

CATALOG_COLUMNS = (
    _CATALOG_JSONB_COLUMNS - _CATALOG_BINDING_COLUMNS
) | _CATALOG_SCALAR_COLUMNS

# Plugin provenance is writable too, but stays OUT of ``CATALOG_COLUMNS``:
# that set is what a request body binds against, so ownership can never be
# smuggled in from the wire. The catalog-edit service is the only caller that
# names these, and only to clear them.
_CATALOG_PROVENANCE_COLUMNS = frozenset({"plugin_id", "plugin_server_key"})
_WRITABLE_CATALOG_COLUMNS = (
    CATALOG_COLUMNS | _CATALOG_PROVENANCE_COLUMNS | _CATALOG_BINDING_COLUMNS
)


# ---------------------------------------------------------------------------
# User-level catalog (templates)
# ---------------------------------------------------------------------------


# The catalog SELECT list, qualified for the plugin LEFT JOIN. Projection
# only — catalog readers must keep returning plugin-disabled rows (cap
# counting, secret redaction, vault invalidation, and the OAuth lifecycle
# all need to see them); the delivery filter lives solely on
# ``list_enabled_user_servers``.
_CATALOG_SELECT = """
    SELECT s.user_mcp_server_id, s.user_id, s.name, s.transport, s.command,
           s.args, s.url, s.env, s.headers, s.description, s.instruction,
           s.tool_exposure_mode, s.discovery_uses_secrets, s.enabled,
           s.enabled_in_new_workspaces,
           s.tool_binding, s.binding_preset, s.order_approval,
           s.probe_kicked_at,
           s.created_at, s.updated_at, s.plugin_id, s.plugin_server_key,
           p.name AS plugin_name, p.enabled AS plugin_enabled,
           tp.level AS trading_level,
           tp.agreement_version AS trading_agreement_version
    FROM user_mcp_servers s
    LEFT JOIN user_plugins p ON p.user_plugin_id = s.plugin_id
    LEFT JOIN trading_permissions tp ON tp.user_id = s.user_id
"""


async def _read_catalog_row(cur, user_id: str, name: str) -> dict[str, Any] | None:
    """Re-read a catalog row through ``_CATALOG_SELECT``, on the caller's cursor.

    Every writer returns its row this way instead of listing columns in its own
    RETURNING clause: RETURNING cannot join, so the plugin display fields would
    come back None and ``plugin_name is None`` would mean either "no owner" or
    "the writer could not say". Inside the writer's transaction the re-read sees
    its own uncommitted write, which keeps the joined shape the only shape any
    caller ever handles.
    """
    await cur.execute(
        _CATALOG_SELECT + "WHERE s.user_id = %s AND s.name = %s",
        (user_id, name),
    )
    return await cur.fetchone()


async def list_catalog_servers(user_id: str) -> list[dict[str, Any]]:
    """List all catalog templates for a user, ordered by name."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _CATALOG_SELECT + "WHERE s.user_id = %s ORDER BY s.name",
                (user_id,),
            )
            return [_catalog_row_to_dict(r) for r in await cur.fetchall()]


async def get_catalog_server(
    user_id: str,
    name: str,
    *,
    conn=None,
    for_share: bool = False,
    for_update: bool = False,
) -> dict[str, Any] | None:
    """Return a single catalog template by name, or None.

    ``for_share`` locks the row so concurrent edits block until the caller's
    transaction ends — pass ``conn`` with it so a snapshot write can fence on
    the config still being the one it read. ``for_update`` is that fence plus
    exclusion between the fencing readers themselves, which is what a writer
    needs when what it writes depends on what it read; the two are mutually
    exclusive because a caller wanting both wants ``for_update``.
    """
    if for_share and for_update:
        raise ValueError("for_share and for_update are mutually exclusive")
    lock = (
        " FOR UPDATE OF s" if for_update
        else " FOR SHARE OF s" if for_share
        else ""
    )
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _CATALOG_SELECT + "WHERE s.user_id = %s AND s.name = %s" + lock,
                (user_id, name),
            )
            row = await cur.fetchone()
            return _catalog_row_to_dict(row) if row else None


async def create_catalog_server(
    user_id: str,
    name: str,
    *,
    transport: str = "stdio",
    command: str | None = None,
    args: list[str] | None = None,
    url: str | None = None,
    env: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    description: str = "",
    instruction: str = "",
    tool_exposure_mode: str = "summary",
    discovery_uses_secrets: bool = False,
    enabled: bool = False,
    enabled_in_new_workspaces: bool = True,
    plugin_id: str | None = None,
    plugin_server_key: str | None = None,
    conn=None,
) -> dict[str, Any]:
    """Insert a catalog template. Raises ValueError on duplicate name or over cap.

    Enforces ``MAX_CATALOG_SERVERS_PER_USER`` under an advisory lock on the
    user so concurrent creates can't slip past the cap. ``enabled`` defaults
    False (rows land as inert templates); the plugin install path passes True
    so an installed component works without a second write. The plugin
    provenance kwargs and ``enabled_in_new_workspaces`` sit outside
    ``CATALOG_COLUMNS`` so a request body can never smuggle ownership in or
    decide where a server starts.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent catalog creates for this user.
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM user_mcp_servers "
                    "WHERE user_id = %s AND name <> %s",
                    (user_id, name),
                )
                cnt = (await cur.fetchone())["cnt"]
                if cnt >= MAX_CATALOG_SERVERS_PER_USER:
                    raise ValueError(
                        f"Maximum of {MAX_CATALOG_SERVERS_PER_USER} "
                        "MCP catalog servers per user reached"
                    )

                await cur.execute(
                    """
                    INSERT INTO user_mcp_servers
                        (user_id, name, transport, command, args, url, env, headers,
                         description, instruction, tool_exposure_mode,
                         discovery_uses_secrets, enabled, enabled_in_new_workspaces,
                         plugin_id, plugin_server_key, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, name) DO NOTHING
                    RETURNING user_mcp_server_id
                    """,
                    (
                        user_id, name, transport, command, Json(args or []), url,
                        Json(env or {}), Json(headers or {}), description, instruction,
                        tool_exposure_mode, discovery_uses_secrets, enabled,
                        enabled_in_new_workspaces, plugin_id, plugin_server_key,
                    ),
                )
                if not await cur.fetchone():
                    raise ValueError(
                        f"MCP catalog server {name!r} already exists for this user"
                    )
                # An enabled row is live in every workspace once this commits,
                # so its bump commits with it. A caller bumping afterwards can
                # fail or die first, and a warm session whose version still
                # matches keeps the old config with nothing left to move it.
                if enabled:
                    await bump_user_versions(cur, user_id)
                logger.info(f"[mcp_db] create_catalog_server user_id={user_id} name={name}")
                return _catalog_row_to_dict(
                    await _read_catalog_row(cur, user_id, name)
                )


async def create_workspace_catalog_server(
    user_id: str, workspace_id: str, name: str, *, conn=None, **fields: Any
) -> dict[str, Any]:
    """Create a live user server that is switched on only in ``workspace_id``.

    One transaction for the row, a tombstone in every other live workspace of
    the user (Flash included) and the version fan-out: a workspace that
    re-resolved between a separate create and its tombstone would start the
    server where the user never asked for it. The tombstone overwrites
    whatever holds the slot, because the name is the user's own and nothing
    else may keep it on elsewhere. The row starts with
    ``enabled_in_new_workspaces`` off, so a workspace created later gets its
    tombstone from ``start_new_workspace_selection``. Raises ValueError like
    ``create_catalog_server``.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            row = await create_catalog_server(
                user_id, name, enabled=True, enabled_in_new_workspaces=False,
                conn=conn, **fields
            )
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM workspace_mcp_servers "
                    "WHERE workspace_id = %s AND name = %s",
                    (workspace_id, name),
                )
                await cur.execute(
                    """
                    INSERT INTO workspace_mcp_servers
                        (workspace_id, name, source, enabled, config,
                         created_at, updated_at)
                    SELECT w.workspace_id, %s, 'user', FALSE, NULL, NOW(), NOW()
                    FROM workspaces w
                    WHERE w.user_id = %s AND w.status <> 'deleted'
                      AND w.workspace_id <> %s
                    ON CONFLICT (workspace_id, name) DO UPDATE
                        SET source = 'user', enabled = FALSE, config = NULL,
                            updated_at = NOW()
                    """,
                    (name, user_id, workspace_id),
                )
                # No bump of its own: the create above bumped inside this
                # transaction, so these tombstones commit under that one.
            logger.info(
                f"[mcp_db] create_workspace_catalog_server user_id={user_id} "
                f"workspace_id={workspace_id} name={name}"
            )
            return row


async def update_catalog_server(
    user_id: str,
    name: str,
    *,
    updates: Mapping[str, Any],
    owned_by_plugin: str | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Partial update of a catalog template. Returns the row, or None if absent.

    Writes exactly the columns it is handed and nothing else. Fork-on-edit —
    clearing ``plugin_id``/``plugin_server_key`` so a later plugin update sees
    the name un-owned and skips it instead of overwriting the customization —
    is a policy decision and lives in ``services/mcp_catalog.apply_catalog_edit``.
    A writer that detached by default would strip a user's plugin provenance
    for any caller that merely forgot to opt out.

    ``owned_by_plugin`` narrows the write to a row that plugin still owns, the
    same predicate and for the same reason as ``delete_catalog_server``: a
    plugin path decides to write by reading ownership earlier, and a Customize
    landing in that window makes the row the user's. Without it the fork is
    overwritten by the very update that was supposed to skip it.

    Raises ValueError on a key outside ``_WRITABLE_CATALOG_COLUMNS``: a caller
    that misspells a column must not have the write silently dropped.
    """
    unknown = sorted(set(updates) - _WRITABLE_CATALOG_COLUMNS)
    if unknown:
        raise ValueError(f"unknown catalog column(s): {', '.join(unknown)}")
    if not updates:
        return await get_catalog_server(user_id, name, conn=conn)

    parts: list[str] = [f"{col} = %s" for col in updates]
    params: list[Any] = [
        Json(val) if col in _CATALOG_JSONB_COLUMNS else val
        for col, val in updates.items()
    ]
    parts.append("updated_at = NOW()")
    params.extend([user_id, name, owned_by_plugin, owned_by_plugin])

    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"UPDATE user_mcp_servers SET {', '.join(parts)} "
                    "WHERE user_id = %s AND name = %s "
                    "AND (%s::uuid IS NULL OR plugin_id = %s::uuid) "
                    "RETURNING user_mcp_server_id",
                    params,
                )
                if not await cur.fetchone():
                    return None
                row = await _read_catalog_row(cur, user_id, name)
                # A live (enabled) server changed shape — every workspace of the
                # user must re-resolve on next acquire.
                if row["enabled"]:
                    await bump_user_versions(cur, user_id)
                logger.info(f"[mcp_db] update_catalog_server user_id={user_id} name={name}")
                return _catalog_row_to_dict(row)


async def delete_catalog_server(
    user_id: str, name: str, *, owned_by_plugin: str | None = None, conn=None
) -> bool:
    """Delete a user server by name. Returns True if a row existed.

    The same transaction always purges the per-workspace disable-markers (a
    surviving marker would squat the UNIQUE(workspace_id, name) slot forever)
    and the user-level discovery cache (OAuth schema refresh has no ``enabled``
    check, so even a never-enabled server can hold schema rows a same-name
    recreate would resurrect). Only the version fan-out is conditional: an
    inert row reaches no workspace, so nothing needs to re-resolve.
    ``conn`` lets plugin uninstall run every component delete in one
    transaction; the purges then ride the caller's commit.

    ``owned_by_plugin`` narrows the delete to a row that plugin still owns.
    Plugin paths pass it because they decided to delete by reading ownership
    earlier: without the predicate, a Customize that detaches the row in the
    window between that read and this write is silently overridden and the
    user's forked copy is deleted anyway.

    Under ``lock_user_writes``, which ``tombstone_user_server`` takes too, so
    a workspace switching this server off lands before the purge or not at all.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "DELETE FROM user_mcp_servers WHERE user_id = %s AND name = %s "
                    "AND (%s::uuid IS NULL OR plugin_id = %s::uuid) "
                    "RETURNING enabled",
                    (user_id, name, owned_by_plugin, owned_by_plugin),
                )
                row = await cur.fetchone()
                if not row:
                    return False
                await cur.execute(
                    """
                    DELETE FROM workspace_mcp_servers
                    WHERE name = %s AND source = 'user' AND workspace_id IN
                        (SELECT workspace_id FROM workspaces WHERE user_id = %s)
                    """,
                    (name, user_id),
                )
                await cur.execute(
                    "DELETE FROM user_mcp_tool_schemas "
                    "WHERE user_id = %s AND server_name = %s",
                    (user_id, name),
                )
                # And each workspace's: in-sandbox discovery caches there, and a
                # same-name recreate with the same config would be served the
                # deleted server's tools, which a stdio probe can never replace.
                await cur.execute(
                    """
                    DELETE FROM workspace_mcp_tool_schemas
                    WHERE server_name = %s AND workspace_id IN
                        (SELECT workspace_id FROM workspaces WHERE user_id = %s)
                    """,
                    (name, user_id),
                )
                if row["enabled"]:
                    await bump_user_versions(cur, user_id)
                logger.info(f"[mcp_db] delete_catalog_server user_id={user_id} name={name}")
                return True


async def set_catalog_server_enabled(
    user_id: str, name: str, enabled: bool
) -> dict[str, Any] | None:
    """Toggle a user server live/inert. Returns the row, or None if absent.

    Both directions change every workspace's effective set, so the fan-out
    bump always runs in the same transaction.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE user_mcp_servers
                    SET enabled = %s, updated_at = NOW()
                    WHERE user_id = %s AND name = %s
                    RETURNING user_mcp_server_id
                    """,
                    (enabled, user_id, name),
                )
                if not await cur.fetchone():
                    return None
                row = await _read_catalog_row(cur, user_id, name)
                await bump_user_versions(cur, user_id)
                logger.info(
                    f"[mcp_db] set_catalog_server_enabled user_id={user_id} "
                    f"name={name} enabled={enabled}"
                )
                return _catalog_row_to_dict(row)


async def set_catalog_server_new_workspace_default(
    user_id: str, name: str, enabled: bool
) -> dict[str, Any] | None:
    """Choose whether workspaces created from now on start with this server on.

    Returns the row, or None if absent. No version bump: no existing
    workspace's effective set changes. Under ``lock_user_writes`` so a
    workspace create reads the flag either before this write or after it.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    """
                    UPDATE user_mcp_servers
                    SET enabled_in_new_workspaces = %s, updated_at = NOW()
                    WHERE user_id = %s AND name = %s
                    RETURNING user_mcp_server_id
                    """,
                    (enabled, user_id, name),
                )
                if not await cur.fetchone():
                    return None
                row = await _read_catalog_row(cur, user_id, name)
                logger.info(
                    f"[mcp_db] set_catalog_server_new_workspace_default "
                    f"user_id={user_id} name={name} enabled={enabled}"
                )
                return _catalog_row_to_dict(row)


async def claim_probe_kick(
    user_id: str, name: str, *, throttle_s: float | None = None
) -> datetime | None:
    """Stamp this row's probe clock; the stamp written, or None if refused.

    The throttle behind the list route's self-heal, held in Postgres because
    the route runs on whichever worker took the request and a rate limit only
    one of them can see is not one. ``throttle_s`` skips a row stamped more
    recently than that; a caller that passes None (a write, an edit) always
    wins and stamps anyway, so the next self-heal counts from its probe rather
    than firing on top of it.

    The stamp comes back so the probe it authorizes can fence its own write on
    it: the discovery fingerprint hashes ``${vault:NAME}`` refs and never
    values, so a rotated secret leaves it identical and only this clock can
    tell a probe that dialled with the old key from the one that replaced it.

    On the catalog row rather than the snapshot: the kick this rate-limits is
    the one for a row that has no snapshot yet, so the snapshot is the one row
    that cannot carry the clock.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_mcp_servers
                SET probe_kicked_at = NOW()
                WHERE user_id = %s AND name = %s
                  AND (
                    %s::float8 IS NULL
                    OR probe_kicked_at IS NULL
                    OR probe_kicked_at < NOW() - make_interval(secs => %s)
                  )
                RETURNING probe_kicked_at
                """,
                (user_id, name, throttle_s, throttle_s or 0),
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def list_enabled_user_servers(user_id: str) -> list[dict[str, Any]]:
    """Enabled (live) user servers, for the resolve-time merge.

    The single runtime chokepoint, and therefore the one place plugin-level
    disable applies: a row owned by a disabled plugin is withheld here, while
    every catalog reader keeps returning it (caps, redaction, OAuth lifecycle
    all must still see the row).
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _CATALOG_SELECT
                + """WHERE s.user_id = %s AND s.enabled = TRUE
                  AND (s.plugin_id IS NULL OR p.enabled = TRUE)
                ORDER BY s.name
                """,
                (user_id,),
            )
            return [_catalog_row_to_dict(r) for r in await cur.fetchall()]


# ---------------------------------------------------------------------------
# Per-workspace rows (source of truth): every write bumps mcp_config_version,
# except the selection a new workspace starts with
# ---------------------------------------------------------------------------


async def list_workspace_servers(workspace_id: str) -> list[dict[str, Any]]:
    """List all MCP rows for a workspace (disable-markers and tombstones)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT workspace_mcp_server_id, workspace_id, name, source, enabled,
                       config, created_at, updated_at
                FROM workspace_mcp_servers
                WHERE workspace_id = %s
                ORDER BY name
                """,
                (workspace_id,),
            )
            return [_workspace_row_to_dict(r) for r in await cur.fetchall()]


async def list_scope_markers_for_user(user_id: str) -> list[dict[str, Any]]:
    """Disable-marker rows (inherited tombstones + builtin markers) across ALL
    of a user's workspaces.

    Feeds the all-scopes catalog view's per-name "active in" checklist; one
    query instead of one per workspace. Soft-deleted workspaces are excluded:
    a tombstone in one is not a scope the user can still act on.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT workspace_id, name, source FROM workspace_mcp_servers
                WHERE source IN ('user', 'builtin') AND enabled = FALSE
                  AND workspace_id IN
                    (SELECT w.workspace_id FROM workspaces w
                      WHERE w.user_id = %s AND w.status <> 'deleted')
                """,
                (user_id,),
            )
            return [
                {
                    "workspace_id": str(r["workspace_id"]),
                    "name": r["name"],
                    "source": r["source"],
                }
                for r in await cur.fetchall()
            ]


async def get_workspace_servers_and_version(
    workspace_id: str,
) -> tuple[list[dict[str, Any]], int]:
    """Read a workspace's mcp_config_version then its MCP rows. Order matters.

    The shared connection is READ COMMITTED, so the two SELECTs are not one
    snapshot; a mutation (rows + version bump in one txn) can land between them.
    Reading the version FIRST bounds the only possible skew to (older version,
    newer rows) — safe, because the live version is then higher than what the
    caller caches, so its next acquire re-resolves and self-corrects. The reverse
    order would cache stale rows under the new version, and the matching version
    would short-circuit re-resolve, making the drift stick.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT mcp_config_version FROM workspaces WHERE workspace_id = %s",
                    (workspace_id,),
                )
                ws = await cur.fetchone()
                await cur.execute(
                    """
                    SELECT workspace_mcp_server_id, workspace_id, name, source, enabled,
                           config, created_at, updated_at
                    FROM workspace_mcp_servers
                    WHERE workspace_id = %s
                    ORDER BY name
                    """,
                    (workspace_id,),
                )
                rows = [_workspace_row_to_dict(r) for r in await cur.fetchall()]
    version = int((ws or {}).get("mcp_config_version") or 0)
    return rows, version


async def upsert_workspace_server(
    workspace_id: str,
    name: str,
    *,
    source: str,
    enabled: bool,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert or update a workspace MCP row; bumps mcp_config_version in the txn."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent mutations for this workspace.
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                await cur.execute(
                    """
                    INSERT INTO workspace_mcp_servers
                        (workspace_id, name, source, enabled, config, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (workspace_id, name) DO UPDATE
                        SET source = EXCLUDED.source,
                            enabled = EXCLUDED.enabled,
                            config = EXCLUDED.config,
                            updated_at = NOW()
                    RETURNING workspace_mcp_server_id, workspace_id, name, source,
                              enabled, config, created_at, updated_at
                    """,
                    (
                        workspace_id, name, source, enabled,
                        Json(config) if config is not None else None,
                    ),
                )
                row = await cur.fetchone()
                await bump_workspace_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] upsert_workspace_server workspace_id={workspace_id} "
                    f"name={name} source={source} enabled={enabled}"
                )
                return _workspace_row_to_dict(row)


async def tombstone_user_server(
    user_id: str, workspace_id: str, name: str, server_id: str
) -> bool:
    """Switch a user server off in one workspace; False once it is gone or replaced.

    Written only while the server row the caller read exists, under the lock
    its delete holds: the delete purges every tombstone of the name, so one
    landing after it would hold the workspace's slot for the name with nothing
    left to clear it. A same-name recreate is another server, placed by its
    own creator, so a switch decided on the old one leaves it alone.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                await cur.execute(
                    """
                    INSERT INTO workspace_mcp_servers
                        (workspace_id, name, source, enabled, config,
                         created_at, updated_at)
                    SELECT %(workspace_id)s::uuid, %(name)s::text, 'user', FALSE, NULL,
                           NOW(), NOW()
                    WHERE EXISTS (
                        SELECT 1 FROM user_mcp_servers
                        WHERE user_id = %(user_id)s AND name = %(name)s::text
                          AND user_mcp_server_id = %(server_id)s::uuid
                    )
                    ON CONFLICT (workspace_id, name) DO UPDATE
                        SET source = 'user', enabled = FALSE, config = NULL,
                            updated_at = NOW()
                    """,
                    {
                        "workspace_id": workspace_id, "name": name,
                        "user_id": user_id, "server_id": server_id,
                    },
                )
                if cur.rowcount == 0:
                    return False
                await bump_workspace_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] tombstone_user_server workspace_id={workspace_id} "
                    f"name={name}"
                )
                return True


def runs_on_account(row: Mapping[str, Any]) -> bool:
    """Whether a catalog row's own switch, and its plugin's, leave it on."""
    return bool(row.get("enabled")) and (
        row.get("plugin_id") is None or bool(row.get("plugin_enabled"))
    )


async def untombstone_user_server(
    user_id: str, workspace_id: str, name: str, tombstone_id: str
) -> Literal["enabled", "account_off", "gone"]:
    """Switch a user server back on in one workspace, if its tombstone is the one read.

    A delete purges the name's tombstones and a recreate writes its own, so a
    tombstone under another id belongs to a replacement its creator scoped off
    here. Under the lock both of those hold, so the account check reads the
    server whose tombstone this drops.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                catalog = await _read_catalog_row(cur, user_id, name)
                if catalog is None:
                    return "gone"
                if not runs_on_account(catalog):
                    return "account_off"
                await cur.execute(
                    """
                    DELETE FROM workspace_mcp_servers
                    WHERE workspace_mcp_server_id = %s AND workspace_id = %s
                      AND name = %s AND source = 'user' AND NOT enabled
                    """,
                    (tombstone_id, workspace_id, name),
                )
                if cur.rowcount == 0:
                    # Another enable already dropped it, or a replacement's
                    # tombstone stands in its place.
                    await cur.execute(
                        "SELECT 1 FROM workspace_mcp_servers "
                        "WHERE workspace_id = %s AND name = %s",
                        (workspace_id, name),
                    )
                    return "gone" if await cur.fetchone() else "enabled"
                await cur.execute(
                    "DELETE FROM workspace_mcp_tool_schemas "
                    "WHERE workspace_id = %s AND server_name = %s",
                    (workspace_id, name),
                )
                await bump_workspace_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] untombstone_user_server workspace_id={workspace_id} "
                    f"name={name}"
                )
                return "enabled"


async def delete_workspace_server(workspace_id: str, name: str) -> bool:
    """Delete a workspace MCP row; bumps version. False if no row existed."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                await cur.execute(
                    "DELETE FROM workspace_mcp_servers "
                    "WHERE workspace_id = %s AND name = %s",
                    (workspace_id, name),
                )
                if cur.rowcount == 0:
                    return False
                await cur.execute(
                    "DELETE FROM workspace_mcp_tool_schemas "
                    "WHERE workspace_id = %s AND server_name = %s",
                    (workspace_id, name),
                )
                await bump_workspace_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] delete_workspace_server workspace_id={workspace_id} "
                    f"name={name}"
                )
                return True


async def start_new_workspace_selection(
    cur,
    user_id: str,
    workspace_id: str,
    *,
    like_workspace_id: str | None = None,
) -> None:
    """Switch off, in a workspace being created, what it starts without.

    A new workspace starts without the servers kept out of new ones: a server
    added from inside a workspace has ``enabled_in_new_workspaces`` off, and
    its tombstone lands whether the row is live or inert, so the choice holds
    if it is switched live later. A duplicate (``like_workspace_id``) starts
    with its source's selection instead: every user server and built-in
    switched off there, whatever the new-workspace default says.

    Runs in the workspace's INSERT transaction, after ``lock_user_writes``.
    No version bump: nothing can have resolved an uncommitted workspace.
    Migration 055's insert trigger writes the new-workspace tombstones for a
    build that never calls this, so they may already be here.
    """
    params = {
        "workspace_id": workspace_id,
        "user_id": user_id,
        "like": like_workspace_id,
    }
    # The lock was a separate, earlier statement, and a server create holds
    # it through its tombstone fan-out and commit. READ COMMITTED gives these
    # reads a fresh snapshot, so either that fan-out saw this workspace or
    # this read sees that server, or the source's tombstone for it.
    if like_workspace_id is None:
        await cur.execute(
            """
            INSERT INTO workspace_mcp_servers
                (workspace_id, name, source, enabled, config, created_at, updated_at)
            SELECT %(workspace_id)s::uuid, s.name, 'user', FALSE, NULL, NOW(), NOW()
            FROM user_mcp_servers s
            WHERE s.user_id = %(user_id)s AND NOT s.enabled_in_new_workspaces
            ON CONFLICT (workspace_id, name) DO NOTHING
            """,
            params,
        )
        return
    # The trigger cannot know the source, so it switched off here what the
    # source has on.
    await cur.execute(
        """
        DELETE FROM workspace_mcp_servers t
        WHERE t.workspace_id = %(workspace_id)s::uuid
          AND t.source = 'user' AND NOT t.enabled
          AND NOT EXISTS (
              SELECT 1 FROM workspace_mcp_servers l
              WHERE l.workspace_id = %(like)s::uuid AND l.name = t.name
                AND l.source = 'user' AND NOT l.enabled
          )
        """,
        params,
    )
    await cur.execute(
        """
        INSERT INTO workspace_mcp_servers
            (workspace_id, name, source, enabled, config, created_at, updated_at)
        SELECT %(workspace_id)s::uuid, l.name, l.source, FALSE, NULL, NOW(), NOW()
        FROM workspace_mcp_servers l
        WHERE l.workspace_id = %(like)s::uuid AND NOT l.enabled
          AND (l.source = 'builtin' OR (l.source = 'user' AND EXISTS (
              SELECT 1 FROM user_mcp_servers s
              WHERE s.user_id = %(user_id)s AND s.name = l.name
          )))
        ON CONFLICT (workspace_id, name) DO NOTHING
        """,
        params,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _catalog_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a user_mcp_servers row into a plain JSON-friendly dict.

    Takes ``_CATALOG_SELECT``'s joined shape, which is what every reader and
    every writer hands back, so ``plugin_name is None`` means the row has no
    plugin owner and nothing else.
    """
    return {
        "user_mcp_server_id": str(row["user_mcp_server_id"]),
        "user_id": row["user_id"],
        "name": row["name"],
        "plugin_id": (
            str(row["plugin_id"]) if row["plugin_id"] is not None else None
        ),
        "plugin_server_key": row["plugin_server_key"],
        "plugin_name": row["plugin_name"],
        "plugin_enabled": row["plugin_enabled"],
        "transport": row["transport"],
        "command": row["command"],
        "args": row["args"] or [],
        "url": row["url"],
        "env": row["env"] or {},
        "headers": row["headers"] or {},
        "description": row["description"] or "",
        "instruction": row["instruction"] or "",
        "tool_exposure_mode": row["tool_exposure_mode"],
        "discovery_uses_secrets": bool(row["discovery_uses_secrets"]),
        "enabled": bool(row["enabled"]),
        "enabled_in_new_workspaces": bool(row["enabled_in_new_workspaces"]),
        # .get(): rows built by tests and by the plugin planner predate the
        # binding columns; an absent value is the untouched-row default.
        "tool_binding": dict(row.get("tool_binding") or {}),
        "binding_preset": row.get("binding_preset"),
        # Left as stored rather than filled out here: a row stores only the
        # modes a user set, and the rest are filled in under the level by the
        # one reader that knows the rule (``inputs_from_row``).
        "order_approval": row.get("order_approval"),
        # The owner's trading permission as stored, joined on so every reader
        # that judges an order off a row judges it under the level of that
        # moment; the per-call gate re-reads the row for this. Passed through
        # raw: ``inputs_from_row`` is the one place the agreement is judged.
        "trading_level": row["trading_level"],
        "trading_agreement_version": row["trading_agreement_version"],
        # When a probe was last claimed for this row, so a reader can tell a
        # kick still in flight from one that never started. Indexed, not
        # .get(): it is part of ``_CATALOG_SELECT``.
        "probe_kicked_at": (
            row["probe_kicked_at"].isoformat() if row["probe_kicked_at"] else None
        ),
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


def _workspace_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a workspace_mcp_servers row into a plain dict."""
    return {
        "workspace_mcp_server_id": str(row["workspace_mcp_server_id"]),
        "workspace_id": str(row["workspace_id"]),
        "name": row["name"],
        "source": row["source"],
        "enabled": row["enabled"],
        "config": row["config"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }
