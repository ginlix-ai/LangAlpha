"""Database CRUD for per-workspace and user-level MCP server configuration.

Two concerns live here:
- User-level servers (``user_mcp_servers``): CRUD by ``(user_id, name)``.
  ``enabled`` rows are LIVE config inherited by every workspace of the user at
  resolve time; disabled rows are inert templates (the pre-connectors
  behavior). Any mutation of an enabled row fans out a version bump to ALL the
  user's workspaces in the same transaction — convergence is next-acquire.
- Per-workspace rows (``workspace_mcp_servers``): the source of truth for a
  workspace's effective MCP set. EVERY write bumps ``workspaces.mcp_config_version``
  in the SAME transaction so sessions can detect drift on their next acquire.

The discovery schema cache for both tiers lives in ``mcp_tool_schemas``.

Secrets are never stored here — env/header values hold ``${vault:NAME}``
references resolved against ``workspace_vault_secrets`` inside the sandbox.
"""

import logging
from collections.abc import Mapping
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

# Hard cap on user-configured (source='workspace') servers per workspace.
MAX_MCP_SERVERS_PER_WORKSPACE = 20

# Hard cap on catalog templates per user.
MAX_CATALOG_SERVERS_PER_USER = 50

# Mutable catalog columns, split by how a value binds. Anything outside the
# union is rejected by ``update_catalog_server`` rather than silently dropped.
_CATALOG_JSONB_COLUMNS = frozenset({"args", "env", "headers"})
_CATALOG_SCALAR_COLUMNS = frozenset({
    "transport", "command", "url", "description", "instruction",
    "tool_exposure_mode", "discovery_uses_secrets",
})
CATALOG_COLUMNS = _CATALOG_JSONB_COLUMNS | _CATALOG_SCALAR_COLUMNS

# Plugin provenance is writable too, but stays OUT of ``CATALOG_COLUMNS``:
# that set is what a request body binds against, so ownership can never be
# smuggled in from the wire. The catalog-edit service is the only caller that
# names these, and only to clear them.
_CATALOG_PROVENANCE_COLUMNS = frozenset({"plugin_id", "plugin_server_key"})
_WRITABLE_CATALOG_COLUMNS = CATALOG_COLUMNS | _CATALOG_PROVENANCE_COLUMNS


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
           s.created_at, s.updated_at, s.plugin_id, s.plugin_server_key,
           p.name AS plugin_name, p.enabled AS plugin_enabled
    FROM user_mcp_servers s
    LEFT JOIN user_plugins p ON p.user_plugin_id = s.plugin_id
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
    user_id: str, name: str, *, conn=None, for_share: bool = False
) -> dict[str, Any] | None:
    """Return a single catalog template by name, or None.

    ``for_share`` locks the row so concurrent edits block until the caller's
    transaction ends — pass ``conn`` with it so a snapshot write can fence on
    the config still being the one it read.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _CATALOG_SELECT
                + "WHERE s.user_id = %s AND s.name = %s"
                + (" FOR SHARE OF s" if for_share else ""),
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
    plugin_id: str | None = None,
    plugin_server_key: str | None = None,
    conn=None,
) -> dict[str, Any]:
    """Insert a catalog template. Raises ValueError on duplicate name or over cap.

    Enforces ``MAX_CATALOG_SERVERS_PER_USER`` under an advisory lock on the
    user so concurrent creates can't slip past the cap. ``enabled`` defaults
    False (rows land as inert templates); the plugin install path passes True
    so an installed component works without a second write. The plugin
    provenance kwargs sit outside ``CATALOG_COLUMNS`` so a request body can
    never smuggle ownership in.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent catalog creates for this user.
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (user_id,),
                )
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
                         discovery_uses_secrets, enabled, plugin_id,
                         plugin_server_key, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, name) DO NOTHING
                    RETURNING user_mcp_server_id
                    """,
                    (
                        user_id, name, transport, command, Json(args or []), url,
                        Json(env or {}), Json(headers or {}), description, instruction,
                        tool_exposure_mode, discovery_uses_secrets, enabled,
                        plugin_id, plugin_server_key,
                    ),
                )
                if not await cur.fetchone():
                    raise ValueError(
                        f"MCP catalog server {name!r} already exists for this user"
                    )
                logger.info(f"[mcp_db] create_catalog_server user_id={user_id} name={name}")
                return _catalog_row_to_dict(
                    await _read_catalog_row(cur, user_id, name)
                )


async def update_catalog_server(
    user_id: str,
    name: str,
    *,
    updates: Mapping[str, Any],
    conn=None,
) -> dict[str, Any] | None:
    """Partial update of a catalog template. Returns the row, or None if absent.

    Writes exactly the columns it is handed and nothing else. Fork-on-edit —
    clearing ``plugin_id``/``plugin_server_key`` so a later plugin update sees
    the name un-owned and skips it instead of overwriting the customization —
    is a policy decision and lives in ``services/mcp_catalog.apply_catalog_edit``.
    A writer that detached by default would strip a user's plugin provenance
    for any caller that merely forgot to opt out.

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
    params.extend([user_id, name])

    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"UPDATE user_mcp_servers SET {', '.join(parts)} "
                    "WHERE user_id = %s AND name = %s "
                    "RETURNING user_mcp_server_id",
                    params,
                )
                if not await cur.fetchone():
                    return None
                row = await _read_catalog_row(cur, user_id, name)
                # A live (enabled) server changed shape — every workspace of the
                # user must re-resolve on next acquire.
                if row["enabled"]:
                    await _bump_user_versions(cur, user_id)
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
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
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
                if row["enabled"]:
                    await _bump_user_versions(cur, user_id)
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
                await _bump_user_versions(cur, user_id)
                logger.info(
                    f"[mcp_db] set_catalog_server_enabled user_id={user_id} "
                    f"name={name} enabled={enabled}"
                )
                return _catalog_row_to_dict(row)


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


async def bump_user_workspaces_mcp_version(user_id: str) -> int:
    """Bump mcp_config_version on ALL of a user's workspaces (own transaction).

    For out-of-band user-level invalidation (OAuth connect/disconnect, user
    vault changes referenced by live servers). Returns workspaces touched.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await _bump_user_versions(cur, user_id)
            return cur.rowcount


async def list_user_builtin_disables(user_id: str) -> set[str]:
    """Builtin server names this user disabled account-wide."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name FROM user_mcp_builtin_disables WHERE user_id = %s",
                (user_id,),
            )
            return {r["name"] for r in await cur.fetchall()}


async def set_user_builtin_disable(user_id: str, name: str, disabled: bool) -> None:
    """Write/clear an account-wide builtin disable.

    Both directions change every workspace's effective set, so the fan-out
    bump runs in the same transaction (next-acquire convergence).
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                if disabled:
                    await cur.execute(
                        """
                        INSERT INTO user_mcp_builtin_disables (user_id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, name) DO NOTHING
                        """,
                        (user_id, name),
                    )
                else:
                    await cur.execute(
                        """
                        DELETE FROM user_mcp_builtin_disables
                        WHERE user_id = %s AND name = %s
                        """,
                        (user_id, name),
                    )
                await _bump_user_versions(cur, user_id)
                logger.info(
                    f"[mcp_db] set_user_builtin_disable user_id={user_id} "
                    f"name={name} disabled={disabled}"
                )


# ---------------------------------------------------------------------------
# Per-workspace rows (source of truth) — every write bumps mcp_config_version
# ---------------------------------------------------------------------------


async def list_workspace_servers(workspace_id: str) -> list[dict[str, Any]]:
    """List all MCP rows for a workspace (both disable-markers and user servers)."""
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


async def list_local_servers_for_user(
    user_id: str, *, live_only: bool = False
) -> list[dict[str, Any]]:
    """Workspace-LOCAL rows (source='workspace') across ALL of a user's workspaces.

    For user-tier vault invalidation: the sandbox resolves one merged secret
    namespace, so a user secret satisfies a local server's ``${vault:NAME}``
    too, and nothing scoped to the catalog would ever reach that server's cached
    snapshot. Stopped workspaces and disabled rows included — a snapshot
    outlives both the sandbox that wrote it and the row being switched off.

    ``live_only`` drops soft-deleted workspaces, for the callers that render
    these rows to a user rather than sweeping their leftovers.
    """
    status_filter = "AND w.status <> 'deleted'" if live_only else ""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT workspace_mcp_server_id, workspace_id, name, source, enabled,
                       config, created_at, updated_at
                FROM workspace_mcp_servers
                WHERE source = 'workspace' AND workspace_id IN
                    (SELECT w.workspace_id FROM workspaces w
                      WHERE w.user_id = %s {status_filter})
                ORDER BY name
                """,
                (user_id,),
            )
            return [_workspace_row_to_dict(r) for r in await cur.fetchall()]


async def list_scope_markers_for_user(user_id: str) -> list[dict[str, Any]]:
    """Disable-marker rows (inherited tombstones + builtin markers) across ALL
    of a user's workspaces.

    Feeds the all-scopes catalog view's per-name "active in" checklist; one
    query instead of one per workspace. Real servers (source='workspace')
    are excluded — those are rows, not markers. Soft-deleted workspaces are
    excluded too: a tombstone in one is not a scope the user can still act on.
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
    """Insert or update a workspace MCP row; bumps mcp_config_version in the txn.

    On insert of a new ``source='workspace'`` row, enforces
    ``MAX_MCP_SERVERS_PER_WORKSPACE`` under an advisory lock so concurrent
    creates can't slip past the cap. Disable-markers (``source='builtin'``)
    do not count against the cap.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent mutations for this workspace.
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                if source == "workspace":
                    await cur.execute(
                        """
                        SELECT COUNT(*) AS cnt FROM workspace_mcp_servers
                        WHERE workspace_id = %s AND source = 'workspace'
                          AND name <> %s
                        """,
                        (workspace_id, name),
                    )
                    cnt = (await cur.fetchone())["cnt"]
                    if cnt >= MAX_MCP_SERVERS_PER_WORKSPACE:
                        raise ValueError(
                            f"Maximum of {MAX_MCP_SERVERS_PER_WORKSPACE} "
                            "MCP servers per workspace reached"
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
                await _bump_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] upsert_workspace_server workspace_id={workspace_id} "
                    f"name={name} source={source} enabled={enabled}"
                )
                return _workspace_row_to_dict(row)


async def insert_workspace_server(
    workspace_id: str,
    name: str,
    *,
    config: dict[str, Any] | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Insert a NEW source='workspace' row; bumps version. None on name conflict.

    Uses ``ON CONFLICT DO NOTHING`` so a concurrent create of the same new name
    can't silently turn into an UPDATE (last-write-wins). Returns None when the
    name already exists, which the router maps to a 409. Enforces
    ``MAX_MCP_SERVERS_PER_WORKSPACE`` under the same advisory lock as upsert.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent mutations for this workspace.
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                await cur.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM workspace_mcp_servers
                    WHERE workspace_id = %s AND source = 'workspace'
                    """,
                    (workspace_id,),
                )
                cnt = (await cur.fetchone())["cnt"]
                if cnt >= MAX_MCP_SERVERS_PER_WORKSPACE:
                    raise ValueError(
                        f"Maximum of {MAX_MCP_SERVERS_PER_WORKSPACE} "
                        "MCP servers per workspace reached"
                    )

                await cur.execute(
                    """
                    INSERT INTO workspace_mcp_servers
                        (workspace_id, name, source, enabled, config, created_at, updated_at)
                    VALUES (%s, %s, 'workspace', TRUE, %s, NOW(), NOW())
                    ON CONFLICT (workspace_id, name) DO NOTHING
                    RETURNING workspace_mcp_server_id, workspace_id, name, source,
                              enabled, config, created_at, updated_at
                    """,
                    (
                        workspace_id, name,
                        Json(config) if config is not None else None,
                    ),
                )
                row = await cur.fetchone()
                if row is None:
                    # Name already exists ⇒ conflict; don't bump version.
                    return None
                await _bump_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] insert_workspace_server workspace_id={workspace_id} "
                    f"name={name}"
                )
                return _workspace_row_to_dict(row)


async def set_workspace_server_enabled(
    workspace_id: str, name: str, enabled: bool
) -> bool:
    """Toggle a workspace MCP row's enabled flag; bumps version. False if absent."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (workspace_id,),
                )
                await cur.execute(
                    "UPDATE workspace_mcp_servers SET enabled = %s, updated_at = NOW() "
                    "WHERE workspace_id = %s AND name = %s",
                    (enabled, workspace_id, name),
                )
                if cur.rowcount == 0:
                    return False
                await _bump_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] set_workspace_server_enabled workspace_id={workspace_id} "
                    f"name={name} enabled={enabled}"
                )
                return True


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
                await _bump_version(cur, workspace_id)
                logger.info(
                    f"[mcp_db] delete_workspace_server workspace_id={workspace_id} "
                    f"name={name}"
                )
                return True


async def bump_workspace_mcp_version(workspace_id: str) -> None:
    """Bump mcp_config_version outside a row mutation (own transaction).

    For out-of-band invalidation (vault secret changes) where no
    ``workspace_mcp_servers`` row is written but live sessions must re-resolve.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await _bump_version(cur, workspace_id)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


async def _bump_version(cur, workspace_id: str) -> None:
    """Atomically increment a workspace's mcp_config_version (same txn)."""
    await cur.execute(
        "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
        "WHERE workspace_id = %s",
        (workspace_id,),
    )


async def _bump_user_versions(cur, user_id: str) -> None:
    """Increment mcp_config_version on every workspace of a user (same txn).

    One statement, unpaginated on purpose: a user-level change must never
    leave a subset of workspaces on the old version.
    """
    await cur.execute(
        "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
        "WHERE user_id = %s",
        (user_id,),
    )


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
