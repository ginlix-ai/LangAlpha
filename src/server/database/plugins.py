"""Database CRUD for the user_plugins entity.

A plugin row owns bundle identity and the plugin-level enable switch; the
components it installed live in user_mcp_servers / user_skills stamped with
plugin_id (written by their own modules). Everything here is per-user by
(user_id, name), mirroring the catalog idioms in mcp_servers.py.

The enable toggle bumps every workspace's mcp_config_version in the same
transaction: the toggle changes the effective server set through the join
predicate in list_enabled_user_servers, and next-acquire convergence is how
that reaches live sessions. Skills need no version fan-out — the delivery
bundle is recomputed per turn from list_enabled_user_skills, which carries
the same predicate.
"""

import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

# Hard cap on installed plugins per user. Components additionally count against
# their own per-user caps (``MAX_CATALOG_SERVERS_PER_USER``,
# ``MAX_SKILLS_PER_USER``), which are the caps that bound real resource use --
# this one only bounds how many packages a user may hold.
#
# Named rather than restated, because the numbers here drifted out of date once
# already: this comment claimed 50 skills long after that limit became 200.
MAX_PLUGINS_PER_USER = 50

_PLUGIN_COLUMNS = """
    user_plugin_id, user_id, name, version, source_type, source_ref,
    manifest, mcp_document, content_hash, enabled, installed_at, updated_at
"""


def _row_to_dict(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize a raw row: UUIDs to str, timestamps to ISO 8601."""
    if row is None:
        return None
    out = dict(row)
    out["user_plugin_id"] = str(out["user_plugin_id"])
    for key in ("installed_at", "updated_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


async def list_plugins(user_id: str) -> list[dict[str, Any]]:
    """All of a user's installed plugins, ordered by name."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_PLUGIN_COLUMNS} FROM user_plugins "
                "WHERE user_id = %s ORDER BY name",
                (user_id,),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def get_plugin(
    user_id: str, name: str, *, conn=None
) -> dict[str, Any] | None:
    """One plugin by name, or None."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_PLUGIN_COLUMNS} FROM user_plugins "
                "WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            return _row_to_dict(await cur.fetchone())


async def create_plugin(
    user_id: str,
    name: str,
    *,
    version: str | None,
    source_type: str,
    source_ref: str | None,
    manifest: dict[str, Any],
    mcp_document: dict[str, Any] | None,
) -> dict[str, Any]:
    """Insert a plugin row. Raises ValueError on duplicate name or over cap.

    The row lands with an empty ``content_hash`` and takes no parameter for
    one: the hash is the claim "this exact tree is installed", which is only
    true once the components have landed, so ``stamp_plugin_content_hash``
    writes it at the end of the install. A crash in between leaves a row that
    matches no package, which is what makes update reconcile it.

    Takes the same per-user advisory lock as the catalog and skills caps
    (hashtext(user_id)) — one key, so an install serializes per user across
    every cap it will touch, with no second lock to deadlock against.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))",
                    (user_id,),
                )
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM user_plugins "
                    "WHERE user_id = %s AND name <> %s",
                    (user_id, name),
                )
                cnt = (await cur.fetchone())["cnt"]
                if cnt >= MAX_PLUGINS_PER_USER:
                    raise ValueError(
                        f"Maximum of {MAX_PLUGINS_PER_USER} plugins per user reached"
                    )
                await cur.execute(
                    f"""
                    INSERT INTO user_plugins
                        (user_id, name, version, source_type, source_ref,
                         manifest, mcp_document, content_hash,
                         installed_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, '', NOW(), NOW())
                    ON CONFLICT (user_id, name) DO NOTHING
                    RETURNING {_PLUGIN_COLUMNS}
                    """,
                    (
                        user_id, name, version, source_type, source_ref,
                        Json(manifest),
                        Json(mcp_document) if mcp_document is not None else None,
                    ),
                )
                row = await cur.fetchone()
                if not row:
                    raise ValueError(
                        f"Plugin {name!r} is already installed; use update"
                    )
                logger.info(
                    f"[plugins_db] create user_id={user_id} name={name} "
                    f"source={source_type}"
                )
                return _row_to_dict(row)


async def update_plugin_row(
    user_id: str,
    name: str,
    *,
    version: str | None,
    source_ref: str | None,
    manifest: dict[str, Any],
    mcp_document: dict[str, Any] | None,
    content_hash: str | None,
) -> dict[str, Any] | None:
    """Replace a plugin's manifests after a successful re-fetch. None if absent.

    ``content_hash=None`` keeps the stored one: a reconcile that left a
    component in error has not installed this tree, and the stale hash is what
    makes the next update try again instead of reporting up to date.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE user_plugins
                SET version = %s, source_ref = COALESCE(%s, source_ref),
                    manifest = %s, mcp_document = %s,
                    content_hash = COALESCE(%s, content_hash),
                    updated_at = NOW()
                WHERE user_id = %s AND name = %s
                RETURNING {_PLUGIN_COLUMNS}
                """,
                (
                    version, source_ref, Json(manifest),
                    Json(mcp_document) if mcp_document is not None else None,
                    content_hash, user_id, name,
                ),
            )
            row = _row_to_dict(await cur.fetchone())
            if row:
                logger.info(f"[plugins_db] update user_id={user_id} name={name}")
            return row


async def stamp_plugin_content_hash(
    user_id: str, name: str, content_hash: str
) -> dict[str, Any] | None:
    """Record which tree is installed, once its components have landed."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE user_plugins SET content_hash = %s
                WHERE user_id = %s AND name = %s
                RETURNING {_PLUGIN_COLUMNS}
                """,
                (content_hash, user_id, name),
            )
            return _row_to_dict(await cur.fetchone())


async def set_plugin_enabled(
    user_id: str, name: str, enabled: bool
) -> dict[str, Any] | None:
    """Toggle a plugin. Returns the row, or None if absent.

    Both directions change every workspace's effective MCP set (through the
    delivery join predicate), so the version fan-out runs in the same
    transaction.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE user_plugins
                    SET enabled = %s, updated_at = NOW()
                    WHERE user_id = %s AND name = %s
                    RETURNING {_PLUGIN_COLUMNS}
                    """,
                    (enabled, user_id, name),
                )
                row = await cur.fetchone()
                if not row:
                    return None
                await cur.execute(
                    "UPDATE workspaces "
                    "SET mcp_config_version = mcp_config_version + 1 "
                    "WHERE user_id = %s",
                    (user_id,),
                )
                logger.info(
                    f"[plugins_db] set_enabled user_id={user_id} name={name} "
                    f"enabled={enabled}"
                )
                return _row_to_dict(row)


async def delete_plugin_row(
    user_id: str, user_plugin_id: str, *, conn=None
) -> bool:
    """Delete the plugin row itself. True if a row existed.

    The provenance FKs are ON DELETE RESTRICT, so this must run after the
    uninstall service has deleted every still-owned component through the
    component helpers (which own their side-effect purges).

    Keyed on the id, not the name: the id is what the components point at, so
    it is the only key that cannot drop a different row than the one whose
    components were just deleted (uninstall, reinstall under the same name).
    """
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_plugins "
                "WHERE user_id = %s AND user_plugin_id = %s",
                (user_id, user_plugin_id),
            )
            deleted = cur.rowcount > 0
            if deleted:
                logger.info(
                    f"[plugins_db] delete user_id={user_id} id={user_plugin_id}"
                )
            return deleted


async def lock_plugin_row(
    user_id: str, user_plugin_id: str, *, conn
) -> dict[str, Any] | None:
    """Take the plugin row's write lock. None if it is already gone.

    Uninstall's, and taken before it touches anything else, which is what puts
    it in the same lock order as the enable toggle. Uninstall otherwise reaches
    ``workspaces`` (through each component delete's version fan-out) before
    ``user_plugins``, while the toggle takes them the other way round — the
    classic pair that deadlocks under concurrency. Locking the plugin row up
    front puts ``user_plugins`` first on both paths.

    Install and update do not take it. They hold no second lock to order this
    one against, and each component write carries its own ``owned_by_plugin``
    predicate, so a row that changed hands underneath them is refused at the
    write rather than held still around it.

    It also makes the enumerate-then-delete safe, and not because anything
    else cooperates: the components' FK on (user_id, plugin_id) means a
    concurrent insert takes FOR KEY SHARE on this row, which conflicts with
    this FOR UPDATE. So it blocks here rather than landing behind the
    enumeration and leaving the RESTRICT FK to abort the transaction after the
    OAuth fence has already revoked live grants on its own connection.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_PLUGIN_COLUMNS} FROM user_plugins "
            "WHERE user_id = %s AND user_plugin_id = %s FOR UPDATE",
            (user_id, user_plugin_id),
        )
        row = await cur.fetchone()
        return _row_to_dict(row) if row else None


async def list_plugin_referenced_secrets(
    user_id: str, user_plugin_id: str, *, conn=None
) -> set[str]:
    """Vault names the plugin's own server rows already reference.

    Half of the grant record: a ref only reaches a row through a grant, so the
    rows say what was granted without a column to keep honest alongside them.
    The other half is ``list_plugin_owned_secrets``, for the grants that never
    reached a row because every entry that would have carried one was held
    back at install.
    """
    from src.server.services.plugins.grants import secret_names_in

    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT env, headers, args FROM user_mcp_servers "
                "WHERE user_id = %s AND plugin_id = %s",
                (user_id, user_plugin_id),
            )
            found: set[str] = set()
            for row in await cur.fetchall():
                found |= secret_names_in(dict(row))
            return found


async def list_plugin_owned_secrets(
    user_id: str, user_plugin_id: str, *, conn=None
) -> set[str]:
    """Vault names this plugin introduced, whether or not a row uses them yet."""
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "SELECT name FROM user_vault_secrets "
                "WHERE user_id = %s AND plugin_id = %s",
                (user_id, user_plugin_id),
            )
            return {row[0] for row in await cur.fetchall()}


async def claim_plugin_secrets(
    user_id: str, user_plugin_id: str, names: list[str], *, conn=None
) -> None:
    """Record that this plugin introduced these vault names.

    ``plugin_id IS NULL`` in the predicate, so a claim is only ever made on an
    unclaimed secret. Callers stamp the names they created, but a create can
    race a create, and a stamp that could overwrite an existing claim would
    hand one plugin the grant on another's credential.
    """
    if not names:
        return
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "UPDATE user_vault_secrets SET plugin_id = %s "
                "WHERE user_id = %s AND name = ANY(%s) AND plugin_id IS NULL",
                (user_plugin_id, user_id, names),
            )


async def list_plugin_server_names(
    user_id: str, user_plugin_id: str, *, conn=None
) -> list[dict[str, Any]]:
    """Name + original key of every server row still owned by this plugin."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name, plugin_server_key FROM user_mcp_servers "
                "WHERE user_id = %s AND plugin_id = %s ORDER BY name",
                (user_id, user_plugin_id),
            )
            return [dict(r) for r in await cur.fetchall()]


async def list_plugin_skill_names(
    user_id: str, user_plugin_id: str, *, conn=None
) -> list[dict[str, Any]]:
    """Every skill row still owned by this plugin: name, original dir, and the
    archive locator.

    The locator rides along so export can fetch the bytes straight from this
    result instead of re-reading each row it just enumerated. Both extra
    columns are narrow by design — the inline blob is fetched separately by id,
    so uninstall and update pay nothing for carrying them.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name, plugin_skill_dir, workspace_id, user_skill_id, "
                "archive_key FROM user_skills "
                "WHERE user_id = %s AND plugin_id = %s ORDER BY name",
                (user_id, user_plugin_id),
            )
            out = []
            for r in await cur.fetchall():
                d = dict(r)
                for uuid_col in ("workspace_id", "user_skill_id"):
                    if d.get(uuid_col) is not None:
                        d[uuid_col] = str(d[uuid_col])
                out.append(d)
            return out
