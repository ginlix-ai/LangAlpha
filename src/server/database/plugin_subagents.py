"""Storage for the subagents installed plugins ship (``plugin_subagents``).

One row per ``agents/<name>.md`` (see migration 071). The user's changes to
one are override rows in ``user_subagents`` keyed ``plugin:<plugin>/<name>``,
so the deletes here take that table's per-user lock and drop the overrides
in the same transaction: a pane save racing an uninstall then either lands
first and goes with the plugin, or finds the base gone.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.database.user_subagents import lock_user_subagents

logger = logging.getLogger(__name__)

#: Bounds the roster a user's plugins can add to every turn's prompt.
MAX_PLUGIN_SUBAGENTS_PER_USER = 50

# Without ``file_text``, which only export and update read.
_COLUMNS = ", ".join(
    f"s.{c}" for c in (
        "plugin_subagent_id", "user_id", "plugin_id", "name", "fields",
        "instructions", "roles", "created_at", "updated_at",
    )
)


def plugin_key(plugin_name: str, name: str) -> str:
    """How an override names a plugin's subagent."""
    return f"plugin:{plugin_name}/{name}"


def _row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("plugin_subagent_id", "plugin_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


async def list_user_plugin_subagents(user_id: str, *, conn=None) -> list[dict[str, Any]]:
    """Every plugin subagent the user has, with its plugin's name and switch,
    in install order: the order they join the roster."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS}, p.name AS plugin_name, p.enabled AS plugin_enabled "
                "FROM plugin_subagents s JOIN user_plugins p "
                "ON p.user_id = s.user_id AND p.user_plugin_id = s.plugin_id "
                "WHERE s.user_id = %s ORDER BY p.installed_at, p.name, s.name",
                (user_id,),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def list_user_plugin_subagent_names(user_id: str, *, conn=None) -> list[dict[str, Any]]:
    """Name, plugin_id and plugin name of every plugin subagent the user has,
    in ``list_user_plugin_subagents``' order, and whether the user changed it.

    ``edited`` holds while the plugin is off too, when the Subagents list
    leaves the subagent out but uninstall still deletes the user's changes.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT s.name, s.plugin_id, p.name AS plugin_name, EXISTS ("
                "SELECT 1 FROM user_subagents o WHERE o.user_id = s.user_id "
                "AND o.kind = 'override' AND o.base_key = 'plugin:' || p.name || '/' || s.name "
                "AND o.fields <> '{}'::jsonb"
                ") AS edited "
                "FROM plugin_subagents s JOIN user_plugins p "
                "ON p.user_id = s.user_id AND p.user_plugin_id = s.plugin_id "
                "WHERE s.user_id = %s ORDER BY p.installed_at, p.name, s.name",
                (user_id,),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def list_plugin_subagents(
    user_id: str, plugin_id: str, *, conn=None
) -> list[dict[str, Any]]:
    """One plugin's subagents, by name, with the files they were read from."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS}, s.file_text FROM plugin_subagents s "
                "WHERE s.user_id = %s AND s.plugin_id = %s ORDER BY s.name",
                (user_id, plugin_id),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def count_plugin_subagents(user_id: str, *, conn=None) -> int:
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM plugin_subagents WHERE user_id = %s", (user_id,)
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0


async def insert_plugin_subagent(
    user_id: str,
    plugin_id: str,
    row: Mapping[str, Any],
    file_text: str,
    *,
    conn=None,
) -> dict[str, Any] | None:
    """Add one; None when another row holds the name, which is left alone.

    ``row`` is shaped like a custom subagent's (``name``, ``fields``,
    ``instructions``, ``roles``), which is how the merge reads it back.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO plugin_subagents AS s "
                "(user_id, plugin_id, name, fields, instructions, roles, file_text) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                f"ON CONFLICT (user_id, name) DO NOTHING RETURNING {_COLUMNS}",
                (
                    user_id, plugin_id, row["name"], Json(row["fields"]),
                    row["instructions"], row["roles"], file_text,
                ),
            )
            stored = await cur.fetchone()
            return _row_to_dict(stored) if stored else None


async def replace_plugin_subagent(
    user_id: str,
    plugin_id: str,
    row: Mapping[str, Any],
    file_text: str,
    *,
    conn=None,
) -> bool:
    """Replace one with a new version of its file. False when the plugin no longer has it.

    Run it inside ``overrides_locked``: a pane save diffs the user's override
    against the base under that lock, and a base replaced past it would have
    the save drop a key the user set to the old base's value.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "UPDATE plugin_subagents SET fields = %s, instructions = %s, roles = %s, "
                "file_text = %s WHERE user_id = %s AND plugin_id = %s AND name = %s",
                (
                    Json(row["fields"]), row["instructions"], row["roles"], file_text,
                    user_id, plugin_id, row["name"],
                ),
            )
            return cur.rowcount > 0


@asynccontextmanager
async def overrides_locked(user_id: str) -> AsyncIterator[Any]:
    """A connection in a transaction holding the user's override lock, the
    one ``save_subagent`` takes, for writes that change a plugin's bases."""
    async with get_db_connection() as db:
        async with db.transaction():
            await lock_user_subagents(user_id, conn=db)
            yield db


@asynccontextmanager
async def subagents_snapshot() -> AsyncIterator[Any]:
    """A read-only connection whose reads all see one committed state, so a
    turn's rows and bases come from the same side of ``overrides_locked``."""
    async with get_db_connection() as db:
        async with db.transaction():
            await db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            yield db


async def clear_plugin_overrides(user_id: str, plugin_name: str, *, conn) -> int:
    """Delete the user's overrides of ``plugin_name``'s subagents; inside
    ``overrides_locked``. Returns how many went."""
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM user_subagents WHERE user_id = %s AND kind = 'override' "
            "AND starts_with(base_key, %s)",
            (user_id, plugin_key(plugin_name, "")),
        )
        return cur.rowcount


async def delete_plugin_subagents(
    user_id: str, plugin_id: str, plugin_name: str, *, conn
) -> list[str]:
    """Delete the plugin's subagents and the user's overrides of them, in
    ``conn``'s transaction, which the override lock needs. Returns the names
    deleted."""
    await lock_user_subagents(user_id, conn=conn)
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM plugin_subagents WHERE user_id = %s AND plugin_id = %s "
            "RETURNING name",
            (user_id, plugin_id),
        )
        deleted = sorted(r[0] for r in await cur.fetchall())
    # By plugin name, not by the rows just deleted: an override written while
    # the plugin lacked that subagent is its too.
    overrides = await clear_plugin_overrides(user_id, plugin_name, conn=conn)
    if deleted or overrides:
        logger.info(
            f"[plugin_subagents] delete user_id={user_id} plugin={plugin_name} "
            f"subagents={deleted} overrides={overrides}"
        )
    return deleted


async def delete_plugin_subagent(
    user_id: str, plugin_id: str, plugin_name: str, name: str, *, conn
) -> bool:
    """Delete one of the plugin's subagents and the user's override of it;
    inside ``overrides_locked``. False when the plugin no longer has it."""
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM plugin_subagents WHERE user_id = %s AND plugin_id = %s "
            "AND name = %s",
            (user_id, plugin_id, name),
        )
        deleted = cur.rowcount > 0
        await cur.execute(
            "DELETE FROM user_subagents WHERE user_id = %s AND kind = 'override' "
            "AND base_key = %s",
            (user_id, plugin_key(plugin_name, name)),
        )
        overrides = cur.rowcount
    if deleted or overrides:
        logger.info(
            f"[plugin_subagents] delete user_id={user_id} plugin={plugin_name} "
            f"subagents={[name] if deleted else []} overrides={overrides}"
        )
    return deleted
