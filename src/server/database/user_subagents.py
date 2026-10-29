"""Storage for each user's subagents (``user_subagents``).

A row is a custom subagent or an override of a base one; see migration 070 for
the shape. ``fields`` holds only the keys the user set, so these functions
store and return it as written: what a field means, and what the effective
subagent is, are the merge's (``ptc_agent.agent.subagents.merge``).

Every function here is user-tier only (``workspace_id IS NULL``), the one
scope this version has.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

SubagentKind = Literal["custom", "override"]
UpdatedBy = Literal["user", "agent"]

_COLUMNS = (
    "user_subagent_id, user_id, name, workspace_id, kind, base_key, fields, "
    "instructions, roles, updated_by, updated_thread_id, created_at, updated_at"
)


async def lock_user_subagents(user_id: str, *, conn) -> None:
    """One writer of the user's subagents at a time, for the rest of
    ``conn``'s transaction: versions, the custom cap and renames read then
    write, and a plugin changing its bases takes the same lock so a save
    racing it finds the base gone rather than outliving it."""
    from src.server.database.sql_fences import advisory_key

    await conn.execute(
        "SELECT pg_advisory_xact_lock(%s)", (advisory_key("user_subagents", user_id),)
    )


def _row_to_dict(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """UUIDs to str and timestamps to ISO 8601, as the other row readers do."""
    if row is None:
        return None
    out = dict(row)
    for key in ("user_subagent_id", "workspace_id", "updated_thread_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    out["fields"] = out.get("fields") or {}
    out["roles"] = list(out["roles"]) if out.get("roles") is not None else None
    return out


async def list_user_subagents(user_id: str, *, conn=None) -> list[dict[str, Any]]:
    """Every row the user has, ordered by name: the per-turn merge's input."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM user_subagents "
                "WHERE user_id = %s AND workspace_id IS NULL ORDER BY name",
                (user_id,),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def upsert_user_subagent(
    user_id: str,
    name: str,
    *,
    kind: SubagentKind,
    fields: dict[str, Any],
    updated_by: UpdatedBy,
    base_key: str | None = None,
    instructions: str | None = None,
    roles: list[str] | None = None,
    updated_thread_id: str | None = None,
    conn=None,
) -> dict[str, Any]:
    """Write the row for ``name`` whole, creating it or replacing it.

    Whole, not merged: ``fields`` is the complete set of keys the user has
    set, so a key left out is one they reset. A version check, when the
    caller has one, belongs before this in the same transaction.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO user_subagents
                    (user_id, name, kind, base_key, fields, instructions, roles,
                     updated_by, updated_thread_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, name) WHERE workspace_id IS NULL
                DO UPDATE SET
                    kind = EXCLUDED.kind,
                    base_key = EXCLUDED.base_key,
                    fields = EXCLUDED.fields,
                    instructions = EXCLUDED.instructions,
                    roles = EXCLUDED.roles,
                    updated_by = EXCLUDED.updated_by,
                    updated_thread_id = EXCLUDED.updated_thread_id
                RETURNING {_COLUMNS}
                """,
                (
                    user_id,
                    name,
                    kind,
                    base_key,
                    Json(fields),
                    instructions,
                    roles,
                    updated_by,
                    updated_thread_id,
                ),
            )
            return _row_to_dict(await cur.fetchone())


async def rename_user_subagent(
    user_id: str,
    name: str,
    new_name: str,
    *,
    updated_by: UpdatedBy,
    updated_thread_id: str | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Rename a custom subagent; None when there is no such row.

    The caller checks that the target is free under the user's lock, which
    every writer of these rows takes.
    """
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE user_subagents
                SET name = %s, updated_by = %s, updated_thread_id = %s
                WHERE user_id = %s AND name = %s AND workspace_id IS NULL
                  AND kind = 'custom'
                RETURNING {_COLUMNS}
                """,
                (new_name, updated_by, updated_thread_id, user_id, name),
            )
            return _row_to_dict(await cur.fetchone())


async def delete_user_subagent(user_id: str, name: str, *, conn=None) -> bool:
    """Delete the row for ``name``: a custom subagent, or the reset of a base."""
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_subagents "
                "WHERE user_id = %s AND name = %s AND workspace_id IS NULL",
                (user_id, name),
            )
            return cur.rowcount > 0
