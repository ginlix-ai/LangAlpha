"""Each user's place in the first-run onboarding (``onboarding_progress``, migration 135)."""

from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from psycopg import sql
from psycopg.errors import ForeignKeyViolation
from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

ANSWER_COLUMNS = ("models", "runs_on", "this_mac", "setup_on", "setup_step")
STAMP_COLUMNS = {"plan_step_done": "plan_step_done_at", "setup_completed": "setup_completed_at"}

_COLUMNS = (
    "models, runs_on, plan_step_done_at, this_mac, setup_on, setup_step, "
    "setup_completed_at, created_at, updated_at"
)


async def get_onboarding_progress(user_id: str) -> Optional[dict[str, Any]]:
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM onboarding_progress WHERE user_id = %s",
            (user_id,),
        )
        return await cur.fetchone()


async def patch_onboarding_progress(
    user_id: str, changes: Mapping[str, Any]
) -> Optional[dict[str, Any]]:
    """Write the keys in ``changes`` and return the record; None when no such user exists.

    One upsert, so concurrent writers each land whole. A stamp is taken from
    the row when it already holds one (``COALESCE`` against the row, not the
    request), so two writers saying "done" at once keep whichever stamped first.
    """
    columns: list[sql.Composable] = []
    inserted: list[sql.Composable] = []
    updates: list[sql.Composable] = []
    values: list[Any] = []
    for key, value in changes.items():
        if key in ANSWER_COLUMNS:
            column = sql.Identifier(key)
            inserted.append(sql.Placeholder())
            updates.append(sql.SQL("{c} = EXCLUDED.{c}").format(c=column))
        elif key in STAMP_COLUMNS:
            column = sql.Identifier(STAMP_COLUMNS[key])
            value = bool(value)
            inserted.append(sql.SQL("CASE WHEN %s THEN NOW() END"))
            updates.append(
                sql.SQL(
                    "{c} = CASE WHEN EXCLUDED.{c} IS NULL THEN NULL "
                    "ELSE COALESCE(onboarding_progress.{c}, EXCLUDED.{c}) END"
                ).format(c=column)
            )
        else:
            raise ValueError(f"Unknown onboarding key {key!r}")
        columns.append(column)
        values.append(value)

    query = sql.SQL(
        "INSERT INTO onboarding_progress (user_id{cols}) VALUES (%s{vals}) "
        "ON CONFLICT (user_id) DO UPDATE SET {updates} RETURNING {returning}"
    ).format(
        cols=sql.SQL("").join(sql.SQL(", ") + c for c in columns),
        vals=sql.SQL("").join(sql.SQL(", ") + v for v in inserted),
        updates=sql.SQL(", ").join([*updates, sql.SQL("updated_at = NOW()")]),
        returning=sql.SQL(_COLUMNS),
    )
    try:
        async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (user_id, *values))
            return await cur.fetchone()
    except ForeignKeyViolation:
        logger.info(f"[onboarding] no user row for user_id={user_id}")
        return None
