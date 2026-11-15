"""When a user's Home ran on the cloud, and how far that work is handed back (migration 129).

A Home turn admitted while the machine Home lives on can't be reached runs in
Home's stand-in and marks the user away here (an upsert, so workers admitting
at once agree). What it wrote is owed to Home while ``last_away_at`` is newer
than ``handed_back_through``. The hand-back moves the watermark with a guarded
update under its advisory lock (``services.home_handback``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.home_workspace import (
    get_home_stand_in_id,
    get_home_workspace_id,
)
from src.server.database.pool import get_db_connection

_COLUMNS = (
    "user_id, away_since, last_away_at, handed_back_through, last_handback, "
    "handback_seen_at"
)

# Whether a row's cloud work is not on Home yet, as SQL over home_away.
OWED_SQL = (
    "last_away_at IS NOT NULL"
    " AND last_away_at > COALESCE(handed_back_through, '-infinity'::timestamptz)"
)


def is_owed(row: Optional[Dict[str, Any]]) -> bool:
    """``OWED_SQL`` for a row already read."""
    if not row or row.get("last_away_at") is None:
        return False
    through = row.get("handed_back_through")
    return through is None or row["last_away_at"] > through


def stand_in_lock_key(user_id: str) -> str:
    """The advisory lock a Home turn's mark shares and a removal or retirement
    of the stand-in's computer holds alone (``home_placement.stand_in_held``)."""
    return f"HOME_STAND_IN:{user_id}"


class StandInGone(Exception):
    """The stand-in a Home turn was placed in was removed before the turn
    started: refused, so nothing is written into a folder already deleted."""


async def mark_away(user_id: str, *, conn=None) -> None:
    """Record a Home turn admitted onto the stand-in, now.

    Run in the turn's START transaction (``conn``), after it took Home's row
    FOR SHARE, so the mark is later than any hand-back's watermark read under
    that row's lock. The clock is read here, not at the transaction's start,
    for the same reason. New work owes a hand-back its retries afresh.

    The shared side of ``stand_in_lock_key`` is held to the transaction's end:
    a removal that read nothing owed has already deleted the stand-in, which
    this then finds, or waits for this mark and reads it."""
    async with get_db_connection(conn) as c, c.cursor() as cur:
        await cur.execute(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s, 0))",
            (stand_in_lock_key(user_id),),
        )
        await cur.execute(
            "SELECT 1 FROM workspaces WHERE workspace_id = %s AND status <> 'deleted'",
            (get_home_stand_in_id(user_id),),
        )
        if await cur.fetchone() is None:
            raise StandInGone()
        await cur.execute(
            """
            INSERT INTO home_away (user_id, home_id, away_since, last_away_at)
            VALUES (%(user_id)s, %(home_id)s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (user_id) DO UPDATE SET
                away_since = COALESCE(home_away.away_since, clock_timestamp()),
                last_away_at = clock_timestamp(),
                handback_attempts = 0,
                next_handback_at = NULL,
                updated_at = NOW()
            """,
            {"user_id": user_id, "home_id": get_home_workspace_id(user_id)},
        )


async def get_home_away(user_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT {_COLUMNS} FROM home_away WHERE user_id = %s", (user_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def handback_owed(user_id: str, *, conn=None) -> bool:
    """Whether files Home's turns wrote on its stand-in are not on Home yet."""
    return is_owed(await get_home_away(user_id, conn=conn))


async def list_owed(limit: int = 50) -> List[Dict[str, Any]]:
    """Users with work on the stand-in not yet on Home, oldest first. Read
    without a lock: the hand-back takes the user's advisory lock and moves the
    watermark with a guarded update."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_COLUMNS} FROM home_away
            WHERE {OWED_SQL}
            ORDER BY last_away_at
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def finish_handback(
    user_id: str,
    *,
    previous: Optional[datetime],
    through: datetime,
    record: Optional[Dict[str, Any]],
    conn=None,
) -> bool:
    """Files on the stand-in up to ``through`` are on Home now. ``record`` is
    what arrived, for the user's notice; None when nothing did, which leaves
    the last notice as it was. Guarded on the watermark the caller read
    (``previous``), so a second writer that slipped past the lock changes
    nothing. False when the watermark had moved."""
    async with get_db_connection(conn) as c, c.cursor() as cur:
        await cur.execute(
            """
            UPDATE home_away SET
                handed_back_through = %(through)s,
                away_since = CASE WHEN last_away_at <= %(through)s
                    THEN NULL ELSE away_since END,
                last_handback = COALESCE(%(record)s::jsonb, last_handback),
                handback_seen_at = CASE WHEN %(record)s::jsonb IS NULL
                    THEN handback_seen_at ELSE NULL END,
                updated_at = NOW()
            WHERE user_id = %(user_id)s
              AND handed_back_through IS NOT DISTINCT FROM %(previous)s
            """,
            {
                "through": through,
                "previous": previous,
                "record": Json(record) if record is not None else None,
                "user_id": user_id,
            },
        )
        return cur.rowcount > 0


async def mark_handback_seen(user_id: str, handback_id: Optional[str] = None) -> None:
    """The user dismissed the notice; only the one they saw when named."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE home_away SET handback_seen_at = NOW(), updated_at = NOW()
            WHERE user_id = %s AND handback_seen_at IS NULL
              AND (%s::text IS NULL OR last_handback->>'handback_id' = %s::text)
            """,
            (user_id, handback_id, handback_id),
        )
