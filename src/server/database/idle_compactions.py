"""The idle compactions armed after completed turns (migration 108).

One row per thread at most: a later turn's arm replaces it. Every worker's
sweeper claims the rows that came due by deleting them under ``SKIP LOCKED``,
so no two workers fire one, and a worker that dies mid-firing leaves nothing
to fire again: a firing is never retried.
"""

from datetime import datetime
from typing import Any, Dict, List

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection

_COLUMNS = """
    conversation_thread_id::text AS thread_id, kind, fire_at, deadline,
    base_checkpoint_id, armed_by_run_id::text AS armed_by_run_id, user_id
"""


async def arm(
    thread_id: str,
    *,
    kind: str,
    fire_at: datetime,
    deadline: datetime,
    base_checkpoint_id: str,
    armed_by_run_id: str,
    armed_run_seq: int,
    user_id: str,
) -> None:
    """Arm the thread's firing, replacing the one an earlier run armed, never
    a later run's."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO idle_compactions (
                conversation_thread_id, kind, fire_at, deadline,
                base_checkpoint_id, armed_by_run_id, armed_run_seq, user_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (conversation_thread_id) DO UPDATE SET
                kind = EXCLUDED.kind,
                fire_at = EXCLUDED.fire_at,
                deadline = EXCLUDED.deadline,
                base_checkpoint_id = EXCLUDED.base_checkpoint_id,
                armed_by_run_id = EXCLUDED.armed_by_run_id,
                armed_run_seq = EXCLUDED.armed_run_seq,
                user_id = EXCLUDED.user_id,
                armed_at = NOW()
            WHERE idle_compactions.armed_run_seq <= EXCLUDED.armed_run_seq
            """,
            (thread_id, kind, fire_at, deadline, base_checkpoint_id, armed_by_run_id, armed_run_seq, user_id),
        )


async def disarm(thread_id: str, run_seq: int) -> None:
    """Drop the firing a run up to ``run_seq`` armed, which a later turn left
    nothing to fire."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM idle_compactions WHERE conversation_thread_id = %s AND armed_run_seq <= %s",
            (thread_id, run_seq),
        )


async def drop_expired() -> List[Dict[str, Any]]:
    """Drop the firings whose deadline passed before any worker claimed one,
    without a call: the entry each would read is about to expire."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"DELETE FROM idle_compactions WHERE deadline <= NOW() RETURNING {_COLUMNS}"
        )
        return [dict(row) for row in await cur.fetchall()]


async def claim_due(limit: int) -> List[Dict[str, Any]]:
    """Take up to ``limit`` firings that came due and can still run, soonest
    first, off the table for this worker alone."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            DELETE FROM idle_compactions
            WHERE conversation_thread_id IN (
                SELECT conversation_thread_id FROM idle_compactions
                WHERE fire_at <= NOW() AND deadline > NOW()
                ORDER BY fire_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING {_COLUMNS}
            """,
            (limit,),
        )
        return [dict(row) for row in await cur.fetchall()]
