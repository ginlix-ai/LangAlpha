"""Each member's read cursor and pin on a shared thread (migration 142).

A personal thread keeps both on its own row (``conversation_threads``). A
shared workspace's thread keeps them here, one row per member who has read or
pinned it, so one member's reading or pinning says nothing of another's. The
cursor moves the way the personal one does: only to a run the member was
shown, and never back.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from psycopg.rows import dict_row, tuple_row

from src.server.contracts.status import TERMINAL_STATUSES
from src.server.database.conversation import _sql
from src.server.database.conversation.viewer_threads import member_baseline_sql
from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import normalize_uuid


async def stamp_member_seen(
    thread_id: str, user_id: str, run_id: str
) -> Optional[Dict[str, Any]]:
    """``stamp_thread_seen`` for one member: their cursor to the observed
    run's seq, when the run is this thread's and has ended. Returns the
    ``{last_seen_run_seq, latest_run_seq}`` cursors, or None when the run
    does not qualify."""
    run_id = normalize_uuid(run_id)
    thread_id = normalize_uuid(thread_id)
    if run_id is None or thread_id is None:
        return None
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            WITH observed AS (
                SELECT run_seq FROM conversation_responses
                WHERE conversation_response_id = %(run)s
                  AND conversation_thread_id = %(thread)s
                  AND status IN ({_sql.sql_literals(TERMINAL_STATUSES)})
            )
            INSERT INTO shared_thread_reads AS sr
                (conversation_thread_id, user_id, last_seen_run_seq)
            SELECT ct.conversation_thread_id, %(user)s::varchar,
                   GREATEST(observed.run_seq, {member_baseline_sql("%(user)s::varchar")})
              FROM observed
              JOIN conversation_threads ct
                ON ct.conversation_thread_id = %(thread)s
            ON CONFLICT (conversation_thread_id, user_id) DO UPDATE
               SET last_seen_run_seq =
                       GREATEST(sr.last_seen_run_seq, EXCLUDED.last_seen_run_seq),
                   updated_at = now()
            RETURNING sr.last_seen_run_seq,
                (SELECT COALESCE(MAX(run_seq), 0) FROM conversation_responses
                 WHERE conversation_thread_id = %(thread)s) AS latest_run_seq
            """,
            {"run": run_id, "thread": thread_id, "user": user_id},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def set_member_pin(thread_id: str, user_id: str, pinned: bool) -> None:
    """A member's first row starts at the cursor they read with none, so a
    pin changes the order only."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO shared_thread_reads AS sr
                (conversation_thread_id, user_id, is_pinned, last_seen_run_seq)
            SELECT ct.conversation_thread_id, %(user)s::varchar, %(pinned)s,
                   {member_baseline_sql("%(user)s::varchar")}
              FROM conversation_threads ct
             WHERE ct.conversation_thread_id = %(thread)s
            ON CONFLICT (conversation_thread_id, user_id) DO UPDATE
               SET is_pinned = EXCLUDED.is_pinned, updated_at = now()
            """,
            {"thread": thread_id, "user": user_id, "pinned": pinned},
        )


async def member_seen(thread_id: str, user_id: str) -> int:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT last_seen_run_seq FROM shared_thread_reads "
            "WHERE conversation_thread_id = %s AND user_id = %s",
            (thread_id, user_id),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def keep_members_seen_over_quiet(conn, thread_id: str, run_seq: int) -> None:
    """``_keep_seen_over_quiet`` for a shared thread: carry the cursor of
    each member who had read it up to this run over a quiet check that
    changed nothing. A member with turns unread keeps them unread."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE shared_thread_reads
               SET last_seen_run_seq = GREATEST(last_seen_run_seq, %(seq)s),
                   updated_at = now()
             WHERE conversation_thread_id = %(thread)s
               AND last_seen_run_seq >= (
                   SELECT COALESCE(MAX(run_seq), 0) FROM conversation_responses
                   WHERE conversation_thread_id = %(thread)s AND run_seq < %(seq)s
               )
            """,
            {"seq": run_seq, "thread": thread_id},
        )
