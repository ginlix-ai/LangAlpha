"""Reading wakeups: the tool, the thread API, the price monitor, delivery."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.runs.outbox import waiting_alert_sql
from src.server.database.wakeups.contract import (
    TOO_LATE_SQL,
    WAKEUP_COLUMNS,
    attempt_sql,
    cols,
    is_uuid,
    jobs_of_sql,
    normalize,
    open_sql,
    pending_sql,
    unread_fire_sql,
    uuids,
)


async def get_wakeup(wakeup_id: str) -> Optional[Dict[str, Any]]:
    """One wakeup; an order's carries ``attempt``, the order it watches."""
    if not is_uuid(wakeup_id):
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {cols('w')}, {attempt_sql('w')} FROM wakeups w"
                " WHERE w.wakeup_id = %s",
                (str(wakeup_id),),
            )
            return normalize(await cur.fetchone())


async def get_thread_wakeup(thread_id: str, short_id: str) -> Optional[Dict[str, Any]]:
    """The thread's wakeup by its short id, as ``get_wakeup`` reads one."""
    if not is_uuid(thread_id):
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {cols('w')}, {attempt_sql('w')} FROM wakeups w"
                " WHERE w.conversation_thread_id = %s AND w.short_id = %s",
                (str(thread_id), short_id),
            )
            return normalize(await cur.fetchone())


async def list_pending(thread_id: str) -> List[Dict[str, Any]]:
    """What can still fire, in the order it was set, each order's with ``attempt``."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {cols("w")}, {attempt_sql("w")} FROM wakeups w
                WHERE w.conversation_thread_id = %s AND {pending_sql("w")}
                ORDER BY w.created_at
                """,
                (thread_id,),
            )
            return [normalize(r) for r in await cur.fetchall()]


async def list_thread_wakeups(
    thread_id: str, *, ended_since: Optional[datetime], ended_limit: int = 20
) -> List[Dict[str, Any]]:
    """Open wakeups in set order, then those that ended since
    ``ended_since`` (none without it), newest first, at most ``ended_limit``.

    Each row carries where its unread notices are in the outbox: whether one
    is past a cancel's reach (``notice_too_late``, as ``TOO_LATE_SQL`` reads
    it) and whether one is held after its wake was refused
    (``notice_waiting``), which is what tells a notice waiting on usage from
    one waiting on the next turn. An order's row carries ``attempt``, the
    order it watches.
    """
    open_w = open_sql("w")
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH picked AS (
                    SELECT w.wakeup_id, true AS is_open FROM wakeups w
                    WHERE w.conversation_thread_id = %(thread)s AND {open_w}
                    UNION ALL
                    (SELECT w.wakeup_id, false FROM wakeups w
                     WHERE w.conversation_thread_id = %(thread)s
                       AND NOT {open_w}
                       AND %(since)s::timestamptz IS NOT NULL
                       AND w.updated_at >= %(since)s::timestamptz
                     ORDER BY w.updated_at DESC
                     LIMIT %(limit)s)
                )
                SELECT {cols("w")}, picked.is_open,
                       COALESCE(j.notice_too_late, false) AS notice_too_late,
                       COALESCE(j.notice_waiting, false) AS notice_waiting,
                       {attempt_sql("w")}
                FROM picked
                JOIN wakeups w ON w.wakeup_id = picked.wakeup_id
                LEFT JOIN LATERAL (
                    SELECT bool_or({TOO_LATE_SQL}) AS notice_too_late,
                           bool_or({waiting_alert_sql("o")}) AS notice_waiting
                    FROM hook_outbox o
                    WHERE {jobs_of_sql("o", "w")}
                      AND o.status IN ('pending', 'claimed')
                      AND {unread_fire_sql("o", "w")}
                ) j ON true
                ORDER BY picked.is_open DESC,
                         CASE WHEN picked.is_open THEN w.created_at END,
                         w.updated_at DESC
                """,
                {"thread": thread_id, "since": ended_since, "limit": ended_limit},
            )
            return [normalize(r) for r in await cur.fetchall()]


async def list_armed_price_wakeups(
    thread_ids: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Armed price wakeups of every thread, or of ``thread_ids`` only: a change
    names the thread it touched, so a monitor re-reads that thread alone."""
    clauses = [pending_sql(), "kind = 'price'"]
    params: List[Any] = []
    if thread_ids is not None:
        clauses.append("conversation_thread_id = ANY(%s::uuid[])")
        params.append(uuids(thread_ids))
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {WAKEUP_COLUMNS} FROM wakeups WHERE {' AND '.join(clauses)}",
                params,
            )
            return [normalize(r) for r in await cur.fetchall()]
