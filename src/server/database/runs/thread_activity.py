"""When a thread was busy: its turns and the background tasks running on it.

A Drive check-out lapses once its chat has been quiet for an hour, read
under the space's lock, so this takes the caller's connection.
"""

from __future__ import annotations

from datetime import datetime

from psycopg.rows import dict_row

from src.server.utils.pg_sanitize import normalize_uuid

#: When the settled turn ``r`` (an alias of ``conversation_responses``)
#: ended: settled, else its start plus how long it ran. A turn starts at its
#: ``created_at``.
TURN_END_SQL = (
    "GREATEST(r.created_at, COALESCE(r.usage_settled_at,"
    " r.created_at + COALESCE(r.execution_time, 0) * INTERVAL '1 second'))"
)

_BUSY_SQL = f"""
    SELECT r.created_at AS start,
           CASE WHEN r.status = 'in_progress' THEN NULL ELSE {TURN_END_SQL} END AS "end"
      FROM conversation_responses r
     WHERE r.conversation_thread_id = %(thread)s::uuid
       AND (r.status = 'in_progress' OR {TURN_END_SQL} > %(since)s)
    UNION ALL
    SELECT s.started_at,
           CASE WHEN s.status = 'in_progress' THEN NULL ELSE s.finalized_at END
      FROM subagent_runs s
     WHERE s.thread_id = %(thread)s::uuid
       AND (s.status = 'in_progress' OR s.finalized_at > %(since)s)
     ORDER BY 1
"""


async def thread_busy(
    conn, thread_id: str, since: datetime
) -> list[tuple[datetime, datetime | None]]:
    """When ``thread_id`` was busy from ``since`` on, as ``(start, end)``
    spans, ``end`` None for one running now: each turn and each background
    task that ended after ``since`` or still runs. A task counts from its
    own start to its own end, since its work goes on after the turn that
    launched it ended. A handed-off command does not count: one may be a
    server that never ends, which would keep the thread busy for good."""
    thread = normalize_uuid(thread_id)
    if thread is None:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_BUSY_SQL, {"thread": thread, "since": since})
        return [(r["start"], r["end"]) for r in await cur.fetchall()]
