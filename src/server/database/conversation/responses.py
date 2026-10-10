"""conversation_responses: SSE-event patches, provenance sync, and per-thread readers."""

import logging
from collections.abc import Collection
from typing import Optional, List, Dict, Any, Tuple

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.database.replay_facts import DROP_LEGACY_SQL
from src.server.utils.pg_sanitize import SafeJson, safe_jsonb_array

logger = logging.getLogger(__name__)


def _sse_has_provenance(sse_events: Optional[Any]) -> bool:
    """True if any accumulated SSE event is a provenance entry."""
    if not sse_events:
        return False
    return any(
        isinstance(e, dict) and e.get("event") == "provenance" for e in sse_events
    )


async def _sync_provenance_for_response(
    conn,
    *,
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
    sse_events: Optional[Any],
    strict: bool = False,
    appended_only: bool = True,
    lanes: Collection[str],
) -> None:
    """(Re)derive provenance_records from sse_events on the caller's connection.

    Imported lazily to avoid a circular import (provenance imports this module).
    Best-effort by default; ``strict=True`` re-raises so a transaction-bound
    caller (the finalize CAS) aborts instead of committing over a poisoned txn.
    ``lanes`` scopes the rewrite to those agents' rows.
    """
    # Most persists carry no provenance (a turn with no external data access, or
    # a non-provenance event drain). A write that only appends cannot have
    # removed an entry, so with none present there is nothing to rewrite and the
    # extract + delete-then-insert is skipped. A write that strips rows can take
    # the last entry away, and its records must go with it.
    if appended_only and not _sse_has_provenance(sse_events):
        return

    from src.server.database.provenance import sync_provenance_for_response

    await sync_provenance_for_response(
        conn,
        conversation_response_id=conversation_response_id,
        conversation_thread_id=conversation_thread_id,
        turn_index=turn_index,
        sse_events=sse_events,
        strict=strict,
        lanes=lanes,
    )


def _event_agents(events: List[Dict[str, Any]]) -> frozenset[str]:
    """The agents whose rows ``events`` replace: each string ``data.agent``.

    Matches the filter in ``_REPLACE_AGENT_EVENTS_SQL``, which compares only
    strings: an event with no agent, or a non-string one, names no rows, so a
    batch can never strip the turn's agentless rows.
    """
    agents = set()
    for event in events:
        data = event.get("data")
        agent = data.get("agent") if isinstance(data, dict) else None
        if isinstance(agent, str):
            agents.add(agent)
    return frozenset(agents)


# One statement, so the row lock it takes orders it against append_sse_event:
# under READ COMMITTED a waiting UPDATE re-reads the newest row. The strip is
# one jsonpath filter over the stored array, so the kept rows are never
# unnested or sorted, and the blob never leaves Postgres.
_REPLACE_AGENT_EVENTS_SQL = f"""
    UPDATE conversation_responses
    SET sse_events = jsonb_path_query_array(
            COALESCE(sse_events, '[]'::jsonb),
            '$[*] ? (!exists(@.data.agent ? (@ == $drop[*])))',
            jsonb_build_object('drop', %(drop)s::text[])
        ) || %(append)s::jsonb,
        {DROP_LEGACY_SQL}
    WHERE conversation_response_id = %(id)s
    RETURNING conversation_thread_id, turn_index
"""


async def replace_agent_events(
    conversation_response_id: str,
    events: List[Dict[str, Any]],
) -> bool:
    """Replace the rows of every agent ``events`` names with ``events``, atomically.

    A concurrent ``append_sse_event`` waits on the row lock the UPDATE takes
    and lands on the new value instead of being erased by it. Only ``events``
    are encoded here: the stored array is filtered in Postgres. Returns False
    when the row is missing.
    """
    appended = await safe_jsonb_array(events)
    agents = _event_agents(events)
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    _REPLACE_AGENT_EVENTS_SQL,
                    {
                        "id": conversation_response_id,
                        "drop": sorted(agents),
                        "append": appended,
                    },
                )
                urow = await cur.fetchone()
            if urow is None:
                logger.warning(
                    f"[conversation_db] replace_agent_events: no row found for "
                    f"response_id={conversation_response_id}"
                )
                return False
            # The batch is the whole record of every lane it names, so those
            # lanes' provenance is rewritten from it in the same transaction,
            # and a lane it empties loses its rows with them. Other lanes keep
            # theirs: the main lane is written when the run settles.
            await _sync_provenance_for_response(
                conn,
                conversation_response_id=conversation_response_id,
                conversation_thread_id=str(urow["conversation_thread_id"]),
                turn_index=urow["turn_index"],
                sse_events=events,
                appended_only=False,
                lanes=agents,
            )
            logger.info(
                f"[conversation_db] replace_agent_events response_id="
                f"{conversation_response_id} appended={len(events)}"
            )
            return True


async def append_sse_event(
    conversation_thread_id: str,
    event: Dict[str, Any],
    conn=None,
) -> bool:
    """Atomically append one SSE event to the thread's latest response blob.

    A server-side ``sse_events || event`` JSONB concat scoped to the most-recent
    response (by run_seq — the one monotonic run ordering; turn_index is reused
    by retries and lowered by branch rewinds). Avoids reading the whole blob
    into Python and rewriting it, and is race-free against concurrent appenders
    (the read-modify-write it replaces is not). Returns True when a row was
    updated, False when the thread has no response row yet.
    """
    sql = f"""
        UPDATE conversation_responses
        SET sse_events = COALESCE(sse_events, '[]'::jsonb) || %s::jsonb,
            {DROP_LEGACY_SQL}
        WHERE conversation_response_id = (
            SELECT conversation_response_id
            FROM conversation_responses
            WHERE conversation_thread_id = %s
            ORDER BY run_seq DESC
            LIMIT 1
        )
    """
    params = (SafeJson([event]), conversation_thread_id)
    try:
        if conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                updated = cur.rowcount > 0
        else:
            async with pool.get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    updated = cur.rowcount > 0

        return updated

    except Exception as e:
        logger.error(f"Error appending sse_event: {e}")
        raise


async def get_responses_for_thread(
    conversation_thread_id: str, limit: Optional[int] = None, offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """Get the settled responses for a thread — latest attempt per turn."""
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Total = settled turns, not raw rows (attempts would inflate).
                await cur.execute(
                    """
                    SELECT COUNT(DISTINCT turn_index) as total
                    FROM conversation_responses
                    WHERE conversation_thread_id = %s AND status <> 'in_progress'
                """,
                    (conversation_thread_id,),
                )

                total_result = await cur.fetchone()
                total_count = total_result["total"]

                # Get responses
                if limit:
                    await cur.execute(
                        f"""
                        SELECT {_sql._RESPONSE_COLUMNS}
                        FROM ({_sql._SETTLED_ATTEMPTS}) r
                        ORDER BY turn_index ASC
                        LIMIT %s OFFSET %s
                    """,
                        (conversation_thread_id, limit, offset),
                    )
                else:
                    await cur.execute(
                        f"""
                        SELECT {_sql._RESPONSE_COLUMNS}
                        FROM ({_sql._SETTLED_ATTEMPTS}) r
                        ORDER BY turn_index ASC
                    """,
                        (conversation_thread_id,),
                    )

                responses = await cur.fetchall()
                return [dict(row) for row in responses], total_count

    except Exception as e:
        logger.error(f"Error getting responses for thread: {e}")
        raise


async def get_recent_responses_for_thread(
    conversation_thread_id: str, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Return the most-recent turns in chronological order (oldest -> newest).

    Selects the newest ``limit`` settled turns (latest attempt each) via
    ``turn_index DESC`` (so a window keeps the latest turns, not the oldest)
    and reverses them to chronological order. ``limit=None`` returns every
    turn. Without ``sse_events`` and ``replay_facts``, which grow with the
    turn: ``get_replay_responses`` reads a row whole when needed.
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                base = f"""
                    SELECT {_sql._LIGHT_RESPONSE_COLUMNS}
                    FROM ({_sql._SETTLED_ATTEMPTS}) r
                    ORDER BY turn_index DESC
                """
                if limit:
                    await cur.execute(base + " LIMIT %s", (conversation_thread_id, limit))
                else:
                    await cur.execute(base, (conversation_thread_id,))

                rows = await cur.fetchall()
                # SQL yields newest-first; reverse to chronological order.
                return [dict(row) for row in reversed(rows)]

    except Exception as e:
        logger.error(f"Error getting recent responses for thread: {e}")
        raise

