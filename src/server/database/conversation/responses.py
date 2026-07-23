"""conversation_responses: SSE-event patches, provenance sync, and per-thread readers."""

import logging
from typing import Optional, List, Dict, Any, Tuple

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.utils.pg_sanitize import SafeJson

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
) -> None:
    """(Re)derive provenance_records from sse_events on the caller's connection.

    Imported lazily to avoid a circular import (provenance imports this module).
    Best-effort by default; ``strict=True`` re-raises so a transaction-bound
    caller (the finalize CAS) aborts instead of committing over a poisoned txn.
    """
    # Most persists carry no provenance (a turn with no external data access, or
    # a non-provenance event drain). Skip the extract + delete-then-insert when
    # there's no provenance entry: nothing to (re)write, and within a turn events
    # only accumulate, so "none now" means "none ever" for this response.
    if not _sse_has_provenance(sse_events):
        return

    from src.server.database.provenance import sync_provenance_for_response

    await sync_provenance_for_response(
        conn,
        conversation_response_id=conversation_response_id,
        conversation_thread_id=conversation_thread_id,
        turn_index=turn_index,
        sse_events=sse_events,
        strict=strict,
    )


async def rebase_sse_events(
    conversation_response_id: str,
    drop_agents: set,
    append_events: List[Dict[str, Any]],
    fallback_base: List[Dict[str, Any]],
) -> bool:
    """Replace a set of agents' rows in sse_events, atomically.

    One transaction: the row is read under FOR UPDATE, rows whose
    ``data.agent`` is in ``drop_agents`` are stripped, ``append_events`` are
    appended, and the result is written before the lock releases — a
    concurrent ``append_sse_event`` blocks on the row lock and lands on the
    new value instead of being erased by it. ``fallback_base`` seeds the
    blob when the row is missing (the write then updates nothing and this
    returns False).
    """
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT sse_events FROM conversation_responses
                    WHERE conversation_response_id = %s
                    FOR UPDATE
                    """,
                    (conversation_response_id,),
                )
                row = await cur.fetchone()
            base = (
                (row["sse_events"] or [])
                if row is not None
                else list(fallback_base)
            )
            updated_chunks = [
                c
                for c in base
                if str((c.get("data") or {}).get("agent", "")) not in drop_agents
            ] + append_events
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE conversation_responses
                    SET sse_events = %s
                    WHERE conversation_response_id = %s
                    RETURNING conversation_thread_id, turn_index
                    """,
                    (SafeJson(updated_chunks), conversation_response_id),
                )
                urow = await cur.fetchone()
            if urow is None:
                logger.warning(
                    f"[conversation_db] rebase_sse_events: no row found for "
                    f"response_id={conversation_response_id}"
                )
                return False
            # Re-derive provenance_records inside the same transaction — this
            # is the choke point for the background subagent drain, which
            # bypasses the turn-finalize path.
            await _sync_provenance_for_response(
                conn,
                conversation_response_id=conversation_response_id,
                conversation_thread_id=str(urow["conversation_thread_id"]),
                turn_index=urow["turn_index"],
                sse_events=updated_chunks,
            )
            logger.info(
                f"[conversation_db] rebase_sse_events response_id="
                f"{conversation_response_id} events={len(updated_chunks)}"
            )
            return True


async def append_sse_event(
    conversation_thread_id: str,
    event: Dict[str, Any],
    conn=None,
) -> bool:
    """Atomically append one SSE event to the thread's latest response blob.

    A server-side ``sse_events || event`` JSONB concat scoped to the most-recent
    response (by turn_index). Avoids reading the whole blob into Python and
    rewriting it, and is race-free against concurrent appenders (the
    read-modify-write it replaces is not). Returns True when a row was updated,
    False when the thread has no response row yet.
    """
    sql = """
        UPDATE conversation_responses
        SET sse_events = COALESCE(sse_events, '[]'::jsonb) || %s::jsonb
        WHERE conversation_response_id = (
            SELECT conversation_response_id
            FROM conversation_responses
            WHERE conversation_thread_id = %s
            ORDER BY turn_index DESC, attempt_no DESC
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
    turn.
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                base = f"""
                    SELECT {_sql._RESPONSE_COLUMNS}
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
