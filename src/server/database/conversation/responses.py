"""conversation_responses: provenance sync and per-thread readers."""

import logging
from collections.abc import Collection
from typing import Optional, List, Dict, Any

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql

logger = logging.getLogger(__name__)


def _has_provenance(events: Optional[Any]) -> bool:
    """True if any SSE event is a provenance entry."""
    if not events:
        return False
    return any(isinstance(e, dict) and e.get("event") == "provenance" for e in events)


async def _sync_provenance_for_response(
    conn,
    *,
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
    provenance: Optional[Any],
    strict: bool = False,
    appended_only: bool = True,
    lanes: Collection[str],
) -> None:
    """(Re)derive provenance_records from ``provenance`` events on the
    caller's connection.

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
    if appended_only and not _has_provenance(provenance):
        return

    from src.server.database.provenance import sync_provenance_for_response

    await sync_provenance_for_response(
        conn,
        conversation_response_id=conversation_response_id,
        conversation_thread_id=conversation_thread_id,
        turn_index=turn_index,
        provenance=provenance,
        strict=strict,
        lanes=lanes,
    )


async def replace_lane_provenance(
    conversation_response_id: str,
    events: List[Dict[str, Any]],
    lanes: Collection[str],
) -> bool:
    """Rewrite ``lanes``' provenance on a response from ``events``.

    ``events`` hold each lane's whole record, so a lane they name no
    provenance for loses its rows; other lanes keep theirs. Raises on a
    failed write, so the caller can retry it; returns False when the row is
    missing.
    """
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT conversation_thread_id, turn_index "
                    "FROM conversation_responses WHERE conversation_response_id = %s",
                    (conversation_response_id,),
                )
                row = await cur.fetchone()
            if row is None:
                logger.warning(
                    f"[conversation_db] replace_lane_provenance: no row found for "
                    f"response_id={conversation_response_id}"
                )
                return False
            await _sync_provenance_for_response(
                conn,
                conversation_response_id=conversation_response_id,
                conversation_thread_id=str(row["conversation_thread_id"]),
                turn_index=row["turn_index"],
                provenance=events,
                strict=True,
                appended_only=False,
                lanes=lanes,
            )
            return True


async def get_recent_responses_for_thread(
    conversation_thread_id: str, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Return the most-recent turns in chronological order (oldest -> newest).

    Selects the newest ``limit`` settled turns (latest attempt each) via
    ``turn_index DESC`` (so a window keeps the latest turns, not the oldest)
    and reverses them to chronological order. ``limit=None`` returns every
    turn. Without ``replay_facts``, which grow with the turn:
    ``get_replay_responses`` reads them when needed.
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

