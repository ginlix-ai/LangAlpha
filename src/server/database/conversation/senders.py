"""Who sent a thread's turns, and who started it.

In a shared workspace's thread each turn runs under the member who sent it
(``conversation_responses.user_id``) and the thread names its starter
(``conversation_threads.created_by``); these reads name those people.
"""

from __future__ import annotations

from typing import Any, Optional

from psycopg.rows import dict_row, tuple_row

from src.server.contracts.status import INTERRUPT_REASONS_AWAITING_ANSWER
from src.server.contracts.turn_kinds import posted_by_server
from src.server.database.conversation.thread_reach import OTHERS_SENT_SQL
from src.server.database.pool import get_db_connection


async def others_turns_from(thread_id: str, turn_index: int, user_id: str) -> bool:
    """Whether any turn of the thread from ``turn_index`` on was sent by
    someone other than ``user_id`` (or names no sender)."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM conversation_responses
                 WHERE conversation_thread_id = %s AND turn_index >= %s
                   AND user_id IS DISTINCT FROM %s)
            """,
            (thread_id, turn_index, user_id),
        )
        return bool((await cur.fetchone())[0])


async def latest_turn_sender(thread_id: str) -> Optional[str]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT user_id FROM conversation_responses
             WHERE conversation_thread_id = %s
             ORDER BY run_seq DESC LIMIT 1
            """,
            (thread_id,),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def paused_turn_sender(thread_id: str) -> Optional[str]:
    """Who sent the thread's latest run when it ended paused on a question or
    an approval: the checkpoint still waits on that person's answer. A pause
    that waits on no answer names nobody."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT user_id, status, interrupt_reason FROM conversation_responses
             WHERE conversation_thread_id = %s
             ORDER BY run_seq DESC LIMIT 1
            """,
            (thread_id,),
        )
        row = await cur.fetchone()
    if row is None or row[1] != "interrupted":
        return None
    return row[0] if row[2] in INTERRUPT_REASONS_AWAITING_ANSWER else None


#: Set on the query row an edit or a regenerate writes anew in place of a
#: turn no person typed (an automation's, another agent's), which comes back
#: without that turn's origin: the row reads as untyped, as the turn did.
UNTYPED_KEY = "untyped"


def _typed(query_type: str, no_origin: bool, platform: Optional[str]) -> bool:
    return bool(no_origin) and not posted_by_server(query_type, platform)


async def turn_sender(thread_id: str, turn_index: int) -> Optional[str]:
    """Who sent the turn at ``turn_index``, by its latest attempt, as
    :func:`turn_senders` and every per-turn reader pick it: a retry runs as
    the turn's own sender, so every run of a turn names the same one."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT user_id FROM conversation_responses
             WHERE conversation_thread_id = %s AND turn_index = %s
             ORDER BY attempt_no DESC LIMIT 1
            """,
            (thread_id, turn_index),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def run_sender(run_id: str) -> Optional[str]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT user_id FROM conversation_responses WHERE conversation_response_id = %s",
            (run_id,),
        )
        row = await cur.fetchone()
    return row[0] if row else None


#: Whether a query row reads as typed: no origin, and no ``UNTYPED_KEY``.
_TYPED_SQL = "q.metadata->'origin' IS NULL AND NOT COALESCE(q.metadata ? 'untyped', false)"

#: The turn an answer to an interrupt carries on: the thread's latest run
#: that ended paused, before the answer, whose own turn is no answer. Each
#: answer resumes the newest pause, and a run that answered and paused again
#: is an answer too, so the chain back ends there. A send whose START wrote
#: its rows and never reached the graph ended no pause, so it is never taken.
_CARRIED_ON_SQL = f"""
    SELECT q.type, {_TYPED_SQL}, q.metadata->>'platform'
      FROM conversation_responses r
      JOIN conversation_queries q
        ON q.conversation_thread_id = r.conversation_thread_id
       AND q.turn_index = r.turn_index
     WHERE r.conversation_thread_id = %(thread)s
       AND r.status = 'interrupted'
       AND q.type <> 'resume_feedback'
       AND (%(before)s::bigint IS NULL OR r.run_seq < %(before)s::bigint)
     ORDER BY r.run_seq DESC
     LIMIT 1
"""


async def _typed_row(cur: Any, own: Optional[tuple]) -> bool:
    """Whether ``own`` (a turn's query type, whether it reads as typed, its
    platform, its thread and the first run_seq that bounds an answer) was
    typed: as its row reads, or for an answer by the turn it carried on."""
    if own is None:
        return False
    query_type, typed, platform, thread_id, before = own
    if query_type != "resume_feedback":
        return _typed(query_type, typed, platform)
    if before is None:
        return False  # an answer with no run of its own to bound the chain
    await cur.execute(_CARRIED_ON_SQL, {"thread": thread_id, "before": before})
    row = await cur.fetchone()
    return row is not None and _typed(*row)


async def run_turn_typed(run_id: str) -> bool:
    """Whether a person typed the turn ``run_id`` ran: its query row is
    there, records no origin (``provenance_metadata``) nor ``UNTYPED_KEY``
    and is none the server posts. A retry rebuilds its request without
    those, so this tells it. A run that answered an interrupt is told by the
    turn it carried on (``_CARRIED_ON_SQL``), as :func:`resumed_turn_typed`
    tells a resume."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"""
            SELECT q.type, {_TYPED_SQL}, q.metadata->>'platform',
                   r.conversation_thread_id, r.run_seq
              FROM conversation_responses r
              JOIN conversation_queries q
                ON q.conversation_thread_id = r.conversation_thread_id
               AND q.turn_index = r.turn_index
             WHERE r.conversation_response_id = %s
            """,
            (run_id,),
        )
        return await _typed_row(cur, await cur.fetchone())


async def resumed_turn_typed(thread_id: str) -> bool:
    """Whether a person typed the turn a resume carries on
    (``_CARRIED_ON_SQL``). Its request comes back without the origin that
    turn was sent with."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(_CARRIED_ON_SQL, {"thread": thread_id, "before": None})
        row = await cur.fetchone()
    return row is not None and _typed(*row)


async def turn_typed(thread_id: str, turn_index: int) -> bool:
    """Whether a person typed the thread's turn ``turn_index``, as
    :func:`run_turn_typed` tells it, an answer to an interrupt by the turn it
    carried on: a regenerate replays that turn's message from the checkpoint
    and comes back without the origin it was sent with."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"""
            SELECT q.type, {_TYPED_SQL}, q.metadata->>'platform', q.conversation_thread_id,
                   (SELECT min(r.run_seq) FROM conversation_responses r
                     WHERE r.conversation_thread_id = q.conversation_thread_id
                       AND r.turn_index = q.turn_index)
              FROM conversation_queries q
             WHERE q.conversation_thread_id = %s AND q.turn_index = %s
            """,
            (thread_id, turn_index),
        )
        return await _typed_row(cur, await cur.fetchone())


async def thread_creator(thread_id: str) -> Optional[str]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT created_by FROM conversation_threads WHERE conversation_thread_id = %s",
            (thread_id,),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def stamp_thread_creator(thread_id: str, user_id: str) -> None:
    """Name who started a shared workspace's thread, once: the first send's
    sender, whichever door made the row."""
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET created_by = %s"
            " WHERE conversation_thread_id = %s AND created_by IS NULL",
            (user_id, thread_id),
        )

async def thread_senders(thread_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Each thread's starter and the sender of its latest turn, with names."""
    if not thread_ids:
        return {}
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT t.conversation_thread_id::text AS thread_id, t.created_by,
                   cu.name AS created_by_name, last.user_id AS last_sender,
                   lu.name AS last_sender_name,
                   t.workspace_shared_at IS NOT NULL AS shared_with_workspace,
                   {OTHERS_SENT_SQL} AS others_sent
              FROM conversation_threads t
              LEFT JOIN users cu ON cu.user_id = t.created_by
              LEFT JOIN LATERAL (
                  SELECT r.user_id FROM conversation_responses r
                   WHERE r.conversation_thread_id = t.conversation_thread_id
                   ORDER BY r.run_seq DESC LIMIT 1
              ) last ON TRUE
              LEFT JOIN users lu ON lu.user_id = last.user_id
             WHERE t.conversation_thread_id = ANY(%s::uuid[])
            """,
            (list(thread_ids),),
        )
        return {r["thread_id"]: dict(r) for r in await cur.fetchall()}


async def turn_senders(thread_id: str) -> dict[int, dict[str, Any]]:
    """Who sent each turn of a thread (its latest attempt), with names,
    including people no longer members."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT ON (r.turn_index) r.turn_index, r.user_id, u.name
              FROM conversation_responses r
              LEFT JOIN users u ON u.user_id = r.user_id
             WHERE r.conversation_thread_id = %s AND r.user_id IS NOT NULL
             ORDER BY r.turn_index, r.attempt_no DESC
            """,
            (thread_id,),
        )
        return {
            r["turn_index"]: {"user_id": r["user_id"], "name": r["name"]}
            for r in await cur.fetchall()
        }
