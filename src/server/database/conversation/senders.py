"""Who sent a thread's turns, and who started it.

In a shared workspace's thread each turn runs under the member who sent it
(``conversation_responses.user_id``) and the thread names its starter
(``conversation_threads.created_by``); these reads name those people.
"""

from __future__ import annotations

from typing import Any, Optional

from psycopg.rows import dict_row, tuple_row

from src.server.contracts.status import INTERRUPT_REASONS_AWAITING_ANSWER
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
