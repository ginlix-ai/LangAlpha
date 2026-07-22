"""One-shot read of every table a thread replay needs."""

import logging

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)


async def get_replay_thread_data(
    thread_id: str,
) -> tuple[str | None, dict | None, list[dict], list[dict], list[dict], list[dict]]:
    """Fetch all data needed for thread replay in a single connection.

    Consolidates the separate connection acquisitions (owner check, thread
    summary, queries, responses, usage rows, provenance rows) into 1, reducing
    pool pressure during concurrent replays.

    Returns:
        (owner_user_id, thread_summary, queries, responses, usages, provenance)
    """
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None, None, [], [], [], []

    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Owner check (join threads -> workspaces)
            await cur.execute(
                """SELECT w.user_id
                   FROM conversation_threads t
                   JOIN workspaces w ON w.workspace_id = t.workspace_id
                   WHERE t.conversation_thread_id = %s""",
                (thread_id,),
            )
            owner_row = await cur.fetchone()
            owner_id = owner_row["user_id"] if owner_row else None

            if owner_id is None:
                return None, None, [], [], [], []

            # 2. Thread summary
            await cur.execute(
                """SELECT conversation_thread_id, workspace_id, current_status,
                          thread_index, latest_checkpoint_id, created_at, updated_at
                   FROM conversation_threads
                   WHERE conversation_thread_id = %s""",
                (thread_id,),
            )
            thread = await cur.fetchone()

            # 3. Queries
            await cur.execute(
                """SELECT * FROM conversation_queries
                   WHERE conversation_thread_id = %s
                   ORDER BY turn_index ASC""",
                (thread_id,),
            )
            queries = [dict(r) for r in await cur.fetchall()]

            # 4. Responses — one settled row per turn; an in_progress slot or
            # superseded attempt must never drive replay rendering.
            await cur.execute(
                _sql._SETTLED_ATTEMPTS,
                (thread_id,),
            )
            responses = [dict(r) for r in await cur.fetchall()]

            # 5. Main-workflow usage rows (table-sourced credit_usage
            # reconstruction). Background subagents persist additional
            # msg_type='task' rows under the same response id, but the live
            # credit_usage event represents the main workflow only.
            await cur.execute(
                """SELECT conversation_response_id, msg_type, token_usage,
                          total_credits, created_at
                   FROM conversation_usages
                   WHERE conversation_thread_id = %s
                     AND msg_type <> 'task'
                   ORDER BY created_at ASC""",
                (thread_id,),
            )
            usages = [dict(r) for r in await cur.fetchall()]

            # 6. Provenance rows (table-sourced provenance reconstruction)
            await cur.execute(
                """SELECT provenance_record_id, conversation_response_id,
                          turn_index, tool_call_id, source_type, identifier,
                          title, detail, args_fingerprint, args, result_sha256,
                          result_size, result_snippet, agent, provider,
                          source_timestamp, created_at
                   FROM provenance_records
                   WHERE conversation_thread_id = %s
                   ORDER BY turn_index ASC,
                            source_timestamp ASC NULLS LAST, created_at ASC""",
                (thread_id,),
            )
            provenance = [dict(r) for r in await cur.fetchall()]

    return (
        owner_id,
        dict(thread) if thread else None,
        queries,
        responses,
        usages,
        provenance,
    )
