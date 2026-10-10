"""One-shot read of every table a thread replay needs."""

import logging
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.database.replay_facts import LEGACY_VERSION_SQL, RUN_FACTS_SQL
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadRows:
    """Every row a thread's replay reads. A turn's stored lines are keyed by
    the versions of these rows, so a read checks and projects against one
    set of them."""

    # Canonical, as the rows store it.
    thread_id: str
    owner_id: str
    # The thread row's summary; ``latest_checkpoint_id`` is the commit pointer.
    thread: dict[str, Any]
    queries: list[dict[str, Any]]
    # One settled response per turn, without its stored events.
    responses_by_turn: dict[Any, dict[str, Any]]
    # Main-workflow usage rows.
    usages: list[dict[str, Any]]
    provenance: list[dict[str, Any]]
    # The thread's runs that hold replay facts, each with its launching turn.
    run_facts: list[dict[str, Any]]


async def get_branch_pointer(thread_id: str) -> tuple[str, list[Any]] | None:
    """A thread's commit pointer and its persisted turn indexes ascending:
    what pairing its branch's turns takes, without the rows a replay reads.
    None when the thread is unknown or never committed a turn."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT latest_checkpoint_id,
                          ARRAY(SELECT DISTINCT turn_index
                                FROM conversation_queries q
                                WHERE q.conversation_thread_id = t.conversation_thread_id
                                  AND turn_index IS NOT NULL
                                ORDER BY turn_index) AS turn_indexes
                   FROM conversation_threads t
                   WHERE conversation_thread_id = %s""",
                (thread_id,),
            )
            row = await cur.fetchone()
    if row is None or not row["latest_checkpoint_id"]:
        return None
    return row["latest_checkpoint_id"], list(row["turn_indexes"])


async def get_replay_thread_data(thread_id: str) -> ThreadRows | None:
    """Every row a thread's replay reads, on one connection, which keeps
    concurrent replays from each holding several. None when the thread does
    not exist or the id is not a UUID."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None

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
                return None

            # 2. Thread summary
            await cur.execute(
                """SELECT conversation_thread_id, workspace_id, current_status,
                          thread_index, latest_checkpoint_id, created_at, updated_at
                   FROM conversation_threads
                   WHERE conversation_thread_id = %s""",
                (thread_id,),
            )
            thread = await cur.fetchone()
            if thread is None:
                return None

            # 3. Queries, with the row version replay keys stored lines by
            await cur.execute(
                """SELECT *, xmin FROM conversation_queries
                   WHERE conversation_thread_id = %s
                   ORDER BY turn_index ASC""",
                (thread_id,),
            )
            queries = [dict(r) for r in await cur.fetchall()]

            # 4. Responses — one settled row per turn; an in_progress slot or
            # superseded attempt must never drive replay rendering. Without
            # their stored events: a turn replay projects afresh re-reads its
            # row whole (see get_replay_responses).
            await cur.execute(
                _sql.settled_attempts(_sql._LIGHT_RESPONSE_COLUMNS),
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

            # 7. Replay facts of the thread's runs, keyed by launching turn
            await cur.execute(RUN_FACTS_SQL, (thread_id,))
            run_facts = [dict(r) for r in await cur.fetchall()]

    return ThreadRows(
        thread_id=thread_id,
        owner_id=owner_id,
        thread=dict(thread),
        queries=queries,
        responses_by_turn={r.get("turn_index"): r for r in responses},
        usages=usages,
        provenance=provenance,
        run_facts=run_facts,
    )


async def get_replay_responses(
    response_ids: list[str], *, legacy_facts: int | None = None
) -> dict[str, dict]:
    """Response rows by id, whole: ``sse_events``, ``replay_facts`` and
    ``xmin`` included.

    One read per row keeps a row's stored events and its version (the
    projection cache's key) from the same tuple, so a projection keyed by
    ``xmin`` was built from exactly that version's events.

    ``legacy_facts``: a row backfilled with legacy facts of this version
    comes without its stored events, which those facts stand in for. The
    events are never detoasted for it.
    """
    ids = [normalize_uuid(r) for r in response_ids]
    ids = [r for r in ids if r]
    if not ids:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT {_sql._LIGHT_RESPONSE_COLUMNS}, replay_facts,
                          CASE WHEN %(version)s::text IS NULL
                                 OR ({LEGACY_VERSION_SQL})
                                    IS DISTINCT FROM %(version)s::text
                               THEN sse_events END AS sse_events,
                          xmin
                   FROM conversation_responses
                   WHERE conversation_response_id = ANY(%(ids)s::uuid[])""",
                {
                    "ids": ids,
                    "version": None if legacy_facts is None else str(legacy_facts),
                },
            )
            rows = await cur.fetchall()
    return {str(r["conversation_response_id"]): dict(r) for r in rows}
