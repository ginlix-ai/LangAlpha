"""The ``replay_facts`` column: what a settled run shows on replay that its
checkpoint does not keep.

Both ledgers carry it. A response row holds its main agent's reasoning
durations (written by the finalize CAS) and, once backfilled, the facts its
stored events resolve to under ``LEGACY_KEY``. A subagent run's row holds the
captured URLs of the images its lane referenced (``images``) and the steering
it handed back unread (``steering_returned``), both learned after its settle.
"""

from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.utils.pg_sanitize import SafeJson

LEGACY_KEY = "legacy"

# Legacy facts derived from a row's stored events stand in for them on
# replay, so a write to the events takes the facts away in the same
# statement: the row replays from its events until the backfill derives the
# facts again.
DROP_LEGACY_SQL = f"replay_facts = replay_facts - '{LEGACY_KEY}'"

# The version a row's legacy facts were resolved at, as text.
LEGACY_VERSION_SQL = f"replay_facts #>> '{{{LEGACY_KEY},v}}'"


async def append_returned_steering(
    task_run_id: str, returned: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Append steering a settled run handed back, returning its ``thread_id``
    and ``parent_run_id`` (None when no row matched).

    Not the settle CAS: the sweep that follows it reads the queue. The merge
    is one statement, so an images write landing on the row at the same
    time keeps its key.
    """
    if not returned:
        return None
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE subagent_runs
                SET replay_facts = COALESCE(replay_facts, '{}'::jsonb)
                    || jsonb_build_object('steering_returned',
                        COALESCE(replay_facts -> 'steering_returned', '[]'::jsonb)
                        || %(returned)s::jsonb)
                WHERE task_run_id = %(id)s
                RETURNING thread_id, parent_run_id
                """,
                {"id": task_run_id, "returned": SafeJson(returned)},
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def append_lane_images(
    thread_id: str, response_id: str, task_id: str, images: Dict[str, str]
) -> bool:
    """Merge a lane's captured images into every run the turn launched for
    that task; False when it launched none.

    The archive's records are fenced to the turn, so they come from exactly
    these runs, and replay reads a run's facts only through the turn that
    launched it. ``thread_id`` is there for the ``(thread_id, task_id)``
    index, as ``parent_run_id`` has none of its own.
    """
    if not images:
        return False
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE subagent_runs
                SET replay_facts = COALESCE(replay_facts, '{}'::jsonb)
                    || jsonb_build_object('images',
                        COALESCE(replay_facts -> 'images', '{}'::jsonb)
                        || %(images)s::jsonb)
                WHERE thread_id = %(thread)s AND task_id = %(task)s
                  AND parent_run_id = %(response)s
                """,
                {
                    "thread": thread_id,
                    "task": task_id,
                    "response": response_id,
                    "images": SafeJson(images),
                },
            )
            return cur.rowcount > 0


# Every run of a thread that holds replay facts, with the turn index of the
# response that launched it: replay keys a turn's stored lines on the facts of
# the runs it shows, so they are read with the thread's other replay rows.
RUN_FACTS_SQL = """
    SELECT r.turn_index, s.task_run_id, s.replay_facts
    FROM subagent_runs s
    JOIN conversation_responses r
        ON r.conversation_response_id = s.parent_run_id
    WHERE s.thread_id = %s AND s.replay_facts IS NOT NULL
    ORDER BY s.task_run_id
"""
