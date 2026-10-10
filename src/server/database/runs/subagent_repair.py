"""Task-chain repair: re-anchor subagent_tasks rows to their surviving runs.

Deleting conversation_responses cascades their subagent_runs away, which can
strand a task pointing at nothing (rewind to the newest survivor) or with
nothing left at all (delete the row). Left unrepaired, a later resume reads
the empty pointer and starts an unchained run with no predecessor and no
start pin.
"""

import logging
from typing import Any, Dict

from src.server.database import pool

logger = logging.getLogger(__name__)

# The lazy global sweep runs unfenced, so it ignores rows younger than this.
# A task row is born without runs for the width of the START transaction; the
# FK from subagent_runs already makes deleting a run-bearing task impossible,
# and this makes the sweep's zero-run read stale-proof as well.
SWEEP_MIN_AGE_SECONDS = 300

# A task's latest_run_id is dangling when nothing in subagent_runs answers to
# it. NULL counts: the FK is ON DELETE SET NULL, so a cascade-deleted run
# leaves the pointer empty rather than pointing at a tombstone. Both shapes
# read the same to every consumer — the task has no resolvable latest run.
_DANGLING_LATEST = """
    NOT EXISTS (
        SELECT 1 FROM subagent_runs r WHERE r.task_run_id = t.latest_run_id
    )
"""

_HAS_SURVIVING_RUN = """
    EXISTS (
        SELECT 1 FROM subagent_runs r
        WHERE r.thread_id = t.thread_id AND r.task_id = t.task_id
    )
"""


async def _rechain(conn, scope: str, param: Any) -> Dict[str, int]:
    """Rewind the dangling tasks ``scope`` selects to their newest surviving
    run, and delete those with none left."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE subagent_tasks t
            SET latest_run_id = (
                    SELECT r.task_run_id FROM subagent_runs r
                    WHERE r.thread_id = t.thread_id
                      AND r.task_id = t.task_id
                    ORDER BY r.started_at DESC
                    LIMIT 1
                ),
                updated_at = NOW()
            WHERE {scope}
              AND {_DANGLING_LATEST}
              AND {_HAS_SURVIVING_RUN}
            """,
            (param,),
        )
        rewound = cur.rowcount

        await cur.execute(
            f"""
            DELETE FROM subagent_tasks t
            WHERE {scope}
              AND NOT {_HAS_SURVIVING_RUN}
            """,
            (param,),
        )
        return {"rewound": rewound, "deleted": cur.rowcount}


async def repair_task_chains(thread_id: str, conn=None) -> Dict[str, int]:
    """Re-anchor a thread's task rows to their surviving runs.

    Idempotent and safe on any thread — a healthy thread matches neither
    statement — so truncation paths can call it unconditionally inside their
    own transaction.
    """
    async with pool.get_db_connection(conn) as conn:
        result = await _rechain(conn, "t.thread_id = %s", thread_id)
    if result["rewound"] or result["deleted"]:
        logger.info(
            f"[subagent_runs] REPAIR thread={thread_id} "
            f"rewound={result['rewound']} deleted={result['deleted']}"
        )
    return result


async def repair_dangling_task_chains(
    min_age_seconds: int = SWEEP_MIN_AGE_SECONDS,
) -> Dict[str, int]:
    """The global, thread-agnostic form of repair_task_chains.

    Heals damage that predates the transactional rewind — the shadow-deploy
    window, and any truncation path that ever escapes the guard. Drives off
    the dangling-pointer anti-join so a healthy ledger scans a small table and
    updates nothing.
    """
    async with pool.get_db_connection() as conn:
        return await _rechain(
            conn, "t.updated_at < NOW() - MAKE_INTERVAL(secs => %s)", min_age_seconds
        )
