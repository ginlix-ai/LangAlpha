"""Ledger reads and writes for resuming the work a graceful restart stopped
(``services.report_back.restart``)."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from ptc_agent.agent.middleware.background_subagent.task import WORKFLOW_SUBAGENT_TYPE
from src.server.contracts.status import (
    RESTART_INTERRUPTED_META,
    RESTART_STOP_ERROR_TYPE,
    is_restart_interrupted,
)
from src.server.database import pool
from src.server.database.runs.hook_jobs import (
    automation_handoff_job,
    restart_held_jobs,
    restart_resume_job,
)
from src.server.database.runs.outbox import enqueue_hooks, release_deferred_notifications


async def read_resume_state(thread_id: str, run_id: str) -> Optional[Dict[str, Any]]:
    """The thread's owner, workspace, whether a turn may still come to it, its
    latest run, and the status, metadata and start of the run a resume job
    names, under their row names; None when the thread is gone."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT w.user_id, c.workspace_id,
                       (c.archived_at IS NOT NULL OR w.status = 'deleted') AS no_turn,
                       latest.conversation_response_id AS latest_run_id,
                       r.status, r.metadata, r.created_at
                FROM conversation_threads c
                JOIN workspaces w ON w.workspace_id = c.workspace_id
                LEFT JOIN LATERAL (
                    SELECT conversation_response_id FROM conversation_responses
                    WHERE conversation_thread_id = c.conversation_thread_id
                    ORDER BY run_seq DESC LIMIT 1
                ) latest ON true
                LEFT JOIN conversation_responses r
                  ON r.conversation_response_id = %s
                 AND r.conversation_thread_id = c.conversation_thread_id
                WHERE c.conversation_thread_id = %s
                """,
                (run_id, thread_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_restart_stopped_tasks(
    thread_id: str, since: datetime
) -> List[Dict[str, Any]]:
    """The thread's subagent tasks whose latest run a restart stopped at or
    after ``since``, with the turn that launched that run. A resume of one
    starts a newer run, which takes it off. A workflow run is never resumed."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT t.task_id, t.subagent_type, t.description, r.parent_run_id
                FROM subagent_tasks t
                JOIN subagent_runs r ON r.task_run_id = t.latest_run_id
                WHERE t.thread_id = %s
                  AND r.status = 'cancelled'
                  AND r.failure->>'error_type' = %s
                  AND t.subagent_type <> %s
                  AND r.finalized_at >= %s
                ORDER BY r.started_at
                """,
                (thread_id, RESTART_STOP_ERROR_TYPE, WORKFLOW_SUBAGENT_TYPE, since),
            )
            return [dict(row) for row in await cur.fetchall()]


async def has_unsettled_tasks(thread_id: str, started_before: datetime) -> bool:
    """Whether a subagent run the thread started before ``started_before`` is
    still running: one a restart stopped that has not settled its stop yet."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM subagent_runs
                    WHERE thread_id = %s AND status = 'in_progress' AND started_at < %s
                )
                """,
                (thread_id, started_before),
            )
            return bool((await cur.fetchone())[0])


async def enqueue_for_stopped_tasks(thread_id: str) -> None:
    """Record subagents a restart stopped on the thread, against its latest
    run: the same job that run's own finalize writes if the restart
    interrupted it too, so the thread gets one resume either way."""
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT conversation_response_id
                    FROM conversation_responses
                    WHERE conversation_thread_id = %s
                    ORDER BY run_seq DESC LIMIT 1
                    """,
                    (thread_id,),
                )
                latest = await cur.fetchone()
            if latest is None:
                return
            run_id = str(latest["conversation_response_id"])
            await enqueue_hooks(
                conn,
                run_id=run_id,
                thread_id=thread_id,
                jobs=[restart_resume_job(run_id=run_id, thread_id=thread_id)],
            )


async def settle_declined(thread_id: str, run_id: str, *, refire: bool = False) -> bool:
    """Settle a run whose resume will not come, when it is still the thread's
    last: it becomes an ordinary stop, which no later turn carries on, its
    parked reports are released, and what its finalize held back for the
    resume is written. With ``refire``, for a firing's turn stopped before
    its input reached the graph, the firing goes back to ``pending`` and
    runs its turn again (``automation_handoff_job``) instead of settling.
    False when a newer run settles the thread instead."""
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT * FROM conversation_responses
                    WHERE conversation_thread_id = %s
                    ORDER BY run_seq DESC LIMIT 1
                    FOR SHARE
                    """,
                    (thread_id,),
                )
                latest = await cur.fetchone()
            if latest is None or str(latest["conversation_response_id"]) != run_id:
                return False
            await release_deferred_notifications(conn, thread_id)
            if is_restart_interrupted(latest):
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE conversation_responses SET metadata = metadata - %s::text
                        WHERE conversation_response_id = %s
                        """,
                        (RESTART_INTERRUPTED_META, run_id),
                    )
                jobs = restart_held_jobs(latest)
                if refire and await _refire(conn, latest):
                    meta = latest.get("metadata") or {}
                    jobs = [j for j in jobs if j.hook_type != "automation_settle"]
                    jobs.append(
                        automation_handoff_job(
                            execution_id=str(meta["automation_execution_id"]),
                            automation_id=str(meta["automation_id"]),
                            user_id=str(meta["user_id"]),
                        )
                    )
                await enqueue_hooks(conn, run_id=run_id, thread_id=thread_id, jobs=jobs)
            return True


async def _refire(conn, run: Dict[str, Any]) -> bool:
    """Put the firing still running on ``run`` back to ``pending`` on the
    caller's transaction; False when nothing was left to put back."""
    execution_id = (run.get("metadata") or {}).get("automation_execution_id")
    if not execution_id:
        return False
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE automation_executions SET status = 'pending', heartbeat_at = NOW()
            WHERE automation_execution_id = %s AND status = 'running'
              AND conversation_response_id = %s
            """,
            (execution_id, run["conversation_response_id"]),
        )
        return cur.rowcount == 1
