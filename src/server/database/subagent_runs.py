"""Subagent run ledger (SQL layer) — the task-run mirror of turn_lifecycle.

A task run IS its subagent_runs row: born 'in_progress' before the writer
spawns, transitioned exactly once by the guarded finalize CAS. Writes are
admission-authoritative from day one — a unique violation here rejects the
spawn/resume; a ledger that tolerates conflicting writers is worse than none.
Constraint names map 1:1 to admission semantics: active slot, duplicate
launch call (checkpoint re-execution), claimed predecessor (resume race).
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from src.server.database import conversation as qr_db
from src.server.utils.pg_sanitize import SafeJson

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = ("completed", "interrupted", "error", "cancelled")


class SubagentRunError(Exception):
    """Base for task-run-ledger admission/finalize signals."""


class TaskRunSlotBusyError(SubagentRunError):
    """Another run is live for this task (uq_subagent_runs_active_slot)."""

    def __init__(
        self,
        thread_id: str,
        task_id: str,
        active_run: Optional[Dict[str, Any]] = None,
    ):
        self.thread_id = thread_id
        self.task_id = task_id
        self.active_run = active_run
        run_id = active_run.get("task_run_id") if active_run else "unknown"
        super().__init__(f"task {task_id} on thread {thread_id} has a live run ({run_id})")


class DuplicateLaunchError(SubagentRunError):
    """This (parent_run_id, launch_tool_call_id) already spawned a run —
    checkpoint re-execution of the same Task tool call."""

    def __init__(self, existing_run: Dict[str, Any]):
        self.existing_run = existing_run
        super().__init__(
            f"launch call {existing_run.get('launch_tool_call_id')} already "
            f"spawned run {existing_run.get('task_run_id')}"
        )


class PredecessorClaimedError(SubagentRunError):
    """The predecessor run already has a successor (resume race lost)."""

    def __init__(self, predecessor_run_id: str, successor: Optional[Dict[str, Any]] = None):
        self.predecessor_run_id = predecessor_run_id
        self.successor = successor
        run_id = successor.get("task_run_id") if successor else "unknown"
        super().__init__(
            f"run {predecessor_run_id} already resumed as {run_id}"
        )


class TaskRunNotFoundError(SubagentRunError):
    """No run row for the given task_run_id."""


class _AlreadyTerminal(Exception):
    """Internal control flow: guarded UPDATE matched zero rows."""


@asynccontextmanager
async def _ledger_connection(conn=None):
    """Yield the caller-pinned session as-is, or a pool connection."""
    if conn is not None:
        yield conn
        return
    async with qr_db.get_db_connection() as pool_conn:
        yield pool_conn


def _violated_constraint(exc: Exception) -> Optional[str]:
    diag = getattr(exc, "diag", None)
    return getattr(diag, "constraint_name", None) if diag else None


async def start_task_run(
    *,
    task_run_id: str,
    thread_id: str,
    task_id: str,
    cause: str,
    description: str = "",
    subagent_type: str = "general-purpose",
    parent_run_id: Optional[str] = None,
    launch_tool_call_id: Optional[str] = None,
    predecessor_run_id: Optional[str] = None,
    start_checkpoint_id: Optional[str] = None,
    conn=None,
) -> Dict[str, Any]:
    """The task-run START transaction: task upsert + in_progress run row +
    latest_run_id advance, atomically.

    Raises TaskRunSlotBusyError / DuplicateLaunchError /
    PredecessorClaimedError — each backed by a DB constraint, so two workers
    racing the same spawn cannot both win regardless of what they read.
    """
    conflict: Optional[str] = None

    try:
        async with _ledger_connection(conn) as conn:
            async with conn.transaction():
                # Fast-path dedup probe for checkpoint re-execution; the
                # partial unique index below is the race-safe backstop.
                if parent_run_id and launch_tool_call_id:
                    existing = await find_run_by_launch_call(
                        parent_run_id, launch_tool_call_id, conn=conn
                    )
                    if existing:
                        raise DuplicateLaunchError(existing)

                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO subagent_tasks (
                            thread_id, task_id, description, subagent_type
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (thread_id, task_id)
                        DO UPDATE SET updated_at = NOW()
                        """,
                        (thread_id, task_id, description, subagent_type),
                    )

                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        INSERT INTO subagent_runs (
                            task_run_id, thread_id, task_id, parent_run_id,
                            launch_tool_call_id, predecessor_run_id, cause,
                            start_checkpoint_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            task_run_id,
                            thread_id,
                            task_id,
                            parent_run_id,
                            launch_tool_call_id,
                            predecessor_run_id,
                            cause,
                            start_checkpoint_id,
                        ),
                    )
                    run_row = dict(await cur.fetchone())

                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE subagent_tasks
                        SET latest_run_id = %s, updated_at = NOW()
                        WHERE thread_id = %s AND task_id = %s
                        """,
                        (task_run_id, thread_id, task_id),
                    )

        logger.info(
            f"[subagent_runs] START task_run={task_run_id} thread={thread_id} "
            f"task={task_id} cause={cause} predecessor={predecessor_run_id}"
        )
        return run_row

    except psycopg.errors.UniqueViolation as e:
        conflict = _violated_constraint(e)
        if conflict not in (
            "uq_subagent_runs_active_slot",
            "uq_subagent_runs_launch_call",
            "uq_subagent_runs_predecessor",
        ):
            raise

    # The aborted transaction has rolled back; classify with fresh reads.
    if conflict == "uq_subagent_runs_launch_call":
        existing = await find_run_by_launch_call(parent_run_id, launch_tool_call_id)
        if existing:
            raise DuplicateLaunchError(existing)
        raise SubagentRunError(
            f"launch-call collision vanished for {launch_tool_call_id}"
        )
    if conflict == "uq_subagent_runs_active_slot":
        raise TaskRunSlotBusyError(
            thread_id, task_id, await get_active_task_run(thread_id, task_id)
        )
    raise PredecessorClaimedError(
        str(predecessor_run_id), await find_successor_run(str(predecessor_run_id))
    )


async def _enqueue_report_back_job(conn, run_row: Dict[str, Any]) -> None:
    """Insert the run's task_report_back outbox job on the finalize txn.

    Keyed per (parent run, task, task_run) — the same shape the collector
    enqueue used, so any transitional double-observation dedups on the
    idempotency index. Pointer style only: the executor derives the result
    from the durable archive; nothing volatile rides the payload.
    """
    parent_run_id = run_row.get("parent_run_id")
    if not parent_run_id:
        return
    from src.server.database import hook_outbox as outbox_db

    task_id = str(run_row["task_id"])
    task_run_id = str(run_row["task_run_id"])
    final_pin = run_row.get("final_checkpoint_id")
    await outbox_db.enqueue_compensation_job(
        run_id=str(parent_run_id),
        thread_id=str(run_row["thread_id"]),
        hook_type="task_report_back",
        payload={
            "task_id": task_id,
            "task_run_id": task_run_id,
            "display_id": f"Task-{task_id}",
            "subagent_type": str(run_row.get("subagent_type") or "subagent"),
            "description": str(run_row.get("description") or "")[:500],
            "style": "pointer",
            "final_checkpoint_id": str(final_pin) if final_pin else None,
        },
        ordering_key=str(run_row["thread_id"]),
        idempotency_key=(
            f"{parent_run_id}:task:{task_id}:{task_run_id}:report_back"
        ),
        conn=conn,
    )


async def finalize_task_run(
    *,
    task_run_id: str,
    status: str,
    failure: Optional[Dict[str, Any]] = None,
    final_checkpoint_id: Optional[str] = None,
    conn=None,
) -> Dict[str, Any]:
    """Exactly one CAS from in_progress to terminal.

    Committed cancel intent is authoritative (same I3 semantics as root runs):
    a row stamped with cancel_requested_at before this CAS lands finalizes as
    'cancelled' regardless of the requested status — read the terminal status
    from the returned row. Raises _AlreadyTerminal internally; use
    finalize_task_run_idempotent for the applied-flag surface.
    """
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"finalize_task_run: {status!r} is not a terminal status")

    async with _ledger_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE subagent_runs
                    SET status = CASE
                            WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
                            ELSE %s
                        END,
                        cancel_requested_at = CASE
                            WHEN cancel_requested_at IS NULL AND %s = 'cancelled'
                                THEN NOW()
                            ELSE cancel_requested_at
                        END,
                        failure = COALESCE(%s::jsonb, failure),
                        final_checkpoint_id = COALESCE(%s, final_checkpoint_id),
                        finalized_at = NOW()
                    WHERE task_run_id = %s AND status = 'in_progress'
                    RETURNING *
                    """,
                    (
                        status,
                        status,
                        SafeJson(failure) if failure is not None else None,
                        final_checkpoint_id,
                        task_run_id,
                    ),
                )
                run_row = await cur.fetchone()
            if run_row is not None and run_row["status"] == "completed":
                # Report-back owed ⟺ run completed: the outbox row commits
                # with the terminal CAS, so no crash window can lose the
                # notification or record it against a run that never
                # terminalized. Eligibility (already delivered, parent
                # still live/interrupted) is the executor's call at claim
                # time, against the ledger — not decided here.
                await _enqueue_report_back_job(conn, dict(run_row))

    if run_row is None:
        raise _AlreadyTerminal()

    run_row = dict(run_row)
    final_status = run_row["status"]
    if final_status != status:
        logger.info(
            f"[subagent_runs] durable cancel intent overrode finalize for "
            f"task_run={task_run_id}: {status} -> {final_status}"
        )
    logger.info(
        f"[subagent_runs] FINALIZE task_run={task_run_id} status={final_status}"
    )
    return run_row


async def finalize_task_run_idempotent(**kwargs) -> Dict[str, Any]:
    """finalize_task_run, mapping the zero-row CAS to the survivor row.

    Returns {"applied": bool, "run": row}.
    """
    try:
        run = await finalize_task_run(**kwargs)
        return {"applied": True, "run": run}
    except _AlreadyTerminal:
        run = await get_task_run(kwargs["task_run_id"])
        if run is None:
            raise TaskRunNotFoundError(kwargs["task_run_id"])
        logger.info(
            f"[subagent_runs] finalize no-op: task_run={kwargs['task_run_id']} "
            f"already {run['status']} (wanted {kwargs['status']})"
        )
        return {"applied": False, "run": run}


async def request_task_run_cancel(
    task_run_id: str, thread_id: Optional[str] = None
) -> Dict[str, Any]:
    """Durable cancel intent; idempotent, never a recorded losing cancel.

    Two stamp laps for the START-commit visibility race (see
    turn_lifecycle.request_run_cancel). Returns {"state": "requested"|
    "already_requested"|"already_terminal"|"not_found", "run": row|None}.
    """
    sql = """
        UPDATE subagent_runs
        SET cancel_requested_at = NOW()
        WHERE task_run_id = %s
          AND status = 'in_progress'
          AND cancel_requested_at IS NULL
    """
    params: list = [task_run_id]
    if thread_id is not None:
        sql += "  AND thread_id = %s\n"
        params.append(thread_id)
    sql += "        RETURNING *"

    run: Optional[Dict[str, Any]] = None
    for _lap in range(2):
        async with qr_db.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                row = await cur.fetchone()
                if row:
                    return {"state": "requested", "run": dict(row)}

        run = await get_task_run(task_run_id)
        if run is None or (
            thread_id is not None and str(run["thread_id"]) != thread_id
        ):
            return {"state": "not_found", "run": None}
        if run["status"] != "in_progress":
            return {"state": "already_terminal", "run": run}
        if run.get("cancel_requested_at"):
            return {"state": "already_requested", "run": run}
    return {"state": "already_requested", "run": run}


async def mark_result_delivered(task_run_id: str) -> bool:
    """Stamp result_delivered_at once, on terminal rows only. Delivery
    legitimately trails the terminal CAS (the trigger permits it) — but a
    still-in_progress row (a transiently failed finalize) must not be
    stamped: recovery may yet terminalize it, and a delivered-but-open run
    is a contradiction the readers would have to special-case."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE subagent_runs
                SET result_delivered_at = NOW()
                WHERE task_run_id = %s
                  AND result_delivered_at IS NULL
                  AND status = ANY(%s)
                """,
                (task_run_id, list(TERMINAL_STATUSES)),
            )
            return cur.rowcount > 0


async def get_task_run(task_run_id: str) -> Optional[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM subagent_runs WHERE task_run_id = %s",
                (task_run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_active_task_run(
    thread_id: str, task_id: str
) -> Optional[Dict[str, Any]]:
    """The task's live run, if any — one row by the slot index."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE thread_id = %s AND task_id = %s AND status = 'in_progress'
                """,
                (thread_id, task_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def find_run_by_launch_call(
    parent_run_id: str, launch_tool_call_id: str, conn=None
) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT * FROM subagent_runs
        WHERE parent_run_id = %s AND launch_tool_call_id = %s
    """
    params = (parent_run_id, launch_tool_call_id)
    async with _ledger_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            row = await cur.fetchone()
            return dict(row) if row else None


async def find_successor_run(predecessor_run_id: str) -> Optional[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM subagent_runs WHERE predecessor_run_id = %s",
                (predecessor_run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_task_runs(thread_id: str, task_id: str) -> List[Dict[str, Any]]:
    """The task's full run chain, oldest first."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE thread_id = %s AND task_id = %s
                ORDER BY started_at
                """,
                (thread_id, task_id),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def list_runs_for_thread(thread_id: str) -> List[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE thread_id = %s
                ORDER BY started_at
                """,
                (thread_id,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def list_open_runs_for_thread(thread_id: str) -> List[Dict[str, Any]]:
    """The thread's live runs, oldest first — the durable discovery backstop
    for consumers whose Redis-side signal (active set, meta hash) can lapse
    while the run is still open."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE thread_id = %s AND status = 'in_progress'
                ORDER BY started_at
                """,
                (thread_id,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def count_open_runs_for_workspace(workspace_id: str) -> int:
    """Live task runs across every thread of the workspace — the durable
    signal that a background subagent still needs the sandbox, regardless of
    which worker owns it or whether any root run is active."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*)
                FROM subagent_runs r
                JOIN conversation_threads t
                  ON t.conversation_thread_id = r.thread_id
                WHERE t.workspace_id = %s AND r.status = 'in_progress'
                """,
                (workspace_id,),
            )
            return (await cur.fetchone())[0]


async def list_open_task_runs() -> List[Dict[str, Any]]:
    """All in_progress task runs, oldest first (orphan-recovery scan input)."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE status = 'in_progress'
                ORDER BY started_at
                """
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_latest_run_statuses(
    thread_id: str, task_ids: List[str]
) -> Dict[str, str]:
    """task_id -> latest run's status, for tasks that HAVE a ledgered run.

    Tasks absent from the result (no task row, or dangling latest_run_id)
    are pre-ledger / shadow-damaged — callers fall back to legacy inference.
    """
    if not task_ids:
        return {}
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT t.task_id, r.status
                FROM subagent_tasks t
                JOIN subagent_runs r ON r.task_run_id = t.latest_run_id
                WHERE t.thread_id = %s AND t.task_id = ANY(%s)
                """,
                (thread_id, list(task_ids)),
            )
            rows = await cur.fetchall()
            return {str(r["task_id"]): str(r["status"]) for r in rows}


# ------------------------------------------------------------------ repair

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


async def repair_task_chains(thread_id: str, conn=None) -> Dict[str, int]:
    """Re-anchor a thread's task rows to their surviving runs.

    Deleting conversation_responses cascades their subagent_runs away, which
    can strand a task pointing at nothing (rewind to the newest survivor) or
    with nothing left at all (delete the row). Left unrepaired, a later resume
    reads the empty pointer and starts an unchained run with no predecessor
    and no start pin. Idempotent and safe on any thread — a healthy thread
    matches neither statement — so truncation paths can call it unconditionally
    inside their own transaction.
    """
    async with _ledger_connection(conn) as conn:
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
                WHERE t.thread_id = %s
                  AND {_DANGLING_LATEST}
                  AND {_HAS_SURVIVING_RUN}
                """,
                (thread_id,),
            )
            rewound = cur.rowcount

            await cur.execute(
                f"""
                DELETE FROM subagent_tasks t
                WHERE t.thread_id = %s
                  AND NOT {_HAS_SURVIVING_RUN}
                """,
                (thread_id,),
            )
            deleted = cur.rowcount

    if rewound or deleted:
        logger.info(
            f"[subagent_runs] REPAIR thread={thread_id} rewound={rewound} "
            f"deleted={deleted}"
        )
    return {"rewound": rewound, "deleted": deleted}


async def repair_dangling_task_chains(
    min_age_seconds: int = SWEEP_MIN_AGE_SECONDS,
) -> Dict[str, int]:
    """The global, thread-agnostic form of repair_task_chains.

    Heals damage that predates the transactional rewind — the shadow-deploy
    window, and any truncation path that ever escapes the guard. Drives off
    the dangling-pointer anti-join so a healthy ledger scans a small table and
    updates nothing.
    """
    async with qr_db.get_db_connection() as conn:
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
                WHERE t.updated_at < NOW() - MAKE_INTERVAL(secs => %s)
                  AND {_DANGLING_LATEST}
                  AND {_HAS_SURVIVING_RUN}
                """,
                (min_age_seconds,),
            )
            rewound = cur.rowcount

            await cur.execute(
                f"""
                DELETE FROM subagent_tasks t
                WHERE t.updated_at < NOW() - MAKE_INTERVAL(secs => %s)
                  AND NOT {_HAS_SURVIVING_RUN}
                """,
                (min_age_seconds,),
            )
            deleted = cur.rowcount

    return {"rewound": rewound, "deleted": deleted}


# ------------------------------------------------------------ mutation guard


async def count_open_runs_for_responses(
    thread_id: str, response_ids: List[str], conn=None
) -> int:
    """Live runs dispatched by any of these response rows.

    Deleting those rows would cascade the runs away under their live
    executors, so a mutation that plans to must refuse first.
    """
    if not response_ids:
        return 0
    async with _ledger_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*) FROM subagent_runs
                WHERE thread_id = %s
                  AND status = 'in_progress'
                  AND parent_run_id = ANY(%s)
                """,
                (thread_id, [str(r) for r in response_ids]),
            )
            return (await cur.fetchone())[0]


async def find_open_run_from_turn(
    thread_id: str, from_turn_index: int, conn=None
) -> Optional[Dict[str, Any]]:
    """One live run whose dispatching response sits at or past the fork cut.

    The same guard as count_open_runs_for_responses, expressed for the fork
    path: it knows the turn cut rather than the row ids, and resolving those
    separately would race the truncation it performs in the same transaction.
    Returns the offending row so the refusal can name it.
    """
    async with _ledger_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_runs
                WHERE thread_id = %s
                  AND status = 'in_progress'
                  AND parent_run_id IN (
                        SELECT conversation_response_id
                        FROM conversation_responses
                        WHERE conversation_thread_id = %s
                          AND turn_index >= %s
                    )
                ORDER BY started_at
                LIMIT 1
                """,
                (thread_id, thread_id, from_turn_index),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def count_open_runs_for_thread(thread_id: str, conn=None) -> int:
    """Every live run on the thread — the full-delete case, where the cascade
    reaches all of them regardless of which response dispatched them."""
    async with _ledger_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*) FROM subagent_runs
                WHERE thread_id = %s AND status = 'in_progress'
                """,
                (thread_id,),
            )
            return (await cur.fetchone())[0]


async def get_task(thread_id: str, task_id: str) -> Optional[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_tasks
                WHERE thread_id = %s AND task_id = %s
                """,
                (thread_id, task_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_tasks_for_thread(thread_id: str) -> List[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM subagent_tasks
                WHERE thread_id = %s
                ORDER BY created_at
                """,
                (thread_id,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
