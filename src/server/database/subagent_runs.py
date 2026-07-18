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
