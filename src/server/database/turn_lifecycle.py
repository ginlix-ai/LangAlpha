"""Turn lifecycle v4 — the durable run ledger (SQL layer).

A run IS its conversation_responses row: born 'in_progress' in the START
transaction, transitioned exactly once by the guarded finalize CAS. The
partial unique index uq_responses_in_progress_slot makes that row the
single live-run slot per thread; constraint violations here are admission
semantics, not errors to retry. All writes that must be atomic with a run
transition (query row, thread projection, usage, outbox jobs, provenance)
happen inside these two transactions and nowhere else.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import psycopg
from psycopg.rows import dict_row

from src.server.database import conversation as qr_db
from src.server.utils.pg_sanitize import SafeJson

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = ("completed", "interrupted", "error", "cancelled")


class TurnLifecycleError(Exception):
    """Base for run-ledger admission/finalize signals."""


class RunSlotBusyError(TurnLifecycleError):
    """Another run is live on this thread (uq_responses_in_progress_slot)."""

    def __init__(self, thread_id: str, active_run: Optional[Dict[str, Any]] = None):
        self.thread_id = thread_id
        self.active_run = active_run
        run_id = active_run.get("conversation_response_id") if active_run else "unknown"
        super().__init__(f"thread {thread_id} has a live run ({run_id})")


class DuplicateRequestError(TurnLifecycleError):
    """This request_key already produced a run — the caller retransmitted."""

    def __init__(self, existing_run: Dict[str, Any]):
        self.existing_run = existing_run
        super().__init__(
            f"request_key {existing_run.get('request_key')} already ran as "
            f"run {existing_run.get('conversation_response_id')}"
        )


class AttemptConflictError(TurnLifecycleError):
    """A concurrent retry claimed this (thread, turn, attempt) or predecessor."""


class RunNotFoundError(TurnLifecycleError):
    """No run row for the given run_id."""


@dataclass
class HookJob:
    """One outbox row: a post-commit effect that must survive a crash."""

    hook_type: str
    idempotency_key: str
    payload: Dict[str, Any] = field(default_factory=dict)
    ordering_key: Optional[str] = None


def build_finalize_jobs(
    *,
    run_id: str,
    thread_id: str,
    msg_type: str,
    user_id: Optional[str] = None,
    burst_slot_id: Optional[str] = None,
    report_back_ptc_thread_id: Optional[str] = None,
) -> Callable[[str], List[HookJob]]:
    """The one decision table mapping a run's final status to its hook jobs.

    Returned callable is invoked inside the finalize transaction with the
    CAS-adopted final status (a durable cancel may have overridden the
    requested one). Ordering keys serialize all jobs touching one PTC
    thread's report-back lifecycle, so a completed run's report_back can
    never be overtaken by a later watch_clear. Pure and synchronous — it
    runs inside the finalize transaction and must not do I/O.
    """

    def _jobs(final_status: str) -> List[HookJob]:
        jobs: List[HookJob] = []
        is_ptc = msg_type == "ptc"
        rb_ptc = report_back_ptc_thread_id or thread_id

        if user_id and burst_slot_id:
            jobs.append(
                HookJob(
                    hook_type="burst_release",
                    idempotency_key=f"{run_id}:burst_release",
                    payload={"user_id": user_id, "slot_id": burst_slot_id},
                )
            )

        if final_status == "completed" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="report_back",
                    idempotency_key=f"{run_id}:report_back",
                    payload={"ptc_thread_id": thread_id},
                    ordering_key=thread_id,
                )
            )
        elif final_status == "interrupted" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="needs_input_wake",
                    idempotency_key=f"{run_id}:needs_input_wake",
                    payload={"ptc_thread_id": thread_id},
                    ordering_key=thread_id,
                )
            )

        if final_status in ("error", "cancelled") or (
            final_status == "completed"
            and not is_ptc
            and report_back_ptc_thread_id
        ):
            # error/cancelled: tear down any watch this run held open (a
            # dispatched PTC directly, a report-back flash run via its
            # origin id). completed flash WITH an origin id: consumption
            # clear — the report-back summary landed, release the watch.
            jobs.append(
                HookJob(
                    hook_type="watch_clear",
                    idempotency_key=f"{run_id}:watch_clear",
                    payload={
                        "ptc_thread_id": rb_ptc,
                        "user_id": user_id,
                        "error_wake": final_status in ("error", "cancelled"),
                    },
                    ordering_key=rb_ptc,
                )
            )
        return jobs

    return _jobs


def build_finalize_jobs_from_run_row(
    run: Dict[str, Any],
) -> Callable[[str], List[HookJob]]:
    """Factory from the durable row alone: everything the decision table
    needs was stamped into run metadata at START. This is finalize_run's
    DEFAULT — no finalize path can skip required terminal effects (I5)."""
    meta = run.get("metadata") or {}
    return build_finalize_jobs(
        run_id=str(run["conversation_response_id"]),
        thread_id=str(run["conversation_thread_id"]),
        msg_type=meta.get("msg_type") or "ptc",
        user_id=meta.get("user_id"),
        burst_slot_id=meta.get("burst_slot_id"),
        report_back_ptc_thread_id=meta.get("report_back_ptc_thread_id"),
    )


@dataclass
class QuerySpec:
    """The user-input row to write in the START txn (None for retries/replays)."""

    query_id: str
    content: str
    query_type: str
    feedback_action: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


@dataclass
class FinalizeResult:
    applied: bool  # False = run was already terminal (idempotent no-op)
    run: Optional[Dict[str, Any]] = None


async def allocate_turn_index(conn, thread_id: str) -> int:
    """Next turn = MAX+1 across queries ∪ responses (gap-robust, unlike COUNT)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(turn_index) FROM conversation_queries
                          WHERE conversation_thread_id = %s), -1),
                COALESCE((SELECT MAX(turn_index) FROM conversation_responses
                          WHERE conversation_thread_id = %s), -1)
            ) + 1 AS next_turn
            """,
            (thread_id, thread_id),
        )
        row = await cur.fetchone()
        return row["next_turn"]


def _violated_constraint(exc: Exception) -> Optional[str]:
    diag = getattr(exc, "diag", None)
    return getattr(diag, "constraint_name", None) if diag else None


async def start_run(
    *,
    run_id: str,
    thread_id: str,
    request_key: str,
    turn_index: Optional[int] = None,
    attempt_no: int = 1,
    retry_of_run_id: Optional[str] = None,
    query: Optional[QuerySpec] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """The START transaction: query row + in_progress run row + thread projection.

    Returns the run row. Raises DuplicateRequestError / RunSlotBusyError /
    AttemptConflictError — each backed by a DB constraint, so two workers
    racing the same admission cannot both win regardless of what they read.
    """
    created_at = created_at or datetime.now(timezone.utc)
    conflict: Optional[str] = None

    try:
        async with qr_db.get_db_connection() as conn:
            async with conn.transaction():
                # Fast-path dedup probe. The unique index below is the
                # race-safe backstop; this just avoids burning a turn_index.
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        "SELECT * FROM conversation_responses WHERE request_key = %s",
                        (request_key,),
                    )
                    existing = await cur.fetchone()
                    if existing:
                        raise DuplicateRequestError(dict(existing))

                if turn_index is None:
                    turn_index = await allocate_turn_index(conn, thread_id)

                if query is not None:
                    await qr_db.create_query(
                        conversation_query_id=query.query_id,
                        conversation_thread_id=thread_id,
                        turn_index=turn_index,
                        content=query.content,
                        query_type=query.query_type,
                        feedback_action=query.feedback_action,
                        metadata=query.metadata,
                        created_at=query.created_at or created_at,
                        conn=conn,
                    )

                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        INSERT INTO conversation_responses (
                            conversation_response_id, conversation_thread_id,
                            turn_index, status, metadata, created_at,
                            attempt_no, retry_of_run_id, request_key
                        )
                        VALUES (%s, %s, %s, 'in_progress', %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            run_id,
                            thread_id,
                            turn_index,
                            SafeJson(metadata or {}),
                            created_at,
                            attempt_no,
                            retry_of_run_id,
                            request_key,
                        ),
                    )
                    run_row = dict(await cur.fetchone())

                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE conversation_threads
                        SET current_status = 'in_progress', updated_at = NOW()
                        WHERE conversation_thread_id = %s
                        """,
                        (thread_id,),
                    )

        logger.info(
            f"[turn_lifecycle] START run={run_id} thread={thread_id} "
            f"turn={turn_index} attempt={attempt_no}"
        )
        return run_row

    except psycopg.errors.UniqueViolation as e:
        conflict = _violated_constraint(e)
        if conflict not in (
            "uq_responses_in_progress_slot",
            "uq_responses_request_key",
            "uq_responses_thread_turn_attempt",
            "uq_responses_retry_of",
        ):
            raise

    # The aborted transaction has rolled back; classify the conflict with
    # fresh reads on a clean connection.
    if conflict == "uq_responses_request_key":
        existing = await find_run_by_request_key(request_key)
        if existing:
            raise DuplicateRequestError(existing)
        raise AttemptConflictError(f"request_key collision vanished for {request_key}")
    if conflict == "uq_responses_in_progress_slot":
        raise RunSlotBusyError(thread_id, await get_active_run(thread_id))
    raise AttemptConflictError(
        f"concurrent attempt on thread={thread_id} turn={turn_index} ({conflict})"
    )


async def finalize_run(
    *,
    run_id: str,
    thread_id: str,
    status: str,
    interrupt_reason: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
    execution_time: Optional[float] = None,
    sse_events: Optional[List[Dict[str, Any]]] = None,
    checkpoint_id: Optional[str] = None,
    usage_writer: Optional[Callable[[Any, str], Awaitable[None]]] = None,
    outbox_jobs: Optional[
        Union[List[HookJob], Callable[[str], List[HookJob]]]
    ] = None,
) -> FinalizeResult:
    """The finalize transaction: exactly one CAS from in_progress to terminal.

    Zero rows from the guarded UPDATE means someone else already finalized —
    the caller lost an intended race (cancel vs owner, janitor vs owner) and
    gets applied=False with the surviving row; nothing else is written.
    Committed cancel intent is authoritative: a row stamped with
    cancel_requested_at before this CAS lands finalizes as 'cancelled'
    regardless of the requested status (I3: the durable cancel that locks
    the row first wins — the row lock linearizes cancel vs finalize).
    Callers must read the terminal status from the returned row, not from
    what they asked for. Pre-existing intent stamps metadata.cancelled_by_user
    regardless of the requested status (intent only ever comes from a user
    /cancel); a requested 'cancelled' finalize backfills cancel_requested_at
    so terminal cancelled rows are self-consistent — on terminal rows the
    flag is the user-provenance marker, the timestamp only the decision time. usage_writer runs inside the transaction on the
    same connection so a persist failure aborts the terminal transition
    with it (no more swallowed-exception zombie turns).
    """
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"finalize_run: {status!r} is not a terminal status")

    async with qr_db.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE conversation_responses
                    SET status = CASE
                            WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
                            ELSE %s
                        END,
                        interrupt_reason = CASE
                            WHEN cancel_requested_at IS NOT NULL THEN NULL
                            ELSE %s
                        END,
                        cancel_requested_at = CASE
                            WHEN cancel_requested_at IS NULL AND %s = 'cancelled'
                                THEN NOW()
                            ELSE cancel_requested_at
                        END,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                            || CASE
                                WHEN cancel_requested_at IS NOT NULL
                                    THEN '{"cancelled_by_user": true}'::jsonb
                                ELSE '{}'::jsonb
                            END,
                        warnings = %s,
                        errors = %s,
                        execution_time = %s,
                        sse_events = %s
                    WHERE conversation_response_id = %s AND status = 'in_progress'
                    RETURNING *
                    """,
                    (
                        status,
                        interrupt_reason,
                        status,
                        SafeJson(metadata or {}),
                        warnings or [],
                        errors or [],
                        execution_time,
                        SafeJson(sse_events) if sse_events else None,
                        run_id,
                    ),
                )
                run_row = await cur.fetchone()

            if run_row is None:
                # Lost the race (or bad id) — decide outside this txn.
                raise _AlreadyTerminal()

            run_row = dict(run_row)
            final_status = run_row["status"]
            if final_status != status:
                logger.info(
                    f"[turn_lifecycle] durable cancel intent overrode finalize "
                    f"for run={run_id}: {status} -> {final_status}"
                )

            # The helpers below historically swallow failures (return False /
            # catch-all). Inside this transaction a swallowed SQL error leaves
            # the txn aborted, and Postgres turns the commit into a silent
            # rollback — finalize would report applied=True with nothing
            # written. Every helper result is therefore checked, and the
            # transaction status is verified before commit as a backstop.
            if not await qr_db.update_thread_status(
                thread_id, final_status, checkpoint_id=checkpoint_id, conn=conn
            ):
                raise RuntimeError(
                    f"thread projection update failed for thread={thread_id}"
                )

            await qr_db._sync_provenance_for_response(
                conn,
                conversation_response_id=run_id,
                conversation_thread_id=thread_id,
                turn_index=run_row["turn_index"],
                sse_events=sse_events,
                strict=True,
            )

            if usage_writer is not None:
                await usage_writer(conn, final_status)

            # Job selection must see the CAS-ADOPTED status, not the requested
            # one (durable cancel intent can flip error->cancelled), so a
            # callable is invoked here, inside the transaction, after the CAS.
            jobs = outbox_jobs(final_status) if callable(outbox_jobs) else outbox_jobs
            if not jobs:
                # Default (I5): every finalize path — fail_open, error
                # funnels, sweep, fallback — gets the required terminal
                # effects derived from the row's START-stamped metadata;
                # no caller can forget to attach them.
                jobs = build_finalize_jobs_from_run_row(run_row)(final_status)
            if jobs:
                await enqueue_hooks(
                    conn, run_id=run_id, thread_id=thread_id, jobs=jobs
                )

            if conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
                raise RuntimeError(
                    f"finalize transaction for run={run_id} poisoned by a "
                    f"swallowed SQL error"
                )

    logger.info(
        f"[turn_lifecycle] FINALIZE run={run_id} thread={thread_id} "
        f"status={final_status}"
    )
    return FinalizeResult(applied=True, run=run_row)


class _AlreadyTerminal(Exception):
    """Internal control flow: guarded UPDATE matched zero rows."""


async def finalize_run_idempotent(**kwargs) -> FinalizeResult:
    """finalize_run, mapping the zero-row CAS to applied=False + survivor row."""
    try:
        return await finalize_run(**kwargs)
    except _AlreadyTerminal:
        run = await get_run(kwargs["run_id"])
        if run is None:
            raise RunNotFoundError(kwargs["run_id"])
        logger.info(
            f"[turn_lifecycle] finalize no-op: run={kwargs['run_id']} already "
            f"{run['status']} (wanted {kwargs['status']})"
        )
        return FinalizeResult(applied=False, run=run)


async def request_run_cancel(
    run_id: str, thread_id: Optional[str] = None
) -> Dict[str, Any]:
    """Durable cancel intent: honest, idempotent, never a recorded losing cancel.

    ``thread_id`` scopes the write so a caller-supplied run_id can't stamp
    intent on another thread's run. Returns {"state": "requested"|
    "already_requested"|"already_terminal"|"not_found", "run": row|None}.

    Two stamp laps: a cancel racing the START commit can run its guarded
    UPDATE before the row is visible (zero rows), then read the committed
    row as in_progress — one lap would misreport that as already_requested
    while no intent ever landed. The second lap stamps the now-visible row.
    """
    sql = """
        UPDATE conversation_responses
        SET cancel_requested_at = NOW()
        WHERE conversation_response_id = %s
          AND status = 'in_progress'
          AND cancel_requested_at IS NULL
    """
    params: list = [run_id]
    if thread_id is not None:
        sql += "  AND conversation_thread_id = %s\n"
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

        run = await get_run(run_id)
        if run is None or (
            thread_id is not None and str(run["conversation_thread_id"]) != thread_id
        ):
            return {"state": "not_found", "run": None}
        if run["status"] != "in_progress":
            return {"state": "already_terminal", "run": run}
        if run.get("cancel_requested_at"):
            return {"state": "already_requested", "run": run}
        # in_progress with NULL intent, yet our UPDATE matched nothing:
        # the START-commit visibility race — go around once more.
    return {"state": "already_requested", "run": run}


async def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM conversation_responses WHERE conversation_response_id = %s",
                (run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_active_run(thread_id: str) -> Optional[Dict[str, Any]]:
    """The thread's live run, if any — one row by the slot index."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM conversation_responses
                WHERE conversation_thread_id = %s AND status = 'in_progress'
                """,
                (thread_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_latest_attempt(thread_id: str) -> Optional[Dict[str, Any]]:
    """The thread's most recent attempt row — /retry's validation target."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM conversation_responses
                WHERE conversation_thread_id = %s
                ORDER BY turn_index DESC, attempt_no DESC
                LIMIT 1
                """,
                (thread_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_open_runs() -> List[Dict[str, Any]]:
    """All in_progress runs, oldest first (Phase-1 startup sweep input)."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM conversation_responses
                WHERE status = 'in_progress'
                ORDER BY created_at
                """
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def find_run_by_request_key(request_key: str) -> Optional[Dict[str, Any]]:
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM conversation_responses WHERE request_key = %s",
                (request_key,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def append_sse_events_patch(
    run_id: str, events: List[Dict[str, Any]], conn=None
) -> bool:
    """Run-keyed JSONB append to a (possibly terminal) row's archive.

    The lifecycle trigger permits this on terminal rows; it is the only
    legal post-terminal write (late subagent collectors, salvage).
    """
    sql = """
        UPDATE conversation_responses
        SET sse_events = COALESCE(sse_events, '[]'::jsonb) || %s::jsonb
        WHERE conversation_response_id = %s
    """
    params = (SafeJson(events), run_id)
    if conn is not None:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount > 0
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount > 0


async def enqueue_hooks(
    conn, *, run_id: str, thread_id: str, jobs: List[HookJob]
) -> None:
    """Write outbox rows on the caller's (finalize) transaction.

    ON CONFLICT DO NOTHING on idempotency_key: re-finalizing after a lost
    race never double-registers an effect.
    """
    async with conn.cursor() as cur:
        for job in jobs:
            await cur.execute(
                """
                INSERT INTO hook_outbox (
                    run_id, conversation_thread_id, hook_type,
                    payload, ordering_key, idempotency_key
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (
                    run_id,
                    thread_id,
                    job.hook_type,
                    SafeJson(job.payload),
                    job.ordering_key,
                    job.idempotency_key,
                ),
            )


async def claim_outbox_jobs(
    limit: int = 10, lease_seconds: int = 60
) -> List[Dict[str, Any]]:
    """Lease a batch of due jobs: pending-and-due, or claimed with an
    expired lease (crash/stall recovery — reclaim IS the startup recovery).

    Ordering keys serialize per key: a job stays invisible while an earlier
    open (pending/claimed) job shares its key, so a stuck head blocks its
    chain but nothing else. The claim commits before execution; attempts
    counts leases handed out, not completions.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT o.hook_outbox_id FROM hook_outbox o
                    WHERE (
                        (o.status = 'pending'
                         AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW()))
                        OR (o.status = 'claimed' AND o.lease_expires_at <= NOW())
                    )
                    AND (
                        o.ordering_key IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM hook_outbox p
                            WHERE p.ordering_key = o.ordering_key
                              AND p.status IN ('pending', 'claimed')
                              AND (p.created_at, p.hook_outbox_id)
                                  < (o.created_at, o.hook_outbox_id)
                        )
                    )
                    ORDER BY o.created_at, o.hook_outbox_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE hook_outbox h
                SET status = 'claimed',
                    attempts = h.attempts + 1,
                    lease_expires_at = NOW() + make_interval(secs => %s)
                FROM due
                WHERE h.hook_outbox_id = due.hook_outbox_id
                RETURNING h.*
                """,
                (limit, float(lease_seconds)),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def ack_outbox_job(job_id: str) -> bool:
    """Mark a claimed job done. False = the row wasn't ours anymore
    (lease expired and another drainer took it) — never re-open it."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'done', completed_at = NOW()
                WHERE hook_outbox_id = %s AND status = 'claimed'
                """,
                (job_id,),
            )
            return cur.rowcount > 0


async def nack_outbox_job(job_id: str, *, max_attempts: int = 5) -> Optional[str]:
    """Return a failed claimed job to pending with exponential backoff,
    or park it dead at max_attempts. Returns the new status, or None if
    the row wasn't claimed (lost lease)."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = CASE WHEN attempts >= %s THEN 'dead' ELSE 'pending' END,
                    next_retry_at = NOW() + make_interval(
                        secs => LEAST(POWER(2, attempts), 60)::float
                    ),
                    lease_expires_at = NULL
                WHERE hook_outbox_id = %s AND status = 'claimed'
                RETURNING status
                """,
                (max_attempts, job_id),
            )
            row = await cur.fetchone()
            return row["status"] if row else None
