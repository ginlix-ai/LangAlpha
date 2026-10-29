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
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from src.server.database import conversation as qr_db
from src.server.database import order_attempts as oa_db
from src.server.database import pool
from src.server.database import thread_transcripts as tt_db
from src.server.database.runs import subagent_repair
from src.server.database.runs import subagent_runs as sr_db
from src.server.database.runs.outbox import (
    build_finalize_jobs_from_run_row,
    cancel_run_steers,
    count_held_user_steers,
    enqueue_hooks,
    land_fork_steers,
    lift_fork_steers,
    list_returned_steers,
    release_deferred_notifications,
    settle_run_steers,
    steer_ordering_key,
)
from src.server.contracts.status import TERMINAL_STATUSES
from src.server.database.provenance import MAIN_PROVENANCE_LANE
from src.server.database.runs.hook_jobs import is_unchanged_quiet
from src.server.utils.pg_sanitize import SafeJson, normalize_uuid

logger = logging.getLogger(__name__)


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
    """This request_key already produced a run — the caller retransmitted.
    ``steered`` when its message went into that run as a steer."""

    def __init__(self, existing_run: Dict[str, Any], steered: bool = False):
        self.existing_run = existing_run
        self.steered = steered
        super().__init__(
            f"request_key {existing_run.get('request_key')} already ran as "
            f"run {existing_run.get('conversation_response_id')}"
        )


class AttemptConflictError(TurnLifecycleError):
    """A concurrent retry claimed this (thread, turn, attempt) or predecessor."""


class RunNotFoundError(TurnLifecycleError):
    """No run row for the given run_id."""


class StaleForkError(TurnLifecycleError):
    """The branch a fork or a HITL answer was validated against is no longer
    the thread's."""

    def __init__(self, thread_id: str, branch_tip: Optional[str]):
        self.thread_id = thread_id
        self.branch_tip = branch_tip
        super().__init__(
            f"thread {thread_id} moved off branch tip {branch_tip} before the "
            "run was admitted"
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
class ForkSpec:
    """Edit/regenerate cleanup folded into the START txn (v4 2.4): truncation,
    checkpoint pin, and the new attempt commit or roll back together — a slot
    conflict or duplicate retransmit can no longer leave a half-truncated
    thread behind.

    ``branch_tip`` is the thread's ``latest_checkpoint_id`` that ``from_turn``
    was read off (None when the thread had none), so it has no default: a
    fork that names no tip has nothing for START to check. ``run_horizon``
    (the newest ``run_seq`` read with that tip) refuses the fork once any run
    started after the read. With it, ``on_branch`` (whether ``branch_tip`` is
    on the branch a given pointer names) lets START admit the fork after the
    pointer moved along the same branch; without both, any move refuses it.
    ``write_root`` writes a ``checkpoint_id`` no graph wrote (the empty fork
    root) on START's connection, so it lands with the pin or not at all."""

    from_turn: int
    checkpoint_id: str
    branch_tip: Optional[str]
    preserve_query_at_fork: bool = False
    run_horizon: Optional[int] = None
    on_branch: Optional[Callable[[str], Awaitable[bool]]] = None
    write_root: Optional[Callable[[Any], Awaitable[None]]] = None


# Run-row metadata: the checkpoint a fork's START pinned the pointer to.
FORK_BASE_KEY = "fork_base"


@dataclass(frozen=True)
class NewestCheckpoint:
    """The thread's newest root checkpoint at finalize, and the run whose graph
    step wrote it (None when no step names one)."""

    checkpoint_id: str
    run_id: Optional[str]


# The row a finalize hands back: the terminal status plus what the post-CAS
# effects read (outbox jobs, the transcript export). Never the archived event
# stream an older row holds: it runs to tens of MB, and decoding it holds the
# event loop. The start and the settle the CAS stamps time the run for the
# report it owes.
_FINALIZE_ROW_COLUMNS = """
    conversation_response_id, conversation_thread_id, turn_index, run_seq,
    status, interrupt_reason, metadata, created_at, usage_settled_at
"""


@dataclass
class FinalizeResult:
    applied: bool  # False = run was already terminal (idempotent no-op)
    run: Optional[Dict[str, Any]] = None  # _FINALIZE_ROW_COLUMNS only


@dataclass(frozen=True)
class ProducerRecord:
    """What a run's stream kept apart for its row, beside the status.

    ``provenance`` is the provenance the stream held, the main lane's plus
    the task lanes a stop drained, less what a stopped or failed run never
    saved. ``reasoning_ms`` is None when nothing could time the main agent's
    thinking (a recovered run whose stream or checkpoint could not be read),
    so the row's facts stay unset. ``model_fallbacks`` are the main lane's
    fallback ui records as streamed, kept only for a stopped or failed run,
    whose last step may never have committed its own. ``error_frame`` is the
    main lane's last error frame as streamed, kept only for a failed run.
    """

    provenance: List[Dict[str, Any]] = field(default_factory=list)
    reasoning_ms: Optional[Dict[str, List[int]]] = None
    model_fallbacks: List[Dict[str, Any]] = field(default_factory=list)
    error_frame: Optional[Dict[str, Any]] = None


@dataclass
class RunOutcome:
    """Everything finalize writes, resolved in-band by the run's own executor."""

    status: str  # completed | interrupted | error | cancelled
    interrupt_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None
    execution_time: Optional[float] = None
    per_call_records: Optional[list] = None
    tool_usage: Optional[Dict[str, int]] = None
    # None for a run with no stream to keep apart (one that never started).
    record: Optional[ProducerRecord] = None
    # Sandbox path -> URL of the images the run's saved text still names,
    # captured as it ended.
    images: Dict[str, str] = field(default_factory=dict)


@asynccontextmanager
async def _lifecycle_connection(conn=None):
    """Yield the caller-pinned session, or a pool connection.

    The pinned session prepares its checkpoint writes, but this SQL reads
    ``SELECT *`` / ``RETURNING *``: prepared, a migration that changes those
    columns during a deploy fails the old version's finalize with "cached plan
    must not change result type", and the turn is left for recovery. It runs
    once per turn, so preparing it saves nothing. The caller holds the guard
    mutex, so no checkpoint write runs while preparation is off.
    """
    if conn is not None:
        threshold = conn.prepare_threshold
        conn.prepare_threshold = None
        try:
            yield conn
        finally:
            conn.prepare_threshold = threshold
        return
    async with pool.get_db_connection() as pool_conn:
        yield pool_conn


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


async def _run_by_request_key(conn, request_key: str) -> Optional[Dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM conversation_responses WHERE request_key = %s",
            (request_key,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def _stale_refusal(
    conn, thread_id: str, request_key: str, branch_tip: Optional[str]
) -> TurnLifecycleError:
    """What START raises when, holding the thread row, it finds the branch moved.

    A retransmit and its first copy both miss the fast probe while neither
    START has committed, so the retransmit waits on the row and finds the
    first copy's own move. The key then names that run: a duplicate to
    reconnect to, as the unique index would report, not a stale request.
    """
    existing = await _run_by_request_key(conn, request_key)
    if existing:
        return DuplicateRequestError(existing)
    return StaleForkError(thread_id, branch_tip)


async def _run_since_read(conn, thread_id: str, run_horizon: Optional[int]) -> bool:
    """Whether a run started after ``run_horizon`` was read.

    That run is a turn or fork the client never saw, so it refuses the request
    whatever it did to the pointer: one that failed before its first
    checkpoint left the pointer where the request read it. A fork would delete
    it; a HITL answer would resume a pause it replaced. The caller holds the
    thread row's lock, so this read sees every START that committed before it.
    """
    if run_horizon is None:
        return False
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM conversation_responses
                WHERE conversation_thread_id = %s AND run_seq > %s
            ) AS run_since
            """,
            (thread_id, run_horizon),
        )
        return (await cur.fetchone())["run_since"]


async def _pin_along_branch(conn, thread_id: str, fork: ForkSpec) -> bool:
    """Pin the fork onto a pointer that moved after its turn was read, when
    the move only extended the branch the fork was read off.

    A stop's teardown, a compaction or an offload writes on top of the branch
    without starting a run, so the tip the fork was read off is still under
    the pointer and the fork's turn still names the same rows. A walk that
    fails proves nothing, so it refuses like a branch that left.
    """
    if fork.run_horizon is None or fork.on_branch is None:
        return False
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT latest_checkpoint_id FROM conversation_threads
            WHERE conversation_thread_id = %s
            FOR UPDATE
            """,
            (thread_id,),
        )
        row = await cur.fetchone()
        pointer = row["latest_checkpoint_id"] if row else None
    if pointer is None or await _run_since_read(conn, thread_id, fork.run_horizon):
        return False
    try:
        along = await fork.on_branch(pointer)
    except Exception:
        logger.warning(
            f"[turn_lifecycle] fork branch walk failed thread={thread_id} "
            f"read_tip={fork.branch_tip} pointer={pointer}",
            exc_info=True,
        )
        return False
    if not along:
        return False
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE conversation_threads
            SET latest_checkpoint_id = %s, updated_at = NOW()
            WHERE conversation_thread_id = %s
            """,
            (fork.checkpoint_id, thread_id),
        )
    logger.info(
        f"[turn_lifecycle] fork admitted past a pointer move thread={thread_id} "
        f"read_tip={fork.branch_tip} pointer={pointer}"
    )
    return True


async def start_run(
    *,
    run_id: str,
    thread_id: str,
    request_key: str,
    turn_index: Optional[int] = None,
    attempt_no: int = 1,
    retry_of_run_id: Optional[str] = None,
    query: Optional[QuerySpec] = None,
    fork: Optional[ForkSpec] = None,
    run_horizon: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at: Optional[datetime] = None,
    user_id: Optional[str] = None,
    before_commit: Optional[Callable[[Any], Awaitable[None]]] = None,
    conn=None,
) -> Dict[str, Any]:
    """The START transaction: fork cleanup + query row + in_progress run row +
    thread projection.

    Returns the run row. Raises DuplicateRequestError / RunSlotBusyError /
    AttemptConflictError, each backed by a DB constraint, so two workers
    racing the same admission cannot both win regardless of what they read,
    and, for a fork, StaleForkError when the branch tip moved. A HITL answer
    passes the ``run_horizon`` its pause was read at, and StaleForkError
    refuses it when a run started since.
    ``conn`` pins the transaction to the caller's session (WriterGuard);
    the post-conflict classification reads always use fresh pool reads.
    ``before_commit`` runs last, while the uncommitted row holds the slot,
    and gets the transaction's connection, so a read it makes waits on no
    second pool slot while this transaction holds the thread's locks.
    """
    created_at = created_at or datetime.now(timezone.utc)
    conflict: Optional[str] = None

    try:
        async with _lifecycle_connection(conn) as conn:
            async with conn.transaction():
                from src.server.database.workspace import lock_run_workspace

                await lock_run_workspace(conn, thread_id)

                # Fast-path dedup probe. The unique index below is the
                # race-safe backstop; this just avoids burning a turn_index
                # (and, on a fork, re-truncating rows the first transmit's
                # run already rebuilt).
                existing = await _run_by_request_key(conn, request_key)
                if existing:
                    raise DuplicateRequestError(existing)

                kept_steers: List[Dict[str, Any]] = []
                if fork is not None:
                    # The fork's turn was read off the branch the thread's
                    # pointer named, outside this transaction. A turn or fork
                    # another worker finished since has moved the pointer:
                    # truncating against the old branch deletes a turn its
                    # user never saw, or pins an abandoned branch under the
                    # other fork's rows. Pinning only from the validated tip,
                    # or from a pointer that move merely extended
                    # (``_pin_along_branch``), makes that check atomic with the
                    # truncation, and the row lock holds every other writer of
                    # the pointer until this transaction ends.
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE conversation_threads
                            SET latest_checkpoint_id = %s, updated_at = NOW()
                            WHERE conversation_thread_id = %s
                              AND latest_checkpoint_id IS NOT DISTINCT FROM %s
                            """,
                            (fork.checkpoint_id, thread_id, fork.branch_tip),
                        )
                        pinned = cur.rowcount > 0
                    if pinned:
                        # The pointer did not move, but a run that failed
                        # before its first checkpoint leaves it unmoved.
                        if await _run_since_read(conn, thread_id, fork.run_horizon):
                            raise await _stale_refusal(
                                conn, thread_id, request_key, fork.branch_tip
                            )
                    elif not await _pin_along_branch(conn, thread_id, fork):
                        raise await _stale_refusal(
                            conn, thread_id, request_key, fork.branch_tip
                        )
                    # Truncation would DELETE any in_progress row at/after the
                    # fork turn — silently freeing the slot under a live run —
                    # so a fork refuses while ANY run is open on the thread.
                    # The slot index still backstops the read: a run admitted
                    # after this check conflicts on insert below and rolls the
                    # truncation back with the transaction.
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            """
                            SELECT * FROM conversation_responses
                            WHERE conversation_thread_id = %s
                              AND status = 'in_progress'
                            """,
                            (thread_id,),
                        )
                        live = await cur.fetchone()
                        if live:
                            raise RunSlotBusyError(thread_id, dict(live))
                    # A background subagent outlives the turn that dispatched
                    # it, so the root-run check above can pass while a task run
                    # is still live under a response row about to be deleted.
                    # The cascade would erase it out from under its executor.
                    live_task = await sr_db.find_open_run_from_turn(
                        thread_id, fork.from_turn, conn=conn
                    )
                    if live_task:
                        raise sr_db.TaskRunSlotBusyError(
                            thread_id, str(live_task["task_id"]), live_task
                        )
                    if fork.write_root is not None:
                        await fork.write_root(conn)
                    # An order still waiting for a verdict or a call hangs off
                    # a response row about to be deleted, and nothing settles
                    # it once the row is gone. After the guards, so a refused
                    # fork refuses no order; before the truncation, whose rows
                    # it selects; in this transaction, so a rollback undoes it.
                    await oa_db.refuse_attempts_on_fork(
                        thread_id, fork.from_turn, conn=conn
                    )
                    # The truncation's cascade would delete the user's steers
                    # with the runs that own them; they move to this run.
                    kept_steers, input_sent_at = await lift_fork_steers(
                        conn,
                        thread_id,
                        fork.from_turn,
                        edit=not fork.preserve_query_at_fork,
                    )
                    deleted = await qr_db.truncate_thread_from_turn(
                        thread_id,
                        fork.from_turn,
                        preserve_query_at_fork=fork.preserve_query_at_fork,
                        conn=conn,
                    )
                    # Truncation cascaded away the task runs the surviving
                    # branch no longer owns; re-anchor the task rows in the
                    # same transaction so no window exists where a resume can
                    # read an empty latest_run_id.
                    await subagent_repair.repair_task_chains(thread_id, conn=conn)
                    await tt_db.drop_rewound(thread_id, fork.checkpoint_id, conn=conn)
                    # The run row is the one record of this pin that outlives a
                    # run failing before its first step (``pointer_pinned_by_fork``).
                    metadata = {**(metadata or {}), FORK_BASE_KEY: fork.checkpoint_id}
                    if input_sent_at is not None:
                        metadata["input_sent_at"] = input_sent_at.isoformat()
                    logger.info(
                        f"[turn_lifecycle] fork truncated {deleted} rows from "
                        f"turn>={fork.from_turn} thread={thread_id} "
                        f"checkpoint={fork.checkpoint_id}"
                    )
                elif run_horizon is not None:
                    # A HITL answer resumes the newest checkpoint, which the
                    # route found on the branch outside this transaction. A
                    # fork admitted since replaced that pause, and one whose
                    # run failed before its first step left the replaced pause
                    # newest. START holds no lock that serializes STARTs, so
                    # the row lock does: it waits out a fork committing now and
                    # holds off the next until this run's row is in.
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT 1 FROM conversation_threads
                            WHERE conversation_thread_id = %s
                            FOR UPDATE
                            """,
                            (thread_id,),
                        )
                    if await _run_since_read(conn, thread_id, run_horizon):
                        raise await _stale_refusal(conn, thread_id, request_key, None)

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
                            attempt_no, retry_of_run_id, request_key, user_id
                        )
                        VALUES (%s, %s, %s, 'in_progress', %s, %s, %s, %s, %s, %s)
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
                            user_id,
                        ),
                    )
                    run_row = dict(await cur.fetchone())
                await land_fork_steers(conn, kept_steers, run_id)

                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE conversation_threads
                        SET current_status = 'in_progress', updated_at = NOW()
                        WHERE conversation_thread_id = %s
                        """,
                        (thread_id,),
                    )

                if before_commit is not None:
                    await before_commit(conn)

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
            # Legacy (thread, turn_index) uniqueness — exists only in the
            # 017→018 expand/contract window; a retry tripping it degrades to
            # a clean conflict instead of an unclassified re-raise.
            "unique_turn_index_per_thread_response",
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


class _AlreadyTerminal(Exception):
    """Internal control flow: guarded UPDATE matched zero rows."""


async def _keep_seen_over_quiet(
    conn, thread_id: str, run_seq: Optional[int]
) -> Optional[int]:
    """Carry the thread's seen pointer over a quiet check that changed nothing,
    and return where it now stands, or None when it stayed.

    Every settled run past the pointer marks the thread unseen, so without
    this each quiet check of a series would badge it. Only a thread the user
    had seen up to this run is carried: one with unseen turns stays unseen.
    """
    if run_seq is None:
        return None
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE conversation_threads
            SET last_seen_run_seq = GREATEST(COALESCE(last_seen_run_seq, 0), %(seq)s)
            WHERE conversation_thread_id = %(thread)s
              AND COALESCE(last_seen_run_seq, 0) >= (
                  SELECT COALESCE(MAX(run_seq), 0) FROM conversation_responses
                  WHERE conversation_thread_id = %(thread)s AND run_seq < %(seq)s
              )
            RETURNING last_seen_run_seq
            """,
            {"seq": run_seq, "thread": thread_id},
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def finalize_run(
    *,
    run_id: str,
    thread_id: str,
    outcome: RunOutcome,
    newest: Optional[NewestCheckpoint] = None,
    usage_writer: Optional[Callable[[Any, str], Awaitable[None]]] = None,
    conn=None,
) -> FinalizeResult:
    """The finalize transaction: exactly one CAS from in_progress to terminal.

    Zero rows from the guarded UPDATE means someone else already finalized —
    the caller lost an intended race (cancel vs owner, janitor vs owner) and
    gets applied=False with the surviving row (RunNotFoundError when no row
    survives at all — e.g. the thread was deleted mid-run); nothing else is
    written. Committed cancel intent is authoritative: a row stamped with
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

    The outcome's record and images land with the status they describe, so
    a settled row never lacks the facts its replay reads, and its
    provenance replaces the main lane's rows. Subagent lanes are their
    archive's to write.

    The thread's pointer moves to ``newest`` only when this run wrote it.
    """
    status = outcome.status
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"finalize_run: {status!r} is not a terminal status")
    record = outcome.record or ProducerRecord()
    replay_facts: Dict[str, Any] = {}
    if record.reasoning_ms is not None:
        replay_facts["reasoning_ms"] = record.reasoning_ms
    if record.model_fallbacks:
        replay_facts["model_fallbacks"] = record.model_fallbacks
    if record.error_frame:
        replay_facts["error_frame"] = record.error_frame
    if outcome.images:
        replay_facts["images"] = outcome.images
    run_metadata = dict(outcome.metadata or {})
    # A checkpoint the run did not write was there before START. For a fork
    # whose run died before its first step, that is the abandoned branch's
    # tip, under the rows START truncated to the fork, so the pointer stays
    # where START pinned it.
    commit_checkpoint_id = (
        newest.checkpoint_id if newest is not None and newest.run_id == run_id else None
    )

    try:
        async with _lifecycle_connection(conn) as conn:
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
                            -- The turn's own usage (when any) is inserted by
                            -- usage_writer inside this same transaction, so the
                            -- terminal CAS is also the billing settle for the
                            -- row: its heartbeated in-flight spend stops
                            -- counting the instant the real usage row exists.
                            -- Subagent spend settles on subagent_runs rows,
                            -- never here — a tail-draining child keeps counting
                            -- on its own row after this parent settles.
                            usage_settled_at = NOW(),
                            replay_facts = COALESCE(%s::jsonb, replay_facts)
                        WHERE conversation_response_id = %s AND status = 'in_progress'
                        RETURNING """
                        + _FINALIZE_ROW_COLUMNS,
                        (
                            status,
                            outcome.interrupt_reason,
                            status,
                            SafeJson(run_metadata),
                            outcome.warnings or [],
                            outcome.errors or [],
                            outcome.execution_time,
                            SafeJson(replay_facts) if replay_facts else None,
                            run_id,
                        ),
                    )
                    run_row = await cur.fetchone()

                if run_row is None:
                    # Lost the race (or bad id) — decide outside this txn.
                    raise _AlreadyTerminal()

                run_row = dict(run_row)
                final_status = run_row["status"]
                run_meta = run_row.get("metadata") or {}
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
                    thread_id,
                    final_status,
                    checkpoint_id=commit_checkpoint_id,
                    conn=conn,
                ):
                    raise RuntimeError(
                        f"thread projection update failed for thread={thread_id}"
                    )

                seen_run_seq = None
                if final_status == "completed" and is_unchanged_quiet(run_meta):
                    seen_run_seq = await _keep_seen_over_quiet(
                        conn, thread_id, run_row["run_seq"]
                    )

                await qr_db._sync_provenance_for_response(
                    conn,
                    conversation_response_id=run_id,
                    conversation_thread_id=thread_id,
                    turn_index=run_row["turn_index"],
                    provenance=record.provenance,
                    strict=True,
                    lanes=(MAIN_PROVENANCE_LANE,),
                )

                if usage_writer is not None:
                    await usage_writer(conn, final_status)

                # I5: every finalize path — fail_open, error funnels, sweep,
                # fallback — gets its terminal effects derived from the row's
                # START-stamped metadata, selected on the CAS-ADOPTED status
                # (durable cancel intent can flip error->cancelled). No caller
                # can attach, override, or forget them.
                jobs = build_finalize_jobs_from_run_row(
                    run_row, last_seen_run_seq=seen_run_seq
                )(final_status)
                if jobs:
                    await enqueue_hooks(
                        conn, run_id=run_id, thread_id=thread_id, jobs=jobs
                    )

                # Deferred report-backs (a task's, an analyst's) and market
                # target alerts wait for exactly this event: a dead-turn
                # finalize (completed, error, or cancelled) proves no pending
                # HITL checkpoint can collide with their synthetic POST, and
                # no live turn is left to take them in as a steer.
                # Interrupted keeps them parked: the checkpoint is live and a
                # POST would collide with it, and the turn that resumes it
                # takes them in as a steer. An interrupted turn that later
                # errors or cancels releases here, so a parked job can never
                # be stranded. Same transaction as the terminal CAS, so
                # release and successor hooks commit together.
                if final_status in ("completed", "error", "cancelled"):
                    released = await release_deferred_notifications(
                        conn, thread_id
                    )
                    if released:
                        logger.info(
                            f"[turn_lifecycle] released {released} deferred "
                            f"notification job(s) for thread={thread_id}"
                        )

                # The thread's steers settle with the run they were sent to,
                # in the same transaction, so no steer outlives its run
                # without an outcome or a turn to read it.
                await settle_run_steers(
                    conn,
                    thread_id=thread_id,
                    run_id=run_id,
                    final_status=final_status,
                    user_cancel=(
                        final_status == "cancelled"
                        and bool(run_meta.get("cancelled_by_user"))
                        and not run_meta.get("stopped_by_agent")
                    ),
                    agent_stop=(
                        final_status == "cancelled"
                        and bool(run_meta.get("stopped_by_agent"))
                    ),
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
    except _AlreadyTerminal:
        run = await _get_finalize_row(run_id)
        if run is None:
            raise RunNotFoundError(run_id)
        logger.info(
            f"[turn_lifecycle] finalize no-op: run={run_id} already "
            f"{run['status']} (wanted {status})"
        )
        return FinalizeResult(applied=False, run=run)


async def _get_finalize_row(run_id: str) -> Optional[Dict[str, Any]]:
    """The survivor of a lost finalize, in the shape a won one returns."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT "
                + _FINALIZE_ROW_COLUMNS
                + " FROM conversation_responses WHERE conversation_response_id = %s",
                (run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def request_run_cancel(
    run_id: str, thread_id: Optional[str] = None, *, by_user: bool = True
) -> Dict[str, Any]:
    """Durable cancel intent: honest, idempotent, never a recorded losing cancel.

    ``thread_id`` scopes the write so a caller-supplied run_id can't stamp
    intent on another thread's run. Returns {"state": "requested"|
    "already_requested"|"already_terminal"|"not_found", "run": row|None,
    "returned_steers": [...], "held_steers": int}: the user's steers the stop
    gave back, settled in the stamp's transaction so a steer is either read by
    the run or returned here, never both (re-read for a stop asked again),
    and how many of the user's the run still holds. Those are already in its
    messages, so its ending settles them: delivered if its checkpoint read
    them, else returned (``list_returned_steers`` once it has ended).

    Two stamp laps: a cancel racing the START commit can run its guarded
    UPDATE before the row is visible (zero rows), then read the committed
    row as in_progress — one lap would misreport that as already_requested
    while no intent ever landed. The second lap stamps the now-visible row.

    ``by_user=False`` is an agent's stop: it marks the run ``stopped_by_agent``
    and leaves the user's steers for the run's ending to keep.
    """
    sql = """
        UPDATE conversation_responses
        SET cancel_requested_at = NOW()"""
    if not by_user:
        sql += """,
            metadata = COALESCE(metadata, '{}'::jsonb)
                || '{"stopped_by_agent": true}'::jsonb"""
    sql += """
        WHERE conversation_response_id = %s
          AND status = 'in_progress'
          AND cancel_requested_at IS NULL
    """
    params: list = [run_id]
    if thread_id is not None:
        sql += "  AND conversation_thread_id = %s\n"
        params.append(thread_id)
    sql += "        RETURNING *"

    async def _held(conn, thread: str) -> int:
        if not by_user:
            return 0
        return await count_held_user_steers(
            conn, ordering_key=steer_ordering_key(thread), owner_id=run_id
        )

    async def _settled(state: str, run: Dict[str, Any]) -> Dict[str, Any]:
        held = 0
        if state != "already_terminal":
            async with pool.get_db_connection() as conn:
                held = await _held(conn, str(run["conversation_thread_id"]))
        return {
            "state": state,
            "run": run,
            "returned_steers": await list_returned_steers(run_id),
            "held_steers": held,
        }

    run: Optional[Dict[str, Any]] = None
    for _lap in range(2):
        async with pool.get_db_connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(sql, params)
                    row = await cur.fetchone()
                    if row:
                        row = dict(row)
                        returned = await cancel_run_steers(
                            conn,
                            thread_id=str(row["conversation_thread_id"]),
                            run_id=run_id,
                            by_user=by_user,
                        )
                        return {
                            "state": "requested",
                            "run": row,
                            "returned_steers": returned,
                            "held_steers": await _held(
                                conn, str(row["conversation_thread_id"])
                            ),
                        }

        run = await get_run(run_id)
        if run is None or (
            thread_id is not None and str(run["conversation_thread_id"]) != thread_id
        ):
            return {
                "state": "not_found",
                "run": None,
                "returned_steers": [],
                "held_steers": 0,
            }
        if run["status"] != "in_progress":
            return await _settled("already_terminal", run)
        if run.get("cancel_requested_at"):
            return await _settled("already_requested", run)
        # in_progress with NULL intent, yet our UPDATE matched nothing:
        # the START-commit visibility race — go around once more.
    return await _settled("already_requested", run)


async def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM conversation_responses WHERE conversation_response_id = %s",
                (run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_run_brief(conn, run_id: str) -> Optional[Dict[str, Any]]:
    """A run's id, thread, request key and status, read on ``conn``.

    For a read inside START's transaction: on its own connection it waits on
    no pool slot while START holds the thread's locks, and it leaves out the
    archived event stream of an older row that ``get_run`` loads.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT conversation_response_id, conversation_thread_id,
                   request_key, status
            FROM conversation_responses
            WHERE conversation_response_id = %s
            """,
            (run_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_run_ending(run_id: str) -> Optional[Dict[str, Any]]:
    """A run's status and metadata, with the workspace and user it ran for.

    Narrow on purpose: ``get_run`` loads an older row's archived event
    stream too, which a billing read on the resume path has no use for.
    """
    run_id = normalize_uuid(run_id)
    if run_id is None:
        return None
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT r.status, r.metadata, t.workspace_id,
                       COALESCE(r.user_id, w.user_id) AS user_id
                FROM conversation_responses r
                JOIN conversation_threads t
                  ON t.conversation_thread_id = r.conversation_thread_id
                JOIN workspaces w ON w.workspace_id = t.workspace_id
                WHERE r.conversation_response_id = %s
                """,
                (run_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_active_run(thread_id: str) -> Optional[Dict[str, Any]]:
    """The thread's live run, if any — one row by the slot index."""
    async with pool.get_db_connection() as conn:
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


async def workspace_has_active_run(workspace_id: str) -> bool:
    """Any live root run on any thread of the workspace — cross-worker,
    unlike the in-process task map."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM conversation_responses r
                    JOIN conversation_threads t
                      ON t.conversation_thread_id = r.conversation_thread_id
                    WHERE t.workspace_id = %s AND r.status = 'in_progress'
                )
                """,
                (workspace_id,),
            )
            return bool((await cur.fetchone())[0])


async def workspace_has_live_runs(cur, workspace_id: str) -> bool:
    """Any root or background run in progress on the workspace's threads.

    Read on the caller's dict-row cursor, so a caller holding the workspace
    row FOR UPDATE sees every run admitted before it: admission holds that row
    FOR SHARE until its run row commits.
    """
    await cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM conversation_responses r
            JOIN conversation_threads t
              ON t.conversation_thread_id = r.conversation_thread_id
            WHERE t.workspace_id = %(workspace_id)s AND r.status = 'in_progress'
            UNION ALL
            SELECT 1 FROM subagent_runs r
            JOIN conversation_threads t
              ON t.conversation_thread_id = r.thread_id
            WHERE t.workspace_id = %(workspace_id)s AND r.status = 'in_progress'
        ) AS busy
        """,
        {"workspace_id": workspace_id},
    )
    return (await cur.fetchone())["busy"]


async def computer_has_active_run(computer_id: str) -> bool:
    """Any live root run on any workspace bound to the computer.

    The machine-scoped twin of ``workspace_has_active_run``, and a join rather
    than a counter column because a torn-down computer takes every project on it
    with it: the answer has to come from the runs themselves, not from a number
    a crashed worker could have left behind. At one workspace per computer the
    two functions return the same thing.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM conversation_responses r
                    JOIN conversation_threads t
                      ON t.conversation_thread_id = r.conversation_thread_id
                    JOIN workspaces w ON w.workspace_id = t.workspace_id
                    WHERE w.computer_id = %s AND r.status = 'in_progress'
                )
                """,
                (computer_id,),
            )
            return bool((await cur.fetchone())[0])


async def get_latest_attempt(
    thread_id: str,
    *,
    before_turn: Optional[int] = None,
    wait_for_finalize: bool = False,
) -> Optional[Dict[str, Any]]:
    """The thread's most recent attempt row — /retry's validation target.

    Ordered by ``run_seq`` — the one monotonic run ordering. turn_index is
    reused by retries and lowered by branch rewinds, so it cannot define
    "latest" (two coexisting definitions of latest is a drift bug).

    ``before_turn`` narrows the search to attempts of earlier turns: a fork
    from turn N discards N and everything after it, so its predecessor is the
    latest attempt that will survive, not the latest attempt there is.

    ``wait_for_finalize`` reads the row ``FOR SHARE``, which waits out a
    finalize in flight: its terminal CAS holds the row until it commits, so
    a run whose finalize has already run its release pass never reads live.
    """
    clauses = ["conversation_thread_id = %s"]
    params: list[Any] = [thread_id]
    if before_turn is not None:
        clauses.append("turn_index < %s")
        params.append(before_turn)
    lock = "FOR SHARE" if wait_for_finalize else ""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT * FROM conversation_responses
                WHERE {" AND ".join(clauses)}
                ORDER BY run_seq DESC
                LIMIT 1
                {lock}
                """,
                tuple(params),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def thread_has_dispatch_gen(thread_id: str, dispatch_gen: str) -> bool:
    """True if any attempt on this thread was admitted under this dispatch
    generation (``origin_dispatch_gen`` is stamped into run metadata at
    START) — the durable admission record the orphan resolver defers to.
    A hand-off steered into a running run is admitted on that run's
    ``dispatch_gens``."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 1 FROM conversation_responses
                WHERE conversation_thread_id = %s
                  AND (metadata->>'origin_dispatch_gen' = %s
                       OR metadata->'dispatch_gens' ? %s)
                LIMIT 1
                """,
                (thread_id, dispatch_gen, dispatch_gen),
            )
            return await cur.fetchone() is not None


async def run_has_dispatch_gen(run_id: str, dispatch_gen: str) -> bool:
    """True if ``run_id`` itself was admitted under this generation, at START
    or as a hand-off steered into it."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 1 FROM conversation_responses
                WHERE conversation_response_id = %s::uuid
                  AND (metadata->>'origin_dispatch_gen' = %s
                       OR metadata->'dispatch_gens' ? %s)
                """,
                (run_id, dispatch_gen, dispatch_gen),
            )
            return await cur.fetchone() is not None


async def list_open_runs() -> List[Dict[str, Any]]:
    """All in_progress runs, oldest first (Phase-1 startup sweep input)."""
    async with pool.get_db_connection() as conn:
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


async def heal_stale_thread_projections() -> int:
    """Reset thread projections stuck on a live spelling with no open run
    (pre-v4 leftovers, or a projection write that raced a crash) to the
    latest attempt's terminal status. Returns rows healed."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE conversation_threads ct
                SET current_status = COALESCE(
                        (
                            SELECT cr.status FROM conversation_responses cr
                            WHERE cr.conversation_thread_id
                                = ct.conversation_thread_id
                            ORDER BY cr.run_seq DESC
                            LIMIT 1
                        ),
                        'completed'
                    ),
                    updated_at = NOW()
                WHERE ct.current_status IN ('in_progress', 'active')
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses cr2
                      WHERE cr2.conversation_thread_id
                          = ct.conversation_thread_id
                        AND cr2.status = 'in_progress'
                  )
                """
            )
            return cur.rowcount


# What a liveness read says of a run, and the joins that keep its reads (a
# thread's latest run here, a run by id in ``handoff_liveness``) to runs the
# caller owns.
_LIVENESS_RUN_COLUMNS = """
    cr.conversation_response_id, cr.conversation_thread_id,
    cr.status, cr.cancel_requested_at,
    -- What run_completed_at reads to say when it settled.
    cr.created_at, cr.usage_settled_at, cr.execution_time
"""
_OWNED_RUNS_FROM = """
    FROM conversation_responses cr
    JOIN conversation_threads ct
        ON ct.conversation_thread_id = cr.conversation_thread_id
    JOIN workspaces w ON w.workspace_id = ct.workspace_id
"""


async def get_latest_attempts_for_threads(
    thread_ids: List[str], user_id: str
) -> Dict[str, Dict[str, Any]]:
    """Latest attempt row per thread, ownership-filtered — one query.

    Feeds batched liveness reads: ownership comes from the thread row (no
    per-thread authorization round-trips), threads with no attempts are
    simply absent from the result. Non-UUID ids are dropped pre-bind — one
    malformed client id must not 22P02 the whole batch.
    """
    normalized = [nid for nid in (normalize_uuid(t) for t in thread_ids) if nid]
    if not normalized:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT DISTINCT ON (cr.conversation_thread_id)
                    {_LIVENESS_RUN_COLUMNS},
                    cr.interrupt_reason, cr.run_seq,
                    -- A run settled before usage_settled_at existed falls
                    -- back to its start plus measured duration, as replay's
                    -- run_completed_at does.
                    GREATEST(
                        cr.created_at,
                        COALESCE(
                            cr.usage_settled_at,
                            cr.created_at + cr.execution_time * INTERVAL '1 second'
                        )
                    ) AS last_activity_at,
                    NULLIF(ct.title, '') AS title,
                    w.name AS workspace_name
                {_OWNED_RUNS_FROM}
                WHERE cr.conversation_thread_id = ANY(%s)
                  AND w.user_id = %s
                ORDER BY cr.conversation_thread_id, cr.run_seq DESC
                """,
                (normalized, user_id),
            )
            rows = await cur.fetchall()
            return {str(r["conversation_thread_id"]): dict(r) for r in rows}


# How far back a report's run is followed. A chain longer than this names its
# newest legs only, which leaves the hand-off without its report link, not the
# report undelivered.
_CONTINUED_RUNS_LIMIT = 50


async def get_continued_runs(thread_id: str, run_id: str) -> List[str]:
    """``run_id`` and the runs it continued on ``thread_id``, oldest first.

    A run that ends interrupted waits on the user and reports nothing; the
    next attempt on the thread, a resume or a message past the interrupt,
    carries its work on and reports for it, as ``carried_pair`` has it carry
    a summary's pair. So a report on the last leg answers every leg back to
    the first one that did not follow an interrupt, which is the run a
    hand-off named. Empty when ``run_id`` is not a run of ``thread_id``.
    """
    thread, run = normalize_uuid(thread_id), normalize_uuid(run_id)
    if thread is None or run is None:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT conversation_response_id, status
                FROM conversation_responses
                WHERE conversation_thread_id = %s
                  AND run_seq <= (
                      SELECT run_seq FROM conversation_responses
                      WHERE conversation_response_id = %s
                        AND conversation_thread_id = %s
                  )
                ORDER BY run_seq DESC
                LIMIT %s
                """,
                (thread, run, thread, _CONTINUED_RUNS_LIMIT),
            )
            rows = await cur.fetchall()
    # The first row is the run itself; each earlier one joins while it ended
    # interrupted.
    chain = [str(r["conversation_response_id"]) for r in rows[:1]]
    for row in rows[1:]:
        if row["status"] != "interrupted":
            break
        chain.append(str(row["conversation_response_id"]))
    return chain[::-1]


async def get_run_statuses(run_ids: List[str]) -> Dict[str, str]:
    """run_id -> status, for the ids that HAVE a row.

    Absence means the row does not exist — load-bearing for the retention
    sweeper, which treats a missing root run as garbage. That inference is
    only sound because the row is inserted before the run's first XADD.
    Non-UUID ids are dropped pre-bind so one malformed key can't 22P02 the
    whole batch.
    """
    normalized = [nid for nid in (normalize_uuid(r) for r in run_ids) if nid]
    if not normalized:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT conversation_response_id, status
                FROM conversation_responses
                WHERE conversation_response_id = ANY(%s)
                """,
                (normalized,),
            )
            rows = await cur.fetchall()
            return {
                str(r["conversation_response_id"]): str(r["status"]) for r in rows
            }


async def get_run_seqs(run_ids: List[str]) -> Dict[str, int]:
    """run_id -> run_seq, for the ids that HAVE a row; orders runs a caller
    gathered from more than one source."""
    normalized = [nid for nid in (normalize_uuid(r) for r in run_ids) if nid]
    if not normalized:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT conversation_response_id, run_seq
                FROM conversation_responses
                WHERE conversation_response_id = ANY(%s)
                """,
                (normalized,),
            )
            rows = await cur.fetchall()
            return {
                str(r["conversation_response_id"]): int(r["run_seq"]) for r in rows
            }


async def pointer_pinned_by_fork(
    thread_id: str, pointer: str, step_run_id: Optional[str]
) -> bool:
    """Whether a fork's START pinned ``pointer`` and replaced the newest step
    beyond it, the one ``step_run_id`` wrote.

    A step whose run still has a row was not replaced: the fork's own run or
    a later one wrote it, and it stays the newest. Without a pin, a step
    naming no run (written before runs were stamped) or a run that never had
    a row is a turn that ended without moving the pointer, which keeps the
    newest too.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM conversation_responses
                    WHERE conversation_thread_id = %(thread_id)s
                      AND metadata->>%(key)s = %(pointer)s
                ) AND NOT EXISTS (
                    SELECT 1 FROM conversation_responses
                    WHERE conversation_response_id = %(step_run_id)s
                ) AS pinned
                """,
                {
                    "thread_id": thread_id,
                    "key": FORK_BASE_KEY,
                    "pointer": pointer,
                    "step_run_id": normalize_uuid(step_run_id),
                },
            )
            return bool((await cur.fetchone())["pinned"])


async def find_run_by_request_key(request_key: str) -> Optional[Dict[str, Any]]:
    async with pool.get_db_connection() as conn:
        return await _run_by_request_key(conn, request_key)


