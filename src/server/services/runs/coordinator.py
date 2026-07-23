"""Turn lifecycle v4 — RunCoordinator: the only code that transitions run state.

START happens eagerly at the HTTP boundary (before the StreamingResponse /
dispatch 202), so every accepted request has a durable in_progress attempt
row before any execution begins. Finalize is one CAS transaction — response
terminal + thread projection + usage + outbox — with post-commit transport
effects afterward. Every terminal path (complete, interrupt, error, cancel,
setup failure, abandoned generator) funnels through finalize_run; nothing
else may write a run's terminal state.
"""

import asyncio
import logging
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional
from uuid import uuid4

from src.server.database.runs import lifecycle as tl_db
from src.server.database.runs.lifecycle import (  # re-exported for callers
    AttemptConflictError,
    DuplicateRequestError,
    FinalizeResult,
    ForkSpec,
    QuerySpec,
    RunSlotBusyError,
)
from src.server.utils.error_sanitization import sanitize_error_text

__all__ = [
    "RunCoordinator",
    "RunHandle",
    "RunOutcome",
    "QuerySpec",
    "ForkSpec",
    "FinalizeResult",
    "RunSlotBusyError",
    "DuplicateRequestError",
    "AttemptConflictError",
    "protected_finalize",
]

logger = logging.getLogger(__name__)

# Strong refs for finalize tasks whose awaiter was cancelled out from under
# them — the task must keep running detached until the terminal write lands.
_protected_tasks: set = set()


def _reap_protected(task: asyncio.Task) -> None:
    _protected_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()  # marks the exception retrieved for detached tasks
    if exc is not None:
        logger.critical(
            f"[RunCoordinator] protected finalize {task.get_name()} failed",
            exc_info=exc,
        )


async def protected_finalize(coro: Coroutine, label: str):
    """Run a finalize coroutine immune to the awaiting task's cancellation.

    Pre-handoff finalization runs on the client-stream task; a disconnect
    injects CancelledError mid-await, which would abort the terminal
    transaction and leave the run with no owner. The coroutine runs in its
    own strongly-referenced task: if the awaiter is cancelled, the
    CancelledError still propagates to it, but the finalize completes
    detached.
    """
    task = asyncio.create_task(coro, name=f"turn-finalize-{label}")
    _protected_tasks.add(task)
    task.add_done_callback(_reap_protected)
    return await asyncio.shield(task)


@dataclass
class RunOutcome:
    """Everything finalize needs, resolved in-band by the run's own executor."""

    status: str  # completed | interrupted | error | cancelled
    interrupt_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: Optional[List[str]] = None
    errors: Optional[List[str]] = None
    execution_time: Optional[float] = None
    sse_events: Optional[List[Dict[str, Any]]] = None
    per_call_records: Optional[list] = None
    tool_usage: Optional[Dict[str, int]] = None


@dataclass
class RunHandle:
    """Identity of one run (= one conversation_responses row), START to finalize."""

    run_id: str
    thread_id: str
    turn_index: int
    attempt_no: int
    msg_type: str  # 'ptc' | 'flash'
    workspace_id: Optional[str] = None
    user_id: Optional[str] = None
    is_byok: bool = False
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finalized: bool = False
    # Phase 2 (I2): the run's pinned PG session — advisory-lock fence,
    # lifecycle SQL, and per-run saver on one connection. None = Phase-1
    # fallback (memory saver, split app/checkpoint DBs, or pool not open).
    guard: Optional[Any] = None

    @property
    def checkpointer(self) -> Optional[Any]:
        """The saver this run's graph MUST use: the guard's session-bound
        saver when fenced, else the global pooled saver."""
        if self.guard is not None:
            return self.guard.saver
        from src.server.app import setup

        return setup.checkpointer


class RunCoordinator:
    """Stateless singleton — all run state lives in Postgres rows and RunHandles."""

    _instance: Optional["RunCoordinator"] = None

    @classmethod
    def get_instance(cls) -> "RunCoordinator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------ START

    async def start_run(
        self,
        *,
        thread_id: str,
        run_id: str,
        msg_type: str,
        request_key: Optional[str] = None,
        workspace_id: Optional[str] = None,
        user_id: Optional[str] = None,
        is_byok: bool = False,
        query: Optional[QuerySpec] = None,
        fork: Optional[ForkSpec] = None,
        turn_index: Optional[int] = None,
        attempt_no: int = 1,
        retry_of_run_id: Optional[str] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
    ) -> RunHandle:
        """Create the durable attempt: query row + in_progress run + projection.

        Raises RunSlotBusyError (409 admission), DuplicateRequestError
        (idempotent retransmit — reconnect to the existing run, guard
        released first), AttemptConflictError, or WriterGuardUnavailable
        (503 — pinned-session budget/lock bounded refusal). A server-minted
        request_key is the legacy fallback only; callers should supply one
        for dedup to mean anything.
        """
        from src.server.services import writer_guard as wg

        metadata = {"msg_type": msg_type, **(run_metadata or {})}

        guard = None
        if wg.guard_enabled():
            guard = await wg.WriterGuard.acquire_root(
                thread_id=thread_id, run_id=run_id
            )
        try:
            async with (guard.mutex if guard is not None else nullcontext()):
                row = await tl_db.start_run(
                    run_id=run_id,
                    thread_id=thread_id,
                    request_key=request_key or str(uuid4()),
                    turn_index=turn_index,
                    attempt_no=attempt_no,
                    retry_of_run_id=retry_of_run_id,
                    query=query,
                    fork=fork,
                    metadata=metadata,
                    conn=guard.conn if guard is not None else None,
                )
            handle = RunHandle(
                run_id=run_id,
                thread_id=thread_id,
                turn_index=row["turn_index"],
                attempt_no=row["attempt_no"],
                msg_type=msg_type,
                workspace_id=workspace_id,
                user_id=user_id,
                is_byok=is_byok,
                started_at=row["created_at"],
                guard=guard,
            )
            # Announce the durably-born run on the thread's control lane so an
            # attached mux admits the main-lane channel push-style (best-effort).
            from src.server.services.thread_control_stream import (
                announce_run_started,
            )

            await announce_run_started(thread_id, run_id)
        except BaseException:
            # Covers post-commit failures too (incl. CancelledError from the
            # announce await): releasing the guard here is what lets the
            # recovery scanner reclaim the committed in_progress row instead
            # of it wedging the thread behind a leaked advisory lock.
            if guard is not None:
                await guard.release()
            raise
        return handle

    # --------------------------------------------------------------- FINALIZE

    async def finalize_run(
        self,
        handle: RunHandle,
        outcome: RunOutcome,
        *,
        post_commit: Optional[Callable[[], Awaitable[None]]] = None,
        tail_drain: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> FinalizeResult:
        """The single terminal transition. Idempotent: losing a finalize race
        (cancel vs owner) returns applied=False and performs no writes.

        Usage rows ride the same transaction — a billing persist failure
        aborts the terminal write with it and surfaces, instead of being
        swallowed into a zombie ACTIVE turn.

        Owns the guard teardown: the finalize SQL runs on the pinned session
        (a lost session downgrades the outcome — see ``_downgrade_if_guard_
        lost``); afterwards N(root) drops so the next turn can start, and the
        session is released — immediately, or after ``tail_drain`` when
        background-subagent writers outlive the turn (their saver IS this
        session).
        """
        try:
            checkpoint_id = await self._latest_checkpoint_id(handle)

            usage_writer = None
            if outcome.per_call_records or outcome.tool_usage:
                usage_writer = self._build_usage_writer(handle, outcome)

            if outcome.errors:
                outcome.errors = [
                    sanitize_error_text(e) if isinstance(e, str) else e
                    for e in outcome.errors
                ]

            # ANY guard's mutex gates the CAS — including an already-lost
            # one: a lost-but-open session's saver can still execute, and
            # only its mutex serializes those ops against the pool CAS.
            raw_guard = handle.guard
            async with (raw_guard.mutex if raw_guard is not None else nullcontext()):
                # The loss decision and the CAS must be atomic: the monitor
                # sets `lost` while holding the guard mutex, so deciding
                # HERE — inside the mutex — is race-free where a snapshot
                # taken outside would be stale (monitor queued ahead of
                # finalize).
                guard = raw_guard
                if guard is not None and not guard.usable:
                    guard = None
                outcome = self._downgrade_if_guard_lost(handle, outcome, guard)
                result = await tl_db.finalize_run(
                    run_id=handle.run_id,
                    thread_id=handle.thread_id,
                    status=outcome.status,
                    interrupt_reason=outcome.interrupt_reason,
                    metadata=outcome.metadata,
                    warnings=outcome.warnings,
                    errors=outcome.errors,
                    execution_time=outcome.execution_time,
                    sse_events=outcome.sse_events,
                    checkpoint_id=checkpoint_id,
                    usage_writer=usage_writer,
                    conn=guard.conn if guard is not None else None,
                )
        except BaseException:
            # A failed finalize leaves the row in_progress for recovery; the
            # session must not stay pinned while nothing owns the run — and
            # tail writers may still be live, so the session is discarded
            # (closed), never clean-released out from under them.
            self._teardown_guard(handle, tail_drain=None, discard=True)
            raise
        handle.finalized = True
        self._teardown_guard(handle, tail_drain=tail_drain)

        if result.applied:
            if post_commit is not None:
                try:
                    await post_commit()
                except Exception:
                    logger.warning(
                        f"[RunCoordinator] post_commit hook failed for "
                        f"run={handle.run_id}",
                        exc_info=True,
                    )
            self.post_finalize_tail(handle.thread_id)
        return result

    async def fail_open_run(
        self,
        handle: RunHandle,
        error_message: str,
        *,
        status: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[FinalizeResult]:
        """Guarantee-finalize for paths that die between START and execution.

        Phase 1 has no recovery scanner: any code path that STARTed a run and
        cannot hand it to the executor MUST call this, or the slot leaks until
        operator intervention. Death paths run on the client-stream task, so
        the finalize is internally shielded (protected_finalize) — callers
        never need to wrap it. Never raises, except propagating the caller's
        own cancellation after the detached finalize is underway.
        """
        if handle.finalized:
            return None
        try:
            return await protected_finalize(
                self.finalize_run(
                    handle,
                    RunOutcome(
                        status=status,
                        metadata=metadata or {},
                        errors=[error_message],
                        execution_time=(
                            datetime.now(timezone.utc) - handle.started_at
                        ).total_seconds(),
                    ),
                ),
                label=handle.run_id,
            )
        except Exception:
            logger.error(
                f"[RunCoordinator] fail_open_run could not finalize "
                f"run={handle.run_id}; slot remains held",
                exc_info=True,
            )
            return None

    async def finalize_detached_run(
        self,
        thread_id: str,
        run_id: str,
        outcome: RunOutcome,
        *,
        checkpoint_id: Optional[str] = None,
        error_frame: Optional[Dict[str, Any]] = None,
    ) -> FinalizeResult:
        """The guard-less finalize funnel for runs with no live owner
        (recovery scanner, dispatch reconcile): the same terminal CAS as the
        owner path, then its entire tail — projection refresh + drainer nudge
        on a won CAS, and terminal-frame emission through the run_end gate
        even on a lost one (the survivor may have died between its commit and
        its emission; the gate keeps the close exactly-once against a live
        emitter). ``error_frame`` is the caller's client-facing failure
        payload, emitted only when the verified terminal status is ``error``.
        Raises like the CAS it wraps; callers that must never raise wrap it.
        """
        result = await tl_db.finalize_run(
            run_id=run_id,
            thread_id=thread_id,
            status=outcome.status,
            interrupt_reason=outcome.interrupt_reason,
            metadata=outcome.metadata,
            warnings=outcome.warnings,
            errors=outcome.errors,
            execution_time=outcome.execution_time,
            sse_events=outcome.sse_events,
            checkpoint_id=checkpoint_id,
        )
        if result.applied:
            self.post_finalize_tail(thread_id)
        final_status = (result.run or {}).get("status")
        if final_status:
            from src.server.services.runs.executor import (
                LocalRunExecutor,
            )

            await LocalRunExecutor.get_instance().append_run_end_event(
                thread_id,
                run_id,
                final_status,
                error_frame=error_frame if final_status == "error" else None,
            )
        return result

    async def reconcile_orphaned_dispatch(
        self,
        thread_id: str,
        run_id: str,
        *,
        error_text: Optional[str] = None,
        label: str = "dispatch",
    ) -> Optional[str]:
        """Last-resort owner (I6) for a dispatched run whose consumer died
        without the executor settling the row.

        One funnel call: CAS a still-in_progress row to error (durable cancel
        intent may adopt 'cancelled'), then close the transport with the
        verified terminal status through the run_end gate. When nothing
        durable can be established (row missing, CAS failure) an error frame
        still tells attached clients the workflow died — without claiming a
        terminal. Returns the verified terminal status, or None. Never raises.
        """
        error_frame = {
            "thread_id": thread_id,
            "content": "background workflow failed",
            "error_type": "background_failure",
            "error": error_text or "background workflow failed",
        }
        try:
            result = await self.finalize_detached_run(
                thread_id,
                run_id,
                RunOutcome(
                    status="error",
                    errors=[error_text or "background workflow failed"],
                    metadata={"recovery": "dispatch_consumer_crash"},
                ),
                error_frame=error_frame,
            )
            return (result.run or {}).get("status")
        except Exception:
            logger.critical(
                f"[{label}] last-resort finalize failed for run={run_id}; "
                f"row (if any) remains in_progress for recovery",
                exc_info=True,
            )
            try:
                from src.server.services.runs.executor import (
                    LocalRunExecutor,
                )

                await LocalRunExecutor.get_instance().append_run_end_event(
                    thread_id, run_id, None, error_frame=error_frame
                )
            except Exception:
                logger.warning(
                    f"[{label}] failed to emit terminal error SSE for "
                    f"thread_id={thread_id} run_id={run_id}",
                    exc_info=True,
                )
            return None

    # ------------------------------------------------------------ guard utils

    def _downgrade_if_guard_lost(
        self, handle: RunHandle, outcome: RunOutcome, usable_guard
    ) -> RunOutcome:
        """A run whose pinned session died cannot vouch for its final
        checkpoints (the saver died with the session), so success statuses
        are dishonest: downgrade completed/interrupted to error, exactly as
        the recovery scanner would classify it. The finalize then runs on
        the pool — same CAS, one winner — with the loss stamped.

        ``usable_guard`` is the caller's guard view re-validated under the
        guard mutex — the one the CAS will use — so the loss decision and
        the CAS connection can never diverge."""
        if handle.guard is None or usable_guard is not None:
            return outcome
        if outcome.status != "error":
            # Even a requested 'cancelled' downgrades: without durable intent
            # it would backfill cancel_requested_at and masquerade as a user
            # stop, when the truth is an infra abort. Real user intent is on
            # the row and the CAS adopts 'cancelled' from it regardless.
            logger.critical(
                f"[RunCoordinator] guard session lost for run={handle.run_id}; "
                f"downgrading {outcome.status} -> error"
            )
            outcome.status = "error"
            outcome.interrupt_reason = None
            outcome.errors = (outcome.errors or []) + [
                "guard_session_lost: the run's writer session died before "
                "its final checkpoints could be verified"
            ]
        outcome.metadata = {**outcome.metadata, "guard_session_lost": True}
        return outcome

    def _teardown_guard(
        self,
        handle: RunHandle,
        *,
        tail_drain: Optional[Callable[[], Awaitable[None]]],
        discard: bool = False,
    ) -> None:
        """Detached, exactly-once guard teardown: drop N(root) now so the
        thread's next turn can start, hold the session through the tail
        writers' drain, then unlock and return the connection.

        A drain failure (registry error, deadline) means writers may still
        be live, so it flips to ``discard``: a clean release would retarget
        their saver at the pool and let them checkpoint unfenced — the
        session is closed instead, and any late write fails loudly."""
        guard = handle.guard
        if guard is None:
            return
        handle.guard = None

        async def _run() -> None:
            must_discard = discard
            try:
                if tail_drain is not None and not must_discard:
                    await guard.demote_to_tail()
                    try:
                        await tail_drain()
                    except BaseException:
                        must_discard = True
                        logger.warning(
                            f"[RunCoordinator] tail drain failed for "
                            f"run={handle.run_id}; discarding the session",
                            exc_info=True,
                        )
            finally:
                await guard.release(discard=must_discard)

        task = asyncio.create_task(
            _run(), name=f"writer-guard-teardown-{handle.run_id[:8]}"
        )
        _protected_tasks.add(task)
        task.add_done_callback(_reap_protected)

    # ------------------------------------------------------------------ utils

    def _build_usage_writer(self, handle: RunHandle, outcome: RunOutcome):
        async def _write(conn, final_status: str) -> None:
            from src.server.services.persistence.usage import UsagePersistenceService

            usage = UsagePersistenceService(
                thread_id=handle.thread_id,
                workspace_id=handle.workspace_id,
                user_id=handle.user_id,
            )
            if outcome.per_call_records:
                await usage.track_llm_usage(outcome.per_call_records)
            if outcome.tool_usage:
                usage.record_tool_usage_batch(outcome.tool_usage)
            # msg_type is the producer mode, never a status — interrupted
            # turns bill as their real mode ('interrupted' rows were the old
            # vocabulary pollution). final_status comes from the CAS row, not
            # outcome.status: durable cancel intent may have overridden it.
            ok = await usage.persist_usage(
                response_id=handle.run_id,
                msg_type=outcome.metadata.get("msg_type") or handle.msg_type,
                deepthinking=outcome.metadata.get("deepthinking", False),
                status=final_status,
                conn=conn,
                is_byok=outcome.metadata.get("is_byok", handle.is_byok),
            )
            if not ok:
                # persist_usage swallows into False; inside the finalize txn
                # that would commit a terminal row without its billing rows.
                raise RuntimeError(
                    f"usage persist failed for run={handle.run_id}; "
                    f"aborting finalize transaction"
                )

        return _write

    async def _latest_checkpoint_id(self, handle: RunHandle) -> Optional[str]:
        """Checkpoint tip via the run's own session when fenced (reads its
        own writes; a lock-lost session can't answer), else the pool saver."""
        try:
            guard = handle.guard
            if guard is not None and guard.usable:
                saver = guard.saver
            else:
                from src.server.app import setup

                saver = setup.checkpointer
            if not saver:
                return None
            cp = await saver.aget_tuple(
                {"configurable": {"thread_id": handle.thread_id}}
            )
            return cp.config["configurable"]["checkpoint_id"] if cp else None
        except Exception:
            logger.warning(
                f"[RunCoordinator] checkpoint_id read failed for "
                f"{handle.thread_id}",
                exc_info=True,
            )
            return None

    def post_finalize_tail(self, thread_id: str) -> None:
        """The uniform after-a-won-CAS tail: every site that applies a
        terminal transition schedules the projection refresh and nudges the
        drainer, so the two never drift apart per call site."""
        self._schedule_projection_refresh(thread_id)
        self._nudge_hook_drainer()

    def _nudge_hook_drainer(self) -> None:
        """Post-commit hint only — the drainer's poll is the delivery
        guarantee, so a failed nudge is never an error."""
        try:
            from src.server.services.hook_outbox import HookOutboxDrainer

            HookOutboxDrainer.get_instance().nudge()
        except Exception:
            logger.warning("[RunCoordinator] hook drainer nudge failed", exc_info=True)

    def _schedule_projection_refresh(self, thread_id: str) -> None:
        try:
            from src.server.services.history.projection_cache import (
                schedule_projection_refresh,
            )

            schedule_projection_refresh(thread_id)
        except Exception:
            logger.warning(
                f"[RunCoordinator] projection refresh scheduling failed for "
                f"{thread_id}",
                exc_info=True,
            )
