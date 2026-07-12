"""Turn lifecycle v4 — TurnCoordinator: the only code that transitions run state.

START happens eagerly at the HTTP boundary (before the StreamingResponse /
dispatch 202), so every accepted request has a durable in_progress attempt
row before any execution begins. Finalize is one CAS transaction — response
terminal + thread projection + usage + outbox — with post-commit transport
effects afterward. Every terminal path (complete, interrupt, error, cancel,
setup failure, abandoned generator) funnels through finalize_turn; nothing
else may write a run's terminal state.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Union
from uuid import uuid4

from src.server.database import turn_lifecycle as tl_db
from src.server.database.turn_lifecycle import (  # re-exported for callers
    AttemptConflictError,
    DuplicateRequestError,
    FinalizeResult,
    HookJob,
    QuerySpec,
    RunSlotBusyError,
)
from src.server.utils.error_sanitization import sanitize_error_text

__all__ = [
    "TurnCoordinator",
    "RunHandle",
    "TurnOutcome",
    "QuerySpec",
    "HookJob",
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
            f"[TurnCoordinator] protected finalize {task.get_name()} failed",
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
class TurnOutcome:
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
    # A static list, or a factory invoked inside the finalize transaction
    # with the CAS-adopted final status (build_finalize_jobs) — the adopted
    # status may differ from outcome.status when durable cancel intent wins.
    outbox_jobs: Union[List[HookJob], Callable[[str], List[HookJob]]] = field(
        default_factory=list
    )


@dataclass
class RunHandle:
    """Identity of one run (= one conversation_responses row), START to finalize.

    ``outcome_hint`` is the in-band interrupt signal: the streaming handler
    sets it after the durability barrier verifies a pending interrupt, so
    finalize classification never depends on a timeout-prone state read.
    """

    run_id: str
    thread_id: str
    turn_index: int
    attempt_no: int
    request_key: str
    msg_type: str  # 'ptc' | 'flash'
    workspace_id: Optional[str] = None
    user_id: Optional[str] = None
    query_id: Optional[str] = None
    is_byok: bool = False
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    outcome_hint: Optional[TurnOutcome] = None
    finalized: bool = False


class TurnCoordinator:
    """Stateless singleton — all run state lives in Postgres rows and RunHandles."""

    _instance: Optional["TurnCoordinator"] = None

    @classmethod
    def get_instance(cls) -> "TurnCoordinator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------ START

    async def start_turn(
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
        turn_index: Optional[int] = None,
        attempt_no: int = 1,
        retry_of_run_id: Optional[str] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
    ) -> RunHandle:
        """Create the durable attempt: query row + in_progress run + projection.

        Raises RunSlotBusyError (409 admission), DuplicateRequestError
        (idempotent retransmit — reconnect to the existing run), or
        AttemptConflictError. A server-minted request_key is the legacy
        fallback only; callers should supply one for dedup to mean anything.
        """
        metadata = {"msg_type": msg_type, **(run_metadata or {})}
        row = await tl_db.start_run(
            run_id=run_id,
            thread_id=thread_id,
            request_key=request_key or str(uuid4()),
            turn_index=turn_index,
            attempt_no=attempt_no,
            retry_of_run_id=retry_of_run_id,
            query=query,
            metadata=metadata,
        )
        return RunHandle(
            run_id=run_id,
            thread_id=thread_id,
            turn_index=row["turn_index"],
            attempt_no=row["attempt_no"],
            request_key=str(row["request_key"]),
            msg_type=msg_type,
            workspace_id=workspace_id,
            user_id=user_id,
            query_id=query.query_id if query else None,
            is_byok=is_byok,
            started_at=row["created_at"],
        )

    # --------------------------------------------------------------- FINALIZE

    async def finalize_turn(
        self,
        handle: RunHandle,
        outcome: TurnOutcome,
        *,
        post_commit: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> FinalizeResult:
        """The single terminal transition. Idempotent: losing a finalize race
        (cancel vs owner) returns applied=False and performs no writes.

        Usage rows ride the same transaction — a billing persist failure
        aborts the terminal write with it and surfaces, instead of being
        swallowed into a zombie ACTIVE turn.
        """
        checkpoint_id = await self._latest_checkpoint_id(handle.thread_id)

        usage_writer = None
        if outcome.per_call_records or outcome.tool_usage:
            usage_writer = self._build_usage_writer(handle, outcome)

        if outcome.errors:
            outcome.errors = [
                sanitize_error_text(e) if isinstance(e, str) else e
                for e in outcome.errors
            ]

        result = await tl_db.finalize_run_idempotent(
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
            outbox_jobs=outcome.outbox_jobs,
        )
        handle.finalized = True

        if result.applied:
            if post_commit is not None:
                try:
                    await post_commit()
                except Exception:
                    logger.warning(
                        f"[TurnCoordinator] post_commit hook failed for "
                        f"run={handle.run_id}",
                        exc_info=True,
                    )
            self._schedule_projection_refresh(handle.thread_id)
            self._nudge_hook_drainer()
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
        operator intervention. Never raises.
        """
        if handle.finalized:
            return None
        try:
            return await self.finalize_turn(
                handle,
                TurnOutcome(
                    status=status,
                    metadata=metadata or {},
                    errors=[error_message],
                    execution_time=(
                        datetime.now(timezone.utc) - handle.started_at
                    ).total_seconds(),
                ),
            )
        except Exception:
            logger.error(
                f"[TurnCoordinator] fail_open_run could not finalize "
                f"run={handle.run_id}; slot remains held",
                exc_info=True,
            )
            return None

    async def sweep_stale_runs(self) -> int:
        """Startup-only (Phase 1, single worker): a server restart proves every
        open run's executor is dead, so finalize them — cancelled if durable
        cancel intent was recorded, else error(worker_lost). Phase 2 replaces
        this with the guard-acquiring recovery scanner.
        """
        swept = 0
        try:
            open_runs = await tl_db.list_open_runs()
        except Exception:
            logger.error("[TurnCoordinator] stale-run sweep query failed", exc_info=True)
            return 0
        for run in open_runs:
            run_id = str(run["conversation_response_id"])
            thread_id = str(run["conversation_thread_id"])
            status = "cancelled" if run.get("cancel_requested_at") else "error"
            try:
                # finalize_run's default derives the terminal hooks from the
                # row's START-stamped metadata — a crashed run still
                # releases its burst slot and clears its watch.
                result = await tl_db.finalize_run_idempotent(
                    run_id=run_id,
                    thread_id=thread_id,
                    status=status,
                    metadata={"recovery": "startup_sweep"},
                    errors=(
                        None
                        if status == "cancelled"
                        else ["worker_lost: server restarted while run was in progress"]
                    ),
                )
                if result.applied:
                    swept += 1
                    logger.warning(
                        f"[TurnCoordinator] swept stale run {run_id} "
                        f"(thread={thread_id}) -> {status}"
                    )
            except Exception:
                logger.error(
                    f"[TurnCoordinator] failed to sweep stale run {run_id}",
                    exc_info=True,
                )
        if swept:
            self._nudge_hook_drainer()
        return swept

    # ------------------------------------------------------------------ utils

    def _build_usage_writer(self, handle: RunHandle, outcome: TurnOutcome):
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

    async def _latest_checkpoint_id(self, thread_id: str) -> Optional[str]:
        try:
            from src.server.app import setup

            if not setup.checkpointer:
                return None
            cp = await setup.checkpointer.aget_tuple(
                {"configurable": {"thread_id": thread_id}}
            )
            return cp.config["configurable"]["checkpoint_id"] if cp else None
        except Exception:
            logger.warning(
                f"[TurnCoordinator] checkpoint_id read failed for {thread_id}",
                exc_info=True,
            )
            return None

    def _nudge_hook_drainer(self) -> None:
        """Post-commit hint only — the drainer's poll is the delivery
        guarantee, so a failed nudge is never an error."""
        try:
            from src.server.services.hook_outbox import HookOutboxDrainer

            HookOutboxDrainer.get_instance().nudge()
        except Exception:
            logger.warning("[TurnCoordinator] hook drainer nudge failed", exc_info=True)

    def _schedule_projection_refresh(self, thread_id: str) -> None:
        try:
            from src.server.services.history.projection_cache import (
                schedule_projection_refresh,
            )

            schedule_projection_refresh(thread_id)
        except Exception:
            logger.warning(
                f"[TurnCoordinator] projection refresh scheduling failed for "
                f"{thread_id}",
                exc_info=True,
            )
