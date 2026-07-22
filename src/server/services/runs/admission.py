"""Thread admission: the pre-START wait policy, RunScope, and the START entry.

``wait_for_admission`` decides whether a new turn may start on a thread
(ledger-driven, identical on every worker); ``RunScope`` owns the
admitted run's resources until handoff to the executor.

The burst-slot lease, the per-thread admission lock, and the open START row
were previously tracked by hand-rolled flags (``slot_owned`` /
``admission_held``) duplicated across both workflow generators, the HTTP
handlers, and the error path — 26 scattered release sites. RunScope is the
one state machine: it owns failure cleanup from admission until ownership is
handed to the executor (``transfer_to_executor``, at the BTM done-callback);
after that the executor/outbox owns the durable release. Every release is
idempotent under the ownership flag.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Literal, Optional

from fastapi import HTTPException

from src.server.models.chat import ChatRequest

from src.config.settings import (
    get_admission_compaction_wait_timeout,
    get_checkpoint_flush_timeout,
    get_compaction_timeout,
)

# Module import (not symbol) so definition-site patches on
# usage_limits.release_burst_slot intercept every scope-routed release.
from src.server.dependencies import usage_limits

logger = logging.getLogger(__name__)


class RunScope:
    def __init__(
        self, *, user_id: Optional[str], burst_slot_id: Optional[str]
    ) -> None:
        self._user_id = user_id
        self._burst_slot_id = burst_slot_id
        self._slot_owned = True
        self._admission_lock: Optional[asyncio.Lock] = None
        self._run_handle: Any = None

    @property
    def slot_owned(self) -> bool:
        return self._slot_owned

    @property
    def owned_run_handle(self) -> Any:
        """The open START row, only while this scope still owns cleanup.

        After handoff (or release) the run belongs to the executor — error
        paths must not finalize it from here.
        """
        return self._run_handle if self._slot_owned else None

    def attach_run(self, run_handle: Any) -> None:
        self._run_handle = run_handle

    def hold_admission(self, lock: asyncio.Lock) -> None:
        self._admission_lock = lock

    def release_admission(self) -> None:
        """Idempotent: safe from both the controlled paths and the finally."""
        if self._admission_lock is not None:
            self._admission_lock.release()
            self._admission_lock = None

    async def release_slot(self) -> None:
        """Release the burst lease at most once from in-process paths."""
        if not self._slot_owned:
            return
        self._slot_owned = False
        await usage_limits.release_burst_slot(self._user_id, self._burst_slot_id)

    def transfer_to_executor(self) -> None:
        """Executor's done-callback is armed — it owns cleanup from here."""
        self._slot_owned = False

    async def fail_open(self, reason: str, *, status: str = "cancelled") -> None:
        """Death-path teardown: release the lease and settle the open run.

        fail_open_run shields its write internally, so a second cancel on an
        already-cancelled stream task cannot abort it.
        """
        run_handle = self.owned_run_handle
        await self.release_slot()
        if run_handle is not None:
            from src.server.services.runs.coordinator import RunCoordinator

            await RunCoordinator.get_instance().fail_open_run(
                run_handle, reason, status=status
            )


# Margin added to the checkpoint-flush timeout when a new turn waits for a
# stopping turn's teardown to finish. Teardown does more than flush (subagent
# drain, registry clear, persist), so the wait must outlast the flush alone;
# past it, admission returns "stopping" → 409 retry rather than racing a
# second checkpoint writer.
ADMISSION_TEARDOWN_MARGIN_S = 2.0

# Admission floors its compaction wait at compaction_timeout + this margin so
# a healthy in-progress compaction is never 409'd before its own call budget
# self-terminates. The margin covers the compaction's post-LLM work (state
# write + persistence) and the except-handler cleanup that finally sets the
# guard's Event after the call returns or times out.
COMPACTION_ADMISSION_MARGIN_S = 20.0


async def wait_for_admission(
    thread_id: str,
    *,
    local_task_probe: Callable[[str, str], Awaitable[Optional[asyncio.Task]]],
) -> tuple[
    Literal["fresh", "running", "stopping", "compacting"],
    Optional[Dict[str, Any]],
]:
    """Decide whether a new turn can start on ``thread_id``.

    Ledger-driven (v4 2.4c): the thread's in_progress row decides, so
    the answer is identical on every worker — a run live on a peer must
    route to steering, never to a doomed START. ``local_task_probe``
    (the executor registry's lookup) is consulted only as a fast path
    for awaiting a stopping run's teardown. Returns ``(state, active_row)``:

    - ``("fresh", None)`` — no live run: start a new turn.
    - ``("running", row)`` — a run is live (any worker): steer it
      (or 409 if steering fails). The row lets the caller run-stamp.
    - ``("stopping", row)`` — durable cancel intent whose teardown
      outlived the wait: 409 "stopping, retry" (never start a second
      writer while the checkpoint flush may still be running).
    - ``("compacting", None)`` — a thread mutation outlived the wait:
      409 "compacting, retry".
    """
    # Hold the new turn until any in-progress mutation finishes (a local
    # op's done-Event, or another worker's advertised op key), then read
    # the slot: an auto compaction leaves the turn's row in_progress
    # (caller steers); a manual mutation leaves no row (caller starts
    # fresh).
    from src.server.services.thread_mutation import ThreadMutationRunner

    runner = ThreadMutationRunner.get_instance()
    if await runner.is_mutating(thread_id):
        # Floor the wait at compaction_timeout + margin so a healthy
        # compaction is never 409'd before its own call budget self-
        # terminates and the runner's finally closes the op.
        backstop = max(
            get_admission_compaction_wait_timeout(),
            get_compaction_timeout() + COMPACTION_ADMISSION_MARGIN_S,
        )
        if not await runner.wait_until_idle(thread_id, timeout=backstop):
            logger.warning(
                f"[Admission] Mutation on thread {thread_id} "
                f"did not finish within admission wait; rejecting new turn "
                f"with 409 (compacting)"
            )
            return "compacting", None

    from src.server.database.runs import lifecycle as tl_db

    row = await tl_db.get_active_run(thread_id)
    if row is None:
        return "fresh", None
    if row["cancel_requested_at"] is None:
        return "running", row

    # Stopping: durable cancel intent on the live row. Wait for the
    # teardown to settle — awaiting the local task when this worker
    # hosts the run (NEVER bare-await: it ends via CancelledError, and
    # ``asyncio.wait`` swallows it), else polling the slot until a
    # peer's finalize or the recovery scanner clears it.
    run_id = str(row["conversation_response_id"])
    local_task = await local_task_probe(thread_id, run_id)
    timeout = get_checkpoint_flush_timeout() + ADMISSION_TEARDOWN_MARGIN_S
    logger.info(
        f"[Admission] Waiting for stopping run "
        f"({thread_id}, {run_id}) to finish teardown (timeout={timeout}s)"
    )
    if local_task is not None:
        await asyncio.wait({local_task}, timeout=timeout)
    else:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            current = await tl_db.get_active_run(thread_id)
            if (
                current is None
                or str(current["conversation_response_id"]) != run_id
            ):
                break
            await asyncio.sleep(1.0)

    row = await tl_db.get_active_run(thread_id)
    if row is None:
        return "fresh", None
    if row["cancel_requested_at"] is None:
        # A new run raced in while the stopped one drained.
        return "running", row
    logger.warning(
        f"[Admission] Stopping run on {thread_id} still live "
        f"after {timeout}s; rejecting new turn with 409"
    )
    return "stopping", row


async def dedup_retransmit_or_raise(request: ChatRequest) -> None:
    """Resolve a retransmitted request_key to its existing run — or pass.

    Must run under the admission lock BEFORE any steering, fork, or retry
    path can act on the duplicate: steering would inject the retransmit
    into the live run as a new message; a fork retransmit would truncate
    the very rows holding the key. START's unique index remains the
    race-safe backstop for keys that haven't produced a row yet.
    """
    if not request.request_key:
        return
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services.runs.coordinator import DuplicateRequestError

    existing = await tl_db.find_run_by_request_key(request.request_key)
    if existing is not None:
        raise DuplicateRequestError(existing)


async def resolve_retry_of(request: ChatRequest, thread_id: str):
    """Resolve and re-validate the attempt-chain predecessor for a retry.

    The /retry route validated latest-attempt + retryable-terminal before
    dispatch, but the generator may run later (dispatched flows) — re-check
    against live state so a stale retry can't chain onto the wrong run.
    Returns the predecessor row, or None when this isn't a retry.
    """
    if not request.retry_of_run_id:
        return None
    if request.fork_from_turn is not None:
        # A fork truncates; a retry chains. Combining them would truncate
        # first and then chain onto a deleted predecessor.
        raise HTTPException(
            status_code=400,
            detail="retry_of_run_id cannot be combined with fork_from_turn",
        )
    from src.server.database.runs import lifecycle as tl_db

    prev = await tl_db.get_run(request.retry_of_run_id)
    if prev is None or str(prev["conversation_thread_id"]) != thread_id:
        # Provenance is route-internal, so a vanished predecessor means a
        # fork/delete truncated it after route validation — a stale retry.
        # Structured 409 routes through the no-persist protocol branch;
        # an unstructured 404 here would hit mark_failed and could clobber
        # the newer turn's tracker state.
        latest = await tl_db.get_latest_attempt(thread_id)
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_retry",
                "message": "The run to retry no longer exists on this thread.",
                "latest_run_id": (
                    str(latest["conversation_response_id"]) if latest else None
                ),
                "latest_status": latest["status"] if latest else None,
            },
        )
    if prev["status"] != "error":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "not_retryable",
                "message": f"Run to retry is {prev['status']}, not a failed run.",
            },
        )
    latest = await tl_db.get_latest_attempt(thread_id)
    if latest is None or str(latest["conversation_response_id"]) != str(
        prev["conversation_response_id"]
    ):
        # Newer turns/attempts landed between route validation and this
        # generator running — retrying an older failure would fork history.
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_retry",
                "message": "The run to retry is no longer the latest attempt.",
                "latest_run_id": (
                    str(latest["conversation_response_id"]) if latest else None
                ),
                "latest_status": latest["status"] if latest else None,
            },
        )
    return prev


async def begin_run(
    request: "ChatRequest",
    *,
    thread_id: str,
    run_id: str,
    msg_type: str,
    workspace_id: Optional[str],
    user_id: Optional[str],
    is_byok: bool,
    query_content,
    query_type,
    feedback_action,
    query_metadata: dict,
    fork,
    is_checkpoint_replay: bool,
    extra_run_metadata: Optional[dict] = None,
):
    """The one START-txn entrypoint: maps a ChatRequest onto the durable
    attempt chain — retries chain onto the failed run's turn, forked
    checkpoint replays pin their turn and reuse the preserved query row,
    everything else allocates MAX+1 — so the derivation can never drift
    between agent modes. ``fork`` (a ForkSpec) executes its truncation +
    checkpoint pin inside the same transaction."""
    from uuid import uuid4

    from src.server.services.runs.coordinator import QuerySpec, RunCoordinator

    retry_of = await resolve_retry_of(request, thread_id)
    return await RunCoordinator.get_instance().start_run(
        thread_id=thread_id,
        run_id=run_id,
        msg_type=msg_type,
        request_key=request.request_key,
        workspace_id=workspace_id,
        user_id=user_id,
        is_byok=is_byok,
        query=(
            None
            if is_checkpoint_replay
            else QuerySpec(
                query_id=str(uuid4()),
                content=query_content,
                query_type=query_type,
                feedback_action=feedback_action,
                metadata=query_metadata,
            )
        ),
        fork=fork,
        turn_index=(
            retry_of["turn_index"]
            if retry_of is not None
            else request.fork_from_turn
            if (fork is not None and is_checkpoint_replay)
            else None
        ),
        attempt_no=(retry_of["attempt_no"] + 1 if retry_of is not None else 1),
        retry_of_run_id=(
            str(retry_of["conversation_response_id"])
            if retry_of is not None
            else None
        ),
        # Durable on the row so the startup sweep can enqueue this run's
        # terminal hooks (burst release, watch clear) without any in-process
        # context surviving the crash.
        run_metadata={
            "user_id": user_id,
            "burst_slot_id": request.burst_slot_id,
            **(extra_run_metadata or {}),
        },
    )