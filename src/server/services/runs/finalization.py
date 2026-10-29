"""State-free half of the run finalize: outcome classification, artifact
assembly, and the single terminal CAS.

Called from ``LocalRunExecutor._finalize_run``, which owns the
executor-local half (task-table reads, local terminal mark, post-terminal
effects). Everything here operates on values the caller resolved — no task
table, no locks.
"""

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import Literal, Optional

from ptc_agent.agent.middleware.image_capture import CAPTURE_TIMEOUT_SECONDS
from src.config.settings import get_checkpoint_flush_timeout
from src.server.contracts.status import (
    INTERRUPT_REASON_CREDIT_PAUSE,
    classify_interrupts,
    is_user_stop,
)
from src.server.database.runs.hook_jobs import WAKEUP_QUIET_META
from src.server.database.runs.lifecycle import ProducerRecord, RunOutcome
from src.server.services.runs.stream_errors import (
    failure_kind_metadata,
    model_call_failure,
)
from src.server.utils.persistence_utils import (
    calculate_execution_time,
    get_token_usage_from_callback,
    get_tool_usage_from_handler,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FinalizeVerdict:
    """What one finalize CAS settled, for the effects that follow it.

    ``applied`` gates every terminal business effect: only the winner runs
    them, with the status it adopted. A loser carries the survivor's status
    for its local mark and runs nothing. ``stopped_by_user`` is read off the
    winning row, so a stop another worker took counts.
    """

    applied: bool
    status: str
    survivor_status: Optional[str] = None
    stopped_by_user: bool = False

    @property
    def concluded(self) -> bool:
        """The row is terminal. False only for a thrown finalize, which leaves
        it in_progress for recovery."""
        return self.applied or self.survivor_status is not None


async def classify_outcome(
    kind: Literal["stream_end", "cancelled", "failed"],
    *,
    handler,
    graph,
    thread_id: str,
    error: Optional[str],
) -> tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """In-band outcome classification -> (status, phase, interrupt_reason,
    pause_message, error). Interrupted-vs-completed comes from the streaming
    handler's durability barrier; the timeout-prone aget_state probe survives
    only for handler-less runs."""
    interrupt_reason: Optional[str] = None
    pause_message: Optional[str] = None
    if kind == "cancelled":
        status, phase = "cancelled", "cancellation"
    elif kind == "failed":
        status, phase = "error", "error"
    elif handler is not None and getattr(handler, "saw_interrupt", False):
        if handler.interrupt_verified:
            status, phase = "interrupted", "interrupt"
            # Already classified by the handler's durability barrier
            # (classify_interrupts over the buffered payloads) — never
            # respelled here.
            interrupt_reason = handler.interrupt_reason
            pause_message = handler.pause_message
        else:
            # I8: a pause that never reached the checkpointer must not
            # advertise resumability.
            status, phase = "error", "error"
            error = error or (
                "interrupt_not_durable: the pause was not checkpointed"
            )
    elif handler is not None:
        status, phase = "completed", "completion"
    else:
        # Handler-less run (defensive): legacy state probe as fallback.
        status, phase = "completed", "completion"
        try:
            if graph:
                snapshot = await asyncio.wait_for(
                    graph.aget_state({"configurable": {"thread_id": thread_id}}),
                    timeout=get_checkpoint_flush_timeout(),
                )
                if snapshot and snapshot.next:
                    status, phase = "interrupted", "interrupt"
                    interrupt_reason, pause_message = classify_interrupts(
                        intr
                        for task in (snapshot.tasks or ())
                        for intr in (getattr(task, "interrupts", ()) or ())
                    )
        except Exception:
            logger.warning(
                f"[Finalize] fallback state probe failed for "
                f"({thread_id}, ...)",
                exc_info=True,
            )
    return status, phase, interrupt_reason, pause_message, error


async def assemble_finalize_artifacts(
    key: tuple,
    *,
    metadata: dict,
    status: str,
    phase: str,
    interrupt_reason: Optional[str],
    pause_message: Optional[str],
    error: Optional[str],
    handler,
    cancelled_by_user: bool,
    workspace_id: Optional[str],
    user_id: Optional[str],
    stop_events: Optional[list[dict]] = None,
    exc: Optional[BaseException] = None,
) -> RunOutcome:
    """Build everything the finalize CAS writes: usage records, the persist
    metadata and the producer's record.

    ``stop_events`` are the subagent events a stop teardown drained from the
    tasks its kill evicted, which the caller passes only for a cancelled run.
    """
    execution_time = calculate_execution_time(metadata)
    thread_id = key[0]
    _, per_call_records = get_token_usage_from_callback(metadata, phase, thread_id)
    tool_usage = get_tool_usage_from_handler(metadata, phase, thread_id)

    persist_metadata = {
        "stock_code": metadata.get("stock_code"),
        "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
        "deepthinking": metadata.get("deepthinking", False),
        "is_byok": metadata.get("is_byok", False),
    }
    for extra in ("workspace_id", "sandbox_id", "locale", "timezone"):
        if metadata.get(extra):
            persist_metadata[extra] = metadata[extra]
    if status == "cancelled":
        persist_metadata["cancelled_by_user"] = cancelled_by_user
    if exc is not None:
        persist_metadata.update(
            model_call_failure(
                exc,
                getattr(
                    getattr(handler, "agent_config", None), "credential_source", None
                ),
            )
        )
        if status == "error":
            persist_metadata.update(failure_kind_metadata(exc))
    # The pause's own words, which settling the run's automation relays. A
    # pause without words is stamped null, so settling knows there is
    # nothing to relay.
    if interrupt_reason == INTERRUPT_REASON_CREDIT_PAUSE:
        persist_metadata["credit_pause_message"] = pause_message
    # A wake turn the agent ended quietly, which the settle keeps the thread
    # seen over when it changed nothing.
    quiet = getattr(handler, "quiet", None)
    if quiet is not None:
        persist_metadata[WAKEUP_QUIET_META] = {"changed": quiet["changed"]}
    # Steering inputs archive on the owning response (v4 identity model:
    # steering = no run, no turn), from the steer rows this run confirmed
    # delivered.
    steering_inputs = await _delivered_steering_inputs(key)
    if steering_inputs:
        persist_metadata["steering_inputs"] = steering_inputs
    if not (workspace_id and user_id):
        # Usage rows need both; without them the ledger transition still
        # happens, billing artifacts are simply absent.
        per_call_records = None
        tool_usage = None

    return RunOutcome(
        status=status,
        interrupt_reason=interrupt_reason,
        metadata=persist_metadata,
        errors=[error] if error else None,
        execution_time=execution_time,
        per_call_records=per_call_records,
        tool_usage=tool_usage,
        record=await _producer_record(key, handler, stop_events, status),
        images=await _late_images(key, metadata, handler),
    )


async def _delivered_steering_inputs(key: tuple) -> list[str]:
    """The steers this run read, oldest first. Empty on a read fault: the
    archive is a record, and the run must still settle."""
    from src.server.database.runs import outbox as outbox_db

    try:
        rows = await outbox_db.list_delivered_steers(key[1])
    except Exception:
        logger.warning(
            f"[Finalize] delivered steers unreadable for run {key[1]}",
            exc_info=True,
        )
        return []
    return [row["content"] for row in rows if row.get("content")]


async def _late_images(key: tuple, metadata: dict, handler) -> dict[str, str]:
    """The URLs of the sandbox images the run's saved text still names,
    which the model-time capture left as paths. Captured on every ending,
    since a stopped, failed or paused run kept its earlier steps' text, and
    bounded like that capture: a run whose sandbox died must still settle."""
    artifact_hook = metadata.get("artifact_hook")
    image_paths = getattr(handler, "saved_image_paths", None)
    if not (artifact_hook and image_paths):
        return {}
    try:
        return await asyncio.wait_for(
            artifact_hook(image_paths), CAPTURE_TIMEOUT_SECONDS
        )
    except Exception:
        logger.warning(
            f"[Finalize] late image capture failed for {key}; left as paths",
            exc_info=True,
        )
        return {}


async def _producer_record(
    key: tuple, handler, stop_events, status: str
) -> Optional[ProducerRecord]:
    """The producer's record, or None when it cannot be read: a raise here
    would leave the row in_progress, where a missing record only costs the
    row its replay facts."""
    from src.server.services.history.committed import UNSAVED_RESULT_ENDINGS

    if handler is None:
        return None
    try:
        record = handler.record(stop_events)
        if status != "error":
            # Replay shows the frame only beside the failure the row records.
            record = replace(record, error_frame=None)
        if status not in UNSAVED_RESULT_ENDINGS:
            # Every step of a run that ended otherwise committed its notices.
            return replace(record, model_fallbacks=[])
        if record.provenance:
            record = replace(
                record, provenance=await _saved_provenance(key, record.provenance)
            )
        return record
    except Exception:
        logger.warning(f"[Finalize] producer record unavailable for {key}", exc_info=True)
        return None


async def _saved_provenance(key: tuple, provenance: list[dict]) -> list[dict]:
    """A stopped or failed run's provenance for the tool calls its
    checkpoints saved. The stop can land after one of a step's parallel calls
    streamed its sources and before the step saved its result, which the
    turn then never shows. Reads that outlast the flush timeout keep them
    all, as a lane whose read fails does."""
    from src.server.services.history.committed import saved_lane_provenance

    thread_id, run_id = key
    try:
        return await asyncio.wait_for(
            saved_lane_provenance(
                thread_id, provenance, run_id=run_id, log_tag="[Finalize]"
            ),
            timeout=get_checkpoint_flush_timeout(),
        )
    except Exception:
        logger.error(
            f"[Finalize] checkpoint reads failed for {key}; its provenance is "
            f"recorded unchecked",
            exc_info=True,
        )
        return provenance


async def drive_finalize_cas(
    key: tuple, handle, outcome, *, tail_drain=None
) -> FinalizeVerdict:
    """One finalize CAS, and the verdict the post-terminal effects act on.

    Winners adopt the row's final status (durable cancel intent can flip
    it); losers report the survivor's. A failed persist leaves the row
    in_progress, honest and recoverable, never a masked terminal turn.
    """
    from src.server.services.runs.coordinator import RunCoordinator

    status = outcome.status
    try:
        # 1.5: no post_commit DEL — the stream is retained to its
        # redis_event_ttl so post-terminal reconnects replay from
        # Redis and see the run_end frame appended after the commit.
        result = await RunCoordinator.get_instance().finalize_run(
            handle,
            outcome,
            tail_drain=tail_drain,
        )
        run = result.run or {}
        if result.applied:
            final_status = run.get("status")
            if final_status and final_status != status:
                logger.info(
                    f"[Finalize] finalize adopted durable "
                    f"cancel for {key}: {status} -> {final_status}"
                )
                status = final_status
            return FinalizeVerdict(
                applied=True, status=status, stopped_by_user=is_user_stop(run)
            )
        survivor_status = run.get("status")
        logger.warning(
            f"[Finalize] lost finalize race for {key}: "
            f"row already {survivor_status} (wanted {status}); "
            f"terminal side effects skipped"
        )
        return FinalizeVerdict(
            applied=False, status=status, survivor_status=survivor_status
        )
    except Exception:
        logger.critical(
            f"[Finalize] FINALIZE FAILED for {key}: run row "
            f"remains in_progress for recovery",
            exc_info=True,
        )
        return FinalizeVerdict(applied=False, status=status)
