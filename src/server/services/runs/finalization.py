"""State-free half of the run finalize: outcome classification, artifact
assembly, and the single terminal CAS.

Called from ``LocalRunExecutor._finalize_run``, which owns the
executor-local half (task-table reads, local terminal mark, post-terminal
effects). Everything here operates on values the caller resolved — no task
table, no locks.
"""

import asyncio
import logging
from typing import Literal, Optional

from src.config.settings import get_checkpoint_flush_timeout
from src.server.utils.persistence_utils import (
    calculate_execution_time,
    get_sse_events_from_handler,
    get_token_usage_from_callback,
    get_tool_usage_from_handler,
)

logger = logging.getLogger(__name__)


async def classify_outcome(
    kind: Literal["stream_end", "cancelled", "failed"],
    *,
    handler,
    graph,
    thread_id: str,
    error: Optional[str],
) -> tuple[str, str, Optional[str], Optional[str]]:
    """In-band outcome classification -> (status, phase, interrupt_reason,
    error). Interrupted-vs-completed comes from the streaming handler's
    durability barrier; the timeout-prone aget_state probe survives only
    for handler-less runs."""
    interrupt_reason: Optional[str] = None
    if kind == "cancelled":
        status, phase = "cancelled", "cancellation"
    elif kind == "failed":
        status, phase = "error", "error"
    elif handler is not None and getattr(handler, "saw_interrupt", False):
        if handler.interrupt_verified:
            status, phase = "interrupted", "interrupt"
            # Already classified by the handler's durability barrier
            # (classify_interrupt_reason over the buffered payloads) —
            # never respelled here.
            interrupt_reason = handler.interrupt_reason
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
                    from src.server.contracts.status import (
                        classify_interrupt_reason,
                    )

                    status, phase = "interrupted", "interrupt"
                    interrupt_reason = classify_interrupt_reason(
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
    return status, phase, interrupt_reason, error


async def assemble_finalize_artifacts(
    key: tuple,
    *,
    metadata: dict,
    status: str,
    phase: str,
    handler,
    cancelled_by_user: bool,
    workspace_id: Optional[str],
    user_id: Optional[str],
) -> tuple:
    """Build everything the finalize CAS archives: usage records, the
    (possibly stop-reconciled) sse_events, and the persist metadata."""
    execution_time = calculate_execution_time(metadata)
    thread_id = key[0]
    _, per_call_records = get_token_usage_from_callback(metadata, phase, thread_id)
    tool_usage = get_tool_usage_from_handler(metadata, phase, thread_id)

    # User-pressed Stop reconciles the transcript (close open reasoning /
    # tool-call / artifact structures) so replay doesn't render zombies;
    # system cancels leave raw events untouched.
    sse_events = None
    if (
        status == "cancelled"
        and cancelled_by_user
        and handler is not None
        and hasattr(handler, "finalize_stopped_events")
    ):
        try:
            sse_events = handler.finalize_stopped_events()
        except Exception as recon_err:
            logger.warning(
                f"[Finalize] finalize_stopped_events failed "
                f"for {key}: {recon_err}"
            )
    if sse_events is None:
        sse_events = get_sse_events_from_handler(metadata, phase, thread_id)
    if status == "cancelled":
        stop_subagent_events = metadata.get("_stop_subagent_events")
        if stop_subagent_events:
            sse_events = (sse_events or []) + stop_subagent_events

    persist_metadata = {
        "msg_type": metadata.get("msg_type"),
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
    # Steering inputs archive on the owning response (v4 identity model:
    # steering = no run, no turn). Replaces the old backfill that
    # fabricated query rows for orphan turn indexes.
    if handler is not None and getattr(handler, "injected_steerings", None):
        persist_metadata["steering_inputs"] = [
            m.get("content")
            for m in handler.injected_steerings
            if m.get("content")
        ]
    if not (workspace_id and user_id):
        # Usage rows need both; without them the ledger transition still
        # happens, billing artifacts are simply absent.
        per_call_records = None
        tool_usage = None

    # Pre-finalize artifact hook (sandbox image capture → storage URL
    # rewrite) must run before sse_events are archived.
    artifact_hook = metadata.get("artifact_hook")
    if status == "completed" and artifact_hook and sse_events:
        try:
            await artifact_hook(sse_events)
        except Exception:
            logger.warning(
                f"[Finalize] artifact hook failed for {key}",
                exc_info=True,
            )
    return execution_time, per_call_records, tool_usage, sse_events, persist_metadata


async def drive_finalize_cas(
    key: tuple, handle, outcome, *, tail_drain=None
) -> tuple[bool, str, Optional[str]]:
    """One finalize CAS -> (applied, adopted_status, survivor_status).

    Winners adopt the row's final status (durable cancel intent can flip
    it); losers report the survivor's. A failed persist leaves the row
    in_progress — honest and recoverable — never a masked terminal turn.
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
        if result.applied:
            final_status = (result.run or {}).get("status")
            if final_status and final_status != status:
                logger.info(
                    f"[Finalize] finalize adopted durable "
                    f"cancel for {key}: {status} -> {final_status}"
                )
                status = final_status
            return True, status, None
        survivor_status = (result.run or {}).get("status")
        logger.warning(
            f"[Finalize] lost finalize race for {key}: "
            f"row already {survivor_status} (wanted {status}); "
            f"terminal side effects skipped"
        )
        return False, status, survivor_status
    except Exception:
        logger.critical(
            f"[Finalize] FINALIZE FAILED for {key}: run row "
            f"remains in_progress for recovery",
            exc_info=True,
        )
        return False, status, None
