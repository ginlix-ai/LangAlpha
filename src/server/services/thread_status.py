"""Thread runtime-status read model.

Aggregates a thread's public status from the run ledger (one latest-attempt
read — the in_progress slot always sorts newest), checkpoint metadata, and
the report-back slice. Read-only: nothing here transitions run state.
"""

import logging
from typing import Optional

from fastapi import HTTPException

from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Helper Functions for Checkpointer Access
# ============================================================================


async def get_checkpoint_tuple(thread_id: str, checkpoint_id: str = None):
    """
    Get checkpoint tuple from checkpointer.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional specific checkpoint ID

    Returns:
        CheckpointTuple or None if not found
    """
    checkpointer = get_checkpointer()
    config = build_checkpoint_config(thread_id, checkpoint_id)
    return await checkpointer.aget_tuple(config)


def extract_state_values(checkpoint_tuple) -> dict:
    """
    Extract state values from checkpoint tuple.

    The checkpoint contains serialized channel values that we can extract.
    """
    if not checkpoint_tuple or not checkpoint_tuple.checkpoint:
        return {}

    checkpoint = checkpoint_tuple.checkpoint
    channel_values = checkpoint.get("channel_values", {})

    # Return the channel values as state
    return channel_values


async def read_thread_runtime_status(
    thread_id: str, is_shared: Optional[bool] = None, msg_type: Optional[str] = None
) -> dict:
    """
    Get current workflow execution status.

    Args:
        thread_id: Thread ID to check status for
        is_shared: Pre-resolved share flag. When provided (e.g. the ``/status``
            route already read it while authorizing), skips a redundant thread
            lookup; when ``None``, this fetches it.
        msg_type: Pre-resolved thread kind; picks the report-back read model
            (``ptc`` → task outbox, else flash watch set). Fetched with the
            thread row when ``None`` and the row is read anyway.

    Returns:
        Dict with current status, reconnectability, and progress info
    """
    try:
        # Checkpoint read feeds only the payload's `progress` garnish — never
        # status truth (a checkpoint with no pending sends says nothing about
        # whether a run is live).
        checkpoint_info = None
        try:
            checkpoint_tuple = await get_checkpoint_tuple(thread_id)
            if checkpoint_tuple:
                state_values = extract_state_values(checkpoint_tuple)
                checkpoint_data = checkpoint_tuple.checkpoint or {}
                pending_sends = checkpoint_data.get("pending_sends", [])

                checkpoint_info = {
                    "has_plan": False,  # PTC doesn't use plans
                    "has_final_report": bool(state_values.get("final_report")),
                    "completed": len(pending_sends) == 0,
                    "checkpoint_id": checkpoint_tuple.config.get(
                        "configurable", {}
                    ).get("checkpoint_id"),
                }
        except Exception as e:
            logger.debug(f"Could not fetch checkpoint info for {thread_id}: {e}")

        # The ledger decides (v4 2.4): ONE read of the latest attempt. The
        # in_progress slot ALWAYS sorts latest (turn_index DESC, attempt_no
        # DESC — a live run is the newest attempt of the newest turn), so a
        # single row answers live and settled alike; a get_active_run +
        # get_latest_attempt pair reads two snapshots and a START committing
        # between them yields status="running" with run_id=None and reconnect
        # disabled (review F7). Live rows speak the live vocabulary
        # (stopping = durable cancel intent), terminal rows their terminal
        # status, no row = idle. Same answer on every worker — the tracker
        # blob, the checkpoint status fallback, and the stale-ACTIVE heal are
        # gone from this path (the recovery scanner owns orphan convergence).
        from src.server.database.runs import lifecycle as tl_db
        from src.server.contracts.status import to_public

        run_id = None
        can_reconnect = False
        row_ts = None
        latest = await tl_db.get_latest_attempt(thread_id)
        if latest is not None and latest["status"] == "in_progress":
            run_id = str(latest["conversation_response_id"])
            status = to_public(
                latest["status"],
                cancel_requested_at=latest.get("cancel_requested_at"),
            )
            # Reconnect attaches to the shared Redis stream, so it is
            # worker-agnostic: no local-executor requirement. A crashed
            # owner's stream still terminates — the scanner's finalize
            # appends the visible run_end.
            can_reconnect = status in ("running", "stopping")
            row_ts = latest.get("created_at")
        else:
            status = to_public(latest["status"]) if latest is not None else "idle"
            row_ts = latest.get("created_at") if latest is not None else None
        last_update = row_ts.isoformat() if row_ts is not None else None

        # Include share status so the UI can show the correct icon without an
        # extra API call. The ``/status`` route resolves this while authorizing
        # and passes it in; only fetch when a caller didn't.
        if is_shared is None:
            try:
                from src.server.database.conversation import get_thread_by_id

                thread_row = await get_thread_by_id(thread_id)
                is_shared = bool(thread_row.get("is_shared")) if thread_row else False
                if msg_type is None and thread_row:
                    msg_type = thread_row.get("msg_type")
            except Exception as e:
                logger.debug(f"Could not fetch share status for {thread_id}: {e}")
                is_shared = False

        # Report-back pendingness AND the subagent ids for per-task reattach,
        # from the thread kind's own read model (task slice = outbox row +
        # ledger-backed active tasks; flash = Redis watch set, no subagents).
        # active_tasks is NOT gated on an in_progress run: tail-mode
        # subagents outlive the run's terminal row.
        from src.server.services.report_back.flash.status import read_report_back_slice

        rb = await read_report_back_slice(thread_id, msg_type)
        active_tasks = rb.get("active_tasks", [])
        pending_report_back = rb["pending_report_back"]
        report_back_run_id = rb["report_back_run_id"]
        recent_report_back_run_ids = rb.get("recent_report_back_run_ids", [])

        # Staleness signal for cached (never-unmounted) frontend views: once a
        # run is terminal there is no reconnectable run_id, so the persisted
        # turn counter is the only way such a view can tell it missed a turn.
        # Present for terminal AND live threads; None when the thread has no
        # persisted turns (or the read failed).
        from src.server.database.conversation import get_latest_turn_index

        latest_turn_index = await get_latest_turn_index(thread_id)

        response = {
            "thread_id": thread_id,
            "run_id": run_id,
            "status": status,
            "can_reconnect": can_reconnect,
            "latest_turn_index": latest_turn_index,
            "last_update": last_update,
            "progress": checkpoint_info,
            "active_tasks": active_tasks,
            "is_shared": is_shared,
            "pending_report_back": pending_report_back,
            "report_back_run_id": report_back_run_id,
            "recent_report_back_run_ids": recent_report_back_run_ids,
        }

        logger.debug(f"Status check for {thread_id}: {status}")

        return response

    except Exception as e:
        logger.exception(f"Error checking workflow status for {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to check workflow status: {str(e)}"
        )