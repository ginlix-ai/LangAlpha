"""
Workflow Handler — Business logic for workflow control operations.

Extracted from src/server/app/workflow.py to separate business logic from route definitions.
"""

import asyncio
import logging
from typing import Optional

from fastapi import HTTPException

from src.server.handlers.cancellation import cancellation_as_http
from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
)

# Import setup module to access initialized globals
from src.server.app import setup

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


async def cancel_workflow(thread_id: str, run_id: Optional[str] = None) -> dict:
    """
    Explicitly cancel a workflow execution (user stop).

    Signal-only: sets the cancel flag, marks status, and force-cancels the
    in-flight task via ``manager.cancel_workflow`` (which interrupts the
    current step immediately). The subagent kill + registry wipe is owned by
    the single-owner teardown in ``BackgroundTaskManager`` when the
    ``CancelledError`` lands — this handler only runs ``cancel_and_clear`` as a
    safety net when no active task exists (e.g. an orphaned registry left by a
    crash), so the deterministic teardown sequence (flush → drain → clear →
    persist) isn't raced.

    ``run_id`` targets a specific run so a slow/retried stop can't cancel a
    *newer* turn the user started after the stopped one finished (the manager
    otherwise falls back to "latest active run"). Omitted = latest active run.

    Args:
        thread_id: Thread ID to cancel
        run_id: Specific run to cancel; None falls back to the latest active run

    Returns:
        Confirmation of cancellation with thread_id
    """
    try:
        from src.server.services.background_task_manager import (
            BackgroundTaskManager,
        )

        manager = BackgroundTaskManager.get_instance()
        has_active = await manager.has_active_task_for_thread(thread_id)

        # Manual compaction stop. A manual /compact|/offload registers no
        # workflow task (it runs inside its own HTTP request handler), so when
        # there is no active workflow, cancelling the in-flight compaction is
        # the entire job. Take this path before the workflow-cancel tracker
        # writes below (cancel flag / mark_cancelled / "cancelled" thread
        # status) so a pure compaction stop doesn't mislabel the thread as a
        # stopped turn. (An AUTO compaction runs inside the turn's task — there
        # has_active is True, so we fall through and cancel_workflow's
        # inner_task cancel interrupts the summarize.)
        if not has_active and manager.cancel_compaction(thread_id):
            logger.info(f"Manual compaction stopped by user: {thread_id}")
            return {
                "cancelled": True,
                "thread_id": thread_id,
                "message": "Compaction stopped.",
            }

        from src.server.services.workflow_tracker import WorkflowTracker

        tracker = WorkflowTracker.get_instance()

        # A /cancel that reaches here with no BTM task AND no in-flight
        # compaction is almost always a Stop click racing a compaction that
        # JUST finished (its finally already cleared the guard). Marking such an
        # idle thread "cancelled" would mislabel a successful compaction as a
        # stopped turn, so only write the cancel signal/status when a turn is
        # genuinely active — a BTM task, or a tracker-reported ACTIVE/INTERRUPTED
        # dispatched turn. The orphan-registry safety net below still runs.
        turn_is_active = has_active or await _thread_turn_is_active(
            tracker, thread_id
        )

        success = True
        if turn_is_active:
            # Set cancellation flag (checked by exception handler)
            success = await tracker.set_cancel_flag(thread_id)

            # Mark workflow as cancelled immediately for fast frontend feedback.
            await tracker.mark_cancelled(thread_id)

            # Update thread status in database for consistency
            from src.server.database import conversation as qr_db

            await qr_db.update_thread_status(thread_id, "cancelled")

        cancel_success = await manager.cancel_workflow(thread_id, run_id)

        if not cancel_success and not await manager.has_active_task_for_thread(
            thread_id
        ):
            logger.warning(
                f"Could not cancel background task for {thread_id} "
                "(may be already completed or not found)"
            )
            # Safety net: no active task owns the teardown, so wipe any
            # orphaned registry left behind (e.g. after a crash). When a task
            # IS active, its except-handler teardown owns cancel_and_clear and
            # we must NOT race it here — nor wipe a *different* still-running
            # turn's registry when a run-targeted cancel missed its run.
            from src.server.services.background_registry_store import (
                BackgroundRegistryStore,
            )

            registry_store = BackgroundRegistryStore.get_instance()
            await registry_store.cancel_and_clear(thread_id, force=True)

        if not success:
            logger.warning(
                f"Failed to set cancel flag for {thread_id} (Redis may be unavailable)"
            )

        logger.info(f"Workflow cancelled: {thread_id}")

        return {
            "cancelled": True,
            "thread_id": thread_id,
            "message": "Cancellation signal sent. Workflow will stop shortly.",
        }

    except Exception as e:
        logger.exception(f"Error cancelling workflow {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to cancel workflow: {str(e)}"
        )


async def get_workflow_status(thread_id: str) -> dict:
    """
    Get current workflow execution status.

    Args:
        thread_id: Thread ID to check status for

    Returns:
        Dict with current status, reconnectability, and progress info
    """
    try:
        from src.server.services.workflow_tracker import (
            RECONNECTABLE_STATUSES,
            WorkflowStatus,
            WorkflowTracker,
        )

        tracker = WorkflowTracker.get_instance()

        # Get status from Redis
        redis_status = await tracker.get_status(thread_id)

        # Check checkpoint for additional info
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

        # Determine overall status
        if redis_status:
            status = redis_status.get("status", WorkflowStatus.UNKNOWN)
            last_update = redis_status.get("last_update")
            workspace_id = redis_status.get("workspace_id")
            user_id = redis_status.get("user_id")
        elif checkpoint_info and checkpoint_info.get("completed"):
            # Found in checkpoint but not in Redis = old completed workflow
            status = WorkflowStatus.COMPLETED
            last_update = None
            workspace_id = None
            user_id = None
        else:
            # Not in Redis, not in checkpoint = unknown
            status = WorkflowStatus.UNKNOWN
            last_update = None
            workspace_id = None
            user_id = None

        # Determine if reconnection is possible
        can_reconnect = status in RECONNECTABLE_STATUSES

        # Get subagent info from background task manager
        active_tasks = []
        run_id = None

        try:
            from src.server.services.background_task_manager import (
                BackgroundTaskManager,
            )

            manager = BackgroundTaskManager.get_instance()
            bg_status = await manager.get_workflow_status(thread_id)
            if bg_status.get("status") != "not_found":
                active_tasks = bg_status.get("active_tasks", [])
                run_id = bg_status.get("run_id")
            elif can_reconnect:
                # Redis says active/disconnected but BackgroundTaskManager has no
                # record — likely a stale Redis key surviving a server restart.
                # Downgrade can_reconnect to avoid a guaranteed 404 on /messages/stream.
                logger.info(
                    f"Stale workflow status for {thread_id}: Redis says {status} "
                    f"but BackgroundTaskManager has no task info. Clearing stale status."
                )
                can_reconnect = False
                status = WorkflowStatus.COMPLETED
                # Clean up the stale Redis key so future requests don't hit this path
                try:
                    await tracker.mark_completed(thread_id)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(
                f"Could not get background task status for {thread_id}: {e}"
            )

        # Include share status so the UI can show the correct icon without an extra API call
        is_shared = False
        try:
            from src.server.database.conversation import get_thread_by_id

            thread_row = await get_thread_by_id(thread_id)
            if thread_row:
                is_shared = bool(thread_row.get("is_shared"))
        except Exception as e:
            logger.debug(f"Could not fetch share status for {thread_id}: {e}")

        # Check if this flash thread has pending PTC report-backs
        # (flash_watch is a Redis SET of dispatched ptc_thread_ids). When
        # pending, surface the report-back run_id the backend recorded so the
        # client attaches to the exact run instead of inferring it from run_id
        # changes.
        pending_report_back = False
        report_back_run_id = None
        try:
            from src.server.handlers.chat.ptc_workflow import flash_rb_run_key
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if cache.enabled and cache.client:
                count = await cache.client.scard(f"flash_watch:{thread_id}")
                if count and count > 0:
                    pending_report_back = True
                    rb = await cache.get(flash_rb_run_key(thread_id))
                    if isinstance(rb, dict):
                        report_back_run_id = rb.get("run_id")
        except Exception:
            pass

        response = {
            "thread_id": thread_id,
            "run_id": run_id,
            "status": status,
            "can_reconnect": can_reconnect,
            "last_update": last_update,
            "workspace_id": workspace_id,
            "user_id": user_id,
            "progress": checkpoint_info,
            "active_tasks": active_tasks,
            "is_shared": is_shared,
            "pending_report_back": pending_report_back,
            "report_back_run_id": report_back_run_id,
        }

        logger.debug(f"Status check for {thread_id}: {status}")

        return response

    except Exception as e:
        logger.exception(f"Error checking workflow status for {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to check workflow status: {str(e)}"
        )


async def _resolve_graph_and_state(thread_id: str, verb: str, config=None) -> tuple:
    """Validate thread, build graph, get state, build backend.

    ``config`` is the resolved AgentConfig; defaults to ``setup.agent_config``.

    Returns:
        (graph, lg_config, state, messages, backend)
    """
    from src.server.database import conversation as qr_db
    from src.server.services.workspace_manager import WorkspaceManager
    from ptc_agent.agent.graph import build_ptc_graph_with_session
    from ptc_agent.agent.backends.sandbox import SandboxBackend

    # Validate thread + workspace
    thread_info = await qr_db.get_thread_with_summary(thread_id)
    if not thread_info:
        raise HTTPException(status_code=404, detail=f"Thread not found: {thread_id}")
    workspace_id = thread_info.get("workspace_id")
    if not workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Thread {thread_id} has no associated workspace",
        )

    # Session
    workspace_manager = WorkspaceManager.get_instance()
    try:
        session = await workspace_manager.get_session_for_workspace(workspace_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Graph
    checkpointer = get_checkpointer()
    effective_config = config if config is not None else setup.agent_config
    if not effective_config:
        raise HTTPException(
            status_code=500, detail="Agent configuration not initialized"
        )
    from src.server.app.workspace_sandbox import _set_cached_signed_url

    graph = await build_ptc_graph_with_session(
        session=session, config=effective_config, checkpointer=checkpointer,
        on_signed_url=_set_cached_signed_url,
    )

    # State with timeout
    lg_config = build_checkpoint_config(thread_id)
    try:
        state = await asyncio.wait_for(graph.aget_state(lg_config), timeout=10.0)
    except asyncio.TimeoutError:
        logger.error(f"aget_state timed out for thread {thread_id} during {verb}")
        raise HTTPException(
            status_code=504,
            detail=f"Timed out retrieving state for thread: {thread_id}",
        )
    if not state or not state.values:
        raise HTTPException(
            status_code=404, detail=f"No state found for thread: {thread_id}"
        )
    messages = state.values.get("messages", [])
    if not messages:
        raise HTTPException(status_code=400, detail=f"No messages to {verb}")

    # Backend
    backend = None
    if hasattr(session, "sandbox") and session.sandbox is not None:
        backend = SandboxBackend(session.sandbox)

    return graph, lg_config, state, messages, backend


async def _update_graph_state(
    graph, config: dict, values: dict, thread_id: str, verb: str
) -> None:
    """Timeout-wrapped aupdate_state call."""
    try:
        await asyncio.wait_for(graph.aupdate_state(config, values), timeout=10.0)
    except asyncio.TimeoutError:
        logger.error(f"aupdate_state timed out for thread {thread_id} during {verb}")
        raise HTTPException(
            status_code=504,
            detail=f"Timed out updating state for thread: {thread_id}",
        )


def _gate_unverifiable(verb: str) -> HTTPException:
    """409 raised when the workflow-active gate can't be verified under a
    fail-closed verb (offload)."""
    return HTTPException(
        status_code=409,
        detail={
            "code": "workflow_unverifiable",
            "verb": verb,
            "message": (
                f"Cannot safely {verb} right now: the thread's activity status "
                "is unavailable (the workflow tracker is down). Try again once "
                "it recovers."
            ),
        },
    )


async def _require_no_active_workflow(
    thread_id: str, verb: str, *, fail_closed: bool = False
) -> None:
    """Reject manual compact/offload while a workflow is running on the thread.

    Both /compact and /offload perform read-modify-write on LangGraph state and
    on ``conversation_responses.sse_events``. Running them concurrently with an
    in-flight chat workflow races the workflow's own writes (can clobber SSE
    events mid-stream, or overwrite checkpoint state the middleware just
    updated). Gate at the edge with a clear 409 so the UI can surface a
    "wait for the current turn to finish" banner instead of silently
    corrupting state.

    Raises HTTPException(409) for ACTIVE / INTERRUPTED. Allows
    None / COMPLETED / CANCELLED.

    When the gate can't be verified (tracker disabled / get_status raises) the
    default is to fail OPEN — chat workflows are already degraded under a Redis
    outage and admin actions should stay usable. ``fail_closed=True`` (used by
    /offload, a non-critical optimization) instead raises 409 so the action is
    skipped rather than run blind during a possibly-active turn.
    """
    from src.server.services.workflow_tracker import (
        WorkflowStatus,
        WorkflowTracker,
    )

    tracker = WorkflowTracker.get_instance()

    def _handle_unverifiable(reason: str) -> None:
        # The gate can't confirm the thread is idle — Redis down, so either the
        # tracker is disabled or get_status raised. Fail OPEN by default (log +
        # continue) so admin actions stay usable while chat is already degraded;
        # fail CLOSED (offload) by raising 409 so we never write blind into a
        # possibly-active turn.
        action = "skipped (fail-closed)" if fail_closed else "bypassed"
        logger.warning(
            f"[{verb}] {reason}; workflow-active gate {action} for thread {thread_id}"
        )
        if fail_closed:
            raise _gate_unverifiable(verb)

    if not getattr(tracker, "enabled", True):
        _handle_unverifiable("WorkflowTracker disabled (Redis unavailable)")
        return
    try:
        status = await tracker.get_status(thread_id)
    except Exception as e:
        _handle_unverifiable(f"WorkflowTracker.get_status failed: {e}")
        return
    if not status:
        return
    raw = status.get("status")
    # WorkflowStatus(str, Enum) makes enum instances compare equal to their
    # string values, so a single set membership check covers both the in-memory
    # enum form and the Redis-round-tripped string form.
    blocking = {
        WorkflowStatus.ACTIVE,
        WorkflowStatus.INTERRUPTED,
    }
    if raw in blocking:
        # code kept short + stable so the frontend can branch on it
        raise HTTPException(
            status_code=409,
            detail={
                "code": "workflow_active",
                "verb": verb,
                "message": (
                    f"Cannot {verb} while a response is streaming on this "
                    "thread. Wait for the current turn to finish, then try "
                    "again."
                ),
            },
        )


async def _thread_turn_is_active(tracker, thread_id: str) -> bool:
    """Best-effort: is a turn genuinely active on the thread?

    Returns True for a tracker-reported ACTIVE/INTERRUPTED turn, and True
    (fail-safe) when the tracker is disabled or errors — we cannot confirm the
    thread is idle, so a real cancel is never skipped. Returns False only when
    the tracker is reachable and reports no active turn.
    """
    from src.server.services.workflow_tracker import WorkflowStatus

    if not getattr(tracker, "enabled", True):
        return True
    try:
        status = await tracker.get_status(thread_id)
    except Exception:
        return True
    if not status:
        return False
    return status.get("status") in {
        WorkflowStatus.ACTIVE,
        WorkflowStatus.INTERRUPTED,
    }


def _open_manual_compaction(manager, thread_id: str, verb: str) -> None:
    """Open the per-thread compaction guard for a MANUAL /compact|/offload and
    register this request's task so a user Stop (/cancel) can interrupt the
    in-flight call. Raises 409 ``compaction_in_progress`` if another compaction
    already holds the thread (reject rather than clobber it). Callers MUST pair
    this with ``_close_manual_compaction`` in a finally.
    """
    if not manager.begin_compaction(thread_id):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "compaction_in_progress",
                "verb": verb,
                "message": (
                    "Another compaction is already running on this thread. "
                    "Wait for it to finish, then try again."
                ),
            },
        )
    task = asyncio.current_task()
    if task is not None:
        manager.set_compaction_task(thread_id, task)


def _close_manual_compaction(manager, thread_id: str) -> None:
    """Release the manual-compaction guard + task registration. Idempotent."""
    manager.clear_compaction_task(thread_id)
    manager.end_compaction(thread_id)


@cancellation_as_http("compact")
async def trigger_compaction(
    thread_id: str,
    keep_messages: int = 5,
    *,
    user_id: str | None = None,
) -> dict:
    """Manually trigger context compaction for a thread.

    When ``user_id`` is set, applies that user's compaction_model + profile
    so manual /compact matches the auto path.
    """
    from src.server.services.background_task_manager import BackgroundTaskManager

    manager = BackgroundTaskManager.get_instance()
    started_compaction = False
    try:
        from ptc_agent.agent.middleware.compaction import compact_messages
        from src.server.app import setup

        # Gate FIRST — before any graph state reads or writes. Otherwise we can
        # clobber the running workflow's sse_events or checkpoint state.
        await _require_no_active_workflow(thread_id, "compact")

        # Open the admission guard so a concurrent message POST waits this
        # manual compaction out instead of being admitted "fresh" and racing
        # the checkpoint read-modify-write below (manual compaction registers no
        # BackgroundTaskManager task). Also registers this request's task so a
        # user Stop can interrupt the in-flight summarize. Rejects with 409 if
        # another compaction already holds the thread.
        _open_manual_compaction(manager, thread_id, "compact")
        started_compaction = True

        agent_cfg = setup.agent_config
        if user_id and agent_cfg is not None:
            try:
                from src.server.database.api_keys import is_byok_active
                from src.server.handlers.chat.llm_config import resolve_llm_config

                is_byok = await is_byok_active(user_id)
                agent_cfg = await resolve_llm_config(
                    setup.agent_config,
                    user_id,
                    request_model=None,
                    is_byok=is_byok,
                    mode="ptc",
                    thread_id=thread_id,
                )
            except HTTPException:
                # 402 insufficient credits, 403 revoked key, etc. are intentional
                # user-facing signals — don't silently downgrade to platform config.
                raise
            except Exception as e:
                logger.warning(
                    f"[compact] resolve_llm_config failed for user {user_id}: {e}; "
                    "falling back to base agent_config"
                )
                agent_cfg = setup.agent_config

        graph, lg_config, state, messages, backend = await _resolve_graph_and_state(
            thread_id, "compact", config=agent_cfg
        )

        original_count = len(messages)

        compaction_cfg = agent_cfg.compaction if agent_cfg else None
        model_name = (agent_cfg.llm.compaction or "") if agent_cfg and agent_cfg.llm else ""

        # Mirror PTCAgent.create_agent client priority: subsidiary → main → factory.
        # Copy before handing the client to compact_messages — it calls
        # maybe_disable_streaming (src/llms/api_call.py) which sets
        # streaming=False in-place. Without the copy, the fallback path
        # (agent_cfg == setup.agent_config) would permanently mutate the
        # shared main-agent client and break SSE streaming for every
        # subsequent chat workflow.
        compaction_client = None
        if agent_cfg is not None:
            subsidiary = agent_cfg.subsidiary_llm_clients.get("compaction")
            if subsidiary is not None:
                compaction_client = subsidiary.model_copy()
            elif agent_cfg.llm_client is not None:
                compaction_client = agent_cfg.llm_client.model_copy()

        # Read previous event from state (for chained compactions).
        # The state key "_summarization_event" is preserved as a wire/storage
        # contract (values live in the LangGraph checkpointer DB).
        previous_event = state.values.get("_summarization_event")

        try:
            result = await compact_messages(
                messages=messages,
                keep_messages=keep_messages,
                model_name=model_name,
                backend=backend,
                previous_event=previous_event,
                compaction_config=compaction_cfg,
                llm_client=compaction_client,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Merge any Tier 1 offloaded IDs from compact_messages into existing state
        existing_arg_ids = set(state.values.get("_offloaded_tool_call_ids") or ())
        existing_read_ids = set(state.values.get("_offloaded_read_result_ids") or ())

        # Write CompactionEvent + offloaded IDs + reset batch counter.
        # State key "_summarization_event" preserved for DB compatibility.
        await _update_graph_state(
            graph,
            lg_config,
            {
                "_summarization_event": result["event"],
                "_truncation_batch_count": 0,
                "_offloaded_tool_call_ids": (
                    existing_arg_ids | result.get("offloaded_arg_ids", set())
                ),
                "_offloaded_read_result_ids": (
                    existing_read_ids | result.get("offloaded_read_ids", set())
                ),
            },
            thread_id,
            "compact",
        )

        new_message_count = result["preserved_count"]
        summary_text = result.get("summary_text", "")
        summary_length = len(summary_text)

        logger.info(
            f"Manual compaction completed for thread {thread_id}: "
            f"{original_count} -> {new_message_count} messages"
        )

        # Persist context_window event to last response for replay.
        # Action value "summarize" preserved as SSE wire protocol.
        # summary_text is stored so the history-replay view can show the
        # collapsible "View summary" panel just like the live-stream path.
        await _persist_context_window_event(
            thread_id,
            {
                "action": "summarize",
                "signal": "complete",
                "original_message_count": original_count,
                "new_message_count": new_message_count,
                "summary_length": summary_length,
                "summary_text": summary_text,
            },
        )

        return {
            "success": True,
            "thread_id": thread_id,
            "original_message_count": original_count,
            "new_message_count": new_message_count,
            "summary_length": summary_length,
            "summary_text": summary_text,
        }

    except HTTPException:
        raise
    except Exception as e:
        # CancelledError (user Stop / client disconnect) is handled by the
        # @cancellation_as_http wrapper, which sees it after this finally runs.
        logger.exception(f"Error triggering compaction for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger compaction: {str(e)}"
        )
    finally:
        if started_compaction:
            _close_manual_compaction(manager, thread_id)


@cancellation_as_http("offload")
async def trigger_offload(thread_id: str) -> dict:
    """
    Manually trigger tool-arg offloading for a thread (Tier 1 only).

    Truncates large tool arguments in older messages and offloads the
    originals to the sandbox filesystem. No LLM summarization is performed.

    Args:
        thread_id: The thread/conversation ID to offload

    Returns:
        Dict with success, thread_id, message_count, offloaded_args, offloaded_reads
    """
    from src.server.services.background_task_manager import BackgroundTaskManager

    manager = BackgroundTaskManager.get_instance()
    started_compaction = False
    try:
        from ptc_agent.agent.middleware.compaction import offload_tool_args

        # Same gate as /compact — /offload also writes checkpoint state and
        # could race a running workflow's _offloaded_tool_call_ids updates.
        # Fail CLOSED: offload is a non-critical optimization, so if the gate
        # can't confirm the thread is idle we skip it rather than write into a
        # possibly-active turn.
        await _require_no_active_workflow(thread_id, "offload", fail_closed=True)

        # Open the admission guard (same rationale as /compact): hold a
        # concurrent message POST until this manual offload's checkpoint
        # read-modify-write finishes, register the task so a Stop can interrupt
        # it, and reject with 409 if another compaction is already active.
        _open_manual_compaction(manager, thread_id, "offload")
        started_compaction = True

        graph, lg_config, state, messages, backend = await _resolve_graph_and_state(
            thread_id, "offload"
        )

        # Load already-offloaded IDs from graph state (persisted in checkpoint)
        already_offloaded: set[str] = set(
            state.values.get("_offloaded_tool_call_ids") or ()
        )
        already_offloaded_reads: set[str] = set(
            state.values.get("_offloaded_read_result_ids") or ()
        )
        if already_offloaded:
            logger.info(
                f"Loaded {len(already_offloaded)} already-offloaded IDs "
                f"for thread {thread_id}"
            )

        # Call offload_tool_args (Tier 1 only)
        compaction_cfg = setup.agent_config.compaction if setup.agent_config else None
        try:
            result = await offload_tool_args(
                messages=messages,
                backend=backend,
                already_offloaded=already_offloaded,
                compaction_config=compaction_cfg,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        offloaded_args = result["offloaded_args"]
        offloaded_reads = result["offloaded_reads"]
        new_ids = result.get("new_offloaded_ids", set())

        # Update graph state: truncated messages + offloaded IDs + batch counter
        state_update: dict = {"messages": result["messages"]}
        if new_ids:
            # new_offloaded_ids contains both arg and read IDs — merge into both
            # state fields (extra IDs in either set are harmless, they're just guards)
            state_update["_offloaded_tool_call_ids"] = already_offloaded | new_ids
            state_update["_offloaded_read_result_ids"] = (
                already_offloaded_reads | new_ids
            )
            state_update["_truncation_batch_count"] = len(messages)

        await _update_graph_state(
            graph,
            lg_config,
            state_update,
            thread_id,
            "offload",
        )

        logger.info(
            f"Manual offload completed for thread {thread_id}: "
            f"{offloaded_args} tool args, {offloaded_reads} read results"
            f"{f', {len(already_offloaded)} previously offloaded (skipped)' if already_offloaded else ''}"
        )

        # Persist context_window event to last response for replay
        await _persist_context_window_event(
            thread_id,
            {
                "action": "offload",
                "signal": "complete",
                "offloaded_args": offloaded_args,
                "offloaded_reads": offloaded_reads,
            },
        )

        return {
            "success": True,
            "thread_id": thread_id,
            "message_count": result["original_count"],
            "offloaded_args": offloaded_args,
            "offloaded_reads": offloaded_reads,
        }

    except HTTPException:
        raise
    except Exception as e:
        # CancelledError (user Stop / client disconnect) is handled by the
        # @cancellation_as_http wrapper, which sees it after this finally runs.
        logger.exception(f"Error triggering offload for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger offload: {str(e)}"
        )
    finally:
        if started_compaction:
            _close_manual_compaction(manager, thread_id)


async def _persist_context_window_event(thread_id: str, data: dict) -> None:
    """Append a context_window SSE event to the latest response's sse_events for replay.

    Best-effort: logs warnings on failure but never raises. Uses a server-side
    JSONB append so we never read or rewrite the whole sse_events blob per model
    call (the old read-modify-write also clobbered concurrent appends).
    """
    try:
        from src.server.database.conversation import append_sse_event

        cw_event = {
            "event": "context_window",
            "data": {
                "thread_id": thread_id,
                "agent": "agent",
                **data,
            },
        }
        updated = await append_sse_event(thread_id, cw_event)
        if not updated:
            logger.debug(
                f"No responses found for thread {thread_id}, skipping context_window persist"
            )
            return

        logger.debug(
            f"Persisted context_window event ({data.get('action')}) "
            f"for thread {thread_id}"
        )
    except Exception as e:
        logger.warning(f"Failed to persist context_window event for {thread_id}: {e}")
