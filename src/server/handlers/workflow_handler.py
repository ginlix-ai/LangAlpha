"""
Workflow Handler — Business logic for workflow control operations.

Extracted from src/server/app/workflow.py to separate business logic from route definitions.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
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
    Explicitly cancel a workflow execution (user stop) — v4 honest cancel.

    Records durable cancel *intent* on the run's in_progress row
    (``cancel_requested_at``), then signals the local task via
    ``manager.cancel_workflow`` (which interrupts the current step
    immediately). The terminal ``cancelled`` state is written only by the
    finalize CAS when the teardown actually completes — never eagerly from
    here. A cancel that arrives after the run finalized is an honest
    idempotent "already finished", not a recorded losing cancel.

    The subagent kill + registry wipe is owned by the single-owner teardown
    in ``BackgroundTaskManager`` when the ``CancelledError`` lands — this
    handler only runs ``cancel_and_clear`` as a safety net when no active
    task exists (e.g. an orphaned registry left by a crash).

    ``run_id`` targets a specific run so a slow/retried stop can't cancel a
    *newer* turn the user started after the stopped one finished. Omitted =
    the thread's active run.
    """
    try:
        from src.server.services.background_task_manager import (
            BackgroundTaskManager,
        )

        manager = BackgroundTaskManager.get_instance()
        has_active = await manager.has_active_task_for_thread(thread_id)

        # Manual mutation stop. A manual /compact|/offload registers no
        # workflow task (it runs inside its own HTTP request handler), so when
        # there is no active workflow, stopping the in-flight mutation is the
        # entire job — and it must not stamp cancel intent on a run row. The
        # runner cancels a locally-owned op directly, or flags the stop key a
        # foreign worker's heartbeat polls. (An AUTO compaction runs inside
        # the turn's task — there has_active is True, so we fall through and
        # cancel_workflow's inner_task cancel interrupts the summarize.)
        if not has_active:
            from src.server.services.thread_mutation import ThreadMutationRunner

            stopped = await ThreadMutationRunner.get_instance().request_stop(
                thread_id
            )
            if stopped != "none":
                logger.info(
                    f"Manual mutation stop ({stopped}) by user: {thread_id}"
                )
                return {
                    "cancelled": True,
                    "thread_id": thread_id,
                    "message": (
                        "Compaction stopped."
                        if stopped == "cancelled"
                        else "Compaction stop signalled to its worker."
                    ),
                }

        # Durable cancel intent on the run row. Only an in_progress row
        # accepts it (the row lock linearizes cancel vs finalize), so this is
        # self-gating: no active run, nothing stamped — the old eager
        # tracker/thread-status "cancelled" writes are gone with it.
        from src.server.database import turn_lifecycle as tl_db

        # `or None`: an empty-string run_id (e.g. `?run_id=`) must resolve
        # like an omitted one, not skip both the active-run lookup and the
        # honest no_active_run response below.
        target_run_id = run_id or None
        if target_run_id is None:
            active = await tl_db.get_active_run(thread_id)
            if active:
                target_run_id = str(active["conversation_response_id"])

        intent_state = None
        if target_run_id:
            intent = await tl_db.request_run_cancel(target_run_id, thread_id=thread_id)
            intent_state = intent["state"]
            logger.info(
                f"[cancel] durable intent for run={target_run_id} "
                f"thread={thread_id}: {intent_state}"
            )

        # Local execution signal. Signal the SAME run the intent was stamped
        # on: if the resolved run finalizes and a newer one starts between
        # the stamp and this call, an untargeted (None) signal would cancel
        # the newer run. None only when no ledger row exists — the pre-START
        # window, where the manager's thread scan is the only handle.
        cancel_success = await manager.cancel_workflow(thread_id, target_run_id)

        # F5 nudge: intent stamped but no local executor — the owner is
        # (likely) another worker; nudge it to interrupt now. Best-effort:
        # a lost nudge still converges via the finalize CAS adopting
        # 'cancelled' from the durable intent.
        if not cancel_success and intent_state in ("requested", "already_requested"):
            from src.server.services.turn_cancel_pubsub import publish_cancel_nudge

            await publish_cancel_nudge(thread_id, target_run_id)

        if not cancel_success and not await manager.has_active_task_for_thread(
            thread_id
        ):
            logger.warning(
                f"Could not cancel background task for {thread_id} "
                "(may be already completed or not found)"
            )
            # Safety net, RUN-scoped: cancel any local subagents the target
            # run left behind (e.g. its main task settled but tail writers
            # survive — an explicit cancel of that run stops its tail too).
            # Never a thread-wide wipe: the target run may live on ANOTHER
            # worker while this registry holds a terminal local run's live
            # tail, whose guard drain must keep seeing its writers.
            if target_run_id:
                from src.server.services.background_registry_store import (
                    BackgroundRegistryStore,
                )

                registry_store = BackgroundRegistryStore.get_instance()
                await registry_store.cancel_run_tasks(
                    thread_id, target_run_id, force=True
                )

        if intent_state == "already_terminal":
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "already_finished",
                "message": "Run already finished; nothing to cancel.",
            }
        if intent_state == "not_found" and not cancel_success:
            # A caller-supplied run_id that matches neither a ledger row nor
            # a local task (wrong id, or another thread's run). Distinct from
            # the pre-START dispatched window, where the placeholder cancel
            # succeeds (cancel_success=True) despite the row not existing yet.
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "run_not_found",
                "message": "No such run on this thread; nothing to cancel.",
            }
        if target_run_id is None and not cancel_success:
            # No durable run, no local task, no compaction — an honest no-op
            # instead of pretending a signal was sent.
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "no_active_run",
                "message": "No active run to cancel.",
            }

        logger.info(f"Workflow cancel requested: {thread_id}")
        return {
            "cancelled": bool(intent_state in ("requested", "already_requested"))
            or cancel_success,
            "thread_id": thread_id,
            "message": "Cancellation signal sent. Workflow will stop shortly.",
        }

    except Exception as e:
        logger.exception(f"Error cancelling workflow {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to cancel workflow: {str(e)}"
        )


async def get_workflow_status(
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
        from src.server.database import turn_lifecycle as tl_db
        from src.server.services.status_vocabulary import to_public

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

        # Subagent ids for the frontend's per-task reattach — cluster-wide
        # (Redis active set verified against the namespace advisory locks),
        # and NOT gated on an in_progress run: tail-mode subagents outlive
        # the run's terminal row.
        active_tasks = []
        try:
            from src.server.services.background_task_manager import (
                BackgroundTaskManager,
            )

            live = await BackgroundTaskManager.get_instance().get_live_task_info(
                thread_id
            )
            active_tasks = live.get("active_tasks", [])
        except Exception as e:
            logger.debug(
                f"Could not get background task status for {thread_id}: {e}"
            )

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

        # Pending report-backs, from the thread kind's own read model: a PTC
        # thread's pendingness is its open task_report_back outbox row; a
        # flash thread's is the Redis watch set.
        if msg_type == "ptc":
            from src.server.handlers.chat.task_report_back import (
                read_task_report_back_status,
            )

            rb = await read_task_report_back_status(thread_id)
        else:
            from src.server.handlers.chat.report_back import (
                read_report_back_status,
            )

            rb = await read_report_back_status(thread_id)
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


async def _resolve_graph_and_state(
    thread_id: str, verb: str, config=None, checkpointer=None
) -> tuple:
    """Validate thread, build graph, get state, build backend.

    ``config`` is the resolved AgentConfig; defaults to ``setup.agent_config``.
    ``checkpointer`` overrides the global pooled saver — a mutation passes its
    fence-bound saver so checkpoint writes die with the lock session (I2).

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
    checkpointer = checkpointer if checkpointer is not None else get_checkpointer()
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


@asynccontextmanager
async def _hold_thread_mutation(thread_id: str, verb: str):
    """Hold the exclusive-T mutation fence for a manual /compact|/offload|
    /delete, mapping the runner's refusals onto the HTTP contract (409 with a
    stable code the frontend branches on; 503 on budget exhaustion)."""
    from src.server.services.thread_mutation import (
        MutationConflict,
        MutationUnavailable,
        ThreadMutationRunner,
    )

    runner = ThreadMutationRunner.get_instance()
    try:
        async with runner.exclusive(thread_id, verb) as session:
            yield session
    except MutationConflict as e:
        raise HTTPException(status_code=409, detail=e.detail)
    except MutationUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))


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
    try:
        from ptc_agent.agent.middleware.compaction import compact_messages
        from src.server.app import setup

        # The mutation fence FIRST — before any graph state reads or writes:
        # exclusive T(thread) refuses while a fenced run or tail writer is
        # live (any worker), the ledger gate refuses on an in_progress row,
        # and the op key holds concurrent message POSTs at admission. The
        # runner also owns the user-Stop path (local cancel / cross-worker
        # stop flag).
        async with _hold_thread_mutation(thread_id, "compact") as mutation:
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
                thread_id, "compact", config=agent_cfg,
                checkpointer=mutation.saver,
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
        # @cancellation_as_http wrapper, which sees it after the mutation
        # fence releases.
        logger.exception(f"Error triggering compaction for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger compaction: {str(e)}"
        )


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
    try:
        from ptc_agent.agent.middleware.compaction import offload_tool_args

        # Same fence as /compact — /offload also writes checkpoint state and
        # could race a running workflow's _offloaded_tool_call_ids updates.
        # The exclusive-T lock + ledger gate are deterministic, so the old
        # fail-open/fail-closed tracker asymmetry is gone.
        async with _hold_thread_mutation(thread_id, "offload") as mutation:
            graph, lg_config, state, messages, backend = await _resolve_graph_and_state(
                thread_id, "offload", checkpointer=mutation.saver
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
        # @cancellation_as_http wrapper, which sees it after the mutation
        # fence releases.
        logger.exception(f"Error triggering offload for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger offload: {str(e)}"
        )


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
