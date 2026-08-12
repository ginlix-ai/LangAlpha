"""Thread maintenance — compact and offload under the exclusive thread guard.

Both mutations resolve the thread's graph and checkpoint state, run the
operation with a cross-worker stop key, and persist the resulting
context_window event. Cancel dispatch and status reads live in
services/cancel_dispatch.py and services/thread_status.py.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import HTTPException

from src.server.handlers.cancellation import cancellation_as_http
from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)


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
                    from src.server.services.llm.config import resolve_llm_config

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
