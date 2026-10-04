"""Thread maintenance — compact and offload under the exclusive thread guard.

Both mutations resolve the thread's graph and checkpoint state, run the
operation with a cross-worker stop key, and persist the resulting
context_window event. Cancel dispatch and status reads live in
services/cancel_dispatch.py and services/thread_status.py.
"""

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager

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
    thread_id: str,
    verb: str,
    config=None,
    checkpointer=None,
    user_id=None,
    *,
    held: AsyncExitStack,
) -> tuple:
    """Validate thread, build graph, get state, build backend.

    ``config`` is the resolved AgentConfig; defaults to ``setup.agent_config``.
    ``checkpointer`` overrides the global pooled saver — a mutation passes its
    fence-bound saver so checkpoint writes die with the lock session (I2).
    ``user_id`` identifies the caller to the session acquire, whose MCP resolve
    is owner-scoped. ``held`` is the caller's stack, which keeps the backend's
    folder in place until the caller's block ends.

    Returns:
        (graph, lg_config, state, messages, backend)
    """
    from src.server.database import conversation as qr_db
    from src.server.database.workspace import get_workspace
    from src.server.database.workspace_folders import (
        WorkspaceFolderMoving,
        is_top_level,
        workspace_folder_in_use,
    )
    from src.server.utils.error_sanitization import sandbox_unreachable_detail
    from src.server.services.workspace_manager import WorkspaceManager
    from ptc_agent.agent.graph import build_ptc_graph_with_session
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.core.paths import SandboxLayout

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
        session = await workspace_manager.get_session_for_workspace(
            workspace_id, user_id=user_id
        )
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
        session=session,
        tool_view=workspace_manager.tool_view(session, workspace_id),
        config=effective_config,
        checkpointer=checkpointer,
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

    # Backend. Pinned to the thread's workspace folder because these routes
    # run outside a turn, where nothing has bound a project: an unpinned
    # backend would file this thread's offloads on the machine root, which a
    # later delete of the workspace would leave behind. No run keeps a settle
    # off this folder, and an offload is the only copy once the checkpoint is
    # truncated: ``held`` keeps the folder in place, and the row is read under it.
    backend = None
    if hasattr(session, "sandbox") and session.sandbox is not None:
        try:
            await held.enter_async_context(workspace_folder_in_use(workspace_id))
        except WorkspaceFolderMoving as e:
            raise HTTPException(status_code=503, detail=sandbox_unreachable_detail(e)) from None
        row = await get_workspace(workspace_id) or {}
        dir_name = row.get("dir_name")
        if dir_name and not is_top_level(dir_name):
            raise HTTPException(
                status_code=503,
                detail=sandbox_unreachable_detail(WorkspaceFolderMoving(workspace_id)),
            )
        layout = SandboxLayout(session.sandbox.working_dir).for_workspace(dir_name)
        backend = SandboxBackend(session.sandbox, layout.workspace)

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

    When ``user_id`` is set, applies that user's compaction_model + profile,
    resolved against the thread's own model as its turns are, so manual
    /compact matches the auto path.
    """
    try:
        from ptc_agent.agent.middleware.compaction import (
            compact_messages,
            resolve_compaction_client,
        )
        from src.server.app import setup

        # The mutation fence FIRST — before any graph state reads or writes:
        # exclusive T(thread) refuses while a fenced run or tail writer is
        # live (any worker), the ledger gate refuses on an in_progress row,
        # and the op key holds concurrent message POSTs at admission. The
        # runner also owns the user-Stop path (local cancel / cross-worker
        # stop flag).
        async with (
            _hold_thread_mutation(thread_id, "compact") as mutation,
            AsyncExitStack() as held,
        ):
            agent_cfg = setup.agent_config
            if user_id and agent_cfg is not None:
                try:
                    from src.server.database.api_keys import is_byok_active
                    from src.server.database.conversation import get_thread_auth_meta
                    from src.server.services.llm import thread_model
                    from src.server.services.llm.config import resolve_llm_config

                    is_byok = await is_byok_active(user_id)
                    meta = await get_thread_auth_meta(thread_id)
                    # The thread's own mode: a flash thread keeps its model in
                    # the flash slot, which the compaction model falls back to,
                    # so PTC mode would summarize on the account's flash default.
                    mode = "flash" if meta and meta.get("msg_type") == "flash" else "ptc"

                    async def resolve(model: str | None):
                        return await resolve_llm_config(
                            setup.agent_config,
                            user_id,
                            request_model=model,
                            is_byok=is_byok,
                            mode=mode,
                            thread_id=thread_id,
                        )

                    agent_cfg = await thread_model.resolve_turn_config(
                        user_id,
                        thread_id,
                        named=None,
                        held=meta.get("llm_model") if meta else None,
                        # A summary changes no answer, so it runs on the
                        # default rather than waiting on a reconnect.
                        fall_back=True,
                        resolve=resolve,
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
                checkpointer=mutation.saver, user_id=user_id, held=held,
            )

            original_count = len(messages)

            compaction_cfg = agent_cfg.compaction if agent_cfg else None
            model_name = (agent_cfg.llm.compaction_name or "") if agent_cfg and agent_cfg.llm else ""

            # Same resolver as automatic compaction, so a manual /compact runs the
            # same model: a named compaction model without a role client resolves
            # by name, not on a copy of the main client. It returns a copy, which
            # matters because compact_messages sets streaming=False in place.
            compaction_client = (
                resolve_compaction_client(agent_cfg) if agent_cfg is not None else None
            )

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
                    thread_id=thread_id,
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
async def trigger_offload(thread_id: str, *, user_id: str | None = None) -> dict:
    """
    Manually trigger tool-arg offloading for a thread (Tier 1 only).

    Records large tool arguments and stale Read results in older messages as
    offloaded and writes the original arguments to the sandbox filesystem. No
    LLM summarization is performed. ``user_id`` identifies the caller to the
    session acquire, same as
    :func:`trigger_compaction`.

    Args:
        thread_id: The thread/conversation ID to offload

    Returns:
        Dict with success, thread_id, message_count, offloaded_args, offloaded_reads
    """
    try:
        from ptc_agent.agent.middleware.compaction import (
            get_effective_messages,
            offload_tool_args,
        )

        # Same fence as /compact — /offload also writes checkpoint state and
        # could race a running workflow's _offloaded_tool_call_ids updates.
        # The exclusive-T lock + ledger gate are deterministic, so the old
        # fail-open/fail-closed tracker asymmetry is gone.
        async with (
            _hold_thread_mutation(thread_id, "offload") as mutation,
            AsyncExitStack() as held,
        ):
            graph, lg_config, state, messages, backend = await _resolve_graph_and_state(
                thread_id, "offload", checkpointer=mutation.saver, user_id=user_id, held=held
            )

            # Recorded offloads are the whole record: the checkpoint keeps full
            # messages and the middleware re-applies these ids on every call.
            already_offloaded: set[str] = set(
                state.values.get("_offloaded_tool_call_ids") or ()
            )
            already_offloaded_reads: set[str] = set(
                state.values.get("_offloaded_read_result_ids") or ()
            )
            effective = get_effective_messages(
                messages, state.values.get("_summarization_event")
            )

            compaction_cfg = setup.agent_config.compaction if setup.agent_config else None
            try:
                result = await offload_tool_args(
                    messages=effective,
                    backend=backend,
                    already_offloaded=already_offloaded,
                    already_offloaded_reads=already_offloaded_reads,
                    compaction_config=compaction_cfg,
                    thread_id=thread_id,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            offloaded_args = result["offloaded_args"]
            offloaded_reads = result["offloaded_reads"]

            # Ids and the batch counter only, never messages. The counter
            # holds the middleware's own auto pass off for one batch.
            await _update_graph_state(
                graph,
                lg_config,
                {
                    "_offloaded_tool_call_ids": (
                        already_offloaded | result["offloaded_arg_ids"]
                    ),
                    "_offloaded_read_result_ids": (
                        already_offloaded_reads | result["offloaded_read_ids"]
                    ),
                    "_truncation_batch_count": len(effective),
                },
                thread_id,
                "offload",
            )

            logger.info(
                f"Manual offload completed for thread {thread_id}: "
                f"{offloaded_args} tool args, {offloaded_reads} read results"
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
                "message_count": len(messages),
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
