"""Flash agent workflow — async generator streaming SSE events.

This module contains the ``astream_flash_workflow`` function, refactored from
the monolithic ``chat_handler.py``.  Common setup, persistence, error handling,
and streaming logic is delegated to shared helpers in ``_common``.

Flash mode is optimised for speed: no sandbox, no MCP, no workspace, and only
external tools (web search, market data, SEC filings).
"""

from __future__ import annotations

import json
import time
from datetime import datetime

from fastapi import HTTPException
from langgraph.types import Command

from src.server.app import setup
from src.server.database.workspace import get_or_create_flash_workspace
from src.server.handlers.streaming_handler import WorkflowStreamHandler
from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
)
from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.workflow_tracker import WorkflowTracker
from src.server.utils.directive_context import (
    build_directive_reminder,
    parse_directive_contexts,
)
from src.server.utils.multimodal_context import (
    build_attachment_metadata,
    inject_multimodal_context,
    parse_multimodal_contexts,
)
from src.utils.tracking import ExecutionTracker
from src.server.dependencies.usage_limits import release_burst_slot
from ptc_agent.agent.flash import build_flash_graph
from ptc_agent.agent.graph import get_user_profile_for_prompt

from ._common import (
    _append_to_last_user_message,
    _resolve_timezone,
    _setup_fork_and_persistence,
    apply_fetch_override,
    build_graph_config,
    ensure_thread,
    handle_workflow_error,
    init_tracking,
    inject_skills,
    logger,
    normalize_request_messages,
    persist_or_skip_replay,
    process_hitl_response,
    serialize_context_metadata,
    setup_steering_tracking,
    stream_live_events,
    wait_or_steer,
)
from .llm_config import resolve_llm_config
from .steering import backfill_steering_queries, steer_thread


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def astream_flash_workflow(
    request: ChatRequest,
    thread_id: str,
    user_input: str,
    user_id: str,
    is_byok: bool = False,
    config=None,
):
    """
    Async generator that streams Flash agent workflow events.

    Flash mode is optimized for speed - no sandbox, no MCP, no workspace required.
    Uses only external tools (web search, market data, SEC filings).

    Args:
        request: The chat request
        thread_id: Thread identifier
        user_input: Extracted user input text
        user_id: User identifier
        is_byok: Whether the user is using their own API key
        config: Pre-resolved LLM config (optional; resolved here if absent)

    Yields:
        SSE-formatted event strings
    """
    start_time = time.time()
    handler = None
    token_callback = None
    tool_tracker = None
    flash_graph = None
    persistence_service = None
    workspace_id = None
    timezone_str = None

    ExecutionTracker.start_tracking()
    logger.info(f"[FLASH_CHAT] Starting flash workflow: thread_id={thread_id}")

    try:
        # Validate agent_config is available
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="Flash Agent not initialized. Check server startup logs.",
            )

        # =================================================================
        # Database Persistence Setup
        # =================================================================

        # Get or create the shared flash workspace for this user
        flash_ws = await get_or_create_flash_workspace(user_id)
        workspace_id = str(flash_ws["workspace_id"])

        # Ensure thread exists in database
        await ensure_thread(
            request, thread_id, workspace_id, user_id, msg_type="flash",
            initial_query=user_input,
        )

        query_type, is_fork, persistence_service = await _setup_fork_and_persistence(
            request=request,
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            log_prefix="FLASH_FORK",
        )
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start (with attachment and context metadata for display
        # in history).  This block is flash-specific because of multimodal guard
        # differences vs PTC.
        effective_model = config.llm.flash if config else None
        query_metadata = {"msg_type": "flash"}
        if effective_model:
            query_metadata["llm_model"] = effective_model
        if request.additional_context:
            multimodal_ctxs = parse_multimodal_contexts(request.additional_context)
            if multimodal_ctxs:
                query_metadata["attachments"] = await build_attachment_metadata(
                    multimodal_ctxs, thread_id
                )

        # Persist lightweight additional_context + slash command fallback
        serialize_context_metadata(request, query_metadata, user_input, mode="flash")

        # Extract HITL answer metadata for persistence
        feedback_action = None
        query_content = user_input

        if request.hitl_response:
            feedback_action, query_content, hitl_answers, interrupt_ids = (
                process_hitl_response(request)
            )
            query_metadata["hitl_interrupt_ids"] = interrupt_ids
            if hitl_answers:
                query_metadata["hitl_answers"] = hitl_answers

        # Skip query persistence for checkpoint replay (regenerate/retry)
        await persist_or_skip_replay(
            persistence_service,
            is_checkpoint_replay,
            request,
            query_content,
            query_type,
            feedback_action,
            query_metadata,
            thread_id,
            log_prefix="FLASH_CHAT",
        )

        logger.info(
            f"[FLASH_CHAT] Database records created: workspace_id={workspace_id}"
        )

        # =================================================================
        # Token and Tool Tracking
        # =================================================================

        token_callback, tool_tracker = init_tracking(thread_id)

        # =================================================================
        # Build Flash Agent Graph
        # =================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for
        # standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config,
                user_id,
                request.llm_model,
                is_byok,
                mode="flash",
                reasoning_effort=getattr(request, "reasoning_effort", None),
                fast_mode=getattr(request, "fast_mode", None),
            )

        # Resolve timezone for metadata (observability only -- agent clock
        # uses DB user_profile)
        timezone_str = _resolve_timezone(request.timezone, request.locale)

        # Propagate fetch model override to tool context
        apply_fetch_override(config)

        # Fetch user profile for prompt injection
        flash_user_profile = None
        if user_id:
            flash_user_profile = await get_user_profile_for_prompt(user_id)

        # Build flash graph (no sandbox, no session)
        flash_graph = build_flash_graph(
            config=config,
            checkpointer=setup.checkpointer,
            user_profile=flash_user_profile,
            store=setup.store,
        )

        # Build input state from messages
        messages = normalize_request_messages(request)

        # Multimodal Context Injection (images and PDFs) -- Flash-specific
        # ordering: inject multimodal before skills.
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts:
            messages = inject_multimodal_context(messages, multimodal_contexts)
            logger.info(
                f"[FLASH_CHAT] Multimodal context injected: "
                f"{len(multimodal_contexts)} attachment(s)"
            )

        # Skill Context Injection (Flash mode)
        loaded_skill_names = inject_skills(messages, request, config, mode="flash")

        # Directive Context Injection (inline with user message) --
        # Flash-specific
        directives = parse_directive_contexts(request.additional_context)
        directive_reminder = build_directive_reminder(directives)
        if directive_reminder:
            _append_to_last_user_message(messages, directive_reminder)
            logger.info(
                f"[FLASH_CHAT] Directive context injected inline "
                f"({len(directives)} directives)"
            )

        # Build input state or resume command -- Flash-specific (no
        # ``current_agent`` key)
        if request.hitl_response:
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[FLASH_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            input_state = None
            logger.info(
                f"[FLASH_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {"messages": messages}
            if loaded_skill_names:
                input_state["loaded_skills"] = loaded_skill_names

        # Build LangGraph config
        graph_config = build_graph_config(
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            mode="flash",
            timezone_str=timezone_str,
            token_callback=token_callback,
            request=request,
            effective_model=effective_model,
            is_byok=is_byok,
            recursion_limit=100,
        )

        # Create stream handler
        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
        )

        # Track steering messages injected mid-workflow for post-completion backfill
        setup_steering_tracking(handler)

        # =================================================================
        # Background Execution (same pattern as PTC for reconnection
        # support)
        # =================================================================

        tracker = WorkflowTracker.get_instance()
        manager = BackgroundTaskManager.get_instance()

        # Wait for any running/soft-interrupted workflow to complete before
        # starting new one
        ready, steering_event = await wait_or_steer(
            manager, thread_id, user_input, user_id
        )
        if not ready:
            if steering_event:
                yield steering_event
            return

        # Mark workflow as active in Redis tracker
        await tracker.mark_active(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            metadata={
                "started_at": datetime.now().isoformat(),
                "msg_type": "flash",
                "locale": request.locale,
                "timezone": timezone_str,
            },
        )

        # Completion callback for background persistence
        async def on_flash_workflow_complete():
            try:
                execution_time = time.time() - start_time
                _per_call_records = (
                    token_callback.per_call_records if token_callback else None
                )
                _tool_usage = handler.get_tool_usage() if handler else None
                _sse_events = handler.get_sse_events() if handler else None

                if persistence_service:
                    await persistence_service.persist_completion(
                        metadata={
                            "workspace_id": workspace_id,
                            "locale": request.locale,
                            "timezone": timezone_str,
                            "msg_type": "flash",
                            "is_byok": is_byok,
                        },
                        execution_time=execution_time,
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                        sse_events=_sse_events,
                    )

                await tracker.mark_completed(
                    thread_id=thread_id,
                    metadata={
                        "completed_at": datetime.now().isoformat(),
                        "execution_time": execution_time,
                    },
                )

                # Backfill query records for steering messages that produced
                # orphan responses
                if handler and handler.injected_steerings:
                    await backfill_steering_queries(
                        thread_id, handler.injected_steerings
                    )

                logger.info(
                    f"[FLASH_COMPLETE] Background completion persisted: "
                    f"thread_id={thread_id} duration={execution_time:.2f}s"
                )
            except Exception as e:
                logger.error(
                    f"[FLASH_CHAT] Background completion persistence failed: {e}",
                    exc_info=True,
                )
            finally:
                await release_burst_slot(user_id)

        # Start workflow in background
        try:
            await manager.start_workflow(
                thread_id=thread_id,
                workflow_generator=handler.stream_workflow(
                    graph=flash_graph,
                    input_state=input_state,
                    config=graph_config,
                ),
                metadata={
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "started_at": datetime.now().isoformat(),
                    "start_time": start_time,
                    "msg_type": "flash",
                    "is_byok": is_byok,
                    "locale": request.locale,
                    "timezone": timezone_str,
                    "handler": handler,
                    "token_callback": token_callback,
                },
                completion_callback=on_flash_workflow_complete,
                graph=flash_graph,
            )
        except RuntimeError:
            # Race condition: another request registered first -- queue the
            # message
            await release_burst_slot(user_id)
            result = await steer_thread(thread_id, user_input, user_id)
            if result:
                event_data = json.dumps(
                    {
                        "thread_id": thread_id,
                        "content": user_input,
                        "position": result["position"],
                    }
                )
                yield f"event: steering_accepted\ndata: {event_data}\n\n"
                return

            raise HTTPException(
                status_code=409,
                detail=(
                    f"Workflow {thread_id} is still running. "
                    "Wait a moment, or use /reconnect to continue streaming, "
                    "or /cancel to stop it."
                ),
            )

        # Stream live events from background task to client
        async for event in stream_live_events(
            manager=manager,
            tracker=tracker,
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            handler=handler,
            token_callback=token_callback,
            persistence_service=persistence_service,
            start_time=start_time,
            request=request,
            is_byok=is_byok,
            log_prefix="FLASH_CHAT",
        ):
            yield event

    except Exception as e:
        async for event in handle_workflow_error(
            e,
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            handler=handler,
            token_callback=token_callback,
            persistence_service=persistence_service,
            start_time=start_time,
            request=request,
            is_byok=is_byok,
            msg_type="flash",
            log_prefix="FLASH_CHAT",
            timezone_str=timezone_str,
        ):
            yield event

        raise

    finally:
        ExecutionTracker.stop_tracking()
        logger.debug("Flash execution tracking stopped")
