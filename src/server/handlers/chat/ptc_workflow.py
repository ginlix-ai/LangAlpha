"""PTC (Programmatic Tool Calling) workflow — async SSE generator.

This module contains the ``astream_ptc_workflow`` async generator, refactored
from the monolithic ``chat_handler.py``.  Common setup, persistence, error
handling, and streaming logic is delegated to shared helpers in ``_common.py``;
PTC-specific concerns (workspace session, sandbox, plan mode, background
subagent orchestration, completion callback) remain inline.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime

from fastapi import HTTPException
from langgraph.types import Command

from src.server.app import setup
from src.server.database.workspace import (
    update_workspace_activity,
    get_workspace as db_get_workspace,
)
from src.server.handlers.streaming_handler import WorkflowStreamHandler
from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.persistence.conversation import (
    ConversationPersistenceService,
)
from src.server.services.workflow_tracker import WorkflowTracker
from src.server.services.workspace_manager import WorkspaceManager
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

from ptc_agent.agent.graph import build_ptc_graph_with_session

from ._common import (
    _append_to_last_user_message,
    _is_plan_interrupt_pending,
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
from src.config.settings import get_ptc_recursion_limit

from .llm_config import resolve_llm_config
from .steering import backfill_steering_queries


async def astream_ptc_workflow(
    request: ChatRequest,
    thread_id: str,
    user_input: str,
    user_id: str,
    workspace_id: str,
    is_byok: bool = False,
    config=None,
):
    """Async generator that streams PTC agent workflow events.

    Uses build_ptc_graph to create a per-workspace LangGraph graph,
    then reuses the standard WorkflowStreamHandler for SSE streaming.

    Args:
        request: The chat request
        thread_id: Thread identifier
        user_input: Extracted user input text
        user_id: User identifier
        workspace_id: Workspace identifier

    Yields:
        SSE-formatted event strings
    """
    start_time = time.time()
    handler = None
    persistence_service = None
    token_callback = None
    tool_tracker = None
    ptc_graph = None
    timezone_str = None

    # Start execution tracking to capture agent messages
    ExecutionTracker.start_tracking()
    logger.debug("PTC execution tracking started")

    try:
        # Validate agent_config is available
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # =====================================================================
        # Phase 1: Database Persistence Setup
        # =====================================================================

        # Ensure thread exists in database (linked to workspace)
        await ensure_thread(
            request, thread_id, workspace_id, user_id, msg_type="ptc",
            initial_query=user_input,
        )

        query_type, is_fork, persistence_service = await _setup_fork_and_persistence(
            request=request,
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            log_prefix="PTC_FORK",
        )
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start
        feedback_action = None
        query_content = user_input
        effective_model = config.llm.name if config else None
        query_metadata = {
            "workspace_id": request.workspace_id,
            "msg_type": "ptc",
        }
        if effective_model:
            query_metadata["llm_model"] = effective_model

        # Extract attachment and context metadata for display in history
        # (PTC skips this block for HITL resumes — contrast with Flash)
        if request.additional_context and not request.hitl_response:
            multimodal_ctxs = parse_multimodal_contexts(request.additional_context)
            if multimodal_ctxs:
                query_metadata["attachments"] = await build_attachment_metadata(
                    multimodal_ctxs, thread_id
                )

        # Persist lightweight additional_context + slash command fallback
        # (serialize_context_metadata's slash-command branch already guards
        # on `not request.hitl_response`, so this is safe to call always.)
        if not request.hitl_response:
            serialize_context_metadata(request, query_metadata, user_input, mode="ptc")

        if request.hitl_response:
            feedback_action, query_content, hitl_answers, interrupt_ids = (
                process_hitl_response(request)
            )
            query_metadata["hitl_interrupt_ids"] = interrupt_ids
            if hitl_answers:
                query_metadata["hitl_answers"] = hitl_answers

        await persist_or_skip_replay(
            persistence_service=persistence_service,
            is_checkpoint_replay=is_checkpoint_replay,
            request=request,
            query_content=query_content,
            query_type=query_type,
            feedback_action=feedback_action,
            query_metadata=query_metadata,
            thread_id=thread_id,
            log_prefix="PTC_CHAT",
        )
        if not is_checkpoint_replay:
            logger.info(
                f"[PTC_CHAT] Database records created: workspace_id={workspace_id} "
                f"thread_id={thread_id} query_type={query_type}"
            )

        # =====================================================================
        # Timezone and Locale Validation
        # =====================================================================

        timezone_str = _resolve_timezone(request.timezone, request.locale)

        # =====================================================================
        # Phase 2: Token and Tool Tracking
        # =====================================================================

        token_callback, tool_tracker = init_tracking(thread_id)

        # =====================================================================
        # Session and Graph Setup
        # =====================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config, user_id, request.llm_model, is_byok, mode="ptc",
                reasoning_effort=getattr(request, "reasoning_effort", None),
                fast_mode=getattr(request, "fast_mode", None),
            )

        # Propagate fetch model override to tool context
        apply_fetch_override(config)

        subagents = request.subagents_enabled or config.subagents.enabled
        sandbox_id = None

        # Use WorkspaceManager for workspace-based sessions
        logger.info(f"[PTC_CHAT] Using workspace: {workspace_id}")
        workspace_manager = WorkspaceManager.get_instance()

        # Check if workspace needs startup -- emit early notification so frontend
        # can show "Starting workspace..." instead of a silent wait.
        workspace_record = await db_get_workspace(workspace_id)
        ws_status = workspace_record.get("status") if workspace_record else None
        if ws_status == "stopped":
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id})}\n\n"

        session = await workspace_manager.get_session_for_workspace(
            workspace_id, user_id=user_id
        )

        if ws_status == "stopped":
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'ready', 'workspace_id': workspace_id})}\n\n"

        # Update workspace activity
        await update_workspace_activity(workspace_id)

        registry_store = BackgroundRegistryStore.get_instance()
        background_registry = await registry_store.get_or_create_registry(thread_id)

        # Effective plan_mode: only enable if explicitly requested or resuming
        # from a SubmitPlan interrupt. Other interrupt types (AskUserQuestion,
        # onboarding) must NOT activate plan mode.
        if request.plan_mode:
            effective_plan_mode = True
        elif request.hitl_response:
            effective_plan_mode = await _is_plan_interrupt_pending(thread_id)
        else:
            effective_plan_mode = False

        # Build graph with the workspace's session
        # Note: agent.md is injected dynamically by WorkspaceContextMiddleware
        # on every model call, ensuring it's always the latest content.
        from src.server.app.workspace_sandbox import _set_cached_signed_url

        ptc_graph = await build_ptc_graph_with_session(
            session=session,
            config=config,
            subagent_names=subagents,
            operation_callback=None,
            checkpointer=setup.checkpointer,
            background_registry=background_registry,
            user_id=user_id,
            plan_mode=effective_plan_mode,
            thread_id=thread_id,
            store=setup.store,
            on_signed_url=_set_cached_signed_url,
        )

        if session.sandbox:
            sandbox_id = getattr(session.sandbox, "sandbox_id", None)

        # PTC-only: set global for snapshot access
        setup.graph = ptc_graph

        # Build input state from messages
        messages = normalize_request_messages(request)

        # =====================================================================
        # Skill Context Injection (inline with last user message)
        # =====================================================================
        # When skills are requested via additional_context, load SKILL.md content
        # and append inline to the last user message using <loaded-skill> tags.
        # The original user_input is preserved for database persistence.
        #
        # Server-side slash command detection: also scan the last user message
        # for /<command> prefixes as a fallback when additional_context is missing.
        #
        # PTC guards skill injection with `not request.hitl_response` because the
        # helper does not guard the build_skill_content call itself.
        if not request.hitl_response:
            loaded_skill_names = inject_skills(messages, request, config, mode="ptc")
        else:
            loaded_skill_names = []

        # Multimodal Context Injection (images and PDFs)
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts and not request.hitl_response:
            messages = inject_multimodal_context(messages, multimodal_contexts)
            logger.info(
                f"[PTC_CHAT] Multimodal context injected: {len(multimodal_contexts)} attachment(s)"
            )

        # Build input state or resume command
        if request.hitl_response:
            # Structured HITL resume payload.
            # Pydantic validates this into HITLResponse models, but LangChain's
            # HumanInTheLoopMiddleware expects plain dicts (subscriptable).
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[PTC_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            # Checkpoint replay/regenerate: no new messages, resume from checkpoint_id.
            # LangGraph will re-execute from the specified checkpoint state.
            input_state = None
            logger.info(
                f"[PTC_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {
                "messages": messages,
                "current_agent": "ptc",  # For FileOperationMiddleware SSE events
            }
            # Auto-load skill tools when skills were injected via additional_context
            if loaded_skill_names:
                input_state["loaded_skills"] = loaded_skill_names

        # =====================================================================
        # Plan Mode Injection
        # =====================================================================
        # When plan_mode is enabled, inject a reminder for the agent to create
        # a plan and submit it for approval before executing any changes.
        if effective_plan_mode and not request.hitl_response:
            plan_mode_reminder = (
                "\n\n<system-reminder>\n"
                "[PLAN MODE ENABLED]\n"
                "Before making any changes, you MUST:\n"
                "1. Explore the codebase to understand the current state\n"
                "2. Create a detailed plan describing what you intend to do\n"
                "3. Call the `SubmitPlan` tool with your plan description\n"
                "4. Wait for user approval before proceeding with execution\n"
                "Do NOT execute any write operations until the plan is approved.\n"
                "</system-reminder>"
            )
            # Append reminder to the last user message
            if isinstance(input_state, dict) and input_state.get("messages"):
                _append_to_last_user_message(
                    input_state["messages"], plan_mode_reminder
                )
            logger.info(f"[PTC_CHAT] Plan mode enabled for thread_id={thread_id}")

        # =====================================================================
        # Directive Context Injection (inline with user message)
        # =====================================================================
        directives = parse_directive_contexts(request.additional_context)
        directive_reminder = build_directive_reminder(directives)
        if directive_reminder and not request.hitl_response:
            if isinstance(input_state, dict) and input_state.get("messages"):
                _append_to_last_user_message(
                    input_state["messages"], directive_reminder
                )
                logger.info(
                    f"[PTC_CHAT] Directive context injected inline ({len(directives)} directives)"
                )

        # =====================================================================
        # Save user request to system thread directory (non-critical)
        # =====================================================================
        if not request.hitl_response and session.sandbox:
            short_id = thread_id[:8]
            try:
                request_path = session.sandbox.normalize_path(
                    f".agents/threads/{short_id}/request.md"
                )
                await session.sandbox.awrite_file_text(request_path, user_input)
            except Exception:
                pass  # Non-critical, don't fail the request

        # =====================================================================
        # LangSmith Tracing Configuration
        # =====================================================================

        graph_config = build_graph_config(
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            mode="ptc",
            timezone_str=timezone_str,
            token_callback=token_callback,
            request=request,
            effective_model=effective_model,
            is_byok=is_byok,
            recursion_limit=get_ptc_recursion_limit(),
            plan_mode=effective_plan_mode,
        )

        # Extract background task registry from orchestrator (single source of truth for SSE events)
        # The orchestrator wraps the middleware which owns the registry
        background_registry = None
        if hasattr(ptc_graph, "middleware") and hasattr(
            ptc_graph.middleware, "registry"
        ):
            background_registry = ptc_graph.middleware.registry
            logger.debug(
                f"[PTC_CHAT] Background registry attached for thread_id={thread_id}"
            )

        # Reuse WorkflowStreamHandler for SSE streaming
        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
            background_registry=background_registry,
        )

        # Track steering messages injected mid-workflow for post-completion backfill
        setup_steering_tracking(handler)

        # Initialize workflow tracker
        tracker = WorkflowTracker.get_instance()
        await tracker.mark_active(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            metadata={
                "type": "ptc_agent",
                "sandbox_id": sandbox_id,
                "locale": request.locale,
                "timezone": timezone_str,
            },
        )

        # =====================================================================
        # Phase 3: Background Execution with Completion Callback
        # =====================================================================

        manager = BackgroundTaskManager.get_instance()

        # Wait for any soft-interrupted workflow to complete before starting new one
        ready, steering_event = await wait_or_steer(
            manager, thread_id, user_input, user_id
        )
        if not ready:
            if steering_event:
                yield steering_event
            return

        # Define completion callback for background persistence
        async def on_background_workflow_complete():
            """Persists workflow data after background execution completes.

            Reads fresh handler/token_callback from task_info metadata because
            reinvocation may have replaced them with new instances.
            """
            try:
                # Read fresh refs from task_info (may have been updated by reinvoke)
                task_info = manager.tasks.get(thread_id)
                _handler = task_info.metadata.get("handler") if task_info else handler
                _token_cb = (
                    task_info.metadata.get("token_callback")
                    if task_info
                    else token_callback
                )
                _start_time = (
                    task_info.metadata.get("start_time", start_time)
                    if task_info
                    else start_time
                )

                execution_time = time.time() - _start_time

                _persistence_service = ConversationPersistenceService.get_instance(
                    thread_id
                )
                _persistence_service._on_pair_persisted = (
                    lambda: manager.clear_event_buffer(thread_id)
                )

                # Get per-call records for usage tracking
                _per_call_records = _token_cb.per_call_records if _token_cb else None

                # Get tool usage summary from handler
                _tool_usage = None
                if _handler:
                    _tool_usage = _handler.get_tool_usage()

                # Persist completion to database
                _sse_events = _handler.get_sse_events() if _handler else None

                # Capture sandbox images -> upload to cloud storage -> rewrite storage URLs
                if _sse_events and session and session.sandbox:
                    try:
                        from src.server.services.persistence.image_capture import (
                            capture_and_rewrite_images,
                        )

                        await capture_and_rewrite_images(
                            _sse_events, session.sandbox, thread_id=thread_id,
                        )
                    except Exception:
                        logger.warning(
                            "[IMAGE_CAPTURE] Hook A failed", exc_info=True,
                        )

                await _persistence_service.persist_completion(
                    metadata={
                        "workspace_id": request.workspace_id,
                        "sandbox_id": sandbox_id,
                        "locale": request.locale,
                        "timezone": timezone_str,
                        "msg_type": "ptc",
                        "is_byok": is_byok,
                    },
                    execution_time=execution_time,
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                    sse_events=_sse_events,
                )

                # Mark completed in Redis tracker
                await tracker.mark_completed(
                    thread_id=thread_id,
                    metadata={
                        "completed_at": datetime.now().isoformat(),
                        "execution_time": execution_time,
                    },
                )

                # Backfill query records for steering messages that produced orphan responses
                if _handler and _handler.injected_steerings:
                    await backfill_steering_queries(
                        thread_id, _handler.injected_steerings
                    )

                logger.info(
                    f"[PTC_COMPLETE] Background completion persisted: thread_id={thread_id} "
                    f"duration={execution_time:.2f}s"
                )

                # Post-completion sandbox housekeeping (parallel)
                ws_manager = WorkspaceManager.get_instance()
                housekeeping = [ws_manager._backup_files_to_db(request.workspace_id)]
                if session and session.sandbox:
                    housekeeping.append(session.sandbox.sync_skills_lock())
                results = await asyncio.gather(*housekeeping, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        task_name = "file backup" if i == 0 else "lock sync"
                        logger.warning(
                            f"[PTC_COMPLETE] {task_name} failed for {thread_id}: {result}"
                        )

            except Exception as e:
                logger.error(
                    f"[PTC_CHAT] Background completion persistence failed for {thread_id}: {e}",
                    exc_info=True,
                )
            finally:
                # Release burst slot so it doesn't block future requests
                await release_burst_slot(user_id)

        # Start workflow in background with event buffering
        await manager.start_workflow(
            thread_id=thread_id,
            workflow_generator=handler.stream_workflow(
                graph=ptc_graph,
                input_state=input_state,
                config=graph_config,
            ),
            metadata={
                "workspace_id": workspace_id,
                "user_id": user_id,
                "sandbox_id": sandbox_id,
                "sandbox": session.sandbox if session else None,
                "started_at": datetime.now().isoformat(),
                "start_time": start_time,
                "msg_type": "ptc",
                "is_byok": is_byok,
                "locale": request.locale,
                "timezone": timezone_str,
                "handler": handler,
                "token_callback": token_callback,
            },
            completion_callback=on_background_workflow_complete,
            graph=ptc_graph,  # Pass graph for state queries in completion/error handlers
        )

        # Stream live SSE events to the client
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
            log_prefix="PTC_CHAT",
        ):
            yield event

    except Exception as e:
        # =====================================================================
        # Phase 4: Error Recovery with Retry Logic
        # =====================================================================
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
            msg_type="ptc",
            log_prefix="PTC_CHAT",
            timezone_str=timezone_str,
        ):
            yield event

        raise

    finally:
        # Always stop execution tracking to prevent memory leaks and context pollution
        ExecutionTracker.stop_tracking()
        logger.debug("PTC execution tracking stopped")
