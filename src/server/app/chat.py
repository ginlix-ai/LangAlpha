"""
Chat Endpoint

This module provides the /api/v1/chat/stream endpoint that uses the ptc-agent
library for code execution in Daytona sandboxes.

Features:
- SSE streaming for real-time responses (reuses WorkflowStreamHandler)
- Session management (sandbox reuse per conversation)
- Per-conversation LangGraph graphs via ptc_agent.agent.graph
- Reconnection support
- Token tracking (optional)
- Interrupt/resume for plan review
- Database persistence (conversations, threads, queries, responses)
- Background execution with event buffering
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from langgraph.types import Command

from src.server.models.chat import (
    ChatRequest,
    StatusResponse,
    serialize_hitl_response_map,
    summarize_hitl_response_map,
)
from src.server.handlers.streaming_handler import WorkflowStreamHandler
from ptc_agent.agent.graph import build_ptc_graph, build_ptc_graph_with_session
from src.server.services.session_manager import SessionService, get_session_provider
from src.server.services.workspace_manager import WorkspaceManager
from src.server.database.workspace_db import update_workspace_activity
from src.server.services.background_task_manager import BackgroundTaskManager, TaskStatus
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.workflow_tracker import WorkflowTracker

# Database persistence imports
from src.server.database import conversation_db as qr_db
from src.server.services.conversation_persistence_service import ConversationPersistenceService

# Token and tool tracking imports
from src.utils.tracking import (
    TokenTrackingManager,
    ExecutionTracker,
    serialize_agent_message,
)
from src.server.models.workflow import serialize_state_snapshot
from src.tools.decorators import ToolUsageTracker

# File operation tracking
from src.server.services.file_logger import FileOperationLogger

# State restoration imports
from src.server.utils.state_restoration import (
    parse_last_thread_id,
    restore_state_with_fallback,
)

# Locale/timezone configuration
from src.config.settings import (
    get_locale_config,
    get_langsmith_tags,
    get_langsmith_metadata,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)

# Create router with v1 prefix
router = APIRouter(prefix="/api/v1/chat", tags=["Chat"])


@router.post("/stream")
async def chat_stream(request: ChatRequest):
    """
    Stream PTC agent responses as Server-Sent Events.

    This endpoint:
    - Uses a Daytona sandbox session from the specified workspace
    - Streams agent responses in real-time
    - Supports tool execution and file operations
    - Handles interrupts for plan review

    Args:
        request: ChatRequest with messages and configuration (workspace_id required)

    Returns:
        StreamingResponse with SSE events
    """
    # Extract identity fields
    user_id = request.user_id
    workspace_id = request.workspace_id
    thread_id = request.thread_id
    if thread_id == "__default__":
        thread_id = str(uuid4())

    # Validate that agent_config is initialized
    if not hasattr(setup, 'agent_config') or setup.agent_config is None:
        raise HTTPException(
            status_code=503,
            detail="PTC Agent not initialized. Check server startup logs."
        )

    # Extract user input
    user_input = ""
    if request.messages:
        last_msg = request.messages[-1]
        if isinstance(last_msg.content, str):
            user_input = last_msg.content
        elif isinstance(last_msg.content, list):
            for item in last_msg.content:
                if hasattr(item, 'text') and item.text:
                    user_input = item.text
                    break

    logger.info(
        f"[PTC_CHAT] New request: workspace_id={workspace_id} "
        f"thread_id={thread_id} user_id={user_id}"
    )

    return StreamingResponse(
        _astream_workflow(
            request=request,
            thread_id=thread_id,
            user_input=user_input,
            user_id=user_id,
            workspace_id=workspace_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


async def _astream_workflow(
    request: ChatRequest,
    thread_id: str,
    user_input: str,
    user_id: str,
    workspace_id: str,
):
    """
    Async generator that streams PTC agent workflow events.

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

    # Start execution tracking to capture agent messages
    ExecutionTracker.start_tracking()
    logger.debug("PTC execution tracking started")

    try:
        # Validate agent_config is available
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs."
            )

        # =====================================================================
        # Phase 1: Database Persistence Setup
        # =====================================================================

        # Determine query type based on whether this is an interrupt resume
        is_resume = request.hitl_response or request.interrupt_feedback
        query_type = "resume_feedback" if is_resume else "initial"

        # Ensure thread exists in database (linked to workspace)
        await qr_db.ensure_thread_exists(
            workspace_id=workspace_id,
            thread_id=thread_id,
            user_id=user_id,
            initial_query=user_input,
            initial_status="in_progress",
            msg_type="ptc",
        )

        # Initialize persistence service for this thread
        persistence_service = ConversationPersistenceService.get_instance(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id
        )

        # Get current pair_index for this thread (will be used by file logger)
        current_pair_index = await persistence_service.get_or_calculate_pair_index()

        # Persist query start
        feedback_action = None
        query_content = user_input
        query_metadata = {
            "workspace_id": request.workspace_id,
            "msg_type": "ptc",
        }

        if request.hitl_response:
            # HITL resume payloads typically have empty user_input (CLI sends message="").
            summary = summarize_hitl_response_map(request.hitl_response)
            feedback_action = summary["feedback_action"]
            query_content = summary["content"]
            query_metadata["hitl_interrupt_ids"] = summary["interrupt_ids"]
        elif request.interrupt_feedback:
            # Legacy string-based feedback (deprecated).
            feedback_action = request.interrupt_feedback

        await persistence_service.persist_query_start(
            content=query_content,
            query_type=query_type,
            feedback_action=feedback_action,
            metadata=query_metadata,
        )

        logger.info(
            f"[PTC_CHAT] Database records created: workspace_id={workspace_id} "
            f"thread_id={thread_id} query_type={query_type}"
        )

        # =====================================================================
        # Timezone and Locale Validation
        # =====================================================================

        timezone_str = "UTC"  # Default

        if request.timezone:
            # Validate user-provided timezone
            try:
                ZoneInfo(request.timezone)
                timezone_str = request.timezone
                logger.debug(f"[PTC_CHAT] Using user-provided timezone: {timezone_str}")
            except ZoneInfoNotFoundError as e:
                logger.warning(
                    f"[PTC_CHAT] Invalid timezone '{request.timezone}': {e}. "
                    f"Falling back to locale-based timezone."
                )
                timezone_str = None  # Will use locale fallback

        if not timezone_str or timezone_str == "UTC":
            # Fallback to locale-based timezone
            locale_config = get_locale_config(
                request.locale or "en-US",
                "en"  # Default prompt language
            )
            timezone_str = locale_config.get("timezone", "UTC")
            logger.debug(
                f"[PTC_CHAT] Using locale-based timezone: {timezone_str} "
                f"(locale: {request.locale})"
            )

        # =====================================================================
        # Phase 3: Token and Tool Tracking
        # =====================================================================

        # Initialize variables that may be used in error handling
        ptc_graph = None
        token_callback = None

        # Initialize token tracking if enabled
        token_callback = TokenTrackingManager.initialize_tracking(
            thread_id=thread_id,
            track_tokens=request.track_tokens
        )

        # Create tool tracker for infrastructure cost tracking
        tool_tracker = None
        if request.track_tokens:
            tool_tracker = ToolUsageTracker(thread_id=thread_id)

        # =====================================================================
        # Session and Graph Setup
        # =====================================================================

        # Resolve LLM config for this request
        config = setup.agent_config
        if request.llm_model:
            config = config.model_copy(deep=True)
            config.llm.name = request.llm_model
            config.llm_client = None  # Force rebuild with new model
            logger.info(f"[PTC_CHAT] Using per-request LLM model: {request.llm_model}")

        subagents = request.subagents_enabled or config.subagents_enabled
        sandbox_id = None

        # File operation logger for audit trail
        file_logger = FileOperationLogger(
            workspace_id=workspace_id,
            thread_id=thread_id,
            pair_index=current_pair_index,
            agent="ptc_agent",
        )
        operation_callback = file_logger.create_sync_callback()

        # Use WorkspaceManager for workspace-based sessions
        logger.info(f"[PTC_CHAT] Using workspace: {workspace_id}")
        workspace_manager = WorkspaceManager.get_instance()
        session = await workspace_manager.get_session_for_workspace(workspace_id)

        # Update workspace activity
        await update_workspace_activity(workspace_id)

        registry_store = BackgroundRegistryStore.get_instance()
        background_registry = await registry_store.get_or_create_registry(thread_id)

        # Build graph with the workspace's session
        ptc_graph = await build_ptc_graph_with_session(
            session=session,
            config=config,
            subagent_names=subagents,
            operation_callback=operation_callback,
            checkpointer=setup.checkpointer,
            background_registry=background_registry,
        )

        if session.sandbox:
            sandbox_id = getattr(session.sandbox, 'sandbox_id', None)

        # Store graph for persistence snapshots
        setup.graph = ptc_graph

        # =====================================================================
        # State Restoration (from additional_context)
        # =====================================================================

        restored_state = None
        last_thread_id = parse_last_thread_id(request.additional_context)

        if last_thread_id:
            logger.info(f"[PTC_CHAT] Attempting state restoration from thread: {last_thread_id}")

            # Restore state from the previous thread
            restored_state = await restore_state_with_fallback(
                graph=ptc_graph,
                last_thread_id=last_thread_id
            )

            if restored_state:
                logger.info(
                    f"[PTC_CHAT] State restored from thread {last_thread_id}: "
                    f"messages={len(restored_state.get('messages', []))}"
                )
            else:
                logger.warning(
                    f"[PTC_CHAT] Failed to restore state from thread {last_thread_id}, "
                    f"starting fresh"
                )

        # Build input state from messages
        messages = []
        for msg in request.messages:
            if isinstance(msg.content, str):
                messages.append({"role": msg.role, "content": msg.content})
            elif isinstance(msg.content, list):
                # Handle multi-part content
                content_items = []
                for item in msg.content:
                    if hasattr(item, 'type'):
                        if item.type == "text" and item.text:
                            content_items.append({"type": "text", "text": item.text})
                        elif item.type == "image" and item.image_url:
                            content_items.append({"type": "image_url", "image_url": item.image_url})
                messages.append({"role": msg.role, "content": content_items or str(msg.content)})

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
        elif request.interrupt_feedback:
            # Legacy: String-based feedback (deprecated but still supported)
            resume_msg = f"[{request.interrupt_feedback}]"
            if user_input:
                resume_msg += f" {user_input}"
            input_state = Command(resume=resume_msg)
            logger.info(f"[PTC_RESUME] thread_id={thread_id} feedback={request.interrupt_feedback}")
        elif restored_state:
            # Merge restored state with new messages
            # For PTC, we preserve the restored messages and append new ones
            existing_messages = restored_state.get("messages", [])

            # Build merged state
            input_state = dict(restored_state)
            input_state["messages"] = existing_messages + messages
            input_state["current_agent"] = "ptc"  # For FileOperationMiddleware SSE events

            logger.info(
                f"[PTC_CHAT] Merged state: {len(existing_messages)} existing + "
                f"{len(messages)} new messages"
            )
        else:
            input_state = {
                "messages": messages,
                "current_agent": "ptc",  # For FileOperationMiddleware SSE events
            }

        # =====================================================================
        # Plan Mode Injection
        # =====================================================================
        # When plan_mode is enabled, inject a reminder for the agent to create
        # a plan and submit it for approval before executing any changes.
        if request.plan_mode and not request.hitl_response and not request.interrupt_feedback:
            plan_mode_reminder = (
                "\n\n[PLAN MODE ENABLED]\n"
                "Before making any changes, you MUST:\n"
                "1. Explore the codebase to understand the current state\n"
                "2. Create a detailed plan describing what you intend to do\n"
                "3. Call the `submit_plan` tool with your plan description\n"
                "4. Wait for user approval before proceeding with execution\n"
                "Do NOT execute any write operations until the plan is approved."
            )
            # Append reminder to the last user message
            if isinstance(input_state, dict) and input_state.get("messages"):
                last_msg = input_state["messages"][-1]
                if isinstance(last_msg, dict) and last_msg.get("role") == "user":
                    if isinstance(last_msg.get("content"), str):
                        last_msg["content"] = last_msg["content"] + plan_mode_reminder
                    elif isinstance(last_msg.get("content"), list):
                        # Multi-part content - add as text item
                        last_msg["content"].append({"type": "text", "text": plan_mode_reminder})
            logger.info(f"[PTC_CHAT] Plan mode enabled for thread_id={thread_id}")

        # =====================================================================
        # LangSmith Tracing Configuration
        # =====================================================================

        # Build LangSmith tags for filtering/grouping traces
        langsmith_tags = get_langsmith_tags(
            msg_type="ptc",
            deepthinking=False,  # PTC agent doesn't use deep thinking mode
            auto_accepted_plan=False,
            locale=request.locale,
        )

        # Build LangSmith metadata for detailed trace context
        langsmith_metadata = get_langsmith_metadata(
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
            workflow_type="ptc_agent",
            locale=request.locale,
            timezone=timezone_str,
            deepthinking=False,
            auto_accepted_plan=False,
            track_tokens=request.track_tokens,
        )

        # Build LangGraph config
        config = {
            "configurable": {
                "thread_id": thread_id,
            },
            "recursion_limit": 1000,
            "tags": langsmith_tags,
            "metadata": langsmith_metadata,
        }

        if request.checkpoint_id:
            config["configurable"]["checkpoint_id"] = request.checkpoint_id

        # Add callbacks to config if token tracking is enabled
        if request.track_tokens and token_callback:
            config["callbacks"] = [token_callback]

        # Extract background task registry from orchestrator (single source of truth for SSE events)
        # The orchestrator wraps the middleware which owns the registry
        background_registry = None
        if hasattr(ptc_graph, 'middleware') and hasattr(ptc_graph.middleware, 'registry'):
            background_registry = ptc_graph.middleware.registry
            logger.debug(f"[PTC_CHAT] Background registry attached for thread_id={thread_id}")

        # Reuse WorkflowStreamHandler for SSE streaming
        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            track_tokens=request.track_tokens,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
            background_registry=background_registry,
        )

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
            }
        )

        # =====================================================================
        # Phase 2: Background Execution with Completion Callback
        # =====================================================================

        manager = BackgroundTaskManager.get_instance()

        # Wait for any soft-interrupted workflow to complete before starting new one
        # This ensures seamless continuation after ESC interrupt
        ready_for_new_request = await manager.wait_for_soft_interrupted(thread_id, timeout=30.0)
        if not ready_for_new_request:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Workflow {thread_id} is still running. "
                    "Wait a moment, or use /reconnect to continue streaming, or /cancel to stop it."
                ),
            )

        # Define completion callback for background persistence
        async def on_background_workflow_complete():
            """Persists workflow data after background execution completes."""
            try:
                execution_time = time.time() - start_time
                _persistence_service = ConversationPersistenceService.get_instance(thread_id)

                # Get per-call records for usage tracking
                _per_call_records = token_callback.per_call_records if token_callback else None

                # Get tool usage summary from handler
                _tool_usage = None
                if handler:
                    _tool_usage = handler.get_tool_usage()

                # Get tracking context and serialize agent messages
                tracking_context = ExecutionTracker.get_context()
                _agent_messages = {}
                if tracking_context and tracking_context.agent_messages:
                    for agent_name, msgs in tracking_context.agent_messages.items():
                        _agent_messages[agent_name] = [
                            serialize_agent_message(msg) for msg in msgs
                        ]
                    logger.debug(
                        f"[PTC_COMPLETE] Serialized {sum(len(m) for m in _agent_messages.values())} "
                        f"messages from {len(_agent_messages)} agents"
                    )

                state_snapshot = None
                try:
                    snapshot = await ptc_graph.aget_state({"configurable": {"thread_id": thread_id}})
                    if snapshot and snapshot.values:
                        state_snapshot = serialize_state_snapshot(snapshot.values)
                except Exception as state_error:
                    logger.warning(f"[PTC_COMPLETE] Failed to get state snapshot: {state_error}")

                # Persist completion to database
                await _persistence_service.persist_completion(
                    agent_messages=_agent_messages or None,
                    metadata={
                        "workspace_id": request.workspace_id,
                        "sandbox_id": sandbox_id,
                        "locale": request.locale,
                        "timezone": timezone_str,
                        "msg_type": "ptc",
                    },
                    state_snapshot=state_snapshot,
                    execution_time=execution_time,
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                )

                # Mark completed in Redis tracker
                await tracker.mark_completed(
                    thread_id=thread_id,
                    metadata={
                        "completed_at": datetime.now().isoformat(),
                        "execution_time": execution_time,
                    }
                )

                logger.info(
                    f"[PTC_COMPLETE] Background completion persisted: thread_id={thread_id} "
                    f"duration={execution_time:.2f}s"
                )

            except Exception as e:
                logger.error(
                    f"[PTC_CHAT] Background completion persistence failed for {thread_id}: {e}",
                    exc_info=True
                )

        # Clear event buffer when resuming from interrupt
        if request.hitl_response or request.interrupt_feedback:
            logger.info(f"[PTC_CHAT] Clearing event buffer for interrupt resume: {thread_id}")
            await manager.clear_event_buffer(thread_id)

        # Start workflow in background with event buffering
        task_info = await manager.start_workflow(
            thread_id=thread_id,
            workflow_generator=handler.stream_workflow(
                graph=ptc_graph,
                input_state=input_state,
                config=config,
            ),
            metadata={
                "workspace_id": workspace_id,
                "user_id": user_id,
                "sandbox_id": sandbox_id,
                "started_at": datetime.now().isoformat(),
                "start_time": start_time,
                "msg_type": "ptc",
                "locale": request.locale,
                "timezone": timezone_str,
            },
            completion_callback=on_background_workflow_complete,
            graph=ptc_graph,  # Pass graph for state queries in completion/error handlers
        )

        # Create local queue for this connection to receive live events
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

        # Subscribe to live events from the background workflow
        await manager.subscribe_to_live_events(thread_id, live_queue)
        await manager.increment_connection(thread_id)

        try:
            # Stream live events as they're generated
            while True:
                try:
                    sse_event = await asyncio.wait_for(live_queue.get(), timeout=1.0)

                    if sse_event is None:  # Sentinel value - workflow completed
                        break

                    yield sse_event

                except asyncio.TimeoutError:
                    # No events yet, check if workflow completed
                    status = await manager.get_task_status(thread_id)
                    if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                        break
                    continue  # Keep waiting for events

        except asyncio.CancelledError:
            # SSE client disconnected - but background task continues!
            is_explicit_cancel = await tracker.is_cancelled(thread_id)

            if is_explicit_cancel:
                logger.info(f"[PTC_CHAT] Workflow explicitly cancelled by user: thread_id={thread_id}")
                await tracker.mark_cancelled(thread_id)

                # Get token/tool usage for billing
                _per_call_records = token_callback.per_call_records if token_callback else None
                _tool_usage = handler.get_tool_usage() if handler else None

                state_snapshot = None
                try:
                    snapshot = await ptc_graph.aget_state({"configurable": {"thread_id": thread_id}})
                    if snapshot and snapshot.values:
                        state_snapshot = serialize_state_snapshot(snapshot.values)
                except Exception as state_error:
                    logger.warning(f"[PTC_CHAT] Failed to get state snapshot for cancellation: {state_error}")

                # Persist cancellation to database
                try:
                    await persistence_service.persist_cancelled(
                        execution_time=time.time() - start_time,
                        metadata={
                            "workspace_id": request.workspace_id,
                            "msg_type": "ptc",
                        },
                        state_snapshot=state_snapshot,
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                    )
                except Exception as persist_error:
                    logger.error(f"[PTC_CHAT] Failed to persist cancellation: {persist_error}")

                # Cancel the background workflow
                await manager.cancel_workflow(thread_id)

                registry_store = BackgroundRegistryStore.get_instance()
                await registry_store.cancel_and_clear(thread_id, force=True)
            else:
                logger.info(
                    f"[PTC_CHAT] SSE client disconnected, but workflow continues in background: "
                    f"thread_id={thread_id}"
                )
                await tracker.mark_disconnected(
                    thread_id=thread_id,
                    metadata={
                        "workspace_id": workspace_id,
                        "user_id": user_id,
                        "disconnected_at": datetime.now().isoformat()
                    }
                )
                # Background task will continue running!
                logger.info(f"[PTC_CHAT] Background task for {thread_id} will complete independently")

            raise

        finally:
            # Cleanup: unsubscribe from live events
            await manager.unsubscribe_from_live_events(thread_id, live_queue)
            await manager.decrement_connection(thread_id)

    except Exception as e:
        # =====================================================================
        # Phase 4: Error Recovery with Retry Logic
        # =====================================================================

        # Get token/tool usage for billing even on errors
        _per_call_records = token_callback.per_call_records if token_callback else None
        _tool_usage = handler.get_tool_usage() if handler else None

        state_snapshot = None
        try:
            snapshot = await ptc_graph.aget_state({"configurable": {"thread_id": thread_id}})
            if snapshot and snapshot.values:
                state_snapshot = serialize_state_snapshot(snapshot.values)
        except Exception as state_error:
            logger.warning(f"[PTC_CHAT] Failed to get state snapshot after error: {state_error}")

        # Non-recoverable error types (code bugs, config issues)
        non_recoverable_types = (
            AttributeError,   # Code bug - missing attribute
            NameError,        # Code bug - undefined variable
            SyntaxError,      # Code bug - syntax error
            ImportError,      # Missing dependency
            TypeError,        # Wrong type passed
            KeyError,         # Missing key (usually code issue)
        )

        is_non_recoverable = isinstance(e, non_recoverable_types)

        # Recoverable error patterns (transient issues)
        import psycopg

        is_postgres_connection = (
            isinstance(e, psycopg.OperationalError) and
            "server closed the connection" in str(e)
        )

        is_timeout = (
            isinstance(e, TimeoutError) or
            "timeout" in str(e).lower() or
            "timed out" in str(e).lower()
        )

        is_network_issue = (
            isinstance(e, ConnectionError) or
            "connection" in str(e).lower() or
            "network" in str(e).lower() or
            "unreachable" in str(e).lower() or
            "connection refused" in str(e).lower()
        )

        # API errors (transient server errors, rate limits, etc.)
        is_api_error = False
        error_str = str(e).lower()
        error_type_name = type(e).__name__.lower()
        
        # Check for API error types (InternalServerError, APIError, etc.)
        api_error_indicators = [
            "internal server error",
            "api_error",
            "system error",
            "error code: 500",
            "error code: 502",
            "error code: 503",
            "error code: 429",  # Rate limit
            "rate limit",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        ]
        
        is_api_error = (
            any(indicator in error_str for indicator in api_error_indicators) or
            "internal" in error_type_name or
            "api" in error_type_name or
            "server" in error_type_name
        )

        # Determine if error is recoverable
        is_recoverable = (
            (is_postgres_connection or is_timeout or is_network_issue or is_api_error) and
            not is_non_recoverable
        )

        MAX_RETRIES = 3  # Maximum automatic retries

        if is_recoverable:
            # Recoverable error - check retry count
            retry_count = await tracker.increment_retry_count(thread_id)

            error_type = (
                "connection_error" if is_postgres_connection or is_network_issue
                else "timeout_error" if is_timeout
                else "api_error" if is_api_error
                else "transient_error"
            )

            if retry_count > MAX_RETRIES:
                # Exceeded max retries - treat as non-recoverable
                logger.error(
                    f"[PTC_CHAT] Max retries exceeded ({retry_count}/{MAX_RETRIES}) for "
                    f"thread_id={thread_id}: {type(e).__name__}: {str(e)[:100]}"
                )

                # Persist error with retry info
                if persistence_service:
                    try:
                        error_msg = f"Max retries exceeded ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {str(e)}"
                        await persistence_service.persist_error(
                            error_message=error_msg,
                            errors=[error_msg],
                            execution_time=time.time() - start_time,
                            metadata={
                                "workspace_id": request.workspace_id,
                                "msg_type": "ptc",
                            },
                            state_snapshot=state_snapshot,
                            per_call_records=_per_call_records,
                            tool_usage=_tool_usage,
                        )
                    except Exception as persist_error:
                        logger.error(f"[PTC_CHAT] Failed to persist error: {persist_error}")

                # Yield error with retry info
                error_data = {
                    "message": f"Workflow failed after {MAX_RETRIES} retry attempts",
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retry_count": retry_count,
                    "max_retries": MAX_RETRIES,
                    "thread_id": thread_id,
                }
                yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
            else:
                # Within retry limit - allow retry
                logger.warning(
                    f"[PTC_CHAT] Recoverable error ({error_type}) for thread_id={thread_id} "
                    f"(retry {retry_count}/{MAX_RETRIES}): "
                    f"{type(e).__name__}: {str(e)[:100]}"
                )

                # Yield retry info event (not error)
                retry_data = {
                    "message": "Temporary error occurred, you can retry or resume the workflow",
                    "thread_id": thread_id,
                    "auto_retry": True,
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retry_count": retry_count,
                    "max_retries": MAX_RETRIES
                }
                yield f"event: retry\ndata: {json.dumps(retry_data)}\n\n"

                # Mark as interrupted (not error) so it can be resumed
                await qr_db.update_thread_status(thread_id, "interrupted")

        else:
            # Non-recoverable error - persist and fail
            logger.exception(f"[PTC_ERROR] thread_id={thread_id}: {e}")

            # Persist error to database
            if persistence_service:
                try:
                    await persistence_service.persist_error(
                        error_message=str(e),
                        execution_time=time.time() - start_time,
                        metadata={
                            "workspace_id": request.workspace_id,
                            "msg_type": "ptc",
                        },
                        state_snapshot=state_snapshot,
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                    )
                except Exception as persist_error:
                    logger.error(f"[PTC_CHAT] Failed to persist error: {persist_error}")

            # Yield error event using handler's format method if available
            if handler:
                error_event = handler._format_sse_event(
                    "error",
                    {
                        "thread_id": thread_id,
                        "error": str(e),
                        "type": "workflow_error",
                    }
                )
                yield error_event
            else:
                # Fallback error formatting
                error_event = json.dumps({
                    "thread_id": thread_id,
                    "error": str(e),
                    "type": "workflow_error",
                })
                yield f"event: error\ndata: {error_event}\n\n"

        raise

    finally:
        # Always stop execution tracking to prevent memory leaks and context pollution
        ExecutionTracker.stop_tracking()
        logger.debug("PTC execution tracking stopped")


@router.get("/stream/{thread_id}/reconnect")
async def reconnect_to_workflow(
    thread_id: str,
    last_event_id: Optional[int] = Query(None, description="Last received event ID"),
):
    """
    Reconnect to a running or completed PTC workflow.

    Args:
        thread_id: Workflow thread identifier
        last_event_id: Optional last event ID for filtering duplicates

    Returns:
        StreamingResponse with SSE events
    """
    manager = BackgroundTaskManager.get_instance()
    tracker = WorkflowTracker.get_instance()

    # Get workflow info
    task_info = await manager.get_task_info(thread_id)
    workflow_status = await tracker.get_status(thread_id)

    if not task_info:
        if workflow_status and workflow_status.get("status") == "completed":
            raise HTTPException(
                status_code=410,
                detail="Workflow completed and results expired"
            )
        raise HTTPException(
            status_code=404,
            detail=f"Workflow {thread_id} not found"
        )

    async def stream_reconnection():
        try:
            # Replay buffered events
            buffered_events = await manager.get_buffered_events_redis(
                thread_id,
                from_beginning=True,
                after_event_id=last_event_id,
            )

            logger.info(
                f"[PTC_RECONNECT] Replaying {len(buffered_events)} events "
                f"for {thread_id}"
            )

            for event in buffered_events:
                yield event

            # Attach to live stream if still running
            status = await manager.get_task_status(thread_id)

            if status == TaskStatus.RUNNING:
                live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
                await manager.subscribe_to_live_events(thread_id, live_queue)
                await manager.increment_connection(thread_id)

                try:
                    while True:
                        try:
                            event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                            if event is None:
                                break
                            yield event
                        except asyncio.TimeoutError:
                            current_status = await manager.get_task_status(thread_id)
                            if current_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                                break
                            continue
                finally:
                    await manager.unsubscribe_from_live_events(thread_id, live_queue)
                    await manager.decrement_connection(thread_id)

        except Exception as e:
            logger.error(f"[PTC_RECONNECT] Error: {e}", exc_info=True)
            yield f"event: error\ndata: {{\"error\": \"Reconnection failed: {str(e)}\"}}\n\n"

    return StreamingResponse(
        stream_reconnection(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@router.get("/stream/{thread_id}/status")
async def stream_subagent_status(thread_id: str):
    """Stream subagent status updates for a thread."""
    registry_store = BackgroundRegistryStore.get_instance()

    async def stream_status():
        event_id = 0
        last_payload: dict[str, list] | None = None

        while True:
            registry = await registry_store.get_registry(thread_id)
            if registry:
                tasks = await registry.get_all_tasks()
                pending = [task for task in tasks if task.is_pending]
                completed = [task for task in tasks if task.completed]

                active_tasks = [
                    {
                        "id": task.display_id,
                        "description": task.description[:100] if task.description else "",
                        "type": task.subagent_type,
                        "tool_calls": task.total_tool_calls,
                        "current_tool": task.current_tool,
                    }
                    for task in sorted(pending, key=lambda task: task.display_id)
                ]
                completed_tasks = sorted([task.display_id for task in completed])
            else:
                active_tasks = []
                completed_tasks = []

            payload = {
                "active_tasks": active_tasks,
                "completed_tasks": completed_tasks,
            }

            if payload != last_payload:
                event_id += 1
                last_payload = payload
                event_payload = {
                    "thread_id": thread_id,
                    **payload,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                yield f"id: {event_id}\nevent: subagent_status\ndata: {json.dumps(event_payload)}\n\n"

            if not active_tasks:
                break

            await asyncio.sleep(1.0)

    return StreamingResponse(
        stream_status(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/cancel/{thread_id}")
async def cancel_workflow(thread_id: str):
    """
    Cancel a running PTC workflow.

    Args:
        thread_id: Workflow thread identifier

    Returns:
        Status of cancellation
    """
    tracker = WorkflowTracker.get_instance()
    manager = BackgroundTaskManager.get_instance()

    # Mark as cancelled
    await tracker.request_cancellation(thread_id)

    # Cancel background task
    await manager.cancel_workflow(thread_id)

    registry_store = BackgroundRegistryStore.get_instance()
    await registry_store.cancel_and_clear(thread_id, force=True)

    logger.info(f"[PTC_CANCEL] thread_id={thread_id}")

    return {"status": "cancelled", "thread_id": thread_id}


@router.get("/status/{thread_id}")
async def get_chat_workflow_status(thread_id: str) -> StatusResponse:
    """
    Get status of a PTC workflow.

    Args:
        thread_id: Workflow thread identifier

    Returns:
        StatusResponse with workflow status
    """
    tracker = WorkflowTracker.get_instance()
    manager = BackgroundTaskManager.get_instance()

    # Get status from tracker
    status_info = await tracker.get_status(thread_id)

    if not status_info:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow {thread_id} not found"
        )

    # Get task info for additional details
    task_info = await manager.get_task_info(thread_id)

    return StatusResponse(
        thread_id=thread_id,
        status=status_info.get("status", "unknown"),
        workspace_id=status_info.get("workspace_id"),
        sandbox_id=task_info.get("sandbox_id") if task_info else None,
        started_at=status_info.get("started_at"),
        completed_at=status_info.get("completed_at"),
        error=status_info.get("error"),
    )


@router.get("/sessions")
async def get_chat_sessions():
    """
    Get information about active PTC sessions.

    Returns:
        Dict with session statistics and details
    """
    try:
        session_service = SessionService.get_instance()
        return session_service.get_stats()
    except ValueError:
        # Service not initialized
        return {
            "active_sessions": 0,
            "message": "PTC Session Service not initialized",
        }
