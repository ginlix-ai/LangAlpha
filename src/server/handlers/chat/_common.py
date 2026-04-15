"""Shared helpers for chat handler modules (flash & PTC).

This module consolidates private helpers, error classification, and common
setup routines that are identical (or near-identical) between the flash and
PTC workflow handlers.  Keeping them in one place eliminates duplication and
ensures behavioural parity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import psycopg
from fastapi import HTTPException

from src.config.langsmith import get_filtered_langsmith_tracer
from src.config.settings import (
    get_langsmith_metadata,
    get_langsmith_tags,
    get_live_queue_maxsize,
    get_locale_config,
    get_max_workflow_retries,
    is_sse_event_log_enabled,
)
from src.server.app import setup
from src.server.database import conversation as qr_db
from src.server.models.chat import summarize_hitl_response_map
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.server.services.persistence.conversation import (
    ConversationPersistenceService,
)
from src.server.services.workflow_tracker import WorkflowTracker
from src.server.utils.skill_context import (
    build_skill_content,
    detect_slash_commands,
    parse_skill_contexts,
)
from src.tools.fetch import fetch_llm_client_override, fetch_model_override
from src.utils.tracking import TokenTrackingManager
from src.tools.decorators import ToolUsageTracker
from src.server.dependencies.usage_limits import release_burst_slot

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Hard-coded logger name to preserve existing log routing regardless of where
# the module physically lives after refactoring.
logger = logging.getLogger("src.server.handlers.chat_handler")
_sse_logger = logging.getLogger("sse_events")

_SSE_LOG_ENABLED = is_sse_event_log_enabled()


# ---------------------------------------------------------------------------
# Private helpers (moved as-is from original chat_handler.py)
# ---------------------------------------------------------------------------


def _append_to_last_user_message(messages: list[dict], text: str) -> None:
    """Append text to the last user message in a message list (mutates in-place)."""
    if not messages:
        return
    last_msg = messages[-1]
    if not isinstance(last_msg, dict) or last_msg.get("role") != "user":
        return
    content = last_msg.get("content")
    if isinstance(content, str):
        last_msg["content"] = content + text
    elif isinstance(content, list):
        last_msg["content"].append({"type": "text", "text": text})


def _resolve_timezone(request_timezone: Optional[str], locale: Optional[str]) -> str:
    """Validate request timezone, falling back to locale-based default."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    if request_timezone:
        try:
            ZoneInfo(request_timezone)
            return request_timezone
        except ZoneInfoNotFoundError:
            logger.warning(
                f"Invalid timezone '{request_timezone}', falling back to locale-based timezone."
            )

    locale_config = get_locale_config(locale or "en-US", "en")
    return locale_config.get("timezone", "UTC")


async def _setup_fork_and_persistence(
    *,
    request: ChatRequest,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    log_prefix: str = "FORK",
) -> tuple[str, bool, ConversationPersistenceService]:
    """Compute query_type, apply fork cleanup, and init persistence service.

    Shared by both flash and PTC handlers. Returns (query_type, is_fork, persistence_service).
    """
    # Determine query type (allow explicit override for internal callers)
    if request.query_type:
        query_type = request.query_type
    else:
        is_resume = bool(request.hitl_response)
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)
        if is_resume:
            query_type = "resume_feedback"
        elif is_checkpoint_replay:
            query_type = "regenerate"
        else:
            query_type = "initial"

    # Fork cleanup: truncate app DB when branching from a checkpoint
    is_fork = request.fork_from_turn is not None and request.checkpoint_id
    if is_fork:
        # Parallelize the two DB operations (different tables, no dependency)
        deleted, _ = await asyncio.gather(
            qr_db.truncate_thread_from_turn(
                thread_id,
                request.fork_from_turn,
                preserve_query_at_fork=is_checkpoint_replay,
            ),
            qr_db.update_thread_checkpoint_id(thread_id, request.checkpoint_id),
        )
        logger.info(
            f"[{log_prefix}] Truncated {deleted} rows from turn_index>={request.fork_from_turn} "
            f"thread_id={thread_id} checkpoint_id={request.checkpoint_id}"
        )
        # Clear Redis event buffer (stale events from old branch) — fault-tolerant
        try:
            manager = BackgroundTaskManager.get_instance()
            await manager.clear_event_buffer(thread_id)
        except Exception as e:
            logger.warning(f"[{log_prefix}] Failed to clear event buffer: {e}")

    # Initialize persistence service
    persistence_service = ConversationPersistenceService.get_instance(
        thread_id=thread_id, workspace_id=workspace_id, user_id=user_id
    )

    # Reset persistence cache if forking, otherwise calculate from DB
    if is_fork:
        persistence_service.reset_for_fork(request.fork_from_turn)
    else:
        await persistence_service.get_or_calculate_turn_index()

    return query_type, is_fork, persistence_service


async def _handle_sse_disconnect(
    tracker,
    manager,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    live_queue,
    handler,
    token_callback,
    persistence_service,
    start_time: float,
    request,
    is_byok: bool = False,
):
    """Fire-and-forget cleanup when the SSE client disconnects.

    Runs as an independent asyncio.Task outside Starlette's anyio cancel scope,
    so awaits work normally. Handles both explicit cancel (user clicked cancel)
    and accidental disconnect (tab close, refresh, network drop).
    """
    try:
        is_explicit_cancel = await tracker.is_cancelled(thread_id)

        if is_explicit_cancel:
            logger.info(
                f"[CHAT] Workflow explicitly cancelled by user: thread_id={thread_id}"
            )
            await tracker.mark_cancelled(thread_id)

            _per_call_records = (
                token_callback.per_call_records if token_callback else None
            )
            _tool_usage = handler.get_tool_usage() if handler else None

            try:
                _sse_events = handler.get_sse_events() if handler else None
                await persistence_service.persist_cancelled(
                    execution_time=time.time() - start_time,
                    metadata={
                        "workspace_id": request.workspace_id,
                        "is_byok": is_byok,
                    },
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                    sse_events=_sse_events,
                )
            except Exception as persist_error:
                logger.error(f"[CHAT] Failed to persist cancellation: {persist_error}")

            await manager.cancel_workflow(thread_id)

            registry_store = BackgroundRegistryStore.get_instance()
            await registry_store.cancel_and_clear(thread_id, force=True)
        else:
            logger.info(
                f"[CHAT] SSE client disconnected, workflow continues in "
                f"background: thread_id={thread_id}"
            )
            await tracker.mark_disconnected(
                thread_id=thread_id,
                metadata={
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "disconnected_at": datetime.now().isoformat(),
                },
            )
    except Exception as e:
        logger.error(
            f"[CHAT] Error during SSE disconnect cleanup for {thread_id}: {e}",
            exc_info=True,
        )
    finally:
        try:
            await manager.unsubscribe_from_live_events(thread_id, live_queue)
        except Exception:
            pass
        try:
            await manager.decrement_connection(thread_id)
        except Exception:
            pass


async def _is_plan_interrupt_pending(thread_id: str) -> bool:
    """Check if the pending interrupt is a SubmitPlan (plan mode) interrupt.

    Plan interrupts from HumanInTheLoopMiddleware have action_requests with
    name="SubmitPlan". Other interrupts (AskUserQuestion, onboarding) use
    a "type" field instead. Returns False on any error.
    """
    try:
        checkpointer = setup.checkpointer
        if not checkpointer:
            return False
        config = {"configurable": {"thread_id": thread_id}}
        checkpoint_tuple = await checkpointer.aget_tuple(config)
        if not checkpoint_tuple or not checkpoint_tuple.pending_writes:
            return False
        for _task_id, channel, value in checkpoint_tuple.pending_writes:
            if channel != "__interrupt__":
                continue
            interrupts = value if isinstance(value, list) else [value]
            for intr in interrupts:
                intr_value = (
                    getattr(intr, "value", intr)
                    if not isinstance(intr, dict)
                    else intr.get("value", intr)
                )
                if not isinstance(intr_value, dict):
                    continue
                action_requests = intr_value.get("action_requests", [])
                if action_requests and isinstance(action_requests[0], dict):
                    if action_requests[0].get("name") == "SubmitPlan":
                        return True
        return False
    except Exception:
        logger.warning(
            f"[PTC_CHAT] Failed to check pending interrupt type for "
            f"thread_id={thread_id}, defaulting to non-plan mode",
            exc_info=True,
        )
        return False


# ---------------------------------------------------------------------------
# NEW extracted DRY helpers
# ---------------------------------------------------------------------------


def classify_error(e: Exception) -> dict:
    """Classify an exception as recoverable or non-recoverable.

    Returns a dict with keys:
        is_recoverable  (bool)
        is_non_recoverable  (bool)
        error_type  (str | None)  — one of "connection_error", "timeout_error",
                                     "api_error", "transient_error", or None
                                     for non-recoverable errors.
    """
    # Non-recoverable error types (code bugs, config issues)
    non_recoverable_types = (
        AttributeError,
        NameError,
        SyntaxError,
        ImportError,
        TypeError,
        KeyError,
    )

    is_non_recoverable = isinstance(e, non_recoverable_types)

    # Recoverable error patterns (transient issues)
    is_postgres_connection = isinstance(
        e, psycopg.OperationalError
    ) and "server closed the connection" in str(e)

    is_timeout = (
        isinstance(e, TimeoutError)
        or "timeout" in str(e).lower()
        or "timed out" in str(e).lower()
    )

    is_network_issue = (
        isinstance(e, ConnectionError)
        or "connection" in str(e).lower()
        or "network" in str(e).lower()
        or "unreachable" in str(e).lower()
        or "connection refused" in str(e).lower()
    )

    # API errors (transient server errors, rate limits, etc.)
    error_str = str(e).lower()
    error_type_name = type(e).__name__.lower()

    api_error_indicators = [
        "internal server error",
        "api_error",
        "system error",
        "error code: 500",
        "error code: 502",
        "error code: 503",
        "error code: 429",
        "rate limit",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
    ]

    is_api_error = (
        any(indicator in error_str for indicator in api_error_indicators)
        or "internal" in error_type_name
        or "api" in error_type_name
        or "server" in error_type_name
    )

    is_recoverable = (
        is_postgres_connection or is_timeout or is_network_issue or is_api_error
    ) and not is_non_recoverable

    # Determine specific error_type label
    if is_recoverable:
        if is_postgres_connection or is_network_issue:
            error_type = "connection_error"
        elif is_timeout:
            error_type = "timeout_error"
        elif is_api_error:
            error_type = "api_error"
        else:
            error_type = "transient_error"
    else:
        error_type = None

    return {
        "is_recoverable": is_recoverable,
        "is_non_recoverable": is_non_recoverable,
        "error_type": error_type,
    }


def process_hitl_response(request: ChatRequest) -> tuple[str, str, dict, list]:
    """Extract HITL answer metadata for persistence.

    Returns (feedback_action, query_content, hitl_answers, interrupt_ids).
    ``feedback_action`` is "QUESTION_ANSWERED" or "QUESTION_SKIPPED".
    ``query_content`` is the summarized content string.
    ``hitl_answers`` maps interrupt_id -> answer string | None.
    ``interrupt_ids`` is the list of interrupt IDs from the response map.
    """
    summary = summarize_hitl_response_map(request.hitl_response)
    feedback_action = summary["feedback_action"]
    query_content = summary["content"]
    interrupt_ids = summary["interrupt_ids"]

    hitl_answers: dict = {}
    for interrupt_id, response in request.hitl_response.items():
        decisions = (
            response.decisions
            if hasattr(response, "decisions")
            else response.get("decisions", [])
        )
        for d in decisions:
            d_type = d.type if hasattr(d, "type") else d.get("type")
            d_msg = (
                d.message if hasattr(d, "message") else d.get("message")
            ) or ""
            if d_type == "approve" and d_msg:
                hitl_answers[interrupt_id] = d_msg
            elif d_type == "reject" and not d_msg:
                hitl_answers[interrupt_id] = None

    if hitl_answers:
        has_answers = any(v is not None for v in hitl_answers.values())
        feedback_action = (
            "QUESTION_ANSWERED" if has_answers else "QUESTION_SKIPPED"
        )

    return feedback_action, query_content, hitl_answers, interrupt_ids


def serialize_context_metadata(
    request: ChatRequest,
    query_metadata: dict,
    user_input: str,
    mode: str,
) -> None:
    """Serialize additional_context into lightweight persistence metadata.

    Handles two cases:
    1. ``request.additional_context`` is present — serialize ``skills`` and
       ``directive`` entries (skip heavy multimodal data).
    2. Fallback — detect slash commands from ``user_input`` text when no
       context was provided by the frontend.

    Mutates *query_metadata* in-place.
    """
    if request.additional_context:
        serialized_ctx = []
        for ctx in request.additional_context:
            ctx_type = getattr(ctx, "type", None)
            if ctx_type == "skills":
                serialized_ctx.append({"type": "skills", "name": ctx.name})
            elif ctx_type == "directive":
                serialized_ctx.append({"type": "directive", "content": ctx.content})
        if serialized_ctx:
            query_metadata["additional_context"] = serialized_ctx

    # Detect slash commands from message text when additional_context is absent
    if not request.hitl_response and "additional_context" not in query_metadata:
        _, early_detected = detect_slash_commands(user_input, mode=mode)
        if early_detected:
            query_metadata["additional_context"] = [
                {"type": "skills", "name": s.name} for s in early_detected
            ]


def setup_steering_tracking(handler) -> None:
    """Wire up steering tracking on a ``WorkflowStreamHandler``.

    Registers a callback so that messages injected mid-workflow are tracked
    for post-completion query backfill.
    """

    async def _track_steerings(messages):
        handler.injected_steerings.extend(
            msg for msg in messages if msg.get("content")
        )

    handler.on_steering_delivered = _track_steerings


def normalize_request_messages(request: ChatRequest) -> list[dict]:
    """Convert ``request.messages`` to a flat list of ``{"role": ..., "content": ...}`` dicts.

    Handles both plain-string and multi-part (text / image_url) content items.
    """
    messages: list[dict] = []
    for msg in request.messages:
        if isinstance(msg.content, str):
            messages.append({"role": msg.role, "content": msg.content})
        elif isinstance(msg.content, list):
            content_items = []
            for item in msg.content:
                if hasattr(item, "type"):
                    if item.type == "text" and item.text:
                        content_items.append({"type": "text", "text": item.text})
                    elif item.type == "image" and item.image_url:
                        content_items.append(
                            {
                                "type": "image_url",
                                "image_url": {"url": item.image_url},
                            }
                        )
            messages.append(
                {"role": msg.role, "content": content_items or str(msg.content)}
            )
    return messages


def init_tracking(thread_id: str) -> tuple[TokenTrackingManager, ToolUsageTracker]:
    """Initialise token + tool tracking for a workflow.

    Returns ``(token_callback, tool_tracker)``.
    """
    token_callback = TokenTrackingManager.initialize_tracking(
        thread_id=thread_id, track_tokens=True
    )
    tool_tracker = ToolUsageTracker(thread_id=thread_id)
    return token_callback, tool_tracker


def apply_fetch_override(config) -> None:
    """Propagate fetch model / client overrides from *config* into context vars."""
    if config.llm and config.llm.fetch:
        fetch_model_override.set(config.llm.fetch)
        fetch_client = config.subsidiary_llm_clients.get("fetch")
        if fetch_client:
            fetch_llm_client_override.set(fetch_client)


async def ensure_thread(
    request: ChatRequest,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    msg_type: str,
    initial_query: str = "",
) -> None:
    """Ensure a thread record exists in the database, optionally with external linkage."""
    ensure_kwargs = dict(
        workspace_id=workspace_id,
        conversation_thread_id=thread_id,
        user_id=user_id,
        initial_query=initial_query,
        initial_status="in_progress",
        msg_type=msg_type,
    )
    if request.external_thread_id and request.platform:
        ensure_kwargs["external_id"] = request.external_thread_id
        ensure_kwargs["platform"] = request.platform
    await qr_db.ensure_thread_exists(**ensure_kwargs)


async def persist_or_skip_replay(
    persistence_service: ConversationPersistenceService,
    is_checkpoint_replay: bool,
    request: ChatRequest,
    query_content: str,
    query_type: str,
    feedback_action: str | None,
    query_metadata: dict,
    thread_id: str,
    log_prefix: str,
) -> None:
    """Persist query start or skip persistence for checkpoint replay.

    For checkpoint replays (regenerate/retry), marks the preserved query turn
    as already persisted so ``persist_completion`` doesn't skip.  Otherwise
    calls ``persist_query_start``.
    """
    if is_checkpoint_replay:
        turn_to_mark = (
            request.fork_from_turn
            if request.fork_from_turn is not None
            else await persistence_service.get_or_calculate_turn_index()
        )
        persistence_service.mark_query_persisted(turn_to_mark)
        logger.debug(
            f"[{log_prefix}] Skipped query persist (checkpoint replay): "
            f"thread_id={thread_id} turn_index={turn_to_mark}"
        )
    else:
        await persistence_service.persist_query_start(
            content=query_content,
            query_type=query_type,
            feedback_action=feedback_action,
            metadata=query_metadata,
        )


def inject_skills(
    messages: list[dict],
    request: ChatRequest,
    config,
    mode: str,
) -> list[str]:
    """Detect and inject skill content into the last user message.

    Handles slash-command detection as a fallback when ``additional_context``
    does not contain ``skill_contexts``.

    Returns the list of loaded skill names.
    """
    loaded_skill_names: list[str] = []
    skill_contexts = parse_skill_contexts(request.additional_context)

    # Detect slash commands from message text (fallback for missing additional_context)
    if not skill_contexts and not request.hitl_response and messages:
        last_msg = messages[-1]
        msg_text = last_msg.get("content", "") if isinstance(last_msg.get("content"), str) else ""
        if msg_text:
            cleaned_text, detected = detect_slash_commands(msg_text, mode=mode)
            if detected:
                skill_contexts = detected
                if cleaned_text != msg_text:
                    last_msg["content"] = cleaned_text

    if skill_contexts:
        skill_dirs = [
            local_dir
            for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()
        ]
        skill_result = build_skill_content(
            skill_contexts, skill_dirs=skill_dirs, mode=mode
        )
        if skill_result:
            _append_to_last_user_message(messages, "\n\n" + skill_result.content)
            loaded_skill_names = skill_result.loaded_skill_names
            logger.info(f"[{mode.upper()}_CHAT] Skills injected: {loaded_skill_names}")

    return loaded_skill_names


def build_graph_config(
    thread_id: str,
    user_id: str,
    workspace_id: str,
    mode: str,
    timezone_str: str,
    token_callback,
    request: ChatRequest,
    effective_model: str | None,
    is_byok: bool,
    recursion_limit: int,
    plan_mode: bool | None = None,
    extra_configurable: dict | None = None,
) -> dict:
    """Build the LangGraph ``config`` dict shared by flash and PTC handlers.

    ``mode`` should be ``"flash"`` or ``"ptc"``.
    ``extra_configurable`` is an optional dict merged into ``configurable``.
    """
    workflow_type = "flash_agent" if mode == "flash" else "ptc_agent"

    langsmith_tags = get_langsmith_tags(
        msg_type=mode,
        locale=request.locale,
    )
    langsmith_metadata = get_langsmith_metadata(
        user_id=user_id,
        workspace_id=workspace_id,
        thread_id=thread_id,
        workflow_type=workflow_type,
        locale=request.locale,
        timezone=timezone_str,
        llm_model=effective_model,
        reasoning_effort=getattr(request, "reasoning_effort", None),
        fast_mode=getattr(request, "fast_mode", None),
        is_byok=is_byok,
        platform=request.platform,
        **({"plan_mode": plan_mode} if plan_mode is not None else {}),
    )

    configurable: dict = {
        "thread_id": thread_id,
        "user_id": user_id,
        "workspace_id": workspace_id,
        "agent_mode": mode,
        "timezone": timezone_str,
    }
    if extra_configurable:
        configurable.update(extra_configurable)

    graph_config: dict = {
        "configurable": configurable,
        "recursion_limit": recursion_limit,
        "tags": langsmith_tags,
        "metadata": langsmith_metadata,
    }

    if request.checkpoint_id:
        graph_config["configurable"]["checkpoint_id"] = request.checkpoint_id

    # Add callbacks: token tracking + LangSmith trace filtering
    callbacks: list = []
    if token_callback:
        callbacks.append(token_callback)

    langsmith_tracer = get_filtered_langsmith_tracer()
    if langsmith_tracer:
        callbacks.append(langsmith_tracer)

    if callbacks:
        graph_config["callbacks"] = callbacks

    return graph_config


async def wait_or_steer(
    manager: BackgroundTaskManager,
    thread_id: str,
    user_input: str,
    user_id: str,
) -> tuple[bool, str | None]:
    """Wait for a soft-interrupted workflow, or steer the running workflow.

    Returns ``(ready, steering_event)`` where ``ready=True`` means the caller
    should proceed with a new workflow.  If ``ready=False`` and
    ``steering_event`` is not None, the caller should yield that SSE string
    and return.  If neither succeeds, raises HTTP 409.
    """
    # Deferred to avoid circular import: steering imports _common at
    # module level, so _common must not import steering at module level.
    from src.server.handlers.chat.steering import steer_thread

    ready_for_new_request = await manager.wait_for_soft_interrupted(thread_id)
    if ready_for_new_request:
        return True, None

    # Try to steer the running workflow
    result = await steer_thread(thread_id, user_input, user_id)
    if result:
        event_data = json.dumps(
            {
                "thread_id": thread_id,
                "content": user_input,
                "position": result["position"],
            }
        )
        return False, f"event: steering_accepted\ndata: {event_data}\n\n"

    # Fallback: raise 409 if steering failed
    raise HTTPException(
        status_code=409,
        detail=(
            f"Workflow {thread_id} is still running. "
            "Wait a moment, or use /reconnect to continue streaming, or /cancel to stop it."
        ),
    )


async def stream_live_events(
    manager: BackgroundTaskManager,
    tracker: WorkflowTracker,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    handler,
    token_callback,
    persistence_service: ConversationPersistenceService,
    start_time: float,
    request: ChatRequest,
    is_byok: bool,
    log_prefix: str = "CHAT",
) -> AsyncGenerator[str, None]:
    """Subscribe to live SSE events from a background workflow and yield them.

    Handles client disconnect by spawning ``_handle_sse_disconnect`` as an
    independent asyncio task.  After the workflow ends, drains any unconsumed
    steering messages and emits a ``steering_returned`` event.
    """
    # Deferred to avoid circular import: steering imports _common at
    # module level, so _common must not import steering at module level.
    from src.server.handlers.chat.steering import drain_steering_return_event

    live_queue: asyncio.Queue = asyncio.Queue(maxsize=get_live_queue_maxsize())
    await manager.subscribe_to_live_events(thread_id, live_queue)
    await manager.increment_connection(thread_id)

    _disconnected = False
    try:
        while True:
            try:
                sse_event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                if sse_event is None:
                    break
                yield sse_event
            except asyncio.TimeoutError:
                status = await manager.get_task_status(thread_id)
                if status in [
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ]:
                    break
                continue

        # After workflow ends, return any unconsumed steering messages to the client
        steering_event = await drain_steering_return_event(thread_id)
        if steering_event:
            logger.info(
                f"[{log_prefix}] Returning unconsumed steering "
                f"message(s) to client: thread_id={thread_id}"
            )
            yield steering_event

    except (asyncio.CancelledError, GeneratorExit):
        _disconnected = True
        asyncio.create_task(
            _handle_sse_disconnect(
                tracker=tracker,
                manager=manager,
                thread_id=thread_id,
                workspace_id=workspace_id,
                user_id=user_id,
                live_queue=live_queue,
                handler=handler,
                token_callback=token_callback,
                persistence_service=persistence_service,
                start_time=start_time,
                request=request,
                is_byok=is_byok,
            ),
            name=f"sse-disconnect-cleanup-{thread_id}",
        )
        raise
    finally:
        if not _disconnected:
            try:
                await manager.unsubscribe_from_live_events(thread_id, live_queue)
            except Exception:
                pass
            try:
                await manager.decrement_connection(thread_id)
            except Exception:
                pass


async def handle_workflow_error(
    e: Exception,
    thread_id: str,
    user_id: str,
    workspace_id: str | None,
    handler,
    token_callback,
    persistence_service: ConversationPersistenceService | None,
    start_time: float,
    request: ChatRequest,
    is_byok: bool,
    msg_type: str,
    log_prefix: str,
    timezone_str: str | None = None,
) -> AsyncGenerator[str, None]:
    """Handle a workflow exception: classify, retry-or-fail, persist, yield SSE events.

    This is an async generator that yields SSE event strings (``retry`` or
    ``error``).  Call it with ``async for event in handle_workflow_error(...): yield event``.

    ``workspace_id`` accepts ``None`` to guard against the case where the
    error occurred before the workspace was resolved.
    ``timezone_str`` is the resolved timezone; falls back to ``request.timezone``.
    """
    MAX_RETRIES = get_max_workflow_retries()

    # Release burst slot on error (setup errors before background task starts)
    await release_burst_slot(user_id)

    # Gather tracking data for persistence
    _per_call_records = (
        token_callback.per_call_records if token_callback else None
    )
    _tool_usage = handler.get_tool_usage() if handler else None
    _sse_events = handler.get_sse_events() if handler else None

    classification = classify_error(e)
    is_recoverable = classification["is_recoverable"]
    error_type = classification["error_type"]

    # Build metadata for persistence calls
    persist_metadata = {
        "msg_type": msg_type,
        "is_byok": is_byok,
    }
    if workspace_id is not None:
        persist_metadata["workspace_id"] = workspace_id
    # Prefer request.workspace_id when available (PTC sets it on the request)
    if hasattr(request, "workspace_id") and request.workspace_id:
        persist_metadata["workspace_id"] = request.workspace_id
    if hasattr(request, "locale") and request.locale:
        persist_metadata["locale"] = request.locale
    # Use the resolved timezone_str (validated/defaulted) when available,
    # falling back to the raw request field.
    _tz = timezone_str or getattr(request, "timezone", None)
    if _tz:
        persist_metadata["timezone"] = _tz

    if is_recoverable:
        tracker = WorkflowTracker.get_instance()
        retry_count = await tracker.increment_retry_count(thread_id)

        if retry_count > MAX_RETRIES:
            logger.error(
                f"[{log_prefix}] Max retries exceeded ({retry_count}/{MAX_RETRIES}) for "
                f"thread_id={thread_id}: {type(e).__name__}: {str(e)[:100]}"
            )

            if persistence_service:
                try:
                    error_msg = f"Max retries exceeded ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {str(e)}"
                    await persistence_service.persist_error(
                        error_message=error_msg,
                        errors=[error_msg],
                        execution_time=time.time() - start_time,
                        metadata=persist_metadata,
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                        sse_events=_sse_events,
                    )
                except Exception as persist_error:
                    logger.error(
                        f"[{log_prefix}] Failed to persist error: {persist_error}"
                    )

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
            logger.warning(
                f"[{log_prefix}] Recoverable error ({error_type}) for thread_id={thread_id} "
                f"(retry {retry_count}/{MAX_RETRIES}): "
                f"{type(e).__name__}: {str(e)[:100]}"
            )

            retry_data = {
                "message": "Temporary error occurred, you can retry or resume the workflow",
                "thread_id": thread_id,
                "auto_retry": True,
                "error_type": error_type,
                "error_class": type(e).__name__,
                "retry_count": retry_count,
                "max_retries": MAX_RETRIES,
            }
            yield f"event: retry\ndata: {json.dumps(retry_data)}\n\n"

            await qr_db.update_thread_status(thread_id, "interrupted")

    else:
        # Non-recoverable error
        logger.exception(f"[{log_prefix.replace('CHAT', 'ERROR')}] thread_id={thread_id}: {e}")

        if persistence_service:
            try:
                await persistence_service.persist_error(
                    error_message=str(e),
                    execution_time=time.time() - start_time,
                    metadata=persist_metadata,
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                    sse_events=_sse_events,
                )
            except Exception as persist_error:
                logger.error(f"[{log_prefix}] Failed to persist error: {persist_error}")

        if handler:
            error_event = handler._format_sse_event(
                "error",
                {
                    "thread_id": thread_id,
                    "error": str(e),
                    "type": "workflow_error",
                },
            )
            yield error_event
        else:
            error_event = json.dumps(
                {
                    "thread_id": thread_id,
                    "error": str(e),
                    "type": "workflow_error",
                }
            )
            yield f"event: error\ndata: {error_event}\n\n"
