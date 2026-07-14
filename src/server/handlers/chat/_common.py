"""Shared helpers for chat handler modules (flash & PTC).

This module consolidates private helpers, turn-lifecycle wiring, and common
setup routines that are identical (or near-identical) between the flash and
PTC workflow handlers.  Keeping them in one place eliminates duplication and
ensures behavioural parity. Turn admission lives in ``admission``; error
classification and the terminal error funnel live in ``error_handling``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

from fastapi import HTTPException

from src.config.settings import (
    get_langsmith_metadata,
    get_langsmith_tags,
    get_locale_config,
    is_sse_event_log_enabled,
)
from src.server.app import setup
from src.server.database import conversation as qr_db
from src.server.models.chat import summarize_hitl_response_map
from src.server.utils.skill_context import (
    detect_slash_commands,
    parse_skill_contexts,
)
from src.tools.web.fetch import fetch_llm_client_override, fetch_model_override
from src.utils.tracking import TokenTrackingManager
from src.tools.decorators import ToolUsageTracker
from src.server.dependencies.usage_limits import release_burst_slot

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest
    from src.server.services.turn_lifecycle import ForkSpec

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Hard-coded logger name for backward-compat with existing log routing.
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


def inject_inline_reminders(
    messages: Optional[list[dict]],
    reminders: list[Optional[str]],
) -> None:
    """Append each present reminder to the last user message, in order.

    Falsy reminders are skipped. No-op when ``messages`` is falsy — callers pass
    ``None`` on HITL-resume / checkpoint-replay turns, where the appended list
    is discarded downstream.
    """
    if not messages:
        return
    for reminder in reminders:
        if reminder:
            _append_to_last_user_message(messages, reminder)


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


def _resolve_fork(*, request: ChatRequest) -> tuple[str, Optional[ForkSpec]]:
    """Compute query_type and the fork cleanup spec (truncation + checkpoint
    pin), shared by flash and PTC handlers.

    The cleanup itself executes INSIDE the START transaction (v4 2.4,
    ``start_run(fork=...)``): truncation, checkpoint pin, and the new attempt
    commit or roll back together, so a slot conflict or a duplicate
    retransmit can no longer leave a half-truncated thread behind — and the
    MAX+1 turn allocation still sees the post-fork row set.
    """
    from src.server.services.turn_lifecycle import ForkSpec

    is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)
    if request.query_type:
        query_type = request.query_type
    elif request.hitl_response:
        query_type = "resume_feedback"
    elif is_checkpoint_replay:
        query_type = "regenerate"
    else:
        query_type = "initial"

    fork = None
    if request.fork_from_turn is not None and request.checkpoint_id:
        fork = ForkSpec(
            from_turn=request.fork_from_turn,
            checkpoint_id=request.checkpoint_id,
            preserve_query_at_fork=is_checkpoint_replay,
        )

    return query_type, fork


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
# Shared helpers
# ---------------------------------------------------------------------------


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
    if request.platform:
        ensure_kwargs["platform"] = request.platform
    if request.external_thread_id:
        ensure_kwargs["external_id"] = request.external_thread_id
    await qr_db.ensure_thread_exists(**ensure_kwargs)


def _slash_text_target(content: Any) -> tuple[str, dict | None]:
    """Locate the leading-slash text for command detection in a user message.

    Returns ``(text, block)`` where ``block`` is the content-list element to
    rewrite when stripping the prefix (``None`` for plain-string content). User
    content is a block list when a supported attachment rewrites it
    (``inject_multimodal_context``) or the client sends a multi-part message;
    without scanning it, slash commands on those turns would be invisible. Only a
    block whose text starts with ``/`` is considered, so attachment-label blocks
    never false-match.
    """
    if isinstance(content, str):
        return content, None
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text", "") or ""
                if text.startswith("/"):
                    return text, block
    return "", None


def prepare_skill_contexts(
    messages: list[dict],
    request: ChatRequest,
    mode: str,
) -> list[dict]:
    """Resolve which skills this turn activates, for the agent to inject.

    Parses ``additional_context`` skill items and, as a fallback when none are
    present, detects a leading ``/command`` in the last user message (stripping
    the prefix in place). Returns plain ``{"name", "instruction"}`` dicts to thread
    through ``config["configurable"]["skill_contexts"]`` — ``SkillsMiddleware`` then
    loads the SKILL.md body once and dedups against bodies already live in the
    thread. No body loading or checkpoint reads happen here.
    """
    skill_contexts = parse_skill_contexts(request.additional_context)

    # Detect slash commands from message text (fallback for missing additional_context).
    # Handles both plain-string content and the content-block list a supported
    # attachment leaves behind, so `/cmd` + an image/PDF still activates the skill.
    if not skill_contexts and not request.hitl_response and messages:
        last_msg = messages[-1]
        msg_text, text_block = _slash_text_target(last_msg.get("content"))
        if msg_text:
            cleaned_text, detected = detect_slash_commands(msg_text, mode=mode)
            if detected:
                skill_contexts = detected
                if cleaned_text != msg_text:
                    if text_block is None:
                        last_msg["content"] = cleaned_text
                    else:
                        text_block["text"] = cleaned_text

    if skill_contexts:
        logger.info(
            f"[{mode.upper()}_CHAT] Skills requested: {[s.name for s in skill_contexts]}"
        )

    return [
        {"name": s.name, "instruction": s.instruction} for s in skill_contexts
    ]


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
    skill_contexts: list[dict] | None = None,
    skill_dirs: list[str] | None = None,
    run_id: str | None = None,
    turn_index: int | None = None,
) -> dict:
    """Build the LangGraph ``config`` dict shared by flash and PTC handlers.

    ``mode`` should be ``"flash"`` or ``"ptc"``.
    ``extra_configurable`` is an optional dict merged into ``configurable``.
    ``skill_contexts`` (+ ``skill_dirs``) are passed to ``SkillsMiddleware`` so it
    injects each requested skill's SKILL.md body once; omit on HITL/replay turns.
    ``run_id`` / ``turn_index`` are stamped into config metadata so the run's
    checkpoints self-describe (checkpoint-sourced replay can correlate turns by
    these instead of ordinal matching).
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
    if run_id is not None:
        langsmith_metadata["run_id"] = run_id
    if turn_index is not None:
        langsmith_metadata["turn_index"] = turn_index

    configurable: dict = {
        "thread_id": thread_id,
        "user_id": user_id,
        "workspace_id": workspace_id,
        "agent_mode": mode,
        "timezone": timezone_str,
    }
    if extra_configurable:
        configurable.update(extra_configurable)
    if skill_contexts:
        configurable["skill_contexts"] = skill_contexts
        if skill_dirs:
            configurable["skill_dirs"] = skill_dirs

    graph_config: dict = {
        "configurable": configurable,
        "recursion_limit": recursion_limit,
        "tags": langsmith_tags,
        "metadata": langsmith_metadata,
    }

    if request.checkpoint_id:
        graph_config["configurable"]["checkpoint_id"] = request.checkpoint_id

    # Token tracking callback. LangSmith tracing is handled by the SDK's
    # ambient auto-tracer activated via LANGSMITH_TRACING env var.
    if token_callback:
        graph_config["callbacks"] = [token_callback]

    return graph_config


async def dedup_retransmit_or_raise(request: ChatRequest) -> None:
    """Resolve a retransmitted request_key to its existing run — or pass.

    Must run under the admission lock BEFORE any steering, fork, or retry
    path can act on the duplicate: steering would inject the retransmit
    into the live run as a new message; a fork retransmit would truncate
    the very rows holding the key. START's unique index remains the
    race-safe backstop for keys that haven't produced a row yet.
    """
    if not request.request_key:
        return
    from src.server.database import turn_lifecycle as tl_db
    from src.server.services.turn_lifecycle import DuplicateRequestError

    existing = await tl_db.find_run_by_request_key(request.request_key)
    if existing is not None:
        raise DuplicateRequestError(existing)


async def resolve_retry_of(request: ChatRequest, thread_id: str):
    """Resolve and re-validate the attempt-chain predecessor for a retry.

    The /retry route validated latest-attempt + retryable-terminal before
    dispatch, but the generator may run later (dispatched flows) — re-check
    against live state so a stale retry can't chain onto the wrong run.
    Returns the predecessor row, or None when this isn't a retry.
    """
    if not request.retry_of_run_id:
        return None
    if request.fork_from_turn is not None:
        # A fork truncates; a retry chains. Combining them would truncate
        # first and then chain onto a deleted predecessor.
        raise HTTPException(
            status_code=400,
            detail="retry_of_run_id cannot be combined with fork_from_turn",
        )
    from src.server.database import turn_lifecycle as tl_db

    prev = await tl_db.get_run(request.retry_of_run_id)
    if prev is None or str(prev["conversation_thread_id"]) != thread_id:
        # Provenance is route-internal, so a vanished predecessor means a
        # fork/delete truncated it after route validation — a stale retry.
        # Structured 409 routes through the no-persist protocol branch;
        # an unstructured 404 here would hit mark_failed and could clobber
        # the newer turn's tracker state.
        latest = await tl_db.get_latest_attempt(thread_id)
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_retry",
                "message": "The run to retry no longer exists on this thread.",
                "latest_run_id": (
                    str(latest["conversation_response_id"]) if latest else None
                ),
                "latest_status": latest["status"] if latest else None,
            },
        )
    if prev["status"] != "error":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "not_retryable",
                "message": f"Run to retry is {prev['status']}, not a failed run.",
            },
        )
    latest = await tl_db.get_latest_attempt(thread_id)
    if latest is None or str(latest["conversation_response_id"]) != str(
        prev["conversation_response_id"]
    ):
        # Newer turns/attempts landed between route validation and this
        # generator running — retrying an older failure would fork history.
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_retry",
                "message": "The run to retry is no longer the latest attempt.",
                "latest_run_id": (
                    str(latest["conversation_response_id"]) if latest else None
                ),
                "latest_status": latest["status"] if latest else None,
            },
        )
    return prev


async def begin_turn(
    request: "ChatRequest",
    *,
    thread_id: str,
    run_id: str,
    msg_type: str,
    workspace_id: Optional[str],
    user_id: Optional[str],
    is_byok: bool,
    query_content,
    query_type,
    feedback_action,
    query_metadata: dict,
    fork,
    is_checkpoint_replay: bool,
    extra_run_metadata: Optional[dict] = None,
):
    """The one START-txn entrypoint: maps a ChatRequest onto the durable
    attempt chain — retries chain onto the failed run's turn, forked
    checkpoint replays pin their turn and reuse the preserved query row,
    everything else allocates MAX+1 — so the derivation can never drift
    between agent modes. ``fork`` (a ForkSpec) executes its truncation +
    checkpoint pin inside the same transaction."""
    from uuid import uuid4

    from src.server.services.turn_lifecycle import QuerySpec, TurnCoordinator

    retry_of = await resolve_retry_of(request, thread_id)
    return await TurnCoordinator.get_instance().start_turn(
        thread_id=thread_id,
        run_id=run_id,
        msg_type=msg_type,
        request_key=request.request_key,
        workspace_id=workspace_id,
        user_id=user_id,
        is_byok=is_byok,
        query=(
            None
            if is_checkpoint_replay
            else QuerySpec(
                query_id=str(uuid4()),
                content=query_content,
                query_type=query_type,
                feedback_action=feedback_action,
                metadata=query_metadata,
            )
        ),
        fork=fork,
        turn_index=(
            retry_of["turn_index"]
            if retry_of is not None
            else request.fork_from_turn
            if (fork is not None and is_checkpoint_replay)
            else None
        ),
        attempt_no=(retry_of["attempt_no"] + 1 if retry_of is not None else 1),
        retry_of_run_id=(
            str(retry_of["conversation_response_id"])
            if retry_of is not None
            else None
        ),
        # Durable on the row so the startup sweep can enqueue this run's
        # terminal hooks (burst release, watch clear) without any in-process
        # context surviving the crash.
        run_metadata={
            "user_id": user_id,
            "burst_slot_id": request.burst_slot_id,
            **(extra_run_metadata or {}),
        },
    )


async def release_and_fail_open(run_handle, *, user_id, burst_slot_id) -> None:
    """Death-path teardown for a generator dying between START and BTM
    handoff: release the burst slot and settle the open run (Phase 1 has no
    recovery scanner). fail_open_run shields its write internally, so a
    second cancel on the already-cancelled stream task cannot abort it."""
    from src.server.services.turn_lifecycle import TurnCoordinator

    await release_burst_slot(user_id, burst_slot_id)
    if run_handle is not None:
        await TurnCoordinator.get_instance().fail_open_run(
            run_handle, "client disconnected during setup", status="cancelled"
        )
