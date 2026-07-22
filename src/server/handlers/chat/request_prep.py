"""Chat request preparation — shared by the flash & PTC run generators.

Everything that turns a raw ``ChatRequest`` into a runnable graph invocation:
message normalization, HITL response translation, fork/timezone resolution,
skill contexts, steering setup, thread bootstrap, and graph config assembly.
Identical between the two run flavors, so it lives once here. The START
entry and retransmit dedup live in ``services.runs.admission``; error
classification and the terminal error funnel live in ``error_handling``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional


from src.config.settings import (
    get_langsmith_metadata,
    get_langsmith_tags,
    get_locale_config,
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

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.coordinator import ForkSpec

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Hard-coded logger name for backward-compat with existing log routing.
logger = logging.getLogger("src.server.handlers.chat_handler")

# Yielded exactly once by a dispatched workflow generator immediately after
# its START txn commits (v4 2.4c). The dispatch handler primes the generator
# to this marker before returning its 200 JSON response, making that response
# a durable receipt — the primer consumes it, so it never enters the SSE
# stream.
DISPATCH_STARTED_MARKER = "__dispatch_run_started__"


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
    from src.server.services.runs.coordinator import ForkSpec

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
    """Wire up steering tracking on a ``RunSSEProducer``.

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
