"""
Request and response models for Chat API endpoint.

This module defines Pydantic models for the /api/v1/threads/messages endpoints
that use the ptc-agent library for code execution in Daytona sandboxes.
"""

import copy
from typing import Any, Dict, List, Literal, Mapping, Optional, Union

from pydantic import BaseModel, Field

from src.server.models.additional_context import AdditionalContext


# =============================================================================
# HITL (Human-in-the-Loop) Models
# =============================================================================


class HITLDecision(BaseModel):
    """Decision for a single HITL action request."""

    type: Literal["approve", "reject"] = Field(
        description="Whether to approve or reject the action"
    )
    message: Optional[str] = Field(
        None,
        description="Feedback message, typically used when rejecting to explain why or request changes",
    )


class HITLResponse(BaseModel):
    """Response to a HITL interrupt containing decisions for each action request."""

    decisions: List[HITLDecision] = Field(
        description="List of decisions corresponding to each action request in the interrupt"
    )


def _format_rejection_message(user_feedback: Optional[str]) -> str:
    """Format a clear rejection message for the agent.

    Args:
        user_feedback: Optional feedback from the user explaining why they rejected.

    Returns:
        Formatted rejection message that clearly indicates the plan was rejected.
    """
    if user_feedback and user_feedback.strip():
        return f"User rejected the plan with the following feedback: {user_feedback.strip()}"
    return "User rejected the plan. No specific feedback was provided."


def serialize_hitl_response_map(hitl_response: Mapping[str, Any]) -> Dict[str, dict]:
    """Convert validated HITLResponse models into plain dicts for LangGraph resume.

    LangChain's HumanInTheLoopMiddleware expects `resume` payloads to be
    subscriptable dicts (e.g. `{"decisions": [...]}`), not Pydantic model
    instances.

    For rejection decisions, this function also formats the message to clearly
    indicate that the user rejected the plan and includes their feedback.
    """

    serialized: Dict[str, dict] = {}
    for interrupt_id, response in hitl_response.items():
        if hasattr(response, "model_dump"):
            response_dict = response.model_dump()  # type: ignore[call-arg]
        elif hasattr(response, "dict"):
            response_dict = response.dict()  # type: ignore[call-arg]
        elif isinstance(response, dict):
            response_dict = copy.deepcopy(
                response
            )  # Deep copy to avoid mutating nested structures
        else:
            raise TypeError(
                "Unsupported HITL response type: "
                f"interrupt_id={interrupt_id} type={type(response)!r}"
            )

        # Format rejection messages to be clear and include feedback
        if "decisions" in response_dict:
            for decision in response_dict["decisions"]:
                if decision.get("type") == "reject":
                    decision["message"] = _format_rejection_message(
                        decision.get("message")
                    )

        serialized[interrupt_id] = response_dict

    return serialized


def summarize_hitl_response_map(hitl_response: Mapping[str, Any]) -> Dict[str, Any]:
    """Summarize a HITL response map for persistence.

    Returns:
        Dict with keys:
            - feedback_action: "APPROVED" if all decisions approve; else "DECLINED"
            - content: concatenated reject messages (may be empty)
            - interrupt_ids: list of interrupt ids present
    """

    interrupt_ids = list(hitl_response.keys())
    reject_messages: List[str] = []
    any_reject = False

    for interrupt_id, response in hitl_response.items():
        if hasattr(response, "decisions"):
            decisions = getattr(response, "decisions")
        elif isinstance(response, dict):
            decisions = response.get("decisions") or []
        else:
            raise TypeError(
                "Unsupported HITL response type: "
                f"interrupt_id={interrupt_id} type={type(response)!r}"
            )

        for decision in decisions:
            if hasattr(decision, "type"):
                decision_type = getattr(decision, "type")
                message = getattr(decision, "message", None)
            elif isinstance(decision, dict):
                decision_type = decision.get("type")
                message = decision.get("message")
            else:
                raise TypeError(
                    "Unsupported HITL decision type: "
                    f"interrupt_id={interrupt_id} type={type(decision)!r}"
                )

            if decision_type == "reject":
                any_reject = True
                if message:
                    msg = str(message).strip()
                    if msg:
                        reject_messages.append(msg)

    feedback_action = "DECLINED" if any_reject else "APPROVED"
    content = "\n".join(reject_messages) if reject_messages else ""

    return {
        "feedback_action": feedback_action,
        "content": content,
        "interrupt_ids": interrupt_ids,
    }


# =============================================================================
# Common Message Types
# =============================================================================


class ContentItem(BaseModel):
    type: str = Field(..., description="The type of content (text, image, etc.)")
    text: Optional[str] = Field(None, description="The text content if type is 'text'")
    image_url: Optional[str] = Field(
        None, description="The image URL if type is 'image'"
    )


class ChatMessage(BaseModel):
    role: str = Field(
        ..., description="The role of the message sender (user or assistant)"
    )
    content: Union[str, List[ContentItem]] = Field(
        ...,
        description="The content of the message, either a string or a list of content items",
    )


# =============================================================================
# Chat Request/Response Models
# =============================================================================


class ChatRequest(BaseModel):
    """Request model for streaming chat endpoint."""

    # Agent mode selection
    agent_mode: Optional[Literal["ptc", "flash"]] = Field(
        default=None,
        description="Agent mode: 'ptc' (default) for sandbox-based execution, "
        "'flash' for lightweight, fast responses without sandbox",
    )

    # Identity fields (user_id comes from Bearer token JWT sub claim)
    workspace_id: Optional[str] = Field(
        default=None,
        description="Workspace identifier - required for 'full' mode, optional for 'flash' mode",
    )

    # Messages
    messages: List[ChatMessage] = Field(
        default_factory=list,
        description="History of messages between the user and the assistant",
    )

    # Agent options
    subagents_enabled: Optional[List[str]] = Field(
        default=None,
        description="List of subagent names to enable (default: from config)",
    )
    plan_mode: bool = Field(
        default=False,
        description="When True, agent must submit a plan for approval via submit_plan tool before execution",
    )

    # Interrupt/resume support (HITL)
    hitl_response: Optional[Dict[str, HITLResponse]] = Field(
        default=None,
        description="Structured HITL response: {interrupt_id: HITLResponse}. "
        "Use this to respond to interrupt events with approve/reject decisions.",
    )
    checkpoint_id: Optional[str] = Field(
        default=None, description="Specific checkpoint ID to resume from"
    )
    fork_from_turn: Optional[int] = Field(
        default=None,
        ge=0,
        description="Turn index to truncate app DB from on edit/regenerate. "
        "Deletes all queries/responses at turn_index >= this value before persisting.",
    )

    # Localization and context
    locale: Optional[str] = Field(
        default=None, description="Locale for output language, e.g., 'en-US' or 'zh-CN'"
    )
    timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone identifier (e.g., 'America/New_York', 'Asia/Shanghai')",
    )

    # Skill loading
    additional_context: Optional[List[AdditionalContext]] = Field(
        default=None,
        description="Additional context to be included. Supports: skills (skill instructions)",
    )

    # LLM selection (optional - defaults to agent_config.yaml setting)
    llm_model: Optional[str] = Field(
        default=None,
        description="LLM model name from models.json (e.g., 'minimax-m2.1', 'claude-sonnet-4-5')",
    )

    # Reasoning effort override (optional - defaults to model's configured level)
    # xhigh is honored only by Anthropic adaptive thinking (Opus 4.7+); other
    # providers clamp it to high in src/llms/reasoning.py.
    reasoning_effort: Optional[Literal["low", "medium", "high", "xhigh"]] = Field(
        default=None,
        description="Override reasoning effort for this request (low/medium/high/xhigh)",
    )

    fast_mode: Optional[bool] = Field(
        default=None,
        description="When True, enables priority service tier for faster responses (Codex OAuth models only)",
    )

    # Internal: query type override (e.g., "system" for report-back invocations)
    query_type: Optional[str] = Field(
        default=None,
        description="Override query_type for this request. Internal use only.",
    )

    # External thread identity (for channel integrations like Telegram, Slack)
    external_thread_id: Optional[str] = Field(
        default=None,
        description="Stable external thread identifier (e.g. 'chat_id:topic_id'). "
        "When provided with platform, langalpha resolves to an existing thread or creates a new one.",
    )
    # Format: lowercase prefix (a-z, underscore), optionally `:<UPPERCASE_TOKEN>`
    # where the suffix accepts A-Z 0-9 and `.` (for tickers like BRK.B).
    # Current namespace: 'web', 'market_view:<SYMBOL>', 'telegram', 'slack',
    # 'discord', 'feishu'. Reserved prefixes shouldn't be reused for other origins.
    platform: Optional[str] = Field(
        default=None,
        pattern=r"^[a-z_]+(:[A-Z][A-Z0-9.]*)?$",
        max_length=50,
        description="Origin/platform identifier. Examples: 'web', "
        "'market_view:AAPL', 'telegram', 'slack', 'discord', 'feishu'.",
    )


# =============================================================================
# Utility Request Models
# =============================================================================


class TTSRequest(BaseModel):
    text: str = Field(..., description="The text to convert to speech")
    voice_type: Optional[str] = Field(
        "BV700_V2_streaming", description="The voice type to use"
    )
    encoding: Optional[str] = Field("mp3", description="The audio encoding format")
    speed_ratio: Optional[float] = Field(1.0, description="Speech speed ratio")
    volume_ratio: Optional[float] = Field(1.0, description="Speech volume ratio")
    pitch_ratio: Optional[float] = Field(1.0, description="Speech pitch ratio")
    text_type: Optional[str] = Field("plain", description="Text type (plain or ssml)")
    with_frontend: Optional[int] = Field(
        1, description="Whether to use frontend processing"
    )
    frontend_type: Optional[str] = Field("unitTson", description="Frontend type")


class GeneratePodcastRequest(BaseModel):
    content: str = Field(..., description="The content of the podcast")


class SubagentMessageRequest(BaseModel):
    """Request model for sending a message to a running subagent."""

    content: str = Field(..., description="The instruction/message to send to the subagent")


