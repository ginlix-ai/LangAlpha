"""Follow-up suggestion generation service.

Generates 1-5 follow-up questions after each assistant reply, respecting
the user's ``suggestion_enabled`` preference (default: enabled). Results
are appended to the ``sse_events`` JSONB column as a ``suggestions`` event
so the frontend can retrieve them via the dedicated suggestions API endpoint.

Model selection: reads the user's ``preferred_flash_model`` from
``other_preference``, falls back to ``agent_config.llm.flash``.
Credential resolution uses ``resolve_model_client`` (OAuth → BYOK →
platform fallback) so BYOK / custom provider keys are discovered. This
avoids ``resolve_llm_config`` → ``_cow`` → ``model_copy(deep=True)``
which breaks on non-picklable objects attached to the global config by
middleware/LangGraph during workflow execution.
"""

from __future__ import annotations

import logging
from typing import Any, List

from src.llms.api_call import make_api_call
from src.server.database.user import get_user_preferences
from src.server.handlers.chat.llm_config import resolve_model_client
from src.server.models.suggestion import SuggestionResponse

SUGGESTION_SYSTEM_PROMPT = """\
You are a helpful assistant that generates follow-up questions. Based on the \
assistant's last reply, suggest 1-3 concise, natural follow-up questions the \
user might want to ask next. The suggestions should:

- Be in the same language as the assistant's reply
- Be specific and directly relevant to the conversation
- Help the user dig deeper into the topic or explore related angles
- Be short (one sentence each, ideally under 15 words)
- NOT repeat questions the user has already asked
- NOT be generic ("tell me more") — they should be substantive

Return at most 5 suggestions. If the conversation is complete or there are \
no meaningful follow-ups, return fewer (or none)."""

logger = logging.getLogger(__name__)


async def generate_suggestions(
    *,
    user_id: str,
    agent_config: Any,
    assistant_text: str,
    call_logger: logging.Logger | None = None,
) -> List[str]:
    """Generate follow-up suggestions for the given assistant reply.

    Args:
        user_id: The authenticated user's ID.
        agent_config: Agent configuration (for system default flash model).
        assistant_text: The full text content of the last assistant reply.
        call_logger: Optional logger (defaults to module-level logger).

    Returns:
        A list of 0-5 suggestion strings. Empty if the feature is disabled
        or generation fails.
    """
    log = call_logger or logger

    # --- Check user preference -------------------------------------------------
    try:
        prefs = await get_user_preferences(user_id)
    except Exception:
        log.warning(
            "[SUGGESTION] Failed to load preferences, defaulting to enabled",
            exc_info=True,
        )
        prefs = None

    other_pref: dict = (prefs or {}).get("other_preference") or {}
    # Default to enabled when the key is absent.
    if other_pref.get("suggestion_enabled") is False:
        log.debug("[SUGGESTION] Disabled via user preference, skipping")
        return []

    # --- Resolve model ---------------------------------------------------------
    if not assistant_text.strip():
        return []

    # Use user's preferred flash model, fall back to system default.
    preferred = other_pref.get("preferred_flash_model")
    system_default = getattr(agent_config.llm, "flash", None) if agent_config else None
    model_name = preferred or system_default
    if not model_name:
        log.warning("[SUGGESTION] No flash model available, skipping")
        return []

    # --- Resolve credentials ---------------------------------------------------
    # Use resolve_model_client (OAuth → BYOK → platform fallback) instead of
    # create_llm so BYOK / custom provider keys stored in user preferences
    # are discovered. Avoids resolve_llm_config → _cow → model_copy(deep=True)
    # which can fail on mutated global config objects.
    try:
        resolved = await resolve_model_client(
            user_id,
            str(model_name),
            is_byok=True,
            allow_platform_fallback=True,
        )
        llm = resolved.client
        if llm is None:
            log.warning(
                "[SUGGESTION] No credentials resolved for model=%s (source=%s credential=%s)",
                model_name,
                resolved.source,
                resolved.credential_source,
            )
            return []
    except Exception:
        log.warning(
            "[SUGGESTION] Credential resolution failed for model=%s",
            model_name,
            exc_info=True,
        )
        return []

    # --- Call LLM --------------------------------------------------------------
    try:
        result = await make_api_call(
            llm,
            system_prompt=SUGGESTION_SYSTEM_PROMPT,
            user_prompt=_build_user_prompt(assistant_text),
            response_schema=SuggestionResponse,
        )
        suggestions: list[str] = [
            item.text.strip() for item in (result.suggestions or []) if item.text.strip()
        ]
        log.info(
            "[SUGGESTION] Generated %d suggestions for user=%s",
            len(suggestions),
            user_id,
        )
        return suggestions[:5]
    except Exception:
        log.warning(
            "[SUGGESTION] LLM call failed, returning empty suggestions",
            exc_info=True,
        )
        return []


def _build_user_prompt(assistant_text: str) -> str:
    """Build the user prompt for the suggestion-generation LLM call."""
    # Truncate to a reasonable context window for the lightweight flash model.
    truncated = assistant_text.strip()
    if len(truncated) > 4000:
        truncated = truncated[:4000] + "\n... (truncated)"
    return (
        "Here is the assistant's last reply in a conversation:\n\n"
        f"{truncated}\n\n"
        "Based on this reply, generate 1-3 follow-up questions the user "
        "might want to ask next."
    )


async def push_suggestions_to_redis(
    thread_id: str,
    run_id: str,
    suggestions: List[str],
) -> bool:
    """Push a ``suggestions`` SSE event to the per-run Redis stream.

    Called from the completion callback *before* ``tracker.mark_completed()``
    so the frontend SSE consumer receives the event as part of the live stream
    (no extra HTTP round-trip).

    Returns ``True`` if the event was pushed, ``False`` otherwise.
    """
    import json as _json

    from src.server.services.background_task_manager import stream_key
    from src.utils.cache.redis_cache import get_cache_client

    try:
        cache = get_cache_client()
    except Exception:
        logger.warning("[SUGGESTION] Redis unavailable, cannot push to stream")
        return False

    if not cache.enabled:
        return False

    data = {"suggestions": suggestions}
    sse_str = (
        f"id: 0\nevent: suggestions\n"
        f"data: {_json.dumps(data, ensure_ascii=False)}\n\n"
    )
    stream_k = stream_key(thread_id, run_id)
    try:
        await cache.client.xadd(stream_k, {"event": sse_str}, maxlen=10000)
        logger.debug(
            "[SUGGESTION] Pushed %d suggestions to Redis stream=%s",
            len(suggestions),
            stream_k,
        )
        return True
    except Exception:
        logger.warning(
            "[SUGGESTION] Failed to push to Redis stream=%s",
            stream_k,
            exc_info=True,
        )
        return False


def build_suggestion_sse_event(suggestions: List[str]) -> dict:
    """Build an SSE event dict for appending to ``sse_events``.

    Args:
        suggestions: The suggestion strings to embed.

    Returns:
        A dict with ``event`` and ``data`` keys suitable for appending to
        the ``conversation_responses.sse_events`` JSONB array.
    """
    return {
        "event": "suggestions",
        "data": {"suggestions": suggestions},
    }


def extract_assistant_text(sse_events: list) -> str:
    """Extract concatenated assistant text from SSE events.

    Iterates over ``message_chunk`` events and joins their ``content``
    fields. This is used as input for the suggestion-generation LLM call.
    """
    parts: list[str] = []
    for evt in sse_events:
        if isinstance(evt, dict) and evt.get("event") == "message_chunk":
            data = evt.get("data") or {}
            content = data.get("content")
            if isinstance(content, str) and content:
                parts.append(content)
    return "".join(parts)
