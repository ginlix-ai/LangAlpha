"""The ``token_usage`` event: what the context ring shows of one model call.

One builder for the live emitters and for replay, which reads the same fields
off the checkpointed message, so a thread reads the same reloaded as it did
streaming. Its own module because replay's stored lines are keyed by the code
that shapes them (``replay.keys.projection_sources``).
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage

from src.llms.token_counter import extract_token_usage

#: The ``response_metadata`` key holding the summary trigger a model call ran
#: under. The trigger is per turn (the user's or the model's compaction
#: preset), and the checkpoint is all replay has of the turn, so it rides on
#: the response whose usage the ring shows.
CONTEXT_THRESHOLD_KEY = "context_threshold"

#: The ``response_metadata`` key holding the window of the model a call went
#: to. Stamped for the reason the threshold is: the model is the turn's, and
#: replay has only the checkpoint.
CONTEXT_LIMIT_KEY = "context_limit"


def stamp_usage(message: AIMessage, threshold: int, limit: int | None = None) -> dict[str, Any] | None:
    """Stamp ``message`` with what replay shows of its call, the
    ``threshold`` its context fills and its model's window ``limit`` where
    the model declares one, and return the event a live emitter sends
    (``token_usage_fields``). None, stamping nothing, when it reports no input."""
    if extract_token_usage(message).get("input_tokens", 0) <= 0:
        return None
    stamps: dict[str, Any] = {CONTEXT_THRESHOLD_KEY: threshold}
    if limit:
        stamps[CONTEXT_LIMIT_KEY] = limit
    message.response_metadata = {**(message.response_metadata or {}), **stamps}
    return token_usage_fields(message)


def context_threshold(message: Any) -> int | None:
    """The summary trigger stamped on ``message``, or None for a message
    saved before the stamp."""
    value = (getattr(message, "response_metadata", None) or {}).get(CONTEXT_THRESHOLD_KEY)
    return value if isinstance(value, int) and value > 0 else None


def token_usage_fields(message: Any) -> dict[str, Any] | None:
    """What a ``token_usage`` event states of the call that wrote
    ``message``: its tokens, how many of its input the provider read from
    cache, and what the call was stamped with. None when it reports no input."""
    usage = extract_token_usage(message)
    input_tokens = usage.get("input_tokens", 0)
    if input_tokens <= 0:
        return None
    output_tokens = usage.get("output_tokens", 0)
    fields: dict[str, Any] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cached_tokens": min(usage.get("cached_tokens", 0), input_tokens),
    }
    threshold = context_threshold(message)
    if threshold is not None:
        fields["threshold"] = threshold
    limit = (getattr(message, "response_metadata", None) or {}).get(CONTEXT_LIMIT_KEY)
    if isinstance(limit, int) and limit > 0:
        fields[CONTEXT_LIMIT_KEY] = limit
    return fields
