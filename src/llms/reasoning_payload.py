"""Reasoning-payload predicates and strippers over a message history.

Pure transforms with no middleware or graph dependency: the agent's request path
uses them to keep provider-bound reasoning inside its lineage, and
``ModelResilienceMiddleware`` uses them to strip everything after a provider has
already rejected a payload. Lives beside ``reasoning_lineage`` so the layer that
owns provider routing also owns the shape of what routing has to protect.
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage, AnyMessage

# Content-block types carrying provider-bound reasoning. ``reasoning`` covers
# both the standardized v1 block and the raw OpenAI Responses reasoning item.
REASONING_BLOCK_TYPES = frozenset({"thinking", "redacted_thinking", "reasoning"})

# Reasoning carried outside ``content``. The vendored ``ChatZai`` re-injects
# these into the outgoing payload, so clearing content alone is not enough.
REASONING_KWARG_KEYS = ("reasoning_content", "reasoning")


def is_reasoning_block(block: Any) -> bool:
    return isinstance(block, dict) and block.get("type") in REASONING_BLOCK_TYPES


def drop_if_empty(msg: AIMessage) -> AIMessage | None:
    """``None`` once nothing sendable is left — the single emptiness rule.

    Providers reject an assistant message with no content. Returning ``None`` is
    safe precisely because it has no tool call: anything paired with a
    ``ToolMessage`` keeps its calls and survives with empty content, which the
    provider formatters rebuild from them. ``invalid_tool_calls`` counts for
    exactly that reason — ``PatchToolCallsMiddleware`` answers those with a
    synthetic ``ToolMessage`` too, and the OpenAI formatter serializes them
    alongside the valid ones, so dropping their parent orphans a tool result.
    """
    return msg if (msg.content or msg.tool_calls or msg.invalid_tool_calls) else None


def strip_reasoning(msg: AIMessage) -> AIMessage | None:
    """Remove every reasoning payload from ``msg``; ``None`` means drop it."""
    update: dict[str, Any] = {}

    content = msg.content
    if isinstance(content, list):
        kept = [b for b in content if not is_reasoning_block(b)]
        if len(kept) != len(content):
            update["content"] = kept

    kwargs = msg.additional_kwargs or {}
    if any(kwargs.get(key) for key in REASONING_KWARG_KEYS):
        update["additional_kwargs"] = {
            k: v for k, v in kwargs.items() if k not in REASONING_KWARG_KEYS
        }

    if not update:
        return msg
    return drop_if_empty(msg.model_copy(update=update))


def strip_all_reasoning(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Drop every reasoning payload from a history, regardless of origin.

    The escalated-recovery lever: used after a provider has already rejected the
    request's reasoning, when correctness matters more than continuity. Returns
    the original list unchanged when there was nothing to strip, so callers can
    test identity to decide whether a retry is worth making.
    """
    result: list[AnyMessage] = []
    changed = False
    for msg in messages:
        if not isinstance(msg, AIMessage):
            result.append(msg)
            continue
        stripped = strip_reasoning(msg)
        if stripped is not msg:
            changed = True
        if stripped is not None:
            result.append(stripped)
    return result if changed else messages
