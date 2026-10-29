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

from src.llms.provider_compaction import without_blocks

# Content-block types carrying provider-bound reasoning. ``reasoning`` covers
# both the standardized v1 block and the raw OpenAI Responses reasoning item.
REASONING_BLOCK_TYPES = frozenset({"thinking", "redacted_thinking", "reasoning"})

# Reasoning carried outside ``content``. The vendored ``ChatZai`` and
# ``ChatOpenRouter`` re-inject these into the outgoing payload, so clearing
# content alone is not enough; OpenRouter's ``reasoning_details`` carry the
# upstream's signed or encrypted blocks.
REASONING_KWARG_KEYS = ("reasoning_content", "reasoning", "reasoning_details")

# Blocks whose ``id`` names the item on the server that issued it, not the call:
# a Responses ``function_call`` pairs with its output by ``call_id``. Another
# server cannot resolve that id and may check its shape, as OpenAI does when it
# rejects DashScope's ``msg_`` ids where it expects ``fc_``. Anthropic's
# ``tool_use.id`` is the call id itself, so it is not one of these.
ITEM_ID_BLOCK_TYPES = frozenset({"function_call"})

# Carries no reasoning: it only fills the place an API requires a reasoning
# block to hold, so nothing is invented and no other model's reasoning is sent.
EMPTY_THINKING_BLOCK = {"type": "thinking", "thinking": ""}


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


def strip_item_ids(msg: AIMessage) -> AIMessage:
    """Remove the ids ``msg``'s own server gave its items; calls still pair."""
    content = msg.content
    if not isinstance(content, list):
        return msg
    blocks = [
        {k: v for k, v in b.items() if k != "id"}
        if isinstance(b, dict) and b.get("type") in ITEM_ID_BLOCK_TYPES and "id" in b
        else b
        for b in content
    ]
    if blocks == content:
        return msg
    return msg.model_copy(update={"content": blocks})


def pad_tool_turn(msg: AIMessage) -> AIMessage:
    """Put an empty thinking block first on a tool-calling turn that has none.

    A call whose arguments failed to parse is still a tool-calling turn on the
    wire: its ``tool_use`` block stays in content and is sent with empty input.
    """
    if not (msg.tool_calls or msg.invalid_tool_calls):
        return msg
    content = msg.content
    if isinstance(content, list):
        if any(is_reasoning_block(b) for b in content):
            return msg
        blocks = list(content)
    else:
        blocks = [{"type": "text", "text": content}] if content else []
    return msg.model_copy(update={"content": [dict(EMPTY_THINKING_BLOCK), *blocks]})


def strip_all_reasoning(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Drop every reasoning payload from a history, regardless of origin.

    The escalated-recovery lever: used after a provider has already rejected the
    request's reasoning, when correctness matters more than continuity. Returns
    the original list unchanged when there was nothing to strip, so callers can
    test identity to decide whether a retry is worth making.
    """
    # A provider's signed summary goes too: it is bound to the provider as
    # the reasoning is, and its message reads the summary text without it.
    readable = without_blocks(messages)
    changed = readable is not None
    result: list[AnyMessage] = []
    for msg in readable or messages:
        if not isinstance(msg, AIMessage):
            result.append(msg)
            continue
        stripped = strip_reasoning(msg)
        if stripped is not msg:
            changed = True
        if stripped is not None:
            result.append(stripped)
    return result if changed else messages
