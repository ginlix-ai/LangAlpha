"""The calls a thread's newest assistant message made that no result answers yet."""

from __future__ import annotations

from collections.abc import Sequence

from langchain_core.messages import AIMessage, BaseMessage, ToolCall, ToolMessage


def pending_calls(messages: Sequence[BaseMessage]) -> tuple[int | None, list[ToolCall]]:
    """Where the newest assistant message is, and its calls no result after it answers."""
    at = next((i for i in range(len(messages) - 1, -1, -1) if isinstance(messages[i], AIMessage)), None)
    if at is None:
        return None, []
    answered = {m.tool_call_id for m in messages[at + 1 :] if isinstance(m, ToolMessage)}
    return at, [c for c in messages[at].tool_calls if c["id"] not in answered]
