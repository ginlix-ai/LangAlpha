"""Repair orphan Anthropic thinking blocks before the API call.

langchain-anthropic streams `signature_delta` events into content blocks shaped
`{"type": "thinking", "signature": "...", "index": N}` — no `thinking` field.
LangGraph checkpoints them, and the next turn fails with
`messages[i].content: missing field 'thinking'`. We inject `thinking: ""`
on blocks that have a non-empty signature; blocks lacking a signature are
unrecoverable corruption and are passed through so the API rejects them.
"""

from collections.abc import Awaitable, Callable
import logging
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, AnyMessage

logger = logging.getLogger(__name__)


def _sanitize_thinking_block(block: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if block.get("type") != "thinking":
        return block, False
    if isinstance(block.get("thinking"), str):
        return block, False
    signature = block.get("signature")
    if not (isinstance(signature, str) and signature):
        return block, False
    return {**block, "thinking": ""}, True


def _sanitize_message(msg: AnyMessage) -> tuple[AnyMessage, int]:
    if not isinstance(msg, AIMessage):
        return msg, 0
    content = msg.content
    if not isinstance(content, list):
        return msg, 0

    new_blocks: list[Any] = []
    repaired_count = 0
    changed = False

    for block in content:
        if isinstance(block, dict):
            new_block, block_changed = _sanitize_thinking_block(block)
            if block_changed:
                repaired_count += 1
                changed = True
            new_blocks.append(new_block)
        else:
            new_blocks.append(block)

    if not changed:
        return msg, 0

    return msg.model_copy(update={"content": new_blocks}), repaired_count


def _sanitize_messages(messages: list[AnyMessage]) -> tuple[list[AnyMessage], int]:
    result: list[AnyMessage] = []
    total = 0
    changed = False
    for msg in messages:
        new_msg, count = _sanitize_message(msg)
        total += count
        if new_msg is not msg:
            changed = True
        result.append(new_msg)
    return (result, total) if changed else (messages, 0)


class AnthropicThinkingSanitizerMiddleware(AgentMiddleware):
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        sanitized, repaired = _sanitize_messages(request.messages)
        if repaired:
            logger.warning(
                "[ThinkingSanitizer] repaired %d orphan thinking block(s)", repaired
            )
            request = request.override(messages=sanitized)
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        sanitized, repaired = _sanitize_messages(request.messages)
        if repaired:
            logger.warning(
                "[ThinkingSanitizer] repaired %d orphan thinking block(s)", repaired
            )
            request = request.override(messages=sanitized)
        return await handler(request)
