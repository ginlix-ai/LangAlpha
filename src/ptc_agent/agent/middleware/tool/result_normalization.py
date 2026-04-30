"""Tool result normalization middleware.

Coerces tool results to strings for LLM compatibility and strips NUL bytes so
the content is safe to persist into Postgres TEXT/JSONB columns downstream.
"""
import json
import logging
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

logger = logging.getLogger(__name__)


class ToolResultNormalizationMiddleware(AgentMiddleware):
    """Normalize tool results to LLM-compatible strings and scrub NUL bytes.

    Two concerns, one chokepoint:

    1. **Type coercion** — some tools return Python objects (lists, dicts, None)
       that LLM APIs reject. OpenAI for example raises BadRequestError on array
       ToolMessage content: "Mismatch type string with value array". We coerce
       to a string here so every downstream consumer sees a uniform shape.

    2. **NUL safety** — sandbox stdout, file reads, web-fetch markdown, etc. can
       carry literal `\\x00` bytes. Postgres rejects these in TEXT (`cannot
       contain NUL`) and JSONB (`UntranslatableCharacter` on the `\\u0000`
       escape), making affected threads permanently unresumable. Stripping at
       this single point keeps the rest of the system — agent state, SSE
       events, LangSmith traces, msgpack checkpoints — clean.
    """

    def _normalize_result(self, result: Any) -> str:
        """Normalize tool result to a NUL-free string.

        Args:
            result: The result from tool execution (any type)

        Returns:
            Normalized string representation with NUL bytes stripped.
        """
        # Track whether `s` came from json.dumps so the `\u0000` escape check
        # only runs on serialized output. On the string-passthrough path the
        # literal six-char sequence `\u0000` is just text and Postgres TEXT
        # accepts it fine — stripping it there would mangle docs/LLM output
        # that happens to mention the escape.
        from_dumps = False

        if isinstance(result, str):
            s = result
        elif result is None:
            s = json.dumps([])
        elif isinstance(result, (list, dict)):
            try:
                s = json.dumps(result, ensure_ascii=False)
                from_dumps = True
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to JSON serialize tool result: {e}, falling back to str()")
                s = str(result)
        else:
            s = str(result)

        # Strip NUL bytes so the result is safe to persist to PG TEXT/JSONB.
        # Raw `\x00` is the only form that reaches a TEXT bind; the `\u0000`
        # escape is what json.dumps emits for an embedded NUL, which JSONB
        # then rejects. Both `in` checks are C-level so the clean path costs
        # one scan and no allocation.
        has_raw = "\x00" in s
        has_esc = from_dumps and "\\u0000" in s
        if has_raw or has_esc:
            logger.warning("Stripped NUL bytes from tool result")
            if has_raw:
                s = s.replace("\x00", "")
            if has_esc:
                s = s.replace("\\u0000", "")
        return s

    def wrap_tool_call(self, request, handler):
        """Synchronous tool result normalizer."""
        result = handler(request)

        # Normalize ToolMessage content
        if isinstance(result, ToolMessage):
            result.content = self._normalize_result(result.content)

        return result

    async def awrap_tool_call(self, request, handler):
        """Asynchronous tool result normalizer."""
        result = await handler(request)

        # Normalize ToolMessage content
        if isinstance(result, ToolMessage):
            result.content = self._normalize_result(result.content)

        return result
