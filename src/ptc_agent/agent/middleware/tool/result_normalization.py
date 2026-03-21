"""Tool result normalization middleware.

Ensures all tool results are strings for LLM compatibility.
"""
import json
import logging
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

logger = logging.getLogger(__name__)


class ToolResultNormalizationMiddleware(AgentMiddleware):
    """Middleware that normalizes tool results to strings for LLM compatibility.

    This middleware ensures all tool results are returned as strings for LLM compatibility.
    Some tools may return Python objects (lists, dicts, None, etc.) that cause type errors
    when sent to LLM APIs that expect ToolMessage content to be a string.

    Common issue: OpenAI API returns BadRequestError when ToolMessage content is an array:
    "The parameter `input` specified in the request are not valid: `Mismatch type string with value array`"

    This middleware normalizes all tool results:
    - Strings: Pass through unchanged
    - Lists/Dicts: Convert to JSON string using json.dumps()
    - None: Convert to empty string ""
    - Other types: Convert to string using str()
    """

    def _normalize_result(self, result: Any) -> str:
        """Normalize tool result to a string.

        Args:
            result: The result from tool execution (any type)

        Returns:
            Normalized string representation
        """
        # Already a string - pass through
        if isinstance(result, str):
            return result

        # None - return empty JSON array
        if result is None:
            return json.dumps([])

        # Lists and dicts - convert to JSON string
        if isinstance(result, (list, dict)):
            try:
                return json.dumps(result, ensure_ascii=False)
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to JSON serialize tool result: {e}, falling back to str()")
                return str(result)

        # Other types - convert to string
        return str(result)

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
