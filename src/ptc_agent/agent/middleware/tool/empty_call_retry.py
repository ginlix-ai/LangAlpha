"""Retry middleware for empty tool_calls with tool_use stop_reason.

Some LLM providers (observed on Anthropic-protocol coding-plan routes) may return
stop_reason="tool_use" but with an empty tool_calls list — the model intended
to call tools but the content was malformed/truncated and the SDK silently
dropped it.  LangGraph sees empty tool_calls and routes to __END__, stopping
the agent mid-task.

This middleware detects that mismatch, injects a remediation hint into the
message history so the model knows what went wrong, and retries the model call.
"""

import dataclasses
import logging

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.thinking_binding import is_bound_answer
from ptc_agent.agent.transcript.classify import EMPTY_CALL_RETRY_SOURCE

logger = logging.getLogger(__name__)

# stop_reason values that indicate the model intended to call tools
_TOOL_USE_STOP_REASONS = {"tool_use", "tool_calls"}

_REMEDIATION_HINT = (
    "Your previous response had stop_reason=tool_use but the tool_calls field "
    "was empty. This typically means the JSON in the tool-call arguments was "
    "malformed — most commonly an unescaped quote or control character inside "
    "a long string value, or the response was truncated mid-token.\n\n"
    "If you intended to call a tool, please retry now, paying special attention to:\n"
    "- Every `\"` inside a string value must be escaped as `\\\"`\n"
    "- Every `\\` inside a string value must be escaped as `\\\\`\n"
    "- Raw newlines and control characters are not allowed inside JSON string values\n"
    "- The full arguments object must be valid JSON end-to-end"
)


class EmptyToolCallRetryMiddleware(AgentMiddleware):
    """Retries when stop_reason indicates tool_use but tool_calls is empty."""

    def __init__(self, max_retries: int = 2):
        self.max_retries = max_retries

    def _should_retry(self, ai_msg: AIMessage) -> bool:
        meta = getattr(ai_msg, "response_metadata", {}) or {}
        stop = meta.get("stop_reason") or meta.get("finish_reason") or ""
        return (
            stop in _TOOL_USE_STOP_REASONS
            and not getattr(ai_msg, "tool_calls", None)
            and not getattr(ai_msg, "invalid_tool_calls", None)
        )

    def _log_retry(self, ai_msg: AIMessage, attempt: int) -> None:
        meta = getattr(ai_msg, "response_metadata", {}) or {}
        stop = meta.get("stop_reason") or meta.get("finish_reason")
        logger.warning(
            "[EmptyToolCallRetry] stop_reason=%s but tool_calls is empty, "
            "retrying (%d/%d)",
            stop,
            attempt + 1,
            self.max_retries,
        )

    def _inject_hint(self, request, ai_msg: AIMessage) -> list:
        """Append the broken AIMessage + a corrective HumanMessage to request.

        Replaces ``request.messages`` with a new list so we don't mutate any
        list the caller may still reference. Returns the two messages, which
        go into history ahead of the answer: it was produced behind them, and
        its thinking stays valid only while every later request sends them too.
        """
        injected = [
            ai_msg,
            HumanMessage(
                content=_REMEDIATION_HINT,
                additional_kwargs={"lc_source": EMPTY_CALL_RETRY_SOURCE},
            ),
        ]
        request.messages = list(request.messages) + injected
        return injected

    @staticmethod
    def _keep_injected(response, injected: list):
        """Only an answer carrying Claude's thinking needs them kept; any
        other answer goes into history alone, as it did."""
        if not injected or not is_bound_answer(response.result[0]):
            return response
        return dataclasses.replace(response, result=[*injected, *response.result])

    def wrap_model_call(self, request, handler):
        injected: list = []
        for attempt in range(1 + self.max_retries):
            response = handler(request)
            ai_msg = response.result[0]
            if not self._should_retry(ai_msg):
                return self._keep_injected(response, injected)
            self._log_retry(ai_msg, attempt)
            if not injected:
                injected = self._inject_hint(request, ai_msg)
        return self._keep_injected(response, injected)  # last response even if still broken

    async def awrap_model_call(self, request, handler):
        injected: list = []
        for attempt in range(1 + self.max_retries):
            response = await handler(request)
            ai_msg = response.result[0]
            if not self._should_retry(ai_msg):
                return self._keep_injected(response, injected)
            self._log_retry(ai_msg, attempt)
            if not injected:
                injected = self._inject_hint(request, ai_msg)
        return self._keep_injected(response, injected)  # last response even if still broken
