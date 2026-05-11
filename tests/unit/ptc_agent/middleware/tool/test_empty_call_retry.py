"""Tests for EmptyToolCallRetryMiddleware.

When the model returns stop_reason=tool_use but tool_calls is empty (typically
because the tool-call argument JSON was malformed and dropped upstream), the
middleware should:

1. Retry the model call up to max_retries times.
2. On the first retry, inject the failing AIMessage plus a corrective
   HumanMessage so the model gets a chance to see why it failed.
3. Not inject the hint repeatedly.
4. Stop retrying as soon as the model returns valid tool_calls.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.tool.empty_call_retry import (
    EmptyToolCallRetryMiddleware,
    _REMEDIATION_HINT,
)


def _make_request(messages):
    return SimpleNamespace(messages=list(messages))


def _empty_call_ai_msg():
    return AIMessage(
        content="",
        response_metadata={"stop_reason": "tool_use"},
        tool_calls=[],
    )


def _good_ai_msg():
    return AIMessage(
        content="",
        response_metadata={"stop_reason": "tool_use"},
        tool_calls=[
            {"name": "write_file", "args": {"file_path": "x.md", "content": "hi"}, "id": "1", "type": "tool_call"}
        ],
    )


def _resp(ai_msg):
    return SimpleNamespace(result=[ai_msg])


class TestEmptyToolCallRetry:
    def test_returns_immediately_on_valid_response(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=2)
        request = _make_request([HumanMessage(content="hello")])
        handler = MagicMock(return_value=_resp(_good_ai_msg()))

        result = mw.wrap_model_call(request, handler)

        assert handler.call_count == 1
        assert result.result[0].tool_calls
        # No hint injected on happy path.
        assert len(request.messages) == 1

    def test_injects_hint_on_first_retry(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=2)
        request = _make_request([HumanMessage(content="hello")])
        broken = _empty_call_ai_msg()
        good = _good_ai_msg()
        handler = MagicMock(side_effect=[_resp(broken), _resp(good)])

        result = mw.wrap_model_call(request, handler)

        assert handler.call_count == 2
        assert result.result[0].tool_calls
        # Hint is now in the request messages: original + broken AIMessage + HumanMessage hint.
        assert len(request.messages) == 3
        assert isinstance(request.messages[-1], HumanMessage)
        assert request.messages[-1].content == _REMEDIATION_HINT
        # The broken AIMessage was included so the model can see what it said.
        assert request.messages[-2] is broken

    def test_hint_injected_only_once_across_retries(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=3)
        request = _make_request([HumanMessage(content="hello")])
        broken1 = _empty_call_ai_msg()
        broken2 = _empty_call_ai_msg()
        good = _good_ai_msg()
        handler = MagicMock(side_effect=[_resp(broken1), _resp(broken2), _resp(good)])

        mw.wrap_model_call(request, handler)

        assert handler.call_count == 3
        # Only one hint message appended, not two — broken AIMessage from
        # attempt #1 plus one HumanMessage hint. The second broken AIMessage
        # does NOT re-trigger injection.
        hint_count = sum(
            1
            for m in request.messages
            if isinstance(m, HumanMessage) and m.content == _REMEDIATION_HINT
        )
        assert hint_count == 1

    def test_exhausts_retries_when_always_broken(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=2)
        request = _make_request([HumanMessage(content="hello")])
        handler = MagicMock(side_effect=[_resp(_empty_call_ai_msg())] * 3)

        result = mw.wrap_model_call(request, handler)

        # 1 initial + 2 retries = 3 calls.
        assert handler.call_count == 3
        # Last response is still the broken one.
        assert not result.result[0].tool_calls

    def test_does_not_retry_when_not_tool_use_stop_reason(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=2)
        request = _make_request([HumanMessage(content="hello")])
        end_msg = AIMessage(
            content="done",
            response_metadata={"stop_reason": "end_turn"},
            tool_calls=[],
        )
        handler = MagicMock(return_value=_resp(end_msg))

        mw.wrap_model_call(request, handler)

        assert handler.call_count == 1
        assert len(request.messages) == 1

    @pytest.mark.asyncio
    async def test_async_path_injects_hint(self):
        mw = EmptyToolCallRetryMiddleware(max_retries=2)
        request = _make_request([HumanMessage(content="hello")])
        broken = _empty_call_ai_msg()
        good = _good_ai_msg()
        handler = AsyncMock(side_effect=[_resp(broken), _resp(good)])

        result = await mw.awrap_model_call(request, handler)

        assert handler.await_count == 2
        assert result.result[0].tool_calls
        assert isinstance(request.messages[-1], HumanMessage)
        assert request.messages[-1].content == _REMEDIATION_HINT
