"""Tests for ToolResultNormalizationMiddleware.

This middleware is the single chokepoint where every tool result becomes a
NUL-free string. It serves two concerns:

1. Type coercion — non-string results become strings so LLM APIs that require
   string ToolMessage content don't error out.
2. NUL stripping — a `\\x00` byte in tool output would break Postgres
   TEXT/JSONB binds, making affected threads permanently unresumable.

Both behaviors are pinned here.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.tool.result_normalization import (
    ToolResultNormalizationMiddleware,
)


@pytest.fixture
def mw() -> ToolResultNormalizationMiddleware:
    return ToolResultNormalizationMiddleware()


# ---------------------------------------------------------------------------
# _normalize_result — type coercion
# ---------------------------------------------------------------------------


class TestTypeCoercion:
    def test_string_passthrough(self, mw):
        assert mw._normalize_result("hello") == "hello"

    def test_none_becomes_empty_json_array(self, mw):
        assert mw._normalize_result(None) == "[]"

    def test_dict_becomes_json(self, mw):
        out = mw._normalize_result({"k": "v"})
        assert json.loads(out) == {"k": "v"}

    def test_list_becomes_json(self, mw):
        out = mw._normalize_result([1, "two", {"three": 3}])
        assert json.loads(out) == [1, "two", {"three": 3}]

    def test_other_type_via_str(self, mw):
        assert mw._normalize_result(42) == "42"

    def test_unicode_preserved(self, mw):
        # ensure_ascii=False — Chinese characters etc. survive intact.
        out = mw._normalize_result({"msg": "你好"})
        assert "你好" in out


# ---------------------------------------------------------------------------
# _normalize_result — NUL stripping
# ---------------------------------------------------------------------------


class TestNulStripping:
    def test_strips_nul_from_string(self, mw, caplog):
        out = mw._normalize_result("ok\x00bad")
        assert out == "okbad"
        # Warning fires so logs make NUL occurrences observable.
        assert any("Stripped NUL" in r.message for r in caplog.records)

    def test_strips_nul_inside_dict_value(self, mw):
        # Dict path goes through json.dumps first; the resulting string
        # contains the JSON `\\u0000` escape which we then strip.
        out = mw._normalize_result({"content": "stdout\x00"})
        # Either form is unacceptable in Postgres JSONB.
        assert "\\u0000" not in out
        assert "\x00" not in out

    def test_strips_nul_inside_list(self, mw):
        out = mw._normalize_result(["a\x00", "b"])
        assert "\\u0000" not in out
        assert "\x00" not in out

    def test_clean_string_does_not_emit_warning(self, mw, caplog):
        out = mw._normalize_result("clean output")
        assert out == "clean output"
        assert not any("Stripped NUL" in r.message for r in caplog.records)

    def test_multiple_nuls_all_stripped(self, mw):
        assert mw._normalize_result("\x00a\x00b\x00c\x00") == "abc"


# ---------------------------------------------------------------------------
# wrap_tool_call / awrap_tool_call — integration with ToolMessage
# ---------------------------------------------------------------------------


class TestWrapToolCall:
    def test_sync_path_strips_nul_in_toolmessage(self, mw):
        msg = ToolMessage(content="hello\x00world", tool_call_id="call-1")
        handler = MagicMock(return_value=msg)
        out = mw.wrap_tool_call(request=MagicMock(), handler=handler)
        assert out is msg
        assert msg.content == "helloworld"

    def test_sync_path_passthrough_for_non_toolmessage(self, mw):
        # If something other than a ToolMessage comes back, leave it alone.
        sentinel = object()
        handler = MagicMock(return_value=sentinel)
        assert mw.wrap_tool_call(request=MagicMock(), handler=handler) is sentinel

    @pytest.mark.asyncio
    async def test_async_path_strips_nul_in_toolmessage(self, mw):
        msg = ToolMessage(content="hello\x00world", tool_call_id="call-1")
        handler = AsyncMock(return_value=msg)
        out = await mw.awrap_tool_call(request=MagicMock(), handler=handler)
        assert out is msg
        assert msg.content == "helloworld"

    @pytest.mark.asyncio
    async def test_async_path_coerces_dict_then_strips(self, mw):
        # Tool returns a dict directly into ToolMessage.content (some tools do
        # this before LangChain wraps it). Middleware coerces to JSON string,
        # then strips the \\u0000 escape that the dict's NUL-bearing value
        # would have produced.
        msg = ToolMessage(content={"data": "stdout\x00"}, tool_call_id="call-2")
        handler = AsyncMock(return_value=msg)
        await mw.awrap_tool_call(request=MagicMock(), handler=handler)
        assert isinstance(msg.content, str)
        assert "\\u0000" not in msg.content
        assert "\x00" not in msg.content
