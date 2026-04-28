"""Tests for AnthropicThinkingSanitizerMiddleware.

Protects against the Anthropic 400 error:
  `messages.<i>.content.<j>.thinking.thinking: Field required`

which surfaces when the stream produces a signature-only thinking block and
that malformed block gets persisted into LangGraph state. The middleware
repairs such blocks by injecting `thinking: ""` before the outgoing API call.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.anthropic_thinking_sanitizer import (
    AnthropicThinkingSanitizerMiddleware,
    _sanitize_message,
    _sanitize_messages,
    _sanitize_thinking_block,
)


# ---------------------------------------------------------------------------
# _sanitize_thinking_block — individual block repair
# ---------------------------------------------------------------------------


class TestSanitizeBlock:
    def test_orphan_signature_block_gets_empty_thinking(self):
        block = {"type": "thinking", "signature": "sig-abc", "index": 0}
        out, changed = _sanitize_thinking_block(block)
        assert changed is True
        assert out["thinking"] == ""
        assert out["signature"] == "sig-abc"
        assert out["index"] == 0

    def test_well_formed_thinking_block_unchanged(self):
        block = {"type": "thinking", "thinking": "I am reasoning", "signature": "s"}
        out, changed = _sanitize_thinking_block(block)
        assert changed is False
        assert out is block

    def test_empty_string_thinking_not_repaired(self):
        block = {"type": "thinking", "thinking": "", "signature": "s"}
        out, changed = _sanitize_thinking_block(block)
        assert changed is False
        assert out is block

    def test_thinking_field_none_triggers_repair(self):
        block = {"type": "thinking", "thinking": None, "signature": "s"}
        out, changed = _sanitize_thinking_block(block)
        assert changed is True
        assert out["thinking"] == ""

    def test_thinking_non_string_triggers_repair(self):
        block = {"type": "thinking", "thinking": {"oops": 1}, "signature": "s"}
        out, changed = _sanitize_thinking_block(block)
        assert changed is True
        assert out["thinking"] == ""

    @pytest.mark.parametrize(
        "block",
        [
            {"type": "text", "text": "hello"},
            {"type": "tool_use", "id": "t1", "name": "x", "input": {}},
            {"type": "tool_result", "tool_use_id": "t1", "content": "ok"},
        ],
    )
    def test_non_thinking_blocks_untouched(self, block):
        out, changed = _sanitize_thinking_block(block)
        assert changed is False
        assert out is block

    @pytest.mark.parametrize(
        "block",
        [
            {"type": "thinking"},
            {"type": "thinking", "signature": ""},
            {"type": "thinking", "signature": None},
            {"type": "thinking", "thinking": None},
        ],
    )
    def test_no_signature_blocks_left_alone(self, block):
        out, changed = _sanitize_thinking_block(block)
        assert changed is False
        assert out is block


# ---------------------------------------------------------------------------
# _sanitize_message — whole-message scan
# ---------------------------------------------------------------------------


class TestSanitizeMessage:
    def test_non_ai_message_untouched(self):
        msg = HumanMessage(content=[{"type": "thinking", "signature": "s"}])
        out, count = _sanitize_message(msg)
        assert out is msg
        assert count == 0

    def test_str_content_untouched(self):
        msg = AIMessage(content="plain text")
        out, count = _sanitize_message(msg)
        assert out is msg
        assert count == 0

    def test_non_dict_entries_preserved(self):
        msg = AIMessage(
            content=["bare string", {"type": "thinking", "signature": "s"}]
        )
        out, count = _sanitize_message(msg)
        assert count == 1
        assert out.content[0] == "bare string"
        assert out.content[1]["thinking"] == ""

    def test_multiple_orphans_counted(self):
        msg = AIMessage(
            content=[
                {"type": "thinking", "signature": "a"},
                {"type": "text", "text": "ok"},
                {"type": "thinking", "signature": "b"},
            ]
        )
        out, count = _sanitize_message(msg)
        assert count == 2
        assert out.content[0]["thinking"] == ""
        assert out.content[1] == {"type": "text", "text": "ok"}
        assert out.content[2]["thinking"] == ""

    def test_well_formed_content_returns_same_message(self):
        msg = AIMessage(
            content=[
                {"type": "thinking", "thinking": "reasoned", "signature": "s"},
                {"type": "text", "text": "done"},
            ]
        )
        out, count = _sanitize_message(msg)
        assert out is msg
        assert count == 0


# ---------------------------------------------------------------------------
# _sanitize_messages — list-level identity + counting
# ---------------------------------------------------------------------------


class TestSanitizeMessages:
    def test_empty_list(self):
        out, total = _sanitize_messages([])
        assert out == []
        assert total == 0

    def test_no_changes_returns_same_list_identity(self):
        msgs = [AIMessage(content="hi"), HumanMessage(content="hey")]
        out, total = _sanitize_messages(msgs)
        assert out is msgs
        assert total == 0

    def test_repairs_across_messages(self):
        msgs = [
            AIMessage(content=[{"type": "thinking", "signature": "a"}]),
            HumanMessage(content="question"),
            AIMessage(
                content=[
                    {"type": "thinking", "signature": "b"},
                    {"type": "text", "text": "answer"},
                ]
            ),
        ]
        out, total = _sanitize_messages(msgs)
        assert total == 2
        assert out is not msgs
        assert out[0].content[0]["thinking"] == ""
        assert out[1] is msgs[1]  # human message passes through by identity
        assert out[2].content[0]["thinking"] == ""


# ---------------------------------------------------------------------------
# Middleware wrapper — override / handler contract
# ---------------------------------------------------------------------------


class TestMiddlewareAsync:
    @pytest.mark.asyncio
    async def test_repairs_and_calls_override(self):
        mw = AnthropicThinkingSanitizerMiddleware()
        req = MagicMock()
        req.messages = [AIMessage(content=[{"type": "thinking", "signature": "s"}])]
        req.override.return_value = req
        handler = AsyncMock(return_value="response")

        result = await mw.awrap_model_call(req, handler)

        req.override.assert_called_once()
        handler.assert_awaited_once_with(req)
        assert result == "response"

    @pytest.mark.asyncio
    async def test_noop_skips_override(self):
        mw = AnthropicThinkingSanitizerMiddleware()
        req = MagicMock()
        req.messages = [AIMessage(content="plain text")]
        handler = AsyncMock(return_value="response")

        result = await mw.awrap_model_call(req, handler)

        req.override.assert_not_called()
        handler.assert_awaited_once_with(req)
        assert result == "response"


class TestMiddlewareSync:
    def test_repairs_and_calls_override(self):
        mw = AnthropicThinkingSanitizerMiddleware()
        req = MagicMock()
        req.messages = [AIMessage(content=[{"type": "thinking", "signature": "s"}])]
        req.override.return_value = req
        handler = MagicMock(return_value="response")

        result = mw.wrap_model_call(req, handler)

        req.override.assert_called_once()
        handler.assert_called_once_with(req)
        assert result == "response"

    def test_noop_skips_override(self):
        mw = AnthropicThinkingSanitizerMiddleware()
        req = MagicMock()
        req.messages = [AIMessage(content="plain text")]
        handler = MagicMock(return_value="response")

        result = mw.wrap_model_call(req, handler)

        req.override.assert_not_called()
        handler.assert_called_once_with(req)
        assert result == "response"
