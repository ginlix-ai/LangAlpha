"""Tests for the pure reasoning-payload transforms."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.llms.reasoning_payload import strip_all_reasoning, strip_reasoning

TEXT = {"type": "text", "text": "answer"}
TOOL_USE = {"type": "tool_use", "id": "tu_1", "name": "lookup", "input": {}}
TOOL_CALL = {"name": "lookup", "args": {}, "id": "tu_1", "type": "tool_call"}
THINKING = {"type": "thinking", "thinking": "reasoned", "signature": "sig"}


def types_of(msg):
    return [b["type"] for b in msg.content if isinstance(b, dict)]


class TestWhatCountsAsReasoning:
    """Which payloads the stripper recognizes — identity means "left alone"."""

    @pytest.mark.parametrize(
        "block",
        [
            THINKING,
            {"type": "redacted_thinking", "data": "opaque"},
            {"type": "reasoning", "id": "rs_1", "encrypted_content": "opaque"},
        ],
    )
    def test_reasoning_block_types(self, block):
        assert types_of(strip_reasoning(AIMessage(content=[block, TEXT]))) == ["text"]

    def test_additional_kwargs_count_as_payload(self):
        msg = AIMessage(content="hi", additional_kwargs={"reasoning_content": "x"})
        assert strip_reasoning(msg).additional_kwargs == {}

    def test_empty_additional_kwargs_value_does_not(self):
        msg = AIMessage(content="hi", additional_kwargs={"reasoning_content": ""})
        assert strip_reasoning(msg) is msg

    @pytest.mark.parametrize(
        "msg",
        [
            AIMessage(content=[TEXT, TOOL_USE]),
            AIMessage(content="plain string"),
            HumanMessage(content="q"),
            ToolMessage(content="42", tool_call_id="tu_1"),
        ],
    )
    def test_no_payload(self, msg):
        assert strip_all_reasoning([msg]) == [msg]


class TestStripReasoning:
    def test_clean_message_keeps_identity(self):
        msg = AIMessage(content=[TEXT])
        assert strip_reasoning(msg) is msg

    def test_tool_call_survives_with_empty_content(self):
        """The formatters rebuild tool_use from tool_calls, so this stays sendable."""
        msg = AIMessage(content=[THINKING], tool_calls=[TOOL_CALL])
        out = strip_reasoning(msg)
        assert out is not None
        assert out.content == []
        assert len(out.tool_calls) == 1

    def test_reasoning_only_message_is_dropped(self):
        assert strip_reasoning(AIMessage(content=[THINKING])) is None

    def test_invalid_tool_call_survives_with_empty_content(self):
        """Dropping this would orphan the ToolMessage PatchToolCalls added for it.

        A malformed/truncated call lands in ``invalid_tool_calls``, never
        ``tool_calls``, and the OpenAI formatter serializes it all the same.
        """
        msg = AIMessage(
            content=[THINKING],
            invalid_tool_calls=[
                {
                    "name": "lookup",
                    "args": "{unparseable",
                    "id": "tu_1",
                    "error": "malformed",
                    "type": "invalid_tool_call",
                }
            ],
        )
        out = strip_reasoning(msg)
        assert out is not None
        assert out.content == []
        assert len(out.invalid_tool_calls) == 1

    def test_kwargs_cleared_but_other_keys_kept(self):
        msg = AIMessage(
            content=[TEXT], additional_kwargs={"reasoning_content": "x", "keep": 1}
        )
        assert strip_reasoning(msg).additional_kwargs == {"keep": 1}


class TestStripAllReasoning:
    def test_drops_every_origin(self):
        msgs = [
            AIMessage(content=[THINKING, TEXT], response_metadata={"x": 1}),
            AIMessage(content=[THINKING, TOOL_USE], tool_calls=[TOOL_CALL]),
        ]
        out = strip_all_reasoning(msgs)
        assert types_of(out[0]) == ["text"]
        assert types_of(out[1]) == ["tool_use"]

    def test_non_ai_messages_pass_through(self):
        msgs = [HumanMessage("q"), AIMessage(content=[THINKING, TEXT])]
        out = strip_all_reasoning(msgs)
        assert out[0] is msgs[0]

    def test_clean_history_returns_the_same_list(self):
        """Identity is the caller's signal that a retry would be pointless."""
        msgs = [HumanMessage("q"), AIMessage(content=[TEXT])]
        assert strip_all_reasoning(msgs) is msgs
