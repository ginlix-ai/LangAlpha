"""Tests for the pure reasoning-payload transforms."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.llms.reasoning_payload import (
    pad_tool_turn,
    strip_all_reasoning,
    strip_item_ids,
    strip_reasoning,
)

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

    def test_openrouter_reasoning_details_are_stripped(self):
        """ChatOpenRouter sends them back as the upstream's signed blocks."""
        msg = AIMessage(
            content=[TEXT],
            additional_kwargs={"reasoning_details": [{"type": "reasoning.text", "signature": "s"}]},
        )
        assert strip_reasoning(msg).additional_kwargs == {}


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


class TestStripItemIds:
    def test_function_call_loses_its_item_id_but_keeps_the_call_id(self):
        call = {
            "type": "function_call",
            "id": "msg_1",
            "call_id": "tu_1",
            "name": "lookup",
        }
        msg = AIMessage(content=[call], tool_calls=[TOOL_CALL])
        out = strip_item_ids(msg)
        assert out.content == [
            {"type": "function_call", "call_id": "tu_1", "name": "lookup"}
        ]
        assert out.tool_calls == msg.tool_calls

    @pytest.mark.parametrize(
        "content",
        [
            "plain",
            [TOOL_USE],  # tool_use.id is the call id itself
            [{"type": "function_call", "call_id": "tu_1", "name": "lookup"}],
        ],
    )
    def test_nothing_to_drop_keeps_identity(self, content):
        msg = AIMessage(content=content)
        assert strip_item_ids(msg) is msg


class TestPadToolTurn:
    PLACEHOLDER = {"type": "thinking", "thinking": ""}

    def test_tool_turn_without_reasoning_gets_the_placeholder_first(self):
        msg = AIMessage(content=[TEXT, TOOL_USE], tool_calls=[TOOL_CALL])
        out = pad_tool_turn(msg)
        assert out.content == [self.PLACEHOLDER, TEXT, TOOL_USE]
        assert out.tool_calls == msg.tool_calls

    def test_a_turn_whose_only_call_failed_to_parse_is_padded(self):
        broken = {"type": "tool_use", "id": "tu_1", "name": "lookup", "input": {}}
        invalid = {"name": "lookup", "args": "{", "id": "tu_1", "error": None}
        msg = AIMessage(content=[broken], invalid_tool_calls=[invalid])
        assert pad_tool_turn(msg).content == [self.PLACEHOLDER, broken]

    @pytest.mark.parametrize(
        ("content", "expected_rest"),
        [("checking", [{"type": "text", "text": "checking"}]), ("", [])],
    )
    def test_string_content(self, content, expected_rest):
        out = pad_tool_turn(AIMessage(content=content, tool_calls=[TOOL_CALL]))
        assert out.content == [self.PLACEHOLDER, *expected_rest]

    @pytest.mark.parametrize(
        "msg",
        [
            AIMessage(content=[THINKING, TOOL_USE], tool_calls=[TOOL_CALL]),
            AIMessage(content=[TEXT]),
        ],
        ids=["has-reasoning", "no-tool-calls"],
    )
    def test_left_alone(self, msg):
        assert pad_tool_turn(msg) is msg
