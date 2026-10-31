"""Tests for the pure reasoning-payload transforms."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.llms.reasoning_payload import (
    is_unfinished_reply,
    pad_tool_turn,
    strip_all_reasoning,
    strip_item_ids,
    strip_reasoning,
    strip_reasoning_references,
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


# What a Responses stream leaves when the server closes it after the first item
# started: the reasoning item's id, none of its content.
BARE_REASONING = {"type": "reasoning", "id": "rs_cut", "summary": [], "index": 0}
SEALED_REASONING = {"type": "reasoning", "id": "rs_done", "summary": [], "encrypted_content": "opaque"}


class TestUnfinishedReply:
    """A Responses reply is finished once its terminal event set ``status``."""

    def test_a_reply_cut_before_its_terminal_event(self):
        msg = AIMessage(content=[BARE_REASONING], response_metadata={"id": "resp_1", "model_provider": "openai"})
        assert is_unfinished_reply(msg)

    @pytest.mark.parametrize(
        "terminal",
        [{"status": "completed"}, {"status": "incomplete"}, {"object": "response", "created_at": 1.0}],
        ids=["completed", "incomplete", "a server leaving out status"],
    )
    def test_a_reply_its_terminal_event_reached(self, terminal):
        msg = AIMessage(content=[SEALED_REASONING], response_metadata={"id": "resp_1", **terminal})
        assert not is_unfinished_reply(msg)

    @pytest.mark.parametrize(
        "metadata",
        [
            {},
            {"id": "chatcmpl-1"},
            {"id": "msg_01abc", "stop_reason": "end_turn"},
            {"id": "gen-17"},
        ],
    )
    def test_replies_from_other_apis_never_count(self, metadata):
        assert not is_unfinished_reply(AIMessage(content="answer", response_metadata=metadata))

    def test_only_a_model_reply_counts(self):
        assert not is_unfinished_reply(HumanMessage("q"))


class TestStripReasoningReferences:
    """Only reasoning sent by id alone goes; everything else is kept."""

    def test_drops_the_bare_item_and_keeps_the_sealed_one(self):
        earlier = AIMessage(content=[SEALED_REASONING, TEXT], id="a-1")
        cut = AIMessage(content=[BARE_REASONING], id="a-2")
        later = AIMessage(content=[THINKING, TEXT], id="a-3")

        result = strip_reasoning_references([HumanMessage("q"), earlier, cut, later])

        assert [getattr(m, "id", None) for m in result] == [None, "a-1", "a-3"]
        assert result[1] is earlier
        assert result[2] is later

    def test_a_message_keeping_other_content_survives(self):
        msg = AIMessage(content=[BARE_REASONING, TEXT])
        (result,) = strip_reasoning_references([msg])
        assert types_of(result) == ["text"]

    def test_returns_the_same_list_when_nothing_is_bare(self):
        messages = [HumanMessage("q"), AIMessage(content=[SEALED_REASONING, TEXT]), AIMessage("plain")]
        assert strip_reasoning_references(messages) is messages


class TestAgainstTheOpenAIClient:
    """The predicate reads metadata langchain-openai builds from the stream, so
    an upgrade that stopped copying the terminal event's fields would make every
    Responses reply look unfinished. Streamed through the real client here."""

    RESPONSE = {"id": "resp_1", "object": "response", "created_at": 1.0, "status": "in_progress", "model": "gpt-test", "output": []}
    ITEM = {"type": "reasoning", "id": "rs_1", "summary": []}
    SEALED = {**ITEM, "encrypted_content": "opaque"}
    USAGE = {
        "input_tokens": 1,
        "output_tokens": 1,
        "total_tokens": 2,
        "input_tokens_details": {"cached_tokens": 0},
        "output_tokens_details": {"reasoning_tokens": 1},
    }

    def _events(self, *, finished):
        events = [
            {"type": "response.created", "sequence_number": 0, "response": self.RESPONSE},
            {"type": "response.output_item.added", "sequence_number": 1, "output_index": 0, "item": self.ITEM},
        ]
        if finished:
            events += [
                {"type": "response.output_item.done", "sequence_number": 2, "output_index": 0, "item": self.SEALED},
                {
                    "type": "response.completed",
                    "sequence_number": 3,
                    "response": {**self.RESPONSE, "status": "completed", "output": [self.SEALED], "usage": self.USAGE},
                },
            ]
        return events

    async def _streamed(self, *, finished):
        import json

        import httpx
        from langchain_openai import ChatOpenAI

        body = "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in self._events(finished=finished))
        transport = httpx.MockTransport(
            lambda _request: httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body.encode())
        )
        llm = ChatOpenAI(
            model="gpt-test",
            api_key="test",
            output_version="responses/v1",
            use_responses_api=True,
            streaming=True,
            http_async_client=httpx.AsyncClient(transport=transport),
        )
        reply = None
        async for chunk in llm.astream([HumanMessage("q")]):
            reply = chunk if reply is None else reply + chunk
        return reply

    @pytest.mark.asyncio
    async def test_a_stream_that_reached_its_terminal_event(self):
        assert not is_unfinished_reply(await self._streamed(finished=True))

    @pytest.mark.asyncio
    async def test_a_stream_closed_before_it(self):
        reply = await self._streamed(finished=False)
        assert is_unfinished_reply(reply)
        assert [b["type"] for b in reply.content] == ["reasoning"]
