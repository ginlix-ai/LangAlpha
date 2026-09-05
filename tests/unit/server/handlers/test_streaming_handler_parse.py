"""Tests for `_parse_tool_args` in `src/server/services/runs/sse_producer.py`.

Frontier models occasionally emit tool-call argument JSON with an unescaped
quote inside a long string value, causing strict `json.loads` to raise
`Expecting ',' delimiter` and the call to be silently dropped. The fallback
through `json_repair` recovers the dominant case.
"""

import json

import pytest

from src.server.services.runs.sse_producer import RunSSEProducer, _parse_tool_args


class TestParseToolArgs:
    def test_strict_valid_json(self):
        parsed, err_repr, err_window = _parse_tool_args(
            '{"file_path": "results/note.md", "content": "hello"}'
        )
        assert parsed == {"file_path": "results/note.md", "content": "hello"}
        assert err_repr == ""
        assert err_window == ""

    def test_unescaped_quote_inside_content_repairs(self):
        # Dominant failure shape: literal `"` inside the content field that
        # the model didn't escape. Strict json.loads raises
        # "Expecting ',' delimiter".
        parsed, err_repr, err_window = _parse_tool_args(
            '{"file_path": "x.md", "content": "He said "hi" to me."}'
        )
        assert parsed is not None
        assert parsed.get("file_path") == "x.md"
        assert isinstance(parsed.get("content"), str)
        assert err_repr == ""
        assert err_window == ""

    def test_chinese_content_with_embedded_quote_repairs(self):
        # CJK content with an embedded unescaped quote — exercises multi-byte
        # code-point handling in json_repair.
        raw = (
            '{"file_path":"results/analysis.md",'
            '"content":"# 分析报告\\n标的"AMZN.US"表现良好。"}'
        )
        parsed, err_repr, err_window = _parse_tool_args(raw)
        assert parsed is not None
        assert parsed.get("file_path", "").endswith(".md")
        assert isinstance(parsed.get("content"), str)
        assert err_repr == ""

    def test_empty_string_returns_none_with_error(self):
        parsed, err_repr, err_window = _parse_tool_args("")
        assert parsed is None
        # Empty input is a JSONDecodeError, so err_repr carries the parser's message.
        assert "Expecting" in err_repr or "char 0" in err_repr
        assert err_window == ""

    def test_strict_valid_non_dict_is_rejected(self):
        # Bare strings/arrays/scalars parse via strict JSON but are not valid
        # tool-call arg shapes. err_repr should call out the shape mismatch
        # explicitly so logs aren't misleading.
        for raw, expected_type in (
            ('"just a string"', "str"),
            ("[1, 2, 3]", "list"),
            ("null", "NoneType"),
            ("42", "int"),
        ):
            parsed, err_repr, err_window = _parse_tool_args(raw)
            assert parsed is None, f"unexpected parse for {raw!r}"
            assert "non-dict" in err_repr, f"expected non-dict marker for {raw!r}, got {err_repr!r}"
            assert expected_type in err_repr
            assert err_window  # 200-char prefix carries the offending payload

    def test_repair_returning_non_dict_is_rejected(self):
        # json_repair will turn unstructured garbage into nested lists like
        # `[[["..."]]]`. Reject these so downstream tool dispatch never sees
        # a non-dict args shape. err_repr should carry the original
        # JSONDecodeError context (not the post-repair shape) since the
        # strict failure is what the caller cares about for diagnosis.
        parsed, err_repr, err_window = _parse_tool_args("{{{not json")
        assert parsed is None
        assert err_repr  # carries the JSONDecodeError message
        assert err_window  # carries bytes around the failure point


class TestParseErrorEmission:
    """A tool call whose args survive neither strict parse nor repair must
    still be emitted — the graph parses args independently and may run the
    call, so a dropped event leaves the stream without a card for a
    tool_call_result that arrives later."""

    def _producer(self):
        return RunSSEProducer(thread_id="t-parse", run_id="r-parse")

    @staticmethod
    def _tool_calls_payloads(events):
        return [
            json.loads(e.split("data: ", 1)[1].strip())
            for e in events
            if "event: tool_calls\n" in e
        ]

    @pytest.mark.asyncio
    async def test_unparseable_args_emit_marked_call_not_drop(self):
        from langchain_core.messages import AIMessageChunk

        producer = self._producer()
        producer.anthropic_tool_call_state[("agent", 0)] = {
            "name": "execute_code",
            "id": "call-broken",
            "args_accumulated": '"just a string"',  # valid JSON, non-dict: unrepairable
        }
        chunk = AIMessageChunk(content="", id="msg-1")
        chunk.response_metadata = {"stop_reason": "tool_use"}

        events = [e async for e in producer._process_message_chunk(chunk, "agent")]

        payloads = self._tool_calls_payloads(events)
        assert len(payloads) == 1
        payload = payloads[0]
        assert payload["args_parse_error"] is True
        assert payload["tool_calls"] == [
            {
                "name": "execute_code",
                "args": {},
                "id": "call-broken",
                "type": "tool_call",
            }
        ]
        # State cleared so the broken call doesn't leak across turns.
        assert not producer.anthropic_tool_call_state

    @pytest.mark.asyncio
    async def test_parseable_args_emit_without_marker(self):
        from langchain_core.messages import AIMessageChunk

        producer = self._producer()
        producer.anthropic_tool_call_state[("agent", 0)] = {
            "name": "execute_code",
            "id": "call-ok",
            "args_accumulated": '{"code": "print(1)"}',
        }
        chunk = AIMessageChunk(content="", id="msg-2")
        chunk.response_metadata = {"stop_reason": "tool_use"}

        events = [e async for e in producer._process_message_chunk(chunk, "agent")]

        payloads = self._tool_calls_payloads(events)
        assert len(payloads) == 1
        payload = payloads[0]
        assert "args_parse_error" not in payload
        assert payload["tool_calls"][0]["args"] == {"code": "print(1)"}
