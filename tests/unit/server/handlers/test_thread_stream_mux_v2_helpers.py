"""Mux v2 pure helpers: cursor grammar, envelope shape, entry ordering.

The generator itself is verified on the wire (shadow diff at M6-D) like the
v1 mux; these lock the parts a refactor could silently bend.
"""

import json

import pytest
from fastapi import HTTPException

from src.server.handlers.chat.thread_stream_mux_v2 import (
    _entry_after,
    _envelope,
    _parse_main_entry,
    _parse_task_entry,
    _RunChan,
    parse_mux_cursors_v2,
)


def _chan(lane: str = "task:abc123") -> _RunChan:
    return _RunChan(
        run_id="run-1", lane=lane, stream_key=b"k", cursor=b"0"
    )


def test_cursor_grammar_roundtrip():
    parsed = parse_mux_cursors_v2(
        "run:0b8f0515-7902-410e-bc66-0c04432b3793#1784420444536-0"
    )
    assert parsed == {
        "0b8f0515-7902-410e-bc66-0c04432b3793": "1784420444536-0"
    }


@pytest.mark.parametrize(
    "raw",
    [
        "task:abc@run-1#1-0",  # v1 grammar
        "run:x#nonsense",
        "run:run!bad#1-0",
        "run:a#1-0,run:a#2-0",  # duplicate
    ],
)
def test_cursor_grammar_rejects(raw):
    with pytest.raises(HTTPException):
        parse_mux_cursors_v2(raw)


def test_envelope_carries_cursor_and_contract_fields():
    frame = _envelope(_chan(), "9-0", "message_chunk", '{"seq": 4}')
    lines = frame.split("\n")
    assert lines[0] == "id: run:run-1#9-0"
    assert lines[1] == "event: message_chunk"
    data = json.loads(lines[2][6:])
    assert data == {
        "run_id": "run-1",
        "seq": "9-0",
        "lane": "task:abc123",
        "type": "message_chunk",
        "payload": {"seq": 4},
    }


def test_envelope_null_payload():
    frame = _envelope(_chan("main"), "3-0", "run_end", "")
    data = json.loads(frame.split("\n")[2][6:])
    assert data["payload"] is None
    assert data["lane"] == "main"


def test_parse_main_entry_from_rendered_sse():
    fields = {b"event": b'id: 7\nevent: metadata\ndata: {"a": 1}\n\n'}
    assert _parse_main_entry(fields) == ("metadata", '{"a": 1}')


def test_parse_task_entry():
    fields = {b"type": b"lane_open", b"payload": b'{"task_id": "abc123"}'}
    assert _parse_task_entry(fields) == ("lane_open", '{"task_id": "abc123"}')


def test_entry_ordering_is_numeric_not_lexicographic():
    assert _entry_after(b"10-0", "9-0")
    assert not _entry_after(b"9-0", "10-0")
    assert _entry_after(b"1-2", "1-1")
    assert not _entry_after(b"1-1", "1-1")
