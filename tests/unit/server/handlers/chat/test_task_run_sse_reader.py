"""Locks for the v2-native per-task SSE reader (Phase 7 port).

The wire contract is v1-identical: content frames render exactly the string
``spill_task_record`` pre-rendered onto the v1 leg; control frames never
surface; ``run_end`` closes; the seq cursor resumes exclusively.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.task_run_sse_reader import (
    _record_to_v1_sse,
    stream_task_run_sse,
)

_MOD = "src.server.handlers.chat.task_run_sse_reader"


def _v1_prerender(record: dict, thread_id: str, task_id: str) -> str:
    """The exact rendering spill_task_record wrote to the v1 leg."""
    seq = int(record.get("seq") or 0)
    data = {
        **(record.get("data") or {}),
        "thread_id": thread_id,
        "agent": f"task:{task_id}",
    }
    return (
        f"id: {seq}\n"
        f"event: {record.get('event') or 'message_chunk'}\n"
        f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    )


class TestRenderParity:
    def test_byte_identical_to_v1_prerender(self):
        record = {
            "seq": 7,
            "event": "tool_calls",
            "data": {"content": "héllo — ünïcode", "n": 3, "f": 1.5},
            "agent_id": "ns-uuid",
            "ts": 123.4,
        }
        assert _record_to_v1_sse(record, "t1", "ab12") == _v1_prerender(
            record, "t1", "ab12"
        )

    def test_event_defaults_to_message_chunk_and_injections_win(self):
        record = {"seq": 1, "data": {"thread_id": "spoof", "agent": "spoof"}}
        out = _record_to_v1_sse(record, "t1", "ab12")
        assert out.startswith("id: 1\nevent: message_chunk\n")
        payload = json.loads(out.split("data: ", 1)[1])
        assert payload["thread_id"] == "t1"
        assert payload["agent"] == "task:ab12"


def _entry(fields: dict) -> tuple[bytes, dict]:
    return (b"1-1", {k.encode(): v.encode() for k, v in fields.items()})


def _content_entry(seq: int, event: str = "message_chunk") -> tuple[bytes, dict]:
    payload = json.dumps({"seq": seq, "event": event, "data": {"content": f"c{seq}"}})
    return (
        f"{seq}-0".encode(),
        {b"type": event.encode(), b"payload": payload.encode()},
    )


def _mock_cache(batches):
    """Cache whose xread returns each batch once, then blocks empty."""
    cache = MagicMock()
    cache.enabled = True
    seq = list(batches)

    async def xread(streams, block=None, count=None):
        if seq:
            return [(b"key", seq.pop(0))]
        return []

    cache.client.xread = xread
    return cache


async def _collect(gen, limit=50):
    out = []
    async for frame in gen:
        out.append(frame)
        if len(out) >= limit:
            break
    return out


class TestStreamLoop:
    @pytest.mark.asyncio
    async def test_control_frames_skipped_and_run_end_closes(self):
        lane_open = _entry(
            {
                "type": "lane_open",
                "payload": json.dumps({"task_run_id": "r1", "task_id": "ab12"}),
            }
        )
        run_end = _entry({"type": "run_end", "payload": json.dumps({"outcome": "completed"})})
        cache = _mock_cache([[lane_open, _content_entry(1), _content_entry(2), run_end]])
        with (
            patch(f"{_MOD}._resolve_task_run_id", AsyncMock(return_value="r1")),
            patch(f"{_MOD}.get_cache_client", return_value=cache),
        ):
            frames = await _collect(stream_task_run_sse("t1", "ab12"))
        assert [f.split("\n")[0] for f in frames] == ["id: 1", "id: 2"]
        assert not any("run_end" in f or "lane_open" in f for f in frames)

    @pytest.mark.asyncio
    async def test_seq_cursor_resumes_exclusively(self):
        entries = [_content_entry(s) for s in (1, 2, 3, 4)]
        run_end = _entry({"type": "run_end", "payload": "{}"})
        cache = _mock_cache([entries + [run_end]])
        with (
            patch(f"{_MOD}._resolve_task_run_id", AsyncMock(return_value="r1")),
            patch(f"{_MOD}.get_cache_client", return_value=cache),
        ):
            frames = await _collect(stream_task_run_sse("t1", "ab12", last_event_id=2))
        assert [f.split("\n")[0] for f in frames] == ["id: 3", "id: 4"]

    @pytest.mark.asyncio
    async def test_trimmed_head_yields_stream_gap(self):
        cache = _mock_cache([
            [_content_entry(10), _entry({"type": "run_end", "payload": "{}"})]
        ])
        with (
            patch(f"{_MOD}._resolve_task_run_id", AsyncMock(return_value="r1")),
            patch(f"{_MOD}.get_cache_client", return_value=cache),
        ):
            frames = await _collect(stream_task_run_sse("t1", "ab12", last_event_id=2))
        assert frames[0].startswith("event: stream_gap\n")
        assert json.loads(frames[0].split("data: ", 1)[1]) == {
            "expected_from": 3,
            "first_available": 10,
        }
        assert frames[1].startswith("id: 10\n")

    @pytest.mark.asyncio
    async def test_unresolved_task_falls_back_to_legacy_reader(self):
        async def legacy(thread_id, task_id, last_event_id=None):
            yield "legacy-frame"

        with (
            patch(f"{_MOD}._resolve_task_run_id", AsyncMock(return_value=None)),
            patch(
                "src.server.handlers.chat.legacy_task_sse_reader.stream_subagent_from_log",
                legacy,
            ),
        ):
            frames = await _collect(stream_task_run_sse("t1", "ab12"))
        assert frames == ["legacy-frame"]
