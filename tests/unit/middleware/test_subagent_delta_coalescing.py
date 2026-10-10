"""A subagent's task stream joins streamed deltas the way the run stream does.

The registry holds one text or reasoning delta per task; every other append
sends it first, so frames keep the order they were captured in, and a record's
seq is assigned only when it is spilled, so the completeness gates still see
every seq up to the high-water.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessageChunk, ToolMessage

from ptc_agent.agent.middleware.background_subagent import redis_stream
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.run_executor import (
    _run_background_task,
)
from ptc_agent.agent.middleware.background_subagent.token_forwarder import (
    _SubagentTokenForwarder,
)
from src.utils.stream_coalescing import HEARTBEAT_EVENT_TYPE
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _CapturingRegistry(BackgroundTaskRegistry):
    def __init__(self) -> None:
        super().__init__(thread_id="thread-x")
        self.clock = _Clock()
        self._clock = self.clock
        self.spilled: list[dict] = []
        self.batches: list[list[int]] = []

    async def _spill_records_to_redis(self, task, records) -> None:
        self.batches.append([r["seq"] for r in records])
        self.spilled.extend(records)


def _text(content: str, content_type: str = "text") -> dict:
    return {
        "event": "message_chunk",
        "data": {
            "agent": "task:x",
            "id": "m1",
            "role": "assistant",
            "content": content,
            "content_type": content_type,
            "finish_reason": None,
        },
        "ts": 1.0,
    }


def _tool_calls() -> dict:
    """The shape ``SubagentEventCaptureMiddleware`` appends for a tool call."""
    return {
        "event": "tool_calls",
        "data": {
            "agent": "task:x",
            "id": "m1",
            "tool_calls": [{"name": "bash", "args": {"cmd": "ls"}, "id": "c1"}],
        },
        "ts": 2.0,
    }


def _shape(records) -> list[tuple]:
    return [
        (r["seq"], r["event"], r["data"].get("content"), r["data"].get("content_type"))
        for r in records
    ]


async def _task(registry):
    return await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )


@pytest.mark.asyncio
async def test_a_capture_append_sends_the_held_delta_first() -> None:
    registry = _CapturingRegistry()
    task = await _task(registry)

    for piece in ("Hel", "lo"):
        await registry.append_captured_event("tc1", _text(piece))
    assert registry.spilled == []
    # Nothing held has a seq yet: the gates count only what was spilled.
    assert task.captured_event_seq == 0

    await registry.append_captured_event("tc1", _tool_calls())
    await registry.append_captured_event("tc1", _text("done"))
    await registry.flush_held_delta("tc1")

    assert _shape(registry.spilled) == [
        (1, "message_chunk", "Hello", "text"),
        (2, "tool_calls", None, None),
        (3, "message_chunk", "done", "text"),
    ]
    # The joined delta and the frame behind it went out in one batch.
    assert registry.batches == [[1, 2], [3]]
    assert task.captured_event_seq == task.captured_event_count == 3
    # A joined record keeps its first piece's capture time.
    assert registry.spilled[0]["ts"] == 1.0


@pytest.mark.asyncio
async def test_text_and_reasoning_hold_separately() -> None:
    registry = _CapturingRegistry()
    await _task(registry)

    for event in (
        _text("hmm ", "reasoning"),
        _text("ok", "reasoning"),
        _text("Hi", "text"),
        _text(" there", "text"),
    ):
        await registry.append_captured_event("tc1", event)
    await registry.flush_held_delta("tc1")

    assert _shape(registry.spilled) == [
        (1, "message_chunk", "hmm ok", "reasoning"),
        (2, "message_chunk", "Hi there", "text"),
    ]


@pytest.mark.asyncio
async def test_the_window_releases_on_a_tick() -> None:
    registry = _CapturingRegistry()
    await _task(registry)

    await registry.append_captured_event("tc1", _text("a"))
    registry.clock.now = 0.05
    await registry.flush_held_delta("tc1", due_only=True)
    assert registry.spilled == []

    registry.clock.now = 0.1
    await registry.flush_held_delta("tc1", due_only=True)
    assert _shape(registry.spilled) == [(1, "message_chunk", "a", "text")]


@pytest.mark.asyncio
async def test_a_held_delta_goes_out_at_the_char_cap() -> None:
    registry = _CapturingRegistry()
    await _task(registry)

    for _ in range(6):
        await registry.append_captured_event("tc1", _text("x" * 100))
    assert [len(r["data"]["content"]) for r in registry.spilled] == [500]

    await registry.flush_held_delta("tc1")
    assert [len(r["data"]["content"]) for r in registry.spilled] == [500, 100]


@pytest.mark.asyncio
async def test_a_finish_goes_out_on_its_own() -> None:
    registry = _CapturingRegistry()
    await _task(registry)

    finish = _text("", "text")
    finish["data"]["finish_reason"] = "stop"
    await registry.append_captured_event("tc1", _text("bye"))
    await registry.append_captured_event("tc1", finish)

    assert [(r["data"]["content"], r["data"]["finish_reason"]) for r in registry.spilled] == [
        ("bye", None),
        ("", "stop"),
    ]


def _handler_leaving(registry, piece: str):
    """A subagent whose forwarder never sent its last delta."""

    async def handler(_request):
        await registry.append_captured_event("tc1", _text(piece))
        return ToolMessage(content="ok", tool_call_id="tc1", name="Task")

    return handler


@pytest.mark.asyncio
async def test_the_writer_sends_a_still_held_delta_before_the_terminal_frames(
    monkeypatch,
) -> None:
    registry = _CapturingRegistry()
    task = await _task(registry)
    order: list[str] = []

    async def _meta(t, status) -> None:
        order.append(f"meta after {len(registry.spilled)}")

    async def _sentinel(thread_id, t) -> None:
        order.append(f"sentinel after {len(registry.spilled)}")

    monkeypatch.setattr(registry, "write_task_meta", _meta)
    monkeypatch.setattr(redis_stream, "append_task_sentinel", _sentinel)
    monkeypatch.setattr(registry, "stamp_terminal_retention_for_task", AsyncMock())

    result = await _run_background_task(
        task,
        _handler_leaving(registry, "last words"),
        request=object(),
        tracker=PerCallTokenTracker(),
        label="test",
        registry=registry,
    )

    assert result["success"] is True
    assert _shape(registry.spilled) == [(1, "message_chunk", "last words", "text")]
    assert order == ["meta after 1", "sentinel after 1"]


@pytest.mark.asyncio
async def test_a_held_delta_that_fails_to_spill_tears_the_run(monkeypatch) -> None:
    """The outcome is judged after the last flush: a run whose final delta
    never reached its stream must not settle completed."""
    registry = _CapturingRegistry()
    task = await _task(registry)

    async def _circuit_opens(t, records) -> None:
        t.redis_write_failed = True

    monkeypatch.setattr(registry, "_spill_records_to_redis", _circuit_opens)
    monkeypatch.setattr(registry, "write_task_meta", AsyncMock())
    monkeypatch.setattr(redis_stream, "append_task_sentinel", AsyncMock())
    monkeypatch.setattr(registry, "stamp_terminal_retention_for_task", AsyncMock())

    result = await _run_background_task(
        task,
        _handler_leaving(registry, "last words"),
        request=object(),
        tracker=PerCallTokenTracker(),
        label="test",
        registry=registry,
    )

    assert result["success"] is False
    assert result["error_type"] == "transport_lost"


@pytest.mark.asyncio
async def test_a_kill_drops_the_held_delta() -> None:
    registry = _CapturingRegistry()
    task = await _task(registry)

    await registry.append_captured_event("tc1", _text("cut off"))
    task.mark_cancelled()
    await registry.flush_held_delta("tc1")
    assert registry.spilled == []

    # Unwind bookkeeping still lands, without the dropped delta ahead of it.
    await registry.append_event_for_task(
        task,
        {"event": "steering_returned", "data": {"messages": []}},
        terminal=True,
    )
    assert _shape(registry.spilled) == [(1, "steering_returned", None, None)]


@pytest.mark.asyncio
async def test_forwarder_ticks_release_on_time_and_finalize_flushes() -> None:
    registry = _CapturingRegistry()
    await _task(registry)
    forwarder = _SubagentTokenForwarder(registry, "tc1", "task:x")

    await forwarder.forward(AIMessageChunk(content="Hel", id="m1"))
    await forwarder.forward(AIMessageChunk(content="lo", id="m1"))
    await forwarder.tick()
    assert registry.spilled == []

    registry.clock.now = 0.2
    await forwarder.tick()
    await forwarder.forward(AIMessageChunk(content=" world", id="m1"))
    await forwarder.finalize()

    assert [r["data"]["content"] for r in registry.spilled] == ["Hello", " world"]


@pytest.mark.asyncio
async def test_a_batch_spills_under_one_lock_hold(monkeypatch) -> None:
    """A joined delta and the frame that released it carry consecutive seqs.
    Releasing the spill lock between them would let a concurrent append's
    later seq reach the stream first, and the id fence rejects the earlier
    one."""
    landed: list[int] = []

    async def _buffer(stream_key, *, event_id, **kwargs):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        landed.append(event_id)

    cache = MagicMock()
    cache.enabled = True
    cache.pipelined_event_buffer = AsyncMock(side_effect=_buffer)
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _task(registry)

    await registry.append_captured_event("tc1", _text("held"))
    await asyncio.gather(
        registry.append_captured_event("tc1", _tool_calls()),
        registry.append_captured_event("tc1", _tool_calls()),
    )

    assert landed == [1, 2, 3]
    assert not task.redis_write_failed


@pytest.mark.asyncio
async def test_the_forwarder_never_records_a_heartbeat() -> None:
    registry = _CapturingRegistry()
    await _task(registry)
    forwarder = _SubagentTokenForwarder(registry, "tc1", "task:x")

    await forwarder.forward_custom({"type": HEARTBEAT_EVENT_TYPE})
    await forwarder.finalize()
    assert registry.spilled == []
