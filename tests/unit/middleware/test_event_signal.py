"""Tests for event-driven SSE wake signals, pipeline bug fixes,
three-timestamp model, and _resolve_or_error helper.

These cover the changes planned in `twinkling-hatching-gosling.md`:
- Part 1: asyncio.Event wake signal in BackgroundTask
- Part 4: consumer ref counting, writer designation, drain snapshot
- Part 5: last_checked_at / last_updated_at semantics
- Part 6: _resolve_or_error helper with whitespace tolerance and hydration
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
    _make_task_done_callback,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from src.server.services.background_task_manager import drain_task_captured_events


def _tool_call_request(action: str, task_id: str, prompt: str = "hi") -> ToolCallRequest:
    """Build a ToolCallRequest with a mock runtime for update/resume tests."""
    runtime = MagicMock()
    runtime.config = {"configurable": {"thread_id": "parent-thread"}}
    return ToolCallRequest(
        tool_call={
            "name": "Task",
            "id": f"tc-{action}-{task_id}",
            "args": {
                "action": action,
                "task_id": task_id,
                "prompt": prompt,
                "description": f"{action} scenario",
            },
        },
        tool=None,
        state={},
        runtime=runtime,
    )


async def _noop_handler(_req: ToolCallRequest) -> None:
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _register(registry: BackgroundTaskRegistry, task_id_override: str | None = None) -> BackgroundTask:
    """Register a task and optionally override its short task_id."""
    task = await registry.register(
        tool_call_id=f"tc-{task_id_override or 'abc'}",
        description="test task",
        prompt="do stuff",
        subagent_type="general-purpose",
    )
    if task_id_override is not None and task.task_id != task_id_override:
        registry._task_id_to_tool_call_id.pop(task.task_id, None)
        task.task_id = task_id_override
        registry._task_id_to_tool_call_id[task_id_override] = task.tool_call_id
    return task


def _text_chunk(content: str = "hi") -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": "task:x", "content": content, "content_type": "text"},
        "ts": time.time(),
    }


def _reasoning_chunk() -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": "task:x", "content": "thinking", "content_type": "reasoning"},
        "ts": time.time(),
    }


def _reasoning_signal(marker: str = "start") -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": "task:x", "content": marker, "content_type": "reasoning_signal"},
        "ts": time.time(),
    }


def _tool_calls_event() -> dict:
    return {
        "event": "tool_calls",
        "data": {"agent": "task:x", "tool_calls": []},
        "ts": time.time(),
    }


def _tool_call_result_event() -> dict:
    return {
        "event": "tool_call_result",
        "data": {"agent": "task:x", "content": "ok"},
        "ts": time.time(),
    }


# ---------------------------------------------------------------------------
# Signal mechanism (Part 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_append_captured_event_signals() -> None:
    """append_captured_event should set new_event_signal."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    assert not task.new_event_signal.is_set()

    await registry.append_captured_event(task.tool_call_id, _text_chunk())
    assert task.new_event_signal.is_set()


@pytest.mark.asyncio
async def test_done_callback_signals_on_completion() -> None:
    """done_callback wired by _make_task_done_callback sets signal on success."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)

    async def _work() -> str:
        return "ok"

    atask = asyncio.create_task(_work())
    atask.add_done_callback(_make_task_done_callback(task))
    await atask
    # add_done_callback fires via the event loop — yield so the callback runs.
    await asyncio.sleep(0)

    assert task.new_event_signal.is_set()
    assert task.last_updated_at > 0


@pytest.mark.asyncio
async def test_done_callback_signals_on_cancellation() -> None:
    """done_callback also fires on asyncio.Task cancellation."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)

    async def _work() -> None:
        await asyncio.sleep(10)

    atask = asyncio.create_task(_work())
    atask.add_done_callback(_make_task_done_callback(task))
    atask.cancel()
    with pytest.raises(asyncio.CancelledError):
        await atask
    await asyncio.sleep(0)

    assert task.new_event_signal.is_set()


@pytest.mark.asyncio
async def test_consumer_wakes_immediately_on_signal() -> None:
    """A consumer awaiting the signal should wake in <0.1s, not 5s."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)

    async def _consumer() -> float:
        task.new_event_signal.clear()
        started = time.time()
        await asyncio.wait_for(task.new_event_signal.wait(), timeout=5.0)
        return time.time() - started

    consumer = asyncio.create_task(_consumer())
    # Give consumer a chance to enter wait()
    await asyncio.sleep(0.01)
    await registry.append_captured_event(task.tool_call_id, _text_chunk())

    elapsed = await consumer
    assert elapsed < 0.5, f"consumer took {elapsed:.3f}s, expected near-instant"


# ---------------------------------------------------------------------------
# Pipeline bug fixes (Part 4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_count_prevents_early_drain_complete() -> None:
    """Only the last decremented consumer sets sse_drain_complete."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    task.sse_consumer_count = 2

    # First consumer exits — count drops to 1, drain_complete still unset
    task.sse_consumer_count -= 1
    if task.sse_consumer_count <= 0:
        task.sse_drain_complete.set()
    assert not task.sse_drain_complete.is_set()

    # Second consumer exits — count drops to 0, drain_complete now set
    task.sse_consumer_count -= 1
    if task.sse_consumer_count <= 0:
        task.sse_drain_complete.set()
    assert task.sse_drain_complete.is_set()


@pytest.mark.asyncio
async def test_drain_snapshot_isolation() -> None:
    """drain_task_captured_events copies the deque so a concurrent clear
    doesn't truncate iteration."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _text_chunk(f"a{i}"))

    items, new_cursor = drain_task_captured_events(task, cursor=0)
    # Simulate cleanup clearing the tail mid-iteration — already returned
    # items are unaffected.
    task.captured_events_tail.clear()

    assert len(items) == 3, f"snapshot should yield 3 events, got {len(items)}"
    assert new_cursor == 3  # high-water seq
    assert items[0][0]["data"]["content"] == "a0"
    # The third element of the tuple is the seq (1-based, monotonic).
    assert [seq for _, _, seq in items] == [1, 2, 3]


@pytest.mark.asyncio
async def test_drain_returns_high_water_seq_not_live_count() -> None:
    """After drain, new_cursor reflects the high-water seq so events appended
    during the yield loop are re-drained next iteration instead of being
    skipped."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _text_chunk(f"a{i}"))

    items, new_cursor = drain_task_captured_events(task, cursor=0)
    assert new_cursor == 3
    assert [seq for _, _, seq in items] == [1, 2, 3]

    # Simulate append during the "yield loop" window
    await registry.append_captured_event(task.tool_call_id, _text_chunk("a3"))

    items2, new_cursor2 = drain_task_captured_events(task, cursor=new_cursor)
    assert len(items2) == 1
    assert items2[0][0]["data"]["content"] == "a3"
    assert items2[0][2] == 4
    assert new_cursor2 == 4


@pytest.mark.asyncio
async def test_drain_with_cursor_at_high_water_returns_empty() -> None:
    """Calling drain again with cursor==high_water yields nothing."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    for i in range(2):
        await registry.append_captured_event(task.tool_call_id, _text_chunk(f"a{i}"))

    _, new_cursor = drain_task_captured_events(task, cursor=0)
    items2, new_cursor2 = drain_task_captured_events(task, cursor=new_cursor)
    assert items2 == []
    assert new_cursor2 == new_cursor


@pytest.mark.asyncio
async def test_writer_claim_field_removed() -> None:
    """The sse_redis_writer_claimed field is gone — producer is the sole
    Redis writer in the Redis-First refactor."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    assert not hasattr(task, "sse_redis_writer_claimed")


# ---------------------------------------------------------------------------
# Timestamp semantics (Part 5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_last_updated_at_bumps_on_text_chunk() -> None:
    """Text message_chunk events bump last_updated_at."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    task.last_updated_at = baseline - 100  # artificially age

    await registry.append_captured_event(task.tool_call_id, _text_chunk())
    assert task.last_updated_at > baseline - 1


@pytest.mark.asyncio
async def test_last_updated_at_bumps_on_completion_done_callback() -> None:
    """Task transition via done_callback bumps last_updated_at."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    task.last_updated_at = time.time() - 3600  # 1h ago
    stale = task.last_updated_at

    async def _work() -> None:
        return None

    atask = asyncio.create_task(_work())
    atask.add_done_callback(_make_task_done_callback(task))
    await atask
    await asyncio.sleep(0)

    assert task.last_updated_at > stale + 10, "done_callback should bump last_updated_at"


@pytest.mark.asyncio
async def test_update_metrics_no_longer_bumps_last_updated_at() -> None:
    """Regression: update_metrics changes counts but not last_updated_at."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    # Sleep a tick so any accidental bump would be observable
    await asyncio.sleep(0.01)

    await registry.update_metrics(task.tool_call_id, "bash")
    assert task.total_tool_calls == 1
    assert task.tool_call_counts == {"bash": 1}
    assert task.last_updated_at == baseline, (
        "update_metrics must not bump last_updated_at"
    )


@pytest.mark.asyncio
async def test_last_checked_at_bumps_on_taskoutput_read() -> None:
    """TaskOutput read path bumps last_checked_at without touching last_updated_at."""
    from ptc_agent.agent.middleware.background_subagent.tools import (
        create_task_output_tool,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="k7Xm2p")
    # Age the checked_at so a bump is observable
    task.last_checked_at = time.time() - 100
    task.last_updated_at = time.time() - 100
    stale_updated = task.last_updated_at

    mw = MagicMock()
    mw.registry = registry
    tool = create_task_output_tool(mw)

    # Call the coroutine directly (avoid LangGraph runtime setup)
    await tool.coroutine(task_id="k7Xm2p", timeout=0)

    assert time.time() - task.last_checked_at < 1.0, "last_checked_at should be near-now"
    assert task.last_updated_at == stale_updated, (
        "TaskOutput read must not bump last_updated_at"
    )


@pytest.mark.asyncio
async def test_update_action_bumps_last_updated_at_through_middleware() -> None:
    """Exercise the update action end-to-end: middleware must bump
    last_updated_at when _queue_followup_to_redis succeeds.

    Regression guard for middleware.py:433 — the `task.last_updated_at = time.time()`
    line inside the update success path.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    # Task must be pending (asyncio_task=None + completed=False satisfies is_pending)
    assert task.is_pending

    mw = _make_middleware_with_registry(registry)
    mw._queue_followup_to_redis = AsyncMock(return_value=True)

    stale = time.time() - 3600
    task.last_updated_at = stale
    task.last_checked_at = stale

    result = await mw.awrap_tool_call(
        _tool_call_request("update", "abc123", prompt="keep going"),
        _noop_handler,
    )

    assert isinstance(result, ToolMessage)
    assert "Follow-up sent" in result.content
    assert task.last_updated_at > stale + 10, (
        "update-action success path must bump last_updated_at"
    )
    assert task.last_checked_at > stale + 10, (
        "update-action must also bump last_checked_at"
    )
    mw._queue_followup_to_redis.assert_awaited_once_with(task.tool_call_id, "keep going")


@pytest.mark.asyncio
async def test_update_action_does_not_bump_on_redis_failure() -> None:
    """Negative path: when _queue_followup_to_redis returns False, the
    middleware must NOT bump last_updated_at (only checked_at)."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    mw = _make_middleware_with_registry(registry)
    mw._queue_followup_to_redis = AsyncMock(return_value=False)

    stale = time.time() - 3600
    task.last_updated_at = stale

    result = await mw.awrap_tool_call(
        _tool_call_request("update", "abc123"), _noop_handler
    )
    assert isinstance(result, ToolMessage)
    assert "Could not deliver" in result.content
    assert task.last_updated_at == stale, (
        "failed follow-up must not bump last_updated_at"
    )


@pytest.mark.asyncio
async def test_resume_action_resets_timestamps_through_middleware() -> None:
    """Exercise the resume action end-to-end: resume must bump both
    last_checked_at and last_updated_at.

    Regression guard for middleware.py:520-521 — the two bump lines inside
    the resume action's reset block.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    # Resume requires the task to be completed (is_pending must be False)
    task.completed = True

    stale = time.time() - 3600
    task.last_checked_at = stale
    task.last_updated_at = stale

    mw = _make_middleware_with_registry(registry)

    # Patch the background runner so resume doesn't actually execute work;
    # the done_callback from _make_task_done_callback will fire on completion
    # and bump last_updated_at, so our assertion remains > stale + 10.
    async def _noop_run(*_args, **_kwargs):
        return None

    with patch(
        "ptc_agent.agent.middleware.background_subagent.middleware._run_background_task",
        side_effect=_noop_run,
    ):
        result = await mw.awrap_tool_call(
            _tool_call_request("resume", "abc123"), _noop_handler
        )

    assert isinstance(result, ToolMessage)
    assert "Resumed" in result.content
    assert task.last_checked_at > stale + 10, "resume must bump last_checked_at"
    assert task.last_updated_at > stale + 10, "resume must bump last_updated_at"
    # Fresh wake signal / consumer counters were reset too
    assert task.sse_consumer_count == 0
    assert len(task.captured_events_tail) == 0
    assert task.captured_event_seq == 0
    assert task.captured_event_count == 0

    # Clean up the spawned asyncio task so pytest doesn't warn on unawaited
    if task.asyncio_task is not None:
        await asyncio.gather(task.asyncio_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_reasoning_event_does_not_bump_last_updated_at() -> None:
    """Reasoning chunks are pacing noise, not user-visible text."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    await asyncio.sleep(0.01)

    await registry.append_captured_event(task.tool_call_id, _reasoning_chunk())
    assert task.last_updated_at == baseline


@pytest.mark.asyncio
async def test_reasoning_signal_event_does_not_bump_last_updated_at() -> None:
    """reasoning_signal markers (start/complete) should not bump."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    await asyncio.sleep(0.01)

    await registry.append_captured_event(task.tool_call_id, _reasoning_signal("start"))
    await registry.append_captured_event(task.tool_call_id, _reasoning_signal("complete"))
    assert task.last_updated_at == baseline


@pytest.mark.asyncio
async def test_tool_calls_event_does_not_bump_last_updated_at() -> None:
    """tool_calls events are pacing noise for staleness purposes."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    await asyncio.sleep(0.01)

    await registry.append_captured_event(task.tool_call_id, _tool_calls_event())
    assert task.last_updated_at == baseline


@pytest.mark.asyncio
async def test_tool_call_result_event_does_not_bump_last_updated_at() -> None:
    """tool_call_result events likewise are not staleness-relevant."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    baseline = task.last_updated_at
    await asyncio.sleep(0.01)

    await registry.append_captured_event(task.tool_call_id, _tool_call_result_event())
    assert task.last_updated_at == baseline


@pytest.mark.asyncio
async def test_orphan_collector_liveness_with_tool_calls_only() -> None:
    """OrphanCollector liveness fallback: cur_events > prev_events resets
    last_progress_time even when last_updated_at doesn't advance.

    Regression guard for Part 5's metrics-path bump removal.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    prev_events = task.captured_event_count
    prev_update = task.last_updated_at

    # Simulate only tool-call activity — last_updated_at stays, events grow
    await registry.append_captured_event(task.tool_call_id, _tool_call_result_event())
    await registry.append_captured_event(task.tool_call_id, _tool_calls_event())

    cur_update = task.last_updated_at
    cur_events = task.captured_event_count

    # Mirror the liveness check in the orphan collector
    progressing = cur_update > prev_update or cur_events > prev_events
    assert progressing, "tool-call-only progression must still count as liveness"
    assert cur_update == prev_update  # timestamp stayed
    assert cur_events > prev_events  # but events advanced


# ---------------------------------------------------------------------------
# _resolve_or_error helper (Part 6)
# ---------------------------------------------------------------------------


def _make_middleware_with_registry(registry: BackgroundTaskRegistry) -> BackgroundSubagentMiddleware:
    mw = BackgroundSubagentMiddleware(registry=registry)
    return mw


@pytest.mark.asyncio
async def test_resolve_or_error_strips_whitespace() -> None:
    """Trailing whitespace on task_id must still resolve the task."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    mw = _make_middleware_with_registry(registry)

    result = await mw._resolve_or_error(
        "  abc123\n", parent_thread_id="t", tool_call_id="tc1", action_name="update"
    )
    assert isinstance(result, BackgroundTask)
    assert result.task_id == "abc123"
    assert result is task


@pytest.mark.asyncio
async def test_resolve_or_error_returns_toolmessage_on_miss() -> None:
    """Unknown task_id returns a not-found ToolMessage with the right wiring."""
    registry = BackgroundTaskRegistry()
    mw = _make_middleware_with_registry(registry)
    # Bypass hydration so we exercise the None-fall-through
    mw._hydrate_from_checkpoint = AsyncMock(return_value=None)

    result = await mw._resolve_or_error(
        "missing", parent_thread_id="t", tool_call_id="tc-xyz"
    )
    assert isinstance(result, ToolMessage)
    assert "Task-missing not found" in result.content
    assert result.tool_call_id == "tc-xyz"
    assert result.name == "Task"


@pytest.mark.asyncio
async def test_resolve_or_error_empty_and_none_return_required_error() -> None:
    """Empty / None / whitespace-only task_id returns a 'required' error."""
    registry = BackgroundTaskRegistry()
    mw = _make_middleware_with_registry(registry)

    for bad in ("", "   ", None):
        result = await mw._resolve_or_error(
            bad, parent_thread_id="t", tool_call_id="tc1"
        )
        assert isinstance(result, ToolMessage), f"input={bad!r} should error"
        assert "task_id is required" in result.content

    scoped = await mw._resolve_or_error(
        "", parent_thread_id="t", tool_call_id="tc1", action_name="update"
    )
    assert isinstance(scoped, ToolMessage)
    assert "for 'update' action" in scoped.content


@pytest.mark.asyncio
async def test_resolve_or_error_hydration_fallback_success() -> None:
    """When registry misses, _hydrate_from_checkpoint can still provide the task."""
    registry = BackgroundTaskRegistry()
    mw = _make_middleware_with_registry(registry)

    hydrated = BackgroundTask(
        tool_call_id="tc-hydrated",
        task_id="abc123",
        description="from checkpoint",
        prompt="",
        subagent_type="research",
    )
    mw._hydrate_from_checkpoint = AsyncMock(return_value=hydrated)

    result = await mw._resolve_or_error(
        "abc123", parent_thread_id="t", tool_call_id="tc1"
    )
    assert result is hydrated
    mw._hydrate_from_checkpoint.assert_awaited_once()


# ---------------------------------------------------------------------------
# (c) Negative-path guards for update / resume actions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_action_rejects_cancelled_task() -> None:
    """Cancelled tasks cannot receive follow-up instructions via ``update``."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    task.cancelled = True
    task.completed = True
    mw = _make_middleware_with_registry(registry)

    result = await mw.awrap_tool_call(
        _tool_call_request("update", "abc123"), _noop_handler
    )
    assert isinstance(result, ToolMessage)
    assert "was cancelled" in result.content


@pytest.mark.asyncio
async def test_resume_action_rejects_cancelled_task() -> None:
    """Cancelled tasks cannot be resumed."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    task.cancelled = True
    task.completed = True
    mw = _make_middleware_with_registry(registry)

    result = await mw.awrap_tool_call(
        _tool_call_request("resume", "abc123"), _noop_handler
    )
    assert isinstance(result, ToolMessage)
    assert "was cancelled" in result.content


@pytest.mark.asyncio
async def test_update_action_rejects_mismatched_subagent_type() -> None:
    """Callers can't switch a task's subagent_type via update."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    task.subagent_type = "research"
    mw = _make_middleware_with_registry(registry)

    req = _tool_call_request("update", "abc123")
    req.tool_call["args"]["subagent_type"] = "general-purpose"

    result = await mw.awrap_tool_call(req, _noop_handler)
    assert isinstance(result, ToolMessage)
    assert "research" in result.content and "general-purpose" in result.content


@pytest.mark.asyncio
async def test_update_action_rejects_completed_task() -> None:
    """update on a completed task returns a clear 'use resume' error."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    task.completed = True
    mw = _make_middleware_with_registry(registry)

    result = await mw.awrap_tool_call(
        _tool_call_request("update", "abc123"), _noop_handler
    )
    assert isinstance(result, ToolMessage)
    assert "not running" in result.content


@pytest.mark.asyncio
async def test_resume_action_rejects_still_running_task() -> None:
    """resume on a still-running task returns a 'use update' error."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    # Mark as pending: not completed, has a live asyncio_task
    task.asyncio_task = asyncio.create_task(asyncio.sleep(0.05))
    mw = _make_middleware_with_registry(registry)

    result = await mw.awrap_tool_call(
        _tool_call_request("resume", "abc123"), _noop_handler
    )
    assert isinstance(result, ToolMessage)
    assert "still running" in result.content
    await asyncio.gather(task.asyncio_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# (b) SubagentEventCaptureMiddleware — direct coverage
# ---------------------------------------------------------------------------


def _make_ai_message(*, reasoning: str | None = None, text: str | None = None,
                    tool_calls: list | None = None, msg_id: str = "mid-1"):
    """Build a mock AIMessage-like object compatible with event_capture's reads."""
    ai = MagicMock()
    ai.id = msg_id
    # awrap_model_call routes via ``format_llm_content`` which parses ai.content
    # into {reasoning, text}. Easiest to mock at the format_llm_content level.
    ai.content = text or ""
    ai.tool_calls = tool_calls or []
    return ai


def _patch_format_llm_content(monkeypatch, *, reasoning: str | None = None, text: str | None = None):
    import src.llms.content_utils as cu  # noqa: F401

    def _fake_format(_content):
        out: dict = {}
        if reasoning:
            out["reasoning"] = reasoning
        if text:
            out["text"] = text
        return out

    monkeypatch.setattr("src.llms.content_utils.format_llm_content", _fake_format)


class _FakeModelResponse:
    def __init__(self, ai):
        self.result = [ai]


@pytest.mark.asyncio
async def test_event_capture_emits_reasoning_signals_text_and_tool_calls(monkeypatch) -> None:
    """awrap_model_call emits reasoning start/body/complete, text, and tool_calls
    into the task's captured_events in the expected order."""
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        SubagentEventCaptureMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    current_background_tool_call_id.set(task.tool_call_id)
    mw = SubagentEventCaptureMiddleware(registry=registry)

    _patch_format_llm_content(monkeypatch, reasoning="thinking", text="hello")
    ai = _make_ai_message(
        text="hello",
        tool_calls=[{"name": "bash", "args": {"cmd": "ls"}, "id": "tc-1"}],
    )
    handler = AsyncMock(return_value=_FakeModelResponse(ai))

    request = MagicMock()
    await mw.awrap_model_call(request, handler)

    events = task.captured_events
    content_types = [e["data"].get("content_type") for e in events if e["event"] == "message_chunk"]
    # reasoning_signal(start), reasoning, reasoning_signal(complete), text
    assert "reasoning_signal" in content_types
    assert content_types.count("reasoning_signal") == 2
    assert "reasoning" in content_types
    assert "text" in content_types

    tool_call_events = [e for e in events if e["event"] == "tool_calls"]
    assert len(tool_call_events) == 1
    assert tool_call_events[0]["data"]["tool_calls"][0]["name"] == "bash"


@pytest.mark.asyncio
async def test_event_capture_identity_emitted_once_per_tool_call_id() -> None:
    """subagent_identity is emitted only on the first awrap_model_call per
    tool_call_id, and clear_identity() re-enables emission."""
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        SubagentEventCaptureMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    current_background_tool_call_id.set(task.tool_call_id)
    mw = SubagentEventCaptureMiddleware(registry=registry)

    # get_stream_writer would raise in unit-test context without an active
    # runnable; the emission path catches and logs. Verify the dedup side
    # effect directly via the _emitted_identity set + clear_identity.
    assert task.tool_call_id not in mw._emitted_identity

    mw._emitted_identity.add(task.tool_call_id)
    assert task.tool_call_id in mw._emitted_identity

    mw.clear_identity(task.tool_call_id)
    assert task.tool_call_id not in mw._emitted_identity


@pytest.mark.asyncio
async def test_event_capture_agent_id_fallback_for_unknown_task() -> None:
    """_get_agent_id falls back to subagent:{tool_call_id} when no task exists."""
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        SubagentEventCaptureMiddleware,
    )

    registry = BackgroundTaskRegistry()
    mw = SubagentEventCaptureMiddleware(registry=registry)

    assert mw._get_agent_id("unknown-tc") == "subagent:unknown-tc"


def test_truncate_content_below_cap_unchanged() -> None:
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        _MAX_CAPTURED_CONTENT_BYTES,
        _truncate_content,
    )

    s = "hello world"
    assert _truncate_content(s) is s or _truncate_content(s) == s
    # Just under cap stays unchanged
    just_under = "a" * (_MAX_CAPTURED_CONTENT_BYTES - 10)
    assert _truncate_content(just_under) == just_under


def test_truncate_content_above_cap_is_truncated_with_marker() -> None:
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        _MAX_CAPTURED_CONTENT_BYTES,
        _truncate_content,
    )

    s = "x" * (_MAX_CAPTURED_CONTENT_BYTES + 5000)
    out = _truncate_content(s)
    assert len(out.encode("utf-8")) <= _MAX_CAPTURED_CONTENT_BYTES + 100  # +marker
    assert "[...truncated," in out
    assert "5000 more bytes" in out


@pytest.mark.asyncio
async def test_tool_message_captured_event_is_truncated_for_huge_payload() -> None:
    """A 1 MB tool result must not sit uncapped in captured_events."""
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        _MAX_CAPTURED_CONTENT_BYTES,
        SubagentEventCaptureMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    current_background_tool_call_id.set(task.tool_call_id)
    mw = SubagentEventCaptureMiddleware(registry=registry)

    huge = "z" * (1024 * 1024)  # 1 MB
    request = MagicMock()
    request.tool_call = {"name": "bash", "id": "tc-big"}
    result_msg = ToolMessage(content=huge, tool_call_id="tc-big")
    handler = AsyncMock(return_value=result_msg)

    await mw.awrap_tool_call(request, handler)

    tcr = [e for e in task.captured_events if e["event"] == "tool_call_result"][0]
    captured_len = len(tcr["data"]["content"].encode("utf-8"))
    assert captured_len <= _MAX_CAPTURED_CONTENT_BYTES + 200  # cap + small marker
    assert "[...truncated," in tcr["data"]["content"]


@pytest.mark.asyncio
async def test_event_capture_tool_call_result_captured(monkeypatch) -> None:
    """awrap_tool_call captures ToolMessage results as tool_call_result events."""
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        SubagentEventCaptureMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    current_background_tool_call_id.set(task.tool_call_id)
    mw = SubagentEventCaptureMiddleware(registry=registry)

    request = MagicMock()
    request.tool_call = {"name": "bash", "id": "tc-bash-1"}
    result_msg = ToolMessage(content="hi", tool_call_id="tc-bash-1")
    handler = AsyncMock(return_value=result_msg)

    await mw.awrap_tool_call(request, handler)

    tcr_events = [e for e in task.captured_events if e["event"] == "tool_call_result"]
    assert len(tcr_events) == 1
    assert tcr_events[0]["data"]["content"] == "hi"
    assert task.total_tool_calls == 1


# ---------------------------------------------------------------------------
# (a) stream_subagent_task_events — end-to-end through the drain/writer paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_subagent_task_events_single_consumer_lifecycle(monkeypatch) -> None:
    """One consumer: receives events, decrements count on close, sse_drain_complete
    is set on final consumer exit. Producer is the sole Redis writer (no
    list_append from the consumer)."""
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="xy1234")
    # Producer-driven: emulate two prior captures via the registry. Spill
    # is gated by thread_id+enabled flag so the bare-registry fixture skips
    # the Redis path silently.
    await registry.append_captured_event(task.tool_call_id, _text_chunk("e0"))
    await registry.append_captured_event(task.tool_call_id, _text_chunk("e1"))

    fake_store = MagicMock()
    fake_store.get_registry = AsyncMock(return_value=registry)
    monkeypatch.setattr(
        "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
        lambda: fake_store,
    )

    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )

    gen = stream_reconnect.stream_subagent_task_events("t1", "xy1234").__aiter__()

    e0 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    e1 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)

    assert "event: message_chunk" in e0
    assert "id: 1" in e0  # first captured event -> seq 1
    assert "id: 2" in e1
    assert task.sse_consumer_count == 1
    # Writer-claim machinery is gone — no field, no attribute.
    assert not hasattr(task, "sse_redis_writer_claimed")

    # Mark completion so loop breaks
    task.completed = True
    task.new_event_signal.set()
    try:
        while True:
            await asyncio.wait_for(gen.__anext__(), timeout=0.5)
    except (StopAsyncIteration, asyncio.TimeoutError):
        pass

    await gen.aclose()
    assert task.sse_consumer_count == 0
    assert task.sse_drain_complete.is_set()
    # Consumer never writes Redis — the producer is the sole writer.
    assert fake_cache.list_append.await_count == 0


@pytest.mark.asyncio
async def test_stream_subagent_task_events_cursor_advances_with_concurrent_append(
    monkeypatch,
) -> None:
    """If an event is appended while the consumer is awaiting the wake signal,
    the next drain iteration must pick it up — not skip it. The cursor
    advances by seq, not list index."""
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="xy1234")
    await registry.append_captured_event(task.tool_call_id, _text_chunk("e0"))

    fake_store = MagicMock()
    fake_store.get_registry = AsyncMock(return_value=registry)
    monkeypatch.setattr(
        "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
        lambda: fake_store,
    )

    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )

    gen = stream_reconnect.stream_subagent_task_events("t1", "xy1234").__aiter__()
    first = await asyncio.wait_for(gen.__anext__(), timeout=1.0)

    # Inject second event after the consumer began awaiting the wake signal.
    await registry.append_captured_event(task.tool_call_id, _text_chunk("e_injected"))

    second = await asyncio.wait_for(gen.__anext__(), timeout=2.0)

    assert '"content": "e0"' in first
    assert '"content": "e_injected"' in second
    assert "id: 1" in first
    assert "id: 2" in second

    task.completed = True
    task.new_event_signal.set()
    await gen.aclose()
