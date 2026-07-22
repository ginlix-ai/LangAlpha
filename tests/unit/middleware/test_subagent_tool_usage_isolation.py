"""Per-task infrastructure (tool) usage isolation for background subagents.

Each subagent gets its own ToolUsageTracker set on the _tool_usage_context
ContextVar inside the spawned task body. Subagent tool calls increment only
that per-task tracker — never the parent's shared tracker — and the snapshot
lands on ``task.tool_usage`` for billing on the ``msg_type='task'`` row.
"""

from __future__ import annotations

import asyncio

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent.run_executor import (
    _run_background_task,
)
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask
from src.tools.decorators import (
    ToolUsageTracker,
    _tool_usage_context,
    get_tool_tracker,
)
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker


def _make_task(task_id: str = "abc123") -> BackgroundTask:
    return BackgroundTask(
        tool_call_id=f"tc-{task_id}",
        task_id=task_id,
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )


def _handler_recording(*keys: str):
    """Build a fake subagent handler that records the given billing keys."""

    async def handler(_request):
        tracker = get_tool_tracker()
        for key in keys:
            tracker.record_usage(key, count=1)
        return ToolMessage(content="ok", tool_call_id="tc", name="Task")

    return handler


@pytest.mark.asyncio
async def test_per_task_tracker_isolated_from_parent():
    """Subagent tool calls hit the per-task tracker; the parent's shared
    tracker stays empty and the ContextVar still points at it afterward."""
    parent_tracker = ToolUsageTracker()
    token = _tool_usage_context.set(parent_tracker)
    try:
        task = _make_task()
        result = await _run_background_task(
            task,
            _handler_recording("TavilySearchTool:deep", "TavilySearchTool:deep"),
            request=object(),
            tracker=PerCallTokenTracker(),
            label="test",
        )

        assert result["success"] is True
        assert task.tool_usage == {"TavilySearchTool:deep": 2}
        # Parent's shared tracker must not have received the subagent's usage.
        assert parent_tracker.get_summary() == {}
        # Parent context still points at the shared tracker.
        assert _tool_usage_context.get() is parent_tracker
    finally:
        _tool_usage_context.reset(token)


@pytest.mark.asyncio
async def test_parallel_subagents_do_not_cross_contaminate():
    """Concurrent subagents recording different keys keep isolated trackers."""
    task_a = _make_task("aaa111")
    task_b = _make_task("bbb222")

    results = await asyncio.gather(
        _run_background_task(
            task_a,
            _handler_recording("TavilySearchTool:deep"),
            request=object(),
            tracker=PerCallTokenTracker(),
            label="a",
        ),
        _run_background_task(
            task_b,
            _handler_recording("SerperSearchTool:basic", "SerperSearchTool:basic"),
            request=object(),
            tracker=PerCallTokenTracker(),
            label="b",
        ),
    )

    assert all(r["success"] for r in results)
    assert task_a.tool_usage == {"TavilySearchTool:deep": 1}
    assert task_b.tool_usage == {"SerperSearchTool:basic": 2}


@pytest.mark.asyncio
async def test_crash_path_captures_partial_tool_usage():
    """A subagent that records usage then raises still snapshots usage made
    before the failure (mirrors per_call_records capture on the error path)."""

    async def handler(_request):
        get_tool_tracker().record_usage("TavilySearchTool:deep", count=1)
        raise RuntimeError("boom")

    task = _make_task("crash1")
    result = await _run_background_task(
        task,
        handler,
        request=object(),
        tracker=PerCallTokenTracker(),
        label="crash",
    )

    assert result["success"] is False
    assert task.tool_usage == {"TavilySearchTool:deep": 1}


@pytest.mark.asyncio
async def test_completion_merges_into_unpersisted_usage():
    """When a task still carries unpersisted usage (e.g. a resume before the
    collector billed run-1), the next completion sums tool counts and appends
    token records rather than replacing them."""
    task = _make_task("merge1")
    task.per_call_records = [{"run": 1}]
    task.tool_usage = {"TavilySearchTool:deep": 1, "SerperSearchTool:basic": 1}

    result = await _run_background_task(
        task,
        _handler_recording("TavilySearchTool:deep"),
        request=object(),
        tracker=PerCallTokenTracker(),
        label="merge",
    )

    assert result["success"] is True
    # run-1 count preserved, run-2 summed in
    assert task.tool_usage == {
        "TavilySearchTool:deep": 2,
        "SerperSearchTool:basic": 1,
    }
    # run-1 token record preserved alongside run-2's (here run-2 has none)
    assert task.per_call_records == [{"run": 1}]
