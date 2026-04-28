"""Tests for ``drain_task_captured_events`` seq-cursor semantics.

The drain helper now operates on the bounded ``captured_events_tail`` deque
and uses a last-seen-seq cursor instead of a list index. Redis-fallback for
events that rotated out of the tail is the caller's responsibility — the
drain only returns what's still in the tail.
"""

from __future__ import annotations

from collections import deque

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.services.background_task_manager import drain_task_captured_events


def _event(i: int) -> dict:
    return {
        "event": "tool_calls",
        "data": {"agent": "task:x", "i": i},
    }


@pytest.mark.asyncio
async def test_drain_with_cursor_zero_returns_full_tail() -> None:
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    items, new_cursor = drain_task_captured_events(task, cursor=0)
    assert [seq for _, _, seq in items] == [1, 2, 3]
    assert new_cursor == 3


@pytest.mark.asyncio
async def test_drain_with_cursor_at_high_water_returns_empty() -> None:
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    for i in range(2):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    items, new_cursor = drain_task_captured_events(task, cursor=2)
    assert items == []
    assert new_cursor == 2


@pytest.mark.asyncio
async def test_drain_returns_only_what_is_in_tail() -> None:
    """If the tail rotated past cursor, drain returns ONLY tail records — not
    Redis. The caller is expected to fall back to Redis for the gap."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # Force a tiny tail and push past it
    task.captured_events_tail = deque(maxlen=2)
    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    # Cursor=0 says "give me everything you have"; tail only holds seq 4 and 5.
    items, new_cursor = drain_task_captured_events(task, cursor=0)
    assert [seq for _, _, seq in items] == [4, 5]
    assert new_cursor == 5  # high-water seq


@pytest.mark.asyncio
async def test_drain_returns_record_with_seq_and_agent_id() -> None:
    """The 3-tuple shape is (record_dict, agent_id, seq)."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    await registry.append_captured_event(task.tool_call_id, _event(0))

    items, _ = drain_task_captured_events(task, cursor=0)
    assert len(items) == 1
    record, agent_id, seq = items[0]
    assert seq == 1
    assert record["seq"] == 1
    assert record["event"] == "tool_calls"
    assert record["data"] == {"agent": "task:x", "i": 0}
    assert agent_id == record["agent_id"]
    assert agent_id.startswith("general-purpose:")
