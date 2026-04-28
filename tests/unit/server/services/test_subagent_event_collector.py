"""Tests for ``iter_subagent_events_full`` Redis-fallback collector helper.

Covers:
- Yields full history when the tail covers everything (no Redis read)
- Reads Redis for ``[1, tail_front_seq)`` gap when the tail rotated
- High-water snapshot frozen at entry (late events deferred to next pass)
- Malformed Redis records are skipped, not raised
"""

from __future__ import annotations

import json
from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.services.background_task_manager import (
    iter_subagent_events_full,
)


def _event(i: int) -> dict:
    return {
        "event": "tool_calls",
        "data": {"agent": "task:x", "i": i},
    }


def _record(seq: int, agent_id: str, i: int) -> dict:
    return {
        "seq": seq,
        "event": "tool_calls",
        "data": {"agent": "task:x", "i": i},
        "agent_id": agent_id,
    }


@pytest.mark.asyncio
async def test_yields_full_history_when_tail_covers_everything(monkeypatch) -> None:
    """When the tail still holds everything, no Redis read happens."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.list_range = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 2, 3, 4, 5]
    fake_cache.list_range.assert_not_called()


@pytest.mark.asyncio
async def test_reads_redis_for_gap_when_tail_rotated(monkeypatch) -> None:
    """If the tail rotated past seq 1, Redis is read to fill the gap."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    # Redis holds 5 records by seq; pretend that's the durable copy
    redis_records = [
        json.dumps(_record(seq, "agent-x", seq - 1)) for seq in range(1, 6)
    ]
    fake_cache.list_range = AsyncMock(return_value=redis_records)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # Tail too small to hold all 5 — only last 2 stay.
    task.captured_events_tail = deque(maxlen=2)
    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert len(task.captured_events_tail) == 2
    tail_front_seq = task.captured_events_tail[0]["seq"]
    assert tail_front_seq == 4

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 2, 3, 4, 5]
    fake_cache.list_range.assert_awaited_once()
    args, _ = fake_cache.list_range.call_args
    assert args[0] == f"subagent:events:thread-x:{task.task_id}"


@pytest.mark.asyncio
async def test_high_water_snapshot_frozen(monkeypatch) -> None:
    """Events appended after the iterator started are NOT included this pass."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.list_range = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    seen = []
    iterator = iter_subagent_events_full("thread-x", task)
    seen.append((await iterator.__anext__())["seq"])

    # Producer adds two more events mid-iteration. The frozen high-water
    # mark means iter_subagent_events_full's pass STILL only emits up to 3.
    await registry.append_captured_event(task.tool_call_id, _event(99))
    await registry.append_captured_event(task.tool_call_id, _event(100))

    async for rec in iterator:
        seen.append(rec["seq"])

    assert seen == [1, 2, 3]
    # The new events are still on the tail for the next collector pass
    assert task.captured_event_seq == 5


@pytest.mark.asyncio
async def test_malformed_redis_records_are_skipped(monkeypatch) -> None:
    """JSON garbage in Redis must not raise — just gets skipped."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.list_range = AsyncMock(
        return_value=[
            "{not json",
            json.dumps(_record(1, "agent-x", 0)),
            json.dumps(_record(2, "agent-x", 1)),
            None,
            "",
        ]
    )
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # Force tail rotation so Redis is consulted
    task.captured_events_tail = deque(maxlen=1)
    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    # 1 and 2 from Redis (3 was filtered as >= tail_front_seq), 3 from tail
    assert seqs == [1, 2, 3]


@pytest.mark.asyncio
async def test_empty_high_water_yields_nothing() -> None:
    """A task with no captured events emits no records."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    out = [rec async for rec in iter_subagent_events_full("thread-x", task)]
    assert out == []


@pytest.mark.asyncio
async def test_warns_when_redis_unavailable_and_tail_rotated(monkeypatch, caplog) -> None:
    """If the tail rotated and Redis is disabled, persistence is silently
    truncated. The collector must surface a structured ``subagent_history_truncated``
    warning so the gap is observable in production logs rather than shipping
    incomplete ``conversation_responses.sse_events``."""
    fake_cache = MagicMock()
    fake_cache.enabled = False  # Redis disabled — no records recoverable
    fake_cache.list_range = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_events_tail = deque(maxlen=2)
    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    import logging
    caplog.set_level(logging.WARNING)
    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]

    # Tail rotated past seq 1; Redis returned nothing → records 1-3 missing.
    assert seqs == [4, 5]
    truncated = [r for r in caplog.records if "subagent_history_truncated" in r.getMessage()]
    assert truncated, "expected a subagent_history_truncated warning"


@pytest.mark.asyncio
async def test_no_warn_when_redis_covers_full_gap(monkeypatch, caplog) -> None:
    """When Redis has the full [1, tail_front_seq) range, no truncation warning."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.list_range = AsyncMock(
        return_value=[json.dumps(_record(seq, "agent-x", seq - 1)) for seq in range(1, 4)]
    )
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_events_tail = deque(maxlen=2)
    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    import logging
    caplog.set_level(logging.WARNING)
    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]

    assert seqs == [1, 2, 3, 4, 5]
    truncated = [r for r in caplog.records if "subagent_history_truncated" in r.getMessage()]
    assert not truncated, "should not warn when Redis covers the full gap"
