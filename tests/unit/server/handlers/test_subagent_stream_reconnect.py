"""Tests for the per-subagent SSE reconnect endpoint after the Redis-First refactor.

Covers:
- Cold reconnect with last_event_id=0 yields all records from Redis in seq order
- Mid-stream reconnect with last_event_id=N skips events with seq <= N
- Live phase wakes via new_event_signal and drains delta from the tail
- The sse_redis_writer_claimed field is gone (producer is sole writer)
- Malformed JSON in Redis is logged + skipped, not raised
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)


def _record(seq: int, content: str = "") -> dict:
    return {
        "seq": seq,
        "event": "message_chunk",
        "data": {
            "agent": "task:xy1234",
            "content": content,
            "content_type": "text",
        },
        "agent_id": "task:xy1234",
    }


async def _register_task(registry: BackgroundTaskRegistry, task_id: str = "xy1234"):
    task = await registry.register(
        tool_call_id=f"tc-{task_id}",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )
    if task.task_id != task_id:
        registry._task_id_to_tool_call_id.pop(task.task_id, None)
        task.task_id = task_id
        registry._task_id_to_tool_call_id[task_id] = task.tool_call_id
    return task


def _wire_fakes(monkeypatch, registry, fake_cache):
    fake_store = MagicMock()
    fake_store.get_registry = AsyncMock(return_value=registry)
    monkeypatch.setattr(
        "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
        lambda: fake_store,
    )
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )


@pytest.mark.asyncio
async def test_cold_reconnect_yields_all_redis_records_in_seq_order(monkeypatch) -> None:
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register_task(registry)

    redis_records = [json.dumps(_record(seq, f"e{seq}")) for seq in (1, 2, 3)]
    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=redis_records)
    _wire_fakes(monkeypatch, registry, fake_cache)

    gen = stream_reconnect.stream_subagent_task_events(
        "t1", task.task_id, last_event_id=0
    ).__aiter__()

    out = []
    for _ in range(3):
        out.append(await asyncio.wait_for(gen.__anext__(), timeout=1.0))

    assert "id: 1" in out[0] and '"content": "e1"' in out[0]
    assert "id: 2" in out[1] and '"content": "e2"' in out[1]
    assert "id: 3" in out[2] and '"content": "e3"' in out[2]
    # Consumer never wrote Redis
    assert fake_cache.list_append.await_count == 0

    task.completed = True
    task.new_event_signal.set()
    await gen.aclose()


@pytest.mark.asyncio
async def test_midstream_reconnect_skips_seq_le_last_event_id(monkeypatch) -> None:
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register_task(registry)

    redis_records = [json.dumps(_record(seq, f"e{seq}")) for seq in range(1, 6)]
    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=redis_records)
    _wire_fakes(monkeypatch, registry, fake_cache)

    gen = stream_reconnect.stream_subagent_task_events(
        "t1", task.task_id, last_event_id=3
    ).__aiter__()

    e4 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    e5 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    assert "id: 4" in e4 and "id: 5" in e5

    task.completed = True
    task.new_event_signal.set()
    await gen.aclose()


@pytest.mark.asyncio
async def test_live_phase_wakes_on_signal(monkeypatch) -> None:
    """No last_event_id: the consumer skips Redis replay and drains the tail
    as the producer fills it."""
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register_task(registry)

    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=[])
    _wire_fakes(monkeypatch, registry, fake_cache)

    gen = stream_reconnect.stream_subagent_task_events("t1", task.task_id).__aiter__()

    # Producer captures one event before the consumer drains
    await registry.append_captured_event(
        task.tool_call_id,
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:xy1234",
                "content": "live-1",
                "content_type": "text",
            },
        },
    )

    first = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    assert "id: 1" in first
    assert '"content": "live-1"' in first

    # Producer captures another event while consumer awaits the signal
    await registry.append_captured_event(
        task.tool_call_id,
        {
            "event": "message_chunk",
            "data": {
                "agent": "task:xy1234",
                "content": "live-2",
                "content_type": "text",
            },
        },
    )

    second = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
    assert "id: 2" in second
    assert '"content": "live-2"' in second
    # Consumer never writes Redis
    assert fake_cache.list_append.await_count == 0

    task.completed = True
    task.new_event_signal.set()
    await gen.aclose()


@pytest.mark.asyncio
async def test_writer_claim_field_absent() -> None:
    """sse_redis_writer_claimed has been removed from BackgroundTask."""
    registry = BackgroundTaskRegistry()
    task = await _register_task(registry)
    assert not hasattr(task, "sse_redis_writer_claimed")


@pytest.mark.asyncio
async def test_malformed_redis_record_is_skipped(monkeypatch) -> None:
    from src.server.handlers.chat import stream_reconnect

    registry = BackgroundTaskRegistry()
    task = await _register_task(registry)

    redis_records = [
        "{not json",
        None,
        "",
        json.dumps(_record(1, "real-1")),
        json.dumps(_record(2, "real-2")),
    ]
    fake_cache = MagicMock()
    fake_cache.list_append = AsyncMock()
    fake_cache.list_range = AsyncMock(return_value=redis_records)
    _wire_fakes(monkeypatch, registry, fake_cache)

    gen = stream_reconnect.stream_subagent_task_events(
        "t1", task.task_id, last_event_id=0
    ).__aiter__()

    e1 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    e2 = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
    assert "id: 1" in e1 and '"content": "real-1"' in e1
    assert "id: 2" in e2 and '"content": "real-2"' in e2

    task.completed = True
    task.new_event_signal.set()
    await gen.aclose()
