"""Tests for the subagent SSE producer path (stream-only).

Covers:
- Monotonic ``captured_event_seq`` under concurrent appends.
- Bytes counter accumulates.
- Redis XADD is invoked for every event when enabled and thread_id is set.
- XADD carries both ``b"event"`` (pre-rendered SSE wire) and the JSON record
  via ``stream_record`` for the post-turn collector's XRANGE read.
- Redis spill failure flips ``redis_write_failed`` without raising.
- ``spill_subagent_events_to_redis: false`` skips Redis entirely.
- Per-task lock serializes concurrent spills.
- Sentinel write hits XADD on the per-task stream, no persistence side-effects.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)


def _event(i: int) -> dict:
    return {
        "event": "tool_calls",
        "data": {"agent": "task:x", "i": i},
    }


def _text_event(i: int) -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": "task:x", "content": f"hi-{i}", "content_type": "text"},
    }


@pytest.mark.asyncio
async def test_seq_is_monotonic_under_concurrent_appends() -> None:
    """append_captured_event assigns monotonic seq even under concurrency."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    async def worker(start: int, n: int) -> None:
        for i in range(n):
            await registry.append_captured_event(task.tool_call_id, _event(start + i))

    await asyncio.gather(worker(0, 25), worker(100, 25), worker(200, 25), worker(300, 25))

    assert task.captured_event_seq == 100
    assert task.captured_event_count == 100


@pytest.mark.asyncio
async def test_bytes_counter_accumulates() -> None:
    """captured_event_bytes grows with each appended event."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    assert task.captured_event_bytes == 0

    await registry.append_captured_event(task.tool_call_id, _event(0))
    after_first = task.captured_event_bytes
    assert after_first > 0

    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.captured_event_bytes > after_first


@pytest.mark.asyncio
async def test_redis_spill_called_for_every_event(monkeypatch) -> None:
    """Each captured event triggers exactly one ``pipelined_event_buffer`` call
    with the per-task stream key and a ``stream_record`` JSON payload so the
    post-turn collector can XRANGE the record back out."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert fake_cache.pipelined_event_buffer.await_count == 5
    seqs = [
        call.kwargs["last_event_id"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    ]
    assert seqs == [1, 2, 3, 4, 5]
    meta_keys = {
        call.kwargs["meta_key"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    }
    assert meta_keys == {f"subagent:events:meta:thread-x:{task.task_id}"}
    stream_keys = {
        call.kwargs["stream_key"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    }
    assert stream_keys == {f"subagent:stream:thread-x:{task.task_id}"}
    # Every spill carries the JSON record for the post-turn collector.
    for call in fake_cache.pipelined_event_buffer.await_args_list:
        assert "stream_record" in call.kwargs
        assert call.kwargs["stream_record"], "stream_record must be a non-empty payload"
        # No List key in the new signature — events_key was removed.
        assert "events_key" not in call.kwargs
    assert not task.redis_write_failed


@pytest.mark.asyncio
async def test_redis_spill_failure_sets_flag_no_raise(monkeypatch) -> None:
    """Pipeline returning (False, 0) flips redis_write_failed without raising."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))
    await registry.append_captured_event(task.tool_call_id, _event(1))

    assert task.redis_write_failed is True
    # The seq counter still advanced even though spills failed.
    assert task.captured_event_seq == 2


@pytest.mark.asyncio
async def test_redis_spill_exception_sets_flag_no_raise(monkeypatch) -> None:
    """Pipeline raising flips redis_write_failed without propagating."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))

    assert task.redis_write_failed is True
    assert task.captured_event_seq == 1


@pytest.mark.asyncio
async def test_redis_spill_timeout_flips_flag_no_hang(monkeypatch) -> None:
    """A hung pipeline must not pace the subagent: ``asyncio.wait_for`` aborts
    after ``_SPILL_TIMEOUT_SECONDS`` and trips the circuit so the next append
    short-circuits without re-entering Redis."""

    async def hang(**_kwargs):
        await asyncio.sleep(10)
        return True, 1

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=hang)
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.registry._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))
    assert task.redis_write_failed is True

    await registry.append_captured_event(task.tool_call_id, _event(1))
    # Only the first call reached Redis; the circuit-breaker short-circuits
    # subsequent appends so a degraded Redis can't pace subagent execution.
    assert fake_cache.pipelined_event_buffer.await_count == 1
    assert task.captured_event_seq == 2


@pytest.mark.asyncio
async def test_redis_spill_circuit_breaker_short_circuits(monkeypatch) -> None:
    """Once ``redis_write_failed`` is set, ``_spill_record_to_redis`` returns
    immediately on every subsequent append for that task — no cache fetch,
    no pipeline call."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.redis_write_failed = True  # simulate prior failure

    for i in range(5):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert fake_cache.pipelined_event_buffer.await_count == 0
    assert task.captured_event_seq == 5


@pytest.mark.asyncio
async def test_spill_disabled_skips_redis(monkeypatch) -> None:
    """spill_subagent_events_to_redis: false → no Redis call ever."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: False
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    for i in range(3):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert fake_cache.pipelined_event_buffer.await_count == 0
    assert task.captured_event_seq == 3
    assert task.redis_write_failed is False


@pytest.mark.asyncio
async def test_redis_spill_uses_durable_persistence_cap(monkeypatch) -> None:
    """The Redis spool MUST use the durable per-workflow cap
    (``get_max_stored_messages_per_agent`` / ``get_redis_ttl_workflow_events``).
    A regression that read a smaller per-task buffer cap would silently truncate
    early events for long-running subagents, corrupting
    ``conversation_responses.sse_events`` on persistence.
    """
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "src.config.settings.get_max_stored_messages_per_agent", lambda: 150_000
    )
    monkeypatch.setattr(
        "src.config.settings.get_redis_ttl_workflow_events", lambda: 86_400
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    for i in range(5_000):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert fake_cache.pipelined_event_buffer.await_count == 5_000
    for call in fake_cache.pipelined_event_buffer.await_args_list:
        assert call.kwargs["max_size"] == 150_000
        assert call.kwargs["ttl"] == 86_400


@pytest.mark.asyncio
async def test_per_task_lock_serializes_concurrent_spills(monkeypatch) -> None:
    """Concurrent appends to the same task must spill to Redis in seq order.

    The registry-wide lock is released before Redis I/O, so two concurrent
    appends can each hold distinct pool connections and race to the server.
    The per-task ``redis_spill_lock`` serializes I/O so the stream lands in
    explicit ``<seq>-0`` order regardless of pool scheduling.
    """
    started: list[int] = []
    finished: list[int] = []

    async def slow_then_fast(**kwargs):
        seq = kwargs["last_event_id"]
        started.append(seq)
        if seq == 1:
            await asyncio.sleep(0.05)
        else:
            await asyncio.sleep(0.01)
        finished.append(seq)
        return True, seq

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=slow_then_fast)
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await asyncio.gather(
        registry.append_captured_event(task.tool_call_id, _event(0)),
        registry.append_captured_event(task.tool_call_id, _event(1)),
    )

    assert finished == [1, 2], f"spills landed out of order: finished={finished}"
    seqs_in_call_order = [
        call.kwargs["last_event_id"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    ]
    assert seqs_in_call_order == [1, 2]


def _make_pipeline_capture(execute_return=None):
    """Build a fake redis pipeline that records xadd/expire calls."""
    queued: dict[str, list] = {"xadd": [], "expire": []}

    class _FakePipe:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        def xadd(self, name, fields, maxlen=None, approximate=True):
            queued["xadd"].append(
                {
                    "name": name,
                    "fields": fields,
                    "maxlen": maxlen,
                    "approximate": approximate,
                }
            )
            return self

        def expire(self, name, ttl):
            queued["expire"].append({"name": name, "ttl": ttl})
            return self

        async def execute(self):
            if isinstance(execute_return, BaseException):
                raise execute_return
            return execute_return or []

    pipe = _FakePipe()

    def _new_pipe(transaction=False):
        return pipe

    return queued, _new_pipe


@pytest.mark.asyncio
async def test_sentinel_writes_xadd_no_seq_bump(monkeypatch) -> None:
    """``append_sentinel_to_stream`` writes one XADD on the per-task Stream
    key and bumps its TTL. The sentinel is a transport signal — it must NOT
    advance the seq counter (which feeds persistence)."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    queued, _new_pipe = _make_pipeline_capture()
    fake_cache.client.pipeline = _new_pipe
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "src.config.settings.get_max_stored_messages_per_agent", lambda: 1000
    )
    monkeypatch.setattr(
        "src.config.settings.get_redis_ttl_workflow_events", lambda: 86400
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_sentinel_to_stream(task.tool_call_id)

    assert len(queued["xadd"]) == 1
    write = queued["xadd"][0]
    assert write["name"] == f"subagent:stream:thread-x:{task.task_id}"
    assert write["maxlen"] == 1000
    assert write["approximate"] is True
    fields = write["fields"]
    assert b"event" in fields
    payload = fields[b"event"]
    assert isinstance(payload, bytes)
    assert b'"event": "subagent_stream_end"' in payload

    assert queued["expire"] == [
        {"name": f"subagent:stream:thread-x:{task.task_id}", "ttl": 86400}
    ]

    assert task.captured_event_seq == 0
    assert task.captured_event_count == 0


@pytest.mark.asyncio
async def test_sentinel_skipped_when_redis_write_failed_sticky(monkeypatch) -> None:
    """If the per-task circuit-breaker is open, the sentinel write must
    short-circuit so the recovery path doesn't loop on the same degraded Redis."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    queued, _new_pipe = _make_pipeline_capture()
    fake_cache.client.pipeline = _new_pipe
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.redis_write_failed = True

    await registry.append_sentinel_to_stream(task.tool_call_id)

    assert queued["xadd"] == []
    assert queued["expire"] == []


@pytest.mark.asyncio
async def test_sentinel_no_op_without_thread_id(monkeypatch) -> None:
    """A registry with no ``thread_id`` has no Redis stream key — no-op."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    queued, _new_pipe = _make_pipeline_capture()
    fake_cache.client.pipeline = _new_pipe
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )

    registry = BackgroundTaskRegistry()  # no thread_id
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_sentinel_to_stream(task.tool_call_id)

    assert queued["xadd"] == []


@pytest.mark.asyncio
async def test_sentinel_swallows_pipeline_exception(monkeypatch) -> None:
    """The sentinel write is best-effort. If Redis throws mid-pipeline, the
    method must not propagate."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    queued, _new_pipe = _make_pipeline_capture(
        execute_return=RuntimeError("pipeline boom")
    )
    fake_cache.client.pipeline = _new_pipe
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "src.config.settings.get_max_stored_messages_per_agent", lambda: 1000
    )
    monkeypatch.setattr(
        "src.config.settings.get_redis_ttl_workflow_events", lambda: 86400
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_sentinel_to_stream(task.tool_call_id)


@pytest.mark.asyncio
async def test_text_event_bumps_last_updated_at_with_new_path() -> None:
    """The text-chunk last_updated_at bump survives the producer rewrite."""
    import time as _time

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.last_updated_at = _time.time() - 3600
    stale = task.last_updated_at

    await registry.append_captured_event(task.tool_call_id, _text_event(0))
    assert task.last_updated_at > stale + 10

    # Non-text events do NOT bump
    snapshot = task.last_updated_at
    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.last_updated_at == snapshot
