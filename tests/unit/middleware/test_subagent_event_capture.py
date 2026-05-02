"""Tests for the Redis-First Subagent SSE Refactor producer path.

Covers:
- Monotonic ``captured_event_seq`` under concurrent appends
- Tail respects ``maxlen`` (older events evicted)
- Bytes counter accumulates
- Redis spill is invoked for every event when enabled and thread_id is set
- Redis spill failure flips ``redis_write_failed`` without raising
- ``spill_subagent_events_to_redis: false`` skips Redis entirely
"""

from __future__ import annotations

import asyncio
from collections import deque
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
    """append_captured_event assigns monotonic seq even when called concurrently."""
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
    seqs = [rec["seq"] for rec in task.captured_events_tail]
    assert seqs == sorted(seqs), "seq must be monotonic in tail order"
    assert seqs[-1] == 100


@pytest.mark.asyncio
async def test_tail_respects_maxlen(monkeypatch) -> None:
    """When maxlen=200 and we push 1500 events, only the last 200 stay in the tail
    while captured_event_seq tracks all 1500."""
    monkeypatch.setattr(
        "src.config.settings.get_in_memory_event_tail_max_events", lambda: 200
    )
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # Force the cap on the just-registered task (registry resolved at register time).
    task.captured_events_tail = deque(maxlen=200)

    for i in range(1500):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert len(task.captured_events_tail) == 200
    assert task.captured_event_seq == 1500
    assert task.captured_event_count == 1500
    # The oldest events were evicted
    front_seq = task.captured_events_tail[0]["seq"]
    back_seq = task.captured_events_tail[-1]["seq"]
    assert back_seq == 1500
    assert front_seq == 1301  # 1500 - 200 + 1


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
    """Each captured event triggers exactly one cache.pipelined_event_buffer call."""
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
    # Verify keys are right and seq monotonic across calls
    seqs = [
        call.kwargs["last_event_id"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    ]
    assert seqs == [1, 2, 3, 4, 5]
    keys = {
        call.kwargs["events_key"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    }
    assert keys == {f"subagent:events:thread-x:{task.task_id}"}
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
    assert not task.redis_write_failed


@pytest.mark.asyncio
async def test_redis_spill_failure_sets_flag_no_raise(monkeypatch) -> None:
    """Pipeline returning (False, 0) flips redis_write_failed without raising
    and the in-memory tail keeps growing."""
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
    # Tail still grew despite Redis failure
    assert len(task.captured_events_tail) == 2
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
    assert len(task.captured_events_tail) == 1


@pytest.mark.asyncio
async def test_redis_spill_timeout_flips_flag_no_hang(monkeypatch) -> None:
    """A hung pipeline must not pace the subagent: ``asyncio.wait_for`` aborts
    after ``_SPILL_TIMEOUT_SECONDS`` and trips the circuit so the next append
    short-circuits without re-entering Redis."""

    async def hang(**_kwargs):
        await asyncio.sleep(10)  # would exceed the test timeout if not aborted
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
    assert task.captured_event_seq == 2  # tail still grew


@pytest.mark.asyncio
async def test_redis_spill_circuit_breaker_short_circuits(monkeypatch) -> None:
    """Once ``redis_write_failed`` is set, ``_spill_record_to_redis`` returns
    immediately on every subsequent append for that task — no cache fetch,
    no pipeline call, no flag-load."""
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
    assert task.captured_event_seq == 5  # tail still grew


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
    assert len(task.captured_events_tail) == 3
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

    # Fire enough events to make any miscaller obvious.
    for i in range(5_000):
        await registry.append_captured_event(task.tool_call_id, _event(i))

    assert fake_cache.pipelined_event_buffer.await_count == 5_000
    # Every spill MUST pass the durable cap + TTL — the replay-buffer cap
    # would lose events for long-running subagents.
    for call in fake_cache.pipelined_event_buffer.await_args_list:
        assert call.kwargs["max_size"] == 150_000
        assert call.kwargs["ttl"] == 86_400


@pytest.mark.asyncio
async def test_per_task_lock_serializes_concurrent_spills(monkeypatch) -> None:
    """Concurrent appends to the same task must spill to Redis in seq order.

    The registry-wide lock is released before Redis I/O, so two concurrent
    appends can each hold distinct pool connections and race to the server.
    The per-task ``redis_spill_lock`` serializes I/O so the Redis list
    always lands in seq order regardless of pool scheduling.

    The mock completes the first spill slowly (50 ms) and the second quickly
    (10 ms); without the lock the second would finish first.
    """
    started: list[int] = []
    finished: list[int] = []

    async def slow_then_fast(**kwargs):
        seq = kwargs["last_event_id"]
        started.append(seq)
        # First call sleeps long, second sleeps short. If the per-task lock
        # is missing, the second's pipeline finishes first and gets recorded
        # first in `finished`; with the lock, both must be sequential.
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

    # Fire two appends concurrently. Each takes the registry-wide lock just
    # long enough to grab seq + snapshot the record, then races for the
    # spill. Without the per-task lock, finished would be [2, 1].
    await asyncio.gather(
        registry.append_captured_event(task.tool_call_id, _event(0)),
        registry.append_captured_event(task.tool_call_id, _event(1)),
    )

    # Ordering guarantee: the second spill cannot start until the first
    # finishes, AND the order in mock.call_args_list matches seq order.
    assert finished == [1, 2], f"spills landed out of order: finished={finished}"
    seqs_in_call_order = [
        call.kwargs["last_event_id"]
        for call in fake_cache.pipelined_event_buffer.await_args_list
    ]
    assert seqs_in_call_order == [1, 2]


def _make_pipeline_capture(execute_return=None):
    """Build a fake redis pipeline that records xadd/expire calls.

    The registry's ``append_sentinel_to_stream`` opens a non-transactional
    pipeline, queues XADD + EXPIRE, then awaits ``pipe.execute()``. The
    fake mirrors that contract and records what was queued so tests can
    assert the stream key, fields, MAXLEN, and TTL refresh.
    """
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
        # Mirror the registry's call signature; transaction kwarg ignored.
        return pipe

    return queued, _new_pipe


@pytest.mark.asyncio
async def test_sentinel_writes_xadd_only_no_deque_no_persistence(monkeypatch) -> None:
    """``append_sentinel_to_stream`` writes one XADD on the per-task Stream
    key and bumps its TTL. It MUST NOT write to ``captured_events_tail``
    (which gets persisted to Postgres + replayed) or to the legacy
    Redis List (read by the pre-flag consumer). The sentinel is a
    transport signal, not content.
    """
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

    # Exactly one XADD on the per-task Stream key.
    assert len(queued["xadd"]) == 1
    write = queued["xadd"][0]
    assert write["name"] == f"subagent:stream:thread-x:{task.task_id}"
    assert write["maxlen"] == 1000
    assert write["approximate"] is True
    # Payload is the sentinel JSON record under the canonical b"event" field.
    fields = write["fields"]
    assert b"event" in fields
    payload = fields[b"event"]
    assert isinstance(payload, bytes)
    assert b'"event": "subagent_stream_end"' in payload

    # TTL refresh on the same stream key.
    assert queued["expire"] == [
        {"name": f"subagent:stream:thread-x:{task.task_id}", "ttl": 86400}
    ]

    # Crucial: NOT in the in-memory tail (which feeds Postgres persistence)
    # and the seq counter is not bumped (sentinel is not a content event).
    assert all(
        e.get("event") != "subagent_stream_end"
        for e in task.captured_events_tail
    )
    assert task.captured_event_seq == 0
    assert task.captured_event_count == 0


@pytest.mark.asyncio
async def test_sentinel_skipped_when_redis_write_failed_sticky(monkeypatch) -> None:
    """If the per-task circuit-breaker is open (a prior content spill failed),
    the sentinel write must short-circuit so the recovery path doesn't loop
    on the same degraded Redis."""
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
    """A registry with no ``thread_id`` (test fixtures, in-process scratchpads)
    has no Redis stream key to write to — the sentinel must be a no-op."""
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
    method must not propagate — the SSE consumer's ``terminal_check``
    fallback closes the stream once the asyncio task finishes."""
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

    # Should not raise.
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
