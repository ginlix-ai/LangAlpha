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
async def test_record_carries_run_stamp(monkeypatch) -> None:
    """Each spilled record is stamped with the writer's spawned_run_id — the
    durable cross-worker fence replay filters on. Tasks without a run id
    (compat shim) spill unstamped records."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    spilled: list[dict] = []

    async def _capture(t, record):
        spilled.append(record)

    monkeypatch.setattr(registry, "_spill_record_to_redis", _capture)

    await registry.append_captured_event(task.tool_call_id, _event(0))
    task.spawned_run_id = "run-2"
    await registry.append_captured_event(task.tool_call_id, _event(1))

    assert "run" not in spilled[0]
    assert spilled[1]["run"] == "run-2"


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
    after ``_SPILL_TIMEOUT_SECONDS``; the write is verified against the stream
    tail (absent here) and retried once, then the circuit trips so the next
    append short-circuits without re-entering Redis."""

    async def hang(**_kwargs):
        await asyncio.sleep(10)
        return True, 1

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=hang)
    fake_cache.client.xrevrange = AsyncMock(return_value=[])
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.redis_stream._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))
    assert task.redis_write_failed is True

    await registry.append_captured_event(task.tool_call_id, _event(1))
    # Timeout → tail verify (empty) → one retry → circuit. Only the first
    # append reached Redis (twice); the circuit-breaker short-circuits
    # subsequent appends so a degraded Redis can't pace subagent execution.
    assert fake_cache.pipelined_event_buffer.await_count == 2
    assert task.captured_event_seq == 2


@pytest.mark.asyncio
async def test_redis_spill_timeout_landed_write_recovers(monkeypatch) -> None:
    """A timed-out write whose entry is at the stream tail LANDED — the run
    must continue (no circuit, no retry). Guards against the false-positive
    kill where ``wait_for`` fired on event-loop stall, not Redis."""
    calls = {"n": 0}

    async def hang_first(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(10)
        return True, 2

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=hang_first)
    fake_cache.client.xrevrange = AsyncMock(
        return_value=[(b"1-1", {b"record": b'{"seq": 1}'})]
    )
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.redis_stream._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))
    # Tail seq matches the timed-out write → treated as delivered, no retry.
    assert task.redis_write_failed is False
    assert fake_cache.pipelined_event_buffer.await_count == 1

    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.redis_write_failed is False
    assert fake_cache.pipelined_event_buffer.await_count == 2


@pytest.mark.asyncio
async def test_redis_spill_timeout_retry_succeeds(monkeypatch) -> None:
    """A timed-out write that did NOT land gets exactly one retry; a
    successful retry keeps the transport healthy."""
    calls = {"n": 0}

    async def hang_first(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(10)
        return True, 1

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(side_effect=hang_first)
    fake_cache.client.xrevrange = AsyncMock(return_value=[])
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.redis_stream._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(0))
    assert task.redis_write_failed is False
    assert fake_cache.pipelined_event_buffer.await_count == 2


@pytest.mark.asyncio
async def test_redis_spill_v2_timeout_landed_recovers(monkeypatch) -> None:
    """The v2 per-run XADD gets the same verify-then-retry treatment: a
    timed-out write found at the v2 stream tail is delivered — no circuit."""

    async def hang(*_args, **_kwargs):
        await asyncio.sleep(10)

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    fake_cache.client.xadd = AsyncMock(side_effect=hang)
    fake_cache.client.xrevrange = AsyncMock(
        return_value=[(b"1-1", {b"payload": b'{"seq": 1}'})]
    )
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.redis_stream._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.task_run_id = "run-v2"

    await registry.append_captured_event(task.tool_call_id, _event(0))
    assert task.redis_write_failed is False
    assert fake_cache.client.xadd.await_count == 1
    # Verified against the v2 per-run stream key.
    v2_key = f"subagent:stream:thread-x:{task.task_run_id}"
    assert fake_cache.client.xrevrange.await_args.args[0] == v2_key


@pytest.mark.asyncio
async def test_redis_spill_v2_timeout_not_landed_trips_circuit(monkeypatch) -> None:
    """A v2 XADD that times out twice with nothing at the tail is a real
    transport failure — the circuit opens (phase=v2_pipeline)."""

    async def hang(*_args, **_kwargs):
        await asyncio.sleep(10)

    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    fake_cache.client.xadd = AsyncMock(side_effect=hang)
    fake_cache.client.xrevrange = AsyncMock(return_value=[])
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache)
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.redis_stream._SPILL_TIMEOUT_SECONDS",
        0.05,
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.task_run_id = "run-v2"

    await registry.append_captured_event(task.tool_call_id, _event(0))
    assert task.redis_write_failed is True
    assert fake_cache.client.xadd.await_count == 2


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
    """Active spills carry NO TTL (retention contract: an active stream must
    not expire mid-run; the attach-grace TTL is stamped at terminal) and a
    2x MAXLEN backstop over the quota — FIFO trim must never engage before
    the quota check opens the circuit, or early events would silently
    truncate and corrupt ``conversation_responses.sse_events``.
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
        assert call.kwargs["max_size"] == 300_000
        assert call.kwargs["ttl"] is None


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

        def expire(self, name, ttl, nx=False):
            queued["expire"].append({"name": name, "ttl": ttl, "nx": nx})
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
    assert write["maxlen"] == 2000  # 2x backstop over the quota
    assert write["approximate"] is True
    fields = write["fields"]
    assert b"event" in fields
    payload = fields[b"event"]
    assert isinstance(payload, bytes)
    assert b'"event": "subagent_stream_end"' in payload

    assert queued["expire"] == [
        {"name": f"subagent:stream:thread-x:{task.task_id}", "ttl": 86400, "nx": False}
    ]

    assert task.captured_event_seq == 0
    assert task.captured_event_count == 0


@pytest.mark.asyncio
async def test_quota_breach_opens_the_spill_circuit(monkeypatch) -> None:
    """A write that lands PAST the per-agent quota flips redis_write_failed —
    the abort loop + terminal escalation then finalize error(transport_lost)
    instead of the 2x MAXLEN backstop silently trimming the served head."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 151))
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "src.config.settings.get_max_stored_messages_per_agent", lambda: 150
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.redis_write_failed is True

    # Circuit open: further appends never reach the pipeline.
    await registry.append_captured_event(task.tool_call_id, _event(2))
    assert fake_cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_v2_dual_write_failure_opens_the_circuit(monkeypatch) -> None:
    """The per-run v2 stream is canonical at mux-v2 cutover: a failed frame
    is a hole readers can't detect (opaque XADD ids), so it must tear the
    run as error(transport_lost) — never a stream served as complete."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.client.xadd = AsyncMock(side_effect=RuntimeError("v2 down"))
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
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
    task.task_run_id = "run-42"

    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.redis_write_failed is True


@pytest.mark.asyncio
async def test_v2_dual_write_lands_on_the_run_scoped_key(monkeypatch) -> None:
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.client.xadd = AsyncMock(return_value=b"1-0")
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
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
    task.task_run_id = "run-42"

    await registry.append_captured_event(task.tool_call_id, _event(1))

    assert task.redis_write_failed is False
    (key, fields), _ = fake_cache.client.xadd.await_args
    assert key == "subagent:stream:thread-x:run-42"
    assert fields[b"run_id"] == b"run-42"
    assert fields[b"lane"] == f"task:{task.task_id}".encode()
    # Active v2 streams carry no TTL — the content path never expires them.
    fake_cache.client.expire.assert_not_called()


@pytest.mark.asyncio
async def test_at_quota_write_does_not_open_the_circuit(monkeypatch) -> None:
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 150))
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    monkeypatch.setattr(
        "src.config.settings.get_max_stored_messages_per_agent", lambda: 150
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )

    await registry.append_captured_event(task.tool_call_id, _event(1))
    assert task.redis_write_failed is False


@pytest.mark.asyncio
async def test_stamp_terminal_retention_runs_even_when_circuit_open(
    monkeypatch,
) -> None:
    """Active streams carry no TTL, so the terminal stamp is the only place
    their expiry clock starts — it must run even for a torn stream (the
    sentinel is skipped then), or the partial stream leaks forever."""
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    queued, _new_pipe = _make_pipeline_capture()
    fake_cache.client.pipeline = _new_pipe
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: fake_cache
    )
    monkeypatch.setattr(
        "src.config.settings.get_redis_ttl_workflow_events", lambda: 86400
    )

    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.redis_write_failed = True
    task.task_run_id = "run-77"

    await registry.stamp_terminal_retention(task.tool_call_id)

    # nx=True: the attach-grace stamp is set-if-absent so it can never
    # resurrect the shorter post-collection retention window.
    assert queued["expire"] == [
        {"name": f"subagent:stream:thread-x:{task.task_id}", "ttl": 86400, "nx": True},
        {"name": f"subagent:events:meta:thread-x:{task.task_id}", "ttl": 86400, "nx": True},
        {"name": "subagent:stream:thread-x:run-77", "ttl": 86400, "nx": True},
    ]
    assert queued["xadd"] == []


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


@pytest.mark.asyncio
async def test_cancelled_task_appends_are_dropped() -> None:
    """A killed task's streams are final: appends from writers surviving the
    bounded unwind are dropped, so post-kill output can't outrun the stop
    drain's high-water into the live stream or the archive."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    await registry.append_captured_event(task.tool_call_id, _event(0))
    task.cancelled = True
    await registry.append_captured_event(task.tool_call_id, _event(1))

    assert task.captured_event_seq == 1
    assert task.captured_event_count == 1


@pytest.mark.asyncio
async def test_cancelled_task_terminal_append_lands() -> None:
    """The seal exempts terminal unwind bookkeeping: the steering-return
    sweep runs inside the bounded unwind (before the stop drain reads its
    high-water), and dropping its ``steering_returned`` record would erase
    acknowledged user input — neither delivered nor returned anywhere."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.cancelled = True
    await registry.append_captured_event(task.tool_call_id, _event(0))
    await registry.append_captured_event(
        task.tool_call_id,
        {"event": "steering_returned", "data": {"agent": "task:x"}},
        terminal=True,
    )

    assert task.captured_event_seq == 1
    assert task.captured_event_count == 1
