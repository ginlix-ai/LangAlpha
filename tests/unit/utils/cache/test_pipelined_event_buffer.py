"""Unit tests for RedisCacheClient.pipelined_event_buffer dual-write semantics.

Verifies that when ``stream_key`` and ``last_event_id`` are both provided, the
helper queues an XADD with explicit ID ``f"{last_event_id}-0"`` and an
EXPIRE on the stream key — alongside the existing List + meta hash writes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.cache.redis_cache import RedisCacheClient


def _make_pipeline_mock() -> tuple[MagicMock, MagicMock]:
    """Build a redis-py-like async pipeline mock recording queued commands."""
    pipe = MagicMock()
    # Each queued command (rpush, ltrim, expire, hincrby, etc.) is a no-op
    # that returns the pipe object — we only care about call args, not order
    # of magic-method binding.
    for fn in (
        "rpush",
        "ltrim",
        "expire",
        "hincrby",
        "hsetnx",
        "hset",
        "hdel",
        "xadd",
        "delete",
    ):
        setattr(pipe, fn, MagicMock(return_value=pipe))
    # The implementation pulls ``seq`` from the HINCRBY result whose index
    # depends on whether the dirty-resume guard fired (and whether a stream
    # key is configured). Returning ``7`` at every position keeps the
    # ``seq == 7`` assertion stable across all guard variants without
    # hard-coding the per-test command count.
    pipe.execute = AsyncMock(return_value=[7] * 20)

    pipeline_ctx = MagicMock()
    pipeline_ctx.__aenter__ = AsyncMock(return_value=pipe)
    pipeline_ctx.__aexit__ = AsyncMock(return_value=None)
    return pipe, pipeline_ctx


def _make_client_with_pipeline(pipeline_ctx: MagicMock) -> RedisCacheClient:
    client = RedisCacheClient.__new__(RedisCacheClient)
    client.enabled = True
    client.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}
    redis_mock = MagicMock()
    redis_mock.pipeline = MagicMock(return_value=pipeline_ctx)
    client.client = redis_mock
    return client


@pytest.mark.asyncio
async def test_xadd_queued_when_stream_key_and_last_event_id_present():
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    success, seq = await cache.pipelined_event_buffer(
        events_key="workflow:events:t1",
        meta_key="workflow:events:meta:t1",
        event="id: 42\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=42,
        stream_key="workflow:stream:t1",
    )

    assert success is True
    assert seq == 7
    pipe.xadd.assert_called_once()
    args, kwargs = pipe.xadd.call_args
    assert args[0] == "workflow:stream:t1"
    # Payload uses bytes key 'event' carrying UTF-8 SSE bytes.
    assert args[1] == {b"event": b"id: 42\nevent: x\ndata: hi\n\n"}
    assert kwargs["id"] == "42-0"
    assert kwargs["maxlen"] == 1000
    assert kwargs["approximate"] is True
    # EXPIRE is called for events_key, meta_key, AND stream_key — three total.
    assert pipe.expire.call_count == 3
    expire_keys = [call.args[0] for call in pipe.expire.call_args_list]
    assert "workflow:stream:t1" in expire_keys


@pytest.mark.asyncio
async def test_xadd_skipped_when_stream_key_missing():
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="k",
        meta_key="m",
        event="id: 1\ndata: x\n\n",
        max_size=10,
        ttl=60,
        last_event_id=1,
        stream_key=None,
    )

    pipe.xadd.assert_not_called()
    # Two EXPIREs (events + meta), not three.
    assert pipe.expire.call_count == 2


@pytest.mark.asyncio
async def test_xadd_skipped_when_last_event_id_missing():
    """Without an integer last_event_id we cannot construct the explicit
    XADD ID; skip the dual-write rather than fall back to auto IDs (which
    would produce mismatched cursor semantics)."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="k",
        meta_key="m",
        event="event: x\ndata: hi\n\n",
        max_size=10,
        ttl=60,
        last_event_id=None,
        stream_key="workflow:stream:t1",
    )

    pipe.xadd.assert_not_called()
    assert pipe.expire.call_count == 2


@pytest.mark.asyncio
async def test_dirty_resume_guard_resets_stream_list_and_seq_when_last_event_id_is_one():
    """First event of a fresh handler instance must reset all three writers
    (Stream, List, meta ``seq`` counter) inside the same MULTI/EXEC.

    DEL'ing only the Stream while leaving the List + ``seq`` counter behind
    breaks ``LLEN == XLEN`` parity (the integration test
    ``test_list_and_stream_are_in_lockstep`` invariant) and drifts the
    HINCRBY-returned seq away from the SSE wire id. ``created_at`` on the
    meta hash is preserved — it documents thread-first-write, not per-turn
    state."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="workflow:events:t1",
        meta_key="workflow:events:meta:t1",
        event="id: 1\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )

    delete_calls = [call.args for call in pipe.delete.call_args_list]
    assert ("workflow:stream:t1",) in delete_calls
    assert ("workflow:events:t1",) in delete_calls
    pipe.hdel.assert_called_once_with("workflow:events:meta:t1", "seq")
    # ``created_at`` is preserved (HSETNX still runs) — only ``seq`` is HDEL'd.
    pipe.xadd.assert_called_once()
    assert pipe.xadd.call_args.kwargs["id"] == "1-0"


@pytest.mark.asyncio
async def test_dirty_resume_guard_resets_list_and_seq_without_stream_key():
    """The List+seq reset must fire even when no Stream key is configured —
    a producer that never opted into dual-write still benefits from the
    crash-recovery guarantee."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="workflow:events:t1",
        meta_key="workflow:events:meta:t1",
        event="id: 1\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=1,
        stream_key=None,
    )

    delete_calls = [call.args for call in pipe.delete.call_args_list]
    assert ("workflow:events:t1",) in delete_calls
    pipe.hdel.assert_called_once_with("workflow:events:meta:t1", "seq")
    pipe.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_no_dirty_resume_del_when_last_event_id_is_not_one():
    """Mid-turn events (last_event_id > 1) must NOT trigger the guard — DEL
    would wipe in-flight stream contents and break attached consumers."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="workflow:events:t1",
        meta_key="workflow:events:meta:t1",
        event="id: 7\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=7,
        stream_key="workflow:stream:t1",
    )

    pipe.delete.assert_not_called()
    pipe.hdel.assert_not_called()
    pipe.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_stream_event_overrides_xadd_payload_when_provided():
    """Subagent producer renders SSE wire format inline and passes it as
    stream_event so the consumer is a pass-through. The List still gets the
    JSON record (legacy compat)."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        events_key="subagent:events:t1:abc",
        meta_key="subagent:events:meta:t1:abc",
        event='{"seq": 5, "event": "message_chunk"}',  # JSON for the List
        max_size=1000,
        ttl=86400,
        last_event_id=5,
        stream_key="subagent:stream:t1:abc",
        stream_event="id: 5\nevent: message_chunk\ndata: {}\n\n",
    )

    pipe.rpush.assert_called_once()
    rpush_args = pipe.rpush.call_args.args
    assert rpush_args[1] == '{"seq": 5, "event": "message_chunk"}'

    pipe.xadd.assert_called_once()
    xadd_args = pipe.xadd.call_args.args
    assert xadd_args[1] == {b"event": b"id: 5\nevent: message_chunk\ndata: {}\n\n"}


@pytest.mark.asyncio
async def test_returns_false_zero_when_disabled():
    cache = RedisCacheClient.__new__(RedisCacheClient)
    cache.enabled = False
    cache.client = None
    cache.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}

    success, seq = await cache.pipelined_event_buffer(
        events_key="k",
        meta_key="m",
        event="x",
        max_size=10,
        ttl=60,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )
    assert success is False
    assert seq == 0
