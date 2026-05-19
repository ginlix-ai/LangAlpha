"""Unit tests for RedisCacheClient.pipelined_event_buffer.

Verifies the Stream-only semantics post-cutover:
- Meta hash + XADD only (no List writes).
- ``stream_record`` adds a second ``b"record"`` field on the XADD entry so
  the post-turn collector can XRANGE it back out without a separate List.
- Stream branch gated on both ``stream_key`` AND ``last_event_id``.
- Dirty-resume guard DELs the stream and HDELs the seq counter.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.cache.redis_cache import RedisCacheClient


def _make_pipeline_mock() -> tuple[MagicMock, MagicMock]:
    """Build a redis-py-like async pipeline mock recording queued commands."""
    pipe = MagicMock()
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
    # depends on whether the dirty-resume guard fired. Returning ``7`` at
    # every position keeps the ``seq == 7`` assertion stable across all
    # guard variants without hard-coding the per-test command count.
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
async def test_main_workflow_stream_only():
    """Main-workflow caller writes the meta hash + XADD only — no RPUSH/LTRIM."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    success, seq = await cache.pipelined_event_buffer(
        meta_key="workflow:events:meta:t1",
        event="id: 42\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=42,
        stream_key="workflow:stream:t1",
    )

    assert success is True
    assert seq == 7
    pipe.rpush.assert_not_called()
    pipe.ltrim.assert_not_called()
    pipe.xadd.assert_called_once()
    args, kwargs = pipe.xadd.call_args
    assert args[0] == "workflow:stream:t1"
    assert args[1] == {b"event": b"id: 42\nevent: x\ndata: hi\n\n"}
    assert kwargs["id"] == "42-0"
    assert kwargs["maxlen"] == 1000
    assert kwargs["approximate"] is True
    # EXPIRE on meta + stream only.
    assert pipe.expire.call_count == 2
    expire_keys = [call.args[0] for call in pipe.expire.call_args_list]
    assert "workflow:stream:t1" in expire_keys
    assert "workflow:events:meta:t1" in expire_keys


@pytest.mark.asyncio
async def test_subagent_xadd_carries_record_field_when_stream_record_provided():
    """Subagent caller passes ``stream_event`` + ``stream_record`` (no
    ``event``) → XADD entry has both ``b"event"`` (pre-rendered SSE wire)
    and ``b"record"`` (JSON record) so the post-turn collector can XRANGE
    the record back out."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        meta_key="subagent:events:meta:t1:abc",
        max_size=1000,
        ttl=86400,
        last_event_id=5,
        stream_key="subagent:stream:t1:abc",
        stream_event="id: 5\nevent: message_chunk\ndata: {}\n\n",
        stream_record='{"seq": 5, "event": "message_chunk"}',
    )

    pipe.rpush.assert_not_called()
    pipe.xadd.assert_called_once()
    xadd_args = pipe.xadd.call_args.args
    fields = xadd_args[1]
    assert fields[b"event"] == b"id: 5\nevent: message_chunk\ndata: {}\n\n"
    assert fields[b"record"] == b'{"seq": 5, "event": "message_chunk"}'
    # EXPIRE on meta_key + stream_key only.
    assert pipe.expire.call_count == 2


@pytest.mark.asyncio
async def test_stream_write_without_payload_returns_failure():
    """Requesting a stream write (stream_key + last_event_id) with neither
    ``event`` nor ``stream_event`` would advance the meta ``seq`` counter
    past an event that was never written, leaving a permanent gap. The
    helper raises ValueError; the outer except returns ``(False, 0)`` so
    the caller sees a clean failure rather than silent meta/stream drift."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    success, seq = await cache.pipelined_event_buffer(
        meta_key="workflow:events:meta:t1",
        max_size=1000,
        ttl=86400,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )

    assert success is False
    assert seq == 0
    pipe.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_xadd_omits_record_field_when_stream_record_missing():
    """Without ``stream_record``, XADD writes only the ``b"event"`` field —
    used by the main-workflow path where there is nothing to collect."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        meta_key="workflow:events:meta:t1",
        event="id: 1\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )

    pipe.xadd.assert_called_once()
    fields = pipe.xadd.call_args.args[1]
    assert b"event" in fields
    assert b"record" not in fields


@pytest.mark.asyncio
async def test_xadd_skipped_when_stream_key_missing():
    """No stream_key → meta-only writes. Currently exercised only by tests;
    production callers always provide a stream_key."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        meta_key="m",
        event="id: 1\ndata: x\n\n",
        max_size=10,
        ttl=60,
        last_event_id=1,
        stream_key=None,
    )

    pipe.xadd.assert_not_called()
    pipe.rpush.assert_not_called()
    # EXPIRE on meta only.
    assert pipe.expire.call_count == 1


@pytest.mark.asyncio
async def test_xadd_skipped_when_last_event_id_missing():
    """Without an integer last_event_id we cannot construct the explicit
    XADD ID; skip the Stream write rather than fall back to auto IDs (which
    would produce mismatched cursor semantics)."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        meta_key="m",
        event="event: x\ndata: hi\n\n",
        max_size=10,
        ttl=60,
        last_event_id=None,
        stream_key="workflow:stream:t1",
    )

    pipe.xadd.assert_not_called()
    pipe.rpush.assert_not_called()
    assert pipe.expire.call_count == 1


@pytest.mark.asyncio
async def test_dirty_resume_guard_resets_stream_and_seq():
    """First event of a fresh turn must DEL the Stream and HDEL the seq
    counter atomically. ``created_at`` is preserved (HDEL only ``seq``)."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
        meta_key="workflow:events:meta:t1",
        event="id: 1\nevent: x\ndata: hi\n\n",
        max_size=1000,
        ttl=86400,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )

    delete_calls = [call.args for call in pipe.delete.call_args_list]
    assert ("workflow:stream:t1",) in delete_calls
    pipe.hdel.assert_called_once_with("workflow:events:meta:t1", "seq")
    pipe.xadd.assert_called_once()
    assert pipe.xadd.call_args.kwargs["id"] == "1-0"


@pytest.mark.asyncio
async def test_no_dirty_resume_del_when_last_event_id_is_not_one():
    """Mid-turn events (last_event_id > 1) must NOT trigger the guard — DEL
    would wipe in-flight stream contents and break attached consumers."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client_with_pipeline(pipeline_ctx)

    await cache.pipelined_event_buffer(
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
async def test_returns_false_zero_when_disabled():
    cache = RedisCacheClient.__new__(RedisCacheClient)
    cache.enabled = False
    cache.client = None
    cache.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}

    success, seq = await cache.pipelined_event_buffer(
        meta_key="m",
        event="x",
        max_size=10,
        ttl=60,
        last_event_id=1,
        stream_key="workflow:stream:t1",
    )
    assert success is False
    assert seq == 0
