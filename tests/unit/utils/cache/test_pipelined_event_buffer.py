"""Unit tests for RedisCacheClient.pipelined_event_buffer.

The hot path is one bare XADD. Everything else — the epoch DEL, the retention
heal, an explicit TTL — is an exception that pulls the write into a MULTI, and
each of those exceptions is what these tests pin. The other half of the
contract is that failures RAISE: the caller's retry policy classifies by
exception type, and a boolean return would erase the difference between
"nothing was sent" and "the reply was lost".
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.exceptions as redis_exceptions

from src.utils.cache.redis_cache import (
    EventBufferUnavailableError,
    RedisCacheClient,
)


def _make_pipeline_mock() -> tuple[MagicMock, MagicMock]:
    """Build a redis-py-like async pipeline mock recording queued commands."""
    pipe = MagicMock()
    for fn in ("expire", "persist", "xadd", "delete"):
        setattr(pipe, fn, MagicMock(return_value=pipe))
    pipe.execute = AsyncMock(return_value=[b"7-0"])

    pipeline_ctx = MagicMock()
    pipeline_ctx.__aenter__ = AsyncMock(return_value=pipe)
    pipeline_ctx.__aexit__ = AsyncMock(return_value=None)
    return pipe, pipeline_ctx


def _make_client(pipeline_ctx: MagicMock) -> RedisCacheClient:
    client = RedisCacheClient.__new__(RedisCacheClient)
    client.enabled = True
    client.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}
    redis_mock = MagicMock()
    redis_mock.pipeline = MagicMock(return_value=pipeline_ctx)
    redis_mock.xadd = AsyncMock(return_value=b"7-0")
    client.client = redis_mock
    return client


@pytest.mark.asyncio
async def test_steady_state_is_a_single_bare_xadd():
    """No meta hash, no PERSIST, no pipeline — one command per event."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=42,
        max_size=1000,
        stream_event="id: 42\nevent: x\ndata: hi\n\n",
    )

    cache.client.pipeline.assert_not_called()
    cache.client.xadd.assert_awaited_once()
    args, kwargs = cache.client.xadd.call_args
    assert args[0] == "workflow:stream:t1"
    assert args[1] == {b"event": b"id: 42\nevent: x\ndata: hi\n\n"}
    assert kwargs["id"] == "42-0"
    assert kwargs["maxlen"] == 1000
    assert kwargs["approximate"] is True


@pytest.mark.asyncio
async def test_stream_record_rides_the_same_entry():
    """The collector reads ``b"record"`` back with XRANGE instead of a List."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "subagent:stream:t1:abc",
        event_id=5,
        max_size=1000,
        stream_event="id: 5\nevent: message_chunk\ndata: {}\n\n",
        stream_record='{"seq": 5, "event": "message_chunk"}',
    )

    fields = cache.client.xadd.call_args.args[1]
    assert fields[b"event"] == b"id: 5\nevent: message_chunk\ndata: {}\n\n"
    assert fields[b"record"] == b'{"seq": 5, "event": "message_chunk"}'


@pytest.mark.asyncio
async def test_no_record_field_when_not_supplied():
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    # Mid-turn id: the bare path, where the fields go straight to client.xadd.
    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=7,
        max_size=1000,
        stream_event="id: 7\nevent: x\ndata: hi\n\n",
    )

    fields = cache.client.xadd.call_args.args[1]
    assert b"event" in fields
    assert b"record" not in fields


@pytest.mark.asyncio
async def test_epoch_reset_dels_and_writes_atomically():
    """A crashed predecessor's leftovers must be gone before id 1-0 lands, or
    the XADD is rejected against a stream whose tail is already past it."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=1,
        max_size=1000,
        stream_event="id: 1\nevent: x\ndata: hi\n\n",
    )

    cache.client.xadd.assert_not_awaited()
    pipe.delete.assert_called_once_with("workflow:stream:t1")
    assert pipe.xadd.call_args.kwargs["id"] == "1-0"
    pipe.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_mid_turn_write_never_dels():
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=7,
        max_size=1000,
        stream_event="id: 7\nevent: x\ndata: hi\n\n",
    )

    cache.client.pipeline.assert_not_called()
    cache.client.xadd.assert_awaited_once()


@pytest.mark.asyncio
async def test_heal_retention_persists_the_stream():
    """Periodic re-assertion that an active stream carries no TTL — a failed
    cleanup or a stale collector stamp would otherwise expire it mid-run."""
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=512,
        max_size=1000,
        stream_event="id: 512\nevent: x\ndata: hi\n\n",
    )

    pipe.persist.assert_called_once_with("workflow:stream:t1")
    pipe.expire.assert_not_called()


@pytest.mark.asyncio
async def test_heal_rides_one_write_in_512_not_the_one_before():
    """The cadence is the whole saving: id 511 must take the bare path."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=511,
        max_size=1000,
        stream_event="id: 511\nevent: x\ndata: hi\n\n",
    )

    cache.client.pipeline.assert_not_called()
    cache.client.xadd.assert_awaited_once()


@pytest.mark.asyncio
async def test_bare_suppresses_both_extras():
    """What a retry sends. Replaying the DEL could erase a frame this process
    never wrote, and replaying the PERSIST could re-immortalize a stream
    something else already stamped terminal."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    for event_id in (1, 512):
        cache.client.xadd.reset_mock()
        cache.client.pipeline.reset_mock()
        await cache.pipelined_event_buffer(
            "workflow:stream:t1",
            event_id=event_id,
            max_size=1000,
            stream_event=f"id: {event_id}\nevent: x\ndata: hi\n\n",
            bare=True,
        )
        cache.client.pipeline.assert_not_called()
        cache.client.xadd.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_ttl_expires_and_never_persists():
    pipe, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)

    await cache.pipelined_event_buffer(
        "workflow:stream:t1",
        event_id=512,
        max_size=1000,
        stream_event="id: 512\nevent: x\ndata: hi\n\n",
        ttl=86400,
    )

    pipe.expire.assert_called_once_with("workflow:stream:t1", 86400)
    pipe.persist.assert_not_called()


@pytest.mark.asyncio
async def test_transport_errors_propagate_with_their_type():
    """The retry policy branches on the exception; swallowing it to a bool
    would make "pool exhausted" and "reply lost" indistinguishable."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xadd = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(redis_exceptions.TimeoutError):
        await cache.pipelined_event_buffer(
            "workflow:stream:t1",
            event_id=3,
            max_size=10,
            stream_event="id: 3\ndata: x\n\n",
        )
    assert cache.stats["errors"] == 1


@pytest.mark.asyncio
async def test_disabled_cache_raises_a_distinct_unretryable_error():
    cache = RedisCacheClient.__new__(RedisCacheClient)
    cache.enabled = False
    cache.client = None
    cache.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}

    with pytest.raises(EventBufferUnavailableError):
        await cache.pipelined_event_buffer(
            "workflow:stream:t1",
            event_id=1,
            max_size=10,
            stream_event="x",
        )


@pytest.mark.asyncio
async def test_stream_tail_returns_the_entry_id_and_its_payload():
    """All three, from one XREVRANGE: the id alone cannot prove authorship."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xrevrange = AsyncMock(return_value=[(b"41-0", {b"event": b"x"})])

    assert await cache.stream_tail("workflow:stream:t1") == (41, b"x", None)


@pytest.mark.asyncio
async def test_stream_tail_returns_the_record_field_too():
    """The rendered frame omits the writer's ownership ids; ``record`` carries
    them, so the ambiguous-write witness needs it in the same round trip."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xrevrange = AsyncMock(
        return_value=[(b"41-0", {b"event": b"x", b"record": b'{"seq": 41}'})]
    )

    assert await cache.stream_tail("workflow:stream:t1") == (
        41,
        b"x",
        b'{"seq": 41}',
    )


@pytest.mark.asyncio
async def test_stream_tail_is_none_for_an_empty_stream():
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xrevrange = AsyncMock(return_value=[])

    assert await cache.stream_tail("workflow:stream:t1") is None


@pytest.mark.asyncio
async def test_stream_tail_payload_is_none_when_the_entry_has_no_event_field():
    """A foreign writer's entry need not carry our field; that must read as
    "not our bytes", not blow up the probe."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xrevrange = AsyncMock(return_value=[(b"7-0", {b"other": b"x"})])

    assert await cache.stream_tail("workflow:stream:t1") == (7, None, None)


@pytest.mark.asyncio
async def test_stream_tail_sorts_auto_ids_above_event_ids():
    """An auto-id frame (run_end, or a recovery-appended error) carries a ms
    timestamp — the caller relies on it comparing above any event id so a
    foreign append is never mistaken for its own landed write."""
    _, pipeline_ctx = _make_pipeline_mock()
    cache = _make_client(pipeline_ctx)
    cache.client.xrevrange = AsyncMock(
        return_value=[(b"1769000000000-0", {b"event": b"run_end"})]
    )

    tail = await cache.stream_tail("workflow:stream:t1")
    assert tail is not None and tail[0] > 10_000
