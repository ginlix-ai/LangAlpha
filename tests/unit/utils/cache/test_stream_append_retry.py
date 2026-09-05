"""Retry protocol for one event-stream append.

A single 5s socket timeout on one write used to kill a whole turn. Retrying is
only safe because the explicit ``{event_id}-0`` id fences duplicates — these
tests pin the classification that rests on that fence, and above all the two
branches where a naive retry would violate I6: a tail PAST our id means another
writer appended, and a tail AT our id carrying another writer's entry means a
predecessor's frame is sitting there. Calling either "success" completes a run
whose archive it never wrote.

"Another writer's entry" is the whole entry, not the rendered frame: the
``record`` field carries ownership ids the SSE bytes never show, so the witness
compares both fields whenever the caller wrote both.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.exceptions as redis_exceptions

from src.utils.cache.redis_cache import EventBufferUnavailableError
from src.utils.cache.stream_append import (
    StreamAppendError,
    stream_append_with_retry,
)


FRAME = "id: 5\ndata: x\n\n"
OURS = FRAME.encode("utf-8")

# The subagent spill writes a second ``record`` field alongside the rendered
# frame; it is the only place the entry names its writer.
RECORD = '{"seq": 5, "run": "run-a", "task_run": "trun-a"}'
OUR_RECORD = RECORD.encode("utf-8")
FOREIGN_RECORD = b'{"seq": 5, "run": "run-b", "task_run": "trun-b"}'


def _cache(**overrides) -> MagicMock:
    cache = MagicMock()
    cache.enabled = True
    cache.pipelined_event_buffer = AsyncMock(return_value=None)
    cache.stream_tail = AsyncMock(return_value=None)
    for k, v in overrides.items():
        setattr(cache, k, v)
    return cache


async def _append(cache, **kwargs):
    await stream_append_with_retry(
        cache,
        "workflow:stream:t1:r1",
        event_id=kwargs.pop("event_id", 5),
        max_size=1000,
        stream_event=FRAME,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_happy_path_writes_once():
    cache = _cache()
    await _append(cache)
    assert cache.pipelined_event_buffer.await_count == 1
    cache.stream_tail.assert_not_awaited()


@pytest.mark.asyncio
async def test_pool_exhaustion_replays_the_identical_write():
    """Nothing reached the server, so there is nothing to probe — and the
    epoch DEL is safe to repeat because it never ran."""
    cache = _cache()
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.ConnectionError("No connection available."),
            None,
        ]
    )
    await _append(cache, event_id=1)

    assert cache.pipelined_event_buffer.await_count == 2
    cache.stream_tail.assert_not_awaited()
    # Still not bare: nothing was sent, so the epoch DEL never ran and the
    # replay must carry it.
    assert cache.pipelined_event_buffer.await_args.kwargs["bare"] is False


@pytest.mark.asyncio
async def test_ambiguous_write_that_landed_is_accepted():
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    await _append(cache, event_id=5)

    assert cache.pipelined_event_buffer.await_count == 1
    cache.stream_tail.assert_awaited_once()


@pytest.mark.asyncio
async def test_tail_past_our_id_is_fatal_never_success():
    """The I6 guard. An auto-id frame (a recovery-appended error, or run_end)
    puts the tail far above our id; treating the resulting duplicate rejection
    as success would let the run complete with this frame missing."""
    cache = _cache(stream_tail=AsyncMock(return_value=(1769000000000, b"run_end", None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(StreamAppendError, match="past"):
        await _append(cache, event_id=5)

    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_our_id_carrying_other_bytes_is_fatal_never_success():
    """The other I6 guard, and the reason the probe reads the payload at all.

    A run's stream is DELed at event 1, so a crashed predecessor's frame can
    sit under the very id being written. The id matches; the write never
    happened. Accepting it archives a frame this run did not write.
    """
    cache = _cache(stream_tail=AsyncMock(return_value=(1, b"a predecessor", None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(StreamAppendError, match="different frame"):
        await _append(cache, event_id=1)

    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_an_entry_without_our_field_is_not_read_as_ours():
    """A foreign writer need not carry the ``event`` field; a missing payload
    must read as "not ours", never as a vacuous match."""
    cache = _cache(stream_tail=AsyncMock(return_value=(5, None, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(StreamAppendError, match="different frame"):
        await _append(cache, event_id=5)


@pytest.mark.asyncio
async def test_our_id_and_bytes_but_a_foreign_record_is_fatal():
    """The rendered SSE frame is not a complete witness.

    ``record`` carries the ownership ids the frame never renders, so two
    writers can produce byte-identical ``event`` fields for the same seq.
    Accepting on the frame alone hands the run an entry it did not write —
    and the collector withholds the whole subagent archive on one mismatch.
    """
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, FOREIGN_RECORD)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(StreamAppendError, match="different frame"):
        await _append(cache, event_id=5, stream_record=RECORD)

    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_our_id_bytes_and_record_together_are_accepted():
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, OUR_RECORD)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    await _append(cache, event_id=5, stream_record=RECORD)

    assert cache.pipelined_event_buffer.await_count == 1
    cache.stream_tail.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_entry_without_a_record_is_not_ours_when_we_write_one():
    """Same rule as the missing ``event`` field: absence is "not ours", never
    a vacuous match. Only a writer that omits ``record`` itself (the root lane)
    is allowed to accept on the frame alone."""
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    with pytest.raises(StreamAppendError, match="different frame"):
        await _append(cache, event_id=5, stream_record=RECORD)


@pytest.mark.asyncio
async def test_a_writer_that_omits_the_record_does_not_demand_one():
    """The check is one-sided on purpose: only a writer that supplies a record
    can be held to it. The root lane writes no ``record``, so its witness stays
    the frame alone and a recovered root-lane write is still accepted.
    """
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, b"someone else")))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    await _append(cache, event_id=5)

    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_ambiguous_write_that_did_not_land_is_retried_bare():
    """The heal PERSIST is dropped on retry: re-running it could re-immortalize
    a stream something else already stamped terminal."""
    cache = _cache(stream_tail=AsyncMock(return_value=(4, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.TimeoutError("Timeout reading from redis"),
            None,
        ]
    )

    await _append(cache, event_id=5)

    assert cache.pipelined_event_buffer.await_count == 2
    first, second = cache.pipelined_event_buffer.await_args_list
    assert first.kwargs["bare"] is False
    assert second.kwargs["bare"] is True


@pytest.mark.asyncio
async def test_ambiguous_epoch_reset_retries_without_repeating_the_del():
    """Repeating the DEL could erase a frame this process never wrote. Dropping
    it costs nothing: if it was needed but never ran, the leftover tail rejects
    our id instead of being silently overwritten."""
    cache = _cache(stream_tail=AsyncMock(return_value=None))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.TimeoutError("Timeout reading from redis"),
            None,
        ]
    )

    await _append(cache, event_id=1)

    assert cache.pipelined_event_buffer.await_count == 2
    first, second = cache.pipelined_event_buffer.await_args_list
    assert first.kwargs["bare"] is False
    assert second.kwargs["bare"] is True


@pytest.mark.asyncio
async def test_duplicate_id_on_a_first_attempt_is_fatal():
    """Nothing ambiguous has happened yet, so a rejected id means the stream
    already holds state this run did not write — not a lost reply."""
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.ResponseError(
            "The ID specified in XADD is equal or smaller than the target "
            "stream top item"
        )
    )

    with pytest.raises(StreamAppendError, match="first attempt"):
        await _append(cache, event_id=5)

    cache.stream_tail.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_non_duplicate_response_error_is_fatal_without_claiming_a_fence():
    """OOM / READONLY / NOPERM / MISCONF are ResponseErrors too.

    The outcome is the same fatal — the server refused, so probing would
    classify a provably-refused write as ambiguous — but the message must not
    say "rejected id", which sends the reader hunting a fence conflict that
    never happened.
    """
    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.OutOfMemoryError(
            "OOM command not allowed when used memory > 'maxmemory'"
        )
    )

    with pytest.raises(StreamAppendError) as excinfo:
        await _append(cache, event_id=5)

    assert "rejected id" not in str(excinfo.value)
    assert "OOM" in str(excinfo.value)
    cache.stream_tail.assert_not_awaited()
    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_loading_replays_the_identical_write_and_never_probes():
    """LOADING is a definitive refusal wearing a ConnectionError's clothes.

    The server answers ``-LOADING`` before the dataset is readable, so nothing
    was written. Classifying it ambiguous would latch ``bare`` — suppressing
    the epoch DEL — and buy a tail probe, the only path that can mistake a
    predecessor's frame for our own landed write.
    """
    assert issubclass(redis_exceptions.BusyLoadingError, redis_exceptions.ConnectionError)
    assert not issubclass(redis_exceptions.BusyLoadingError, redis_exceptions.ResponseError)

    cache = _cache(stream_tail=AsyncMock(return_value=(1, b"a predecessor", None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.BusyLoadingError(
                "Redis is loading the dataset in memory"
            ),
            None,
        ]
    )

    await _append(cache, event_id=1)

    assert cache.pipelined_event_buffer.await_count == 2
    cache.stream_tail.assert_not_awaited()
    # Nothing was written, so the epoch DEL never ran and the replay carries it.
    for call in cache.pipelined_event_buffer.await_args_list:
        assert call.kwargs["bare"] is False


@pytest.mark.asyncio
async def test_duplicate_id_after_an_ambiguous_attempt_reads_as_landed():
    cache = _cache(stream_tail=AsyncMock(side_effect=[None, (5, OURS, None)]))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.TimeoutError("Timeout reading from redis"),
            redis_exceptions.ResponseError(
                "The ID specified in XADD is equal or smaller than the target "
                "stream top item"
            ),
        ]
    )

    await _append(cache, event_id=5)

    assert cache.pipelined_event_buffer.await_count == 2


@pytest.mark.asyncio
async def test_failed_probe_reads_as_unknown_and_retries():
    """A probe that itself errors must not be read as "the stream is empty" —
    retrying the bare XADD is safe either way, the fence decides."""
    cache = _cache(
        stream_tail=AsyncMock(side_effect=redis_exceptions.RedisError("down"))
    )
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.TimeoutError("Timeout reading from redis"),
            None,
        ]
    )

    await _append(cache, event_id=5)
    assert cache.pipelined_event_buffer.await_count == 2


@pytest.mark.asyncio
async def test_unavailable_transport_is_not_retried():
    """Nothing to wait for — burning the retry budget only delays the failure."""
    cache = _cache()
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=EventBufferUnavailableError("disabled")
    )

    with pytest.raises(StreamAppendError):
        await _append(cache)

    assert cache.pipelined_event_buffer.await_count == 1


@pytest.mark.asyncio
async def test_a_recovered_write_is_counted_centrally_with_its_path(monkeypatch):
    """The process-local int is only a log throttle: under ``--workers N`` it
    sees 1/N of the traffic, so the counter is the fleet-wide signal."""
    from src.observability import metrics as obs_metrics

    recorded: list[tuple[int, dict]] = []

    class _FakeCounter:
        def add(self, value, attributes=None):
            recorded.append((value, attributes or {}))

    monkeypatch.setattr(obs_metrics, "redis_stream_writes_recovered", _FakeCounter())

    landed = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    landed.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )
    await _append(landed, event_id=5)

    rewritten = _cache(stream_tail=AsyncMock(return_value=None))
    rewritten.pipelined_event_buffer = AsyncMock(
        side_effect=[
            redis_exceptions.TimeoutError("Timeout reading from redis"),
            None,
        ]
    )
    await _append(rewritten, event_id=5)

    assert recorded == [(1, {"via": "tail_probe"}), (1, {"via": "rewrite"})]


@pytest.mark.asyncio
async def test_a_broken_counter_does_not_fail_a_write_that_survived(monkeypatch):
    """Telemetry sits on the recovery path — an exporter fault must not turn a
    write the run already survived into a fatal."""
    from src.observability import metrics as obs_metrics

    class _Exploding:
        def add(self, *_args, **_kwargs):
            raise RuntimeError("exporter down")

    monkeypatch.setattr(obs_metrics, "redis_stream_writes_recovered", _Exploding())

    cache = _cache(stream_tail=AsyncMock(return_value=(5, OURS, None)))
    cache.pipelined_event_buffer = AsyncMock(
        side_effect=redis_exceptions.TimeoutError("Timeout reading from redis")
    )

    await _append(cache, event_id=5)
