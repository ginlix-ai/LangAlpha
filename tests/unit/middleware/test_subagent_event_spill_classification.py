"""Failure classification for the v2 per-run (auto-id) subagent spill write.

The v2 leg has no id fence, and nothing downstream dedupes by payload seq, so
the only question that matters on a failed XADD is how far it got:

- pre-send  -> replay blind; nothing reached the server, a duplicate is impossible
- ambiguous -> prove the frame is absent (tail probe) before replaying
- fatal     -> open the circuit, which tears the subagent run

Before this, everything except pool exhaustion was fatal — so a single
health-check PING failure on an idle pooled connection threw away the finished
work of a subagent that had already succeeded.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.exceptions as redis_exceptions

from ptc_agent.agent.middleware.background_subagent import redis_stream
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)

THREAD_ID = "thread-x"
TASK_RUN_ID = "run-v2"

# redis-py 7.4.0 messages, verbatim in shape. Line refs are
# ``redis/asyncio/connection.py`` unless noted.
PING_HEALTH_CHECK = redis_exceptions.ConnectionError(
    "Bad response from PING health check"  # :629
)
CONNECT_REFUSED = redis_exceptions.ConnectionError(
    "Error 111 connecting to cache-host:6379. Connection refused."  # :387
)
POOL_EXHAUSTED = redis_exceptions.ConnectionError(
    "No connection available."  # :1691
)
READ_SIDE = redis_exceptions.ConnectionError(
    "Error while reading from cache-host:6379 : (104, 'Connection reset by peer')"  # :748
)
WRITE_SIDE = redis_exceptions.ConnectionError(
    "Error 32 while writing to socket. Broken pipe."  # :681
)
SOCKET_TIMEOUT = redis_exceptions.TimeoutError("Timeout reading from cache-host:6379")


def _event() -> dict:
    return {"event": "message_chunk", "data": {"content": "hi", "content_type": "text"}}


def _fake_cache(*, xadd_side_effect, tail: list | None = None) -> MagicMock:
    cache = MagicMock()
    cache.enabled = True
    # v1 fenced leg always succeeds here; these tests are about the v2 leg.
    cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))
    cache.client.xadd = AsyncMock(side_effect=xadd_side_effect)
    cache.client.xrevrange = AsyncMock(return_value=tail if tail is not None else [])
    return cache


async def _spill_one(monkeypatch, cache: MagicMock):
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: cache
    )
    monkeypatch.setattr(
        "src.config.settings.is_subagent_event_redis_spill_enabled", lambda: True
    )
    registry = BackgroundTaskRegistry(thread_id=THREAD_ID)
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.task_run_id = TASK_RUN_ID
    await registry.append_captured_event(task.tool_call_id, _event())
    return task


# --------------------------------------------------------------- classifier


@pytest.mark.parametrize(
    "exc,verdict",
    [
        (POOL_EXHAUSTED, "pre_send"),
        (redis_exceptions.MaxConnectionsError("Too many connections"), "pre_send"),
        (PING_HEALTH_CHECK, "pre_send"),
        (CONNECT_REFUSED, "pre_send"),
        (redis_exceptions.ConnectionError("Connection not ready"), "pre_send"),
        (redis_exceptions.ConnectionError("Connection has data"), "pre_send"),
        (READ_SIDE, "ambiguous"),
        # Conservative: drain() can also raise after the server executed a
        # fully-flushed command, so the write side is not provably pre-send.
        (WRITE_SIDE, "ambiguous"),
        (SOCKET_TIMEOUT, "ambiguous"),
        (redis_exceptions.ResponseError("WRONGTYPE"), "fatal"),
        (redis_exceptions.ConnectionError("Invalid RESP version"), "fatal"),
        (RuntimeError("boom"), "fatal"),
    ],
)
def test_classification_table(exc: BaseException, verdict: str) -> None:
    assert redis_stream._classify_v2_write_failure(exc) == verdict


def test_redis_timeout_is_not_builtin_timeout() -> None:
    """The pre-existing ``except asyncio.TimeoutError`` probe branch never saw a
    socket timeout — ``redis.exceptions.TimeoutError`` is a ``RedisError``."""
    assert not isinstance(SOCKET_TIMEOUT, asyncio.TimeoutError)
    assert redis_stream._classify_v2_write_failure(SOCKET_TIMEOUT) == "ambiguous"


# ------------------------------------------------------------- pre-send legs


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [PING_HEALTH_CHECK, CONNECT_REFUSED, POOL_EXHAUSTED])
async def test_pre_send_failure_replays_and_never_probes(monkeypatch, exc) -> None:
    """Nothing reached the server, so the identical write is replayed blind —
    no probe round-trip, and the run survives."""
    cache = _fake_cache(xadd_side_effect=[exc, b"5-0"])

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is False
    assert cache.client.xadd.await_count == 2
    cache.client.xrevrange.assert_not_awaited()


@pytest.mark.asyncio
async def test_health_check_failure_on_both_attempts_still_tears(monkeypatch) -> None:
    """Retrying is bounded: a Redis that is down for both attempts is still a
    torn run, not an unbounded loop."""
    cache = _fake_cache(xadd_side_effect=[PING_HEALTH_CHECK, PING_HEALTH_CHECK])

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is True
    assert cache.client.xadd.await_count == 2


# ------------------------------------------------------------ ambiguous legs


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [READ_SIDE, WRITE_SIDE, SOCKET_TIMEOUT])
async def test_ambiguous_failure_probes_then_retries(monkeypatch, exc) -> None:
    """The reply may have been lost after the server saw the write, so the tail
    is read first; an absent frame is then replayed."""
    cache = _fake_cache(xadd_side_effect=[exc, b"5-0"], tail=[])

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is False
    cache.client.xrevrange.assert_awaited_once()
    assert (
        cache.client.xrevrange.await_args.args[0]
        == f"subagent:stream:{THREAD_ID}:{TASK_RUN_ID}"
    )
    assert cache.client.xadd.await_count == 2


@pytest.mark.asyncio
async def test_ambiguous_failure_that_landed_is_never_replayed(monkeypatch) -> None:
    """The duplicate-token guard: a write found at the tail is delivered, so it
    is not written a second time under a new auto-id."""
    cache = _fake_cache(
        xadd_side_effect=[READ_SIDE, b"5-0"],
        tail=[(b"5-0", {b"payload": b'{"seq": 1}'})],
    )

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is False
    assert cache.client.xadd.await_count == 1


# --------------------------------------------------------------- fatal legs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc", [RuntimeError("boom"), redis_exceptions.ResponseError("WRONGTYPE")]
)
async def test_unclassified_failure_stays_fatal(monkeypatch, exc) -> None:
    """Unrecognized failures keep the old behavior: no probe, no replay, circuit
    open. Probing them would buy a replay for writes that may well have landed."""
    cache = _fake_cache(xadd_side_effect=exc)

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is True
    assert cache.client.xadd.await_count == 1
    cache.client.xrevrange.assert_not_awaited()


# ------------------------------------------------------- tail probe scanning


@pytest.mark.asyncio
async def test_probe_skips_control_frames_at_the_tail(monkeypatch) -> None:
    """The coordinator appends seq-less control frames (``lane_open``,
    ``run_end``) to the same key without holding ``redis_spill_lock``. Reading
    only the newest entry read one of those as "our write is missing" and
    replayed it — a user-visible duplicate token."""
    cache = _fake_cache(
        xadd_side_effect=[READ_SIDE, b"9-0"],
        tail=[
            (
                b"9-0",
                {
                    b"run_id": TASK_RUN_ID.encode(),
                    b"lane": b"task:abc123",
                    b"type": b"run_end",
                    b"payload": b'{"outcome": "completed"}',
                },
            ),
            (b"8-0", {b"payload": b'{"seq": 1}'}),
        ],
    )

    task = await _spill_one(monkeypatch, cache)

    assert task.redis_write_failed is False
    # Found under the control frame: delivered, so never replayed.
    assert cache.client.xadd.await_count == 1


@pytest.mark.asyncio
async def test_probe_reads_a_bounded_window() -> None:
    cache = MagicMock()
    cache.client.xrevrange = AsyncMock(return_value=[])

    assert await redis_stream._stream_tail_seq(cache, "k", b"payload") is None
    count = cache.client.xrevrange.await_args.kwargs["count"]
    assert 1 < count == redis_stream._TAIL_PROBE_SCAN <= 32


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tail,expected",
    [
        ([], None),
        ([(b"1-0", {b"payload": b'{"seq": 7}'})], 7),
        # lane_open, the other seq-less control frame.
        (
            [
                (b"2-0", {b"payload": b'{"task_run_id": "run-v2", "cause": "spawn"}'}),
                (b"1-0", {b"payload": b'{"seq": 7}'}),
            ],
            7,
        ),
        # Nothing but control frames: unknown, which reads as "not landed".
        ([(b"2-0", {b"payload": b'{"outcome": "completed"}'})], None),
        # Undecodable payload is skipped, not fatal to the scan.
        (
            [
                (b"2-0", {b"payload": b"not json"}),
                (b"1-0", {b"payload": b'{"seq": 7}'}),
            ],
            7,
        ),
        # Field missing entirely.
        ([(b"2-0", {b"event": b"x"}), (b"1-0", {b"payload": b'{"seq": 7}'})], 7),
    ],
)
async def test_probe_returns_newest_seq_bearing_frame(tail, expected) -> None:
    cache = MagicMock()
    cache.client.xrevrange = AsyncMock(return_value=tail)

    assert await redis_stream._stream_tail_seq(cache, "k", b"payload") == expected


@pytest.mark.asyncio
async def test_probe_failure_reads_as_unknown() -> None:
    cache = MagicMock()
    cache.client.xrevrange = AsyncMock(side_effect=READ_SIDE)

    assert await redis_stream._stream_tail_seq(cache, "k", b"payload") is None
