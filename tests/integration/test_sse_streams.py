"""Single-consumer integration tests for the Redis-Streams SSE path.

Validates the architectural win: every scenario (first-connect, reconnect,
second-tab, late-subscriber, subagent-live, subagent-reconnect) flows
through the SAME ``_stream_from_redis_log`` call with a different cursor.

Requires a real Redis instance — run ``make setup-db`` before invoking.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
import pytest_asyncio


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def real_cache():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")

    from src.utils.cache.redis_cache import RedisCacheClient

    cache = RedisCacheClient(url=redis_url, max_connections=10)
    try:
        await cache.connect()
    except Exception as exc:
        pytest.skip(f"Redis not reachable: {exc}")
    if not cache.enabled or not cache.client:
        pytest.skip("Redis client did not initialize")
    yield cache
    try:
        await cache.client.aclose()
    except Exception:
        pass


@pytest_asyncio.fixture
async def thread_id():
    """Per-test unique stream namespace so concurrent tests don't collide."""
    return f"itest-{uuid.uuid4().hex[:8]}"


async def _produce(cache, thread_id: str, n: int, stream_key: str = None):
    """Write n events to the workflow stream with explicit IDs `<i>-0`."""
    stream_key = stream_key or f"workflow:stream:{thread_id}"
    events_key = f"workflow:events:{thread_id}"
    meta_key = f"workflow:events:meta:{thread_id}"
    for i in range(1, n + 1):
        sse = f"id: {i}\nevent: token\ndata: {{\"i\": {i}}}\n\n"
        ok, _ = await cache.pipelined_event_buffer(
            events_key=events_key,
            meta_key=meta_key,
            event=sse,
            max_size=1000,
            ttl=60,
            last_event_id=i,
            stream_key=stream_key,
        )
        assert ok


async def _drain_until_empty(gen, max_events: int = 100, timeout: float = 5.0):
    """Collect SSE strings (excluding keepalives) until the generator exits."""
    out = []
    try:
        while len(out) < max_events:
            ev = await asyncio.wait_for(gen.__anext__(), timeout=timeout)
            if ev.startswith(":keepalive"):
                continue
            out.append(ev)
    except (StopAsyncIteration, asyncio.TimeoutError):
        pass
    return out


def _ids(events: list[str]) -> list[int]:
    """Pull integer ids from a list of SSE strings."""
    out = []
    for ev in events:
        first, _, _ = ev.partition("\n")
        try:
            out.append(int(first.replace("id: ", "").strip()))
        except ValueError:
            pass
    return out


@pytest.mark.asyncio
async def test_unified_consumer_paths_against_real_redis(real_cache, thread_id, monkeypatch):
    """One stream key, four consumers. Each scenario uses the same call."""
    from src.server.handlers.chat import stream_from_log as sfl_mod

    # Patch get_cache_client so the consumer uses our test client.
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: real_cache)

    # Simulate workflow lifecycle via a flag the terminal_check reads.
    workflow_done = asyncio.Event()

    async def terminal() -> bool:
        return workflow_done.is_set()

    stream_key = f"workflow:stream:{thread_id}"
    events_key = f"workflow:events:{thread_id}"
    meta_key = f"workflow:events:meta:{thread_id}"

    # Pre-clean.
    await real_cache.client.delete(stream_key, events_key, meta_key)

    # Produce 5 events.
    await _produce(real_cache, thread_id, n=5)

    # Scenario A: REPLAY from beginning (last_event_id=0). Should see all 5.
    a = sfl_mod._stream_from_redis_log(
        stream_key=stream_key,
        terminal_check=terminal,
        last_event_id=0,
    )
    workflow_done.set()
    a_events = await _drain_until_empty(a, max_events=10, timeout=2.0)
    assert _ids(a_events) == [1, 2, 3, 4, 5]

    # Reset terminal so next scenarios run with workflow "still active."
    workflow_done.clear()

    # Scenario B: RESUME after seq 2 → see 3, 4, 5.
    b = sfl_mod._stream_from_redis_log(
        stream_key=stream_key,
        terminal_check=terminal,
        last_event_id=2,
    )
    workflow_done.set()
    b_events = await _drain_until_empty(b, max_events=10, timeout=2.0)
    assert _ids(b_events) == [3, 4, 5]

    # Scenario C: SECOND-TAB attaches with last_event_id=None — the
    # implementation maps that to cursor=0 (replay everything, then wait),
    # not Redis Streams' "$" live-tail. The docstring on
    # ``_stream_from_redis_log`` calls this out: "$" is intentionally NOT
    # exposed because chat clients want history first, then live updates.
    # ``_produce(n=7)`` triggers the dirty-resume DEL inside the producer's
    # MULTI/EXEC (the ``last_event_id == 1`` branch in
    # ``redis_cache.pipelined_event_buffer``), then writes entries 1-0
    # through 7-0 atomically. The second-tab consumer, attaching after that
    # rewrite, replays the full 1-7 range.
    workflow_done.clear()

    second_tab_gen = sfl_mod._stream_from_redis_log(
        stream_key=stream_key,
        terminal_check=terminal,
        last_event_id=None,
    )
    await _produce(real_cache, thread_id, n=7)
    workflow_done.set()
    c_events = await _drain_until_empty(second_tab_gen, max_events=10, timeout=2.0)
    assert _ids(c_events) == [1, 2, 3, 4, 5, 6, 7]

    # Scenario D: LATE SUBSCRIBER after stream is DEL'd. XREAD on a non-
    # existent stream returns empty; with terminal=True we exit cleanly.
    await real_cache.client.delete(stream_key, events_key, meta_key)
    d = sfl_mod._stream_from_redis_log(
        stream_key=stream_key,
        terminal_check=terminal,
        last_event_id=0,
    )
    d_events = await _drain_until_empty(d, max_events=10, timeout=2.0)
    assert d_events == []
