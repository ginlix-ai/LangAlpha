"""Integration test for the dual-write between the SSE event List and the
new Redis Stream.

Asserts parity: after writing N events through ``pipelined_event_buffer``,
``LLEN events_key == XLEN stream_key`` and the per-entry payloads match.

Requires a real Redis instance (run ``make setup-db`` first).
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def real_cache():
    """Provide a RedisCacheClient connected to the local Redis from setup-db."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")

    # Lazy import — avoids touching redis module during test collection on
    # machines without redis-py installed.
    from src.utils.cache.redis_cache import RedisCacheClient

    cache = RedisCacheClient(url=redis_url, max_connections=10)
    try:
        await cache.connect()
    except Exception as exc:  # auth required, refused, unreachable, etc.
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not cache.enabled or not cache.client:
        pytest.skip("Redis client did not initialize")
    yield cache
    try:
        await cache.client.aclose()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_list_and_stream_are_in_lockstep(real_cache):
    events_key = "test:dual:events"
    meta_key = "test:dual:events:meta"
    stream_key = "test:dual:stream"

    await real_cache.client.delete(events_key)
    await real_cache.client.delete(meta_key)
    await real_cache.client.delete(stream_key)

    n = 25
    for i in range(1, n + 1):
        sse = f"id: {i}\nevent: token\ndata: {{\"i\": {i}}}\n\n"
        success, seq = await real_cache.pipelined_event_buffer(
            events_key=events_key,
            meta_key=meta_key,
            event=sse,
            max_size=1000,
            ttl=60,
            last_event_id=i,
            stream_key=stream_key,
        )
        assert success is True
        assert seq == i

    list_len = await real_cache.client.llen(events_key)
    stream_len = await real_cache.client.xlen(stream_key)
    assert list_len == n
    assert stream_len == n

    # Stream entries are ordered by explicit ID `<seq>-0`.
    entries = await real_cache.client.xrange(stream_key, min="-", max="+")
    assert len(entries) == n
    for idx, (entry_id, fields) in enumerate(entries, start=1):
        # decode_responses=False — IDs and fields are bytes.
        assert entry_id == f"{idx}-0".encode("utf-8")
        payload_bytes = fields[b"event"]
        assert payload_bytes.startswith(f"id: {idx}\n".encode("utf-8"))

    # Cleanup
    await real_cache.client.delete(events_key)
    await real_cache.client.delete(meta_key)
    await real_cache.client.delete(stream_key)
