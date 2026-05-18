"""Integration test for the subagent stream-only dual-payload path.

After the cutover, the subagent caller writes a single XADD entry per event
with two fields: ``b"event"`` (pre-rendered SSE wire string for live
consumers) and ``b"record"`` (JSON record for the post-turn collector's
XRANGE read). No List is involved.

Requires a real Redis instance (run ``make setup-db`` first).
"""

from __future__ import annotations

import json
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

    from src.utils.cache.redis_cache import RedisCacheClient

    cache = RedisCacheClient(url=redis_url, max_connections=10)
    try:
        await cache.connect()
    except Exception as exc:
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not cache.enabled or not cache.client:
        pytest.skip("Redis client did not initialize")
    yield cache
    try:
        await cache.client.aclose()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_subagent_xadd_carries_event_and_record_fields(real_cache):
    """Each XADD entry carries both fields; XRANGE recovers ordered records."""
    meta_key = "test:dual:events:meta"
    stream_key = "test:dual:stream"

    await real_cache.client.delete(meta_key)
    await real_cache.client.delete(stream_key)

    n = 25
    for i in range(1, n + 1):
        sse = f"id: {i}\nevent: token\ndata: {{\"i\": {i}}}\n\n"
        record_payload = json.dumps(
            {"seq": i, "event": "token", "data": {"i": i}, "agent_id": "x"}
        )
        success, seq = await real_cache.pipelined_event_buffer(
            meta_key=meta_key,
            event=sse,
            max_size=1000,
            ttl=60,
            last_event_id=i,
            stream_key=stream_key,
            stream_event=sse,
            stream_record=record_payload,
        )
        assert success is True
        assert seq == i

    stream_len = await real_cache.client.xlen(stream_key)
    assert stream_len == n

    entries = await real_cache.client.xrange(stream_key, min="-", max="+")
    assert len(entries) == n
    for idx, (entry_id, fields) in enumerate(entries, start=1):
        assert entry_id == f"{idx}-0".encode("utf-8")
        # b"event" is the SSE wire string for live consumers.
        assert fields[b"event"].startswith(f"id: {idx}\n".encode("utf-8"))
        # b"record" is the JSON record for the post-turn collector.
        record = json.loads(fields[b"record"].decode("utf-8"))
        assert record["seq"] == idx
        assert record["event"] == "token"

    await real_cache.client.delete(meta_key)
    await real_cache.client.delete(stream_key)
