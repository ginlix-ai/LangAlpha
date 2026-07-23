"""Tests for the thread `/watch` SSE endpoint (src/server/app/threads.py).

Regression guard for the report-back persistent-watch fix: a flash thread can
dispatch N concurrent PTC analyses whose report-backs arrive as separate runs.
The watch must forward EVERY ``workflow_started`` wake on one pub/sub
subscription — the old one-shot ``break`` after the first wake dropped wake #2+,
so only the first report-back streamed and the rest needed a page refresh.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

THREADS_MOD = "src.server.app.threads.messaging"
AUTH_MOD = "src.server.utils.api"


def _pubsub_cache(queue):
    """A cache whose pubsub yields each Redis frame in `queue`, then pings."""

    async def fake_get_message(*_args, **_kwargs):
        # Yield to the loop so the client reader can drain each frame.
        await asyncio.sleep(0)
        return queue.pop(0) if queue else None

    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.get_message = fake_get_message
    pubsub.unsubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()

    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.pubsub = MagicMock(return_value=pubsub)
    return cache, pubsub


@pytest.mark.asyncio
async def test_watch_forwards_every_wake_on_one_subscription(threads_client):
    # Two report-back wakes, delivered as distinct runs on the same thread.
    queue = [
        {"type": "message", "data": b'{"run_id": "rb-1"}'},
        {"type": "message", "data": b'{"run_id": "rb-2"}'},
    ]
    cache, pubsub = _pubsub_cache(queue)

    # Once both wakes drain, jump the clock past the 30-min cap so the generator
    # hits its OWN timeout/break and ends the stream — deterministic, instead of
    # relying on a client-side cancel of the otherwise-infinite keepalive loop.
    real_monotonic = time.monotonic

    def fake_monotonic():
        return real_monotonic() + (0 if queue else 10**9)

    with patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()), patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch("time.monotonic", fake_monotonic):
        body = ""
        async with threads_client.stream(
            "GET", "/api/v1/threads/th-flash/watch"
        ) as resp:
            assert resp.status_code == 200
            async for line in resp.aiter_lines():
                body += line + "\n"

    # BOTH wakes arrived on ONE connection (the old `break` would yield only rb-1).
    assert "rb-1" in body
    assert "rb-2" in body
    # One subscription served the whole chain, torn down on disconnect.
    pubsub.subscribe.assert_awaited_once()
    pubsub.unsubscribe.assert_awaited_once()


@pytest.mark.asyncio
async def test_watch_emits_error_when_cache_unavailable(threads_client):
    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    with patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()), patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        async with threads_client.stream(
            "GET", "/api/v1/threads/th-flash/watch"
        ) as resp:
            assert resp.status_code == 200
            # The generator yields one error frame and returns, so the stream
            # ends on its own — read it fully (no infinite keepalive loop here).
            body = "".join([line async for line in resp.aiter_lines()])
    assert "watch unavailable" in body
