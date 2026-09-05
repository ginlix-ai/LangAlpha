"""Tests for the thread `/watch` SSE endpoint (src/server/app/threads.py).

Regression guard for the report-back persistent-watch fix: a flash thread can
dispatch N concurrent PTC analyses whose report-backs arrive as separate runs.
The watch must forward EVERY ``workflow_started`` wake on one pub/sub
subscription — the old one-shot ``break`` after the first wake dropped wake #2+,
so only the first report-back streamed and the rest needed a page refresh.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.report_back.flash.keys import (
    thread_wake_key,
    thread_wake_pattern,
)
from src.server.services.report_back.flash import wake
from src.server.services.report_back.flash.wake_listener import ThreadWakeListener

THREADS_MOD = "src.server.app.threads.messaging"
AUTH_MOD = "src.server.utils.api"


@pytest.fixture
def wake_listener():
    """A fresh demux; the listener is a process-global singleton."""
    ThreadWakeListener._instance = None
    inst = ThreadWakeListener.get_instance()
    yield inst
    ThreadWakeListener._instance = None


def _pattern_pubsub(frames, ready):
    """A pattern subscription: acks each psubscribe, then serves `frames`.

    Frames are withheld until ``ready()``. Pub/sub has no replay, so a wake
    the listener consumes before the viewer attaches is genuinely gone —
    that's what the snapshot covers, and it is not what this test is pinning.
    """
    state = {"pending_ack": 0}

    async def psubscribe(_pattern):
        state["pending_ack"] += 1

    async def get_message(ignore_subscribe_messages=False, timeout=None):
        # Yield to the loop so the client reader can drain each frame.
        await asyncio.sleep(0)
        if state["pending_ack"]:
            state["pending_ack"] -= 1
            return {
                "type": "psubscribe",
                "channel": thread_wake_pattern().encode(),
                "data": 1,
            }
        if frames and ready():
            return frames.pop(0)
        await asyncio.sleep(0.01)
        return None

    pubsub = MagicMock()
    pubsub.psubscribe = AsyncMock(side_effect=psubscribe)
    pubsub.get_message = get_message
    pubsub.ping = AsyncMock()
    pubsub.aclose = AsyncMock()

    client = MagicMock()
    client.pubsub = MagicMock(return_value=pubsub)
    return client, pubsub


def _wake_frame(thread_id: str, payload: bytes) -> dict:
    return {
        "type": "pmessage",
        "channel": thread_wake_key(thread_id).encode(),
        "data": payload,
    }


@pytest.mark.asyncio
async def test_watch_forwards_every_wake_on_one_subscription(
    threads_client, wake_listener
):
    # Two report-back wakes, delivered as distinct runs on the same thread.
    client, pubsub = _pattern_pubsub(
        [
            _wake_frame("th-flash", b'{"run_id": "rb-1"}'),
            _wake_frame("th-flash", b'{"run_id": "rb-2"}'),
        ],
        ready=lambda: "th-flash" in wake_listener._subs,
    )

    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()

    # End the stream server-side on its own max-duration close, rather than
    # by cancelling the client: the ASGI transport runs the generator inline,
    # so a client-side break would leave it producing keepalives for 30 min.
    with patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()), patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
        AsyncMock(return_value=client),
    ), patch.object(wake, "WAKE_KEEPALIVE_INTERVAL", 0.05), patch.object(
        wake, "WAKE_MAX_WATCH_DURATION", 2.0
    ):
        wake_listener.start()
        try:
            body = ""
            async with threads_client.stream(
                "GET", "/api/v1/threads/th-flash/watch"
            ) as resp:
                assert resp.status_code == 200
                async for line in resp.aiter_lines():
                    body += line + "\n"
        finally:
            await wake_listener.stop()

    # BOTH wakes arrived on ONE connection (the old `break` would yield only rb-1).
    assert "rb-1" in body
    assert "rb-2" in body
    # ONE pattern subscription served the viewer — not one per viewer, which is
    # what pinned a shared-pool connection per open tab.
    pubsub.psubscribe.assert_awaited_once_with(thread_wake_pattern())
    # And the viewer's slot is gone. detach is synchronous, so a disconnect —
    # which tears the generator down under cancellation — cannot skip it.
    assert "th-flash" not in wake_listener._subs


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
