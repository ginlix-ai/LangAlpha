"""Locks for the per-worker report-back wake demux.

The reason this suite exists: under per-channel SUBSCRIBE, delivering one
thread's wake to another thread's viewer was structurally impossible. Pattern
subscribing moves that guarantee into ``_dispatch``, so the routing pin and the
no-cross-delivery pin below are load-bearing, not decorative.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.services.report_back.flash.keys import (
    parse_thread_wake_key,
    thread_wake_key,
    thread_wake_pattern,
)
from src.server.services.report_back.flash.wake_listener import (
    ThreadWakeListener,
    WakeSubscription,
    _QUEUE_MAX,
)


@pytest.fixture
def listener():
    """A fresh listener; the singleton is process-global."""
    ThreadWakeListener._instance = None
    inst = ThreadWakeListener.get_instance()
    yield inst
    ThreadWakeListener._instance = None


def _pmessage(thread_id: str, data: str) -> dict:
    return {
        "type": "pmessage",
        "pattern": thread_wake_pattern().encode(),
        "channel": thread_wake_key(thread_id).encode(),
        "data": data.encode(),
    }


class TestKeyParser:
    def test_round_trips_the_publisher_key(self):
        assert parse_thread_wake_key(thread_wake_key("th-1")) == "th-1"

    def test_ids_containing_the_separator_survive_verbatim(self):
        # The remainder is returned whole: routing is string equality against
        # the id the subscriber attached with, so splitting would be wrong.
        assert parse_thread_wake_key("thread:wake:a:b") == "a:b"

    @pytest.mark.parametrize(
        "channel",
        ["thread:wake:", "thread:wak:x", "turn:cancel", "", "xthread:wake:1"],
    )
    def test_rejects_anything_else(self, channel):
        assert parse_thread_wake_key(channel) is None


class TestDispatch:
    def test_delivers_to_the_addressed_thread(self, listener):
        sub = listener.attach("th-1")
        listener._dispatch(_pmessage("th-1", '{"run_id": "rb-1"}'))
        assert sub.queue.get_nowait() == '{"run_id": "rb-1"}'

    def test_never_crosses_threads(self, listener):
        mine = listener.attach("th-1")
        theirs = listener.attach("th-2")
        listener._dispatch(_pmessage("th-1", "mine"))
        assert mine.queue.get_nowait() == "mine"
        assert theirs.queue.empty()

    def test_fans_out_to_every_viewer_of_one_thread(self, listener):
        a = listener.attach("th-1")
        b = listener.attach("th-1")
        listener._dispatch(_pmessage("th-1", "wake"))
        assert a.queue.get_nowait() == "wake"
        assert b.queue.get_nowait() == "wake"

    def test_unroutable_channel_is_dropped_not_broadcast(self, listener):
        sub = listener.attach("th-1")
        listener._dispatch(
            {"type": "pmessage", "channel": b"not:a:wake:channel", "data": b"x"}
        )
        assert sub.queue.empty()

    def test_detach_stops_delivery(self, listener):
        sub = listener.attach("th-1")
        listener.detach(sub)
        listener._dispatch(_pmessage("th-1", "wake"))
        assert sub.queue.empty()
        assert "th-1" not in listener._subs

    def test_detach_is_idempotent(self, listener):
        sub = listener.attach("th-1")
        listener.detach(sub)
        listener.detach(sub)  # must not raise

    def test_attach_refuses_once_stopping(self, listener):
        listener._stopping.set()
        assert listener.attach("th-1") is None


class TestOverflow:
    def test_full_queue_degrades_to_a_resync(self, listener):
        sub = listener.attach("th-1")
        for i in range(_QUEUE_MAX + 1):
            listener._dispatch(_pmessage("th-1", f"w{i}"))
        assert sub.needs_resync is True
        # The backlog is gone — the snapshot supersedes it — leaving only the
        # nudge that wakes a viewer parked on get().
        assert sub.queue.get_nowait() is WakeSubscription.NUDGE
        assert sub.queue.empty()


class TestLiveness:
    def test_reconnect_rearms_every_viewer(self, listener):
        a = listener.attach("th-1")
        b = listener.attach("th-2")
        listener._go_live()
        a.needs_resync = b.needs_resync = False
        listener._go_dark()
        listener._go_live()
        # Anything published while dark is gone for good — pub/sub has no
        # replay — so neither viewer may resume deltas across the hole.
        assert (a.needs_resync, b.needs_resync) == (True, True)

    @pytest.mark.asyncio
    async def test_dark_for_is_zero_while_live(self, listener):
        listener.start()
        try:
            listener._go_live()
            assert listener.dark_for() == 0.0
            listener._go_dark()
            assert listener.dark_for() >= 0.0
        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_wait_live_times_out_when_never_subscribed(self, listener):
        assert await listener.wait_live(0.01) is False


def _pubsub(frames, *, ack_pattern=None, pong=True):
    """Fake pattern subscription: one psubscribe ack, then `frames`."""
    pubsub = MagicMock()
    pubsub.psubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    state = {"acked": False, "nonce": None}

    async def ping(message=None):
        state["nonce"] = message

    async def get_message(ignore_subscribe_messages=False, timeout=None):
        await asyncio.sleep(0)
        if not state["acked"]:
            state["acked"] = True
            pattern = ack_pattern if ack_pattern is not None else thread_wake_pattern()
            return {"type": "psubscribe", "channel": pattern.encode(), "data": 1}
        if frames:
            return frames.pop(0)
        if pong and state["nonce"] is not None:
            nonce, state["nonce"] = state["nonce"], None
            return {"type": "pong", "data": nonce.encode()}
        await asyncio.sleep(0.01)
        return None

    pubsub.get_message = get_message
    pubsub.ping = ping
    client = MagicMock()
    client.pubsub = MagicMock(return_value=pubsub)
    return client, pubsub


class TestSubscriptionLoop:
    @pytest.mark.asyncio
    async def test_goes_live_and_routes_a_real_frame(self, listener, monkeypatch):
        client, pubsub = _pubsub([_pmessage("th-1", "hello")])
        monkeypatch.setattr(
            "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
            AsyncMock(return_value=client),
        )
        sub = listener.attach("th-1")
        listener.start()
        try:
            assert await listener.wait_live(2.0) is True
            # Going live re-arms an already-attached viewer (it was attached
            # across a window with no registration), so the nudge leads.
            item = await asyncio.wait_for(sub.queue.get(), timeout=2.0)
            if item is WakeSubscription.NUDGE:
                item = await asyncio.wait_for(sub.queue.get(), timeout=2.0)
            assert item == "hello"
            pubsub.psubscribe.assert_awaited_once_with(thread_wake_pattern())
        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_ack_for_a_different_pattern_never_goes_live(
        self, listener, monkeypatch
    ):
        # An ack we didn't ask for is not proof of the registration we need.
        client, _ = _pubsub([], ack_pattern="thread:wake:th-1")
        monkeypatch.setattr(
            "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
            AsyncMock(return_value=client),
        )
        monkeypatch.setattr(
            "src.server.services.report_back.flash.wake_listener._ACK_TIMEOUT_S", 0.05
        )
        listener.start()
        try:
            assert await listener.wait_live(0.4) is False
        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_unanswered_ping_drops_a_blackholed_socket(
        self, listener, monkeypatch
    ):
        # get_message returns None forever on a blackholed socket without ever
        # disconnecting, so idleness alone can't be trusted.
        client, pubsub = _pubsub([], pong=False)
        monkeypatch.setattr(
            "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
            AsyncMock(return_value=client),
        )
        mod = "src.server.services.report_back.flash.wake_listener."
        monkeypatch.setattr(mod + "_PING_IDLE_S", 0.0)
        monkeypatch.setattr(mod + "_PING_TIMEOUT_S", 0.02)
        monkeypatch.setattr(mod + "_RETRY_BACKOFF_S", 0.01)
        listener.start()
        try:
            assert await listener.wait_live(2.0) is True
            for _ in range(200):
                await asyncio.sleep(0.01)
                if pubsub.aclose.await_count:
                    break
            assert pubsub.aclose.await_count >= 1
        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_never_goes_live_when_there_is_no_pubsub_pool(
        self, listener, monkeypatch
    ):
        monkeypatch.setattr(
            "src.server.services.workspace_status_pubsub.get_shared_pubsub_client",
            AsyncMock(return_value=None),
        )
        listener.start()
        try:
            assert await listener.wait_live(0.2) is False
        finally:
            await listener.stop()
