"""Unit tests for SharedWSConnectionManager — ref-counted subscriptions and message routing."""

import pytest
import pytest_asyncio

from src.server.services.shared_ws_manager import (
    SharedWSConnectionManager,
    WSConsumerHandle,
    parse_ws_bar,
)


# ---------------------------------------------------------------------------
# parse_ws_bar tests
# ---------------------------------------------------------------------------


class TestParseWsBar:
    def test_parse_raw_am_event(self):
        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":1234567,"s":1710000000000}'
        bar = parse_ws_bar(raw)
        assert bar is not None
        assert bar["symbol"] == "AAPL"
        assert bar["close"] == 176.0
        assert bar["time"] == 1710000000000

    def test_parse_raw_a_event(self):
        raw = '{"ev":"A","sym":"TSLA","o":250.0,"h":251.0,"l":249.0,"c":250.5,"v":500,"s":1710000001000}'
        bar = parse_ws_bar(raw)
        assert bar is not None
        assert bar["symbol"] == "TSLA"
        assert bar["close"] == 250.5

    def test_parse_wrapped_format(self):
        raw = '{"type":"aggregate","symbol":"MSFT","data":{"open":400,"high":401,"low":399,"close":400.5,"volume":100,"time":1710000000000}}'
        bar = parse_ws_bar(raw)
        assert bar is not None
        assert bar["symbol"] == "MSFT"
        assert bar["close"] == 400.5
        assert bar["time"] == 1710000000000

    def test_non_aggregate_returns_none(self):
        assert parse_ws_bar('{"type":"keepalive","time":1710000000000}') is None
        assert parse_ws_bar('{"type":"subscribed","symbols":["AAPL"]}') is None
        assert parse_ws_bar('{"type":"pong"}') is None

    def test_invalid_json_returns_none(self):
        assert parse_ws_bar("not json") is None
        assert parse_ws_bar("") is None

    def test_missing_close_returns_none(self):
        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"v":1234567,"s":1710000000000}'
        assert parse_ws_bar(raw) is None

    def test_timestamp_seconds_normalized_to_ms(self):
        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000}'
        bar = parse_ws_bar(raw)
        assert bar is not None
        assert bar["time"] == 1710000000000

    def test_symbol_uppercased(self):
        raw = '{"ev":"AM","sym":"aapl","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000000}'
        bar = parse_ws_bar(raw)
        assert bar["symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# SharedWSConnectionManager — subscription ref counting
# ---------------------------------------------------------------------------


class TestRefCounting:
    def setup_method(self):
        # Reset singleton for each test
        SharedWSConnectionManager._instance = None
        self.manager = SharedWSConnectionManager()

    def test_register_and_remove_consumer(self):
        callback = lambda raw, bar: None
        handle = self.manager.register_consumer("c1", callback)
        assert "c1" in self.manager._consumers
        self.manager._remove_consumer("c1")
        assert "c1" not in self.manager._consumers

    @pytest.mark.asyncio
    async def test_subscribe_increments_refcount(self):
        callback = lambda raw, bar: None
        handle = self.manager.register_consumer("c1", callback)
        await handle.subscribe(["AAPL", "TSLA"])
        assert self.manager._symbol_refcount["AAPL"] == 1
        assert self.manager._symbol_refcount["TSLA"] == 1
        assert handle.subscribed_symbols == {"AAPL", "TSLA"}

    @pytest.mark.asyncio
    async def test_multiple_consumers_same_symbol(self):
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])
        assert self.manager._symbol_refcount["AAPL"] == 2

    @pytest.mark.asyncio
    async def test_unsubscribe_decrements_refcount(self):
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])
        await h1.unsubscribe(["AAPL"])
        assert self.manager._symbol_refcount["AAPL"] == 1
        assert "AAPL" in self.manager._subscribed_symbols  # still subscribed upstream

    @pytest.mark.asyncio
    async def test_refcount_zero_removes_from_subscribed(self):
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h1.unsubscribe(["AAPL"])
        assert "AAPL" not in self.manager._symbol_refcount
        assert "AAPL" not in self.manager._subscribed_symbols

    @pytest.mark.asyncio
    async def test_close_handle_removes_all_subscriptions(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.subscribe(["AAPL", "TSLA", "MSFT"])
        await handle.close()
        assert "AAPL" not in self.manager._symbol_refcount
        assert "TSLA" not in self.manager._symbol_refcount
        assert "MSFT" not in self.manager._symbol_refcount
        assert "c1" not in self.manager._consumers

    @pytest.mark.asyncio
    async def test_duplicate_subscribe_is_idempotent(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.subscribe(["AAPL"])
        await handle.subscribe(["AAPL"])  # should not double-count
        assert self.manager._symbol_refcount["AAPL"] == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_unknown_symbol_is_noop(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.unsubscribe(["AAPL"])  # never subscribed
        assert "AAPL" not in self.manager._symbol_refcount


# ---------------------------------------------------------------------------
# SharedWSConnectionManager — message dispatch
# ---------------------------------------------------------------------------


class TestMessageDispatch:
    def setup_method(self):
        SharedWSConnectionManager._instance = None
        self.manager = SharedWSConnectionManager()

    @pytest.mark.asyncio
    async def test_dispatches_aggregate_to_subscribed_consumer(self):
        received = []

        async def callback(raw_msg, bar):
            received.append(bar)

        handle = self.manager.register_consumer("c1", callback)
        await handle.subscribe(["AAPL"])

        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000000}'
        await self.manager._dispatch_message(raw)

        assert len(received) == 1
        assert received[0]["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_does_not_dispatch_to_unsubscribed_consumer(self):
        received = []

        async def callback(raw_msg, bar):
            received.append(bar)

        handle = self.manager.register_consumer("c1", callback)
        await handle.subscribe(["TSLA"])  # subscribed to TSLA, not AAPL

        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000000}'
        await self.manager._dispatch_message(raw)

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_dispatches_non_aggregate_to_all_consumers(self):
        received_1 = []
        received_2 = []

        async def cb1(raw, bar):
            received_1.append(raw)

        async def cb2(raw, bar):
            received_2.append(raw)

        self.manager.register_consumer("c1", cb1)
        self.manager.register_consumer("c2", cb2)

        raw = '{"type":"keepalive","time":1710000000000}'
        await self.manager._dispatch_message(raw)

        assert len(received_1) == 1
        assert len(received_2) == 1

    @pytest.mark.asyncio
    async def test_dispatches_to_multiple_consumers_for_same_symbol(self):
        received_1 = []
        received_2 = []

        async def cb1(raw, bar):
            received_1.append(bar)

        async def cb2(raw, bar):
            received_2.append(bar)

        h1 = self.manager.register_consumer("c1", cb1)
        h2 = self.manager.register_consumer("c2", cb2)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])

        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000000}'
        await self.manager._dispatch_message(raw)

        assert len(received_1) == 1
        assert len(received_2) == 1

    @pytest.mark.asyncio
    async def test_consumer_callback_error_does_not_crash_dispatch(self):
        """If one consumer's callback raises, others still receive the message."""
        received = []

        async def bad_callback(raw, bar):
            raise RuntimeError("oops")

        async def good_callback(raw, bar):
            received.append(bar)

        h1 = self.manager.register_consumer("bad", bad_callback)
        h2 = self.manager.register_consumer("good", good_callback)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])

        raw = '{"ev":"AM","sym":"AAPL","o":175.5,"h":176.2,"l":175.1,"c":176.0,"v":100,"s":1710000000000}'
        await self.manager._dispatch_message(raw)

        assert len(received) == 1
