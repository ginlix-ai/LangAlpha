"""Unit tests for MarketDataFeed — ref-counted subscriptions and message routing."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from src.server.services import market_data_feed as feed_mod
from src.server.services.market_data_feed import (
    MarketDataFeed,
    FeedSubscription,
    parse_ws_bar,
    to_protocol_record,
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
# to_protocol_record — upstream parser → CMDP record entry (WS role 1)
# ---------------------------------------------------------------------------


class TestToProtocolRecord:
    def test_stock_am_frame_golden(self):
        """AM frame → protocol entry: AAPL → AAPL.XNAS, second feed → ohlcv-1s,
        is_final False, ts_event = the `s` (window-open) field."""
        parsed = parse_ws_bar(
            '{"ev":"AM","sym":"AAPL","o":100.0,"h":101.0,"l":99.5,"c":100.5,'
            '"v":1200,"s":1782000000000,"e":1782000060000}'
        )
        entry = to_protocol_record(parsed, "stock")
        assert entry is not None
        assert entry["instrument_key"] == "AAPL.XNAS"
        assert entry["schema"] == "ohlcv-1s"
        record = entry["record"]
        assert record["ts_event"] == 1782000000000  # `s`, not `e`
        assert record["time"] == 1782000000000  # legacy alias retained
        assert record["is_final"] is False
        assert record["open"] == 100.0
        assert record["high"] == 101.0
        assert record["low"] == 99.5
        assert record["close"] == 100.5
        assert record["volume"] == 1200

    def test_index_symbol_maps_to_canonical_index_key(self):
        """Index feeds carry the Polygon `I:SPX` wire spelling → SPX.INDEX."""
        parsed = parse_ws_bar(
            '{"ev":"AM","sym":"I:SPX","o":5000.0,"h":5010.0,"l":4990.0,'
            '"c":5005.0,"v":0,"s":1782000000000}'
        )
        entry = to_protocol_record(parsed, "index")
        assert entry is not None
        assert entry["instrument_key"] == "SPX.INDEX"
        assert entry["schema"] == "ohlcv-1s"
        assert entry["record"]["is_final"] is False

    def test_minute_interval_maps_to_ohlcv_1m(self):
        parsed = parse_ws_bar(
            '{"ev":"AM","sym":"AAPL","o":1,"h":1,"l":1,"c":1,"v":1,"s":1782000000000}'
        )
        entry = to_protocol_record(parsed, "stock", "minute")
        assert entry is not None
        assert entry["schema"] == "ohlcv-1m"

    def test_unknown_interval_returns_none(self):
        parsed = parse_ws_bar(
            '{"ev":"AM","sym":"AAPL","o":1,"h":1,"l":1,"c":1,"v":1,"s":1782000000000}'
        )
        assert to_protocol_record(parsed, "stock", "hour") is None

    def test_record_validates_through_ohlcv_bar(self):
        """The record is an OhlcvBar dump — carries the protocol fields and no
        stray keys from the parsed dict (e.g. `symbol`)."""
        parsed = parse_ws_bar(
            '{"ev":"AM","sym":"AAPL","o":1,"h":1,"l":1,"c":1,"v":1,"s":1782000000000}'
        )
        record = to_protocol_record(parsed, "stock")["record"]
        assert "symbol" not in record
        assert {"ts_event", "time", "open", "high", "low", "close", "volume", "is_final"} <= set(record)


# ---------------------------------------------------------------------------
# MarketDataFeed — subscription ref counting
# ---------------------------------------------------------------------------


class TestRefCounting:
    def setup_method(self):
        # Reset singletons for each test
        MarketDataFeed._instances = {}
        self.manager = MarketDataFeed()

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
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 1
        assert self.manager._symbol_refcount["TSLA.XNAS"] == 1
        assert handle.subscribed_symbols == {"AAPL", "TSLA"}

    @pytest.mark.asyncio
    async def test_multiple_consumers_same_symbol(self):
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 2

    @pytest.mark.asyncio
    async def test_unsubscribe_decrements_refcount(self):
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])
        await h1.unsubscribe(["AAPL"])
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 1
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
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_unknown_symbol_is_noop(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.unsubscribe(["AAPL"])  # never subscribed
        assert "AAPL" not in self.manager._symbol_refcount

    @pytest.mark.asyncio
    async def test_duplicate_unsubscribe_decrements_once(self):
        """A repeated symbol in one unsubscribe frame must count once — a
        double decrement would cut off the other consumer's feed."""
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["AAPL"])
        await h2.subscribe(["AAPL"])
        await h1.unsubscribe(["AAPL", "AAPL"])
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 1
        assert "AAPL" in self.manager._subscribed_symbols  # c2 keeps its feed


# ---------------------------------------------------------------------------
# MarketDataFeed — message dispatch
# ---------------------------------------------------------------------------


class TestMessageDispatch:
    def setup_method(self):
        MarketDataFeed._instances = {}
        self.manager = MarketDataFeed()

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


class TestInstrumentKeyRefcounting:
    """Phase 4: refcount + dispatch key on canonical instrument_key, so every
    spelling of one instrument shares a single upstream subscription."""

    def setup_method(self):
        MarketDataFeed._instances = {}
        self.manager = MarketDataFeed(market="index", interval="second", tier="realtime")

    @pytest.mark.asyncio
    async def test_spellings_collapse_to_one_upstream_subscribe(self):
        sent = []
        self.manager._send_subscribe = AsyncMock(side_effect=lambda syms: sent.extend(syms))
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["GSPC"])
        await h2.subscribe(["^GSPC"])
        assert self.manager._symbol_refcount["SPX.INDEX"] == 2
        assert sent == ["GSPC"]  # one wire subscribe, first spelling wins

    @pytest.mark.asyncio
    async def test_upstream_unsubscribe_only_when_last_spelling_leaves(self):
        unsent = []
        self.manager._send_subscribe = AsyncMock()
        self.manager._send_unsubscribe = AsyncMock(side_effect=lambda syms: unsent.extend(syms))
        h1 = self.manager.register_consumer("c1", lambda r, b: None)
        h2 = self.manager.register_consumer("c2", lambda r, b: None)
        await h1.subscribe(["GSPC"])
        await h2.subscribe(["I:SPX"])
        await h1.unsubscribe(["GSPC"])
        assert unsent == []
        await h2.unsubscribe(["I:SPX"])
        assert unsent == ["GSPC"]  # wire symbol of the original subscribe

    @pytest.mark.asyncio
    async def test_one_consumer_two_spellings_survives_partial_unsubscribe(self):
        """One consumer holding two spellings of one instrument keeps delivery
        after unsubscribing just one; the wire unsubscribe waits for the last."""
        unsent = []
        got = []
        self.manager._send_subscribe = AsyncMock()
        self.manager._send_unsubscribe = AsyncMock(side_effect=lambda syms: unsent.extend(syms))

        async def cb(raw, bar):
            got.append(bar["symbol"])

        handle = self.manager.register_consumer("c1", cb)
        await handle.subscribe(["GSPC"])
        await handle.subscribe(["I:SPX"])  # second spelling of the same instrument
        assert self.manager._symbol_refcount["SPX.INDEX"] == 2

        await handle.unsubscribe(["GSPC"])
        assert unsent == []  # I:SPX still holds the instrument — no wire unsubscribe

        # Bars still route to the consumer via the surviving canonical key.
        raw = json.dumps({
            "ev": "AM", "sym": "GSPC", "o": 1.0, "h": 1.0,
            "l": 1.0, "c": 1.0, "v": 0, "s": 1750000000000,
        })
        await self.manager._dispatch_message(raw)
        assert got == ["GSPC"]

        await handle.unsubscribe(["I:SPX"])
        assert unsent == ["GSPC"]  # last spelling gone → wire unsubscribe fires
        assert "SPX.INDEX" not in self.manager._symbol_refcount

    @pytest.mark.asyncio
    async def test_dispatch_reverse_maps_bar_symbol_to_subscribers(self):
        got = []

        async def cb(raw, bar):
            got.append(bar["symbol"])

        handle = self.manager.register_consumer("c1", cb)
        await handle.subscribe(["^GSPC"])
        raw = json.dumps({
            "ev": "AM", "sym": "GSPC", "o": 1.0, "h": 1.0,
            "l": 1.0, "c": 1.0, "v": 0, "s": 1750000000000,
        })
        await self.manager._dispatch_message(raw)
        assert got == ["GSPC"]  # delivered despite the caret spelling


class TestIntraCallDedupe:
    """A duplicate symbol inside a single ``subscribe()`` call must count once.
    Otherwise the manager refcount inflates past what ``close()``/``unsubscribe``
    can ever decrement, stranding a phantom upstream subscription forever."""

    def setup_method(self):
        MarketDataFeed._instances = {}
        self.manager = MarketDataFeed()  # stock market → AAPL.XNAS
        self.sent: list[str] = []
        self.unsent: list[str] = []
        self.manager._send_subscribe = AsyncMock(
            side_effect=lambda syms: self.sent.extend(syms)
        )
        self.manager._send_unsubscribe = AsyncMock(
            side_effect=lambda syms: self.unsent.extend(syms)
        )

    @pytest.mark.asyncio
    async def test_duplicate_within_one_call_counts_once(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.subscribe(["AAPL", "AAPL"])
        assert self.manager._symbol_refcount["AAPL.XNAS"] == 1
        assert self.sent == ["AAPL"]  # a single upstream subscribe, not two
        assert handle.subscribed_symbols == {"AAPL"}

    @pytest.mark.asyncio
    async def test_close_after_duplicate_releases_fully(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.subscribe(["AAPL", "AAPL"])
        await handle.close()
        assert "AAPL.XNAS" not in self.manager._symbol_refcount
        assert "AAPL" not in self.manager._subscribed_symbols
        assert self.unsent == ["AAPL"]  # exactly one upstream unsubscribe
        assert "c1" not in self.manager._consumers

    @pytest.mark.asyncio
    async def test_unsubscribe_after_duplicate_releases_fully(self):
        handle = self.manager.register_consumer("c1", lambda r, b: None)
        await handle.subscribe(["AAPL", "AAPL"])
        await handle.unsubscribe(["AAPL"])
        assert "AAPL.XNAS" not in self.manager._symbol_refcount
        assert "AAPL" not in self.manager._subscribed_symbols
        assert self.unsent == ["AAPL"]  # one release fully clears the refcount
        # The consumer's own alias counter is drained too — no leaked entry.
        assert self.manager._consumer_symbols["c1"].get("AAPL.XNAS", 0) == 0


class TestConnectionBackoff:
    """`_connection_loop` only resets backoff to ``_INITIAL_BACKOFF`` after a
    *productive* connection (≥1 message). An accept-then-immediate-close upstream
    (bad token, rate limit) delivers zero messages and must keep backing off."""

    @pytest.mark.asyncio
    async def test_backoff_keeps_doubling_when_unproductive(self, monkeypatch):
        b = feed_mod._INITIAL_BACKOFF
        timeouts = await self._drive(monkeypatch, [0, 0, 0])
        # Zero messages every time → never reset → 1s, 2s, 4s.
        assert timeouts == [b, 2 * b, 4 * b]

    @pytest.mark.asyncio
    async def test_backoff_resets_after_a_productive_connection(self, monkeypatch):
        b = feed_mod._INITIAL_BACKOFF
        timeouts = await self._drive(monkeypatch, [0, 0, 5, 5])
        # Doubles while unproductive (1s, 2s), then the first message-bearing
        # connection snaps it back to the initial floor (1s, 1s).
        assert timeouts == [b, 2 * b, b, b]

    async def _drive(self, monkeypatch, message_counts: list[int]) -> list[float]:
        """Drive ``_connection_loop`` deterministically with no real sleeps.

        Each simulated connection sets ``_messages_since_connect`` to the next
        value in *message_counts*; ``asyncio.wait_for`` (the backoff sleep) is
        stubbed to record its ``timeout`` and end the loop once every connection
        has been consumed. Returns the captured backoff timeouts, in order.
        """
        feed = MarketDataFeed()
        idx = {"i": 0}

        def _connect() -> None:
            feed._messages_since_connect = message_counts[idx["i"]]
            idx["i"] += 1

        feed._connect_and_receive = AsyncMock(side_effect=_connect)
        timeouts: list[float] = []

        async def _fake_wait_for(awaitable, timeout):
            awaitable.close()
            timeouts.append(timeout)
            if len(timeouts) >= len(message_counts):
                return
            raise asyncio.TimeoutError

        monkeypatch.setattr(feed_mod.asyncio, "wait_for", _fake_wait_for)
        await feed._connection_loop()
        return timeouts
