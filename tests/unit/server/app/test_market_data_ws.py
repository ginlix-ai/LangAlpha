"""Unit tests for the WS proxy three-role split (CMDP Phase 1).

Roles under test:
- role 2 (cache serializer)  → ``_buffer_tick`` buffers the canonical protocol
  record (from the precomputed ``to_protocol_record`` entry) into Redis,
  independent of any client's format.
- role 3 (outbound serializer) → ``_outbound_frame`` is raw passthrough by
  default and CMDP protocol frames under ``?format=cmdp``.

Also covers the ``ws_market_data_proxy`` route guards driven through a real
Starlette ``TestClient`` websocket handshake: pre-accept auth denial, the
interval allowlist, and control-frame robustness in the client message loop.
"""

import asyncio
import json
import time as _time
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI, WebSocketException, status
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from src.server.app import market_data_ws as mod

_AM_FRAME = (
    '{"ev":"AM","sym":"AAPL","o":100.0,"h":101.0,"l":99.5,"c":100.5,'
    '"v":1200,"s":1782000000000,"e":1782000060000}'
)
_PARSED = {
    "symbol": "AAPL", "time": 1782000000000, "open": 100.0,
    "high": 101.0, "low": 99.5, "close": 100.5, "volume": 1200,
}
_STATUS_MSG = '{"type":"status","status":"connected"}'


# ---------------------------------------------------------------------------
# role 2 — cache serializer (Redis buffer shape)
# ---------------------------------------------------------------------------


class TestCacheSerializer:
    def test_buffer_tick_writes_protocol_record(self):
        """The buffered value is the protocol record from the precomputed entry,
        regardless of client format. Hold the throttle so the tick stays in the
        pending buffer and no flush task is scheduled (keeps the test
        event-loop-free)."""
        key = mod._cache_key_for("AAPL", "stock", "1s")
        entry = mod.to_protocol_record(_PARSED, "stock", "second")
        mod._pending_bars.pop(key, None)
        # last flush "in the future" → now - last < interval → throttled hold
        mod._last_flush[key] = _time.monotonic() + 100.0
        try:
            mod._buffer_tick(_PARSED, "stock", "1s", entry)
            buffered = mod._pending_bars[key]
            assert len(buffered) == 1
            record = buffered[0]
            assert record["ts_event"] == 1782000000000
            assert record["time"] == 1782000000000  # legacy alias for merge/REST readers
            assert record["is_final"] is False
            assert record["close"] == 100.5
            # It's the flat record — NOT the wrapping entry, so existing envelope
            # readers (which key off top-level `time`/OHLCV) keep working.
            assert "symbol" not in record
            assert "instrument_key" not in record
            assert "record" not in record
        finally:
            mod._pending_bars.pop(key, None)
            mod._last_flush.pop(key, None)

    def test_buffer_tick_index_symbol(self):
        parsed = {**_PARSED, "symbol": "I:SPX", "volume": 0}
        entry = mod.to_protocol_record(parsed, "index", "second")
        key = mod._cache_key_for("I:SPX", "index", "1s")
        mod._pending_bars.pop(key, None)
        mod._last_flush[key] = _time.monotonic() + 100.0
        try:
            mod._buffer_tick(parsed, "index", "1s", entry)
            record = mod._pending_bars[key][0]
            assert record["ts_event"] == 1782000000000
            assert record["is_final"] is False
        finally:
            mod._pending_bars.pop(key, None)
            mod._last_flush.pop(key, None)

    def test_buffer_tick_drops_when_entry_none(self):
        """A None entry (un-canonicalizable symbol) is dropped, never buffered."""
        key = mod._cache_key_for("AAPL", "stock", "1s")
        mod._pending_bars.pop(key, None)
        mod._last_flush[key] = _time.monotonic() + 100.0
        try:
            mod._buffer_tick(_PARSED, "stock", "1s", None)
            assert key not in mod._pending_bars
        finally:
            mod._pending_bars.pop(key, None)
            mod._last_flush.pop(key, None)


# ---------------------------------------------------------------------------
# role 3 — per-client outbound serializer
# ---------------------------------------------------------------------------


class TestOutboundFrame:
    def test_default_is_raw_passthrough_byte_identical(self):
        entry = mod.to_protocol_record(_PARSED, "stock", "second")
        assert entry is not None  # sanity: this bar IS mappable
        out = mod._outbound_frame(_AM_FRAME, entry, cmdp=False)
        assert out == _AM_FRAME  # unchanged, exactly the upstream string

    def test_cmdp_wraps_aggregate_as_protocol_frame(self):
        entry = mod.to_protocol_record(_PARSED, "stock", "second")
        out = mod._outbound_frame(_AM_FRAME, entry, cmdp=True)
        frame = json.loads(out)
        assert frame["type"] == "ohlcv"
        assert frame["instrument_key"] == "AAPL.XNAS"
        assert frame["schema"] == "ohlcv-1s"
        assert frame["record"]["ts_event"] == 1782000000000
        assert frame["record"]["is_final"] is False

    def test_non_aggregate_passthrough_in_both_modes(self):
        # entry is None for non-aggregate upstream messages (status/keepalive)
        assert mod._outbound_frame(_STATUS_MSG, None, cmdp=False) == _STATUS_MSG
        assert mod._outbound_frame(_STATUS_MSG, None, cmdp=True) == _STATUS_MSG

    def test_cmdp_index_frame(self):
        parsed = {**_PARSED, "symbol": "I:SPX", "volume": 0}
        entry = mod.to_protocol_record(parsed, "index", "second")
        out = mod._outbound_frame('{"ev":"AM","sym":"I:SPX"}', entry, cmdp=True)
        frame = json.loads(out)
        assert frame["instrument_key"] == "SPX.INDEX"
        assert frame["type"] == "ohlcv"


# ---------------------------------------------------------------------------
# ws_market_data_proxy route — auth denial, interval allowlist, control frames
# ---------------------------------------------------------------------------


class _FakeHandle:
    """Stand-in FeedSubscription with observable async methods."""

    def __init__(self):
        self.subscribe = AsyncMock()
        self.unsubscribe = AsyncMock()
        self.close = AsyncMock()
        self.subscribed_symbols: set[str] = set()  # read by the subscribe cap


class _FakeFeed:
    """Stand-in MarketDataFeed instance — never emits, only records wiring."""

    def __init__(self):
        self.start = AsyncMock()
        self.handle = _FakeHandle()

    def register_consumer(self, consumer_id, callback):
        return self.handle


def _ws_app() -> TestClient:
    app = FastAPI()
    app.include_router(mod.router)
    return TestClient(app)


class TestWsRouteAuthDenied:
    """When ``authenticate_websocket`` rejects (before accept), the route bails
    out and never touches ``MarketDataFeed`` — so no consumer is registered."""

    def test_auth_failure_returns_without_registering_consumer(self, monkeypatch):
        async def _deny(ws):
            # ws_auth closes the socket then raises; the route relies on that.
            await ws.close(
                code=status.WS_1008_POLICY_VIOLATION, reason="Invalid or expired token"
            )
            raise WebSocketException(
                code=status.WS_1008_POLICY_VIOLATION, reason="Invalid or expired token"
            )

        monkeypatch.setattr(mod, "authenticate_websocket", AsyncMock(side_effect=_deny))
        get_instance = Mock()
        monkeypatch.setattr(mod.MarketDataFeed, "get_instance", get_instance)

        client = _ws_app()
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/v1/market-data/aggregates/stock?interval=second"
            ):
                pass
        assert exc.value.code == status.WS_1008_POLICY_VIOLATION
        # The load-bearing regression: the feed is never reached, so nothing is
        # started and no consumer is ever registered on the auth-denied path.
        get_instance.assert_not_called()


class TestWsIntervalAllowlist:
    """An interval outside ``_WS_INTERVAL_TO_CACHE`` is rejected with 1008
    before the feed is minted (bounds ``MarketDataFeed._instances`)."""

    def test_unknown_interval_closes_1008_before_get_instance(self, monkeypatch):
        # Auth would pass — the interval guard runs first, so it never matters.
        monkeypatch.setattr(mod, "authenticate_websocket", AsyncMock(return_value="u1"))
        get_instance = Mock()
        monkeypatch.setattr(mod.MarketDataFeed, "get_instance", get_instance)

        client = _ws_app()
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/v1/market-data/aggregates/stock?interval=hour"
            ):
                pass
        assert exc.value.code == 1008
        get_instance.assert_not_called()

    def test_allowed_interval_reaches_get_instance(self, monkeypatch):
        """Control: a whitelisted interval accepts and does mint the feed —
        proving the 1008 above is the interval guard, not a blanket refusal."""
        monkeypatch.setattr(mod, "authenticate_websocket", AsyncMock(return_value="u1"))
        feed = _FakeFeed()
        monkeypatch.setattr(mod.MarketDataFeed, "get_instance", Mock(return_value=feed))

        client = _ws_app()
        with client.websocket_connect(
            "/ws/v1/market-data/aggregates/stock?interval=minute"
        ) as ws:
            ws.send_json({"action": "ping"})
            assert ws.receive_json() == {"type": "pong"}
        feed.start.assert_awaited()


class TestWsControlFrames:
    """Client message loop shrugs off malformed control frames and keeps
    serving — a following valid subscribe still routes through the handle."""

    def _connect(self, monkeypatch):
        monkeypatch.setattr(mod, "authenticate_websocket", AsyncMock(return_value="u1"))
        feed = _FakeFeed()
        monkeypatch.setattr(mod.MarketDataFeed, "get_instance", Mock(return_value=feed))
        client = _ws_app()
        ws = client.websocket_connect(
            "/ws/v1/market-data/aggregates/stock?interval=second"
        )
        return feed, ws

    def test_non_object_json_frames_ignored_then_valid_subscribe_works(self, monkeypatch):
        feed, ws_ctx = self._connect(monkeypatch)
        with ws_ctx as ws:
            # Valid JSON but not a control object — a list and a bare string.
            ws.send_json([1, 2])
            ws.send_json("AAPL")
            # A genuine subscribe after the junk must still be honored.
            ws.send_json({"action": "subscribe", "symbols": ["AAPL"]})
            # ping/pong is the loop's in-order barrier: once the pong lands,
            # every earlier frame has already been processed.
            ws.send_json({"action": "ping"})
            assert ws.receive_json() == {"type": "pong"}
        feed.handle.subscribe.assert_awaited_once_with(["AAPL"])

    def test_string_symbols_are_not_subscribed_char_by_char(self, monkeypatch):
        feed, ws_ctx = self._connect(monkeypatch)
        with ws_ctx as ws:
            # symbols is a string, not a list — must be dropped, never expanded
            # into per-character subscriptions ("A", "A", "P", "L").
            ws.send_json({"action": "subscribe", "symbols": "AAPL"})
            ws.send_json({"action": "ping"})
            assert ws.receive_json() == {"type": "pong"}
            feed.handle.subscribe.assert_not_awaited()
            # And the loop is still alive: a well-formed list subscribe works.
            ws.send_json({"action": "subscribe", "symbols": ["AAPL"]})
            ws.send_json({"action": "ping"})
            assert ws.receive_json() == {"type": "pong"}
        feed.handle.subscribe.assert_awaited_once_with(["AAPL"])


# ---------------------------------------------------------------------------
# cache flush resilience — requeue, delayed flush, lineage, subscribe cap
# ---------------------------------------------------------------------------


def _flush_bar(t: int, close: float = 100.0) -> dict:
    return {
        "symbol": "AAPL", "time": t, "open": close,
        "high": close, "low": close, "close": close, "volume": 10,
    }


class _FlushStubCache:
    def __init__(self, store=None):
        self.store = dict(store or {})
        self.sets: list[tuple[str, dict, int | None]] = []

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ttl=None):
        self.sets.append((key, value, ttl))
        self.store[key] = value


class TestRequeuePending:
    def test_requeue_into_empty_buffer(self):
        key = "ohlcv:AAPL.XNAS:ohlcv-1s"
        mod._pending_bars.pop(key, None)
        batch = [_flush_bar(1000), _flush_bar(2000)]
        mod._requeue_pending(key, batch)
        assert mod._pending_bars.pop(key) == batch

    def test_requeue_prepends_and_buffered_version_wins_shared_time(self):
        key = "ohlcv:AAPL.XNAS:ohlcv-1s"
        mod._pending_bars[key] = [_flush_bar(2000, close=201.0), _flush_bar(3000)]
        mod._requeue_pending(key, [_flush_bar(1000), _flush_bar(2000, close=200.0)])
        merged = mod._pending_bars.pop(key)
        assert [b["time"] for b in merged] == [1000, 2000, 3000]
        assert merged[1]["close"] == 201.0  # ticks buffered after the pop are newer


class TestFlushResilience:
    @pytest.mark.asyncio
    async def test_lock_held_requeues_batch_and_schedules_retry(self, monkeypatch):
        """A delta refresh holding the shared lock must not cost the popped
        batch — the old early-return silently dropped every racing tick."""
        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        key = "ohlcv:AAPL.XNAS:ohlcv-1s"
        mod._pending_bars.pop(key, None)
        mod._scheduled_flushes.pop(key, None)

        lock = asyncio.Lock()
        await lock.acquire()
        svc = Mock()
        svc._get_refresh_lock = Mock(return_value=lock)
        monkeypatch.setattr(IntradayCacheService, "get_instance", Mock(return_value=svc))

        batch = [_flush_bar(1000)]
        try:
            await mod._flush_to_redis(key, batch)
            assert mod._pending_bars.get(key) == batch  # requeued, not dropped
            task = mod._scheduled_flushes.get(key)
            assert task is not None  # retry scheduled
        finally:
            lock.release()
            task = mod._scheduled_flushes.pop(key, None)
            if task:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            mod._pending_bars.pop(key, None)

    @pytest.mark.asyncio
    async def test_flush_preserves_rest_lineage(self, monkeypatch):
        """A WS tick over a REST-filled envelope keeps the REST publisher and
        revision — stamping it ginlix-data would pin future delta refreshes to
        the wrong provider."""
        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        key = "ohlcv:AAPL.XNAS:ohlcv-1m"
        existing = mod._build_envelope(
            [_flush_bar(1000)], "open", complete=False, stored_ttl=30, truncated=False,
            instrument_key="AAPL.XNAS", schema="ohlcv-1m", publisher="fmp", revision=3,
        )
        cache = _FlushStubCache({key: existing})
        monkeypatch.setattr(mod, "get_cache_client", lambda: cache)
        svc = Mock()
        svc._get_refresh_lock = Mock(return_value=asyncio.Lock())
        monkeypatch.setattr(IntradayCacheService, "get_instance", Mock(return_value=svc))

        await mod._flush_to_redis(key, [_flush_bar(2000, close=101.0)])

        written = mod._parse_envelope(cache.sets[-1][1])
        assert written["header"]["publisher"] == "fmp"
        assert written["header"]["revision"] == 3
        assert [b["time"] for b in written["bars"]] == [1000, 2000]

    @pytest.mark.asyncio
    async def test_flush_without_prior_envelope_is_ws_published(self, monkeypatch):
        from src.server.services.cache.intraday_cache_service import IntradayCacheService

        key = "ohlcv:AAPL.XNAS:ohlcv-1s"
        cache = _FlushStubCache()
        monkeypatch.setattr(mod, "get_cache_client", lambda: cache)
        svc = Mock()
        svc._get_refresh_lock = Mock(return_value=asyncio.Lock())
        monkeypatch.setattr(IntradayCacheService, "get_instance", Mock(return_value=svc))

        await mod._flush_to_redis(key, [_flush_bar(1000)])

        written = mod._parse_envelope(cache.sets[-1][1])
        assert written["header"]["publisher"] == mod._WS_SOURCE
        assert written["header"]["revision"] == 0

    @pytest.mark.asyncio
    async def test_throttled_tick_gets_delayed_flush(self, monkeypatch):
        """A tick inside the throttle window must still reach Redis via the
        one-shot delayed flush — a quiet symbol gets no next tick to carry it."""
        key = mod._cache_key_for("AAPL", "stock", "1s")
        mod._pending_bars.pop(key, None)
        mod._scheduled_flushes.pop(key, None)
        monkeypatch.setattr(mod, "_FLUSH_INTERVAL", 0.01)
        mod._last_flush[key] = _time.monotonic()  # hold the throttle

        flushed = asyncio.Event()
        calls: list[tuple[str, list]] = []

        async def fake_flush(cache_key, bars):
            calls.append((cache_key, bars))
            flushed.set()

        monkeypatch.setattr(mod, "_flush_to_redis", fake_flush)

        entry = mod.to_protocol_record(_PARSED, "stock", "second")
        try:
            mod._buffer_tick(_PARSED, "stock", "1s", entry)
            assert key in mod._scheduled_flushes
            await asyncio.wait_for(flushed.wait(), timeout=1.0)
            assert calls[0][0] == key and len(calls[0][1]) == 1
            assert key not in mod._pending_bars  # delivered, not stranded
        finally:
            mod._pending_bars.pop(key, None)
            mod._last_flush.pop(key, None)
            task = mod._scheduled_flushes.pop(key, None)
            if task:
                task.cancel()


class TestCapSubscribe:
    def test_malformed_and_oversize_entries_dropped(self):
        out = mod._cap_subscribe(set(), ["AAPL", 42, "", "X" * 25, None])
        assert out == ["AAPL"]

    def test_per_connection_budget_enforced(self, monkeypatch):
        monkeypatch.setattr(mod, "_MAX_WS_SYMBOLS", 3)
        out = mod._cap_subscribe({"A", "B"}, ["C", "D"])
        assert out == ["C"]

    def test_already_subscribed_symbols_cost_no_budget(self, monkeypatch):
        monkeypatch.setattr(mod, "_MAX_WS_SYMBOLS", 2)
        out = mod._cap_subscribe({"A", "B"}, ["a", "C"])
        assert out == ["a"]  # a re-subscribe passes at cap; the new symbol drops
