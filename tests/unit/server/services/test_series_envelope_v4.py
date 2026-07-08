"""Phase 3 Series container (envelope v4) + key cutover + pinning contracts.

Covers the storage format (v4 header + records), the canonical
``ohlcv:{instrument_key}:{schema}`` key builder (spelling collapse), legacy v3
dual-read with adopt-on-read, splice-discontinuity refusal, and the series pin
(pinned publisher refills its own series; fallback re-pins after data write).
"""

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.server.services.cache._ohlcv_envelope import (
    ENVELOPE_VERSION,
    _build_envelope,
    _merge_bars,
    _parse_envelope,
    adopt_v3_envelope,
    canonical_series_key,
    pin_key,
    splice_is_discontinuous,
)
from src.server.services.cache.daily_cache_service import DailyCacheService

_MS = 1_750_000_000_000


def _bar(t, close=10.0, open_=10.0):
    return {"time": t, "open": open_, "high": close, "low": open_, "close": close, "volume": 100}


_ET = ZoneInfo("America/New_York")
# A fixed mid-session weekday moment. Freezing the clock pins the trading-date
# and market-open rollovers so an envelope built "fresh" stays fresh when the
# service re-evaluates staleness a few lines later.
_FROZEN_ET_NOON = datetime(2026, 7, 1, 12, 0, tzinfo=_ET)  # Wed, regular session


class _FrozenDatetime:
    """Minimal datetime shim so market_hours' ``datetime.now(ET)`` returns a
    fixed instant under monkeypatch (mirrors test_ohlcv_envelope_staleness)."""

    def __init__(self, now: datetime):
        self._now = now

    def now(self, tz=None):
        return self._now if tz is None else self._now.astimezone(tz)

    def combine(self, *args, **kwargs):
        from datetime import datetime as _dt

        return _dt.combine(*args, **kwargs)


def _fresh_v3(clock, close: float) -> dict:
    """A genuinely-fresh legacy v3 daily envelope for *clock*'s current instant."""
    import time as _t
    from datetime import date, datetime as _dt, time as dtime

    latest = date.fromisoformat(clock.expected_latest_daily_date())
    wm = int(_dt.combine(latest, dtime(), tzinfo=_ET).timestamp() * 1000)
    return {
        "v": 3, "bars": [_bar(wm, close)], "watermark": wm,
        "fetched_at": _t.time(), "market_phase": clock.market_phase(),
        "complete": clock.is_closed(), "stored_ttl": 3600,
        "data_date": clock.current_trading_date(), "truncated": False,
    }


# ---------------------------------------------------------------------------
# Canonical keys
# ---------------------------------------------------------------------------

class TestCanonicalSeriesKey:
    def test_live_key_shape(self):
        assert canonical_series_key("AAPL", "1hour") == "ohlcv:AAPL.XNAS:ohlcv-1h"

    def test_historical_key_appends_window(self):
        key = canonical_series_key(
            "AAPL", "1hour", "2026-06-01", "2026-06-05", live=False,
        )
        assert key == "ohlcv:AAPL.XNAS:ohlcv-1h:2026-06-01:2026-06-05"

    def test_every_index_spelling_collapses_to_one_key(self):
        keys = {
            canonical_series_key(spelling, "1s", is_index=True)
            for spelling in ("GSPC", "^GSPC", "I:SPX", "SPX")
        }
        assert keys == {"ohlcv:SPX.INDEX:ohlcv-1s"}

    def test_hk_and_daily_schema(self):
        assert canonical_series_key("0700.HK", "1day") == "ohlcv:0700.XHKG:ohlcv-1d"

    def test_pin_key_shape(self):
        assert pin_key("AAPL", "1min") == "pin:AAPL.XNAS:ohlcv-1m"
        assert pin_key("GSPC", "1day", is_index=True) == "pin:SPX.INDEX:ohlcv-1d"


# ---------------------------------------------------------------------------
# v4 storage form + working view
# ---------------------------------------------------------------------------

class TestEnvelopeV4:
    def test_build_produces_header_and_records(self):
        env = _build_envelope(
            [_bar(_MS)], "open", complete=False, stored_ttl=60,
            data_date="2026-07-03",
            instrument_key="AAPL.XNAS", schema="ohlcv-1h", publisher="ginlix-data",
        )
        assert env["v"] == ENVELOPE_VERSION
        h = env["header"]
        assert h["instrument_key"] == "AAPL.XNAS"
        assert h["schema"] == "ohlcv-1h"
        assert h["publisher"] == "ginlix-data"
        assert h["price_treatment"] == "split_adjusted"
        assert h["ts_unit"] == "ms"
        assert h["latest_trading_date"] == "2026-07-03"
        assert h["revision"] == 0
        assert h["watermark"] == _MS
        assert h["coverage"] == {"truncated": False}
        # Records carry ts_event alongside the legacy time alias.
        assert env["records"][0]["ts_event"] == _MS
        assert env["records"][0]["time"] == _MS
        # Cache-operational flags stay top-level.
        assert env["market_phase"] == "open"
        assert env["complete"] is False
        assert env["stored_ttl"] == 60

    def test_parse_v4_yields_working_view(self):
        env = _build_envelope(
            [_bar(_MS)], "closed", complete=True, stored_ttl=90, truncated=True,
            data_date="2026-07-03",
            instrument_key="0700.XHKG", schema="ohlcv-1h", publisher="yfinance",
        )
        w = _parse_envelope(env)
        assert w["bars"] == env["records"]
        assert w["watermark"] == _MS
        assert w["data_date"] == "2026-07-03"
        assert w["truncated"] is True
        assert w["complete"] is True
        assert w["market_phase"] == "closed"
        assert w["stored_ttl"] == 90
        assert w["header"]["publisher"] == "yfinance"
        assert w["header"]["tier"] == "delayed_15m"

    def test_parse_accepts_legacy_v3(self):
        v3 = {
            "v": 3, "bars": [_bar(_MS)], "watermark": _MS, "fetched_at": 1.0,
            "market_phase": "open", "complete": False, "stored_ttl": 60,
            "data_date": "2026-07-03", "truncated": False,
        }
        assert _parse_envelope(v3) is v3

    def test_parse_rejects_unknown_shapes(self):
        assert _parse_envelope(None) is None
        assert _parse_envelope({"v": 2, "bars": []}) is None
        assert _parse_envelope({"v": ENVELOPE_VERSION, "records": []}) is None  # header missing
        assert _parse_envelope({"v": 3}) is None  # bars missing

    def test_adopt_v3_preserves_operational_fields(self):
        v3 = {
            "v": 3, "bars": [_bar(_MS)], "watermark": _MS, "fetched_at": 123.0,
            "market_phase": "post", "complete": True, "stored_ttl": 300,
            "data_date": "2026-07-02", "truncated": True,
        }
        adopted = adopt_v3_envelope(v3, "VOD.XLON", "ohlcv-1d", publisher="fmp")
        h = adopted["header"]
        assert h["fetched_at"] == 123.0  # NOT re-stamped
        assert h["watermark"] == _MS
        assert h["latest_trading_date"] == "2026-07-02"
        assert h["publisher"] == "fmp"
        assert h["coverage"] == {"truncated": True}
        assert adopted["records"][0]["ts_event"] == _MS
        w = _parse_envelope(adopted)
        assert w["fetched_at"] == 123.0
        assert w["truncated"] is True


# ---------------------------------------------------------------------------
# Merge: ts_event awareness + discontinuity refusal
# ---------------------------------------------------------------------------

class TestMergeDiscontinuity:
    def test_merge_reads_ts_event_only_bars(self):
        existing = [_bar(_MS), _bar(_MS + 60_000)]
        delta = [
            {"ts_event": _MS + 60_000, "close": 11.0},
            {"ts_event": _MS + 120_000, "close": 12.0},
        ]
        merged = _merge_bars(existing, delta, watermark=_MS + 60_000)
        assert [b.get("ts_event") or b["time"] for b in merged] == [
            _MS, _MS + 60_000, _MS + 120_000,
        ]

    def test_identical_overlap_is_continuous(self):
        existing = [_bar(_MS, 10.0), _bar(_MS + 60_000, 11.0)]
        delta = [_bar(_MS, 10.0), _bar(_MS + 60_000, 99.0)]
        # Only the forming bar (at the watermark) differs — legitimate.
        assert splice_is_discontinuous(existing, delta, _MS + 60_000) is False

    def test_final_bar_shift_is_discontinuous(self):
        existing = [_bar(_MS, 10.0), _bar(_MS + 60_000, 11.0)]
        delta = [_bar(_MS, 5.0), _bar(_MS + 60_000, 5.5)]  # 2:1-split shaped
        assert splice_is_discontinuous(existing, delta, _MS + 60_000) is True

    def test_sub_tolerance_noise_is_continuous(self):
        existing = [_bar(_MS, 10.0)]
        delta = [_bar(_MS, 10.0001)]
        assert splice_is_discontinuous(existing, delta, _MS + 60_000) is False

    def test_disjoint_delta_is_continuous(self):
        existing = [_bar(_MS)]
        delta = [_bar(_MS + 120_000)]
        assert splice_is_discontinuous(existing, delta, _MS + 60_000) is False


# ---------------------------------------------------------------------------
# Service-level: dual-read adoption + pinning (daily service, stubbed cache)
# ---------------------------------------------------------------------------

class _StubCache:
    def __init__(self):
        self.store: dict[str, dict] = {}

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.store[key] = value


class _Provider:
    """Stub provider: chain fetch serves from `chain_source`; single-source
    fetch records the requested publisher and can be told to fail."""

    def __init__(self):
        self.source_names = ["ginlix-data", "fmp", "yfinance"]
        self.chain_source = "fmp"
        self.from_calls: list[str] = []
        self.fail_pinned = False

    def source_names_for(self, symbol, capability=None):
        return list(self.source_names)

    async def get_daily_with_source(self, symbol, from_date, to_date, is_index, user_id):
        return [_bar(_MS, 20.0)], self.chain_source, False

    async def get_daily_from(self, source_name, symbol, from_date=None, to_date=None,
                             is_index=False, user_id=None):
        self.from_calls.append(source_name)
        if self.fail_pinned:
            raise RuntimeError("pinned source down")
        return [_bar(_MS, 21.0)], source_name, False


@pytest.fixture
def daily_svc(monkeypatch):
    from src.server.services.cache import daily_cache_service as dcs

    DailyCacheService._instance = None
    provider = _Provider()
    cache = _StubCache()

    async def _get_provider():
        return provider

    monkeypatch.setattr(dcs, "get_market_data_provider", _get_provider)
    monkeypatch.setattr(dcs, "get_cache_client", lambda: cache)
    yield DailyCacheService.get_instance(), provider, cache
    DailyCacheService._instance = None


@pytest.mark.asyncio
async def test_legacy_v3_key_is_adopted_on_read(daily_svc, monkeypatch):
    svc, provider, cache = daily_svc
    from src.server.services.cache._instrument_clock import UsClock

    # Freeze the clock so the envelope built "fresh" here is still fresh when
    # get_stock_daily re-checks staleness — otherwise a trading-date (04:00 ET)
    # or market-open (09:30 ET) rollover between the two reads flips the result.
    monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(_FROZEN_ET_NOON))

    # Warm pre-cutover cache: v3 under the legacy fmp-segmented key, genuinely
    # fresh so no staleness path interferes with the adoption assertion.
    cache.store["ohlcv:fmp:stock:AAPL:1day"] = _fresh_v3(UsClock(), 15.0)

    result = await svc.get_stock_daily("AAPL")

    assert result.cached is True
    assert result.cache_key == "ohlcv:AAPL.XNAS:ohlcv-1d"
    assert result.data[0]["close"] == 15.0
    adopted = cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]
    assert adopted["v"] == ENVELOPE_VERSION
    assert adopted["header"]["publisher"] == "fmp"


@pytest.mark.asyncio
async def test_adoption_prefers_capability_ordered_source(monkeypatch):
    """Dual-read adopts from the capability-preferred publisher, not config order:
    the daily service threads capability 'daily' and follows that ordering."""
    from src.server.services.cache import daily_cache_service as dcs
    from src.server.services.cache._instrument_clock import UsClock

    class _CapProvider:
        def __init__(self):
            self.capabilities: list = []

        def source_names_for(self, symbol, capability=None):
            self.capabilities.append(capability)
            # Daily prefers fmp; every other capability prefers yfinance.
            return ["fmp", "yfinance"] if capability == "daily" else ["yfinance", "fmp"]

    DailyCacheService._instance = None
    provider = _CapProvider()
    cache = _StubCache()

    async def _get_provider():
        return provider

    monkeypatch.setattr(dcs, "get_market_data_provider", _get_provider)
    monkeypatch.setattr(dcs, "get_cache_client", lambda: cache)
    monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(_FROZEN_ET_NOON))
    try:
        svc = DailyCacheService.get_instance()
        clock = UsClock()
        # Both legacy source keys are warm and fresh with distinct closes; the
        # daily-preferred source (fmp) must win the adoption, not config order.
        cache.store["ohlcv:fmp:stock:AAPL:1day"] = _fresh_v3(clock, 15.0)
        cache.store["ohlcv:yfinance:stock:AAPL:1day"] = _fresh_v3(clock, 99.0)

        result = await svc.get_stock_daily("AAPL")

        assert "daily" in provider.capabilities  # capability threaded through
        assert result.data[0]["close"] == 15.0  # fmp (daily-preferred), not yfinance
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "fmp"
    finally:
        DailyCacheService._instance = None


@pytest.mark.asyncio
async def test_miss_writes_v4_and_pin(daily_svc):
    svc, provider, cache = daily_svc

    result = await svc.get_stock_daily("AAPL")

    assert result.cached is False
    assert result.cache_key == "ohlcv:AAPL.XNAS:ohlcv-1d"
    stored = cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]
    assert stored["v"] == ENVELOPE_VERSION
    assert stored["header"]["publisher"] == "fmp"
    assert cache.store["pin:AAPL.XNAS:ohlcv-1d"] == {"publisher": "fmp"}


@pytest.mark.asyncio
async def test_pinned_publisher_serves_next_miss(daily_svc):
    svc, provider, cache = daily_svc
    cache.store["pin:AAPL.XNAS:ohlcv-1d"] = {"publisher": "yfinance"}

    result = await svc.get_stock_daily("AAPL")

    assert provider.from_calls == ["yfinance"]
    assert result.data[0]["close"] == 21.0  # single-source payload, not chain
    assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "yfinance"


@pytest.mark.asyncio
async def test_pinned_failure_falls_back_and_repins(daily_svc):
    svc, provider, cache = daily_svc
    cache.store["pin:AAPL.XNAS:ohlcv-1d"] = {"publisher": "yfinance"}
    provider.fail_pinned = True

    result = await svc.get_stock_daily("AAPL")

    assert provider.from_calls == ["yfinance"]  # tried the pin first
    assert result.data[0]["close"] == 20.0  # chain payload
    # Data key written from the chain source, then the pin swapped to it.
    assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "fmp"
    assert cache.store["pin:AAPL.XNAS:ohlcv-1d"] == {"publisher": "fmp"}


@pytest.mark.asyncio
async def test_delta_refresh_uses_series_publisher(daily_svc):
    svc, provider, cache = daily_svc
    import time as _t
    key = "ohlcv:AAPL.XNAS:ohlcv-1d"
    cache.store[key] = _build_envelope(
        [_bar(_MS, 15.0)], "open", complete=False, stored_ttl=3600,
        data_date="2026-07-03",
        instrument_key="AAPL.XNAS", schema="ohlcv-1d", publisher="yfinance",
    )
    cache.store[key]["header"]["fetched_at"] = _t.time()

    await svc._delta_refresh(key, "AAPL", "1day")

    assert provider.from_calls == ["yfinance"]
    assert cache.store[key]["header"]["publisher"] == "yfinance"


@pytest.mark.asyncio
async def test_delta_discontinuity_triggers_full_refetch_and_revision_bump(daily_svc):
    svc, provider, cache = daily_svc
    import time as _t
    key = "ohlcv:AAPL.XNAS:ohlcv-1d"
    # Cached final bar at close=10; the stub's single-source fetch returns
    # close=21 for the same ts — a >0.5% disagreement with final history.
    cache.store[key] = _build_envelope(
        [_bar(_MS, 10.0), _bar(_MS + 86_400_000, 10.5)], "open",
        complete=False, stored_ttl=3600, data_date="2026-07-03",
        instrument_key="AAPL.XNAS", schema="ohlcv-1d", publisher="yfinance",
    )
    cache.store[key]["header"]["fetched_at"] = _t.time()

    await svc._delta_refresh(key, "AAPL", "1day")

    # Delta then full refetch — both from the pinned publisher only.
    assert provider.from_calls == ["yfinance", "yfinance"]
    stored = cache.store[key]
    assert stored["header"]["revision"] == 1
    assert [b["close"] for b in stored["records"]] == [21.0]


@pytest.mark.asyncio
async def test_concurrent_misses_still_coalesce(daily_svc):
    """Pin plumbing must not break the single-flight full-fetch lock."""
    svc, provider, cache = daily_svc
    calls = 0
    orig = provider.get_daily_with_source

    async def counting(*a, **k):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return await orig(*a, **k)

    provider.get_daily_with_source = counting
    results = await asyncio.gather(*(svc.get_stock_daily("AAPL") for _ in range(5)))
    assert calls == 1
    assert all(r.data for r in results)
