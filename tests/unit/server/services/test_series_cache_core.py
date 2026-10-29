"""Unit tests for the OHLCV series-cache core helpers.

Covers ``is_live_window`` — the "may this window still grow?" TTL gate. The
guard compares *to_date* against the western-most plausible venue-local date
(UTC minus 12h), NOT the server's ``date.today()``. On a UTC host that matters:
at 00:30 UTC an ET trading window is still "today" in New York (20:30 the prior
day), so the window must read live. A naive ``date.today()`` on UTC would flip
it historical at 00:00 UTC and freeze the live evening session.
"""

import datetime as dt

import pytest

from src.server.services.cache import _series_cache_core as mod
from src.server.services.cache._series_cache_core import is_live_window

# 00:30 UTC on 2026-07-07. UTC-12h floors to 2026-07-06 — i.e. ET's "today",
# since New York at this instant is 20:30 on 2026-07-06.
_FIXED_UTC = dt.datetime(2026, 7, 7, 0, 30, tzinfo=dt.timezone.utc)
_UTC_YESTERDAY = "2026-07-06"  # == the UTC-12 floor / venue-local today (ET)
_TWO_DAYS_BACK = "2026-07-05"


class _FrozenDatetime(dt.datetime):
    """`datetime` whose ``now()`` is pinned to ``_FIXED_UTC``."""

    @classmethod
    def now(cls, tz=None):
        return _FIXED_UTC if tz is None else _FIXED_UTC.astimezone(tz)


@pytest.fixture
def _frozen_clock(monkeypatch):
    monkeypatch.setattr(mod, "datetime", _FrozenDatetime)


class TestIsLiveWindow:
    def test_venue_local_today_is_live(self, _frozen_clock):
        # to_date == the UTC-12 floor (ET's current date) → still growable.
        assert is_live_window(_UTC_YESTERDAY) is True

    def test_two_days_back_is_historical(self, _frozen_clock):
        # A window ending before the floor can no longer grow.
        assert is_live_window(_TWO_DAYS_BACK) is False

    def test_none_to_date_is_live(self, _frozen_clock):
        # An open-ended window (no explicit to_date) is always live.
        assert is_live_window(None) is True

    def test_unparseable_to_date_defaults_to_live(self, _frozen_clock):
        # Fail-open: a non-ISO string is never treated as narrower than live.
        assert is_live_window("not-a-date") is True


class TestExplicitLiveOverride:
    """``live=False`` pins a date-suffixed key even when the heuristic reads
    the window as live — a /bars ``before=`` page whose right edge lands in
    the UTC-12 zone must never read or fill the window-less live key."""

    def _svc(self):
        from src.server.services.cache.daily_cache_service import DailyCacheService

        DailyCacheService._instance = None
        return DailyCacheService.get_instance()

    def test_build_key_default_follows_heuristic(self, _frozen_clock):
        key = self._svc()._build_key("AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False)
        assert key == "ohlcv:AAPL.XNAS:ohlcv-1d"  # heuristic: live

    def test_build_key_live_false_forces_windowed_key(self, _frozen_clock):
        key = self._svc()._build_key(
            "AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False, live=False,
        )
        assert key == f"ohlcv:AAPL.XNAS:ohlcv-1d:2026-01-01:{_UTC_YESTERDAY}"

    @pytest.mark.asyncio
    async def test_find_cached_live_false_skips_legacy_dual_read(
        self, _frozen_clock, monkeypatch,
    ):
        """In the disagreement zone (heuristic says live, caller says
        historical) any legacy hit sits under the legacy LIVE key — adopting
        it would graft a live series onto a bounded window."""
        from src.server.services.cache import daily_cache_service as dcs

        svc = self._svc()

        class _EmptyCache:
            async def get(self, key):
                return None

            async def mget(self, keys):
                raise AssertionError("legacy dual-read must be skipped")

        monkeypatch.setattr(dcs, "get_cache_client", lambda: _EmptyCache())

        async def _no_provider():
            raise AssertionError("legacy dual-read must be skipped")

        monkeypatch.setattr(dcs, "get_market_data_provider", _no_provider)

        key, envelope = await svc._find_cached(
            "AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False, live=False,
        )
        assert (key, envelope) == (None, None)


# ── Delta-splice discontinuity guard ───────────────────────────────────────
#
# The guard only compares delta bars strictly BEFORE the watermark. A delta
# window that starts AT the watermark therefore overlaps nothing on a daily
# series, and an upstream re-base (a qfq CN provider re-basing its whole
# history on a corporate action; any provider's split re-adjustment) splices in
# as a permanent step at the seam. The delta window must start one bar back.

_DAY_MS = 86_400_000
# 16:00 ET on five consecutive trading days (2026-06-22 .. 2026-06-26).
_D1 = int(dt.datetime(2026, 6, 22, 20, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
_DAYS = [_D1 + i * _DAY_MS for i in range(5)]


def _bar(ts: int, close: float) -> dict:
    return {"time": ts, "ts_event": ts, "open": close, "high": close,
            "low": close, "close": close, "volume": 1000.0}


class _FakeCache:
    """Minimal async cache: dict-backed get/set; TTLs are recorded, not enforced."""

    def __init__(self, store=None):
        self.store = store if store is not None else {}
        self.ttls: dict = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        self.ttls[key] = ttl

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]


@pytest.fixture
def _daily_svc(monkeypatch):
    """DailyCacheService wired to a fake cache and a stub provider."""
    from src.server.services.cache import daily_cache_service as dcs

    dcs.DailyCacheService._instance = None
    svc = dcs.DailyCacheService.get_instance()
    cache = _FakeCache()
    monkeypatch.setattr(dcs, "get_cache_client", lambda: cache)

    class _StubProvider:
        @staticmethod
        def source_names_for(symbol, capability, **_kw):
            return []

    async def _provider():
        return _StubProvider()

    monkeypatch.setattr(dcs, "get_market_data_provider", _provider)
    yield svc, cache
    dcs.DailyCacheService._instance = None


def _seed_envelope(cache, key, bars, publisher="fmp", revision=0):
    from src.server.services.cache._ohlcv_envelope import _build_envelope

    env = _build_envelope(
        list(bars), "open", complete=False, stored_ttl=600,
        data_date="2026-06-26", instrument_key="AAPL.XNAS", schema="ohlcv-1d",
        publisher=publisher, revision=revision,
    )
    cache.store[key] = env
    return env


def _ref(symbol: str):
    from src.data_client.normalize import ref_for_key
    from src.server.services.cache._ohlcv_envelope import series_identity

    return ref_for_key(series_identity(symbol, "1day", False)[0])


async def _cold_fill(svc, symbol: str):
    """One full fetch-and-store of *symbol*'s daily series → ``(bars, truncated, envelope)``."""
    from src.data_client.instrument_clock import clock_for

    return await svc._fetch_and_store(
        svc._build_key(symbol, "1day", None, None, False), symbol, "1day",
        None, None, False, None,
        phase="closed", clock=clock_for(symbol, False), base_ttl=600,
    )


class TestDeltaSpliceDiscontinuity:
    @pytest.mark.asyncio
    async def test_rebased_final_bar_forces_full_refetch_and_bumps_revision(
        self, _daily_svc, monkeypatch,
    ):
        """A delta whose LAST FINAL bar moved (upstream re-base) must discard
        the cached prefix, re-fetch the whole series, and bump ``revision``."""
        svc, cache = _daily_svc
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        cached = [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)]
        _seed_envelope(cache, key, cached)

        calls = []

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            calls.append(from_date)
            if from_date is None:  # full re-fetch after the guard fires
                return [_bar(t, (100.0 + i) / 2) for i, t in enumerate(_DAYS)], "fmp", False
            # Delta overlaps the last final bar (d4), re-based 50% by a split.
            return [_bar(_DAYS[3], 103.0 / 2), _bar(_DAYS[4], 104.0 / 2)], "fmp", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)

        await svc._delta_refresh(key, "AAPL", "1day")

        # The delta window starts at d4 — the last cached bar strictly BEFORE
        # the watermark — not at d5, or the guard has nothing to compare.
        assert calls[0] == "2026-06-25"
        assert calls[1] is None  # guard fired → full re-fetch
        stored = cache.store[key]
        assert stored["header"]["revision"] == 1
        assert [b["close"] for b in stored["records"]] == [50.0, 50.5, 51.0, 51.5, 52.0]

    @pytest.mark.asyncio
    async def test_matching_overlap_splices_normally(self, _daily_svc, monkeypatch):
        """When the overlapping final bar agrees, the cached prefix is kept and
        only the watermark bar onward comes from the delta."""
        svc, cache = _daily_svc
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        cached = [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)]
        _seed_envelope(cache, key, cached)

        calls = []

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            calls.append(from_date)
            return [_bar(_DAYS[3], 103.0), _bar(_DAYS[4], 104.5)], "fmp", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)

        await svc._delta_refresh(key, "AAPL", "1day")

        assert calls == ["2026-06-25"]  # one fetch only — no full re-fetch
        stored = cache.store[key]
        assert stored["header"]["revision"] == 0
        # d1..d4 from cache (the pre-watermark overlap is guard-only, never
        # duplicated), d5 refreshed from the delta.
        assert [b["close"] for b in stored["records"]] == [100.0, 101.0, 102.0, 103.0, 104.5]

    def test_delta_from_falls_back_to_watermark_for_single_bar(self):
        from src.server.services.cache.daily_cache_service import DailyCacheService

        only = [_bar(_DAYS[0], 100.0)]
        assert DailyCacheService._delta_from(only, _DAYS[0], None) == "2026-06-22"

    @pytest.mark.asyncio
    async def test_a_full_refill_that_moves_final_history_bumps_the_stored_revision(
        self, _daily_svc, monkeypatch,
    ):
        """A sync refill (complete past the reopen, a stale date) stamped revision
        0 whatever the series held, so a chart polling across an overnight split
        saw no rebuild and merged the new tail onto pre-split history."""
        svc, cache = _daily_svc
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        _seed_envelope(cache, key, [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], revision=2)
        split = [_bar(t, (100.0 + i) / 2) for i, t in enumerate(_DAYS)]

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return split, "fmp", False

        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)

        _, _, env = await _cold_fill(svc, "AAPL")
        assert (env["header"]["publisher"], env["header"]["revision"]) == ("fmp", 3)
        # Refilled again on the same history, the series keeps its revision.
        _, _, env = await _cold_fill(svc, "AAPL")
        assert env["header"]["revision"] == 3


class _ClosedClock:
    """A venue closed until the next open, past every settle point."""

    tz = None
    daily_backstop = False

    def market_phase(self, now=None):
        return "closed"

    def is_closed(self, now=None):
        return True

    def current_trading_date(self, now=None):
        return "2026-06-26"

    def expected_latest_bar_ms(self, interval, now=None):
        return 0

    def seconds_until_next_open(self, now=None):
        return 50_000


class TestTruncatedClosedSeries:
    @pytest.mark.asyncio
    async def test_a_complete_truncated_series_still_refills_while_closed(
        self, _daily_svc, monkeypatch,
    ):
        """A CN series missing an archive session, fetched after the close, is
        stored complete; both refresh gates read complete-and-closed as final,
        so the gap held until the next open."""
        import time

        from src.server.services.cache._ohlcv_envelope import (
            _build_envelope,
            _needs_refresh,
            _parse_envelope,
        )

        svc, cache = _daily_svc
        clock = _ClosedClock()
        monkeypatch.setattr(mod, "clock_for", lambda *a, **k: clock)
        key = "ohlcv:600519.XSHG:ohlcv-1d"
        held = [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS) if i != 2]
        env = _build_envelope(
            held, "closed", complete=True, stored_ttl=86_400, truncated=True,
            data_date="2026-06-26", instrument_key="600519.XSHG", schema="ohlcv-1d",
            publisher="tushare",
        )
        env["header"]["fetched_at"] = time.time() - 86_400 * 0.3
        cache.store[key] = env

        assert _needs_refresh(
            _parse_envelope(env), 86_400, interval="1day", symbol="600519.SS", clock=clock,
        )

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            assert from_date is None  # refilled whole, not extended from the last bar
            return [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], publisher, False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        await svc._delta_refresh(key, "600519.SS", "1day")

        stored = cache.store[key]
        assert [b["time"] for b in stored["records"]] == _DAYS
        assert stored["header"]["coverage"] == {"truncated": False}


# ── Empty answers ─────────────────────────────────────────────────────────
#
# yfinance reads an upstream 500 as an empty frame, and a publisher can stop
# serving a symbol. Neither may replace a good series or freeze a dead pin.

class TestEmptyAnswers:
    @pytest.mark.asyncio
    async def test_an_empty_full_refetch_keeps_the_cached_series(self, _daily_svc, monkeypatch):
        """A truncated series full-refetches on refresh; an empty answer used to
        overwrite it, stored for the daily base TTL (a day)."""
        import time

        from src.server.services.cache._ohlcv_envelope import _build_envelope

        svc, cache = _daily_svc
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        env = _build_envelope(
            [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], "open", complete=False,
            stored_ttl=86400, truncated=True, data_date="2026-06-26",
            instrument_key="AAPL.XNAS", schema="ohlcv-1d", publisher="fmp", revision=3,
        )
        env["header"]["fetched_at"] = time.time() - 1000
        cache.store[key] = env

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            assert from_date is None  # truncated base → full re-fetch
            return [], publisher, False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        await svc._delta_refresh(key, "AAPL", "1day")

        kept = cache.store[key]
        assert [b["close"] for b in kept["records"]] == [100.0, 101.0, 102.0, 103.0, 104.0]
        assert kept["header"]["coverage"] == {"truncated": True}  # still retried
        assert kept["header"]["revision"] == 3
        assert kept["header"]["fetched_at"] > time.time() - 5  # paces the next retry
        assert 85_000 < cache.ttls[key] <= 85_400  # the key's own expiry, not a new day

    @pytest.mark.asyncio
    async def test_an_empty_refresh_with_nothing_cached_gets_the_empty_ttl(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import _EMPTY_RESULT_TTL, pin_key

        svc, cache = _daily_svc
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"

        async def _fetch_chain(provider, symbol, interval, from_date, to_date,
                               is_index, user_id):
            return [], "yfinance", False

        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(key, "AAPL", "1day")

        assert cache.store[key]["records"] == []
        assert cache.ttls[key] == _EMPTY_RESULT_TTL
        assert pin_key("AAPL", "1day", False) not in cache.store  # an empty fill never pins

    @pytest.mark.asyncio
    async def test_an_empty_pinned_answer_falls_back_to_the_chain(self, _daily_svc, monkeypatch):
        """An empty fill never re-pins, so accepting the pinned publisher's empty
        answer kept the series empty for the pin's whole lifetime."""
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        pin = pin_key("AAPL", "1day", False)
        cache.store[pin] = {"publisher": "fmp"}
        used = []

        async def _fetch_from(provider, publisher, *a, **k):
            used.append(publisher)
            return [], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date,
                               is_index, user_id):
            used.append("chain")
            return [_bar(_DAYS[4], 104.0)], "yfinance", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        bars, _, env = await _cold_fill(svc, "AAPL")

        assert used == ["fmp", "chain"]
        assert [b["close"] for b in bars] == [104.0]
        assert env["header"]["publisher"] == "yfinance"
        # fmp and yfinance share a basis, so the pin follows the series.
        assert cache.store[pin] == {"publisher": "yfinance"}

    @pytest.mark.asyncio
    async def test_a_failed_chain_after_an_empty_pin_stores_the_empty_answer(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import _EMPTY_RESULT_TTL, pin_key

        svc, cache = _daily_svc
        pin = pin_key("AAPL", "1day", False)
        cache.store[pin] = {"publisher": "fmp"}

        async def _fetch_from(provider, publisher, *a, **k):
            return [], publisher, False

        async def _fetch_chain(*a, **k):
            raise RuntimeError("every source down")

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        bars, _, env = await _cold_fill(svc, "AAPL")

        assert bars == [] and env["stored_ttl"] == _EMPTY_RESULT_TTL
        assert cache.store[pin] == {"publisher": "fmp"}


# ── Adjustment-basis pin protection ───────────────────────────────────────

class TestBasisTransition:
    """A pinned publisher's transient failure must not silently swap the
    series' adjustment basis and pin the substitute for the pin's lifetime."""

    @pytest.mark.asyncio
    async def test_differing_treatment_keeps_pin_and_bumps_revision(
        self, _daily_svc, caplog,
    ):
        import logging

        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        cache.store[pin_key("600519.SS", "1day", False)] = {"publisher": "tushare"}

        with caplog.at_level(logging.WARNING):
            revision, may_repin = await svc._basis_transition(
                "600519.SS", "1day", False, "fmp", _ref("600519.SS"),
            )

        # tushare CN equities are qfq (dividend_adjusted); fmp serves CN venues raw.
        assert (revision, may_repin) == (1, False)
        assert "tushare" in caplog.text and "fmp" in caplog.text
        assert "dividend_adjusted" in caplog.text and "raw" in caplog.text

    @pytest.mark.asyncio
    async def test_same_treatment_repins_as_before(self, _daily_svc):
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        cache.store[pin_key("AAPL", "1day", False)] = {"publisher": "fmp"}
        # fmp and yfinance both serve split-adjusted bars — a plain vendor swap.
        assert await svc._basis_transition(
            "AAPL", "1day", False, "yfinance", _ref("AAPL"),
        ) == (0, True)

    @pytest.mark.asyncio
    async def test_no_pin_repins(self, _daily_svc):
        svc, _ = _daily_svc
        assert await svc._basis_transition("AAPL", "1day", False, "fmp", _ref("AAPL")) == (0, True)

    @pytest.mark.asyncio
    async def test_full_fetch_does_not_repin_across_a_basis_change(self, _daily_svc, monkeypatch):
        """End to end: the cold-fetch path leaves the pin on the failed
        publisher so the next refresh retries it, and stamps revision 1."""
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        pin = pin_key("600519.SS", "1day", False)
        cache.store[pin] = {"publisher": "tushare"}

        async def _fetch_from(*args, **kwargs):
            raise RuntimeError("tushare timeout")

        async def _fetch_chain(provider, symbol, interval, from_date, to_date,
                               is_index, user_id):
            return [_bar(_DAYS[4], 1700.0)], "fmp", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)

        result = await svc._full_fetch(
            "600519.SS", None, None, False, None, "closed", 600,
        )

        assert result.error is None
        assert cache.store[pin] == {"publisher": "tushare"}  # pin untouched
        assert result.header["revision"] == 1
        assert result.header["publisher"] == "fmp"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("symbol, chain, capped", [
        # fmp serves CN venues raw, the tushare head qfq: kept briefly.
        ("600519.SS", ["tushare", "fmp"], True),
        # fmp and yfinance are both split-adjusted: a plain vendor swap.
        ("AAPL", ["yfinance", "fmp"], False),
    ])
    async def test_a_window_a_fallback_fills_on_another_basis_is_kept_briefly(
        self, _daily_svc, monkeypatch, symbol, chain, capped,
    ):
        """Cached for its full TTL, the window would outlive the outage and
        page in beside the head's series across an adjustment seam."""
        from src.server.services.cache import daily_cache_service as dcs
        from src.server.services.cache._series_cache_core import _STAND_IN_TTL

        svc, cache = _daily_svc

        class _Chain:
            @staticmethod
            def source_names_for(symbol, capability, **_kw):
                return chain

        async def _provider():
            return _Chain()

        async def _fetch_chain(*args):
            return [_bar(_DAYS[4], 1700.0)], "fmp", False

        monkeypatch.setattr(dcs, "get_market_data_provider", _provider)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)

        result = await svc._full_fetch(
            symbol, "2026-01-02", "2026-03-31", False, None, "closed", 86400,
        )

        assert result.error is None
        assert (cache.ttls[result.cache_key] == _STAND_IN_TTL) is capped


class TestRulesetSupersedesPin:
    """A re-probed ruleset that moves a cell must win over pin stickiness."""

    @staticmethod
    def _route(monkeypatch, svc, first, *rest):
        from src.server.services.cache import daily_cache_service as dcs

        class _RoutedProvider:
            @staticmethod
            def source_names_for(symbol, capability, **_kw):
                return [first, *rest]

        async def _provider():
            return _RoutedProvider()

        monkeypatch.setattr(dcs, "get_market_data_provider", _provider)

    @pytest.mark.asyncio
    async def test_stale_pin_is_bypassed_on_a_cold_fetch(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        self._route(monkeypatch, svc, first="tushare")
        cache.store[pin_key("600519.SS", "1day", False)] = {"publisher": "fmp"}
        used = []

        async def _fetch_from(provider, publisher, *a, **k):
            used.append(("pinned", publisher))
            return [_bar(_DAYS[0], 1.0)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            used.append(("chain", None))
            return [_bar(_DAYS[0], 2.0)], "tushare", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        _, _, env = await _cold_fill(svc, "600519.SS")
        assert used == [("chain", None)] and env["header"]["publisher"] == "tushare"
        # A deliberate re-route: the basis change bumps the revision and re-pins.
        assert env["header"]["revision"] == 1
        assert cache.store[pin_key("600519.SS", "1day", False)] == {"publisher": "tushare"}

    @pytest.mark.asyncio
    async def test_current_pin_is_still_honored(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        self._route(monkeypatch, svc, first="fmp")
        cache.store[pin_key("600519.SS", "1day", False)] = {"publisher": "fmp"}

        async def _fetch_from(provider, publisher, *a, **k):
            return [_bar(_DAYS[0], 1.0)], publisher, False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        _, _, env = await _cold_fill(svc, "600519.SS")
        assert env["header"]["publisher"] == "fmp"

    @pytest.mark.asyncio
    async def test_rerouted_basis_change_bumps_revision_and_repins(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        self._route(monkeypatch, svc, first="tushare")
        cache.store[pin_key("600519.SS", "1day", False)] = {"publisher": "fmp"}
        # fmp CN daily is raw, tushare is qfq: a basis change, but a deliberate one.
        assert await svc._basis_transition(
            "600519.SS", "1day", False, "tushare", _ref("600519.SS"),
        ) == (1, True)

    @pytest.mark.asyncio
    async def test_transient_failover_still_keeps_the_pin(self, _daily_svc, monkeypatch):
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        self._route(monkeypatch, svc, first="tushare")
        cache.store[pin_key("600519.SS", "1day", False)] = {"publisher": "tushare"}
        assert await svc._basis_transition(
            "600519.SS", "1day", False, "fmp", _ref("600519.SS"),
        ) == (1, False)

    @pytest.mark.asyncio
    async def test_a_pin_off_the_head_of_the_configured_chain_is_not_current(
        self, _daily_svc, monkeypatch,
    ):
        """No ruleset loaded: a CN series pinned to fmp before tushare was
        configured ahead of it must move, or every weekly read re-pins fmp and
        the chart keeps raw bars while the sandbox reads the qfq series."""
        from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry
        from src.server.services.cache import daily_cache_service as dcs
        from src.server.services.cache._ohlcv_envelope import pin_key

        svc, cache = _daily_svc
        provider = MarketDataProvider([
            ProviderEntry("tushare", object(), {"cn"}),
            ProviderEntry("fmp", object(), {"all"}),
        ])

        async def _provider():
            return provider

        monkeypatch.setattr(dcs, "get_market_data_provider", _provider)
        assert svc._pin_is_current(provider, "fmp", "600519.SS", "1day", False) is False
        assert svc._pin_is_current(provider, "tushare", "600519.SS", "1day", False) is True

        pin = pin_key("600519.SS", "1day", False)
        cache.store[pin] = {"publisher": "fmp"}
        used = []

        async def _fetch_from(provider, publisher, *a, **k):
            used.append(publisher)
            return [_bar(_DAYS[0], 1.0)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            used.append("chain")
            return [_bar(_DAYS[0], 2.0)], "tushare", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        _, _, env = await _cold_fill(svc, "600519.SS")

        assert used == ["chain"]
        assert (env["header"]["publisher"], env["header"]["revision"]) == ("tushare", 1)
        assert cache.store[pin] == {"publisher": "tushare"}

    @pytest.mark.asyncio
    async def test_delta_refresh_takes_the_new_head_whole(self, _daily_svc, monkeypatch):
        svc, cache = _daily_svc
        self._route(monkeypatch, svc, "tushare", "fmp")
        key = "ohlcv:600519.XSHG:ohlcv-1d"
        _seed_envelope(cache, key, [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], publisher="fmp")
        cache.store["pin:600519.XSHG:ohlcv-1d"] = {"publisher": "fmp"}
        calls = []

        async def _fetch_from(provider, publisher, symbol, interval, from_date, *a, **k):
            calls.append((publisher, from_date))
            return [_bar(t, 50.0 + i) for i, t in enumerate(_DAYS)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(("chain", from_date))
            raise AssertionError("the head answered; the chain is not needed")

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(key, "600519.SS", "1day")

        assert calls == [("tushare", None)]  # whole from the new head, never a delta from fmp
        stored = cache.store[key]
        assert stored["header"]["publisher"] == "tushare"
        assert stored["header"]["revision"] == 1
        assert cache.store["pin:600519.XSHG:ohlcv-1d"] == {"publisher": "tushare"}

    @pytest.mark.asyncio
    async def test_a_publisher_that_left_the_chain_refetches_in_full(
        self, _daily_svc, monkeypatch,
    ):
        svc, cache = _daily_svc
        self._route(monkeypatch, svc, "tushare")
        key = "ohlcv:600519.XSHG:ohlcv-1d"
        _seed_envelope(cache, key, [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], publisher="fmp")
        calls = []

        async def _fetch_from(provider, publisher, symbol, interval, from_date, *a, **k):
            calls.append((publisher, from_date))
            return [], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(("chain", from_date))
            return [_bar(t, 50.0 + i) for i, t in enumerate(_DAYS)], "tushare", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(key, "600519.SS", "1day")

        assert calls == [("tushare", None), ("chain", None)]
        assert cache.store[key]["header"]["publisher"] == "tushare"

    @pytest.mark.asyncio
    async def test_a_pinned_substitute_extends_by_delta_while_the_head_is_out(
        self, _daily_svc, monkeypatch,
    ):
        """A same-treatment fallback re-pins itself. Read as a moved head, every
        refresh then refetched the whole history through the chain."""
        svc, cache = _daily_svc
        self._route(monkeypatch, svc, "tushare", "fmp")
        key = "ohlcv:600519.XSHG:ohlcv-1d"
        _seed_envelope(cache, key, [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], publisher="fmp")
        cache.store["pin:600519.XSHG:ohlcv-1d"] = {"publisher": "fmp"}
        calls = []

        async def _fetch_from(provider, publisher, symbol, interval, from_date, *a, **k):
            calls.append((publisher, from_date is None))
            if publisher == "tushare":
                raise RuntimeError("tushare timeout")
            return [_bar(_DAYS[4], 104.5)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(("chain", from_date is None))
            raise AssertionError("an outage must not cost a full refetch")

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(key, "600519.SS", "1day")

        assert calls == [("tushare", True), ("fmp", False)]
        stored = cache.store[key]
        assert (stored["header"]["publisher"], stored["header"]["revision"]) == ("fmp", 0)
        assert [b["close"] for b in stored["records"]][-1] == 104.5
        assert cache.store["pin:600519.XSHG:ohlcv-1d"] == {"publisher": "fmp"}


class TestFallbackFilledSeries:
    """A series a fallback filled carries the substitute as its publisher while
    the pin keeps the head of the chain: tushare (qfq) timed out, fmp (raw)
    answered, and the pin stayed on tushare at revision 1."""

    _KEY = "ohlcv:600519.XSHG:ohlcv-1d"
    _PIN = "pin:600519.XSHG:ohlcv-1d"

    def _seed(self, monkeypatch, svc, cache):
        TestRulesetSupersedesPin._route(monkeypatch, svc, "tushare", "fmp")
        _seed_envelope(
            cache, self._KEY, [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)],
            publisher="fmp", revision=1,
        )
        cache.store[self._PIN] = {"publisher": "tushare"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("outage", ["raises", "empty"])
    async def test_a_refresh_while_the_pin_is_out_extends_the_substitute(
        self, _daily_svc, monkeypatch, outage,
    ):
        """Each refresh used to refetch the whole series through the chain and
        bump the revision again, though the stored basis never moved."""
        svc, cache = _daily_svc
        self._seed(monkeypatch, svc, cache)
        calls = []

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            calls.append((publisher, from_date))
            if publisher == "tushare":
                if outage == "raises":
                    raise RuntimeError("tushare timeout")
                return [], publisher, False
            return [_bar(_DAYS[3], 103.0), _bar(_DAYS[4], 104.5)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(("chain", from_date))
            return [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)], "fmp", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(self._KEY, "600519.SS", "1day")

        # The pin is retried whole, then the substitute's own series extends.
        assert [(publisher, start is None) for publisher, start in calls] == [
            ("tushare", True), ("fmp", False),
        ]
        stored = cache.store[self._KEY]
        assert (stored["header"]["publisher"], stored["header"]["revision"]) == ("fmp", 1)
        assert [b["close"] for b in stored["records"]][-1] == 104.5
        assert cache.store[self._PIN] == {"publisher": "tushare"}

    @pytest.mark.asyncio
    async def test_a_recovered_pin_refills_whole_and_bumps_the_revision(
        self, _daily_svc, monkeypatch,
    ):
        """Returning to the pin's basis is a basis change too. Compared against
        the pin it read as no change, so a qfq series replaced the raw one
        under the same revision."""
        svc, cache = _daily_svc
        self._seed(monkeypatch, svc, cache)
        calls = []

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            calls.append((publisher, from_date))
            return [_bar(t, 50.0 + i) for i, t in enumerate(_DAYS)], publisher, False

        async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(("chain", from_date))
            return [_bar(t, 50.0 + i) for i, t in enumerate(_DAYS)], "tushare", False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
        await svc._delta_refresh(self._KEY, "600519.SS", "1day")

        assert calls == [("tushare", None)]
        stored = cache.store[self._KEY]
        assert (stored["header"]["publisher"], stored["header"]["revision"]) == ("tushare", 2)
        assert [b["close"] for b in stored["records"]] == [50.0, 51.0, 52.0, 53.0, 54.0]
        assert cache.store[self._PIN] == {"publisher": "tushare"}

    @pytest.mark.asyncio
    async def test_the_revision_follows_the_stored_series_not_the_pin(
        self, _daily_svc, monkeypatch,
    ):
        svc, cache = _daily_svc
        TestRulesetSupersedesPin._route(monkeypatch, svc, first="tushare")
        cache.store[self._PIN] = {"publisher": "tushare"}
        ref = _ref("600519.SS")

        # Another fmp fill over an fmp series: same basis, the pin stays put.
        assert await svc._basis_transition(
            "600519.SS", "1day", False, "fmp", ref, basis="fmp",
        ) == (0, False)
        # The pin answering over that series moves the basis back.
        assert await svc._basis_transition(
            "600519.SS", "1day", False, "tushare", ref, basis="fmp",
        ) == (1, True)
        # With no series in hand the pin stands in for its basis, as before.
        assert await svc._basis_transition(
            "600519.SS", "1day", False, "fmp", ref,
        ) == (1, False)
