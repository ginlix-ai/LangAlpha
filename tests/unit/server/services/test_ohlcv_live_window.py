"""The window a live OHLCV series holds.

One live key serves every window of an instrument. The first chart to fill it
must not decide the history every later chart sees: a request is served from
the stored series only when its window covers the request's start, and a fill
extends the stored series instead of narrowing it.
"""

import datetime as dt

import pytest

from src.server.services.cache import _series_cache_core as mod
from tests.unit.server.services import test_series_cache_core as core
from tests.unit.server.services.test_series_cache_core import _DAY_MS, _DAYS, _ClosedClock, _bar

_daily_svc = core._daily_svc


@pytest.fixture
def _closed_daily(_daily_svc, monkeypatch):
    """The daily service on a closed venue, with a chain that answers each
    start from five sessions of fmp bars (06-23 onward when no start is given)."""
    from src.server.services.cache import daily_cache_service as dcs

    svc, cache = _daily_svc
    clock = _ClosedClock()
    monkeypatch.setattr(mod, "clock_for", lambda *a, **k: clock)
    monkeypatch.setattr(dcs, "clock_for", lambda *a, **k: clock)
    history = [_bar(t, 100.0 + i) for i, t in enumerate(_DAYS)]
    calls: list = []

    async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
        calls.append(from_date)
        start = from_date or "2026-06-23"
        return [b for b in history if _day(b) >= start], "fmp", False

    async def _fetch_from(provider, publisher, symbol, interval, from_date, to_date, is_index, user_id):
        return await svc._fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id)

    monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
    monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
    return svc, cache, calls

@pytest.fixture
def _fmp_heads_the_chain(_closed_daily, monkeypatch):
    """The chain fmp heads, yfinance standing in for it."""
    svc = _closed_daily[0]

    class _Chain:
        @staticmethod
        def source_names_for(symbol, capability, **_kw):
            return ["fmp", "yfinance"]

    async def _provider():
        return _Chain()

    monkeypatch.setattr(svc, "_provider", _provider)


def _day(bar) -> str:
    return dt.datetime.fromtimestamp(bar["time"] / 1000, dt.timezone.utc).date().isoformat()


def _days(result) -> list:
    return [_day(b) for b in result.data]


class TestLiveWindow:
    @pytest.mark.asyncio
    async def test_a_short_window_does_not_cut_a_later_longer_one(self, _closed_daily):
        svc, cache, calls = _closed_daily

        short = await svc.get_stock_daily("AAPL", from_date="2026-06-25")
        assert _days(short) == ["2026-06-25", "2026-06-26"]

        full = await svc.get_stock_daily("AAPL")
        assert full.cached is False
        assert _days(full) == ["2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26"]
        assert calls == ["2026-06-25", None]

    @pytest.mark.asyncio
    async def test_a_covered_window_is_served_cached_and_cut_to_its_start(self, _closed_daily):
        svc, cache, calls = _closed_daily

        await svc.get_stock_daily("AAPL", from_date="2026-06-22")
        later = await svc.get_stock_daily("AAPL", from_date="2026-06-25")

        assert later.cached is True
        assert _days(later) == ["2026-06-25", "2026-06-26"]
        assert calls == ["2026-06-22"]

    @pytest.mark.asyncio
    async def test_a_fill_extends_the_stored_series_instead_of_narrowing_it(self, _closed_daily):
        svc, cache, calls = _closed_daily

        await svc.get_stock_daily("AAPL", from_date="2026-06-22")
        full = await svc.get_stock_daily("AAPL")
        again = await svc.get_stock_daily("AAPL", from_date="2026-06-22")

        assert calls == ["2026-06-22", None]
        assert len(full.data) == 5
        assert again.cached is True and len(again.data) == 5
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["window"] == {"from": "2026-06-22", "default": True}

    @pytest.mark.asyncio
    async def test_another_publishers_history_is_never_spliced_in(self, _closed_daily, monkeypatch):
        svc, cache, calls = _closed_daily
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        await svc.get_stock_daily("AAPL", from_date="2026-06-22")

        async def _yfinance(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 200.0) for t in _DAYS[2:] + [_DAYS[-1] + _DAY_MS]], "yfinance", False

        monkeypatch.setattr(svc, "_fetch_chain", _yfinance)
        full = await svc.get_stock_daily("AAPL")

        assert [b["close"] for b in full.data] == [200.0] * 4
        assert cache.store[key]["window"] == {"from": None, "default": True}

    @pytest.mark.asyncio
    async def test_an_empty_answer_for_a_wider_window_keeps_the_series(
        self, _closed_daily, monkeypatch,
    ):
        """A start past a provider's lookback cap answers nothing; the warm
        series must keep serving instead of being replaced by an empty one."""
        svc, cache, calls = _closed_daily
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        await svc.get_stock_daily("AAPL")

        async def _nothing(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(from_date)
            return [], "fmp", False

        monkeypatch.setattr(svc, "_fetch_chain", _nothing)
        wide = await svc.get_stock_daily("AAPL", from_date="2026-01-02")
        again = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert len(wide.data) == 4 and len(again.data) == 4
        assert wide.cached is False
        assert again.cached is True
        assert calls == [None, "2026-01-02", "2026-01-02"]  # the head, then the chain, once
        assert len(cache.store[key]["records"]) == 4
        assert cache.store[key]["window"] == {"from": "2026-01-02", "default": True}

    @pytest.mark.asyncio
    async def test_a_shorter_stand_in_for_a_wider_window_keeps_the_series(
        self, _closed_daily, monkeypatch,
    ):
        """The head answers nothing past its lookback cap and a fallback answers
        less history than the series holds: the series stays, unblended."""
        svc, cache, calls = _closed_daily
        await svc.get_stock_daily("AAPL")

        async def _head(provider, publisher, symbol, interval, from_date, to_date, is_index, user_id):
            return [], publisher, False

        async def _stand_in(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 200.0) for t in _DAYS[3:]], "yfinance", False

        monkeypatch.setattr(svc, "_fetch_from", _head)
        monkeypatch.setattr(svc, "_fetch_chain", _stand_in)
        wide = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert [b["close"] for b in wide.data] == [101.0, 102.0, 103.0, 104.0]
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "fmp"
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["window"]["from"] == "2026-01-02"

    @pytest.mark.asyncio
    async def test_a_shorter_fill_whose_own_publisher_moved_history_replaces_the_series(
        self, _closed_daily, monkeypatch,
    ):
        svc, cache, calls = _closed_daily
        await svc.get_stock_daily("AAPL")

        async def _split(provider, publisher, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 50.0) for t in _DAYS[3:]], "fmp", False

        monkeypatch.setattr(svc, "_fetch_from", _split)
        wide = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert [b["close"] for b in wide.data] == [50.0, 50.0]
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["revision"] == 1

    @pytest.mark.asyncio
    async def test_the_chain_head_replaces_a_stand_ins_longer_series(self, _closed_daily, _fmp_heads_the_chain, monkeypatch):
        """A stand-in filled the series during a head outage; the head's own
        answer replaces it even when it reaches less far back."""
        svc, cache, calls = _closed_daily

        async def _stand_in(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 100.0) for t in _DAYS], "yfinance", False

        async def _head(provider, publisher, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 100.5) for t in _DAYS[3:]], "fmp", False

        monkeypatch.setattr(svc, "_fetch_chain", _stand_in)
        await svc.get_stock_daily("AAPL", from_date="2026-06-22")
        monkeypatch.setattr(svc, "_fetch_chain", _head)
        monkeypatch.setattr(svc, "_fetch_from", _head)
        await cache.set("pin:AAPL.XNAS:ohlcv-1d", {"publisher": "fmp"})
        wide = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert [b["close"] for b in wide.data] == [100.5, 100.5]
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "fmp"

    @pytest.mark.asyncio
    async def test_an_empty_head_answer_keeps_a_stand_ins_series(self, _closed_daily, _fmp_heads_the_chain, monkeypatch):
        svc, cache, calls = _closed_daily

        async def _stand_in(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return [_bar(t, 100.0) for t in _DAYS], "yfinance", False

        async def _nothing(provider, *args):
            return [], "fmp", False

        monkeypatch.setattr(svc, "_fetch_chain", _stand_in)
        await svc.get_stock_daily("AAPL", from_date="2026-06-22")
        monkeypatch.setattr(svc, "_fetch_chain", _nothing)
        monkeypatch.setattr(svc, "_fetch_from", _nothing)
        wide = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert len(wide.data) == 5
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["header"]["publisher"] == "yfinance"

    @pytest.mark.asyncio
    async def test_an_unpinned_empty_head_answer_claims_the_window(self, _closed_daily, _fmp_heads_the_chain, monkeypatch):
        """With no pin to read, the chain's empty answer named for its head is
        still the head's own, so the start is not asked for again."""
        svc, cache, calls = _closed_daily

        async def _nothing(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(from_date)
            return [], "fmp", False

        await svc.get_stock_daily("AAPL")
        cache.store.pop("pin:AAPL.XNAS:ohlcv-1d", None)
        monkeypatch.setattr(svc, "_fetch_chain", _nothing)
        await svc.get_stock_daily("AAPL", from_date="2026-01-02")
        again = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert again.cached is True
        assert calls == [None, "2026-01-02"]

    @pytest.mark.asyncio
    async def test_an_empty_default_answer_never_claims_the_default_window(
        self, _closed_daily, monkeypatch,
    ):
        """The held bars sit inside the default window, so an empty answer for
        it is an upstream blip: the short series must not pass for the default."""
        svc, cache, calls = _closed_daily
        await svc.get_stock_daily("AAPL", from_date="2026-06-25")
        recover = svc._fetch_chain

        async def _blip(provider, symbol, interval, from_date, to_date, is_index, user_id):
            return [], "fmp", False

        monkeypatch.setattr(svc, "_fetch_chain", _blip)
        await svc.get_stock_daily("AAPL")
        monkeypatch.setattr(svc, "_fetch_chain", recover)
        full = await svc.get_stock_daily("AAPL")

        assert full.cached is False and len(full.data) == 4

    @pytest.mark.asyncio
    async def test_a_head_failure_keeps_the_series_without_claiming_the_window(
        self, _closed_daily, monkeypatch,
    ):
        """Only the head's own answer says it has nothing more; after it fails,
        a later request for the same start asks again."""
        svc, cache, calls = _closed_daily
        await svc.get_stock_daily("AAPL")

        async def _down(provider, publisher, symbol, interval, from_date, to_date, is_index, user_id):
            raise TimeoutError("head down")

        async def _stand_in(provider, symbol, interval, from_date, to_date, is_index, user_id):
            calls.append(from_date)
            return [_bar(t, 200.0) for t in _DAYS[3:]], "yfinance", False

        monkeypatch.setattr(svc, "_fetch_from", _down)
        monkeypatch.setattr(svc, "_fetch_chain", _stand_in)
        first = await svc.get_stock_daily("AAPL", from_date="2026-01-02")
        second = await svc.get_stock_daily("AAPL", from_date="2026-01-02")

        assert len(first.data) == 4 and second.cached is False
        assert calls == [None, "2026-01-02", "2026-01-02"]
        assert cache.store["ohlcv:AAPL.XNAS:ohlcv-1d"]["window"] == {"from": None, "default": True}

    @pytest.mark.asyncio
    async def test_a_truncated_series_refills_on_the_default_window_and_keeps_its_older_bars(
        self, _daily_svc, monkeypatch,
    ):
        """Asking again for a window that came back cut comes back cut again,
        so the refill takes the default window and keeps the older bars in front."""
        from src.server.services.cache._ohlcv_envelope import _build_envelope

        svc, cache = _daily_svc
        monkeypatch.setattr(mod, "clock_for", lambda *a, **k: _ClosedClock())
        key = "ohlcv:AAPL.XNAS:ohlcv-1d"
        cache.store[key] = _build_envelope(
            [_bar(t, 100.0) for t in _DAYS[1:]], "closed", complete=False, stored_ttl=600,
            truncated=True, data_date="2026-06-26", instrument_key="AAPL.XNAS",
            schema="ohlcv-1d", publisher="fmp", window={"from": "2026-06-22", "default": False},
        )
        asked = []

        async def _fetch_from(provider, publisher, symbol, interval,
                              from_date, to_date, is_index, user_id):
            asked.append(from_date)
            return [_bar(t, 100.0) for t in _DAYS[2:]], publisher, False

        monkeypatch.setattr(svc, "_fetch_from", _fetch_from)
        await svc._delta_refresh(key, "AAPL", "1day")

        stored = cache.store[key]
        assert asked == [None]
        assert [b["time"] for b in stored["records"]] == _DAYS[1:]
        assert stored["header"]["coverage"] == {"truncated": False}
        # The kept older bars are the cut answer: the window claims only the
        # whole days they hold, so a request for 06-22 asks again.
        assert stored["window"] == {"from": "2026-06-24", "default": True}

class TestExtendSeries:
    @staticmethod
    def _stored(bars, publisher="fmp", window=None):
        from src.server.services.cache._ohlcv_envelope import _build_envelope, _parse_envelope

        return _parse_envelope(_build_envelope(
            list(bars), "open", complete=False, stored_ttl=600, data_date="2026-06-26",
            instrument_key="AAPL.XNAS", schema="ohlcv-1d", publisher=publisher,
            window=window or {"from": "2026-06-23", "default": False},
        ))

    def test_a_lagging_wider_answer_keeps_the_newer_stored_bars(self):
        from src.server.services.cache._ohlcv_envelope import extend_series

        stored = self._stored([_bar(t, 100.0) for t in _DAYS[1:]])
        merged, window = extend_series(stored, [_bar(t, 100.0) for t in _DAYS[:4]], "fmp", "2026-06-22")

        assert [b["time"] for b in merged] == _DAYS
        assert window == {"from": "2026-06-22", "default": False}

    def test_a_fetch_from_the_forming_bar_never_carries_the_prefix_unchecked(self):
        from src.server.services.cache._ohlcv_envelope import extend_series

        stored = self._stored([_bar(t, 100.0) for t in _DAYS])
        merged, _ = extend_series(stored, [_bar(_DAYS[4], 50.0)], "fmp", "2026-06-26")

        assert [b["time"] for b in merged] == [_DAYS[4]]

    def test_another_publishers_answer_takes_none_of_the_stored_window(self):
        from src.server.services.cache._ohlcv_envelope import extend_series

        stored = self._stored([_bar(t, 100.0) for t in _DAYS[2:]], window={"from": "2020-01-01", "default": False})
        merged, window = extend_series(stored, [_bar(t, 100.0) for t in _DAYS], "yfinance", None)

        assert len(merged) == 5
        assert window == {"from": None, "default": True}
