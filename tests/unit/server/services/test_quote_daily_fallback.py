"""Daily-bar closing quotes for listings the snapshot chain dropped."""

from __future__ import annotations

from datetime import date, datetime, time
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from market_protocol import AssetClass, to_canonical
from src.data_client.freshness import measure_quote
from src.server.services.cache import quote_daily_fallback as qdf

_DAY_MS = 86_400_000


def _bars(n: int) -> list[dict]:
    return [
        {"time": (i + 1) * _DAY_MS, "open": 10.0 + i, "high": 10.0 + i,
         "low": 10.0 + i, "close": 10.0 + i, "volume": 1000}
        for i in range(n)
    ]


@pytest.fixture
def closed_venue(monkeypatch):
    monkeypatch.setattr(qdf, "clock_for_ref", lambda ref: MagicMock(is_closed=lambda: True))


@pytest.fixture
def daily(monkeypatch):
    """The daily cache as the fallback sees it, answering two bars for anything."""
    service = MagicMock()
    service.get_stock_daily = AsyncMock(return_value=MagicMock(data=_bars(2), error=None))
    monkeypatch.setattr(qdf.DailyCacheService, "get_instance", lambda: service)
    return service.get_stock_daily


@pytest.mark.asyncio
async def test_one_request_runs_a_bounded_number_of_fallbacks(closed_venue, daily):
    """Each fallback can walk the whole daily chain on a miss; a batch of
    unknown symbols on a weekend must not fan out once per symbol."""
    refs = [to_canonical(f"{830000 + i}.BJ") for i in range(qdf._MAX_FALLBACKS + 10)]

    out = await qdf.fill_from_daily([], refs, None)

    assert daily.await_count == qdf._MAX_FALLBACKS
    assert [ref for ref, _ in out] == refs[: qdf._MAX_FALLBACKS]


@pytest.mark.asyncio
async def test_an_index_is_never_read_as_equity_bars(closed_venue, daily):
    spx = to_canonical("^GSPC", asset_class=AssetClass.INDEX)
    bj = to_canonical("920300.BJ")

    out = await qdf.fill_from_daily([], [spx, bj], None)

    assert [ref for ref, _ in out] == [bj]
    assert [c.kwargs["symbol"] for c in daily.await_args_list] == ["920300.BJ"]


@pytest.mark.asyncio
async def test_the_fallback_fills_the_live_series_whole(closed_venue, monkeypatch):
    """The fallback reads the series the chart reads. A dated 21-day window
    ending today landed on that same live key, and every later chart read of
    the symbol got 21 bars until the next full fetch."""
    from src.server.services.cache import daily_cache_service as dcs
    from src.server.services.cache._ohlcv_envelope import canonical_series_key

    class _Cache:
        def __init__(self):
            self.store: dict = {}

        async def get(self, key):
            return self.store.get(key)

        async def mget(self, keys):
            return [self.store.get(k) for k in keys]

        async def set(self, key, value, ttl=None):
            self.store[key] = value

    class _Provider:
        @staticmethod
        def source_names_for(symbol, capability, **_kw):
            return []

    async def _provider():
        return _Provider()

    cache = _Cache()
    monkeypatch.setattr(dcs, "get_cache_client", lambda: cache)
    monkeypatch.setattr(dcs, "get_market_data_provider", _provider)
    dcs.DailyCacheService._instance = None
    svc = dcs.DailyCacheService.get_instance()
    windows = []

    async def _fetch_chain(provider, symbol, interval, from_date, to_date, is_index, user_id):
        windows.append((from_date, to_date))
        return _bars(300), "fmp", False

    monkeypatch.setattr(svc, "_fetch_chain", _fetch_chain)
    try:
        [(_, row)] = await qdf.fill_from_daily([], [to_canonical("920300.BJ")], None)
    finally:
        dcs.DailyCacheService._instance = None

    assert windows == [(None, None)]
    live = cache.store[canonical_series_key("920300.BJ", "1day")]
    assert len(live["records"]) == 300
    assert (row["price"], row["previous_close"]) == (309.0, 308.0)


_SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.asyncio
@pytest.mark.parametrize("bar_day, label", [
    (date(2026, 9, 30), "live"),   # the last session before National Day
    (date(2026, 9, 29), "stale"),  # a session behind it
])
async def test_a_settled_close_reads_live_on_a_closed_venue(closed_venue, monkeypatch, bar_day, label):
    """A daily bar is stamped at venue midnight. Carried as the quote's print
    time it sat ~15h short of the last close, so every fallback row on a closed
    venue read stale; stamped at its session's close it reads live, and only a
    bar from an older session stays stale."""
    bar_ms = int(datetime.combine(bar_day, time(0), tzinfo=_SHANGHAI).timestamp() * 1000)
    service = MagicMock()
    service.get_stock_daily = AsyncMock(return_value=MagicMock(
        data=[{"time": bar_ms, "open": 9.9, "high": 10.0, "low": 9.8, "close": 9.93, "volume": 1000}],
        error=None,
    ))
    monkeypatch.setattr(qdf.DailyCacheService, "get_instance", lambda: service)

    [(_, row)] = await qdf.fill_from_daily([], [to_canonical("920300.BJ")], None)

    holiday = datetime(2026, 10, 3, 10, 0, tzinfo=_SHANGHAI)
    freshness = measure_quote(row["symbol"], row["as_of"], row["tier"], now=holiday)
    assert (freshness.label, freshness.closed) == (label, True)


def test_a_forming_session_is_not_stamped_in_the_future():
    ref = to_canonical("920300.BJ")
    bar_ms = int(datetime(2026, 9, 30, tzinfo=_SHANGHAI).timestamp() * 1000)
    mid_session = datetime(2026, 9, 30, 10, 0, tzinfo=_SHANGHAI)

    assert qdf._print_time_ms(ref, bar_ms, now=mid_session) == int(mid_session.timestamp() * 1000)
