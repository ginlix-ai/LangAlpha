"""Batch snapshot endpoint guards on the market-data router.

The per-request symbol cap is enforced before any upstream fetch; the quote
cache service is stubbed so no Redis/provider is touched.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from market_protocol import to_canonical, to_legacy_api
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_MAX = 250  # documented max symbols per batch snapshot request


@pytest_asyncio.fixture
async def client():
    from src.server.app.market_data import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


_FALLBACK = "src.server.services.cache.quote_daily_fallback"


def _stub_quotes(rows=None):
    """Patch the quote cache under the daily fallback so it serves ``rows``, each
    to the requested instrument its symbol resolves to."""
    by_key = {to_canonical(r["symbol"]).instrument_key: r for r in rows or []}

    async def _cached(refs, user_id):
        return [(ref, by_key[ref.instrument_key]) for ref in refs if ref.instrument_key in by_key], None

    return patch(
        "src.server.services.cache.quote_cache_service.QuoteCacheService._cached_quotes",
        AsyncMock(side_effect=_cached),
    )


def _asked(cached) -> list[str]:
    """The instruments the route asked the quote cache for, in legacy spelling."""
    return [to_legacy_api(ref) for ref in cached.await_args.args[0]]


@pytest.fixture(autouse=True)
def _daily_fallback_is_a_miss():
    """The daily fallback is exercised by its own tests; everywhere else a
    dropped symbol must stay dropped so the assertions read the quote layer."""
    with patch(
        f"{_FALLBACK}.DailyCacheService.get_instance",
        return_value=MagicMock(get_stock_daily=AsyncMock(return_value=MagicMock(data=[], error=None))),
    ):
        yield


async def test_too_many_symbols_is_422(client):
    symbols = ",".join(f"A{i}" for i in range(_MAX + 1))  # 251 distinct tickers
    with _stub_quotes():
        resp = await client.get(f"/api/v1/market-data/snapshots/stocks?symbols={symbols}")
    assert resp.status_code == 422
    assert "Too many symbols" in resp.json()["detail"]


async def test_symbol_cap_boundary_passes_validation(client):
    # Exactly at the cap: not rejected — the request reaches the cache service.
    symbols = ",".join(f"A{i}" for i in range(_MAX))  # 250 distinct tickers
    with _stub_quotes() as cached:
        resp = await client.get(f"/api/v1/market-data/snapshots/stocks?symbols={symbols}")
    assert resp.status_code == 200
    cached.assert_awaited_once()


async def test_snapshot_row_source_passes_through(client):
    # The provider chain stamps each resolved row with the filling provider;
    # the endpoint model must expose it, absent-source rows serialize null.
    rows = [
        {"symbol": "AAPL", "price": 190.0, "source": "ginlix-data"},
        {"symbol": "MSFT", "price": 420.0},
    ]
    with _stub_quotes(rows):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=AAPL,MSFT")
    assert resp.status_code == 200
    snaps = {s["symbol"]: s for s in resp.json()["snapshots"]}
    assert snaps["AAPL"]["source"] == "ginlix-data"
    assert snaps["MSFT"]["source"] is None


async def test_snapshot_rows_carry_the_instrument_currency(client):
    # The protocol resolves currency per symbol; the REST boundary must not
    # drop it, or the client renders a CNY quote with a dollar sign.
    rows = [
        {"symbol": "AAPL", "price": 190.0},
        {"symbol": "600519.SS", "price": 1500.0},
        {"symbol": "0700.HK", "price": 400.0},
    ]
    with _stub_quotes(rows):
        resp = await client.get(
            "/api/v1/market-data/snapshots/stocks?symbols=AAPL,600519.SS,0700.HK"
        )
    assert resp.status_code == 200
    assert [s["currency"] for s in resp.json()["snapshots"]] == ["USD", "CNY", "HKD"]


async def test_single_snapshot_carries_the_instrument_currency(client):
    with _stub_quotes([{"symbol": "600519.SS", "price": 1500.0}]):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/600519.SS")
    assert resp.status_code == 200
    assert resp.json()["currency"] == "CNY"


async def test_upstream_currency_wins_over_the_derived_one(client):
    with _stub_quotes([{"symbol": "AAPL", "price": 190.0, "currency": "EUR"}]):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/AAPL")
    assert resp.status_code == 200
    assert resp.json()["currency"] == "EUR"


async def test_single_snapshot_echoes_the_requested_spelling(client):
    """``600519.ss`` collapses to ``600519.SH`` for the cache, and the caller keys
    its row by what it asked for, so the response answers in that spelling with
    the suffix in ours, whatever spelling the vendor's row carries."""
    with _stub_quotes([{"symbol": "600519.SS", "price": 1290.88}]) as cached:
        resp = await client.get("/api/v1/market-data/snapshots/stocks/600519.ss")
    assert resp.status_code == 200
    assert resp.json()["symbol"] == "600519.SH"
    assert resp.json()["currency"] == "CNY"
    cached.assert_awaited_once()
    assert _asked(cached) == ["600519.SH"]


async def test_batch_snapshots_answer_each_requested_spelling(client):
    """Two widgets can hold two spellings of one instrument. The response shows
    it once, in the display spelling, and ``requested`` names both, so a client
    keyed on either finds the row; the upstream fetch happens once."""
    rows = [{"symbol": "600519.SH", "price": 1290.88}, {"symbol": "AAPL", "price": 190.0}]
    with _stub_quotes(rows) as cached, _no_daily_fallback():
        resp = await client.get(
            "/api/v1/market-data/snapshots/stocks?symbols=600519.SH,AAPL,600519.SS"
        )
    assert resp.status_code == 200
    snaps = resp.json()["snapshots"]
    assert [(s["symbol"], s["requested"]) for s in snaps] == [
        ("600519.SH", ["600519.SH", "600519.SS"]), ("AAPL", ["AAPL"]),
    ]
    assert _asked(cached) == ["600519.SH", "AAPL"]


def _no_daily_fallback():
    return patch(f"{_FALLBACK}.daily_fallback_snapshot", AsyncMock(return_value=None))


def _venue(closed: bool):
    """Pin the instrument clock the fallback consults."""
    return patch(
        f"{_FALLBACK}.clock_for_ref",
        return_value=MagicMock(is_closed=lambda: closed),
    )


def _stub_daily(bars, error=None):
    """Patch DailyCacheService so the fallback sees ``bars`` for any symbol."""
    result = MagicMock(data=bars, error=error)
    service = MagicMock()
    service.get_stock_daily = AsyncMock(return_value=result)
    return patch(
        f"{_FALLBACK}.DailyCacheService.get_instance",
        return_value=service,
    )


_BSE_BARS = [
    {"time": 1, "open": 9.9, "high": 10.0, "low": 9.8, "close": 9.95, "volume": 1000},
    {"time": 2, "open": 10.1, "high": 10.1, "low": 9.87, "close": 9.93, "volume": 664474},
]


async def test_missing_snapshot_falls_back_to_the_last_daily_bar(client):
    """No realtime publisher covers Beijing, but tushare serves its daily bars:
    the header gets last close + day change instead of an empty row."""
    with _stub_quotes([{"symbol": "AAPL", "price": 190.0}]), _stub_daily(_BSE_BARS), _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=920300.BJ,AAPL")
    assert resp.status_code == 200
    rows = {s["symbol"]: s for s in resp.json()["snapshots"]}
    assert set(rows) == {"920300.BJ", "AAPL"}
    bj = rows["920300.BJ"]
    assert (bj["price"], bj["previous_close"], bj["change"]) == (9.93, 9.95, -0.02)
    assert bj["change_percent"] == pytest.approx(-0.201, abs=1e-3)
    assert (bj["open"], bj["high"], bj["low"], bj["volume"]) == (10.1, 10.1, 9.87, 664474)
    assert (bj["source"], bj["market_status"], bj["currency"]) == ("daily", "closed", "CNY")


async def test_snapshot_rows_pass_the_quote_tier_through(client):
    rows = [{"symbol": "600519.SS", "price": 1290.88, "tier": "realtime", "as_of": 1788937200000}]
    with _stub_quotes(rows):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/600519.SS")
    assert resp.status_code == 200
    assert (resp.json()["tier"], resp.json()["as_of"]) == ("realtime", 1788937200000)


async def test_daily_fallback_rows_are_eod(client):
    with _stub_quotes([]), _stub_daily(_BSE_BARS), _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/920300.BJ")
    assert (resp.json()["tier"], resp.json()["as_of"]) == ("eod", 2)


async def test_single_snapshot_falls_back_to_the_last_daily_bar(client):
    with _stub_quotes([]), _stub_daily(_BSE_BARS), _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/920300.bj")
    assert resp.status_code == 200
    assert (resp.json()["symbol"], resp.json()["price"], resp.json()["source"]) == ("920300.BJ", 9.93, "daily")


async def test_no_daily_bars_is_still_a_404(client):
    with _stub_quotes([]), _stub_daily([]), _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/NOPE")
    assert resp.status_code == 404


async def test_daily_fallback_error_never_breaks_the_batch(client):
    with _stub_quotes([{"symbol": "AAPL", "price": 190.0}]), _stub_daily([], error="upstream down"), _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=NOPE,AAPL")
    assert resp.status_code == 200
    assert [s["symbol"] for s in resp.json()["snapshots"]] == ["AAPL"]


async def test_daily_fallback_stays_out_of_an_open_session(client):
    """A snapshot outage during the session must not dress yesterday's close
    up as a live quote (or flag an open market as closed)."""
    with _stub_quotes([]), _stub_daily(_BSE_BARS) as daily, _venue(closed=False):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=AAPL")
    assert resp.status_code == 200
    assert resp.json()["snapshots"] == []
    daily.return_value.get_stock_daily.assert_not_awaited()


async def test_fallback_never_drops_a_row_the_chain_echoed_differently(client):
    """The stocks endpoint keeps a caret index's caret, but the chain echoes
    the bare legacy spelling; that row is served, not treated as missing."""
    with _stub_quotes([{"symbol": "GSPC", "price": 5000.0}]), _stub_daily(_BSE_BARS) as daily, _venue(closed=True):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=^GSPC")
    assert resp.status_code == 200
    assert [(s["symbol"], s["price"]) for s in resp.json()["snapshots"]] == [("^GSPC", 5000.0)]
    daily.return_value.get_stock_daily.assert_not_awaited()


async def test_a_caret_index_on_the_stocks_endpoint_is_stated_as_an_index(client):
    """The chain echoes ``^GSPC`` as bare ``GSPC``, which alone reads as an
    equity; the row states the instrument the cache resolved, as the index
    endpoint does."""
    with _stub_quotes([{"symbol": "GSPC", "price": 5000.0}]):
        stocks = await client.get("/api/v1/market-data/snapshots/stocks?symbols=^GSPC")
        indexes = await client.get("/api/v1/market-data/snapshots/indexes?symbols=^GSPC")
    [via_stocks] = stocks.json()["snapshots"]
    [via_indexes] = indexes.json()["snapshots"]
    assert (via_stocks["asset_class"], via_stocks["currency"]) == ("index", "USD")
    assert (via_indexes["symbol"], via_indexes["requested"], via_indexes["asset_class"]) == (
        "GSPC", ["^GSPC"], "index",
    )


# --- Measured freshness -------------------------------------------------------
# Freshness is computed at response time, not stored. A test that reads a label
# off a print time pins the clock, so the label never depends on the day the
# suite runs.

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _at(moment: datetime):
    """Pin the freshness clock to *moment*, leaving an explicit ``now`` alone."""
    return patch("src.data_client.freshness._now", side_effect=lambda now: now or moment)


async def test_rows_with_no_print_time_fall_back_to_the_declared_tier(client):
    rows = [
        {"symbol": "AAPL", "price": 190.0, "source": "fmp", "tier": "delayed_15m"},
        {"symbol": "MSFT", "price": 420.0, "source": "fmp"},
    ]
    with _stub_quotes(rows):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=AAPL,MSFT")
    assert resp.status_code == 200
    snaps = {s["symbol"]: s["freshness"] for s in resp.json()["snapshots"]}
    # ``closed`` follows the wall clock; with the venue resolved it is always stated.
    assert isinstance(snaps["AAPL"].pop("closed"), bool)
    assert snaps["AAPL"] == {
        "expected_latest": None, "actual_latest": None, "lag_s": None,
        "label": "delayed", "measured": False, "source": "fmp", "interval": None,
    }
    # No tier and no timestamp: nothing is claimed.
    assert (snaps["MSFT"]["label"], snaps["MSFT"]["measured"]) == ("unknown", False)


async def test_a_print_time_is_actually_measured(client):
    """A row declared realtime whose print is yesterday's close reads stale
    once the next session trades: the label is measured, not declared."""
    rows = [{"symbol": "600519.SS", "price": 1290.88, "source": "tushare",
             "tier": "realtime", "as_of": 1788937200000}]  # 2026-09-09 15:00 Shanghai
    with _stub_quotes(rows), _at(datetime(2026, 9, 10, 9, 40, tzinfo=_SHANGHAI)):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/600519.SS")
    fresh = resp.json()["freshness"]
    assert fresh["measured"] is True
    assert fresh["actual_latest"] == 1788937200000
    assert fresh["source"] == "tushare"
    assert (fresh["label"], fresh["lag_s"], fresh["closed"]) == ("stale", 67200, False)


async def test_daily_fallback_rows_measure_stale(client):
    with _stub_quotes([]), _stub_daily(_BSE_BARS), _venue(closed=True), \
         _at(datetime(2026, 9, 9, 16, 0, tzinfo=_SHANGHAI)):
        resp = await client.get("/api/v1/market-data/snapshots/stocks/920300.BJ")
    fresh = resp.json()["freshness"]
    assert (fresh["label"], fresh["measured"], fresh["source"]) == ("stale", True, "daily")


async def test_a_collapsed_spelling_row_carries_freshness(client):
    rows = [{"symbol": "600519.SS", "price": 1290.88, "source": "tushare", "tier": "realtime"}]
    with _stub_quotes(rows):
        resp = await client.get(
            "/api/v1/market-data/snapshots/stocks?symbols=600519.SH,600519.SS"
        )
    snaps = resp.json()["snapshots"]
    assert [(s["symbol"], s["requested"]) for s in snaps] == [
        ("600519.SH", ["600519.SH", "600519.SS"]),
    ]
    assert snaps[0]["freshness"]["label"] == "live"
