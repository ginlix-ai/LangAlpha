"""Tests for the progressive bars router (src/server/app/bars.py).

Covers schema validation, the three access modes (default / after / before),
period-aligned paging windows per schema family, and index-spelling routing.
The cache services and the raw cache client are stubbed so no Redis/provider
is touched — the router's job is contract shaping, not fetching.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.cache.daily_cache_service import DailyFetchResult
from src.server.services.cache.intraday_cache_service import IntradayFetchResult
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_MS = 1_750_000_000_000  # neutral placeholder anchor (Unix ms)


def _bar(t: int, close: float = 10.0) -> dict:
    return {"time": t, "open": close, "high": close, "low": close, "close": close, "volume": 100}


def _v4_envelope(bars, publisher="stub-pub", **overrides) -> dict:
    header = {
        "instrument_key": "AAPL.XNAS",
        "schema": "ohlcv-1m",
        "publisher": publisher,
        "price_treatment": "split_adjusted",
        "tier": "realtime",
        "feed_scope": "composite",
        "ts_unit": "ms",
        "latest_trading_date": "2026-06-30",
        "revision": 0,
        "asof": 1_751_000_000.0,
        "coverage": {"truncated": False},
        "fetched_at": 1_751_000_000.0,
        "watermark": bars[-1]["time"] if bars else 0,
        **overrides,
    }
    return {"v": 4, "header": header, "records": bars,
            "market_phase": "open", "complete": False, "stored_ttl": 60}


_DEFAULT = object()  # sentinel: build the stub header from the bars


def _intraday_result(bars, *, phase="open", cached=True, cache_key="ohlcv:AAPL.XNAS:ohlcv-1m",
                     interval="1min", symbol="AAPL", header=_DEFAULT) -> IntradayFetchResult:
    return IntradayFetchResult(
        symbol=symbol, interval=interval, data=bars, cached=cached,
        ttl_remaining=60, background_refresh_triggered=False, cache_key=cache_key,
        watermark=(bars[-1]["time"] if bars else 0), complete=(phase == "closed"),
        market_phase=phase, truncated=False,
        header=(_v4_envelope(bars)["header"] if header is _DEFAULT else header),
    )


def _daily_result(bars, *, phase="closed", cache_key="ohlcv:AAPL.XNAS:ohlcv-1d",
                  header=_DEFAULT) -> DailyFetchResult:
    return DailyFetchResult(
        symbol="AAPL", data=bars, cached=True, ttl_remaining=60,
        background_refresh_triggered=False, cache_key=cache_key,
        watermark=(bars[-1]["time"] if bars else 0), complete=(phase == "closed"),
        market_phase=phase, truncated=False,
        header=(_v4_envelope(bars)["header"] if header is _DEFAULT else header),
    )


@contextmanager
def _stub(*, intraday_result=None, daily_result=None):
    """Patch the two cache-service singletons.

    The v4 header rides on the FetchResult (``header=``), so the router no longer
    re-reads the cache key — no raw cache client to stub.
    """
    intraday = MagicMock()
    intraday.get_stock_intraday = AsyncMock(return_value=intraday_result)
    intraday.get_index_intraday = AsyncMock(return_value=intraday_result)
    daily = MagicMock()
    daily.get_stock_daily = AsyncMock(return_value=daily_result)
    with (
        patch("src.server.app.bars.IntradayCacheService.get_instance", return_value=intraday),
        patch("src.server.app.bars.DailyCacheService.get_instance", return_value=daily),
    ):
        yield intraday, daily


@pytest_asyncio.fixture
async def client():
    from src.server.app.bars import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

async def test_unknown_schema_is_422(client):
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get("/api/v1/market-data/bars/AAPL?schema=ohlcv-7m")
    assert resp.status_code == 422
    assert "Unknown schema" in resp.json()["detail"]


async def test_missing_schema_is_422(client):
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get("/api/v1/market-data/bars/AAPL")
    assert resp.status_code == 422


async def test_ohlcv_1s_is_422(client):
    # ohlcv-1s remains a valid protocol schema (WS forming-bar records) but is
    # not REST-servable — no provider serves second bars.
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get("/api/v1/market-data/bars/AAPL?schema=ohlcv-1s")
    assert resp.status_code == 422
    assert "WS-only" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Default mode
# ---------------------------------------------------------------------------

async def test_default_mode_returns_header_and_records(client):
    bars = [_bar(_MS), _bar(_MS + 60_000)]
    with _stub(
        intraday_result=_intraday_result(bars, phase="open"),
    ) as (intraday, _daily):
        resp = await client.get("/api/v1/market-data/bars/aapl?schema=ohlcv-1m")

    assert resp.status_code == 200
    body = resp.json()
    header = body["series"]["header"]
    assert header["instrument_key"] == "AAPL.XNAS"
    assert header["schema"] == "ohlcv-1m"
    assert header["ts_unit"] == "ms"
    assert header["publisher"] == "stub-pub"
    # currency fields come from the InstrumentRef, not the envelope
    assert header["price_currency"] == "USD"
    assert header["display_decimals"] == 2

    records = body["series"]["records"]
    assert len(records) == 2
    assert all("ts_event" in r and "time" in r for r in records)
    assert all(r["ts_event"] == r["time"] for r in records)
    # market open → the forming (last) bar is not final; earlier bars are
    assert records[0]["is_final"] is True
    assert records[-1]["is_final"] is False

    assert body["cache"]["cached"] is True
    assert body["cache"]["cache_key"] == "ohlcv:AAPL.XNAS:ohlcv-1m"
    assert body["cache"]["market_phase"] == "open"
    # US venues always have a next calendar boundary (wall-clock derived).
    assert isinstance(body["cache"]["next_change_at"], int)
    assert body["cache"]["next_change_at"] > 0
    assert body["page"]["has_more"] is True
    assert isinstance(body["page"]["next_cursor"], str)
    # live path: no historical window passed to the service
    intraday.get_stock_intraday.assert_awaited_once_with(
        symbol="AAPL", interval="1min", from_date=None, to_date=None,
        user_id="test-user-123", live=None,
    )


async def test_closed_market_last_bar_is_final(client):
    bars = [_bar(_MS), _bar(_MS + 60_000)]
    with _stub(intraday_result=_intraday_result(bars, phase="closed")):
        resp = await client.get("/api/v1/market-data/bars/AAPL?schema=ohlcv-1m")
    body = resp.json()
    assert all(r["is_final"] is True for r in body["series"]["records"])
    assert body["cache"]["market_phase"] == "closed"


async def test_header_defaults_when_envelope_absent(client):
    bars = [_bar(_MS)]
    with _stub(intraday_result=_intraday_result(bars, header=None)):
        resp = await client.get("/api/v1/market-data/bars/AAPL?schema=ohlcv-1m")
    header = resp.json()["series"]["header"]
    assert header["publisher"] is None
    assert header["price_treatment"] == "split_adjusted"
    assert header["tier"] == "realtime"
    assert header["watermark"] == _MS  # falls back to the FetchResult watermark


# ---------------------------------------------------------------------------
# after= delta poll
# ---------------------------------------------------------------------------

async def test_after_filters_records(client):
    bars = [_bar(_MS - 60_000), _bar(_MS), _bar(_MS + 60_000), _bar(_MS + 120_000)]
    with _stub(intraday_result=_intraday_result(bars)) as (intraday, _):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&after={_MS}")

    body = resp.json()
    times = [r["time"] for r in body["series"]["records"]]
    # Inclusive of the cursor bar — it was forming at the client's last poll,
    # so its settled form must be re-delivered. Older bars are filtered.
    assert times == [_MS, _MS + 60_000, _MS + 120_000]
    assert body["page"] == {"next_cursor": None, "has_more": False}
    # same live fetch as default — no extra upstream window
    intraday.get_stock_intraday.assert_awaited_once_with(
        symbol="AAPL", interval="1min", from_date=None, to_date=None,
        user_id="test-user-123", live=None,
    )


async def test_after_at_watermark_returns_forming_bar(client):
    # The forming bar's anchor never advances while it fills — a poll at
    # after == watermark must still return it so clients see intra-bar updates.
    bars = [_bar(_MS), _bar(_MS + 60_000)]
    with _stub(intraday_result=_intraday_result(bars, phase="open")):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&after={_MS + 60_000}")

    records = resp.json()["series"]["records"]
    assert [r["time"] for r in records] == [_MS + 60_000]
    assert records[0]["is_final"] is False


async def test_after_at_watermark_returns_settled_head_when_closed(client):
    # Closed market: the cursor bar is still re-delivered — the client's copy
    # may predate settlement (it polled while the bar was forming), and the
    # server can't distinguish that. One redundant bar; client merge is
    # idempotent.
    bars = [_bar(_MS), _bar(_MS + 60_000)]
    with _stub(intraday_result=_intraday_result(bars, phase="closed")):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&after={_MS + 60_000}")

    records = resp.json()["series"]["records"]
    assert [r["time"] for r in records] == [_MS + 60_000]
    assert records[0]["is_final"] is True


async def test_after_behind_server_watermark_excludes_stale_head(client):
    # Client ahead of a stale server cache: nothing useful to send back.
    bars = [_bar(_MS)]
    with _stub(intraday_result=_intraday_result(bars, phase="open")):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&after={_MS + 60_000}")

    assert resp.json()["series"]["records"] == []


async def test_after_non_int_is_422(client):
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get("/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&after=notanint")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# before= period-aligned paging
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "schema,expect_from,expect_to",
    [
        ("ohlcv-1m", "2026-06-22", "2026-06-28"),   # ISO week (Mon–Sun)
        ("ohlcv-5m", "2026-06-22", "2026-06-28"),
        ("ohlcv-1h", "2026-06-01", "2026-06-28"),   # calendar month
        ("ohlcv-4h", "2026-06-01", "2026-06-28"),
    ],
)
async def test_before_window_intraday(client, schema, expect_from, expect_to):
    bars = [_bar(_MS)]
    with _stub(intraday_result=_intraday_result(bars, cache_key="hist")) as (intraday, _):
        resp = await client.get(
            f"/api/v1/market-data/bars/AAPL?schema={schema}&before=2026-06-29"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["page"] == {"next_cursor": expect_from, "has_more": True}
    call = intraday.get_stock_intraday.await_args
    assert call.kwargs["from_date"] == expect_from
    assert call.kwargs["to_date"] == expect_to
    # A paging window is historical by definition — never the live cache key,
    # even when its right edge falls inside the live-classification zone.
    assert call.kwargs["live"] is False


async def test_before_window_daily_is_calendar_year(client):
    bars = [_bar(_MS)]
    with _stub(daily_result=_daily_result(bars, cache_key="hist")) as (_intraday, daily):
        resp = await client.get(
            "/api/v1/market-data/bars/AAPL?schema=ohlcv-1d&before=2026-06-29"
        )

    assert resp.status_code == 200
    assert resp.json()["page"] == {"next_cursor": "2026-01-01", "has_more": True}
    call = daily.get_stock_daily.await_args
    assert call.kwargs["from_date"] == "2026-01-01"
    assert call.kwargs["to_date"] == "2026-06-28"
    assert call.kwargs["live"] is False


async def test_before_empty_page_stops_paging(client):
    with _stub(intraday_result=_intraday_result([], cache_key="hist", header=None)):
        resp = await client.get(
            "/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&before=2026-06-29"
        )
    assert resp.status_code == 200
    assert resp.json()["page"] == {"next_cursor": None, "has_more": False}


async def test_invalid_cursor_is_422(client):
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get(
            "/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&before=not-a-date"
        )
    assert resp.status_code == 422
    assert "Invalid cursor" in resp.json()["detail"]


async def test_phaseless_result_falls_back_to_the_clock(client):
    # Windowed fetches don't carry a market_phase; the router must answer
    # from the instrument clock, never a hardcoded "closed" default.
    stub_clock = MagicMock()
    stub_clock.market_phase.return_value = "open"
    stub_clock.next_phase_change_ms.return_value = 1_800_000_000_000
    with (
        _stub(intraday_result=_intraday_result([_bar(_MS)], phase=None, cache_key="hist")),
        patch("src.server.app.bars.clock_for", return_value=stub_clock),
    ):
        resp = await client.get(
            "/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&before=2026-06-29"
        )
    assert resp.status_code == 200
    cache = resp.json()["cache"]
    assert cache["market_phase"] == "open"
    assert cache["next_change_at"] == 1_800_000_000_000


# ---------------------------------------------------------------------------
# Index routing
# ---------------------------------------------------------------------------

async def test_index_spelling_routes_to_index_path(client):
    bars = [_bar(_MS)]
    result = _intraday_result(bars, symbol="GSPC", cache_key="ohlcv:SPX.INDEX:ohlcv-1m")
    with _stub(intraday_result=result) as (intraday, _):
        resp = await client.get("/api/v1/market-data/bars/%5EGSPC?schema=ohlcv-1m")  # ^GSPC

    assert resp.status_code == 200
    assert resp.json()["series"]["header"]["instrument_key"] == "SPX.INDEX"
    intraday.get_index_intraday.assert_awaited_once()
    intraday.get_stock_intraday.assert_not_awaited()
    # legacy bare index spelling reaches the service
    assert intraday.get_index_intraday.await_args.kwargs["symbol"] == "GSPC"


async def test_asset_class_hint_forces_index(client):
    bars = [_bar(_MS)]
    result = _intraday_result(bars, symbol="GSPC", cache_key="ohlcv:SPX.INDEX:ohlcv-1m")
    with _stub(intraday_result=result) as (intraday, _):
        resp = await client.get("/api/v1/market-data/bars/GSPC?schema=ohlcv-1m&asset_class=index")

    assert resp.status_code == 200
    assert resp.json()["series"]["header"]["instrument_key"] == "SPX.INDEX"
    intraday.get_index_intraday.assert_awaited_once()


# ---------------------------------------------------------------------------
# asset_class validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["crypto", "fx"])
async def test_unsupported_asset_class_is_422(client, bad):
    # crypto/fx hints were silently misrouted as US equities — now refused.
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&asset_class={bad}")
    assert resp.status_code == 422
    assert "asset_class" in resp.json()["detail"]


@pytest.mark.parametrize("ok", ["equity", "stock"])
async def test_equity_asset_class_hints_pass_validation(client, ok):
    # equity/stock are accepted and route through the normal (non-index) path.
    with _stub(intraday_result=_intraday_result([_bar(_MS)])):
        resp = await client.get(f"/api/v1/market-data/bars/AAPL?schema=ohlcv-1m&asset_class={ok}")
    assert resp.status_code == 200
