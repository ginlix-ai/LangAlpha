"""Shape + invariant locks for intraday and daily bar endpoints."""

import pytest

from .conftest import (
    HK_STOCK,
    LSE_STOCK,
    US_INDEX,
    US_STOCK,
    assert_ohlcv_response,
)

pytestmark = pytest.mark.regression


@pytest.mark.parametrize("interval", ["1min", "5min", "1hour"])
def test_stock_intraday_shape(http, interval):
    r = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": interval})
    assert r.status_code == 200
    assert_ohlcv_response(r.json(), symbol=US_STOCK, expect_interval=interval)


@pytest.mark.parametrize("symbol", [HK_STOCK, LSE_STOCK])
def test_non_us_stock_intraday_shape(http, symbol):
    r = http.get(f"/intraday/stocks/{symbol}", params={"interval": "1hour"})
    assert r.status_code == 200
    assert_ohlcv_response(r.json(), symbol=symbol, expect_interval="1hour")


def test_index_intraday_shape(http):
    r = http.get(f"/intraday/indexes/{US_INDEX}", params={"interval": "1hour"})
    assert r.status_code == 200
    payload = r.json()
    assert_ohlcv_response(payload, symbol=US_INDEX, expect_interval="1hour")
    # Index bars carry volume as int 0 today (protocol will make them null;
    # this legacy wrapper must keep returning ints through Phase 4)
    assert all(isinstance(b["volume"], int) for b in payload["data"])


@pytest.mark.parametrize("symbol,endpoint", [
    (US_STOCK, "/daily/stocks"),
    (HK_STOCK, "/daily/stocks"),
    (LSE_STOCK, "/daily/stocks"),
    (US_INDEX, "/daily/indexes"),
])
def test_daily_shape(http, symbol, endpoint):
    r = http.get(f"{endpoint}/{symbol}")
    assert r.status_code == 200
    assert_ohlcv_response(r.json(), symbol=symbol)


def test_daily_date_range_window(http):
    r = http.get(f"/daily/stocks/{US_STOCK}", params={"from": "2026-01-02", "to": "2026-03-31"})
    assert r.status_code == 200
    payload = r.json()
    assert_ohlcv_response(payload, symbol=US_STOCK)
    # ~60 trading days in Q1; a hard historical window must not leak bars outside it
    assert 40 <= payload["count"] <= 70, f"unexpected Q1 bar count {payload['count']}"
    times = [b["time"] for b in payload["data"]]
    jan1_ms, apr2_ms = 1_767_225_600_000, 1_775_088_000_000  # generous UTC bounds
    assert min(times) >= jan1_ms and max(times) <= apr2_ms


def test_repeat_call_hits_cache(http):
    first = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1hour"}).json()
    second = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1hour"}).json()
    assert second["cache"]["cached"] is True
    if second["cache"]["market_phase"] == "closed":
        assert second["data"] == first["data"], "closed-market cache hit must be byte-stable"


def test_invalid_interval_rejected(http):
    r = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "7min"})
    assert r.status_code == 422
    assert "Invalid interval" in r.json()["detail"]
