"""Shape + contract locks for the batch intraday endpoints."""

import pytest

from .conftest import (
    HK_STOCK,
    US_INDEX,
    US_INDEX_2,
    US_STOCK,
    US_STOCK_2,
    assert_bar_shape,
    assert_bars_monotonic,
)

pytestmark = pytest.mark.regression

BATCH_KEYS = {"interval", "results", "errors", "cache_stats"}
CACHE_STATS_KEYS = {"total_requests", "cache_hits", "cache_misses", "background_refreshes"}


def assert_batch_response(payload: dict, *, symbols: list[str], interval: str) -> None:
    assert set(payload.keys()) == BATCH_KEYS, f"batch keys drifted: {sorted(payload.keys())}"
    assert payload["interval"] == interval
    assert set(payload["cache_stats"].keys()) == CACHE_STATS_KEYS
    assert payload["cache_stats"]["total_requests"] == len(symbols)
    for sym in symbols:
        assert sym in payload["results"] or sym in payload["errors"], f"{sym} missing from results and errors"
    for sym, bars in payload["results"].items():
        assert len(bars) > 0, f"{sym}: empty bar list in results"
        for bar in bars:
            assert_bar_shape(bar, context=f"batch:{sym}")
        assert_bars_monotonic(bars, context=f"batch:{sym}")


def test_batch_stocks_shape(http):
    symbols = [US_STOCK, US_STOCK_2, HK_STOCK]
    r = http.post("/intraday/stocks", json={"symbols": symbols, "interval": "15min"})
    assert r.status_code == 200
    assert_batch_response(r.json(), symbols=symbols, interval="15min")


def test_batch_indexes_shape(http):
    symbols = [US_INDEX, US_INDEX_2]
    r = http.post("/intraday/indexes", json={"symbols": symbols, "interval": "1hour"})
    assert r.status_code == 200
    assert_batch_response(r.json(), symbols=symbols, interval="1hour")


def test_rejects_1s_interval(http):
    # 1s was removed from the REST API entirely (WS forming-bar stream only):
    # both batch and single-symbol reject it as an invalid interval.
    r = http.post("/intraday/stocks", json={"symbols": [US_STOCK], "interval": "1s"})
    assert r.status_code == 422
    assert "Invalid interval '1s'" in r.json()["detail"]

    r = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1s"})
    assert r.status_code == 422


def test_batch_rejects_over_50_symbols(http):
    symbols = [f"FAKE{i}" for i in range(51)]
    r = http.post("/intraday/stocks", json={"symbols": symbols, "interval": "1hour"})
    assert r.status_code == 422
