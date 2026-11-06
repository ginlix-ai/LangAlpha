"""The yfinance quote's previous close, volume and precision — no network."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from src.data_client.yfinance.data_source import _fetch_single_snapshot, _normalize_bar, _quoted


def _snapshot(sym: str, tz: str, closes: dict[str, float], meta: dict, volume: int = 1000) -> dict:
    """Snapshot of *sym* over a daily series of *closes* (date -> close)."""
    zone = ZoneInfo(tz)
    days = pd.DataFrame(
        [{"Open": c, "High": c, "Low": c, "Close": c, "Volume": volume} for c in closes.values()],
        index=[pd.Timestamp(d, tz=zone) for d in closes],
    )
    ticker = MagicMock()
    ticker.history.return_value = days
    ticker.history_metadata = {"exchangeTimezoneName": tz, **meta}
    with patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker):
        snap = _fetch_single_snapshot(sym)
    assert snap is not None
    return snap


def _at(stamp: str, tz: str) -> pd.Timestamp:
    return pd.Timestamp(stamp, tz=tz)


def test_previous_close_is_the_prior_sessions_bar_not_a_stale_chart_close():
    """399001.SZ: the chart's previous close was two sessions back."""
    snap = _snapshot(
        "399001.SZ", "Asia/Shanghai",
        {"2026-11-02": 11010.68, "2026-11-03": 10876.99, "2026-11-04": 11335.69},
        {
            "regularMarketPrice": 11335.69, "chartPreviousClose": 11010.7, "instrumentType": "INDEX",
            "regularMarketTime": _at("2026-11-04 15:00", "Asia/Shanghai"),
        },
    )
    assert snap["previous_close"] == 10876.99
    assert snap["volume"] == 0
    assert snap["change_percent"] == pytest.approx(4.2171, abs=1e-4)


def test_a_missing_prior_bar_falls_back_to_the_chart_close():
    """000300.SS: Yahoo's daily series held only today's bar."""
    snap = _snapshot(
        "000300.SS", "Asia/Shanghai", {"2026-11-04": 4101.335},
        {
            "regularMarketPrice": 4101.335, "chartPreviousClose": 4008.8748, "instrumentType": "INDEX",
            "regularMarketTime": _at("2026-11-04 15:00", "Asia/Shanghai"),
        },
    )
    assert snap["previous_close"] == 4008.8748


def test_a_futures_evening_measures_from_todays_settlement():
    """After the 17:00 CT reopen the session is tomorrow's, so the base is
    today's settlement, not yesterday's."""
    snap = _snapshot(
        "CL=F", "America/New_York", {"2026-11-02": 96.46, "2026-11-03": 96.09, "2026-11-04": 96.5},
        {"regularMarketPrice": 96.5, "chartPreviousClose": 104.12, "regularMarketTime": _at("2026-11-03 18:30", "America/New_York")},
    )
    assert snap["previous_close"] == 96.09


def test_a_venue_without_a_calendar_reads_its_own_dates():
    """^KS11 has no calendar here; the New York one would put its 15:30 KST
    close on the previous New York session."""
    snap = _snapshot(
        "^KS11", "Asia/Seoul", {"2026-10-30": 5700.0, "2026-11-02": 5859.08, "2026-11-03": 5648.79, "2026-11-04": 5683.8},
        {"regularMarketPrice": 5683.8, "chartPreviousClose": 5859.08, "regularMarketTime": _at("2026-11-04 15:30", "Asia/Seoul")},
    )
    assert (snap["previous_close"], snap["open"]) == (5648.79, 5683.8)


def test_a_market_that_never_closes_takes_the_chart_day_boundary():
    """A sub-cent coin's daily bars are flat and rounded; its quote keeps every digit."""
    snap = _snapshot(
        "SHIB-USD", "UTC", {"2026-11-03": 5e-06, "2026-11-04": 4.65e-06},
        {"regularMarketPrice": 4.65e-06, "chartPreviousClose": 4.631e-06, "regularMarketTime": _at("2026-11-04 07:00", "UTC")},
    )
    assert (snap["price"], snap["previous_close"]) == (4.65e-06, 4.631e-06)
    assert snap["change_percent"] == pytest.approx(0.41028, abs=1e-5)


@pytest.mark.parametrize(("sym", "volume"), [("000300.SS", 0), ("399001.SZ", 0), ("510300.SS", 3047391701)])
def test_an_a_share_index_carries_no_volume(sym, volume):
    snap = _snapshot(
        sym, "Asia/Shanghai", {"2026-11-03": 4.092, "2026-11-04": 4.175},
        {"regularMarketPrice": 4.175, "regularMarketTime": _at("2026-11-04 15:00", "Asia/Shanghai")},
        volume=3047391701,
    )
    assert snap["volume"] == volume


def test_quoted_restores_the_float32_quote_and_passes_exact_values():
    assert _quoted(float(np.float32(4.62e-06))) == 4.62e-06
    assert _quoted(float(np.float32(4059.5269))) == 4059.5269
    assert _quoted(0.1) == 0.1


def test_a_sub_cent_bar_keeps_its_digits():
    widened = float(np.float32(4.631e-06))
    row = {"Open": widened, "High": widened, "Low": widened, "Close": widened, "Volume": 7}
    bar = _normalize_bar(datetime(2026, 11, 4, tzinfo=ZoneInfo("UTC")), row)
    assert bar["close"] == 4.631e-06
