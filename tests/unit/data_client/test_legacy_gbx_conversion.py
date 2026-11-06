"""Legacy-path GBX (pence) → major-unit conversion for fmp and yfinance.

The legacy bar/snapshot interface (get_intraday / get_daily / get_snapshots)
scales price-like fields by 0.01 for XLON (GBX-quoted) symbols and leaves US
symbols untouched; change_percent and volume never scale. The protocol path
(normalize_series) applies the same rule independently from its own raw rows —
these tests pin single-conversion per path and that the two never double up.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.data_client.fmp.data_source import FMPDataSource
from src.data_client.fmp.data_source import normalize_series as fmp_normalize_series
from src.data_client.fmp.financial_source import FMPFinancialSource
from src.data_client.normalize import minor_unit_places, scale_quote_prices
from src.data_client.yfinance.data_source import YFinanceDataSource, _normalize_bar
from src.data_client.yfinance.data_source import normalize_series as yf_normalize_series
from src.data_client.yfinance.financial_source import _get_profile as yf_profile
from src.data_client.yfinance.financial_source import _get_realtime_quote as yf_realtime_quote
from market_protocol import to_canonical

LSE = "VOD.L"   # XLON — quotes GBX (pence)
US = "AAPL"     # XNAS — quotes USD (major units)
_LONDON = ZoneInfo("Europe/London")


# --- FMP fakes --------------------------------------------------------------

class _FakeFMPClient:
    """Async-context-manager stand-in for FMPClient with canned responses."""

    def __init__(self, *, intraday=None, daily=None, quotes=None):
        self._intraday = intraday or []
        self._daily = daily or []
        self._quotes = quotes or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get_intraday_chart(self, **_):
        return self._intraday

    async def get_stock_price(self, **_):
        return self._daily

    async def get_batch_quotes(self, _symbols):
        return self._quotes


def _fmp_bar(**over):
    row = {"date": "2024-01-15 12:00:00", "open": 98.0, "high": 99.0,
           "low": 97.0, "close": 98.5, "volume": 1000}
    row.update(over)
    return row


def _fmp_quote(symbol, **over):
    q = {"symbol": symbol, "name": "n", "price": 98.5, "change": 1.5,
         "changePercentage": 1.55, "previousClose": 97.0, "open": 97.5,
         "dayHigh": 99.0, "dayLow": 96.5, "volume": 1000}
    q.update(over)
    return q


# --- FMP legacy path --------------------------------------------------------

@pytest.mark.asyncio
async def test_fmp_intraday_lse_scaled_to_major_units():
    fake = _FakeFMPClient(intraday=[_fmp_bar()])
    with patch("src.data_client.fmp.data_source.FMPClient", return_value=fake):
        bars = await FMPDataSource().get_intraday(LSE, "1hour")
    assert len(bars) == 1
    bar = bars[0]
    assert set(bar) == {"time", "open", "high", "low", "close", "volume"}
    assert bar["open"] == pytest.approx(0.98)
    assert bar["high"] == pytest.approx(0.99)
    assert bar["low"] == pytest.approx(0.97)
    assert bar["close"] == pytest.approx(0.985)
    assert bar["volume"] == 1000  # share count — never scaled


@pytest.mark.asyncio
async def test_fmp_intraday_us_untouched():
    fake = _FakeFMPClient(
        intraday=[_fmp_bar(open=190.0, high=191.0, low=189.0, close=190.5)]
    )
    with patch("src.data_client.fmp.data_source.FMPClient", return_value=fake):
        bars = await FMPDataSource().get_intraday(US, "1hour")
    bar = bars[0]
    assert bar["open"] == 190.0 and bar["close"] == 190.5
    assert bar["high"] == 191.0 and bar["low"] == 189.0


@pytest.mark.asyncio
async def test_fmp_snapshot_lse_scaled_percent_and_volume_invariant():
    fake = _FakeFMPClient(quotes=[_fmp_quote(LSE)])
    with patch("src.data_client.fmp.data_source.FMPClient", return_value=fake):
        snaps = await FMPDataSource().get_snapshots([LSE])
    s = snaps[0]
    assert s["price"] == pytest.approx(0.985)
    assert s["change"] == pytest.approx(0.015)
    assert s["previous_close"] == pytest.approx(0.97)
    assert s["open"] == pytest.approx(0.975)
    assert s["high"] == pytest.approx(0.99)
    assert s["low"] == pytest.approx(0.965)
    assert s["change_percent"] == 1.55  # ratio — scale-invariant
    assert s["volume"] == 1000          # share count — never scaled


@pytest.mark.asyncio
async def test_fmp_snapshot_us_untouched():
    fake = _FakeFMPClient(quotes=[_fmp_quote(
        US, price=190.0, change=2.0, previousClose=188.0,
        open=189.0, dayHigh=191.0, dayLow=188.5,
    )])
    with patch("src.data_client.fmp.data_source.FMPClient", return_value=fake):
        snaps = await FMPDataSource().get_snapshots([US])
    s = snaps[0]
    assert s["price"] == 190.0 and s["previous_close"] == 188.0
    assert s["high"] == 191.0 and s["low"] == 188.5


# --- yfinance fakes ---------------------------------------------------------

class _FakeTicker:
    def __init__(self, df):
        self._df = df

    def history(self, **_):
        return self._df


def _yf_df(open_=98.0, high=99.0, low=97.0, close=98.5, volume=1000):
    import pandas as pd

    idx = pd.DatetimeIndex([pd.Timestamp("2024-01-15 12:00:00", tz="UTC")])
    return pd.DataFrame(
        {"Open": [open_], "High": [high], "Low": [low],
         "Close": [close], "Volume": [volume]},
        index=idx,
    )


def _yf_snapshot(sym, **over):
    snap = {"symbol": sym, "name": None, "price": 98.5, "change": 1.5,
            "change_percent": 1.55, "previous_close": 97.0, "open": 97.5,
            "high": 99.0, "low": 96.5, "volume": 1000, "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None}
    snap.update(over)
    return snap


# --- yfinance legacy path ---------------------------------------------------

@pytest.mark.asyncio
async def test_yfinance_intraday_lse_scaled_to_major_units():
    with patch(
        "src.data_client.yfinance.yahoo.yf.Ticker",
        return_value=_FakeTicker(_yf_df()),
    ):
        bars = await YFinanceDataSource().get_intraday(LSE, "1hour")
    assert len(bars) == 1
    bar = bars[0]
    assert set(bar) == {"time", "open", "high", "low", "close", "volume"}
    assert bar["open"] == pytest.approx(0.98)
    assert bar["close"] == pytest.approx(0.985)
    assert bar["volume"] == 1000


@pytest.mark.asyncio
async def test_yfinance_intraday_us_untouched():
    with patch(
        "src.data_client.yfinance.yahoo.yf.Ticker",
        return_value=_FakeTicker(_yf_df(open_=190.0, high=191.0, low=189.0, close=190.5)),
    ):
        bars = await YFinanceDataSource().get_intraday(US, "1hour")
    bar = bars[0]
    assert bar["open"] == 190.0 and bar["close"] == 190.5


@pytest.mark.asyncio
async def test_yfinance_snapshot_lse_scaled_percent_and_volume_invariant():
    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=lambda sym: _yf_snapshot(sym),
    ):
        snaps = await YFinanceDataSource().get_snapshots([LSE])
    s = snaps[0]
    assert s["symbol"] == LSE
    assert s["price"] == pytest.approx(0.985)
    assert s["change"] == pytest.approx(0.015)
    assert s["previous_close"] == pytest.approx(0.97)
    assert s["high"] == pytest.approx(0.99)
    assert s["low"] == pytest.approx(0.965)
    assert s["change_percent"] == 1.55
    assert s["volume"] == 1000


@pytest.mark.asyncio
async def test_yfinance_snapshot_us_untouched():
    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=lambda sym: _yf_snapshot(
            sym, price=190.0, previous_close=188.0, high=191.0, low=188.5,
        ),
    ):
        snaps = await YFinanceDataSource().get_snapshots([US])
    s = snaps[0]
    assert s["price"] == 190.0 and s["previous_close"] == 188.0


# --- realtime quote (financial source) --------------------------------------

_PENCE_QUOTE = {
    "symbol": LSE, "price": 117.2, "change": 0.35, "changePercentage": 0.3,
    "previousClose": 116.85, "open": 116.9, "dayHigh": 118.0, "dayLow": 116.0,
    "yearHigh": 132.072, "yearLow": 85.235, "priceAvg50": 110.0, "priceAvg200": 100.0,
    "volume": 24879602, "marketCap": 27152024882, "pe": 20.5, "eps": 5.7,
}


def test_quote_prices_scale_to_major_units_without_touching_the_input():
    rows = [dict(_PENCE_QUOTE)]
    scaled = scale_quote_prices(rows, LSE)[0]
    assert scaled["price"] == pytest.approx(1.172)
    assert scaled["change"] == pytest.approx(0.0035)
    assert scaled["yearHigh"] == pytest.approx(1.32072)
    assert scaled["yearLow"] == pytest.approx(0.85235)
    assert scaled["priceAvg50"] == pytest.approx(1.10)
    for unscaled in ("changePercentage", "volume", "marketCap", "pe", "eps"):
        assert scaled[unscaled] == _PENCE_QUOTE[unscaled]
    assert rows == [_PENCE_QUOTE]


def test_a_us_quote_passes_through_unchanged():
    rows = [{"price": 331.9, "yearHigh": 351.73, "yearLow": 243.42}]
    assert scale_quote_prices(rows, US) is rows
    assert scale_quote_prices([], LSE) == []


def test_a_quote_scales_only_the_price_fields_it_carries():
    assert scale_quote_prices([{"price": 117.2}], LSE) == [{"price": pytest.approx(1.172)}]


@pytest.mark.asyncio
async def test_fmp_realtime_quote_lse_scaled_to_major_units():
    client = MagicMock()
    client.get_quote = AsyncMock(return_value=[dict(_PENCE_QUOTE)])
    row = (await FMPFinancialSource(client).get_realtime_quote(LSE))[0]
    assert row["price"] == pytest.approx(1.172)
    assert row["yearLow"] == pytest.approx(0.85235)
    assert (row["volume"], row["eps"]) == (24879602, 5.7)


@pytest.mark.asyncio
async def test_fmp_realtime_quote_us_untouched():
    client = MagicMock()
    client.get_quote = AsyncMock(return_value=[{"price": 331.9}])
    assert await FMPFinancialSource(client).get_realtime_quote(US) == [{"price": 331.9}]


@pytest.mark.asyncio
async def test_fmp_profile_lse_price_scaled_to_major_units():
    client = MagicMock()
    client.get_profile = AsyncMock(return_value=[{"price": 117.2, "change": 0.35, "marketCap": 27152024882}])
    row = (await FMPFinancialSource(client).get_company_profile(LSE))[0]
    assert (row["price"], row["change"]) == (pytest.approx(1.172), pytest.approx(0.0035))
    assert row["marketCap"] == 27152024882


def test_yfinance_profile_lse_price_scaled_to_major_units():
    ticker = MagicMock()
    ticker.fast_info = {"lastPrice": 117.2, "marketCap": 27152024882, "currency": "GBp"}
    ticker.info = {"longName": "Vodafone Group Plc"}
    with patch("src.data_client.yfinance.financial_source.yahoo_ticker", return_value=ticker):
        row = yf_profile(LSE)[0]
    assert row["price"] == pytest.approx(1.172)
    assert row["marketCap"] == 27152024882


def _lse_quote_ticker(info: dict):
    import pandas as pd

    ticker = MagicMock()
    ticker.history.return_value = pd.DataFrame(
        [{"Open": 116.9, "High": 118.0, "Low": 116.0, "Close": 117.2, "Volume": 1000}],
        index=[pd.Timestamp("2026-07-01", tz="Europe/London")],
    )
    ticker.history_metadata = {
        "regularMarketPrice": 117.2, "chartPreviousClose": 116.85,
        "exchangeTimezoneName": "Europe/London",
        "regularMarketTime": pd.Timestamp("2026-07-01 16:30", tz="Europe/London"),
    }
    ticker.info = info
    return ticker


def test_yfinance_realtime_quote_lse_scaled_to_major_units():
    ticker = _lse_quote_ticker({"marketCap": 27152024882, "trailingPE": 20.5, "trailingEps": 5.7})
    with patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker):
        row = yf_realtime_quote(LSE)[0]
    assert row["price"] == pytest.approx(1.172)
    assert row["change"] == pytest.approx(0.0035)
    assert row["dayHigh"] == pytest.approx(1.18)
    assert (row["volume"], row["marketCap"], row["eps"]) == (1000, 27152024882, 5.7)


def test_yfinance_realtime_quote_prices_a_missing_market_cap_in_pounds():
    """Without info's market cap, shares times the pound price, never the pence one."""
    ticker = _lse_quote_ticker({})
    ticker.fast_info = {"shares": 1000, "marketCap": 117200.0}
    with patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker):
        row = yf_realtime_quote(LSE)[0]
    assert row["marketCap"] == pytest.approx(1172.0)

# --- scale resolution + no double-conversion --------------------------------

def test_minor_unit_places_resolution():
    assert minor_unit_places(LSE) == 2
    assert minor_unit_places(US) == 0
    assert minor_unit_places("0700.HK") == 0  # HKD listing, not pence


def test_no_double_conversion_fmp():
    """FMP protocol and legacy paths each convert the same raw pence row exactly
    once — to the same major-unit magnitude, never twice (0.00985) nor un-
    converted (98.5)."""
    ref = to_canonical(LSE)
    raw = _fmp_bar()
    protocol = fmp_normalize_series([raw], ref=ref, schema="ohlcv-1d").records[0]
    legacy = FMPDataSource._normalize(raw, _LONDON, minor_unit_places(LSE))
    assert protocol.close == pytest.approx(0.985)
    assert legacy["close"] == pytest.approx(0.985)
    assert 0.1 < legacy["close"] < 10


def test_no_double_conversion_yfinance():
    """normalize_series consumes RAW (pence) rows and scales once; feeding it the
    already-scaled legacy bar would double-scale — proving the two paths must
    stay independent (normalize_series has no legacy-fed caller in src/)."""
    ref = to_canonical(LSE)
    raw_row = {"time": 1_705_320_000_000, "open": 98.0, "high": 99.0,
               "low": 97.0, "close": 98.5, "volume": 1000}
    protocol = yf_normalize_series([raw_row], ref=ref, schema="ohlcv-1d").records[0]
    assert protocol.close == pytest.approx(0.985)  # ×0.01, applied once

    idx = datetime(2024, 1, 15, 12, tzinfo=timezone.utc)
    legacy_bar = _normalize_bar(
        idx, {"Open": 98.0, "High": 99.0, "Low": 97.0, "Close": 98.5, "Volume": 1000},
        minor_unit_places(LSE),
    )
    assert legacy_bar["close"] == pytest.approx(0.985)
    doubled = yf_normalize_series([legacy_bar], ref=ref, schema="ohlcv-1d").records[0]
    assert doubled.close == pytest.approx(0.00985)  # why the paths must not chain
