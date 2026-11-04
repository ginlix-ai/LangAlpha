"""Unit tests for the yfinance data source — no network."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.data_client.yfinance.data_source import (
    YFinanceDataSource,
    _fetch_history,
    _place_in_sessions,
)
from src.data_client.yfinance.yahoo import yahoo_symbol


def _history_kwargs(**call_kwargs) -> dict:
    """Run _fetch_history with a stubbed Ticker; return the kwargs it passed."""
    ticker = MagicMock()
    ticker.history.return_value = None  # empty result — we only care about kwargs
    with patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker):
        _fetch_history("0700.HK", "1m", **call_kwargs)
    return ticker.history.call_args.kwargs


def test_fetch_history_end_date_is_inclusive():
    """to_date is inclusive per the provider contract, but yfinance's ``end``
    is an exclusive exchange-local midnight. Without the +1-day shift, a window
    ending on the current venue date silently drops today's live session —
    non-US charts froze on the prior session (frozen 0700.HK chart bug).
    """
    kwargs = _history_kwargs(start="2026-07-03", end="2026-07-06")
    assert kwargs["start"] == "2026-07-03"
    assert kwargs["end"] == "2026-07-07"


def test_fetch_history_open_ended_window_has_no_end_bound():
    kwargs = _history_kwargs(start="2026-07-03", end=None)
    assert kwargs["start"] == "2026-07-03"
    assert "end" not in kwargs


def test_fetch_history_unparseable_end_passes_through():
    kwargs = _history_kwargs(start=None, end="garbage")
    assert kwargs["end"] == "garbage"


def _cst(hhmm: str) -> int:
    """Epoch ms of 2026-09-30 HH:MM in Shanghai; the base is that midnight."""
    h, m = map(int, hhmm.split(":"))
    return 1790697600000 + (h * 60 + m) * 60_000


def test_a_shares_drop_yahoo_lunch_padding_and_start_the_afternoon_at_the_reopen():
    """Yahoo padded 11:30-13:00 with flat zero-volume 5m bars (66 a day, not
    48), and its hourly grid put the 13:00-13:30 trading in a 12:30 bar."""
    five = [{"time": _cst(t), "volume": v} for t, v in (("11:25", 9), ("11:30", 0), ("12:55", 0), ("13:00", 7))]
    assert [b["time"] for b in _place_in_sessions(five, "000001.SZ", False, "5min")] == [
        _cst("11:25"), _cst("13:00"),
    ]
    hourly = [{"time": _cst(t), "volume": 1} for t in ("10:30", "11:30", "12:30", "13:30")]
    assert [b["time"] for b in _place_in_sessions(hourly, "000001.SZ", False, "1hour")] == [
        _cst("10:30"), _cst("13:00"), _cst("13:30"),
    ]
    us = [{"time": _cst("12:00"), "volume": 1}]
    assert _place_in_sessions(us, "AAPL", False, "5min") == us


@pytest.mark.asyncio
async def test_get_snapshots_returns_bare_index_symbol():
    """Indices are queried from Yahoo with a caret ("^GSPC"), but the snapshot
    must echo back the bare requested symbol ("GSPC") so the provider chain
    matches it instead of dropping it as unrequested. Regression for #287.
    """
    def fake_fetch(sym: str) -> dict:
        # Mirrors the real fetch: echoes whatever symbol it was queried with.
        return {"symbol": sym, "price": 5000.0}

    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=fake_fetch,
    ):
        result = await YFinanceDataSource().get_snapshots(
            ["GSPC"], asset_type="indices"
        )

    assert result == [{"symbol": "GSPC", "price": 5000.0}]


@pytest.mark.asyncio
async def test_get_snapshots_preserves_order_and_drops_failures():
    """Symbol restoration stays aligned when a fetch returns None."""
    def fake_fetch(sym: str) -> dict | None:
        return None if sym == "^IXIC" else {"symbol": sym, "price": 1.0}

    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=fake_fetch,
    ):
        result = await YFinanceDataSource().get_snapshots(
            ["GSPC", "IXIC", "DJI"], asset_type="indices"
        )

    assert [r["symbol"] for r in result] == ["GSPC", "DJI"]


@pytest.mark.asyncio
async def test_get_snapshots_skips_only_the_symbol_yahoo_cannot_be_asked_for():
    """An unspellable symbol is a miss, not a failure of the batch; a batch of
    nothing else is refused with the reason."""
    def fake_fetch(sym: str) -> dict:
        return {"symbol": sym, "price": 1.0}

    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=fake_fetch,
    ) as fetch:
        result = await YFinanceDataSource().get_snapshots(["AAPL", "EUR/USD", "MSFT"])
        with pytest.raises(ValueError, match="EUR/USD"):
            await YFinanceDataSource().get_snapshots(["EUR/USD"])

    assert [r["symbol"] for r in result] == ["AAPL", "MSFT"]
    assert [c.args[0] for c in fetch.call_args_list] == ["AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_get_snapshots_stocks_pass_symbol_through_unchanged():
    """Stocks aren't caret-prefixed; the symbol is returned as requested."""
    def fake_fetch(sym: str) -> dict:
        return {"symbol": sym, "price": 190.0}

    with patch(
        "src.data_client.yfinance.data_source._fetch_single_snapshot",
        side_effect=fake_fetch,
    ):
        result = await YFinanceDataSource().get_snapshots(["AAPL"])

    assert result == [{"symbol": "AAPL", "price": 190.0}]


@pytest.mark.parametrize(
    ("symbol", "yahoo"),
    [
        # On Yahoo, EUR-USD is a defunct coin and USD-JPY is no symbol at all.
        ("EUR-USD", "EURUSD=X"), ("USD-JPY", "USDJPY=X"), ("JPY=X", "USDJPY=X"),
        ("BTCUSD", "BTC-USD"), ("BTC-USD", "BTC-USD"),
        ("600519.SH", "600519.SS"), ("GC=F", "GC=F"), ("AAPL", "AAPL"),
    ],
)
def test_yahoo_spells_pairs_its_own_way(symbol, yahoo):
    assert yahoo_symbol(symbol) == yahoo


def test_a_daily_bar_keeps_yahoos_date_in_the_instruments_zone():
    """Yahoo stamps an FX day at London midnight, the previous evening in UTC in summer."""
    import pandas as pd
    from zoneinfo import ZoneInfo

    london = ZoneInfo("Europe/London")
    days = pd.DatetimeIndex([pd.Timestamp("2026-07-01", tz=london), pd.Timestamp("2026-07-02", tz=london)])
    frame = pd.DataFrame(
        {"Open": [1.14, 1.13], "High": [1.15, 1.14], "Low": [1.13, 1.12], "Close": [1.14, 1.13], "Volume": [0, 0]},
        index=days,
    )
    ticker = MagicMock()
    ticker.history.return_value = frame
    with patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker):
        bars = _fetch_history("EURUSD=X", "1d", "2026-07-01", "2026-07-02", day_tz=ZoneInfo("UTC"))
    assert [b["time"] for b in bars] == [1782864000000, 1782950400000]  # 00:00 UTC on each date
