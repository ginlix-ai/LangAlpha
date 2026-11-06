"""An options snapshot row carries exchange-local times and says when its session is stale."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.data_client.ginlix_data.option_snapshots import snapshot_row

_NY = ZoneInfo("America/New_York")


def _ns(*args: int) -> int:
    return int(datetime(*args, tzinfo=_NY).timestamp() * 1e9)


def _raw(last_updated: int | None, timeframe: str | None = "DELAYED") -> dict:
    session = {"close": 222.0, "change_percent": 0.0, "volume": 1.0}
    if last_updated is not None:
        session["last_updated"] = last_updated
    if timeframe:
        session["timeframe"] = timeframe
    return {
        "ticker": "O:ZZZZ261120C00125000",
        "type": "options",
        "name": "ZZZZ $125.00 call",
        "market_status": "open",
        "session": session,
        "details": {"strike_price": 125.0},
        "greeks": {"delta": 0.98, "gamma": 0.0002, "theta": -0.3, "vega": 0.04},
        "implied_volatility": 0.39,
        "open_interest": 33,
        "break_even_price": 345.05,
        "underlying_asset": {
            "ticker": "ZZZZ", "price": 343.32, "last_updated": _ns(2026, 11, 5, 12, 0, 1),
            "timeframe": "REAL-TIME", "change_to_break_even": 1.73,
        },
    }


def test_a_session_from_an_earlier_trading_day_is_stale_and_dated():
    row = snapshot_row(_raw(_ns(2026, 10, 23, 10, 54, 3)), datetime(2026, 11, 5, 12, 0, tzinfo=_NY))

    assert row["session"]["last_updated"] == "2026-10-23 10:54:03"
    assert row["session"]["stale"] is True
    assert row["session"]["close"] == 222.0


def test_a_session_updated_today_is_not_stale():
    row = snapshot_row(_raw(_ns(2026, 11, 5, 11, 46, 2)), datetime(2026, 11, 5, 12, 0, tzinfo=_NY))

    assert row["session"]["stale"] is False


def test_a_delayed_feed_is_not_stale_before_it_shows_the_open():
    # At 09:40 a 15-minute delayed feed still shows the previous session, so
    # yesterday's close is current; a realtime feed has reached today's open.
    yesterday = _ns(2026, 11, 4, 15, 46, 2)
    at = datetime(2026, 11, 5, 9, 40, tzinfo=_NY)

    assert snapshot_row(_raw(yesterday, "DELAYED"), at)["session"]["stale"] is False
    assert snapshot_row(_raw(yesterday, "REAL-TIME"), at)["session"]["stale"] is True


def test_a_weekend_or_holiday_read_keeps_the_last_session_current():
    friday = _ns(2026, 11, 6, 15, 46, 2)
    wednesday = _ns(2026, 11, 25, 15, 46, 2)

    saturday = snapshot_row(_raw(friday), datetime(2026, 11, 7, 12, 0, tzinfo=_NY))
    thanksgiving = snapshot_row(_raw(wednesday), datetime(2026, 11, 26, 12, 0, tzinfo=_NY))

    assert saturday["session"]["stale"] is False
    assert thanksgiving["session"]["stale"] is False


def test_the_row_keeps_every_vendor_field_but_the_terms_and_times_them_locally():
    raw = _raw(_ns(2026, 11, 5, 11, 46, 2))
    raw["last_quote"] = {"bid": 1.0, "ask": 1.1, "last_updated": _ns(2026, 11, 5, 11, 59, 0)}

    row = snapshot_row(raw, datetime(2026, 11, 5, 12, 0, tzinfo=_NY))

    assert set(row) == set(raw) - {"details"}
    assert row["last_quote"] == {"bid": 1.0, "ask": 1.1, "last_updated": "2026-11-05 11:59:00"}
    assert row["underlying_asset"] == {
        "ticker": "ZZZZ", "price": 343.32, "last_updated": "2026-11-05 12:00:01",
        "timeframe": "REAL-TIME", "change_to_break_even": 1.73,
    }


def test_a_session_without_a_time_gets_no_stale_mark():
    # An older ginlix-data, or a lookup that ran out of time, sends no session time.
    raw = {"ticker": "O:ZZZZ261120C00125000", "type": "options", "name": "ZZZZ $125.00 call",
           "market_status": "closed", "session": {"close": 222.0}}

    row = snapshot_row(raw, datetime(2026, 11, 5, 12, 0, tzinfo=_NY))

    assert row == {"ticker": "O:ZZZZ261120C00125000", "type": "options", "name": "ZZZZ $125.00 call",
                   "market_status": "closed", "session": {"close": 222.0}}



def test_an_index_underlying_is_priced_from_its_value():
    raw = {"ticker": "O:SPX261120C05000000", "session": {"close": 1.0},
           "underlying_asset": {"ticker": "I:SPX", "value": 6000.5}}

    row = snapshot_row(raw, datetime(2026, 11, 5, 12, 0, tzinfo=_NY))

    assert row["underlying_asset"] == {"ticker": "I:SPX", "price": 6000.5}
