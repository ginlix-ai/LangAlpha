"""Snapshot normalization for the ginlix-data source.

The frontend header derives its settled-close presentation from this row, so
the mapping of the provider's session block — in particular the provider-exact
``regular_close`` next to the reduced-precision (1dp) change fields — is a
wire contract, not an implementation detail.
"""

from __future__ import annotations

from types import SimpleNamespace
from zoneinfo import ZoneInfo

from src.data_client.ginlix_data import data_source
from src.data_client.ginlix_data.data_source import GinlixDataSource


def _raw_snapshot() -> dict:
    return {
        "ticker": "TST",
        "name": "Test Corp",
        "market_status": "closed",
        "session": {
            "change": -10.0,
            "change_percent": -5.0,
            "close": 189.96,
            "early_trading_change": -9.1,
            "early_trading_change_percent": -4.55,
            "high": 195.5,
            "late_trading_change": -1.06,
            "late_trading_change_percent": -0.558,
            "low": 186.2,
            "open": 195.0,
            "previous_close": 200.0,
            "regular_trading_change": -10.0,
            "regular_trading_change_percent": -5.0,
            "volume": 1000.0,
        },
        "last_trade": {"price": 188.9},
        "last_minute": {"close": 189.2, "open": 188.9, "volume": 500.0},
    }


def test_normalize_snapshot_maps_exact_session_fields():
    row = GinlixDataSource._normalize_snapshot(_raw_snapshot())

    # price and regular_close both map the provider's session.close; the
    # separate key exists because downstream live-tick write-through
    # overwrites price, while the settled close must stay untouched.
    assert row["price"] == 189.96
    assert row["regular_close"] == 189.96
    assert row["previous_close"] == 200.0
    # Exact dollar moves alongside the rounded percents.
    assert row["late_trading_change"] == -1.06
    assert row["early_trading_change"] == -9.1
    assert row["regular_trading_change"] == -10.0
    assert row["last_trade_price"] == 188.9
    # Consolidated last sale (aggregate close) — may differ from last_trade
    # when the final print is an odd lot.
    assert row["last_minute_close"] == 189.2
    assert row["market_status"] == "closed"


def test_normalize_snapshot_tolerates_missing_session_fields():
    raw = {"ticker": "TST", "session": {}, "last_trade": {}}
    row = GinlixDataSource._normalize_snapshot(raw)

    assert row["symbol"] == "TST"
    assert row["price"] is None
    assert row["regular_close"] is None
    assert row["late_trading_change"] is None
    assert row["last_minute_close"] is None
    assert row["volume"] is None


def test_the_print_time_is_the_last_trade_in_ms():
    # Massive stamps trades in nanoseconds. Without an as_of the quote is never
    # measured, and a declared realtime tier then reads live on a weekend.
    raw = _raw_snapshot()
    raw["last_trade"]["last_updated"] = 1_789_000_000_123_456_789
    assert GinlixDataSource._normalize_snapshot(raw)["as_of"] == 1_789_000_000_123

    index = {"ticker": "I:TST", "session": {"close": 1.0}, "last_updated": 1_789_000_000_000_000_000}
    assert GinlixDataSource._normalize_snapshot(index, "indices")["as_of"] == 1_789_000_000_000

    assert GinlixDataSource._normalize_snapshot({"ticker": "TST", "session": {}})["as_of"] is None


def test_a_class_share_row_comes_back_in_our_spelling():
    # Massive answers BRK.B; FMP's fallback row for the same listing reads BRK-B.
    raw = {"ticker": "BRK.B", "session": {"close": 557.07}}
    assert GinlixDataSource._normalize_snapshot(raw)["symbol"] == "BRK-B"


def test_an_index_moves_by_its_own_level():
    """The feed's index change is rounded and trails the level in session; the
    move shown must be the level's own."""
    raw = {"ticker": "I:DJI", "session": {"close": 51786.72, "previous_close": 51983.98,
                                          "change": -197.0, "change_percent": -0.379}}
    row = GinlixDataSource._normalize_snapshot(raw, "indices")
    assert row["symbol"] == "DJI"
    assert round(row["change"], 2) == -197.26
    assert round(row["change_percent"], 4) == -0.3795


def _newest_opened_session(monkeypatch, day: str) -> None:
    """Pin the session the venue clock calls the newest that has opened."""
    clock = SimpleNamespace(tz=ZoneInfo("America/New_York"), expected_latest_daily_date=lambda now=None: day)
    monkeypatch.setattr(data_source, "clock_for", lambda *_args, **_kw: clock)


def test_an_index_level_that_outran_its_stamp_is_not_dated_by_it(monkeypatch):
    """The feed kept moving I:DJI's level a session past its stamp (2026-11-04
    16:04 ET), with its move stuck at 0: the stamp would date the 11-05 level
    to the day before."""
    raw = {"ticker": "I:DJI", "last_updated": 1_793_826_274_015_023_000,
           "session": {"close": 53594.36, "previous_close": 53356.12, "change": 0.0, "change_percent": 0.0}}
    _newest_opened_session(monkeypatch, "2026-11-05")
    row = GinlixDataSource._normalize_snapshot(raw, "indices")
    assert row["as_of"] is None
    assert round(row["change_percent"], 2) == 0.45

    unmoved = {**raw, "session": {**raw["session"], "close": 53356.12}}
    assert GinlixDataSource._normalize_snapshot(unmoved, "indices")["as_of"] == 1_793_826_274_015

    # Read before 11-05 opens, the zero may be the stamped session's own.
    _newest_opened_session(monkeypatch, "2026-11-04")
    assert GinlixDataSource._normalize_snapshot(raw, "indices")["as_of"] == 1_793_826_274_015


def test_an_unmoved_index_on_a_rolled_previous_close_has_no_move_yet(monkeypatch):
    """At 09:38 ET the delayed feed rolled I:SPX's previous close onto the
    11-04 close it still prints: the zero is no move of the new session."""
    raw = {"ticker": "I:SPX", "last_updated": 1_793_826_274_015_023_000,
           "session": {"close": 7688.62, "previous_close": 7688.62, "change": 0.0, "change_percent": 0.0}}
    _newest_opened_session(monkeypatch, "2026-11-05")
    row = GinlixDataSource._normalize_snapshot(raw, "indices")
    assert (row["change"], row["change_percent"]) == (None, None)
    assert row["as_of"] == 1_793_826_274_015

    _newest_opened_session(monkeypatch, "2026-11-04")
    row = GinlixDataSource._normalize_snapshot(raw, "indices")
    assert (row["change"], row["change_percent"]) == (0.0, 0.0)


def test_a_stock_keeps_the_vendor_move():
    row = GinlixDataSource._normalize_snapshot(_raw_snapshot())
    assert (row["change"], row["change_percent"]) == (-10.0, -5.0)
