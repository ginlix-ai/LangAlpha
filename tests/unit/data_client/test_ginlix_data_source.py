"""Snapshot normalization for the ginlix-data source.

The frontend header derives its settled-close presentation from this row, so
the mapping of the provider's session block — in particular the provider-exact
``regular_close`` next to the reduced-precision (1dp) change fields — is a
wire contract, not an implementation detail.
"""

from __future__ import annotations

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
