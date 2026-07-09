"""Tests for the shared quote formatter."""

from datetime import datetime
from unittest.mock import patch

import pytz

from src.market_protocol import MarketPhase
from src.tools.market_data.quote_format import (
    build_live_stamp,
    current_price,
    format_quote_block,
    format_quote_line,
)

_MOD = "src.tools.market_data.quote_format"
_ET = pytz.timezone("US/Eastern")
_FIXED_ET = _ET.localize(datetime(2026, 7, 1, 14, 32, 5))


def _snap(**overrides):
    base = {
        "symbol": "NVDA",
        "price": 231.00,
        "change_percent": 2.31,
        "volume": 187_234_567,
        "last_trade_price": 233.45,
        "market_status": "open",
    }
    base.update(overrides)
    return base


class TestCurrentPrice:
    def test_prefers_last_trade_price(self):
        assert current_price(_snap()) == 233.45

    def test_falls_back_to_session_close(self):
        assert current_price(_snap(last_trade_price=None)) == 231.00

    def test_none_when_no_price(self):
        assert current_price(_snap(last_trade_price=None, price=None)) is None

    def test_nan_last_trade_falls_back_to_price(self):
        # A NaN last trade (forming bar / stale feed) must not surface as
        # "$nan" — fall back to the session close instead.
        assert current_price(_snap(last_trade_price=float("nan"), price=100.0)) == 100.0

    def test_all_nan_snapshot_returns_none(self):
        snap = _snap(last_trade_price=float("nan"), price=float("nan"))
        assert current_price(snap) is None


class TestFormatQuoteLine:
    def test_basic_line(self):
        line = format_quote_line(_snap())
        assert "NVDA" in line
        assert "$233.45" in line
        assert "+2.31%" in line
        assert "187.2M" in line

    def test_change_since_prev(self):
        line = format_quote_line(_snap(), prev_price=232.50)
        assert "+0.41% since last check" in line

    def test_missing_fields_graceful(self):
        line = format_quote_line({"symbol": "XYZ"})
        assert line.startswith("XYZ")
        assert "$" not in line


class TestFormatQuoteBlock:
    def test_header_and_lines(self):
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            block = format_quote_block([_snap(), _snap(symbol="TSLA", last_trade_price=412.10)])
        assert "14:32:05 ET" in block
        assert "market open" in block
        assert block.count("\n") >= 2
        assert "TSLA" in block


class TestBuildLiveStamp:
    def test_stamp_during_regular_hours(self):
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap()])
        assert stamp.startswith("[Live: ")
        assert "NVDA $233.45 (+2.31%)" in stamp
        assert "as of 14:32:05 ET" in stamp
        assert stamp.endswith("]")

    def test_none_when_closed(self):
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            assert build_live_stamp([_snap()]) is None

    def test_none_when_no_snaps(self):
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            assert build_live_stamp([]) is None
            assert build_live_stamp([_snap(price=None, last_trade_price=None)]) is None


class TestCurrencyAndVenueAwareness:
    def test_hk_symbol_uses_hk_dollar_and_phase_suffix(self):
        # 0700.HK is priced in HKD and its venue phase is surfaced per line. The
        # currency comes from the real protocol resolution; only the calendar
        # (phase) boundary is mocked so the suffix is deterministic.
        snap = {"symbol": "0700.HK", "last_trade_price": 318.20,
                "change_percent": -0.5, "volume": 12_000_000}
        with patch(f"{_MOD}.get_calendar") as gc:
            gc.return_value.phase_at.return_value = MarketPhase.CLOSED
            line = format_quote_line(snap)
        assert "HK$318.20" in line
        assert "(closed)" in line

    def test_regular_hours_have_no_phase_suffix(self):
        snap = {"symbol": "0700.HK", "last_trade_price": 318.20}
        with patch(f"{_MOD}.get_calendar") as gc:
            gc.return_value.phase_at.return_value = MarketPhase.REGULAR
            line = format_quote_line(snap)
        assert "HK$318.20" in line
        assert "(" not in line  # no phase suffix during regular hours

    def test_unresolvable_symbol_falls_back_to_dollar(self):
        # A symbol the protocol can't resolve degrades to '$' with no suffix and
        # never raises (the formatter sits in tool-output + middleware paths).
        snap = {"symbol": "ZZZ", "last_trade_price": 12.34}
        with patch(f"{_MOD}.resolve_ref", return_value=None):
            line = format_quote_line(snap)
        assert line.startswith("ZZZ")
        assert "$12.34" in line

    def test_block_header_drops_us_label_for_foreign_venue(self):
        # A block containing a non-US listing must not claim a US session label.
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)), \
             patch(f"{_MOD}.get_calendar") as gc:
            gc.return_value.phase_at.return_value = MarketPhase.REGULAR
            block = format_quote_block([{"symbol": "0700.HK", "last_trade_price": 318.20}])
        header = block.split("\n")[0]
        assert "14:32:05 ET" in header
        assert "market open" not in header
