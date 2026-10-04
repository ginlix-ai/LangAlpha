"""Tests for the shared quote formatter."""

import re
from datetime import datetime, timezone
from unittest.mock import patch

import pytz

from market_protocol import AssetClass, MarketPhase
from src.tools.market_data.quote_format import (
    block_clock,
    build_live_stamp,
    current_price,
    format_quote_block,
    format_quote_line,
    quote_freshness,
)

_MOD = "src.tools.market_data.quote_format"
_ET = pytz.timezone("US/Eastern")
# A Wednesday NYSE session. Every stamp test passes it as ``at``: a stamp
# measured against the wall clock would fail whenever New York is shut.
_FIXED_ET = _ET.localize(datetime(2026, 7, 1, 14, 32, 5))


def _snap(**overrides):
    # Every row the provider chain returns carries a declared tier; the
    # formatter treats a row without one as unknown, never as current.
    base = {
        "symbol": "NVDA",
        "price": 231.00,
        "change_percent": 2.31,
        "volume": 187_234_567,
        "last_trade_price": 233.45,
        "market_status": "open",
        "tier": "realtime",
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
        assert block.startswith("Retrieved ")
        assert "market open" in block
        assert block.count("\n") >= 2
        assert "TSLA" in block


class TestBuildLiveStamp:
    def test_stamp_during_regular_hours(self):
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap()], at=_FIXED_ET)
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

    def test_nan_change_percent_is_omitted_not_rendered(self):
        # A still-forming bar can carry NaN; it must drop the pct suffix, not
        # print "nan%" into agent-visible stamp text.
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap(change_percent=float("nan"))], at=_FIXED_ET)
        assert "NVDA $233.45" in stamp
        assert "nan" not in stamp.lower()


class TestCurrencyAndVenueAwareness:
    def test_hk_symbol_uses_hk_dollar_and_phase_suffix(self):
        # 0700.HK is priced in HKD; its venue phase AND market-local clock are
        # surfaced per line. The currency comes from the real protocol
        # resolution; only the calendar (phase) boundary is mocked so the
        # phase half of the suffix is deterministic.
        snap = {"symbol": "0700.HK", "last_trade_price": 318.20,
                "change_percent": -0.5, "volume": 12_000_000,
                "tier": "realtime"}
        with patch(f"{_MOD}.venue_phase", return_value=MarketPhase.CLOSED):
            line = format_quote_line(snap)
        assert "HK$318.20" in line
        assert re.search(r"\(closed, \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} HKT\)$", line)

    def test_regular_hours_suffix_is_venue_clock_only(self):
        snap = {"symbol": "0700.HK", "last_trade_price": 318.20,
                "tier": "realtime"}
        with patch(f"{_MOD}.venue_phase", return_value=MarketPhase.REGULAR):
            line = format_quote_line(snap)
        assert "HK$318.20" in line
        assert re.search(r"\(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} HKT\)$", line)
        assert "closed" not in line

    def test_us_symbol_line_has_no_venue_clock(self):
        # US listings ride the block header's ET clock — no per-line stamp.
        with patch(f"{_MOD}.venue_phase", return_value=MarketPhase.REGULAR):
            line = format_quote_line(_snap())
        assert "ET" not in line
        assert "(" not in line

    def test_unresolvable_symbol_falls_back_to_dollar(self):
        # A symbol the protocol can't resolve degrades to '$' with no suffix and
        # never raises (the formatter sits in tool-output + middleware paths).
        snap = {"symbol": "ZZZ", "last_trade_price": 12.34, "tier": "realtime"}
        with patch(f"{_MOD}.resolve_ref", return_value=None):
            line = format_quote_line(snap)
        assert line.startswith("ZZZ")
        assert "$12.34" in line

    def test_grouping_parity_with_canonical_fmt_price(self):
        # get_quote groups thousands ("$6,120.50"); the canonical
        # company-overview path does not ("$6120.50"). Both now derive from the
        # same fmt_price — the ONLY difference is the group flag, so stripping
        # commas from the quote line yields the canonical spelling exactly.
        from src.tools.market_data.currency import fmt_price

        snap = {"symbol": "SPY", "last_trade_price": 6120.5, "tier": "realtime"}
        with patch(f"{_MOD}.venue_phase", return_value=MarketPhase.REGULAR):
            line = format_quote_line(snap)
        assert "$6,120.50" in line
        assert fmt_price(6120.5, "USD") == "$6120.50"
        assert line.replace(",", "").split()[1] == fmt_price(6120.5, "USD")

    def test_block_header_drops_us_label_for_foreign_venue(self):
        # A block containing a non-US listing must not claim a US session label.
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)), \
             patch(f"{_MOD}.venue_phase", return_value=MarketPhase.REGULAR):
            block = format_quote_block(
                [{"symbol": "0700.HK", "last_trade_price": 318.20, "tier": "realtime"}]
            )
        header = block.split("\n")[0]
        assert "14:32:05 ET" in header
        assert "market open" not in header


class TestLiveStampIsVenueGated:
    """`build_live_stamp` used to gate on the US session and label every quote
    ET — a Shanghai listing was suppressed all through its own trading day and
    stamped with a New York clock the rest of the time."""

    _SHANGHAI_1030 = datetime(2026, 7, 1, 2, 30, tzinfo=timezone.utc)   # 10:30 CST
    _ET_1030 = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)        # 10:30 ET

    def _cn_snap(self):
        return {"symbol": "600519.SS", "last_trade_price": 1712.40,
                "change_percent": 1.25, "tier": "realtime"}

    def test_cn_symbol_suppressed_while_its_own_venue_is_shut(self):
        # 10:30 ET is 22:30 in Shanghai — SSE is closed, so there is no live
        # quote to stamp even though the US session is wide open.
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            assert build_live_stamp([self._cn_snap()], at=self._ET_1030) is None

    def test_cn_symbol_stamped_with_its_own_venue_clock(self):
        # 10:30 in Shanghai on a trading Wednesday: the stamp exists and is
        # labelled with the venue clock, never "ET". The vendor row reads
        # ``.SS``; the stamp shows the display spelling.
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            stamp = build_live_stamp([self._cn_snap()], at=self._SHANGHAI_1030)
        assert stamp is not None
        assert "600519.SH" in stamp and "(+1.25%)" in stamp
        assert "2026-07-01 10:30:00 CST" in stamp
        assert "ET" not in stamp

    def test_us_only_stamp_is_unchanged(self):
        # The US path keeps its exact legacy shape (ET clock + session label).
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap()], at=_FIXED_ET)
        assert stamp == "[Live: NVDA $233.45 (+2.31%) — as of 14:32:05 ET, market open]"

    def test_mixed_block_drops_the_us_session_line(self):
        # A stamp carrying a foreign venue must not assert a US session label.
        # Both venues trade at 10:30 ET (15:30 in London); Shanghai never
        # overlaps New York, and the calendar drops a venue it has closed.
        london = {"symbol": "VOD.L", "last_trade_price": 72.10,
                  "change_percent": 0.4, "tier": "realtime"}
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap(), london], at=self._ET_1030)
        assert "NVDA $233.45" in stamp and "VOD.L" in stamp
        assert "market open" not in stamp

    def test_us_row_the_calendar_has_closed_is_not_stamped(self):
        # 2026-07-03 is a NYSE holiday the US session gate cannot see: its
        # last print is no quote of an open venue.
        holiday = datetime(2026, 7, 3, 14, 30, tzinfo=timezone.utc)
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            assert build_live_stamp([_snap()], at=holiday) is None


class TestFreshnessSplitsTheStamp:
    """A quote is stamped live only when it is measured live, or its provider
    declares a realtime tier; a 15-minute feed reads as delayed instead."""

    # 2026-07-02 is a Thursday session in Hong Kong; 10:30 HKT is 02:30 UTC.
    _HK_1030 = datetime(2026, 7, 2, 2, 30, tzinfo=timezone.utc)
    # 2026-09-09 is a Wednesday session in Shanghai; 10:30 CST is 02:30 UTC.
    _CN_1030 = datetime(2026, 9, 9, 2, 30, tzinfo=timezone.utc)

    def _hk_delayed(self):
        # FMP outside the US tape: a quote with no print time, declared 15m.
        return {"symbol": "0700.HK", "last_trade_price": 318.20,
                "change_percent": -0.5, "tier": "delayed_15m", "source": "fmp"}

    def _cn_realtime(self, at):
        return {"symbol": "600519.SH", "last_trade_price": 1712.40,
                "change_percent": 1.25, "tier": "realtime", "source": "tushare",
                "as_of": int(at.timestamp() * 1000) - 10_000}

    def test_delayed_hk_quote_is_not_stamped_live(self):
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            stamp = build_live_stamp([self._hk_delayed()], at=self._HK_1030)
        assert stamp.startswith("[Delayed 15m: 0700.HK HK$318.20 (-0.50%)")
        assert "[Live" not in stamp

    def test_delayed_hk_quote_says_so_on_its_line(self):
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            line = format_quote_line(self._hk_delayed(), at=self._HK_1030)
        assert "delayed 15m" in line

    def test_measured_realtime_cn_quote_is_stamped_live(self):
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            stamp = build_live_stamp(
                [self._cn_realtime(self._CN_1030)], at=self._CN_1030
            )
        assert stamp.startswith("[Live: 600519.SH ")
        assert "delayed" not in stamp.lower()

    def test_live_quote_carries_its_print_time_not_the_retrieval_clock(self):
        # as_of is 10 seconds before the retrieval moment: the stamp clock is
        # the print, so it reads 10:29:50 rather than 10:30:00.
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            stamp = build_live_stamp(
                [self._cn_realtime(self._CN_1030)], at=self._CN_1030
            )
        assert "2026-09-09 10:29:50 CST" in stamp

    def test_mixed_batch_splits_into_two_groups(self):
        # Both venues are open at 10:30 CST/HKT on their own calendars, so the
        # split is by freshness, not by session.
        with patch(f"{_MOD}.get_market_session", return_value=("CLOSED", _FIXED_ET)):
            stamp = build_live_stamp(
                [self._cn_realtime(self._CN_1030),
                 {**self._hk_delayed(), "symbol": "0700.HK"}],
                at=self._CN_1030,
            )
        live, delayed = stamp.split("\n")
        assert live.startswith("[Live: 600519.SH ")
        assert delayed.startswith("[Delayed 15m: 0700.HK ")

    def test_untiered_quote_is_never_called_live(self):
        # Nothing declared and nothing measured: unknown, not a guessed delay.
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap(tier=None)], at=_FIXED_ET)
        assert stamp == "[Freshness unknown: NVDA $233.45 (+2.31%)]"

    def test_hours_old_print_is_stale_not_delayed(self):
        # A "realtime" feed whose last print is three hours behind an open
        # session measures stale; the stamp must not call it 15 minutes late.
        three_hours_ago = int(_FIXED_ET.timestamp() * 1000) - 3 * 3600 * 1000
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp([_snap(as_of=three_hours_ago)], at=_FIXED_ET)
        assert stamp == "[Stale: NVDA $233.45 (+2.31%)]"

    def test_each_freshness_gets_its_own_group(self):
        ten_minutes_ago = int(_FIXED_ET.timestamp() * 1000) - 10 * 60 * 1000
        with patch(f"{_MOD}.get_market_session", return_value=("REGULAR_HOURS", _FIXED_ET)):
            stamp = build_live_stamp(
                [_snap(symbol="AAPL", tier=None), _snap(as_of=ten_minutes_ago)],
                at=_FIXED_ET,
            )
        assert stamp.split("\n") == [
            "[Delayed 15m: NVDA $233.45 (+2.31%)]",
            "[Freshness unknown: AAPL $233.45 (+2.31%)]",
        ]


def test_block_clock_uses_the_clock_its_rows_share():
    at = datetime(2026, 9, 27, 1, 0, tzinfo=timezone.utc)
    assert block_clock([{"symbol": "AAPL"}, {"symbol": "MSFT"}], at) == "2026-09-26 21:00:00 ET"
    assert block_clock([{"symbol": "600519.SS"}, {"symbol": "000858.SZ"}], at) == "2026-09-27 09:00:00 CST"
    # Mixed venues: no clock is neutral, so the card falls back to the reader's.
    assert block_clock([{"symbol": "600519.SS"}, {"symbol": "AAPL"}], at) is None


class TestKnownAssetClass:
    """A bare COMP is the Nasdaq Composite to autodetection and Compass Inc.
    to a stock endpoint. Whoever knows which one the row is decides its clock:
    the index has no extended hours, so the stock's after-hours print would
    read closed."""

    _AFTER_HOURS = _ET.localize(datetime(2026, 7, 1, 17, 30))

    def _comp(self, **over):
        printed = int(self._AFTER_HOURS.timestamp() * 1000) - 10_000
        return _snap(symbol="COMP", last_trade_price=9.12, as_of=printed, **over)

    def test_stock_row_is_measured_on_the_stocks_clock(self):
        fresh = quote_freshness(
            self._comp(), at=self._AFTER_HOURS, asset_class=AssetClass.EQUITY
        )
        assert fresh.label == "live"
        assert fresh.closed is False

    def test_row_that_names_its_class_needs_no_hint(self):
        fresh = quote_freshness(self._comp(asset_class="equity"), at=self._AFTER_HOURS)
        assert fresh.closed is False

    def test_unknown_class_still_autodetects_the_index(self):
        assert quote_freshness(self._comp(), at=self._AFTER_HOURS).closed is True

    def test_stock_line_reads_post_not_closed(self):
        line = format_quote_line(
            self._comp(), at=self._AFTER_HOURS, asset_class=AssetClass.EQUITY
        )
        assert f"({MarketPhase.POST.value})" in line
        assert "closed" not in line

    def test_stock_after_hours_print_is_stamped_live(self):
        stamp = build_live_stamp(
            [self._comp()], at=self._AFTER_HOURS, asset_class=AssetClass.EQUITY
        )
        assert stamp.startswith("[Live: COMP $9.12 ")
        assert "after-hours" in stamp
