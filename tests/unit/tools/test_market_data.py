"""Tests for src/tools/market_data/ utility functions and formatters.

Tests the pure helper functions in utils.py and the normalize/formatting
logic in implementations.py without hitting any external APIs.
"""

from src.tools.market_data.utils import (
    format_number,
    format_percentage,
    get_rating_label,
)
from src.tools.market_data._shared import _normalize_market_bars, _safe_result
from src.tools.market_data.company import _margin


class TestFormatNumber:
    """Tests for format_number."""

    def test_none_returns_na(self):
        assert format_number(None) == "N/A"

    def test_trillions(self):
        result = format_number(3.68e12)
        assert result == "$3.68T"

    def test_billions(self):
        result = format_number(2.5e9)
        assert result == "$2.50B"

    def test_millions(self):
        result = format_number(150e6)
        assert result == "$150.00M"

    def test_small_number_with_suffix(self):
        result = format_number(247.92)
        assert result == "$247.92"

    def test_no_suffix(self):
        result = format_number(1e9, suffix=False)
        assert result == "1,000,000,000.00"
        assert "$" not in result

    def test_negative_trillions(self):
        result = format_number(-1.5e12)
        assert result == "$-1.50T"


class TestFormatPercentage:
    """Tests for format_percentage."""

    def test_none_returns_na(self):
        assert format_percentage(None) == "N/A"

    def test_positive_value(self):
        result = format_percentage(5.23)
        assert result == "+5.23%"

    def test_negative_value(self):
        result = format_percentage(-2.15)
        assert result == "-2.15%"

    def test_zero(self):
        result = format_percentage(0)
        assert result == "+0.00%"

    def test_non_numeric_passthrough(self):
        result = format_percentage("N/A")
        assert result == "N/A"


class TestGetRatingLabel:
    """Tests for get_rating_label."""

    def test_highest_score(self):
        assert get_rating_label(5) == "A+"
        assert get_rating_label(4.5) == "A+"

    def test_mid_scores(self):
        assert get_rating_label(4.0) == "A"
        assert get_rating_label(3.5) == "A-"
        assert get_rating_label(3.0) == "B+"
        assert get_rating_label(2.5) == "B"
        assert get_rating_label(2.0) == "B-"

    def test_low_scores(self):
        assert get_rating_label(1.5) == "C"
        assert get_rating_label(1.0) == "D"
        assert get_rating_label(0) == "D"


class TestSafeResult:
    """Tests for _safe_result helper."""

    def test_normal_value(self):
        assert _safe_result(42) == 42

    def test_exception_returns_default(self):
        assert _safe_result(ValueError("err")) is None
        assert _safe_result(ValueError("err"), default=[]) == []

    def test_none_returns_default(self):
        assert _safe_result(None, default="fallback") == "fallback"

    def test_none_without_default(self):
        assert _safe_result(None) is None


class TestNormalizeMarketBars:
    """Tests for _normalize_market_bars."""

    def test_empty_bars(self):
        assert _normalize_market_bars([], "AAPL") == []

    def test_single_bar_no_change(self):
        bars = [{"time": 1704067200000, "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000}]
        result = _normalize_market_bars(bars, "AAPL")
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"
        assert result[0]["close"] == 103
        # First bar has no previous close, so change should be None
        assert result[0]["change"] is None
        assert result[0]["changePercent"] is None

    def test_multiple_bars_compute_change(self):
        bars = [
            {"time": 1704067200000, "open": 100, "high": 105, "low": 99, "close": 100, "volume": 1000},
            {"time": 1704153600000, "open": 101, "high": 106, "low": 100, "close": 110, "volume": 1500},
        ]
        result = _normalize_market_bars(bars, "AAPL")
        # Newest first
        assert result[0]["close"] == 110
        assert result[0]["change"] == 10.0
        assert result[0]["changePercent"] == 10.0
        assert result[1]["change"] is None  # First bar chronologically

    def test_returns_newest_first(self):
        bars = [
            {"time": 1704153600000, "open": 101, "high": 106, "low": 100, "close": 110, "volume": 1500},
            {"time": 1704067200000, "open": 100, "high": 105, "low": 99, "close": 100, "volume": 1000},
        ]
        result = _normalize_market_bars(bars, "AAPL")
        # Should be sorted newest-first regardless of input order
        assert result[0]["close"] == 110
        assert result[1]["close"] == 100


class TestMargin:
    """Tests for _margin — income-statement margin fractions.

    FMP's stable API dropped the v3 ``*Ratio`` fields, so the helper must
    derive margins from the raw dollar fields when the ratio is absent.
    """

    def test_prefers_provider_ratio_when_present(self):
        # v3-shaped row: the declared ratio wins even if it disagrees with
        # the raw fields (provider may apply adjustments we don't know).
        stmt = {"revenue": 200.0, "grossProfit": 100.0, "grossProfitRatio": 0.51}
        assert _margin(stmt, "grossProfitRatio", "grossProfit") == 0.51

    def test_computes_from_raw_fields_when_ratio_absent(self):
        # stable-shaped row: no *Ratio fields at all.
        stmt = {"revenue": 200.0, "grossProfit": 150.0, "operatingIncome": 80.0, "netIncome": 50.0}
        assert _margin(stmt, "grossProfitRatio", "grossProfit") == 0.75
        assert _margin(stmt, "operatingIncomeRatio", "operatingIncome") == 0.40
        assert _margin(stmt, "netIncomeRatio", "netIncome") == 0.25

    def test_net_loss_yields_negative_margin(self):
        stmt = {"revenue": 200.0, "netIncome": -50.0}
        assert _margin(stmt, "netIncomeRatio", "netIncome") == -0.25

    def test_zero_or_missing_revenue_returns_none(self):
        assert _margin({"revenue": 0, "netIncome": 50.0}, "netIncomeRatio", "netIncome") is None
        assert _margin({"netIncome": 50.0}, "netIncomeRatio", "netIncome") is None

    def test_missing_numerator_returns_none(self):
        assert _margin({"revenue": 200.0}, "grossProfitRatio", "grossProfit") is None

    def test_empty_row_returns_none(self):
        assert _margin({}, "grossProfitRatio", "grossProfit") is None
