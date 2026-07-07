"""Tests for currency-aware market-data display formatting.

Covers the pure helpers in currency.py plus output-golden checks that exercise
the real formatting paths in implementations.py. The US goldens assert
byte-identical output to the legacy hardcoded ``$`` formatting (compat
guarantee); the XLON/XHKG goldens assert the localized prefixes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.market_data.currency import (
    DisplaySpec,
    currency_symbol,
    fmt_money,
    fmt_price,
)
from src.tools.market_data.implementations import (
    _format_price_data_as_table,
    _format_price_summary,
    _symbol_currency,
    fetch_stock_screener,
)
from src.tools.market_data.utils import format_number
from src.market_protocol import to_canonical

_MOD = "src.tools.market_data.implementations"


# ---------------------------------------------------------------------------
# currency_symbol
# ---------------------------------------------------------------------------

class TestCurrencySymbol:
    def test_known_codes(self):
        assert currency_symbol("USD") == "$"
        assert currency_symbol("GBP") == "£"
        assert currency_symbol("HKD") == "HK$"
        assert currency_symbol("EUR") == "€"
        assert currency_symbol("JPY") == "¥"
        assert currency_symbol("CNY") == "CN¥"

    def test_case_insensitive(self):
        assert currency_symbol("gbp") == "£"
        assert currency_symbol("hkd") == "HK$"

    def test_none_defaults_to_usd(self):
        assert currency_symbol(None) == "$"
        assert currency_symbol("") == "$"

    def test_unknown_code_gets_iso_prefix(self):
        assert currency_symbol("CHF") == "CHF "
        assert currency_symbol("sek") == "SEK "


# ---------------------------------------------------------------------------
# fmt_price
# ---------------------------------------------------------------------------

class TestFmtPrice:
    def test_examples_from_spec(self):
        assert fmt_price(0.99, "GBP") == "£0.99"
        assert fmt_price(318.20, "HKD") == "HK$318.20"
        assert fmt_price(12.34, "CHF") == "CHF 12.34"

    def test_usd_and_default(self):
        assert fmt_price(100.0, "USD") == "$100.00"
        assert fmt_price(100.0, None) == "$100.00"

    def test_usd_byte_identical_to_legacy(self):
        for v in (0.0, 1.5, 247.92, -3.25, 1234.5):
            assert fmt_price(v, "USD") == f"${v:.2f}"

    def test_none_value(self):
        assert fmt_price(None, "USD") == "N/A"
        assert fmt_price(None, "GBP") == "N/A"

    def test_zero_decimals_ride_on_the_spec(self):
        # fmt_price no longer special-cases any currency: a bare code defaults to
        # 2 decimals; zero-decimal precision now comes from the DisplaySpec
        # (protocol authority via display_decimals_for), not a table inside here.
        assert fmt_price(1234.5, "JPY") == "¥1234.50"
        assert fmt_price(1234.5, DisplaySpec("JPY", 0)) == "¥1234"
        # explicit decimals override the spec
        assert fmt_price(1234.5, DisplaySpec("JPY", 0), decimals=2) == "¥1234.50"

    def test_decimals_override(self):
        assert fmt_price(1.23456, "USD", decimals=4) == "$1.2346"

    def test_eur_and_cny(self):
        assert fmt_price(12.3, "EUR") == "€12.30"
        assert fmt_price(12.3, "CNY") == "CN¥12.30"


# ---------------------------------------------------------------------------
# fmt_money
# ---------------------------------------------------------------------------

class TestFmtMoney:
    def test_suffixes(self):
        assert fmt_money(3.68e12, "USD") == "$3.68T"
        assert fmt_money(2.5e9, "HKD") == "HK$2.50B"
        assert fmt_money(150e6, "GBP") == "£150.00M"
        assert fmt_money(247.92, "USD") == "$247.92"

    def test_none_value(self):
        assert fmt_money(None, "USD") == "N/A"

    def test_no_suffix_drops_prefix(self):
        # Mirrors format_number(suffix=False): plain number, no currency.
        assert fmt_money(1e9, "HKD", suffix=False) == "1,000,000,000.00"

    def test_usd_byte_identical_to_format_number(self):
        for v in (0.0, 247.92, 150e6, 2.5e9, 3.68e12, -1.5e12):
            assert fmt_money(v, "USD") == format_number(v)


# ---------------------------------------------------------------------------
# Golden output — _format_price_summary (single symbol, self-resolves)
# ---------------------------------------------------------------------------

def _summary_stats(symbol: str) -> dict:
    """Controlled stats dict that yields exactly the four OHLC rows."""
    return {
        "symbol": symbol,
        "period_days": 10,
        "start_date": "2025-01-01",
        "end_date": "2025-01-10",
        "period_open": 150.0,
        "period_close": 160.0,
        "period_high": 165.0,
        "period_low": 148.0,
    }


def _expected_summary(stats: dict, prefix: str) -> str:
    """Reconstruct the summary using the given currency prefix directly."""
    rows = [
        ("Period Open", f"{prefix}{stats['period_open']:.2f}"),
        ("Period Close", f"{prefix}{stats['period_close']:.2f}"),
        ("Period High", f"{prefix}{stats['period_high']:.2f}"),
        ("Period Low", f"{prefix}{stats['period_low']:.2f}"),
    ]
    lines = [
        f"**Period:** {stats['start_date']} to {stats['end_date']} "
        f"({stats['period_days']} trading days)",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]
    lines += [f"| {m} | {v} |" for m, v in rows]
    lines.append("")
    return "\n".join(lines)


class TestGoldenPriceSummary:
    def test_us_byte_identical(self):
        stats = _summary_stats("AAPL")
        # Expected side uses the legacy "$" formatting formula.
        assert _format_price_summary(stats) == _expected_summary(stats, "$")

    def test_xlon_pounds(self):
        stats = _summary_stats("VOD.L")
        result = _format_price_summary(stats)
        assert result == _expected_summary(stats, "£")
        assert "$" not in result

    def test_xhkg_hk_dollar(self):
        stats = _summary_stats("0700.HK")
        result = _format_price_summary(stats)
        assert result == _expected_summary(stats, "HK$")
        assert "| Period Open | HK$150.00 |" in result


# ---------------------------------------------------------------------------
# KRW — zero-decimal precision sourced from the protocol table, not fmt_price
# ---------------------------------------------------------------------------

class TestKrwZeroDecimals:
    """A ``.KS`` listing resolves to KRW / 0 decimals via ``display_decimals_for``.
    ``.KS`` is a neutral placeholder suffix here — no real ticker involved."""

    def test_symbol_currency_spec(self):
        # _symbol_currency now takes a resolved ref (callers resolve once); pass the
        # canonicalized instrument rather than re-parsing the string inside.
        assert _symbol_currency(to_canonical("ABCD.KS")) == DisplaySpec("KRW", 0)
        assert _symbol_currency(to_canonical("AAPL")) == DisplaySpec("USD", 2)

    def test_price_summary_renders_zero_decimals(self):
        result = _format_price_summary(_summary_stats("ABCD.KS"))
        assert "| Period Open | KRW 150 |" in result
        assert "| Period Close | KRW 160 |" in result
        assert "| Period High | KRW 165 |" in result
        assert "| Period Low | KRW 148 |" in result
        assert "$" not in result


# ---------------------------------------------------------------------------
# Golden output — _format_price_data_as_table (short-period table path)
# ---------------------------------------------------------------------------

def _table_record(symbol: str) -> list[dict]:
    return [
        {
            "date": "2025-01-02",
            "symbol": symbol,
            "open": 150.0,
            "high": 152.0,
            "low": 149.0,
            "close": 151.0,
            "volume": 1_000_000,
            "changePercent": 0.67,
        }
    ]


class TestGoldenPriceTable:
    def test_us_cells_unchanged(self):
        result = _format_price_data_as_table(_table_record("AAPL"))
        # Each price cell matches the legacy "$" expression exactly.
        for v in (150.0, 152.0, 149.0, 151.0):
            assert f"${v:.2f}" in result
        assert "£" not in result and "HK$" not in result and "€" not in result

    def test_xlon_cells_pounds(self):
        result = _format_price_data_as_table(_table_record("VOD.L"))
        for v in (150.0, 152.0, 149.0, 151.0):
            assert f"£{v:.2f}" in result
        assert "$" not in result  # £ prefix has no "$"

    def test_xhkg_cells_hk_dollar(self):
        result = _format_price_data_as_table(_table_record("0700.HK"))
        for v in (150.0, 152.0, 149.0, 151.0):
            assert f"HK${v:.2f}" in result


# ---------------------------------------------------------------------------
# Per-row currency threading — fetch_stock_screener
# ---------------------------------------------------------------------------

def _screener_provider(results):
    financial = AsyncMock()
    financial.screen_stocks = AsyncMock(return_value=results)
    provider = MagicMock()
    provider.financial = financial
    return provider


class TestScreenerPerRowCurrency:
    @pytest.mark.asyncio
    async def test_mixed_market_rows_resolve_per_symbol(self):
        results = [
            {
                "symbol": "AAPL",
                "companyName": "Apple Inc.",
                "price": 235.50,
                "marketCap": 3_500_000_000_000,
                "sector": "Technology",
                "beta": 1.24,
                "volume": 55_000_000,
                "change": 2.30,
            },
            {
                "symbol": "0700.HK",
                "companyName": "Tencent Holdings",
                "price": 318.20,
                "marketCap": 3_000_000_000_000,
                "sector": "Technology",
                "beta": 0.90,
                "volume": 20_000_000,
                "change": -1.50,
            },
        ]
        provider = _screener_provider(results)

        with patch(f"{_MOD}.get_financial_data_provider", return_value=provider):
            content, _ = await fetch_stock_screener(sector="Technology")

        # US row keeps "$"; HK row localizes to "HK$" for both price and cap.
        assert "$235.50" in content
        assert "HK$318.20" in content
        assert "HK$3.00T" in content
