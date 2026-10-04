"""Unit tests for yfinance financial source — synthetic dataframes, no network."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


def _make_income_stmt_df(rows: list[tuple[str, list]], dates: list[str]) -> pd.DataFrame:
    """Build a yfinance-shaped income statement DataFrame.

    yfinance returns metrics as rows, dates as columns (most recent first).
    """
    cols = [pd.Timestamp(d) for d in dates]
    return pd.DataFrame({c: [vals[i] for _, vals in rows] for i, c in enumerate(cols)},
                        index=[name for name, _ in rows])


class TestGetIncomeStatementsPlaceholderRows:
    """Regression: yfinance returns a placeholder row for newly-reported quarters
    with only EPS populated (full income line items land later). These must be
    skipped so callers see only complete rows — matching FMP behavior.
    """

    def _run(self, df, *, period="quarter", limit=4):
        from src.data_client.yfinance.financial_source import _get_income_statements

        with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
            ticker = MagicMock()
            ticker.quarterly_income_stmt = df
            ticker.income_stmt = df
            ticker_cls.return_value = ticker
            return _get_income_statements("TEST", period, limit)

    def test_skips_placeholder_row_with_nan_revenue(self):
        df = _make_income_stmt_df(
            rows=[
                ("Total Revenue", [np.nan, 100.0, 90.0]),
                ("Cost Of Revenue", [np.nan, 60.0, 55.0]),
                ("Gross Profit", [np.nan, 40.0, 35.0]),
                ("Net Income", [np.nan, 20.0, 18.0]),
                ("Basic EPS", [2.02, 1.50, 1.40]),
            ],
            dates=["2026-03-31", "2025-12-31", "2025-09-30"],
        )
        result = self._run(df)

        assert len(result) == 2
        assert result[0]["date"] == "2025-12-31"
        assert result[0]["revenue"] == 100.0
        assert result[0]["grossProfitRatio"] == 0.4
        assert result[0]["netIncomeRatio"] == 0.2

    def test_keeps_all_rows_when_revenue_present(self):
        df = _make_income_stmt_df(
            rows=[
                ("Total Revenue", [200.0, 100.0]),
                ("Gross Profit", [80.0, 40.0]),
            ],
            dates=["2026-03-31", "2025-12-31"],
        )
        result = self._run(df)

        assert len(result) == 2
        assert [r["date"] for r in result] == ["2026-03-31", "2025-12-31"]
        assert result[0]["grossProfitRatio"] == 0.4

    def test_limit_applied_after_filtering(self):
        df = _make_income_stmt_df(
            rows=[
                ("Total Revenue", [np.nan, 100.0, 90.0, 80.0, 70.0]),
                ("Gross Profit", [np.nan, 40.0, 35.0, 32.0, 28.0]),
            ],
            dates=["2026-03-31", "2025-12-31", "2025-09-30", "2025-06-30", "2025-03-31"],
        )
        result = self._run(df, limit=3)

        assert len(result) == 3
        assert [r["date"] for r in result] == ["2025-12-31", "2025-09-30", "2025-06-30"]

    def test_empty_dataframe_returns_empty(self):
        result = self._run(pd.DataFrame())
        assert result == []

    def test_zero_revenue_row_skipped(self):
        df = _make_income_stmt_df(
            rows=[("Total Revenue", [0.0, 100.0]), ("Gross Profit", [0.0, 40.0])],
            dates=["2026-03-31", "2025-12-31"],
        )
        result = self._run(df)
        assert len(result) == 1
        assert result[0]["date"] == "2025-12-31"


@pytest.mark.parametrize(
    "missing_metric",
    ["Gross Profit", "Operating Income", "Net Income"],
)
def test_ratio_omitted_when_underlying_metric_missing(missing_metric):
    """Margin ratios are only set when the underlying numerator is numeric.
    Missing metrics shouldn't crash or produce bogus ratios.
    """
    from src.data_client.yfinance.financial_source import _get_income_statements

    base_rows = {
        "Total Revenue": 100.0,
        "Gross Profit": 40.0,
        "Operating Income": 30.0,
        "Net Income": 20.0,
    }
    base_rows[missing_metric] = np.nan
    df = _make_income_stmt_df(
        rows=[(name, [val]) for name, val in base_rows.items()],
        dates=["2025-12-31"],
    )
    with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
        ticker = MagicMock()
        ticker.quarterly_income_stmt = df
        ticker_cls.return_value = ticker
        result = _get_income_statements("TEST", "quarter", 4)

    ratio_key = {
        "Gross Profit": "grossProfitRatio",
        "Operating Income": "operatingIncomeRatio",
        "Net Income": "netIncomeRatio",
    }[missing_metric]
    assert ratio_key not in result[0]


class TestKeyMetricsAndRatiosShape:
    """yfinance stands in for FMP's stable key-metrics-ttm / ratios-ttm, so its
    rows carry those names and those units: Yahoo reports debtToEquity and
    dividendYield as percents (154.0, 0.41) where FMP reports 1.54 and 0.0041.
    """

    _INFO = {
        "trailingPE": 38.44,
        "trailingPegRatio": 2.71,
        "priceToBook": 57.97,
        "priceToSalesTrailing12Months": 9.3,
        "enterpriseToEbitda": 27.3,
        "enterpriseValue": 3_870_000_000_000,
        "returnOnEquity": 1.55,
        "returnOnAssets": 0.30,
        "grossMargins": 0.467,
        "profitMargins": 0.243,
        "operatingMargins": 0.319,
        "debtToEquity": 154.0,
        "currentRatio": 0.87,
        "quickRatio": 0.83,
        "dividendYield": 0.41,
        "payoutRatio": 0.155,
    }

    def _run(self, fn_name, info):
        from src.data_client.yfinance import financial_source

        with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
            ticker = MagicMock()
            ticker.info = info
            ticker.fast_info = {"marketCap": 3_500_000_000_000}
            ticker_cls.return_value = ticker
            return getattr(financial_source, fn_name)("TEST")[0]

    def test_ratios_use_stable_names_and_fraction_units(self):
        r = self._run("_get_financial_ratios", self._INFO)
        assert r["priceToEarningsRatioTTM"] == 38.44
        assert r["priceToEarningsGrowthRatioTTM"] == 2.71
        assert r["priceToBookRatioTTM"] == 57.97
        assert r["netProfitMarginTTM"] == 0.243
        assert r["debtToEquityRatioTTM"] == pytest.approx(1.54)
        assert r["dividendYieldTTM"] == pytest.approx(0.0041)
        assert r["dividendPayoutRatioTTM"] == 0.155

    def test_key_metrics_use_stable_names_and_fraction_units(self):
        m = self._run("_get_key_metrics", self._INFO)
        assert m["marketCap"] == 3_500_000_000_000
        assert m["returnOnEquityTTM"] == 1.55
        assert m["returnOnAssetsTTM"] == 0.30
        assert m["evToEBITDATTM"] == 27.3
        assert m["earningsYieldTTM"] == pytest.approx(1 / 38.44)

    def test_missing_percent_fields_stay_none(self):
        info = {k: v for k, v in self._INFO.items()
                if k not in ("debtToEquity", "dividendYield")}
        r = self._run("_get_financial_ratios", info)
        assert r["debtToEquityRatioTTM"] is None
        assert r["dividendYieldTTM"] is None

    def test_missing_peg_stays_none(self):
        info = {k: v for k, v in self._INFO.items() if k != "trailingPegRatio"}
        r = self._run("_get_financial_ratios", info)
        assert r["priceToEarningsGrowthRatioTTM"] is None


class TestReportedCurrency:
    """Statement rows carry the issuer's reporting currency, not the listing's."""

    def _df(self):
        return _make_income_stmt_df(
            rows=[("Total Revenue", [100.0]), ("Net Income", [10.0])],
            dates=["2025-12-31"],
        )

    def _run(self, fn_name, info):
        from src.data_client.yfinance import financial_source

        with patch("src.data_client.yfinance.financial_source.yf.Ticker") as ticker_cls:
            ticker = MagicMock()
            ticker.quarterly_income_stmt = ticker.income_stmt = self._df()
            ticker.quarterly_cashflow = ticker.cashflow = _make_income_stmt_df(
                rows=[("Free Cash Flow", [5.0])], dates=["2025-12-31"]
            )
            ticker.info = info
            ticker_cls.return_value = ticker
            return getattr(financial_source, fn_name)("0700.HK", "quarter", 4)

    @pytest.mark.parametrize("fn_name", ["_get_income_statements", "_get_cash_flows"])
    def test_rows_take_financial_currency(self, fn_name):
        rows = self._run(fn_name, {"currency": "HKD", "financialCurrency": "CNY"})
        assert rows and all(r["reportedCurrency"] == "CNY" for r in rows)

    @pytest.mark.parametrize("fn_name", ["_get_income_statements", "_get_cash_flows"])
    def test_unknown_currency_left_absent(self, fn_name):
        rows = self._run(fn_name, {"currency": "HKD"})
        assert rows and all("reportedCurrency" not in r for r in rows)
