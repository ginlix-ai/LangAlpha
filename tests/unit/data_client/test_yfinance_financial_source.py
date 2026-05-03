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
