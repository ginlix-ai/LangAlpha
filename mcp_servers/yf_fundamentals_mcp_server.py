#!/usr/bin/env python3
"""YFinance Fundamentals MCP Server.

Financial statements, earnings, company info, and valuations via yfinance,
wrapped in the standard market-data envelope (see
mcp_servers/AGENT_CONTRACT.md).

Tools:
- get_income_statement / get_balance_sheet / get_cash_flow: statements
- get_company_info: company metadata
- get_earnings_dates: upcoming/past earnings with EPS estimate vs actual
- get_earnings_data: historical EPS actual vs estimate
- compare_financials / compare_valuations / get_multiple_stocks_earnings: multi
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    import _bootstrap  # noqa: F401  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers import _bootstrap  # noqa: F401

import json
from typing import List

import pandas as pd
import yfinance as yf
from mcp.server.fastmcp import FastMCP

from mcp_servers._envelope import make_error, make_response
from mcp_servers._yf_common import boundary, format_datetime, safe_detail, serialize_records

SOURCE = "yfinance"


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _serialize_dataframe(df: pd.DataFrame) -> dict:
    """Statement DataFrame → {metric: {date: value}}; date keys newest first."""
    if df is None or df.empty:
        return {}
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.strftime("%Y-%m-%d")
    if isinstance(df.columns, pd.DatetimeIndex):
        df.columns = df.columns.strftime("%Y-%m-%d")
    return json.loads(df.fillna("N/A").to_json(orient="index"))


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = FastMCP("YFinanceFundamentalsMCP")


# ---------------------------------------------------------------------------
# Single-ticker tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_income_statement(ticker: str, quarterly: bool = True) -> dict:
    """Income statement (revenue, expenses, margins, net income). Use for
    profitability and top-line trends.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".
        quarterly: Quarterly if True, else annual.

    Returns:
        dict: {symbol, quarterly, count, data, source}. data is Yahoo-native
        {metric: {date: value}} — metric names are Yahoo-native (e.g.
        "Total Revenue", "Net Income"); date keys are "YYYY-MM-DD" ordered newest
        first; values are in the company's reporting currency (native units).
        count is the number of metrics. On error: {error, detail, symbol} with
        error not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        df = stock.quarterly_income_stmt if quarterly else stock.income_stmt
        if df is None or df.empty:
            return make_error(
                "not_found", f"No income statement for {symbol}.", symbol=symbol
            )
        data = _serialize_dataframe(df)
        return make_response(
            data, source=SOURCE, symbol=symbol, count=len(data), quarterly=quarterly
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Income statement", e), symbol=symbol
        )


@mcp.tool()
def get_balance_sheet(ticker: str, quarterly: bool = True) -> dict:
    """Balance sheet (assets, liabilities, equity). Use for leverage, liquidity,
    and capital-structure analysis.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".
        quarterly: Quarterly if True, else annual.

    Returns:
        dict: {symbol, quarterly, count, data, source}. data is Yahoo-native
        {metric: {date: value}} — metric names Yahoo-native (e.g. "Total Assets",
        "Total Debt"); date keys "YYYY-MM-DD" ordered newest first; values in the
        company's reporting currency (native units). count is the number of
        metrics. On error: {error, detail, symbol} with error
        not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        df = stock.quarterly_balance_sheet if quarterly else stock.balance_sheet
        if df is None or df.empty:
            return make_error(
                "not_found", f"No balance sheet for {symbol}.", symbol=symbol
            )
        data = _serialize_dataframe(df)
        return make_response(
            data, source=SOURCE, symbol=symbol, count=len(data), quarterly=quarterly
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Balance sheet", e), symbol=symbol
        )


@mcp.tool()
def get_cash_flow(ticker: str, quarterly: bool = True) -> dict:
    """Cash flow statement (operating, investing, financing). Use for cash
    generation and capex analysis.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".
        quarterly: Quarterly if True, else annual.

    Returns:
        dict: {symbol, quarterly, count, data, source}. data is Yahoo-native
        {metric: {date: value}} — metric names Yahoo-native (e.g.
        "Operating Cash Flow", "Capital Expenditure"); date keys "YYYY-MM-DD"
        ordered newest first; values in the company's reporting currency (native
        units). count is the number of metrics. On error:
        {error, detail, symbol} with error not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        df = stock.quarterly_cashflow if quarterly else stock.cashflow
        if df is None or df.empty:
            return make_error(
                "not_found", f"No cash flow statement for {symbol}.", symbol=symbol
            )
        data = _serialize_dataframe(df)
        return make_response(
            data, source=SOURCE, symbol=symbol, count=len(data), quarterly=quarterly
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Cash flow", e), symbol=symbol
        )


@mcp.tool()
def get_company_info(ticker: str) -> dict:
    """Company profile and key statistics. Use for sector/industry, market cap,
    valuation ratios, and business summary.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, currency?, count, data, source}. data is one Yahoo-native
        record — a flat dict of Yahoo `info` fields (e.g. shortName, sector,
        industry, marketCap, trailingPE, forwardPE, dividendYield,
        longBusinessSummary); field names are Yahoo-native, price-scale fields
        are unconverted, and None values are dropped. currency is Yahoo's
        declared trading currency (a minor-unit label like GBp on some venues),
        omitted when Yahoo declares none; count is 1. On error:
        {error, detail, symbol} with error not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        info = yf.Ticker(yf_symbol).info
        if not info:
            return make_error(
                "not_found", f"No company info for {symbol}.", symbol=symbol
            )

        cleaned = {}
        for key, value in info.items():
            if value is None:
                continue
            if isinstance(value, float) and value != value:  # NaN
                continue
            if hasattr(value, "isoformat"):
                cleaned[key] = format_datetime(value)
            else:
                cleaned[key] = value

        return make_response(
            cleaned,
            source=SOURCE,
            symbol=symbol,
            currency=info.get("currency"),
            count=1,
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Company info", e), symbol=symbol
        )


@mcp.tool()
def get_earnings_dates(ticker: str) -> dict:
    """Earnings announcement dates with EPS estimate vs. actual. Use for the
    earnings schedule and recent surprises.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {earnings_date, eps_estimate, reported_eps, surprise_pct} ordered
        newest first (future dates included, with null reported_eps/surprise_pct);
        earnings_date is exchange-local "YYYY-MM-DD HH:MM:SS". On error:
        {error, detail, symbol} with error not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        dates = yf.Ticker(yf_symbol).earnings_dates
        if dates is None or dates.empty:
            return make_error(
                "not_found", f"No earnings dates for {symbol}.", symbol=symbol
            )
        return make_response(
            serialize_records(dates), source=SOURCE, symbol=symbol
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Earnings dates", e), symbol=symbol
        )


@mcp.tool()
def get_earnings_data(ticker: str) -> dict:
    """Historical EPS estimate vs. actual per quarter. Use to review earnings
    beats/misses. Same data as get_earnings_history (analysis server).

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {quarter, epsestimate, epsactual, epsdifference, surprisepercent}
        in Yahoo's order (oldest first); quarter is "YYYY-MM-DD". On error:
        {error, detail, symbol} with error not_found|upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        earnings = yf.Ticker(yf_symbol).earnings_history
        if earnings is None or earnings.empty:
            return make_error(
                "not_found", f"No earnings data for {symbol}.", symbol=symbol
            )
        return make_response(
            serialize_records(earnings), source=SOURCE, symbol=symbol
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Earnings data", e), symbol=symbol
        )


# ---------------------------------------------------------------------------
# Multi-ticker tools
# ---------------------------------------------------------------------------


@mcp.tool()
def compare_financials(
    tickers: List[str],
    statement_type: str = "income",
    quarterly: bool = True,
) -> dict:
    """Financial statements for several companies for side-by-side comparison.

    Args:
        tickers: Symbols, e.g. ["AAPL", "MSFT", "GOOGL"].
        statement_type: "income", "balance", or "cashflow".
        quarterly: Quarterly if True, else annual.

    Returns:
        dict: {statement_type, quarterly, count, data, source,
        successful_tickers, errors?}. data is keyed by canonical symbol →
        Yahoo-native {metric: {date: value}} (date keys "YYYY-MM-DD" newest
        first, reporting-currency native units, Yahoo-native metric names); count
        is total metrics across symbols. Failed/empty symbols are omitted and
        listed in errors as {error, detail, symbol}. On a bad statement_type:
        {error: invalid_argument, detail, supported}.
    """
    if statement_type not in ("income", "balance", "cashflow"):
        return make_error(
            "invalid_argument",
            "statement_type must be one of income, balance, cashflow.",
            supported=["income", "balance", "cashflow"],
        )

    data: dict[str, dict] = {}
    errors: list[dict] = []
    total = 0

    for ticker in tickers:
        symbol, yf_symbol, _ = boundary(ticker)
        try:
            stock = yf.Ticker(yf_symbol)
            if statement_type == "income":
                df = stock.quarterly_income_stmt if quarterly else stock.income_stmt
            elif statement_type == "balance":
                df = stock.quarterly_balance_sheet if quarterly else stock.balance_sheet
            else:
                df = stock.quarterly_cashflow if quarterly else stock.cashflow

            if df is None or df.empty:
                errors.append(
                    make_error(
                        "not_found",
                        f"No {statement_type} statement for {symbol}.",
                        symbol=symbol,
                    )
                )
                continue

            serialized = _serialize_dataframe(df)
            data[symbol] = serialized
            total += len(serialized)
        except Exception as e:  # noqa: BLE001
            errors.append(
                make_error(
                    "upstream_error",
                    safe_detail("Financial statement", e),
                    symbol=symbol,
                )
            )

    result = make_response(
        data,
        source=SOURCE,
        count=total,
        statement_type=statement_type,
        quarterly=quarterly,
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


@mcp.tool()
def compare_valuations(tickers: List[str]) -> dict:
    """Valuation multiples across several stocks for side-by-side comparison.

    Args:
        tickers: Symbols, e.g. ["AAPL", "MSFT", "GOOGL"].

    Returns:
        dict: {count, data, source, successful_tickers, errors?}. data is keyed
        by canonical symbol → a dict of snake_case metrics: trailing_p_e,
        forward_p_e, price_to_book, price_to_sales_trailing12_months,
        enterprise_to_ebitda, enterprise_to_revenue, peg_ratio, dividend_yield,
        payout_ratio, market_cap, enterprise_value, beta,
        fifty_two_week_high/low, fifty_day_average, two_hundred_day_average,
        current_price (nulls where absent); count is the number of symbols
        returned. Failed symbols are omitted and listed in errors as
        {error, detail, symbol}.
    """
    valuation_keys = [
        "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months",
        "enterpriseToEbitda", "enterpriseToRevenue", "pegRatio",
        "dividendYield", "payoutRatio", "marketCap", "enterpriseValue",
        "beta", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyDayAverage",
        "twoHundredDayAverage", "currentPrice",
    ]

    data: dict[str, dict] = {}
    errors: list[dict] = []

    for ticker in tickers:
        symbol, yf_symbol, _ = boundary(ticker)
        try:
            info = yf.Ticker(yf_symbol).info
            if not info:
                errors.append(
                    make_error(
                        "not_found", f"No info for {symbol}.", symbol=symbol
                    )
                )
                continue

            valuations = {}
            for key in valuation_keys:
                val = info.get(key)
                snake_key = "".join(
                    ["_" + c.lower() if c.isupper() else c for c in key]
                ).lstrip("_")
                if val is None or (isinstance(val, float) and val != val):
                    valuations[snake_key] = None
                else:
                    valuations[snake_key] = val

            data[symbol] = valuations
        except Exception as e:  # noqa: BLE001
            errors.append(
                make_error(
                    "upstream_error", safe_detail("Valuation", e), symbol=symbol
                )
            )

    result = make_response(
        data,
        source=SOURCE,
        count=len(data),
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


@mcp.tool()
def get_multiple_stocks_earnings(tickers: List[str]) -> dict:
    """Historical earnings (EPS estimate vs. actual) for several stocks at once.

    Args:
        tickers: Symbols, e.g. ["AAPL", "MSFT", "GOOGL"].

    Returns:
        dict: {count, data, source, successful_tickers, errors?}. data is keyed
        by canonical symbol → {count, earnings}; each earnings is a list of
        Yahoo-native records {quarter, epsestimate, epsactual, epsdifference,
        surprisepercent} in Yahoo's order (oldest first), quarter "YYYY-MM-DD".
        Top-level count is total earnings records across symbols. Failed/empty
        symbols are omitted and listed in errors as {error, detail, symbol}.
    """
    data: dict[str, dict] = {}
    errors: list[dict] = []
    total = 0

    for ticker in tickers:
        symbol, yf_symbol, _ = boundary(ticker)
        try:
            earnings = yf.Ticker(yf_symbol).earnings_history
            if earnings is None or earnings.empty:
                errors.append(
                    make_error(
                        "not_found",
                        f"No earnings data for {symbol}.",
                        symbol=symbol,
                    )
                )
                continue
            records = serialize_records(earnings)
            data[symbol] = {"count": len(records), "earnings": records}
            total += len(records)
        except Exception as e:  # noqa: BLE001
            errors.append(
                make_error(
                    "upstream_error", safe_detail("Earnings", e), symbol=symbol
                )
            )

    result = make_response(
        data,
        source=SOURCE,
        count=total,
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


if __name__ == "__main__":
    mcp.run(transport="stdio")
