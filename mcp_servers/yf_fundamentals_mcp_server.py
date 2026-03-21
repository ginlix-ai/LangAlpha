#!/usr/bin/env python3
"""YFinance Fundamentals MCP Server.

Financial statements, earnings, company info, and valuations via yfinance.

Tools:
- get_income_statement: Quarterly/annual income statement
- get_balance_sheet: Quarterly/annual balance sheet
- get_cash_flow: Quarterly/annual cash flow statement
- get_company_info: Comprehensive company metadata
- get_earnings_dates: Earnings calendar with EPS estimates vs actuals
- get_earnings_data: Historical EPS actuals vs estimates (earnings_history)
- compare_financials: Side-by-side financial statements for multiple tickers
- compare_valuations: Valuation multiples for multiple tickers
- get_multiple_stocks_earnings: Earnings data for multiple tickers
"""

import json
from typing import Any, List, Optional

import pandas as pd
import yfinance as yf
from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Helpers (inlined — each yf server is deployed as a single file)
# ---------------------------------------------------------------------------


def _format_datetime(value) -> str:
    """YYYY-MM-DD for dates, YYYY-MM-DD HH:MM:SS for datetimes with time."""
    if hasattr(value, "hour"):
        if value.hour or value.minute or value.second:
            return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _serialize_dataframe(df: pd.DataFrame) -> dict:
    """Convert financial statement DataFrame to {metric: {date: value}} dict."""
    if df is None or df.empty:
        return {}
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.strftime("%Y-%m-%d")
    if isinstance(df.columns, pd.DatetimeIndex):
        df.columns = df.columns.strftime("%Y-%m-%d")
    return json.loads(df.fillna("N/A").to_json(orient="index"))


def _serialize_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts with clean keys and values."""
    if df is None or df.empty:
        return []
    df = df.reset_index() if not isinstance(df.index, pd.RangeIndex) else df.copy()
    records = df.to_dict(orient="records")
    cleaned = []
    for rec in records:
        clean_rec = {}
        for key, value in rec.items():
            clean_key = (
                str(key)
                .lower()
                .replace(" ", "_")
                .replace("(%)", "_pct")
                .replace("%", "pct")
                .replace("(", "")
                .replace(")", "")
            )
            if hasattr(value, "isoformat"):
                clean_rec[clean_key] = _format_datetime(value)
            elif isinstance(value, float) and value != value:  # NaN
                clean_rec[clean_key] = None
            else:
                clean_rec[clean_key] = value
        cleaned.append(clean_rec)
    return cleaned


def _make_response(
    data_type: str, data: Any, count: Optional[int] = None, **extra: Any
) -> dict:
    resp = {"data_type": data_type, "source": "yfinance", "data": data}
    if count is not None:
        resp["count"] = count
    elif isinstance(data, list):
        resp["count"] = len(data)
    elif isinstance(data, dict):
        resp["count"] = len(data)
    resp.update(extra)
    return resp


def _make_error(msg: str) -> dict:
    return {"error": msg}


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = FastMCP("YFinanceFundamentalsMCP")


# ---------------------------------------------------------------------------
# Single-ticker tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_income_statement(ticker: str, quarterly: bool = True) -> dict:
    """Get income statement data (revenue, expenses, net income, margins, etc.)

    Returns a dict keyed by metric name (e.g. "Total Revenue", "Net Income"),
    each mapping dates to values: {metric: {date: value, ...}, ...}.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
        quarterly: If True returns quarterly data; if False returns annual data
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.quarterly_income_stmt if quarterly else stock.income_stmt
        if df is None or df.empty:
            return _make_error(f"No income statement data for {ticker}")
        return _make_response(
            "income_statement",
            _serialize_dataframe(df),
            ticker=ticker,
            quarterly=quarterly,
        )
    except Exception as e:
        return _make_error(f"Failed to fetch income statement for {ticker}: {e}")


@mcp.tool()
def get_balance_sheet(ticker: str, quarterly: bool = True) -> dict:
    """Get balance sheet data (assets, liabilities, equity).

    Returns a dict keyed by metric name (e.g. "Total Assets", "Total Debt"),
    each mapping dates to values: {metric: {date: value, ...}, ...}.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
        quarterly: If True returns quarterly data; if False returns annual data
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.quarterly_balance_sheet if quarterly else stock.balance_sheet
        if df is None or df.empty:
            return _make_error(f"No balance sheet data for {ticker}")
        return _make_response(
            "balance_sheet",
            _serialize_dataframe(df),
            ticker=ticker,
            quarterly=quarterly,
        )
    except Exception as e:
        return _make_error(f"Failed to fetch balance sheet for {ticker}: {e}")


@mcp.tool()
def get_cash_flow(ticker: str, quarterly: bool = True) -> dict:
    """Get cash flow statement data (operating, investing, financing activities).

    Returns a dict keyed by metric name (e.g. "Operating Cash Flow", "Capital Expenditure"),
    each mapping dates to values: {metric: {date: value, ...}, ...}.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
        quarterly: If True returns quarterly data; if False returns annual data
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.quarterly_cashflow if quarterly else stock.cashflow
        if df is None or df.empty:
            return _make_error(f"No cash flow data for {ticker}")
        return _make_response(
            "cash_flow",
            _serialize_dataframe(df),
            ticker=ticker,
            quarterly=quarterly,
        )
    except Exception as e:
        return _make_error(f"Failed to fetch cash flow for {ticker}: {e}")


@mcp.tool()
def get_company_info(ticker: str) -> dict:
    """Get comprehensive company information (sector, industry, market cap, ratios, etc.)

    Returns a flat dict with keys like shortName, sector, industry, marketCap,
    trailingPE, forwardPE, dividendYield, fullTimeEmployees, longBusinessSummary,
    etc. None values are omitted.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        if not info:
            return _make_error(f"No company info for {ticker}")

        cleaned = {}
        for key, value in info.items():
            if value is None:
                continue
            if isinstance(value, float) and value != value:  # NaN
                continue
            if hasattr(value, "isoformat"):
                cleaned[key] = _format_datetime(value)
            else:
                cleaned[key] = value

        return _make_response("company_info", cleaned, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to fetch company info for {ticker}: {e}")


@mcp.tool()
def get_earnings_dates(ticker: str) -> dict:
    """Get earnings announcement dates with EPS estimates vs actuals.

    Returns a list of records with keys: earnings_date (YYYY-MM-DD HH:MM:SS),
    eps_estimate, reported_eps, surprise_pct. Future dates will have null
    for reported_eps and surprise_pct.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
    """
    try:
        stock = yf.Ticker(ticker)
        dates = stock.earnings_dates
        if dates is None or dates.empty:
            return _make_error(f"No earnings dates for {ticker}")
        return _make_response(
            "earnings_dates", _serialize_records(dates), ticker=ticker
        )
    except Exception as e:
        return _make_error(f"Failed to fetch earnings dates for {ticker}: {e}")


@mcp.tool()
def get_earnings_data(ticker: str) -> dict:
    """Get historical earnings data (EPS estimates vs actuals per quarter).

    Returns a list of records with keys: epsestimate, epsactual,
    epsdifference, surprisepercent, plus a quarter date field.
    Same underlying data as get_earnings_history in the analysis server.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
    """
    try:
        stock = yf.Ticker(ticker)
        earnings = stock.earnings_history
        if earnings is None or earnings.empty:
            return _make_error(f"No earnings data for {ticker}")
        return _make_response(
            "earnings_data", _serialize_records(earnings), ticker=ticker
        )
    except Exception as e:
        return _make_error(f"Failed to fetch earnings for {ticker}: {e}")


# ---------------------------------------------------------------------------
# Multi-ticker tools
# ---------------------------------------------------------------------------


@mcp.tool()
def compare_financials(
    tickers: List[str],
    statement_type: str = "income",
    quarterly: bool = True,
) -> dict:
    """Get financial statements for multiple companies for side-by-side comparison.

    Returns data keyed by ticker symbol, where each value is a
    {metric: {date: value}} dict. Failed tickers appear in "errors".

    Args:
        tickers: List of ticker symbols (e.g. ["AAPL", "MSFT", "GOOGL"])
        statement_type: "income", "balance", or "cashflow"
        quarterly: If True returns quarterly data; if False returns annual
    """
    if statement_type not in ("income", "balance", "cashflow"):
        return _make_error(f"Invalid statement_type '{statement_type}'. Must be 'income', 'balance', or 'cashflow'.")

    data = {}
    errors = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            if statement_type == "income":
                df = stock.quarterly_income_stmt if quarterly else stock.income_stmt
            elif statement_type == "balance":
                df = stock.quarterly_balance_sheet if quarterly else stock.balance_sheet
            else:
                df = stock.quarterly_cashflow if quarterly else stock.cashflow

            if df is None or df.empty:
                errors.append(f"No {statement_type} data for {ticker}")
                continue

            data[ticker] = _serialize_dataframe(df)
        except Exception as e:
            errors.append(f"{ticker}: {e}")

    result = _make_response(
        "compare_financials",
        data,
        statement_type=statement_type,
        quarterly=quarterly,
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


@mcp.tool()
def compare_valuations(tickers: List[str]) -> dict:
    """Compare valuation metrics (P/E, P/B, dividend yield, etc.) across multiple stocks.

    Returns data keyed by ticker symbol. Each value is a dict with snake_case
    metric names: trailing_p_e, forward_p_e, price_to_book, dividend_yield,
    market_cap, enterprise_value, beta, current_price, peg_ratio, etc.

    Args:
        tickers: List of ticker symbols (e.g. ["AAPL", "MSFT", "GOOGL"])
    """
    valuation_keys = [
        "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months",
        "enterpriseToEbitda", "enterpriseToRevenue", "pegRatio",
        "dividendYield", "payoutRatio", "marketCap", "enterpriseValue",
        "beta", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyDayAverage",
        "twoHundredDayAverage", "currentPrice",
    ]

    data = {}
    errors = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if not info:
                errors.append(f"No info data for {ticker}")
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

            data[ticker] = valuations
        except Exception as e:
            errors.append(f"{ticker}: {e}")

    result = _make_response(
        "compare_valuations",
        data,
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


@mcp.tool()
def get_multiple_stocks_earnings(tickers: List[str]) -> dict:
    """Get earnings data for multiple stocks in a single call.

    Returns data keyed by ticker symbol, each with an "earnings" list of
    records (epsestimate, epsactual, epsdifference, surprisepercent) and
    a "count". Failed tickers appear in "errors".

    Args:
        tickers: List of ticker symbols (e.g. ["AAPL", "MSFT", "GOOGL"])
    """
    data = {}
    errors = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            earnings = stock.earnings_history
            if earnings is None or earnings.empty:
                errors.append(f"No earnings data for {ticker}")
                continue
            records = _serialize_records(earnings)
            data[ticker] = {"earnings": records, "count": len(records)}
        except Exception as e:
            errors.append(f"{ticker}: {e}")

    result = _make_response(
        "multiple_stocks_earnings",
        data,
        successful_tickers=list(data.keys()),
    )
    if errors:
        result["errors"] = errors
    return result


if __name__ == "__main__":
    mcp.run(transport="stdio")
