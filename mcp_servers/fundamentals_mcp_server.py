#!/usr/bin/env python3
"""Fundamentals MCP Server.

Raw FMP fundamental data for programmatic analysis via MCP. Payloads stay
vendor-native inside `data`; the envelope around them is the standard
market-data contract (AGENT_CONTRACT.md).

Tools:
- get_financial_statements: Raw income/balance/cash flow (multi-year)
- get_financial_ratios: Raw ratios and key metrics (multi-year)
- get_growth_metrics: Raw growth rates (multi-year)
- get_historical_valuation: Raw DCF and enterprise value (multi-year)
- get_insider_trades: Insider trading transactions and aggregate stats
- get_dividends_and_splits: Dividend history and stock split history
- get_shares_float: Shares float, outstanding shares, and float percentage
- get_key_executives: Key executives with title and compensation
- get_technical_indicator: Technical indicators (RSI, EMA, MACD, etc.)
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

from typing import Literal

from mcp.server.fastmcp import FastMCP

from data_client.fmp import get_fmp_client, fmp_lifespan
from mcp_servers._envelope import error_from_exception, make_error, make_response
from src.market_protocol.symbology import to_canonical, to_display


mcp = FastMCP("FundamentalsMCP", lifespan=fmp_lifespan)

_SOURCE = "fmp"
_CLIENT_UNAVAILABLE = "FMP client is unavailable"
_UPSTREAM_FAILED = "FMP request failed"


def _canonical(symbol: str) -> str:
    """Canonical display spelling for an input ticker; echo input on failure."""
    try:
        return to_display(to_canonical(symbol))
    except Exception:  # noqa: BLE001
        return symbol


@mcp.tool()
async def get_financial_statements(
    symbol: str,
    statement_type: Literal["income", "balance", "cash", "all"] = "all",
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical financial statements for trend analysis or model
    building. statement_type="all" returns income, balance sheet, and cash flow.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SS".
        statement_type: "income" | "balance" | "cash" | "all".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, statement_type, period}.
        One statement_type → a list of period records; "all" → {income_statement,
        balance_sheet, cash_flow} lists with count the total. Common camelCase
        fields: date, revenue, netIncome, eps, totalAssets, operatingCashFlow.
        date is "YYYY-MM-DD"; records are newest-first as returned by FMP. On
        error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        if statement_type == "income":
            data = await client.get_income_statement(symbol, period=period, limit=limit)
        elif statement_type == "balance":
            data = await client.get_balance_sheet(symbol, period=period, limit=limit)
        elif statement_type == "cash":
            data = await client.get_cash_flow(symbol, period=period, limit=limit)
        else:  # "all"
            income = await client.get_income_statement(symbol, period=period, limit=limit)
            balance = await client.get_balance_sheet(symbol, period=period, limit=limit)
            cash_flow = await client.get_cash_flow(symbol, period=period, limit=limit)
            data = {
                "income_statement": income or [],
                "balance_sheet": balance or [],
                "cash_flow": cash_flow or [],
            }

        return make_response(
            data if statement_type == "all" else (data or []),
            source=_SOURCE,
            symbol=disp,
            data_type="financial_statements",
            statement_type=statement_type,
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_financial_ratios(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical key metrics and financial ratios — track P/E, ROE,
    and margins over time, or compare valuation across companies.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SS".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {key_metrics, ratios}, each a list of period records; count is the total.
        key_metrics fields: date, marketCap, enterpriseValue, evToEBITDA,
        freeCashFlowYield. ratios fields: date, netProfitMargin, returnOnEquity,
        currentRatio, debtToEquityRatio, priceToEarningsRatio. Field names are
        FMP-native camelCase; date is "YYYY-MM-DD"; records are newest-first as
        returned by FMP. On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        key_metrics = await client.get_key_metrics(symbol, period=period, limit=limit)
        ratios = await client.get_financial_ratios(symbol, period=period, limit=limit)

        return make_response(
            {"key_metrics": key_metrics or [], "ratios": ratios or []},
            source=_SOURCE,
            symbol=disp,
            data_type="financial_ratios",
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_growth_metrics(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical growth rates for trend analysis — chart revenue/EPS
    trajectory or compare growth across competitors.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SS".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {financial_growth, income_statement_growth}, each a list; count is the
        total. financial_growth fields: date, revenueGrowth, netIncomeGrowth,
        epsgrowth; income_statement_growth fields: date, growthRevenue,
        growthNetIncome, growthEPS. Growth values are decimal fractions
        (0.1 = 10%). Field names are FMP-native camelCase; date is "YYYY-MM-DD";
        records are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        financial_growth = await client.get_financial_growth(symbol, period=period, limit=limit)
        income_growth = await client.get_income_statement_growth(symbol, period=period, limit=limit)

        return make_response(
            {
                "financial_growth": financial_growth or [],
                "income_statement_growth": income_growth or [],
            },
            source=_SOURCE,
            symbol=disp,
            data_type="growth_metrics",
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_historical_valuation(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch DCF fair value and enterprise value history — track fair value vs
    price or build valuation trend charts.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SS".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {current_dcf, historical_dcf, enterprise_value}, each a list; count is
        the total. current_dcf fields: symbol, date, dcf, stockPrice.
        historical_dcf is always [] — the stable FMP API no longer exposes it.
        enterprise_value fields: date, stockPrice, marketCapitalization,
        enterpriseValue. Field names are FMP-native camelCase; date is
        "YYYY-MM-DD"; enterprise_value is newest-first as returned by FMP. On
        error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        current_dcf = await client.get_dcf(symbol)
        historical_dcf = await client.get_historical_dcf(symbol, period=period, limit=limit)
        enterprise_value = await client.get_enterprise_value(symbol, period=period, limit=limit)

        return make_response(
            {
                "current_dcf": current_dcf or [],
                "historical_dcf": historical_dcf or [],
                "enterprise_value": enterprise_value or [],
            },
            source=_SOURCE,
            symbol=disp,
            data_type="historical_valuation",
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_insider_trades(
    symbol: str,
    limit: int = 50,
) -> dict:
    """Fetch insider trading transactions and aggregate buy/sell statistics —
    detect insider buying clusters, screen unusual activity, or gauge C-suite
    confidence.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS".
        limit: Number of recent transactions to fetch (default 50).

    Returns:
        dict: {symbol, count, data, source, data_type}. data is {trades, stats},
        each a list; count is the total across both. trades fields: symbol,
        filingDate, transactionDate, reportingName, transactionType,
        securitiesTransacted, price. stats fields: year, quarter, totalBought,
        totalSold, buySellRatio. Field names are FMP-native camelCase; dates are
        "YYYY-MM-DD"; trades are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        trades = await client.get_insider_trades(symbol, limit=limit)
        stats = await client.get_insider_trade_stats(symbol)

        return make_response(
            {"trades": trades or [], "stats": stats or []},
            source=_SOURCE,
            symbol=disp,
            data_type="insider_trades",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_dividends_and_splits(
    symbol: str,
) -> dict:
    """Fetch historical dividend payments and stock splits — analyze dividend
    growth, adjust prices for splits, or compare dividend history across peers.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source, data_type}. data is
        {dividends, splits}, each a list; count is the total across both.
        dividends fields: date, recordDate, paymentDate, declarationDate,
        adjDividend, dividend, yield, frequency. splits fields: date, numerator,
        denominator. Field names are FMP-native camelCase; date is "YYYY-MM-DD";
        records are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        dividends = await client.get_dividends(symbol)
        splits = await client.get_splits(symbol)

        return make_response(
            {"dividends": dividends or [], "splits": splits or []},
            source=_SOURCE,
            symbol=disp,
            data_type="dividends_and_splits",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_shares_float(
    symbol: str,
) -> dict:
    """Fetch shares float, outstanding shares, and float percentage — flag
    low-float names, gauge ownership concentration, or screen squeeze candidates.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source, data_type}. data is a list of
        records; count is the record total. Fields: symbol, date, freeFloat,
        floatShares, outstandingShares. Field names are FMP-native camelCase;
        date is "YYYY-MM-DD"; latest snapshot first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_shares_float(symbol)

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="shares_float",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_key_executives(
    symbol: str,
) -> dict:
    """Fetch key executives with title and compensation — identify the
    management team or compare executive pay across peers.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source, data_type}. data is a list of
        executive records; count is the record total. Fields: name, title, pay,
        currencyPay, gender, yearBorn, titleSince. Field names are FMP-native
        camelCase; pay is an integer in currencyPay units; order is as returned
        by FMP (not time-ordered). On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_key_executives(symbol)

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="key_executives",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


@mcp.tool()
async def get_technical_indicator(
    symbol: str,
    indicator: str,
    period: int = 14,
    timeframe: str = "1day",
) -> dict:
    """Fetch a technical indicator time series over OHLCV bars — plot RSI,
    overlay EMA/MACD, or screen by technical signals.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SS".
        indicator: FMP name — "rsi", "ema", "sma", "wma", "adx", "williams".
        period: Indicator lookback length (default 14).
        timeframe: FMP-native bar — "1min"…"4hour", "1day" (default "1day").

    Returns:
        dict: {symbol, count, data, source, data_type, indicator, period,
        timeframe}. data is a list of bars; count is the bar total. Fields: date,
        open, high, low, close, volume, and the indicator value keyed by its
        name. date is "YYYY-MM-DD" (1day) or "YYYY-MM-DD HH:MM:SS"
        (intraday); field names are FMP-native camelCase; bars newest-first.
        On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_technical_indicator(
            symbol, indicator=indicator, period=period, timeframe=timeframe
        )

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="technical_indicator",
            indicator=indicator,
            period=period,
            timeframe=timeframe,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


if __name__ == "__main__":
    mcp.run()
