#!/usr/bin/env python3
"""Fundamentals MCP Server.

Provides raw fundamental data for programmatic analysis via MCP.
Designed for multi-year trend analysis, cross-company comparison, and chart creation.

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

from __future__ import annotations

from typing import Literal

from mcp.server.fastmcp import FastMCP

from data_client.fmp import get_fmp_client, fmp_lifespan


mcp = FastMCP("FundamentalsMCP", lifespan=fmp_lifespan)


def _add_quarterly_empty_note(result: dict, period: str) -> None:
    count = result.get("count")
    is_empty = count == 0 or (
        isinstance(count, dict) and all(value == 0 for value in count.values())
    )
    if period == "quarter" and is_empty:
        result["note"] = (
            "Quarterly data returned 0 records. Some FMP subscription tiers do "
            "not include quarterly fundamentals. Try period='annual'."
        )


@mcp.tool()
async def get_financial_statements(
    symbol: str,
    statement_type: Literal["income", "balance", "cash", "all"] = "all",
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical financial statements for multi-year trend analysis.

    Use cases:
    - Plot revenue/earnings growth over 10 years
    - Compare profit margins across competitors
    - Build financial models with historical data

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        statement_type: "income", "balance", "cash", or "all" (default: "all")
        period: "annual" or "quarter" (default: "annual")
        limit: Number of periods to fetch (default: 10)

    Returns:
        Raw JSON with full statement data (all fields, not summarized)
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    result = {
        "symbol": symbol,
        "data_type": "financial_statements",
        "statement_type": statement_type,
        "period": period,
        "source": "fmp",
    }

    try:
        if statement_type == "income":
            data = await client.get_income_statement(
                symbol, period=period, limit=limit
            )
            result["data"] = data
            result["count"] = len(data) if data else 0

        elif statement_type == "balance":
            data = await client.get_balance_sheet(
                symbol, period=period, limit=limit
            )
            result["data"] = data
            result["count"] = len(data) if data else 0

        elif statement_type == "cash":
            data = await client.get_cash_flow(symbol, period=period, limit=limit)
            result["data"] = data
            result["count"] = len(data) if data else 0

        else:  # "all"
            income = await client.get_income_statement(
                symbol, period=period, limit=limit
            )
            balance = await client.get_balance_sheet(
                symbol, period=period, limit=limit
            )
            cash_flow = await client.get_cash_flow(
                symbol, period=period, limit=limit
            )

            result["data"] = {
                "income_statement": income or [],
                "balance_sheet": balance or [],
                "cash_flow": cash_flow or [],
            }
            result["count"] = {
                "income_statement": len(income) if income else 0,
                "balance_sheet": len(balance) if balance else 0,
                "cash_flow": len(cash_flow) if cash_flow else 0,
            }

        _add_quarterly_empty_note(result, period)

        return result

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_financial_ratios(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical financial ratios and key metrics for quantitative analysis.

    Use cases:
    - Track P/E, P/B, ROE trends over time
    - Screen stocks by ratio thresholds
    - Compare valuation metrics across companies

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        period: "annual" or "quarter" (default: "annual")
        limit: Number of periods to fetch (default: 10)

    Returns:
        Raw JSON with key metrics and financial ratios per period

    Note: historical depth is approximately 2 years on standard FMP subscription
    tiers; older data may not be returned.
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        key_metrics = await client.get_key_metrics(
            symbol, period=period, limit=limit
        )
        ratios = await client.get_financial_ratios(
            symbol, period=period, limit=limit
        )

        result = {
            "symbol": symbol,
            "data_type": "financial_ratios",
            "period": period,
            "count": {
                "key_metrics": len(key_metrics) if key_metrics else 0,
                "ratios": len(ratios) if ratios else 0,
            },
            "data": {
                "key_metrics": key_metrics or [],
                "ratios": ratios or [],
            },
            "source": "fmp",
        }
        _add_quarterly_empty_note(result, period)

        return result

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_growth_metrics(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch raw historical growth rates for trend analysis.

    Use cases:
    - Chart revenue/EPS growth trajectory
    - Identify growth acceleration/deceleration
    - Compare growth rates across competitors

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        period: "annual" or "quarter" (default: "annual")
        limit: Number of periods to fetch (default: 10)

    Returns:
        Raw JSON with growth rates per period

    Note: historical depth is approximately 2 years on standard FMP subscription
    tiers; older data may not be returned.
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        financial_growth = await client.get_financial_growth(
            symbol, period=period, limit=limit
        )
        income_growth = await client.get_income_statement_growth(
            symbol, period=period, limit=limit
        )

        result = {
            "symbol": symbol,
            "data_type": "growth_metrics",
            "period": period,
            "count": {
                "financial_growth": len(financial_growth) if financial_growth else 0,
                "income_statement_growth": len(income_growth) if income_growth else 0,
            },
            "data": {
                "financial_growth": financial_growth or [],
                "income_statement_growth": income_growth or [],
            },
            "source": "fmp",
        }
        _add_quarterly_empty_note(result, period)

        return result

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_historical_valuation(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> dict:
    """Fetch historical DCF and enterprise value data for valuation analysis.

    Use cases:
    - Track fair value estimates over time
    - Compare current price vs historical DCF
    - Build valuation trend charts

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        period: "annual" or "quarter" (default: "annual")
        limit: Number of periods to fetch (default: 10)

    Returns:
        Raw JSON with current DCF, historical DCF, and enterprise value per period
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        current_dcf = await client.get_dcf(symbol)
        historical_dcf = await client.get_historical_dcf(
            symbol, period=period, limit=limit
        )
        enterprise_value = await client.get_enterprise_value(
            symbol, period=period, limit=limit
        )

        return {
            "symbol": symbol,
            "data_type": "historical_valuation",
            "period": period,
            "count": {
                "current_dcf": len(current_dcf) if current_dcf else 0,
                "historical_dcf": len(historical_dcf) if historical_dcf else 0,
                "enterprise_value": len(enterprise_value) if enterprise_value else 0,
            },
            "data": {
                "current_dcf": current_dcf or [],
                "historical_dcf": historical_dcf or [],
                "enterprise_value": enterprise_value or [],
            },
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_insider_trades(
    symbol: str,
    limit: int = 50,
) -> dict:
    """Fetch insider trading transactions and aggregate buy/sell statistics.

    Use cases:
    - Detect insider buying clusters as bullish signals
    - Screen for unusual insider activity
    - Track C-suite confidence via their own stock transactions

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        limit: Number of recent transactions to fetch (default: 50)

    Returns:
        Raw JSON with insider trades list and aggregate buy/sell stats
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        trades = await client.get_insider_trades(symbol, limit=limit)
        stats = await client.get_insider_trade_stats(symbol)

        return {
            "symbol": symbol,
            "data_type": "insider_trades",
            "count": {
                "trades": len(trades) if trades else 0,
                "stats": len(stats) if stats else 0,
            },
            "data": {
                "trades": trades or [],
                "stats": stats or [],
            },
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_dividends_and_splits(
    symbol: str,
) -> dict:
    """Fetch historical dividend payments and stock splits.

    Use cases:
    - Analyze dividend growth trajectory for income-oriented models
    - Adjust historical prices for splits in backtesting
    - Compare dividend yield history across peers

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)

    Returns:
        Raw JSON with dividend history and split history
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        dividends = await client.get_dividends(symbol)
        splits = await client.get_splits(symbol)

        return {
            "symbol": symbol,
            "data_type": "dividends_and_splits",
            "count": {
                "dividends": len(dividends) if dividends else 0,
                "splits": len(splits) if splits else 0,
            },
            "data": {
                "dividends": dividends or [],
                "splits": splits or [],
            },
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_shares_float(
    symbol: str,
) -> dict:
    """Fetch shares float, outstanding shares, and float percentage.

    Use cases:
    - Identify low-float stocks for volatility analysis
    - Calculate institutional ownership concentration
    - Screen for potential short squeeze candidates

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)

    Returns:
        Raw JSON with float shares, outstanding shares, and float percentage
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        data = await client.get_shares_float(symbol)

        return {
            "symbol": symbol,
            "data_type": "shares_float",
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_key_executives(
    symbol: str,
) -> dict:
    """Fetch key executives with title and compensation details.

    Use cases:
    - Identify management team for company analysis
    - Compare executive compensation across peers
    - Track management changes in earnings analysis

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)

    Returns:
        Raw JSON with executive name, title, pay, and currency
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        data = await client.get_key_executives(symbol)

        return {
            "symbol": symbol,
            "data_type": "key_executives",
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


@mcp.tool()
async def get_technical_indicator(
    symbol: str,
    indicator: str,
    period: int = 14,
    timeframe: str = "1day",
) -> dict:
    """Fetch technical indicator time series data.

    Use cases:
    - Plot RSI to identify overbought/oversold conditions
    - Overlay EMA/MACD on price charts for trend analysis
    - Screen stocks by technical signals

    Args:
        symbol: Stock ticker (e.g., AAPL, MSFT)
        indicator: Indicator name — "rsi", "ema", "macd", "adx", "wma", "dema", "tema", "williams", "standardDeviation"
        period: Indicator period length (default: 14)
        timeframe: "1min", "5min", "15min", "30min", "1hour", "4hour", "1day" (default: "1day")

    Returns:
        Raw JSON with date, OHLCV, and indicator values
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "symbol": symbol}

    try:
        data = await client.get_technical_indicator(
            symbol, indicator=indicator, period=period, timeframe=timeframe
        )

        return {
            "symbol": symbol,
            "data_type": "technical_indicator",
            "indicator": indicator,
            "period": period,
            "timeframe": timeframe,
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "symbol": symbol}


if __name__ == "__main__":
    mcp.run()
