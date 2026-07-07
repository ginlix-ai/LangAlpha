"""
LangChain tool wrappers for market data operations.

This module provides @tool decorated functions that serve as the LangChain interface.
The actual business logic is implemented in implementations.py.
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (the
# agent's call-time decision aid) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read the direct-tool paragraph of
# mcp_servers/AGENT_CONTRACT.md before editing; intentional changes must
# regenerate agent_docstring_lock.json.

from typing import Any, Dict, List, Optional, Tuple, Union

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from .implementations import (
    fetch_company_overview,
    fetch_market_indices,
    fetch_market_movers,
    fetch_options_chain,
    fetch_sector_performance,
    fetch_stock_daily_prices,
    fetch_stock_screener,
)


@tool(response_format="content_and_artifact")
async def get_stock_daily_prices(
    symbol: str,
    config: RunnableConfig,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
    """
    Historical daily OHLCV bars for one stock — for price history, trends, returns,
    and charting.

    Args:
        symbol: US "AAPL", A-share "600519.SS", HK "0700.HK".
        start_date: Start "YYYY-MM-DD" (optional).
        end_date: End "YYYY-MM-DD" (optional).
        limit: Max records when no date range is given (default 60 trading days).
    """
    content, artifact = await fetch_stock_daily_prices(
        symbol, start_date, end_date, limit, config=config
    )
    return content, artifact


@tool(response_format="content_and_artifact")
async def get_company_overview(
    symbol: str,
    config: RunnableConfig,
) -> Tuple[str, Dict[str, Any]]:
    """
    Full investment snapshot for one company — quote, financial health, analyst
    consensus, earnings, and revenue segmentation.

    Args:
        symbol: US "AAPL", A-share "600519.SS", HK "0700.HK".
    """
    content, artifact = await fetch_company_overview(symbol, config=config)
    return content, artifact


@tool(response_format="content_and_artifact")
async def get_market_indices(
    config: RunnableConfig,
    indices: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 60,
) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
    """
    Historical OHLCV bars for one or more market indices (default: major US indices).

    Args:
        indices: Index symbols; default ["^GSPC", "^IXIC", "^DJI", "^RUT"]
            (S&P 500, NASDAQ, Dow Jones, Russell 2000).
        start_date: Start "YYYY-MM-DD" (optional).
        end_date: End "YYYY-MM-DD" (optional).
        limit: Records per index (default 60).
    """
    content, artifact = await fetch_market_indices(
        indices, start_date, end_date, limit, config=config
    )
    return content, artifact


@tool(response_format="content_and_artifact")
async def get_sector_performance(
    date: Optional[str] = None,
) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
    """
    US stock-market sector performance — which sectors (Technology, Healthcare,
    Energy, Financials, …) are up or down on the day. US only; not for non-US markets.

    Args:
        date: Analysis date "YYYY-MM-DD" (default: latest available). Historical
            dates may be unavailable on the current data plan.
    """
    content, artifact = await fetch_sector_performance(date)
    return content, artifact


@tool(response_format="content_and_artifact")
async def screen_stocks(
    market_cap_more_than: Optional[float] = None,
    market_cap_lower_than: Optional[float] = None,
    price_more_than: Optional[float] = None,
    price_lower_than: Optional[float] = None,
    volume_more_than: Optional[float] = None,
    volume_lower_than: Optional[float] = None,
    beta_more_than: Optional[float] = None,
    beta_lower_than: Optional[float] = None,
    dividend_more_than: Optional[float] = None,
    dividend_lower_than: Optional[float] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    exchange: Optional[str] = None,
    country: Optional[str] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    is_actively_trading: Optional[bool] = None,
    limit: int = 50,
) -> Tuple[Union[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """
    Screen/discover stocks by fundamental and market filters — market cap, price,
    volume, beta, dividend, sector, industry, exchange, country, and type — via the
    FMP company screener.

    Args:
        market_cap_more_than: Minimum market capitalization (e.g. 1e9 for $1B).
        market_cap_lower_than: Maximum market capitalization.
        price_more_than: Minimum share price.
        price_lower_than: Maximum share price.
        volume_more_than: Minimum daily volume.
        volume_lower_than: Maximum daily volume.
        beta_more_than: Minimum beta.
        beta_lower_than: Maximum beta.
        dividend_more_than: Minimum dividend yield.
        dividend_lower_than: Maximum dividend yield.
        sector: e.g. "Technology", "Healthcare", "Financial Services".
        industry: e.g. "Software", "Biotechnology".
        exchange: e.g. "NASDAQ", "NYSE", "AMEX".
        country: e.g. "US", "CN", "GB".
        is_etf: ETFs only (True) or exclude ETFs (False).
        is_fund: Funds only (True) or exclude funds (False).
        is_actively_trading: Restrict to actively trading names.
        limit: Max results (default 50).
    """
    content, artifact = await fetch_stock_screener(
        market_cap_more_than=market_cap_more_than,
        market_cap_lower_than=market_cap_lower_than,
        price_more_than=price_more_than,
        price_lower_than=price_lower_than,
        volume_more_than=volume_more_than,
        volume_lower_than=volume_lower_than,
        beta_more_than=beta_more_than,
        beta_lower_than=beta_lower_than,
        dividend_more_than=dividend_more_than,
        dividend_lower_than=dividend_lower_than,
        sector=sector,
        industry=industry,
        exchange=exchange,
        country=country,
        is_etf=is_etf,
        is_fund=is_fund,
        is_actively_trading=is_actively_trading,
        limit=limit,
    )
    return content, artifact


@tool(response_format="content_and_artifact")
async def get_options_chain(
    underlying: str,
    config: RunnableConfig,
    contract_type: Optional[str] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
    limit: int = 20,
) -> Tuple[str, Dict[str, Any]]:
    """
    Options contracts for an underlying with current session pricing, filterable by
    type, expiration range, and strike. US-listed options only — not for non-US
    underlyings.

    Args:
        underlying: Underlying ticker (e.g. "AAPL", "TSLA").
        contract_type: "call" or "put" (default: both).
        expiration_date_gte: Min expiration "YYYY-MM-DD".
        expiration_date_lte: Max expiration "YYYY-MM-DD".
        strike_min: Min strike filter.
        strike_max: Max strike filter.
        limit: Max contracts (default 20).
    """
    content, artifact = await fetch_options_chain(
        underlying, contract_type, expiration_date_gte, expiration_date_lte,
        strike_min, strike_max, limit, config=config,
    )
    return content, artifact


@tool(response_format="content_and_artifact")
async def get_market_movers(
    config: RunnableConfig,
    direction: str = "gainers",
) -> Tuple[str, Dict[str, Any]]:
    """
    Top US market movers — the biggest daily gainers or losers among tickers with
    significant volume. US market only.

    Args:
        direction: "gainers" or "losers" (default "gainers").
    """
    content, artifact = await fetch_market_movers(direction, config=config)
    return content, artifact
