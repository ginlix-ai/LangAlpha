"""YFinance Market MCP Server.

Exposes market-level capabilities: search, screener, calendars,
market status, and sector/industry data.
"""

from typing import Any, Optional

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


def _clean_value(obj):
    """Recursively clean a value for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _clean_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_value(item) for item in obj]
    if hasattr(obj, "isoformat"):
        return _format_datetime(obj)
    if isinstance(obj, float) and (obj != obj):  # NaN
        return None
    return obj


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

mcp = FastMCP("YFinanceMarketMCP")


def _build_equity_query(filter_dict: dict) -> yf.EquityQuery:
    """Recursively build an EquityQuery from a filter dict."""
    operator = filter_dict["operator"].upper()
    operands = filter_dict["operands"]

    # Check if operands contain nested filter dicts (for AND/OR)
    if operands and isinstance(operands[0], dict):
        nested = [_build_equity_query(op) for op in operands]
        return yf.EquityQuery(operator, nested)

    return yf.EquityQuery(operator, operands)


@mcp.tool()
def search_tickers(
    query: str, max_results: int = 8, news_count: int = 5
) -> dict:
    """Search for tickers and related news by keyword.

    Returns {"quotes": [...], "news": [...]} where quotes are dicts with
    a "symbol" key and news are raw article dicts from Yahoo Finance.

    Args:
        query: Search keyword (e.g. "apple", "electric vehicles", "AAPL")
        max_results: Max number of ticker quotes to return (default 8)
        news_count: Max number of news articles to return (default 5)
    """
    try:
        s = yf.Search(query, max_results=max_results, news_count=news_count)
        return _make_response(
            "search_results",
            {
                "quotes": [_clean_value(q) for q in s.quotes],
                "news": [_clean_value(a) for a in s.news],
            },
        )
    except Exception as e:
        return _make_error(f"Search failed: {e}")


@mcp.tool()
def get_market_status(market: str = "US") -> dict:
    """Get current market status and summary for a given market.

    Returns {"status": {open, close, tz, ...}, "summary": {exchange: quote_data, ...}}.
    The status dict contains market open/close times and timezone info.
    The summary dict is keyed by exchange with price/change data per index.

    Valid markets: US, GB, ASIA, EUROPE, RATES, COMMODITIES, CURRENCIES, CRYPTOCURRENCIES
    """
    try:
        m = yf.Market(market)
        return _make_response(
            "market_status",
            _clean_value({"status": m.status, "summary": m.summary}),
            market=market,
        )
    except Exception as e:
        return _make_error(f"Market status failed: {e}")


@mcp.tool()
def screen_stocks(
    filters: list[dict],
    sort_field: str = "percentchange",
    sort_asc: bool = False,
    count: int = 25,
) -> dict:
    """Screen stocks using custom filters.

    Each filter is a dict with 'operator' and 'operands' keys.
    Operators: gt, lt, gte, lte, eq, btwn, is-in, and, or.
    Example: {"operator": "gt", "operands": ["percentchange", 3]}
    Nested: {"operator": "and", "operands": [<filter>, <filter>]}
    Between: {"operator": "btwn", "operands": ["percentchange", 1, 5]}

    Returns a dict with "quotes" (list of matching stocks) and "total" count.

    Args:
        filters: List of filter dicts. Multiple filters are auto-wrapped in AND.
        sort_field: Field to sort results by (default "percentchange")
        sort_asc: Sort ascending if True (default False, descending)
        count: Max results to return (default 25, max 250)
    """
    try:
        if len(filters) == 1:
            query = _build_equity_query(filters[0])
        else:
            query = _build_equity_query(
                {"operator": "AND", "operands": filters}
            )
        result = yf.screen(
            query, sortField=sort_field, sortAsc=sort_asc, size=count
        )
        return _make_response("screen_results", _clean_value(result))
    except Exception as e:
        return _make_error(f"Screen failed: {e}")


@mcp.tool()
def get_predefined_screen(screen_name: str) -> dict:
    """Run a predefined stock screener by name.

    Returns a dict with "quotes" (list of matching stocks) and "total" count.
    Returns an error with available screen names if the name is invalid.

    Available screens: aggressive_small_caps, day_gainers, day_losers,
    growth_technology_stocks, most_actives, most_shorted_stocks,
    small_cap_gainers, undervalued_growth_stocks, undervalued_large_caps,
    conservative_foreign_funds, high_yield_bond, portfolio_anchors,
    solid_large_growth_funds, solid_midcap_growth_funds, top_mutual_funds
    """
    try:
        if screen_name not in yf.PREDEFINED_SCREENER_QUERIES:
            available = list(yf.PREDEFINED_SCREENER_QUERIES.keys())
            return _make_error(
                f"Unknown screen '{screen_name}'. Available: {available}"
            )
        result = yf.screen(screen_name)
        return _make_response(
            "predefined_screen", _clean_value(result), screen_name=screen_name
        )
    except Exception as e:
        return _make_error(f"Predefined screen failed: {e}")


@mcp.tool()
def get_earnings_calendar(start: str, end: str) -> dict:
    """Get earnings calendar for a date range.

    Returns a list of records with keys: symbol, company, marketcap,
    event_start_date (YYYY-MM-DD HH:MM:SS), timing, eps_estimate,
    reported_eps, surprise_pct.

    Args:
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format
    """
    try:
        cal = yf.Calendars(start=start, end=end)
        records = _serialize_records(cal.earnings_calendar)
        return _make_response(
            "earnings_calendar", records, start=start, end=end
        )
    except Exception as e:
        return _make_error(f"Earnings calendar failed: {e}")


@mcp.tool()
def get_sector_info(sector_key: str) -> dict:
    """Get sector overview, top companies, top ETFs, and industries.

    Returns {"overview": dict, "top_companies": [records], "top_etfs": {symbol: name},
    "industries": [records]}. Overview includes companies_count, market_cap,
    market_weight, etc.

    Common sector keys: technology, healthcare, financial-services,
    consumer-cyclical, industrials, communication-services,
    consumer-defensive, energy, basic-materials, real-estate, utilities
    """
    try:
        s = yf.Sector(sector_key)
        return _make_response(
            "sector_info",
            {
                "overview": _clean_value(s.overview),
                "top_companies": _serialize_records(s.top_companies),
                "top_etfs": _clean_value(s.top_etfs),
                "industries": _serialize_records(s.industries),
            },
            sector=sector_key,
        )
    except Exception as e:
        return _make_error(f"Sector info failed: {e}")


@mcp.tool()
def get_industry_info(industry_key: str) -> dict:
    """Get industry overview, top performing and top growth companies.

    Returns {"overview": dict, "top_performing_companies": [records],
    "top_growth_companies": [records], "sector_key": str, "sector_name": str}.
    Top performing records include name, ytd_return, last_price, target_price.
    Top growth records include name, ytd_return, growth_estimate.
    """
    try:
        i = yf.Industry(industry_key)
        return _make_response(
            "industry_info",
            {
                "overview": _clean_value(i.overview),
                "top_performing_companies": _serialize_records(
                    i.top_performing_companies
                ),
                "top_growth_companies": _serialize_records(
                    i.top_growth_companies
                ),
                "sector_key": i.sector_key,
                "sector_name": i.sector_name,
            },
            industry=industry_key,
        )
    except Exception as e:
        return _make_error(f"Industry info failed: {e}")


if __name__ == "__main__":
    mcp.run(transport="stdio")
