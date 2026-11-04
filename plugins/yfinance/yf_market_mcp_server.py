"""YFinance Market MCP Server.

Market-level tools — search, screener, calendars, market status, and
sector/industry data — wrapped in the standard market-data envelope
(see mcp_servers/AGENT_CONTRACT.md).
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    from _bootstrap import MCPServer  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._bootstrap import MCPServer

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from mcp_servers._envelope import make_error, make_response
from mcp_servers._schemas import (
    BOOL,
    OBJECT,
    RECORDS,
    STR,
    envelope_schema,
    output_model,
)
from mcp_servers._yf_common import clean_value, safe_detail, serialize_records

SOURCE = "yfinance"

# Yahoo caps a calendar page at 100 rows, and the calendar covers US listings.
_CALENDAR_ROWS = 100
_CALENDAR_ZONE = "America/New_York"

# A quote whose last regular-session trade is older than the newest one in the
# result by more than this did not trade in the session the rest are ranked on.
# Sessions on different continents sit well inside it.
_STALE_AFTER_SECONDS = 18 * 3600


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = MCPServer("YFinanceMarketMCP")


def _build_equity_query(filter_dict: dict) -> yf.EquityQuery:
    """Recursively build an EquityQuery from a filter dict."""
    operator = filter_dict["operator"].upper()
    operands = filter_dict["operands"]

    # Nested filter dicts (for AND/OR) vs. leaf operands.
    if operands and isinstance(operands[0], dict):
        nested = [_build_equity_query(op) for op in operands]
        return yf.EquityQuery(operator, nested)

    return yf.EquityQuery(operator, operands)


_OUT_SEARCH_TICKERS = output_model("SearchTickersOut", envelope_schema(OBJECT))


@mcp.tool()
def search_tickers(query: str, max_results: int = 8, news_count: int = 5) -> _OUT_SEARCH_TICKERS:
    """Look up tickers and related news by free-text keyword. Use to resolve a
    company name to symbols or scan headlines.

    Args:
        query: Keyword — company name or symbol, e.g. "apple", "AAPL".
        max_results: Max ticker quotes to return (default 8).
        news_count: Max news articles to return (default 5).

    Returns:
        dict: {count, data, source}. data is {quotes: [...], news: [...]}, both
        Yahoo-native and in Yahoo's relevance order; quote dicts carry a
        `symbol` key plus name/exchange fields, news dicts are raw Yahoo article
        records. count is quotes + news. On error: {error, detail} with error
        upstream_error.
    """
    try:
        s = yf.Search(query, max_results=max_results, news_count=news_count)
        return make_response(
            {
                "quotes": [clean_value(q) for q in s.quotes],
                "news": [clean_value(a) for a in s.news],
            },
            source=SOURCE,
        )
    except Exception as e:  # noqa: BLE001
        return make_error("upstream_error", safe_detail("Ticker search", e))


_OUT_GET_MARKET_STATUS = output_model(
    "GetMarketStatusOut",
    envelope_schema(OBJECT, frame=("timezone",), echo={"market": STR}),
)


@mcp.tool()
def get_market_status(market: str = "US") -> _OUT_GET_MARKET_STATUS:
    """Current status and index summary for a market. Use to check whether a
    market is open and read headline index moves.
    Markets: US, GB, ASIA, EUROPE, RATES, COMMODITIES, CURRENCIES,
    CRYPTOCURRENCIES.

    Args:
        market: Market code (see above; default "US").

    Returns:
        dict: {market, timezone, count, data, source}. data is
        {status, summary}, both Yahoo-native: status holds open/close times
        ("YYYY-MM-DD HH:MM:SS" in `timezone`) and timezone fields, summary is
        keyed by exchange/index with price and change fields. count is 1. On
        error: {error, detail} with error upstream_error.
    """
    try:
        m = yf.Market(market)
        status = m.status
        zone = None
        if isinstance(status, dict):
            status = dict(status)
            zone = (status.get("timezone") or {}).get("$text")
            if zone:
                for key in ("open", "close"):
                    if isinstance(status.get(key), datetime):
                        status[key] = status[key].astimezone(ZoneInfo(zone))
        return make_response(
            clean_value({"status": status, "summary": m.summary}),
            source=SOURCE,
            timezone=zone or "UTC",
            market=market,
            count=1,
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Market status", e), market=market
        )


_OUT_SCREEN_STOCKS = output_model("ScreenStocksOut", envelope_schema(OBJECT))


@mcp.tool()
def screen_stocks(
    filters: list[dict],
    sort_field: str = "percentchange",
    sort_asc: bool = False,
    count: int = 25,
) -> _OUT_SCREEN_STOCKS:
    """Screen equities with Yahoo filters on numeric/categorical criteria.
    Each filter is {operator, operands}; operators: gt, lt, gte, lte, eq, btwn,
    is-in, and, or (nest with and/or; top-level filters are AND-ed). E.g.
    {"operator": "gt", "operands": ["percentchange", 3]}.

    Args:
        filters: List of filter dicts (see above).
        sort_field: Field to sort by (default "percentchange").
        sort_asc: Ascending if True (default False).
        count: Max rows (default 25, max 250).

    Returns:
        dict: {count, data, source}. data is the Yahoo-native screen result
        {quotes: [...], ...}; quotes are raw Yahoo quote dicts ordered by
        sort_field, plus stale_price? (true: last trade over 18h before the
        result's newest quote). On error: {error, detail} —
        invalid_argument | upstream_error.
    """
    try:
        if len(filters) == 1:
            query = _build_equity_query(filters[0])
        else:
            query = _build_equity_query({"operator": "AND", "operands": filters})
        result = clean_value(
            yf.screen(query, sortField=sort_field, sortAsc=sort_asc, size=count)
        )
        quotes = result.get("quotes", []) if isinstance(result, dict) else []
        traded = [
            q["regularMarketTime"]
            for q in quotes
            if isinstance(q.get("regularMarketTime"), (int, float))
        ]
        newest = max(traded, default=None)
        for q in quotes:
            last = q.get("regularMarketTime")
            if (
                newest is not None
                and isinstance(last, (int, float))
                and newest - last > _STALE_AFTER_SECONDS
            ):
                q["stale_price"] = True
        return make_response(result, source=SOURCE, count=len(quotes))
    except (ValueError, TypeError) as e:
        # Local EquityQuery validation — names the bad field/operator and
        # carries no URLs, unlike yfinance I/O errors (requests exceptions),
        # which stay in the sanitized branch below.
        return make_error("invalid_argument", f"Invalid screen query: {str(e)[:300]}")
    except Exception as e:  # noqa: BLE001
        return make_error("upstream_error", safe_detail("Stock screen", e))


_OUT_GET_PREDEFINED_SCREEN = output_model(
    "GetPredefinedScreenOut",
    envelope_schema(OBJECT, echo={"screen_name": STR}),
)


@mcp.tool()
def get_predefined_screen(screen_name: str) -> _OUT_GET_PREDEFINED_SCREEN:
    """Run a named Yahoo predefined screener. Use for common ready-made scans
    (day_gainers, day_losers, most_actives, most_shorted_stocks,
    undervalued_growth_stocks, growth_technology_stocks, ...); an unknown name
    returns the full list of valid names in `supported`.

    Args:
        screen_name: A Yahoo predefined screener name (see above).

    Returns:
        dict: {screen_name, count, data, source}. data is the Yahoo-native
        screen result {quotes: [...], ...}; quotes are raw Yahoo quote dicts,
        count is the number returned. On an unknown name:
        {error: invalid_argument, detail, supported}. On upstream failure:
        {error: upstream_error, detail}.
    """
    try:
        if screen_name not in yf.PREDEFINED_SCREENER_QUERIES:
            return make_error(
                "invalid_argument",
                f"Unknown predefined screen '{screen_name}'.",
                supported=list(yf.PREDEFINED_SCREENER_QUERIES.keys()),
            )
        result = clean_value(yf.screen(screen_name))
        n = len(result.get("quotes", [])) if isinstance(result, dict) else 0
        return make_response(
            result, source=SOURCE, screen_name=screen_name, count=n
        )
    except Exception as e:  # noqa: BLE001
        return make_error("upstream_error", safe_detail("Predefined screen", e))


_OUT_GET_EARNINGS_CALENDAR = output_model(
    "GetEarningsCalendarOut",
    envelope_schema(
        RECORDS, frame=("timezone",), echo={"start": STR, "end": STR, "truncated": BOOL}
    ),
)


@mcp.tool()
def get_earnings_calendar(start: str, end: str) -> _OUT_GET_EARNINGS_CALENDAR:
    """Earnings announcements scheduled within a date range. Use to find which
    companies report in a window.
    US listings only; at most 100 rows.

    Args:
        start: Start date "YYYY-MM-DD".
        end: End date "YYYY-MM-DD", inclusive.

    Returns:
        dict: {start, end, timezone, count, data, source, truncated?}. data is
        a list of records {symbol, company (may be null), marketcap,
        event_start_date, timing, eps_estimate, reported_eps, surprise_pct},
        largest marketcap first; event_start_date is "YYYY-MM-DD HH:MM:SS" in
        `timezone`. Field names are Yahoo-native. truncated: true if a size
        cap cut the smallest-cap rows. On error: {error, detail} —
        invalid_argument | upstream_error.
    """
    try:
        first = date.fromisoformat(start)
        after = date.fromisoformat(end) + timedelta(days=1)
    except ValueError:
        return make_error(
            "invalid_argument",
            "start and end must be YYYY-MM-DD dates.",
            start=start,
            end=end,
        )
    try:
        # Yahoo reads end as midnight, so the last day needs the day after;
        # the default query is also the 12 most active names, newest day first.
        cal = yf.Calendars(start=first.isoformat(), end=after.isoformat())
        rows = cal.get_earnings_calendar(
            filter_most_active=False, limit=_CALENDAR_ROWS, force=True
        )
        cut = {"truncated": True} if len(rows) >= _CALENDAR_ROWS else {}
        if rows.empty:
            return make_response(
                [], source=SOURCE, timezone=_CALENDAR_ZONE, start=start, end=end
            )
        rows = rows[rows["Event Start Date"] < pd.Timestamp(after, tz="UTC")]
        rows = rows.assign(
            **{"Event Start Date": rows["Event Start Date"].dt.tz_convert(_CALENDAR_ZONE)}
        ).sort_values("Marketcap", ascending=False)
        return make_response(
            serialize_records(rows),
            source=SOURCE,
            timezone=_CALENDAR_ZONE,
            start=start,
            end=end,
            **cut,
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Earnings calendar", e),
            start=start,
            end=end,
        )


_OUT_GET_SECTOR_INFO = output_model(
    "GetSectorInfoOut",
    envelope_schema(OBJECT, echo={"sector": STR}),
)


@mcp.tool()
def get_sector_info(sector_key: str) -> _OUT_GET_SECTOR_INFO:
    """Overview of a market sector — top companies, ETFs, and its industries.
    Use for sector-level context and constituents.
    Sector keys: technology, healthcare, financial-services, consumer-cyclical,
    industrials, communication-services, consumer-defensive, energy,
    basic-materials, real-estate, utilities.

    Args:
        sector_key: One of the keys above.

    Returns:
        dict: {sector, count, data, source}. data is {overview, top_companies,
        top_etfs, industries}: overview and top_etfs are Yahoo-native dicts,
        top_companies and industries are lists of Yahoo-native records. count is
        top_companies + industries. On error: {error, detail} with error
        upstream_error.
    """
    try:
        s = yf.Sector(sector_key)
        top_companies = serialize_records(s.top_companies)
        industries = serialize_records(s.industries)
        return make_response(
            {
                "overview": clean_value(s.overview),
                "top_companies": top_companies,
                "top_etfs": clean_value(s.top_etfs),
                "industries": industries,
            },
            source=SOURCE,
            sector=sector_key,
            count=len(top_companies) + len(industries),
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Sector info", e), sector=sector_key
        )


_OUT_GET_INDUSTRY_INFO = output_model(
    "GetIndustryInfoOut",
    envelope_schema(OBJECT, echo={"industry": STR}),
)


@mcp.tool()
def get_industry_info(industry_key: str) -> _OUT_GET_INDUSTRY_INFO:
    """Overview of an industry — top performing and top growth companies plus
    its parent sector. Use for industry-level context and constituents.

    Args:
        industry_key: Yahoo industry key, e.g. "software-infrastructure".

    Returns:
        dict: {industry, count, data, source}. data is {overview,
        top_performing_companies, top_growth_companies, sector_key,
        sector_name}: overview is a Yahoo-native dict, the two company lists are
        Yahoo-native records (name, ytd_return, last_price/target_price or
        growth_estimate). count is top_performing + top_growth. On error:
        {error, detail} with error upstream_error.
    """
    try:
        i = yf.Industry(industry_key)
        top_performing = serialize_records(i.top_performing_companies)
        top_growth = serialize_records(i.top_growth_companies)
        return make_response(
            {
                "overview": clean_value(i.overview),
                "top_performing_companies": top_performing,
                "top_growth_companies": top_growth,
                "sector_key": i.sector_key,
                "sector_name": i.sector_name,
            },
            source=SOURCE,
            industry=industry_key,
            count=len(top_performing) + len(top_growth),
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Industry info", e),
            industry=industry_key,
        )


if __name__ == "__main__":
    mcp.run(transport="stdio")
