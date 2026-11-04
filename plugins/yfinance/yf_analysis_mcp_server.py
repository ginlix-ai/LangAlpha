"""YFinance Analysis MCP Server.

Analyst data, holdings, insider activity, news, ESG, and estimate tools,
wrapped in the standard market-data envelope (see
mcp_servers/AGENT_CONTRACT.md).
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

import math
from datetime import datetime, timezone

import yfinance as yf

from mcp_servers._envelope import make_error, make_response
from mcp_servers._schemas import OBJECT, RECORDS, envelope_schema, output_model
from mcp_servers._yf_common import boundary, clean_value, safe_detail, serialize_records

SOURCE = "yfinance"


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = MCPServer("YFinanceAnalysisMCP")


_OUT_GET_ANALYST_RECOMMENDATIONS = output_model(
    "GetAnalystRecommendationsOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_analyst_recommendations(ticker: str) -> _OUT_GET_ANALYST_RECOMMENDATIONS:
    """Aggregated analyst recommendation counts by period. Use to gauge the
    buy/hold/sell balance over recent periods.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {period, strongbuy, buy, hold, sell, strongsell} in Yahoo's
        order (period "0m" current, "-1m", "-2m", "-3m"). Empty data means none
        published. On error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).recommendations)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Analyst recommendations", e),
            symbol=symbol,
        )


_OUT_GET_SUSTAINABILITY_DATA = output_model(
    "GetSustainabilityDataOut", envelope_schema(OBJECT, frame=("symbol",))
)


@mcp.tool()
def get_sustainability_data(ticker: str) -> _OUT_GET_SUSTAINABILITY_DATA:
    """ESG / sustainability scores for a company. Use for ESG risk context.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a single Yahoo-native ESG
        record mapping metric name → value (e.g. totalEsg, environmentScore,
        socialScore, governanceScore); missing metrics are null. count is 1 when
        data exists, else 0 (empty {} when Yahoo has no ESG coverage). On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        sus = yf.Ticker(yf_symbol).sustainability
        if sus is None or sus.empty:
            return make_response({}, source=SOURCE, symbol=symbol, count=0)
        data = {str(k): v for k, v in sus.iloc[:, 0].items()}
        data = {
            k: (None if isinstance(v, float) and math.isnan(v) else v)
            for k, v in data.items()
        }
        return make_response(data, source=SOURCE, symbol=symbol, count=1)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Sustainability data", e), symbol=symbol
        )


_OUT_GET_INSTITUTIONAL_HOLDERS = output_model(
    "GetInstitutionalHoldersOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_institutional_holders(ticker: str) -> _OUT_GET_INSTITUTIONAL_HOLDERS:
    """Top institutional holders of a stock. Use to see which institutions hold
    the largest positions.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {date_reported, holder, shares, value, pctheld} in Yahoo's order
        (largest holders first); date_reported is "YYYY-MM-DD". Empty data means
        none reported. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).institutional_holders)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Institutional holders", e),
            symbol=symbol,
        )


_OUT_GET_MUTUALFUND_HOLDERS = output_model(
    "GetMutualfundHoldersOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_mutualfund_holders(ticker: str) -> _OUT_GET_MUTUALFUND_HOLDERS:
    """Top mutual-fund holders of a stock. Use to see the largest fund
    positions.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {date_reported, holder, shares, value, pctheld} in Yahoo's order
        (largest first); date_reported is "YYYY-MM-DD". Empty data means none
        reported. On error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).mutualfund_holders)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Mutual fund holders", e),
            symbol=symbol,
        )


_OUT_GET_INSIDER_TRANSACTIONS = output_model(
    "GetInsiderTransactionsOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_insider_transactions(ticker: str) -> _OUT_GET_INSIDER_TRANSACTIONS:
    """Recent insider buy/sell transactions. Use to track insider activity.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {start_date, insider, position, url, transaction, text, shares,
        value, ownership} in Yahoo's order (most recent first); start_date is
        "YYYY-MM-DD". Empty data means none reported. On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).insider_transactions)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Insider transactions", e),
            symbol=symbol,
        )


_OUT_GET_INSIDER_ROSTER = output_model(
    "GetInsiderRosterOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_insider_roster(ticker: str) -> _OUT_GET_INSIDER_ROSTER:
    """Current insiders and their holdings. Use to see who the insiders are and
    their share positions.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {name, position, url, most_recent_transaction,
        latest_transaction_date, shares_owned_directly,
        shares_owned_indirectly, ...} in Yahoo's order; date fields are
        "YYYY-MM-DD". Empty data means none reported. On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).insider_roster_holders)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Insider roster", e), symbol=symbol
        )


def _search_news(yf_symbol: str, count: int) -> list[dict]:
    """News from Yahoo's search endpoint, in the shape ``get_news`` returns.

    yfinance reads news from an endpoint Yahoo has stopped serving (it answers
    404, which yfinance 1.x raises as ``YFDataException``; older releases
    returned an empty list), while search still carries headlines. Each item
    is reshaped into the ``content`` layout the primary endpoint used, so
    callers see one format. Search cannot filter by tab.
    """
    try:
        items = yf.Search(yf_symbol, news_count=count, max_results=0).news
    except Exception:  # noqa: BLE001 - the caller decides what no result means
        return []
    articles = []
    for item in items or []:
        published = item.get("providerPublishTime")
        articles.append(
            {
                "id": item.get("uuid"),
                "relatedTickers": item.get("relatedTickers"),
                "content": {
                    "title": item.get("title"),
                    "contentType": item.get("type"),
                    "pubDate": datetime.fromtimestamp(published, timezone.utc).isoformat()
                    if isinstance(published, (int, float))
                    else None,
                    "provider": {"displayName": item.get("publisher")},
                    "canonicalUrl": {"url": item.get("link")},
                },
            }
        )
    return articles


def _fetch_news(yf_symbol: str, count: int, tab: str) -> list[dict]:
    """Ticker news, from search when the news endpoint is down or empty.

    Press releases and unknown tabs have no search equivalent, so their errors
    stand; an outage that search cannot cover also re-raises rather than
    reading as no articles.
    """
    ticker = yf.Ticker(yf_symbol)
    if tab.lower() not in ("news", "all"):
        return ticker.get_news(count=count, tab=tab)
    try:
        articles = ticker.get_news(count=count, tab=tab)
    except Exception:  # noqa: BLE001
        articles = _search_news(yf_symbol, count)
        if not articles:
            raise
        return articles
    return articles or _search_news(yf_symbol, count)


_OUT_GET_NEWS = output_model(
    "GetNewsOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_news(ticker: str, count: int = 10, tab: str = "news") -> _OUT_GET_NEWS:
    """Latest news articles for a stock. Use to pull recent headlines and
    coverage.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".
        count: Number of articles (default 10).
        tab: One of "news", "all", or "press releases".

    Returns:
        dict: {symbol, count, data, source}. data is a list of raw Yahoo-native
        article dicts in Yahoo's order (most recent first); each has nested
        `content` with title, publisher, url, and publish-date fields (exact
        shape is Yahoo-native and varies), and may carry relatedTickers; check
        it before treating an item as about this ticker. Empty data means no
        articles. On error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        articles = _fetch_news(yf_symbol, count, tab)
        data = [clean_value(item) for item in articles or []]
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error("upstream_error", safe_detail("News", e), symbol=symbol)


_OUT_GET_ANALYST_PRICE_TARGETS = output_model(
    "GetAnalystPriceTargetsOut", envelope_schema(OBJECT, frame=("symbol",))
)


@mcp.tool()
def get_analyst_price_targets(ticker: str) -> _OUT_GET_ANALYST_PRICE_TARGETS:
    """Analyst price-target summary for a stock. Use for consensus target vs.
    current price.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a single Yahoo-native
        record {current, low, high, mean, median}; price values are as reported
        by Yahoo (minor units on some venues, e.g. LSE pence) and currency is
        omitted — no declared-currency source on this path. count is 1 when data
        exists, else 0 (empty {} when unavailable). On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        targets = yf.Ticker(yf_symbol).analyst_price_targets
        if not targets:
            return make_response({}, source=SOURCE, symbol=symbol, count=0)
        return make_response(
            clean_value(targets),
            source=SOURCE,
            symbol=symbol,
            count=1,
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Analyst price targets", e),
            symbol=symbol,
        )


_OUT_GET_UPGRADES_DOWNGRADES = output_model(
    "GetUpgradesDowngradesOut",
    envelope_schema(RECORDS, frame=("symbol", "timezone")),
)


@mcp.tool()
def get_upgrades_downgrades(ticker: str) -> _OUT_GET_UPGRADES_DOWNGRADES:
    """History of analyst rating changes (upgrades/downgrades). Use to track
    how the sell-side rating evolved.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, timezone, count, data, source}. data is a list of
        Yahoo-native records {gradedate, firm, tograde, fromgrade, action} in
        Yahoo's order (most recent first); gradedate is "YYYY-MM-DD HH:MM:SS"
        in `timezone`. Empty data means none reported. On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        grades = yf.Ticker(yf_symbol).upgrades_downgrades
        return make_response(
            serialize_records(grades), source=SOURCE, symbol=symbol, timezone="UTC"
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Upgrades/downgrades", e),
            symbol=symbol,
        )


_OUT_GET_EARNINGS_HISTORY = output_model(
    "GetEarningsHistoryOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_earnings_history(ticker: str) -> _OUT_GET_EARNINGS_HISTORY:
    """Historical EPS estimate vs. actual with surprise. Use to review recent
    earnings beats/misses. Same data as get_earnings_data (fundamentals server).

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {quarter, epsestimate, epsactual, epsdifference, surprisepercent}
        in Yahoo's order (oldest first); quarter is "YYYY-MM-DD". Empty data
        means none reported. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).earnings_history)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Earnings history", e), symbol=symbol
        )


def _flag_stale_periods(
    records: list[dict],
    ends: dict[str, str],
    reported: str | None,
    stale_keys: tuple[str, ...],
) -> list[dict]:
    """Add each period's end date and null the year-ago figures Yahoo left behind.

    Right after a company reports, Yahoo moves period labels and estimates
    forward but not the year-ago actuals: AAPL's next quarter (Dec-2026) was
    set against Sep-2025, a +57% growth that is really +2%. A period already
    reported, or sharing its end with another quarter or another year, still
    carries the old basis.
    """
    for rec in records:
        period = rec.get("period")
        end = ends.get(period)
        if not end:
            continue
        rec["period_end"] = end
        kind = period[-1:]
        shares_end = any(
            other != period and other[-1:] == kind and other_end == end
            for other, other_end in ends.items()
        )
        if (reported and end <= reported) or shares_end:
            rec.update({key: None for key in stale_keys})
            rec["year_ago_stale"] = True
    return records


def _annotate_periods(
    stock: yf.Ticker, records: list[dict], stale_keys: tuple[str, ...]
) -> list[dict]:
    trend = getattr(getattr(stock, "_analysis", None), "_earnings_trend", None) or []
    ends = {item["period"]: item["endDate"] for item in trend if item.get("endDate")}
    try:
        history = stock.earnings_history
        reported = str(history.index.max())[:10] if not history.empty else None
    except Exception:  # noqa: BLE001 - the end dates alone still help
        reported = None
    return _flag_stale_periods(records, ends, reported, stale_keys)


_OUT_GET_EARNINGS_ESTIMATES = output_model(
    "GetEarningsEstimatesOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_earnings_estimates(ticker: str) -> _OUT_GET_EARNINGS_ESTIMATES:
    """Forward EPS estimates by period.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y), period_end?,
        numberofanalysts, avg, low, high, yearagoeps, growth,
        year_ago_stale?} in Yahoo's period order; growth is a fraction (0.07
        = 7%). year_ago_stale: true when the year-ago basis is out of date;
        yearagoeps and growth are then null and period_end may still name
        the period just reported. Empty data means none published. On
        error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        data = _annotate_periods(
            stock, serialize_records(stock.earnings_estimate), ("yearagoeps", "growth")
        )
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Earnings estimates", e), symbol=symbol
        )


_OUT_GET_REVENUE_ESTIMATES = output_model(
    "GetRevenueEstimatesOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_revenue_estimates(ticker: str) -> _OUT_GET_REVENUE_ESTIMATES:
    """Forward revenue estimates by period. Use for consensus revenue for coming
    quarters and years.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y), period_end?,
        numberofanalysts, avg, low, high, yearagorevenue, growth,
        year_ago_stale?} in Yahoo's period order; growth is a fraction (0.07
        = 7%). year_ago_stale: true when the year-ago basis is out of date;
        yearagorevenue and growth are then null and period_end may still
        name the period just reported. Empty data means none published. On
        error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        data = _annotate_periods(
            stock,
            serialize_records(stock.revenue_estimate),
            ("yearagorevenue", "growth"),
        )
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Revenue estimates", e), symbol=symbol
        )


_OUT_GET_GROWTH_ESTIMATES = output_model(
    "GetGrowthEstimatesOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_growth_estimates(ticker: str) -> _OUT_GET_GROWTH_ESTIMATES:
    """Growth estimates for the stock vs. the index.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y, LTG), period_end?,
        stocktrend, indextrend, year_ago_stale?} in Yahoo's period order; the
        trends are fractions (0.07 = 7%). year_ago_stale: true when the
        year-ago basis is out of date; stocktrend is then null and period_end
        may still name the period just reported. Empty data means none
        published. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        data = _annotate_periods(
            stock, serialize_records(stock.growth_estimates), ("stocktrend",)
        )
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Growth estimates", e), symbol=symbol
        )


_OUT_GET_MAJOR_HOLDERS = output_model(
    "GetMajorHoldersOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_major_holders(ticker: str) -> _OUT_GET_MAJOR_HOLDERS:
    """Ownership breakdown (insider %, institution %, etc.). Use for a
    high-level ownership summary.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {breakdown, value}: value is a fraction (0.66 = 66%) for
        *PercentHeld rows and a count for *Count rows. Empty data means
        unavailable. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        holders = yf.Ticker(yf_symbol).major_holders
        if holders is not None:
            holders = holders.rename_axis("breakdown")
        data = serialize_records(holders)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Major holders", e), symbol=symbol
        )


if __name__ == "__main__":
    mcp.run(transport="stdio")
