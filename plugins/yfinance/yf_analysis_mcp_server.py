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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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


_OUT_GET_NEWS = output_model(
    "GetNewsOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_news(ticker: str, count: int = 10, tab: str = "news") -> _OUT_GET_NEWS:
    """Latest news articles for a stock. Use to pull recent headlines and
    coverage.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".
        count: Number of articles (default 10).
        tab: One of "news", "all", or "press releases".

    Returns:
        dict: {symbol, count, data, source}. data is a list of raw Yahoo-native
        article dicts in Yahoo's order (most recent first); each has nested
        `content` with title, publisher, url, and publish-date fields (exact
        shape is Yahoo-native and varies). Empty data means no articles. On
        error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        articles = yf.Ticker(yf_symbol).get_news(count=count, tab=tab)
        if not articles:
            return make_response([], source=SOURCE, symbol=symbol)
        data = [clean_value(item) for item in articles]
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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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
    "GetUpgradesDowngradesOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_upgrades_downgrades(ticker: str) -> _OUT_GET_UPGRADES_DOWNGRADES:
    """History of analyst rating changes (upgrades/downgrades). Use to track
    how the sell-side rating evolved.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {gradedate, firm, tograde, fromgrade, action} in Yahoo's order
        (most recent first); gradedate is "YYYY-MM-DD". Empty data means none
        reported. On error: {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).upgrades_downgrades)
        return make_response(data, source=SOURCE, symbol=symbol)
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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

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


_OUT_GET_EARNINGS_ESTIMATES = output_model(
    "GetEarningsEstimatesOut", envelope_schema(RECORDS, frame=("symbol",))
)


@mcp.tool()
def get_earnings_estimates(ticker: str) -> _OUT_GET_EARNINGS_ESTIMATES:
    """Forward EPS estimates by period. Use for consensus EPS for coming
    quarters and years.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y), numberofanalysts,
        avg, low, high, yearagoeps, growth} in Yahoo's period order. Empty data
        means none published. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).earnings_estimate)
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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y), numberofanalysts,
        avg, low, high, yearagorevenue, growth} in Yahoo's period order. Empty
        data means none published. On error: {error, detail, symbol} with error
        upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).revenue_estimate)
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
    """Growth estimates for the stock vs. industry, sector, and index. Use to
    compare a name's growth outlook to its peers.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records, one per period {period (0q, +1q, 0y, +1y, +5y, -5y), stock,
        industry, sector, index}, each a growth rate, in Yahoo's period order.
        Empty data means none published. On error: {error, detail, symbol} with
        error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).growth_estimates)
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
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SS".

    Returns:
        dict: {symbol, count, data, source}. data is a list of Yahoo-native
        records {breakdown, value} where breakdown is the label and value the
        percentage or count. Empty data means unavailable. On error:
        {error, detail, symbol} with error upstream_error.
    """
    symbol, yf_symbol, _ = boundary(ticker)
    try:
        data = serialize_records(yf.Ticker(yf_symbol).major_holders)
        return make_response(data, source=SOURCE, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Major holders", e), symbol=symbol
        )


if __name__ == "__main__":
    mcp.run(transport="stdio")
