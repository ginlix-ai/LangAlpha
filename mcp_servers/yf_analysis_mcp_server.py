"""YFinance Analysis MCP Server.

Provides analyst data, holdings, insider activity, news, and ESG tools.
"""

import math
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

mcp = FastMCP("YFinanceAnalysisMCP")


@mcp.tool()
def get_analyst_recommendations(ticker: str) -> dict:
    """Get analyst recommendations for a stock.

    Returns a list of records with keys: period, strongbuy, buy, hold,
    sell, strongsell — aggregated recommendation counts by period.
    """
    try:
        stock = yf.Ticker(ticker)
        recs = stock.recommendations
        data = _serialize_records(recs)
        return _make_response("analyst_recommendations", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get analyst recommendations for {ticker}: {e}")


@mcp.tool()
def get_sustainability_data(ticker: str) -> dict:
    """Get ESG/sustainability scores and metrics for a stock.

    Returns a dict of ESG metric names to values. Returns empty dict
    if no sustainability data is available for this ticker.
    """
    try:
        stock = yf.Ticker(ticker)
        sus = stock.sustainability
        if sus is None or sus.empty:
            return _make_response("sustainability", {}, ticker=ticker)
        data = {str(k): v for k, v in sus.iloc[:, 0].items()}
        # Clean NaN values
        data = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in data.items()}
        return _make_response("sustainability", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get sustainability data for {ticker}: {e}")


@mcp.tool()
def get_institutional_holders(ticker: str) -> dict:
    """Get institutional holders for a stock.

    Returns a list of records with keys: date_reported (YYYY-MM-DD),
    holder, shares, value, pctheld.
    """
    try:
        stock = yf.Ticker(ticker)
        holders = stock.institutional_holders
        data = _serialize_records(holders)
        return _make_response("institutional_holders", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get institutional holders for {ticker}: {e}")


@mcp.tool()
def get_mutualfund_holders(ticker: str) -> dict:
    """Get mutual fund holders for a stock.

    Returns a list of records with keys: date_reported (YYYY-MM-DD),
    holder, shares, value.
    """
    try:
        stock = yf.Ticker(ticker)
        holders = stock.mutualfund_holders
        data = _serialize_records(holders)
        return _make_response("mutualfund_holders", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get mutual fund holders for {ticker}: {e}")


@mcp.tool()
def get_insider_transactions(ticker: str) -> dict:
    """Get insider transactions for a stock.

    Returns a list of records with keys: start_date (YYYY-MM-DD),
    insider, position, url, transaction, text, shares, value, ownership.
    """
    try:
        stock = yf.Ticker(ticker)
        txns = stock.insider_transactions
        data = _serialize_records(txns)
        return _make_response("insider_transactions", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get insider transactions for {ticker}: {e}")


@mcp.tool()
def get_insider_roster(ticker: str) -> dict:
    """Get insider roster (list of insiders with their positions and holdings).

    Returns a list of records with keys: name, position, url,
    most_recent_transaction, latest_transaction_date,
    shares_owned_directly, shares_owned_indirectly, etc.
    """
    try:
        stock = yf.Ticker(ticker)
        roster = stock.insider_roster_holders
        data = _serialize_records(roster)
        return _make_response("insider_roster", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get insider roster for {ticker}: {e}")


@mcp.tool()
def get_news(ticker: str, count: int = 10, tab: str = "news") -> dict:
    """Get latest news articles for a stock.

    Returns a list of article dicts from Yahoo Finance. Article structure
    varies but typically includes nested content with title, publisher,
    url, and publish date fields.

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL", "MSFT")
        count: Number of articles to return (default 10)
        tab: News tab — "news", "all", or "press releases"
    """
    try:
        stock = yf.Ticker(ticker)
        articles = stock.get_news(count=count, tab=tab)
        if not articles:
            return _make_response("news", [], ticker=ticker)

        # Light cleanup: convert datetime objects and NaN values
        cleaned = []
        for item in articles:
            cleaned.append(_clean_value(item))
        return _make_response("news", cleaned, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get news for {ticker}: {e}")


@mcp.tool()
def get_analyst_price_targets(ticker: str) -> dict:
    """Get analyst price target summary for a stock.

    Returns a dict with keys: current, low, high, mean, median — all
    numeric price values. Returns empty dict if no targets available.
    """
    try:
        stock = yf.Ticker(ticker)
        targets = stock.analyst_price_targets
        if not targets:
            return _make_response("analyst_price_targets", {}, ticker=ticker)
        return _make_response("analyst_price_targets", _clean_value(targets), ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get analyst price targets for {ticker}: {e}")


@mcp.tool()
def get_upgrades_downgrades(ticker: str) -> dict:
    """Get history of analyst upgrades and downgrades for a stock.

    Returns a list of records with keys: gradedate (YYYY-MM-DD),
    firm, tograde, fromgrade, action.
    """
    try:
        stock = yf.Ticker(ticker)
        ud = stock.upgrades_downgrades
        data = _serialize_records(ud)
        return _make_response("upgrades_downgrades", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get upgrades/downgrades for {ticker}: {e}")


@mcp.tool()
def get_earnings_history(ticker: str) -> dict:
    """Get historical earnings per share (estimate vs actual, surprise %).

    Returns a list of records with keys: quarter (YYYY-MM-DD), epsestimate,
    epsactual, epsdifference, surprisepercent. Same underlying data as
    get_earnings_data in the fundamentals server.
    """
    try:
        stock = yf.Ticker(ticker)
        eh = stock.earnings_history
        data = _serialize_records(eh)
        return _make_response("earnings_history", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get earnings history for {ticker}: {e}")


@mcp.tool()
def get_earnings_estimates(ticker: str) -> dict:
    """Get earnings estimates for upcoming quarters and years.

    Returns a list of records indexed by period (0q, +1q, 0y, +1y) with
    keys: numberofanalysts, avg, low, high, yearagoeps, growth.
    """
    try:
        stock = yf.Ticker(ticker)
        ee = stock.earnings_estimate
        data = _serialize_records(ee)
        return _make_response("earnings_estimates", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get earnings estimates for {ticker}: {e}")


@mcp.tool()
def get_revenue_estimates(ticker: str) -> dict:
    """Get revenue estimates for upcoming quarters and years.

    Returns a list of records indexed by period (0q, +1q, 0y, +1y) with
    keys: numberofanalysts, avg, low, high, yearagorevenue, growth.
    """
    try:
        stock = yf.Ticker(ticker)
        re_ = stock.revenue_estimate
        data = _serialize_records(re_)
        return _make_response("revenue_estimates", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get revenue estimates for {ticker}: {e}")


@mcp.tool()
def get_growth_estimates(ticker: str) -> dict:
    """Get growth estimates comparing stock vs industry, sector, and index.

    Returns a list of records indexed by period (0q, +1q, 0y, +1y, +5y, -5y)
    with keys: stock, industry, sector, index — each a growth rate.
    """
    try:
        stock = yf.Ticker(ticker)
        ge = stock.growth_estimates
        data = _serialize_records(ge)
        return _make_response("growth_estimates", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get growth estimates for {ticker}: {e}")


@mcp.tool()
def get_major_holders(ticker: str) -> dict:
    """Get major holders breakdown (insider %, institutions %, etc.).

    Returns a list of records with keys: breakdown (label) and value
    (percentage or count).
    """
    try:
        stock = yf.Ticker(ticker)
        mh = stock.major_holders
        data = _serialize_records(mh)
        return _make_response("major_holders", data, ticker=ticker)
    except Exception as e:
        return _make_error(f"Failed to get major holders for {ticker}: {e}")


if __name__ == "__main__":
    mcp.run(transport="stdio")
