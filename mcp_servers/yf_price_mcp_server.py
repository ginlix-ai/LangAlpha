"""YFinance Price MCP Server.

Provides stock price history and dividend/split data via yfinance.

Tools:
- get_stock_history: OHLCV history for a single ticker
- get_multiple_stocks_history: OHLCV history for multiple tickers
- get_dividends_and_splits: dividend and split history for a ticker
- get_multiple_stocks_dividends: dividend history for multiple tickers
"""

from typing import Any, List, Optional

import pandas as pd
import yfinance as yf
from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Helpers (inlined — each yf server is deployed as a single file)
# ---------------------------------------------------------------------------


def _serialize_history(df: pd.DataFrame) -> list[dict]:
    """Convert historical price DataFrame to list of record dicts.

    Timestamp convention (matches FMP price servers):
    - Daily+: YYYY-MM-DD
    - Intraday: YYYY-MM-DD HH:MM:SS (exchange-local time, no tz offset)
    """
    if df is None or df.empty:
        return []

    records = []
    for idx, row in df.iterrows():
        dt_str = (
            idx.strftime("%Y-%m-%d %H:%M:%S")
            if (idx.hour or idx.minute or idx.second)
            else idx.strftime("%Y-%m-%d")
        )
        record = {
            "date": dt_str,
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
        }
        if "Dividends" in df.columns:
            record["dividends"] = round(float(row["Dividends"]), 4)
        if "Stock Splits" in df.columns:
            record["splits"] = float(row["Stock Splits"])
        records.append(record)

    return records


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

mcp = FastMCP("YFinancePriceMCP")


@mcp.tool()
def get_stock_history(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
) -> dict:
    """Get historical OHLCV price data for a stock.

    Returns bars with open, high, low, close, volume, plus dividends and
    stock splits when available. Timestamps are in exchange-local time
    (e.g. US Eastern for US stocks). Format: YYYY-MM-DD for daily+,
    YYYY-MM-DD HH:MM:SS for intraday.

    Args:
        ticker: Stock symbol (e.g. AAPL, MSFT, TSLA)
        period: How far back — 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
        interval: Bar size — 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

    Note: Intraday intervals have max lookback limits — 1m: 7 days,
    2m/5m/15m/30m: 60 days, 60m/90m/1h: 730 days.
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)
        history = _serialize_history(df)
        if not history:
            return _make_error(f"No data found for {ticker} with period={period}, interval={interval}")
        return _make_response(
            "stock_history",
            history,
            ticker=ticker,
            period=period,
            interval=interval,
        )
    except Exception as e:  # noqa: BLE001
        return _make_error(f"Failed to fetch history for {ticker}: {e}")


@mcp.tool()
def get_multiple_stocks_history(
    tickers: List[str],
    period: str = "1y",
    interval: str = "1d",
) -> dict:
    """Get historical OHLCV price data for multiple stocks at once.

    Returns per-ticker history arrays keyed by symbol, each with a "data"
    list of OHLCV records and a "count". Tickers that fail appear in
    an "errors" list instead of aborting the whole request. Timestamps
    are in exchange-local time (YYYY-MM-DD for daily+, YYYY-MM-DD HH:MM:SS
    for intraday).

    Args:
        tickers: List of stock symbols (e.g. ["AAPL", "MSFT", "GOOGL"])
        period: How far back — 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
        interval: Bar size — 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

    Note: Intraday intervals have max lookback limits — 1m: 7 days,
    2m/5m/15m/30m: 60 days, 60m/90m/1h: 730 days.
    """
    per_ticker_data = {}
    errors = []
    total_data_points = 0

    for t in tickers:
        try:
            stock = yf.Ticker(t)
            df = stock.history(period=period, interval=interval)
            history = _serialize_history(df)
            per_ticker_data[t] = {"data": history, "count": len(history)}
            total_data_points += len(history)
        except Exception as e:  # noqa: BLE001
            errors.append({"ticker": t, "error": str(e)})

    result = _make_response(
        "multiple_stocks_history",
        per_ticker_data,
        total_data_points=total_data_points,
        period=period,
        interval=interval,
    )
    if errors:
        result["errors"] = errors
    return result


@mcp.tool()
def get_dividends_and_splits(ticker: str) -> dict:
    """Get complete dividend and stock split history for a ticker.

    Returns {"dividends": [{date, amount}, ...], "splits": [{date, ratio}, ...]}.
    Also includes dividend_count and split_count in the envelope.

    Args:
        ticker: Stock symbol (e.g. AAPL, MSFT)
    """
    try:
        stock = yf.Ticker(ticker)
        dividends_series = stock.dividends
        splits_series = stock.splits

        dividends = [
            {"date": idx.strftime("%Y-%m-%d"), "amount": round(float(val), 4)}
            for idx, val in dividends_series.items()
        ]
        splits = [
            {"date": idx.strftime("%Y-%m-%d"), "ratio": float(val)}
            for idx, val in splits_series.items()
        ]

        return _make_response(
            "dividends_and_splits",
            {"dividends": dividends, "splits": splits},
            ticker=ticker,
            dividend_count=len(dividends),
            split_count=len(splits),
        )
    except Exception as e:  # noqa: BLE001
        return _make_error(f"Failed to fetch dividends/splits for {ticker}: {e}")


@mcp.tool()
def get_multiple_stocks_dividends(tickers: List[str]) -> dict:
    """Get dividend history for multiple stocks at once.

    Returns per-ticker dividend arrays keyed by symbol, each with a
    "dividends" list of {date, amount} records and a "count". Tickers
    that fail appear in an "errors" list.

    Args:
        tickers: List of stock symbols (e.g. ["AAPL", "MSFT", "JNJ"])
    """
    per_ticker_data = {}
    errors = []
    total_dividends = 0

    for t in tickers:
        try:
            stock = yf.Ticker(t)
            dividends_series = stock.dividends
            dividends = [
                {"date": idx.strftime("%Y-%m-%d"), "amount": round(float(val), 4)}
                for idx, val in dividends_series.items()
            ]
            per_ticker_data[t] = {"dividends": dividends, "count": len(dividends)}
            total_dividends += len(dividends)
        except Exception as e:  # noqa: BLE001
            errors.append({"ticker": t, "error": str(e)})

    result = _make_response(
        "multiple_stocks_dividends",
        per_ticker_data,
        total_dividends=total_dividends,
    )
    if errors:
        result["errors"] = errors
    return result


if __name__ == "__main__":
    mcp.run(transport="stdio")
