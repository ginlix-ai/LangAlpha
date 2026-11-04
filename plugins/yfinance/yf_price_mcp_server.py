"""YFinance Price MCP Server.

Stock OHLCV history and dividend/split history via yfinance, wrapped in the
standard market-data envelope (see mcp_servers/AGENT_CONTRACT.md).

Tools:
- get_stock_history: OHLCV history for one ticker
- get_multiple_stocks_history: OHLCV history for several tickers
- get_dividends_and_splits: dividend + split history for one ticker
- get_multiple_stocks_dividends: dividend history for several tickers
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
from typing import Any, List, Optional

import pandas as pd
import yfinance as yf
from data_client.normalize import series_labels
from market_protocol import InstrumentRef, PriceTreatment, to_canonical

from mcp_servers._envelope import make_error, make_response, normalize_interval
from mcp_servers._schemas import (
    ERRORS,
    INT,
    OBJECT,
    OBJECTS_BY_KEY,
    RECORDS,
    STR,
    envelope_schema,
    output_model,
)
from mcp_servers._yf_common import boundary, format_datetime, minor_unit, safe_detail

SOURCE = "yfinance"

# Canonical interval → yfinance spelling for the upstream call.
_YF_INTERVAL = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1hour": "1h",
    "1day": "1d",
    "1week": "1wk",
    "1month": "1mo",
}
# yfinance-native granularities with no canonical equivalent — passed through
# and echoed verbatim. (4hour is canonical but yfinance has no such bar.)
_YF_NATIVE_INTERVALS = frozenset({"2m", "90m", "5d", "3mo"})
_SUPPORTED_INTERVALS = sorted(_YF_INTERVAL) + sorted(_YF_NATIVE_INTERVALS)

# ---------------------------------------------------------------------------
# Boundary + serialization helpers
# ---------------------------------------------------------------------------


def _ref(ticker: str) -> Optional[InstrumentRef]:
    try:
        return to_canonical(ticker)
    except Exception:  # noqa: BLE001
        return None


def _series_labels(ticker: str, history: list[dict], interval: str) -> dict[str, str]:
    """:func:`series_labels` for *history*. Its history is auto-adjusted for
    dividends, so what yfinance's declaration calls split-adjusted is
    dividend-adjusted here."""
    newest = history[-1]["date"] if history else None
    labels = series_labels(SOURCE, _ref(ticker), interval, newest)
    if labels["price_treatment"] == PriceTreatment.SPLIT_ADJUSTED.value:
        labels["price_treatment"] = PriceTreatment.DIVIDEND_ADJUSTED.value
    return labels


def _index_tz(index: Any) -> Optional[str]:
    """IANA zone name of a tz-aware pandas index, else None."""
    tz = getattr(index, "tz", None)
    return str(tz) if tz is not None else None


def _resolve_interval(interval: str) -> tuple[Optional[str], Optional[str], Optional[dict]]:
    """Map a caller interval to ``(yfinance_spelling, echo_spelling, error)``.

    ``echo_spelling`` is the canonical vocab when one exists, else the native
    yfinance spelling. On an unsupported interval the first two are None and the
    third is a ready-to-return error envelope.
    """
    raw = (interval or "").strip().lower()
    canon = normalize_interval(raw)
    if canon in _YF_INTERVAL:
        return _YF_INTERVAL[canon], canon, None
    if raw in _YF_NATIVE_INTERVALS:
        return raw, raw, None
    return None, None, make_error(
        "unsupported_interval",
        f"Interval '{interval}' is not supported by yfinance.",
        supported=_SUPPORTED_INTERVALS,
    )


def _declared_currency(stock: Any) -> Optional[str]:
    """yfinance's own declared quote currency for the last price fetch.

    Prefers ``history_metadata`` (populated by the same chart response — no
    extra network); falls back to ``fast_info.currency`` only when metadata is
    absent. Returns None when nothing usable is found.
    """
    meta = getattr(stock, "history_metadata", None)
    if isinstance(meta, dict):
        cur = meta.get("currency")
        if isinstance(cur, str) and cur:
            return cur
    try:
        fast = getattr(stock, "fast_info", None)
        cur = fast["currency"] if fast is not None else None
    except Exception:  # noqa: BLE001 - an unknown symbol raises KeyError here
        cur = None
    return cur if isinstance(cur, str) and cur else None


def _price_scale(stock: Any, ref_currency: Optional[str]) -> tuple[Optional[str], float]:
    """Resolve ``(envelope_currency, price_divisor)`` for a fetched ``stock``.

    When yfinance declares a minor-unit quote currency (e.g. GBp), report the
    major code (GBP) and a divisor to convert price fields; otherwise keep the
    boundary ``ref_currency`` and a no-op divisor of 1.
    """
    minor = minor_unit(_declared_currency(stock))
    if minor is not None:
        return minor[0], float(minor[1])
    return ref_currency, 1.0


_OHLC_COLUMNS = ("Open", "High", "Low", "Close")


def _round_price(value: float) -> float:
    """Six significant digits, never fewer than two decimals.

    Yahoo sends float32-grade values, so the digits past the sixth are noise,
    while a fixed two decimals flattens sub-dollar quotes: an FX day range of
    30 pips and DOGE's 7.4 to 7.6 cent move would both read as one price.
    """
    if value == 0 or not math.isfinite(value):
        return value
    return round(value, max(2, 5 - math.floor(math.log10(abs(value)))))


def _as_paid_dividends(dividends: pd.Series, splits: pd.Series) -> pd.Series:
    """Dividends per share as declared on their date.

    yfinance divides each dividend by every split that came after it (AAPL's
    1987 dividends read 0.0005 against 0.12 paid), so multiplying those splits
    back gives the amount paid. Splits are listed beside these amounts, and
    applying them to adjusted amounts would count each one twice.
    """
    factors = [(idx, float(ratio)) for idx, ratio in splits.items() if ratio > 0]
    paid = []
    for idx, amount in dividends.items():
        later = math.prod(r for split_date, r in factors if split_date > idx)
        paid.append(float(amount) * later)
    return pd.Series(paid, index=dividends.index, dtype="float64")


def _dividend_records(
    dividends: pd.Series, splits: pd.Series, divisor: float
) -> list[dict]:
    return [
        {"date": idx.strftime("%Y-%m-%d"), "amount": _round_price(val / divisor)}
        for idx, val in _as_paid_dividends(dividends, splits).items()
    ]


def _serialize_history(df: pd.DataFrame, price_divisor: float = 1.0) -> list[dict]:
    """OHLCV DataFrame → list of record dicts, ascending (oldest first).

    Date is exchange-local "%Y-%m-%d" for daily+ bars and
    "%Y-%m-%d %H:%M:%S" for intraday bars (those carrying a time component).
    ``price_divisor`` converts minor-unit quotes to major units (prices and
    dividend amounts only — never volume or split ratios).

    Bars with any NaN OHLC value are dropped: yfinance appends a placeholder
    row for an in-progress or dataless session, and a priceless bar is not a
    real observation (NaN is also not valid JSON). A missing volume on an
    otherwise-priced bar is coerced to 0 rather than dropped.
    """
    if df is None or df.empty:
        return []

    ohlc = [c for c in _OHLC_COLUMNS if c in df.columns]
    if ohlc:
        df = df.dropna(subset=ohlc)
    if df.empty:
        return []

    records = []
    for idx, row in df.iterrows():
        dt_str = format_datetime(idx)
        volume = row["Volume"]
        record = {
            "date": dt_str,
            "open": _round_price(float(row["Open"]) / price_divisor),
            "high": _round_price(float(row["High"]) / price_divisor),
            "low": _round_price(float(row["Low"]) / price_divisor),
            "close": _round_price(float(row["Close"]) / price_divisor),
            "volume": int(volume) if pd.notna(volume) else 0,
        }
        if "Dividends" in df.columns:
            record["dividends"] = _round_price(float(row["Dividends"]) / price_divisor)
        if "Stock Splits" in df.columns:
            record["splits"] = float(row["Stock Splits"])
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = MCPServer("YFinancePriceMCP")


_OUT_GET_STOCK_HISTORY = output_model(
    "GetStockHistoryOut",
    envelope_schema(
        RECORDS,
        frame=("symbol", "interval", "currency", "timezone"),
        echo={"period": STR},
    ),
)


@mcp.tool()
def get_stock_history(
    ticker: str,
    period: str = "1y",
    interval: str = "1d",
) -> _OUT_GET_STOCK_HISTORY:
    """Historical OHLCV price bars for one stock — charts, returns, technicals.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".
        period: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max.
        interval: 1min|5min|15min|30min|1hour|1day|1week|1month (native
            2m|90m|5d|3mo accepted; 4hour unsupported). Intraday lookback is
            capped by yfinance.

    Returns:
        dict: {symbol, interval, period, currency, timezone, count, data,
        source}. data is a list of {date, open, high, low, close, volume,
        dividends?, splits?}
        ascending (oldest first); date is exchange-local "YYYY-MM-DD" or
        "YYYY-MM-DD HH:MM:SS" intraday, timezone is the IANA zone, prices in
        `currency` major units (minor-unit GBp/pence auto-converted to major).
        On error: {error, detail} — not_found|unsupported_interval|upstream_error.
    """
    symbol, yf_symbol, currency = boundary(ticker)
    yf_interval, echo_interval, err = _resolve_interval(interval)
    if err is not None:
        err["symbol"] = symbol
        return err
    try:
        stock = yf.Ticker(yf_symbol)
        df = stock.history(period=period, interval=yf_interval)
        resp_currency, divisor = _price_scale(stock, currency)
        history = _serialize_history(df, divisor)
        if not history:
            return make_error(
                "not_found",
                f"No price history for {symbol} (period={period}, "
                f"interval={echo_interval}).",
                symbol=symbol,
            )
        return make_response(
            history,
            source=SOURCE,
            symbol=symbol,
            interval=echo_interval,
            currency=resp_currency,
            timezone=_index_tz(df.index),
            period=period,
            **_series_labels(ticker, history, echo_interval),
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error", safe_detail("Price history request", e), symbol=symbol
        )


_OUT_GET_MULTIPLE_STOCKS_HISTORY = output_model(
    "GetMultipleStocksHistoryOut",
    envelope_schema(
        OBJECTS_BY_KEY,
        frame=("interval",),
        echo={"period": STR, "errors": ERRORS},
    ),
)


@mcp.tool()
def get_multiple_stocks_history(
    tickers: List[str],
    period: str = "1y",
    interval: str = "1d",
) -> _OUT_GET_MULTIPLE_STOCKS_HISTORY:
    """Historical OHLCV bars for several stocks — compare price series.

    Args:
        tickers: Symbols, e.g. ["AAPL", "MSFT", "0700.HK"].
        period: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max.
        interval: 1min|5min|15min|30min|1hour|1day|1week|1month (native
            2m|90m|5d|3mo accepted; 4hour unsupported).

    Returns:
        dict: {interval, count, data, source, period, errors?}. data is keyed by
        canonical symbol → {count, currency?, timezone?, data}; each inner data
        is a list of {date, open, high, low, close, volume, dividends?, splits?}
        ascending (oldest first), exchange-local "YYYY-MM-DD[ HH:MM:SS]"; count
        is total bars. Prices in major units (GBp/pence auto-converted).
        Failed symbols are listed in errors as
        {error, detail, symbol}; a bad interval returns
        {error: unsupported_interval, detail, supported}.
    """
    yf_interval, echo_interval, err = _resolve_interval(interval)
    if err is not None:
        return err

    per_ticker: dict[str, Any] = {}
    errors: list[dict] = []
    total = 0

    for t in tickers:
        symbol, yf_symbol, currency = boundary(t)
        try:
            stock = yf.Ticker(yf_symbol)
            df = stock.history(period=period, interval=yf_interval)
            resp_currency, divisor = _price_scale(stock, currency)
            history = _serialize_history(df, divisor)
            entry: dict[str, Any] = {"count": len(history)}
            if resp_currency:
                entry["currency"] = resp_currency
            tz = _index_tz(df.index)
            if tz:
                entry["timezone"] = tz
            entry.update(_series_labels(t, history, echo_interval))
            entry["data"] = history
            per_ticker[symbol] = entry
            total += len(history)
        except Exception as e:  # noqa: BLE001
            errors.append(
                make_error(
                    "upstream_error",
                    safe_detail("Price history request", e),
                    symbol=symbol,
                )
            )

    result = make_response(
        per_ticker,
        source=SOURCE,
        interval=echo_interval,
        count=total,
        period=period,
    )
    if errors:
        result["errors"] = errors
    return result


_OUT_GET_DIVIDENDS_AND_SPLITS = output_model(
    "GetDividendsAndSplitsOut",
    envelope_schema(
        OBJECT,
        frame=("symbol", "currency", "timezone"),
        echo={"dividend_count": INT, "split_count": INT},
    ),
)


@mcp.tool()
def get_dividends_and_splits(ticker: str) -> _OUT_GET_DIVIDENDS_AND_SPLITS:
    """Full dividend and stock-split history for one ticker.

    Args:
        ticker: Symbol — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, currency, timezone, count, data, source,
        dividend_count, split_count}. data is
        {dividends: [{date, amount}], splits: [{date, ratio}]}, each ascending
        (oldest first); date is exchange-local "YYYY-MM-DD"; amount is per
        share as paid, in `currency` major units (minor-unit quotes like GBp
        are converted), not adjusted for later splits; ratio is the split
        factor (4.0 = 4-for-1); count is dividends + splits. On error:
        {error, detail, symbol} — not_found | upstream_error.
    """
    symbol, yf_symbol, currency = boundary(ticker)
    try:
        stock = yf.Ticker(yf_symbol)
        dividends_series = stock.dividends
        if dividends_series is None:
            return make_error(
                "not_found", f"No dividend data for {symbol}.", symbol=symbol
            )
        splits_series = stock.splits
        resp_currency, divisor = _price_scale(stock, currency)

        dividends = _dividend_records(dividends_series, splits_series, divisor)
        splits = [
            {"date": idx.strftime("%Y-%m-%d"), "ratio": float(val)}
            for idx, val in splits_series.items()
        ]

        tz = _index_tz(dividends_series.index) or _index_tz(splits_series.index)
        return make_response(
            {"dividends": dividends, "splits": splits},
            source=SOURCE,
            symbol=symbol,
            currency=resp_currency,
            timezone=tz,
            dividend_count=len(dividends),
            split_count=len(splits),
        )
    except Exception as e:  # noqa: BLE001
        return make_error(
            "upstream_error",
            safe_detail("Dividend/split request", e),
            symbol=symbol,
        )


_OUT_GET_MULTIPLE_STOCKS_DIVIDENDS = output_model(
    "GetMultipleStocksDividendsOut",
    envelope_schema(OBJECTS_BY_KEY, echo={"errors": ERRORS}),
)


@mcp.tool()
def get_multiple_stocks_dividends(tickers: List[str]) -> _OUT_GET_MULTIPLE_STOCKS_DIVIDENDS:
    """Dividend history for several stocks in one call. Use to compare payout
    histories across tickers.

    Args:
        tickers: Symbols, e.g. ["AAPL", "MSFT", "JNJ"].

    Returns:
        dict: {count, data, source, errors?}. data is keyed by canonical symbol
        → {count, currency?, dividends}; each dividends is a list of
        {date, amount} ascending (oldest first), date exchange-local
        "YYYY-MM-DD", amount per share as paid, in that symbol's major
        currency units (minor-unit quotes like GBp are converted), not
        adjusted for later splits; count is the total dividends across
        symbols. Failed symbols are omitted from data and listed in errors as
        {error, detail, symbol}.
    """
    per_ticker: dict[str, Any] = {}
    errors: list[dict] = []
    total = 0

    for t in tickers:
        symbol, yf_symbol, currency = boundary(t)
        try:
            stock = yf.Ticker(yf_symbol)
            if stock.dividends is None:
                errors.append(
                    make_error(
                        "not_found", f"No dividend data for {symbol}.", symbol=symbol
                    )
                )
                continue
            resp_currency, divisor = _price_scale(stock, currency)
            dividends = _dividend_records(stock.dividends, stock.splits, divisor)
            entry: dict[str, Any] = {"count": len(dividends)}
            if resp_currency:
                entry["currency"] = resp_currency
            entry["dividends"] = dividends
            per_ticker[symbol] = entry
            total += len(dividends)
        except Exception as e:  # noqa: BLE001
            errors.append(
                make_error(
                    "upstream_error",
                    safe_detail("Dividend request", e),
                    symbol=symbol,
                )
            )

    result = make_response(per_ticker, source=SOURCE, count=total)
    if errors:
        result["errors"] = errors
    return result


if __name__ == "__main__":
    mcp.run(transport="stdio")
