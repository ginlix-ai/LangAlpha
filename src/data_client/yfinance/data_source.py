"""MarketDataSource implementation backed by yfinance.

Free fallback provider — requires no API key. Used when both ginlix-data
and FMP are unavailable (e.g. OSS / self-hosted deployments).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import yfinance as yf

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# Map data_client interval names → yfinance interval strings.
# None means unsupported — raises ValueError so the chain can skip this source.
_INTERVAL_MAP: dict[str, str | None] = {
    "1s": None,
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1hour": "1h",
    "4hour": None,
}

# Default lookback when no from_date is given, keyed by yfinance interval.
_DEFAULT_LOOKBACK: dict[str, timedelta] = {
    "1m": timedelta(days=7),
    "5m": timedelta(days=59),
    "15m": timedelta(days=59),
    "30m": timedelta(days=59),
    "1h": timedelta(days=729),
}


def _normalize_bar(idx, row: Any) -> dict[str, Any]:
    """Convert a yfinance history row to the standard OHLCV bar shape."""
    if hasattr(idx, "timestamp"):
        t = int(idx.timestamp() * 1000)
    else:
        t = 0
    return {
        "time": t,
        "open": round(float(row["Open"]), 4),
        "high": round(float(row["High"]), 4),
        "low": round(float(row["Low"]), 4),
        "close": round(float(row["Close"]), 4),
        "volume": int(row["Volume"]),
    }


def _fetch_history(
    symbol: str,
    interval: str,
    start: str | None,
    end: str | None,
) -> list[dict[str, Any]]:
    """Synchronous helper — called via ``asyncio.to_thread``."""
    ticker = yf.Ticker(symbol)

    kwargs: dict[str, Any] = {"interval": interval, "auto_adjust": True}
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end
    if not start and not end:
        lookback = _DEFAULT_LOOKBACK.get(interval, timedelta(days=730))
        kwargs["start"] = (datetime.now(_ET) - lookback).strftime("%Y-%m-%d")

    df = ticker.history(**kwargs)
    if df is None or df.empty:
        return []

    df = df.copy()
    return [_normalize_bar(idx, row) for idx, row in df.iterrows()]


# Yahoo Finance exchange suffixes to try when a plain symbol returns no data.
# Order matters — most common international exchanges first.
_YF_EXCHANGE_SUFFIXES = [".L", ".TO", ".V", ".SW", ".AS", ".PA", ".DE", ".HK", ".AX"]


def _try_yf_snapshot(yf_sym: str, original_sym: str) -> dict[str, Any] | None:
    """Attempt to fetch a snapshot for a single Yahoo Finance symbol."""
    try:
        ticker = yf.Ticker(yf_sym)
        fi = ticker.fast_info
        price = float(fi.get("lastPrice", 0) or 0)
        if price == 0:
            return None
        prev = float(fi.get("previousClose", 0) or 0)
        change = price - prev if prev else 0.0
        change_pct = (change / prev * 100) if prev else 0.0
        return {
            "symbol": original_sym,
            "name": None,
            "price": round(price, 4),
            "change": round(change, 4),
            "change_percent": round(change_pct, 4),
            "previous_close": round(prev, 4),
            "open": round(float(fi.get("open", 0) or 0), 4),
            "high": round(float(fi.get("dayHigh", 0) or 0), 4),
            "low": round(float(fi.get("dayLow", 0) or 0), 4),
            "volume": int(fi.get("lastVolume", 0) or 0),
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
        }
    except Exception:
        return None


def _fetch_single_snapshot(sym: str) -> dict[str, Any] | None:
    """Fetch snapshot for a single symbol.

    Tries the plain symbol first.  If that returns no price data and the
    symbol doesn't already contain a dot (exchange suffix), retries with
    common Yahoo Finance exchange suffixes (.L, .TO, .V, .SW, etc.).
    """
    result = _try_yf_snapshot(sym, sym)
    if result is not None:
        return result

    # Only try suffixes for plain symbols (no dot = no exchange already specified)
    if "." not in sym:
        for suffix in _YF_EXCHANGE_SUFFIXES:
            result = _try_yf_snapshot(f"{sym}{suffix}", sym)
            if result is not None:
                return result

    logger.warning("yfinance.snapshot.failed | symbol=%s (tried suffixes)", sym)
    return None


class YFinanceDataSource:
    """Market data source backed by Yahoo Finance (yfinance library)."""

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        yf_interval = _INTERVAL_MAP.get(interval)
        if yf_interval is None:
            raise ValueError(
                f"Interval '{interval}' is not supported by yfinance"
            )
        api_symbol = f"^{symbol}" if is_index and not symbol.startswith("^") else symbol
        return await asyncio.to_thread(
            _fetch_history, api_symbol, yf_interval, from_date, to_date
        )

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = f"^{symbol}" if is_index and not symbol.startswith("^") else symbol
        return await asyncio.to_thread(
            _fetch_history, api_symbol, "1d", from_date, to_date
        )

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        prepared = [
            (f"^{s}" if asset_type == "indices" and not s.startswith("^") else s)
            for s in symbols
        ]
        if not prepared:
            return []
        results = await asyncio.gather(
            *(asyncio.to_thread(_fetch_single_snapshot, s) for s in prepared)
        )
        return [r for r in results if r is not None]

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        from src.utils.market_hours import current_market_phase

        phase = current_market_phase()
        return {
            "market": (
                "open"
                if phase == "open"
                else ("extended-hours" if phase in ("pre", "post") else "closed")
            ),
            "afterHours": phase == "post",
            "earlyHours": phase == "pre",
            "serverTime": datetime.now(_ET).isoformat(),
            "exchanges": None,
        }

    async def close(self) -> None:
        pass
