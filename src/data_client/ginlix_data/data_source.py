"""ginlix-data implementation of MarketDataSource."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from src.data_client.base import FetchResult
from src.utils.market_hours import current_trading_date

from .client import GinlixDataClient

logger = logging.getLogger(__name__)

# FMP-style interval → ginlix-data (timespan, multiplier)
INTERVAL_MAP: dict[str, tuple[str, int]] = {
    "1s": ("second", 1),
    "1min": ("minute", 1),
    "5min": ("minute", 5),
    "15min": ("minute", 15),
    "30min": ("minute", 30),
    "1hour": ("hour", 1),
    "4hour": ("hour", 4),
}

# Yahoo Finance / FMP symbol
_INDEX_SYMBOL_MAP: dict[str, str] = {
    "GSPC": "I:SPX",
    "DJI": "I:DJI",
    "IXIC": "I:COMP",
    "RUT": "I:RUT",
    "VIX": "I:VIX",
    "NDX": "I:NDX",
}
_REVERSE_INDEX_SYMBOL_MAP: dict[str, str] = {v: k for k, v in _INDEX_SYMBOL_MAP.items()}


class GinlixDataSource:
    """Market data source backed by ginlix-data REST API."""

    # Interval-aware lookback windows (trading days).
    # Each live cache key stores bars from this window; incoming requests
    # with a from/to that falls within the window are served from cache.
    # Per-page limit for the upstream API (max 50000).  The client auto-
    # paginates, so the actual result set may exceed this.
    _LIMIT_BY_INTERVAL: dict[str, int] = {
        "1s": 50000,  # max per page; auto-pagination fetches remaining
    }
    _DEFAULT_LIMIT = 5000

    _LOOKBACK_BY_INTERVAL: dict[str, int] = {
        "1s": 0,       # today only; extended hours can reach 57K bars, desc sort keeps newest
        "1min": 5,     # ~1,950 bars, ~230 KB
        "5min": 10,    # ~780 bars, ~95 KB
        "15min": 10,   # ~260 bars, ~32 KB
        "30min": 10,   # ~130 bars, ~16 KB
        "1hour": 10,   # ~65 bars, ~8 KB
        "4hour": 10,   # ~17 bars, ~2 KB
    }
    _DAILY_LOOKBACK_DAYS = 365 * 2  # ~504 bars, ~55 KB

    def __init__(self, client: GinlixDataClient) -> None:
        self.client = client

    @classmethod
    def lookback_days_for(cls, interval: str) -> int:
        """Return the default lookback window in calendar days for *interval*."""
        return cls._LOOKBACK_BY_INTERVAL.get(interval, 7)

    @staticmethod
    def _index_symbol(symbol: str) -> str:
        """Convert a Yahoo/FMP-style index symbol to ginlix-data format."""
        if symbol.startswith("I:"):
            return symbol
        bare = symbol.lstrip("^").upper()
        return _INDEX_SYMBOL_MAP.get(bare, f"I:{bare}")

    @staticmethod
    def _default_dates(
        from_date: str | None, to_date: str | None, lookback_days: int
    ) -> tuple[str, str]:
        """ginlix-data requires from/to — supply sensible defaults.

        For zero-lookback intervals (e.g. 1s), use ``current_trading_date()``
        so that after-hours / overnight queries fetch the most recent trading
        day's data instead of an empty future date.
        """
        if to_date is None:
            if lookback_days == 0:
                to_date = current_trading_date()
            else:
                to_date = date.today().strftime("%Y-%m-%d")
        if from_date is None:
            if lookback_days == 0:
                from_date = current_trading_date()
            else:
                from_date = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        return from_date, to_date

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> FetchResult:
        market = "index" if is_index else "stock"
        api_symbol = self._index_symbol(symbol) if is_index else symbol
        if interval not in INTERVAL_MAP:
            raise ValueError(f"Unsupported interval: {interval}")
        timespan, multiplier = INTERVAL_MAP[interval]
        lookback = self._LOOKBACK_BY_INTERVAL.get(interval, 7)
        from_date, to_date = self._default_dates(from_date, to_date, lookback)
        limit = self._LIMIT_BY_INTERVAL.get(interval, self._DEFAULT_LIMIT)
        logger.info(
            "get_intraday %s %s from=%s to=%s limit=%d",
            api_symbol, interval, from_date, to_date, limit,
        )
        raw, truncated = await self.client.get_aggregates(
            market=market,
            symbol=api_symbol,
            timespan=timespan,
            multiplier=multiplier,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            user_id=user_id,
        )
        bars = [self._normalize(r) for r in raw]
        bars.sort(key=lambda b: b.get("time", 0))  # ensure ascending for cache watermark/merge
        first_t = bars[0].get("time") if bars else None
        last_t = bars[-1].get("time") if bars else None
        logger.info(
            "get_intraday %s %s → %d bars, first=%s last=%s",
            api_symbol, interval, len(bars), first_t, last_t,
        )
        return FetchResult(bars=bars, truncated=truncated)

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> FetchResult:
        market = "index" if is_index else "stock"
        api_symbol = self._index_symbol(symbol) if is_index else symbol
        from_date, to_date = self._default_dates(
            from_date, to_date, self._DAILY_LOOKBACK_DAYS
        )
        limit = self._DEFAULT_LIMIT
        raw, truncated = await self.client.get_aggregates(
            market=market,
            symbol=api_symbol,
            timespan="day",
            multiplier=1,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            user_id=user_id,
        )
        return FetchResult(bars=[self._normalize(r) for r in raw], truncated=truncated)

    @staticmethod
    def _normalize(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a ginlix-data bar to the standard OHLCV shape."""
        return {
            "time": row.get("time", 0),
            "open": row.get("open", 0.0),
            "high": row.get("high", 0.0),
            "low": row.get("low", 0.0),
            "close": row.get("close", 0.0),
            "volume": int(row.get("volume", 0)),
        }

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch batch snapshots, converting index symbols as needed."""
        if asset_type == "indices":
            api_symbols = [self._index_symbol(s) for s in symbols]
        else:
            api_symbols = symbols
        raw = await self.client.get_snapshots(asset_type, api_symbols, user_id=user_id)
        return [self._normalize_snapshot(item, asset_type) for item in raw]

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Fetch current market status from ginlix-data."""
        return await self.client.get_market_status(user_id=user_id)

    @staticmethod
    def _normalize_snapshot(raw: dict[str, Any], asset_type: str = "stocks") -> dict[str, Any]:
        """Normalize a ginlix-data snapshot to the unified snapshot shape."""
        session = raw.get("session", {})
        last_trade = raw.get("last_trade", {})
        ticker = raw.get("ticker", "")
        # For indices, reverse-map I:SPX → GSPC etc.
        if asset_type == "indices":
            ticker = _REVERSE_INDEX_SYMBOL_MAP.get(ticker, ticker.removeprefix("I:"))
        return {
            "symbol": ticker,
            "name": raw.get("name"),
            "price": session.get("close"),
            "change": session.get("change"),
            "change_percent": session.get("change_percent"),
            "previous_close": session.get("previous_close"),
            "open": session.get("open"),
            "high": session.get("high"),
            "low": session.get("low"),
            "volume": int(session["volume"]) if session.get("volume") is not None else None,
            "market_status": raw.get("market_status"),
            "last_trade_price": last_trade.get("price") if last_trade else None,
            "regular_trading_change": session.get("regular_trading_change"),
            "regular_trading_change_percent": session.get("regular_trading_change_percent"),
            "early_trading_change": session.get("early_trading_change"),
            "early_trading_change_percent": session.get("early_trading_change_percent"),
            "late_trading_change": session.get("late_trading_change"),
            "late_trading_change_percent": session.get("late_trading_change_percent"),
        }

    async def close(self) -> None:
        await self.client.close()


# Backward-compatible alias
GinlixDataPriceProvider = GinlixDataSource
