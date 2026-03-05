"""FMP implementation of MarketDataSource.

Thin wrapper around :class:`FMPClient` that conforms to the
:class:`~src.data_client.base.MarketDataSource` protocol.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .fmp_client import FMPClient

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


class FMPDataSource:
    """Market data source backed by Financial Modeling Prep."""

    # FMP supports these intraday intervals; anything else should be rejected
    # so the chain can fall through to a provider that does support it.
    _SUPPORTED_INTERVALS = frozenset({"1min", "5min", "15min", "30min", "1hour", "4hour"})

    @staticmethod
    def _api_symbol(symbol: str, is_index: bool) -> str:
        return f"^{symbol}" if is_index and not symbol.startswith("^") else symbol

    @staticmethod
    def _normalize(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a raw FMP bar to the standard OHLCV shape.

        FMP returns ``date`` as an ET string (``"2024-01-15 09:30:00"`` or
        ``"2024-01-15"``).  Convert to ``time`` as Unix ms to match the
        envelope contract (``ENVELOPE_VERSION = 2``).
        """
        t = row.get("time")
        if t is None:
            date_str = row.get("date", "")
            if date_str:
                try:
                    fmt = "%Y-%m-%d %H:%M:%S" if " " in date_str else "%Y-%m-%d"
                    dt = datetime.strptime(date_str, fmt).replace(tzinfo=_ET)
                    t = int(dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    t = 0
            else:
                t = 0
        return {
            "time": t,
            "open": row.get("open", 0.0),
            "high": row.get("high", 0.0),
            "low": row.get("low", 0.0),
            "close": row.get("close", 0.0),
            "volume": int(row.get("volume") or 0),
        }

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if interval not in self._SUPPORTED_INTERVALS:
            raise ValueError(
                f"Interval '{interval}' is not supported by this data source"
            )
        api_symbol = self._api_symbol(symbol, is_index)
        async with FMPClient() as client:
            data = await client.get_intraday_chart(
                symbol=api_symbol,
                interval=interval,
                from_date=from_date,
                to_date=to_date,
            )
        return [self._normalize(bar) for bar in (data or [])]

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = self._api_symbol(symbol, is_index)
        async with FMPClient() as client:
            data = await client.get_stock_price(
                symbol=api_symbol,
                from_date=from_date,
                to_date=to_date,
            )
        return [self._normalize(bar) for bar in (data or [])]

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch batch snapshots via FMP batch quote endpoint.

        Uses a single batch call. Extended-hours fields (early/late trading)
        are not available from FMP and returned as None — the frontend
        gracefully hides them when absent.
        """
        api_symbols = [
            self._api_symbol(s, is_index=(asset_type == "indices"))
            for s in symbols
        ]
        async with FMPClient() as client:
            quotes = await client.get_batch_quotes(api_symbols)
        return [self._normalize_quote(q, asset_type) for q in (quotes or [])]

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Derive market status from current time (FMP has no dedicated endpoint)."""
        from src.utils.market_hours import current_market_phase

        phase = current_market_phase()
        return {
            "market": "open" if phase == "open" else ("extended-hours" if phase in ("pre", "post") else "closed"),
            "afterHours": phase == "post",
            "earlyHours": phase == "pre",
            "serverTime": datetime.now(_ET).isoformat(),
            "exchanges": None,
        }

    @staticmethod
    def _normalize_quote(q: dict[str, Any], asset_type: str = "stocks") -> dict[str, Any]:
        """Normalize an FMP quote response to the unified snapshot shape."""
        symbol = q.get("symbol", "")
        if asset_type == "indices":
            symbol = symbol.lstrip("^")
        return {
            "symbol": symbol,
            "name": q.get("name"),
            "price": q.get("price"),
            "change": q.get("change"),
            "change_percent": q.get("changesPercentage"),
            "previous_close": q.get("previousClose"),
            "open": q.get("open"),
            "high": q.get("dayHigh"),
            "low": q.get("dayLow"),
            "volume": int(q["volume"]) if q.get("volume") is not None else None,
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
        }

    async def close(self) -> None:
        pass  # FMPClient manages its own lifecycle per-request


# Backward-compatible alias
FMPPriceProvider = FMPDataSource
