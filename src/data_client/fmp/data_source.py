"""FMP implementation of MarketDataSource.

Thin wrapper around :class:`FMPClient` that conforms to the
:class:`~src.data_client.base.MarketDataSource` protocol.

FMP stamps bars with exchange-local wall-clock strings and no timezone;
timestamps are localized per symbol (``symbol_timezone``) and bars are
returned ascending. Quotes/prices for LSE symbols arrive in GBX (pence);
both interfaces convert them to major units — the legacy bar/snapshot path
via ``minor_unit_scale`` and the protocol path (:func:`normalize_series`)
independently, so conversion is applied exactly once per path.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.data_client.market_data_provider import symbol_timezone
from src.data_client.normalize import (
    build_series,
    minor_unit_scale,
    scale_price,
    scale_snapshot_prices,
)
from src.market_protocol import InstrumentRef, Series

from .fmp_client import FMPClient

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def _parse_row_time(row: dict[str, Any], tz: ZoneInfo) -> int:
    """Row ``date`` string (exchange-local wall clock) → Unix ms."""
    t = row.get("time")
    if t is not None:
        return t
    date_str = row.get("date", "")
    if not date_str:
        return 0
    try:
        fmt = "%Y-%m-%d %H:%M:%S" if " " in date_str else "%Y-%m-%d"
        dt = datetime.strptime(date_str, fmt).replace(tzinfo=tz)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def normalize_series(rows: list[dict[str, Any]], *, ref: InstrumentRef, schema: str) -> Series:
    """Normalize raw FMP rows to a protocol Series (FMP parses wall-clock dates)."""
    tz = ZoneInfo(ref.tz)
    return build_series(
        rows, ref=ref, schema=schema, publisher="fmp",
        ts_of=lambda row: _parse_row_time(row, tz),
    )


class FMPDataSource:
    """Market data source backed by Financial Modeling Prep."""

    # FMP supports these intraday intervals; anything else should be rejected
    # so the chain can fall through to a provider that does support it.
    _SUPPORTED_INTERVALS = frozenset({"1min", "5min", "15min", "30min", "1hour", "4hour"})

    @staticmethod
    def _api_symbol(symbol: str, is_index: bool) -> str:
        # Suffixed index symbols (000300.SS) are already valid — never caret them.
        if not is_index or symbol.startswith("^") or "." in symbol:
            return symbol
        return f"^{symbol}"

    @staticmethod
    def _normalize(row: dict[str, Any], tz: ZoneInfo, scale: float) -> dict[str, Any]:
        """Normalize a raw FMP bar to the standard OHLCV shape.

        FMP returns ``date`` as an exchange-local string
        (``"2024-01-15 09:30:00"`` or ``"2024-01-15"``) — localized with the
        symbol's exchange tz, NOT assumed ET. ``time`` is Unix ms to match
        the envelope contract. ``scale`` is the minor-unit factor (0.01 for
        GBX/pence venues, else 1.0); volume is a share count, never scaled.
        """
        return {
            "time": _parse_row_time(row, tz),
            "open": scale_price(row.get("open", 0.0), scale),
            "high": scale_price(row.get("high", 0.0), scale),
            "low": scale_price(row.get("low", 0.0), scale),
            "close": scale_price(row.get("close", 0.0), scale),
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
        tz = symbol_timezone(symbol)
        scale = minor_unit_scale(symbol)
        async with FMPClient() as client:
            data = await client.get_intraday_chart(
                symbol=api_symbol,
                interval=interval,
                from_date=from_date,
                to_date=to_date,
            )
        bars = [self._normalize(bar, tz, scale) for bar in (data or [])]
        bars.sort(key=lambda b: b["time"])
        return bars

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = self._api_symbol(symbol, is_index)
        tz = symbol_timezone(symbol)
        scale = minor_unit_scale(symbol)
        async with FMPClient() as client:
            data = await client.get_stock_price(
                symbol=api_symbol,
                from_date=from_date,
                to_date=to_date,
            )
        bars = [self._normalize(bar, tz, scale) for bar in (data or [])]
        bars.sort(key=lambda b: b["time"])
        return bars

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
        """Normalize an FMP quote response to the unified snapshot shape.

        Price-like fields are scaled to major units per the quote's own symbol
        (GBX venues → ×0.01), so a mixed-market batch converts each row
        correctly; change_percent is a ratio and volume a share count — both
        left untouched.
        """
        symbol = q.get("symbol", "")
        if asset_type == "indices":
            symbol = symbol.lstrip("^")
        snap = {
            "symbol": symbol,
            "name": q.get("name"),
            "price": q.get("price"),
            "change": q.get("change"),
            "change_percent": q.get("changePercentage"),
            "previous_close": q.get("previousClose"),
            "open": q.get("open"),
            "high": q.get("dayHigh"),
            "low": q.get("dayLow"),
            "volume": int(q["volume"]) if q.get("volume") is not None else None,
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
        }
        return scale_snapshot_prices(snap, minor_unit_scale(symbol))

    async def close(self) -> None:
        pass  # FMPClient manages its own lifecycle per-request

# Backward-compatible alias
FMPPriceProvider = FMPDataSource
