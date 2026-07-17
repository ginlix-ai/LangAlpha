"""MarketDataSource implementation backed by yfinance.

Free fallback provider — requires no API key. Used when both ginlix-data
and FMP are unavailable (e.g. OSS / self-hosted deployments).
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import yfinance as yf

from src.data_client.normalize import build_series, minor_unit_scale, scale_snapshot_prices
from src.market_protocol import InstrumentRef, Series

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def normalize_series(rows: list[dict], *, ref: InstrumentRef, schema: str) -> Series:
    """Normalize yfinance bars (epoch-ms ``time`` already UTC-correct) to a Series."""
    return build_series(
        rows, ref=ref, schema=schema, publisher="yfinance",
        ts_of=lambda row: int(row["time"]) if row.get("time") else 0,
    )

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
# Two days inside Yahoo's caps (1m: 8d, sub-hour: 60d, 1h: 730d): the start is
# a date string Yahoo reads at exchange-local midnight, so a cap-boundary
# lookback overflows by the time-of-day plus any ET-to-exchange date shift and
# Yahoo rejects the whole request.
_DEFAULT_LOOKBACK: dict[str, timedelta] = {
    "1m": timedelta(days=6),
    "5m": timedelta(days=57),
    "15m": timedelta(days=57),
    "30m": timedelta(days=57),
    "1h": timedelta(days=727),
}


def _exclusive_end(end: str) -> str:
    """Advance an inclusive to_date to yfinance's exclusive ``end`` bound.

    The provider contract treats to_date as inclusive; yfinance's ``end`` is an
    exclusive exchange-local midnight, so passing it through drops the final
    trading day — including today's live session on windows ending today.
    """
    try:
        return (date.fromisoformat(end) + timedelta(days=1)).isoformat()
    except ValueError:
        return end


def _finite(v: Any) -> float | None:
    """Coerce to float, returning None for None/non-finite/unparseable values.

    None (not 0.0): a missing price coerced to zero reads as a real $0.00
    quote downstream, while None lets callers drop or omit the field.
    """
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _normalize_bar(idx, row: Any, scale: float = 1.0) -> dict[str, Any]:
    """Convert a yfinance history row to the standard OHLCV bar shape.

    ``scale`` is the minor-unit factor (0.01 for GBX/pence venues, else 1.0);
    OHLC prices are scaled, volume (a share count) is not.
    """
    if hasattr(idx, "timestamp"):
        t = int(idx.timestamp() * 1000)
    else:
        t = 0
    return {
        "time": t,
        "open": round(float(row["Open"]) * scale, 4),
        "high": round(float(row["High"]) * scale, 4),
        "low": round(float(row["Low"]) * scale, 4),
        "close": round(float(row["Close"]) * scale, 4),
        # NaN != NaN: index/sparse rows carry NaN volume and int(NaN) raises,
        # killing the whole fetch for one bad row.
        "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
    }


def _fetch_history(
    symbol: str,
    interval: str,
    start: str | None,
    end: str | None,
    scale: float = 1.0,
) -> list[dict[str, Any]]:
    """Synchronous helper — called via ``asyncio.to_thread``."""
    ticker = yf.Ticker(symbol)

    # auto_adjust=False: Yahoo's Close is split-adjusted only (dividends live
    # in Adj Close, which we don't read) — the whole provider chain declares
    # price_treatment=split_adjusted, so conventions never blend across a
    # provider failover.
    kwargs: dict[str, Any] = {"interval": interval, "auto_adjust": False}
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = _exclusive_end(end)
    if not start and not end:
        lookback = _DEFAULT_LOOKBACK.get(interval, timedelta(days=730))
        kwargs["start"] = (datetime.now(_ET) - lookback).strftime("%Y-%m-%d")

    df = ticker.history(**kwargs)
    if df is None or df.empty:
        return []

    # A NaN-priced row (halted session, yfinance gap padding) would propagate
    # NaN into the cache and render as "nan" downstream — drop it here.
    df = df.copy().dropna(subset=["Open", "High", "Low", "Close"])
    return [_normalize_bar(idx, row, scale) for idx, row in df.iterrows()]


def _fetch_single_snapshot(sym: str) -> dict[str, Any] | None:
    """Fetch snapshot for a single symbol. Returns None on failure."""
    try:
        ticker = yf.Ticker(sym)
        fi = ticker.fast_info
        # NaN is truthy, so `nan or 0` stays NaN — route every numeric field
        # through _finite() so a snapshot can never carry non-finite floats.
        # No finite last price means no quote: a None snapshot beats a $0.00
        # phantom; missing optional fields stay None rather than becoming 0.
        price = _finite(fi.get("lastPrice"))
        if price is None:
            return None
        prev = _finite(fi.get("previousClose"))
        change = price - prev if prev else 0.0
        change_pct = (change / prev * 100) if prev else 0.0
        open_ = _finite(fi.get("open"))
        high = _finite(fi.get("dayHigh"))
        low = _finite(fi.get("dayLow"))
        volume = _finite(fi.get("lastVolume"))
        return {
            "symbol": sym,
            "name": None,
            "price": round(price, 4),
            "change": round(change, 4),
            "change_percent": round(change_pct, 4),
            "previous_close": round(prev, 4) if prev is not None else None,
            "open": round(open_, 4) if open_ is not None else None,
            "high": round(high, 4) if high is not None else None,
            "low": round(low, 4) if low is not None else None,
            "volume": int(volume) if volume is not None else 0,
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
        }
    except Exception:
        logger.warning("yfinance.snapshot.failed | symbol=%s", sym, exc_info=True)
        return None


class YFinanceDataSource:
    """Market data source backed by Yahoo Finance (yfinance library)."""

    @staticmethod
    def _api_symbol(symbol: str, is_index: bool) -> str:
        # Suffixed index symbols (000300.SS) are already valid — never caret them.
        if not is_index or symbol.startswith("^") or "." in symbol:
            return symbol
        return f"^{symbol}"

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
        api_symbol = self._api_symbol(symbol, is_index)
        scale = minor_unit_scale(symbol)
        return await asyncio.to_thread(
            _fetch_history, api_symbol, yf_interval, from_date, to_date, scale
        )

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = self._api_symbol(symbol, is_index)
        scale = minor_unit_scale(symbol)
        return await asyncio.to_thread(
            _fetch_history, api_symbol, "1d", from_date, to_date, scale
        )

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        prepared = [
            self._api_symbol(s, is_index=(asset_type == "indices")) for s in symbols
        ]
        if not prepared:
            return []
        results = await asyncio.gather(
            *(asyncio.to_thread(_fetch_single_snapshot, s) for s in prepared)
        )
        # gather preserves order, so results[i] maps back to symbols[i]. Restore
        # the originally-requested (bare) symbol — the caret was only for the
        # Yahoo query — so the provider chain matches on the requested ticker
        # instead of dropping "^GSPC" as unrequested. Keeps yfinance consistent
        # with FMP/ginlix-data, which already return bare index symbols.
        out: list[dict[str, Any]] = []
        for original, snap in zip(symbols, results):
            if snap is None:
                continue
            snap["symbol"] = original
            # Scale price-like fields per the requested symbol (GBX → ×0.01).
            scale_snapshot_prices(snap, minor_unit_scale(original))
            out.append(snap)
        return out

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
