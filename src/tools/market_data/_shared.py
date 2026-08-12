# pyright: ignore
"""Config/user-id, result-safety, bar-normalization, and FMP-request helpers
shared by the market-data tool implementations."""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import logging

from langchain_core.runnables import RunnableConfig

from .utils import finite_or_none
from src.data_client.market_data_provider import symbol_timezone

logger = logging.getLogger(__name__)


def _get_user_id(config: Optional[RunnableConfig] = None) -> Optional[str]:
    """Extract user_id from RunnableConfig, or return None."""
    if config is None:
        return None
    return config.get("configurable", {}).get("user_id")


def _safe_result(result, default=None):
    """Extract result from asyncio.gather, returning default if exception."""
    if isinstance(result, Exception):
        return default
    return result if result is not None else default


def _normalize_market_bars(
    bars: list, symbol: str, datetime_format: bool = False
) -> List[Dict[str, Any]]:
    """Convert MarketDataSource bars to the format expected by formatting helpers.

    MarketDataSource returns ``{time, open, high, low, close, volume}``.
    Formatters expect ``{date, open, high, low, close, volume, change,
    changePercent, symbol}``.  Returns newest-first.
    """
    if not bars:
        return []

    # Sort ascending for correct change computation
    sorted_bars = sorted(bars, key=lambda b: b.get("time", 0))
    result: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None

    tz = symbol_timezone(symbol)

    for bar in sorted_bars:
        ts = bar.get("time")
        if ts is not None and isinstance(ts, (int, float)) and ts > 0:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(tz)
            date_str = (
                dt.strftime("%Y-%m-%d %H:%M:%S")
                if datetime_format
                else dt.strftime("%Y-%m-%d")
            )
        else:
            date_str = bar.get("date", "N/A")

        # A forming bar can carry NaN OHLCV; drop non-finite fields to None so
        # they never reach downstream JSON.
        open_ = finite_or_none(bar.get("open"))
        high = finite_or_none(bar.get("high"))
        low = finite_or_none(bar.get("low"))
        close = finite_or_none(bar.get("close"))
        volume = finite_or_none(bar.get("volume"))
        change: Optional[float] = None
        change_pct: Optional[float] = None
        if close is not None and prev_close is not None and prev_close != 0:
            change = close - prev_close
            change_pct = (change / prev_close) * 100

        result.append(
            {
                "date": date_str,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "change": change,
                "changePercent": change_pct,
                "symbol": symbol,
            }
        )

        if close is not None:
            prev_close = close

    # Return newest-first (matching FMP's original order)
    result.reverse()
    return result


async def _fmp_request(method: str, *args: Any, **kwargs: Any) -> Any:
    """Make a direct FMP API call for methods not in the protocol."""
    from src.data_client.fmp import get_fmp_client

    client = await get_fmp_client()
    return await getattr(client, method)(*args, **kwargs)
