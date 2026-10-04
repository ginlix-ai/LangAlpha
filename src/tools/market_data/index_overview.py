"""The overview of an index: a level and its ranges, with no company behind it."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from market_protocol import AssetClass, InstrumentRef

from src.data_client import get_market_data_provider

from ._shared import _safe_result
from .currency import DisplaySpec, fmt_price
from .prices import _calculate_price_statistics
from .quote_format import current_price, stamp_quote
from .utils import finite_or_none


async def fetch_index_overview(
    symbol: str, ref: InstrumentRef, user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Level, day range and 52-week range for the index *ref*, spelled *symbol*.

    The 52-week range comes from a year of daily bars, the one source every index
    route serves; the level and day range come from the snapshot. Both failing
    raises, as a company overview's provider failure does: an empty artifact
    would read as an index with no data, and the REST route would cache it.
    """
    provider = await get_market_data_provider()
    today = datetime.now(timezone.utc).date()
    snaps_result, daily_result = await asyncio.gather(
        provider.get_snapshots([symbol], asset_type="indices", user_id=user_id),
        provider.get_daily_with_source(
            symbol, from_date=(today - timedelta(days=365)).isoformat(),
            to_date=today.isoformat(), is_index=True, user_id=user_id,
        ),
        return_exceptions=True,
    )
    if isinstance(snaps_result, BaseException) and isinstance(daily_result, BaseException):
        raise snaps_result
    snap = next(iter(_safe_result(snaps_result, [])), None) or {}
    bars = (_safe_result(daily_result, None) or ([],))[0] or []
    year = _calculate_price_statistics(bars)

    artifact: Dict[str, Any] = {
        "type": "company_overview",
        "symbol": symbol,
        "name": snap.get("name_local") or snap.get("name"),
        "nameEn": snap.get("name_en"),
        "currency": ref.price_currency,
        "assetClass": AssetClass.INDEX.value,
    }
    quote = {
        "price": current_price(snap),
        "change": snap.get("change"),
        "changePct": snap.get("change_percent"),
        "open": snap.get("open"),
        "previousClose": snap.get("previous_close"),
        "dayHigh": snap.get("high"),
        "dayLow": snap.get("low"),
        "yearHigh": year.get("period_high"),
        "yearLow": year.get("period_low"),
        "volume": snap.get("volume"),
    }
    if any(v is not None for v in quote.values()):
        artifact["quote"] = {**quote, **(stamp_quote(snap, ref=ref) if snap else {})}
    return artifact


def index_overview_text(artifact: Dict[str, Any], spec: DisplaySpec) -> str:
    """What the agent reads for an index: its level and ranges, and why there is
    no company data to follow."""
    symbol = artifact["symbol"]
    q = artifact.get("quote") or {}

    def level(v: Any) -> str:
        return fmt_price(finite_or_none(v), spec, group=True)

    lines = [
        f"## Index Overview: {symbol}",
        f"**Retrieved:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    if artifact.get("name"):
        lines.append(f"**Name:** {artifact['name']}")
    lines += ["", f"{symbol} is an index: it has a level, not a company profile, statements or analyst coverage.", ""]
    if not q:
        return "\n".join(lines + ["No level data available."])
    change, pct = finite_or_none(q.get("change")), finite_or_none(q.get("changePct"))
    lines += ["| Metric | Value |", "|--------|-------|", f"| Level | {level(q.get('price'))} |"]
    if change is not None and pct is not None:
        lines.append(f"| Day Change | {'+' if change > 0 else ''}{level(change)} ({pct:+.2f}%) |")
    if q.get("dayLow") is not None and q.get("dayHigh") is not None:
        lines.append(f"| Day Range | {level(q['dayLow'])} - {level(q['dayHigh'])} |")
    if q.get("yearLow") is not None and q.get("yearHigh") is not None:
        lines.append(f"| 52-Week Range | {level(q['yearLow'])} - {level(q['yearHigh'])} |")
    return "\n".join(lines)
