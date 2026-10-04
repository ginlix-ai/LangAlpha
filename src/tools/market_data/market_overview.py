# pyright: ignore
"""Market-level views: sector performance and the index-snapshot market overview."""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
import logging
import asyncio
import math

from langchain_core.runnables import RunnableConfig

from .currency import fmt_count
from .display import resolve_ref, venue_local_time
from .quote_format import build_live_stamp, current_price, print_time, quote_freshness
from .utils import finite_or_none
from src.data_client import get_financial_data_provider, get_market_data_provider
from src.data_client.freshness import measure_daily
from src.data_client.instrument_clock import clock_for
from src.data_client.market_data_provider import snapshot_key
from src.data_client.normalize import change_from
from market_protocol import (
    CARET_INDEX_REGIONS,
    AssetClass,
    is_family_index,
    to_canonical,
    to_legacy_api,
)

from ._shared import _get_user_id, _normalize_market_bars
from .prices import _calculate_price_statistics

logger = logging.getLogger(__name__)


def _format_sectors_as_table(sectors_data: List[Dict[str, Any]]) -> str:
    """
    Format sector performance data as a markdown table.

    Args:
        sectors_data: List of sector performance dictionaries

    Returns:
        Markdown-formatted table string
    """
    if not sectors_data or len(sectors_data) == 0:
        return "No sector performance data available."

    lines = []

    # Header
    lines.append("## Sector Performance")
    lines.append("")

    # Table header
    lines.append("| Sector                      | Change    | Status    |")
    lines.append("|-----------------------------|-----------|-----------|")

    # Parse and sort sectors by performance
    parsed_sectors = []
    for sector in sectors_data:
        sector_name = sector.get("sector", "N/A")
        change_str = sector.get("changePctStr", "0%")

        # Parse percentage (handle formats like "+1.50%" or "-0.42%")
        try:
            change_val = float(change_str.replace("%", "").replace("+", ""))
        except (ValueError, AttributeError):
            change_val = 0.0

        parsed_sectors.append(
            {"name": sector_name, "change_str": change_str, "change_val": change_val}
        )

    # Sort by performance (descending)
    parsed_sectors.sort(key=lambda x: x["change_val"], reverse=True)

    # Table rows
    for sector in parsed_sectors:
        name = sector["name"]
        change_str = sector["change_str"]
        change_val = sector["change_val"]

        # Add status indicator
        if change_val > 0:
            status = "📈 Up"
        elif change_val < 0:
            status = "📉 Down"
        else:
            status = "➡️ Flat"

        # Pad percentage for alignment
        if not change_str.startswith("+") and not change_str.startswith("-"):
            if change_val >= 0:
                change_str = "+" + change_str

        lines.append(f"| {name:27} | {change_str:>9} | {status:9} |")

    # Summary
    if parsed_sectors:
        best = parsed_sectors[0]
        worst = parsed_sectors[-1]

        lines.append("")
        lines.append(f"**Best Performing:** {best['name']} ({best['change_str']})")
        lines.append(f"**Worst Performing:** {worst['name']} ({worst['change_str']})")

    return "\n".join(lines)


def _basket_spelling(symbol: str) -> str:
    """A symbol in the spelling the baskets and name table use.

    ``SPX`` becomes ``^GSPC`` and ``000300.SS`` becomes ``000300.SH``; anything
    the protocol cannot resolve passes through as given.
    """
    try:
        ref = to_canonical(symbol)
    except ValueError:
        return symbol
    legacy = to_legacy_api(ref)
    return f"^{legacy}" if is_family_index(ref) else legacy


def _get_index_name(symbol: str) -> str:
    """Get human-readable name for common market indices."""
    index_names = {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ Composite",
        "^DJI": "Dow Jones Industrial",
        "^RUT": "Russell 2000",
        "^VIX": "CBOE Volatility Index",
        "000001.SH": "SSE Composite",
        "399001.SZ": "SZSE Component",
        "000300.SH": "CSI 300",
        "^HSI": "Hang Seng Index",
        "^HSCE": "Hang Seng China Enterprises",
        "^N225": "Nikkei 225",
        "^FTSE": "FTSE 100",
        "^GDAXI": "DAX",
        "^FCHI": "CAC 40",
        "^STOXX50E": "EURO STOXX 50",
    }
    return index_names.get(symbol, symbol)


async def fetch_sector_performance(
    date: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch market sector performance.

    Args:
        date: Analysis date in YYYY-MM-DD format (default: latest available)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """

    def _build_sector_artifact(
        raw_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build structured artifact from raw sector results."""
        sectors = []
        for sector in raw_results:
            sector_name = sector.get("sector", "N/A")
            change_str = sector.get("changePctStr", "0%")
            try:
                change_val = float(change_str.replace("%", "").replace("+", ""))
            except (ValueError, AttributeError):
                change_val = 0.0
            sectors.append({"sector": sector_name, "changePercentage": change_val})
        # Sort descending by performance
        sectors.sort(key=lambda x: x["changePercentage"], reverse=True)
        return {"type": "sector_performance", "sectors": sectors}

    try:
        provider = await get_financial_data_provider()

        # Generate file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        requested_date = date or _venue_today(["^GSPC"])
        date_str = f" ({date})" if date else ""
        header = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Market:** US Stock Market

"""

        # Protocol path — passes date through to FMP's date-aware endpoint;
        # yfinance ignores `target_date` and always returns today's snapshot.
        if provider.financial is not None:
            try:
                results = await provider.financial.get_sector_performance(
                    target_date=date
                )
                if results:
                    logger.debug(f"Retrieved performance data for {len(results)} sectors")
                    actual_date = results[0].get("date") if isinstance(results[0], dict) else None
                    if actual_date and actual_date != requested_date:
                        fallback_notice = (
                            f"> ⚠️ **No data for {requested_date}** "
                            f"(weekend / holiday / not yet published). "
                            f"Showing the most recent available trading day: "
                            f"**{actual_date}**.\n\n"
                        )
                        content = header + fallback_notice + _format_sectors_as_table(results)
                    else:
                        content = header + _format_sectors_as_table(results)
                    return content, _build_sector_artifact(results)
            except Exception:
                logger.exception("Sector performance provider call failed")

        logger.warning(
            "No sector performance data found - endpoint may not be available on this FMP plan"
        )
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** No data available

No sector performance data available for the specified period."""
        return content, {"type": "sector_performance", "sectors": []}

    except Exception as e:
        logger.error(f"Error retrieving sector performance: {e}")
        logger.warning("Sector performance endpoint may require a higher FMP API tier")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        date_str = f" ({date})" if date else ""
        content = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving sector performance data: {str(e)}"""
        return content, {
            "type": "sector_performance",
            "sectors": [],
            "error": str(e),
        }


# Foreign caret-index baskets derive from the protocol's region table, so
# adding an index there routes AND appears in its region basket in one touch.
_FOREIGN_BASKETS: Dict[str, List[str]] = {}
for _sym, _region in CARET_INDEX_REGIONS.items():
    _FOREIGN_BASKETS.setdefault(_region, []).append(_sym)

_REGION_INDEX_BASKETS: Dict[str, List[str]] = {
    "us": ["^GSPC", "^IXIC", "^DJI", "^RUT"],
    "cn": ["000001.SH", "399001.SZ", "000300.SH"],
    **_FOREIGN_BASKETS,
    "global": ["^GSPC", "^IXIC", "^HSI", "^N225", "^FTSE", "^GDAXI"],
}


# Calendar-day lookback feeding the snapshot's day-change and chart context.
_SNAPSHOT_CONTEXT_DAYS = 45


def _format_index_day_row(symbol: str, latest: Dict[str, Any]) -> str:
    """One markdown table row for an index's latest daily bar."""
    close = latest.get("close")
    close_str = f"{close:,.2f}" if close is not None else "N/A"
    change = latest.get("change")
    change_pct = latest.get("changePercent")
    if change is not None and change_pct is not None:
        sign = "+" if change >= 0 else ""
        move_str = f"{sign}{change:,.2f} ({sign}{change_pct:.2f}%)"
    else:
        move_str = "N/A"
    volume = latest.get("volume")
    volume_str = fmt_count(volume) if volume else "—"
    date_str = f"{latest.get('date')} (intraday)" if latest.get("forming") else latest.get("date")
    return (
        f"| {_get_index_name(symbol)} ({symbol}) | {close_str} "
        f"| {move_str} | {volume_str} | {date_str} |"
    )


def _format_index_day_table(rows: List[str], missing: List[str]) -> str:
    """Assemble the snapshot table, noting any symbols that returned no data."""
    lines = [
        "| Index | Close | Day Change | Volume | Date |",
        "|---|---|---|---|---|",
        *rows,
    ]
    if missing:
        lines.append("")
        lines.append(f"_No data: {', '.join(missing)}_")
    return "\n".join(lines)


def _snapshot_is_session(
    snap: Dict[str, Any], session: str, newest: Optional[Dict[str, Any]], tz: Any
) -> bool:
    """Whether *snap* prices the *session* the daily series has not reached.

    Just after the open a snapshot can still hold the previous session's close
    and move. Its print time settles which session it is; without one, a
    previous close equal to the series' last close puts it exactly one session
    ahead of the series.
    """
    printed = print_time(snap)
    if printed is not None:
        return printed.astimezone(tz).date().isoformat() == session
    prev_close = finite_or_none(snap.get("previous_close"))
    last_close = finite_or_none((newest or {}).get("close"))
    if prev_close is None or last_close is None:
        return False
    return math.isclose(prev_close, last_close, rel_tol=1e-6, abs_tol=0.01)


def _forming_bar(
    symbol: str, snap: Dict[str, Any], newest: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Today's row for an index whose daily publisher has not posted it yet.

    A daily-only feed (Tushare posts index closes after the session) leaves the
    card one session behind all day. The snapshot chain has the forming bar's
    level, so when the venue expects a newer daily date than the series carries
    (*newest* is its latest bar) and the snapshot prices that session, the quote
    stands in as the last row, in the bar shape the card reads.
    """
    price = current_price(snap)
    if price is None:
        return None
    clock = clock_for(symbol, True)
    expected = clock.expected_latest_daily_date()
    newest_date = (newest or {}).get("date")
    if newest_date is not None and newest_date >= expected:
        return None
    if not _snapshot_is_session(snap, expected, newest, clock.tz):
        return None
    derived, derived_pct = change_from(price, snap.get("previous_close"))
    change = snap.get("change")
    change_pct = snap.get("change_percent")
    if change is None:
        change = derived
    if change_pct is None:
        change_pct = derived_pct
    return {
        "date": expected,
        "open": snap.get("open"),
        "high": snap.get("high"),
        "low": snap.get("low"),
        "close": price,
        "volume": snap.get("volume"),
        "change": change,
        "changePercent": change_pct,
        "symbol": symbol,
        "forming": True,
    }


async def _fetch_index_day_snapshot(
    basket: List[str],
    target_date: str,
    config: Optional[RunnableConfig] = None,
    snaps: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[str, Dict[str, Any], Optional[str]]:
    """Per-index closes for the last trading day on or before ``target_date``.

    Returns (content, market_indices artifact, resolved snapshot date). The
    artifact keeps the nested per-index shape the frontend charts expect, with
    ``stats.period_change_pct`` carrying the day move rather than a window move.
    ``snaps`` (``snapshot_key`` -> snapshot) supplies today's forming row when
    the daily publisher is a session behind; pass it only for a today request.
    """
    provider = await get_market_data_provider()
    user_id = _get_user_id(config)
    end_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    fetch_start = (end_dt - timedelta(days=_SNAPSHOT_CONTEXT_DAYS)).isoformat()
    snaps = snaps or {}
    # A past date is a closed window; measuring it against now would label it
    # stale. "Today" is each index's own venue date: after 20:00 ET the UTC
    # date is already tomorrow, which would turn today's US request historical.
    def open_ended(symbol: str) -> bool:
        local = venue_local_time(to_canonical(symbol, asset_class=AssetClass.INDEX))
        return target_date >= (local or datetime.now(timezone.utc)).date().isoformat()

    async def fetch_single(symbol: str):
        try:
            raw, source, _ = await provider.get_daily_with_source(
                symbol, from_date=fetch_start, to_date=target_date,
                is_index=True, user_id=user_id,
            )
            # Measured off the bars just returned — the same rows the artifact
            # carries, so describing them costs no second call.
            freshness = (
                measure_daily(symbol, raw, is_index=True, source=source) if open_ended(symbol) else None
            )
            return symbol, _normalize_market_bars(raw, symbol), source, freshness
        except Exception as e:
            logger.warning(f"Error fetching data for index {symbol}: {e}")
            return symbol, None, None, None

    results = await asyncio.gather(*[fetch_single(sym) for sym in basket])

    artifact_indices: Dict[str, Any] = {}
    rows: List[str] = []
    missing: List[str] = []
    snapshot_date: Optional[str] = None
    for symbol, bars, source, freshness in results:
        bars = list(bars or [])
        row_tier = None
        forming = None
        snap = snaps.get(snapshot_key(symbol))
        if snap is not None:
            forming = _forming_bar(symbol, snap, bars[0] if bars else None)
        if forming is not None:
            bars.insert(0, forming)
            source = snap.get("source") or source
            row_tier = snap.get("tier")
            freshness = quote_freshness(snap, asset_class=AssetClass.INDEX)
        if not bars:
            missing.append(symbol)
            continue
        latest = bars[0]  # newest-first
        day_date = latest.get("date")
        if day_date and (snapshot_date is None or day_date > snapshot_date):
            snapshot_date = day_date

        ohlcv = [
            {
                "date": b.get("date"),
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b.get("close"),
                "volume": b.get("volume"),
            }
            for b in reversed(bars)
            if b.get("date")
        ]
        stats = _calculate_price_statistics(bars)
        artifact_indices[symbol] = {
            "name": _get_index_name(symbol),
            "ohlcv": ohlcv,
            "chart_ohlcv": ohlcv,
            "chart_interval": "daily",
            "source": source,
            "tier": row_tier,
            "freshness": freshness.model_dump(mode="json") if freshness else None,
            "stats": {
                "period_change_pct": latest.get("changePercent"),
                "ma_20": stats.get("ma_20"),
                "ma_50": stats.get("ma_50"),
                "volatility": stats.get("volatility"),
            },
        }

        rows.append(_format_index_day_row(symbol, latest))

    if not artifact_indices:
        return (
            "No index data available for the requested date.",
            {"type": "market_indices", "indices": {}},
            None,
        )

    return (
        _format_index_day_table(rows, missing),
        {"type": "market_indices", "indices": artifact_indices},
        snapshot_date,
    )


def _venue_today(symbols: List[str]) -> str:
    """Today on the newest venue clock in *symbols*: what a request with no date means.

    The UTC date is a day off for a venue on the far side of midnight from it:
    after 20:00 ET it already names tomorrow, so the US default asked for a bar
    that cannot exist yet and warned of it every evening. The newest date keeps
    every member of a mixed basket inside the fetch window.
    """
    now = datetime.now(timezone.utc)
    dates = [
        (venue_local_time(resolve_ref(s, AssetClass.INDEX), now) or now).date().isoformat()
        for s in symbols
    ]
    return max(dates, default=now.date().isoformat())


async def fetch_market_overview(
    region: str = "us",
    indices: Optional[List[str]] = None,
    date: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Single-day market snapshot: index closes + sector performance (US only).

    Defaults to the latest trading day; a non-trading ``date`` falls back to
    the prior trading day with a notice in the content.
    """
    region = (region or "us").strip().lower()
    basket = indices or _REGION_INDEX_BASKETS.get(region)
    if basket is None:
        supported = ", ".join(sorted(_REGION_INDEX_BASKETS))
        return (
            f"Unknown region '{region}'. Supported regions: {supported}.",
            {"type": "market_overview", "region": region},
        )

    basket = [_basket_spelling(s) for s in basket]

    today_iso = _venue_today(basket)
    if date:
        try:
            requested = datetime.strptime(date.strip(), "%Y-%m-%d").date().isoformat()
        except ValueError:
            return (
                f"Invalid date '{date}'. Expected YYYY-MM-DD format.",
                {
                    "type": "market_overview",
                    "region": region,
                    "error": f"invalid date: {date}",
                },
            )
    else:
        requested = today_iso

    try:
        # Today's quotes: the live stamp for the text, and the forming daily
        # row for any index whose daily publisher is still a session behind.
        snaps: List[Dict[str, Any]] = []
        if requested == today_iso:
            try:
                provider = await get_market_data_provider()
                snaps = list(await provider.get_snapshots(
                    basket, asset_type="indices", user_id=_get_user_id(config)
                ) or [])
            except Exception as e:
                logger.warning(f"Index snapshots unavailable for {region}: {e}")
        # Providers answer an index caret-free (``GSPC`` for ``^GSPC``).
        by_symbol = {snapshot_key(s.get("symbol")): s for s in snaps if s.get("symbol")}
        idx_content, idx_artifact, snapshot_date = await _fetch_index_day_snapshot(
            basket, requested, config=config, snaps=by_symbol or None
        )

        if region == "us":
            # Align the sector half to the resolved trading day so both halves
            # describe the same session; if the indices half came up empty,
            # let the sector provider resolve the requested date itself.
            sector_date = snapshot_date or (requested if date else None)
            sec_content, sec_artifact = await fetch_sector_performance(date=sector_date)
        else:
            sec_content, sec_artifact = None, None

        parts = [f"## Market Overview — {region.upper()} ({snapshot_date or requested})"]
        if snapshot_date and snapshot_date != requested:
            parts.append(
                f"> ⚠️ **No daily bar for {requested}** "
                f"(weekend / holiday / not yet published). "
                f"Showing the last trading day: **{snapshot_date}**."
            )
        parts.append(idx_content)
        artifact: Dict[str, Any] = {
            "type": "market_overview",
            "region": region,
            "date": snapshot_date or requested,
            "indices": idx_artifact,
        }
        if sec_content is not None:
            parts.append(sec_content)
            artifact["sectors"] = sec_artifact
        else:
            parts.append("_Sector breakdown unavailable for this region (US only)._")

        # Live index-level stamp during open sessions (best-effort, never
        # raises); skipped for historical snapshots.
        if snaps:
            try:
                stamp = build_live_stamp(snaps, asset_class=AssetClass.INDEX)
            except Exception:
                stamp = None
            if stamp:
                parts.insert(0, stamp)
        return "\n\n".join(parts), artifact
    except Exception as e:
        logger.error(f"Error building market overview for region {region}: {e}")
        return (
            f"## Market Overview — {region.upper()}\n"
            f"**Status:** Error\n\n"
            f"Error retrieving market overview: {str(e)}",
            {"type": "market_overview", "region": region, "error": str(e)},
        )
