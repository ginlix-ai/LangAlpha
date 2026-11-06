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
from src.data_client import get_market_data_provider
from src.data_client.freshness import measure_daily
from src.data_client.instrument_clock import clock_for
from src.data_client.market_data_provider import MarketNotCovered, snapshot_key
from src.data_client.normalize import bar_in_progress, change_from, series_in_progress, series_lineage
from market_protocol import (
    CARET_INDEX_REGIONS,
    AssetClass,
    InvalidSymbol,
    to_canonical,
    to_request_spelling,
)

from ._shared import _get_user_id, _mark_failure, _mark_no_data, _normalize_market_bars
from .prices import _calculate_price_statistics

logger = logging.getLogger(__name__)


# The S&P 500's eleven GICS sectors, each priced by its Select Sector SPDR fund.
_SECTOR_FUNDS: Dict[str, str] = {
    "XLK": "Information Technology",
    "XLF": "Financials",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
    "XLC": "Communication Services",
    "XLI": "Industrials",
    "XLP": "Consumer Staples",
    "XLE": "Energy",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
    "XLB": "Materials",
}


# Calendar days back from the asked date: a long holiday weekend still leaves a session.
_SECTOR_LOOKBACK_DAYS = 10


def _format_sectors_as_table(
    ranked: List[Tuple[str, float]], session: str, in_progress: bool, failed: List[str]
) -> str:
    """The sector table from *ranked* ``(fund, change)`` pairs, best first, with
    the session it measures, the funds whose read *failed*, and the sectors it has
    no bar for."""
    when = f"{session} (in progress)" if in_progress else session
    lines = [
        f"## Sector Performance: S&P 500 sectors, {when}",
        "Day change of each sector's Select Sector SPDR fund against its previous close.",
        "",
        "| Sector | Fund | Change |",
        "|---|---|---|",
        *(f"| {_SECTOR_FUNDS[fund]} | {fund} | {pct:+.2f}% |" for fund, pct in ranked),
    ]
    shown = dict(ranked)
    missing = [f"{name} ({fund})" for fund, name in _SECTOR_FUNDS.items() if fund not in shown and fund not in failed]
    if missing:
        lines += ["", f"_No bar for {session}: {', '.join(missing)}_"]
    if failed:
        lines += ["", f"_Read failed: {', '.join(f'{_SECTOR_FUNDS[fund]} ({fund})' for fund in failed)}_"]
    (best, best_pct), (worst, worst_pct) = ranked[0], ranked[-1]
    lines += [
        "",
        f"**Best Performing:** {_SECTOR_FUNDS[best]} ({best_pct:+.2f}%)",
        f"**Worst Performing:** {_SECTOR_FUNDS[worst]} ({worst_pct:+.2f}%)",
    ]
    return "\n".join(lines)


def _basket_spelling(symbol: str) -> str:
    """A symbol in the spelling the baskets and name table use.

    ``SPX`` becomes ``^GSPC`` and ``000300.SS`` becomes ``000300.SH``; anything
    the protocol cannot resolve passes through as given.
    """
    try:
        return to_request_spelling(to_canonical(symbol))
    except ValueError:
        return symbol


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
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Each S&P 500 sector's day change on the last session on or before *date*
    (default: the latest), from its Select Sector SPDR fund's daily closes.

    Returns (content, sector_performance artifact). A fund with no bar for the
    newest session among them is left out rather than shown for another day.
    The change reads only the close, which settles at the regular close even
    though the day's aggregate keeps taking after-hours volume.
    """
    provider = await get_market_data_provider()
    user_id = _get_user_id(config)
    end = date or _venue_today(["^GSPC"])
    start = (datetime.strptime(end, "%Y-%m-%d").date() - timedelta(days=_SECTOR_LOOKBACK_DAYS)).isoformat()

    async def latest_bar(symbol: str) -> Optional[Dict[str, Any]]:
        raw, source, _ = await provider.get_daily_with_source(
            symbol, from_date=start, to_date=end, is_index=False, user_id=user_id,
        )
        bars = _normalize_market_bars(raw, symbol)
        if not bars:
            return None
        ref = resolve_ref(symbol, AssetClass.EQUITY)
        _treatment, tier = series_lineage(source, ref)
        forming = bar_in_progress(bars[0].get("date"), ref, "1day", tier=tier, regular_only=True)
        return {**bars[0], "in_progress": forming}

    results = await asyncio.gather(*map(latest_bar, _SECTOR_FUNDS), return_exceptions=True)
    bars, errors = {}, {}
    for symbol, result in zip(_SECTOR_FUNDS, results):
        if isinstance(result, BaseException):
            logger.warning(f"Sector fund {symbol} unavailable: {result}")
            errors[symbol] = result
        elif isinstance(result, dict) and result.get("changePercent") is not None:
            bars[symbol] = result
    session = max((bar["date"] for bar in bars.values()), default=None)
    if session is None and errors:
        # With no fund priced, a failed read may be what hid the session.
        error = next(iter(errors.values()))
        _mark_failure(error)
        return (
            f"## Sector Performance\nSector fund data unavailable: {len(errors)} of"
            f" {len(_SECTOR_FUNDS)} fund reads failed ({error}).",
            {"type": "sector_performance", "sectors": [], "error": str(error)},
        )
    if session is None:
        return (
            "## Sector Performance\nNo sector fund data available for the requested date.",
            {"type": "sector_performance", "sectors": []},
        )
    shown = {fund: bar for fund, bar in bars.items() if bar["date"] == session}
    ranked = sorted(
        ((fund, bar["changePercent"]) for fund, bar in shown.items()), key=lambda kv: kv[1], reverse=True
    )
    forming = any(bar["in_progress"] for bar in shown.values())
    return (
        _format_sectors_as_table(ranked, session, forming, list(errors)),
        {
            "type": "sector_performance",
            "date": session,
            "in_progress": forming,
            "sectors": [{"sector": _SECTOR_FUNDS[fund], "changePercentage": pct} for fund, pct in ranked],
        },
    )


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
    date_str = f"{latest.get('date')} (in progress)" if latest.get("forming") else latest.get("date")
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
            return symbol, _normalize_market_bars(raw, symbol), source, freshness, None
        except Exception as e:
            logger.warning(f"Error fetching data for index {symbol}: {e}")
            return symbol, None, None, None, e

    results = await asyncio.gather(*[fetch_single(sym) for sym in basket])

    artifact_indices: Dict[str, Any] = {}
    rows: List[str] = []
    missing: List[str] = []
    snapshot_date: Optional[str] = None
    for symbol, bars, source, freshness, _ in results:
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
        ref = resolve_ref(symbol, AssetClass.INDEX)
        bars[0] = {**bars[0], "forming": series_in_progress(source, ref, bars[0].get("date"), "1day")}
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
        # Each index's error is caught so the rest of the basket still shows,
        # which leaves none to mark the run when every one of them failed. The
        # worst marks it: a source failing, then an uncovered market, then a
        # malformed symbol.
        errors = [e for *_, e in results if e is not None]
        if errors:
            _mark_failure(min(
                errors,
                key=lambda e: (isinstance(e, InvalidSymbol), isinstance(e, MarketNotCovered)),
            ))
        else:
            _mark_no_data()
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


def _no_bar_reason(target: str, opened: str, basket: List[str]) -> str:
    """Why *target* has no daily bar, given the newest session that has *opened*."""
    if not any(clock_for(s, True).is_trading_day(datetime.strptime(target, "%Y-%m-%d").date()) for s in basket):
        return "weekend / holiday"
    if target > opened:
        return "the session has not opened yet"
    if target == opened:
        return "the session has opened but has no published bar yet"
    return "missing from the source"


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
    asked: Optional[str] = None
    if date:
        try:
            asked = datetime.strptime(date.strip(), "%Y-%m-%d").date().isoformat()
        except ValueError:
            return (
                f"Invalid date '{date}'. Expected YYYY-MM-DD format.",
                {
                    "type": "market_overview",
                    "region": region,
                    "error": f"invalid date: {date}",
                },
            )
    window_end = asked or today_iso
    # A request with no date means the newest session that has opened: before
    # the open that is the previous one, which no notice should call a fallback.
    opened = max(
        clock_for(s, True).expected_latest_daily_date(datetime.now(timezone.utc)) for s in basket
    )
    target = asked or opened

    try:
        # Today's quotes: the live stamp for the text, and the forming daily
        # row for any index whose daily publisher is still a session behind.
        snaps: List[Dict[str, Any]] = []
        if window_end == today_iso:
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
            basket, window_end, config=config, snaps=by_symbol or None
        )

        if region == "us":
            # Align the sector half to the resolved trading day so both halves
            # describe the same session; if the indices half came up empty,
            # the sectors read up to the session the basket targets.
            sector_date = snapshot_date or target
            sec_content, sec_artifact = await fetch_sector_performance(date=sector_date, config=config)
        else:
            sec_content, sec_artifact = None, None

        # With no index row, the sector funds' session is the one shown.
        shown = snapshot_date or (sec_artifact or {}).get("date")
        fell_back = shown is not None and shown < target
        parts = [f"## Market Overview — {region.upper()} ({shown or window_end})"]
        if fell_back:
            parts.append(
                f"> ⚠️ **No daily bar for {target}** ({_no_bar_reason(target, opened, basket)}). "
                f"Showing the last trading day: **{shown}**."
            )
        parts.append(idx_content)
        artifact: Dict[str, Any] = {
            "type": "market_overview",
            "region": region,
            "date": shown or window_end,
            **({"requested_date": target} if fell_back else {}),
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
        _mark_failure(e)
        return (
            f"## Market Overview — {region.upper()}\n"
            f"**Status:** Error\n\n"
            f"Error retrieving market overview: {str(e)}",
            {"type": "market_overview", "region": region, "error": str(e)},
        )
