"""Protocol-native progressive bars endpoint (Phase 4).

``GET /api/v1/market-data/bars/{instrument}?schema=ohlcv-1m`` serves one
instrument's OHLCV series as a protocol ``Series`` (header + records) with
three access modes over the same envelope-v4 cache the legacy router uses:

- **default** (no cursor): the live series.
- **after=<watermark_ms>**: forward delta poll — the live fetch, records
  filtered to bars at or after the cursor (inclusive: the cursor bar was
  forming at the last poll, so its update must be re-delivered).
- **before=<iso-date>**: older-history paging by period-aligned windows so
  each page maps to an immutable ``:{from}:{to}`` chunk key.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from src.data_client.normalize import publisher_lineage, served_display_unit
from src.market_protocol import (
    OHLCV_SCHEMAS,
    AssetClass,
    OhlcvBar,
    Series,
    SeriesHeader,
    display_decimals_for,
    to_canonical,
    to_legacy_api,
)
from src.market_protocol.intervals import is_intraday_schema, legacy_for_schema
from src.server.services.cache._instrument_clock import clock_for
from src.server.services.cache.daily_cache_service import DailyCacheService
from src.server.services.cache.intraday_cache_service import IntradayCacheService
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/market-data",
    tags=["market-data"],
)

# Period family per schema: the aligned window a `before=` page spans back to.
# 1m–30m → ISO week, 1h/4h → calendar month, 1d → calendar year.
_WEEK_SCHEMAS = {"ohlcv-1m", "ohlcv-5m", "ohlcv-15m", "ohlcv-30m"}
_MONTH_SCHEMAS = {"ohlcv-1h", "ohlcv-4h"}

# The wire keeps its pre-refactor compact shape: OhlcvBar's optional enrichment
# fields (vwap/trades) and SeriesHeader's schema_version / rich-Coverage fields
# stay implicit until a producer populates them, so warm clients see a stable
# series. The models remain the single construction authority.
_WIRE_EXCLUDE = {
    "header": {
        "schema_version": True,
        "coverage": {"requested_start", "requested_end", "returned_start",
                     "returned_end", "is_complete", "gaps"},
    },
    "records": {"__all__": {"vwap", "trades"}},
}


def _bar_time(bar: dict[str, Any]) -> int:
    """Bar anchor ms: ``ts_event`` (protocol) with legacy ``time`` fallback."""
    return bar.get("ts_event") or bar.get("time") or 0


def _local_date(ms: int, tz) -> str:
    """ISO date of a Unix-ms instant in the instrument's calendar tz."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(tz).date().isoformat()


def _period_start(right_edge: date, schema: str) -> date:
    """Aligned start of the period containing ``right_edge`` for this schema."""
    if schema in _WEEK_SCHEMAS:
        return right_edge - timedelta(days=right_edge.weekday())  # Monday
    if schema in _MONTH_SCHEMAS:
        return right_edge.replace(day=1)
    if schema == "ohlcv-1d":
        return right_edge.replace(month=1, day=1)
    return right_edge  # unknown → single-day period (defensive)


def _paging_window(cursor: str, schema: str) -> tuple[str, str]:
    """Resolve a ``before=`` cursor to a ``(from_date, to_date)`` window.

    The cursor is the ISO date of the exclusive right edge; the window is
    ``[period_start, cursor - 1 day]`` and ``period_start`` doubles as the
    next (older) cursor.
    """
    try:
        cursor_date = date.fromisoformat(cursor)
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail=f"Invalid cursor: {cursor!r}") from None
    right_edge = cursor_date - timedelta(days=1)
    period_start = _period_start(right_edge, schema)
    return period_start.isoformat(), right_edge.isoformat()


def _shape_records(bars: list[dict[str, Any]], phase: str, live: bool) -> list[OhlcvBar]:
    """Bars → protocol ``OhlcvBar`` records, stamping ``is_final``: settled
    everywhere except the live forming bar at the watermark. ``OhlcvBar`` surfaces
    ``ts_event`` + the legacy ``time`` alias; the router only owns is_final."""
    n = len(bars)
    out: list[OhlcvBar] = []
    for i, bar in enumerate(bars):
        is_final = (phase == "closed") if (live and i == n - 1) else True
        out.append(OhlcvBar.model_validate({**bar, "is_final": is_final}))
    return out


def _to_ms(ts: Optional[float]) -> Optional[int]:
    """Envelope headers store asof/fetched_at as Unix seconds; the wire is ms."""
    if ts is None:
        return None
    return int(ts * 1000) if ts < 1e12 else int(ts)


def _series_header(ref, schema: str, env_header: dict[str, Any], result) -> SeriesHeader:
    """Protocol Series header: lineage from the fetched envelope, currency from
    the InstrumentRef, ``schema`` as the requested id.

    Lineage falls back through :func:`publisher_lineage` for a known-but-partial
    publisher, and to its neutral default only when the header is truly absent.
    ``display_unit`` describes the served (major-unit) values.
    """
    treatment, tier = publisher_lineage(env_header.get("publisher"))
    return SeriesHeader(
        instrument_key=ref.instrument_key,
        schema_id=schema,
        price_treatment=env_header.get("price_treatment") or treatment,
        publisher=env_header.get("publisher"),
        tier=env_header.get("tier") or tier,
        feed_scope=env_header.get("feed_scope", "composite"),
        price_currency=ref.price_currency,
        display_decimals=display_decimals_for(ref.price_currency, ref.asset_class),
        display_unit=served_display_unit(ref.display_unit),
        latest_trading_date=env_header.get("latest_trading_date"),
        revision=env_header.get("revision", 0),
        asof=_to_ms(env_header.get("asof")),
        coverage=env_header.get("coverage") or {"truncated": bool(result.truncated)},
        fetched_at=_to_ms(env_header.get("fetched_at")),
        watermark=env_header.get("watermark", result.watermark),
    )


async def _fetch(
    intraday: bool,
    is_index: bool,
    symbol: str,
    interval: str,
    user_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    live: Optional[bool] = None,
):
    """Route to the intraday or daily cache service, live or windowed.

    ``live=False`` pins a paging window to a date-suffixed historical key.
    Without it the cache's date heuristic classifies a right edge within
    ~12h of today as live, and the page would read — and on a miss write —
    the window-less live series key.
    """
    if intraday:
        svc = IntradayCacheService.get_instance()
        method = svc.get_index_intraday if is_index else svc.get_stock_intraday
        return await method(
            symbol=symbol, interval=interval,
            from_date=from_date, to_date=to_date, user_id=user_id, live=live,
        )
    svc = DailyCacheService.get_instance()
    return await svc.get_stock_daily(
        symbol=symbol, from_date=from_date, to_date=to_date,
        is_index=is_index, user_id=user_id, live=live,
    )


@router.get(
    "/bars/{instrument}",
    summary="Get progressive OHLCV bars",
    description="Protocol-native bars for any instrument spelling, with live / delta / history modes.",
)
async def get_bars(
    instrument: str,
    user_id: CurrentUserId,
    schema: str = Query(..., description="OHLCV schema id (ohlcv-1m … ohlcv-1d)"),
    asset_class: Optional[str] = Query(
        None,
        description="Asset-class hint; 'index' forces index routing (equity/index only)",
    ),
    after: Optional[int] = Query(
        None, description="Delta poll: return bars with ts_event >= after (ms, inclusive)"
    ),
    before: Optional[str] = Query(
        None, description="History paging cursor: ISO date, exclusive right edge"
    ),
) -> dict:
    """Serve one instrument's OHLCV series as a protocol Series (header + records)."""
    if schema not in OHLCV_SCHEMAS:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown schema {schema!r}. Supported: {', '.join(OHLCV_SCHEMAS)}",
        )
    if schema == "ohlcv-1s":
        # ohlcv-1s stays in the protocol as the WS forming-bar record schema,
        # but no REST provider serves second bars — a cache-miss fetch would
        # surface a provider error as a 500. Reject up front instead.
        raise HTTPException(
            status_code=422,
            detail="ohlcv-1s is WS-only (forming-bar stream); the REST bars endpoint serves ohlcv-1m and coarser.",
        )
    interval = legacy_for_schema(schema)
    intraday = is_intraday_schema(schema)

    ac_hint = (asset_class or "").strip().lower()
    if ac_hint and ac_hint not in ("index", "equity", "stock"):
        # crypto/fx hints would silently misroute as US equities — refuse
        # until those asset classes are actually served here.
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported asset_class {asset_class!r}; this endpoint serves equities and indexes.",
        )
    index_hint = ac_hint == "index"
    equity_hint = ac_hint in ("equity", "stock")
    try:
        hint = (
            AssetClass.INDEX if index_hint
            else AssetClass.EQUITY if equity_hint
            else None
        )
        ref = to_canonical(instrument, asset_class=hint)
    except Exception:
        raise HTTPException(status_code=422, detail=f"Invalid instrument: {instrument!r}") from None
    is_index = ref.asset_class is AssetClass.INDEX
    legacy_symbol = to_legacy_api(ref)

    # --- resolve mode + fetch ---
    if before is not None:
        from_date, to_date = _paging_window(before, schema)
        result = await _fetch(
            intraday, is_index, legacy_symbol, interval, user_id,
            from_date=from_date, to_date=to_date, live=False,
        )
        mode = "before"
        next_cursor = from_date
    else:
        result = await _fetch(intraday, is_index, legacy_symbol, interval, user_id)
        mode = "after" if after is not None else "default"

    if result.error:
        raise HTTPException(status_code=500, detail=result.error)

    # The clock is the phase authority when the cache result doesn't carry one
    # (windowed fetches) — the old `or "closed"` default could stamp a closed
    # phase on an open venue.
    clock = clock_for(legacy_symbol, is_index)
    phase = result.market_phase or clock.market_phase()
    bars = result.data or []
    if mode == "after":
        # Inclusive of the cursor bar: the bar AT `after` was the forming bar
        # when the client last polled, so its OHLCV may have moved (or settled)
        # since — a strictly-greater filter would never re-deliver those
        # updates. Clients merge by timestamp, so the overlap is idempotent.
        bars = [b for b in bars if _bar_time(b) >= after]
    records = _shape_records(bars, phase, live=(mode != "before"))

    # Header lineage rides on the fetch result — no second cache read.
    env_header = result.header or {}
    header = _series_header(ref, schema, env_header, result)
    series = Series(header=header, records=records).model_dump(
        by_alias=True, mode="json", exclude=_WIRE_EXCLUDE,
    )

    # --- paging block ---
    # next_cursor is the oldest date this response covers (its inclusive left
    # edge); the client passes it back as `before=` to page strictly older.
    if mode == "before":
        has_more = len(result.data or []) > 0
        page = {"next_cursor": next_cursor if has_more else None, "has_more": has_more}
    elif mode == "after":
        page = {"next_cursor": None, "has_more": False}
    elif bars:
        page = {"next_cursor": _local_date(_bar_time(bars[0]), tz=clock.tz), "has_more": True}
    else:
        page = {"next_cursor": None, "has_more": False}

    return {
        "series": series,
        "page": page,
        # market_phase mirrors the legacy CacheMetadata field: the venue's
        # calendar-derived phase, driving closed-market presentation client-side.
        # next_change_at (Unix ms, null for 24/7 venues) is the phase's next
        # calendar boundary, letting clients flip presentation exactly at the
        # bell instead of on the next poll.
        "cache": {
            "cached": bool(result.cached),
            "cache_key": result.cache_key,
            "market_phase": phase,
            "next_change_at": clock.next_phase_change_ms(),
        },
    }
