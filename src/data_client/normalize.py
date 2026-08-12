"""Shared OHLCV bar normalization.

Single source of truth for converting the canonical
``{time, open, high, low, close, volume}`` shape to display format with
exchange-local timestamps.

Also holds the legacy-path minor-unit scale helpers: providers that quote a
venue in a currency's minor unit (XLON equities quote GBX/pence) multiply
price-like fields by ``minor_unit_scale`` on the legacy bar/snapshot path so
the wire values are major units. The protocol path applies the same rule
independently inside each provider's ``normalize_series`` — the two never
chain, so conversion happens exactly once per path.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from src.market_protocol import (
    InstrumentRef,
    OhlcvBar,
    Series,
    SeriesHeader,
    display_decimals_for,
    to_canonical,
)
from src.market_protocol.enums import PriceTreatment, Tier

from .market_data_provider import symbol_timezone

# Snapshot fields whose values carry a price and therefore scale with the
# quote's minor-currency unit. change_percent (and pre/post percents) are
# ratios — scale-invariant; volume is a share count — never scaled.
_SNAPSHOT_PRICE_FIELDS = (
    "price", "change", "previous_close", "open", "high", "low",
)

# Publisher-declared series lineage: the (price_treatment, tier) each provider's
# feed carries. Declared next to the providers, ONE source of truth — the cache
# header builder (``_ohlcv_envelope``) and the router both import ``publisher_lineage``
# so no layer re-mirrors these constants.
PUBLISHER_LINEAGE: dict[str, tuple[PriceTreatment, Tier]] = {
    "ginlix-data": (PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME),
    "fmp": (PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME),
    "yfinance": (PriceTreatment.SPLIT_ADJUSTED, Tier.DELAYED_15M),
}

# Conservative default for an unknown/absent publisher (matches the legacy
# hardcoded header fallback).
_DEFAULT_LINEAGE: tuple[PriceTreatment, Tier] = (PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME)


def publisher_lineage(publisher: str | None) -> tuple[PriceTreatment, Tier]:
    """(price_treatment, tier) declared for *publisher*; conservative default."""
    return PUBLISHER_LINEAGE.get(publisher or "", _DEFAULT_LINEAGE)


def served_display_unit(display_unit: str | None) -> str | None:
    """Wire ``display_unit`` describing the SERVED values.

    Every serving path converts GBX (pence) to major units before emitting, so
    the pence hint would misdescribe the values — cleared to None for GBX venues.
    """
    return None if display_unit == "GBX" else display_unit


def build_series(
    rows: list[dict[str, Any]],
    *,
    ref: InstrumentRef,
    schema: str,
    publisher: str,
    ts_of: Callable[[dict[str, Any]], int],
) -> Series:
    """Canonical raw-rows → protocol :class:`Series`. The one series/header builder.

    Providers supply their publisher name and a timestamp extractor (FMP parses
    exchange-local wall-clock strings; others read epoch-ms ``time``). Lineage
    comes from :func:`publisher_lineage`; GBX-quoted venues convert to major
    units and the served ``display_unit`` hint is cleared. Records with a
    non-positive timestamp are dropped and the rest sorted ascending.
    """
    treatment, tier = publisher_lineage(publisher)
    scale = 0.01 if ref.display_unit == "GBX" else 1.0
    records = []
    for row in rows:
        ts = ts_of(row)
        if ts <= 0:
            continue
        records.append(OhlcvBar(
            ts_event=ts,
            open=float(row.get("open", 0.0)) * scale,
            high=float(row.get("high", 0.0)) * scale,
            low=float(row.get("low", 0.0)) * scale,
            close=float(row.get("close", 0.0)) * scale,
            volume=float(row["volume"]) if row.get("volume") is not None else None,
        ))
    records.sort(key=lambda r: r.ts_event)
    now_ms = int(time.time() * 1000)
    header = SeriesHeader(
        instrument_key=ref.instrument_key,
        schema_id=schema,
        price_treatment=treatment,
        publisher=publisher,
        tier=tier,
        price_currency=ref.currency,
        display_decimals=display_decimals_for(ref.currency, ref.asset_class),
        display_unit=served_display_unit(ref.display_unit),
        asof=now_ms,
        fetched_at=now_ms,
        watermark=records[-1].ts_event if records else None,
    )
    return Series(header=header, records=records)


def minor_unit_scale(symbol: str) -> float:
    """Legacy-path price scale for *symbol*: 0.01 when quoted in GBX (pence).

    Resolved once per fetch from the canonical instrument's ``display_unit``
    (XLON equities quote in pence). Falls back to 1.0 for anything unresolvable.
    """
    try:
        return 0.01 if to_canonical(symbol).display_unit == "GBX" else 1.0
    except Exception:
        return 1.0


def scale_price(value: Any, scale: float) -> Any:
    """Return ``value * scale`` for numerics; pass None/non-numeric through.

    A ``scale`` of 1.0 returns the value untouched (identity, no float cast),
    preserving exact legacy behavior for non-pence instruments.
    """
    if scale == 1.0 or value is None:
        return value
    try:
        return float(value) * scale
    except (TypeError, ValueError):
        return value


def scale_snapshot_prices(snap: dict, scale: float) -> dict:
    """In-place ×scale of price-like snapshot fields; percents/volume untouched."""
    if scale != 1.0:
        for field in _SNAPSHOT_PRICE_FIELDS:
            if snap.get(field) is not None:
                snap[field] = scale_price(snap[field], scale)
    return snap


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_bars(
    bars: list[dict],
    symbol: str,
    *,
    intraday: bool = False,
) -> list[dict]:
    """Convert bars from internal format (Unix ms) to display format.

    Timestamps are converted to exchange-local time for the given symbol.
    Output: ``{date, open, high, low, close, volume}``, descending by date.
    """
    tz = symbol_timezone(symbol)
    normalized = []
    for bar in bars:
        ts = bar.get("time") or bar.get("t")
        if ts is not None:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(tz)
            fmt = "%Y-%m-%d %H:%M:%S" if intraday else "%Y-%m-%d"
            date_str = dt.strftime(fmt)
        else:
            date_str = bar.get("date", "")
        normalized.append({
            "date": date_str,
            "open": _as_float(bar.get("open") or bar.get("o")),
            "high": _as_float(bar.get("high") or bar.get("h")),
            "low": _as_float(bar.get("low") or bar.get("l")),
            "close": _as_float(bar.get("close") or bar.get("c")),
            "volume": _as_float(bar.get("volume") or bar.get("v")),
        })
    normalized.sort(key=lambda r: r["date"], reverse=True)
    return normalized
