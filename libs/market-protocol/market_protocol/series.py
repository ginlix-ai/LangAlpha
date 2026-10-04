"""Rows to :class:`Series`: the one header builder every publisher shares.

Lineage is an argument, not a lookup: the publisher is the only party that
knows what its feed carries, so it declares ``price_treatment`` and ``tier``
at the call.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from typing import Any

from .enums import PriceTreatment, Tier
from .models import InstrumentRef, OhlcvBar, Series, SeriesHeader
from .symbology import MINOR_UNIT_PLACES, display_decimals_for


# The widest timestamp an int64 reader can hold.
_MAX_TS_MS = 2**63 - 1


def served_display_unit(display_unit: str | None) -> str | None:
    """Wire ``display_unit`` for SERVED values: a minor unit is converted to its major unit, so cleared."""
    return None if display_unit in MINOR_UNIT_PLACES else display_unit


def served_display_decimals(ref: InstrumentRef) -> int:
    """Wire ``display_decimals`` for SERVED values, for any header built outside :func:`build_series`.

    A pence price keeps its own decimals once served in pounds: 54.32p is 0.5432.
    """
    places = MINOR_UNIT_PLACES.get(ref.display_unit or "", 0)
    return display_decimals_for(ref.price_currency, ref.asset_class) + places


def _time(row: dict[str, Any]) -> Any:
    return row["time"]


def _finite(value: Any) -> float | None:
    # A bool is a number to Python but never a price or a count.
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return out if math.isfinite(out) else None


def _count(value: Any) -> int | None:
    out = _finite(value)
    return int(out) if out is not None and out >= 0 and out.is_integer() else None


def build_series(
    rows: list[dict[str, Any]],
    *,
    ref: InstrumentRef,
    schema: str,
    publisher: str,
    price_treatment: PriceTreatment,
    tier: Tier,
    ts_of: Callable[[dict[str, Any]], int] = _time,
) -> Series:
    """Canonical ``{time, open, high, low, close, volume[, vwap, trades, is_final]}`` rows to a Series.

    Rows carry prices in the venue's quote unit (pence on XLON, divided by 100
    here per ``ref.display_unit``, with ``display_decimals`` widened by the
    same two places) and ``ts_of`` returns the window open in Unix
    milliseconds. A row whose timestamp is missing, unreadable or out of
    range, or without a finite open, high, low and close, is dropped rather
    than charted as a zero or failing the batch; a repeated timestamp keeps
    its last row, the later revision of that window.
    """
    # An exact power of ten rounds once, where an inexact 0.01 leaves noise
    # (123.45 * 0.01 != 1.2345).
    divisor = 10 ** MINOR_UNIT_PLACES.get(ref.display_unit or "", 0)
    by_ts: dict[int, OhlcvBar] = {}
    for row in rows:
        try:
            raw = ts_of(row)
            # A bool is an int to Python, so int(True) would chart at 1 ms.
            ts = None if isinstance(raw, bool) else int(raw)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if ts is None or not 0 < ts <= _MAX_TS_MS:
            continue
        o, h, lo, c = (_finite(row.get(k)) for k in ("open", "high", "low", "close"))
        if o is None or h is None or lo is None or c is None:
            continue
        vwap = _finite(row.get("vwap"))
        # Volume counts shares or contracts, so a negative one is a sentinel, not data.
        volume = _finite(row.get("volume"))
        is_final = row.get("is_final")
        by_ts[ts] = OhlcvBar(
            ts_event=ts,
            open=o / divisor,
            high=h / divisor,
            low=lo / divisor,
            close=c / divisor,
            volume=volume if volume is not None and volume >= 0 else None,
            vwap=vwap / divisor if vwap is not None else None,
            trades=_count(row.get("trades")),
            # Only a real bool says the window closed; anything else reads as forming.
            is_final=is_final if isinstance(is_final, bool) else False,
        )
    records = [by_ts[ts] for ts in sorted(by_ts)]
    # The header describes the prices as served: GBX rows leave here in GBP,
    # and a USD-quoted listing on a GBP venue serves USD.
    currency = ref.price_currency
    now_ms = int(time.time() * 1000)
    header = SeriesHeader(
        instrument_key=ref.instrument_key,
        schema_id=schema,
        price_treatment=price_treatment,
        publisher=publisher,
        tier=tier,
        price_currency=currency,
        display_decimals=served_display_decimals(ref),
        display_unit=served_display_unit(ref.display_unit),
        asof=now_ms,
        fetched_at=now_ms,
        watermark=records[-1].ts_event if records else None,
    )
    return Series(header=header, records=records)
