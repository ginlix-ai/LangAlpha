"""Instrument-aware display helpers for market-data tool output.

Venue labels, US-session gating, and the market-status quote line — all derived
from the resolved :class:`InstrumentRef`, never re-parsed from the raw symbol.
Callers resolve the ref once via :func:`resolve_ref` and thread it through; each
helper falls back to the legacy US-centric output when the ref is missing, so
display formatting never fails on an unrecognized symbol.
"""

from datetime import datetime, timezone
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from src.market_protocol import (
    AssetClass,
    InstrumentRef,
    MarketPhase,
    display_decimals_for,
    to_canonical,
)
from src.market_protocol.calendars import get_calendar

from .currency import DisplaySpec


def resolve_ref(symbol: Optional[str]) -> Optional[InstrumentRef]:
    """Canonicalize a symbol to its ``InstrumentRef``; ``None`` if empty or
    unresolvable.

    Lets each fetch entry resolve once and pass the ref to the display helpers
    instead of re-parsing the string in every helper.
    """
    if not symbol:
        return None
    try:
        return to_canonical(symbol)
    except Exception:
        return None


def _symbol_currency(ref: Optional[InstrumentRef]) -> DisplaySpec:
    """Display spec (currency + decimals) for a resolved instrument.

    Decimals come from ``display_decimals_for(price_currency, asset_class)`` so
    the protocol table is the sole authority. Falls back to USD / 2 decimals for a
    missing ref or anything the registry cannot price, so display never fails.
    """
    # TODO: display_unit (e.g. GBX quotes on XLON) is not yet consulted here.
    if ref is None:
        return DisplaySpec(None, 2)
    try:
        return DisplaySpec(
            ref.price_currency,
            display_decimals_for(ref.price_currency, ref.asset_class),
        )
    except Exception:
        return DisplaySpec(None, 2)


# MIC (listing venue) -> human "Market:" label for report headers. US and any
# unresolved USD listing intentionally read "US Stock" so US output stays
# byte-identical to the legacy hardcoded header.
_MIC_MARKET_LABELS: Dict[str, str] = {
    "XNYS": "US Stock",
    "XNAS": "US Stock",
    "XHKG": "HK Stock",
    "XSHG": "A-Share",
    "XSHE": "A-Share",
    "XLON": "UK Stock",
    "XTKS": "Japan Stock",
    "XTSE": "Canada Stock",
    "XASX": "Australia Stock",
    "XPAR": "France Stock",
    "XETR": "Germany Stock",
    "XAMS": "Netherlands Stock",
    "XMIL": "Italy Stock",
    "XMAD": "Spain Stock",
    "XSWX": "Switzerland Stock",
    "XKRX": "Korea Stock",
    "XKOS": "Korea Stock",
    "XTAI": "Taiwan Stock",
    "XSES": "Singapore Stock",
    "XBOM": "India Stock",
    "XNSE": "India Stock",
}


def _market_label(ref: Optional[InstrumentRef]) -> str:
    """Human 'Market:' header label for a resolved listing.

    US / unresolved-USD listings read "US Stock" (byte-compatible with the legacy
    header); other venues name their market. Non-equity asset classes get a
    generic label.
    """
    if ref is None:
        return "US Stock"
    if ref.asset_class is AssetClass.INDEX:
        return "Index"
    if ref.asset_class is AssetClass.CRYPTO:
        return "Crypto"
    if ref.asset_class is AssetClass.FX:
        return "FX"
    label = _MIC_MARKET_LABELS.get(ref.mic)
    if label:
        return label
    return "US Stock" if (ref.price_currency or "USD") == "USD" else f"{ref.price_currency} Stock"


def _is_us_clock(ref: Optional[InstrumentRef]) -> bool:
    """True when the US-Eastern session clock/phase is meaningful for a listing.

    ``get_market_session()`` reports US-Eastern phases only; applying them to a
    non-US listing would misstate its session. Defaults to True for a missing ref
    (matches the legacy US fallback).
    """
    if ref is None:
        return True
    return ref.tz == "America/New_York"


# MarketPhase -> display label for the "Market Status:" quote line.
_PHASE_LABELS: Dict[MarketPhase, str] = {
    MarketPhase.PRE: "Pre-Market",
    MarketPhase.REGULAR: "Regular Hours",
    MarketPhase.LUNCH: "Lunch Break",
    MarketPhase.POST: "After-Hours",
    MarketPhase.CLOSED: "Market Closed",
    MarketPhase.HALTED: "Halted",
}


def _market_status_line(
    ref: Optional[InstrumentRef],
    is_us: bool,
    us_label: str,
    us_clock_et: datetime,
) -> Optional[str]:
    """The ``**Market Status:** <phase> | **As of:** <clock>`` quote line.

    US listings keep the snapshot/session-driven ``us_label`` and the ET clock
    (byte-identical to legacy output). Non-US listings derive the phase from the
    exchange calendar and stamp the exchange-local clock (e.g. HKT), since the
    US-Eastern phase is meaningless for them. Returns ``None`` when a non-US ref
    is missing or its calendar can't be read, so the caller omits the line rather
    than asserting a wrong phase.
    """
    if is_us:
        return f"**Market Status:** {us_label} | **As of:** {us_clock_et.strftime('%H:%M ET')}"
    if ref is None:
        return None
    try:
        now = datetime.now(timezone.utc)
        phase = get_calendar(ref.calendar_id).phase_at(now)
        local = now.astimezone(ZoneInfo(ref.tz))
    except Exception:
        return None
    label = _PHASE_LABELS.get(phase, phase.value.replace("_", " ").title())
    return f"**Market Status:** {label} | **As of:** {local.strftime('%H:%M %Z')}"
