"""Instrument-aware display helpers for market-data tool output.

Venue labels, US-session gating, and the market-status quote line — all derived
from the resolved :class:`InstrumentRef`, never re-parsed from the raw symbol.
Callers resolve the ref once via :func:`resolve_ref` and thread it through; each
helper falls back to the legacy US-centric output when the ref is missing, so
display formatting never fails on an unrecognized symbol.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from market_protocol import (
    AssetClass,
    InstrumentRef,
    MarketPhase,
    display_decimals_for,
    to_canonical,
    to_legacy_api,
)

from src.data_client.instrument_clock import clock_for_ref
from src.data_client.normalize import listing_currency

from .currency import DisplaySpec, fmt_quote


def resolve_ref(
    symbol: Optional[str], asset_class: Optional[AssetClass] = None
) -> Optional[InstrumentRef]:
    """Canonicalize a symbol to its ``InstrumentRef``; ``None`` if empty or
    unresolvable.

    Lets each fetch entry resolve once and pass the ref to the display helpers
    instead of re-parsing the string in every helper. ``asset_class`` is the
    caller's hint for a bare ticker the registry would otherwise read another
    way (a company overview reads ``SPX`` as the S&P 500 only through it).
    """
    if not symbol:
        return None
    try:
        return to_canonical(symbol, asset_class=asset_class)
    except Exception:
        return None


def resolve_listing(
    symbol: str, asset_class: Optional[AssetClass] = None
) -> tuple[Optional[InstrumentRef], str]:
    """The ref plus the spelling the provider chain takes for it.

    An unresolvable symbol travels as typed, so a provider still gets to try it.
    """
    ref = resolve_ref(symbol, asset_class)
    return ref, (to_legacy_api(ref) if ref is not None else symbol)


def _symbol_currency(ref: Optional[InstrumentRef]) -> DisplaySpec:
    """Display spec (currency + decimals) for a resolved instrument.

    Decimals come from ``display_decimals_for(price_currency, asset_class)`` so
    the protocol table is the sole authority. Falls back to USD / 2 decimals for a
    missing ref or anything the registry cannot price, so display never fails.
    A listing with no known currency prints with none at all.
    """
    # TODO: display_unit (e.g. GBX quotes on XLON) is not yet consulted here.
    if ref is None:
        return DisplaySpec(None, 2)
    if listing_currency(ref) is None:
        return DisplaySpec(None, 2, bare=True)
    try:
        return DisplaySpec(
            ref.price_currency,
            display_decimals_for(ref.price_currency, ref.asset_class),
            bare=ref.asset_class is AssetClass.INDEX,
            trim=ref.asset_class is AssetClass.CRYPTO,
        )
    except Exception:
        return DisplaySpec(None, 2)


def price_text(value: Optional[float], ref: Optional[InstrumentRef]) -> str:
    """``$198.00`` or ``HK$318.20``: a price in its listing's currency, grouped,
    at the precision it quotes in (``$0.3734``)."""
    try:
        return fmt_quote(value, _symbol_currency(ref), group=True)
    except Exception:
        return f"{value:,.2f}" if value is not None else "N/A"


def pct_text(value: Any) -> str:
    """``3%`` or ``2.5%``: a percentage as it was given, never padded."""
    return f"{float(value):g}%"


# MIC (listing venue) -> human "Market:" label for report headers. US and any
# unresolved USD listing intentionally read "US Stock" so US output stays
# byte-identical to the legacy hardcoded header.
_MIC_MARKET_LABELS: Dict[str, str] = {
    "XNYS": "US Stock",
    "XNAS": "US Stock",
    "XHKG": "HK Stock",
    "XSHG": "A-Share",
    "XSHE": "A-Share",
    "BJSE": "A-Share",
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
    "GLBX": "Futures",
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


def venue_phase(
    ref: Optional[InstrumentRef], at: Optional[datetime] = None
) -> Optional[MarketPhase]:
    """The listing's phase at ``at`` (default now); ``None`` on any failure.

    Read off the instrument clock, so an index reads its venue's extended hours
    as closed here exactly as its freshness does.
    """
    if ref is None:
        return None
    try:
        return clock_for_ref(ref).phase(at or datetime.now(timezone.utc))
    except Exception:
        return None


def venue_local_time(
    ref: Optional[InstrumentRef], at: Optional[datetime] = None
) -> Optional[datetime]:
    """``at`` (default now) in the listing's exchange-local timezone; ``None`` on any failure."""
    if ref is None:
        return None
    try:
        return (at or datetime.now(timezone.utc)).astimezone(ZoneInfo(ref.tz))
    except Exception:
        return None


# Abbreviations that name more than one venue's zone: CST is China, Taiwan and
# US Central, IST India and Israel, BST British Summer and Bangladesh.
_AMBIGUOUS_ZONES = frozenset({"CST", "IST", "BST"})


def zone_label(local: datetime) -> str:
    """The zone of an aware time as tool text prints it: ``HKT``, or ``UTC+8``
    where the abbreviation is ambiguous, numeric (``+08``) or missing."""
    abbrev = local.strftime("%Z")
    if abbrev and abbrev[0] not in "+-" and abbrev not in _AMBIGUOUS_ZONES:
        return abbrev
    minutes = int(local.utcoffset().total_seconds() // 60)
    hours, rest = divmod(abs(minutes), 60)
    return f"UTC{'-' if minutes < 0 else '+'}{hours}{f':{rest:02d}' if rest else ''}"


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
    printed_at: Optional[str] = None,
) -> Optional[str]:
    """The ``**Market Status:** <phase> | **As of:** <clock>`` quote line.

    US listings keep the snapshot/session-driven ``us_label`` and the ET clock.
    Non-US listings derive the phase from the exchange calendar and stamp the
    exchange-local clock (e.g. HKT), since the US-Eastern phase is meaningless
    for them. Either then gives *printed_at*, the venue-local time the shown
    price printed: the clock is when it was read, and a closed or delayed
    venue's price, or a thin name's old trade, is older. Returns ``None`` when a non-US ref is
    missing or its calendar can't be read, so the caller omits the line rather
    than asserting a wrong phase.
    """
    if is_us:
        line = f"**Market Status:** {us_label} | **As of:** {us_clock_et.strftime('%H:%M ET')}"
    else:
        if ref is None:
            return None
        now = datetime.now(timezone.utc)
        phase = venue_phase(ref, now)
        local = venue_local_time(ref, now)
        if phase is None or local is None:
            return None
        label = _PHASE_LABELS.get(phase, phase.value.replace("_", " ").title())
        line = f"**Market Status:** {label} | **As of:** {local:%H:%M} {zone_label(local)}"
    return f"{line} | **Price as of:** {printed_at}" if printed_at else line
