"""Shared one-line quote formatting for get_quote, freshness stamps, and market watch.

Prices render in each instrument's listing currency (HK$, £, ¥, …) with the
legacy thousands grouping, so USD output stays byte-identical to the old
hardcoded ``$`` formatting. Per-line venue phase (pre/post/lunch/closed) is
surfaced from the exchange calendar. Every protocol lookup is wrapped so a bad
symbol degrades to US/``$`` formatting — the formatter must never raise (it sits
in tool-output paths and the market-watch middleware).
"""

from datetime import datetime
from typing import Any, Optional

from src.market_protocol import MarketPhase

from .currency import fmt_price
from .display import (
    _is_us_clock,
    _symbol_currency,
    resolve_ref,
    venue_local_time,
    venue_phase,
)
from .utils import finite_or_none, get_market_session

_SESSION_LABELS = {
    "PRE_MARKET": "pre-market",
    "REGULAR_HOURS": "market open",
    "AFTER_HOURS": "after-hours",
    "CLOSED": "market closed",
}


def current_price(snap: dict[str, Any]) -> Optional[float]:
    """Best available finite current price: last trade if present, else session close.

    Non-finite values (NaN from a forming bar) count as missing so callers'
    existing None handling skips them instead of rendering "$nan".
    """
    for key in ("last_trade_price", "price"):
        value = finite_or_none(snap.get(key))
        if value is not None:
            return value
    return None


def _fmt_price(price: float, symbol: Optional[str]) -> str:
    """Currency-aware, thousands-grouped price for quote output; '$' on any failure.

    Grouping is the get_quote house style — the only delta from the canonical
    :func:`fmt_price`, which now owns the format. Never raises: the formatter
    sits in tool-output and market-watch middleware paths.
    """
    try:
        return fmt_price(price, _symbol_currency(resolve_ref(symbol)), group=True)
    except Exception:
        return f"${price:,.2f}"


def venue_clock(symbol: Optional[str], at: Optional[datetime] = None) -> Optional[str]:
    """Venue-local stamp ('2026-07-14 15:32:05 HKT') for a non-US listing.

    Carries the date — a venue's local date can differ from the ET header's.
    None for US listings (the ET header clock already covers them),
    unresolvable symbols, or on any failure.
    """
    try:
        ref = resolve_ref(symbol)
        if ref is None or _is_us_clock(ref):
            return None
        local = venue_local_time(ref, at)
        if local is None:
            return None
        abbrev = local.strftime("%Z") or ref.tz
        return f"{local.strftime('%Y-%m-%d %H:%M:%S')} {abbrev}"
    except Exception:
        return None


def _venue_suffix(symbol: Optional[str]) -> str:
    """Per-line venue suffix: phase and/or venue-local clock; '' on any failure.

    A non-regular phase (pre/post/lunch/closed/halted) is surfaced so an
    off-hours quote doesn't read as a live regular-session price. Non-US
    listings additionally carry their market-local clock — the block header's
    ET stamp doesn't locate their price in venue time.
    """
    try:
        ref = resolve_ref(symbol)
        if ref is None:
            return ""
        phase = venue_phase(ref)
        if phase is None:  # calendar unreadable — omit the suffix
            return ""
        clock = venue_clock(symbol)
        parts = [] if phase == MarketPhase.REGULAR else [phase.value]
        if clock:
            parts.append(clock)
        return f" ({', '.join(parts)})" if parts else ""
    except Exception:
        return ""


def _fmt_volume(vol: int) -> str:
    if vol >= 1_000_000_000:
        return f"{vol / 1_000_000_000:.1f}B"
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.1f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.1f}K"
    return str(vol)


def format_quote_line(snap: dict[str, Any], prev_price: Optional[float] = None) -> str:
    """One line per symbol: 'NVDA  $233.45  +2.31% today  vol 187.2M  (closed)'."""
    symbol = snap.get("symbol", "?")
    parts = [symbol]
    price = current_price(snap)
    if price is not None:
        parts.append(_fmt_price(price, symbol))
    pct = snap.get("change_percent")
    if pct is not None:
        parts.append(f"{'+' if pct >= 0 else ''}{pct:.2f}% today")
    vol = snap.get("volume")
    if vol:
        parts.append(f"vol {_fmt_volume(vol)}")
    if prev_price and price is not None and prev_price > 0:
        delta_pct = (price - prev_price) / prev_price * 100
        parts.append(f"({'+' if delta_pct >= 0 else ''}{delta_pct:.2f}% since last check)")
    return "  ".join(parts) + _venue_suffix(symbol)


def format_quote_block(
    snaps: list[dict[str, Any]],
    prev_prices: Optional[dict[str, float]] = None,
) -> str:
    """Multi-symbol block with an as-of header line.

    The header carries the US session label only when every symbol is a US
    listing; a mixed/foreign block drops it (the US-Eastern phase is meaningless
    for a non-US venue — each line carries its own phase suffix instead).
    """
    session_name, now_et = get_market_session()
    clock = now_et.strftime("%Y-%m-%d %H:%M:%S ET")
    all_us = all(_is_us_clock(resolve_ref(s.get("symbol"))) for s in snaps)
    if all_us:
        label = _SESSION_LABELS.get(session_name, session_name.lower())
        header = f"As of {clock} ({label})"
    else:
        header = f"As of {clock}"
    prev_prices = prev_prices or {}
    lines = [
        format_quote_line(s, prev_price=prev_prices.get(s.get("symbol", "")))
        for s in snaps
    ]
    return "\n".join([header, *lines])


def build_live_stamp(snaps: list[dict[str, Any]]) -> Optional[str]:
    """`[Live: ...]` header for existing tools; None when market closed or no priced snaps."""
    session_name, now_et = get_market_session()
    if session_name == "CLOSED":
        return None
    priced = [s for s in snaps if current_price(s) is not None]
    if not priced:
        return None
    quotes = ", ".join(
        f"{s.get('symbol', '?')} {_fmt_price(current_price(s), s.get('symbol'))}"
        + (f" ({'+' if s['change_percent'] >= 0 else ''}{s['change_percent']:.2f}%)"
           if s.get("change_percent") is not None else "")
        for s in priced
    )
    label = _SESSION_LABELS.get(session_name, session_name.lower())
    return f"[Live: {quotes} — as of {now_et.strftime('%H:%M:%S ET')}, {label}]"
