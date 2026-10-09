"""Shared one-line quote formatting for get_quote, freshness stamps, and market watch.

Prices render in each instrument's listing currency (HK$, £, ¥, …) with
thousands grouping, so a USD price keeps its ``$``. Per-line venue phase
(pre/post/lunch/closed) is surfaced from the exchange calendar. Every protocol
lookup is wrapped so a bad symbol degrades to US/``$`` formatting — the
formatter must never raise (it sits in tool-output paths and the market-watch
middleware).
"""

from datetime import datetime, timezone
from typing import Any, NamedTuple, Optional

from src.data_client.freshness import Freshness, FreshnessLabel, is_live, measure_quote
from market_protocol import AssetClass, InstrumentRef, MarketPhase, display_spelling

from .currency import fmt_price
from .display import (
    _is_us_clock,
    _symbol_currency,
    resolve_ref,
    venue_local_time,
    venue_phase,
)
from .utils import finite_or_none, get_market_session
from src.utils.market_hours import ET

_SESSION_LABELS = {
    "PRE_MARKET": "pre-market",
    "REGULAR_HOURS": "market open",
    "AFTER_HOURS": "after-hours",
    "CLOSED": "market closed",
}

# How a row or series that is not current is named in tool text. "live" is
# absent on purpose: a current one says nothing. A quote whose provider declares
# no tier reads as unknown rather than assumed current, since an unearned "live"
# is the one label the agent must never read.
FRESHNESS_WORDS: dict[FreshnessLabel, str] = {
    FreshnessLabel.DELAYED: "delayed 15m",
    FreshnessLabel.STALE: "stale",
    FreshnessLabel.INCOMPLETE: "incomplete",
    FreshnessLabel.UNKNOWN: "freshness unknown",
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


class _Row(NamedTuple):
    """One snapshot row read once: its listing and how current it is.

    The price, suffix and stamp renderers all take this, so a block resolves
    each symbol and measures each row a single time.
    """

    snap: dict[str, Any]
    ref: Optional[InstrumentRef]
    freshness: Freshness


def _measure(
    snap: dict[str, Any], ref: Optional[InstrumentRef], at: Optional[datetime]
) -> Freshness:
    """Measured freshness of one row; ``unknown`` on any failure.

    ``as_of`` (the provider's print time) is what makes the measurement real;
    without it the label degrades to the declared tier with ``measured`` false.
    """
    try:
        return measure_quote(
            str(snap.get("symbol") or ""),
            snap.get("as_of"),
            snap.get("tier"),
            is_index=ref is not None and ref.asset_class is AssetClass.INDEX,
            regular_only=bool(snap.get("regular_only")),
            now=at,
            source=snap.get("source") or snap.get("publisher"),
        )
    except Exception:
        return Freshness(label="unknown", measured=False)


def _ref_of(
    snap: dict[str, Any], asset_class: Optional[AssetClass] = None
) -> Optional[InstrumentRef]:
    """The row's listing, read as the class it is known to be.

    A bare ticker that is also an index alias reads as the index unless told
    otherwise, and the index clock calls the stock COMP's after-hours print
    closed. The row's own ``asset_class`` wins, then the caller's *asset_class*
    (the endpoint it asked); only a row neither names is autodetected.
    """
    declared = snap.get("asset_class")
    if declared:
        try:
            asset_class = AssetClass(declared)
        except ValueError:
            pass
    return resolve_ref(snap.get("symbol"), asset_class)


def _row(
    snap: dict[str, Any],
    at: Optional[datetime] = None,
    asset_class: Optional[AssetClass] = None,
) -> _Row:
    """Read *snap* once. A row :func:`stamp_quote` already stamped keeps that
    measurement rather than taking a second one."""
    ref = _ref_of(snap, asset_class)
    stamped = snap.get("freshness")
    if isinstance(stamped, dict):
        try:
            return _Row(snap, ref, Freshness.model_validate(stamped))
        except Exception:
            pass
    return _Row(snap, ref, _measure(snap, ref, at))


def _symbol_label(snap: dict[str, Any]) -> str:
    """The row's symbol in our spelling (``600519.SH`` for a vendor's ``.SS``)."""
    symbol = snap.get("symbol")
    if not symbol:
        return "?"
    try:
        return display_spelling(str(symbol))
    except Exception:
        return str(symbol)


def _fmt_price(price: float, ref: Optional[InstrumentRef]) -> str:
    """Currency-aware, thousands-grouped price for quote output; '$' on any failure.

    Grouping is the get_quote house style, the only delta from the canonical
    :func:`fmt_price`. Never raises: the formatter sits in tool-output and
    market-watch middleware paths.
    """
    try:
        return fmt_price(price, _symbol_currency(ref), group=True)
    except Exception:
        return f"${price:,.2f}"


def _venue_clock(ref: Optional[InstrumentRef], at: Optional[datetime]) -> Optional[str]:
    try:
        if ref is None or _is_us_clock(ref):
            return None
        local = venue_local_time(ref, at)
        if local is None:
            return None
        abbrev = local.strftime("%Z") or ref.tz
        return f"{local.strftime('%Y-%m-%d %H:%M:%S')} {abbrev}"
    except Exception:
        return None


def quote_freshness(
    snap: dict[str, Any],
    at: Optional[datetime] = None,
    *,
    asset_class: Optional[AssetClass] = None,
) -> Freshness:
    """Measured freshness of one snapshot row; ``unknown`` on any failure.

    Never raises: every caller sits in tool-output or middleware paths.
    """
    return _row(snap, at, asset_class).freshness


def print_time(snap: dict[str, Any]) -> Optional[datetime]:
    """The provider's print time (``as_of``, Unix ms) as an aware datetime."""
    as_of = snap.get("as_of")
    if not isinstance(as_of, (int, float)) or as_of <= 0:
        return None
    try:
        return datetime.fromtimestamp(as_of / 1000, tz=timezone.utc)
    except Exception:
        return None


def _row_clock(row: _Row, at: Optional[datetime]) -> Optional[str]:
    """Venue-local stamp for a row, at its print time when the provider gave one."""
    return _venue_clock(row.ref, print_time(row.snap) or at)


def block_clock(
    snaps: list[dict[str, Any]],
    at: datetime,
    *,
    asset_class: Optional[AssetClass] = None,
) -> Optional[str]:
    """Retrieval clock for a block of quotes, in the time its rows share.

    ET when every row is a US listing, the venue's own clock when every row
    trades on one foreign venue, and None for a mixed block: no venue's clock
    is neutral there, so the reader's own time stands in. Never raises.
    """
    try:
        refs = [_ref_of(s, asset_class) for s in snaps]
        if all(_is_us_clock(r) for r in refs):
            return at.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S ET")
        if None not in refs and len({r.tz for r in refs}) == 1:
            return _venue_clock(refs[0], at)
    except Exception:
        pass
    return None


def stamp_quote(
    snap: dict[str, Any],
    at: Optional[datetime] = None,
    *,
    ref: Optional[InstrumentRef] = None,
) -> dict[str, Any]:
    """What a quote artifact row says about its own freshness.

    The declared ``tier``, the ``freshness`` measured at *at*, the publisher,
    and a venue-local ``as_of_local`` clock: the print time when the provider
    gave one, else the retrieval clock with ``printed`` false, since the
    artifact's ET header does not locate a foreign or delayed print. *ref*
    names the listing when the row's own spelling may not.
    """
    if ref is None:
        ref = _ref_of(snap)
    printed = print_time(snap)
    local = _venue_clock(ref, printed or at or datetime.now(timezone.utc))
    return {
        "tier": snap.get("tier"),
        # *at* stays None when unset so measure_quote reads its own clock.
        "freshness": _measure(snap, ref, at).model_dump(mode="json"),
        **({"source": snap["source"]} if snap.get("source") else {}),
        **({"regular_only": True} if snap.get("regular_only") else {}),
        **({"as_of_local": local, "printed": printed is not None} if local else {}),
    }


def stamp_provider_quote(
    quote: dict[str, Any], symbol: str, ref: Optional[InstrumentRef] = None
) -> dict[str, Any]:
    """:func:`stamp_quote` for a financial provider's quote row.

    These rows are not snapshots: they declare no tier, and only some providers
    carry a print time (``timestamp``, epoch seconds). Whatever is missing is
    reported as missing rather than assumed current.
    """
    ts = quote.get("timestamp")
    as_of = int(ts * 1000) if isinstance(ts, (int, float)) and ts > 0 else None
    row = {"symbol": symbol, "as_of": as_of, "tier": quote.get("tier"), "source": quote.get("source")}
    return stamp_quote(row, ref=ref)


def _venue_suffix(row: _Row, at: Optional[datetime] = None) -> str:
    """Per-line suffix: phase, freshness, and/or venue-local clock; '' on failure.

    A non-regular phase (pre/post/lunch/closed/halted) is surfaced so an
    off-hours quote doesn't read as a live regular-session price, and a quote
    the provider does not serve in realtime says so on its own line. Non-US
    listings additionally carry their market-local clock — at the print time
    when the row has one, so a delayed price is located in venue time rather
    than at retrieval time.
    """
    try:
        if row.ref is None:
            return ""
        phase = venue_phase(row.ref, at)
        if phase is None:  # calendar unreadable — omit the suffix
            return ""
        parts = [] if phase == MarketPhase.REGULAR else [phase.value]
        words = FRESHNESS_WORDS.get(row.freshness.label)
        if words:
            parts.append(words)
        clock = _row_clock(row, at)
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


def _line(row: _Row, prev_price: Optional[float], at: Optional[datetime]) -> str:
    snap = row.snap
    parts = [_symbol_label(snap)]
    price = current_price(snap)
    if price is not None:
        parts.append(_fmt_price(price, row.ref))
    pct = finite_or_none(snap.get("change_percent"))
    if pct is not None:
        parts.append(f"{'+' if pct >= 0 else ''}{pct:.2f}% today")
    vol = finite_or_none(snap.get("volume"))
    if vol:
        parts.append(f"vol {_fmt_volume(int(vol))}")
    if prev_price and price is not None and prev_price > 0:
        delta_pct = (price - prev_price) / prev_price * 100
        parts.append(f"({'+' if delta_pct >= 0 else ''}{delta_pct:.2f}% since last check)")
    return "  ".join(parts) + _venue_suffix(row, at)


def format_quote_line(
    snap: dict[str, Any],
    prev_price: Optional[float] = None,
    at: Optional[datetime] = None,
    *,
    asset_class: Optional[AssetClass] = None,
) -> str:
    """One line per symbol: 'NVDA  $233.45  +2.31% today  vol 187.2M  (closed)'."""
    return _line(_row(snap, at, asset_class), prev_price, at)


def format_quote_block(
    snaps: list[dict[str, Any]],
    prev_prices: Optional[dict[str, float]] = None,
    at: Optional[datetime] = None,
    *,
    asset_class: Optional[AssetClass] = None,
) -> str:
    """Multi-symbol block with a retrieval header line.

    The header clock is when the block was built, not when any price printed —
    a delayed row says so on its own line and carries the print time there, so
    the header never has to stand in for a freshness the quotes don't have. The
    US session label rides the header only when every symbol is a US listing; a
    mixed/foreign block drops it (the US-Eastern phase is meaningless for a
    non-US venue). *asset_class* is the endpoint the rows came from, for a row
    that does not name its own.
    """
    rows = [_row(s, at, asset_class) for s in snaps]
    session_name, now_et = get_market_session(at)
    clock = now_et.strftime("%Y-%m-%d %H:%M:%S ET")
    if all(_is_us_clock(r.ref) for r in rows):
        label = _SESSION_LABELS.get(session_name, session_name.lower())
        header = f"Retrieved {clock} ({label})"
    else:
        header = f"Retrieved {clock}"
    prev_prices = prev_prices or {}
    lines = [_line(r, prev_prices.get(r.snap.get("symbol", "")), at) for r in rows]
    return "\n".join([header, *lines])


def _venue_is_live(ref: Optional[InstrumentRef], session_name: str,
                   at: Optional[datetime] = None) -> bool:
    """True when the listing's own venue is in a tradeable phase right now.

    US listings keep the US-session gate; every other venue is judged by its
    own calendar, so a Shanghai quote isn't suppressed because New York is
    shut — and isn't stamped "live" at 22:30 Shanghai because New York is open.
    Unreadable calendar → fall back to the US gate rather than dropping the
    quote.
    """
    if ref is None or _is_us_clock(ref):
        return session_name != "CLOSED"
    phase = venue_phase(ref, at)
    if phase is None:
        return session_name != "CLOSED"
    return phase not in (MarketPhase.CLOSED, MarketPhase.HALTED)


def _stamp_entry(row: _Row, at: Optional[datetime] = None) -> str:
    """'NVDA $233.45 (+2.31%) @ 2026-07-01 10:30:00 CST' for one stamped row."""
    snap = row.snap
    text = f"{_symbol_label(snap)} {_fmt_price(current_price(snap), row.ref)}"
    pct = finite_or_none(snap.get("change_percent"))
    if pct is not None:
        text += f" ({'+' if pct >= 0 else ''}{pct:.2f}%)"
    clock = _row_clock(row, at)
    if clock:
        text += f" @ {clock}"
    return text


def build_live_stamp(
    snaps: list[dict[str, Any]],
    at: Optional[datetime] = None,
    *,
    asset_class: Optional[AssetClass] = None,
) -> Optional[str]:
    """Freshness-split header for existing tools; None when no snap's venue is open.

    Only a quote that :func:`is_live` reaches the ``[Live: …]`` group. Every
    other row is grouped under the words its own measurement earned
    (``[Delayed 15m: …]``, ``[Stale: …]``, ``[Freshness unknown: …]``), so an
    hours-old print never reads as fifteen minutes behind. Each snapshot is
    gated and clocked by its OWN venue (matching :func:`format_quote_block`):
    non-US quotes carry their exchange-local stamp at the print time, and the
    trailing ET session line is emitted only when every quote in the live group
    is a US listing. A row whose venue the calendar has closed (a US holiday
    inside the session gate's hours) is no quote of an open venue.
    *asset_class* is the endpoint the rows came from, for a row that does not
    name its own.
    """
    session_name, now_et = get_market_session(at)
    rows = [_row(s, at, asset_class) for s in snaps if current_price(s) is not None]
    open_now = [
        r for r in rows
        if _venue_is_live(r.ref, session_name, at) and not r.freshness.closed
    ]
    if not open_now:
        return None

    stamps = []
    live = [r for r in open_now if is_live(r.freshness)]
    if live:
        quotes = ", ".join(_stamp_entry(r, at) for r in live)
        if all(_is_us_clock(r.ref) for r in live):
            label = _SESSION_LABELS.get(session_name, session_name.lower())
            stamps.append(
                f"[Live: {quotes} — as of {now_et.strftime('%H:%M:%S ET')}, {label}]"
            )
        else:
            stamps.append(f"[Live: {quotes}]")
    unknown = FRESHNESS_WORDS[FreshnessLabel.UNKNOWN]
    groups: dict[str, list[_Row]] = {words: [] for words in FRESHNESS_WORDS.values()}
    for r in open_now:
        if not is_live(r.freshness):
            groups[FRESHNESS_WORDS.get(r.freshness.label, unknown)].append(r)
    for words, group in groups.items():
        if group:
            entries = ", ".join(_stamp_entry(r, at) for r in group)
            stamps.append(f"[{words[:1].upper()}{words[1:]}: {entries}]")
    return "\n".join(stamps)
