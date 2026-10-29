"""What a stored wakeup says about itself, in the model's vocabulary.

Every renderer reads a ``wakeups`` row mapping (the 2.1 column names), so the
set confirmation, the pending listing and the cancel notice are all worded
from the one row and cannot disagree about a time or a level; a fire's notice
(``services/wakeups/message.py``) words its price with the same phrase. An
order's row carries ``attempt``, the order it watches, as every read of the
table returns it (``attempt_sql``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from market_protocol import InstrumentRef

from src.server.database.wakeups.contract import as_datetime, as_float
from src.server.services.brokerage_orders.view import order_view
from src.server.services.market_targets.listing import listing_ref, shown_symbol
from src.tools.market_data.display import pct_text, price_text, venue_local_time
from src.tools.wakeups.clock import (
    Zone,
    by_day,
    clock,
    for_user,
    lead,
    same_clock,
    user_zone_of,
    when,
    zone_name,
    zone_of,
)
from src.tools.wakeups.wording import BUILD_PCT_NOW_BASE, BUILD_PRICE_PCT_LINE

_STOP_SET = {
    "24": "stopping at the 24-check limit",
    "until": "stopping at until",
    "7day": "stopping at the 7-day limit",
}
_STOP_PENDING = {
    "24": "the 24-check limit",
    "until": "the last before until",
    "7day": "the 7-day limit",
}


def quoted_line(text: str, cap: Optional[int] = None) -> str:
    """Text from elsewhere (a label the model wrote, a brokerage's failure
    message) made fit to quote inside a wakeup's own markup: one line, with
    no angle brackets to close the block early and no control or format
    characters, cut to ``cap`` when given."""
    kept = "".join(c for c in text if c not in "<>" and (c.isprintable() or c.isspace()))
    line = " ".join(kept.split())
    if cap is not None and len(line) > cap:
        line = line[: cap - 1].rstrip() + "…"
    return line


def schedule_of(row: Mapping[str, Any]) -> list[datetime]:
    return [a for a in (as_datetime(x) for x in row.get("schedule") or ()) if a is not None]


def venue_zone(ref: Optional[InstrumentRef], at: datetime) -> Zone:
    """The clock a listing trades on, which a price's times read on."""
    local = venue_local_time(ref, at)
    return (
        local.tzinfo if local is not None and local.tzinfo is not None else timezone.utc
    )  # type: ignore[return-value]


def _label_paren(row: Mapping[str, Any]) -> str:
    return f" ({row['label']})" if row.get("label") else ""


def _plural(n: int, noun: str) -> str:
    return f"{n} {noun}" if n == 1 else f"{n} {noun}s"


# --------------------------------------------------------------------------
# Price
# --------------------------------------------------------------------------


def price_phrase(
    kind: Any, value: Optional[float], armed_base: Optional[float], ref: Optional[InstrumentRef]
) -> str:
    """``below $198.00`` or ``up 3% from the previous close``, as the agent set it.

    ``armed_base`` is the price a % move measures from when it measures from
    the price at set time: it is named, because a % target set at 200 and
    read back at 215 is otherwise a number with no anchor.
    """
    if kind in ("above", "below"):
        return f"{kind} {price_text(value, ref)}"
    direction = "up" if kind == "pct_up" else "down"
    if armed_base is not None:
        base = BUILD_PCT_NOW_BASE.format(price=price_text(armed_base, ref))
    else:
        base = "the previous close"
    return f"{direction} {pct_text(value)} from {base}"


def _row_phrase(row: Mapping[str, Any], ref: Optional[InstrumentRef]) -> str:
    armed = as_float(row.get("armed_price")) if row.get("pct_from") == "armed" else None
    return price_phrase(row.get("price_kind"), as_float(row.get("price_value")), armed, ref)


def _price_what(row: Mapping[str, Any]) -> str:
    return f"{shown_symbol(row) or row.get('symbol')} {_row_phrase(row, listing_ref(row))}"


def _venue_when(ref: Optional[InstrumentRef], at: datetime, user_zone: Zone) -> str:
    """The time on the venue's clock, with the user's reading when it differs."""
    zone = venue_zone(ref, at)
    text = when(at, zone)
    user = for_user(at, zone, user_zone)
    return f"{text} ({user})" if user else text


def _price_body(
    row: Mapping[str, Any], user_tz: str, last: Optional[float], level: Optional[float]
) -> str:
    ref = listing_ref(row)
    expires = as_datetime(row.get("expires_at"))
    lapses = _venue_when(ref, expires, user_zone_of(user_tz)) if expires else "soon"
    symbol = shown_symbol(row) or row.get("symbol")
    phrase = _row_phrase(row, ref)
    if row.get("price_kind") in ("pct_up", "pct_down") and last and level:
        away = f"{abs(level - last) / last * 100:.2f}".rstrip("0").rstrip(".")
        return BUILD_PRICE_PCT_LINE.format(
            symbol=symbol,
            phrase=phrase,
            last=price_text(last, ref),
            level=price_text(level, ref),
            away=away,
            when=lapses,
        )
    return f"{symbol} {phrase}; lapses {lapses}"


# --------------------------------------------------------------------------
# Order
# --------------------------------------------------------------------------


def _order_summary(row: Mapping[str, Any]) -> Optional[str]:
    view = order_view(row.get("attempt"))
    return view.summary if view is not None else None


def _order_what(row: Mapping[str, Any]) -> str:
    summary = _order_summary(row)
    text = f"order {row.get('vendor_order_id')}"
    return f"{text} ({summary})" if summary else text


# --------------------------------------------------------------------------
# Listing and set lines
# --------------------------------------------------------------------------


def _body(
    row: Mapping[str, Any],
    now: datetime,
    user_tz: str,
    *,
    listing: bool,
    last: Optional[float],
    level: Optional[float],
) -> str:
    kind = row.get("kind")
    user_zone = user_zone_of(user_tz)
    zone = zone_of(row.get("timezone"), user_tz)
    if kind == "at":
        fire = schedule_of(row)[0]
        text = f"at {when(fire, zone)}"
        if not isinstance(zone, timezone):  # an offset the model wrote reads as itself
            same = same_clock(zone, user_zone)
            text += f" in {zone_name(zone)}" + (", the user's timezone" if same else "")
        text += f", {lead(fire, now)}"
        user = for_user(fire, zone, user_zone)
        return text + (f" ({user})" if user else "")
    if kind == "cron":
        return _cron_body(row, now, user_tz, listing=listing)
    if kind == "price":
        return _price_body(row, user_tz, last, level)
    expires = as_datetime(row.get("expires_at"))
    lapses = when(expires, zone) if expires else "soon"
    return f"{_order_what(row)} fills or ends; lapses {lapses} if it does neither"


def _cron_body(
    row: Mapping[str, Any], now: datetime, user_tz: str, *, listing: bool
) -> str:
    user_zone = user_zone_of(user_tz)
    zone = zone_of(row.get("timezone"), user_tz)
    same = same_clock(zone, user_zone)
    fires = schedule_of(row)
    done = int(row.get("fires_done") or 0)
    stop = row.get("stop_reason") or "7day"
    until = as_datetime(row.get("until_at"))
    where = f"in {zone_name(zone)}" + (", the user's timezone" if same else "")
    until_part = (
        (("," if same else "") + f" until {when(until, zone)}") if until else ""
    )
    head = f"cron {row.get('cron')} {where}{until_part}."
    first = fires[done] if listing and done < len(fires) else fires[0]
    last = fires[-1]

    def user(at: datetime) -> str:
        text = for_user(at, zone, user_zone)
        return f" ({text})" if text else ""

    shown = fires[done:] if listing else fires
    tail = by_day(shown, zone)
    by = f". {tail}" if tail else ""
    if listing:
        return (
            f"{head} {done} of {len(fires)} checks done; next {when(first, zone)}, {lead(first, now)}{user(first)}; "
            f"last {when(last, zone)}{user(last)}, {_STOP_PENDING[stop]}{by}"
        )
    return (
        f"{head} {_plural(len(fires), 'check')}, {_STOP_SET[stop]}: first {when(first, zone)}, "
        f"{lead(first, now)}{user(first)}; last {when(last, zone)}{user(last)}{by}"
    )


def _entry(row: Mapping[str, Any], body: str) -> str:
    return f"- {row.get('short_id')}{_label_paren(row)}: {body}."


def set_line(
    row: Mapping[str, Any],
    now: datetime,
    user_tz: str,
    *,
    last: Optional[float] = None,
    level: Optional[float] = None,
) -> str:
    """One entry of a set confirmation: what was understood, to be checked before it fires.

    ``last`` and ``level`` are a % trigger's latest price and the price it
    fires at; they turn "up 3%" into a distance the agent can sanity-check.
    """
    return _entry(row, _body(row, now, user_tz, listing=False, last=last, level=level))


def pending_line(
    row: Mapping[str, Any],
    now: datetime,
    user_tz: str,
    *,
    last: Optional[float] = None,
    level: Optional[float] = None,
) -> str:
    """One entry of the pending listing; a series shows its progress."""
    return _entry(row, _body(row, now, user_tz, listing=True, last=last, level=level))


# --------------------------------------------------------------------------
# Cancel
# --------------------------------------------------------------------------


def _what(row: Mapping[str, Any], user_tz: str, *, order_parens: bool) -> str:
    kind = row.get("kind")
    zone = zone_of(row.get("timezone"), user_tz)
    if kind == "at":
        return f"at {when(schedule_of(row)[0], zone)}"
    if kind == "cron":
        return f"cron {row.get('cron')} in {zone_name(zone)}"
    if kind == "price":
        return _price_what(row)
    if order_parens:
        return _order_what(row)
    summary = _order_summary(row)
    return f"order {row.get('vendor_order_id')}" + (f", {summary}" if summary else "")


def short_desc(row: Mapping[str, Any]) -> str:
    """``NVDA pullback entry: cron */5 10-15 * * 1-5 in America/New_York``, for a cancelled list."""
    user_tz = row.get("timezone") or "UTC"
    what = _what(row, user_tz, order_parens=True)
    return f"{row['label']}: {what}" if row.get("label") else what


def cancel_label(
    row: Mapping[str, Any], user_tz: str, now: Optional[datetime] = None
) -> str:
    """The ``{label}`` of a user-cancelled row: what it was, when it was set, how far it got."""
    created = as_datetime(row.get("created_at"))
    kind = row.get("kind")
    if kind == "price":
        zone = venue_zone(listing_ref(row), created or datetime.now(timezone.utc))
    else:
        zone = zone_of(row.get("timezone"), user_tz)
    # The row's own parentheses already frame a series, so it reads bare
    # where the tool's cancel result names it "cron ...".
    what = (
        f"{row.get('cron')} in {zone_name(zone)}"
        if kind == "cron"
        else _what(row, user_tz, order_parens=False)
    )
    ref = now or created
    set_at = clock(created, zone, ref) if created and ref else None
    set_part = f"set at {set_at}" if set_at else None

    progress = None
    if kind == "cron":
        progress = (
            f"{int(row.get('fires_done') or 0)} of {len(schedule_of(row))} checks done"
        )
    fired = None
    fired_at = as_datetime(row.get("last_fired_at"))
    undelivered = int(row.get("last_fired_seq") or 0) > int(
        row.get("delivered_seq") or 0
    )
    if fired_at and (undelivered or row.get("status") == "fired"):
        fired = f"which had fired at {clock(fired_at, zone, fired_at)}"
        price = as_float(row.get("fired_price"))
        if kind == "price" and price is not None:
            fired += f" ({price_text(price, listing_ref(row))})"
        fired += " before it reached you"

    if row.get("label"):
        inner = ", ".join(x for x in (what, set_part, progress, fired) if x)
        return f"{row['label']} ({inner})"
    text = ", ".join(x for x in (what, set_part) if x)
    if progress:
        text += f" ({progress})"
    if fired:
        text += f", {fired}"
    return text
