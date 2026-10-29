"""What a target's notice says to the model, and what it tells the UI.

The running turn and a wake turn put the same text in the same user-role
slot, so this is the only place it is worded. It opens like the
``<market-watch>`` stamp: a tagged block that says up front it is automated,
because the slot it rides is the one the user's own messages use.
"""

from __future__ import annotations

from datetime import timezone
from typing import Any, Mapping, Optional

from market_protocol import InstrumentRef

from src.server.database.wakeups.contract import as_datetime, as_float
from src.server.services.market_targets.listing import (
    PCT_FROM_TOOL,
    listing_ref,
    shown_symbol,
)
from src.server.services.wakeups.message import Quote, quote_price
from src.tools.market_data.display import pct_text, price_text, venue_local_time

ALERT_OPEN = "<market-alert>"
ALERT_CLOSE = "</market-alert>"
_ALERT_NOTE = (
    "Automated alert from a price target you set with watch_market "
    "(not a user message)."
)


def _iso(value: Any) -> Optional[str]:
    at = as_datetime(value)
    return at.isoformat() if at else None


def _venue_time(ref: Optional[InstrumentRef], value: Any) -> Optional[str]:
    """``2026-10-12 10:31 EDT``: the venue's own clock, since that is the
    clock a session, a close and an expiry are read on."""
    at = as_datetime(value)
    if at is None:
        return None
    local = venue_local_time(ref, at) or at.astimezone(timezone.utc)
    return f"{local.strftime('%Y-%m-%d %H:%M')} {local.strftime('%Z') or 'UTC'}"


def _target_phrase(target: Mapping[str, Any], ref: Optional[InstrumentRef]) -> str:
    """The target as the agent set it: ``above $230.00``, ``up 3% from the
    previous close``."""
    kind = target.get("kind")
    value = target.get("value")
    if kind in ("above", "below"):
        return f"{kind} {price_text(value, ref)}"
    direction = "up" if kind == "pct_up" else "down"
    if target.get("pct_from") == "armed" and target.get("armed_price") is not None:
        base = (
            f"{price_text(target['armed_price'], ref)}, "
            f"its price when you set the target"
        )
    else:
        base = "the previous close"
    return f"{direction} {pct_text(value)} from {base}"


def _move_from_armed(target: Mapping[str, Any], price: Optional[float]) -> str:
    """`` (+3.02%)`` against the arming price, the one base the row holds."""
    base = target.get("armed_price")
    if (
        price is None
        or target.get("pct_from") != "armed"
        or not isinstance(base, (int, float))
        or base <= 0
    ):
        return ""
    change = (price - base) / base * 100
    return f" ({'+' if change >= 0 else ''}{change:.2f}%)"


def render_notice(target: Mapping[str, Any], notice: str, quote: Quote = None) -> str:
    """The ``<market-alert>`` block for one notice.

    ``quote`` is the latest price (a number or a quote row), shown on an
    ``expiring`` notice when known; a hit carries the price it fired at.
    """
    ref = listing_ref(target)
    symbol = shown_symbol(target) or "?"
    phrase = _target_phrase(target, ref)
    if notice == "hit":
        fired_price = target.get("fired_price")
        line = f"{symbol} hit your target ({phrase}): it traded at {price_text(fired_price, ref)}"
        line += _move_from_armed(target, fired_price)
        when = _venue_time(ref, target.get("fired_at"))
        if when:
            line += f" at {when}"
        body = [f"{line}.", "The target is spent and will not fire again."]
    else:
        line = f"Your target on {symbol} ({phrase}) has not hit"
        when = _venue_time(ref, target.get("expires_at"))
        line += f" and expires at {when}." if when else " and expires soon."
        last = quote_price(quote)
        if last is not None:
            line += f" Last price: {price_text(last, ref)}{_move_from_armed(target, last)}."
        body = [
            line,
            "To keep it, set the same target again with schedule_wakeup, "
            "which starts a fresh window; otherwise let it expire.",
        ]
    return "\n".join([ALERT_OPEN, _ALERT_NOTE, *body, ALERT_CLOSE])


def notice_payload(target: Mapping[str, Any], notice: str) -> dict[str, Any]:
    """One notice as the UI reads it: a ``steering_delivered`` entry's
    ``market_alert`` when a live turn took it in, and a wake turn's query
    metadata. Enough to draw the alert row without reading the target back."""
    return {
        "target_id": str(target.get("target_id")),
        "notice": notice,
        "symbol": shown_symbol(target),
        "kind": target.get("kind"),
        "value": as_float(target.get("value")),
        "pct_from": PCT_FROM_TOOL.get(target.get("pct_from") or ""),
        "armed_price": as_float(target.get("armed_price")),
        "fired_price": as_float(target.get("fired_price")),
        "fired_at": _iso(target.get("fired_at")),
        "expires_at": _iso(target.get("expires_at")),
    }
