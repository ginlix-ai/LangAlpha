"""An order said back in words: what a wakeup that watches it shows.

Read off the attempt row once, so the pending listing, the cancel notice, the
fire notice and the thread API word one order the same way, and an order
that does not parse costs each of them its summary, never the line.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Optional

from market_protocol import AssetClass as ProtocolClass
from market_protocol import display_decimals_for

from src.server.services.brokerage_orders.models import BrokerOrder
from src.tools.market_data.currency import DisplaySpec, fmt_price, fmt_quote
from src.tools.market_data.display import resolve_ref

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderView:
    order: BrokerOrder
    #: ``buy 100 NVDA, limit $200.00, at moomoo``
    summary: str
    #: The same with the mode, ``at moomoo (live)``, for where money is at
    #: stake in the sentence: a fill notice.
    summary_live: str
    #: The exchange zone of an equity's listing, so a lapse reads on the clock
    #: it trades on.
    timezone: Optional[str]
    currency: Optional[str]


def plain(value: Any) -> str:
    """``100`` for 100.0 and ``0.5`` for 0.50, never an exponent."""
    try:
        d = Decimal(str(value))
    except Exception:
        return str(value)
    return f"{d:f}".rstrip("0").rstrip(".") if "." in f"{d:f}" else f"{d:f}"


# Option and futures prices tick in their contract's increments, cents at the
# finest; anything else, an order the broker files as "other" included, is
# quoted at the places it trades in.
_CENT_CLASSES = {"option", "option_combo", "future"}


def order_price(order: Optional[BrokerOrder], value: Any) -> str:
    """*value*, a price on *order*, in the order's currency: a share under a
    unit at four places (``$0.1234``), a coin at its own (``$0.00000464``),
    an option premium in cents."""
    if value is None:
        return "N/A"
    currency = (order.currency if order else None) or "USD"
    asset_class = order.asset_class if order else "equity"
    if asset_class in _CENT_CLASSES:
        return fmt_price(float(value), currency, group=True)
    # Two places at least, since a limit can sit on a sub-yen tick.
    spec = DisplaySpec(currency, 2)
    if asset_class == "crypto":
        try:
            spec = DisplaySpec(currency, display_decimals_for(currency, ProtocolClass.CRYPTO), trim=True)
        except Exception:
            pass
    return fmt_quote(float(value), spec, group=True)


def _instrument_text(ref: Any) -> str:
    kind = getattr(ref, "kind", None)
    if kind in ("equity", "future"):
        return ref.symbol
    if kind == "option":
        strike = plain(ref.strike) if ref.strike is not None else ""
        return " ".join(
            x
            for x in (
                ref.underlying,
                f"{ref.expiration:%Y-%m-%d}" if ref.expiration else "",
                f"{strike}{ref.right or ''}",
            )
            if x
        )
    if kind == "crypto":
        return ref.pair
    if kind == "opaque":
        return ref.raw_code
    if kind == "combo":
        symbols = {leg.get("symbol") for leg in ref.legs or ()}
        symbol = symbols.pop() if len(symbols) == 1 else None
        return " ".join(x for x in (symbol, ref.strategy or "combo") if x)
    return ""


def _summary(attempt: Mapping[str, Any], order: BrokerOrder, *, with_mode: bool) -> str:
    parts = []
    side = (order.side or "").replace("_", " ")
    qty = plain(order.qty) if order.qty is not None else None
    parts.append(
        " ".join(x for x in (side, qty, _instrument_text(order.instrument)) if x)
    )
    if order.order_type:
        words = order.order_type.replace("_", " ")
        level = order.limit_price if order.limit_price is not None else order.stop_price
        if level is not None:
            words += f" {order_price(order, level)}"
        if order.limit_price is not None and order.stop_price is not None:
            words += f" (stop {order_price(order, order.stop_price)})"
        parts.append(words)
    vendor = attempt.get("vendor") or order.vendor
    mode = attempt.get("mode") or order.mode
    mode_word = str(getattr(mode, "value", mode)) if with_mode and mode else ""
    if vendor:
        parts.append(f"{mode_word} at {vendor}".strip())
    return ", ".join(p for p in parts if p)


def _timezone(order: BrokerOrder) -> Optional[str]:
    ref = order.instrument
    if getattr(ref, "kind", None) != "equity" or not ref.symbol:
        return None
    resolved = resolve_ref(ref.symbol)
    return getattr(resolved, "tz", None) if resolved is not None else None


def order_view(attempt: Optional[Mapping[str, Any]]) -> Optional[OrderView]:
    """The order ``attempt`` (an ``order_attempts`` row) placed, in words, or
    None when there is no attempt or its order does not read."""
    if not attempt:
        return None
    try:
        order = BrokerOrder.from_json(attempt.get("order_json") or {})
        return OrderView(
            order=order,
            summary=_summary(attempt, order, with_mode=False),
            summary_live=_summary(attempt, order, with_mode=True),
            timezone=_timezone(order),
            currency=order.currency,
        )
    except Exception:
        logger.warning(
            "[Orders] describing attempt %s failed", attempt.get("attempt_id"), exc_info=True
        )
        return None
