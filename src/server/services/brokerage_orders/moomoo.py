"""moomoo's two order families, and the two envelopes they answer in.

One server carries both: the live tools take ``exchange.code`` strings and
answer ``{"s": "ok", "d": ...}``, the simulated ones take integer markets and
sides and answer ``{"ret_code": 0, "data": ...}``. A business failure arrives
inside a ToolMessage whose status is "success" in both families -- live as
``{"s": "error", "errcode": 28, "errmsg": ...}``, paper as ``ret_code: -5`` --
which is why the envelope decides the outcome and the tool status never does.

A third answer is not an envelope at all: a live replace, cancel or confirm
whose upstream call returned no payload comes back as the bare word ``no
data``, and it says that whether or not the order the call named exists. See
:func:`_unanswered`.

The live order object has its own vocabulary, which is not the request's:
``order_status``, ``dealt_qty``, ``dealt_avg_price``, ``updated_time``,
``last_err_msg``, and the instrument as one ``exchange.code`` string with no
separate exchange field. :func:`_live_row` is the only place that knows it.

No moomoo response has a published schema, so every mapping below is empirical
and anything unrecognized is carried out in ``raw`` and ``raw_status``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from src.server.services.brokerage_capabilities import OrderAction, order_tool
from src.server.services.brokerage_orders._coerce import (
    as_decimal,
    as_int,
    as_text,
    extras,
    norm_number,
    target_ids,
)
from src.server.services.brokerage_orders.base import (
    ListingIncomplete,
    MatchKey,
    OrderAdapter,
    ResultBody,
    StatusQuery,
    StatusReadError,
    VendorOrder,
    body_parts,
)
from src.server.services.brokerage_orders.models import (
    AssetClass,
    AttemptStatus,
    BrokerOrder,
    ComboRef,
    EquityRef,
    Failure,
    FutureRef,
    InstrumentRef,
    OpaqueRef,
    OptionRef,
    OrderMode,
    OrderOutcome,
)

VENDOR = "moomoo"

# The reads reconciliation asks an order's state with. Both are in the same
# capability group as the orders they report on, so a connection that could
# place the order can always be asked what became of it. History rather than
# ``account_orders_active``: an attempt is stale precisely because it may have
# left the active book already, and a filled or cancelled order is only in the
# list that keeps both.
LIVE_HISTORY = "account_orders_history"
PAPER_HISTORY = "sim_trade_history_order_list"

_SIDES = {
    "BUY": "buy",
    "SELL": "sell",
    "SELL_SHORT": "sell_short",
    "BUY_BACK": "buy_to_cover",
}
# The enum moomoo documents as "see Side enum" and publishes nowhere, the same
# table brokerage_tool_overlays.py hands the model.
_PAPER_SIDES = {1: "buy", 2: "sell", 3: "sell_short", 4: "buy_to_cover"}

_ORDER_TYPES = {
    "LIMIT": "limit",
    "MARKET": "market",
    "AUCTION": "auction",
    "AUCTION_LIMIT": "auction_limit",
    "STOP": "stop",
    "STOP_LIMIT": "stop_limit",
    "MARKET_IF_TOUCHED": "market_if_touched",
    "LIMIT_IF_TOUCHED": "limit_if_touched",
}
_PAPER_ORDER_TYPES = {1: "limit", 3: "market"}

_TIF = {"DAY": "day", "GTC": "gtc"}
_SESSIONS = {
    "RTH": "rth",
    "RTH+PRE/POST-MKT": "rth_plus_ext",
    "OVERNIGHT": "overnight",
    "ALL_DAY": "all_day",
}
_PAPER_SESSIONS = {1: "rth", 2: "rth_plus_ext"}

_OPTION_VENUES = frozenset({"CBOE"})
_FUTURE_VENUES = frozenset({"CME", "CBOT", "NYMEX", "COMEX", "HKFE"})

# The integer the simulated account list hands out, and the book it addresses.
# 100 and 2 are both the US book: you send 100 and the order comes back
# stamped 2, so a later cancel needs the one that was sent.
_PAPER_MARKETS: dict[int, tuple[str, AssetClass]] = {
    1: ("HK", "equity"),
    2: ("US", "equity"),
    11: ("US", "future"),
    100: ("US", "equity"),
}

# The simulated order states, which moomoo publishes as bare integers and
# documents nowhere. Only these three have been seen on a real paper order:
#
#   2  resting on the book, ``cum_qty`` "0"          -> working
#   4  done, ``cum_qty`` == ``qty``                  -> filled
#   5  cancelled                                     -> cancelled
#
# A code that is not here maps to ``unknown``, which reconciliation declines to
# write over what the row already says. That is deliberately where an
# unobserved code lands: guessing an integer wrong writes a wrong end state to
# the ledger, while guessing nothing only leaves the row where it was. The
# partial-fill code is the notable absence -- see :func:`_paper_status` for why
# it could not be provoked.
_PAPER_STATUSES = {
    2: AttemptStatus.WORKING,
    4: AttemptStatus.FILLED,
    5: AttemptStatus.CANCELLED,
}

# One page is enough to cover anything reconciliation is still waiting on: the
# reads are ordered newest first and an attempt older than a page of activity
# has long since been settled by the answer that did come back.
_STATUS_PAGE_SIZE = 100

# moomoo's live order states, as its own enum spells them. ``FAILED`` is the
# one that has been seen on a real listed order; the rest are still the enum's
# own names, and a name absent here maps to ``unknown``, which reconciliation
# declines to write over anything.
_LIVE_STATUSES = {
    "WAITING_SUBMIT": AttemptStatus.SUBMITTING,
    "SUBMITTING": AttemptStatus.SUBMITTING,
    "SUBMITTED": AttemptStatus.WORKING,
    "FILLED_PART": AttemptStatus.PARTIALLY_FILLED,
    "FILLED_ALL": AttemptStatus.FILLED,
    "CANCELLED_PART": AttemptStatus.CANCELLED,
    "CANCELLED_ALL": AttemptStatus.CANCELLED,
    "SUBMIT_FAILED": AttemptStatus.REJECTED_BY_VENDOR,
    "FAILED": AttemptStatus.REJECTED_BY_VENDOR,
    "TIMEOUT": AttemptStatus.FAILED,
    "DELETED": AttemptStatus.CANCELLED,
}

# The venue a live order was routed to, as the exchange half of a ``code``
# spells it, mapped onto the ``trd_market`` the order reads accept. A venue
# this table does not name asks nothing rather than guessing a book: an empty
# answer from the wrong book reads as "the vendor has no such order", which
# settles a submitting attempt as failed.
#
# CBOE is deliberately absent. It is the exchange half of an options code, and
# ``trd_market`` names no options book, so there is nothing to map it to that
# would not be a guess.
_LIVE_MARKETS = {
    "US": "US", "NASDAQ": "US", "NYSE": "US", "AMEX": "US", "ARCA": "US",
    "HK": "HK", "SEHK": "HK", "HKCC": "HKCC", "SSE": "HKCC", "SZSE": "HKCC",
    "SG": "SG", "SGX": "SG", "CA": "CA", "JP": "JP", "KR": "KR",
    "CME": "FUTURES", "CBOT": "FUTURES", "NYMEX": "FUTURES",
    "COMEX": "FUTURES", "HKFE": "FUTURES",
}

_OCC = re.compile(
    r"^(?P<root>[A-Z]{1,6})(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<right>[CP])(?P<strike>\d{5,8})$"
)


def _target_order_id(request: BrokerOrder | None) -> str | None:
    """The order a replace or a cancel names, which is the order it is about.

    A place creates an id and has none to name; an amend is only ever about one
    that already exists, and moomoo does not echo it back. Carrying it makes
    the row say which order the call touched, and is what lets reconciliation
    go and ask when the answer said nothing.
    """
    return as_text((request.extras.get("order_id") if request else None))


def _live_venue(code: Any) -> str | None:
    """The exchange half of a live ``code``, which is the only place it is.

    A listed live order carries ``US.F``, never a separate exchange field, and
    the exchange is what a later cancel has to be sent back with.
    """
    head, dot, _ = str(code or "").strip().partition(".")
    return head.upper() if dot and head else None


def _micros(value: Any) -> datetime | None:
    """A moomoo microsecond timestamp, which it sends as a string."""
    micros = as_int(value)
    if micros is None:
        return None
    try:
        return datetime.fromtimestamp(0, tz=UTC) + timedelta(microseconds=micros)
    except (OverflowError, ValueError):
        return None


def _occ_ref(symbol: str, code: str) -> OptionRef | None:
    matched = _OCC.match(symbol.strip().upper())
    if not matched:
        return None
    try:
        expiration = date(
            2000 + int(matched["yy"]), int(matched["mm"]), int(matched["dd"])
        )
    except ValueError:
        return None
    return OptionRef(
        underlying=matched["root"],
        expiration=expiration,
        strike=Decimal(matched["strike"]) / 1000,
        right=matched["right"],
        vendor_instrument_id=code,
    )


def _combo_legs(info: Any) -> tuple[dict, ...] | None:
    """The legs of an MLEG order, passed through.

    ``multi_leg_info`` is an object with zero declared properties, so its shape
    is whatever the model sent and the only safe thing to do is carry it.
    """
    if isinstance(info, Mapping):
        legs = info.get("legs")
        if isinstance(legs, list):
            return tuple(dict(leg) for leg in legs if isinstance(leg, Mapping)) or None
        return (dict(info),) if info else None
    if isinstance(info, list):
        return tuple(dict(leg) for leg in info if isinstance(leg, Mapping)) or None
    return None


def _live_instrument(
    args: Mapping[str, Any], code: str, venue: str | None, symbol: str
) -> tuple[AssetClass, InstrumentRef | None]:
    if str(args.get("order_class") or "").strip().upper() == "MLEG":
        return "option_combo", ComboRef(legs=_combo_legs(args.get("multi_leg_info")))
    if not code:
        return "other", None
    if venue in _OPTION_VENUES:
        return "option", _occ_ref(symbol, code) or OpaqueRef(raw_code=code)
    if venue in _FUTURE_VENUES:
        return "future", FutureRef(symbol=symbol, venue=venue)
    return "equity", EquityRef(symbol=symbol, venue=venue)


def _live_place(args: Mapping[str, Any]) -> BrokerOrder:
    consumed = {
        "acc_id",
        "code",
        "side",
        "price",
        "qty",
        "order_type",
        "time_in_force",
        "session",
        "aux_price",
        "remark",
        "order_class",
        "multi_leg_info",
    }
    code = str(args.get("code") or "").strip()
    head, dot, tail = code.partition(".")
    venue, symbol = (head.upper(), tail) if dot else (None, code)
    asset_class, instrument = _live_instrument(args, code, venue, symbol)
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=str(args.get("acc_id") or ""),
        mode=OrderMode.LIVE,
        asset_class=asset_class,
        instrument=instrument,
        side=_SIDES.get(str(args.get("side") or "").strip().upper()),
        qty=as_decimal(args.get("qty")),
        order_type=_ORDER_TYPES.get(str(args.get("order_type") or "").strip().upper()),
        limit_price=as_decimal(args.get("price")),
        stop_price=as_decimal(args.get("aux_price")),
        time_in_force=_TIF.get(str(args.get("time_in_force") or "").strip().upper()),
        session=_SESSIONS.get(str(args.get("session") or "").strip().upper()),
        note=as_text(args.get("remark")),
        extras=extras(args, consumed),
        raw=dict(args),
    )


def _live_amend(args: Mapping[str, Any], action: OrderAction) -> BrokerOrder | None:
    confirm = action is OrderAction.CONFIRM
    ids = ("confirm_id", "order_id", "exchange") if confirm else ("order_id", "exchange")
    targets = target_ids(args, ids)
    # A confirm carries no order of its own. With neither id there is nothing
    # to attach an attempt to, and inventing an empty one would only widen the
    # ledger with rows no vendor call matches.
    if confirm and not (targets.get("confirm_id") or targets.get("order_id")):
        return None
    consumed = {"acc_id", "qty", "price", "aux_price", *ids}
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=str(args.get("acc_id") or ""),
        mode=OrderMode.LIVE,
        # A confirm acts on the confirmation moomoo handed back, not on the
        # order behind it; a replace and a cancel act on the order itself.
        target_ref=as_text(targets.get("confirm_id")) or as_text(targets.get("order_id")),
        qty=as_decimal(args.get("qty")),
        limit_price=as_decimal(args.get("price")),
        stop_price=as_decimal(args.get("aux_price")),
        extras={**targets, **extras(args, consumed)},
        raw=dict(args),
    )


def _paper_place(args: Mapping[str, Any]) -> BrokerOrder:
    consumed = {
        "acc_id",
        "market",
        "symbol",
        "order_side",
        "order_type",
        "qty",
        "price",
        "text",
        "order_trade_time_type",
    }
    market = as_int(args.get("market"))
    venue, asset_class = _PAPER_MARKETS.get(market) or (None, "equity")
    symbol = str(args.get("symbol") or "")
    instrument: InstrumentRef = (
        FutureRef(symbol=symbol, venue=venue)
        if asset_class == "future"
        else EquityRef(symbol=symbol, venue=venue)
    )
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=str(args.get("acc_id") or ""),
        mode=OrderMode.PAPER,
        asset_class=asset_class,
        instrument=instrument,
        side=_PAPER_SIDES.get(as_int(args.get("order_side"))),
        qty=as_decimal(args.get("qty")),
        order_type=_PAPER_ORDER_TYPES.get(as_int(args.get("order_type"))),
        limit_price=as_decimal(args.get("price")),
        session=_PAPER_SESSIONS.get(as_int(args.get("order_trade_time_type"))),
        note=as_text(args.get("text")),
        extras={**target_ids(args, ("market",)), **extras(args, consumed)},
        raw=dict(args),
    )


def _paper_amend(args: Mapping[str, Any]) -> BrokerOrder:
    consumed = {"acc_id", "market", "order_id", "new_qty", "new_price"}
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=str(args.get("acc_id") or ""),
        mode=OrderMode.PAPER,
        target_ref=as_text(args.get("order_id")),
        qty=as_decimal(args.get("new_qty")),
        limit_price=as_decimal(args.get("new_price")),
        extras={
            **target_ids(args, ("order_id", "market")),
            **extras(args, consumed),
        },
        raw=dict(args),
    )


def _order_payload(data: Any, request: BrokerOrder | None) -> dict[str, Any]:
    """The one order object in a response, whichever way the vendor wrapped it."""
    if isinstance(data, Mapping):
        orders = data.get("orders")
        if not isinstance(orders, list):
            return dict(data)
        data = orders
    if isinstance(data, list):
        wanted = str((request.extras.get("order_id") if request else None) or "")
        rows = [row for row in data if isinstance(row, Mapping)]
        for row in rows:
            if wanted and str(row.get("order_id") or "") == wanted:
                return dict(row)
        return dict(rows[0]) if rows else {}
    return {}


def _route(
    request: BrokerOrder | None, payload: Mapping[str, Any], *, paper: bool
) -> dict[str, str]:
    """The tokens a later cancel needs, which the vendor does not echo back.

    The paper market is read from the request first: the response stamps the
    order with a different integer than the one the cancel API accepts.
    """
    if paper:
        market = (request.extras.get("market") if request else None)
        if market is None:
            market = payload.get("market")
        return {"market": str(market)} if market is not None else {}
    exchange = request.extras.get("exchange") if request else None
    if exchange is None and request is not None:
        exchange = getattr(request.instrument, "venue", None)
    if exchange is None:
        exchange = _live_venue(payload.get("code"))
    return {"exchange": str(exchange)} if exchange else {}


def _vendor_rejection(
    *,
    code: Any,
    message: Any,
    route: dict[str, str],
    envelope: Mapping[str, Any],
    vendor_order_id: str | None = None,
) -> OrderOutcome:
    return OrderOutcome(
        status=AttemptStatus.REJECTED_BY_VENDOR,
        failure=Failure(kind="vendor", code=as_text(code), message=as_text(message)),
        vendor_order_id=vendor_order_id,
        route=route,
        raw=dict(envelope),
    )


def _cancelled_when(action: OrderAction | None) -> AttemptStatus:
    """What a command that answered no state did, which is what it was asked to do."""
    return (
        AttemptStatus.CANCELLED
        if action is OrderAction.CANCEL
        else AttemptStatus.SUBMITTED
    )


def _paper_row(
    row: Mapping[str, Any],
    *,
    absent: AttemptStatus = AttemptStatus.UNKNOWN,
    route: Mapping[str, str] | None = None,
    raw: Mapping[str, Any] | None = None,
) -> OrderOutcome:
    """One simulated order, in the vocabulary both the command and the list use.

    ``cum_qty`` and ``avg_fill_price`` are the fill, and they are the same two
    fields on a resting order and a done one -- "0" and "0" while it waits, the
    executed quantity and price once it is done. They are read for every state
    rather than only the filled one, which is what makes a cancel that happened
    after a partial keep the shares it did get: the status decides the
    lifecycle, never whether there is a fill to carry.

    ``absent`` is what a row carrying no ``status`` means, which only a command
    answer does. A state integer we have not seen stays ``unknown`` either way.
    """
    raw_status = row.get("status")
    return OrderOutcome(
        status=(
            _PAPER_STATUSES.get(as_int(raw_status), AttemptStatus.UNKNOWN)
            if raw_status is not None
            else absent
        ),
        raw_status=as_text(raw_status),
        vendor_order_id=as_text(row.get("order_id")),
        route=dict(route or {}),
        filled_qty=as_decimal(row.get("cum_qty")),
        avg_fill_price=as_decimal(row.get("avg_fill_price")),
        created_at=_micros(row.get("create_time")),
        updated_at=_micros(row.get("update_time")),
        raw=dict(row if raw is None else raw),
    )


def _paper_outcome(
    envelope: Mapping[str, Any], action: OrderAction | None, request: BrokerOrder | None
) -> OrderOutcome:
    payload = _order_payload(envelope.get("data"), request)
    route = _route(request, payload, paper=True)
    code = as_int(envelope.get("ret_code"))
    if code is None:
        # A mapping that names no usable code settles nothing, the same way a
        # body that is no envelope does. Reading absence as a rejection would
        # fabricate a terminal verdict and throw the order id away with it,
        # where ``unknown`` leaves an open row for reconciliation to go and ask.
        return _paper_row(
            payload, absent=AttemptStatus.UNKNOWN, route=route, raw=envelope
        )
    if code != 0:
        error = envelope.get("error") if isinstance(envelope.get("error"), Mapping) else {}
        return _vendor_rejection(
            code=envelope.get("ret_code"),
            message=envelope.get("ret_msg") or error.get("message"),
            route=route,
            envelope=envelope,
        )
    return _paper_row(
        payload, absent=_cancelled_when(action), route=route, raw=envelope
    )


def _first(*sources: Mapping[str, Any], key: str) -> Any:
    """A field moomoo puts at the envelope's top level, or inside its payload.

    ``need_order_confirm`` arrives beside ``s``, not inside ``d`` -- an error
    envelope carries it with no ``d`` at all -- while every field that
    describes the order itself is inside. Reading both in order costs nothing
    and stops one more undocumented placement from silently reading as absent.
    """
    for source in sources:
        value = source.get(key)
        if value is not None:
            return value
    return None


def _live_row(
    row: Mapping[str, Any],
    order_id: str | None,
    *,
    status: AttemptStatus | None = None,
    route: Mapping[str, str] | None = None,
    raw: Mapping[str, Any] | None = None,
) -> OrderOutcome:
    """One live order, in the vocabulary both the command and the lists use.

    moomoo's own vocabulary for an order object, which is not the vocabulary of
    the request that made it: the state is ``order_status``, the fill is
    ``dealt_qty``/``dealt_avg_price``, and the second timestamp is
    ``updated_time``. ``last_err_msg`` is where a rejected order keeps its
    reason, and it is the only place the reason survives. These are the only
    live order objects moomoo has been seen to emit, and an answer that spells
    them some other way still reaches the ledger whole in ``raw``.

    ``status`` is the caller's wherever the command's own answer decides the
    state rather than the echoed row: a cancel that came back ``ok`` cancelled
    the order whatever word the row it echoed carries.
    """
    raw_status = row.get("order_status")
    settled = status or _LIVE_STATUSES.get(
        str(raw_status or "").strip().upper(), AttemptStatus.UNKNOWN
    )
    reason = as_text(row.get("last_err_msg"))
    venue = _live_venue(row.get("code"))
    return OrderOutcome(
        status=settled,
        raw_status=as_text(raw_status),
        vendor_order_id=order_id,
        route=(
            dict(route)
            if route is not None
            else ({"exchange": venue} if venue else {})
        ),
        filled_qty=as_decimal(row.get("dealt_qty")),
        avg_fill_price=as_decimal(row.get("dealt_avg_price")),
        failure=(
            Failure(kind="vendor", code=as_text(raw_status), message=reason)
            if reason and settled is AttemptStatus.REJECTED_BY_VENDOR
            else None
        ),
        created_at=_micros(row.get("create_time")),
        updated_at=_micros(row.get("updated_time")),
        raw=dict(row if raw is None else raw),
    )


# The states a live command's answer is taken at its word for. Any other word
# records submitted for reconciliation to settle: an in-flight one would read as
# ``submitting``, an answer that never came back, and ``TIMEOUT`` as a failure
# nothing confirmed.
_LIVE_ANSWERED = frozenset(
    {
        AttemptStatus.WORKING,
        AttemptStatus.PARTIALLY_FILLED,
        AttemptStatus.FILLED,
        AttemptStatus.CANCELLED,
        AttemptStatus.REJECTED_BY_VENDOR,
    }
)


def _live_answered(
    row: Mapping[str, Any], action: OrderAction | None
) -> AttemptStatus:
    """The state an ``ok`` live answer settles, trusting its row past submission."""
    if action is OrderAction.CANCEL:
        return AttemptStatus.CANCELLED
    named = _LIVE_STATUSES.get(str(row.get("order_status") or "").strip().upper())
    return named if named in _LIVE_ANSWERED else AttemptStatus.SUBMITTED


def _live_outcome(
    envelope: Mapping[str, Any], action: OrderAction | None, request: BrokerOrder | None
) -> OrderOutcome:
    payload = _order_payload(envelope.get("d"), request)
    route = _route(request, payload, paper=False)
    named = _target_order_id(request)
    state = envelope.get("s")
    stated = str(state).strip() if state is not None else ""
    if not stated:
        # ``s`` absent is not ``s`` saying no. The same rule as the paper path,
        # and the order id the body or the request names is exactly what the
        # read that settles this row later needs to carry.
        return _live_row(
            payload,
            as_text(payload.get("order_id")) or named,
            status=AttemptStatus.UNKNOWN,
            route=route,
            raw=envelope,
        )
    if stated.casefold() != "ok":
        # ``errcode``/``errmsg``, and nothing else: three real rejections
        # across two tools all named themselves that way. A body that spells it
        # otherwise still reaches the ledger whole in ``raw``.
        return _vendor_rejection(
            code=envelope.get("errcode") or envelope.get("s"),
            message=envelope.get("errmsg"),
            route=route,
            envelope=envelope,
            vendor_order_id=named,
        )
    if _first(envelope, payload, key="need_order_confirm"):
        return OrderOutcome(
            status=AttemptStatus.PENDING_CONFIRM,
            raw_status=as_text(payload.get("order_status")),
            vendor_order_id=as_text(payload.get("order_id")) or named,
            route=route,
            confirm_token=as_text(_first(envelope, payload, key="confirm_id")),
            raw=dict(envelope),
        )
    return _live_row(
        payload,
        as_text(payload.get("order_id")) or named,
        status=_live_answered(payload, action),
        route=route,
        raw=envelope,
    )


def _unanswered(paper: bool, text: str, request: BrokerOrder | None) -> OrderOutcome:
    """A body that is no envelope, which is how a live amend usually answers.

    ``no data`` is the whole of it, and moomoo says it for an order that exists
    and for one that never did, so the call's own answer settles nothing. The
    status stays ``unknown`` -- reconciliation reads that as an open row and
    goes and asks -- and the route and the order the call named ride along,
    because without them there is no read to make.
    """
    return OrderOutcome(
        status=AttemptStatus.UNKNOWN,
        raw_status=as_text(text),
        vendor_order_id=_target_order_id(request),
        route=_route(request, {}, paper=paper),
        raw={"text": text} if text else {},
    )


def _live_code(instrument: InstrumentRef | None) -> str | None:
    """An instrument back in the ``exchange.code`` spelling moomoo lists it as."""
    venue = getattr(instrument, "venue", None)
    symbol = getattr(instrument, "symbol", None) or getattr(
        instrument, "raw_code", None
    )
    if not symbol:
        return None
    return f"{venue}.{symbol}" if venue else str(symbol)


def _fingerprint(
    *,
    acc: Any,
    symbol: Any,
    side: str | None,
    order_type: str | None,
    qty: Any,
    price: Any,
    note: Any,
) -> MatchKey | None:
    """The fingerprint that pairs an order we sent with one moomoo lists.

    There is no field the two sides share besides the order itself, so the
    identity is the order: account, instrument, direction, kind, size, price and
    the free-text remark moomoo echoes back verbatim. The kind is in it because
    a limit and a stop limit for the same size at the same price are different
    instructions, and without it one could be settled as the other.

    Only the instrument and the size have to be there. Every other field is
    compared where both sides name it and skipped where either does not, which
    is what a book listing an order without the remark or the price it was sent
    with actually leaves us: nothing about that field, rather than evidence of
    a difference.

    Without a symbol or a size there is nothing to recognize and the answer is
    None, which leaves the attempt unresolved rather than paired with whatever
    else was placed that minute.
    """
    code = str(symbol or "").strip().upper()
    size = norm_number(qty)
    if not code or not size:
        return None
    return MatchKey.of(
        acc=str(acc or "").strip(),
        symbol=code,
        side=side,
        order_type=order_type,
        qty=size,
        price=norm_number(price),
        note=str(note or "").strip(),
    )


def _paper_key(
    *,
    acc: Any,
    symbol: Any,
    side: Any,
    order_type: Any,
    qty: Any,
    price: Any,
    note: Any,
) -> str | None:
    direction = _PAPER_SIDES.get(as_int(side)) or str(side or "").strip().casefold()
    kind = (
        _PAPER_ORDER_TYPES.get(as_int(order_type))
        or str(order_type or "").strip().casefold()
    )
    return _fingerprint(
        acc=acc,
        symbol=symbol,
        side=direction,
        order_type=kind,
        qty=qty,
        price=price,
        note=note,
    )


def _live_key(
    *, acc: Any, code: Any, side: Any, order_type: Any, qty: Any, price: Any, note: Any
) -> str | None:
    """The live fingerprint, over the fields both live lists echo back.

    The same facts as the paper one, spelled the way the live side spells them:
    the instrument is the whole ``exchange.code`` string, the kind is a name
    rather than an integer, and the remark comes back as ``remark`` rather than
    ``text``.

    Every mapping here reads its own output back to itself, which is what lets
    one function take the vendor's spelling from a listed row and the parsed one
    from a request we sent.
    """
    direction = _SIDES.get(str(side or "").strip().upper()) or str(
        side or ""
    ).strip().casefold()
    kind = _ORDER_TYPES.get(str(order_type or "").strip().upper()) or str(
        order_type or ""
    ).strip().casefold()
    return _fingerprint(
        acc=acc,
        symbol=code,
        side=direction,
        order_type=kind,
        qty=qty,
        price=price,
        note=note,
    )


def _paper_market(
    order: BrokerOrder | None, route: Mapping[str, str] | None
) -> int | None:
    """The market integer a paper read has to be asked with.

    The one the account list hands out and the request was sent with, never the
    one the answer is stamped with: 100 and 2 are the same book and only 100 is
    accepted back.
    """
    for source in (route or {}, (order.extras if order else {}), (order.raw if order else {})):
        market = as_int(source.get("market"))
        if market is not None:
            return market
    return None


def _live_market(order: BrokerOrder | None, route: Mapping[str, str] | None) -> str | None:
    venue = (route or {}).get("exchange") or getattr(
        getattr(order, "instrument", None), "venue", None
    )
    return _LIVE_MARKETS.get(str(venue or "").strip().upper())


def _turn_page(
    query: StatusQuery, arg: str, cursor: Any, *, more: bool
) -> StatusQuery | None:
    """The same read asked for the next page, or None at the end of the list.

    Only ``more`` being false is the end. A vendor that says it has more and
    names no next page, or names the one just sent, cannot be read further and
    has not finished answering either, so the listing is incomplete rather than
    complete: reading an echoed cursor as the end would let an attempt missing
    from the pages read be failed as never placed.
    """
    cursor = as_text(cursor)
    if not more:
        return None
    if not cursor or query.args.get(arg) == cursor:
        raise ListingIncomplete(
            f"{query.tool} says it has more orders but named no next page"
        )
    return StatusQuery(query.tool, {**query.args, arg: cursor})


def _status_rows(payload: Any) -> list[Any] | None:
    """The orders a list read carried, or None when it carried no list at all.

    An answer with no list is not an account with no orders, nor is a listed
    entry with no id a missing order, and reading either that way would settle
    an attempt whose answer never came back as never placed.
    """
    if isinstance(payload, Mapping):
        payload = payload.get("orders")
    if not isinstance(payload, list):
        return None
    return payload


def _paper_status(query: StatusQuery, envelope: Mapping[str, Any]) -> list[VendorOrder]:
    """The orders one simulated read listed, fill included.

    The simulator fills a marketable order in full, at a reference price, two
    to three seconds after it is accepted, whatever the book shows: 24,000
    shares against 1,200 displayed and 80,000 against 400 both came back
    ``status`` 4 with ``cum_qty`` equal to ``qty``. It models no depth, so a
    partial fill could not be provoked and the code that would mean one is
    still unseen.
    """
    if as_int(envelope.get("ret_code")) != 0:
        raise StatusReadError(str(envelope.get("ret_msg") or envelope.get("ret_code")))
    rows = _status_rows(envelope.get("data"))
    if rows is None:
        raise StatusReadError("no order list")
    market = query.args.get("market")
    route = {"market": str(market)} if market is not None else {}
    listed = []
    for row in rows:
        order_id = as_text(row.get("order_id")) if isinstance(row, Mapping) else None
        if order_id is None:
            raise StatusReadError("listed order with no id")
        listed.append(
            VendorOrder(
                vendor_order_id=order_id,
                outcome=_paper_row(row, route=route),
                match_key=_paper_key(
                    acc=query.args.get("acc_id"),
                    symbol=row.get("symbol"),
                    side=row.get("side"),
                    order_type=row.get("order_type"),
                    qty=row.get("qty"),
                    price=row.get("price"),
                    note=row.get("text"),
                ),
                placed_at=_micros(row.get("create_time")),
            )
        )
    return listed


def _live_status(query: StatusQuery, envelope: Mapping[str, Any]) -> list[VendorOrder]:
    """The orders one live read listed, from either live order list.

    ``account_orders_history`` and ``account_orders_active`` answer in the same
    shape -- ``{"s": "ok", "d": {"orders": [...], "page_flag": ..., "completed":
    ...}}`` -- and carry the same order objects, so one reader serves both.
    """
    if str(envelope.get("s") or "").strip().casefold() != "ok":
        raise StatusReadError(
            str(envelope.get("errmsg") or envelope.get("msg") or envelope.get("s"))
        )
    rows = _status_rows(envelope.get("d"))
    if rows is None:
        raise StatusReadError("no order list")
    listed = []
    for row in rows:
        order_id = as_text(row.get("order_id")) if isinstance(row, Mapping) else None
        if order_id is None:
            raise StatusReadError("listed order with no id")
        listed.append(
            VendorOrder(
                vendor_order_id=order_id,
                outcome=_live_row(row, order_id),
                match_key=_live_key(
                    acc=query.args.get("acc_id"),
                    code=row.get("code"),
                    side=row.get("side"),
                    order_type=row.get("order_type"),
                    qty=row.get("qty"),
                    price=row.get("price"),
                    note=row.get("remark"),
                ),
                placed_at=_micros(row.get("create_time")),
            )
        )
    return listed


class MoomooOrderAdapter(OrderAdapter):
    """The one place that knows ``ret_code`` from ``s``."""

    vendor = VENDOR

    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        entry = order_tool(VENDOR, tool)
        if entry is None:
            return None
        data = dict(args or {})
        placing = entry.action is OrderAction.PLACE
        if entry.mode is OrderMode.PAPER:
            return _paper_place(data) if placing else _paper_amend(data)
        return _live_place(data) if placing else _live_amend(data, entry.action)

    def vendor_outcome(
        self,
        tool: str,
        envelope: dict[str, Any] | None,
        text: str,
        request: BrokerOrder | None,
    ) -> OrderOutcome:
        entry = order_tool(VENDOR, tool)
        declared_paper = entry is not None and entry.mode is OrderMode.PAPER
        if envelope is None:
            return _unanswered(declared_paper, text, request)
        # The envelope decides which family answered: one server carries both,
        # and only the body says which of the two spoke.
        paper = (
            "ret_code" in envelope
            if ("ret_code" in envelope or "s" in envelope)
            else declared_paper
        )
        action = entry.action if entry else None
        return (
            _paper_outcome(envelope, action, request)
            if paper
            else _live_outcome(envelope, action, request)
        )

    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        """None: moomoo turns an order down in the body, under a success status.

        Both families do -- live as ``{"s": "error", ...}``, paper as a
        non-zero ``ret_code`` -- so a tool error here really is the transport's,
        and reading one as a rejection would say an order that may be in the
        book never reached it.
        """
        return None

    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        if order is None or not order.account_ref:
            return None
        if order.mode == OrderMode.PAPER:
            market = _paper_market(order, route)
            if market is None:
                return None
            return StatusQuery(
                PAPER_HISTORY,
                {
                    "acc_id": order.account_ref,
                    "market": market,
                    "page_size": _STATUS_PAGE_SIZE,
                },
            )
        market = _live_market(order, route)
        if market is None:
            return None
        return StatusQuery(
            LIVE_HISTORY,
            {
                "acc_id": order.account_ref,
                "trd_market": market,
                "page_size": _STATUS_PAGE_SIZE,
            },
        )

    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        envelope, text = body_parts(body)
        if envelope is None:
            raise StatusReadError(text or "no envelope")
        if query.tool == PAPER_HISTORY:
            return _paper_status(query, envelope)
        return _live_status(query, envelope)

    def next_page(self, query: StatusQuery, body: ResultBody) -> StatusQuery | None:
        """Both families page, each in its own words.

        Paper answers ``{"pagination": {"has_more": bool, "next_key": str}}``
        beside ``data`` and takes the key back as ``next_key``; live answers
        ``{"d": {"page_flag": str, "completed": bool}}`` and takes the flag
        back as ``page_flag``, an empty flag with ``completed`` true being the
        observed last page.
        """
        envelope, _ = body_parts(body)
        if envelope is None:
            return None
        if query.tool == PAPER_HISTORY:
            paging = envelope.get("pagination")
            if not isinstance(paging, Mapping):
                # This vendor always says where it stands in the list, an empty
                # book included: a read of a window holding no orders came back
                # with ``has_more`` false and a next key all the same. So a
                # paper answer carrying orders and no pagination is malformed,
                # and taking it for one complete page would let an attempt
                # missing from it be settled as never placed.
                raise ListingIncomplete(
                    f"{query.tool} listed orders and named no pagination"
                )
            return _turn_page(
                query, "next_key", paging.get("next_key"),
                more=bool(paging.get("has_more")),
            )
        page = envelope.get("d")
        if not isinstance(page, Mapping):
            return None
        completed = page.get("completed")
        if not isinstance(completed, bool):
            # This family says where it stands with a boolean. Anything else, a
            # missing field included, leaves the end of the list unsaid, and
            # reading that as the end would let an attempt missing from the
            # pages read be failed as never placed.
            raise ListingIncomplete(
                f"{query.tool} named no completion state"
            )
        return _turn_page(
            query, "page_flag", page.get("page_flag"), more=not completed,
        )

    def match_key(self, order: BrokerOrder | None) -> str | None:
        if order is None:
            return None
        raw = order.raw or {}
        instrument = order.instrument
        if order.mode == OrderMode.PAPER:
            return _paper_key(
                acc=order.account_ref,
                symbol=getattr(instrument, "symbol", None) or raw.get("symbol"),
                side=order.side if order.side is not None else raw.get("order_side"),
                order_type=(
                    order.order_type
                    if order.order_type is not None
                    else raw.get("order_type")
                ),
                qty=order.qty if order.qty is not None else raw.get("qty"),
                price=(
                    order.limit_price
                    if order.limit_price is not None
                    else raw.get("price")
                ),
                note=order.note or raw.get("text"),
            )
        if order.mode != OrderMode.LIVE:
            return None
        return _live_key(
            acc=order.account_ref,
            code=raw.get("code") or _live_code(instrument),
            side=order.side or raw.get("side"),
            order_type=order.order_type or raw.get("order_type"),
            qty=order.qty if order.qty is not None else raw.get("qty"),
            price=(
                order.limit_price if order.limit_price is not None else raw.get("price")
            ),
            note=order.note or raw.get("remark"),
        )
