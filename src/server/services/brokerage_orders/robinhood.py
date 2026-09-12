"""Robinhood's order tools, as the live broker answered them.

Equity and single-leg option placement and cancellation, and all three order
reads, were run end to end against a real account, so the shapes below are the
ones that came back. Crypto got as far as the account's region allowed:
``preview_crypto_order`` answered and ``place_crypto_order`` was refused, so
the crypto order body here is the previewed one, which the vendor documents a
placement as mirroring. Read from the vendor's schemas and still unverified:
option exercise and its cancel, and multi-leg combos. Nothing here raises on a
shape it does not know; it classifies what it can and carries the rest out in
``raw``.

Two vocabularies, not one. A request says ``type`` alone (``stop_market`` for
equity and options, ``stop_loss`` for crypto) and sizes a notional order with
``dollar_amount``; the order that comes back splits the same fact across
``type`` and ``trigger`` and renames the money ``dollar_based_amount``. Both
are read, because the request and the answer to it land in the same record.

Two account spellings, and a third that is not one. Equity and option tools
take ``account_number`` and crypto takes ``rhs_account_number``; the
``account_id`` a crypto answer carries is an internal UUID for the crypto
sub-account that no request accepts.

A cancel is the one answer that identifies nothing: ``{"accepted": true}`` is
the whole body, so the order it addressed comes off the request that named it.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from src.server.services.brokerage_capabilities import (
    OrderAction,
    OrderTool,
    order_tool,
)
from src.server.services.brokerage_orders._coerce import (
    as_date,
    as_datetime,
    as_decimal,
    as_int,
    as_text,
    extras,
    norm_number,
    target_ids,
)
from src.server.services.brokerage_orders.base import (
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
    CryptoRef,
    EquityRef,
    Failure,
    InstrumentRef,
    Money,
    OpaqueRef,
    OptionRef,
    OrderMode,
    OrderOutcome,
)

VENDOR = "robinhood"

# The read that reports what became of an order of each class, and the name
# that read gives the account. All three are account-scoped and reject a query
# without one, and all three take an ``order_id`` that narrows the page to the
# one order.
_STATUS_TOOLS: dict[str, tuple[str, str]] = {
    "equity": ("get_equity_orders", "account_number"),
    "option": ("get_option_orders", "account_number"),
    "crypto": ("get_crypto_orders", "rhs_account_number"),
    "option_combo": ("get_option_orders", "account_number"),
}
_STATUS_CLASSES: dict[str, AssetClass] = {
    "get_equity_orders": "equity",
    "get_option_orders": "option",
    "get_crypto_orders": "crypto",
}
# Both spellings a request uses for the same account. ``account_id``, which
# only an answer carries, is deliberately absent: it is a UUID for the crypto
# sub-account, and reading it as the account ref would put a number no tool
# accepts in the column every later read is built from.
_ACCOUNT_KEYS = ("account_number", "rhs_account_number")
_ORDER_ID_KEYS = ("order_id", "id")

# Robinhood is a US broker and every amount it reports is dollars. The one
# place it names a currency at all is inside ``dollar_based_amount``.
CURRENCY = "USD"

_SIDES = {"buy": "buy", "sell": "sell"}

# ``gfd`` is the default and the only other value is ``gtc``. Anything else a
# later schema adds stays unmapped in ``raw``.
_TIF = {"gfd": "day", "day": "day", "gtc": "gtc"}

# ``market_hours`` with the shared ``_hours`` suffix taken off. ``regular`` and
# ``extended`` are the two the live account produced; the rest come from the
# vendor's prose, where ``curb`` is its word for the extended session, so the
# two curb values fold onto the sessions they cover.
_SESSIONS = {
    "regular": "rth",
    "extended": "rth_plus_ext",
    "regular_curb": "rth_plus_ext",
    "all_day": "all_day",
    "regular_curb_overnight": "all_day",
    "overnight": "overnight",
}

# What a request says. Equity and options spell a stop ``stop_market``; crypto
# spells the same order ``stop_loss``, and nothing else reads either word, so
# the two live in one table.
_REQUEST_TYPES = {
    "market": "market",
    "limit": "limit",
    "stop_market": "stop",
    "stop_loss": "stop",
    "stop_limit": "stop_limit",
}
# What the order that comes back says, across its two axes.
_ANSWERED_TYPES = {
    ("market", "immediate"): "market",
    ("limit", "immediate"): "limit",
    ("market", "stop"): "stop",
    ("limit", "stop"): "stop_limit",
}

_RIGHTS = {"call": "C", "c": "C", "put": "P", "p": "P"}

# Equity, option and exercise-event states in one table: they overlap and none
# of the three spells a state the others would read differently.
_STATES = {
    "new": AttemptStatus.SUBMITTED,
    "queued": AttemptStatus.SUBMITTED,
    "unconfirmed": AttemptStatus.SUBMITTED,
    "locating": AttemptStatus.SUBMITTED,
    "confirmed": AttemptStatus.WORKING,
    "pending_cancelled": AttemptStatus.WORKING,
    "partially_filled": AttemptStatus.PARTIALLY_FILLED,
    # Terminal at the vendor: the fill it got is kept on the row, and the
    # rest will never fill, so the lifecycle ends as cancelled rather than
    # resting in an open state the reconciler would read forever.
    "partially_filled_rest_cancelled": AttemptStatus.CANCELLED,
    "filled": AttemptStatus.FILLED,
    "cancelled": AttemptStatus.CANCELLED,
    "canceled": AttemptStatus.CANCELLED,
    "voided": AttemptStatus.CANCELLED,
    "reversed": AttemptStatus.CANCELLED,
    "rejected": AttemptStatus.REJECTED_BY_VENDOR,
    "locate_failed": AttemptStatus.REJECTED_BY_VENDOR,
    # The venue turned the order down and the vendor said so. Only a transport
    # error may take the lifecycle's ``failed``.
    "failed": AttemptStatus.REJECTED_BY_VENDOR,
}

# A vendor error arrives as prose, not as the ``{"data": ..., "guide": ...}``
# envelope, and these two openings are the only ones observed.
_API_ERROR = re.compile(r"^API error (?P<code>\d{3})\s*:\s*(?P<body>.*)$", re.DOTALL)
_ERROR_PREFIX = "error:"


def _first(args: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if args.get(key) is not None:
            return args[key]
    return None


def _account_ref(args: Mapping[str, Any]) -> str:
    return str(_first(args, _ACCOUNT_KEYS) or "")


def _order_type(args: Mapping[str, Any]) -> str | None:
    kind = str(args.get("type") or "").strip().casefold()
    trigger = as_text(args.get("trigger"))
    if trigger:
        return _ANSWERED_TYPES.get((kind, trigger.strip().casefold()))
    return _REQUEST_TYPES.get(kind)


def _session(args: Mapping[str, Any]) -> str | None:
    hours = str(args.get("market_hours") or "").strip().casefold()
    if hours:
        return _SESSIONS.get(hours.removesuffix("_hours"))
    return "rth_plus_ext" if args.get("extended_hours") is True else None


def _currency(args: Mapping[str, Any]) -> str:
    """The currency of an amount, which the vendor labels almost nowhere.

    A crypto order's ``currency_code`` is not read here: that names the asset
    (``XCN``), not the money it was bought with.
    """
    wrapped = args.get("dollar_based_amount")
    if isinstance(wrapped, Mapping):
        code = as_text(wrapped.get("currency_code") or wrapped.get("currency"))
        if code:
            return code
    return as_text(args.get("currency")) or CURRENCY


def _side(args: Mapping[str, Any]) -> str | None:
    """The side, which a single-leg option order states only inside the leg.

    A spread whose legs disagree has no one side, and saying nothing is truer
    on the approval card than naming either half of it.
    """
    named = str(args.get("side") or "").strip().casefold()
    if named:
        return _SIDES.get(named)
    legs = args.get("legs")
    if isinstance(legs, list):
        sides = {
            str(leg.get("side") or "").strip().casefold()
            for leg in legs
            if isinstance(leg, Mapping)
        }
        if len(sides) == 1:
            return _SIDES.get(sides.pop())
    return None


def _notional(args: Mapping[str, Any]) -> Money | None:
    """The dollar side of a notional order, flat in a request, wrapped in a reply.

    A crypto answer's ``entered_price`` holds this same amount, but only when
    the order was entered by dollars, and nothing in the body says which -- so
    it stays in ``raw`` rather than being read as money it may not be.
    """
    wrapped = args.get("dollar_based_amount")
    if isinstance(wrapped, Mapping):
        amount = as_decimal(wrapped.get("amount"))
    else:
        amount = as_decimal(_first(args, ("dollar_amount", "quote_amount")))
    if amount is None:
        return None
    return Money(amount=amount, currency=_currency(args))


def _equity_instrument(
    args: Mapping[str, Any],
) -> tuple[AssetClass, InstrumentRef | None]:
    symbol = as_text(args.get("symbol"))
    if symbol:
        return "equity", EquityRef(symbol=symbol)
    opaque = as_text(_first(args, ("instrument_id", "instrument")))
    return ("equity", OpaqueRef(raw_code=opaque)) if opaque else ("other", None)


def _leg_option(
    leg: Mapping[str, Any], *, underlying: str | None = None
) -> InstrumentRef | None:
    """A single option leg, described as far as the caller described it.

    A request leg carries only ``option_id``; the same leg in the answer adds
    the expiration, the strike and the right, while the chain symbol they all
    belong to sits on the order above them and is passed back down.
    """
    underlying = (
        as_text(_first(leg, ("chain_symbol", "symbol", "underlying_symbol")))
        or underlying
    )
    expiration = as_date(_first(leg, ("expiration_date", "expiration")))
    strike = as_decimal(_first(leg, ("strike_price", "strike")))
    right = _RIGHTS.get(str(_first(leg, ("option_type", "right")) or "").casefold())
    option_id = as_text(_first(leg, ("option_id", "option", "instrument_id", "id")))
    if underlying and (expiration or strike or right):
        return OptionRef(
            underlying=underlying,
            expiration=expiration,
            strike=strike,
            right=right,
            vendor_instrument_id=option_id,
        )
    return OpaqueRef(raw_code=option_id) if option_id else None


def _option_instrument(
    args: Mapping[str, Any],
) -> tuple[AssetClass, InstrumentRef | None]:
    """One leg is an option, two or more are a combo, and the legs are the truth.

    The vendor names a strategy too, but only in the answer and only as
    ``opening_strategy`` / ``closing_strategy``, so it rides along with the legs
    rather than standing in for them.
    """
    strategy = as_text(
        _first(args, ("strategy", "opening_strategy", "closing_strategy"))
    )
    chain = as_text(_first(args, ("chain_symbol", "symbol", "underlying_symbol")))
    legs = args.get("legs")
    carried = (
        tuple(dict(leg) for leg in legs if isinstance(leg, Mapping))
        if isinstance(legs, list)
        else ()
    )
    if len(carried) > 1:
        return "option_combo", ComboRef(legs=carried, strategy=strategy)
    single = (
        _leg_option(carried[0], underlying=chain)
        if carried
        else _leg_option(args, underlying=chain)
    )
    if single is not None:
        return "option", single
    if strategy:
        return "option_combo", ComboRef(strategy=strategy)
    return "other", None


def _crypto_instrument(
    args: Mapping[str, Any],
) -> tuple[AssetClass, InstrumentRef | None]:
    pair = as_text(_first(args, ("symbol", "currency_pair", "pair")))
    return ("crypto", CryptoRef(pair=pair)) if pair else ("other", None)


_INSTRUMENT_KEYS: dict[AssetClass, tuple[str, ...]] = {
    "equity": ("symbol", "instrument_id", "instrument"),
    "option": (
        "legs",
        "option_id",
        "option",
        "instrument_id",
        "strategy",
        "opening_strategy",
        "closing_strategy",
        "chain_symbol",
        "symbol",
        "underlying_symbol",
        "expiration_date",
        "expiration",
        "strike_price",
        "strike",
        "option_type",
        "right",
    ),
    "crypto": ("symbol", "currency_pair", "pair"),
}


def _instrument(
    asset_class: AssetClass, args: Mapping[str, Any]
) -> tuple[AssetClass, InstrumentRef | None]:
    if asset_class == "option":
        return _option_instrument(args)
    if asset_class == "crypto":
        return _crypto_instrument(args)
    return _equity_instrument(args)


def _place(args: Mapping[str, Any], asset_class: AssetClass) -> BrokerOrder:
    consumed = {
        *_ACCOUNT_KEYS,
        *_INSTRUMENT_KEYS[asset_class],
        "side",
        "type",
        "trigger",
        "time_in_force",
        "quantity",
        "asset_quantity",
        "dollar_amount",
        "dollar_based_amount",
        "quote_amount",
        "price",
        "limit_price",
        "stop_price",
        "market_hours",
        "extended_hours",
        "currency",
    }
    resolved, instrument = _instrument(asset_class, args)
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=_account_ref(args),
        mode=OrderMode.LIVE,
        asset_class=resolved,
        instrument=instrument,
        side=_side(args),
        qty=as_decimal(_first(args, ("quantity", "asset_quantity"))),
        notional=_notional(args),
        order_type=_order_type(args),
        limit_price=as_decimal(_first(args, ("price", "limit_price"))),
        stop_price=as_decimal(args.get("stop_price")),
        time_in_force=_TIF.get(str(args.get("time_in_force") or "").casefold()),
        session=_session(args),
        currency=_currency(args),
        extras=extras(args, consumed),
        raw=dict(args),
    )


def _cancel(args: Mapping[str, Any], entry: OrderTool) -> BrokerOrder:
    # A cancelled exercise is addressed by the option it was written against,
    # not by the exercise event, which is the one cancel that names no order.
    undoing_exercise = entry.action is OrderAction.CANCEL_EXERCISE
    ids = ("option_id",) if undoing_exercise else _ORDER_ID_KEYS
    asset_class, instrument = (
        _option_instrument(args) if undoing_exercise else ("other", None)
    )
    consumed = {*_ACCOUNT_KEYS, *ids}
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=_account_ref(args),
        mode=OrderMode.LIVE,
        # An exercise cancel names an option contract, and a contract is not an
        # order: putting it here would point the ledger's parent lookup at
        # whatever order happens to carry that number as its own id.
        target_ref=(
            None if undoing_exercise else as_text(_first(args, _ORDER_ID_KEYS))
        ),
        asset_class=entry.asset_class if instrument is None else asset_class,
        instrument=instrument,
        extras={**target_ids(args, ids), **extras(args, consumed)},
        raw=dict(args),
    )


def _exercise(args: Mapping[str, Any]) -> BrokerOrder:
    """An exercise is order-shaped with no side, price or time in force.

    It still gets a record: the option and the size are the whole of what a
    reader needs, and the ledger has nowhere else to keep them.
    """
    consumed = {*_ACCOUNT_KEYS, *_INSTRUMENT_KEYS["option"], "quantity"}
    asset_class, instrument = _option_instrument(args)
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=_account_ref(args),
        mode=OrderMode.LIVE,
        asset_class=asset_class,
        instrument=instrument,
        qty=as_decimal(args.get("quantity")),
        extras=extras(args, consumed),
        raw=dict(args),
    )


def _status_rows(data: Mapping[str, Any]) -> list[Any] | None:
    """Every order in a list read, whichever key the vendor put them under.

    None when no key holds a list, which is a different answer from an empty one.
    """
    for key in ("orders", "results", "items", "data"):
        rows = data.get(key)
        if isinstance(rows, list):
            return rows
    return None


def _data(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """What the vendor put under ``data``, beside the prose ``guide``."""
    data = envelope.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(envelope)


def _record(data: Mapping[str, Any]) -> dict[str, Any]:
    """The one order or exercise event inside ``data``, however it was named."""
    for key in ("order", "event"):
        inner = data.get(key)
        if isinstance(inner, Mapping):
            return dict(inner)
    for key in ("events", "orders"):
        rows = data.get(key)
        if isinstance(rows, list):
            picked = [row for row in rows if isinstance(row, Mapping)]
            if picked:
                return dict(picked[0])
    return dict(data)


def _failure_text(source: Mapping[str, Any]) -> tuple[str | None, str | None] | None:
    """The vendor's own words for a turned-down order, or None if it took it."""
    error = source.get("error")
    if isinstance(error, Mapping):
        message = error.get("message") or error.get("detail")
        return as_text(error.get("code")), as_text(message)
    if isinstance(error, str) and error.strip():
        return None, error.strip()
    errors = source.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, Mapping):
            message = first.get("message") or first.get("detail")
            return as_text(first.get("code")), as_text(message)
        return None, as_text(first)
    for key in ("detail", "reject_reason", "cancel_reason"):
        text = as_text(source.get(key))
        if text:
            return None, text
    return None


def _rejection(text: str, prose: Failure) -> OrderOutcome:
    return OrderOutcome(
        status=AttemptStatus.REJECTED_BY_VENDOR, raw_status=text, failure=prose
    )


def _prose_failure(text: str) -> Failure | None:
    """A vendor error, which arrives as prose rather than as the envelope.

    ``API error 409: {"detail": ...}`` carries the upstream status and a JSON
    body inside a string; ``error: FORBIDDEN ...`` carries neither.
    """
    matched = _API_ERROR.match(text.strip())
    if matched:
        body = matched["body"].strip()
        try:
            decoded = json.loads(body)
        except (ValueError, TypeError):
            decoded = None
        if isinstance(decoded, Mapping):
            body = as_text(decoded.get("detail") or decoded.get("message")) or body
        return Failure(kind="vendor", code=matched["code"], message=body)
    if text.strip().casefold().startswith(_ERROR_PREFIX):
        return Failure(kind="vendor", message=text.strip())
    return None


def _fees(
    payload: Mapping[str, Any], *, beside: Mapping[str, Any] | None = None
) -> Money | None:
    """The money the vendor took, under whichever of its names it used.

    An equity order answers a scalar ``fees``; a crypto order carries ``fee``
    in a list row and ``estimated_fee`` beside the order rather than in it; a
    review nests the breakdown under the same ``fees`` key and totals it in
    ``total_fee``. An option order answers no fee at all.
    """
    wrapped = payload.get("fees")
    if isinstance(wrapped, Mapping):
        amount = as_decimal(_first(wrapped, ("total_fee", "total_fees")))
    else:
        amount = as_decimal(_first(payload, ("fees", "total_fees", "fee")))
    if amount is None and beside is not None:
        amount = as_decimal(_first(beside, ("estimated_fee", "fee")))
    if amount is None:
        return None
    return Money(amount=amount, currency=_currency(payload))


def _cancel_ack(
    data: Mapping[str, Any],
    envelope: Mapping[str, Any],
    *,
    order_id: str | None = None,
    exercise: bool = False,
) -> OrderOutcome | None:
    """A cancel answer, which reports its own acceptance rather than a state.

    ``accepted: true`` means the broker took the request, not that the order is
    cancelled: the cancel is asynchronous, so the attempt is submitted and the
    reconciler settles it by reading ``order_id``, the order the request
    addressed, which is the vendor's own instruction for how to finish a cancel.

    An exercise cancel is final as answered: no tool lists exercise events, so
    nothing could ever read one back, and an open status would poll forever.
    Accepted or a positive count is cancelled; a count of zero undid nothing.
    """
    accepted = data.get("accepted")
    if accepted is False:
        failure = _failure_text(data) or _failure_text(envelope) or (None, None)
        return OrderOutcome(
            status=AttemptStatus.REJECTED_BY_VENDOR,
            raw_status="accepted=false",
            vendor_order_id=order_id,
            failure=Failure(kind="vendor", code=failure[0], message=failure[1]),
            raw=dict(envelope),
        )
    if accepted is True:
        return OrderOutcome(
            status=AttemptStatus.CANCELLED if exercise else AttemptStatus.SUBMITTED,
            raw_status="accepted",
            vendor_order_id=order_id,
            raw=dict(envelope),
        )
    count = as_int(data.get("cancelled_count"))
    if count is None:
        return None
    if count > 0:
        return OrderOutcome(
            status=AttemptStatus.CANCELLED,
            raw_status=f"cancelled_count={count}",
            raw=dict(envelope),
        )
    return OrderOutcome(
        status=AttemptStatus.REJECTED_BY_VENDOR,
        raw_status=f"cancelled_count={count}",
        failure=Failure(
            kind="vendor", code="nothing_queued", message="no queued exercise to cancel"
        ),
        raw=dict(envelope),
    )


_ANSWER_MARKERS = ("state", "status", "id", "order_id", "accepted", "cancelled_count")

# The two actions that take something back rather than create it, and the only
# ones whose answer can be a bare acknowledgement.
_CANCELLING = (OrderAction.CANCEL, OrderAction.CANCEL_EXERCISE)

# The classes whose listed orders repeat enough of the request to be paired
# with an attempt that never got an id back. Crypto is left out: a request
# names the pair (``XCN-USD``) and the order names only the asset (``XCN``), so
# the two do not compare without inventing the other half.
_MATCHABLE = frozenset({"equity", "option", "option_combo"})


def _ref_token(ref: InstrumentRef | None) -> str | None:
    """The instrument, as the shortest string both sides of a match agree on."""
    if isinstance(ref, EquityRef):
        return as_text(ref.symbol)
    if isinstance(ref, OptionRef):
        return as_text(ref.vendor_instrument_id)
    if isinstance(ref, OpaqueRef):
        return as_text(ref.raw_code)
    if isinstance(ref, ComboRef):
        ids = sorted(
            token
            for leg in ref.legs
            if (token := as_text(_first(leg, ("option_id", "id")))) is not None
        )
        return ",".join(ids) or None
    return None


def _size(order: BrokerOrder) -> str | None:
    """How big the order is, in the unit the caller chose.

    One field, not two: a dollar-based order states an amount and the vendor
    divides the shares out of it, so the two units are alternatives. Keeping
    them apart would let a one-dollar order and a one-share order of the same
    stock pass for each other, neither of them naming the field the other
    filled.
    """
    amount = norm_number(order.notional.amount if order.notional else None)
    if amount:
        return f"usd {amount}"
    shares = norm_number(order.qty)
    return f"qty {shares}" if shares else None


def _fingerprint(order: BrokerOrder | None) -> MatchKey | None:
    """What an attempt and a listed order share when no id links them.

    Only what the request stated and the answer repeats, and only field by
    field. The vendor fills in every field but the account, the instrument, the
    side and the size -- an option order's ``type``, which it defaults to
    limit, among them -- and the listing then carries the value the vendor
    chose. A field only one side names says nothing about the order, so it
    takes no part in the comparison; a market order's price is left out of both
    sides by the same rule, since the vendor takes it from the last trade.

    Two orders alike in every field both sides name are indistinguishable here
    on purpose: the reconciler settles nothing on an ambiguous pair.
    """
    if order is None or order.asset_class not in _MATCHABLE:
        return None
    token = _ref_token(order.instrument)
    size = _size(order)
    if token is None or order.side is None or size is None:
        return None
    return MatchKey.of(
        asset_class=order.asset_class,
        token=token,
        side=order.side,
        size=size,
        order_type=order.order_type,
        limit_price=(
            None if order.order_type == "market" else norm_number(order.limit_price)
        ),
        stop_price=norm_number(order.stop_price),
    )


def _order_record(
    row: Mapping[str, Any],
    *,
    absent: AttemptStatus,
    vendor_order_id: str | None,
    beside: Mapping[str, Any] | None = None,
    raw: Mapping[str, Any] | None = None,
) -> OrderOutcome:
    """One order as both a command's answer and a listed row spell it.

    Robinhood answers a placement with the same order object its listing
    carries, so the fill, the fees and the two timestamps are read once here.
    ``absent`` is what a row naming no state means, which only a command answer
    does: it was taken, and the reconciler settles it.
    """
    raw_state = as_text(row.get("state")) or as_text(row.get("status"))
    return OrderOutcome(
        status=(
            _STATES.get(raw_state.casefold(), AttemptStatus.UNKNOWN)
            if raw_state
            else absent
        ),
        raw_status=raw_state,
        vendor_order_id=vendor_order_id,
        filled_qty=as_decimal(
            _first(row, ("cumulative_quantity", "processed_quantity"))
        ),
        avg_fill_price=as_decimal(
            _first(row, ("average_price", "average_fill_price"))
        ),
        fees=_fees(row, beside=beside),
        created_at=as_datetime(row.get("created_at")),
        updated_at=as_datetime(_first(row, ("updated_at", "last_transaction_at"))),
        raw=dict(row if raw is None else raw),
    )


def _outcome(
    envelope: Mapping[str, Any], entry: OrderTool | None, request: BrokerOrder | None
) -> OrderOutcome:
    data = _data(envelope)
    payload = _record(data)
    raw_state = as_text(payload.get("state")) or as_text(payload.get("status"))
    vendor_order_id = as_text(_first(payload, ("id", "order_id")))
    cancelling = entry is not None and entry.action in _CANCELLING
    if raw_state is None and cancelling:
        # An exercise cancel names the contract, not an order, so its id is not
        # one a status read could look up.
        addressed = (
            (as_text(request.target_ref) or as_text(request.extras.get("order_id")))
            if request is not None and entry.action is not OrderAction.CANCEL_EXERCISE
            else None
        )
        acknowledged = _cancel_ack(
            data,
            envelope,
            order_id=addressed,
            exercise=entry.action is OrderAction.CANCEL_EXERCISE,
        )
        if acknowledged is not None:
            return acknowledged
    failure = _failure_text(payload) or _failure_text(data) or _failure_text(envelope)
    status = _STATES.get(raw_state.casefold()) if raw_state else None
    if status is AttemptStatus.REJECTED_BY_VENDOR or (status is None and failure):
        code, message = failure or (None, None)
        return OrderOutcome(
            status=AttemptStatus.REJECTED_BY_VENDOR,
            raw_status=raw_state,
            vendor_order_id=vendor_order_id,
            failure=Failure(kind="vendor", code=code, message=message),
            raw=dict(envelope),
        )
    # Every answer arrives under ``data`` and names a field of the record it
    # carries. A body that does neither is a shape we have not met, and
    # reading "submitted" out of it would settle an attempt on nothing.
    if (raw_state and status is None) or (
        "data" not in envelope and not any(k in payload for k in _ANSWER_MARKERS)
    ):
        return OrderOutcome(
            status=AttemptStatus.UNKNOWN,
            raw_status=raw_state,
            vendor_order_id=vendor_order_id,
            raw=dict(envelope),
        )
    return _order_record(
        payload,
        absent=AttemptStatus.SUBMITTED,
        vendor_order_id=vendor_order_id,
        beside=data,
        raw=envelope,
    )


class RobinhoodOrderAdapter(OrderAdapter):
    """The one place that knows a cancel is accepted before it is done."""

    vendor = VENDOR

    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        entry = order_tool(VENDOR, tool)
        if entry is None:
            return None
        data = dict(args or {})
        if entry.action is OrderAction.PLACE:
            return _place(data, entry.asset_class)
        if entry.action in _CANCELLING:
            return _cancel(data, entry)
        return _exercise(data)

    def vendor_outcome(
        self,
        tool: str,
        envelope: dict[str, Any] | None,
        text: str,
        request: BrokerOrder | None,
    ) -> OrderOutcome:
        if envelope is None:
            prose = _prose_failure(text) if text else None
            if prose is not None:
                return _rejection(text, prose)
            return OrderOutcome(status=AttemptStatus.UNKNOWN, raw_status=as_text(text))
        # No route: an order id addresses a Robinhood order on its own, and the
        # account it belongs to is a column of the attempt.
        return _outcome(envelope, order_tool(VENDOR, tool), request)

    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        """The broker's own words for a turned-down order, when it wrote them.

        Only the two openings ``_prose_failure`` knows prove the broker rather
        than the plumbing wrote the string; anything else keeps its transport
        failure, because calling an unrecognised error a rejection would claim
        the order never reached the venue on the strength of a string.
        """
        return _prose_failure(text) if text else None

    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        if order is None:
            return None
        named = _STATUS_TOOLS.get(order.asset_class)
        if named is None:
            return None
        tool, account_key = named
        # Every read is account-scoped and the vendor rejects one without an
        # account, so an attempt that never recorded which account it was for
        # has no read to make and stays open for a person instead.
        account = (order.account_ref or "").strip()
        if not account:
            return None
        args: dict[str, Any] = {account_key: account}
        if vendor_order_id:
            args["order_id"] = vendor_order_id
        return StatusQuery(tool, args)

    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        envelope, text = body_parts(body)
        if envelope is None:
            raise StatusReadError(text or "no envelope")
        data = _data(envelope)
        rows = _status_rows(data)
        failure = _failure_text(envelope) or _failure_text(data)
        if rows is None or (not rows and failure):
            # An answer with no list is not an account with no orders, nor is a
            # listed entry with no id a missing order, and reading either that way
            # would settle an unanswered attempt as never placed.
            raise StatusReadError(text or "no order list")
        asset_class = _STATUS_CLASSES.get(query.tool)
        listed = []
        for row in rows:
            order_id = (
                as_text(_first(row, _ORDER_ID_KEYS))
                if isinstance(row, Mapping)
                else None
            )
            if order_id is None:
                raise StatusReadError("listed order with no id")
            listed.append(
                VendorOrder(
                    vendor_order_id=order_id,
                    # A listed order and the request that made it go through
                    # the same reading, which is what makes the two
                    # fingerprints comparable.
                    match_key=(
                        _fingerprint(_place(row, asset_class))
                        if asset_class is not None
                        else None
                    ),
                    outcome=_order_record(
                        row,
                        absent=AttemptStatus.UNKNOWN,
                        vendor_order_id=order_id,
                    ),
                    placed_at=as_datetime(row.get("created_at")),
                )
            )
        return listed

    def match_key(self, order: BrokerOrder | None) -> str | None:
        return _fingerprint(order)
