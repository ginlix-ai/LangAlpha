"""Webull's order instructions, which are staged rather than placed.

Webull's hosted MCP server creates an *instruction* for every order it is
asked for, and each tool says so in its own description: it "does NOT place an
order directly", and the instruction becomes a real order only once the user
confirms it in the Webull App or Desktop. So every tool here is the staged
rung, IBKR's, and nothing this stack reads ever maps a row to working or
filled.

The argument shapes below are read off the published input schemas, which is
all Webull publishes: every place tool takes an ``account_id`` and a
``new_orders`` list capped at one entry, each entry a LIMIT order in Webull
OpenAPI's vocabulary (``BUY``/``SELL``, ``DAY``/``GTC``, ``entrust_type``
choosing between a count and ``total_cash_amount``, ``support_trading_session``
naming the hours). An option, single or strategy, states its contracts in
``legs``. A call carrying anything but exactly one order is recorded with
its arguments and no instrument, rather than as whichever entry came first.

The answers have no schema. What the descriptions promise is the most this
file leans on: a create answers with a ``message`` holding the confirmation
link, the pending list carries each instruction's id, status and order
parameters, and the processed list names the outcome as ``APPROVED`` (sent
on as an order), ``REJECTED`` (declined in the app or revoked) or ``EXPIRED``.
The id keys and row keys are read tolerantly, and an answer naming no id is
``unknown`` rather than a guess.

Event contracts and algo orders are not here. An event order buys the yes or
the no of a question, which no side or instrument in the neutral shape can
say, and an algo order is a TWAP, VWAP or participation schedule rather than
an order type. Both are refused at the gate instead of being recorded as
something they are not.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from src.server.services.brokerage_capabilities import OrderAction, order_tool
from src.server.services.brokerage_orders._coerce import (
    as_date,
    as_datetime,
    as_decimal,
    as_text,
    extras,
    norm_number,
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
    CryptoRef,
    EquityRef,
    Failure,
    FutureRef,
    InstrumentRef,
    Money,
    OptionRef,
    OrderMode,
    OrderOutcome,
    OrderType,
    Session,
    Side,
    TimeInForce,
)

VENDOR = "webull"

# The two reads that together say where every instruction stands: still
# waiting for the user, or processed with its outcome named. One listing reads
# the first and then the second, so an instruction missing from both is
# missing from everything Webull will say.
LIST_PENDING = "get_pending_instruction"
LIST_PROCESSED = "get_processed_instruction"

# Both reads take the page size as a string, default 20 and at most 100, and
# page by the id of the last row returned.
PAGE_SIZE = 100
_CURSOR = "last_instruction_id"

_ACCOUNT_KEY = "account_id"
_ORDERS_KEY = "new_orders"
_INSTRUCTION_ID_KEYS = ("instruction_id", "id")
_ROW_KEYS = ("instructions", "items", "list", "records")
_CREATED_KEYS = ("create_time", "created_at", "create_at")

_SIDES: dict[str, Side] = {"BUY": "buy", "SELL": "sell"}
_ORDER_TYPES: dict[str, OrderType] = {"LIMIT": "limit"}
_TIF: dict[str, TimeInForce] = {"DAY": "day", "GTC": "gtc"}

# ``CORE`` is regular hours; ``ALL`` adds the extended sessions; ``NIGHT`` is
# the overnight session alone and ``ALL_DAY`` the 24-hour book.
_SESSIONS: dict[str, Session] = {
    "CORE": "rth",
    "ALL": "rth_plus_ext",
    "NIGHT": "overnight",
    "ALL_DAY": "all_day",
}

_RIGHTS = {"CALL": "C", "PUT": "P"}

# The outcome each listed status names. ``APPROVED`` is the user sending it on
# from Webull's own client: it is an order there now, under Webull's id, and
# only a person there can trade it, so the ledger's part ends.
_STATES: dict[str, AttemptStatus] = {
    "PENDING": AttemptStatus.SUBMITTED,
    "APPROVED": AttemptStatus.HANDED_OFF,
    "REJECTED": AttemptStatus.CANCELLED,
    "EXPIRED": AttemptStatus.EXPIRED,
}

# Webull's US options settle 100 shares a contract, and no leg names a
# multiplier of its own.
_OPTION_MULTIPLIER = 100

# Every instruction trades the US market, the only one the schemas accept,
# and none names a currency.
CURRENCY = "USD"

_LINK = re.compile(r"https://\S+")


def _first(source: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if source.get(key) is not None:
            return source[key]
    return None


def _word(source: Mapping[str, Any], key: str) -> str:
    return str(source.get(key) or "").strip().upper()


def _instruction_id(source: Mapping[str, Any]) -> str | None:
    return as_text(_first(source, _INSTRUCTION_ID_KEYS))


def _only(items: Any) -> dict[str, Any] | None:
    """The one mapping a list holds, or None when it holds anything else."""
    if isinstance(items, list) and len(items) == 1 and isinstance(items[0], Mapping):
        return dict(items[0])
    return None


def _legs(order: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    legs = order.get("legs")
    if not isinstance(legs, list):
        return ()
    return tuple(dict(leg) for leg in legs if isinstance(leg, Mapping))


def _option(leg: Mapping[str, Any], underlying: str | None) -> OptionRef | None:
    underlying = as_text(leg.get("symbol")) or underlying
    if not underlying:
        return None
    return OptionRef(
        underlying=underlying,
        expiration=as_date(leg.get("option_expire_date")),
        strike=as_decimal(leg.get("strike_price")),
        right=_RIGHTS.get(_word(leg, "option_type")),
        multiplier=_OPTION_MULTIPLIER,
    )


def _instrument(
    asset_class: AssetClass, order: Mapping[str, Any]
) -> InstrumentRef | None:
    """What the order trades, in the words its own entry uses.

    A single-leg option names its contract only inside its one leg; a strategy
    is its legs and the strategy Webull names, which is what the approval card
    shows. Either one whose legs are not what its tool promises names nothing.
    """
    symbol = as_text(order.get("symbol"))
    if asset_class == "option":
        leg = _only(order.get("legs"))
        return _option(leg, symbol) if leg is not None else None
    if asset_class == "option_combo":
        legs = _legs(order)
        strategy = as_text(order.get("option_strategy"))
        if not legs and not strategy:
            return None
        return ComboRef(legs=legs or None, strategy=strategy)
    if not symbol:
        return None
    if asset_class == "crypto":
        return CryptoRef(pair=symbol)
    if asset_class == "future":
        return FutureRef(symbol=symbol, venue=as_text(order.get("market")))
    return EquityRef(symbol=symbol, venue=as_text(order.get("market")))


def _notional(order: Mapping[str, Any]) -> Money | None:
    """A cash-sized order's amount, which only an ``AMOUNT`` entrust carries."""
    if _word(order, "entrust_type") != "AMOUNT":
        return None
    amount = as_decimal(order.get("total_cash_amount"))
    return Money(amount=amount, currency=CURRENCY) if amount is not None else None


# What every order entry states that a field here holds. ``instrument_type`` and
# ``combo_type`` take one value per tool and say nothing the tool's name does
# not, ``option_strategy`` is the combo's strategy or a single leg's ``SINGLE``,
# and ``legs`` is the instrument.
_ORDER_KEYS = frozenset(
    {
        "symbol",
        "market",
        "side",
        "quantity",
        "entrust_type",
        "total_cash_amount",
        "limit_price",
        "instrument_type",
        "combo_type",
        "legs",
        "option_strategy",
    }
)


def _read(order: Mapping[str, Any], asset_class: AssetClass) -> dict[str, Any]:
    """One order entry's fields, and the ones nothing here has a word for."""
    order_type = _ORDER_TYPES.get(_word(order, "order_type"))
    tif = _TIF.get(_word(order, "time_in_force"))
    session = _SESSIONS.get(_word(order, "support_trading_session"))
    consumed = set(_ORDER_KEYS)
    # A value the vocabulary has no word for is not consumed, so it reaches
    # ``extras`` rather than disappearing between a None field and a key
    # nobody kept.
    if order_type is not None:
        consumed.add("order_type")
    if tif is not None:
        consumed.add("time_in_force")
    if session is not None:
        consumed.add("support_trading_session")
    instrument = _instrument(asset_class, order)
    if instrument is None:
        consumed -= {"symbol", "legs", "option_strategy"}
    return {
        "asset_class": asset_class,
        "instrument": instrument,
        "side": _SIDES.get(_word(order, "side")),
        "qty": as_decimal(order.get("quantity")),
        "notional": _notional(order),
        "order_type": order_type,
        "limit_price": as_decimal(order.get("limit_price")),
        "time_in_force": tif,
        "session": session,
        "extras": extras(order, consumed),
    }


def _stage(args: Mapping[str, Any], asset_class: AssetClass) -> BrokerOrder:
    order = _only(args.get(_ORDERS_KEY))
    outer = extras(
        args, {_ACCOUNT_KEY} if order is None else {_ACCOUNT_KEY, _ORDERS_KEY}
    )
    fields = (
        _read(order, asset_class)
        if order is not None
        else {"asset_class": asset_class, "extras": {}}
    )
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=as_text(args.get(_ACCOUNT_KEY)) or "",
        mode=OrderMode.STAGED,
        currency=CURRENCY,
        raw=dict(args),
        **{**fields, "extras": {**outer, **fields["extras"]}},
    )


def _revoke(args: Mapping[str, Any]) -> BrokerOrder:
    return BrokerOrder(
        vendor=VENDOR,
        account_ref=as_text(args.get(_ACCOUNT_KEY)) or "",
        mode=OrderMode.STAGED,
        target_ref=as_text(args.get("instruction_id")),
        extras=extras(args, {_ACCOUNT_KEY, "instruction_id"}),
        raw=dict(args),
    )


def _payload(envelope: Mapping[str, Any]) -> dict[str, Any]:
    data = envelope.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(envelope)


def _vendor_failure(envelope: Mapping[str, Any] | None) -> Failure | None:
    """Webull OpenAPI's error object: an ``error_code`` beside a ``message``."""
    if not envelope:
        return None
    code = as_text(envelope.get("error_code"))
    message = envelope.get("message")
    if code is None or not isinstance(message, str):
        return None
    return Failure(kind="vendor", code=code, message=as_text(message))


def _link(*texts: Any) -> str | None:
    """The confirmation link a create answers with, wherever it put it."""
    for text in texts:
        if isinstance(text, str) and (found := _LINK.search(text)):
            return found.group(0).rstrip(").,;\"'")
    return None


def _rows(envelope: Mapping[str, Any]) -> list[Any] | None:
    payload = _payload(envelope)
    rows = _first(payload, _ROW_KEYS)
    if rows is None and isinstance(envelope.get("data"), list):
        rows = envelope["data"]
    return rows if isinstance(rows, list) else None


def _size(order: BrokerOrder) -> str | None:
    """How big the order is, in the unit the caller chose.

    One field, because a cash-sized order and a share-sized one of the same
    symbol must not pass for each other.
    """
    amount = norm_number(order.notional.amount if order.notional else None)
    if amount:
        return f"usd {amount}"
    shares = norm_number(order.qty)
    return f"qty {shares}" if shares else None


def _token(ref: InstrumentRef | None) -> str | None:
    if isinstance(ref, (EquityRef, FutureRef)):
        return as_text(ref.symbol)
    if isinstance(ref, CryptoRef):
        return as_text(ref.pair)
    if isinstance(ref, OptionRef):
        parts = (
            ref.underlying,
            f"{ref.expiration:%Y-%m-%d}" if ref.expiration else "",
            norm_number(ref.strike),
            ref.right or "",
        )
        return " ".join(p for p in parts if p) or None
    if isinstance(ref, ComboRef):
        # The legs, not only the strategy: two verticals on different strikes
        # share a strategy name, side, size and price. Sorted, because a
        # listing need not keep the order the legs were sent in.
        legs = sorted(_leg_token(leg) for leg in ref.legs or ())
        return " | ".join(p for p in (as_text(ref.strategy), *legs) if p) or None
    return None


def _leg_token(leg: Mapping[str, Any]) -> str:
    """One leg as a combo's identity reads it: direction, ratio and contract."""
    contract = _token(_option(leg, None)) or as_text(leg.get("symbol")) or ""
    ratio = norm_number(leg.get("quantity")) or ""
    return " ".join(p for p in (_word(leg, "side"), ratio, contract) if p)


def _fingerprint(order: BrokerOrder | None) -> MatchKey | None:
    """An instruction's identity when its own id never came back.

    None unless the instrument, the side and the size are all there; those are
    what make two rows different orders rather than one. The rest is compared
    only where both sides name it.
    """
    if order is None:
        return None
    token = _token(order.instrument)
    size = _size(order)
    if token is None or order.side is None or size is None:
        return None
    return MatchKey.of(
        asset_class=order.asset_class,
        token=token,
        side=order.side,
        size=size,
        limit_price=norm_number(order.limit_price),
        tif=order.time_in_force,
    )


# What a listed row says it trades, for reading it the way its request was
# read. An option row's legs tell a single from a strategy.
_ROW_CLASSES: dict[str, AssetClass] = {
    "EQUITY": "equity",
    "CRYPTO": "crypto",
    "FUTURES": "future",
}


def _row_class(order: Mapping[str, Any]) -> AssetClass | None:
    kind = _word(order, "instrument_type")
    if kind == "OPTION":
        return (
            "option"
            if _word(order, "option_strategy") in ("", "SINGLE")
            else "option_combo"
        )
    return _ROW_CLASSES.get(kind)


def _listed(row: Any, tool: str) -> VendorOrder:
    instruction_id = _instruction_id(row) if isinstance(row, Mapping) else None
    if instruction_id is None:
        raise StatusReadError("listed instruction with no id")
    raw_status = as_text(row.get("status"))
    if raw_status is not None:
        status = _STATES.get(raw_status.upper(), AttemptStatus.UNKNOWN)
    else:
        # Being on the pending list is the state when the row does not say.
        status = (
            AttemptStatus.SUBMITTED if tool == LIST_PENDING else AttemptStatus.UNKNOWN
        )
    order = _only(row.get(_ORDERS_KEY)) or row
    asset_class = _row_class(order)
    listed = (
        BrokerOrder(vendor=VENDOR, mode=OrderMode.STAGED, **_read(order, asset_class))
        if asset_class is not None
        else None
    )
    created = as_datetime(_first(row, _CREATED_KEYS))
    return VendorOrder(
        vendor_order_id=instruction_id,
        outcome=OrderOutcome(
            status=status,
            raw_status=raw_status,
            vendor_order_id=instruction_id,
            created_at=created,
            raw=dict(row),
        ),
        match_key=_fingerprint(listed),
        placed_at=created,
    )


class WebullOrderAdapter(OrderAdapter):
    """The one place that knows a Webull instruction waits for the app."""

    vendor = VENDOR

    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        entry = order_tool(VENDOR, tool)
        if entry is None:
            return None
        data = dict(args or {})
        if entry.action is OrderAction.STAGE:
            return _stage(data, entry.asset_class)
        return _revoke(data)

    def vendor_outcome(
        self,
        tool: str,
        envelope: dict[str, Any] | None,
        text: str,
        request: BrokerOrder | None,
    ) -> OrderOutcome:
        failure = _vendor_failure(envelope)
        if failure is not None:
            return OrderOutcome(
                status=AttemptStatus.REJECTED_BY_VENDOR,
                failure=failure,
                raw=dict(envelope or {}),
            )
        entry = order_tool(VENDOR, tool)
        if envelope is None:
            return OrderOutcome(
                status=AttemptStatus.UNKNOWN,
                raw_status=as_text(text),
                action_url=_link(text),
            )
        payload = _payload(envelope)
        if entry is not None and entry.action is OrderAction.UNSTAGE:
            # A revoke turns the instruction REJECTED and places nothing.
            return OrderOutcome(
                status=AttemptStatus.CANCELLED,
                raw_status=as_text(payload.get("status")),
                vendor_order_id=_instruction_id(payload)
                or (request.target_ref if request is not None else None),
                raw=dict(envelope),
            )
        instruction_id = _instruction_id(payload)
        link = _link(payload.get("message"), envelope.get("message"), text)
        if instruction_id is None:
            # A create that named no instruction is a shape we have not met,
            # and reading "submitted" out of it would settle an attempt on
            # nothing: the instruction lists are what resolve an unknown one.
            return OrderOutcome(
                status=AttemptStatus.UNKNOWN, action_url=link, raw=dict(envelope)
            )
        return OrderOutcome(
            status=AttemptStatus.SUBMITTED,
            raw_status=as_text(payload.get("status")),
            vendor_order_id=instruction_id,
            # The instruction is written but not an order: this link is where
            # the user confirms it, and there is nowhere else to get it.
            action_url=link,
            created_at=as_datetime(_first(payload, _CREATED_KEYS)),
            raw=dict(envelope),
        )

    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        return _vendor_failure(envelope)

    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        # Both reads require the account, and an attempt that never recorded
        # one has no read to make and stays open for a person instead.
        account = (order.account_ref if order is not None else "").strip()
        if not account:
            return None
        return StatusQuery(
            LIST_PENDING, {_ACCOUNT_KEY: account, "page_size": str(PAGE_SIZE)}
        )

    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        envelope, text = body_parts(body)
        if envelope is None:
            raise StatusReadError(text or "no envelope")
        failure = _vendor_failure(envelope)
        if failure is not None:
            raise StatusReadError(failure.message or "vendor error")
        rows = _rows(envelope)
        if rows is None:
            # An answer with no list is not an account with no instructions,
            # and reading it as one would report an open attempt as gone.
            raise StatusReadError(text or "no instruction list")
        return [_listed(row, query.tool) for row in rows]

    def next_page(self, query: StatusQuery, body: ResultBody) -> StatusQuery | None:
        """The next page of this list, then the processed list after the pending.

        Neither read says whether it has more, so a full page is read as
        having more and the next one is asked for after its last id. A short
        page ends that list.
        """
        envelope, _ = body_parts(body)
        rows = _rows(envelope) if envelope is not None else None
        if rows is None:
            return None
        if len(rows) >= PAGE_SIZE:
            last = _instruction_id(rows[-1]) if isinstance(rows[-1], Mapping) else None
            if last is None:
                raise ListingIncomplete(
                    f"{query.tool} listed a full page and no cursor"
                )
            return StatusQuery(query.tool, {**query.args, _CURSOR: last})
        if query.tool == LIST_PENDING:
            account = {k: v for k, v in query.args.items() if k != _CURSOR}
            return StatusQuery(LIST_PROCESSED, account)
        return None

    def absent_status(self, action: str | None) -> AttemptStatus | None:
        """``handed_off`` for an instruction neither list carries any more.

        Webull lists a processed instruction with its outcome, so this is only
        the end of one whose record it no longer keeps. Only a person there
        can trade it, so Webull's record is the outcome and the ledger stops at
        the hand-off.
        """
        return AttemptStatus.HANDED_OFF if action == OrderAction.STAGE else None

    def match_key(self, order: BrokerOrder | None) -> MatchKey | None:
        return _fingerprint(order)
