"""IBKR's two order families: staged instructions and live orders.

Every staged shape below was read off live calls to ``api.ibkr.com`` rather
than inferred: the create answers ``{"id": ..., "url": ...}`` and nothing else,
the delete answers ``{}``, the list answers ``{"instructions": [...]}``, and a
refused call arrives as an MCP tool error carrying ``{"code", "message",
"data"}``. Three consequences drive the staged half.

There is no account argument and no account in any answer, so ``account_ref``
is empty by construction rather than by a failed lookup. There is no status
word anywhere: IBKR states an instruction by listing it and by dropping it from
the list, which is why the only status this file reads is the one it decides.
And the vendor puts a rejection on the error channel, so a tool error here is
the vendor's word rather than the transport's, and is classified as one.

An instruction is written into the real account but is not an order until a
human releases it in IBKR's own client, so no listed row maps past
``submitted``: there is nothing for this stack to watch working or fill. A
released instruction turns into a live order under a new id, with no link back,
and leaves the list; so does one deleted there or expired, and the list does not
say which. Leaving it is therefore the one end this file names,
``handed_off``, which reconciliation writes once a complete list stops carrying
an instruction.

The live half is read from IBKR's published tool descriptions, not from live
calls, so it parses only the fields those descriptions name. ``submit_order``,
``modify_order``, ``cancel_order`` and ``answer_order_question`` answer in one
shape: a non-null ``pending_question`` while the action is not yet applied,
naming the ``question_id`` an answer must quote; a ``correlation_id`` the answer
must echo; and once settled the ``order_id`` and its ``order_status``, with
``filled_quantity`` on a cancel. ``get_order_status`` reads one order by its id
in the vocabulary :data:`_LIVE_STATUSES` maps. Anything else a live answer
carries rides in ``raw``, and an answer naming none of it is ``unknown``, which
reconciliation asks about rather than trusts.

Two live rules differ from the staged ones. The account is still the consented
one and still unnamed, so ``account_ref`` stays empty. But IBKR's own words for
a live tool that errors are that the broker may already have accepted the
request, so a live error object is not read as a rejection: it is ``unknown``,
carrying the order it named where it named one.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.server.services.brokerage_capabilities import OrderAction, order_tool
from src.server.services.brokerage_orders._coerce import (
    as_datetime,
    as_decimal,
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
    AttemptStatus,
    BrokerOrder,
    EquityRef,
    Failure,
    Money,
    OpaqueRef,
    OrderMode,
    OrderOutcome,
    Side,
    TimeInForce,
)

VENDOR = "ibkr"

# The read that lists what is still staged. It takes no arguments and answers
# with every instruction on the account, which is what reconciliation needs:
# there is no per-instruction read to point at one.
LIST_INSTRUCTIONS = "get_order_instructions"

# The instruction's identity, on the create answer, in a listed row, and as the
# delete's one argument. A decimal string, not a number.
INSTRUCTION_ID = "id"

# The key the list read wraps its rows in.
INSTRUCTION_ROWS = "instructions"

# The read that settles a live order: one order, by the id a live answer named.
# It sits in the account group, which the live order group requires, so a
# connection that can place an order can always be asked what became of it.
ORDER_STATUS = "get_order_status"

# A live order's identity: on a settled answer, on the status read, and as the
# argument a modify, a cancel and the status read address the order by.
ORDER_ID = "order_id"

_SIDES: dict[str, Side] = {"BUY": "buy", "SELL": "sell"}

# The vendor publishes these two and no others. A third would be a change we
# have not read, so it stays unmapped rather than being guessed into a shape
# the ledger would then report as fact.
_ORDER_TYPES = {"MARKET": "market", "LIMIT": "limit"}

# The five the schema accepts. The last three are the reason the neutral
# vocabulary has words past ``day`` and ``gtc``: an overnight order and an
# opening-auction order expire on their own schedule, and flattening either
# onto ``day`` would report a different order than the one that was staged.
_TIF: dict[str, TimeInForce] = {
    "DAY": "day",
    "GTC": "gtc",
    "OVT": "overnight",
    "OND": "overnight_next_day",
    "OPG": "at_the_open",
}

# The four kinds the live tools accept, two more than staging publishes.
_LIVE_ORDER_TYPES = {
    "MARKET": "market",
    "LIMIT": "limit",
    "STOP": "stop",
    "STOP_LIMIT": "stop_limit",
}

# The three durations the live tools accept. Staging takes two more, which a
# live call would only have had refused, so they are not read here.
_LIVE_TIF: dict[str, TimeInForce] = {k: _TIF[k] for k in ("DAY", "OVT", "OND")}

# The twelve states ``get_order_status`` publishes, and only those: a thirteenth
# is a word we have not read, and it maps to ``unknown``, which reconciliation
# declines to write over what a row already says.
#
# ``PENDING_NEW`` is taken and not yet acknowledged; ``NEW`` is resting. The
# pending changes keep the order live, since a cancel is a request the venue
# may still fill ahead of. ``REPLACED`` is live too: a modify keeps its
# ``order_id``, so the order under that id is the one now working at the new
# parameters. ``DONE_FOR_THE_DAY`` is not an end: IBKR names it apart from
# ``EXPIRED``, and an ``OND`` order carries into the next day, so it stays open
# for the read that names one. ``EXPIRED`` is an end of its own rather than a
# cancel nobody asked for: it will not fill further, and any fill it got stays
# on the row.
_LIVE_STATUSES: dict[str, AttemptStatus] = {
    "PENDING_NEW": AttemptStatus.SUBMITTED,
    "NEW": AttemptStatus.WORKING,
    "PENDING_CANCEL": AttemptStatus.WORKING,
    "PENDING_REPLACE": AttemptStatus.WORKING,
    "PENDING_CANCEL_REPLACE": AttemptStatus.WORKING,
    "REPLACED": AttemptStatus.WORKING,
    "DONE_FOR_THE_DAY": AttemptStatus.WORKING,
    "PARTIALLY_FILLED": AttemptStatus.PARTIALLY_FILLED,
    "FILLED": AttemptStatus.FILLED,
    "CANCELLED": AttemptStatus.CANCELLED,
    "EXPIRED": AttemptStatus.EXPIRED,
    "REJECTED": AttemptStatus.REJECTED_BY_VENDOR,
}

# What an answer quotes back, every one of them copied from the result it
# answers.
_ANSWER_FIELDS = ("question_id", "value", "correlation_id")


def _contract_ref(source: Mapping[str, Any]) -> str | None:
    """The contract as IBKR addresses it, from a request or a listed row.

    ``contract_id_ex`` is the identity and wins whenever it is present, which
    is the vendor's own rule: ``contract_id`` is deprecated and a model that
    fills it with 0 beside a real ``contract_id_ex`` is the shape this survives.
    """
    extended = as_text(source.get("contract_id_ex"))
    if extended:
        return extended
    numeric = source.get("contract_id")
    if numeric in (None, 0, "0", ""):
        return None
    return as_text(numeric)


def _fingerprint(
    contract: str | None,
    side: str | None,
    qty: Any,
    order_type: str | None,
    limit_price: Any,
    tif: str | None,
) -> MatchKey | None:
    """One staged instruction's identity when its own id is unknown.

    None unless the instrument, the side and the size are all there: those
    three are what make two rows different orders rather than one, and a key
    missing any of them would pair an attempt with somebody else's instruction.

    The kind, the price and the duration are compared only where both sides
    name them. ``create_order_instruction`` requires ``side`` and nothing else,
    so a call may leave all three to IBKR, and the list then reports the values
    IBKR chose -- which is not a difference between the two orders and must not
    be read as one.
    """
    quantity = norm_number(qty)
    if not contract or not side or not quantity:
        return None
    return MatchKey.of(
        contract=contract,
        side=side,
        qty=quantity,
        order_type=order_type,
        limit_price=norm_number(limit_price),
        tif=tif,
    )


def _create(args: Mapping[str, Any]) -> BrokerOrder:
    contract = _contract_ref(args)
    order_type = _ORDER_TYPES.get(str(args.get("order_type") or "").strip().upper())
    tif = _TIF.get(str(args.get("time_in_force") or "").strip().upper())
    consumed = {
        "contract_id",
        "contract_id_ex",
        "side",
        "quantity",
        "limit_price",
    }
    # A value the vocabulary has no word for is not consumed, so it reaches
    # ``extras`` rather than disappearing between a None field and a key
    # nobody kept.
    if order_type is not None:
        consumed.add("order_type")
    if tif is not None:
        consumed.add("time_in_force")
    return BrokerOrder(
        vendor=VENDOR,
        # IBKR takes no account argument and names none in any answer: the
        # account is whichever one the OAuth token was issued for.
        account_ref="",
        mode=OrderMode.STAGED,
        # One ``contract_id_ex`` addresses every class IBKR stages, and neither
        # the call nor the answer says which one it named, so the class stays
        # unclaimed rather than being read off the id's punctuation.
        asset_class="other",
        instrument=OpaqueRef(raw_code=contract) if contract else None,
        side=_SIDES.get(str(args.get("side") or "").strip().upper()),
        qty=as_decimal(args.get("quantity")),
        order_type=order_type,
        limit_price=as_decimal(args.get("limit_price")),
        time_in_force=tif,
        extras={
            **({"contract_id_ex": contract} if contract else {}),
            **extras(args, consumed),
        },
        raw=dict(args),
    )


def _delete(args: Mapping[str, Any]) -> BrokerOrder:
    target = as_text(args.get(INSTRUCTION_ID))
    return BrokerOrder(
        vendor=VENDOR,
        account_ref="",
        mode=OrderMode.STAGED,
        target_ref=target,
        extras={
            **({INSTRUCTION_ID: target} if target else {}),
            **extras(args, {INSTRUCTION_ID}),
        },
        raw=dict(args),
    )


def _vendor_failure(envelope: Mapping[str, Any] | None) -> Failure | None:
    """IBKR's error object, or None when the body is not one.

    The server refuses an instruction on the tool-error channel rather than in
    a body, so this is the only place its own words for a rejection appear. A
    body carrying an integer ``code`` beside a ``message`` is that object and
    nothing else IBKR answers: a create answers ``id`` and ``url``, a delete
    answers nothing, and a list answers ``instructions``.
    """
    if not envelope:
        return None
    code = envelope.get("code")
    message = envelope.get("message")
    if isinstance(code, bool) or not isinstance(code, int):
        return None
    if not isinstance(message, str):
        return None
    return Failure(kind="vendor", code=str(code), message=as_text(message))


def _target_instruction(request: BrokerOrder | None) -> str | None:
    """The instruction a delete named, which is the only place its id survives."""
    if request is None:
        return None
    return (
        as_text(request.target_ref)
        or as_text(request.extras.get(INSTRUCTION_ID))
        or as_text(request.raw.get(INSTRUCTION_ID))
    )


def _route(request: BrokerOrder | None, payload: Mapping[str, Any]) -> dict[str, str]:
    contract = _contract_ref(request.extras if request else {}) or _contract_ref(
        payload
    )
    return {"contract_id_ex": contract} if contract else {}


def _outcome(
    envelope: Mapping[str, Any], action: OrderAction | None, request: BrokerOrder | None
) -> OrderOutcome:
    if action is OrderAction.UNSTAGE:
        # The delete answers ``{}``: the empty body is the confirmation, and
        # the id it removed exists only in the call that named it.
        return OrderOutcome(
            status=AttemptStatus.CANCELLED,
            vendor_order_id=_target_instruction(request),
            raw=dict(envelope),
        )
    instruction_id = as_text(envelope.get(INSTRUCTION_ID))
    if instruction_id is None:
        # A create that named no instruction is a shape we have not met, and
        # reading "submitted" out of it would settle an attempt on nothing:
        # ``get_order_instructions`` is what resolves an unknown one.
        return OrderOutcome(status=AttemptStatus.UNKNOWN, raw=dict(envelope))
    return OrderOutcome(
        status=AttemptStatus.SUBMITTED,
        vendor_order_id=instruction_id,
        route=_route(request, envelope),
        # The instruction is written but not an order: this link is the page a
        # human releases it on, and there is nowhere else to get it.
        action_url=as_text(envelope.get("url")),
        raw=dict(envelope),
    )


def _listed_instrument(row: Mapping[str, Any]) -> EquityRef | None:
    """The symbol a listed instruction names, which the create answer does not.

    This is the only place IBKR ever says what an instruction is for in words:
    the call takes a ``contract_id_ex`` and the create answers an id and a
    link. The row carries a bare ``symbol`` and no security type, so this is
    the shape that holds a symbol and nothing more -- the attempt's own
    ``asset_class`` is untouched and stays whatever the request could claim.
    """
    symbol = as_text(row.get("symbol"))
    return EquityRef(symbol=symbol) if symbol else None


def _listed(row: Any) -> VendorOrder:
    instruction_id = (
        as_text(row.get(INSTRUCTION_ID)) if isinstance(row, Mapping) else None
    )
    if instruction_id is None:
        raise StatusReadError("listed instruction with no id")
    contract = _contract_ref(row)
    created = as_datetime(row.get("creation_time"))
    return VendorOrder(
        vendor_order_id=instruction_id,
        outcome=OrderOutcome(
            # A listed row carries no status word. Being listed is the state,
            # and it is the only one a staged instruction has: it is not an
            # order, so it cannot be working, and it cannot fill.
            status=AttemptStatus.SUBMITTED,
            vendor_order_id=instruction_id,
            route={"contract_id_ex": contract} if contract else {},
            created_at=created,
            raw=dict(row),
        ),
        match_key=_fingerprint(
            contract,
            _SIDES.get(str(row.get("side") or "").strip().upper()),
            row.get("quantity"),
            _ORDER_TYPES.get(str(row.get("order_type") or "").strip().upper()),
            row.get("limit_price"),
            _TIF.get(str(row.get("tif") or "").strip().upper()),
        ),
        placed_at=created,
        instrument=_listed_instrument(row),
    )


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _live_order(args: Mapping[str, Any], action: OrderAction) -> BrokerOrder:
    """A submit, or a modify, which restates the whole order under its id."""
    contract = as_text(args.get("contract_id_ex"))
    order_type = _LIVE_ORDER_TYPES.get(_upper(args.get("order_type")))
    tif = _LIVE_TIF.get(_upper(args.get("tif")))
    cash = as_decimal(args.get("cash_size"))
    target = as_text(args.get(ORDER_ID)) if action is OrderAction.REPLACE else None
    # ``outside_rth`` and ``is_position_close`` are not consumed: the first only
    # permits a DAY order to trade outside regular hours, which no session word
    # says, and the second has no neutral field at all.
    consumed = {
        "contract_id_ex",
        "side",
        "size",
        "cash_size",
        "limit_price",
        "stop_price",
    }
    if order_type is not None:
        consumed.add("order_type")
    if tif is not None:
        consumed.add("tif")
    if target is not None:
        consumed.add(ORDER_ID)
    return BrokerOrder(
        vendor=VENDOR,
        account_ref="",
        mode=OrderMode.LIVE,
        target_ref=target,
        # Stocks and single-leg options go out under the same ``contract_id_ex``,
        # and the call says which in no field, so the class stays unclaimed.
        asset_class="other",
        instrument=OpaqueRef(raw_code=contract) if contract else None,
        side=_SIDES.get(_upper(args.get("side"))),
        qty=as_decimal(args.get("size")),
        # In the contract's trading currency, which neither the call nor the
        # answer names, so the amount carries none.
        notional=Money(amount=cash) if cash is not None else None,
        order_type=order_type,
        limit_price=as_decimal(args.get("limit_price")),
        stop_price=as_decimal(args.get("stop_price")),
        time_in_force=tif,
        extras={
            **({"contract_id_ex": contract} if contract else {}),
            **({ORDER_ID: target} if target else {}),
            **extras(args, consumed),
        },
        raw=dict(args),
    )


def _live_cancel(args: Mapping[str, Any]) -> BrokerOrder:
    target = as_text(args.get(ORDER_ID))
    return BrokerOrder(
        vendor=VENDOR,
        account_ref="",
        mode=OrderMode.LIVE,
        target_ref=target,
        extras={
            **({ORDER_ID: target} if target else {}),
            **extras(args, {ORDER_ID}),
        },
        raw=dict(args),
    )


def _live_answer(args: Mapping[str, Any]) -> BrokerOrder | None:
    """An answer to a broker question, which names the question and no order.

    The question is what it acts on, and IBKR keeps its id unique across every
    order of the user's, so that id is the target. With none there is nothing
    to attach an attempt to.

    Whether ``value`` confirms or declines is not read: it is quoted verbatim
    from the question's own answers, and no description says which of them is
    which. The result says what the answer did, and that is what is recorded.
    """
    question = as_text(args.get("question_id"))
    if question is None:
        return None
    return BrokerOrder(
        vendor=VENDOR,
        account_ref="",
        mode=OrderMode.LIVE,
        target_ref=question,
        extras={
            **target_ids(args, _ANSWER_FIELDS),
            **extras(args, set(_ANSWER_FIELDS)),
        },
        raw=dict(args),
    )


def _live_target(request: BrokerOrder | None, action: OrderAction | None) -> str | None:
    """The order a modify or a cancel named, which its answer need not repeat.

    Never an answer's: its target is a question, and quoting that as an order id
    would point the status read at an order nobody placed.
    """
    if request is None or action not in (OrderAction.REPLACE, OrderAction.CANCEL):
        return None
    return as_text(request.extras.get(ORDER_ID)) or as_text(request.target_ref)


def _live_route(
    request: BrokerOrder | None, envelope: Mapping[str, Any]
) -> dict[str, str]:
    """The contract, and the ``correlation_id`` an answer to this result echoes.

    The correlation is kept on the row because it is the one token that ties
    an answer to the request it answers: the next call quotes it, and it is how
    a person reading the ledger tells which submit a question belonged to.
    """
    contract = (
        as_text(request.extras.get("contract_id_ex")) if request else None
    ) or as_text(envelope.get("contract_id_ex"))
    correlation = as_text(envelope.get("correlation_id")) or (
        as_text(request.extras.get("correlation_id")) if request else None
    )
    return {
        **({"contract_id_ex": contract} if contract else {}),
        **({"correlation_id": correlation} if correlation else {}),
    }


def _live_state(raw_status: str | None, filled: Any) -> AttemptStatus:
    """The attempt state an ``order_status`` names, ``unknown`` for any other word.

    A state that only says the order is still live names no fill, and the
    quantity beside it does: an order part filled and awaiting a change is
    partially filled, not merely working.
    """
    status = _LIVE_STATUSES.get(_upper(raw_status), AttemptStatus.UNKNOWN)
    quantity = as_decimal(filled)
    if status is AttemptStatus.WORKING and quantity is not None and quantity > 0:
        return AttemptStatus.PARTIALLY_FILLED
    return status


def _live_outcome(
    envelope: Mapping[str, Any], action: OrderAction | None, request: BrokerOrder | None
) -> OrderOutcome:
    raw_status = as_text(envelope.get("order_status"))
    order_id = as_text(envelope.get(ORDER_ID)) or _live_target(request, action)
    route = _live_route(request, envelope)
    pending = envelope.get("pending_question")
    if pending is not None:
        # Not applied: no order on the market for a submit, the old parameters
        # for a modify, a live order for a cancel. A question with no id is one
        # nothing can answer, and a shape no description spells.
        question = (
            as_text(pending.get("question_id")) if isinstance(pending, Mapping) else None
        )
        return OrderOutcome(
            status=AttemptStatus.PENDING_CONFIRM if question else AttemptStatus.UNKNOWN,
            raw_status=raw_status,
            vendor_order_id=order_id,
            route=route,
            confirm_token=question,
            raw=dict(envelope),
        )
    # Settled, so the state is IBKR's own word for it, a cancel's included:
    # the venue may have filled the order before the cancel landed. An answer
    # with no word is ``unknown``, and the order id it carries, or the one the
    # call named, is what the status read then asks after.
    return OrderOutcome(
        status=_live_state(raw_status, envelope.get("filled_quantity")),
        raw_status=raw_status,
        vendor_order_id=order_id,
        route=route,
        filled_qty=as_decimal(envelope.get("filled_quantity")),
        raw=dict(envelope),
    )


def _live_status(
    query: StatusQuery, envelope: Mapping[str, Any], text: str
) -> list[VendorOrder]:
    """The one order ``get_order_status`` read, by the id it was asked for.

    The published result names no ``order_id`` of its own, so the query's is
    the order's; one that does name a different order is not this one's state.
    It names no symbol either, so the listing adds no instrument.
    """
    order_id = as_text(query.args.get(ORDER_ID))
    echoed = as_text(envelope.get(ORDER_ID))
    if order_id is None or (echoed is not None and echoed != order_id):
        raise StatusReadError(f"status read for {order_id!r} answered for {echoed!r}")
    raw_status = as_text(envelope.get("order_status"))
    if raw_status is None:
        raise StatusReadError(text or "no order status")
    contract = as_text(envelope.get("contract_id_ex"))
    return [
        VendorOrder(
            vendor_order_id=order_id,
            outcome=OrderOutcome(
                status=_live_state(raw_status, envelope.get("filled_quantity")),
                raw_status=raw_status,
                vendor_order_id=order_id,
                route={"contract_id_ex": contract} if contract else {},
                filled_qty=as_decimal(envelope.get("filled_quantity")),
                raw=dict(envelope),
            ),
        )
    ]


class IbkrOrderAdapter(OrderAdapter):
    """The one place that knows a staged instruction is not yet an order, and
    that a live one is not applied while a question is pending."""

    vendor = VENDOR

    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        entry = order_tool(VENDOR, tool)
        if entry is None:
            return None
        data = dict(args or {})
        if entry.mode is OrderMode.STAGED:
            return _create(data) if entry.action is OrderAction.STAGE else _delete(data)
        if entry.action in (OrderAction.PLACE, OrderAction.REPLACE):
            return _live_order(data, entry.action)
        if entry.action is OrderAction.CANCEL:
            return _live_cancel(data)
        return _live_answer(data)

    def parse_result(
        self,
        tool: str,
        body: ResultBody,
        *,
        tool_status: str,
        request: BrokerOrder | None = None,
    ) -> OrderOutcome:
        """The shared reading, except that a live tool's error object is not a no.

        IBKR tells the caller of a live tool that errored or timed out not to
        call it again, because the broker may already have accepted the
        request. Read as a rejection, that would be a final answer telling a
        person the order never reached the market, so it is ``unknown``, with
        the vendor's words as its failure and the order the call named, if it
        named one, for the status read to ask after.
        """
        entry = order_tool(VENDOR, tool)
        if entry is not None and entry.mode is OrderMode.LIVE:
            envelope, _ = body_parts(body)
            failure = _vendor_failure(envelope)
            if failure is not None:
                return OrderOutcome(
                    status=AttemptStatus.UNKNOWN,
                    vendor_order_id=_live_target(request, entry.action),
                    route=_live_route(request, {}),
                    failure=failure,
                    raw=dict(envelope or {}),
                )
        return super().parse_result(
            tool, body, tool_status=tool_status, request=request
        )

    def vendor_outcome(
        self,
        tool: str,
        envelope: dict[str, Any] | None,
        text: str,
        request: BrokerOrder | None,
    ) -> OrderOutcome:
        # A tool error already reached ``vendor_error``, which is where this
        # vendor's own rejections arrive; one that comes back in a body instead
        # is read the same way here.
        failure = _vendor_failure(envelope)
        if failure is not None:
            return OrderOutcome(
                status=AttemptStatus.REJECTED_BY_VENDOR,
                failure=failure,
                raw=dict(envelope or {}),
            )
        if envelope is None:
            return OrderOutcome(status=AttemptStatus.UNKNOWN, raw_status=as_text(text))
        entry = order_tool(VENDOR, tool)
        if entry is not None and entry.mode is OrderMode.LIVE:
            return _live_outcome(envelope, entry.action, request)
        return _outcome(envelope, entry.action if entry else None, request)

    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        """IBKR answers a refused call here and nowhere else.

        A live tool's error never reaches this: :meth:`parse_result` reads it
        first, as the unsettled answer it may be.
        """
        return _vendor_failure(envelope)

    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        """The order by its id for a live attempt, the whole list for a staged one.

        A live attempt with no id has nothing to read: the order list IBKR
        publishes does not spell its rows' fields, so a call whose answer never
        came back cannot be recognized in it, and stays open for a person.
        """
        if order is not None and order.mode is OrderMode.LIVE:
            order_id = as_text(vendor_order_id)
            return StatusQuery(ORDER_STATUS, {ORDER_ID: order_id}) if order_id else None
        return StatusQuery(LIST_INSTRUCTIONS)

    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        envelope, text = body_parts(body)
        if envelope is None:
            raise StatusReadError(text or "no envelope")
        failure = _vendor_failure(envelope)
        if failure is not None:
            raise StatusReadError(failure.message or "vendor error")
        if query.tool == ORDER_STATUS:
            return _live_status(query, envelope, text)
        rows = envelope.get(INSTRUCTION_ROWS)
        if not isinstance(rows, list):
            # An answer with no list is not an account with no instructions, nor
            # is a listed entry with no id a missing instruction, and reading either
            # that way would report an open attempt as gone.
            raise StatusReadError(text or "no instruction list")
        return [_listed(row) for row in rows]

    def absent_status(self, action: str | None) -> AttemptStatus | None:
        """``handed_off`` for a staged instruction the list no longer carries.

        It leaves when a person sends it or deletes it in IBKR's own client, or
        when it expires seven days after it was made, and the order a send
        makes has a new id that names no instruction. Only a person there can
        trade it, so IBKR's record is the outcome and the ledger stops at the
        hand-off. A delete was never listed, so it has no absence to read.
        """
        return AttemptStatus.HANDED_OFF if action == OrderAction.STAGE else None

    def match_key(self, order: BrokerOrder | None) -> str | None:
        """A create's fingerprint, comparable with a listed instruction.

        A call whose answer never came back has no id, and the list is the only
        place to find it again. IBKR echoes every field the create sent, so the
        request identifies itself; a delete carries none of them and answers
        None, which is honest -- it already has the id it named.
        """
        if order is None:
            return None
        if order.mode is not OrderMode.STAGED:
            # Nothing lists a live order in a shape this file can read, so a
            # live key could only ever meet a staged row, and there it would
            # make an unanswered submit the rival of an instruction it is not.
            return None
        return _fingerprint(
            _contract_ref(order.extras) or _contract_ref(order.raw),
            order.side,
            order.qty,
            order.order_type,
            order.limit_price,
            order.time_in_force,
        )
