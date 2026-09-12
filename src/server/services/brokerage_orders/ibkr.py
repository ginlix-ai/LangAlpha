"""IBKR's staged instructions, as the server answered them.

Every shape below was read off live calls to ``api.ibkr.com`` rather than
inferred: the create answers ``{"id": ..., "url": ...}`` and nothing else, the
delete answers ``{}``, the list answers ``{"instructions": [...]}``, and a
refused call arrives as an MCP tool error carrying ``{"code", "message",
"data"}``. Three consequences drive the whole file.

There is no account argument and no account in any answer, so ``account_ref``
is empty by construction rather than by a failed lookup. There is no status
word anywhere: IBKR states an instruction by listing it and by dropping it from
the list, which is why the only status this file reads is the one it decides.
And the vendor puts a rejection on the error channel, so a tool error here is
the vendor's word rather than the transport's, and is classified as one.

An instruction is written into the real account but is not an order until a
human releases it in IBKR's own client, so no status here maps past
``submitted``: there is nothing for this stack to watch working or fill, and a
released instruction turns into a live order this list no longer carries.
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


class IbkrOrderAdapter(OrderAdapter):
    """The one place that knows a staged instruction is not yet an order."""

    vendor = VENDOR

    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        entry = order_tool(VENDOR, tool)
        if entry is None:
            return None
        data = dict(args or {})
        return _create(data) if entry.action is OrderAction.STAGE else _delete(data)

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
        return _outcome(envelope, entry.action if entry else None, request)

    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        """IBKR answers a refused instruction here and nowhere else."""
        return _vendor_failure(envelope)

    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        return StatusQuery(LIST_INSTRUCTIONS)

    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        envelope, text = body_parts(body)
        if envelope is None:
            raise StatusReadError(text or "no envelope")
        failure = _vendor_failure(envelope)
        if failure is not None:
            raise StatusReadError(failure.message or "vendor error")
        rows = envelope.get(INSTRUCTION_ROWS)
        if not isinstance(rows, list):
            # An answer with no list is not an account with no instructions, nor
            # is a listed entry with no id a missing instruction, and reading either
            # that way would report an open attempt as gone.
            raise StatusReadError(text or "no instruction list")
        return [_listed(row) for row in rows]

    def match_key(self, order: BrokerOrder | None) -> str | None:
        """A create's fingerprint, comparable with a listed instruction.

        A call whose answer never came back has no id, and the list is the only
        place to find it again. IBKR echoes every field the create sent, so the
        request identifies itself; a delete carries none of them and answers
        None, which is honest -- it already has the id it named.
        """
        if order is None:
            return None
        return _fingerprint(
            _contract_ref(order.extras) or _contract_ref(order.raw),
            order.side,
            order.qty,
            order.order_type,
            order.limit_price,
            order.time_in_force,
        )
