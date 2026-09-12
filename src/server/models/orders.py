"""One ledger row as every surface reads it: the orders API and the chat receipt.

The ledger stores the whole normalized order, including the vendor's untouched
``raw`` body and the ``extras`` no field could name. Neither leaves here, so a
reader cannot come to depend on a vendor's own payload shape through either
door.

There is one projection because there is one row. ``attempt_row_to_response``
is the only place a raw row is read, and the API answers with it while
``receipt_from_attempt`` re-wraps the same values in the envelope the chat card
routes on: two wire shapes, never two readings of the row.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from ptc_agent.agent.middleware.order_governance import RECEIPT_KEY
from src.server.services.brokerage_orders import Failure

# The summary's fields, in the order a reader meets them. Anything the ledger
# carries and this tuple does not name (``raw``, ``extras``, and the vendor,
# mode and account already answered by their own columns) stays behind.
_SUMMARY_FIELDS = (
    "asset_class",
    "target_ref",
    "instrument",
    "side",
    "qty",
    "notional",
    "currency",
    "order_type",
    "limit_price",
    "stop_price",
    "time_in_force",
    "session",
    "note",
)


class OrderFailure(BaseModel):
    kind: str
    code: str | None = None
    message: str | None = None


class OrderFees(BaseModel):
    """What the brokerage charged, as an amount and the currency it is in."""

    amount: str
    currency: str


class OrderSummary(BaseModel):
    """The normalized order, flattened. Decimals travel as strings, as stored."""

    asset_class: str | None = None
    # The vendor order this call acts on: a cancel, a replace, a confirm and an
    # unstage all name one, and it is what makes them readable as a pair with
    # the placement they came from.
    target_ref: str | None = None
    instrument: dict[str, Any] | None = None
    side: str | None = None
    qty: str | None = None
    notional: dict[str, Any] | None = None
    currency: str | None = None
    order_type: str | None = None
    limit_price: str | None = None
    stop_price: str | None = None
    time_in_force: str | None = None
    session: str | None = None
    note: str | None = None


class OrderAttempt(BaseModel):
    attempt_id: str
    thread_id: str | None = None
    workspace_id: str | None = None
    conversation_response_id: str | None = None
    vendor: str
    server: str | None = None
    tool: str | None = None
    action: str | None = None
    mode: str | None = None
    account_ref: str | None = None
    asset_class: str | None = None
    order: OrderSummary | None = None
    status: str
    approval_required: bool = False
    decided_at: datetime | None = None
    decision_message: str | None = None
    executed_at: datetime | None = None
    completed_at: datetime | None = None
    vendor_order_id: str | None = None
    route: dict[str, Any] | None = None
    action_url: str | None = None
    # What the order did, as text: a quantity and an average price are exact
    # decimals at the vendor and a JSON number would round them on the way out.
    filled_qty: str | None = None
    avg_fill_price: str | None = None
    fees: OrderFees | None = None
    failure: OrderFailure | None = None
    parent_attempt_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class OrderAttemptList(BaseModel):
    items: list[OrderAttempt]
    next_cursor: str | None = None


def _summary(order: Any) -> OrderSummary | None:
    if not isinstance(order, dict):
        return None
    return OrderSummary(**{k: order.get(k) for k in _SUMMARY_FIELDS})


def _fees(fees: Any) -> OrderFees | None:
    if not isinstance(fees, dict) or fees.get("amount") is None:
        return None
    return OrderFees(
        amount=str(fees["amount"]), currency=str(fees.get("currency") or "")
    )


def _failure(failure: Any) -> OrderFailure | None:
    """The stored failure through the domain's own reader, never a second one."""
    if not isinstance(failure, dict):
        return None
    return OrderFailure(**Failure.from_json(failure).to_json())


def attempt_row_to_response(row: dict[str, Any]) -> OrderAttempt:
    """One ``order_attempts`` row as the API answers it.

    ``asset_class`` is lifted out of the stored order because it is what the
    list filters on, and a caller should not have to reach into a nested object
    to read back the facet it just asked for.
    """
    order = row.get("order_json")
    summary = _summary(order)
    return OrderAttempt(
        attempt_id=str(row["attempt_id"]),
        thread_id=_opt_str(row.get("thread_id")),
        workspace_id=_opt_str(row.get("workspace_id")),
        conversation_response_id=_opt_str(row.get("conversation_response_id")),
        vendor=str(row.get("vendor") or ""),
        server=row.get("server"),
        tool=row.get("tool"),
        action=row.get("action"),
        mode=row.get("mode"),
        account_ref=row.get("account_ref"),
        asset_class=summary.asset_class if summary else None,
        order=summary,
        status=str(row.get("status") or ""),
        approval_required=bool(row.get("approval_required")),
        decided_at=row.get("decided_at"),
        decision_message=row.get("decision_message"),
        executed_at=row.get("executed_at"),
        completed_at=row.get("completed_at"),
        vendor_order_id=row.get("vendor_order_id"),
        route=row.get("route") if isinstance(row.get("route"), dict) else None,
        action_url=_opt_str(row.get("action_url")),
        filled_qty=_opt_str(row.get("filled_qty")),
        avg_fill_price=_opt_str(row.get("avg_fill_price")),
        fees=_fees(row.get("fees")),
        failure=_failure(row.get("failure")),
        parent_attempt_id=_opt_str(row.get("parent_attempt_id")),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


def order_summary(attempt: OrderAttempt) -> dict[str, Any]:
    """The flat map the approval card and the receipt both draw the order from.

    Flat because the card reads one map: what the call is, then what it is
    about. An order field the ledger has no value for is left out rather than
    sent as a null, so the card can ask whether a key is there.
    """
    summary: dict[str, Any] = {
        "vendor": attempt.vendor,
        "tool": attempt.tool,
        "action": attempt.action,
        "mode": attempt.mode,
        "account_ref": attempt.account_ref,
    }
    order = attempt.order.model_dump(exclude_none=True) if attempt.order else {}
    for key, value in order.items():
        if key not in summary:
            summary[key] = value
    return summary


def receipt_from_attempt(
    attempt: OrderAttempt, *, raw_status: str | None = None
) -> dict[str, Any]:
    """The artifact fragment the chat card is drawn from.

    Everything comes off the row, because completion persists the fill, the
    fees and the vendor's page: the one thing no column holds is ``raw_status``,
    the vendor's own word for the state, which a receipt replayed long after
    the turn therefore does without. ``type`` rides alongside because the
    client routes an artifact to its card by that field.
    """
    return {
        "type": RECEIPT_KEY,
        RECEIPT_KEY: {
            "attempt_id": attempt.attempt_id,
            "vendor": attempt.vendor,
            "tool": attempt.tool,
            "action": attempt.action,
            "mode": attempt.mode,
            "account_ref": attempt.account_ref,
            "order": order_summary(attempt),
            "outcome": {
                "status": attempt.status,
                "raw_status": raw_status,
                "vendor_order_id": attempt.vendor_order_id,
                "route": dict(attempt.route or {}),
                "filled_qty": attempt.filled_qty,
                "avg_fill_price": attempt.avg_fill_price,
                "fees": attempt.fees.model_dump() if attempt.fees else None,
                "action_url": attempt.action_url,
                "failure": attempt.failure.model_dump() if attempt.failure else None,
                # The reason a person gave when they rejected: the receipt is
                # the only place the chat draws it from once the card settled.
                "decision_message": attempt.decision_message,
                "executed_at": _iso(attempt.executed_at),
                "completed_at": _iso(attempt.completed_at),
            },
        },
    }


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()
