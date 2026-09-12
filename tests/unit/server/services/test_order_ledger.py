"""The receipt the chat draws an order from, built off a fake ``order_attempts``.

The interesting part is not the SQL, which is guarded elsewhere, but what
crosses onto the artifact: the row's own verdict, the vendor's answer through
its adapter, and none of the raw bodies either of them carried. Every end state
the design names gets one, including the two that never reach a vendor.

Account ids are invented; a fixture never carries a real one.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.order_governance import RECEIPT_KEY
from src.server.models.orders import attempt_row_to_response, receipt_from_attempt
from src.server.services.brokerage_orders import OrderOutcome
from src.server.services.brokerage_orders.models import TERMINAL_STATUSES
from src.server.services.egress import order_ledger as module
from src.server.services.egress.order_ledger import OrderAttemptLedger

TOOL = "sim_trade_input_order"
ARGS = {
    "acc_id": "1234567",
    "market": 100,
    "symbol": "AAPL",
    "order_side": 1,
    "order_type": 1,
    "qty": "1",
    "price": "50",
    "order_trade_time_type": 1,
}

PLACED = '{"ret_code":0,"ret_msg":"success","data":{"order_id":"900101"}}'
REJECTED = (
    '{"ret_code":-5,"ret_msg":"backend business error",'
    '"error":{"code":"backend_biz_error","message":"backend business error"}}'
)

CANCEL_TOOL = "sim_trade_cancel_order"
CANCEL_ARGS = {"acc_id": "1234567", "market": 100, "order_id": "900101"}
CANCELLED = '{"ret_code":0,"ret_msg":"success","data":{"order_id":"900101"}}'

# One IBKR create, whose whole answer is an id and the page a human releases
# the instruction on.
STAGE_TOOL = "create_order_instruction"
STAGE_ARGS = {
    "side": "BUY",
    "quantity": 1,
    "order_type": "LIMIT",
    "limit_price": 1,
    "time_in_force": "OPG",
    "contract_id_ex": "7712345",
}
STAGED = '{"id":"4417","url":"https://www.example.com/sso/resolver#/orders"}'

PARENT_ATTEMPT = "22222222-2222-2222-2222-222222222222"


def _row(
    status: str = "submitting",
    *,
    vendor: str = "moomoo",
    tool: str = TOOL,
    args: dict[str, Any] | None = None,
    **over: Any,
) -> dict[str, Any]:
    """One attempt as ``_COLUMNS`` reads it back, for a moomoo paper place."""
    from src.server.services.brokerage_orders import adapter_for

    order = adapter_for(vendor).parse_request(tool, args or ARGS)
    row: dict[str, Any] = {
        "attempt_id": "11111111-1111-1111-1111-111111111111",
        "tool_call_id": "call-1",
        "vendor": vendor,
        "server": vendor,
        "tool": tool,
        "action": "place",
        "mode": "paper",
        "account_ref": "1234567",
        "args": dict(args or ARGS),
        "args_sha256": "abc",
        "order_json": order.to_json(),
        "approval_required": True,
        "status": status,
        "vendor_order_id": None,
        "route": None,
        "action_url": None,
        "filled_qty": None,
        "avg_fill_price": None,
        "fees": None,
        "failure": None,
        "decision_message": None,
        "executed_at": datetime(2026, 9, 9, 12, 0, tzinfo=UTC),
        "completed_at": None,
    }
    row.update(over)
    return row


def _fake_db(monkeypatch, row: dict[str, Any]) -> dict[str, Any]:
    """``get_attempt`` and ``complete_attempt`` against one row in memory.

    ``complete_attempt`` writes what the real statement writes, so the receipt
    is built from the settled row rather than from the outcome it was handed.
    """

    async def get_attempt(attempt_id: str) -> dict[str, Any] | None:
        return dict(row) if attempt_id == row["attempt_id"] else None

    async def complete_attempt(
        attempt_id: str, outcome: OrderOutcome, result_sha256: str | None = None
    ) -> dict[str, Any] | None:
        if row["status"] != "submitting":
            return None
        row["status"] = str(outcome.status)
        row["vendor_order_id"] = outcome.vendor_order_id or row["vendor_order_id"]
        row["route"] = dict(outcome.route) or row["route"]
        row["action_url"] = outcome.action_url or row["action_url"]
        # COALESCE, as the column does it: a vendor silent about the fill
        # leaves whatever a status read already put on the row.
        for key, value in (
            ("filled_qty", outcome.filled_qty),
            ("avg_fill_price", outcome.avg_fill_price),
            ("fees", outcome.fees.to_json() if outcome.fees else None),
        ):
            if value is not None:
                row[key] = value
        row["failure"] = outcome.failure.to_json() if outcome.failure else None
        row["result_sha256"] = result_sha256
        if outcome.status in TERMINAL_STATUSES:
            row["completed_at"] = datetime(2026, 9, 9, 12, 0, 1, tzinfo=UTC)
        return dict(row)

    monkeypatch.setattr(module, "get_attempt", get_attempt)
    monkeypatch.setattr(module, "complete_attempt", complete_attempt)
    return row


def _replayed(row: dict[str, Any]) -> dict:
    """The receipt as history rebuilds it: the row alone, no vendor answer."""
    return receipt_from_attempt(attempt_row_to_response(row))[RECEIPT_KEY]


async def _complete(monkeypatch, row: dict[str, Any], result: ToolMessage) -> dict:
    _fake_db(monkeypatch, row)
    ledger = OrderAttemptLedger(user_id="u1", thread_id="t1")
    fragment = await ledger.complete(row["attempt_id"], result)
    assert fragment is not None
    # The client routes an artifact to its card by ``type``, like every other.
    assert fragment["type"] == RECEIPT_KEY
    return fragment[RECEIPT_KEY]


@pytest.mark.asyncio
async def test_a_placed_order_receipts_as_submitted(monkeypatch):
    receipt = await _complete(
        monkeypatch,
        _row(),
        ToolMessage(content=PLACED, tool_call_id="call-1"),
    )

    assert receipt["attempt_id"] == "11111111-1111-1111-1111-111111111111"
    assert receipt["vendor"] == "moomoo"
    assert receipt["tool"] == TOOL
    assert receipt["action"] == "place"
    assert receipt["mode"] == "paper"
    assert receipt["account_ref"] == "1234567"
    assert receipt["outcome"]["status"] == "submitted"
    assert receipt["outcome"]["vendor_order_id"] == "900101"
    assert receipt["outcome"]["failure"] is None
    assert receipt["outcome"]["executed_at"] == "2026-09-09T12:00:00+00:00"
    # Still working at the vendor, so the attempt has no completion time.
    assert receipt["outcome"]["completed_at"] is None


@pytest.mark.asyncio
async def test_the_receipt_carries_the_interrupt_summary_and_no_raw_bodies(monkeypatch):
    receipt = await _complete(
        monkeypatch,
        _row(),
        ToolMessage(content=PLACED, tool_call_id="call-1"),
    )

    order = receipt["order"]
    # The same map the approval card was drawn from, so both read one shape.
    assert order["action"] == "place"
    assert order["mode"] == "paper"
    assert order["vendor"] == "moomoo"
    assert order["account_ref"] == "1234567"
    assert order["side"] == "buy"
    assert order["qty"] == "1"
    assert order["order_type"] == "limit"
    assert order["limit_price"] == "50"
    assert order["instrument"] == {"kind": "equity", "symbol": "AAPL", "venue": "US"}
    assert "raw" not in order and "extras" not in order
    assert "raw" not in receipt["outcome"]


@pytest.mark.asyncio
async def test_a_vendor_rejection_receipts_with_the_vendors_own_code(monkeypatch):
    receipt = await _complete(
        monkeypatch,
        _row(),
        ToolMessage(content=REJECTED, tool_call_id="call-1"),
    )

    assert receipt["outcome"]["status"] == "rejected_by_vendor"
    assert receipt["outcome"]["failure"] == {
        "kind": "vendor",
        "code": "-5",
        "message": "backend business error",
    }
    assert receipt["outcome"]["vendor_order_id"] is None
    assert receipt["outcome"]["completed_at"] == "2026-09-09T12:00:01+00:00"


@pytest.mark.asyncio
async def test_a_transport_error_receipts_as_failed(monkeypatch):
    receipt = await _complete(
        monkeypatch,
        _row(),
        ToolMessage(
            content="Error: relay timeout", tool_call_id="call-1", status="error"
        ),
    )

    assert receipt["outcome"]["status"] == "failed"
    assert receipt["outcome"]["failure"]["kind"] == "transport"
    assert "relay timeout" in receipt["outcome"]["failure"]["message"]


@pytest.mark.asyncio
async def test_a_transport_error_after_dispatch_receipts_as_unknown(monkeypatch):
    """The relay let the frame out, so the vendor may hold the order."""
    receipt = await _complete(
        monkeypatch,
        _row(dispatched_at=datetime(2026, 9, 9, 12, 0, 0, tzinfo=UTC)),
        ToolMessage(
            content="Error: relay timeout", tool_call_id="call-1", status="error"
        ),
    )

    assert receipt["outcome"]["status"] == "unknown"
    assert receipt["outcome"]["failure"]["kind"] == "transport"
    assert receipt["outcome"]["completed_at"] is None


@pytest.mark.asyncio
async def test_a_rejected_decision_receipts_without_a_vendor(monkeypatch):
    receipt = _replayed(
        _row(
            status="rejected_by_user",
            executed_at=None,
            decision_message="too risky",
        )
    )

    assert receipt["outcome"]["status"] == "rejected_by_user"
    assert receipt["outcome"]["vendor_order_id"] is None
    assert receipt["outcome"]["executed_at"] is None
    assert receipt["outcome"]["decision_message"] == "too risky"
    # The order itself still draws, which is the point: the card names what
    # was not sent.
    assert receipt["order"]["side"] == "buy"


@pytest.mark.asyncio
async def test_a_replayed_receipt_draws_the_fill_from_the_row(monkeypatch):
    """The reason the fill has columns at all.

    A receipt rebuilt from history has no vendor answer to read -- the one that
    filled this order arrived in a reconciliation pass hours after the turn --
    so the row is the only place the numbers can come from.
    """
    outcome = _replayed(
        _row(
            status="filled",
            vendor_order_id="900101",
            filled_qty=Decimal("10"),
            avg_fill_price=Decimal("318.62"),
            fees={"amount": "0.35", "currency": "USD"},
            completed_at=datetime(2026, 9, 9, 12, 5, tzinfo=UTC),
        )
    )["outcome"]

    assert outcome["status"] == "filled"
    # Text, not a float: an exact decimal is what the vendor sent.
    assert outcome["filled_qty"] == "10"
    assert outcome["avg_fill_price"] == "318.62"
    assert outcome["fees"] == {"amount": "0.35", "currency": "USD"}


@pytest.mark.asyncio
async def test_a_refusal_receipts_with_the_policy_failure(monkeypatch):
    receipt = _replayed(
        _row(
            status="refused",
            failure={"kind": "policy", "message": "not approved, so it was not sent."},
        )
    )

    assert receipt["outcome"]["status"] == "refused"
    assert receipt["outcome"]["failure"]["kind"] == "policy"


@pytest.mark.asyncio
async def test_an_attempt_that_is_not_there_settles_nothing(monkeypatch):
    _fake_db(monkeypatch, _row())
    ledger = OrderAttemptLedger(user_id="u1", thread_id="t1")

    settled = await ledger.complete(
        "00000000-0000-0000-0000-000000000000",
        ToolMessage(content=PLACED, tool_call_id="call-1"),
    )

    assert settled is None


@pytest.mark.asyncio
async def test_a_cancels_receipt_names_the_order_it_acted_on(monkeypatch):
    """``target_ref`` is what a cancel, a replace or a confirm is about.

    The card and the receipt read one flat map, so the id the call named has to
    be in it rather than in the arguments underneath.
    """
    receipt = await _complete(
        monkeypatch,
        _row(tool=CANCEL_TOOL, args=CANCEL_ARGS, action="cancel"),
        ToolMessage(content=CANCELLED, tool_call_id="call-1"),
    )

    assert receipt["order"]["target_ref"] == "900101"
    assert receipt["outcome"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_a_staged_instruction_receipts_with_the_page_that_releases_it(
    monkeypatch,
):
    receipt = await _complete(
        monkeypatch,
        _row(vendor="ibkr", tool=STAGE_TOOL, args=STAGE_ARGS, action="stage",
             mode="staged", account_ref=""),
        ToolMessage(content=STAGED, tool_call_id="call-1"),
    )

    assert receipt["outcome"]["action_url"] == (
        "https://www.example.com/sso/resolver#/orders"
    )
    assert receipt["order"]["time_in_force"] == "at_the_open"


@pytest.mark.asyncio
async def test_the_page_that_releases_an_instruction_outlives_the_answer(
    monkeypatch,
):
    """The link is stored, so replaying the receipt still offers the page.

    An instruction waits at the vendor until someone opens it, which can be
    long after the turn that staged it and its answer are gone.
    """
    row = _row(vendor="ibkr", tool=STAGE_TOOL, args=STAGE_ARGS, action="stage",
               mode="staged", account_ref="")
    await _complete(
        monkeypatch, row, ToolMessage(content=STAGED, tool_call_id="call-1")
    )

    replayed = _replayed(row)

    assert replayed["outcome"]["action_url"] == (
        "https://www.example.com/sso/resolver#/orders"
    )


@pytest.mark.asyncio
async def test_a_receipt_built_from_the_row_alone_claims_no_link(monkeypatch):
    """A rejection never reached a vendor, so there is no page to open."""
    receipt = _replayed(_row(status="rejected_by_user", executed_at=None))

    assert receipt["outcome"]["action_url"] is None


@pytest.mark.asyncio
async def test_an_amend_is_written_against_the_attempt_that_placed_the_order(
    monkeypatch,
):
    """The chain is looked up by the id the call names, never inferred."""
    written: dict[str, Any] = {}

    async def latest_attempt_for_vendor_order(
        user_id: str, vendor: str, vendor_order_id: str, *, account_ref: str | None
    ) -> str | None:
        # Scoped to the account the call names: an order id is unique only there.
        assert (user_id, vendor, account_ref) == ("u1", "moomoo", "1234567")
        return PARENT_ATTEMPT if vendor_order_id == "900101" else None

    async def insert_attempt(**kwargs: Any) -> dict[str, Any]:
        written.update(kwargs)
        return {
            "attempt_id": "33333333-3333-3333-3333-333333333333",
            "approval_required": kwargs["approval_required"],
            "vendor": kwargs["vendor"],
            "tool": kwargs["tool"],
            "action": kwargs["action"],
            "mode": kwargs["mode"],
            "account_ref": kwargs["account_ref"],
            "order_json": kwargs["order"],
        }

    monkeypatch.setattr(
        module, "latest_attempt_for_vendor_order", latest_attempt_for_vendor_order
    )
    monkeypatch.setattr(module, "insert_attempt", insert_attempt)
    ledger = OrderAttemptLedger(user_id="u1", thread_id="t1")
    stamp = {
        "server": "moomoo",
        "tool": CANCEL_TOOL,
        "vendor": "moomoo",
        "approval": True,
        "order": {"action": "cancel", "mode": "paper"},
    }

    proposed = await ledger.propose(
        "msg-2", "call-2", f"mcp__moomoo__{CANCEL_TOOL}", stamp, CANCEL_ARGS
    )

    assert written["parent_attempt_id"] == PARENT_ATTEMPT
    assert proposed.summary["target_ref"] == "900101"


@pytest.mark.asyncio
async def test_a_placement_names_no_order_and_so_has_no_parent(monkeypatch):
    written: dict[str, Any] = {}

    async def unreachable(*_args: Any, **_kwargs: Any) -> str | None:
        raise AssertionError("a placement has no order to look up")

    async def insert_attempt(**kwargs: Any) -> dict[str, Any]:
        written.update(kwargs)
        return {"attempt_id": "44444444-4444-4444-4444-444444444444"}

    monkeypatch.setattr(module, "latest_attempt_for_vendor_order", unreachable)
    monkeypatch.setattr(module, "insert_attempt", insert_attempt)
    ledger = OrderAttemptLedger(user_id="u1", thread_id="t1")
    stamp = {
        "server": "moomoo",
        "tool": TOOL,
        "vendor": "moomoo",
        "approval": True,
        "order": {"action": "place", "mode": "paper"},
    }

    await ledger.propose("msg-3", "call-3", f"mcp__moomoo__{TOOL}", stamp, ARGS)

    assert written["parent_attempt_id"] is None
    # A call is its message and its id together, never the provider's id alone.
    assert (written["message_id"], written["tool_call_id"]) == ("msg-3", "call-3")
