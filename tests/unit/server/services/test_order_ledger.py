"""The receipt the chat draws an order from, built off a fake ``order_attempts``.

The interesting part is not the SQL, which is guarded elsewhere, but what
crosses onto the artifact: the row's own verdict, the vendor's answer through
its adapter, and none of the raw bodies either of them carried. Every end state
the design names gets one, including the two that never reach a vendor.

Account ids are invented; a fixture never carries a real one.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.order_governance import RECEIPT_KEY
from src.server.database.order_attempts import CONFIRM_TOKEN_KEY
from src.server.models.orders import attempt_row_to_response, receipt_from_attempt
from src.server.services.brokerage_orders import BrokerOrder, OrderOutcome, adapter_for
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


# -- a confirm, linked to the call that raised it ----------------------------


class _ChainDb:
    """``order_attempts`` in memory, with the statements a confirm chain rests on.

    Each one mirrors its SQL: the creating-action filter on the id lookup, the
    token kept in ``route`` and resolved to the chain's root, the guard on the
    completing write, and the hand-up that writes only an empty id. The SQL
    text itself is pinned in ``test_order_confirm_chain.py``.
    """

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def install(self, monkeypatch) -> None:
        for name in (
            "insert_attempt",
            "get_attempt",
            "complete_attempt",
            "latest_attempt_for_vendor_order",
            "latest_attempt_for_confirm_token",
        ):
            monkeypatch.setattr(module, name, getattr(self, name))

    async def insert_attempt(self, **kwargs: Any) -> dict[str, Any]:
        attempt_id = f"00000000-0000-4000-8000-{len(self.rows) + 1:012d}"
        row = {
            **_row(vendor=kwargs["vendor"], tool=kwargs["tool"], args=kwargs["args"]),
            "attempt_id": attempt_id,
            "user_id": kwargs["user_id"],
            "tool_call_id": kwargs["tool_call_id"],
            "action": kwargs["action"],
            "mode": kwargs["mode"],
            "account_ref": kwargs["account_ref"],
            "order_json": kwargs["order"],
            "approval_required": kwargs["approval_required"],
            "status": str(kwargs["status"]),
            "parent_attempt_id": kwargs["parent_attempt_id"],
            "created_at": len(self.rows),
        }
        self.rows[attempt_id] = row
        return dict(row)

    async def get_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        row = self.rows.get(attempt_id)
        return dict(row) if row else None

    async def complete_attempt(
        self, attempt_id: str, outcome: OrderOutcome, result_sha256: str | None = None
    ) -> dict[str, Any] | None:
        row = self.rows[attempt_id]
        if row["status"] != "submitting":
            return None
        route = dict(outcome.route)
        if outcome.confirm_token:
            route[CONFIRM_TOKEN_KEY] = outcome.confirm_token
        row["status"] = str(outcome.status)
        row["vendor_order_id"] = outcome.vendor_order_id or row["vendor_order_id"]
        row["route"] = route or row["route"]
        row["failure"] = outcome.failure.to_json() if outcome.failure else None
        root = self.rows.get(row["parent_attempt_id"] or "")
        if (
            row["action"] == "confirm"
            and root is not None
            and root["user_id"] == row["user_id"]
            and root["vendor_order_id"] is None
            and (row["vendor_order_id"] or "").strip()
        ):
            root["vendor_order_id"] = row["vendor_order_id"]
        return dict(row)

    def _newest(self, user_id, vendor, account_ref, keep) -> dict[str, Any] | None:
        found = [
            r
            for r in self.rows.values()
            if (r["user_id"], r["vendor"], r["account_ref"]) == (user_id, vendor, account_ref)
            and keep(r)
        ]
        return max(found, key=lambda r: r["created_at"]) if found else None

    async def latest_attempt_for_vendor_order(
        self, user_id, vendor, vendor_order_id, *, account_ref
    ) -> str | None:
        row = self._newest(
            user_id,
            vendor,
            account_ref,
            lambda r: r["vendor_order_id"] == vendor_order_id
            and r["action"] in ("place", "stage"),
        )
        return row["attempt_id"] if row else None

    async def latest_attempt_for_confirm_token(
        self, user_id, vendor, confirm_token, *, account_ref
    ) -> str | None:
        row = self._newest(
            user_id,
            vendor,
            account_ref,
            lambda r: (r["route"] or {}).get(CONFIRM_TOKEN_KEY) == confirm_token,
        )
        if row is None:
            return None
        if row["action"] == "confirm" and row["parent_attempt_id"]:
            return row["parent_attempt_id"]
        return row["attempt_id"]


async def _call(
    db: _ChainDb,
    ledger: OrderAttemptLedger,
    vendor: str,
    tool: str,
    action: str,
    args: dict[str, Any],
    answer: Any,
) -> dict[str, Any]:
    """One order call proposed, executed and answered, as its turn runs it."""
    stamp = {
        "server": vendor,
        "tool": tool,
        "vendor": vendor,
        "approval": False,
        "order": {"action": action, "mode": "live"},
    }
    proposed = await ledger.propose(
        f"msg-{len(db.rows)}", f"call-{len(db.rows)}", tool, stamp, args
    )
    # The consume, which only moves the row on.
    db.rows[proposed.attempt_id]["status"] = "submitting"
    await ledger.complete(
        proposed.attempt_id,
        ToolMessage(content=json.dumps(answer), tool_call_id="call"),
    )
    return db.rows[proposed.attempt_id]


IBKR_SUBMIT = {
    "contract_id_ex": "7712345",
    "side": "BUY",
    "order_type": "LIMIT",
    "tif": "DAY",
    "size": 1,
    "limit_price": 1,
}
IBKR_ORDER_ID = "5001001"


def _question(question_id: str) -> dict[str, Any]:
    return {
        "pending_question": {"question_id": question_id, "answers": ["Yes", "No"]},
        "correlation_id": "c-8001",
    }


def _answer(question_id: str) -> dict[str, Any]:
    return {"question_id": question_id, "value": "Yes", "correlation_id": "c-8001"}


IBKR_RELEASED = {
    "pending_question": None,
    "correlation_id": "c-8001",
    "order_id": IBKR_ORDER_ID,
    "order_status": "NEW",
}


@pytest.fixture
def chain(monkeypatch) -> tuple[_ChainDb, OrderAttemptLedger]:
    db = _ChainDb()
    db.install(monkeypatch)
    return db, OrderAttemptLedger(user_id="u1", thread_id="t1")


@pytest.mark.asyncio
async def test_an_answer_is_written_against_the_submit_that_raised_its_question(chain):
    """The answer names a question and no order, so the token is the only link."""
    db, ledger = chain
    submit = await _call(
        db, ledger, "ibkr", "submit_order", "place", IBKR_SUBMIT, _question("q-1")
    )
    assert submit["status"] == "pending_confirm"
    assert submit["route"][CONFIRM_TOKEN_KEY] == "q-1"

    answer = await _call(
        db, ledger, "ibkr", "answer_order_question", "confirm", _answer("q-1"),
        IBKR_RELEASED,
    )

    assert answer["parent_attempt_id"] == submit["attempt_id"]


@pytest.mark.asyncio
async def test_a_chain_of_questions_parents_to_the_call_that_started_it(chain):
    db, ledger = chain
    submit = await _call(
        db, ledger, "ibkr", "submit_order", "place", IBKR_SUBMIT, _question("q-1")
    )
    first = await _call(
        db, ledger, "ibkr", "answer_order_question", "confirm", _answer("q-1"),
        _question("q-2"),
    )
    second = await _call(
        db, ledger, "ibkr", "answer_order_question", "confirm", _answer("q-2"),
        IBKR_RELEASED,
    )

    # The second question was raised by the first answer, and still the chain
    # reads as the submit's history rather than the answer's.
    assert first["route"][CONFIRM_TOKEN_KEY] == "q-2"
    assert first["parent_attempt_id"] == submit["attempt_id"]
    assert second["parent_attempt_id"] == submit["attempt_id"]


@pytest.mark.asyncio
async def test_the_order_a_confirm_releases_lands_on_its_submit_for_the_read(chain):
    """Only the answer learns the id; the submit is what the sweep reads."""
    db, ledger = chain
    submit = await _call(
        db, ledger, "ibkr", "submit_order", "place", IBKR_SUBMIT, _question("q-1")
    )
    assert submit["vendor_order_id"] is None
    await _call(
        db, ledger, "ibkr", "answer_order_question", "confirm", _answer("q-1"),
        IBKR_RELEASED,
    )

    assert submit["vendor_order_id"] == IBKR_ORDER_ID
    # Its state is the read's to settle, not the hand-up's.
    assert submit["status"] == "pending_confirm"
    query = adapter_for("ibkr").status_query(
        BrokerOrder.from_json(submit["order_json"]),
        vendor_order_id=submit["vendor_order_id"],
    )
    assert query is not None
    assert (query.tool, query.args) == ("get_order_status", {"order_id": IBKR_ORDER_ID})


@pytest.mark.asyncio
async def test_an_id_the_submit_already_holds_is_never_replaced(chain):
    db, ledger = chain
    submit = await _call(
        db, ledger, "ibkr", "submit_order", "place", IBKR_SUBMIT,
        {**_question("q-1"), "order_id": "5001000"},
    )
    await _call(
        db, ledger, "ibkr", "answer_order_question", "confirm", _answer("q-1"),
        IBKR_RELEASED,
    )

    assert submit["vendor_order_id"] == "5001000"


@pytest.mark.asyncio
async def test_a_moomoo_confirm_naming_its_confirmation_links_to_the_placement(chain):
    """moomoo's ``confirm_id`` is not the order id, so the id lookup misses it."""
    db, ledger = chain
    place = await _call(
        db, ledger, "moomoo", "trading_order_place", "place",
        {"qty": "1", "code": "US.AAPL", "side": "BUY", "price": "100",
         "acc_id": "1234567", "order_type": "LIMIT", "time_in_force": "DAY"},
        {"s": "ok", "need_order_confirm": True, "confirm_id": "cfm-9",
         "d": {"order_id": "7654321"}},
    )
    confirm = await _call(
        db, ledger, "moomoo", "trading_order_confirm", "confirm",
        {"acc_id": "1234567", "confirm_id": "cfm-9"},
        {"s": "ok", "d": {"order_id": "7654321", "order_status": "SUBMITTED"}},
    )

    assert place["vendor_order_id"] == "7654321"
    assert confirm["parent_attempt_id"] == place["attempt_id"]


@pytest.mark.asyncio
async def test_only_a_confirm_is_linked_by_a_confirmation_token(chain):
    """A cancel names an order; one naming a question id names nothing we placed."""
    db, ledger = chain
    await _call(
        db, ledger, "ibkr", "submit_order", "place", IBKR_SUBMIT, _question("q-1")
    )
    cancel = await _call(
        db, ledger, "ibkr", "cancel_order", "cancel", {"order_id": "q-1"},
        {"order_status": "CANCELLED"},
    )

    assert cancel["parent_attempt_id"] is None
