"""Approval as authorization for one execution.

The four properties the middleware exists for, against a fake ledger: an
approved order runs exactly once, a rejected one never reaches the handler, an
attempt nobody answered is refused rather than allowed, and a second execution
of the same attempt is refused by the ledger rather than by anything the graph
remembers. A failed call still settles its attempt.
"""

from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import StructuredTool

from ptc_agent.agent.middleware import order_governance
from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import (
    INTERRUPT_KIND,
    RECEIPT_KEY,
    ExecutionGrant,
    OrderGovernanceMiddleware,
    Proposed,
    Refusal,
    execution_token,
)

TOOL_NAME = "mcp__moomoo__sim_trade_input_order"
ARGS = {"code": "US.AAPL", "qty": 1, "price": 50.0, "trd_side": "BUY"}

STAMP = {
    "server": "moomoo",
    "vendor": "moomoo",
    "tool": "sim_trade_input_order",
    "approval": True,
    "order": {"action": "place", "mode": "paper"},
}


def _tool(name: str = TOOL_NAME, stamp: dict | None = None) -> StructuredTool:
    tool = StructuredTool.from_function(func=lambda: "ok", name=name, description="t")
    tool.metadata = {METADATA_KEY: dict(stamp if stamp is not None else STAMP)}
    return tool


class _Request:
    def __init__(self, name: str = TOOL_NAME, call_id: str = "call-1") -> None:
        self.tool_call = {"id": call_id, "name": name, "args": dict(ARGS)}


def _state(
    call_id: str = "call-1", name: str = TOOL_NAME, message_id: str | None = "msg-1"
) -> dict[str, Any]:
    ai = AIMessage(
        content="",
        id=message_id,
        tool_calls=[
            {"id": call_id, "name": name, "args": dict(ARGS), "type": "tool_call"}
        ],
    )
    return {"messages": [ai]}


class _FakeLedger:
    """A ledger with the one property the real one buys: guarded transitions."""

    def __init__(self, *, approval_required: bool = True) -> None:
        self.approval_required = approval_required
        self.attempts: dict[tuple[str, str], str] = {}
        self.status: dict[str, str] = {}
        self.decided: list[tuple[str, str, str | None]] = []
        self.refused: list[tuple[str, str]] = []
        self.consumed: list[str] = []
        self.completed: list[tuple[str, ToolMessage]] = []
        self.proposed_args: list[dict] = []
        self._n = 0

    async def propose(self, message_id, tool_call_id, tool_name, stamp, args) -> Proposed:
        self.proposed_args.append(dict(args))
        key = (message_id, tool_call_id)
        attempt_id = self.attempts.get(key)
        if attempt_id is None:
            self._n += 1
            attempt_id = f"att-{self._n}"
            self.attempts[key] = attempt_id
            self.status[attempt_id] = (
                "proposed" if self.approval_required else "approved"
            )
        return Proposed(
            attempt_id=attempt_id,
            # Off the row, as the real ledger reads it: an attempt an earlier
            # pass already decided is no longer proposed and is not re-asked.
            awaiting_decision=self.status[attempt_id] == "proposed",
            summary={"vendor": "moomoo", "action": "place", "mode": "paper"},
        )

    async def decide(self, attempt_id, decision, message) -> dict[str, Any] | None:
        self.decided.append((attempt_id, decision, message))
        self.status[attempt_id] = (
            "approved" if decision == "approve" else "rejected_by_user"
        )
        return self._receipt(attempt_id)

    async def consume(self, attempt_id):
        self.consumed.append(attempt_id)
        if self.status.get(attempt_id) != "approved":
            return Refusal(
                "this order is not approved for execution "
                f"(it is {self.status.get(attempt_id)})",
                self._receipt(attempt_id),
            )
        self.status[attempt_id] = "submitting"
        return ExecutionGrant(token=f"{attempt_id}.9999.sig")

    async def complete(self, attempt_id, result) -> dict[str, Any]:
        self.completed.append((attempt_id, result))
        self.status[attempt_id] = (
            "failed" if result.status == "error" else "submitted"
        )
        return self._receipt(attempt_id)

    async def refuse(self, attempt_id, reason) -> dict[str, Any] | None:
        self.refused.append((attempt_id, reason))
        self.status[attempt_id] = "refused"
        return self._receipt(attempt_id)

    def _receipt(self, attempt_id) -> dict[str, Any] | None:
        if attempt_id not in self.status:
            return None
        return {
            "type": RECEIPT_KEY,
            RECEIPT_KEY: {
                "attempt_id": attempt_id,
                "outcome": {"status": self.status[attempt_id]},
            },
        }


class _Handler:
    """Records every call, and what the transport would have seen on it."""

    def __init__(self, result: ToolMessage | None = None, raises: Exception | None = None):
        self.calls: list[str] = []
        self.tokens: list[str | None] = []
        self._result = result
        self._raises = raises

    async def __call__(self, request):
        self.calls.append(request.tool_call["id"])
        self.tokens.append(execution_token.get())
        if self._raises is not None:
            raise self._raises
        return self._result or ToolMessage(
            content='{"order_id": "OID-1"}', tool_call_id=request.tool_call["id"]
        )


def _answer(monkeypatch, payload: dict[str, Any]) -> list[dict]:
    """Replace the graph interrupt with a scripted answer; keep the payload."""
    seen: list[dict] = []

    def _interrupt(value):
        seen.append(value)
        return payload

    monkeypatch.setattr(order_governance, "interrupt", _interrupt)
    return seen


@pytest.mark.asyncio
async def test_approved_order_runs_exactly_once(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    raised = _answer(
        monkeypatch, {"order_decisions": {"att-1": {"type": "approve"}}}
    )

    state = _state()
    out = await mw.aafter_model(state, None)

    assert raised and raised[0]["kind"] == INTERRUPT_KIND
    request = raised[0]["action_requests"][0]
    assert request["tool_call_id"] == "call-1"
    assert request["attempt_id"] == "att-1"
    assert request["order"]["action"] == "place"
    assert ledger.decided == [("att-1", "approve", None)]
    # The call survives the rewrite: an approved order is left for the tool node.
    assert [c["id"] for c in out["messages"][0].tool_calls] == ["call-1"]

    handler = _Handler()
    result = await mw.awrap_tool_call(_Request(), handler)

    assert handler.calls == ["call-1"]
    assert handler.tokens == ["att-1.9999.sig"]
    assert execution_token.get() is None
    assert result.artifact[RECEIPT_KEY]["outcome"]["status"] == "submitted"
    assert result.artifact["direct_mcp"]["tool"] == "sim_trade_input_order"


@pytest.mark.asyncio
async def test_rejected_order_never_reaches_the_handler(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    _answer(
        monkeypatch,
        {"order_decisions": {"att-1": {"type": "reject", "message": "too risky"}}},
    )

    out = await mw.aafter_model(_state(), None)

    assert ledger.decided == [("att-1", "reject", "too risky")]
    assert [c["id"] for c in out["messages"][0].tool_calls] == ["call-1"]
    blocked = out["messages"][1]
    assert blocked.status == "error"
    assert blocked.tool_call_id == "call-1"
    assert "rejected this order" in blocked.content
    assert "too risky" in blocked.content
    assert blocked.artifact["direct_mcp"]["tool"] == "sim_trade_input_order"
    # The rejection is an end state of the order, so it draws the same card.
    assert blocked.artifact[RECEIPT_KEY]["outcome"]["status"] == "rejected_by_user"

    # And the tool node, were it reached anyway, still refuses on the row.
    handler = _Handler()
    result = await mw.awrap_tool_call(_Request(), handler)
    assert handler.calls == []
    assert result.status == "error"


@pytest.mark.asyncio
async def test_a_missing_decision_is_a_refusal(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    _answer(monkeypatch, {"order_decisions": {"att-other": {"type": "approve"}}})

    out = await mw.aafter_model(_state(), None)

    assert ledger.decided == []
    assert [a for a, _ in ledger.refused] == ["att-1"]
    assert [c["id"] for c in out["messages"][0].tool_calls] == ["call-1"]
    blocked = out["messages"][1]
    assert "was not approved" in blocked.content
    assert blocked.artifact[RECEIPT_KEY]["outcome"]["status"] == "refused"


@pytest.mark.asyncio
async def test_an_empty_answer_is_a_refusal(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    _answer(monkeypatch, {})

    out = await mw.aafter_model(_state(), None)

    assert [a for a, _ in ledger.refused] == ["att-1"]
    assert [c["id"] for c in out["messages"][0].tool_calls] == ["call-1"]


@pytest.mark.asyncio
async def test_second_execution_is_refused_by_the_ledger(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    _answer(monkeypatch, {"order_decisions": {"att-1": {"type": "approve"}}})
    await mw.aafter_model(_state(), None)

    handler = _Handler()
    await mw.awrap_tool_call(_Request(), handler)
    retry = await mw.awrap_tool_call(_Request(), handler)

    assert handler.calls == ["call-1"]
    assert ledger.consumed == ["att-1", "att-1"]
    assert retry.status == "error"
    assert "not approved for execution" in retry.content
    assert retry.artifact["direct_mcp"]["tool"] == "sim_trade_input_order"
    assert retry.artifact[RECEIPT_KEY]["attempt_id"] == "att-1"


@pytest.mark.asyncio
async def test_an_order_with_no_approval_setting_is_never_put_to_the_user(monkeypatch):
    ledger = _FakeLedger(approval_required=False)
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    raised = _answer(monkeypatch, {})

    assert await mw.aafter_model(_state(), None) is None
    assert raised == []

    handler = _Handler()
    result = await mw.awrap_tool_call(_Request(), handler)
    assert handler.calls == ["call-1"]
    assert result.artifact[RECEIPT_KEY]["outcome"]["status"] == "submitted"


@pytest.mark.asyncio
async def test_a_failing_call_still_settles_its_attempt(monkeypatch):
    ledger = _FakeLedger(approval_required=False)
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    await mw.aafter_model(_state(), None)

    handler = _Handler(raises=RuntimeError("relay timeout"))
    result = await mw.awrap_tool_call(_Request(), handler)

    assert [a for a, _ in ledger.completed] == ["att-1"]
    assert result.status == "error"
    assert result.artifact[RECEIPT_KEY]["outcome"]["status"] == "failed"
    assert execution_token.get() is None


@pytest.mark.asyncio
async def test_an_error_result_still_settles_its_attempt(monkeypatch):
    ledger = _FakeLedger(approval_required=False)
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    await mw.aafter_model(_state(), None)

    refusal = ToolMessage(
        content="Refused: rate limited", tool_call_id="call-1", status="error"
    )
    handler = _Handler(result=refusal)
    result = await mw.awrap_tool_call(_Request(), handler)

    assert [a for a, _ in ledger.completed] == ["att-1"]
    assert result.artifact[RECEIPT_KEY]["outcome"]["status"] == "failed"


@pytest.mark.asyncio
async def test_a_call_with_no_attempt_on_this_turn_is_refused():
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])

    handler = _Handler()
    result = await mw.awrap_tool_call(_Request(), handler)

    assert handler.calls == []
    assert ledger.consumed == []
    assert "no approved execution" in result.content


@pytest.mark.asyncio
async def test_a_tool_the_binder_did_not_stamp_as_an_order_passes_through():
    ledger = _FakeLedger()
    plain = _tool("mcp__moomoo__get_positions", stamp={"server": "moomoo", "tool": "x"})
    mw = OrderGovernanceMiddleware(ledger, [plain])

    assert await mw.aafter_model(_state(name="mcp__moomoo__get_positions"), None) is None
    handler = _Handler()
    out = await mw.awrap_tool_call(_Request(name="mcp__moomoo__get_positions"), handler)

    assert handler.calls == ["call-1"]
    assert out.artifact is None
    assert ledger.attempts == {}


def test_the_sync_path_refuses_every_order():
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    state = _state()

    out = mw.after_model(state, None)

    assert [c["id"] for c in out["messages"][0].tool_calls] == ["call-1"]
    assert "runs only on the async path" in out["messages"][1].content
    assert ledger.attempts == {}


@pytest.mark.asyncio
async def test_a_call_id_repeated_in_a_new_message_is_a_new_order(monkeypatch):
    """Some providers number call ids by position, so a new thread repeats one."""
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    raised = _answer(monkeypatch, {})

    await mw.aafter_model(_state(message_id="msg-1"), None)
    await mw.aafter_model(_state(message_id="msg-2"), None)

    assert [r["action_requests"][0]["attempt_id"] for r in raised] == ["att-1", "att-2"]


@pytest.mark.asyncio
async def test_a_stringified_argument_is_hashed_in_the_form_the_relay_will_see(
    monkeypatch,
):
    """The approval binds the frame, so both sides have to hash one form.

    ``ToolArgumentParsingMiddleware`` decodes a JSON-looking string argument in
    place before the tool runs, and the relay hashes what it produced. Hashing
    the model's raw form at propose time instead left the gate comparing two
    different digests: the attempt was already consumed, so the approved order
    came back refused and unretryable. Providers that number call ids by
    position also stringify structured arguments, and Robinhood's
    ``place_option_order`` requires an array.
    """
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    _answer(monkeypatch, {})
    state = _state()
    state["messages"][0].tool_calls[0]["args"] = {
        "legs": '[{"side": "buy", "ratio": 1}]',
        "quantity": 1,
    }

    await mw.aafter_model(state, None)

    # Decoded before the ledger saw it, so the stored hash is over the object.
    assert ledger.proposed_args[0]["legs"] == [{"side": "buy", "ratio": 1}]
    # And written back onto the call itself, so the approval card, the row and
    # the frame the relay hashes all read the same value.
    assert state["messages"][0].tool_calls[0]["args"]["legs"] == [
        {"side": "buy", "ratio": 1}
    ]
    # Untouched otherwise: this canonicalizes serialization, it does not coerce.
    assert ledger.proposed_args[0]["quantity"] == 1


@pytest.mark.asyncio
async def test_an_order_in_a_message_with_no_id_is_refused(monkeypatch):
    ledger = _FakeLedger()
    mw = OrderGovernanceMiddleware(ledger, [_tool()])
    raised = _answer(monkeypatch, {})

    out = await mw.aafter_model(_state(message_id=None), None)

    assert raised == []
    assert ledger.attempts == {}
    assert "no id" in out["messages"][1].content
