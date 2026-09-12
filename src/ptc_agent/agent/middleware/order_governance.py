"""Approval as authorization for one execution, not for a graph transition.

An interrupt that only lets the graph proceed cannot say afterwards which call
it answered or stop that call from running twice: the answer is positional, it
is rebuilt from a mutable config on every resume, and absence of an answer
reads as permission. This middleware inverts all three. Every order call gets a
durable attempt before the user is asked, the verdict is written against that
attempt's id, an attempt nobody answered is refused, and execution takes a
single grant the attempt can only hand out once.

The ledger is a port. This module ships in the agent library and knows no
vendor, no database and no HTTP, exactly the split ``DirectMcpPolicyMiddleware``
and ``CreditGateMiddleware`` keep; the server hands it an object that knows all
three.

Placed after the consent gate and before the plan-mode interrupt: a call
consent no longer covers is refused before anyone is asked to approve it, and
plan approval keeps the stock human-in-the-loop path untouched.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import BaseTool
from langgraph.errors import GraphBubbleUp
from langgraph.types import Command, interrupt

from ptc_agent.agent.middleware.direct_mcp import (
    _stamped,
    direct_tool_meta,
    refused_message,
)
from ptc_agent.agent.middleware.tool.argument_parsing import parse_tool_args
from ptc_agent.agent.middleware.tool.error_handling import format_tool_error

# What the interrupt payload calls itself, and the artifact key a receipt
# rides under. Both are read by the client.
INTERRUPT_KIND = "order_approval"
RECEIPT_KEY = "order_receipt"

# The grant for the call currently in flight, read by the transport that dials
# the relay. Set around one ``handler`` await and reset after it, so it is
# scoped to the call rather than to the turn: the MCP client carries the
# sender's context into the task that posts, which is what lets a per-call
# value reach a session opened long before it.
execution_token: ContextVar[str | None] = ContextVar(
    "order_execution_token", default=None
)


@dataclass(frozen=True)
class Proposed:
    """The attempt written for one order call, before anyone is asked."""

    attempt_id: str
    #: Whether this call still has to be put to the user, read off the row the
    #: write returned: the attempt needs a verdict and has not been given one.
    #: The node that proposes re-runs on every resume, and this is what stops a
    #: stale payload re-deciding an attempt an earlier pass already answered.
    awaiting_decision: bool
    #: What to show the user about the order itself, vendor-neutral. Rendered
    #: by the client; never parsed here.
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionGrant:
    """The single execution an approved attempt hands out."""

    token: str


@dataclass(frozen=True)
class Refusal:
    """Why this call may not run, in words the model and the user both read."""

    reason: str
    #: The attempt as the ledger found it, so the card that asked settles on
    #: the same refusal the model was told. Opaque here; the client reads it.
    receipt: dict[str, Any] | None = None


class OrderLedger(Protocol):
    """The durable side of an order: where attempts live and verdicts land."""

    async def propose(
        self,
        message_id: str,
        tool_call_id: str,
        tool_name: str,
        stamp: Mapping[str, Any],
        args: Mapping[str, Any],
    ) -> Proposed:
        """Record the attempt for this call, or return the one already there.

        A call is the message that made it and its id together: a provider may
        repeat a call id in a later message.
        """
        ...

    async def decide(
        self, attempt_id: str, decision: str, message: str | None
    ) -> dict[str, Any] | None:
        """Write the user's verdict, and answer with the receipt it produced.

        Guarded: a decided attempt does not move, and then there is no receipt
        because this pass wrote nothing.
        """
        ...

    async def consume(self, attempt_id: str) -> ExecutionGrant | Refusal:
        """Take the one execution, or say what state was found instead."""
        ...

    async def complete(self, attempt_id: str, result: ToolMessage) -> dict[str, Any] | None:
        """Settle the attempt with the vendor's answer; returns the receipt."""
        ...

    async def refuse(self, attempt_id: str, reason: str) -> dict[str, Any] | None:
        """End an attempt that will never reach a vendor, and receipt it.

        A refusal ends an order as much as a fill does, and the message
        carrying it is prose, so the receipt is what settles the card.
        """
        ...


def _order_of(stamp: Mapping[str, Any] | None) -> dict[str, Any] | None:
    order = (stamp or {}).get("order")
    return order if isinstance(order, dict) else None


def _describe(tool_call: Mapping[str, Any], stamp: Mapping[str, Any]) -> str:
    """The fallback prompt: the vendor's own tool name and the arguments."""
    return json.dumps(
        {
            "vendor": stamp.get("vendor"),
            "tool": stamp.get("tool"),
            "arguments": tool_call.get("args") or {},
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    )


def _with_receipt(
    result: ToolMessage | Command, receipt: Mapping[str, Any] | None
) -> ToolMessage | Command:
    """Carry the attempt's receipt out beside the vendor stamp.

    Opaque here on purpose: the middleware ships in the agent library and the
    fragment is the ledger's own shape, read only by the client.
    """
    if not receipt or not isinstance(result, ToolMessage):
        return result
    artifact = result.artifact if isinstance(result.artifact, dict) else {}
    result.artifact = {**artifact, **receipt}
    return result


def _order_phrase(stamp: Mapping[str, Any]) -> str:
    order = _order_of(stamp) or {}
    mode = order.get("mode") or ""
    action = order.get("action") or "order"
    vendor = stamp.get("vendor") or "the brokerage"
    return f"{mode} {action} at {vendor}".strip()


class OrderGovernanceMiddleware(AgentMiddleware):
    """Propose, put to the user, and consume exactly once.

    Main-agent only. A tool the binder did not stamp with an ``order`` entry is
    not an order and is left alone.
    """

    def __init__(self, ledger: OrderLedger, tools: Sequence[BaseTool]) -> None:
        self._ledger = ledger
        self._stamps: dict[str, dict[str, Any]] = {}
        for tool in tools:
            stamp = direct_tool_meta(tool)
            if stamp and _order_of(stamp):
                self._stamps[tool.name] = stamp
        # Per-turn execution context, rebuilt by the node that proposes: which
        # attempt each call in flight belongs to. Never truth about whether a
        # call may run -- that answer is the guarded consume, on the row. A
        # miss here refuses the call rather than running it unattributed.
        self._attempts: dict[str, str] = {}

    # -- proposal and verdict ------------------------------------------------

    def _order_calls(self, state: Any) -> tuple[AIMessage | None, list[dict[str, Any]]]:
        messages = (state or {}).get("messages") or []
        last_ai = next(
            (m for m in reversed(messages) if isinstance(m, AIMessage)), None
        )
        if last_ai is None or not last_ai.tool_calls:
            return None, []
        calls = [
            call for call in last_ai.tool_calls if call.get("name") in self._stamps
        ]
        return last_ai, calls

    def _blocked(
        self,
        tool_call: Mapping[str, Any],
        stamp: Mapping[str, Any],
        reason: str,
        receipt: Mapping[str, Any] | None = None,
    ) -> ToolMessage:
        return refused_message(tool_call, reason, stamp, receipt=receipt)

    def _refuse_all(
        self, last_ai: AIMessage, calls: list[dict[str, Any]], why: str
    ) -> dict[str, Any]:
        """Answer every order call in the message with one refusal."""
        messages = []
        for call in calls:
            stamp = self._stamps[call["name"]]
            messages.append(
                self._blocked(
                    call, stamp, f"Refused: an order ({_order_phrase(stamp)}) {why}"
                )
            )
        return {"messages": [last_ai, *messages]}

    def after_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        """Fail closed: the ledger is async, so a sync run may place nothing.

        The same bargain the consent gate makes on its sync path. No attempt is
        written, because on this path no order exists to record.
        """
        last_ai, calls = self._order_calls(state)
        if last_ai is None or not calls:
            return None
        return self._refuse_all(last_ai, calls, "runs only on the async path")

    async def aafter_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        last_ai, calls = self._order_calls(state)
        if last_ai is None or not calls:
            return None
        if not last_ai.id:
            # The message id is half of an attempt's identity: without it the
            # resume could not find the attempt this pass wrote, so none is.
            return self._refuse_all(
                last_ai, calls, "arrived in a message with no id, so it was not sent"
            )

        proposals: dict[str, Proposed] = {}
        for call in calls:
            stamp = self._stamps[call["name"]]
            # Canonicalized here, before anything reads the args, because
            # ``ToolArgumentParsingMiddleware`` rewrites them in place later and
            # the relay hashes what it produced. Hashing the model's raw form
            # instead would refuse the approved call at the gate for a provider
            # that stringifies a structured argument, having already spent the
            # attempt. The same parse also settles what the approval card shows
            # and what the row stores, so all four agree on one form. Idempotent,
            # so the middleware running it again downstream changes nothing.
            call["args"] = parse_tool_args(call.get("args") or {}, call["name"])
            proposed = await self._ledger.propose(
                last_ai.id, call["id"], call["name"], stamp, call["args"]
            )
            proposals[call["id"]] = proposed
            self._attempts[call["id"]] = proposed.attempt_id

        awaiting = [call for call in calls if proposals[call["id"]].awaiting_decision]
        if not awaiting:
            return None

        answer = interrupt(
            {
                "kind": INTERRUPT_KIND,
                "action_requests": [
                    {
                        "name": call["name"],
                        "args": call.get("args") or {},
                        "description": _describe(call, self._stamps[call["name"]]),
                        "tool_call_id": call["id"],
                        "attempt_id": proposals[call["id"]].attempt_id,
                        "order": proposals[call["id"]].summary,
                    }
                    for call in awaiting
                ],
                # The shape the stock interrupt emits, so every projection that
                # already understands one keeps working.
                "review_configs": [
                    {
                        "action_name": call["name"],
                        "allowed_decisions": ["approve", "reject"],
                    }
                    for call in awaiting
                ],
            }
        )
        decisions = answer.get("order_decisions") if isinstance(answer, Mapping) else None
        if not isinstance(decisions, Mapping):
            decisions = {}

        messages: list[ToolMessage] = []
        for call in awaiting:
            stamp = self._stamps[call["name"]]
            attempt_id = proposals[call["id"]].attempt_id
            verdict = decisions.get(attempt_id)
            kind = verdict.get("type") if isinstance(verdict, Mapping) else None
            message = verdict.get("message") if isinstance(verdict, Mapping) else None
            if kind == "approve":
                await self._ledger.decide(attempt_id, "approve", message)
                continue
            if kind == "reject":
                said = f" They said: {message}" if message else ""
                reason = (
                    f"The user rejected this order ({_order_phrase(stamp)}); "
                    f"it was not sent.{said}"
                )
                receipt = await self._ledger.decide(attempt_id, "reject", message)
            else:
                # Absence is refusal. An answer that names no verdict for this
                # attempt is not permission to place its order.
                reason = (
                    f"Refused: this order ({_order_phrase(stamp)}) was not "
                    "approved, so it was not sent."
                )
                receipt = await self._ledger.refuse(attempt_id, reason)
            messages.append(self._blocked(call, stamp, reason, receipt))

        # A refused call keeps its declaration, the way the stock approval
        # middleware keeps one: the tool node skips a call that already has an
        # answer, and a tool result no assistant message declares is rejected
        # by every provider on the next request.
        return {"messages": [last_ai, *messages]}

    # -- execution -----------------------------------------------------------

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        stamp = self._stamps.get(request.tool_call.get("name", ""))
        if stamp is None:
            return handler(request)
        return self._blocked(
            request.tool_call,
            stamp,
            f"Refused: an order ({_order_phrase(stamp)}) runs only on the async path",
        )

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        stamp = self._stamps.get(request.tool_call.get("name", ""))
        if stamp is None:
            return await handler(request)
        tool_call_id = request.tool_call["id"]
        attempt_id = self._attempts.get(tool_call_id)
        if attempt_id is None:
            return self._blocked(
                request.tool_call,
                stamp,
                f"Refused: this order ({_order_phrase(stamp)}) has no approved "
                "execution on this turn.",
            )

        grant = await self._ledger.consume(attempt_id)
        if isinstance(grant, Refusal):
            return self._blocked(
                request.tool_call, stamp, f"Refused: {grant.reason}", grant.receipt
            )

        token = execution_token.set(grant.token)
        try:
            result = await handler(request)
        except GraphBubbleUp:
            raise
        except Exception as e:
            # The attempt is already consumed, so the failure has to be settled
            # here or the row sits in ``submitting`` forever. Converted in the
            # shape the error-handling middleware would have used.
            result = ToolMessage(
                content=format_tool_error(e, request.tool_call.get("name")),
                tool_call_id=tool_call_id,
                name=request.tool_call.get("name"),
                status="error",
            )
        finally:
            execution_token.reset(token)

        if not isinstance(result, ToolMessage):
            return result
        receipt = await self._ledger.complete(attempt_id, result)
        return _with_receipt(_stamped(result, dict(stamp)), receipt)
