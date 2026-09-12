"""The server's half of order governance: rows, adapters and the grant.

``OrderGovernanceMiddleware`` knows that an order has an attempt, a verdict and
one execution. This knows which brokerage it is, how that brokerage spells an
order, and where the row lives. One is built per turn beside the direct MCP
binding and carries nothing but the ids that scope its writes.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.order_governance import (
    ExecutionGrant,
    Proposed,
    Refusal,
)
from ptc_agent.agent.provenance.types import (
    fingerprint_result,
    hash_args,
    redact_args,
)
from src.config.env import EGRESS_RELAY_SECRET
from src.server.database.order_attempts import (
    complete_attempt,
    consume_attempt,
    decide_attempt,
    get_attempt,
    insert_attempt,
    latest_attempt_for_vendor_order,
    refuse_attempt,
)
from src.server.models.orders import (
    attempt_row_to_response,
    order_summary,
    receipt_from_attempt,
)
from src.server.services.brokerage_orders import (
    AttemptStatus,
    BrokerOrder,
    Failure,
    OrderOutcome,
    adapter_for,
    pre_vendor_outcome,
)
from src.server.services.egress.execution_token import (
    ExecutionTokenError,
    mint_execution_token,
)

logger = logging.getLogger(__name__)

__all__ = ["OrderAttemptLedger"]


def _sha256_of(args: Mapping[str, Any] | None) -> str | None:
    return (hash_args(dict(args or {})) or {}).get("sha256")


class OrderAttemptLedger:
    """One turn's writes into ``order_attempts``: the ``OrderLedger`` port.

    Holds ids only. Everything it decides it decides with a guarded statement,
    so two workers running the same turn's tail cannot both approve, both
    consume, or both complete an attempt.
    """

    def __init__(
        self,
        *,
        user_id: str,
        workspace_id: str | None = None,
        thread_id: str | None = None,
        run_id: str | None = None,
        turn_index: int | None = None,
    ) -> None:
        self._user_id = user_id
        self._workspace_id = workspace_id
        self._thread_id = thread_id
        self._run_id = run_id
        self._turn_index = turn_index

    # -- proposal ------------------------------------------------------------

    async def propose(
        self,
        message_id: str,
        tool_call_id: str,
        tool_name: str,
        stamp: Mapping[str, Any],
        args: Mapping[str, Any],
    ) -> Proposed:
        vendor = str(stamp.get("vendor") or "")
        tool = str(stamp.get("tool") or tool_name)
        order = stamp.get("order") if isinstance(stamp.get("order"), Mapping) else {}
        approval_required = bool(stamp.get("approval"))
        parsed = self._parse_request(vendor, tool, args)
        row = await insert_attempt(
            user_id=self._user_id,
            message_id=message_id,
            tool_call_id=tool_call_id,
            vendor=vendor,
            workspace_id=self._workspace_id,
            thread_id=self._thread_id,
            conversation_response_id=self._run_id,
            turn_index=self._turn_index,
            server=stamp.get("server"),
            tool=tool,
            action=order.get("action"),
            mode=order.get("mode"),
            account_ref=parsed.account_ref if parsed else None,
            args=redact_args(dict(args or {})),
            args_sha256=_sha256_of(args),
            order=parsed.to_json() if parsed else None,
            approval_required=approval_required,
            status=(
                AttemptStatus.PROPOSED if approval_required else AttemptStatus.APPROVED
            ),
            parent_attempt_id=await self._parent_of(vendor, parsed),
        )
        attempt = attempt_row_to_response(row)
        return Proposed(
            attempt_id=attempt.attempt_id,
            # Off the row the insert returned, never a second read: the
            # conflict arm answers with the attempt already there, so a resume
            # sees whatever verdict the first pass wrote.
            awaiting_decision=(
                attempt.approval_required
                and attempt.status == AttemptStatus.PROPOSED
            ),
            summary=order_summary(attempt),
        )

    async def _parent_of(self, vendor: str, parsed: BrokerOrder | None) -> str | None:
        """The attempt that placed the order this call acts on, if we placed it.

        A cancel, a replace or a confirm names a vendor order id; when that id
        belongs to one of this user's own attempts, the two rows are one
        order's history and the ledger says so. Never fatal: an amend nobody
        can link is still a valid attempt.
        """
        target = (parsed.target_ref or "").strip() if parsed else ""
        if not parsed or not target:
            return None
        try:
            return await latest_attempt_for_vendor_order(
                self._user_id, vendor, target, account_ref=parsed.account_ref
            )
        except Exception:
            logger.warning(
                "[ORDERS] %s: could not look up the attempt behind %s",
                vendor,
                target,
                exc_info=True,
            )
            return None

    def _parse_request(
        self, vendor: str, tool: str, args: Mapping[str, Any]
    ) -> BrokerOrder | None:
        """The normalized order, or None. Never fatal.

        A vendor we have no adapter for, or a shape one has not met, still
        produces an attempt: the row's job is to authorize an execution, and it
        can do that with the arguments alone.
        """
        adapter = adapter_for(vendor)
        if adapter is None:
            return None
        try:
            return adapter.parse_request(tool, dict(args or {}))
        except Exception:
            logger.warning(
                "[ORDERS] %s/%s: could not parse the order request", vendor, tool,
                exc_info=True,
            )
            return None

    async def decide(
        self, attempt_id: str, decision: str, message: str | None
    ) -> dict[str, Any] | None:
        row = await decide_attempt(attempt_id, decision, message)
        if row is None:
            # Guarded out: another pass, or another worker, already answered.
            logger.info(
                "[ORDERS] attempt %s was already decided; %r ignored",
                attempt_id,
                decision,
            )
            return None
        return receipt_from_attempt(attempt_row_to_response(row))

    async def refuse(self, attempt_id: str, reason: str) -> dict[str, Any] | None:
        row = await refuse_attempt(attempt_id, reason)
        if row is None:
            return None
        return receipt_from_attempt(attempt_row_to_response(row))

    # -- execution -----------------------------------------------------------

    async def consume(self, attempt_id: str) -> ExecutionGrant | Refusal:
        row = await consume_attempt(attempt_id)
        if row is None:
            found = await get_attempt(attempt_id)
            if found is None:
                return Refusal("this order has no attempt on record")
            return Refusal(
                "this order is not approved for execution "
                f"(it is {found.get('status')}); a retry needs a new order",
                receipt_from_attempt(attempt_row_to_response(found)),
            )
        try:
            token = mint_execution_token(
                EGRESS_RELAY_SECRET or "",
                attempt_id=str(row["attempt_id"]),
                tool_call_id=str(row["tool_call_id"]),
                tool=str(row.get("tool") or ""),
                args_sha256=row.get("args_sha256"),
            )
        except ExecutionTokenError as e:
            # The attempt is spent either way: leaving it in ``submitting``
            # would make a call that never left the host look in flight.
            settled = await complete_attempt(
                attempt_id,
                OrderOutcome(
                    status=AttemptStatus.FAILED,
                    failure=Failure(kind="transport", message=str(e)),
                ),
            )
            return Refusal(
                f"this order cannot be authorized for execution: {e}",
                None
                if settled is None
                else receipt_from_attempt(attempt_row_to_response(settled)),
            )
        return ExecutionGrant(token=token)

    async def complete(
        self, attempt_id: str, result: ToolMessage
    ) -> dict[str, Any] | None:
        try:
            row = await get_attempt(attempt_id)
            if row is None:
                return None
            outcome = self._outcome(row, result)
            if _lost_after_dispatch(row, outcome):
                outcome = outcome.model_copy(
                    update={"status": AttemptStatus.UNKNOWN}
                )
            sha256, _, _ = fingerprint_result(result.content)
            settled = await complete_attempt(attempt_id, outcome, sha256)
            if settled is None:
                return None
            # The settled row, not the answer: the completing write is what
            # merged this outcome with whatever a status read had already put
            # on the row, and only the vendor's own word for the state has no
            # column to come back from.
            return receipt_from_attempt(
                attempt_row_to_response(settled), raw_status=outcome.raw_status
            )
        except Exception:
            # A row left in ``submitting`` is the honest state for an answer we
            # could not record; reconciliation settles it from the vendor.
            logger.warning(
                "[ORDERS] attempt %s: could not record the outcome",
                attempt_id,
                exc_info=True,
            )
            return None

    @staticmethod
    def _outcome(row: Mapping[str, Any], result: ToolMessage) -> OrderOutcome:
        """The vendor's answer through its own adapter, or an honest unknown."""
        vendor = str(row.get("vendor") or "")
        tool = str(row.get("tool") or "")
        status = getattr(result, "status", "success") or "success"
        adapter = adapter_for(vendor)
        if adapter is None:
            settled = pre_vendor_outcome(result.content, tool_status=status)
            return settled or OrderOutcome(status=AttemptStatus.UNKNOWN)
        request = None
        order = row.get("order_json")
        if isinstance(order, Mapping):
            try:
                request = BrokerOrder.from_json(order)
            except Exception:
                request = None
        try:
            return adapter.parse_result(
                tool, result.content, tool_status=status, request=request
            )
        except Exception:
            logger.warning(
                "[ORDERS] %s/%s: could not parse the order result",
                vendor,
                tool,
                exc_info=True,
            )
            return OrderOutcome(status=AttemptStatus.UNKNOWN)


def _lost_after_dispatch(row: Mapping[str, Any], outcome: OrderOutcome) -> bool:
    """A transport failure on a frame the relay had already let out.

    ``failed`` is terminal and says the order never reached the vendor. A
    timeout or a dropped connection after the relay claimed the dispatch
    cannot say that: the vendor may hold the order. Such an answer is kept as
    the failure it was but filed under ``unknown``, which the reconciliation
    sweep resolves from the vendor's own list. A frame with no claim never
    left, so its failure stands.
    """
    failure = outcome.failure
    return (
        outcome.status is AttemptStatus.FAILED
        and failure is not None
        and failure.kind == "transport"
        and row.get("dispatched_at") is not None
    )
