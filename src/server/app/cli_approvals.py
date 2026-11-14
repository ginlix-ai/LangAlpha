"""What a computer's ``la`` waits on the user to approve, and the answer.

A person's surface: only a signed-in session (or the server's own call)
lists or answers these, never a token, since an agent approving its own
requests would defeat them. The request waiting on an answer is held by
``dependencies.cli_approval``; answering here releases it.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, StrictBool

from src.server.dependencies.principal import CurrentPrincipal, Principal
from src.server.services import cli_approvals

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/cli/approvals", tags=["CLI approvals"])


class ApprovalDecision(BaseModel):
    approve: StrictBool


def _person(principal: Principal) -> str:
    if not principal.unlimited:
        raise HTTPException(
            status_code=403,
            detail={
                "type": "forbidden_for_token",
                "message": "Only you, signed in to the app, answer what a computer asks",
            },
        )
    return principal.user_id


@router.get("")
async def list_approvals(principal: CurrentPrincipal) -> dict[str, Any]:
    """The requests waiting on the user's answer, oldest first; none, and
    logged, while Redis cannot be read."""
    user_id = _person(principal)
    try:
        waiting = await cli_approvals.pending(user_id)
    except cli_approvals.Unavailable:
        logger.warning("cli approvals not listed (user=%s): Redis unavailable", user_id)
        waiting = []
    return {"approvals": [approval.to_wire() for approval in waiting]}


@router.post("/{approval_id}/decision")
async def decide_approval(
    approval_id: UUID, body: ApprovalDecision, principal: CurrentPrincipal
) -> dict[str, Any]:
    user_id = _person(principal)
    try:
        return await cli_approvals.decide(user_id, str(approval_id), approve=body.approve)
    except cli_approvals.Unknown:
        raise HTTPException(status_code=404, detail="Approval not found") from None
    except cli_approvals.AlreadyDecided:
        raise HTTPException(
            status_code=409,
            detail={
                "type": "approval_decided",
                "message": "This request was already answered or has expired",
            },
        ) from None
    except cli_approvals.Unavailable:
        raise HTTPException(
            status_code=503,
            detail={
                "type": "approval_unavailable",
                "message": "Can't record your answer right now. Try again shortly.",
            },
        ) from None
