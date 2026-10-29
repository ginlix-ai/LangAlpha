"""A thread's wakeups: list them, and cancel one, several or all.

Not gated on the feature: turning wakeups off must never leave an owner
unable to see or stop what was set before.
"""

from typing import List, Optional

from src.server.database.wakeups.cancel import CancelOutcome
from src.server.models.wakeup import (
    WakeupCancelRequest,
    WakeupCancelResponse,
    WakeupCancelResult,
    WakeupsResponse,
)
from src.server.services.wakeups import cancel as wakeups_cancel
from src.server.services.wakeups.listing import thread_wakeups, thread_wakeups_response
from src.server.utils import api as auth_api
from src.server.utils.api import CurrentUserId

from ._deps import logger, router


async def _cancel(thread_id: str, ids: Optional[List[str]]) -> WakeupCancelResponse:
    outcomes: List[CancelOutcome] = await wakeups_cancel.user_cancel(thread_id, ids)
    try:
        wakeups = await thread_wakeups(thread_id)
    except Exception:
        # The cancel has committed, so a 500 would have the user retry what
        # is done; without a listing the client reads it again.
        logger.warning(
            "[Wakeups] listing after cancel failed thread_id=%s", thread_id, exc_info=True
        )
        wakeups = None
    return WakeupCancelResponse(
        results=[
            WakeupCancelResult(wakeup_id=o.wakeup_id, outcome=o.outcome) for o in outcomes
        ],
        wakeups=wakeups,
    )


@router.get("/{thread_id}/wakeups", response_model=WakeupsResponse)
async def list_wakeups(thread_id: str, x_user_id: CurrentUserId):
    """Open wakeups, those that ended in the last day, and the thread's price
    targets from before wakeups."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    return await thread_wakeups_response(thread_id, x_user_id)


@router.delete("/{thread_id}/wakeups/{wakeup_id}", response_model=WakeupCancelResponse)
async def cancel_wakeup(thread_id: str, wakeup_id: str, x_user_id: CurrentUserId):
    await auth_api.require_thread_owner(thread_id, x_user_id)
    return await _cancel(thread_id, [wakeup_id])


@router.post("/{thread_id}/wakeups/cancel", response_model=WakeupCancelResponse)
async def cancel_wakeups(
    thread_id: str, request: WakeupCancelRequest, x_user_id: CurrentUserId
):
    await auth_api.require_thread_owner(thread_id, x_user_id)
    return await _cancel(thread_id, None if request.all else request.wakeup_ids)
