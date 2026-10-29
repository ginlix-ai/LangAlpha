"""Orders API: the user's order attempts, across every thread and vendor.

Read-only on purpose. Nothing here decides, consumes or settles an attempt:
those transitions belong to the governance middleware and the relay, which
guard them against the row. This router only projects what the ledger already
holds, user-scoped, so an attempt id learned from somewhere else still answers
404 to anyone but its owner.

Endpoints (user-scoped):
- GET /api/v1/orders
- GET /api/v1/orders/{attempt_id}
"""

from __future__ import annotations

import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from src.server.database.order_attempts import PAGE_SIZE, get_attempt, list_attempts
from src.server.models.orders import (
    OrderAttempt,
    OrderAttemptList,
    attempt_row_to_response,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orders", tags=["Orders"])


@router.get("")
@handle_api_exceptions("list orders", logger)
async def list_orders(
    user_id: CurrentUserId,
    vendor: str | None = None,
    mode: str | None = None,
    status: Annotated[list[str] | None, Query()] = None,
    asset_class: str | None = None,
    cursor: str | None = None,
    limit: int = PAGE_SIZE,
) -> OrderAttemptList:
    """One page of the user's attempts, newest first.

    ``status`` repeats (``?status=filled&status=working``) because the surface
    filters by end state and several of them mean the same thing to a reader.
    """
    statuses = [s for s in (status or []) if s]
    # The page bound and the cursor are the ledger's: it clamps the size its
    # index was built for, and it refuses an unreadable token rather than
    # quietly restarting at page one, which would page a caller in a circle.
    try:
        rows, next_cursor = await list_attempts(
            user_id,
            vendor=vendor or None,
            mode=mode or None,
            status=statuses or None,
            asset_class=asset_class or None,
            cursor=cursor or None,
            limit=limit,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return OrderAttemptList(
        items=[attempt_row_to_response(row) for row in rows],
        next_cursor=next_cursor,
    )


@router.get("/{attempt_id}")
@handle_api_exceptions("get order", logger)
async def get_order(attempt_id: str, user_id: CurrentUserId) -> OrderAttempt:
    """One attempt. Another user's row is 404, not 403: the id is the secret."""
    try:
        UUID(attempt_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Order not found")
    row = await get_attempt(attempt_id)
    if not row or row.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Order not found")
    return attempt_row_to_response(row)
