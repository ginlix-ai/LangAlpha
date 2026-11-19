"""An organization's activity log, for another service to pull.

GET /api/v1/activity answers only a service token sent with no user, since
one page holds every member's events. It reads what was recorded whatever the
``teams`` flag says now: the rows are the organization's record, and turning
the flag off stops new ones without hiding the old.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from src.server.database.activity_events import PAGE_SIZE, list_for_org
from src.server.models.activity import ActivityPage, event_row_to_response
from src.server.utils.api import ServiceCaller, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/activity", tags=["Activity"])


@router.get("", dependencies=[ServiceCaller])
@handle_api_exceptions("list activity", logger)
async def list_activity(
    org_id: str = Query(..., min_length=1),
    cursor: str | None = None,
    limit: int = PAGE_SIZE,
) -> ActivityPage:
    """One page of the organization's events, oldest first, from ``cursor``.

    An event appears here only once it is ``SETTLE_S`` (a minute) old, so a
    write still committing never lands behind a cursor already handed out.
    """
    try:
        rows, next_cursor, has_more = await list_for_org(
            org_id, cursor=cursor or None, limit=limit
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return ActivityPage(
        items=[event_row_to_response(row) for row in rows],
        next_cursor=next_cursor,
        has_more=has_more,
    )
