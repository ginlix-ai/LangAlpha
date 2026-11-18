"""The organization's activity log as another service pulls it."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ActivityEvent(BaseModel):
    event_id: str
    occurred_at: datetime
    org_id: str
    actor_id: str
    auth: str | None = None
    client_id: str | None = None
    action: str
    outcome: str
    resource_kind: str | None = None
    resource_id: str | None = None
    workspace_id: str | None = None
    thread_id: str | None = None
    detail: dict[str, Any] = {}
    trace_id: str | None = None


class ActivityPage(BaseModel):
    items: list[ActivityEvent]
    #: Where the next pull resumes: after the last event here, or the cursor
    #: given when there was none. A puller keeps it across pulls.
    next_cursor: str | None
    #: Whether more events follow now; when False, ask again later.
    has_more: bool


def event_row_to_response(row: dict[str, Any]) -> ActivityEvent:
    return ActivityEvent(
        **{
            **row,
            "event_id": str(row["event_id"]),
            "workspace_id": str(row["workspace_id"]) if row.get("workspace_id") else None,
            "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
            "detail": row.get("detail") or {},
        }
    )
