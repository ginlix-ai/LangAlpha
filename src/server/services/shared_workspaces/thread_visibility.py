"""Sharing a shared workspace's thread with the workspace, or making it
private again, by its starter.

A thread there starts as its starter's alone. Once shared, everyone in the
workspace reaches it; its starter may take it back only while no one else has
sent a turn in it, since their words would otherwise sit in a conversation
they can no longer open. The decision and the flip are one transaction
(``thread_reach.set_shared``); the organization's feed then tells the
workspace's members, so a colleague who had it open leaves it.
"""

from __future__ import annotations

from fastapi import HTTPException

from src.server.database.conversation import thread_reach
from src.server.services import activity
from src.server.services.activity import Action
from src.server.services.shared_workspaces.access import thread_access
from src.server.services.thread_lifecycle_feed import (
    EVENT_VERSION,
    VISIBILITY_EVENT,
    publish_user_event,
)

NOT_SHARED_WORKSPACE = "not_in_shared_workspace"
STARTER_ONLY = "thread_starter_only"
OTHERS_SENT = "others_sent"


def _refusal(status: int, type_: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"type": type_, "message": message})


async def set_shared(thread_id: str, caller: str, shared: bool) -> bool:
    """Whether making the thread private turned its link off. Raises the
    route's refusal: no thread the caller reaches (404, as for no thread), a
    thread outside a shared workspace (400), someone else's (403), or making
    private one someone else has written in (409)."""
    access = await thread_access(thread_id, caller)
    if not access.is_shared:
        raise _refusal(
            400, NOT_SHARED_WORKSPACE,
            "Only a conversation in a shared workspace can be shared with it.",
        )
    outcome, _, link_off = await thread_reach.set_shared(thread_id, caller, shared)
    if outcome == "missing":
        raise HTTPException(status_code=404, detail="Thread not found")
    if outcome == "not_starter":
        raise _refusal(
            403, STARTER_ONLY,
            "Only the person who started this conversation can change who sees it.",
        )
    if outcome == "others_sent":
        raise _refusal(
            409, OTHERS_SENT,
            "Someone else has written in this conversation, so it can't be made private.",
        )
    if outcome == "done":
        await publish_user_event(
            None,
            {
                "v": EVENT_VERSION,
                "type": VISIBILITY_EVENT,
                "thread_id": thread_id,
                "workspace_id": access.shared.workspace_id,
                "shared": shared,
            },
            shared_org=access.org_id,
        )
    if link_off is not None:
        # The record a link turned off from its own route writes.
        activity.record(
            caller, Action.THREAD_LINK_OFF, resource=("thread", thread_id),
            thread_id=thread_id, workspace_id=access.shared.workspace_id,
            detail={k: v for k, v in link_off.items() if isinstance(v, bool)},
        )
    return link_off is not None
