"""Who may take a shared workspace's work outside it (migration 144).

A restricted shared workspace's managers choose: only managers, any member,
or no one may turn a thread's or file's organization link on or off and
publish its work to the organization library. Under "no one" a link may still
be made, and opens only for the people who can open the workspace, judged
when the link is opened, so the choice holds for links made before it.

The choice applies only while the workspace is restricted: open to everyone,
its links already reach the whole organization, so the stored value waits
until it is restricted again. The organization's own rules come first at
every call site (``library_publish_refusal``, ``refuse_public_link_of``); this
can only narrow them. Every route that shares or publishes a shared
workspace's content asks here, never a copy of the rule.
"""

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException

from src.server.database import shared_workspaces as db
from src.server.database.shared_workspaces import SharedRow, ShareOut
from src.server.services.shared_workspaces.access import (
    Access,
    SharedAccess,
    acts_as_manager,
    caller_seat,
)

MANAGERS: ShareOut = "managers"
MEMBERS: ShareOut = "members"
NONE: ShareOut = "none"
VALUES: tuple[ShareOut, ...] = (MANAGERS, MEMBERS, NONE)

#: The ``type`` of a member's refusal where only a manager may share out.
MANAGERS_ONLY = "share_out_managers_only"
#: The ``type`` of a refusal to publish from a workspace that keeps its work in.
KEPT_IN = "share_out_none"


def in_force(shared: Optional[SharedRow]) -> Optional[ShareOut]:
    """The choice that applies now: None for a personal workspace and for
    one open to everyone in its organization."""
    if shared is None or shared.everyone:
        return None
    return shared.share_out


async def rule_for(shared: Optional[SharedAccess]) -> tuple[Optional[ShareOut], bool]:
    """The choice in force for the caller, and whether they act as a
    manager under it, read only where the choice turns on it."""
    rule = in_force(shared.row if shared is not None else None)
    return rule, rule == MANAGERS and await acts_as_manager(shared)


def may_share_out(rule: Optional[ShareOut], manager: bool) -> bool:
    """Whether the caller may take the workspace's work outside it: share it
    with the organization and publish it to the library."""
    return rule in (None, MEMBERS) or (rule == MANAGERS and manager)


def may_change_links(rule: Optional[ShareOut], manager: bool) -> bool:
    """Whether the caller may turn a thread's or file's link on or off, or
    change what it lets visitors do. Under "no one" anyone may: such a link
    opens only for the people in the workspace."""
    return rule != MANAGERS or manager


def _managers_only(message: str) -> HTTPException:
    return HTTPException(
        status_code=403, detail={"type": MANAGERS_ONLY, "message": message}
    )


async def refuse_link_change(access: Access) -> None:
    """403 to a member where only a manager may share the workspace's work
    with the organization."""
    if not may_change_links(*await rule_for(access.shared)):
        raise _managers_only(
            "Only a manager of this workspace can share its work outside it."
        )


async def refuse_publish(access: Access) -> None:
    """403 to a publish to the library from a workspace whose managers keep
    its work inside it, or by a member where only a manager may publish."""
    rule, manager = await rule_for(access.shared)
    if rule == NONE:
        raise HTTPException(
            status_code=403,
            detail={
                "type": KEPT_IN,
                "message": "Nothing in this workspace can be published to the "
                "library. A manager of the workspace can change that.",
            },
        )
    if rule == MANAGERS and not manager:
        raise _managers_only(
            "Only a manager of this workspace can publish its work to the library."
        )


async def link_opens_for(holder_id: str, viewer_id: str) -> bool:
    """For a members' link of a shared workspace's content, once the viewer
    is known to be an active member of its organization: whether the link
    opens for them. Under "no one" only for the people who can open the
    workspace. With the feature off for the organization nothing is
    narrowed, as nothing about shared workspaces applies there."""
    shared = await db.shared_of_holder(holder_id)
    if in_force(shared) != NONE:
        return True
    seat = await caller_seat(viewer_id)
    if seat is None:
        return True
    return seat.reaches(shared, await db.get_member_role(shared.workspace_id, viewer_id))
