"""Who a link opens to: anyone, or the members of its owner's organization.

An account in an organization is wholly that organization's, so whose a link's
content is follows from whose its owner's account is: the organization the
auth service names the owner a member of, suspended included (a suspended or
removed member's work stays the organization's and readable by its members).
Nothing about the organization is stored on the link, so it can never name
one its owner is not in.

The ``public_links`` rule governs only links that open to anyone; a
members-only link is the organization sharing inside itself.

A shared workspace's content is its holder's, whose organization is the
workspace's, so its links open to that organization's members, and never to
anyone: the holder's policy refuses public links, which keeps the work of
several people from leaving the organization on one member's say. Its
managers may narrow its links further, to the people in the workspace
(``shared_workspaces.share_out``).
"""

from __future__ import annotations

from typing import Literal

from fastapi import HTTPException

from src.server.services.account import (
    AccountUnknown,
    account_for,
    active_in,
    member_of,
)
from src.server.services.features import org_member
from src.server.services.membership import teams_door_open
from src.server.database.shared_workspaces import is_holder
from src.server.services.policy import PUBLIC_LINKS
from src.server.services.account import require_policy
from src.server.services.shared_workspaces.share_out import link_opens_for

Audience = Literal["public", "organization"]
PUBLIC: Audience = "public"
ORGANIZATION: Audience = "organization"

#: The ``type`` a signed-out visitor of a members-only link is answered.
MEMBERS_ONLY = "members_only"

#: One answer for everything a caller may not learn about a link: an unknown
#: code, a private link seen by anyone but its owner, an app seen by anyone
#: else, a members-only link seen by anyone but a member.
LINK_NOT_FOUND = "Shared thread not found"


def members_only() -> HTTPException:
    """A signed-out visitor of a members-only link: naming no organization,
    so the page can only offer to sign in. With the ``teams`` door shut the
    link opens for nobody, so it is not found, and nothing says it is a
    members' link."""
    if not teams_door_open():
        return HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    return HTTPException(
        status_code=401,
        detail={
            "type": MEMBERS_ONLY,
            "message": "Sign in to open this link. It opens only for members of an organization.",
        },
    )


def thread_audience(perms: dict | None) -> Audience:
    return ORGANIZATION if (perms or {}).get("audience") == ORGANIZATION else PUBLIC


def link_audience(stored: str | None) -> Audience:
    return ORGANIZATION if stored == ORGANIZATION else PUBLIC


async def offered(owner_id: str, current: Audience = PUBLIC) -> list[Audience]:
    """The audiences the owner may choose now, with the one set already. The
    organization is offered while the owner is an active member of one that
    allows ``teams``; with the door shut nothing is read."""
    audiences: list[Audience] = [] if is_holder(owner_id) else [PUBLIC]
    if teams_door_open():
        account = await account_for(owner_id)
        if account is not None and account.org is not None and not account.suspended:
            audiences.append(ORGANIZATION)
    if current not in audiences:
        audiences.append(current)
    return audiences


async def require_members_audience(owner_id: str) -> None:
    """Refuse choosing the members audience for an owner it is not offered
    to (403), or whose account cannot be read (503)."""
    if await org_member(owner_id) is None:
        raise _not_offered()


async def refuse_public_link_of(owner_id: str) -> None:
    """Refuse turning on a link that opens without signing in as the user
    (403) for content of ``owner_id``'s; turning one off never asks. A shared
    workspace's never opens to anyone (403), whatever its members' own
    rules allow."""
    if is_holder(owner_id):
        raise HTTPException(
            status_code=403,
            detail={
                "type": SHARED_MEMBERS_ONLY,
                "message": "Links from a shared workspace open only to members of its organization.",
            },
        )

    (await require_policy(owner_id)).require(PUBLIC_LINKS)


#: The ``type`` of a refusal to open a shared workspace's link to anyone.
SHARED_MEMBERS_ONLY = "shared_members_only"


def _not_offered() -> HTTPException:
    return HTTPException(
        status_code=403,
        detail={
            "type": "audience_unavailable",
            "message": "Only members of an organization can share with its members.",
        },
    )


async def admits_member(viewer_id: str | None, owner_id: str | None) -> bool:
    """Whether ``viewer_id`` may open a members-only link of ``owner_id``'s:
    an active member of the organization the owner's account is in (for a
    shared workspace that keeps its work in, also one of its people). Both are
    read on every request, so a removal or suspension ends access at once.
    With the door shut nobody is admitted. Raises the 503 an unknown account
    gets, for either side, rather than guess."""
    if not viewer_id or not owner_id or not teams_door_open():
        return False
    owners = await member_of(owner_id)
    if owners is None:
        return False
    active = (
        not owners.suspended if viewer_id == owner_id else await active_in(viewer_id, owners.id)
    )
    if active is None:
        raise AccountUnknown()
    if not active:
        return False
    if is_holder(owner_id):
        return await link_opens_for(owner_id, viewer_id)
    return True
