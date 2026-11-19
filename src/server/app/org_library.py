"""The organization library: threads, files and skills members publish for
every member of their organization.

A thread or file item is a pointer to a link its publisher keeps on, so a
member opens it through that link and the link's own rules (members-only or
public) decide what they see. A skill item is a snapshot of the publisher's
skill, which a member installs into their own account; publishing the skill
again replaces the snapshot, and an installed copy whose hash differs from
it has an update available. Whose library an item is in is the organization the auth
service names the publisher's account a member of; an account never moves
between organizations, so the item stays there when the publisher is later
suspended or removed.

The routes answer 404 unless the ``org_library`` flag resolves on for the
caller, which needs ``teams`` and the organization's grant. Who may publish
is the ``org_library.publish`` rule, which the auth service sends per account
(owners and admins always allowed). Owners and admins remove other members'
items from the console, through the two service routes at the end.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from src.server.database import org_library as library_db
from src.server.database.artifacts import get_artifact
from src.server.database.share_codes import share_path
from src.server.services import org_library as library
from src.server.services.features import Member, org_member
from src.server.services.features import feature_enabled_for_account
from src.server.services.policy import ORG_LIBRARY_PUBLISH, PUBLIC_LINKS
from src.server.services.shared_workspaces.access import (
    FEATURE as SHARED_FEATURE,
    thread_access,
)
from src.server.services.shared_workspaces.access import Access
from src.server.services.shared_workspaces import share_out
from src.server.utils.api import CurrentUserId, ServiceCaller, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/org-library", tags=["Organization library"])

FEATURE = "org_library"
TITLE_MAX = library_db.TITLE_MAX
SUMMARY_MAX = library.SUMMARY_MAX

Kind = Literal["thread", "file", "skill"]


class LibraryItem(BaseModel):
    item_id: str
    kind: Kind
    title: str
    summary: str
    publisher_name: str | None
    #: The caller published it, so may edit or remove it.
    mine: bool
    #: Where a member opens it in the web app.
    url: str | None
    #: A file's name; None for a thread or a skill.
    name: str | None = None
    audience: Literal["public", "organization"]
    published_at: datetime
    updated_at: datetime
    #: A skill's name, the one it installs under.
    skill_name: str | None = None
    #: The caller installed this skill from the library.
    installed: bool = False
    #: The caller's installed copy is not the snapshot the library holds now.
    update_available: bool = False


class LibraryPage(BaseModel):
    items: list[LibraryItem]
    next_cursor: str | None
    #: Whether the caller may publish, so a client can grey out the action.
    can_publish: bool


class PublishedLookup(BaseModel):
    item: LibraryItem | None
    can_publish: bool
    #: When the content's shared workspace, not the organization, is why
    #: the caller may not publish it: who its managers let publish
    #: (``managers`` or ``none``). Absent otherwise.
    workspace_rule: Literal["managers", "none"] | None = None


class PublishRequest(BaseModel):
    kind: Kind
    #: The thread's id, or the file's ``artifact_id``; not for a skill.
    target_id: UUID | None = None
    #: The name of one of the caller's account-wide skills; only for a skill.
    skill_name: str | None = Field(default=None, min_length=1, max_length=64)
    #: None names it as the app names the content, or keeps the title an
    #: earlier publish gave it.
    title: str | None = Field(default=None, min_length=1, max_length=TITLE_MAX)
    summary: str | None = Field(default=None, max_length=SUMMARY_MAX)

    @model_validator(mode="after")
    def _target_for_kind(self) -> "PublishRequest":
        if (self.kind == "skill") != (self.skill_name is not None):
            raise ValueError("send skill_name for a skill, and only for a skill")
        if (self.kind != "skill") != (self.target_id is not None):
            raise ValueError(
                "send target_id for a thread or a file, and only for those"
            )
        return self


class EditRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=TITLE_MAX)
    summary: str | None = Field(default=None, max_length=SUMMARY_MAX)

    @model_validator(mode="after")
    def _something(self) -> "EditRequest":
        if self.title is None and self.summary is None:
            raise ValueError("send a title or a summary")
        return self


class ServiceLibraryItem(LibraryItem):
    publisher_id: str
    #: A member can open it now: its link is on in a live workspace.
    listed: bool
    thread_id: str | None
    artifact_id: str | None


class ServiceLibraryPage(BaseModel):
    items: list[ServiceLibraryItem]
    next_cursor: str | None


def _item(row: dict[str, Any], user_id: str | None) -> dict[str, Any]:
    url = None
    if row["kind"] == "thread" and row.get("share_token"):
        url = f"/s/{row['share_token']}"
    elif row["kind"] == "file" and row.get("code"):
        url = share_path(row["code"])
    path = row.get("path")
    installed_hash = row.get("installed_hash")
    return {
        "item_id": row["item_id"],
        "kind": row["kind"],
        "title": row["title"],
        "summary": row["summary"],
        "publisher_name": row.get("publisher_name") or None,
        "mine": user_id is not None and row["publisher_id"] == user_id,
        "url": url,
        "name": path.rsplit("/", 1)[-1] if path else None,
        "audience": "organization"
        if row.get("audience") == "organization"
        else "public",
        "published_at": row["published_at"],
        "updated_at": row["updated_at"],
        "skill_name": row.get("skill_name"),
        "installed": installed_hash is not None,
        "update_available": installed_hash is not None
        and installed_hash != row.get("content_hash"),
    }


async def _member(user_id: str) -> Member:
    """The caller, when the library is on for them: an active member of an
    organization granted ``org_library``. 404 otherwise, as an off route; 503
    while the account cannot be read."""
    member = await org_member(user_id, FEATURE)
    if member is None:
        raise library.not_found()
    return member


@router.get("", response_model=LibraryPage)
@handle_api_exceptions("list the library", logger)
async def list_library(
    user_id: CurrentUserId,
    kind: Kind | None = None,
    cursor: str | None = None,
    limit: int = Query(default=library_db.PAGE_SIZE, ge=1, le=100),
) -> LibraryPage:
    """The caller's organization's items a member can open now, newest first."""
    member = await _member(user_id)
    kept_in = await feature_enabled_for_account(
        user_id, SHARED_FEATURE, member.account
    )
    try:
        rows, next_cursor = await library_db.list_items(
            member.org.id, viewer=user_id, viewer_teams=member.account.team_ids,
            kind=kind, cursor=cursor, limit=limit, kept_in=kept_in,
            public_links=member.policy.allows(PUBLIC_LINKS),
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return LibraryPage(
        items=[LibraryItem(**_item(row, user_id)) for row in rows],
        next_cursor=next_cursor,
        can_publish=member.policy.allows(ORG_LIBRARY_PUBLISH),
    )


async def _reach_content(
    thread_id: str | None, artifact_id: str | None, user_id: str
) -> tuple[Any, Access]:
    """Refuse a thread or file the caller does not reach: their own, or a
    shared workspace's they are a member of. The file's record (None for a
    thread), and the caller's access to the content's workspace, whose rows
    it is and whose managers say whether it may leave it."""
    if thread_id is not None:
        return None, await thread_access(thread_id, user_id)
    artifact = await get_artifact(artifact_id, user_id=user_id)
    if artifact is not None:
        return artifact, Access(user_id)
    from src.server.app.artifacts.records import reached

    try:
        return await reached(artifact_id or "", user_id)
    except HTTPException as e:
        if e.status_code == 404:
            raise library.not_found() from None
        raise


@router.get(
    "/published", response_model=PublishedLookup, response_model_exclude_unset=True
)
@handle_api_exceptions("look up a library item", logger)
async def find_published(
    user_id: CurrentUserId,
    thread_id: UUID | None = None,
    artifact_id: UUID | None = None,
    skill_name: str | None = Query(default=None, min_length=1, max_length=64),
) -> PublishedLookup:
    """The item the caller's thread, file or skill is published as, if any,
    for the share dialogs and the skills list."""
    if sum(x is not None for x in (thread_id, artifact_id, skill_name)) != 1:
        raise HTTPException(
            status_code=400, detail="Send one of thread_id, artifact_id, skill_name"
        )
    member = await _member(user_id)
    thread = str(thread_id) if thread_id else None
    artifact = str(artifact_id) if artifact_id else None
    access: Access | None = None
    if skill_name is not None:
        row = await library_db.find_skill_item(member.org.id, user_id, skill_name)
    else:
        _, access = await _reach_content(thread, artifact, user_id)
        row = await library_db.find_item(thread_id=thread, artifact_id=artifact)
    if row is not None and row["org_id"] != member.org.id:
        row = None
    lookup = PublishedLookup(
        item=LibraryItem(**_item(row, user_id)) if row else None,
        can_publish=member.policy.allows(ORG_LIBRARY_PUBLISH),
    )
    if access is not None and lookup.can_publish:
        rule, manager = await share_out.rule_for(access.shared)
        if rule is not None and not share_out.may_share_out(rule, manager):
            # Set only here, so every other lookup answers as it always has.
            lookup.can_publish = False
            lookup.workspace_rule = rule
    return lookup


@router.post("", response_model=LibraryItem)
@handle_api_exceptions("publish to the library", logger)
async def publish_item(request: PublishRequest, user_id: CurrentUserId) -> LibraryItem:
    """Publish a thread or file of the caller's whose link is on, or one of
    their account-wide skills, or edit the item it is already published as
    (for a skill, replace its snapshot).

    The link stays as its owner set it: a members-only link opens to the
    members, a public one to anyone, so publishing never widens who can open
    the content. A link that is off is refused (409 ``link_off``); turn it on
    first.
    """
    member = await _member(user_id)
    member.policy.require(ORG_LIBRARY_PUBLISH)
    title, summary = library.cleaned(request.title, request.summary)
    if request.kind == "skill":
        row = await library.publish_skill(
            member.org.id, user_id, request.skill_name or "", title, summary
        )
        return LibraryItem(**_item(row, user_id))
    target_id = str(request.target_id)
    thread_id, artifact_id = (
        (target_id, None) if request.kind == "thread" else (None, target_id)
    )
    artifact, access = await _reach_content(thread_id, artifact_id, user_id)
    row = await library.publish_target(
        member.org.id, user_id, request.kind, target_id,
        access=access, artifact=artifact, title=title, summary=summary,
    )
    return LibraryItem(**_item(row, user_id))


@router.post("/{item_id}/install", response_model=LibraryItem)
@handle_api_exceptions("install a library skill", logger)
async def install_skill(item_id: UUID, user_id: CurrentUserId) -> LibraryItem:
    """Install a skill item into the caller's account, or update the copy an
    earlier install made to the snapshot the library holds now.

    A skill of the caller's own by that name is never replaced: 409
    ``name_taken``. Installing what is already installed is a no-op that
    answers the same.
    """
    member = await _member(user_id)
    row = await library.install_skill(member.org.id, user_id, str(item_id))
    return LibraryItem(**_item(row, user_id))


@router.patch("/{item_id}", response_model=LibraryItem)
@handle_api_exceptions("edit a library item", logger)
async def edit_item(
    item_id: UUID, request: EditRequest, user_id: CurrentUserId
) -> LibraryItem:
    """Change the title or summary of an item the caller published."""
    member = await _member(user_id)
    row = await library.edit_item(
        member.org.id, user_id, str(item_id), request.title, request.summary
    )
    return LibraryItem(**_item(row, user_id))


@router.delete("/{item_id}", status_code=204)
@handle_api_exceptions("remove a library item", logger)
async def remove_item(item_id: UUID, user_id: CurrentUserId) -> None:
    """Remove an item the caller published. The link itself stays as it is."""
    member = await _member(user_id)
    await library.remove_item(
        str(item_id), org_id=member.org.id, actor_id=user_id, publisher_id=user_id
    )


# --- the console ---------------------------------------------------------------


@router.get("/service", response_model=ServiceLibraryPage, dependencies=[ServiceCaller])
@handle_api_exceptions("list the library for the console", logger)
async def list_for_console(
    org_id: str = Query(..., min_length=1),
    kind: Kind | None = None,
    cursor: str | None = None,
    limit: int = Query(default=library_db.PAGE_SIZE, ge=1, le=100),
) -> ServiceLibraryPage:
    """Every item of the organization, newest first, those a member cannot
    open now (``listed`` false) included, for its owners and admins to
    moderate. Read whatever the flags say now: the items are the
    organization's."""
    try:
        rows, next_cursor = await library_db.list_items(
            org_id, kind=kind, cursor=cursor, limit=limit, listed_only=False
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return ServiceLibraryPage(
        items=[
            ServiceLibraryItem(
                **_item(row, None),
                publisher_id=row["publisher_id"],
                listed=bool(row["listed"]),
                thread_id=row.get("thread_id"),
                artifact_id=row.get("artifact_id"),
            )
            for row in rows
        ],
        next_cursor=next_cursor,
    )


@router.delete("/service/{item_id}", status_code=204, dependencies=[ServiceCaller])
@handle_api_exceptions("remove a library item for the console", logger)
async def remove_for_console(
    item_id: UUID,
    org_id: str = Query(..., min_length=1),
    actor_id: str = Query(..., min_length=1, max_length=255),
) -> None:
    """Remove one of the organization's items, as the owner or admin
    ``actor_id`` asked in the console; the activity log records it as theirs.
    404 for an item of another organization."""
    await library.remove_item(str(item_id), org_id=org_id, actor_id=actor_id)
