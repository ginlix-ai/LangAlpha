"""Publishing to, installing from and removing from the organization
library, each recorded in the organization's activity log. The routes
(``app/org_library.py``) decide who the caller is and what they reach; these
take that as given and answer an item's row."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import HTTPException

from src.server.database import org_library as library_db
from src.server.database.conversation import get_thread_by_id
from src.server.database.user_skills import get_user_skill
from src.server.services import activity, skill_archive_storage
from src.server.services.activity import Action
from src.server.services.shared_workspaces import share_out
from src.server.services.shared_workspaces.access import Access
from src.server.services.user_skills.materialize import fetch_skill_archive
from src.server.services.user_skills.store import store_skill_archive

SUMMARY_MAX = 1000


def not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="Not found")


def _link_off() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={"type": "link_off", "message": "Turn on the link before publishing."},
    )


def cleaned(
    title: str | None, summary: str | None
) -> tuple[str | None, str | None]:
    """A title and summary as sent, with the title's runs of whitespace
    collapsed; one left empty is refused (422)."""
    if title is not None:
        title = " ".join(title.split())
        if title == "":
            raise HTTPException(status_code=422, detail="A title is required")
    return title, summary.strip() if summary is not None else None


async def _item(item_id: str, viewer: str) -> dict[str, Any]:
    row = await library_db.get_item(item_id, viewer=viewer)
    if row is None:
        raise not_found()
    return row


async def publish_target(
    org_id: str,
    user_id: str,
    kind: Literal["thread", "file"],
    target_id: str,
    *,
    access: Access,
    artifact: Any,
    title: str | None,
    summary: str | None,
) -> dict[str, Any]:
    """Publish a thread or file the caller reaches through ``access`` (its
    record is ``artifact``, None for a thread), or edit the item it is
    already published as.

    The link stays as its owner set it: a members-only link opens to the
    members, a public one to anyone, so publishing never widens who can open
    the content. A link that is off is refused (409 ``link_off``).
    """
    await share_out.refuse_publish(access)
    if kind == "thread":
        thread = await get_thread_by_id(target_id)
        if not thread:
            raise not_found()
        link_on, workspace_id = bool(thread.get("is_shared")), thread.get("workspace_id")
    else:
        link_on, workspace_id = artifact.shared, artifact.workspace_id
    if not link_on:
        raise _link_off()
    published = await library_db.publish(
        org_id=org_id,
        publisher_id=user_id,
        owner_id=access.owner_id,
        kind=kind,
        target_id=target_id,
        title=title,
        summary=summary,
    )
    if published is None:
        raise _link_off()
    item_id, created = published
    row = await _item(item_id, user_id)
    activity.record(
        user_id,
        Action.LIBRARY_PUBLISHED if created else Action.LIBRARY_UPDATED,
        resource=("library_item", item_id),
        workspace_id=workspace_id,
        thread_id=target_id if kind == "thread" else None,
        detail={"kind": kind, "title": row["title"][:256]},
    )
    return row


async def publish_skill(
    org_id: str,
    user_id: str,
    skill_name: str,
    title: str | None,
    summary: str | None,
) -> dict[str, Any]:
    """Snapshot one of the caller's account-wide skills into the library.

    The library keeps its own copy, under the organization's prefix when
    object storage is configured and inline otherwise, so deleting or
    replacing the skill later leaves the item installable. A skill a plugin
    installed is refused: it is the plugin's, and members get it from there.
    """
    row = await get_user_skill(user_id, skill_name)
    if row is None:
        raise not_found()
    if row.get("plugin_id"):
        raise HTTPException(
            status_code=409,
            detail={
                "type": "plugin_skill",
                "message": "A skill a plugin installed can't be published. Publish one you uploaded.",
            },
        )
    try:
        content = await fetch_skill_archive(user_id, row)
    except skill_archive_storage.SkillArchiveStorageError as exc:
        raise HTTPException(
            status_code=502, detail="Could not read the skill. Please retry."
        ) from exc
    archive_key: str | None = None
    archive_blob: bytes | None = None
    if skill_archive_storage.is_configured():
        try:
            archive_key = await skill_archive_storage.store_library_archive(
                org_id=org_id, content=content, content_hash=row["content_hash"]
            )
        except skill_archive_storage.SkillArchiveStorageError as exc:
            raise HTTPException(
                status_code=502, detail="Could not store the skill. Please retry."
            ) from exc
    else:
        archive_blob = content
    item_id, created, superseded = await library_db.publish_skill(
        org_id=org_id,
        publisher_id=user_id,
        skill_name=skill_name,
        title=title,
        summary=summary
        if summary is not None
        else (row.get("description") or "")[:SUMMARY_MAX],
        content_hash=row["content_hash"],
        archive_key=archive_key,
        archive_blob=archive_blob,
        archive_bytes=len(content),
    )
    await _drop_snapshot(superseded)
    item = await _item(item_id, user_id)
    activity.record(
        user_id,
        Action.LIBRARY_PUBLISHED if created else Action.LIBRARY_UPDATED,
        resource=("library_item", item_id),
        detail={"kind": "skill", "title": item["title"][:256], "skill": skill_name},
    )
    return item


async def install_skill(org_id: str, user_id: str, item_id: str) -> dict[str, Any]:
    """Install a skill item into the caller's account, or update the copy an
    earlier install made to the snapshot the library holds now.

    A skill of the caller's own by that name is never replaced: 409
    ``name_taken``. Installing what is already installed is a no-op that
    answers the same.
    """
    item = await library_db.get_item(item_id, viewer=user_id)
    if item is None or item["org_id"] != org_id or item["kind"] != "skill":
        raise not_found()
    if item.get("installed_hash") != item["content_hash"]:
        exists, origin = await library_db.own_skill_origin(user_id, item["skill_name"])
        if exists and origin != item_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "type": "name_taken",
                    "message": f"You already have a skill named {item['skill_name']}.",
                    "name": item["skill_name"],
                },
            )
        snapshot = await library_db.skill_archive(item_id)
        if snapshot is None:
            raise not_found()
        key, blob = snapshot
        try:
            raw = (
                blob
                if blob is not None
                else await skill_archive_storage.fetch_archive(key or "")
            )
        except skill_archive_storage.SkillArchiveStorageError as exc:
            raise HTTPException(
                status_code=502, detail="Could not read the skill. Please retry."
            ) from exc
        await store_skill_archive(user_id, raw, org_library_item_id=item_id)
    return await _item(item_id, user_id)


async def edit_item(
    org_id: str, user_id: str, item_id: str, title: str | None, summary: str | None
) -> dict[str, Any]:
    """Change the title or summary of an item the caller published."""
    row = await library_db.get_item(item_id, viewer=user_id)
    if row is None or row["org_id"] != org_id or row["publisher_id"] != user_id:
        raise not_found()
    title, summary = cleaned(title, summary)
    if not await library_db.edit(
        item_id, publisher_id=user_id, title=title, summary=summary
    ):
        raise not_found()
    activity.record(
        user_id,
        Action.LIBRARY_UPDATED,
        resource=("library_item", item_id),
        thread_id=row.get("thread_id"),
        detail={"kind": row["kind"], "title": (title or row["title"])[:256]},
    )
    return await _item(item_id, user_id)


async def remove_item(
    item_id: str, *, org_id: str, actor_id: str, publisher_id: str | None = None
) -> None:
    """Remove an item: the publisher's own (``publisher_id``), or any of the
    organization's, as the owner or admin ``actor_id`` asked in the console,
    which the activity log records as theirs. The link itself stays."""
    removed = await library_db.remove(item_id, publisher_id=publisher_id, org_id=org_id)
    if removed is None:
        raise not_found()
    await _drop_snapshot(removed.get("archive_key"))
    detail = {"kind": removed["kind"], "title": removed["title"][:256]}
    if publisher_id is None:
        detail |= {"publisher_id": removed["publisher_id"], "from_console": True}
    activity.record(
        actor_id,
        Action.LIBRARY_REMOVED,
        resource=("library_item", item_id),
        thread_id=removed.get("thread_id"),
        detail=detail,
    )


async def _drop_snapshot(archive_key: str | None) -> None:
    """Delete a skill snapshot's object once no item holds it; best effort,
    as an orphan object is a hygiene issue, not a correctness one."""
    if archive_key and not await library_db.archive_in_use(archive_key):
        await skill_archive_storage.delete_archive(archive_key)
