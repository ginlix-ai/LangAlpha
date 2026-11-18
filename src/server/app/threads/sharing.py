"""Public thread sharing: toggle share state and read share status.

A shared workspace's thread is shared by its members, as its owner shares a
personal one, as far as the workspace's managers allow
(``shared_workspaces.share_out``); who its link may open to is judged by the
holder's organization, as its visitors are (members of it only, never
anyone)."""

import json
from datetime import datetime, timezone


from fastapi import HTTPException

from src.server.utils.api import (
    CurrentUserId,
)
from src.server.models.conversation import (
    ThreadShareRequest,
    ThreadShareResponse,
    SharePermissions,
)
from src.server.database.conversation import (
    get_thread_by_id,
    update_thread_sharing,
)
from src.server.services import activity
from src.server.services.activity import Action
from src.server.services.share_audience import (
    ORGANIZATION,
    offered,
    refuse_public_link_of,
    require_members_audience,
    thread_audience,
)
from src.server.services.shared_workspaces import access as shared_access
from src.server.services.shared_workspaces.access import thread_access
from src.server.services.shared_workspaces.share_out import refuse_link_change



from ._deps import router


# =============================================================================
# THREAD SHARING
# =============================================================================


def _perms(perms) -> dict:
    if isinstance(perms, str):
        perms = json.loads(perms)
    return perms if isinstance(perms, dict) else {}


def _share_flags(perms) -> dict[str, bool]:
    """What a link lets a visitor do, its yes-or-no permissions only."""
    return {k: v for k, v in _perms(perms).items() if isinstance(v, bool)}


def _record_share_change(
    user_id: str, thread_id: str, thread: dict, updated: dict
) -> None:
    """Record what the request changed about the thread's link: turned on,
    turned off, or, on a link already on, which permissions moved. Asking
    for what already stands records nothing."""
    was_on, is_on = bool(thread.get("is_shared")), bool(updated.get("is_shared"))
    after = _share_flags(updated.get("share_permissions"))
    if is_on and not was_on:
        action, detail = Action.THREAD_LINK_ON, after
        if thread_audience(updated.get("share_permissions")) == ORGANIZATION:
            detail = {**after, "audience": ORGANIZATION}
    elif was_on and not is_on:
        action, detail = Action.THREAD_LINK_OFF, after
    elif is_on:
        before_perms = _perms(thread.get("share_permissions"))
        after_perms = _perms(updated.get("share_permissions"))
        before = _share_flags(before_perms)
        detail = {k: v for k, v in after.items() if before.get(k, False) != v}
        # The folder a link opens is a path, so only that it moved is kept.
        if before_perms.get("root_path") != after_perms.get("root_path"):
            detail["root_path_changed"] = True
        if thread_audience(before_perms) != thread_audience(after_perms):
            detail["audience"] = thread_audience(after_perms)
        if not detail:
            return
        action = Action.THREAD_LINK_PERMISSIONS_CHANGED
    else:
        return
    activity.record(
        user_id, action, resource=("thread", thread_id), thread_id=thread_id,
        workspace_id=updated.get("workspace_id") or thread.get("workspace_id"),
        detail=detail,
    )


@router.post("/{thread_id}/share", response_model=ThreadShareResponse)
async def update_thread_share(
    thread_id: str,
    request: ThreadShareRequest,
    x_user_id: CurrentUserId,
):
    """Toggle sharing for a thread and update permissions and audience.

    ``public_links`` governs only a link that opens to anyone; the members
    audience is asked of the owner's account instead, and only when chosen.
    """
    access = await shared_access.thread_access(thread_id, x_user_id)
    owner_id = access.owner_id
    await refuse_link_change(access)

    thread = await get_thread_by_id(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    chosen = (
        request.permissions.audience
        if request.permissions is not None
        and "audience" in request.permissions.model_fields_set
        else thread_audience(_perms(thread.get("share_permissions")))
    )
    if chosen == ORGANIZATION:
        if request.permissions is not None and "audience" in request.permissions.model_fields_set:
            await require_members_audience(owner_id)
    elif request.is_shared:
        await refuse_public_link_of(owner_id)

    # Build update kwargs
    kwargs: dict = {"is_shared": request.is_shared}

    if request.is_shared:
        kwargs["shared_at"] = datetime.now(timezone.utc)

    # Merge permissions: start from existing, overlay provided fields
    existing_perms = thread.get("share_permissions") or {}
    if isinstance(existing_perms, str):
        existing_perms = json.loads(existing_perms)

    if request.permissions is not None:
        # ``exclude_unset``: a request that speaks only about the flags must not
        # silently widen a token scoped to one report back to the whole
        # workspace, which a defaulted ``root_path`` of "" would do.
        merged = {**existing_perms, **request.permissions.model_dump(exclude_unset=True)}
        # Enforce: download requires files
        if merged.get("allow_download") and not merged.get("allow_files"):
            merged["allow_files"] = True
        kwargs["share_permissions"] = merged

    updated = await update_thread_sharing(thread_id, **kwargs)
    if not updated:
        raise HTTPException(status_code=404, detail="Thread not found")
    _record_share_change(x_user_id, thread_id, thread, updated)

    share_token = updated.get("share_token")
    perms = updated.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)

    return ThreadShareResponse(
        is_shared=updated["is_shared"],
        share_token=share_token if updated["is_shared"] else None,
        share_url=f"/s/{share_token}" if updated["is_shared"] and share_token else None,
        permissions=SharePermissions(**(perms if isinstance(perms, dict) else {})),
        audiences=await offered(owner_id, thread_audience(_perms(perms))),
    )


@router.get("/{thread_id}/share", response_model=ThreadShareResponse)
async def get_thread_share(thread_id: str, x_user_id: CurrentUserId):
    """Get current share status and permissions for a thread."""
    owner_id = (await thread_access(thread_id, x_user_id)).owner_id

    thread = await get_thread_by_id(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    share_token = thread.get("share_token")
    is_shared = thread.get("is_shared", False)
    perms = thread.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)

    return ThreadShareResponse(
        is_shared=is_shared,
        share_token=share_token if is_shared else None,
        share_url=f"/s/{share_token}" if is_shared and share_token else None,
        permissions=SharePermissions(**(perms if isinstance(perms, dict) else {})),
        audiences=await offered(owner_id, thread_audience(_perms(perms))),
    )
