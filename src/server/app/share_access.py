"""What a share token authorizes: a thread, a workspace folder, and a subtree.

The file routes and the replay routes both start here, and neither of them
decides any of it for itself. A file share link resolves here too, for the
one route it may use: it opens a workspace folder and a fixed list of files
rather than a subtree.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from ptc_agent.core.paths import is_agent_notes_path
from src.server.app.workspace_files._shared import _is_serve_blocked_path, work_dir_for
from src.server.database.conversation import get_thread_by_share_token
from src.server.database.share_codes import is_share_code
from src.server.database.share_links import KIND_FILE, ShareLink, get_link
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_file_containment import contained_relative_path
from src.server.services.workspace_files import previous_dir_names_of
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable
from src.server.utils.error_sanitization import single_line

logger = logging.getLogger(__name__)

# The share permission that narrows a token to part of the workspace. A token
# minted without it opens the whole workspace, which is what every token minted
# so far carries and what the shared-thread file panel browses.
SHARE_ROOT_PATH_KEY = "root_path"


async def get_shared_thread(share_token: str) -> dict[str, Any]:
    """Fetch shared thread or raise 404."""
    thread = await get_thread_by_share_token(share_token)
    if not thread:
        raise HTTPException(status_code=404, detail="Shared thread not found")
    return thread


def get_permissions(thread: dict[str, Any]) -> dict[str, Any]:
    """Extract permissions dict from thread record."""
    perms = thread.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)
    return perms


def require_permission(perms: dict[str, Any], key: str) -> None:
    """Raise 403 if a specific permission is not granted."""
    if not perms.get(key):
        raise HTTPException(
            status_code=403,
            detail=f"Permission '{key}' not granted for this shared thread",
        )


@dataclass(frozen=True)
class ShareScope:
    """What a share token resolves to: a workspace and the subtree it opens.

    The pair is the unit, not the workspace alone. A token issued for one report
    serves that path and its subtree; nothing else in the workspace is reachable
    through it, however the URL is spelled.
    """

    workspace_id: str
    root_path: str

    def contains(self, relative_path: str) -> bool:
        if not self.root_path:
            return True
        return relative_path == self.root_path or relative_path.startswith(
            f"{self.root_path}/"
        )


def shared_path_visible(scope: ShareScope, client_path: str) -> bool:
    """Whether a canonical workspace path may leave through this share token.

    Run on the path a sandbox read resolved to, not on the one that was asked
    for: a symlink is how an in-scope request turns into an out-of-scope file.

    The workspace's notes file stays out too, whatever the scope: it is the
    agent's own runtime context, and a shared page cannot tell it from a
    deliverable because the share carries no folder name to recognise it by.
    """
    return (
        scope.contains(client_path)
        and not _is_serve_blocked_path(client_path)
        and not is_agent_notes_path(client_path)
    )


@dataclass(frozen=True)
class SharedFileTarget:
    """One resolution of a token for the file routes, read once per request.

    ``files`` is set for a file share link: the list the owner confirmed, and
    the only paths that leave. It is judged on the canonical path a read
    resolved to, like the scope, so a symlink is not a way around it.
    """

    workspace: dict[str, Any]
    scope: ShareScope
    work_dir: str
    files: frozenset[str] | None = None

    @property
    def workspace_id(self) -> str:
        return self.scope.workspace_id

    def visible(self, client_path: str) -> bool:
        if self.files is not None and client_path not in self.files:
            return False
        return shared_path_visible(self.scope, client_path)


async def _serving_workspace(workspace_id: str) -> tuple[dict[str, Any], str]:
    """The workspace row and its serve root, or this route's uniform 404.

    A workspace whose folder cannot be established answers like a missing
    file: the caller holds no credential but the URL, and the computer root
    is not a fallback for a folder nobody could read.
    """
    try:
        workspace = await db_get_workspace(workspace_id)
    except Exception as e:
        logger.warning(
            f"Workspace read failed for shared workspace {workspace_id}: "
            f"{single_line(str(e))}"
        )
        raise HTTPException(status_code=404, detail="File not found") from None
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    try:
        work_dir = work_dir_for(workspace)
    except WorkspaceLayoutUnavailable as e:
        logger.warning(
            f"Refusing file access to shared workspace {workspace_id}: "
            f"{single_line(str(e))}"
        )
        raise HTTPException(status_code=404, detail="File not found") from None
    return workspace, work_dir


async def resolve_shared_files(
    share_token: str,
    *,
    require_files: bool = False,
    require_download: bool = False,
) -> SharedFileTarget:
    """Resolve a share token to the thread, workspace, scope and serve root.

    The four come back together because none of them is usable alone: the
    scope is relative to the folder, and the folder comes off the workspace row
    that the response bodies already need.

    A stored root path that does not normalise is a malformed token rather than
    a missing file, and a workspace whose folder cannot be established is
    neither, but all three answer 404: this route never tells an anonymous
    caller which of its checks failed, and the computer root is not a fallback
    for a folder nobody could read.
    """
    thread = await get_shared_thread(share_token)
    perms = get_permissions(thread)

    if require_files:
        require_permission(perms, "allow_files")
    if require_download:
        require_permission(perms, "allow_download")

    workspace_id = str(thread["workspace_id"])
    workspace, work_dir = await _serving_workspace(workspace_id)

    raw_root = perms.get(SHARE_ROOT_PATH_KEY) or ""
    if not raw_root:
        return SharedFileTarget(workspace, ShareScope(workspace_id, ""), work_dir)
    root_path = contained_relative_path(
        str(raw_root), work_dir, previous_dir_names_of(workspace)
    )
    if root_path is None:
        logger.warning(
            f"Share token for workspace {workspace_id} carries an unusable "
            f"{SHARE_ROOT_PATH_KEY}; refusing file access"
        )
        raise HTTPException(status_code=404, detail="File not found")
    return SharedFileTarget(workspace, ShareScope(workspace_id, root_path), work_dir)


# One answer for everything a caller may not learn about a link: an unknown
# code, a private link seen by anyone but its owner, an app seen by anyone else.
LINK_NOT_FOUND = "Shared thread not found"


@dataclass(frozen=True)
class LinkAccess:
    """A link this viewer may open, with the workspace it opens."""

    link: ShareLink
    workspace: dict[str, Any]
    work_dir: str
    owner: bool


async def resolve_link(token: str, *, user_id: str | None = None) -> LinkAccess | None:
    """The one rule for who may open a file or app link.

    None when the token is not a link at all, so the caller falls through to
    the thread rules. A private link, which an app link always is, opens for
    the workspace's owner alone; everyone else gets ``LINK_NOT_FOUND``.
    Ownership is judged on the workspace row, never on the link.
    """
    if not is_share_code(token):
        return None
    link = await get_link(token)
    if link is None:
        return None
    if user_id is None and not link.shared:
        # No viewer whose ownership could open it, so no workspace read.
        raise HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    try:
        workspace, work_dir = await _serving_workspace(link.workspace_id)
    except HTTPException:
        raise HTTPException(status_code=404, detail=LINK_NOT_FOUND) from None
    owner = user_id is not None and workspace.get("user_id") == user_id
    if not owner and not link.shared:
        raise HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    return LinkAccess(link, workspace, work_dir, owner)


async def resolve_serve_target(code: str) -> SharedFileTarget:
    """What the serve route may answer for a token of either kind.

    Always resolved as a visitor, so only a shared file link opens anything
    here, and only for the files its owner confirmed. A token that is not a
    link falls through to the thread rules, files permission and all.
    """
    access = await resolve_link(code)
    if access is None:
        return await resolve_shared_files(code, require_files=True)
    link = access.link
    if link.kind != KIND_FILE:
        raise HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    return SharedFileTarget(
        access.workspace,
        ShareScope(link.workspace_id, ""),
        access.work_dir,
        files=frozenset(link.files or ()),
    )
