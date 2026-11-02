"""Reads of a workspace file from a warm sandbox or its persisted record.

Below the routers so the share manifest and the artifact snapshot can reach a
file the way the file routes do, without depending on a route module.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.server.services.computer_manager import MachineTarget
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.persistence.resolve import resolve_file_bytes_or_none
from src.server.services.workspace_file_containment import (
    FileTooLargeToServe,
    contained_absolute_path,
    read_contained_sandbox_file,
)
from src.server.services.workspace_files import to_client_path
from src.server.services.workspace_manager import WorkspaceManager

# What a path has to clear to leave the workspace on this route. A share token
# narrowed to one subtree hands in a stricter one; see ``share_access``.
VisibilityGate = Callable[[str], bool]


async def db_fallback_bytes(
    workspace: dict[str, Any],
    workspace_id: str,
    normalized_path: str,
    extension_mime: str,
) -> tuple[bytes, str] | None:
    """Read a file's bytes from the persisted DB record (no sandbox I/O)."""
    file_record = await FilePersistenceService.get_file_content(
        workspace_id, normalized_path
    )
    if not file_record:
        return None
    content = await resolve_file_bytes_or_none(
        file_record,
        user_id=workspace["user_id"],
        context=f"serving {normalized_path} for workspace {workspace_id}",
    )
    if content is None:
        return None
    # Extension is the authority for known web types; fall back to the
    # DB-stored mime only when the extension is unrecognized.
    if extension_mime == "application/octet-stream" and file_record.get("mime_type"):
        return content, file_record["mime_type"]
    return content, extension_mime


def warm_sandbox(workspace: dict[str, Any], workspace_id: str) -> Any | None:
    """A sandbox handle only from a session this worker already holds.

    Resolved in one shot so the handle is fenced against the row's binding: a
    session bound to a replaced sandbox must read as "not warm" here. Going
    through the acquisition path instead would retire that handle and
    re-attach, which is correct for an authenticated caller and exactly wrong
    for the share and wsfiles routes, since it starts or provisions a sandbox
    from a URL-only request. A stale 'running' row whose sandbox auto-stopped
    has no warm session either.
    """
    if workspace.get("status") != "running":
        return None
    session = WorkspaceManager.get_instance().get_session_if_ready(
        workspace_id,
        expected_sandbox_id=workspace.get("sandbox_id"),
        target=MachineTarget.of_workspace(workspace),
    )
    return getattr(session, "sandbox", None) if session else None


async def warm_sandbox_bytes(
    sandbox: Any,
    normalized_path: str,
    *,
    work_dir: str,
    visible: VisibilityGate,
) -> tuple[str, bytes] | None:
    """The canonical client path and the bytes behind it, or None.

    Canonicalises before reading and judges the canonical path: a symlink under
    the workspace is otherwise a free pass through both the lexical validator
    and whatever the caller's visibility gate hides.
    """
    # The workspace folder, never the sandbox's own fold: the handle is the
    # computer's and folds against its root, which holds every sibling too.
    candidate = contained_absolute_path(normalized_path, work_dir)
    if candidate is None or not sandbox.validate_path(candidate):
        return None
    try:
        resolved = await read_contained_sandbox_file(
            sandbox, candidate, work_dir=work_dir
        )
    except FileTooLargeToServe as too_large:
        # Judged like any other read before the caller falls back to the
        # persisted copy, which may still hold what a symlink replaced.
        if not visible(to_client_path(sandbox, too_large.canonical, work_dir)):
            return None
        raise
    if resolved is None:
        return None
    canonical, content = resolved
    client_path = to_client_path(sandbox, canonical, work_dir)
    if not visible(client_path):
        return None
    return client_path, content
