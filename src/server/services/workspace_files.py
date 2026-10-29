"""The sandbox access the file routes and the upload pull share."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from fastapi import HTTPException

from ptc_agent.core.paths import WorkspaceLayout
from src.observability import safe_record, workspace_fs_bytes
from src.server.database.home_workspace import is_unbound
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.database.workspace_folders import (
    WorkspaceFolderMoving,
    is_top_level,
    workspace_folder_in_use,
)
from src.server.services.workspace_file_containment import (
    contained_absolute_path,
    contained_sandbox_path,
    is_within,
)
from src.server.services.workspace_layout import (
    WorkspaceLayoutUnavailable,
    layout_from_binding,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import require_workspace_owner
from src.server.utils.error_sanitization import (
    sandbox_failure_http,
    sandbox_unreachable_detail,
    single_line,
)

logger = logging.getLogger(__name__)


# Home joins the user's computer on its first start or turn (ensure_home);
# until then a request for its files or sandbox has nothing to reach. The
# reason is the same fact for a response that answers rather than refuses.
HOME_NOT_ON_COMPUTER = "Home is not on a computer yet"
HOME_NOT_ON_COMPUTER_REASON = "home_not_on_computer"


async def owned_workspace_on_computer(workspace_id: str, user_id: str) -> dict[str, Any]:
    """The caller's workspace, for a route that reaches its files or sandbox.

    Refused as any owner check refuses, and with a 400 while it is Home not
    yet on a computer.
    """
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    if is_unbound(workspace):
        raise HTTPException(status_code=400, detail=HOME_NOT_ON_COMPUTER)
    return workspace


def not_running_for_upload(workspace: dict[str, Any]) -> HTTPException:
    """The refusal of an upload into a workspace that is not running, on either path."""
    return HTTPException(
        status_code=409,
        detail=f"Cannot upload files: the workspace is {workspace.get('status')}. Wait for it to be running.",
    )


async def acquire_sandbox(
    workspace_id: str, user_id: str
) -> tuple[Any, dict[str, Any]]:
    """A ready sandbox and the workspace row as the acquisition left it, or the
    failure's answer: 503, or 422 when an owner's SSH machine refused.

    Acquiring is where a renamed workspace's folder moves, so a row read before
    it can name a folder that is gone: a route builds every sandbox path from
    the row this returns, never from the one it read to check ownership. A
    settle on another worker can still move it before the sandbox is read, so
    a file route takes ``acquire_sandbox_to_change`` rather than this.
    """
    manager = WorkspaceManager.get_instance()
    try:
        session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)
    except Exception as e:
        # The app-level handler's answer, status and wording: the file panel
        # categorizes on the 503's detail, whose "starting" sub-case has to
        # survive the boundary, and a refusal is the 422 every route gives it.
        logger.warning(
            f"Sandbox unreachable for workspace {workspace_id}: {single_line(str(e))}"
        )
        raise sandbox_failure_http(e) from None

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(
            status_code=503,
            detail="Sandbox is not reachable: no sandbox attached to the session",
        )
    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return sandbox, workspace


@asynccontextmanager
async def acquire_sandbox_to_change(
    workspace_id: str, user_id: str
) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    """``acquire_sandbox`` for a route that touches files by path: the folder
    stays put until the block ends, and the row is read under that hold.

    Reads need it as much as changes: a folder moved between the row read and
    the sandbox read is gone, or is a sibling's that landed on the old name,
    whose files would answer as this workspace's.
    """
    sandbox, _ = await acquire_sandbox(workspace_id, user_id)
    async with held_workspace_row(workspace_id) as workspace:
        yield sandbox, workspace


@asynccontextmanager
async def held_workspace_row(workspace_id: str) -> AsyncIterator[dict[str, Any]]:
    """The workspace row, read with its folder held until the block ends, for
    any sandbox work on a path under the folder; a folder mid-move is a 503."""
    async with AsyncExitStack() as held:
        try:
            await held.enter_async_context(workspace_folder_in_use(workspace_id))
        except WorkspaceFolderMoving as e:
            raise HTTPException(
                status_code=503, detail=sandbox_unreachable_detail(e)
            ) from None
        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise HTTPException(status_code=404, detail="Workspace not found")
        dir_name = workspace.get("dir_name")
        if dir_name and not is_top_level(dir_name):
            # A move a settle could not finish; the acquisition refuses it too.
            raise HTTPException(
                status_code=503,
                detail=sandbox_unreachable_detail(WorkspaceFolderMoving(workspace_id)),
            )
        yield workspace


def layout_for(workspace: dict[str, Any], *, manager: Any = None) -> WorkspaceLayout:
    """The folder this project owns on its computer: every route's serve root.

    Several projects share one computer root, so the root on its own is no
    longer a serve root for any of them. Takes the row the caller already read
    and raises ``WorkspaceLayoutUnavailable`` when it cannot name a folder,
    because widening to the computer would serve a sibling's files.
    """
    root = workspace.get("computer_root_dir")
    if not root:
        manager = manager or WorkspaceManager.get_instance()
        root = manager.config.to_core_config().filesystem.working_directory
    workspace_id = str((workspace or {}).get("workspace_id") or "")
    return layout_from_binding(workspace_id, workspace, root=root)


def owner_layout(workspace: dict[str, Any], *, manager: Any = None) -> WorkspaceLayout:
    """``layout_for`` as an HTTP answer, for a route that authenticated its owner.

    503 rather than 404: the placement is a fact the row is expected to carry,
    so its absence is a workspace that is not ready rather than a workspace
    that has no files. The owner gets the failure instead of a listing, since
    the only wider answer available is their neighbours' files.
    """
    try:
        return layout_for(workspace, manager=manager)
    except WorkspaceLayoutUnavailable as e:
        logger.warning(
            f"Refusing file access to workspace "
            f"{(workspace or {}).get('workspace_id')}: {single_line(str(e))}"
        )
        raise HTTPException(
            status_code=503, detail="Workspace files are not available yet"
        ) from None


def previous_dir_names_of(workspace: Mapping[str, Any] | None) -> tuple[str, ...]:
    """The folders a rename moved this workspace out of, for folding old spellings.

    Unlike the agent's fold, a former folder a sibling has taken since still
    folds here: a route only ever serves this workspace's folder, so the
    spelling answers as this workspace's file or not at all, and a link that
    carries it was written while the folder was this workspace's.
    """
    current = (workspace or {}).get("dir_name")
    return tuple(
        name
        for name in ((workspace or {}).get("previous_dir_names") or ())
        if name and name != current
    )


async def contained_target(
    sandbox: Any, path: str, work_dir: str, previous_dirs: Sequence[str] = ()
) -> str:
    """The canonical absolute path a route may act on, or 404.

    Two folds the sandbox handle cannot do for a route: the request path folds
    into *this* workspace's folder rather than the computer root the handle
    carries, and the result is canonicalised inside the sandbox so a symlink
    into a sibling's folder is judged on where it lands. 404 rather than 403
    throughout: a path that escapes is not told whether its target exists.
    """
    candidate = contained_absolute_path(path, work_dir, previous_dirs)
    if candidate is None or not sandbox.validate_path(candidate):
        raise HTTPException(status_code=404, detail="File not found")
    canonical = await contained_sandbox_path(sandbox, candidate, work_dir=work_dir)
    if canonical is None or not is_within(work_dir, canonical):
        raise HTTPException(status_code=404, detail="File not found")
    return canonical


def record_fs_bytes(op: str, size: int | None) -> None:
    """Emit workspace.fs.bytes histogram. No-op when size is unknown / negative."""
    if not size or size < 0:
        return
    safe_record(workspace_fs_bytes, int(size), {"op": op})
