"""A deleted comment's pictures, taken off its workspace's computer.

A picture is a file the app wrote into the workspace's ``.agents/comments``
folder as the comment was saved, for the agent to open. Once the comment is
gone nothing names it, so it goes too, but only from a computer that is up:
waking a stopped one to tidy a folder costs the owner a start, and the
comment's row, which is what every reader goes by, is already gone. A picture
left on a stopped computer, or by a worker that stops before this runs, stays
in that folder, read by nothing.
"""

from __future__ import annotations

import logging
import shlex
from collections.abc import Sequence
from contextlib import AsyncExitStack

from src.server.database.artifacts import comments as artifacts_db
from src.server.services.workspace_file_containment import (
    contained_absolute_path,
    contained_sandbox_paths,
)
from src.server.services.workspace_files import (
    held_workspace_row,
    owner_work_dir,
    sandbox_if_running,
)
from src.server.utils.error_sanitization import single_line

logger = logging.getLogger(__name__)


def remove_pictures_soon(
    workspace_id: str, user_id: str, pictures: Sequence[str]
) -> None:
    """``remove_pictures`` without holding up the delete that asks for it."""
    if not pictures:
        return
    from src.server.services.runs.coordinator import spawn_protected

    spawn_protected(
        remove_pictures(workspace_id, user_id, pictures),
        f"comment-pictures-{workspace_id}",
    )


async def remove_pictures(
    workspace_id: str, user_id: str, pictures: Sequence[str]
) -> list[str]:
    """Remove ``pictures`` (workspace-relative, as an anchor names them) from
    the workspace's folder if its computer is running, and return the paths
    ``rm -f`` ran on. Never raises: the comment is deleted already.

    A picture must resolve inside the folder, as the file routes require, so
    a ``.agents/comments`` the agent turned into a link deletes nothing
    beyond it. One already gone is no failure."""
    if not pictures:
        return []
    try:
        # The folder is held first, so the sandbox and the folder it names are
        # read under one hold: a move to another computer cannot land between.
        async with held_workspace_row(workspace_id) as workspace:
            sandbox = await sandbox_if_running(workspace_id, user_id)
            if sandbox is None:
                return []
            work_dir = owner_work_dir(workspace)
            # Asked here, after the delete committed and just before the rm:
            # a comment saved since may name one, and another naming it may
            # be deleted at the same time.
            unnamed = await artifacts_db.unnamed_pictures(workspace_id, list(pictures))
            addressed = [
                path
                for path in (contained_absolute_path(p, work_dir) for p in unnamed)
                if path is not None
            ]
            canonicals = await contained_sandbox_paths(
                sandbox, addressed, work_dir=work_dir
            )
            # The addressed path, not the canonical: unlinking a link's
            # target would delete the file behind it.
            targets = sorted(
                path for path, canonical in zip(addressed, canonicals) if canonical
            )
            if not targets:
                return []
            async with AsyncExitStack() as locks:
                # Sorted, as every batch of path locks is taken.
                for path in targets:
                    await locks.enter_async_context(sandbox.path_write_lock(path))
                result = await sandbox.execute_bash_command(
                    "rm -f -- " + " ".join(shlex.quote(p) for p in targets)
                )
        if not result.get("success"):
            logger.warning(
                "Could not remove %d comment pictures from workspace %s: %s",
                len(targets),
                workspace_id,
                single_line(str(result.get("stderr") or ""))[:200],
            )
            return []
        return targets
    except Exception as e:  # noqa: BLE001 - the delete stands either way
        logger.warning(
            "Could not remove comment pictures from workspace %s: %s",
            workspace_id,
            single_line(str(e))[:200],
        )
        return []
