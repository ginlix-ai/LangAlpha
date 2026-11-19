"""Which per-thread folders of a shared workspace a person sees through the
file routes.

Every thread keeps folders in its workspace's folder named by the first eight
characters of its id (``THREAD_FOLDER_ROOTS``), which hold its turn messages,
notes, logs and outputs. In a shared workspace a file route answers a path in
one as it answers a missing file unless the caller reaches every thread whose
id starts that way; a folder no thread claims is missing to everyone there.
The answer depends only on the threads, never on what is on disk, so it says
nothing about whether a private thread's folder exists. A personal workspace
reads nothing here.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from ptc_agent.core.paths import WorkspaceLayout
from src.server.database.conversation import thread_reach
from src.server.database.shared_workspaces import is_holder

THREAD_FOLDER_ROOTS: tuple[str, ...] = (
    WorkspaceLayout.THREADS_DIR,
    WorkspaceLayout.SCRATCHPAD_DIR,
    WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
)
# Folded, since a machine with a case-insensitive disk opens ``.Agents/Threads``
# as the same folder.
_FOLDED_ROOTS = tuple(f"{root.lower()}/" for root in THREAD_FOLDER_ROOTS)
_THREAD_PREFIX = re.compile(r"[0-9a-f]{8}")


def thread_folder_name(client_path: str) -> str | None:
    """The per-thread folder a workspace-relative path is in or names, folded
    to lower case; None for a path outside those roots."""
    if not client_path:
        return None
    folded = posixpath.normpath(f"/{client_path}").lstrip("/").lower()
    for root in _FOLDED_ROOTS:
        if folded.startswith(root):
            return folded[len(root) :].split("/", 1)[0]
    return None


@dataclass(frozen=True)
class ThreadFolderGate:
    """One request's view of the per-thread folders, built from the workspace
    row the route already checked the caller against."""

    workspace_id: str
    caller: str
    shared: bool

    @classmethod
    def of(cls, workspace: Mapping[str, Any], caller: str) -> ThreadFolderGate:
        return cls(
            str(workspace["workspace_id"]), caller, is_holder(workspace.get("user_id"))
        )

    async def hidden(self, paths: Iterable[str]) -> set[str]:
        """Those of ``paths`` (workspace-relative, as asked or as resolved)
        that are missing to the caller. The one decision every file route
        takes, in one query for all of them."""
        if not self.shared:
            return set()
        folders = {p: n for p in paths if (n := thread_folder_name(p)) is not None}
        if not folders:
            return set()
        names = {n for n in folders.values() if _THREAD_PREFIX.fullmatch(n)}
        unreached = await thread_reach.unreached_folders(
            self.workspace_id, self.caller, names
        )
        return {p for p, n in folders.items() if n not in names or n in unreached}

    async def refuse(self, *paths: str | None, detail: str = "File not found") -> None:
        """The route's own 404 when any of ``paths`` is missing to the caller."""
        if await self.hidden(p for p in paths if p):
            raise HTTPException(status_code=404, detail=detail)

    async def keep(self, paths: list[str]) -> list[str]:
        """``paths`` without those missing to the caller, in order."""
        hidden = await self.hidden(paths)
        return [p for p in paths if p not in hidden] if hidden else paths
