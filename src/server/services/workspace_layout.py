"""Resolve the folder a project owns on its computer, or refuse to guess.

The computer root is not a fallback. A row that names no folder is a row whose
placement nobody has established yet, so a read that cannot answer has to
raise: on the backup path the root makes one project's scan claim its
siblings' files and prune its own manifest, and on the serving path it makes a
scoped share token reach a sibling's report. A caller that genuinely addresses
the machine and no project asks for the root by name (``for_workspace(None)``),
which is why nothing here hands it out as a default.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from ptc_agent.core.project_context import ProjectContext

from src.server.database.workspace import get_workspace_dir_name
from src.server.database.workspace_folders import (
    WorkspaceFolderMoving,
    is_top_level,
    workspace_folder_in_use,
)


class WorkspaceLayoutUnavailable(RuntimeError):
    """The project's folder on its computer could not be established."""


@dataclass(frozen=True, slots=True)
class ProjectPlacement:
    """A project's own folder plus the folders parked beside it."""

    layout: WorkspaceLayout
    sibling_dir_names: tuple[str, ...] = ()
    #: The layout version the project's files were first written under, or
    #: None for a project born on the current one.
    layout_origin: int | None = None
    #: Folders a rename moved the workspace out of that no sibling holds now.
    previous_dir_names: tuple[str, ...] = ()

    @property
    def dir_name(self) -> str:
        return self.layout.dir_name


def layout_from_binding(
    workspace_id: str,
    binding: Optional[Mapping[str, Any]],
    *,
    root: str,
) -> WorkspaceLayout:
    """Turn a workspace row (narrow binding or full row) into its layout.

    No folder name raises whether or not the row names a computer. A row with
    neither is one migration 046 left on the pre-computer path, waiting for the
    adoption that assigns both; reading it as the root owner would be the same
    widening as reading a half-bound row that way, on a machine its siblings
    are already using.
    """
    if not binding:
        raise WorkspaceLayoutUnavailable(
            f"No workspace row for {workspace_id}, so it names no folder"
        )
    dir_name = str(binding.get("dir_name") or "").strip()
    if not dir_name:
        computer_id = binding.get("computer_id")
        where = f"computer {computer_id}" if computer_id else "no computer yet"
        raise WorkspaceLayoutUnavailable(
            f"Workspace {workspace_id} names no folder ({where})"
        )
    return SandboxLayout.for_root(root).for_workspace(dir_name)


def placement_from_binding(
    workspace_id: str,
    binding: Optional[Mapping[str, Any]],
    *,
    root: str,
) -> ProjectPlacement:
    """The layout plus its neighbours, dropping any former folder a sibling holds.

    The old spelling stopped naming this workspace when the sibling took the
    name, and reading it as this workspace's would fold the sibling's paths into
    this folder.
    """
    layout = layout_from_binding(workspace_id, binding, root=root)
    siblings = tuple(
        name
        for name in ((binding or {}).get("sibling_dir_names") or ())
        if name and name != layout.dir_name
    )
    held = {name.casefold() for name in siblings}
    previous = tuple(
        name
        for name in ((binding or {}).get("previous_dir_names") or ())
        if name and name != layout.dir_name and name.casefold() not in held
    )
    origin = (binding or {}).get("layout_origin")
    return ProjectPlacement(
        layout=layout,
        sibling_dir_names=siblings,
        layout_origin=int(origin) if origin is not None else None,
        previous_dir_names=previous,
    )


async def resolve_project_placement(
    workspace_id: str, *, root: str, conn=None
) -> ProjectPlacement:
    """Read the binding and resolve it, raising on a failed or unusable read."""
    from src.server.database.workspace_binding import get_project_binding

    try:
        binding = await get_project_binding(workspace_id, conn=conn)
    except Exception as e:
        raise WorkspaceLayoutUnavailable(
            f"Could not read the folder for workspace {workspace_id}: {e}"
        ) from e
    return placement_from_binding(workspace_id, binding, root=root)


async def resolve_project_context(session: Any, workspace_id: str) -> ProjectContext | None:
    """The workspace folder a turn on ``workspace_id`` runs in, for the
    sandbox path layer; None for a session with no sandbox.

    One read: the folder, and the folders parked beside it on the same machine
    so the agent can name a sibling directory as someone else's work rather
    than as material it is expected to read.

    Propagates ``WorkspaceLayoutUnavailable``, which fails the turn. The turn
    would otherwise run at the computer root, where it writes into a shared
    directory and mirrors every sibling's files back as its own.
    """
    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        return None
    placement = await resolve_project_placement(workspace_id, root=sandbox.working_dir)
    return ProjectContext(
        workspace_id=workspace_id,
        dir_name=placement.dir_name,
        sibling_dir_names=placement.sibling_dir_names,
        layout_origin=placement.layout_origin,
        previous_dir_names=placement.previous_dir_names,
    )


async def resolve_workspace_layout(
    workspace_id: str, *, root: str, conn=None
) -> WorkspaceLayout:
    placement = await resolve_project_placement(workspace_id, root=root, conn=conn)
    return placement.layout


@asynccontextmanager
async def held_workspace_layout(
    workspace_id: str, root: str
) -> AsyncIterator[Optional[WorkspaceLayout]]:
    """The workspace's folder, read under the folder hold and kept in place until exit.

    A settle renames a folder whenever no hold covers it, so a folder read
    earlier (at turn start, when a job was queued) may name one a sibling now
    holds. Raises ``WorkspaceFolderMoving`` while a settle holds the folder or
    left it staged: a file written to the staged path makes a folder the next
    settle cannot tell from the one being moved. None when the row names no
    folder at all.
    """
    async with workspace_folder_in_use(workspace_id):
        try:
            dir_name = await get_workspace_dir_name(workspace_id)
        except Exception as e:
            raise WorkspaceLayoutUnavailable(
                f"Could not read the folder for workspace {workspace_id}: {e}"
            ) from e
        if dir_name and not is_top_level(dir_name):
            raise WorkspaceFolderMoving(workspace_id)
        yield SandboxLayout.for_root(root).for_workspace(dir_name) if dir_name else None
