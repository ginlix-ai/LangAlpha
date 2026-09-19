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

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout


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
    layout = layout_from_binding(workspace_id, binding, root=root)
    siblings = tuple(
        name
        for name in ((binding or {}).get("sibling_dir_names") or ())
        if name and name != layout.dir_name
    )
    origin = (binding or {}).get("layout_origin")
    return ProjectPlacement(
        layout=layout,
        sibling_dir_names=siblings,
        layout_origin=int(origin) if origin is not None else None,
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


async def resolve_workspace_layout(
    workspace_id: str, *, root: str, conn=None
) -> WorkspaceLayout:
    placement = await resolve_project_placement(workspace_id, root=root, conn=conn)
    return placement.layout
