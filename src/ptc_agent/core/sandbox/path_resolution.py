"""Resolving an agent-written path against the turn's workspace.

One question, four things that used to answer it: this module is the only place
that turns what the agent wrote into an absolute sandbox path and decides
whether that path is allowed. The fold itself is pure and lives in
``core.paths``; what is here is the part that needs the live sandbox -- its
root, its allow list, and the turn's workspace folder.
"""

from __future__ import annotations

import structlog

from ptc_agent.core.paths import (
    lexical_path,
    resolve_agent_path,
    virtual_agent_path,
)
from ptc_agent.core.project_context import ProjectContext, current_project

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


def _ambient_project(project: ProjectContext | None) -> ProjectContext | None:
    """The project a caller named, else the turn's own."""
    return project if project is not None else current_project()


def _project_base(sandbox: "PTCSandbox", project: ProjectContext | None) -> str:
    """The directory a relative or virtual agent path hangs off.

    The turn's workspace folder when there is one, else the computer root,
    which is what every caller outside a turn (restore, backup, the
    consolidation job) sees and what a computer looks like before it is split.
    """
    return sandbox.workspace(project).workspace


def normalize_path(
    sandbox: "PTCSandbox", path: str, project: ProjectContext | None = None
) -> str:
    """Normalize virtual path to absolute sandbox path (input normalization).

    ``project`` is for a caller running outside the turn's own task, where the
    ambient one is not bound yet; everything inside a turn omits it. The fold
    itself is ``paths.resolve_agent_path``, shared with the pinned backend.
    """
    ctx = _ambient_project(project)
    return resolve_agent_path(
        path,
        workspace=_project_base(sandbox, project),
        root=sandbox._work_dir,
        allowed=sandbox.config.filesystem.allowed_directories,
        sibling_dir_names=ctx.sibling_dir_names if ctx is not None else (),
    )


def virtualize_path(
    sandbox: "PTCSandbox", path: str, project: ProjectContext | None = None
) -> str:
    """Convert real sandbox path to the virtual spelling the agent sees."""
    return virtual_agent_path(
        path, workspace=_project_base(sandbox, project), root=sandbox._work_dir
    )


def _denied_directories(
    sandbox: "PTCSandbox", project: ProjectContext | None
) -> list[str]:
    """The turn's deny list: the computer's own, plus its siblings' tools.

    ``FilesystemConfig`` carries only what is true for every turn on the
    computer. The sibling half is per turn because the sandbox is shared, so
    it is folded in here rather than written onto the config.
    """
    configured = list(sandbox.config.filesystem.denied_directories)
    ctx = _ambient_project(project)
    if ctx is None or not ctx.sibling_dir_names:
        return configured
    for denied in sandbox.workspace(ctx).denied_directories(
        sandbox.layout, ctx.sibling_dir_names
    ):
        if denied not in configured:
            configured.append(denied)
    return configured


def validate_path(
    sandbox: "PTCSandbox", filepath: str, project: ProjectContext | None = None
) -> bool:
    """Whether a path is inside the directories this turn may touch.

    Pass the path as the caller wrote it. Handing back a path this function
    already normalized is a different question: an absolute path outside the
    allowed roots is a virtual path by ``normalize_path``'s rule, so a second
    pass folds an escape back inside and answers True.
    """
    if not sandbox.config.filesystem.enable_path_validation:
        return True

    # Normalize the path first (handles virtual paths like /work/task/...)
    normalized_path = sandbox.normalize_path(filepath, project)

    # Denylist takes priority over allowlist
    for denied_dir in _denied_directories(sandbox, project):
        if normalized_path == denied_dir or normalized_path.startswith(
            denied_dir + "/"
        ):
            return False

    # Check against allowed directories
    for allowed_dir in sandbox.config.filesystem.allowed_directories:
        # Exact match or path within allowed directory
        if normalized_path == allowed_dir or normalized_path.startswith(
            allowed_dir + "/"
        ):
            return True

    logger.warning(
        "Path validation failed",
        path=filepath,
        normalized_path=normalized_path,
        allowed_dirs=sandbox.config.filesystem.allowed_directories,
    )
    return False


def validate_and_normalize_path(
    sandbox: "PTCSandbox", path: str, project: ProjectContext | None = None
) -> tuple[str, str | None]:
    """Normalize a path and say whether the turn may touch it.

    Both halves read the caller's spelling, so the answer is about the path
    the caller wrote rather than about a re-reading of the result.
    """
    normalized = sandbox.normalize_path(path, project)
    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        path, project
    ):
        return normalized, f"Access denied: {path} is not in allowed directories"
    return normalized, None


def _normalize_search_path(sandbox: "PTCSandbox", path: str) -> str:
    """Resolve a search path to an absolute one, against the turn's folder.

    The base is the bound workspace, so an unqualified pattern stays inside
    it; with no turn bound the base is the computer root, which is the whole
    machine a host-side inspection means to search.
    """
    base = _project_base(sandbox, None)
    if path == ".":
        return base
    if not path.startswith("/"):
        return lexical_path(f"{base}/{path}")
    return lexical_path(path)


def _validate_path_allow_denied(sandbox: "PTCSandbox", path: str) -> bool:
    """Allowlist-only validation, for a user-initiated inspection.

    The agent-infrastructure directories stay out of the agent's own globs,
    but a person who asks for one by name still gets it.
    """

    normalized_path = sandbox._normalize_search_path(path)
    for allowed_dir in sandbox.config.filesystem.allowed_directories:
        if normalized_path == allowed_dir or normalized_path.startswith(
            allowed_dir + "/"
        ):
            return True
    return False
