"""Which workspace folder the running turn belongs to.

One ``PTCSandbox`` serves concurrent turns for different workspaces on the
same computer, so the turn's folder cannot be an attribute of the sandbox:
two turns would overwrite each other's. It rides a ``ContextVar`` instead:
execution context, never truth, and an asyncio task inherits it at creation
so a turn's whole task tree reads its own project.

The set has to happen outside the graph. A node runs in a task created from
the *caller's* context, so a ``ContextVar`` set inside one node is discarded
before the next one starts; ``run_with_project`` wraps the turn's stream,
which is where the run's own task begins.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import Any


__all__ = [
    "ROOT_CLAIM",
    "MachinePaths",
    "ProjectContext",
    "context_without_project",
    "current_project",
    "require_project",
    "run_with_project",
    "set_project",
]


#: Ledger key for a computer whose workspace has no folder of its own yet.
ROOT_CLAIM = "_root"


@dataclass(frozen=True, slots=True)
class MachinePaths:
    """How a call made from another computer names this one: by the machine's
    own paths, so ``/x`` is ``/x``, not the folder's."""

    #: The home of the user commands run as, where file calls run as that
    #: user too: ``~`` names it, and the file tools reach the whole machine,
    #: as a command there does, rather than only the root and ``/tmp``.
    home: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectContext:
    """The workspace a turn is running for, and its folder on the computer."""

    workspace_id: str
    dir_name: str | None
    sibling_dir_names: tuple[str, ...] = field(default=())
    #: Layout version the files predate the folder layout under (3), or None.
    layout_origin: int | None = None
    #: Folders a rename moved this workspace out of that no sibling holds now,
    #: so an old spelling of a path still names this folder.
    previous_dir_names: tuple[str, ...] = field(default=())
    #: Set for a call made from another computer, which knows this one only
    #: by the machine's own paths; None for a turn's own virtual spelling.
    machine: MachinePaths | None = None

    @property
    def claim(self) -> str:
        """This workspace's key in the computer's shared tool ledger.

        A computer that has not been split yet has one workspace with no folder
        and no id, and it still needs a key of its own so its claim is not
        mistaken for a sibling's.
        """
        return self.workspace_id or ROOT_CLAIM


_current: contextvars.ContextVar[ProjectContext | None] = contextvars.ContextVar(
    "ptc_project", default=None
)


def set_project(ctx: ProjectContext) -> contextvars.Token:
    """Bind the project for this task and its children."""
    return _current.set(ctx)


def current_project() -> ProjectContext | None:
    """The running turn's project, or None outside a turn."""
    return _current.get()


def require_project() -> ProjectContext:
    """The running turn's project; raises when there is none.

    For callers that cannot proceed without a folder. Anything on a request
    path reads ``current_project`` and falls back instead, because a missing
    project there is a computer that has not been split yet, not a bug.
    """
    ctx = _current.get()
    if ctx is None:
        raise LookupError("no project bound to this context")
    return ctx


def context_without_project() -> contextvars.Context:
    """A copy of the running context with no project bound, to start a task
    in that does machine work for no folder: waking another computer from
    inside a turn, whose setup would lay the turn's folder out on it."""
    ctx = contextvars.copy_context()
    ctx.run(_current.set, None)
    return ctx


@contextlib.contextmanager
def project_bound(project: ProjectContext | None) -> Iterator[None]:
    """``project`` bound for the block, for work on a turn's folder outside
    a turn's stream; None binds nothing, as ``run_with_project`` does."""
    if project is None:
        yield
        return
    token = set_project(project)
    try:
        yield
    finally:
        _current.reset(token)


async def run_with_project(
    project: ProjectContext | None, stream: AsyncIterator[Any]
) -> AsyncIterator[Any]:
    """Drive a turn's stream with its project bound.

    ``project=None`` passes the stream through untouched, which is every
    caller with no workspace folder of its own (the OSS shape).
    """
    if project is None:
        async with contextlib.aclosing(stream):
            async for event in stream:
                yield event
        return
    token = set_project(project)
    try:
        # Closed here, so closing this stream closes the turn's within the
        # same await rather than whenever the garbage collector reaches it.
        async with contextlib.aclosing(stream):
            async for event in stream:
                yield event
    finally:
        # A generator finalized from a task other than the one that started
        # it cannot reset the token, and that is not worth failing a turn for.
        with contextlib.suppress(ValueError):
            _current.reset(token)
