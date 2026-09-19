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
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any


__all__ = [
    "ROOT_CLAIM",
    "ProjectContext",
    "current_project",
    "require_project",
    "run_with_project",
    "set_project",
]


#: Ledger key for a computer whose workspace has no folder of its own yet.
ROOT_CLAIM = "_root"


@dataclass(frozen=True, slots=True)
class ProjectContext:
    """The workspace a turn is running for, and its folder on the computer."""

    workspace_id: str
    dir_name: str | None
    sibling_dir_names: tuple[str, ...] = field(default=())
    #: Layout version the files predate the folder layout under (3), or None.
    layout_origin: int | None = None

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


async def run_with_project(
    project: ProjectContext | None, stream: AsyncIterator[Any]
) -> AsyncIterator[Any]:
    """Drive a turn's stream with its project bound.

    ``project=None`` passes the stream through untouched, which is every
    caller with no workspace folder of its own (Flash, the OSS shape).
    """
    if project is None:
        async for event in stream:
            yield event
        return
    token = set_project(project)
    try:
        async for event in stream:
            yield event
    finally:
        # A generator finalized from a task other than the one that started
        # it cannot reset the token, and that is not worth failing a turn for.
        with contextlib.suppress(ValueError):
            _current.reset(token)
