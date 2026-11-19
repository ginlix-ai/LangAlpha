"""Stand-ins for the access checks, for route tests that call as the owner.

A route converted to ``thread_access`` / ``workspace_access`` no longer calls
the owner-only helpers, and one still on them does; ``as_owner`` passes the
caller through both, so a test reads the same whichever its route uses.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from unittest.mock import AsyncMock, patch

from src.server.services.shared_workspaces.access import Access


def owner_access(user_id: str = "user-1") -> AsyncMock:
    return AsyncMock(return_value=Access(user_id))


@contextmanager
def as_owner(module: str, *, user_id: str = "user-1") -> Iterator[AsyncMock]:
    """The caller owns the thread: ``require_thread_owner`` passes (yielded,
    for a test that asserts it ran) and ``module.thread_access`` answers
    the owner's access."""
    with (
        patch("src.server.utils.api.require_thread_owner", new=AsyncMock()) as owner,
        patch(f"{module}.thread_access", new=owner_access(user_id)),
    ):
        yield owner


@contextmanager
def owned_thread(owner_id: str, workspace_id: str = "ws-1") -> Iterator[AsyncMock]:
    """The thread's owner as both checks read it: ``get_thread_owner_id``
    (yielded) for ``require_thread_owner`` and ``get_thread_auth_meta`` for
    ``thread_access``."""
    with (
        patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=owner_id),
        ) as owner,
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new=AsyncMock(return_value={"user_id": owner_id, "workspace_id": workspace_id}),
        ),
    ):
        yield owner
