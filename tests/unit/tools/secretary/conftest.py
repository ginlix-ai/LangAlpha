"""Fixtures and fakes shared by the secretary tool suite.

Dispatch picks a free workspace name before it creates one; these tests run
without a database, so every name is free. The fakes below, imported via
``from .conftest import ...``, stand in for the workspace manager and for the
background dispatch POST.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

NEW_WORKSPACE_ID = "33333333-3333-3333-3333-333333333333"


@pytest.fixture(autouse=True)
def _every_workspace_name_is_free():
    with patch(
        "src.server.database.workspace.get_workspace_name_keys",
        AsyncMock(return_value=set()),
    ):
        yield


@pytest.fixture(autouse=True)
def _no_committed_texts():
    """No test reaches the stored slices or the checkpoints: no turn has a
    committed text to read, so every run's comes from its stored events."""
    with patch(
        "src.server.services.history.committed.committed_texts",
        AsyncMock(return_value={}),
    ):
        yield


def workspace_manager(
    row: dict | None = None, delete: AsyncMock | None = None
) -> MagicMock:
    """A ``WorkspaceManager`` whose create returns ``row``, by default one
    holding only the new workspace's id."""
    mgr = MagicMock()
    mgr.create_workspace = AsyncMock(
        return_value=row if row is not None else {"workspace_id": NEW_WORKSPACE_ID}
    )
    mgr.delete_workspace = delete or AsyncMock(return_value=True)
    return mgr


class FakeResp:
    def __init__(self, status: int = 200, body: dict | None = None) -> None:
        self.status = status
        self._body = body if body is not None else {"status": "dispatched"}

    async def __aenter__(self) -> "FakeResp":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def json(self) -> dict:
        return self._body


class FakeSession:
    """An ``aiohttp.ClientSession`` whose post answers ``resp`` or raises
    ``post_exc``."""

    def __init__(
        self, resp: FakeResp | None = None, post_exc: Exception | None = None
    ) -> None:
        self._resp = resp
        self._post_exc = post_exc

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    def post(self, *_args, **_kwargs) -> FakeResp:
        if self._post_exc is not None:
            raise self._post_exc
        return self._resp
