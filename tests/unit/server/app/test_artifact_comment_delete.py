"""Deleting a comment hands the pictures nothing else names to its
computer, after the row is gone, and answers without waiting for them."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.app.artifacts import comments, router
from src.server.database.artifacts import DeletedComment
from tests.conftest import create_test_app

_A = "00000000-0000-0000-0000-00000000000a"
_C = "00000000-0000-0000-0000-00000000000c"
_W = "00000000-0000-0000-0000-00000000000f"


@pytest_asyncio.fixture
async def client():
    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


def _reached():
    artifact = MagicMock(artifact_id=_A, workspace_id=_W, path="NVDA:1day", kind="chart")
    access = MagicMock(owner_id="owner")
    return AsyncMock(return_value=(artifact, access))


@pytest.mark.asyncio
async def test_a_deleted_comments_pictures_go_after_it(client):
    gone = DeletedComment(
        thread_id=None, workspace_id=_W, pictures=(".agents/comments/NVDA-1day.png",)
    )
    with (
        patch.object(comments, "reached", _reached()),
        patch.object(comments.artifacts_db, "delete_comment", AsyncMock(return_value=gone)),
        patch.object(comments, "remove_pictures_soon") as soon,
        patch.object(comments, "publish_comment_change", AsyncMock()),
    ):
        resp = await client.delete(f"/api/v1/artifacts/{_A}/comments/{_C}")
    assert resp.status_code == 204
    soon.assert_called_once_with(_W, "test-user-123", (".agents/comments/NVDA-1day.png",))


@pytest.mark.asyncio
async def test_a_comment_not_there_touches_no_picture(client):
    with (
        patch.object(comments, "reached", _reached()),
        patch.object(comments.artifacts_db, "delete_comment", AsyncMock(return_value=None)),
        patch.object(comments, "remove_pictures_soon") as soon,
    ):
        resp = await client.delete(f"/api/v1/artifacts/{_A}/comments/{_C}")
    assert resp.status_code == 404
    soon.assert_not_called()
