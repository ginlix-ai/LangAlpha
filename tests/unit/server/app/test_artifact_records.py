"""The workspace record routes answer 404 for a workspace that is not the
caller's, as every artifact route does, never a 403 that confirms it exists."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.app.artifacts import workspace_router
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

_DBWS = "src.server.services.workspace_files.db_get_workspace"
_WS = "00000000-0000-0000-0000-00000000000f"


@pytest_asyncio.fixture
async def stranger():
    app = create_test_app(workspace_router)
    app.dependency_overrides[get_current_user_id] = lambda: "someone-else"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [{"workspace_id": _WS, "user_id": "owner"}, None])
async def test_a_workspace_not_the_callers_is_not_found(stranger, workspace):
    with patch(_DBWS, AsyncMock(return_value=workspace)):
        found = await stranger.get(
            f"/api/v1/workspaces/{_WS}/artifacts", params={"path": "a.html"}
        )
        made = await stranger.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"path": "a.html"}
        )
    assert (found.status_code, made.status_code) == (404, 404)


@pytest.mark.asyncio
async def test_a_deleted_workspace_is_not_found(stranger):
    deleted = {"workspace_id": _WS, "user_id": "someone-else", "status": "deleted"}
    with patch(_DBWS, AsyncMock(return_value=deleted)):
        resp = await stranger.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"path": "a.html"}
        )
    assert resp.status_code == 404
