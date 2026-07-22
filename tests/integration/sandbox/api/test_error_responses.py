"""Cross-cutting API error response tests.

Each test class creates its own httpx client with a custom workspace mock
(different status, different user, or None) to exercise error paths in the
workspace files and sandbox routers.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

from .conftest import TEST_WS_ID, TEST_USER_ID, _make_workspace

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

FILES_BASE = f"/api/v1/workspaces/{TEST_WS_ID}/files"


# ---------------------------------------------------------------------------
# TestFlashWorkspaceHandling
# ---------------------------------------------------------------------------


class TestFlashWorkspaceHandling:
    """Flash workspaces return 200 empty for list, 400 for everything else."""

    @pytest_asyncio.fixture
    async def flash_client(self):
        from src.server.app.workspace_files import router

        app = create_test_app(router)
        with patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=_make_workspace(status="flash")),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client

    async def test_list_files_flash_returns_200_empty(self, flash_client):
        resp = await flash_client.get(FILES_BASE)
        assert resp.status_code == 200
        data = resp.json()
        assert data["files"] == []
        assert data.get("flash_workspace") is True

    async def test_read_file_flash_returns_400(self, flash_client):
        resp = await flash_client.get(
            f"{FILES_BASE}/read", params={"path": "data/test.txt"}
        )
        assert resp.status_code == 400

    async def test_write_file_flash_returns_400(self, flash_client):
        resp = await flash_client.put(
            f"{FILES_BASE}/write",
            params={"path": "data/test.txt"},
            json={"content": "hello"},
        )
        assert resp.status_code == 400

    async def test_download_flash_returns_400(self, flash_client):
        resp = await flash_client.get(
            f"{FILES_BASE}/download", params={"path": "data/test.txt"}
        )
        assert resp.status_code == 400

    async def test_upload_flash_returns_400(self, flash_client):
        resp = await flash_client.post(
            f"{FILES_BASE}/upload",
            params={"path": "data/test.txt"},
            files={"file": ("test.txt", b"hello", "text/plain")},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# TestStoppedWorkspaceWriteRejection
# ---------------------------------------------------------------------------


class TestStoppedWorkspaceWriteRejection:
    """Stopped workspaces reject write/upload/delete with 409."""

    @pytest_asyncio.fixture
    async def stopped_client(self):
        from src.server.app.workspace_files import router

        app = create_test_app(router)
        with patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=_make_workspace(status="stopped")),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client

    async def test_write_stopped_returns_409(self, stopped_client):
        resp = await stopped_client.put(
            f"{FILES_BASE}/write",
            params={"path": "data/test.txt"},
            json={"content": "hello"},
        )
        assert resp.status_code == 409

    async def test_upload_stopped_returns_409(self, stopped_client):
        resp = await stopped_client.post(
            f"{FILES_BASE}/upload",
            params={"path": "data/test.txt"},
            files={"file": ("test.txt", b"hello", "text/plain")},
        )
        assert resp.status_code == 409

    async def test_delete_stopped_returns_409(self, stopped_client):
        resp = await stopped_client.request(
            "DELETE",
            FILES_BASE,
            json={"paths": ["data/test.txt"]},
        )
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# TestWorkspaceNotFound
# ---------------------------------------------------------------------------


class TestWorkspaceNotFound:
    """Non-existent workspace returns 404."""

    @pytest_asyncio.fixture
    async def not_found_client(self):
        from src.server.app.workspace_files import router

        app = create_test_app(router)
        with patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=None),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client

    async def test_list_files_not_found_returns_404(self, not_found_client):
        resp = await not_found_client.get(FILES_BASE)
        assert resp.status_code == 404

    async def test_read_file_not_found_returns_404(self, not_found_client):
        resp = await not_found_client.get(
            f"{FILES_BASE}/read", params={"path": "data/test.txt"}
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestOwnershipEnforcement
# ---------------------------------------------------------------------------


class TestOwnershipEnforcement:
    """Workspace owned by a different user returns 403."""

    @pytest_asyncio.fixture
    async def wrong_user_client(self):
        from src.server.app.workspace_files import router

        app = create_test_app(router)
        with patch(
            "src.server.app.workspace_files.crud.db_get_workspace",
            AsyncMock(return_value=_make_workspace(user_id="other-user-999")),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client

    async def test_list_files_wrong_user_returns_403(self, wrong_user_client):
        resp = await wrong_user_client.get(FILES_BASE)
        assert resp.status_code == 403
