"""Integration tests for workspace CRUD operations against real PostgreSQL."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestCreateWorkspace:
    """Test workspace creation."""

    async def test_create_workspace_auto_id(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import create_workspace

        ws = await create_workspace(
            user_id=seed_user["user_id"],
            name="Research",
            description="Financial research workspace",
        )

        assert ws["name"] == "Research"
        assert ws["description"] == "Financial research workspace"
        assert ws["user_id"] == seed_user["user_id"]
        assert ws["status"] == "creating"
        assert ws["workspace_id"] is not None
        assert ws["created_at"] is not None
        assert ws["is_pinned"] is False
        assert ws["sort_order"] == 0

    async def test_create_workspace_explicit_id(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import create_workspace

        explicit_id = str(uuid.uuid4())
        ws = await create_workspace(
            user_id=seed_user["user_id"],
            name="Custom ID",
            workspace_id=explicit_id,
            status="running",
        )

        assert str(ws["workspace_id"]) == explicit_id
        assert ws["status"] == "running"

    async def test_create_workspace_with_config(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import create_workspace

        config = {"model": "claude-sonnet", "tools": ["web_search"]}
        ws = await create_workspace(
            user_id=seed_user["user_id"],
            name="Configured",
            config=config,
        )

        assert ws["config"] == config


class TestGetWorkspace:
    """Test workspace retrieval."""

    async def test_get_workspace_found(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import get_workspace

        ws = await get_workspace(str(seed_workspace["workspace_id"]))

        assert ws is not None
        assert ws["name"] == seed_workspace["name"]
        assert ws["user_id"] == seed_workspace["user_id"]

    async def test_get_workspace_not_found(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import get_workspace

        result = await get_workspace(str(uuid.uuid4()))
        assert result is None

    async def test_get_workspace_excludes_deleted(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import delete_workspace, get_workspace

        ws_id = str(seed_workspace["workspace_id"])
        await delete_workspace(ws_id)  # soft delete

        result = await get_workspace(ws_id)
        assert result is None


class TestUpdateWorkspace:
    """Test workspace updates."""

    async def test_update_name_and_description(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import update_workspace

        ws_id = str(seed_workspace["workspace_id"])
        updated = await update_workspace(
            ws_id, name="Renamed", description="New desc"
        )

        assert updated is not None
        assert updated["name"] == "Renamed"
        assert updated["description"] == "New desc"

    async def test_update_pin_status(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import update_workspace

        ws_id = str(seed_workspace["workspace_id"])
        updated = await update_workspace(ws_id, is_pinned=True)

        assert updated is not None
        assert updated["is_pinned"] is True

    async def test_update_workspace_status(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import update_workspace_status

        ws_id = str(seed_workspace["workspace_id"])
        updated = await update_workspace_status(ws_id, "stopped")

        assert updated is not None
        assert updated["status"] == "stopped"
        assert updated["stopped_at"] is not None

    async def test_update_workspace_status_with_sandbox_id(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import update_workspace_status

        ws_id = str(seed_workspace["workspace_id"])
        updated = await update_workspace_status(
            ws_id, "running", sandbox_id="sandbox-abc-123"
        )

        assert updated is not None
        assert updated["status"] == "running"
        assert updated["sandbox_id"] == "sandbox-abc-123"

    async def test_update_workspace_activity(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import update_workspace_activity

        ws_id = str(seed_workspace["workspace_id"])
        updated = await update_workspace_activity(ws_id)

        # update_workspace_activity returns bool (True if row was updated)
        assert updated is True


class TestDeleteWorkspace:
    """Test workspace deletion."""

    async def test_soft_delete(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import delete_workspace, get_workspace

        ws_id = str(seed_workspace["workspace_id"])
        deleted = await delete_workspace(ws_id)
        assert deleted is True

        # Should not be found via normal get (filters out deleted)
        result = await get_workspace(ws_id)
        assert result is None

    async def test_hard_delete(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.workspace import delete_workspace

        ws_id = str(seed_workspace["workspace_id"])
        deleted = await delete_workspace(ws_id, hard_delete=True)
        assert deleted is True

    async def test_delete_nonexistent(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import delete_workspace

        deleted = await delete_workspace(str(uuid.uuid4()))
        assert deleted is False


class TestListWorkspaces:
    """Test workspace listing and pagination."""

    async def test_get_workspaces_for_user(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import (
            create_workspace,
            get_workspaces_for_user,
        )

        # Create several workspaces
        for i in range(3):
            await create_workspace(
                user_id=seed_user["user_id"],
                name=f"Workspace {i}",
                status="running",
            )

        workspaces, total = await get_workspaces_for_user(seed_user["user_id"])
        assert total == 3
        assert len(workspaces) == 3

    async def test_pagination(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import (
            create_workspace,
            get_workspaces_for_user,
        )

        for i in range(5):
            await create_workspace(
                user_id=seed_user["user_id"],
                name=f"Workspace {i}",
                status="running",
            )

        workspaces, total = await get_workspaces_for_user(
            seed_user["user_id"], limit=2, offset=0
        )
        assert total == 5
        assert len(workspaces) == 2

    async def test_excludes_flash_workspaces(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import (
            create_workspace,
            get_workspaces_for_user,
        )

        await create_workspace(
            user_id=seed_user["user_id"],
            name="Normal",
            status="running",
        )
        await create_workspace(
            user_id=seed_user["user_id"],
            name="Flash",
            status="flash",
        )

        workspaces, total = await get_workspaces_for_user(seed_user["user_id"])
        assert total == 1
        assert workspaces[0]["name"] == "Normal"


class TestBatchSortOrder:
    """Test batch sort order updates."""

    async def test_batch_update_sort_order(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import (
            batch_update_sort_order,
            create_workspace,
            get_workspace,
        )

        ws1 = await create_workspace(
            user_id=seed_user["user_id"], name="WS1", status="running"
        )
        ws2 = await create_workspace(
            user_id=seed_user["user_id"], name="WS2", status="running"
        )

        items = [
            (str(ws1["workspace_id"]), 10),
            (str(ws2["workspace_id"]), 20),
        ]
        await batch_update_sort_order(seed_user["user_id"], items)

        updated_ws1 = await get_workspace(str(ws1["workspace_id"]))
        updated_ws2 = await get_workspace(str(ws2["workspace_id"]))
        assert updated_ws1["sort_order"] == 10
        assert updated_ws2["sort_order"] == 20
