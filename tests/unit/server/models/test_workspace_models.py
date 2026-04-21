"""Tests for workspace Pydantic models.

Validates construction, field constraints, enum values, and defaults for all
workspace request/response models in src/server/models/workspace.py.
"""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.server.models.workspace import (
    WorkspaceActionResponse,
    WorkspaceCreate,
    WorkspaceListResponse,
    WorkspaceReorderItem,
    WorkspaceReorderRequest,
    WorkspaceResponse,
    WorkspaceStatus,
    WorkspaceUpdate,
)


# ---------------------------------------------------------------------------
# WorkspaceStatus enum
# ---------------------------------------------------------------------------


class TestWorkspaceStatus:
    """Verify enum members and string compatibility."""

    def test_all_expected_values(self):
        expected = {
            "creating",
            "starting",
            "running",
            "stopping",
            "stopped",
            "error",
            "deleted",
            "flash",
        }
        actual = {s.value for s in WorkspaceStatus}
        assert actual == expected

    def test_str_enum_comparison(self):
        assert WorkspaceStatus.RUNNING == "running"
        assert WorkspaceStatus.ERROR == "error"


# ---------------------------------------------------------------------------
# WorkspaceCreate
# ---------------------------------------------------------------------------


class TestWorkspaceCreate:
    """WorkspaceCreate construction and validation."""

    def test_valid_minimal(self):
        ws = WorkspaceCreate(name="My Workspace")
        assert ws.name == "My Workspace"
        assert ws.description is None
        assert ws.config is None

    def test_valid_full(self):
        ws = WorkspaceCreate(
            name="Analysis",
            description="Portfolio research workspace",
            config={"theme": "dark"},
        )
        assert ws.description == "Portfolio research workspace"
        assert ws.config == {"theme": "dark"}

    def test_name_too_short(self):
        with pytest.raises(ValidationError) as exc_info:
            WorkspaceCreate(name="")
        errors = exc_info.value.errors()
        assert any(e["type"] == "string_too_short" for e in errors)

    def test_name_too_long(self):
        with pytest.raises(ValidationError):
            WorkspaceCreate(name="x" * 256)

    def test_description_too_long(self):
        with pytest.raises(ValidationError):
            WorkspaceCreate(name="ok", description="d" * 1001)

    def test_name_required(self):
        with pytest.raises(ValidationError) as exc_info:
            WorkspaceCreate()
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("name",) for e in errors)


# ---------------------------------------------------------------------------
# WorkspaceUpdate
# ---------------------------------------------------------------------------


class TestWorkspaceUpdate:
    """WorkspaceUpdate allows partial updates (all fields optional)."""

    def test_empty_update(self):
        wu = WorkspaceUpdate()
        assert wu.name is None
        assert wu.description is None
        assert wu.config is None
        assert wu.is_pinned is None

    def test_partial_update(self):
        wu = WorkspaceUpdate(name="Renamed", is_pinned=True)
        assert wu.name == "Renamed"
        assert wu.is_pinned is True
        assert wu.description is None

    def test_name_min_length_constraint(self):
        with pytest.raises(ValidationError):
            WorkspaceUpdate(name="")


# ---------------------------------------------------------------------------
# WorkspaceResponse
# ---------------------------------------------------------------------------


class TestWorkspaceResponse:
    """WorkspaceResponse construction with defaults."""

    def test_valid_construction(self):
        now = datetime.now(timezone.utc)
        ws = WorkspaceResponse(
            workspace_id="ws-123",
            user_id="user-456",
            name="Test",
            status="running",
            created_at=now,
            updated_at=now,
        )
        assert ws.workspace_id == "ws-123"
        assert ws.is_pinned is False
        assert ws.sort_order == 0
        assert ws.sandbox_id is None
        assert ws.last_activity_at is None
        assert ws.stopped_at is None

    def test_from_attributes_enabled(self):
        assert WorkspaceResponse.model_config.get("from_attributes") is True


# ---------------------------------------------------------------------------
# WorkspaceReorderItem / WorkspaceReorderRequest
# ---------------------------------------------------------------------------


class TestWorkspaceReorder:
    """Reorder models validate UUID and sort_order constraints."""

    def test_valid_reorder_item(self):
        uid = uuid.uuid4()
        item = WorkspaceReorderItem(workspace_id=uid, sort_order=5)
        assert item.workspace_id == uid
        assert item.sort_order == 5

    def test_negative_sort_order(self):
        with pytest.raises(ValidationError):
            WorkspaceReorderItem(workspace_id=uuid.uuid4(), sort_order=-1)

    def test_reorder_request_non_empty(self):
        uid = uuid.uuid4()
        req = WorkspaceReorderRequest(
            items=[WorkspaceReorderItem(workspace_id=uid, sort_order=0)]
        )
        assert len(req.items) == 1

    def test_reorder_request_empty_list(self):
        with pytest.raises(ValidationError):
            WorkspaceReorderRequest(items=[])


# ---------------------------------------------------------------------------
# WorkspaceListResponse
# ---------------------------------------------------------------------------


class TestWorkspaceListResponse:
    """Paginated list response defaults."""

    def test_defaults(self):
        resp = WorkspaceListResponse(limit=20, offset=0)
        assert resp.workspaces == []
        assert resp.total == 0

    def test_with_workspaces(self):
        now = datetime.now(timezone.utc)
        ws = WorkspaceResponse(
            workspace_id="ws-1",
            user_id="u-1",
            name="W",
            status="running",
            created_at=now,
            updated_at=now,
        )
        resp = WorkspaceListResponse(workspaces=[ws], total=1, limit=10, offset=0)
        assert resp.total == 1
        assert len(resp.workspaces) == 1


# ---------------------------------------------------------------------------
# WorkspaceActionResponse
# ---------------------------------------------------------------------------


class TestWorkspaceActionResponse:
    """Action response model."""

    def test_valid_construction(self):
        resp = WorkspaceActionResponse(
            workspace_id="ws-abc",
            status="running",
            message="Workspace started",
        )
        assert resp.status == "running"
        assert resp.message == "Workspace started"
