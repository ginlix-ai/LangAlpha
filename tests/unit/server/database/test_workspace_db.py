"""
Tests for src/server/database/workspace.py

Verifies workspace CRUD operations: create, get, update, delete,
batch sort order, status updates, and flash workspace upsert.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixture: patch get_db_connection at the workspace module's import location
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cursor():
    """AsyncMock cursor with execute/fetchone/fetchall."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.rowcount = 0
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """AsyncMock connection that yields mock_cursor via cursor() context manager."""
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    conn.cursor = _cursor_cm
    conn.execute = AsyncMock()
    return conn


@pytest.fixture
def ws_mock_db(mock_connection):
    """Patch get_db_connection in the workspace module (its import location)."""

    @asynccontextmanager
    async def _fake():
        yield mock_connection

    with patch(
        "src.server.database.workspace.get_db_connection",
        new=_fake,
    ):
        yield mock_connection


# ---------------------------------------------------------------------------
# Helper: sample workspace row
# ---------------------------------------------------------------------------

def _workspace_row(
    workspace_id=None,
    user_id="user-1",
    name="My Workspace",
    status="running",
    **overrides,
):
    now = datetime.now(timezone.utc)
    row = {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": name,
        "description": None,
        "sandbox_id": "sandbox-abc",
        "status": status,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": None,
        "stopped_at": None,
        "config": {},
        "is_pinned": False,
        "sort_order": 0,
    }
    row.update(overrides)
    return row


# ===========================================================================
# Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_flash_workspace_id_deterministic():
    """Same user always produces the same flash workspace ID."""
    from src.server.database.workspace import get_flash_workspace_id

    id1 = get_flash_workspace_id("user-42")
    id2 = get_flash_workspace_id("user-42")
    assert id1 == id2

    id3 = get_flash_workspace_id("user-99")
    assert id3 != id1


@pytest.mark.asyncio
async def test_get_or_create_flash_workspace(ws_mock_db, mock_cursor):
    """get_or_create_flash_workspace executes upsert SQL and returns dict."""
    from src.server.database.workspace import get_or_create_flash_workspace

    row = _workspace_row(name="Flash", status="flash")
    mock_cursor.fetchone.return_value = row

    result = await get_or_create_flash_workspace("user-1")

    assert result["name"] == "Flash"
    assert result["status"] == "flash"
    mock_cursor.execute.assert_awaited_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO workspaces" in sql
    assert "ON CONFLICT" in sql


@pytest.mark.asyncio
async def test_create_workspace_auto_id(ws_mock_db, mock_cursor):
    """create_workspace without workspace_id uses auto-generated UUID SQL."""
    from src.server.database.workspace import create_workspace

    row = _workspace_row()
    mock_cursor.fetchone.return_value = row

    result = await create_workspace("user-1", "New WS", description="desc")

    assert result["name"] == "New WS" or result["name"] == row["name"]
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO workspaces" in sql
    # Auto-generated path does NOT include workspace_id in VALUES
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "user-1"  # user_id is first param


@pytest.mark.asyncio
async def test_create_workspace_with_explicit_id(ws_mock_db, mock_cursor):
    """create_workspace with workspace_id passes it in SQL params."""
    from src.server.database.workspace import create_workspace

    ws_id = str(uuid.uuid4())
    row = _workspace_row(workspace_id=ws_id)
    mock_cursor.fetchone.return_value = row

    result = await create_workspace(
        "user-1", "WS", workspace_id=ws_id, status="flash"
    )

    assert result["workspace_id"] == ws_id
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == ws_id  # workspace_id is first param when explicit


@pytest.mark.asyncio
async def test_get_workspace_found(ws_mock_db, mock_cursor):
    """get_workspace returns dict when row exists."""
    from src.server.database.workspace import get_workspace

    row = _workspace_row(workspace_id="ws-1")
    mock_cursor.fetchone.return_value = row

    result = await get_workspace("ws-1")

    assert result is not None
    assert result["workspace_id"] == "ws-1"
    sql = mock_cursor.execute.call_args[0][0]
    assert "status != 'deleted'" in sql


@pytest.mark.asyncio
async def test_get_workspace_not_found(ws_mock_db, mock_cursor):
    """get_workspace returns None when no row is found."""
    from src.server.database.workspace import get_workspace

    mock_cursor.fetchone.return_value = None

    result = await get_workspace("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_get_workspaces_for_user(ws_mock_db, mock_cursor):
    """get_workspaces_for_user returns paginated list and total count."""
    from src.server.database.workspace import get_workspaces_for_user

    # First call: COUNT query; second call: paginated SELECT
    ws1 = _workspace_row(workspace_id="ws-1")
    ws2 = _workspace_row(workspace_id="ws-2")
    mock_cursor.fetchone.side_effect = [{"total": 2}]
    mock_cursor.fetchall.return_value = [ws1, ws2]

    workspaces, total = await get_workspaces_for_user("user-1", limit=10, offset=0)

    assert total == 2
    assert len(workspaces) == 2
    # Verify flash filter is applied
    count_sql = mock_cursor.execute.call_args_list[0][0][0]
    assert "status != 'flash'" in count_sql


@pytest.mark.asyncio
async def test_update_workspace_name(ws_mock_db, mock_cursor):
    """update_workspace builds dynamic SET clause for provided fields."""
    from src.server.database.workspace import update_workspace

    row = _workspace_row(name="Renamed")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace("ws-1", name="Renamed")

    assert result is not None
    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE workspaces" in sql
    assert "name = %s" in sql


@pytest.mark.asyncio
async def test_update_workspace_status_stopped(ws_mock_db, mock_cursor):
    """update_workspace_status with 'stopped' sets stopped_at."""
    from src.server.database.workspace import update_workspace_status

    row = _workspace_row(status="stopped")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace_status("ws-1", "stopped")

    assert result["status"] == "stopped"
    sql = mock_cursor.execute.call_args[0][0]
    assert "stopped_at" in sql


@pytest.mark.asyncio
async def test_update_workspace_status_running(ws_mock_db, mock_cursor):
    """update_workspace_status with non-stopped status omits stopped_at."""
    from src.server.database.workspace import update_workspace_status

    row = _workspace_row(status="running")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace_status("ws-1", "running", sandbox_id="sb-1")

    assert result["status"] == "running"
    sql = mock_cursor.execute.call_args[0][0]
    assert "sandbox_id = %s" in sql
    # stopped_at should NOT be in the SET clause (it does appear in RETURNING)
    # Extract just the SET portion of the SQL
    set_clause = sql.split("SET")[1].split("WHERE")[0]
    assert "stopped_at" not in set_clause


@pytest.mark.asyncio
async def test_delete_workspace_soft(ws_mock_db, mock_cursor):
    """delete_workspace (soft) updates status to 'deleted'."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = {"workspace_id": "ws-1"}

    deleted = await delete_workspace("ws-1")

    assert deleted is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE workspaces" in sql
    assert "status = 'deleted'" in sql


@pytest.mark.asyncio
async def test_delete_workspace_hard(ws_mock_db, mock_cursor):
    """delete_workspace (hard) uses DELETE FROM."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = {"workspace_id": "ws-1"}

    deleted = await delete_workspace("ws-1", hard_delete=True)

    assert deleted is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "DELETE FROM workspaces" in sql


@pytest.mark.asyncio
async def test_delete_workspace_not_found(ws_mock_db, mock_cursor):
    """delete_workspace returns False when no row is affected."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = None

    deleted = await delete_workspace("nonexistent")
    assert deleted is False


@pytest.mark.asyncio
async def test_batch_update_sort_order(ws_mock_db, mock_cursor):
    """batch_update_sort_order builds VALUES list and includes user_id check."""
    from src.server.database.workspace import batch_update_sort_order

    mock_cursor.rowcount = 2

    await batch_update_sort_order("user-1", [("ws-1", 0), ("ws-2", 1)])

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE workspaces" in sql
    assert "v.wid::uuid" in sql
    params = mock_cursor.execute.call_args[0][1]
    # Last param should be user_id
    assert params[-1] == "user-1"


@pytest.mark.asyncio
async def test_batch_update_sort_order_empty(ws_mock_db, mock_cursor):
    """batch_update_sort_order with empty list returns immediately."""
    from src.server.database.workspace import batch_update_sort_order

    await batch_update_sort_order("user-1", [])

    mock_cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_workspaces_by_status(ws_mock_db, mock_cursor):
    """get_workspaces_by_status filters by status and returns list."""
    from src.server.database.workspace import get_workspaces_by_status

    ws1 = _workspace_row(status="stopped")
    mock_cursor.fetchall.return_value = [ws1]

    result = await get_workspaces_by_status("stopped", limit=50)

    assert len(result) == 1
    sql = mock_cursor.execute.call_args[0][0]
    assert "WHERE status = %s" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "stopped"
    assert params[1] == 50


@pytest.mark.asyncio
async def test_update_workspace_activity(ws_mock_db, mock_cursor):
    """update_workspace_activity uses conditional UPDATE (skip if <60s)."""
    from src.server.database.workspace import update_workspace_activity

    mock_cursor.rowcount = 1

    result = await update_workspace_activity("ws-1")

    assert result is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "last_activity_at" in sql
    assert "status != 'deleted'" in sql
    assert "INTERVAL" in sql  # conditional: skip if <60s


@pytest.mark.asyncio
async def test_update_workspace_activity_not_found(ws_mock_db, mock_cursor):
    """update_workspace_activity returns False when workspace not found (conditional UPDATE, no rows matched)."""
    from src.server.database.workspace import update_workspace_activity

    mock_cursor.rowcount = 0

    result = await update_workspace_activity("nonexistent")
    assert result is False
