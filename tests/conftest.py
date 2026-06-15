"""
Root conftest.py — shared fixtures for all backend tests.

Provides mock database connections, test FastAPI app builder,
and sample data factories.
"""

pytest_plugins = ["tests.integration.sandbox.metrics.conftest"]

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Database mocking
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cursor():
    """AsyncMock cursor with execute/fetchone/fetchall/fetchmany."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.fetchmany = AsyncMock(return_value=[])
    cursor.rowcount = 0
    cursor.description = None
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """AsyncMock connection that yields mock_cursor via cursor() context manager."""
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    conn.cursor = _cursor_cm

    # Real async-CM for conn.transaction() (savepoint/nested-txn paths). A plain
    # AsyncMock would auto-supply __aexit__ that returns truthy and SWALLOWS
    # exceptions, hiding rollback-on-error behavior; this one re-raises.
    @asynccontextmanager
    async def _transaction_cm(*args, **kwargs):
        yield mock_cursor

    conn.transaction = _transaction_cm
    conn.execute = AsyncMock()
    return conn


@pytest.fixture
def mock_db_connection(mock_connection):
    """
    Patches get_db_connection to yield the mock connection.

    This is the single choke point for all database modules:
    all use `async with get_db_connection() as conn`.
    """

    @asynccontextmanager
    async def _fake_get_db_connection():
        yield mock_connection

    with patch(
        "src.server.database.conversation.get_db_connection",
        new=_fake_get_db_connection,
    ) as mock:
        yield mock_connection


# ---------------------------------------------------------------------------
# FastAPI test app builder
# ---------------------------------------------------------------------------


def create_test_app(*routers) -> FastAPI:
    """
    Create a minimal FastAPI app with given routers, no lifespan.

    - No real DB/Redis/Daytona init (avoids setup.py lifespan)
    - Auth bypassed: get_current_user_id returns "test-user-123"
    - Rate limits bypassed: WorkspaceLimitCheck/ChatRateLimited passthrough
    """
    from src.server.dependencies.usage_limits import (
        ChatAuthResult,
        enforce_chat_limit,
        enforce_workspace_limit,
    )
    from src.server.utils.api import get_current_user_id

    app = FastAPI()
    for router in routers:
        app.include_router(router)

    # Override auth
    app.dependency_overrides[get_current_user_id] = lambda: "test-user-123"

    # Override rate limits
    app.dependency_overrides[enforce_workspace_limit] = lambda: "test-user-123"
    app.dependency_overrides[enforce_chat_limit] = lambda: ChatAuthResult(
        user_id="test-user-123"
    )

    return app


@pytest_asyncio.fixture
async def app_client():
    """
    httpx.AsyncClient wrapping a test FastAPI app with all routers.

    Auth is bypassed. No real DB/Redis connections.
    """
    from src.server.app.workspaces import router as workspaces_router

    app = create_test_app(workspaces_router)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Service mocks
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_workspace_manager():
    """AsyncMock of WorkspaceManager with common methods."""
    manager = AsyncMock()
    manager.create_workspace = AsyncMock()
    manager.get_session = AsyncMock()
    manager.stop_workspace = AsyncMock()
    manager.delete_workspace = AsyncMock()
    manager.start_workspace = AsyncMock()
    manager.archive_workspace = AsyncMock()
    manager.shutdown = AsyncMock()
    return manager


@pytest.fixture
def mock_session_service():
    """AsyncMock of SessionService with common methods."""
    service = AsyncMock()
    service.get_session = AsyncMock()
    service.cleanup = AsyncMock()
    service.shutdown = AsyncMock()
    return service


@pytest.fixture
def mock_redis():
    """AsyncMock Redis client."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    redis.delete = AsyncMock()
    redis.exists = AsyncMock(return_value=0)
    redis.pipeline = MagicMock()
    return redis


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_workspace_dict():
    """Factory for workspace DB row dicts."""

    def _make(
        workspace_id=None,
        user_id="test-user-123",
        name="Test Workspace",
        description=None,
        sandbox_id="sandbox-abc",
        status="running",
        mode="ptc",
        sort_order=0,
        **overrides,
    ):
        now = datetime.now(timezone.utc)
        data = {
            "workspace_id": workspace_id or str(uuid.uuid4()),
            "user_id": user_id,
            "name": name,
            "description": description,
            "sandbox_id": sandbox_id,
            "status": status,
            "mode": mode,
            "sort_order": sort_order,
            "created_at": now,
            "updated_at": now,
        }
        data.update(overrides)
        return data

    return _make


@pytest.fixture
def sample_thread_dict():
    """Factory for thread DB row dicts."""

    def _make(
        thread_id=None,
        workspace_id=None,
        user_id="test-user-123",
        title="Test Thread",
        **overrides,
    ):
        now = datetime.now(timezone.utc)
        data = {
            "thread_id": thread_id or str(uuid.uuid4()),
            "workspace_id": workspace_id or str(uuid.uuid4()),
            "user_id": user_id,
            "title": title,
            "is_shared": False,
            "share_token": None,
            "created_at": now,
            "updated_at": now,
        }
        data.update(overrides)
        return data

    return _make
