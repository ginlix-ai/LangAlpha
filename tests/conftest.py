"""
Root conftest.py — shared fixtures for all backend tests.

Provides mock database connections, test FastAPI app builder,
and sample data factories.
"""

pytest_plugins = ["tests.integration.sandbox.metrics.conftest"]

import inspect
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from httpx import ASGITransport, AsyncClient

# Runs before any ``src`` import: ``src.config`` calls ``load_dotenv()``, which
# would otherwise pull ``LANGSMITH_TRACING=true`` from a developer's .env and
# bill every test run as a trace. ``load_dotenv`` never overrides a var that is
# already set, so pinning it here wins. Set LANGALPHA_TEST_TRACING=1 to trace
# a run on purpose.
if os.environ.get("LANGALPHA_TEST_TRACING") != "1":
    for _flag in ("LANGSMITH_TRACING", "LANGCHAIN_TRACING_V2", "LANGCHAIN_TRACING"):
        os.environ[_flag] = "false"


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
    async def _fake_get_db_connection(conn=None):
        # A passed connection is yielded unchanged, as the real one does.
        yield conn if conn is not None else mock_connection

    with patch(
        "src.server.database.pool.get_db_connection",
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
    - Rate limits bypassed: ComputerLimitCheck passthrough, and every turn
      admitted (``pin_chat_limit``)
    """
    from src.server.dependencies.usage_limits import (
        ChatAuthResult,
        enforce_computer_limit,
    )
    from src.server.dependencies.principal import Principal, get_principal
    from src.server.utils.api import get_current_user_id

    app = FastAPI()
    for router in routers:
        app.include_router(router)

    # Override auth. The scoped routes read the caller through get_principal,
    # which follows whatever user a test pins on get_current_user_id, as a
    # session, and runs for real once a test drops that override.
    app.dependency_overrides[get_current_user_id] = lambda: "test-user-123"

    async def principal_as_session(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(
            HTTPBearer(auto_error=False)
        ),
    ) -> Principal:
        pinned = request.app.dependency_overrides.get(get_current_user_id)
        if pinned is None:
            return await get_principal(request, credentials)
        user = pinned()
        return Principal(
            user_id=await user if inspect.isawaitable(user) else user, auth="session"
        )

    app.dependency_overrides[get_principal] = principal_as_session

    # Override rate limits
    app.dependency_overrides[enforce_computer_limit] = lambda: "test-user-123"
    pin_chat_limit(app, ChatAuthResult(user_id="test-user-123"))

    return app


def pin_chat_limit(app: FastAPI, result) -> None:
    """Every turn ``app`` admits holds ``result`` (a ``ChatAuthResult``), in
    place of ``chat_limit_for``."""
    from src.server.dependencies.usage_limits import chat_limiter

    async def limit(user_id: str):
        return result

    app.dependency_overrides[chat_limiter] = lambda: limit


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
    manager.delete_workspace = AsyncMock()
    manager.start_workspace = AsyncMock()
    manager.archive_workspace = AsyncMock()
    manager.shutdown = AsyncMock()
    return manager


@pytest.fixture
def mock_computer_manager():
    """AsyncMock of ComputerManager, with the surface the computer routes call.

    The router imports the manager inside each handler, so a test patches
    ``src.server.app.computers._computer_manager`` to return this.
    """
    manager = AsyncMock()
    manager.get_session_for_computer = AsyncMock()
    manager.start_computer = AsyncMock()
    manager.stop_computer = AsyncMock()
    manager.archive_computer = AsyncMock()
    manager.set_computer_spec = AsyncMock()
    manager.set_computer_always_on = AsyncMock()
    manager.remove_computer_with_workspaces = AsyncMock(return_value=[])
    return manager


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
def sample_computer_dict():
    """Factory for computer DB row dicts (the shape ``_COMPUTER_COLS`` returns)."""

    def _make(
        computer_id=None,
        user_id="test-user-123",
        kind="daytona",
        name="My computer",
        status="running",
        provider_ref="sandbox-abc",
        is_primary=True,
        resource_tier="standard",
        is_always_on=False,
        root_dir="/home/workspace",
        **overrides,
    ):
        now = datetime.now(timezone.utc)
        data = {
            "computer_id": computer_id or str(uuid.uuid4()),
            "user_id": user_id,
            "kind": kind,
            "provider_ref": provider_ref,
            "name": name,
            "is_primary": is_primary,
            "status": status,
            "resource_tier": resource_tier,
            "is_always_on": is_always_on,
            "platform_secret_version": 0,
            "mcp_config_version": 0,
            "root_dir": root_dir,
            "layout_version": 3,
            "provider_config": {},
            "artifacts": {},
            "last_activity_at": None,
            "stopped_at": None,
            "created_at": now,
            "updated_at": now,
            "config": {},
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
