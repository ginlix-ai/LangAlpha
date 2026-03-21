"""Fixtures for integration tests.

Provides:
- Database fixtures for integration tests against real PostgreSQL
- Singleton reset fixtures for MCP clients
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


# ---------------------------------------------------------------------------
# MCP singleton teardown (pre-existing)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def _reset_fmp_singleton():
    """Close and reset the FMP client singleton after each test."""
    yield
    from data_client.fmp import close_fmp_client
    await close_fmp_client()


@pytest_asyncio.fixture(autouse=True)
async def _reset_ginlix_singleton():
    """Close and reset the ginlix-data httpx client after each test."""
    yield
    try:
        import mcp_servers.price_data_mcp_server as mod
        if hasattr(mod, "_ginlix_http") and mod._ginlix_http is not None:
            await mod._ginlix_http.aclose()
            mod._ginlix_http = None
    except (ImportError, AttributeError):
        pass


# ---------------------------------------------------------------------------
# Database integration fixtures
# ---------------------------------------------------------------------------

# Tables in dependency (FK) order -- children first so TRUNCATE CASCADE is safe
_ALL_TABLES = [
    "automation_executions",
    "automations",
    "conversation_feedback",
    "conversation_usages",
    "conversation_responses",
    "conversation_queries",
    "conversation_threads",
    "workspace_files",
    "watchlist_items",
    "watchlists",
    "user_portfolios",
    "user_api_keys",
    "user_preferences",
    "workspaces",
    "users",
]

# Additional tables created by migrations (LangGraph, market_insights, etc.)
_EXTRA_TABLES = [
    "market_insights",
    "user_oauth_tokens",
    "store_migrations",
    "store",
    "checkpoint_writes",
    "checkpoint_blobs",
    "checkpoints",
    "checkpoint_migrations",
]


def _build_db_uri() -> str:
    """Build PostgreSQL connection string from env vars (CI-compatible defaults).

    Uses TEST_DB_* env vars so the app's .env (which sets DB_HOST to
    host.docker.internal for Docker networking) does not bleed into tests
    that run on the host machine.
    """
    host = os.getenv("TEST_DB_HOST", "localhost")
    port = os.getenv("TEST_DB_PORT", "5432")
    name = os.getenv("TEST_DB_NAME", "langalpha_test")
    user = os.getenv("TEST_DB_USER", "postgres")
    password = os.getenv("TEST_DB_PASSWORD", "postgres")
    sslmode = "require" if "supabase.com" in host else "disable"
    return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"


async def _run_alembic_upgrade(db_uri: str) -> None:
    """Run alembic migrations against the given database URI.

    Uses the project's alembic.ini + migrations/ directory as the single
    source of truth, so the test schema always matches production.

    Runs in a thread because the migration uses asyncio.run() internally
    (for LangGraph checkpoint setup), which would conflict with the
    already-running event loop in pytest-asyncio fixtures.
    """
    import asyncio

    from alembic import command
    from alembic.config import Config

    project_root = Path(__file__).resolve().parent.parent.parent
    alembic_cfg = Config(str(project_root / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(project_root / "migrations"))

    # Convert psycopg URI to SQLAlchemy+psycopg URI for alembic
    sa_url = db_uri.replace("postgresql://", "postgresql+psycopg://", 1)
    alembic_cfg.set_main_option("sqlalchemy.url", sa_url)

    await asyncio.to_thread(command.upgrade, alembic_cfg, "head")


@pytest.fixture(scope="session")
def test_db_uri() -> str:
    """Build and return the test database URI."""
    return _build_db_uri()


@pytest_asyncio.fixture(scope="session")
async def test_db_pool(test_db_uri):
    """Session-scoped async connection pool for integration tests.

    Runs alembic migrations (single source of truth) to create the schema,
    yields the pool, then tears down by truncating all tables.
    """
    import psycopg

    # Drop all existing tables to ensure a clean slate before migrations
    async with await psycopg.AsyncConnection.connect(
        test_db_uri, autocommit=False
    ) as conn:
        async with conn.cursor() as cur:
            for table in _ALL_TABLES + _EXTRA_TABLES:
                await cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            # Clear alembic version so migrations run from scratch
            await cur.execute("DROP TABLE IF EXISTS alembic_version CASCADE")
        await conn.commit()

    # Run alembic migrations -- the single source of truth for schema
    await _run_alembic_upgrade(test_db_uri)

    # Now create the pool for actual test operations
    pool = AsyncConnectionPool(
        conninfo=test_db_uri,
        min_size=1,
        max_size=5,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        },
        open=False,
    )
    await pool.open()
    await pool.wait()

    yield pool

    # Teardown: truncate (not drop) so the schema remains for debugging
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for table in _ALL_TABLES:
                await cur.execute(f"TRUNCATE TABLE {table} CASCADE")

    await pool.close()


@pytest_asyncio.fixture
async def cleanup_tables(test_db_pool):
    """Truncate all tables after each test for isolation.

    NOT autouse — only activated when a test explicitly requests it
    (or transitively via seed_user/seed_workspace).
    """
    yield
    async with test_db_pool.connection() as conn:
        async with conn.cursor() as cur:
            for table in _ALL_TABLES:
                await cur.execute(f"TRUNCATE TABLE {table} CASCADE")


@pytest_asyncio.fixture
async def db_conn(test_db_pool):
    """Yield a single async connection from the test pool.

    The connection is used in autocommit mode (pool default).
    """
    async with test_db_pool.connection() as conn:
        yield conn


@pytest_asyncio.fixture
async def patched_get_db_connection(test_db_pool):
    """Patch get_db_connection to use the test pool instead of production.

    This allows database module functions (workspace.py, user.py, etc.)
    to transparently use the test database.
    """
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _test_get_db_connection():
        async with test_db_pool.connection() as conn:
            yield conn

    with patch(
        "src.server.database.conversation.get_db_connection",
        _test_get_db_connection,
    ):
        # Also patch the re-export in every database module that imports it
        with patch(
            "src.server.database.workspace.get_db_connection",
            _test_get_db_connection,
        ), patch(
            "src.server.database.user.get_db_connection",
            _test_get_db_connection,
        ), patch(
            "src.server.database.watchlist.get_db_connection",
            _test_get_db_connection,
        ), patch(
            "src.server.database.portfolio.get_db_connection",
            _test_get_db_connection,
        ), patch(
            "src.server.database.api_keys.get_db_connection",
            _test_get_db_connection,
        ), patch(
            "src.server.database.automation.get_db_connection",
            _test_get_db_connection,
        ):
            yield _test_get_db_connection


@pytest.fixture
def test_user_id() -> str:
    """Deterministic test user ID."""
    return "test-user-integration-001"


@pytest_asyncio.fixture
async def seed_user(patched_get_db_connection, cleanup_tables, test_user_id):
    """Insert a test user and return the user dict.

    Most database modules require a user row to exist (FK constraints).
    """
    from src.server.database.user import create_user

    user = await create_user(
        user_id=test_user_id,
        email="test@example.com",
        name="Test User",
    )
    return user


@pytest_asyncio.fixture
async def seed_workspace(seed_user, patched_get_db_connection):
    """Insert a test workspace and return its dict.

    Depends on seed_user to satisfy FK constraints.
    """
    from src.server.database.workspace import create_workspace

    ws = await create_workspace(
        user_id=seed_user["user_id"],
        name="Test Workspace",
        description="Integration test workspace",
        status="running",
    )
    return ws
