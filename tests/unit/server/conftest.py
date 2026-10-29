"""
Server-specific test fixtures.

Provides per-router test clients for isolated route testing.
"""

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app


@pytest_asyncio.fixture
async def workspaces_client():
    """Client with only the workspaces router."""
    from src.server.app.workspaces import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def threads_client():
    """Client with only the threads router."""
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def public_client():
    """Client with only the public sharing router."""
    from src.server.app.public import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def users_client():
    """Client with only the users router."""
    from src.server.app.users import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def watchlist_client():
    """Client with only the watchlist router."""
    from src.server.app.watchlist import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def portfolio_client():
    """Client with only the portfolio router."""
    from src.server.app.portfolio import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def automations_client():
    """Client with only the automations router."""
    from src.server.app.automations import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def api_keys_client():
    """Client with only the api_keys router."""
    from src.server.app.api_keys import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def skills_client():
    """Client with only the skills router."""
    from src.server.app.skills import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
