"""Route-level regression: a non-UUID thread id is a clean 404, not a 500.

Mirrors the workspace malformed-id tests for the ``get_thread_owner_id`` guard.
A file/dir name from the SPA tree reaching the uuid column used to raise psycopg
InvalidTextRepresentation (22P02) → 500; ``require_thread_owner`` must now return
a clean 404 (the guard short-circuits to None before any DB access).
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app


@pytest_asyncio.fixture
async def threads_client():
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_get_thread_malformed_id_returns_404(threads_client):
    # Directory-name variant (e.g. the `results` dir from the agent file tree).
    resp = await threads_client.get("/api/v1/threads/results")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_thread_status_malformed_id_returns_404(threads_client):
    # Memory-file-key variant on a sub-route that also guards via require_thread_owner.
    resp = await threads_client.get(
        "/api/v1/threads/my_notes.md/status"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_thread_replay_malformed_id_returns_404(threads_client):
    resp = await threads_client.get(
        "/api/v1/threads/results/messages/replay"
    )
    assert resp.status_code == 404
