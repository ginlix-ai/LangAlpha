"""GET /api/v1/threads/{thread_id}/market-watch returns the Redis watch list.

Mirrors the dependency-override + AsyncClient pattern used by the other threads
route tests (see app/test_threads_provenance.py): auth is patched at
``get_thread_owner_id`` and the watch-list read is patched at the point the
route imports it. Neutral placeholder tickers only.
"""

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

THREAD_ID = "11111111-1111-1111-1111-111111111111"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override


@pytest_asyncio.fixture
async def threads_client():
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


class TestGetMarketWatch:
    @pytest.mark.asyncio
    async def test_returns_watchlist(self, threads_client):
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                "src.server.app.threads.user_feature_enabled",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "src.server.app.threads.get_watchlist",
                new=AsyncMock(return_value=["NVDA", "TSLA"]),
            ),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["thread_id"] == THREAD_ID
        assert body["symbols"] == ["NVDA", "TSLA"]

    @pytest.mark.asyncio
    async def test_empty_watchlist(self, threads_client):
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                "src.server.app.threads.user_feature_enabled",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "src.server.app.threads.get_watchlist",
                new=AsyncMock(return_value=[]),
            ),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 200
        assert resp.json()["symbols"] == []


class TestGetMarketWatchDisabled:
    @pytest.mark.asyncio
    async def test_feature_off_reports_empty_without_reading_redis(
        self, threads_client
    ):
        feature_gate = AsyncMock(return_value=False)
        watchlist = AsyncMock(return_value=["NVDA"])
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch("src.server.app.threads.user_feature_enabled", new=feature_gate),
            patch("src.server.app.threads.get_watchlist", new=watchlist),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 200
        assert resp.json()["symbols"] == []
        feature_gate.assert_awaited_once_with(OWNER_ID, "market_watch")
        watchlist.assert_not_awaited()


class TestGetMarketWatchAuth:
    @pytest.mark.asyncio
    async def test_unknown_thread_returns_404(self, threads_client):
        with patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=None),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_non_owner_returns_403(self, threads_client):
        with patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value="someone-else"),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 403
