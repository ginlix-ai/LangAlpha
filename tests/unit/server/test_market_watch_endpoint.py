"""GET /api/v1/threads/{thread_id}/market-watch returns the Redis watch list
and the thread's price targets; DELETE .../targets/{id} cancels one.

Mirrors the dependency-override + AsyncClient pattern used by the other threads
route tests (see app/test_threads_provenance.py): auth is patched at
``get_thread_owner_id`` and the watch-list read is patched at the point the
route imports it. Neutral placeholder tickers only.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

THREADS_MOD = "src.server.app.threads.crud"
TARGET_ID = "22222222-2222-2222-2222-222222222222"

THREAD_ID = "11111111-1111-1111-1111-111111111111"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override


@pytest.fixture(autouse=True)
def no_targets():
    """Targets storage is never the real database; tests that read targets
    override the return value."""
    with (
        patch(f"{THREADS_MOD}.list_thread_targets", new=AsyncMock(return_value=[])) as listed,
        patch(f"{THREADS_MOD}.cancel_targets", new=AsyncMock(return_value=[])) as cancelled,
        patch(f"{THREADS_MOD}.targets_changed", new=AsyncMock()) as changed,
    ):
        yield {"list": listed, "cancel": cancelled, "changed": changed}


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
                f"{THREADS_MOD}.user_feature_enabled",
                new=AsyncMock(return_value=True),
            ),
            patch(
                f"{THREADS_MOD}.get_watchlist",
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
        assert body["targets"] == []

    @pytest.mark.asyncio
    async def test_empty_watchlist(self, threads_client):
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                f"{THREADS_MOD}.user_feature_enabled",
                new=AsyncMock(return_value=True),
            ),
            patch(
                f"{THREADS_MOD}.get_watchlist",
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
        self, threads_client, no_targets
    ):
        feature_gate = AsyncMock(return_value=False)
        watchlist = AsyncMock(return_value=["NVDA"])
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(f"{THREADS_MOD}.user_feature_enabled", new=feature_gate),
            patch(f"{THREADS_MOD}.get_watchlist", new=watchlist),
        ):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/market-watch"
            )
        assert resp.status_code == 200
        assert resp.json()["symbols"] == []
        assert resp.json()["targets"] == []
        feature_gate.assert_awaited_once_with(OWNER_ID, "market_watch")
        watchlist.assert_not_awaited()
        no_targets["list"].assert_not_awaited()


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


def _owner_and_feature(enabled: bool = True):
    return (
        patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=OWNER_ID),
        ),
        patch(f"{THREADS_MOD}.user_feature_enabled", new=AsyncMock(return_value=enabled)),
    )


_AT = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)
_LATER = datetime(2099, 1, 4, 14, 0, tzinfo=timezone.utc)


def _row(**over):
    row = {
        "target_id": TARGET_ID,
        "conversation_thread_id": THREAD_ID,
        "user_id": OWNER_ID,
        "symbol": "NVDA",
        "market": "stock",
        "kind": "pct_up",
        "value": 3.0,
        "pct_from": "armed",
        "armed_price": 200.0,
        "status": "armed",
        "expires_at": _AT,
        "warn_at": _AT,
        "warned_at": None,
        "fired_at": None,
        "fired_price": None,
        "created_at": _AT,
        "updated_at": _AT,
    }
    row.update(over)
    return row


class TestMarketWatchTargets:
    @pytest.mark.asyncio
    async def test_targets_in_the_tools_vocabulary_and_spelling(self, threads_client, no_targets):
        no_targets["list"].return_value = [
            _row(expires_at=_LATER),
            _row(
                target_id="33333333-3333-3333-3333-333333333333",
                symbol="SPX",
                market="index",
                kind="below",
                value=5900.0,
                pct_from=None,
                armed_price=None,
                status="fired",
                fired_at=_AT,
                fired_price=5899.5,
            ),
        ]
        owner, feature = _owner_and_feature()
        with owner, feature, patch(f"{THREADS_MOD}.get_watchlist", new=AsyncMock(return_value=["NVDA", "^GSPC"])):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/market-watch")
        assert resp.status_code == 200
        targets = resp.json()["targets"]
        assert targets[0] == {
            "target_id": TARGET_ID,
            "symbol": "NVDA",
            "kind": "pct_up",
            "value": 3.0,
            "pct_from": "now",
            "armed_price": 200.0,
            "status": "armed",
            "expires_at": "2099-01-04T14:00:00Z",
            "fired_at": None,
            "fired_price": None,
            "created_at": "2026-10-06T14:00:00Z",
        }
        # An index stored under the monitor's key is named as it is watched.
        assert targets[1]["symbol"] == "^GSPC"
        assert targets[1]["pct_from"] is None
        assert (targets[1]["status"], targets[1]["fired_price"]) == ("fired", 5899.5)
        no_targets["list"].assert_awaited_once_with(THREAD_ID)

    @pytest.mark.asyncio
    async def test_a_target_read_failure_keeps_the_watch_list(self, threads_client, no_targets):
        no_targets["list"].side_effect = RuntimeError("db down")
        owner, feature = _owner_and_feature()
        with owner, feature, patch(f"{THREADS_MOD}.get_watchlist", new=AsyncMock(return_value=["NVDA"])):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/market-watch")
        assert resp.status_code == 200
        assert resp.json()["symbols"] == ["NVDA"]
        assert resp.json()["targets"] == []

    @pytest.mark.asyncio
    async def test_cancel_one(self, threads_client, no_targets):
        no_targets["cancel"].return_value = [_row(status="cancelled")]
        owner, feature = _owner_and_feature()
        with owner, feature:
            resp = await threads_client.delete(
                f"/api/v1/threads/{THREAD_ID}/market-watch/targets/{TARGET_ID}"
            )
        assert resp.status_code == 200
        assert resp.json() == {"cancelled": True}
        no_targets["cancel"].assert_awaited_once_with(THREAD_ID, target_id=TARGET_ID)
        no_targets["changed"].assert_awaited_once_with(THREAD_ID)

    @pytest.mark.asyncio
    async def test_cancel_a_target_no_longer_armed(self, threads_client, no_targets):
        owner, feature = _owner_and_feature()
        with owner, feature:
            resp = await threads_client.delete(
                f"/api/v1/threads/{THREAD_ID}/market-watch/targets/{TARGET_ID}"
            )
        assert resp.json() == {"cancelled": False}
        no_targets["changed"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_with_the_feature_off_still_cancels(self, threads_client, no_targets):
        """Turning the feature off must not strand a target armed before."""
        no_targets["cancel"].return_value = [_row(status="cancelled")]
        owner, feature = _owner_and_feature(enabled=False)
        with owner, feature:
            resp = await threads_client.delete(
                f"/api/v1/threads/{THREAD_ID}/market-watch/targets/{TARGET_ID}"
            )
        assert resp.json() == {"cancelled": True}
        no_targets["cancel"].assert_awaited_once_with(THREAD_ID, target_id=TARGET_ID)

    @pytest.mark.asyncio
    async def test_an_armed_target_past_its_expiry_reads_expired(self, threads_client, no_targets):
        """The sweep has not closed it yet, but nothing will check it."""
        no_targets["list"].return_value = [_row()]
        owner, feature = _owner_and_feature()
        with owner, feature, patch(f"{THREADS_MOD}.get_watchlist", new=AsyncMock(return_value=[])):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/market-watch")
        assert resp.json()["targets"][0]["status"] == "expired"

    @pytest.mark.asyncio
    async def test_archiving_a_thread_cancels_its_targets(self, threads_client, no_targets):
        updated = {
            "conversation_thread_id": THREAD_ID,
            "workspace_id": "ws-1",
            "thread_index": 0,
            "current_status": "completed",
            "archived_at": _AT,
            "subagents_allowed": True,
            "created_at": _AT,
            "updated_at": _AT,
        }
        owner, _ = _owner_and_feature()
        with (
            owner,
            patch(f"{THREADS_MOD}.update_thread_fields", new=AsyncMock(return_value=updated)),
            patch(f"{THREADS_MOD}.publish_thread_archived", new=AsyncMock()),
            patch(f"{THREADS_MOD}.prune_thread_dirs_soon"),
            patch(
                "src.server.services.wakeups.cancel.cancel_on_archive", new=AsyncMock()
            ) as cancel_on_archive,
        ):
            resp = await threads_client.patch(
                f"/api/v1/threads/{THREAD_ID}", json={"archived": True}
            )
        assert resp.status_code == 200
        # One call ends both the thread's wakeups and its legacy targets.
        cancel_on_archive.assert_awaited_once_with(THREAD_ID)

    @pytest.mark.asyncio
    async def test_cancel_by_a_non_owner_is_forbidden(self, threads_client, no_targets):
        with patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value="someone-else"),
        ):
            resp = await threads_client.delete(
                f"/api/v1/threads/{THREAD_ID}/market-watch/targets/{TARGET_ID}"
            )
        assert resp.status_code == 403
        no_targets["cancel"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_rejects_a_malformed_id(self, threads_client, no_targets):
        owner, feature = _owner_and_feature()
        with owner, feature:
            resp = await threads_client.delete(
                f"/api/v1/threads/{THREAD_ID}/market-watch/targets/not-a-uuid"
            )
        assert resp.status_code == 422
        no_targets["cancel"].assert_not_awaited()
