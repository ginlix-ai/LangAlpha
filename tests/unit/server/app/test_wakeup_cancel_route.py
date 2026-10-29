"""A cancel that committed answers 200 with its outcomes even when the
listing read after it fails: a 500 would have the user retry what is done."""

from unittest.mock import AsyncMock, patch

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.database.wakeups.cancel import CancelOutcome
from tests.conftest import create_test_app

MOD = "src.server.app.threads.wakeups"
THREAD_ID = "11111111-1111-1111-1111-111111111111"
WAKEUP_ID = "22222222-2222-2222-2222-222222222222"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override


@pytest_asyncio.fixture
async def threads_client():
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method, path, body",
    [
        ("POST", f"/api/v1/threads/{THREAD_ID}/wakeups/cancel", {"wakeup_ids": [WAKEUP_ID]}),
        ("DELETE", f"/api/v1/threads/{THREAD_ID}/wakeups/{WAKEUP_ID}", None),
    ],
)
async def test_a_committed_cancel_answers_its_outcomes_when_the_listing_fails(
    threads_client, method, path, body
):
    cancelled = CancelOutcome(WAKEUP_ID, "w_ab12", "cancelled", {"kind": "at"})
    with (
        patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=OWNER_ID),
        ),
        patch(f"{MOD}.wakeups_cancel.user_cancel", new=AsyncMock(return_value=[cancelled])),
        patch(
            f"{MOD}.thread_wakeups",
            new=AsyncMock(side_effect=psycopg.OperationalError("connection lost")),
        ),
    ):
        r = await threads_client.request(method, path, json=body)

    assert r.status_code == 200
    assert r.json() == {
        "results": [{"wakeup_id": WAKEUP_ID, "outcome": "cancelled"}],
        "wakeups": None,
    }
