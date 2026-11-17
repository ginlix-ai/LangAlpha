"""A stop sent with a computer's token is an agent's `la`, so it stops as an
agent does: the user's queued messages stay queued, since the command has
nowhere to show them. A person's stop gets them back."""

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.dependencies import principal as principal_mod
from src.server.dependencies.principal import Principal
from tests.conftest import create_test_app

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "33333333-3333-4333-8333-333333333333"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override


@pytest_asyncio.fixture
async def app():
    from src.server.app.threads import router

    return create_test_app(router)


async def _cancel(app, caller: Principal, query: str = "", *, status: int = 200) -> AsyncMock:
    app.dependency_overrides[principal_mod._RUN] = lambda: caller
    cancel = AsyncMock(return_value={"cancelled": True})
    with (
        patch("src.server.app.threads.messaging.auth_api.require_thread_owner", new=AsyncMock()),
        patch("src.server.services.cancel_dispatch.cancel_workflow", new=cancel),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/v1/threads/{THREAD_ID}/cancel{query}")
    assert r.status_code == status
    return cancel


@pytest.mark.asyncio
async def test_a_computers_stop_keeps_the_users_queued_messages(app):
    cancel = await _cancel(app, Principal(OWNER_ID, "computer", computer_id="c-1"), f"?run_id={RUN_ID}")

    cancel.assert_awaited_once_with(THREAD_ID, RUN_ID, by_user=False)


@pytest.mark.asyncio
async def test_a_persons_stop_gets_them_back(app):
    cancel = await _cancel(app, Principal(OWNER_ID, "session"))

    cancel.assert_awaited_once_with(THREAD_ID, None, by_user=True)


@pytest.mark.asyncio
async def test_a_run_id_that_is_no_uuid_is_refused_before_anything_is_stopped(app):
    cancel = await _cancel(
        app, Principal(OWNER_ID, "computer", computer_id="c-1"), "?run_id=not-a-run", status=422
    )

    cancel.assert_not_awaited()
