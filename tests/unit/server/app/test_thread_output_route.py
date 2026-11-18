"""``GET /api/v1/threads/{thread_id}/output``: the text a thread's latest
turns committed, as ``la threads output`` prints it."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.services.thread_output import ThreadOutput
from tests.conftest import create_test_app

THREAD = "b2200000-0000-4000-8000-000000000001"


def _app():
    from src.server.app.threads import router

    return create_test_app(router)


@pytest.mark.asyncio
async def test_output_reads_the_latest_turns_in_the_users_zone():
    extract = AsyncMock(
        return_value=ThreadOutput(workspace_id="ws-1", status="completed", text="Margins rose.")
    )
    with (
        patch("src.server.app.threads.crud.thread_access", new=AsyncMock()) as owner,
        patch("src.server.services.thread_output.extract_text_from_thread", new=extract),
        patch(
            "src.server.services.wakeups.message.user_timezone",
            new=AsyncMock(return_value="Asia/Tokyo"),
        ),
    ):
        async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
            resp = await c.get(f"/api/v1/threads/{THREAD}/output", params={"turns": 3})

    assert resp.status_code == 200
    assert resp.json() == {
        "thread_id": THREAD,
        "workspace_id": "ws-1",
        "status": "completed",
        "text": "Margins rose.",
    }
    # Before the read and again after it: the read is by id alone.
    assert owner.await_count == 2
    owner.assert_awaited_with(THREAD, "test-user-123")
    extract.assert_awaited_once_with(THREAD, turns=3, timezone="Asia/Tokyo")


@pytest.mark.asyncio
async def test_output_defaults_to_the_latest_turn_and_takes_zero_for_history():
    extract = AsyncMock(return_value=ThreadOutput(workspace_id="ws-1", status="running", text=""))
    with (
        patch("src.server.app.threads.crud.thread_access", new=AsyncMock()),
        patch("src.server.services.thread_output.extract_text_from_thread", new=extract),
        patch("src.server.services.wakeups.message.user_timezone", new=AsyncMock(return_value="UTC")),
    ):
        async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
            latest = await c.get(f"/api/v1/threads/{THREAD}/output")
            history = await c.get(f"/api/v1/threads/{THREAD}/output", params={"turns": 0})
            assert history.status_code == 200
            negative = await c.get(f"/api/v1/threads/{THREAD}/output", params={"turns": -1})

    assert latest.json()["status"] == "running"
    assert [call.kwargs["turns"] for call in extract.await_args_list] == [1, 0]
    assert negative.status_code == 422


@pytest.mark.asyncio
async def test_output_of_a_thread_the_caller_does_not_own_is_refused():
    extract = AsyncMock()
    with (
        patch(
            "src.server.app.threads.crud.thread_access",
            new=AsyncMock(side_effect=HTTPException(status_code=404, detail="Thread not found")),
        ),
        patch("src.server.services.thread_output.extract_text_from_thread", new=extract),
    ):
        async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
            resp = await c.get(f"/api/v1/threads/{THREAD}/output")

    assert resp.status_code == 404
    extract.assert_not_awaited()


@pytest.mark.asyncio
async def test_output_of_a_thread_deleted_after_the_owner_check_is_not_found():
    with (
        patch("src.server.app.threads.crud.thread_access", new=AsyncMock()),
        patch("src.server.services.thread_output.extract_text_from_thread", new=AsyncMock(return_value=None)),
        patch("src.server.services.wakeups.message.user_timezone", new=AsyncMock(return_value="UTC")),
    ):
        async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
            resp = await c.get(f"/api/v1/threads/{THREAD}/output")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_output_of_a_thread_taken_by_another_account_mid_read_is_refused():
    # A deleted thread's id can be taken again; the text read is then not
    # the caller's.
    owner = AsyncMock(side_effect=[None, HTTPException(status_code=404, detail="Thread not found")])
    extract = AsyncMock(return_value=ThreadOutput(workspace_id="w", status="completed", text="x"))
    with (
        patch("src.server.app.threads.crud.thread_access", new=owner),
        patch("src.server.services.thread_output.extract_text_from_thread", new=extract),
        patch("src.server.services.wakeups.message.user_timezone", new=AsyncMock(return_value="UTC")),
    ):
        async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
            resp = await c.get(f"/api/v1/threads/{THREAD}/output")

    assert resp.status_code == 404
