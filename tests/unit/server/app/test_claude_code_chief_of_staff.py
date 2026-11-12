"""GET /claude-code/chief-of-staff: the Chief of Staff's answer, asked for one
Home thread by older web builds, read only for that thread's owner and only
where Claude Code is on."""

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from src.server.app import claude_code as route
from src.server.models.claude_code import ClaudeCodeChiefOfStaff
from src.server.services.oauth_signin import OAuthRefusal
from tests.conftest import create_test_app

USER = "test-user-123"
ANSWER = ClaudeCodeChiefOfStaff(kind="picked", computer_id="c-9", name="Studio", default_runs=False)


@pytest_asyncio.fixture
async def client():
    app = create_test_app(route.router)
    app.add_exception_handler(
        OAuthRefusal, lambda _, exc: JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code)
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def _read(client, *, owner=USER, on=True):
    answer = AsyncMock(return_value=ANSWER)
    with (
        patch("src.server.database.conversation.get_thread_owner_id", AsyncMock(return_value=owner)),
        patch.object(route.flow, "rows_on", AsyncMock(return_value=on)),
        patch.object(route, "chief_of_staff", answer),
    ):
        response = await client.get("/api/v1/claude-code/chief-of-staff", params={"thread_id": "t-1"})
    return response, answer


@pytest.mark.asyncio
async def test_the_owner_reads_the_answer_every_home_thread_shares(client):
    response, answer = await _read(client)
    assert response.status_code == 200
    assert response.json() == ANSWER.model_dump()
    answer.assert_awaited_once_with(USER)


@pytest.mark.asyncio
@pytest.mark.parametrize(("owner", "status"), [("someone-else", 403), (None, 404)])
async def test_another_users_thread_or_none_reads_nothing(client, owner, status):
    response, answer = await _read(client, owner=owner)
    assert response.status_code == status
    answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_claude_code_off_nothing_is_read(client):
    response, answer = await _read(client, on=False)
    assert response.status_code == 404
    answer.assert_not_awaited()
