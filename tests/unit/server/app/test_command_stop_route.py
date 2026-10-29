"""POST /threads/{thread_id}/commands/{command_id}/stop: the owner's stop of one
background command, answered with whether it stopped and the row after."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.commands.control import StopResult
from tests.conftest import create_test_app

MOD = "src.server.app.threads.commands"
THREAD_ID = "11111111-1111-1111-1111-111111111111"
COMMAND_ID = "bg-3f9a1c2e7d10"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override
PATH = f"/api/v1/threads/{THREAD_ID}/commands/{COMMAND_ID}/stop"
T0 = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)


def _row(status="running", **over):
    row = {
        "command_id": COMMAND_ID,
        "conversation_thread_id": THREAD_ID,
        "tool": "Bash",
        "description": "Run 10-year backtest",
        "status": status,
        "exit_code": None,
        "failure_reason": None,
        "tool_call_id": "toolu_1",
        "log_path": ".agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log",
        "started_at": T0,
        "handed_off_at": T0,
        "ended_at": None if status == "running" else T0,
        # A full row read carries the raw MCP trace.
        "mcp_trace": [{"server": "s", "tool": "t", "args": {"key": "secret"}}],
        "output_tail": "last words",
    }
    row.update(over)
    return row


@pytest_asyncio.fixture
async def client():
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _owner():
    return patch(
        "src.server.database.conversation.get_thread_owner_id",
        new=AsyncMock(return_value=OWNER_ID),
    )


def _stop(result):
    return patch(f"{MOD}.control.stop_thread_command", new=AsyncMock(return_value=result))


@pytest.mark.asyncio
async def test_a_stop_answers_stopped_and_the_row_after_it(client):
    with _owner(), _stop(StopResult("stopped", _row("stopped"))) as stop:
        r = await client.post(PATH)
    assert r.status_code == 200
    body = r.json()
    assert body["stopped"] is True
    assert body["command"]["status"] == "stopped"
    assert body["command"]["tool_call_id"] == "toolu_1"
    # The trace is unredacted: no answer ever carries it.
    assert "mcp_trace" not in body["command"]
    assert "secret" not in r.text
    stop.assert_awaited_once_with(THREAD_ID, COMMAND_ID, by="user")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result, status, stopping",
    [
        (StopResult("ended", _row("completed", exit_code=0)), "completed", False),
        # Marked, and finished by the server on its own: it reads as stopping
        # so the web does not offer the stop again.
        (StopResult("stopping", _row(stopped_by="user")), "running", True),
    ],
)
async def test_a_command_this_stop_did_not_end_answers_not_stopped_with_how_it_stands(
    client, result, status, stopping
):
    with _owner(), _stop(result):
        r = await client.post(PATH)
    assert r.status_code == 200
    assert r.json()["stopped"] is False
    assert r.json()["command"]["status"] == status
    assert r.json()["command"]["stopping"] is stopping
    # The mark says who stopped it, which stays on the server.
    assert "stopped_by" not in r.json()["command"]


@pytest.mark.asyncio
async def test_a_command_not_on_this_thread_is_404(client):
    with _owner(), _stop(StopResult("not_found")):
        r = await client.post(PATH)
    assert r.status_code == 404
