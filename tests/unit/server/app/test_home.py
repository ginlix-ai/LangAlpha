"""Tests for the Home routes (src/server/app/home.py)."""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

NOW = datetime.now(timezone.utc)
USER = "test-user-123"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
CLOUD_ID = "33333333-3333-4333-8333-333333333333"
HOME = "src.server.app.home"


def _home_row(**overrides):
    from src.server.database.home_workspace import get_home_workspace_id

    data = {
        "workspace_id": get_home_workspace_id(USER),
        "user_id": USER,
        "name": "Home",
        "description": None,
        "sandbox_id": None,
        "computer_id": None,
        "status": "unbound",
        "sort_order": 0,
        "is_pinned": False,
        "created_at": NOW,
        "updated_at": NOW,
        "last_activity_at": None,
        "stopped_at": None,
        "config": None,
    }
    data.update(overrides)
    return data


@pytest_asyncio.fixture
async def client():
    from src.server.app.home import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_get_home_workspace(client):
    ws = _home_row()
    with patch(
        f"{HOME}.get_or_create_home_workspace", new_callable=AsyncMock, return_value=ws
    ):
        resp = await client.post("/api/v1/workspaces/home")

    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == ws["workspace_id"]
    assert resp.json()["is_home"] is True


@pytest.mark.asyncio
async def test_get_home_workspace_error(client):
    with patch(
        f"{HOME}.get_or_create_home_workspace",
        new_callable=AsyncMock,
        side_effect=RuntimeError("db down"),
    ):
        resp = await client.post("/api/v1/workspaces/home")

    assert resp.status_code == 500


@pytest.mark.asyncio
async def test_a_home_id_another_account_holds_is_forbidden(client):
    """An account merge carries the merged account's Home onto the account it
    joins. Every app load asks for it, and no retry makes it the caller's."""
    from src.server.database.workspace import HomeWorkspaceTaken

    with patch(
        f"{HOME}.get_or_create_home_workspace",
        new_callable=AsyncMock,
        side_effect=HomeWorkspaceTaken("home-ws", USER),
    ):
        resp = await client.post("/api/v1/workspaces/home")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_a_computer_token_gets_home_like_the_user():
    """Every computer's token reaches Home, wherever Home sits."""
    from src.server.app.home import router
    from tests.unit.server.dependencies.reach_world import OTHER_COMPUTER, act_as_computer

    app = create_test_app(router)
    act_as_computer(app, computer=OTHER_COMPUTER)
    with patch(
        f"{HOME}.get_or_create_home_workspace",
        new_callable=AsyncMock,
        return_value=_home_row(),
    ) as ensure:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/v1/workspaces/home")

    assert resp.status_code == 200
    ensure.assert_awaited_once()


@pytest.mark.asyncio
async def test_placement_answers_in_the_shape_the_client_reads(client):
    """Home away on its machine, its turns on the cloud, and a hand-back the
    user has not dismissed, with the move nested as its own model."""
    from src.server.models.workspace_move import WorkspaceMoveOut

    move = WorkspaceMoveOut(
        move_id=str(uuid.uuid4()),
        workspace_id=str(uuid.uuid4()),
        source_computer_id=CLOUD_ID,
        target_computer_id=COMPUTER_ID,
        make_primary=True,
        phase="done",
        created_at=NOW,
    )
    view = {
        "computer_id": COMPUTER_ID,
        "computer_name": "MacBook Pro",
        "computer_kind": "ssh",
        "unreachable": True,
        "stand_in": {"computer_id": CLOUD_ID, "name": "Cloud computer", "status": "running"},
        "handback_owed": False,
        "handback": {
            "handback_id": "hb-1",
            "folder": "From the cloud/2026-11-15",
            "files": 3,
            "left_on_cloud": ["big.bin"],
            "unreadable": [],
            "computer_name": "MacBook Pro",
            "cloud_name": "Cloud computer",
            "created_at": "2026-11-15T10:00:00+00:00",
        },
        "left_on_stand_in": {"failed": False, "paths": ["big.bin"], "computer_id": CLOUD_ID},
        "move": move,
    }
    with patch(
        "src.server.services.home_placement.home_placement_view",
        AsyncMock(return_value=view),
    ):
        resp = await client.get("/api/v1/workspaces/home/placement")

    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {
        "computer_id",
        "computer_name",
        "computer_kind",
        "unreachable",
        "stand_in",
        "handback_owed",
        "handback",
        "left_on_stand_in",
        "move",
    }
    assert body["stand_in"]["computer_id"] == CLOUD_ID
    assert body["handback"]["folder"] == "From the cloud/2026-11-15"
    assert body["handback"]["failed"] is False
    assert body["left_on_stand_in"] == {"failed": False, "paths": ["big.bin"], "computer_id": CLOUD_ID}
    assert body["move"]["move_id"] == move.move_id


@pytest.mark.asyncio
async def test_placement_of_a_user_with_no_computer_is_all_null(client):
    view = {
        "computer_id": None,
        "computer_name": None,
        "computer_kind": None,
        "unreachable": False,
        "stand_in": None,
        "handback_owed": False,
        "handback": None,
        "left_on_stand_in": None,
        "move": None,
    }
    with patch(
        "src.server.services.home_placement.home_placement_view",
        AsyncMock(return_value=view),
    ):
        resp = await client.get("/api/v1/workspaces/home/placement")

    assert resp.status_code == 200
    assert resp.json() == view


@pytest.mark.asyncio
async def test_handbacks_name_the_stand_in_and_each_folder_filled(client):
    from src.server.database.home_workspace import get_home_stand_in_id

    through = datetime(2026, 11, 15, 10, tzinfo=timezone.utc)
    rows = [
        {"through": through, "folder": "From the cloud/2026-11-15", "files": ["a.md"]},
        {"through": through, "folder": None, "files": [], "kept": {"a.md": "From the cloud/2026-11-15"}},
    ]
    listed = AsyncMock(return_value=rows)
    with patch(f"{HOME}.list_handbacks", listed):
        resp = await client.get(
            "/api/v1/workspaces/home/handbacks", params={"since": "2026-11-15T09:00:00Z"}
        )
        missing = await client.get("/api/v1/workspaces/home/handbacks")

    assert missing.status_code == 422
    assert resp.status_code == 200
    listed.assert_awaited_once_with(USER, datetime(2026, 11, 15, 9, tzinfo=timezone.utc))
    body = resp.json()
    assert body["stand_in_workspace_id"] == get_home_stand_in_id(USER)
    handback, unchanged = body["handbacks"]
    assert handback["folder"] == "From the cloud/2026-11-15"
    assert handback["files"] == ["a.md"] and handback["kept"] == {}
    assert unchanged["folder"] is None
    assert unchanged["kept"] == {"a.md": "From the cloud/2026-11-15"}
    assert datetime.fromisoformat(handback["through"].replace("Z", "+00:00")) == through


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [{"handback_id": "hb-1"}, None])
async def test_dismissing_the_notice_names_the_one_seen(client, body):
    seen = AsyncMock()
    with patch(f"{HOME}.mark_handback_seen", seen):
        resp = await client.post("/api/v1/workspaces/home/placement/seen", json=body)

    assert resp.status_code == 204
    seen.assert_awaited_once_with(USER, body["handback_id"] if body else None)
