"""
Tests for the Workspaces API router (src/server/app/workspaces.py).

Covers CRUD operations, start/stop/archive/delete lifecycle actions,
flash workspace, reorder, and ownership guards.

The lifecycle routes are aliases over the project-addressed manager: each
resolves the workspace to its machine inside ``WorkspaceManager`` and runs one
transition there, so these tests lock that the route hands the manager the
workspace id, never the computer id, and answers from what it returns.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"


def _ws(
    workspace_id=None,
    user_id="test-user-123",
    name="Test Workspace",
    status="running",
    **overrides,
):
    """Build a workspace dict matching DB row shape."""
    data = {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": name,
        "description": None,
        "sandbox_id": "sandbox-abc",
        # A project always runs on a machine; a route that addressed the
        # machine instead of the project would reach for this.
        "computer_id": COMPUTER_ID,
        "status": status,
        "mode": "ptc",
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
    from src.server.app.workspaces import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces — create workspace
# ---------------------------------------------------------------------------


def _route_dependency_names(app, path, method):
    for route in app.routes:
        if getattr(route, "path", None) == path and method in getattr(
            route, "methods", ()
        ):
            return {d.call.__name__ for d in route.dependant.dependencies}
    raise AssertionError(f"no {method} {path} route")


@pytest.mark.asyncio
async def test_create_workspace_success(client):
    ws = _ws()
    with patch(
        "src.server.app.workspaces.WorkspaceManager"
    ) as MockWM:
        mock_manager = AsyncMock()
        mock_manager.create_workspace = AsyncMock(return_value=ws)
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            "/api/v1/workspaces",
            json={"name": "Test Workspace"},
        )

    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == "Test Workspace"
    assert body["workspace_id"] == ws["workspace_id"]


@pytest.mark.asyncio
async def test_create_answers_with_the_machine_and_the_folder(client):
    """Both are what the caller needs next: the machine to watch for readiness,
    the folder to address files on it. Re-reading the row to learn them is the
    round trip this route exists to avoid."""
    computer_id = str(uuid.uuid4())
    ws = _ws(
        status="stopped",
        sandbox_id=None,
        computer_id=computer_id,
        dir_name="test-workspace-ab12",
    )
    with patch("src.server.app.workspaces.WorkspaceManager") as MockWM:
        mock_manager = AsyncMock()
        mock_manager.create_workspace = AsyncMock(return_value=ws)
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            "/api/v1/workspaces", json={"name": "Test Workspace"}
        )

    assert resp.status_code == 201
    body = resp.json()
    assert body["computer_id"] == computer_id
    assert body["dir_name"] == "test-workspace-ab12"
    # Nothing was provisioned, so the row is still the machine's stopped shadow.
    assert body["status"] == "stopped"
    assert body["sandbox_id"] is None


@pytest.mark.asyncio
async def test_create_waits_on_no_sandbox(client):
    """The one property that makes creation instant: the route returns on the
    insert, and every sandbox call sits behind the first turn instead."""
    with patch("src.server.app.workspaces.WorkspaceManager") as MockWM:
        mock_manager = AsyncMock()
        mock_manager.create_workspace = AsyncMock(
            return_value=_ws(status="stopped", sandbox_id=None)
        )
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post("/api/v1/workspaces", json={"name": "W"})

    assert resp.status_code == 201
    mock_manager.create_workspace.assert_awaited_once()
    for blocked in (
        "get_session_for_workspace",
        "start_workspace",
        "_recover_sandbox",
    ):
        assert not getattr(mock_manager, blocked).await_count


@pytest.mark.asyncio
async def test_creating_a_project_is_not_capacity_checked():
    """The plan meters computers; this route allocates none, so gating it here
    would cap projects on a machine the user has already paid for."""
    from src.server.app.workspaces import router

    app = create_test_app(router)
    for path in ("/api/v1/workspaces", "/api/v1/workspaces/{workspace_id}/duplicate"):
        names = _route_dependency_names(app, path, "POST")
        assert not {n for n in names if n.startswith("enforce_")} & {
            "enforce_computer_limit",
            "enforce_workspace_limit",
        }


@pytest.mark.asyncio
async def test_create_workspace_value_error_returns_400(client):
    with patch(
        "src.server.app.workspaces.WorkspaceManager"
    ) as MockWM:
        mock_manager = AsyncMock()
        mock_manager.create_workspace = AsyncMock(
            side_effect=ValueError("bad config")
        )
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            "/api/v1/workspaces",
            json={"name": "Bad"},
        )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_workspace_internal_error(client):
    with patch(
        "src.server.app.workspaces.WorkspaceManager"
    ) as MockWM:
        mock_manager = AsyncMock()
        mock_manager.create_workspace = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            "/api/v1/workspaces",
            json={"name": "Fail"},
        )

    assert resp.status_code == 500


@pytest.mark.asyncio
async def test_create_workspace_validation_empty_name(client):
    resp = await client.post(
        "/api/v1/workspaces",
        json={"name": ""},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/flash
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_flash_workspace(client):
    ws = _ws(status="flash")
    with patch(
        "src.server.app.workspaces.get_or_create_flash_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.post("/api/v1/workspaces/flash")

    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == ws["workspace_id"]


@pytest.mark.asyncio
async def test_get_flash_workspace_error(client):
    with patch(
        "src.server.app.workspaces.get_or_create_flash_workspace",
        new_callable=AsyncMock,
        side_effect=RuntimeError("db down"),
    ):
        resp = await client.post("/api/v1/workspaces/flash")

    assert resp.status_code == 500


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/reorder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reorder_workspaces(client):
    ws_id = str(uuid.uuid4())
    with patch(
        "src.server.app.workspaces.batch_update_sort_order",
        new_callable=AsyncMock,
    ) as mock_reorder:
        resp = await client.post(
            "/api/v1/workspaces/reorder",
            json={"items": [{"workspace_id": ws_id, "sort_order": 1}]},
        )

    assert resp.status_code == 204
    mock_reorder.assert_awaited_once()


@pytest.mark.asyncio
async def test_reorder_workspaces_empty_items(client):
    resp = await client.post(
        "/api/v1/workspaces/reorder",
        json={"items": []},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/workspaces — list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_workspaces(client):
    ws1 = _ws(name="WS1")
    ws2 = _ws(name="WS2")
    with patch(
        "src.server.app.workspaces.get_workspaces_for_user",
        new_callable=AsyncMock,
        return_value=([ws1, ws2], 2),
    ):
        resp = await client.get("/api/v1/workspaces")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["workspaces"]) == 2


@pytest.mark.asyncio
async def test_list_workspaces_with_params(client):
    with patch(
        "src.server.app.workspaces.get_workspaces_for_user",
        new_callable=AsyncMock,
        return_value=([], 0),
    ) as mock_list:
        resp = await client.get(
            "/api/v1/workspaces?limit=5&offset=10&sort_by=activity"
        )

    assert resp.status_code == 200
    mock_list.assert_awaited_once_with(
        user_id="test-user-123", limit=5, offset=10, sort_by="activity",
        include_flash=False,
    )


@pytest.mark.asyncio
async def test_list_workspaces_invalid_sort_by(client):
    resp = await client.get("/api/v1/workspaces?sort_by=invalid")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/workspaces/quota
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_workspace_quota_platform(client):
    """Platform mode surfaces per-tier {used, limit}; /quota is not shadowed by /{id}."""

    async def fake_capacity(_user_id, check_quota):
        return {
            "spec_performance": {"used": 1, "limit": 3},
            "spec_max": {"used": 0, "limit": 0},
            "always_on": {"used": 2, "limit": -1},
        }[check_quota]

    with patch(
        "src.server.app.workspaces.get_capacity_status",
        new=AsyncMock(side_effect=fake_capacity),
    ):
        resp = await client.get("/api/v1/workspaces/quota")

    assert resp.status_code == 200
    assert resp.json() == {
        "performance": {"used": 1, "limit": 3},
        "max": {"used": 0, "limit": 0},
        "always_on": {"used": 2, "limit": -1},
    }


@pytest.mark.asyncio
async def test_get_workspace_quota_oss_all_null(client):
    """OSS mode: every capability is None, so the response fields are null."""
    with patch(
        "src.server.app.workspaces.get_capacity_status",
        new=AsyncMock(return_value=None),
    ):
        resp = await client.get("/api/v1/workspaces/quota")

    assert resp.status_code == 200
    assert resp.json() == {"performance": None, "max": None, "always_on": None}


# ---------------------------------------------------------------------------
# GET /api/v1/workspaces/{workspace_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_workspace_success(client):
    ws = _ws()
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}"
        )

    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == ws["workspace_id"]


@pytest.mark.asyncio
async def test_an_incomplete_restore_is_reported_on_the_detail_and_the_list(client):
    """The reader has to be able to tell a file the restore never recovered
    from a file that was never there. The web seeds the detail query from the
    cached list, so both answers carry it or the notice flickers."""
    incomplete = _ws(name="Missing files", files_restore_incomplete=True)
    intact = _ws(name="Whole")

    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=incomplete,
    ):
        detail = await client.get(f"/api/v1/workspaces/{incomplete['workspace_id']}")

    assert detail.status_code == 200
    assert detail.json()["files_restore_incomplete"] is True

    with patch(
        "src.server.app.workspaces.get_workspaces_for_user",
        new_callable=AsyncMock,
        return_value=([incomplete, intact], 2),
    ):
        listed = await client.get("/api/v1/workspaces")

    assert listed.status_code == 200
    assert [w["files_restore_incomplete"] for w in listed.json()["workspaces"]] == [
        True,
        False,
    ]


@pytest.mark.asyncio
async def test_get_workspace_not_found(client):
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_workspace_forbidden(client):
    ws = _ws(user_id="other-user")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}"
        )

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_get_workspace_malformed_id_returns_404(client):
    """A non-UUID id is a clean 404, not a 500.

    Regression: a memory-file key reaching the uuid column raised psycopg
    InvalidTextRepresentation (22P02) in prod, surfaced as a 500. The guard in
    db_get_workspace must short-circuit to None (→ 404) before any DB access.
    """
    resp = await client.get("/api/v1/workspaces/my_notes.md")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_workspace_events_malformed_id_returns_404(client):
    """The /events endpoint has no try/except, so a malformed id used to be a
    raw 500. The db_get_workspace guard makes it a clean 404."""
    resp = await client.get(
        "/api/v1/workspaces/my_notes.md/events"
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/v1/workspaces/{workspace_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_workspace_success(client):
    ws = _ws()
    updated = {**ws, "name": "Updated Name"}
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.workspaces.db_update_workspace",
            new_callable=AsyncMock,
            return_value=updated,
        ),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}",
            json={"name": "Updated Name"},
        )

    assert resp.status_code == 200
    assert resp.json()["name"] == "Updated Name"


@pytest.mark.asyncio
async def test_update_workspace_not_found(client):
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{uuid.uuid4()}",
            json={"name": "X"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_workspace_forbidden(client):
    ws = _ws(user_id="other-user")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}",
            json={"name": "X"},
        )

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_update_workspace_db_returns_none(client):
    ws = _ws()
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.workspaces.db_update_workspace",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}",
            json={"name": "Gone"},
        )

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/{workspace_id}/start
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_workspace_from_stopped(client):
    ws = _ws(status="stopped")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ) as read,
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.get_session_for_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start"
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    # Keyed by the project, so this workspace's folder is what gets attached.
    mock_manager.get_session_for_workspace.assert_awaited_once_with(
        ws["workspace_id"], user_id="test-user-123"
    )
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_start_workspace_already_running(client):
    ws = _ws(status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start"
        )

    assert resp.status_code == 200
    assert "already running" in resp.json()["message"]


@pytest.mark.asyncio
async def test_start_workspace_invalid_state(client):
    ws = _ws(status="creating")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start"
        )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_start_workspace_not_found(client):
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{uuid.uuid4()}/start"
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_start_workspace_forbidden(client):
    ws = _ws(status="stopped", user_id="other-user")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start"
        )

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_start_workspace_lazy_returns_202_and_schedules(client):
    """lazy=true returns 202 with status='starting' and schedules a background task."""
    ws = _ws(status="stopped")

    # Use a long-running coroutine so we can verify the endpoint did NOT await it.
    started_event = asyncio.Event()
    finish_event = asyncio.Event()

    async def slow_get_session(*args, **kwargs):
        started_event.set()
        await finish_event.wait()

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.get_session_for_workspace = AsyncMock(side_effect=slow_get_session)
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start?lazy=true"
        )

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "starting"
        assert body["workspace_id"] == ws["workspace_id"]

        # The background task should have started but not finished. Wait on the
        # event directly rather than a single scheduler tick (asyncio.sleep(0)),
        # which can flake under load if the task needs more than one tick to
        # reach started_event.set().
        await asyncio.wait_for(started_event.wait(), timeout=0.5)

        # Let the task complete so it doesn't leak into other tests.
        finish_event.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_start_workspace_running_attaches_before_it_answers(client):
    """A running row is a fact about the machine, not about this project.

    A project created or duplicated while its machine was up is born running
    with no folder, no files and no tool overlay of its own, all of which the
    first attach materialises. Answering from the row alone sent the caller
    straight to paths nothing had created.
    """
    ws = _ws(status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.get_session_for_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(f"/api/v1/workspaces/{ws['workspace_id']}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_manager.get_session_for_workspace.assert_awaited_once_with(
        ws["workspace_id"], user_id=ANY
    )


@pytest.mark.asyncio
async def test_start_workspace_lazy_running_answers_starting(client):
    """The lazy caller is told the attach is in flight rather than done."""
    ws = _ws(status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
        patch("src.server.app.workspaces._schedule_warm_restart") as mock_schedule,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start?lazy=true"
        )

    assert resp.status_code == 202
    assert resp.json()["status"] == "starting"
    assert mock_schedule.call_args.args[1] == ws["workspace_id"]


@pytest.mark.asyncio
async def test_start_workspace_lazy_already_starting_short_circuits(client):
    """lazy=true on a starting workspace returns 200 without scheduling."""
    ws = _ws(status="starting")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.get_session_for_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start?lazy=true"
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "starting"
    mock_manager.get_session_for_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_workspace_lazy_invalid_state_rejects(client):
    """lazy=true on a non-startable status returns 400."""
    ws = _ws(status="creating")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        MockWM.get_instance.return_value = AsyncMock()

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/start?lazy=true"
        )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_drain_start_tasks_cancels_in_flight_warms():
    """A warm scheduled through the workspace route is cancelled and awaited by
    the shared drain, so a task cancelled mid-Phase-2 can revert its row
    instead of being torn down with the loop."""
    from src.server.app import background_starts, workspaces as ws_mod

    started = asyncio.Event()

    async def never_finishes(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()  # blocks forever until cancelled

    manager = AsyncMock()
    manager.get_session_for_workspace = AsyncMock(side_effect=never_finishes)
    ws_mod._schedule_warm_restart(manager, "ws-drain", "user-1")
    task = background_starts._start_tasks["workspace:ws-drain"]
    await started.wait()

    await background_starts.drain_start_tasks()

    assert task.cancelled()
    assert "workspace:ws-drain" not in background_starts._start_tasks


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/{workspace_id}/always-on
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_always_on_enable_on_stopped_starts_immediately(client):
    """Enabling always-on on a stopped workspace warms it now, returns 'starting'."""
    ws = _ws(status="stopped")
    persisted = _ws(status="stopped", is_always_on=True)

    started_event = asyncio.Event()
    finish_event = asyncio.Event()

    async def slow_get_session(*args, **kwargs):
        started_event.set()
        await finish_event.wait()

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.workspaces.assert_always_on_allowed",
            new_callable=AsyncMock,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.set_workspace_always_on = AsyncMock(return_value=persisted)
        mock_manager.get_session_for_workspace = AsyncMock(side_effect=slow_get_session)
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/always-on",
            json={"enabled": True},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["is_always_on"] is True
        # Response optimistically reflects the start kicked off this request.
        assert body["status"] == "starting"
        mock_manager.set_workspace_always_on.assert_awaited_once_with(
            ws["workspace_id"], True
        )

        # The warm start was scheduled in the background, not awaited inline.
        await asyncio.wait_for(started_event.wait(), timeout=0.5)
        finish_event.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_always_on_enable_on_running_does_not_start(client):
    """Enabling always-on on a running workspace stays running, no warm start."""
    ws = _ws(status="running")
    persisted = _ws(status="running", is_always_on=True)

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.workspaces.assert_always_on_allowed",
            new_callable=AsyncMock,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.set_workspace_always_on = AsyncMock(return_value=persisted)
        mock_manager.get_session_for_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/always-on",
            json={"enabled": True},
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_manager.get_session_for_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_always_on_disable_does_not_start(client):
    """Disabling always-on never starts the sandbox (and is never gated)."""
    ws = _ws(status="stopped", is_always_on=True)
    persisted = _ws(status="stopped", is_always_on=False)

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.workspaces.assert_always_on_allowed",
            new_callable=AsyncMock,
        ) as mock_gate,
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.set_workspace_always_on = AsyncMock(return_value=persisted)
        mock_manager.get_session_for_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/always-on",
            json={"enabled": False},
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_manager.set_workspace_always_on.assert_awaited_once_with(
        ws["workspace_id"], False
    )
    mock_manager.get_session_for_workspace.assert_not_awaited()
    mock_gate.assert_not_awaited()  # disable is ungated


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/{workspace_id}/duplicate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_workspace_forbidden_for_non_owner(client):
    """Ownership is enforced at the router (403), matching the sibling endpoints."""
    ws = _ws(user_id="other-user", status="stopped")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/duplicate"
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_duplicate_workspace_returns_created_copy(client):
    ws = _ws(status="stopped")
    copy = _ws(status="stopped")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.duplicate_workspace = AsyncMock(return_value=copy)
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/duplicate"
        )

    assert resp.status_code == 201
    assert resp.json()["workspace_id"] == copy["workspace_id"]
    mock_manager.duplicate_workspace.assert_awaited_once()


# ---------------------------------------------------------------------------
# GET /api/v1/workspaces/{workspace_id}/events — SSE status stream
# ---------------------------------------------------------------------------


def _parse_sse(buffer: str):
    """Yield (event_name, data) tuples from an SSE wire buffer."""
    for chunk in buffer.split("\n\n"):
        if not chunk.strip():
            continue
        event_name = ""
        data = ""
        for line in chunk.split("\n"):
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                data += line[5:].strip()
        yield event_name, data


async def _collect_sse_events(client, url, *, want_events: int, timeout: float = 2.0):
    """Open an SSE stream and collect up to `want_events` events, then close."""
    import json as _json

    events: list[tuple[str, dict]] = []
    async with client.stream("GET", url) as resp:
        assert resp.status_code == 200
        buffer = ""

        async def _read_loop():
            nonlocal buffer
            async for chunk in resp.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    raw, _, buffer = buffer.partition("\n\n")
                    for name, data in _parse_sse(raw + "\n\n"):
                        if name == "status" and data:
                            try:
                                events.append((name, _json.loads(data)))
                            except Exception as exc:
                                # Fail fast — a malformed payload is a real
                                # serialization regression, not something to mask.
                                raise AssertionError(
                                    f"Invalid SSE JSON payload for 'status': {data!r}"
                                ) from exc
                        elif name == "timeout":
                            events.append((name, {}))
                    if len(events) >= want_events:
                        return

        try:
            await asyncio.wait_for(_read_loop(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
    return events


@pytest.mark.asyncio
async def test_workspace_events_emits_initial_status_then_pubsub_transition(client):
    """SSE endpoint sends current status immediately, then each pub/sub transition."""
    from contextlib import asynccontextmanager

    ws = _ws(status="starting")
    ws_running = {**ws, "status": "running"}
    # 1st DB read (initial event), 2nd DB read (post-subscribe), 3rd DB read after notify
    db_seq = iter([ws, ws, ws_running])

    async def fake_db(workspace_id, conn=None):
        try:
            return next(db_seq)
        except StopIteration:
            return ws_running

    @asynccontextmanager
    async def fake_subscribe(workspace_id, *, computer_id=None):
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return ("message", {"workspace_id": workspace_id, "status": "running"})
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new=AsyncMock(side_effect=fake_db),
        ),
        patch(
            "src.server.app.workspaces.subscribe_to_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/workspaces/{ws['workspace_id']}/events",
            want_events=2,
            timeout=2.0,
        )

    statuses = [e[1].get("status") for e in events if e[0] == "status"]
    assert "starting" in statuses
    assert "running" in statuses
    # Running is terminal — stream closes immediately after, no further events.


@pytest.mark.asyncio
async def test_workspace_events_forwards_archived_sandbox_state(client):
    """A pub/sub hint carrying sandbox_state='archived' during the 'starting'
    phase is forwarded as a refinement event (no DB re-read, stream stays open)
    so the FE can escalate to the slow-restore spinner even when a background
    warm — not this client — owns the start."""
    from contextlib import asynccontextmanager

    ws = _ws(status="starting")
    ws_running = {**ws, "status": "running"}
    # initial read, post-subscribe read; the archived refinement does NOT
    # re-read; the running transition re-reads.
    db_seq = iter([ws, ws, ws_running])

    async def fake_db(workspace_id, conn=None):
        try:
            return next(db_seq)
        except StopIteration:
            return ws_running

    @asynccontextmanager
    async def fake_subscribe(workspace_id, *, computer_id=None):
        msgs = iter(
            [
                {"workspace_id": workspace_id, "status": "starting",
                 "sandbox_state": "archived"},
                {"workspace_id": workspace_id, "status": "running"},
            ]
        )

        async def wait(timeout):
            payload = next(msgs, None)
            return ("message", payload) if payload else ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new=AsyncMock(side_effect=fake_db),
        ),
        patch(
            "src.server.app.workspaces.subscribe_to_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/workspaces/{ws['workspace_id']}/events",
            want_events=3,
            timeout=2.0,
        )

    status_events = [e[1] for e in events if e[0] == "status"]
    # The archived refinement carries status 'starting' + sandbox_state.
    assert any(
        e.get("status") == "starting" and e.get("sandbox_state") == "archived"
        for e in status_events
    )
    assert any(e.get("status") == "running" for e in status_events)


@pytest.mark.asyncio
async def test_workspace_events_terminates_on_initial_running(client):
    """If the workspace is already running, the stream emits one event then closes."""
    from contextlib import asynccontextmanager

    ws = _ws(status="running")

    @asynccontextmanager
    async def fake_subscribe(workspace_id, *, computer_id=None):
        async def wait(timeout):
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new=AsyncMock(return_value=ws),
        ),
        patch(
            "src.server.app.workspaces.subscribe_to_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/workspaces/{ws['workspace_id']}/events",
            want_events=1,
            timeout=1.0,
        )

    assert events[0][0] == "status"
    assert events[0][1].get("status") == "running"


@pytest.mark.asyncio
async def test_workspace_events_forbidden(client):
    """SSE endpoint enforces ownership."""
    ws = _ws(user_id="other-user", status="stopped")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/events"
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_workspace_events_subscribes_by_computer_and_frames_both_ids(client):
    """The route subscribes on the machine's channel -- where every writer
    publishes -- and names both ids in each frame, so a client holding only a
    workspace id learns its computer from the stream."""
    from contextlib import asynccontextmanager

    ws = _ws(status="starting", computer_id="comp-9")
    ws_running = {**ws, "status": "running"}
    db_seq = iter([ws, ws, ws_running])
    subscribed: list[tuple[str, str | None]] = []

    async def fake_db(workspace_id, conn=None):
        try:
            return next(db_seq)
        except StopIteration:
            return ws_running

    @asynccontextmanager
    async def fake_subscribe(workspace_id, *, computer_id=None):
        subscribed.append((workspace_id, computer_id))
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return ("message", {"computer_id": computer_id, "status": "running"})
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new=AsyncMock(side_effect=fake_db),
        ),
        patch("src.server.app.workspaces.subscribe_to_status", new=fake_subscribe),
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/workspaces/{ws['workspace_id']}/events",
            want_events=2,
            timeout=2.0,
        )

    assert subscribed == [(ws["workspace_id"], "comp-9")]
    status_events = [e[1] for e in events if e[0] == "status"]
    assert all(e["computer_id"] == "comp-9" for e in status_events)
    assert all(e["workspace_id"] == ws["workspace_id"] for e in status_events)
    assert [e["status"] for e in status_events] == ["starting", "running"]


@pytest.mark.asyncio
async def test_workspace_events_falls_back_to_the_workspace_channel(client):
    """A workspace with no machine keeps its own channel, and frames a null
    computer_id rather than dropping the key."""
    from contextlib import asynccontextmanager

    ws = _ws(status="stopped", computer_id=None)
    ws_running = {**ws, "status": "running"}
    db_seq = iter([ws, ws, ws_running])
    subscribed: list[tuple[str, str | None]] = []

    async def fake_db(workspace_id, conn=None):
        try:
            return next(db_seq)
        except StopIteration:
            return ws_running

    @asynccontextmanager
    async def fake_subscribe(workspace_id, *, computer_id=None):
        subscribed.append((workspace_id, computer_id))
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return ("message", {"workspace_id": workspace_id, "status": "running"})
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new=AsyncMock(side_effect=fake_db),
        ),
        patch("src.server.app.workspaces.subscribe_to_status", new=fake_subscribe),
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/workspaces/{ws['workspace_id']}/events",
            want_events=2,
            timeout=2.0,
        )

    assert subscribed == [(ws["workspace_id"], None)]
    status_events = [e[1] for e in events if e[0] == "status"]
    assert all(e["computer_id"] is None for e in status_events)
    assert [e["status"] for e in status_events] == ["stopped", "running"]


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/{workspace_id}/stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_addresses_this_project_and_runs_one_transition(client):
    """The alias hands the manager the workspace id: resolving the machine is
    the manager's job, and a route that stopped the computer itself would be a
    second implementation of the same transition."""
    ws = _ws(status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ) as read,
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.stop_workspace.return_value = {**ws, "status": "stopped"}
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/stop"
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_manager.stop_workspace.assert_awaited_once_with(ws["workspace_id"])
    mock_manager.stop_computer.assert_not_awaited()
    # One read for ownership; the transition is the manager's alone.
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_stop_workspace_not_found(client):
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.post(f"/api/v1/workspaces/{uuid.uuid4()}/stop")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stop_workspace_forbidden(client):
    ws = _ws(user_id="other-user")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/stop"
        )

    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /api/v1/workspaces/{workspace_id}/archive
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_addresses_this_project_and_runs_one_transition(client):
    ws = _ws(status="stopped")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ) as read,
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.archive_workspace.return_value = ws
        MockWM.get_instance.return_value = mock_manager

        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/archive"
        )

    assert resp.status_code == 200
    assert resp.json()["message"] == "Workspace archived successfully"
    mock_manager.archive_workspace.assert_awaited_once_with(ws["workspace_id"])
    mock_manager.archive_computer.assert_not_awaited()
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_archive_workspace_not_found(client):
    """Archive re-raises require_workspace_owner's 404 (via the shared
    _workspace_action_errors context manager), not masked as a 500."""
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{uuid.uuid4()}/archive"
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_archive_workspace_forbidden(client):
    """Archive re-raises require_workspace_owner's 403 for a non-owner."""
    ws = _ws(user_id="other-user", status="stopped")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/archive"
        )

    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# DELETE /api/v1/workspaces/{workspace_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_workspace_success(client):
    ws = _ws(status="stopped")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        mock_manager = AsyncMock()
        mock_manager.delete_workspace = AsyncMock()
        MockWM.get_instance.return_value = mock_manager

        resp = await client.delete(
            f"/api/v1/workspaces/{ws['workspace_id']}"
        )

    assert resp.status_code == 204
    mock_manager.delete_workspace.assert_awaited_once_with(ws["workspace_id"])


@pytest.mark.asyncio
async def test_delete_flash_workspace_blocked(client):
    ws = _ws(status="flash")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.delete(
            f"/api/v1/workspaces/{ws['workspace_id']}"
        )

    assert resp.status_code == 400
    assert "flash" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_delete_workspace_not_found(client):
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.delete(f"/api/v1/workspaces/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_workspace_forbidden(client):
    ws = _ws(user_id="other-user")
    with patch(
        "src.server.app.workspaces.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.delete(
            f"/api/v1/workspaces/{ws['workspace_id']}"
        )

    assert resp.status_code == 403
