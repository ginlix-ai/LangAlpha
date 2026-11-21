"""The browser routes' one setup: who may ask, whether there is a browser, and
which sandbox, if any, a route reaches, with the answers the panel reads."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.app import workspace_browser as module
from src.server.services.browser_view import BrowserView
from src.server.services.preview_capabilities import PreviewProxyNotConfigured
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

USER = "test-user-123"
WS = "ws-browser"
BASE = f"/api/v1/workspaces/{WS}/sandbox"
SANDBOX = SimpleNamespace(sandbox_id="sb-1")

CONTROL = ("/browser-control", {"holder": "view-1", "action": "take"})
NAVIGATE = ("/browser-navigate", {"holder": "view-1", "action": "back"})


def _reach(owner_id: str = USER) -> AsyncMock:
    return AsyncMock(return_value=({"workspace_id": WS, "user_id": owner_id}, "owner"))


MANAGER = SimpleNamespace(keep_sandbox_alive=AsyncMock(return_value=True))


def _setup(
    *, available: bool = True, running: object = SANDBOX, reach: AsyncMock | None = None, on: bool = True
):
    acquire = AsyncMock(return_value=(SANDBOX, {"workspace_id": WS}))
    if_running = AsyncMock(return_value=running)
    MANAGER.keep_sandbox_alive.reset_mock()
    patches = (
        patch.object(module, "reached_workspace_on_computer", reach or _reach()),
        patch.object(module, "user_feature_enabled", AsyncMock(return_value=on)),
        patch.object(module, "browser_available", AsyncMock(return_value=available)),
        patch.object(module, "acquire_sandbox", acquire),
        patch.object(module, "sandbox_if_running", if_running),
        patch.object(module.WorkspaceManager, "get_instance", return_value=MANAGER),
    )
    return patches, acquire, if_running


async def _post(path: str, body: dict, *patches):
    app = create_test_app(module.router)
    for p in patches:
        p.start()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            return await client.post(f"{BASE}{path}", json=body)
    finally:
        for p in patches:
            p.stop()


@pytest.mark.parametrize("start", [False, True])
async def test_a_computer_with_no_browser_is_never_reached(start: bool):
    patches, acquire, if_running = _setup(available=False)

    view = await _post("/browser-view", {"start": start}, *patches)
    control = await _post(*CONTROL, *patches)
    navigate = await _post(*NAVIGATE, *patches)

    assert view.json() == {"state": "unavailable", "url": None, "expires_in": None}
    assert control.json() == {"held": False}
    assert navigate.json() == {"done": False, "reason": "not_running"}
    acquire.assert_not_awaited()
    if_running.assert_not_awaited()


@pytest.mark.parametrize("start", [False, True])
async def test_a_person_who_has_not_turned_the_browser_on_is_shown_none(start: bool):
    """Each person's own switch, read for whoever calls: in a shared workspace
    the owner having it on says nothing about a member."""
    patches, acquire, if_running = _setup(on=False)
    with (
        patch.object(module, "browser_control", AsyncMock(return_value=True)) as hold,
        patch.object(module, "browser_navigate", AsyncMock(return_value=None)) as go,
    ):
        view = await _post("/browser-view", {"start": start}, *patches)
        control = await _post(*CONTROL, *patches)
        navigate = await _post(*NAVIGATE, *patches)

    assert view.json() == {"state": "unavailable", "url": None, "expires_in": None}
    assert control.json() == {"held": False}
    assert navigate.json() == {"done": False, "reason": "not_running"}
    acquire.assert_not_awaited()
    if_running.assert_not_awaited()
    hold.assert_not_awaited()
    go.assert_not_awaited()


async def test_a_stopped_computer_is_read_as_stopped_and_never_started():
    patches, acquire, _ = _setup(running=None)

    view = await _post("/browser-view", {}, *patches)
    control = await _post(*CONTROL, *patches)
    navigate = await _post(*NAVIGATE, *patches)

    assert (view.status_code, view.json()) == (
        200,
        {"state": "stopped", "url": None, "expires_in": None},
    )
    assert control.json() == {"held": False}
    assert navigate.json() == {"done": False, "reason": "not_running"}
    acquire.assert_not_awaited()


async def test_only_a_start_acquires_and_access_is_checked_once():
    reach = _reach()
    patches, acquire, if_running = _setup(reach=reach)
    view = AsyncMock(return_value=BrowserView("running", "https://p.example.com/b/t", 3600))

    with patch.object(module, "browser_view", view):
        resp = await _post("/browser-view", {"start": True}, *patches)

    assert resp.json() == {"state": "running", "url": "https://p.example.com/b/t", "expires_in": 3600}
    reach.assert_awaited_once_with(WS, USER)
    acquire.assert_awaited_once_with(WS, USER, "browser_view")
    if_running.assert_not_awaited()
    assert view.await_args.args[0] is SANDBOX
    assert view.await_args.kwargs == {"start": True, "viewer_id": None}


async def test_a_shared_workspaces_member_gets_a_stream_minted_for_them():
    """A member's stream ends once they may no longer use the workspace, as
    their previews do, so it is minted for them, not for the holder."""
    patches, _, _ = _setup(reach=_reach("shared-team-ws"))
    view = AsyncMock(return_value=BrowserView("running", "https://p.example.com/b/t", 3600))

    with patch.object(module, "browser_view", view):
        await _post("/browser-view", {}, *patches)

    assert view.await_args.kwargs == {"start": False, "viewer_id": USER}


async def test_a_running_computer_is_acted_on_where_it_runs():
    patches, acquire, _ = _setup()
    with (
        patch.object(module, "browser_control", AsyncMock(return_value=True)) as control,
        patch.object(module, "browser_navigate", AsyncMock(return_value="not_held")) as navigate,
    ):
        held = await _post(*CONTROL, *patches)
        went = await _post(*NAVIGATE, *patches)

    assert held.json() == {"held": True}
    assert went.json() == {"done": False, "reason": "not_held"}
    assert control.await_args.args[0] is SANDBOX
    assert navigate.await_args.kwargs == {"holder": "view-1", "action": "back", "address": None}
    acquire.assert_not_awaited()


@pytest.mark.parametrize(("action", "held", "kept"), [
    ("take", True, True), ("renew", True, True), ("renew", False, False), ("release", False, False),
])
async def test_a_page_held_keeps_its_computer_from_the_idle_sweep(action, held, kept):
    """A stream straight from the provider passes no proxy that counts it, so a
    person using the page would otherwise have the computer stopped under them."""
    patches, _, _ = _setup()
    with patch.object(module, "browser_control", AsyncMock(return_value=held)):
        resp = await _post("/browser-control", {"holder": "view-1", "action": action}, *patches)
    assert resp.json() == {"held": held}
    assert MANAGER.keep_sandbox_alive.await_count == (1 if kept else 0)
    if kept:
        assert MANAGER.keep_sandbox_alive.await_args.args == (WS, "sb-1")


async def test_a_heartbeat_that_fails_leaves_the_hold_as_it_is():
    patches, _, _ = _setup()
    MANAGER.keep_sandbox_alive.side_effect = RuntimeError("db down")
    try:
        with patch.object(module, "browser_control", AsyncMock(return_value=True)):
            resp = await _post(*CONTROL, *patches)
    finally:
        MANAGER.keep_sandbox_alive.side_effect = None
    assert resp.json() == {"held": True}


@pytest.mark.parametrize(
    ("raised", "status"),
    [
        (HTTPException(status_code=503, detail="Sandbox is not reachable"), 503),
        (PreviewProxyNotConfigured("no proxy"), 501),
        (RuntimeError("boom"), 500),
    ],
    ids=["start-failed", "no-proxy", "anything-else"],
)
async def test_a_view_that_fails_answers_as_it_always_has(raised, status):
    patches, _, _ = _setup()
    with patch.object(module, "browser_view", AsyncMock(side_effect=raised)):
        resp = await _post("/browser-view", {"start": True}, *patches)
    assert resp.status_code == status


async def test_a_hand_back_passes_on_what_the_person_still_had_down():
    patches, _, _ = _setup()
    releases = [{"key": "Shift"}, {"button": "right", "x": 10.5, "y": 2}]
    body = {"holder": "view-1", "action": "release", "releases": releases}
    with patch.object(module, "browser_control", AsyncMock(return_value=False)) as control:
        resp = await _post("/browser-control", body, *patches)
    assert resp.json() == {"held": False}
    assert control.await_args.kwargs == {"holder": "view-1", "action": "release", "releases": releases}


async def test_a_hand_back_the_machine_did_not_answer_fails_so_the_viewer_asks_again():
    patches, _, _ = _setup()
    body = {"holder": "view-1", "action": "release", "releases": [{"key": "Shift"}]}
    with patch.object(module, "browser_control", AsyncMock(return_value=None)):
        resp = await _post("/browser-control", body, *patches)
    assert resp.status_code == 503
    MANAGER.keep_sandbox_alive.assert_not_awaited()


@pytest.mark.parametrize(
    "releases",
    [[{"key": "--session"}], [{"button": "back", "x": 1, "y": 1}], [{"key": "a"}] * 17, [{"text": "a"}]],
)
async def test_a_release_the_engine_could_not_let_go_of_is_refused(releases):
    patches, _, _ = _setup()
    body = {"holder": "view-1", "action": "release", "releases": releases}
    with patch.object(module, "browser_control", AsyncMock(return_value=False)) as control:
        resp = await _post("/browser-control", body, *patches)
    assert resp.status_code == 422
    control.assert_not_awaited()


async def test_another_users_workspace_is_refused_before_anything_is_read():
    reach = AsyncMock(side_effect=HTTPException(status_code=403, detail="Forbidden"))
    patches, acquire, if_running = _setup(reach=reach)

    for path, body in [("/browser-view", {"start": True}), CONTROL, NAVIGATE]:
        resp = await _post(path, body, *patches)
        assert resp.status_code == 403
    acquire.assert_not_awaited()
    if_running.assert_not_awaited()
