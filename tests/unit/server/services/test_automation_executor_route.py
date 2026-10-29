"""Where an automation's turn runs: the row it names, routed as any turn's."""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.automation_executor import AutomationExecutor, _Place, _turn_place
from src.server.services.turn_runtime import turn_route

_USER = "user-1"
_WS = "11111111-1111-1111-1111-111111111111"
_THREAD = "22222222-2222-2222-2222-222222222222"
_HOME = get_home_workspace_id(_USER)
_PROJECT = {"workspace_id": _WS, "user_id": _USER, "status": "running"}
_HOME_ROW = {"workspace_id": _HOME, "user_id": _USER, "status": "unbound"}


def _automation(**overrides):
    return {
        "user_id": _USER,
        "workspace_id": _HOME,
        "thread_strategy": "continue",
        "conversation_thread_id": _THREAD,
        **overrides,
    }


async def _route(automation, *, thread_meta=None, workspace=None):
    with (
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new=AsyncMock(return_value=thread_meta),
        ),
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(return_value=workspace),
        ),
    ):
        route = turn_route(_USER, (await _turn_place(automation)).workspace)
    return route.workspace_id, route.role


@pytest.mark.asyncio
async def test_continuing_a_workspace_thread_runs_that_workspaces_analyst():
    assert await _route(
        _automation(workspace_id=_WS),
        thread_meta={"user_id": _USER, "workspace_id": _WS},
        workspace=_PROJECT,
    ) == (_WS, "analyst")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "automation, thread_meta",
    [
        (_automation(), {"user_id": _USER, "workspace_id": _HOME}),
        (_automation(conversation_thread_id=None), None),
        (_automation(thread_strategy="new"), {"user_id": _USER, "workspace_id": _WS}),
    ],
    ids=["home-thread", "not-pinned-yet", "new-thread-each-run"],
)
async def test_any_other_firing_of_a_home_automation_runs_in_home(automation, thread_meta):
    assert await _route(
        automation, thread_meta=thread_meta, workspace=_HOME_ROW
    ) == (_HOME, "chief_of_staff")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace, expected",
    [(_PROJECT, (_WS, "analyst")), (_HOME_ROW, (_HOME, "chief_of_staff"))],
    ids=["workspace", "chief-of-staff-home"],
)
async def test_a_firing_runs_in_its_stored_workspace(workspace, expected):
    automation = _automation(workspace_id=workspace["workspace_id"])
    assert await _route(automation, workspace=workspace) == expected


@pytest.mark.asyncio
async def test_a_firing_whose_workspace_is_gone_fails_before_any_thread():
    with pytest.raises(ValueError, match="may have been deleted"):
        await _route(_automation(workspace_id=_WS), workspace=None)



# One rule for every row: its stored workspace, continuing the pin only there.

_OTHER_WS = "33333333-3333-3333-3333-333333333333"
_OTHER = {"workspace_id": _OTHER_WS, "user_id": _USER, "status": "running"}


async def _place_by_id(automation, *, thread_meta, rows):
    """As ``_route``, with each workspace read answered by its id, and the
    thread the run continues (None starts one)."""
    with (
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new=AsyncMock(return_value=thread_meta),
        ),
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(side_effect=lambda ws_id: rows.get(ws_id)),
        ),
    ):
        place = await _turn_place(automation)
    route = turn_route(_USER, place.workspace)
    return route.workspace_id, route.role, place.thread_id


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", [_WS, _HOME], ids=["workspace", "home"])
@pytest.mark.parametrize(
    "thread_meta",
    [
        {"user_id": _USER, "workspace_id": _OTHER_WS},
        {"user_id": _USER, "workspace_id": "44444444-4444-4444-4444-444444444444"},
        {"user_id": "someone-else", "workspace_id": _WS},
    ],
    ids=["another-workspace", "a-deleted-workspace", "another-account"],
)
async def test_a_pin_outside_the_stored_workspace_starts_a_thread_there(stored, thread_meta):
    """The automation runs where it lives; only a thread there is continued."""
    rows = {_WS: _PROJECT, _HOME: _HOME_ROW, _OTHER_WS: _OTHER}
    expected = (_WS, "analyst") if stored == _WS else (_HOME, "chief_of_staff")
    assert await _place_by_id(
        _automation(workspace_id=stored), thread_meta=thread_meta, rows=rows
    ) == (*expected, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "automation, expected",
    [
        (_automation(workspace_id=_WS), (_WS, "analyst")),
        (_automation(workspace_id=_HOME), (_HOME, "chief_of_staff")),
    ],
    ids=["workspace-row", "home-row"],
)
async def test_a_pin_in_the_stored_workspace_is_continued(automation, expected):
    thread_meta = {"user_id": _USER, "workspace_id": automation["workspace_id"]}
    assert await _place_by_id(
        automation, thread_meta=thread_meta, rows={_WS: _PROJECT, _HOME: _HOME_ROW}
    ) == (*expected, _THREAD)


@pytest.mark.asyncio
async def test_a_pinned_firing_whose_workspace_is_gone_still_fails():
    automation = _automation(workspace_id=_WS)
    with pytest.raises(ValueError, match="may have been deleted"):
        await _place_by_id(
            automation,
            thread_meta={"user_id": _USER, "workspace_id": _OTHER_WS},
            rows={_HOME: _HOME_ROW},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "automation, expected",
    [
        (_automation(workspace_id=_WS), (_WS, "analyst")),
        (_automation(workspace_id=_HOME), (_HOME, "chief_of_staff")),
    ],
    ids=["workspace-row", "home-row"],
)
async def test_a_pinned_thread_that_is_gone_is_made_again_where_the_row_lives(automation, expected):
    assert await _place_by_id(
        automation, thread_meta=None, rows={_WS: _PROJECT, _HOME: _HOME_ROW}
    ) == (*expected, _THREAD)


@pytest.mark.asyncio
async def test_a_run_that_leaves_its_pin_behind_pins_the_thread_it_starts():
    """So the next run continues the new thread, not the one it left."""
    executor = AutomationExecutor()
    automation = _automation(workspace_id=_WS, automation_id="auto-1")
    with (
        patch("src.server.services.automation_executor.auto_db") as auto_db,
        patch.object(executor, "_precreate_titled_thread", new=AsyncMock()),
    ):
        auto_db.update_automation = AsyncMock()
        thread_id = await executor._resolve_thread(automation, _Place(_PROJECT, None), _WS)

    assert thread_id != _THREAD
    auto_db.update_automation.assert_awaited_once_with(
        "auto-1", _USER, conversation_thread_id=thread_id
    )
