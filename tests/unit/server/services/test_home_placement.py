"""Where a Home turn runs: on Home's computer, or in Home's stand-in on a cloud
computer the user already has while that machine can't be reached.

The decision is read from Postgres at admission; these pin it with the reads
mocked, and pin that nothing here ever builds a computer for the stand-in.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_stand_in_id, get_home_workspace_id
from src.server.services import home_placement

USER = "user-1"
HOME = get_home_workspace_id(USER)
STAND_IN = get_home_stand_in_id(USER)
MAC = {"computer_id": "mac-1", "name": "MacBook Pro", "kind": "ssh", "user_id": USER}
CLOUD = {"computer_id": "cloud-1", "name": "Cloud computer", "kind": "daytona", "user_id": USER}
_P = "src.server.services.home_placement"


def _stand_in(*, runs=True):
    from src.server.database.home_stand_in import StandIn

    return StandIn(
        row={"workspace_id": STAND_IN, "status": "running"},
        computer=CLOUD,
        kept=True,
        runs=runs,
        owed=False,
        left=False,
    )


@pytest.fixture
def rows():
    """What placement reads, and the stand-in it places."""
    state = {"unreachable": True, "picked": CLOUD, "stand_in": None}

    async def place(user_id):
        if state["stand_in"] is None and state["picked"] is not None:
            state["stand_in"] = _stand_in()
        return state["stand_in"]

    with (
        patch(f"{_P}.get_home_computer", AsyncMock(return_value=MAC)),
        patch(f"{_P}.computer_unreachable", AsyncMock(side_effect=lambda c, **_: state["unreachable"] and c is MAC)),
        patch(f"{_P}.stand_in_computer", AsyncMock(side_effect=lambda *a, **k: state["picked"])),
        patch(f"{_P}.place_home_stand_in", side_effect=place) as placed,
        patch(f"{_P}.read_stand_in", AsyncMock(side_effect=lambda *a, **k: state["stand_in"])),
        # A turn marks its user away in its START transaction, never here.
        patch(
            "src.server.database.home_away.mark_away",
            AsyncMock(side_effect=AssertionError("placement marked the user away")),
        ),
        # Nothing here may build a computer: a stand-in only ever uses one the
        # user already has.
        patch(
            "src.server.services.computer_manager._machines.MachineLifecycleMixin.ensure_primary_computer",
            AsyncMock(side_effect=AssertionError("built a computer for the stand-in")),
        ),
        patch(
            "src.server.services.computer_manager._machines.create_computer",
            AsyncMock(side_effect=AssertionError("built a computer for the stand-in")),
        ),
    ):
        state["placed"] = placed
        yield state


@pytest.mark.asyncio
async def test_a_reachable_home_runs_at_home(rows):
    rows["unreachable"] = False

    assert await home_placement.place_home_turn(USER) is None
    rows["placed"].assert_not_called()


@pytest.mark.asyncio
async def test_an_unreachable_home_runs_in_its_stand_in_on_an_existing_cloud(rows):
    away = await home_placement.place_home_turn(USER)

    assert away.workspace_id == STAND_IN
    assert away.reason == "unreachable"
    assert away.computer_name == "MacBook Pro"
    assert away.cloud_name == "Cloud computer"
    assert away.as_metadata()["cloud_computer_id"] == "cloud-1"


@pytest.mark.asyncio
async def test_with_no_cloud_computer_the_turn_stays_on_home_and_nothing_is_built(rows):
    rows["picked"] = None

    assert await home_placement.place_home_turn(USER) is None


@pytest.mark.asyncio
async def test_a_stand_in_whose_computer_takes_no_turn_leaves_the_turn_at_home(rows):
    """Kept where owed files are though that computer failed: no turn runs
    there, so Home's turn goes where Home is, as it was."""
    rows["stand_in"] = _stand_in(runs=False)

    assert await home_placement.place_home_turn(USER) is None


@pytest.mark.asyncio
async def test_work_between_turns_places_nothing_unasked(rows):
    unplaced = await home_placement.runs_in(USER, HOME, make=False)
    placed = await home_placement.runs_in(USER, HOME, make=True)
    again = await home_placement.runs_in(USER, HOME, make=False)
    workspace = await home_placement.runs_in(USER, "ws-on-the-mac", make=True)

    assert unplaced == HOME
    assert placed == STAND_IN == again
    assert workspace == "ws-on-the-mac"


@pytest.mark.asyncio
async def test_a_report_on_a_command_in_the_stand_in_runs_there_once_home_is_back(rows):
    await home_placement.place_home_turn(USER)
    rows["unreachable"] = False

    follows = await home_placement.place_home_turn(USER, follows=STAND_IN)
    elsewhere = await home_placement.place_home_turn(USER, follows="ws-other")

    assert follows.workspace_id == STAND_IN and follows.reason == "command"
    assert elsewhere is None


@pytest.mark.asyncio
async def test_a_placement_that_fails_runs_the_turn_as_it_was(rows):
    rows["placed"].side_effect = RuntimeError("database gone")

    assert await home_placement.place_home_turn(USER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("picked", "waits"), [(CLOUD, False), (None, True)])
async def test_unattended_home_work_waits_only_without_a_cloud_computer(rows, picked, waits):
    from src.server.services import ssh_computers

    rows["picked"] = picked
    with (
        patch.object(ssh_computers, "get_computer_for_workspace", AsyncMock(return_value=MAC)),
        patch.object(ssh_computers, "computer_unreachable", AsyncMock(return_value=True)),
    ):
        away = await ssh_computers.computer_away(HOME)
        workspace_away = await ssh_computers.computer_away("ws-on-the-mac")

    assert (away is not None) is waits
    # A workspace's own turn runs only on its own computer.
    assert workspace_away is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unreachable", "picked", "expected"),
    [(False, CLOUD, (MAC, False)), (True, CLOUD, (CLOUD, True)), (True, None, (MAC, False))],
    ids=["reachable", "stand-in", "no-cloud"],
)
async def test_a_gate_judges_a_home_turn_on_the_computer_it_would_run_on(
    rows, unreachable, picked, expected
):
    rows["unreachable"], rows["picked"] = unreachable, picked

    assert await home_placement.home_turn_computer(USER) == expected
    rows["placed"].assert_not_called()


def _start(away):
    from src.server.services.home_placement import HomeTurnStart

    return HomeTurnStart(workspace_id=HOME, away=away)


def _away():
    from ptc_agent.agent.middleware.runtime_context import HomeAway

    return HomeAway(STAND_IN, "mac-1", "MacBook Pro", "cloud-1", "Cloud computer", at="t")


def test_a_turn_in_the_stand_in_states_it_on_its_rows_and_stream():
    import json

    away = _away()
    start = _start(away)

    assert start.query_metadata() == {"home_fallback": away.as_metadata()}
    assert start.run_metadata() == {"stand_in_computer": "cloud-1"}
    assert start.moved_off == STAND_IN
    # The frame clients read, byte for byte.
    assert start.frame() == (
        "id: 0\nevent: turn_placement\ndata: "
        + json.dumps({"home_fallback": away.as_metadata(), "workspace_id": HOME})
        + "\n\n"
    )


@pytest.mark.asyncio
async def test_a_turn_at_home_adds_nothing_to_its_start():
    start = _start(None)
    then = AsyncMock()

    assert start.query_metadata() == {} and start.run_metadata() == {}
    assert start.moved_off is None and start.frame() is None
    assert start.in_start(USER, then) is then
    assert start.in_start(USER, None) is None


@pytest.mark.asyncio
async def test_a_stand_in_whose_computer_goes_refuses_the_turn_as_busy():
    from fastapi import HTTPException

    from src.server.database.home_away import StandInGone

    then = AsyncMock()
    with patch("src.server.database.home_away.mark_away", AsyncMock(side_effect=StandInGone())):
        with pytest.raises(HTTPException) as refused:
            await _start(_away()).in_start(USER, then)("conn")

    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "busy"
    then.assert_not_awaited()
