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


@pytest.fixture
def rows():
    """The rows placement reads, by workspace id, and what it writes."""
    state = {
        "bound": {HOME: MAC},
        "unreachable": True,
        "picked": CLOUD,
        "stand_in_row": None,
    }

    async def computer_for(workspace_id, conn=None):
        return state["bound"].get(workspace_id)

    async def place(user_id, computer_id):
        computer = {CLOUD["computer_id"]: CLOUD}.get(computer_id)
        if computer is None:
            return None
        state["bound"][STAND_IN] = computer
        row = {"workspace_id": STAND_IN, "status": "running"}
        state["stand_in_row"] = row
        return row

    async def get_workspace(workspace_id):
        return state["stand_in_row"] if workspace_id == STAND_IN else None

    with (
        patch(f"{_P}.get_computer_for_workspace", side_effect=computer_for),
        patch(f"{_P}.computer_unreachable", AsyncMock(side_effect=lambda c, **_: state["unreachable"] and c is MAC)),
        patch(f"{_P}.pick_stand_in_computer", AsyncMock(side_effect=lambda *a, **k: state["picked"])),
        patch("src.server.database.workspace.place_home_stand_in", side_effect=place) as placed,
        patch("src.server.database.workspace.get_workspace", side_effect=get_workspace),
        # A turn marks its user away in its START transaction, never here.
        patch(
            "src.server.database.home_away.mark_away",
            AsyncMock(side_effect=AssertionError("placement marked the user away")),
        ),
        patch(
            "src.server.database.home_away.handback_owed",
            AsyncMock(side_effect=lambda *a, **k: state.get("owed", False)),
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

    placement = await home_placement.place_home_turn(USER)

    assert placement.runs_in == HOME and placement.away is None
    rows["placed"].assert_not_called()


@pytest.mark.asyncio
async def test_an_unreachable_home_runs_in_its_stand_in_on_an_existing_cloud(rows):
    placement = await home_placement.place_home_turn(USER)

    assert placement.runs_in == STAND_IN
    assert placement.away.reason == "unreachable"
    assert placement.away.computer_name == "MacBook Pro"
    assert placement.away.cloud_name == "Cloud computer"
    assert placement.away.as_metadata()["cloud_computer_id"] == "cloud-1"


@pytest.mark.asyncio
async def test_with_no_cloud_computer_the_turn_stays_on_home_and_nothing_is_built(rows):
    rows["picked"] = None

    placement = await home_placement.place_home_turn(USER)

    assert placement.runs_in == HOME and placement.away is None
    rows["placed"].assert_not_called()


@pytest.mark.asyncio
async def test_work_between_turns_places_nothing_unasked(rows):
    unplaced = await home_placement.place_home_turn(USER, make=False)
    placed = await home_placement.place_home_turn(USER, make=True)
    again = await home_placement.place_home_turn(USER, make=False)

    assert unplaced.runs_in == HOME
    assert placed.runs_in == STAND_IN == again.runs_in


@pytest.mark.asyncio
async def test_a_report_on_a_command_in_the_stand_in_runs_there_once_home_is_back(rows):
    await home_placement.place_home_turn(USER)
    rows["unreachable"] = False

    follows = await home_placement.place_home_turn(USER, follows=STAND_IN)
    elsewhere = await home_placement.place_home_turn(USER, follows="ws-other")

    assert follows.runs_in == STAND_IN and follows.away.reason == "command"
    assert elsewhere.runs_in == HOME


@pytest.mark.asyncio
async def test_a_placement_that_fails_runs_the_turn_as_it_was(rows):
    rows["placed"].side_effect = RuntimeError("database gone")

    placement = await home_placement.place_home_turn(USER)

    assert placement.runs_in == HOME and placement.away is None


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
@pytest.mark.parametrize("broken", [{"status": "error"}, {"config": {"retire_when_empty": True}}])
@pytest.mark.parametrize("owed", [False, True])
async def test_a_stand_in_whose_computer_cannot_keep_it_moves_unless_files_are_owed(
    rows, broken, owed
):
    stuck = {"computer_id": "cloud-0", "name": "Old cloud", "kind": "daytona", **broken}
    rows["bound"][STAND_IN] = stuck
    rows["owed"] = owed

    chosen = await home_placement.stand_in_computer(USER)

    # Owed files are only there, so it stays; otherwise a healthy one is used.
    assert chosen is (stuck if owed else CLOUD)
