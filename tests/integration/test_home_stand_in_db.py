"""Home's stand-in, on a real Postgres.

Where a Home turn runs while the machine Home lives on can't be reached: a
hidden workspace row on one of the user's cloud computers, at a fixed id. Its
placement converges by that id across workers, it never moves, and listings
leave it out, all decided in SQL.
"""

import asyncio
import uuid

import pytest

from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer, update_computer_status
from src.server.database.home_workspace import (
    get_home_stand_in_id,
    get_home_workspace_id,
    is_home_stand_in_row,
)
from src.server.database.workspace_move import MoveRefused
from src.server.services.workspace_move import claim_workspace_move

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _computer(user_id, name, *, primary=False, kind="docker") -> str:
    row = await create_computer(
        user_id,
        kind=kind,
        name=name,
        is_primary=primary,
        status="running",
        provider_ref=f"stand-in-test-{uuid.uuid4()}",
    )
    return str(row["computer_id"])


async def test_the_stand_in_is_placed_once_at_its_fixed_id(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")

    placed = await workspaces.place_home_stand_in(user_id, cloud)
    again = await workspaces.place_home_stand_in(user_id, other)

    assert str(placed["workspace_id"]) == get_home_stand_in_id(user_id)
    assert str(placed["computer_id"]) == cloud
    assert is_home_stand_in_row(placed)
    # Never Home's folder, which Home takes should this computer be primary.
    assert placed["dir_name"].startswith(".home-stand-in")
    # Placed once, it stays where its files are.
    assert str(again["computer_id"]) == cloud


async def test_workers_placing_it_at_once_converge(seed_user):
    user_id = seed_user["user_id"]
    first = await _computer(user_id, "First", primary=True)
    second = await _computer(user_id, "Second")

    rows = await asyncio.gather(
        workspaces.place_home_stand_in(user_id, first),
        workspaces.place_home_stand_in(user_id, second),
    )

    assert len({str(r["computer_id"]) for r in rows}) == 1
    assert {str(r["workspace_id"]) for r in rows} == {get_home_stand_in_id(user_id)}


async def test_a_stand_in_whose_computer_is_gone_is_placed_again(seed_user):
    user_id = seed_user["user_id"]
    gone = await _computer(user_id, "Gone")
    cloud = await _computer(user_id, "Cloud", primary=True)
    await workspaces.place_home_stand_in(user_id, gone)
    await update_computer_status(gone, "deleted")

    placed = await workspaces.place_home_stand_in(user_id, cloud)

    assert str(placed["computer_id"]) == cloud
    assert placed["status"] != "deleted"


async def test_a_computer_being_retired_takes_no_stand_in(seed_user, db_conn):
    user_id = seed_user["user_id"]
    retiring = await _computer(user_id, "Retiring")
    await db_conn.execute(
        "UPDATE computers SET config = '{\"retire_when_empty\": true}' "
        "WHERE computer_id = %s",
        (retiring,),
    )

    assert await workspaces.place_home_stand_in(user_id, retiring) is None


async def test_listings_and_counts_leave_the_stand_in_out(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    await workspaces.create_workspace_on_computer(user_id, "Research", cloud)
    await workspaces.get_or_create_home_workspace(user_id)
    await workspaces.place_home_stand_in(user_id, cloud)

    listed, total = await workspaces.get_workspaces_for_user(user_id)
    with_home, _ = await workspaces.get_workspaces_for_user(user_id, include_home=True)

    assert [w["name"] for w in listed] == ["Research"] and total == 1
    assert get_home_stand_in_id(user_id) not in {str(w["workspace_id"]) for w in with_home}
    assert get_home_workspace_id(user_id) in {str(w["workspace_id"]) for w in with_home}
    assert await workspaces.count_live_workspaces_by_computer([cloud]) == {cloud: 1}
    assert len(await workspaces.get_live_workspace_ids_for_computer(cloud)) == 2
    assert len(
        await workspaces.get_live_workspace_ids_for_computer(cloud, exclude_stand_in=True)
    ) == 1


async def test_the_stand_in_never_moves(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")
    await workspaces.place_home_stand_in(user_id, cloud)

    with pytest.raises(MoveRefused) as refused:
        await claim_workspace_move(
            user_id=user_id,
            workspace_id=get_home_stand_in_id(user_id),
            target_computer_id=other,
        )
    assert refused.value.code == "not_movable"
