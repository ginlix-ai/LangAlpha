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


@pytest.mark.parametrize(
    "breaks",
    [
        "UPDATE computers SET status = 'error' WHERE computer_id = %s",
        "UPDATE computers SET config = '{\"retire_when_empty\": true}' WHERE computer_id = %s",
    ],
    ids=["failed", "retiring"],
)
@pytest.mark.parametrize("owed", [False, True], ids=["handed-back", "owed"])
async def test_a_stand_in_whose_computer_cannot_keep_it_moves_unless_owed(
    seed_user, db_conn, breaks, owed
):
    user_id = seed_user["user_id"]
    broken = await _computer(user_id, "Broken")
    healthy = await _computer(user_id, "Healthy")
    await workspaces.place_home_stand_in(user_id, broken)
    await db_conn.execute(breaks, (broken,))
    if owed:
        await db_conn.execute(
            "INSERT INTO home_away (user_id, home_id, away_since, last_away_at) "
            "VALUES (%s, %s, NOW(), NOW())",
            (user_id, get_home_workspace_id(user_id)),
        )

    placed = await workspaces.place_home_stand_in(user_id, healthy)

    # Owed files are only where it is, so it stays with them.
    assert str(placed["computer_id"]) == (broken if owed else healthy)
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


async def test_a_removal_holding_the_stand_in_and_a_turn_starting_exclude_each_other(
    seed_user, db_conn
):
    """A removal reads what Home is owed and deletes the stand-in under the
    exclusive side; a Home turn's owed mark, in its START transaction, takes
    the shared side, so the turn is never admitted between the two."""
    import psycopg

    from src.server.database import home_away
    from src.server.database.pool import get_db_connection
    from src.server.services import home_placement

    user_id = seed_user["user_id"]
    await workspaces.place_home_stand_in(user_id, await _computer(user_id, "Cloud"))

    # A turn's START holding its mark keeps a removal out.
    async with get_db_connection() as conn, conn.transaction():
        await home_away.mark_away(user_id, conn=conn)
        async with home_placement.stand_in_held(user_id) as held:
            assert not held

    # A removal holding it makes a START wait for it.
    async with home_placement.stand_in_held(user_id) as held:
        assert held
        async with get_db_connection() as conn:
            await conn.execute("SET lock_timeout = '200ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                async with conn.transaction():
                    await home_away.mark_away(user_id, conn=conn)
            await conn.execute("RESET lock_timeout")
        await db_conn.execute(
            "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
            (get_home_stand_in_id(user_id),),
        )
    # And once it deleted the stand-in, the turn is refused, not written there.
    with pytest.raises(home_away.StandInGone):
        await home_away.mark_away(user_id)


async def test_a_home_turn_in_the_stand_in_keeps_the_stand_in_computer_busy(seed_user):
    """The thread is Home's, on the machine Home lives on, so the idle sweep
    finds the turn and the task runs it starts on the stand-in's computer only
    by the computer their rows name."""
    from src.server.database.conversation import create_thread
    from src.server.database.home_workspace import STAND_IN_COMPUTER
    from src.server.database.runs import subagent_runs
    from src.server.database.runs.lifecycle import computer_has_active_run, start_run

    user_id = seed_user["user_id"]
    mac = await _computer(user_id, "Mac", kind="ssh")
    cloud = await _computer(user_id, "Cloud")
    # The join does not ask whose thread it is: any thread on the Mac stands for Home.
    home = await workspaces.create_workspace_on_computer(user_id, computer_id=mac, name="Notes")
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(home["workspace_id"]),
        current_status="in_progress",
        msg_type="ptc",
    )
    assert not await computer_has_active_run(cloud)

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        turn_index=1,
        metadata={"msg_type": "ptc", "user_id": user_id, STAND_IN_COMPUTER: cloud},
    )
    task = await subagent_runs.start_task_run(
        task_run_id=str(uuid.uuid4()),
        thread_id=thread_id,
        task_id="k1",
        cause="init",
        parent_run_id=run_id,
    )

    assert task["replay_facts"] == {STAND_IN_COMPUTER: cloud}
    assert await computer_has_active_run(cloud)
    assert await subagent_runs.count_open_runs_for_computer(cloud) == 1


async def test_a_turn_checked_for_the_stand_ins_computer_starts_only_there(seed_user):
    """A computer token's Home turn is checked on the computer Home's stand-in
    is on, and its START holds it there, not on Home's own."""
    from src.server.database.conversation import create_thread
    from src.server.database.runs.lifecycle import start_run

    user_id = seed_user["user_id"]
    mac = await _computer(user_id, "Mac", kind="ssh")
    cloud = await _computer(user_id, "Cloud")
    notes = await workspaces.create_workspace_on_computer(user_id, computer_id=mac, name="Notes")
    stand_in = str((await workspaces.place_home_stand_in(user_id, cloud))["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(notes["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )

    async def checked_on(computer_id):
        key = str(uuid.uuid4())
        return await start_run(
            run_id=key,
            thread_id=thread_id,
            request_key=key,
            before_commit=lambda conn: workspaces.refuse_moved_off(
                conn, thread_id, computer_id, runs_in=stand_in
            ),
        )

    with pytest.raises(workspaces.WorkspaceMovingError):
        await checked_on(mac)
    assert await checked_on(cloud)
