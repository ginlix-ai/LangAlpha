"""Home's stand-in, on a real Postgres.

Where a Home turn runs while the machine Home lives on can't be reached: a
hidden workspace row on one of the user's cloud computers, at a fixed id. Its
placement converges by that id across workers, it never moves, and listings
leave it out, all decided in SQL.
"""

import asyncio
import uuid

import pytest

from src.server.database import home_stand_in
from src.server.database import workspace as workspaces
from src.server.database.computer import (
    RETIRE_BY_OWNER,
    clear_computer_retirement_pending,
    create_computer,
    mark_computer_retirement_pending,
    update_computer_status,
)
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


async def _touch(db_conn, computer_id):
    """Make ``computer_id`` the one a new placement picks first."""
    await db_conn.execute(
        "UPDATE computers SET last_activity_at = NOW() + interval '1 hour'"
        " WHERE computer_id = %s",
        (computer_id,),
    )


async def test_the_stand_in_is_placed_once_at_its_fixed_id(seed_user, db_conn):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")
    await _touch(db_conn, cloud)

    placed = await home_stand_in.place_home_stand_in(user_id)
    await _touch(db_conn, other)
    again = await home_stand_in.place_home_stand_in(user_id)

    assert str(placed.row["workspace_id"]) == get_home_stand_in_id(user_id)
    assert str(placed.computer["computer_id"]) == cloud
    assert placed.kept and placed.runs and not placed.owed
    assert is_home_stand_in_row(placed.row)
    # Never Home's folder, which Home takes should this computer be primary.
    assert placed.row["dir_name"].startswith(".home-stand-in")
    # Placed once, it stays where its files are, and a turn runs there.
    assert str(again.computer["computer_id"]) == cloud
    assert str((await home_stand_in.stand_in_computer(user_id))["computer_id"]) == cloud


async def test_workers_placing_it_at_once_converge(seed_user):
    user_id = seed_user["user_id"]
    await _computer(user_id, "First", primary=True)
    await _computer(user_id, "Second")

    placed = await asyncio.gather(
        *(home_stand_in.place_home_stand_in(user_id) for _ in range(4))
    )

    assert len({str(p.computer["computer_id"]) for p in placed}) == 1
    assert {str(p.row["workspace_id"]) for p in placed} == {get_home_stand_in_id(user_id)}


async def test_a_stand_in_whose_computer_is_gone_is_placed_again(seed_user, db_conn):
    user_id = seed_user["user_id"]
    gone = await _computer(user_id, "Gone")
    cloud = await _computer(user_id, "Cloud", primary=True)
    await _touch(db_conn, gone)
    await home_stand_in.place_home_stand_in(user_id)
    await update_computer_status(gone, "deleted")
    assert await home_stand_in.read_stand_in(user_id) is None

    placed = await home_stand_in.place_home_stand_in(user_id)

    assert str(placed.computer["computer_id"]) == cloud
    assert placed.row["status"] != "deleted"


async def _owed(db_conn, user_id):
    await db_conn.execute(
        "INSERT INTO home_away (user_id, away_since, last_away_at) VALUES (%s, NOW(), NOW())",
        (user_id,),
    )


async def _left(db_conn, user_id, computer_id):
    """Handed back, with one file too large to carry left in the stand-in on
    ``computer_id``."""
    import json

    await db_conn.execute(
        "INSERT INTO home_away (user_id, last_away_at, handed_back_through,"
        " last_handback) VALUES (%s, NOW(), NOW(), %s::jsonb)",
        (
            user_id,
            json.dumps(
                {
                    "handback_id": str(uuid.uuid4()),
                    "folder": None,
                    "files": 0,
                    "left_on_cloud": ["huge.bin"],
                    "unreadable": [],
                    "computer_name": "Mac",
                    "cloud_name": "Broken",
                    "cloud_computer_id": computer_id,
                    "created_at": "2026-11-16T00:00:00+00:00",
                }
            ),
        ),
    )


@pytest.mark.parametrize(
    "breaks",
    [
        "UPDATE computers SET status = 'error' WHERE computer_id = %s",
        "UPDATE computers SET config = '{\"retire_when_empty\": true}' WHERE computer_id = %s",
    ],
    ids=["failed", "retiring"],
)
@pytest.mark.parametrize("held", [None, "owed", "left"], ids=["handed-back", "owed", "left"])
async def test_a_stand_in_whose_computer_cannot_keep_it_moves_unless_files_hold_it(
    seed_user, db_conn, breaks, held
):
    user_id = seed_user["user_id"]
    broken = await _computer(user_id, "Broken")
    healthy = await _computer(user_id, "Healthy")
    await _touch(db_conn, broken)
    await home_stand_in.place_home_stand_in(user_id)
    await db_conn.execute(breaks, (broken,))
    if held == "owed":
        await _owed(db_conn, user_id)
    elif held == "left":
        await _left(db_conn, user_id, broken)

    placed = await home_stand_in.place_home_stand_in(user_id)

    # Files owed, or left by the last hand-back, are only where it is, so it
    # stays with them; no turn runs on a computer that takes no workspace.
    assert str(placed.computer["computer_id"]) == (broken if held else healthy)
    assert (placed.kept, placed.runs, placed.owed, placed.left) == (
        True,
        not held,
        held == "owed",
        held == "left",
    )
    assert placed.row["status"] != "deleted"


async def test_files_left_on_a_removed_computer_hold_no_other(seed_user, db_conn):
    """What a hand-back left went with the computer its owner removed; the
    stand-in placed elsewhere since holds none of it."""
    from src.server.database.computer import update_computer_status

    user_id = seed_user["user_id"]
    removed = await _computer(user_id, "Removed")
    await _touch(db_conn, removed)
    await home_stand_in.place_home_stand_in(user_id)
    await _left(db_conn, user_id, removed)
    await update_computer_status(removed, "deleted")
    other = await _computer(user_id, "Other")

    placed = await home_stand_in.place_home_stand_in(user_id)
    await db_conn.execute("UPDATE computers SET status = 'error' WHERE computer_id = %s", (other,))
    stays = await home_stand_in.read_stand_in(user_id)

    assert str(placed.computer["computer_id"]) == other
    assert (placed.left, stays.kept, stays.left) == (False, False, False)


async def test_a_computer_being_retired_takes_no_stand_in(seed_user, db_conn):
    user_id = seed_user["user_id"]
    retiring = await _computer(user_id, "Retiring")
    await db_conn.execute(
        "UPDATE computers SET config = '{\"retire_when_empty\": true}' "
        "WHERE computer_id = %s",
        (retiring,),
    )

    assert await home_stand_in.stand_in_computer(user_id) is None
    assert await home_stand_in.place_home_stand_in(user_id) is None


async def test_a_machine_of_the_users_own_takes_no_stand_in(seed_user):
    user_id = seed_user["user_id"]
    await _computer(user_id, "Mac", kind="ssh")

    assert await home_stand_in.place_home_stand_in(user_id) is None


async def test_listings_and_counts_leave_the_stand_in_out(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    await workspaces.create_workspace_on_computer(user_id, "Research", cloud)
    await workspaces.get_or_create_home_workspace(user_id)
    await home_stand_in.place_home_stand_in(user_id)

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
    await _computer(user_id, "Cloud", primary=True)
    await home_stand_in.place_home_stand_in(user_id)
    other = await _computer(user_id, "Other")

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
    import time

    from src.server.database import home_away
    from src.server.database.pool import get_db_connection

    user_id = seed_user["user_id"]
    await _computer(user_id, "Cloud")
    await home_stand_in.place_home_stand_in(user_id)

    # A turn's START holding its mark keeps a removal out.
    async with get_db_connection() as conn, conn.transaction():
        await home_away.mark_away(user_id, conn=conn)
        async with home_away.stand_in_held(user_id) as held:
            assert not held

    # A removal holding it refuses a START at once, rather than holding
    # Home's row and a connection through its teardown; nothing is marked.
    async with home_away.stand_in_held(user_id) as held:
        assert held
        before = await home_away.get_home_away(user_id)
        async with get_db_connection() as conn:
            started = time.monotonic()
            with pytest.raises(home_away.StandInGone):
                async with conn.transaction():
                    await home_away.mark_away(user_id, conn=conn)
            assert time.monotonic() - started < 1
        assert await home_away.get_home_away(user_id) == before
        await db_conn.execute(
            "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
            (get_home_stand_in_id(user_id),),
        )
    # And once it deleted the stand-in, the turn is refused, not written there.
    with pytest.raises(home_away.StandInGone):
        await home_away.mark_away(user_id)


async def test_a_turn_placed_on_a_stand_in_since_moved_is_refused(seed_user):
    """The turn's run row names the computer it was placed on for the idle
    sweep; a stand-in placed on another since would run it unseen there."""
    from src.server.database import home_away

    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud")
    await home_stand_in.place_home_stand_in(user_id)

    with pytest.raises(home_away.StandInGone):
        await home_away.mark_away(user_id, computer_id=str(uuid.uuid4()))
    await home_away.mark_away(user_id, computer_id=cloud)

    assert await home_away.get_home_away(user_id) is not None


async def test_a_stand_in_cannot_move_while_a_turn_placed_on_it_is_admitted(seed_user):
    """The check that the stand-in is still where the turn was placed holds to
    START's end, so a placement moving it in between waits rather than leave
    the turn running on a computer its row no longer names."""
    import psycopg

    from src.server.database import home_away
    from src.server.database.pool import get_db_connection

    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud")
    other = await _computer(user_id, "Other")
    await home_stand_in.place_home_stand_in(user_id)

    async with get_db_connection() as start, start.transaction():
        await home_away.mark_away(user_id, computer_id=cloud, conn=start)
        async with get_db_connection() as mover:
            with pytest.raises(psycopg.errors.LockNotAvailable):
                async with mover.transaction():
                    await mover.execute("SET LOCAL lock_timeout = '300ms'")
                    await mover.execute(
                        "UPDATE workspaces SET computer_id = %s WHERE workspace_id = %s",
                        (other, get_home_stand_in_id(user_id)),
                    )


async def test_a_placement_racing_a_turns_start_keeps_the_stand_in_for_it(seed_user):
    """A placement that would move the stand-in off a computer that just
    failed waits for a START marking a turn there, then sees the turn's mark
    and keeps the stand-in where the turn runs."""
    from src.server.database import home_away
    from src.server.database.pool import get_db_connection

    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud")
    await _computer(user_id, "Other")
    await home_stand_in.place_home_stand_in(user_id)

    # Failed after the turn was placed on it, before its START marks it.
    await update_computer_status(cloud, "error")
    async with get_db_connection() as start, start.transaction():
        await home_away.mark_away(user_id, computer_id=cloud, conn=start)
        placing = asyncio.create_task(home_stand_in.place_home_stand_in(user_id))
        await asyncio.sleep(0.5)
        assert not placing.done()
    placed = await asyncio.wait_for(placing, timeout=10)

    assert str(placed.computer["computer_id"]) == cloud
    assert placed.owed and not placed.runs


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
    stand_in = str((await home_stand_in.place_home_stand_in(user_id)).row["workspace_id"])
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


async def test_the_stand_in_is_needed_only_while_the_primary_is_the_users_own(seed_user):
    """A cloud primary is never away, so nothing need stand in for it."""
    user_id = seed_user["user_id"]
    assert await home_stand_in.stand_in_needed(user_id) is False

    cloud = await _computer(user_id, "Cloud", primary=True)
    assert await home_stand_in.stand_in_needed(user_id) is False

    await update_computer_status(cloud, "deleted")
    await _computer(user_id, "Mac", primary=True, kind="ssh")
    assert await home_stand_in.stand_in_needed(user_id) is True


async def _config(db_conn, computer_id) -> dict:
    cur = await db_conn.execute(
        "SELECT COALESCE(config, '{}'::jsonb) AS config FROM computers WHERE computer_id = %s",
        (computer_id,),
    )
    return (await cur.fetchone())["config"]


async def _marked(db_conn, computer_id) -> bool:
    cur = await db_conn.execute(
        "SELECT COALESCE(config, '{}'::jsonb) ? %s AS marked FROM computers"
        " WHERE computer_id = %s",
        (home_stand_in.STAND_IN_HOST, computer_id),
    )
    return (await cur.fetchone())["marked"]


async def test_adoption_marks_the_computer_the_stand_in_stands_on(seed_user, db_conn):
    """Sizing follows the mark and Home's turns follow the placement, so the
    two must name one computer, whatever an older one would have been."""
    user_id = seed_user["user_id"]
    older = await _computer(user_id, "Older", primary=True)
    newer = await _computer(user_id, "Newer")
    await _touch(db_conn, newer)
    placed = await home_stand_in.place_home_stand_in(user_id)
    assert str(placed.computer["computer_id"]) == newer

    assert await home_stand_in.adopt_stand_in_host(user_id)

    assert await _marked(db_conn, newer)
    assert not await _marked(db_conn, older)


async def test_adoption_passes_over_a_computer_being_stopped_or_retired(seed_user, db_conn):
    """An owner's removal keeps the mark on its tombstone, so marking a
    computer on its way out would read as its owner turning the stand-in's
    computer down."""
    user_id = seed_user["user_id"]
    retiring = await _computer(user_id, "Retiring", primary=True)
    await update_computer_status(retiring, "stopping")

    assert await home_stand_in.adopt_stand_in_host(user_id)

    assert not await _marked(db_conn, retiring)


@pytest.mark.parametrize(
    ("marked_status", "other_status", "touched", "placed_on"),
    [
        ("stopped", "running", "marked", "other"),
        ("running", "running", "other", "marked"),
        ("starting", "stopped", "other", "marked"),
        ("stopping", "stopped", "marked", "other"),
        ("stopping", "running", "marked", "other"),
        ("stopped", "stopped", "other", "marked"),
    ],
    ids=[
        "running-beats-a-stopped-mark",
        "mark-wins-among-running",
        "starting-beats-stopped",
        "stopped-beats-a-stopping-mark",
        "running-beats-a-stopping-mark",
        "mark-wins-among-stopped",
    ],
)
async def test_the_stand_in_goes_by_status_first_then_to_the_marked_computer(
    seed_user, db_conn, marked_status, other_status, touched, placed_on
):
    """A turn waits out any start or stop ahead of it, so the status decides
    first; within one status the marked computer wins, however recently the
    other was used."""
    user_id = seed_user["user_id"]
    marked = await _computer(user_id, "Marked", primary=True)
    assert await home_stand_in.adopt_stand_in_host(user_id)
    assert await _marked(db_conn, marked)
    other = await _computer(user_id, "Other")
    await db_conn.execute(
        "UPDATE computers SET status = %s WHERE computer_id = %s", (marked_status, marked)
    )
    await db_conn.execute(
        "UPDATE computers SET status = %s WHERE computer_id = %s", (other_status, other)
    )
    await _touch(db_conn, marked if touched == "marked" else other)

    placed = await home_stand_in.place_home_stand_in(user_id)

    assert str(placed.computer["computer_id"]) == (marked if placed_on == "marked" else other)


@pytest.mark.parametrize(
    ("by_owner", "torn_down"),
    [(False, False), (True, False), (True, True)],
    ids=["delete", "owners-remove", "owners-remove-torn-down"],
)
async def test_the_retirement_marker_carries_whose_remove_it_is(
    seed_user, db_conn, by_owner, torn_down
):
    """Marked by a delete first, the owner's remove still records itself on
    the marker a sweep reads, and keeping the computer takes every mark off."""
    user_id = seed_user["user_id"]
    computer = await _computer(user_id, "Retiring")
    assert await mark_computer_retirement_pending(computer)
    assert (
        await mark_computer_retirement_pending(computer, by_owner=by_owner, torn_down=torn_down)
        is by_owner
    )

    config = await _config(db_conn, computer)
    assert config.get("retire_when_empty") is True
    assert config.get("retire_by_owner") is (True if by_owner else None)
    assert config.get("retire_torn_down") is (True if torn_down else None)

    assert await clear_computer_retirement_pending(computer)
    config = await _config(db_conn, computer)
    assert not {"retire_when_empty", "retire_by_owner", "retire_torn_down"} & set(config)


async def test_adoption_leaves_a_computer_whose_retirement_is_pending(seed_user, db_conn):
    """Its retirement read it unmarked before the tombstone it is about to
    write, so a mark landing in between would read as its owner's remove."""
    user_id = seed_user["user_id"]
    pending = await _computer(user_id, "Pending")
    assert await mark_computer_retirement_pending(pending)

    assert await home_stand_in.adopt_stand_in_host(user_id)

    assert not await _marked(db_conn, pending)


async def test_adoption_with_no_stand_in_picks_where_one_would_be_placed(seed_user, db_conn):
    """Sizing follows the mark and the stand-in, once placed, follows the
    placement rule, so the mark goes where the placement would go."""
    user_id = seed_user["user_id"]
    older = await _computer(user_id, "Older", primary=True)
    newer = await _computer(user_id, "Newer")
    await update_computer_status(older, "stopped")

    assert await home_stand_in.adopt_stand_in_host(user_id)

    assert await _marked(db_conn, newer)
    assert not await _marked(db_conn, older)
    assert str((await home_stand_in.stand_in_computer(user_id))["computer_id"]) == newer


@pytest.mark.parametrize(
    ("drop", "by_owner"),
    [(True, False), (False, False), (True, True)],
    ids=["dropped", "kept", "kept-for-an-owners-remove-begun-since"],
)
async def test_a_tombstone_can_drop_a_config_key_in_its_own_write(seed_user, db_conn, drop, by_owner):
    """A retirement the owner did not ask for clears the stand-in's mark in the
    same statement that deletes the row, so an adoption between the two
    cannot leave the mark on the tombstone; but not once the row says it is
    its owner's remove, which may have begun after the retirement read it."""
    user_id = seed_user["user_id"]
    computer = await _computer(user_id, "Cloud", primary=True)
    await db_conn.execute(
        "UPDATE computers SET config = COALESCE(config, '{}'::jsonb)"
        " || jsonb_build_object(%s::text, true, 'other', 1) WHERE computer_id = %s",
        (home_stand_in.STAND_IN_HOST, computer),
    )
    if by_owner:
        assert await mark_computer_retirement_pending(computer, by_owner=True)

    row = await update_computer_status(
        computer,
        "deleted",
        expected="running",
        drop_config_key=home_stand_in.STAND_IN_HOST if drop else None,
        unless_config_key=RETIRE_BY_OWNER,
    )

    assert row is not None and row["status"] == "deleted"
    assert await _marked(db_conn, computer) is not (drop and not by_owner)
    cur = await db_conn.execute(
        "SELECT config->>'other' AS other FROM computers WHERE computer_id = %s", (computer,)
    )
    assert (await cur.fetchone())["other"] == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("expected", ["running", "stopped"], ids=["claimed", "missed"])
async def test_a_claim_carries_its_owners_remove_in_its_own_write(
    seed_user, db_conn, expected
):
    """An owner's remove is durable from the moment its claim lands, so no
    exit after it can leave an ordinary retirement; and a claim that misses
    marks nothing for the sweep to read as the owner's."""
    user_id = seed_user["user_id"]
    computer = await _computer(user_id, "Cloud", primary=True)
    await db_conn.execute(
        "UPDATE computers SET config = COALESCE(config, '{}'::jsonb)"
        " || jsonb_build_object(%s::text, true) WHERE computer_id = %s",
        (home_stand_in.STAND_IN_HOST, computer),
    )

    row = await update_computer_status(
        computer,
        "stopping",
        expected=expected,
        merge_config={"retire_when_empty": True, RETIRE_BY_OWNER: True},
    )

    cur = await db_conn.execute(
        "SELECT status, config FROM computers WHERE computer_id = %s", (computer,)
    )
    stored = await cur.fetchone()
    if expected == "running":
        assert row is not None and stored["status"] == "stopping"
        assert stored["config"] == {
            home_stand_in.STAND_IN_HOST: True,
            "retire_when_empty": True,
            RETIRE_BY_OWNER: True,
        }
    else:
        assert row is None and stored["status"] == "running"
        assert stored["config"] == {home_stand_in.STAND_IN_HOST: True}
