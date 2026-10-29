"""Moving a workspace to another computer, on a real Postgres.

The claim, the fence a move puts on its workspace, and the sweep's listing of
the folders moves left behind are each decided in SQL (a row lock, a partial
unique index, a projection on the database clock), so these run them against
the database rather than string-matched mocks.
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Optional

import pytest

from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer, get_computer
from src.server.database.conversation.threads_write import create_thread
from src.server.database.runs import command_runs
from src.server.database.runs.lifecycle import start_run
from src.server.database.runs.subagent_runs import start_task_run
from src.server.database.sql_fences import MOVE_STALE_SECONDS
from src.server.database.user import create_user
from src.server.database.workspace_move import (
    LEFTOVERS_KEEP_DAYS,
    MoveRefused,
    advance_workspace_move,
    cancel_workspace_move,
    commit_workspace_move,
    finish_workspace_move,
    get_latest_workspace_move,
    get_workspace_move,
    heartbeat_workspace_move,
    list_moves_with_leftovers,
    mark_leftovers_removed,
    mark_move_restored,
    stage_workspace_move,
)
# The service layer's, which carry the providers' rule for the primary.
from src.server.services.workspace_move import claim_workspace_move, set_primary_computer
from src.server.database.workspace_folders import WorkspaceFolderMoving, workspace_folder_in_use
from src.server.database.workspace_names import HOME_FOLDER

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_WAITING = 1
_DUE = LEFTOVERS_KEEP_DAYS + 1


async def _computer(
    user_id, name, *, primary=False, status="running", built=True, kind="docker"
) -> str:
    row = await create_computer(
        user_id,
        kind=kind,
        name=name,
        is_primary=primary,
        status=status,
        provider_ref=f"move-test-{uuid.uuid4()}" if built else None,
    )
    return str(row["computer_id"])


async def _machines(user_id):
    """The user's primary and a second computer, both running."""
    return (
        await _computer(user_id, "Primary", primary=True),
        await _computer(user_id, "Second"),
    )


async def _workspace(user_id, name, computer_id) -> str:
    row = await workspaces.create_workspace_on_computer(user_id, name, computer_id)
    return str(row["workspace_id"])


async def _home_on(user_id, computer_id) -> str:
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    bound = await workspaces.bind_workspace_to_computer(
        home, computer_id, expected_computer_id=None, dir_name=HOME_FOLDER
    )
    assert bound is not None
    return home


async def _refused(code, **claim):
    with pytest.raises(MoveRefused) as refused:
        await claim_workspace_move(**claim)
    assert refused.value.code == code


async def _is_primary(computer_id) -> bool:
    return bool((await get_computer(computer_id))["is_primary"])


async def _execute(pool, sql, params=()):
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def _status(pool, workspace_id) -> str:
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT status FROM workspaces WHERE workspace_id = %s", (workspace_id,)
        )
        return (await result.fetchone())["status"]


async def _start(thread_id, background):
    identifier = str(uuid.uuid4())
    if background:
        return await start_task_run(
            task_run_id=identifier, thread_id=thread_id, task_id=identifier, cause="init"
        )
    return await start_run(run_id=identifier, thread_id=thread_id, request_key=identifier)


# ---------------------------------------------------------------------------
# The claim
# ---------------------------------------------------------------------------


async def test_a_claim_records_a_move_in_its_first_phase(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    folder = await workspaces.get_workspace_dir_name(workspace_id)

    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )

    assert move["phase"] == "preparing"
    assert str(move["workspace_id"]) == workspace_id
    assert str(move["source_computer_id"]) == primary
    assert str(move["target_computer_id"]) == second
    assert move["source_folder"] == folder
    assert move["make_primary"] is False
    assert move["stale"] is False
    assert move["leftovers_path"] is None
    assert str((await get_latest_workspace_move(workspace_id))["move_id"]) == str(
        move["move_id"]
    )
    assert await workspaces.workspace_is_moving(workspace_id)
    # Nothing is bound to the target until the commit.
    assert str((await workspaces.get_workspace(workspace_id))["computer_id"]) == primary


async def test_home_moves_only_by_making_its_target_primary(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    home = await _home_on(user_id, primary)
    workspace_id = await _workspace(user_id, "Research", primary)

    await _refused(
        "not_movable", user_id=user_id, workspace_id=home, target_computer_id=second
    )
    await _refused(
        "not_movable",
        user_id=user_id,
        workspace_id=workspace_id,
        target_computer_id=second,
        make_primary=True,
    )

    move = await claim_workspace_move(
        user_id=user_id, workspace_id=home, target_computer_id=second, make_primary=True
    )
    assert move["phase"] == "preparing"
    assert move["make_primary"] is True


async def test_an_unbound_home_has_nothing_to_move(seed_user):
    user_id = seed_user["user_id"]
    _, second = await _machines(user_id)
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])

    await _refused(
        "not_movable",
        user_id=user_id,
        workspace_id=home,
        target_computer_id=second,
        make_primary=True,
    )


async def test_a_claim_refuses_what_it_cannot_move(seed_user, seed_response):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    other_user = (await create_user(user_id="move-test-other", email="o@example.com"))[
        "user_id"
    ]
    others_computer = await _computer(other_user, "Theirs", primary=True)
    others_workspace = await _workspace(other_user, "Theirs", others_computer)
    building = await _computer(user_id, "Building", status="starting", built=False)

    await _refused(
        "same_computer",
        user_id=user_id,
        workspace_id=workspace_id,
        target_computer_id=primary,
    )
    for missing in (str(uuid.uuid4()), "not-a-uuid", others_workspace):
        await _refused(
            "not_found", user_id=user_id, workspace_id=missing, target_computer_id=second
        )
    for unavailable in (others_computer, building, "not-a-uuid"):
        await _refused(
            "target_unavailable",
            user_id=user_id,
            workspace_id=workspace_id,
            target_computer_id=unavailable,
        )

    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "in_progress")
    await seed_response(str(uuid.uuid4()), thread_id, 0, status="in_progress")
    await _refused(
        "turn_active",
        user_id=user_id,
        workspace_id=workspace_id,
        target_computer_id=second,
    )
    assert await get_latest_workspace_move(workspace_id) is None


async def test_a_run_anywhere_on_the_source_refuses_the_claim(seed_user, seed_response):
    """A sibling's agent reaches the folder as ``../<folder>``, so its writes
    would land after the backup and be lost with the folder set aside."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    sibling = await _workspace(user_id, "Sibling", primary)
    elsewhere = await _workspace(user_id, "Elsewhere", second)
    for owner, status in ((elsewhere, "in_progress"), (sibling, "completed")):
        thread_id = str(uuid.uuid4())
        await create_thread(thread_id, owner, status)
        await seed_response(str(uuid.uuid4()), thread_id, 0, status=status)
    # A run on another computer, or one that ended, is no reason to wait.
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )
    assert move["phase"] == "preparing"
    await cancel_workspace_move(workspace_id, user_id)

    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, sibling, "in_progress")
    await seed_response(str(uuid.uuid4()), thread_id, 0, status="in_progress")
    await _refused(
        "busy", user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )


async def test_a_user_moves_one_workspace_at_a_time(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    first = await _workspace(user_id, "First", primary)
    other = await _workspace(user_id, "Other", primary)
    await claim_workspace_move(
        user_id=user_id, workspace_id=first, target_computer_id=second
    )

    await _refused(
        "move_in_progress", user_id=user_id, workspace_id=other, target_computer_id=second
    )
    await _refused(
        "move_in_progress", user_id=user_id, workspace_id=first, target_computer_id=second
    )

    assert (await cancel_workspace_move(first, user_id))["phase"] == "cancelled"
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=other, target_computer_id=second
    )
    assert move["phase"] == "preparing"


async def test_a_cancel_reaches_only_a_move_that_has_not_set_its_folder_aside(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    assert await cancel_workspace_move(workspace_id, user_id) is None
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )
    await _execute(
        test_db_pool,
        "UPDATE workspace_moves SET phase = 'staged' WHERE move_id = %s",
        (move["move_id"],),
    )

    assert await cancel_workspace_move(workspace_id, user_id) is None
    assert (await get_latest_workspace_move(workspace_id))["phase"] == "staged"


# ---------------------------------------------------------------------------
# Make primary
# ---------------------------------------------------------------------------


async def test_make_primary_flips_now_when_home_is_on_no_computer(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    await workspaces.get_or_create_home_workspace(user_id)

    assert await set_primary_computer(user_id, second) is True

    assert await _is_primary(second)
    assert not await _is_primary(primary)


async def test_make_primary_flips_now_when_home_is_already_there(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    await _home_on(user_id, second)

    assert await set_primary_computer(user_id, second) is True

    assert await _is_primary(second)
    assert not await _is_primary(primary)


async def test_make_primary_leaves_the_flip_to_home_s_move(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    await _home_on(user_id, primary)

    assert await set_primary_computer(user_id, second) is False

    assert await _is_primary(primary)
    assert not await _is_primary(second)


async def test_make_primary_waits_for_home_s_move(seed_user):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    third = await _computer(user_id, "Third")
    home = await _home_on(user_id, primary)
    await claim_workspace_move(
        user_id=user_id, workspace_id=home, target_computer_id=second, make_primary=True
    )

    for target in (second, third, primary):
        with pytest.raises(MoveRefused) as refused:
            await set_primary_computer(user_id, target)
        assert refused.value.code == "move_in_progress"
    assert await _is_primary(primary)


async def test_make_primary_refuses_a_computer_that_is_gone_broken_or_building(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    _, second = await _machines(user_id)
    broken = await _computer(user_id, "Broken", status="error")
    building = await _computer(user_id, "Building", status="creating", built=False)
    await workspaces.get_or_create_home_workspace(user_id)
    await _execute(
        test_db_pool,
        "UPDATE computers SET status = 'deleted' WHERE computer_id = %s",
        (second,),
    )

    for target in (second, broken, building, str(uuid.uuid4())):
        with pytest.raises(MoveRefused) as refused:
            await set_primary_computer(user_id, target)
        assert refused.value.code == "target_unavailable"


async def test_a_machine_its_owner_brought_never_becomes_primary(seed_user):
    user_id = seed_user["user_id"]
    primary, _ = await _machines(user_id)
    owned = await _computer(user_id, "Owned", kind="ssh")
    home = await _home_on(user_id, primary)

    with pytest.raises(MoveRefused) as refused:
        await set_primary_computer(user_id, owned)
    assert refused.value.code == "not_primary"
    await _refused(
        "not_primary",
        user_id=user_id,
        workspace_id=home,
        target_computer_id=owned,
        make_primary=True,
    )
    assert await _is_primary(primary)


# ---------------------------------------------------------------------------
# The fence: delete and turn admission
# ---------------------------------------------------------------------------


async def test_a_delete_waits_for_the_move_to_end(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )

    with pytest.raises(workspaces.WorkspaceBusyError):
        await workspaces.delete_workspace(workspace_id)
    assert await _status(test_db_pool, workspace_id) == "running"

    await cancel_workspace_move(workspace_id, user_id)
    assert await workspaces.delete_workspace(workspace_id) is True
    assert await _status(test_db_pool, workspace_id) == "deleted"


@pytest.mark.parametrize("background", [False, True], ids=["root", "subagent"])
async def test_no_turn_starts_while_its_workspace_moves(seed_user, background):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )

    with pytest.raises(workspaces.WorkspaceMovingError):
        await _start(thread_id, background)

    await cancel_workspace_move(workspace_id, user_id)
    assert await _start(thread_id, background)


@pytest.mark.parametrize(
    ("phase", "fenced"),
    [("backing_up", False), ("staged", True), ("committed", True)],
)
async def test_a_stale_move_fences_only_once_its_folder_is_set_aside(
    seed_user, test_db_pool, phase, fenced
):
    """Before staging a dead runner changed nothing, so the workspace is free at
    once; after it the folder is not where the row says until recovery ends it."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )
    await _execute(
        test_db_pool,
        "UPDATE workspace_moves SET phase = %s, "
        "heartbeat_at = NOW() - make_interval(secs => %s) WHERE move_id = %s",
        (phase, MOVE_STALE_SECONDS + 60, move["move_id"]),
    )

    assert await workspaces.workspace_is_moving(workspace_id) is fenced
    if fenced:
        with pytest.raises(workspaces.WorkspaceMovingError):
            await _start(thread_id, False)
    else:
        assert await _start(thread_id, False)


# ---------------------------------------------------------------------------
# The runner's writes: freshness, and the stage
# ---------------------------------------------------------------------------


async def _backing_up(user_id, workspace_id, target):
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=target
    )
    move_id, runner = str(move["move_id"]), str(move["runner_id"])
    assert await advance_workspace_move(move_id, runner, "backing_up")
    return move_id, runner


async def test_a_runner_lapsed_before_staging_can_write_nothing(seed_user, test_db_pool):
    """Its fence lifted when it went stale and a turn may have started since, so
    renewing or staging it now would set aside a folder in use."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    move_id, runner = await _backing_up(user_id, workspace_id, second)
    await _execute(
        test_db_pool,
        "UPDATE workspace_moves SET heartbeat_at = NOW() - make_interval(secs => %s) "
        "WHERE move_id = %s",
        (MOVE_STALE_SECONDS + 60, move_id),
    )

    assert await heartbeat_workspace_move(move_id, runner) is False
    assert await advance_workspace_move(move_id, runner, "backing_up") is None
    assert (
        await stage_workspace_move(
            move_id, runner, source_folder="Research", leftovers_path="away"
        )
        is None
    )
    row = await get_workspace_move(move_id)
    assert (row["phase"], row["leftovers_path"]) == ("backing_up", None)


async def test_staging_waits_out_every_run_on_the_source(seed_user, seed_response, test_db_pool):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    sibling = await _workspace(user_id, "Sibling", primary)
    move_id, runner = await _backing_up(user_id, workspace_id, second)
    # A sibling's turn admitted during the backup, as nothing fences it.
    thread_id = str(uuid.uuid4())
    response_id = str(uuid.uuid4())
    await create_thread(thread_id, sibling, "in_progress")
    await seed_response(response_id, thread_id, 0, status="in_progress")

    with pytest.raises(MoveRefused) as refused:
        await stage_workspace_move(
            move_id, runner, source_folder="Research", leftovers_path="away"
        )
    assert refused.value.code == "busy"
    assert (await get_workspace_move(move_id))["phase"] == "backing_up"

    await _execute(
        test_db_pool,
        "UPDATE conversation_responses SET status = 'completed' "
        "WHERE conversation_response_id = %s",
        (response_id,),
    )
    staged = await stage_workspace_move(
        move_id, runner, source_folder="Research", leftovers_path="away"
    )
    assert (staged["phase"], staged["source_folder"], staged["leftovers_path"]) == (
        "staged",
        "Research",
        "away",
    )
    # A runner that lost its row stages nothing.
    assert (
        await stage_workspace_move(
            move_id, str(uuid.uuid4()), source_folder="Research", leftovers_path="away"
        )
        is None
    )


async def _command_on(user_id, workspace_id, computer_id) -> str:
    """A command handed off from a turn on ``workspace_id`` that has ended."""
    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    command_id = "bg-" + uuid.uuid4().hex[:12]
    await command_runs.record_handoff(
        command_id=command_id,
        thread_id=thread_id,
        user_id=user_id,
        workspace_id=workspace_id,
        computer_id=computer_id,
        run_id=None,
        tool_call_id="toolu_1",
        tool="Bash",
        description="",
        command_preview="python serve.py",
        log_path="l",
        job_ref={
            "command_id": command_id, "state_dir": "/s", "log_path": "/l",
            "trace_path": "/t", "log_relpath": "l",
        },
        started_at=datetime.now(UTC),
    )
    return command_id


async def test_a_command_running_on_the_source_refuses_the_move_until_it_ends(seed_user):
    """A handed-off command reaches every folder on its computer as a turn's
    code does, and outlives every turn."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    sibling = await _workspace(user_id, "Sibling", primary)
    move_id, runner = await _backing_up(user_id, workspace_id, second)
    command_id = await _command_on(user_id, sibling, primary)

    with pytest.raises(MoveRefused) as refused:
        await stage_workspace_move(
            move_id, runner, source_folder="Research", leftovers_path="away"
        )
    assert refused.value.code == "command_running"
    await cancel_workspace_move(workspace_id, user_id)
    await _refused(
        "command_running", user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )

    ended = await command_runs.finalize(
        {"command_id": command_id, "stopped_by": None}, command_runs.Outcome("stopped")
    )
    assert ended is not None
    move = await claim_workspace_move(
        user_id=user_id, workspace_id=workspace_id, target_computer_id=second
    )
    assert move["phase"] == "preparing"


async def test_a_target_in_error_since_the_claim_takes_no_commit(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    move_id, runner = await _backing_up(user_id, workspace_id, second)
    staged = await stage_workspace_move(
        move_id, runner, source_folder="Research", leftovers_path="away"
    )
    # The put-back's window counts from here, not from the claim.
    assert staged["staged_at"] is not None
    await _execute(
        test_db_pool,
        "UPDATE computers SET status = 'error' WHERE computer_id = %s",
        (second,),
    )

    with pytest.raises(MoveRefused) as refused:
        await commit_workspace_move(move_id, runner)
    assert refused.value.code == "target_unavailable"
    assert str((await workspaces.get_workspace(workspace_id))["computer_id"]) == primary
    assert (await get_workspace_move(move_id))["phase"] == "staged"


async def test_no_file_change_holds_a_folder_a_move_has_set_aside(seed_user):
    """From staging the row names the folder the move left until the move ends,
    and a change read under the hold would lay it again; during the backup the
    folder is still where the row says."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    move_id, runner = await _backing_up(user_id, workspace_id, second)
    async with workspace_folder_in_use(workspace_id):
        pass

    await stage_workspace_move(move_id, runner, source_folder="Research", leftovers_path="away")
    with pytest.raises(WorkspaceFolderMoving):
        async with workspace_folder_in_use(workspace_id):
            pass
    await commit_workspace_move(move_id, runner)
    with pytest.raises(WorkspaceFolderMoving):
        async with workspace_folder_in_use(workspace_id):
            pass

    await finish_workspace_move(move_id, runner, "done")
    async with workspace_folder_in_use(workspace_id):
        pass


async def test_home_binds_only_to_a_computer_still_primary(seed_user, test_db_pool):
    """A first turn's bind that read the primary before a flip committed waits
    on the flip's row lock and then takes nothing, so Home never lands on the
    computer that just stopped being primary."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])

    async with test_db_pool.connection() as conn, conn.transaction():
        # In _flip_primary's order: the one-primary index is not deferrable.
        await conn.execute(
            "UPDATE computers SET is_primary = FALSE WHERE computer_id = %s", (primary,)
        )
        await conn.execute(
            "UPDATE computers SET is_primary = TRUE WHERE computer_id = %s", (second,)
        )
        bind = asyncio.create_task(
            workspaces.bind_workspace_to_computer(
                home,
                primary,
                expected_computer_id=None,
                dir_name=HOME_FOLDER,
                primary_only=True,
            )
        )
        await asyncio.sleep(0.5)
        assert not bind.done()
    assert await bind is None

    bound = await workspaces.bind_workspace_to_computer(
        home, second, expected_computer_id=None, dir_name=HOME_FOLDER, primary_only=True
    )
    assert str(bound["computer_id"]) == second


async def test_a_bind_to_another_computer_drops_the_folder_backoff(seed_user, test_db_pool):
    """The backoff is for the full disk of the computer it was recorded on;
    kept, it would hold the folder off its name on a computer with room."""
    user_id = seed_user["user_id"]
    primary, second = await _machines(user_id)
    workspace_id = await _workspace(user_id, "Research", primary)
    blocked = {"target": "Macro", "until": 9e9}
    await _execute(
        test_db_pool,
        "UPDATE workspaces SET config = jsonb_set(COALESCE(config, '{}'::jsonb),"
        " '{folder_move_blocked}', %s::jsonb) WHERE workspace_id = %s",
        (json.dumps(blocked), workspace_id),
    )

    again = await workspaces.bind_workspace_to_computer(
        workspace_id, primary, expected_computer_id=primary
    )
    assert again["config"]["folder_move_blocked"] == blocked

    moved = await workspaces.bind_workspace_to_computer(
        workspace_id, second, expected_computer_id=primary, dir_name="Research"
    )
    assert "folder_move_blocked" not in moved["config"]


# ---------------------------------------------------------------------------
# The sweep's listing of folders moves left behind
# ---------------------------------------------------------------------------


async def _left(
    pool,
    *,
    user_id,
    workspace_id,
    source,
    target,
    phase="done",
    finished: Optional[int] = 30,
    restored: Optional[int] = None,
    removed=False,
    path=True,
) -> str:
    """A move row in a state the runner leaves; ages are days before now."""
    move_id = str(uuid.uuid4())
    await _execute(
        pool,
        """
        INSERT INTO workspace_moves (
            move_id, user_id, workspace_id, source_computer_id, target_computer_id,
            phase, runner_id, leftovers_path, finished_at, restored_at,
            leftovers_removed_at
        ) VALUES (
            %(move_id)s, %(user_id)s, %(workspace_id)s, %(source)s, %(target)s,
            %(phase)s, gen_random_uuid(), %(path)s,
            NOW() - make_interval(days => %(finished)s::int),
            NOW() - make_interval(days => %(restored)s::int),
            CASE WHEN %(removed)s THEN NOW() END
        )
        """,
        {
            "move_id": move_id,
            "user_id": user_id,
            "workspace_id": workspace_id,
            "source": source,
            "target": target,
            "phase": phase,
            "path": f"_internal/leftovers/move-{move_id}" if path else None,
            "finished": finished,
            "restored": restored,
            "removed": removed,
        },
    )
    return move_id


async def _listing(user_id, test_db_pool):
    """A running, a stopped and a deleted source, and the workspaces moves name."""
    target = await _computer(user_id, "Target", primary=True)
    running = await _computer(user_id, "Running")
    stopped = await _computer(user_id, "Stopped", status="stopped")
    gone = await _computer(user_id, "Gone")
    live = await _workspace(user_id, "Moved", target)
    deleted = await _workspace(user_id, "Deleted", target)
    owed = await _workspace(user_id, "Owed", target)
    await _execute(
        test_db_pool,
        "UPDATE computers SET status = 'deleted' WHERE computer_id = %s",
        (gone,),
    )
    await _execute(
        test_db_pool,
        "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
        (deleted,),
    )
    await _execute(
        test_db_pool,
        "UPDATE workspaces SET files_restore_incomplete_at = "
        "CASE WHEN workspace_id = %s THEN NOW() END "
        "WHERE workspace_id = ANY(%s)",
        (owed, [live, owed]),
    )
    return target, running, stopped, gone, live, deleted, owed


async def test_the_sweep_lists_only_what_it_can_act_on_now(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    target, running, stopped, gone, live, deleted, owed = await _listing(
        user_id, test_db_pool
    )

    async def left(source, workspace_id, **row):
        return await _left(
            test_db_pool,
            user_id=user_id,
            workspace_id=workspace_id,
            source=source,
            target=target,
            **row,
        )

    seeded = {
        # Listed, with what to do.
        "source deleted": await left(gone, live, restored=_WAITING),
        "workspace deleted, source running": await left(running, deleted),
        "restored long ago, source running": await left(running, live, restored=_DUE),
        "files back, source stopped": await left(stopped, live),
        "files back, source running": await left(running, live, phase="failed"),
        # Not listed.
        "restored recently": await left(running, live, restored=_WAITING),
        "workspace deleted, source stopped": await left(stopped, deleted),
        "restored long ago, source stopped": await left(stopped, live, restored=_DUE),
        "files still owed": await left(stopped, owed),
        "already removed": await left(running, deleted, removed=True),
        "nothing left behind": await left(gone, deleted, path=False),
        "move still running": await left(gone, live, phase="committed", finished=None),
    }
    label = {move_id: name for name, move_id in seeded.items()}

    listed = {
        label[str(row["move_id"])]: row["action"]
        for row in await list_moves_with_leftovers()
    }

    assert listed == {
        "source deleted": "source_gone",
        "workspace deleted, source running": "remove",
        "restored long ago, source running": "remove",
        "files back, source stopped": "restored",
        "files back, source running": "restored",
    }


async def test_each_action_once_applied_takes_its_row_off_the_list(
    seed_user, test_db_pool
):
    """The sweep converges: marking what it did is what ends a row's listing,
    and a restore mark starts the wait rather than allowing the removal."""
    user_id = seed_user["user_id"]
    target, running, _, gone, live, deleted, _ = await _listing(user_id, test_db_pool)
    row = {"user_id": user_id, "target": target}
    restored = await _left(test_db_pool, source=running, workspace_id=live, **row)
    removed = await _left(test_db_pool, source=running, workspace_id=deleted, **row)
    vanished = await _left(test_db_pool, source=gone, workspace_id=live, **row)
    assert {str(r["move_id"]) for r in await list_moves_with_leftovers()} == {
        restored,
        removed,
        vanished,
    }

    await mark_move_restored(restored)
    await mark_leftovers_removed(removed)
    await mark_leftovers_removed(vanished)

    assert await list_moves_with_leftovers() == []


async def test_rows_waiting_out_their_days_never_crowd_out_a_due_one(
    seed_user, test_db_pool
):
    """The decision runs before the limit: applied after it, enough rows still
    waiting, each finished before the due one, would fill every page."""
    user_id = seed_user["user_id"]
    target, running, _, _, live, _, _ = await _listing(user_id, test_db_pool)
    row = {"user_id": user_id, "target": target, "source": running, "workspace_id": live}
    limit = 2
    waiting = {
        await _left(test_db_pool, finished=20, restored=_WAITING, **row)
        for _ in range(limit + 3)
    }
    due = await _left(test_db_pool, finished=10, restored=_DUE, **row)

    listed = await list_moves_with_leftovers(limit=limit)

    assert [(str(r["move_id"]), r["action"]) for r in listed] == [(due, "remove")]
    assert not waiting & {str(r["move_id"]) for r in listed}
