"""What the server carries out for a computer after the request that asked:
removing the primary once Home's move off it commits, and making a new
computer primary once its first start completes. Both are kept on the
computer's row, so these run on Postgres, a fresh manager standing in for a
worker after a restart.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, SkillsConfig
from ptc_agent.config.core import (
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer, get_computer
from src.server.database.workspace_move import (
    advance_workspace_move,
    commit_workspace_move,
    finish_workspace_move,
    stage_workspace_move,
)
from src.server.database.workspace_names import HOME_FOLDER
from src.server.models.computer import ComputerKeyRemoval
from src.server.services.ssh_removal import MachineLeft
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.workspace_move import move_error_detail

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_SCHEDULE = "src.server.services.computer_manager._move.schedule_start"


def _manager() -> WorkspaceManager:
    manager = WorkspaceManager(
        AgentConfig(
            security=SecurityConfig(),
            logging=LoggingConfig(),
            mcp=MCPConfig(),
            sandbox=SandboxConfig(provider="docker"),
            filesystem=FilesystemConfig(),
            skills=SkillsConfig(enabled=False),
        )
    )
    # Retiring a computer tears its sandbox down; there is none here.
    manager._teardown_machine = AsyncMock()
    return manager


async def _computer(user_id, name, *, primary=False, kind="docker", config=None) -> str:
    row = await create_computer(
        user_id,
        kind=kind,
        name=name,
        is_primary=primary,
        status="running",
        provider_ref=f"follow-up-{uuid.uuid4()}",
        config=config,
    )
    return str(row["computer_id"])


async def _home_on(user_id, computer_id) -> str:
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    assert await workspaces.bind_workspace_to_computer(
        home, computer_id, expected_computer_id=None, dir_name=HOME_FOLDER
    )
    return home


async def _commit(move) -> None:
    """Run Home's move to its end as its runner would, short of the files."""
    move_id, runner = str(move["move_id"]), str(move["runner_id"])
    assert await advance_workspace_move(move_id, runner, "backing_up")
    assert await stage_workspace_move(
        move_id, runner, source_folder=HOME_FOLDER, leftovers_path=None
    )
    assert await commit_workspace_move(move_id, runner)
    assert await finish_workspace_move(move_id, runner, "done")


async def _fail(move, code) -> None:
    move_id, runner = str(move["move_id"]), str(move["runner_id"])
    assert await finish_workspace_move(
        move_id, runner, "failed", error=move_error_detail(code)
    )


async def _removing_primary(user_id):
    old = await _computer(user_id, "Old Mac", primary=True, kind="ssh")
    new = await _computer(user_id, "Cloud")
    home = await _home_on(user_id, old)
    return old, new, home


# ---------------------------------------------------------------------------
# Removing the primary with home_to
# ---------------------------------------------------------------------------


async def test_removing_the_primary_moves_home_then_removes_it(seed_user):
    """The move's runner settles the removal as it ends."""
    user_id = seed_user["user_id"]
    old, new, home = await _removing_primary(user_id)
    manager = _manager()
    manager.run_accepted_move = AsyncMock(side_effect=_commit)
    scheduled = MagicMock()

    left = MachineLeft(
        key=ComputerKeyRemoval(outcome="unreached", fingerprint="SHA256:abc"),
        tunnel_released=True,
    )
    with patch(_SCHEDULE, scheduled), patch(
        "src.server.services.ssh_removal.leave_machine", AsyncMock(return_value=left)
    ) as leave:
        move = await manager.move_home_then_remove(await get_computer(old), new)
        assert move is not None
        waiting = (await get_computer(old))["config"]["removal"]
        assert waiting["state"] == "waiting_for_home"
        assert waiting["move_id"] == str(move["move_id"])

        _key, run = scheduled.call_args.args
        await run()

    assert str((await workspaces.get_workspace(home))["computer_id"]) == new
    assert (await get_computer(new))["is_primary"] is True
    assert await get_computer(old) is None
    leave.assert_awaited_once()
    manager._teardown_machine.assert_awaited_once()
    # What it left on the machine is told off the row it leaves.
    gone = await get_computer(old, include_deleted=True)
    assert gone["config"]["removal"]["state"] == "removed"
    assert gone["config"]["removal"]["removed"]["key"] == {
        "outcome": "unreached",
        "used_by": None,
        "fingerprint": "SHA256:abc",
        "comment": None,
    }


async def test_removing_a_primary_that_is_away_restores_home_from_its_backup(
    seed_user, test_db_pool
):
    """A machine that can't be reached can't hand Home's files over."""
    user_id = seed_user["user_id"]
    old, new, home = await _removing_primary(user_id)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO workspace_files (workspace_id, file_path, file_name, file_size)"
            " VALUES (%s, 'notes.md', 'notes.md', 1)",
            (home,),
        )

    with patch(_SCHEDULE, MagicMock()), patch(
        "src.server.services.ssh_computers.computer_unreachable",
        AsyncMock(side_effect=lambda computer: str(computer["computer_id"]) == old),
    ):
        move = await _manager().move_home_then_remove(await get_computer(old), new)

    assert move["from_backup"] is True


async def test_a_removal_waiting_on_home_survives_a_restart(seed_user):
    """The worker that accepted it went before the move ended; another
    worker's sweep finds the move committed and removes the computer."""
    user_id = seed_user["user_id"]
    old, new, home = await _removing_primary(user_id)
    with patch(_SCHEDULE, MagicMock()):
        move = await _manager().move_home_then_remove(await get_computer(old), new)
    await _commit(move)

    after_restart = _manager()
    left = MachineLeft(key=None, tunnel_released=True)
    with patch(
        "src.server.services.ssh_removal.leave_machine", AsyncMock(return_value=left)
    ):
        await after_restart.remove_after_home_move()

    assert str((await workspaces.get_workspace(home))["computer_id"]) == new
    assert await get_computer(old) is None


async def test_a_move_that_fails_leaves_the_computer_and_says_so(seed_user):
    user_id = seed_user["user_id"]
    old, new, home = await _removing_primary(user_id)
    with patch(_SCHEDULE, MagicMock()):
        move = await _manager().move_home_then_remove(await get_computer(old), new)
    await _fail(move, "source_unavailable")

    manager = _manager()
    await manager.remove_after_home_move()

    row = await get_computer(old)
    assert row is not None and row["is_primary"] is True
    assert row["config"]["removal"] == {
        "state": "home_did_not_move",
        "move_id": str(move["move_id"]),
        "error": {
            "code": "source_unavailable",
            "message": move_error_detail("source_unavailable")["message"],
        },
    }
    assert str((await workspaces.get_workspace(home))["computer_id"]) == old
    manager._teardown_machine.assert_not_awaited()
    # Settled once: a later pass finds nothing waiting.
    await manager.remove_after_home_move()
    assert (await get_computer(old))["config"]["removal"]["state"] == "home_did_not_move"


async def test_a_move_still_running_is_left_alone(seed_user):
    user_id = seed_user["user_id"]
    old, new, _ = await _removing_primary(user_id)
    with patch(_SCHEDULE, MagicMock()):
        await _manager().move_home_then_remove(await get_computer(old), new)

    manager = _manager()
    await manager.remove_after_home_move()

    row = await get_computer(old)
    assert row["config"]["removal"]["state"] == "waiting_for_home"
    manager._teardown_machine.assert_not_awaited()


async def test_the_owner_s_checks_run_with_home_leaving(seed_user):
    """Home on the primary and its being primary refuse nothing once Home is
    leaving; a workspace the owner was not shown still does."""
    user_id = seed_user["user_id"]
    old, _, _ = await _removing_primary(user_id)
    manager = _manager()
    computer = await get_computer(old)

    assert await manager.removal_refusal(computer) == "primary"
    assert await manager.removal_refusal(computer, home_leaves=True) is None
    research = await workspaces.create_workspace_on_computer(user_id, "Research", old)
    assert await manager.removal_refusal(computer, home_leaves=True) == "has_workspaces"
    assert (
        await manager.removal_refusal(
            computer, [str(research["workspace_id"])], home_leaves=True
        )
        is None
    )


# ---------------------------------------------------------------------------
# Make primary, asked with the create
# ---------------------------------------------------------------------------


async def _new_mac(user_id, *, files_backup=True) -> str:
    row = await create_computer(
        user_id,
        kind="ssh",
        name="This Mac",
        is_primary=True,
        status="running",
        provider_ref=f"follow-up-{uuid.uuid4()}",
        config={"make_primary": {"state": "pending"}},
        files_backup=files_backup,
    )
    return str(row["computer_id"])


async def test_a_new_mac_becomes_primary_once_its_first_start_completes(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    home = await _home_on(user_id, cloud)
    mac = await _new_mac(user_id)
    assert (await get_computer(mac))["is_primary"] is False
    manager = _manager()
    manager.run_accepted_move = AsyncMock(side_effect=_commit)
    scheduled = MagicMock()

    with patch(_SCHEDULE, scheduled):
        await manager.apply_make_primary_intent(mac, user_id)
        _key, run = scheduled.call_args.args
        await run()

    row = await get_computer(mac)
    assert row["is_primary"] is True
    assert "make_primary" not in row["config"]
    assert str((await workspaces.get_workspace(home))["computer_id"]) == mac


async def test_make_primary_asked_with_the_create_survives_a_restart(seed_user):
    """The worker the start completed on went before applying it: another
    worker's sweep finds it pending on a running computer."""
    user_id = seed_user["user_id"]
    await _computer(user_id, "Cloud", primary=True)
    await workspaces.get_or_create_home_workspace(user_id)
    mac = await _new_mac(user_id)

    await _manager().apply_pending_make_primary()

    row = await get_computer(mac)
    assert row["is_primary"] is True
    assert "make_primary" not in row["config"]


async def test_make_primary_that_cannot_apply_is_dropped_and_shown(seed_user):
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    await _home_on(user_id, cloud)
    mac = await _new_mac(user_id, files_backup=False)

    await _manager().apply_make_primary_intent(mac, user_id)

    row = await get_computer(mac)
    assert row["is_primary"] is False
    assert row["config"]["make_primary"]["state"] == "dropped"
    assert row["config"]["make_primary"]["error"]["code"] == "not_backed_up"
    assert (await get_computer(cloud))["is_primary"] is True


async def test_a_make_primary_that_must_wait_stays_pending(seed_user):
    """Home's move refused for now (another move under way) is tried again."""
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")
    home = await _home_on(user_id, cloud)
    mac = await _new_mac(user_id)
    with patch(_SCHEDULE, MagicMock()):
        assert await _manager().make_primary(user_id, other)

        await _manager().apply_make_primary_intent(mac, user_id)

    intent = (await get_computer(mac))["config"]["make_primary"]
    assert intent["state"] == "pending" and "taken_at" not in intent
    assert intent["tried_at"]
    assert str((await workspaces.get_workspace(home))["computer_id"]) == cloud


async def test_a_make_primary_whose_worker_went_midway_is_taken_again(seed_user, test_db_pool):
    """Taken, it stays on the row; held past its lease it is pending again."""
    from src.server.database.computer import list_pending_make_primary, take_make_primary_intent

    user_id = seed_user["user_id"]
    await _computer(user_id, "Cloud", primary=True)
    mac = await _new_mac(user_id)

    assert await take_make_primary_intent(mac) is not None
    assert await take_make_primary_intent(mac) is None
    assert mac not in {str(r["computer_id"]) for r in await list_pending_make_primary()}

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET config = jsonb_set(config, '{make_primary,taken_at}',"
            " to_jsonb((NOW() - interval '1 hour')::text)) WHERE computer_id = %s",
            (mac,),
        )
    assert mac in {str(r["computer_id"]) for r in await list_pending_make_primary()}
    await _manager().apply_pending_make_primary()

    row = await get_computer(mac)
    assert row["is_primary"] is True
    assert "make_primary" not in row["config"]


async def test_making_another_computer_primary_ends_a_pending_make_primary(seed_user):
    """The later choice stands: a new Mac still starting does not take Home
    back once it does."""
    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")
    await _home_on(user_id, cloud)
    mac = await _new_mac(user_id)
    manager = _manager()
    manager.run_accepted_move = AsyncMock(side_effect=_commit)
    scheduled = MagicMock()

    with patch(_SCHEDULE, scheduled):
        await manager.make_primary(user_id, other)
        _key, run = scheduled.call_args.args
        await run()

    assert (await get_computer(other))["is_primary"] is True
    assert "make_primary" not in (await get_computer(mac))["config"]


async def test_a_removal_is_recorded_with_the_move_or_not_at_all(seed_user):
    from src.server.database.workspace_move import MoveRefused

    user_id = seed_user["user_id"]
    old, new, _ = await _removing_primary(user_id)
    third = await _computer(user_id, "Third")
    manager = _manager()
    with patch(_SCHEDULE, MagicMock()):
        move = await manager.move_home_then_remove(await get_computer(old), new)
        assert (await get_computer(old))["config"]["removal"]["move_id"] == str(move["move_id"])
        # A second removal meets the first move and records nothing of its own.
        with pytest.raises(MoveRefused):
            await manager.move_home_then_remove(await get_computer(third), new)
    assert "removal" not in (await get_computer(third))["config"]


async def test_one_worker_at_a_time_settles_a_removal(seed_user):
    """The move's runner and every worker's sweep reach the same row."""
    from src.server.database.computer import take_removal_intent

    user_id = seed_user["user_id"]
    old, new, _ = await _removing_primary(user_id)
    with patch(_SCHEDULE, MagicMock()):
        move = await _manager().move_home_then_remove(await get_computer(old), new)
    await _commit(move)

    assert await take_removal_intent(old, str(move["move_id"])) is not None
    other_worker = _manager()
    await other_worker.remove_after_home_move()

    assert await get_computer(old) is not None
    other_worker._teardown_machine.assert_not_awaited()


async def test_a_make_primary_taken_before_another_choice_does_not_undo_it(seed_user):
    """Taken, then ended by a Make primary of another computer while its
    worker checked the machine: applied late, it changes nothing."""
    from src.server.database.computer import take_make_primary_intent
    from src.server.database.workspace_move import MoveRefused, set_primary_computer

    user_id = seed_user["user_id"]
    await _computer(user_id, "Cloud", primary=True)
    other = await _computer(user_id, "Other")
    mac = await _new_mac(user_id)
    stamp = await take_make_primary_intent(mac)

    # Home is on no computer yet, so each flip lands at once.
    assert await set_primary_computer(user_id, other)
    with pytest.raises(MoveRefused) as refused:
        await set_primary_computer(user_id, mac, intent_taken_at=stamp)

    assert refused.value.code == "superseded"
    assert (await get_computer(other))["is_primary"] is True
    assert (await get_computer(mac))["is_primary"] is False


async def test_a_move_claimed_from_a_machine_home_has_left_is_refused(seed_user):
    """``from_backup`` was judged on the machine Home was on; Home on another
    since, the claim is refused to be read again rather than skip its files."""
    from src.server.database.workspace_move import MoveRefused, claim_workspace_move

    user_id = seed_user["user_id"]
    cloud = await _computer(user_id, "Cloud", primary=True)
    target = await _computer(user_id, "Target")
    home = await _home_on(user_id, cloud)

    with pytest.raises(MoveRefused) as refused:
        await claim_workspace_move(
            user_id=user_id,
            workspace_id=home,
            target_computer_id=target,
            make_primary=True,
            from_backup=True,
            expected_source=str(uuid.uuid4()),
        )

    assert refused.value.code == "move_in_progress"
