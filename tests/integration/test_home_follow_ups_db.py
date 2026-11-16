"""What the server carries out for a computer after the request that asked:
removing the primary once Home's move off it commits. It is kept on the
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
        AsyncMock(return_value=True),
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
