"""The folder settle's SQL against a real Postgres.

The planner and the move script are pinned without a database; these pin the
two transactions around them, where parameter typing and the unique folder
index only show up on a real server.
"""

import asyncio
import sys
import uuid
from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ptc_agent.config.agent import AgentConfig, SkillsConfig
from ptc_agent.config.core import (
    FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig,
)
from ptc_agent.core.paths import WorkspaceLayout
from src.server.database import pool as db_pool
from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer
from src.server.database.conversation import create_thread
from src.server.database.runs.lifecycle import start_run
from src.server.database.runs.subagent_runs import start_task_run
from src.server.database.workspace_folders import (
    FOLDER_LAYOUT_VERSION,
    WorkspaceFolderMoving,
    busy_workspace_ids,
    computer_run_in_progress,
    hold_workspace_folder,
    leftovers_path,
    moving_path,
    read_folder_rows,
    record_folder_landings,
    release_former_folder,
    stage_folder_moves,
    workspace_folder_in_use,
    workspace_folders_lock,
)
from src.server.database.workspace_names import placeholder_dir_name
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.persistence.sync_result import BackupIncomplete, SyncResult

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_SYNC = "src.server.services.computer_manager._provisioning.FilePersistenceService.sync_to_db"
_SCRIPT = "src.server.services.computer_manager._folders._run_folder_script"
_SKILL_PARAMS = "src.server.services.computer_manager._provisioning.sandbox_skill_sync_params"


async def _computer(user_id, pool) -> str:
    computer = await create_computer(user_id, kind="docker", name="Test computer")
    computer_id = str(computer["computer_id"])
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET layout_version = %s WHERE computer_id = %s",
            (FOLDER_LAYOUT_VERSION, computer_id),
        )
    return computer_id


async def _row(pool, workspace_id):
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT dir_name, previous_dir_names, config FROM workspaces WHERE workspace_id = %s",
            (workspace_id,),
        )
        return await result.fetchone()


async def _settle(computer_id, landings):
    """Stage, then record what a move script would have reported."""
    async with workspace_folders_lock(computer_id) as conn:
        plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        moves = landings(plan)
        landed = await record_folder_landings(
            conn, computer_id, moves=moves.get("moves", {}), tombstones=moves.get("tombstones", {})
        )
    return plan, landed


async def test_a_rename_stages_then_lands_and_remembers_the_old_folder(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = await workspaces.create_workspace_on_computer(user_id, "Research", computer_id)
    await workspaces.update_workspace(str(a["workspace_id"]), name="Macro")
    a_id = str(a["workspace_id"])

    async with workspace_folders_lock(computer_id) as conn:
        plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        assert [(m.workspace_id, m.source, m.target) for m in plan.moves] == [(a_id, "Research", "Macro")]
        staged = await _row(test_db_pool, a_id)
        assert (staged["dir_name"], staged["previous_dir_names"]) == (moving_path(a_id), ["Research"])
        landed = await record_folder_landings(conn, computer_id, moves={a_id: "Macro"}, tombstones={})

    assert landed == {a_id}
    row = await _row(test_db_pool, a_id)
    assert (row["dir_name"], row["previous_dir_names"]) == ("Macro", ["Research"])
    _version, rows = await read_folder_rows(computer_id)
    assert [r.dir_name for r in rows] == ["Macro"]


async def test_two_workspaces_trade_folders_in_one_settle(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    x = str((await workspaces.create_workspace_on_computer(user_id, "X", computer_id))["workspace_id"])
    y = str((await workspaces.create_workspace_on_computer(user_id, "Y", computer_id))["workspace_id"])
    for workspace_id, name in ((x, "Tmp"), (y, "X"), (x, "Y")):
        await workspaces.update_workspace(workspace_id, name=name)

    _plan, landed = await _settle(
        computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}}
    )

    assert landed == {x, y}
    assert (await _row(test_db_pool, x))["dir_name"] == "Y"
    assert (await _row(test_db_pool, y))["dir_name"] == "X"


async def test_a_tombstone_hands_its_name_to_the_placeholder_holder(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    dead = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status = 'deleted', "
            "config = COALESCE(config, '{}'::jsonb) || '{\"folder_cleanup_pending\": true}' "
            "WHERE workspace_id = %s",
            (dead,),
        )
    live = await workspaces.create_workspace_on_computer(user_id, "Research", computer_id)
    live_id = str(live["workspace_id"])
    assert live["dir_name"].startswith("Research-")

    plan, landed = await _settle(
        computer_id,
        lambda plan: {
            "tombstones": {t.workspace_id: leftovers_path(t.workspace_id) for t in plan.tombstones},
            "moves": {m.workspace_id: m.target for m in plan.moves},
        },
    )

    assert [t.workspace_id for t in plan.tombstones] == [dead]
    assert landed == {live_id}
    assert (await _row(test_db_pool, dead))["dir_name"] == leftovers_path(dead)
    assert (await _row(test_db_pool, live_id))["dir_name"] == "Research"


async def test_a_tombstone_whose_folder_is_gone_frees_its_name(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    dead = str((await workspaces.create_workspace_on_computer(user_id, "Old", computer_id))["workspace_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status = 'deleted', "
            "config = COALESCE(config, '{}'::jsonb) || '{\"folder_cleanup_pending\": true}' "
            "WHERE workspace_id = %s",
            (dead,),
        )

    await _settle(computer_id, lambda plan: {"tombstones": {dead: None}})

    row = await _row(test_db_pool, dead)
    assert row["dir_name"] is None
    assert "folder_cleanup_pending" not in (row["config"] or {})


async def test_a_tombstone_an_earlier_build_cleaned_still_frees_its_name(seed_user, test_db_pool):
    """That build cleared the pending flag but kept ``dir_name``. Unread, the
    row would hold the folder on the unique index, and every landing on it
    would violate and leave the mover staged."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    dead = str((await workspaces.create_workspace_on_computer(user_id, "Q3", computer_id))["workspace_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status = 'deleted', config = '{}'::jsonb WHERE workspace_id = %s",
            (dead,),
        )
    live_id = str((await workspaces.create_workspace_on_computer(user_id, "Macro", computer_id))["workspace_id"])
    await workspaces.update_workspace(live_id, name="Q3")

    plan, landed = await _settle(
        computer_id,
        lambda plan: {
            "tombstones": {t.workspace_id: None for t in plan.tombstones},
            "moves": {m.workspace_id: m.target for m in plan.moves},
        },
    )

    assert [t.workspace_id for t in plan.tombstones] == [dead]
    assert landed == {live_id}
    assert (await _row(test_db_pool, dead))["dir_name"] is None
    assert (await _row(test_db_pool, live_id))["dir_name"] == "Q3"


async def test_a_landing_on_a_folder_taken_meanwhile_stays_staged(seed_user, test_db_pool):
    """A create that claimed the target while the script ran keeps it; the
    mover records nothing and the next settle finds it in staging."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a_id = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a_id, name="Macro")

    async with workspace_folders_lock(computer_id) as conn:
        await stage_folder_moves(conn, computer_id, ignore_busy=True)
        other_row = await workspaces.create_workspace_on_computer(user_id, "Elsewhere", computer_id)
        async with test_db_pool.connection() as other:
            await other.execute(
                "UPDATE workspaces SET dir_name = 'Macro' WHERE workspace_id = %s",
                (other_row["workspace_id"],),
            )
        landed = await record_folder_landings(conn, computer_id, moves={a_id: "Macro"}, tombstones={})

    assert landed == set()
    assert (await _row(test_db_pool, a_id))["dir_name"] == moving_path(a_id)


async def _run_in(workspace_id, *, background=False) -> str:
    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    run_id = str(uuid.uuid4())
    if background:
        await start_task_run(task_run_id=run_id, thread_id=thread_id, task_id=run_id, cause="init")
    else:
        await start_run(run_id=run_id, thread_id=thread_id, request_key=run_id)
    return run_id


async def _pending_changes(user_id, pool, computer_id) -> dict[str, str]:
    """A renamed A, a renamed B and a deleted C awaiting cleanup."""
    ids = {}
    for name in ("A", "B", "C"):
        row = await workspaces.create_workspace_on_computer(user_id, name, computer_id)
        ids[name] = str(row["workspace_id"])
    await workspaces.update_workspace(ids["A"], name="A renamed")
    await workspaces.update_workspace(ids["B"], name="B renamed")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status = 'deleted', "
            "config = COALESCE(config, '{}'::jsonb) || '{\"folder_cleanup_pending\": true}' "
            "WHERE workspace_id = %s",
            (ids["C"],),
        )
    return ids


async def _plan(computer_id, own_run=None):
    async with workspace_folders_lock(computer_id) as conn:
        plan = await stage_folder_moves(conn, computer_id, own_run_id=own_run)
    return sorted(m.workspace_id for m in plan.moves), [t.workspace_id for t in plan.tombstones]


@pytest.mark.parametrize("background", [False, True])
async def test_a_run_in_any_workspace_keeps_every_folder(seed_user, test_db_pool, background):
    """An agent reaches a sibling's folder as ``../<folder>``, so a turn or a
    background task in B keeps A's folder and C's tombstone where they are."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    ids = await _pending_changes(user_id, test_db_pool, computer_id)
    await _run_in(ids["B"], background=background)

    _version, rows = await read_folder_rows(computer_id)
    assert await busy_workspace_ids(computer_id, rows) == set(ids.values())
    assert await _plan(computer_id) == ([], [])
    # A delete's own cleanup of C defers on the same run.
    async with workspace_folders_lock(computer_id) as conn:
        assert await computer_run_in_progress(conn, computer_id)


async def test_only_the_turn_this_acquisition_serves_lets_folders_move(seed_user, test_db_pool):
    """The acquiring turn is not using any folder yet, and a run on another
    computer cannot reach this one's."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    ids = await _pending_changes(user_id, test_db_pool, computer_id)
    own_run = await _run_in(ids["A"])
    elsewhere = await _computer(user_id, test_db_pool)
    other = await workspaces.create_workspace_on_computer(user_id, "Other", elsewhere)
    await _run_in(str(other["workspace_id"]))

    _version, rows = await read_folder_rows(computer_id)
    assert await busy_workspace_ids(computer_id, rows, own_run_id=own_run) == set()
    assert await _plan(computer_id, own_run) == (sorted([ids["A"], ids["B"]]), [ids["C"]])


async def test_a_folder_taken_stops_being_a_siblings_former_folder(seed_user, test_db_pool):
    """Every reader folds a former folder into its workspace, so once a sibling
    holds the name, the old spelling names only the sibling."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a_id = str((await workspaces.create_workspace_on_computer(user_id, "Alpha", computer_id))["workspace_id"])
    await workspaces.update_workspace(a_id, name="Beta")
    await _settle(computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}})
    assert (await _row(test_db_pool, a_id))["previous_dir_names"] == ["Alpha"]

    await workspaces.create_workspace_on_computer(user_id, "alpha", computer_id)
    assert (await _row(test_db_pool, a_id))["previous_dir_names"] == []

    c_id = str((await workspaces.create_workspace_on_computer(user_id, "Gamma", computer_id))["workspace_id"])
    await workspaces.update_workspace(a_id, name="Delta")
    await workspaces.update_workspace(c_id, name="Beta")
    await _settle(computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}})
    assert (await _row(test_db_pool, a_id))["previous_dir_names"] == []
    assert (await _row(test_db_pool, c_id))["previous_dir_names"] == ["Gamma"]


async def test_a_new_workspace_never_takes_the_folder_a_staged_move_left(seed_user, test_db_pool):
    """Staged, but the script never ran: the content is still in the old folder,
    and ``previous_dir_names`` is the only way back to it."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    async with workspace_folders_lock(computer_id) as conn:
        await stage_folder_moves(conn, computer_id, ignore_busy=True)

    b = await workspaces.create_workspace_on_computer(user_id, "research", computer_id)

    assert b["dir_name"].startswith("research-")
    row = await _row(test_db_pool, a)
    assert (row["dir_name"], row["previous_dir_names"]) == (moving_path(a), ["Research"])


async def test_a_file_change_in_flight_keeps_its_folder(seed_user, test_db_pool):
    """A change read its paths from the row, so the folder waits for it; the
    settle lets go of every folder it held once its landings are recorded."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")

    async with workspace_folder_in_use(a):
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        assert not plan.moves
        assert (await _row(test_db_pool, a))["dir_name"] == "Research"

    _plan, landed = await _settle(
        computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}}
    )
    assert landed == {a}
    async with workspace_folder_in_use(a):
        assert (await _row(test_db_pool, a))["dir_name"] == "Macro"


async def _settle_can_hold(pool, workspace_id) -> bool:
    async with pool.connection() as conn:
        held = await hold_workspace_folder(conn, workspace_id)
        await conn.execute("SELECT pg_advisory_unlock_all()")
        return held


async def test_a_folder_stays_held_until_every_hold_on_it_ends(
    patched_get_db_connection, test_db_pool
):
    """Every hold in a process shares one session, where Postgres counts a
    key's shared locks, so one ending leaves the folder held for the rest: one
    stacked inside an attach, or another request's."""
    a = str(uuid.uuid4())
    other_holding, other_done = asyncio.Event(), asyncio.Event()

    async def other_request():
        async with workspace_folder_in_use(a):
            other_holding.set()
            await other_done.wait()

    other = asyncio.create_task(other_request())
    await asyncio.wait_for(other_holding.wait(), timeout=10)
    async with workspace_folder_in_use(a):
        async with workspace_folder_in_use(a):
            pass
        assert not await _settle_can_hold(test_db_pool, a)
    assert not await _settle_can_hold(test_db_pool, a)
    other_done.set()
    await other
    assert await _settle_can_hold(test_db_pool, a)


async def test_a_hold_waits_for_a_settle_without_holding_up_other_folders(
    patched_get_db_connection, test_db_pool
):
    """A settle holds a folder exclusively while it moves. A hold on it polls
    until the settle lets go, or past the wait reports the folder moving, and
    meanwhile the shared session takes and releases other folders' holds."""
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    entered = asyncio.Event()

    async def change():
        async with workspace_folder_in_use(a):
            entered.set()

    async with test_db_pool.connection() as settle:
        assert await hold_workspace_folder(settle, a)
        with (
            patch("src.server.database.workspace_folders._LOCK_WAIT_S", 0.3),
            pytest.raises(WorkspaceFolderMoving),
        ):
            async with workspace_folder_in_use(a):
                pass
        waiting = asyncio.create_task(change())
        async with asyncio.timeout(2):
            async with workspace_folder_in_use(b):
                pass
        await asyncio.sleep(0.3)
        assert not entered.is_set()
        await settle.execute("SELECT pg_advisory_unlock_all()")
    await asyncio.wait_for(waiting, timeout=5)
    assert await _settle_can_hold(test_db_pool, a)
    assert await _settle_can_hold(test_db_pool, b)


async def test_a_deleted_workspace_keeps_its_folder_while_a_change_writes_to_it(
    seed_user, test_db_pool
):
    """The change started before the delete; clearing the folder under it
    would let its write recreate the folder for the name's next owner."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])

    async with workspace_folder_in_use(a):
        async with test_db_pool.connection() as conn:
            await conn.execute("UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s", (a,))
        b = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        assert not plan.tombstones
        assert not plan.moves

    plan, landed = await _settle(
        computer_id,
        lambda plan: {
            "moves": {m.workspace_id: m.target for m in plan.moves},
            "tombstones": {t.workspace_id: None for t in plan.tombstones},
        },
    )
    assert [t.workspace_id for t in plan.tombstones] == [a]
    assert landed == {b}
    assert (await _row(test_db_pool, b))["dir_name"] == "Research"


async def test_a_staged_workspace_keeps_its_name_until_the_move_is_recorded(seed_user, test_db_pool):
    """The last landing went to the name's folder, which only the name keeps
    from being given to another workspace."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    async with workspace_folders_lock(computer_id) as conn:
        await stage_folder_moves(conn, computer_id, ignore_busy=True)

    with pytest.raises(WorkspaceFolderMoving):
        await workspaces.update_workspace(a, name="Elsewhere")
    updated = await workspaces.update_workspace(a, name="Macro", description="notes")
    assert (updated["name"], updated["description"]) == ("Macro", "notes")


async def test_a_new_workspace_never_takes_the_folder_a_deleted_staged_move_landed_on(
    seed_user, test_db_pool
):
    """Deleted mid-move, its content may sit where its last landing put it
    until its clearing takes that folder, whoever holds it by then."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    async with workspace_folders_lock(computer_id) as conn:
        await stage_folder_moves(conn, computer_id, ignore_busy=True)
    async with test_db_pool.connection() as conn:
        await conn.execute("UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s", (a,))

    b = await workspaces.create_workspace_on_computer(user_id, "Macro", computer_id)

    assert b["dir_name"].startswith("Macro-")


async def _landings(pool, workspace_id):
    return ((await _row(pool, workspace_id))["config"] or {}).get("folder_landings")


async def test_every_folder_planned_for_a_staged_row_stays_recorded_until_it_lands(
    seed_user, test_db_pool
):
    """The move script trusts its journal only where it names one of these, so
    a retried pass adds nothing twice and a pass planning elsewhere adds that."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")

    for _ in range(2):
        _plan, landed = await _settle(computer_id, lambda plan: {"moves": {a: moving_path(a)}})
        assert landed == {a}
        assert await _landings(test_db_pool, a) == ["Macro"]

    # A busy workspace on the target keeps it, so the row goes back instead.
    other = str((await workspaces.create_workspace_on_computer(user_id, "Elsewhere", computer_id))["workspace_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute("UPDATE workspaces SET dir_name = 'Macro' WHERE workspace_id = %s", (other,))
    await _run_in(other)
    for folder in (moving_path(a), "Research"):
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id)
            assert [(m.workspace_id, m.target) for m in plan.moves] == [(a, "Research")]
            assert await _landings(test_db_pool, a) == ["Macro", "Research"]
            landed = await record_folder_landings(conn, computer_id, moves={a: folder}, tombstones={})
        assert landed == {a}
    row = await _row(test_db_pool, a)
    assert row["dir_name"] == "Research"
    assert "folder_landings" not in row["config"]


async def test_a_folder_taken_stops_being_a_staged_siblings_landing(seed_user, test_db_pool):
    """Its content is the taker's now, which the sibling's move script would
    take back on a journal entry naming it. Matched under casefold."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Straße")
    await _settle(computer_id, lambda plan: {"moves": {a: moving_path(a)}})
    assert await _landings(test_db_pool, a) == ["Straße"]

    b = str((await workspaces.create_workspace_on_computer(user_id, "Notes", computer_id))["workspace_id"])
    async with test_db_pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await release_former_folder(cur, computer_id=computer_id, workspace_id=b, folder="STRASSE")
    assert await _landings(test_db_pool, a) == []


async def test_a_new_workspace_never_takes_a_folder_a_staged_row_may_have_landed_on(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    await _settle(computer_id, lambda plan: {"moves": {a: moving_path(a)}})
    async with test_db_pool.connection() as conn:
        await conn.execute(
            """UPDATE workspaces SET config = jsonb_set(config, '{folder_landings}', '["Macro", "Q3"]')
               WHERE workspace_id = %s""",
            (a,),
        )

    b = await workspaces.create_workspace_on_computer(user_id, "q3", computer_id)

    assert b["dir_name"].startswith("q3-")


async def test_a_new_workspace_never_takes_the_placeholder_a_stage_plans_meanwhile(
    seed_user, test_db_pool, monkeypatch
):
    """With its name and old folder both held, a staged row lands on a hashed
    placeholder that only its config records. A create that read the held
    folders just before the stage committed would take that placeholder after."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    s = str((await workspaces.create_workspace_on_computer(user_id, "A", computer_id))["workspace_id"])
    await workspaces.update_workspace(s, name="B")
    await _settle(computer_id, lambda plan: {"moves": {s: moving_path(s)}})
    for name, folder in (("Tee", "B"), ("Ewe", "A")):
        other = str((await workspaces.create_workspace_on_computer(user_id, name, computer_id))["workspace_id"])
        async with test_db_pool.connection() as conn:
            await conn.execute("UPDATE workspaces SET dir_name = %s WHERE workspace_id = %s", (folder, other))
    await _run_in(other)
    d = placeholder_dir_name("B", s, hex_chars=8)
    real_read, stages = workspaces.get_workspace_dir_names_for_computer, []

    async def stage():
        async with workspace_folders_lock(computer_id) as conn:
            return await stage_folder_moves(conn, computer_id)

    async def stage_after_the_read(cid, *, conn=None):
        held = await real_read(cid, conn=conn)
        stages.append(asyncio.create_task(stage()))
        # Until the stage has committed, or waits for the create to.
        while not stages[0].done():
            async with test_db_pool.connection() as probe:
                waits = await probe.execute(
                    "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
                )
                if await waits.fetchone():
                    break
            await asyncio.sleep(0.01)
        return held

    monkeypatch.setattr(workspaces, "get_workspace_dir_names_for_computer", stage_after_the_read)
    n = await workspaces.create_workspace_on_computer(user_id, d, computer_id)
    [move] = (await stages[0]).moves

    assert move.workspace_id == s and move.target != n["dir_name"] == d
    assert await _landings(test_db_pool, s) == ["B", move.target]


async def test_a_config_replace_never_touches_the_settles_landings(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    c = str((await workspaces.create_workspace_on_computer(user_id, "Notes", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    await _settle(computer_id, lambda plan: {"moves": {a: moving_path(a)}})

    for workspace_id in (a, c):
        await workspaces.update_workspace(
            workspace_id, config={"folder_landings": ["Notes"], "theme": "dark"}
        )
    assert (await _row(test_db_pool, a))["config"] == {"folder_landings": ["Macro"], "theme": "dark"}
    assert (await _row(test_db_pool, c))["config"] == {"theme": "dark"}


async def test_a_row_given_a_folder_by_a_bind_is_no_longer_mid_move(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    elsewhere = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    await _settle(computer_id, lambda plan: {"moves": {a: moving_path(a)}})

    assert await workspaces.bind_workspace_to_computer(
        a, elsewhere, expected_computer_id=computer_id, dir_name="Macro"
    )
    row = await _row(test_db_pool, a)
    assert row["dir_name"] == "Macro"
    assert "folder_landings" not in row["config"]


def _manager():
    return WorkspaceManager(
        AgentConfig(
            security=SecurityConfig(), logging=LoggingConfig(), mcp=MCPConfig(),
            sandbox=SandboxConfig(provider="docker"), filesystem=FilesystemConfig(),
            skills=SkillsConfig(enabled=False),
        )
    )


def _backup(computer_id, workspace_id, *, stale_folder):
    """A strict backup whose caller read the folder before this pass, as the sweep does."""
    return _manager().backup_project_files(
        workspace_id,
        computer_id=computer_id,
        strict=True,
        expected_sandbox_id="sb-folders",
        session=SimpleNamespace(
            sandbox=SimpleNamespace(
                sandbox_id="sb-folders", ensure_sandbox_ready=AsyncMock()
            )
        ),
        layout=WorkspaceLayout("/home/workspace", stale_folder),
    )


async def test_a_backup_keeps_its_folder_until_the_scan_ends(seed_user, test_db_pool):
    """Moved mid-scan, the folder reads as missing, which counts as mirrored: a
    strict caller would destroy the only copy of what changed since the last
    backup. The scan walks the folder the row names under the hold, and a
    settle leaves it there until the scan ends."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    await _settle(computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}})
    await workspaces.update_workspace(a, name="Final")
    scans = []

    async def scan(_workspace_id, _sandbox, *, layout, conn=None):
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        scans.append((layout.dir_name, [m.workspace_id for m in plan.moves]))
        return SyncResult(synced=1)

    with patch(_SYNC, scan):
        assert await _backup(computer_id, a, stale_folder="Research")

    assert scans == [("Macro", [])]
    _plan, landed = await _settle(
        computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}}
    )
    assert landed == {a}
    assert (await _row(test_db_pool, a))["dir_name"] == "Final"


async def test_a_strict_backup_refuses_a_folder_left_mid_move(seed_user, test_db_pool):
    """Staged and never landed, the content is in one of three folders until
    the next settle finds it, so no scan can say it saved it."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    async with workspace_folders_lock(computer_id) as conn:
        await stage_folder_moves(conn, computer_id, ignore_busy=True)

    sync = AsyncMock(return_value=SyncResult(root_missing=True))
    with patch(_SYNC, sync), pytest.raises(BackupIncomplete, match="moving"):
        await _backup(computer_id, a, stale_folder="Research")

    sync.assert_not_awaited()


async def _lands_every_move(_runtime, _root, *, moves, tombstones, prune, deferred=()):
    """The move script's report when every folder reaches its target."""
    return {"moves": {m["id"]: m["target"] for m in moves}, "tombstones": {}}


async def test_an_acquisition_keeps_its_folder_until_attachment_ends(seed_user, test_db_pool):
    """/start and a file route have no run a settle counts as busy. Moved
    mid-restore, the rest of the restore recreates the old folder and marks
    itself complete, and a backup of the new folder then prunes what never
    arrived. Attachment writes to the folder read under the hold, and a settle
    on another connection leaves it there until attachment ends."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    await workspaces.update_workspace(a, name="Macro")
    manager = _manager()
    session = SimpleNamespace(sandbox=SimpleNamespace(runtime=object()))
    attaching, attached = asyncio.Event(), asyncio.Event()
    folders = []

    async def attach(binding, _session, *, user_id=None):
        folders.append(binding.dir_name)
        attaching.set()
        await attached.wait()

    with (
        patch(_SCRIPT, _lands_every_move),
        patch.object(manager, "_acquire_session", AsyncMock(return_value=session)),
        patch.object(manager, "_ensure_project_attached", attach),
    ):
        acquiring = asyncio.create_task(manager.get_session_for_workspace(a))
        await asyncio.wait_for(attaching.wait(), timeout=10)
        await workspaces.update_workspace(a, name="Final")
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        attached.set()
        assert await acquiring is session

    assert folders == ["Macro"]
    assert not plan.moves
    assert (await _row(test_db_pool, a))["dir_name"] == "Macro"
    _plan, landed = await _settle(
        computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}}
    )
    assert landed == {a}
    assert (await _row(test_db_pool, a))["dir_name"] == "Final"


async def test_an_asset_sync_keeps_its_folder_until_the_overlay_is_written(seed_user, test_db_pool):
    """The acquisition's binding predates a settle on any worker. Built through
    the old folder, the overlay recreates it and stamps the claim current in the
    tool ledger, so the landed folder never gets it. The sync builds into the
    folder the row names under the hold, a settle on another connection leaves
    it there until the sync ends, and a sync under a hold its caller already
    has (an attachment rebuilding its overlay) stacks on it rather than waiting."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    manager = _manager()
    stale = await manager.resolve_binding(a)
    await workspaces.update_workspace(a, name="Macro")
    await _settle(computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}})
    await workspaces.update_workspace(a, name="Final")
    syncs = []

    async def sync(**kwargs):
        async with workspace_folders_lock(computer_id) as conn:
            plan = await stage_folder_moves(conn, computer_id, ignore_busy=True)
        syncs.append((kwargs["project"].dir_name, [m.workspace_id for m in plan.moves]))
        return SimpleNamespace(layout_version=FOLDER_LAYOUT_VERSION)

    sandbox = SimpleNamespace(sync_sandbox_assets=sync, vault_secrets={})
    with (
        patch(_SKILL_PARAMS, AsyncMock(return_value={})),
        patch.object(manager, "_vault_snapshot", AsyncMock(return_value=(None, {}, ""))),
    ):
        async with workspace_folder_in_use(a):
            assert await asyncio.wait_for(
                manager._sync_sandbox_assets(stale, user_id, sandbox), timeout=10
            )

    assert stale.dir_name == "Research"
    assert syncs == [("Macro", [])]
    _plan, landed = await _settle(
        computer_id, lambda plan: {"moves": {m.workspace_id: m.target for m in plan.moves}}
    )
    assert landed == {a}
    assert (await _row(test_db_pool, a))["dir_name"] == "Final"


@asynccontextmanager
async def _pool_of(test_db_uri, patched_get_db_connection, *, max_size, timeout):
    """Every ``get_db_connection`` the suite patched, on a pool of ``max_size``."""
    pool = AsyncConnectionPool(
        conninfo=test_db_uri,
        min_size=max_size,
        max_size=max_size,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
        open=False,
    )

    @asynccontextmanager
    async def connection(conn=None):
        if conn is not None:
            yield conn
            return
        async with pool.connection(timeout=timeout) as owned:
            yield owned

    await pool.open(wait=True)
    try:
        with ExitStack() as stack:
            for name, module in list(sys.modules.items()):
                if name.startswith("src.") and (
                    getattr(module, "get_db_connection", None) is patched_get_db_connection
                ):
                    stack.enter_context(patch.object(module, "get_db_connection", connection))
            yield
    finally:
        await pool.close()


async def test_concurrent_cold_attach_fits_the_pool(
    seed_user, test_db_pool, test_db_uri, patched_get_db_connection
):
    """After a deploy every attach is cold, and each holds its folder while the
    restore and the overlay run, each holding it again inside. On a pooled
    connection per hold, two attaches take every slot of a small pool and wait
    for one more until its timeout fails them both. The holds ride the process's
    lock session instead, so the reads under them share even a one-slot pool."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    ids = [
        str((await workspaces.create_workspace_on_computer(user_id, name, computer_id))["workspace_id"])
        for name in ("Research", "Macro")
    ]
    manager = _manager()
    both_holding = asyncio.Barrier(len(ids))
    synced = []

    async def ensure_dirs(_dir_name):
        # Both attaches hold their folders before either restores.
        await both_holding.wait()

    async def sync(**kwargs):
        synced.append(kwargs["project"].dir_name)
        return SimpleNamespace(layout_version=FOLDER_LAYOUT_VERSION)

    sessions = {
        workspace_id: SimpleNamespace(sandbox=SimpleNamespace(
            runtime=None,
            sandbox_id=f"sb-{workspace_id}",
            vault_secrets={},
            _ensure_workspace_dirs=ensure_dirs,
            # A restore marker is there, so the check only reconciles the flag.
            adownload_file_bytes=AsyncMock(return_value=b"restored"),
            workspace_overlay_missing=AsyncMock(side_effect=[True, False]),
            sync_sandbox_assets=sync,
        ))
        for workspace_id in ids
    }
    # These sandboxes have no runtime, which no pass can link through.
    link_pass = AsyncMock(return_value=True)

    with (
        patch(_SKILL_PARAMS, AsyncMock(return_value={})),
        patch.object(
            manager, "_acquire_session",
            AsyncMock(side_effect=lambda workspace_id, **_kw: sessions[workspace_id]),
        ),
        patch.object(manager, "_reconcile_skills", link_pass),
        patch.object(manager, "_apply_session_mcp", AsyncMock()),
        patch.object(manager, "_workspace_tool_view", MagicMock(return_value=object())),
        patch.object(manager, "_mint_sandbox_tokens", AsyncMock(return_value={})),
        patch.object(manager, "_vault_snapshot", AsyncMock(return_value=(None, {}, ""))),
    ):
        async with _pool_of(test_db_uri, patched_get_db_connection, max_size=1, timeout=3):
            got = await asyncio.wait_for(
                asyncio.gather(*(
                    manager.get_session_for_workspace(workspace_id, user_id=user_id)
                    for workspace_id in ids
                )),
                timeout=60,
            )

    assert got == [sessions[workspace_id] for workspace_id in ids]
    assert sorted(synced) == ["Macro", "Research"]
    assert link_pass.await_count == len(ids)
    assert manager._projects_attached == {(w, f"sb-{w}") for w in ids}


async def test_concurrent_cold_restores_fit_the_pool(
    seed_user, test_db_pool, test_db_uri, patched_get_db_connection
):
    """A recreated sandbox restores under the attach's folder hold, and the
    restore keeps the sync lock on a pooled connection for as long as it copies.
    Were the hold on a slot too, that is two per attach for the whole copy: on a
    pool one larger than the attaches, one restore runs and the rest time out,
    and the acquisition reads the swallowed failure as files restored."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    ids = [
        str((await workspaces.create_workspace_on_computer(user_id, name, computer_id))["workspace_id"])
        for name in ("Research", "Macro")
    ]
    manager = _manager()
    all_restoring = asyncio.Barrier(len(ids))
    restored = []

    async def restore_locked(workspace_id, _sandbox, _conn, _layout):
        # Every restore holds its sync lock at once, as a burst of cold
        # attaches after a deploy does.
        await asyncio.wait_for(all_restoring.wait(), timeout=10)
        restored.append(workspace_id)
        return {"restored": 1, "errors": 0}

    async def sync(**_kwargs):
        return SimpleNamespace(layout_version=FOLDER_LAYOUT_VERSION)

    sessions = {
        workspace_id: SimpleNamespace(sandbox=SimpleNamespace(
            runtime=None,
            sandbox_id=None,
            vault_secrets={},
            _ensure_workspace_dirs=AsyncMock(),
            # No restore marker: the sandbox was recreated empty.
            adownload_file_bytes=AsyncMock(return_value=None),
            workspace_overlay_missing=AsyncMock(side_effect=[True, False]),
            sync_sandbox_assets=sync,
        ))
        for workspace_id in ids
    }

    with (
        patch(_SKILL_PARAMS, AsyncMock(return_value={})),
        patch(
            "src.server.services.persistence.restore.get_files_for_workspace",
            AsyncMock(return_value=[{"file_path": "notes.md"}]),
        ),
        patch("src.server.services.persistence.restore._restore_locked", restore_locked),
        patch.object(
            manager, "_acquire_session",
            AsyncMock(side_effect=lambda workspace_id, **_kw: sessions[workspace_id]),
        ),
        patch.object(manager, "_apply_session_mcp", AsyncMock()),
        patch.object(manager, "_workspace_tool_view", MagicMock(return_value=object())),
        patch.object(manager, "_mint_sandbox_tokens", AsyncMock(return_value={})),
        patch.object(manager, "_vault_snapshot", AsyncMock(return_value=(None, {}, ""))),
    ):
        async with _pool_of(
            test_db_uri, patched_get_db_connection, max_size=len(ids) + 1, timeout=3
        ):
            await asyncio.wait_for(
                asyncio.gather(*(
                    manager.get_session_for_workspace(workspace_id, user_id=user_id)
                    for workspace_id in ids
                )),
                timeout=60,
            )

    assert sorted(restored) == sorted(ids)


async def test_a_settle_and_those_waiting_on_it_take_no_pool_slot(
    seed_user, test_db_pool, test_db_uri, patched_get_db_connection
):
    """A settle keeps the computer's folder lock through a sandbox script that
    can outlast a minute, and every acquisition on the computer waits behind
    it. On pooled connections that is one slot for the holder and one per
    waiter, until requests with nothing to do with folders time out. The
    holder has a connection of its own, and a waiter opens one only once the
    lock is free."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id, test_db_pool)
    a = str((await workspaces.create_workspace_on_computer(user_id, "Research", computer_id))["workspace_id"])
    opened, open_session = [], db_pool.open_session_connection

    async def counted():
        opened.append(1)
        return await open_session()

    async def settle():
        async with workspace_folders_lock(computer_id) as conn:
            return conn is not None

    with patch.object(db_pool, "open_session_connection", counted):
        async with _pool_of(test_db_uri, patched_get_db_connection, max_size=1, timeout=3):
            async with workspace_folders_lock(computer_id) as held:
                assert held is not None
                with patch("src.server.database.workspace_folders._LOCK_WAIT_S", 0.5):
                    assert not await settle()
                waiters = [asyncio.create_task(settle()) for _ in range(2)]
                await asyncio.sleep(0.6)
                assert not any(w.done() for w in waiters)
                assert (await workspaces.get_workspace(a))["name"] == "Research"
                assert len(opened) == 1
            assert await asyncio.wait_for(asyncio.gather(*waiters), timeout=10) == [True, True]


def _counted_sessions(open_session):
    """``open_session_connection`` counting the sessions open at once."""
    live, peak = [0], [0]

    async def opened():
        conn = await open_session()
        live[0] += 1
        peak[0] = max(peak[0], live[0])
        close = conn.close

        async def closed():
            if not conn.closed:
                live[0] -= 1
            await close()

        conn.close = closed
        return conn

    return opened, peak


@pytest.mark.parametrize("holder", ["this process", "another worker"])
async def test_callers_behind_a_freed_lock_open_one_session_at_a_time(
    seed_user, test_db_pool, test_db_uri, patched_get_db_connection, holder
):
    """Every acquisition on a computer with a folder to move waits on its lock.
    Waiters that each opened a session once the lock came free would, across
    workers, open enough outside the pool to exhaust the server; within a
    process only one of them looks at a time."""
    computer_id = await _computer(seed_user["user_id"], test_db_pool)
    open_session = db_pool.open_session_connection
    opened, peak = _counted_sessions(open_session)

    async def settle():
        async with workspace_folders_lock(computer_id) as conn:
            await asyncio.sleep(0.05)
            return conn is not None

    with patch.object(db_pool, "open_session_connection", opened):
        if holder == "this process":
            async with workspace_folders_lock(computer_id) as held:
                assert held is not None
                waiters = [asyncio.create_task(settle()) for _ in range(5)]
                await asyncio.sleep(0.6)
        else:
            other = await open_session()
            try:
                await other.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                    (f"WORKSPACE_FOLDERS:{computer_id}",),
                )
                waiters = [asyncio.create_task(settle()) for _ in range(5)]
                await asyncio.sleep(0.6)
            finally:
                await other.close()
        assert await asyncio.wait_for(asyncio.gather(*waiters), timeout=20) == [True] * 5

    assert peak[0] == 1


async def test_a_process_holds_a_bounded_number_of_folder_lock_sessions(
    seed_user, test_db_pool
):
    """Settles on different computers each hold a session outside the pool.
    Past the bound a settle waits its turn, and past the wait it settles
    nothing this time, as it does behind another holder."""
    first = await _computer(seed_user["user_id"], test_db_pool)
    second = await _computer(seed_user["user_id"], test_db_pool)

    async def settle(computer_id):
        async with workspace_folders_lock(computer_id) as conn:
            return conn is not None

    with patch("src.server.database.workspace_folders._LOCK_SESSIONS_MAX", 1):
        async with workspace_folders_lock(first) as held:
            assert held is not None
            with patch("src.server.database.workspace_folders._LOCK_WAIT_S", 0.5):
                assert not await settle(second)
            waiting = asyncio.create_task(settle(second))
            await asyncio.sleep(0.6)
            assert not waiting.done()
        assert await asyncio.wait_for(waiting, timeout=10)
