"""Folding an account's former Home rows into Home, on a real Postgres.

An account merge leaves the merged account's Home beside the Home the
account's own id derives. Resolving Home moves what that row holds into it and
retires the row, so the account lists one All workspaces entry with all its
history.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.server.database import workspace as workspaces
from src.server.database.home_fold import MOVES, STAYS
from src.server.database.home_workspace import get_home_workspace_id

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_DAY = timedelta(days=1)
_STATUS = "SELECT status FROM workspaces WHERE workspace_id = %s"


async def _former(user_id, old_user_id) -> str:
    row = await workspaces.create_workspace(
        user_id,
        "Home",
        workspace_id=get_home_workspace_id(old_user_id),
        status="unbound",
    )
    return str(row["workspace_id"])


async def _thread(pool, workspace_id, index, created_at) -> str:
    thread_id = str(uuid.uuid4())
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO conversation_threads "
            "(conversation_thread_id, workspace_id, msg_type, current_status, "
            " thread_index, created_at, updated_at) "
            "VALUES (%s, %s, 'ptc', 'completed', %s, %s, %s)",
            (thread_id, workspace_id, index, created_at, created_at),
        )
    return thread_id


async def _rows(pool, sql, *params) -> list[dict]:
    async with pool.connection() as conn:
        result = await conn.execute(sql, params)
        return await result.fetchall()


async def test_every_workspace_keyed_table_moves_or_stays(test_db_pool):
    """A new table keyed by workspace fails here until someone decides whether
    the fold carries it into Home or leaves it on the retired row.

    Any column ending in workspace_id counts, not only workspace_id itself:
    computers.origin_workspace_id names a workspace just the same, so it needs
    the same decision.
    """
    found = {
        row["table_name"]
        for row in await _rows(
            test_db_pool,
            "SELECT DISTINCT c.table_name FROM information_schema.columns c "
            "JOIN information_schema.tables t USING (table_schema, table_name) "
            "WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' "
            "  AND c.column_name ~ '(^|_)workspace_id$' "
            "  AND c.table_name <> 'workspaces'",
        )
    }

    assert not set(MOVES) & set(STAYS)
    assert found == set(MOVES) | set(STAYS), {
        "unclassified": sorted(found - set(MOVES) - set(STAYS)),
        "not in the schema": sorted((set(MOVES) | set(STAYS)) - found),
    }


async def test_former_rows_fold_into_home_and_retire(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    now = datetime.now(timezone.utc)
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    elsewhere = str((await workspaces.create_workspace(user_id, "Research"))["workspace_id"])
    await _thread(test_db_pool, home, 0, now - _DAY)
    first = await _former(user_id, "old-account-1")
    second = await _former(user_id, "old-account-2")
    # Both former rows count their threads from zero.
    newest = await _thread(test_db_pool, first, 0, now - 2 * _DAY)
    oldest = await _thread(test_db_pool, second, 0, now - 9 * _DAY)
    middle = await _thread(test_db_pool, first, 1, now - 5 * _DAY)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO workspace_skill_disables (workspace_id, name) "
            "VALUES (%s, 'stays')",
            (first,),
        )
        await conn.execute(
            "INSERT INTO chart_annotations "
            "(workspace_id, chart_id, symbol, timeframe, annotation_id, payload) "
            "VALUES (%s, 'c', 'AAPL', '1d', 'a', '{\"from\": \"home\"}'), "
            "       (%s, 'c', 'AAPL', '1d', 'a', '{\"from\": \"former\"}')",
            (home, first),
        )
        await conn.execute(
            "INSERT INTO automations "
            "(user_id, name, trigger_type, instruction, workspace_id) "
            "VALUES (%s, 'daily', 'cron', 'hi', %s)",
            (user_id, second),
        )
        # Stored elsewhere by a build that let a pin sit in another workspace.
        await conn.execute(
            "INSERT INTO automations "
            "(user_id, name, trigger_type, instruction, workspace_id, "
            " thread_strategy, conversation_thread_id) "
            "VALUES (%s, 'pinned', 'cron', 'hi', %s, 'continue', %s)",
            (user_id, elsewhere, middle),
        )
        # One run's thread moves with it; the other's is already deleted.
        await conn.execute(
            "INSERT INTO conversation_usages "
            "(conversation_response_id, user_id, conversation_thread_id, "
            " workspace_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s, 'completed'), "
            "       (gen_random_uuid(), %s, gen_random_uuid(), %s, 'completed')",
            (user_id, newest, first, user_id, first),
        )

    resolved = await workspaces.get_or_create_home_workspace(user_id)

    assert str(resolved["workspace_id"]) == home
    threads = await _rows(
        test_db_pool,
        "SELECT conversation_thread_id::text AS id, thread_index, updated_at "
        "FROM conversation_threads WHERE workspace_id = %s ORDER BY thread_index",
        home,
    )
    # Each former row's threads append in the order they were started.
    assert [t["id"] for t in threads][1:] == [middle, newest, oldest]
    assert [t["thread_index"] for t in threads] == [0, 1, 2, 3]
    assert threads[3]["updated_at"] == now - 9 * _DAY
    retired = await _rows(
        test_db_pool,
        "SELECT status, is_pinned FROM workspaces WHERE workspace_id = ANY(%s)",
        [first, second],
    )
    assert retired == [{"status": "deleted", "is_pinned": False}] * 2
    (disable,) = await _rows(
        test_db_pool, "SELECT workspace_id::text AS ws FROM workspace_skill_disables"
    )
    assert disable["ws"] == first
    (annotation,) = await _rows(
        test_db_pool,
        "SELECT payload FROM chart_annotations WHERE workspace_id = %s",
        home,
    )
    assert annotation["payload"] == {"from": "home"}
    automations = await _rows(
        test_db_pool,
        "SELECT name, workspace_id::text AS ws FROM automations ORDER BY name",
    )
    # The pinned one follows its thread.
    assert automations == [{"name": "daily", "ws": home}, {"name": "pinned", "ws": home}]
    usages = await _rows(
        test_db_pool,
        "SELECT workspace_id::text AS ws FROM conversation_usages WHERE user_id = %s",
        user_id,
    )
    assert usages == [{"ws": home}] * 2


@pytest.mark.parametrize("background", [False, True], ids=["root", "subagent"])
async def test_a_former_row_with_a_run_in_progress_waits(
    seed_user, test_db_pool, seed_response, background
):
    """The rule a delete follows: a run in progress records where it runs."""
    from src.server.database.runs.subagent_runs import (
        finalize_task_run,
        start_task_run,
    )

    user_id = seed_user["user_id"]
    await workspaces.get_or_create_home_workspace(user_id)
    former = await _former(user_id, "old-account")
    thread = await _thread(test_db_pool, former, 0, datetime.now(timezone.utc))
    run = str(uuid.uuid4())
    if background:
        await start_task_run(task_run_id=run, thread_id=thread, task_id=run, cause="init")
    else:
        await seed_response(run, thread, 0, status="in_progress")

    await workspaces.get_or_create_home_workspace(user_id)
    assert (await _rows(test_db_pool, _STATUS, former))[0]["status"] == "unbound"

    if background:
        await finalize_task_run(task_run_id=run, status="completed")
    else:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "UPDATE conversation_responses SET status = 'completed' "
                "WHERE conversation_response_id = %s",
                (run,),
            )
    await workspaces.get_or_create_home_workspace(user_id)
    assert (await _rows(test_db_pool, _STATUS, former))[0]["status"] == "deleted"


async def test_a_bound_home_receives_a_former_rows_threads(seed_user, test_db_pool):
    """Bound to a computer, Home takes its status, so the fold finds Home by
    id, never by status."""
    from src.server.database.computer import create_computer
    from src.server.database.workspace_names import HOME_FOLDER

    user_id = seed_user["user_id"]
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    computer = await create_computer(user_id, kind="docker", name="Home test")
    assert await workspaces.bind_workspace_to_computer(
        home,
        str(computer["computer_id"]),
        expected_computer_id=None,
        dir_name=HOME_FOLDER,
    )
    former = await _former(user_id, "old-account")
    thread = await _thread(test_db_pool, former, 0, datetime.now(timezone.utc))

    resolved = await workspaces.get_or_create_home_workspace(user_id)

    assert resolved["status"] != "unbound"
    assert str(resolved["computer_id"]) == str(computer["computer_id"])
    (moved,) = await _rows(
        test_db_pool,
        "SELECT workspace_id::text AS ws FROM conversation_threads "
        "WHERE conversation_thread_id = %s",
        thread,
    )
    assert moved["ws"] == home
    assert (await _rows(test_db_pool, _STATUS, former))[0]["status"] == "deleted"
    assert (await _rows(test_db_pool, _STATUS, home))[0]["status"] != "deleted"


async def test_a_home_id_another_account_holds_is_left_alone(
    seed_user, test_db_pool
):
    """An account merge leaves the merged id's Home with the account it
    joined; that row is the one a turn would run in and Home would bind."""
    from src.server.database.user import create_user

    user_id = seed_user["user_id"]
    other = await create_user(user_id="other-account", email="other@example.com")
    held = await workspaces.create_workspace(
        other["user_id"],
        "Home",
        workspace_id=get_home_workspace_id(user_id),
        status="unbound",
    )
    columns = "SELECT user_id, updated_at, is_pinned FROM workspaces WHERE workspace_id = %s"
    before = await _rows(test_db_pool, columns, str(held["workspace_id"]))

    with pytest.raises(workspaces.HomeWorkspaceTaken):
        await workspaces.get_or_create_home_workspace(user_id)

    assert await _rows(test_db_pool, columns, str(held["workspace_id"])) == before
    assert before[0]["user_id"] == "other-account"
