"""Threads started at once in one workspace, on a real Postgres.

thread_index is unique per workspace and read as MAX + 1, so threads started
together used to read the same number and all but one failed to start. Each
now reads it under the workspace's lock, and so does the fold that moves a
former Home's threads into Home.
"""

import asyncio
import uuid

import pytest

from src.server.database import workspace as workspaces
from src.server.database.conversation import create_thread
from src.server.database.home_fold import fold_former_homes
from src.server.database.home_workspace import get_home_workspace_id

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_AT_ONCE = 24


async def _indexes(pool, workspace_id) -> list[int]:
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT thread_index FROM conversation_threads WHERE workspace_id = %s ORDER BY thread_index",
            (workspace_id,),
        )
        return [row["thread_index"] for row in await result.fetchall()]


def _start(workspace_id: str):
    return create_thread(conversation_thread_id=str(uuid.uuid4()), workspace_id=workspace_id,
                         current_status="completed")


async def test_threads_started_at_once_in_one_workspace_all_start_numbered_in_turn(seed_user, test_db_pool):
    workspace = str((await workspaces.create_workspace(seed_user["user_id"], "Desk"))["workspace_id"])

    started = await asyncio.gather(*(_start(workspace) for _ in range(_AT_ONCE)), return_exceptions=True)

    assert [s for s in started if isinstance(s, BaseException)] == []
    assert await _indexes(test_db_pool, workspace) == list(range(_AT_ONCE))


async def _until_a_session_waits_on_a_lock(pool) -> None:
    async with pool.connection() as conn:
        for _ in range(200):
            result = await conn.execute(
                "SELECT count(*) AS waiting FROM pg_stat_activity"
                " WHERE datname = current_database() AND wait_event_type = 'Lock'"
            )
            if (await result.fetchone())["waiting"]:
                return
            await asyncio.sleep(0.05)
    raise AssertionError("nothing waited on a lock")


async def test_a_fold_into_home_numbers_after_a_thread_being_started_there(seed_user, test_db_pool):
    """The fold waits for a thread started in Home and not yet committed, then
    numbers the moved threads after it. Without the fold's lock it would read
    Home as empty and collide with that thread on commit."""
    user_id = seed_user["user_id"]
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    former = str((await workspaces.create_workspace(
        user_id, "Home", workspace_id=get_home_workspace_id("old-account"), status="unbound",
    ))["workspace_id"])
    for _ in range(8):
        await _start(former)

    async with test_db_pool.connection() as conn, conn.transaction():
        await create_thread(conversation_thread_id=str(uuid.uuid4()), workspace_id=home,
                            current_status="completed", conn=conn)
        fold = asyncio.create_task(fold_former_homes(user_id, home))
        await _until_a_session_waits_on_a_lock(test_db_pool)

    assert await fold == [former]
    assert await _indexes(test_db_pool, home) == list(range(9))
