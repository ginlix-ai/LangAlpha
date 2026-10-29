"""Deletion and run admission share a database lock across server workers."""

import asyncio
import uuid

import pytest

from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer
from src.server.database.conversation import create_thread
from src.server.database.runs.lifecycle import start_run
from src.server.database.runs.subagent_runs import start_task_run

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _project(user_id):
    computer = await create_computer(user_id, kind="docker", name="Delete test")
    workspace = await workspaces.create_workspace_on_computer(
        user_id, "Delete test", str(computer["computer_id"])
    )
    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, str(workspace["workspace_id"]), "completed")
    return str(workspace["workspace_id"]), thread_id


async def _start(thread_id, background, conn=None):
    identifier = str(uuid.uuid4())
    if background:
        return await start_task_run(
            task_run_id=identifier, thread_id=thread_id, task_id=identifier,
            cause="init", conn=conn,
        )
    return await start_run(
        run_id=identifier, thread_id=thread_id, request_key=identifier, conn=conn,
    )


@pytest.mark.parametrize("background", [False, True])
async def test_active_work_rejects_delete(seed_user, background):
    workspace_id, thread_id = await _project(seed_user["user_id"])
    await _start(thread_id, background)
    with pytest.raises(workspaces.WorkspaceBusyError):
        await workspaces.delete_workspace(workspace_id)
    assert (await workspaces.get_workspace(workspace_id))["status"] != "deleted"


@pytest.mark.parametrize("background", [False, True])
async def test_admission_commit_wins_over_delete(seed_user, test_db_pool, background):
    workspace_id, thread_id = await _project(seed_user["user_id"])
    task = None
    try:
        async with test_db_pool.connection() as conn, conn.transaction():
            await _start(thread_id, background, conn)
            task = asyncio.create_task(workspaces.delete_workspace(workspace_id))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 0.1)
        with pytest.raises(workspaces.WorkspaceBusyError):
            await asyncio.wait_for(task, 5)
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("background", [False, True])
async def test_delete_commit_prevents_new_admission(seed_user, test_db_pool, background):
    workspace_id, thread_id = await _project(seed_user["user_id"])
    task = None
    try:
        async with test_db_pool.connection() as conn, conn.transaction():
            assert await workspaces.delete_workspace(workspace_id, conn=conn)
            task = asyncio.create_task(_start(thread_id, background))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 0.1)
        with pytest.raises(ValueError, match="deleted workspace"):
            await asyncio.wait_for(task, 5)
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
