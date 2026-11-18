"""The account a subagent run's row names, against real Postgres.

The in-flight spend is summed per account by ``subagent_runs.user_id``, so
the row names the sender of the turn that launched the run. Until a workspace
can take turns from someone other than its owner, that is the workspace's
owner, which is what the row used to read; the second test shows which one it
takes when they differ, and the third that a run with no such turn row still
falls back to the owner.
"""

from __future__ import annotations

import uuid

import pytest

from src.server.database.conversation import create_thread
from src.server.database.runs import subagent_runs
from src.server.database.runs.lifecycle import start_run
from src.server.database.user import create_user

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _thread(workspace_id: str) -> str:
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id, workspace_id=workspace_id,
        current_status="in_progress", msg_type="ptc",
    )
    return thread_id


async def _turn(thread_id: str, user_id: str | None) -> str:
    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id, thread_id=thread_id, request_key=str(uuid.uuid4()), turn_index=1,
        metadata={"msg_type": "ptc"}, user_id=user_id,
    )
    return run_id


async def _task_run(thread_id: str, *, parent: str | None, spending: str | None) -> dict:
    return await subagent_runs.start_task_run(
        task_run_id=str(uuid.uuid4()), thread_id=thread_id, task_id=f"k-{uuid.uuid4().hex[:6]}",
        cause="init", parent_run_id=parent, spending_run_id=spending,
    )


async def test_a_run_names_the_owner_whose_turn_launched_it(seed_workspace):
    owner = seed_workspace["user_id"]
    thread = await _thread(str(seed_workspace["workspace_id"]))
    turn = await _turn(thread, owner)

    by_task = await _task_run(thread, parent=turn, spending=turn)
    by_workflow = await _task_run(thread, parent=None, spending=turn)

    assert by_task["user_id"] == by_workflow["user_id"] == owner


async def test_a_run_names_its_launching_turns_sender_not_the_workspaces_owner(seed_workspace):
    sender = (await create_user(user_id="test-user-sender-002", email="sender@example.com", name="S"))["user_id"]
    thread = await _thread(str(seed_workspace["workspace_id"]))
    turn = await _turn(thread, sender)

    assert (await _task_run(thread, parent=turn, spending=turn))["user_id"] == sender
    # A workflow's child has no parent and is spent for the turn that ran it.
    assert (await _task_run(thread, parent=None, spending=turn))["user_id"] == sender


async def test_a_run_with_no_turn_to_read_names_the_workspaces_owner(seed_workspace):
    owner = seed_workspace["user_id"]
    thread = await _thread(str(seed_workspace["workspace_id"]))
    unrecorded = await _turn(thread, None)

    assert (await _task_run(thread, parent=None, spending=None))["user_id"] == owner
    assert (await _task_run(thread, parent=unrecorded, spending=unrecorded))["user_id"] == owner
    assert (await _task_run(thread, parent=None, spending="not-a-run-id"))["user_id"] == owner
