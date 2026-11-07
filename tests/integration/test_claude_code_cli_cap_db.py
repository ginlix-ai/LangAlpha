"""A computer's count of the Claude Code CLIs it runs, against real Postgres.

Turns and task runs on Claude Code on any of the computer's workspaces count
together, and two admissions on one computer count one at a time, which only
a real database can show.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from src.server.database.claude_code_hosted import CLAUDE_CODE_TURN
from src.server.database.computer import create_computer
from src.server.database.conversation import create_thread
from src.server.database.runs import subagent_runs
from src.server.database.runs.lifecycle import start_run
from src.server.database.workspace import create_workspace_on_computer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _threads(user_id: str, *names: str) -> tuple[str, list[str]]:
    """A computer, and a thread on a workspace of its own for each name."""
    computer = await create_computer(user_id, kind="daytona", name=f"box {uuid.uuid4().hex[:8]}",
                                     status="running", provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
    computer_id = str(computer["computer_id"])
    threads = []
    for name in names:
        workspace = await create_workspace_on_computer(user_id, computer_id=computer_id, name=name)
        thread_id = str(uuid.uuid4())
        await create_thread(conversation_thread_id=thread_id, workspace_id=str(workspace["workspace_id"]),
                            current_status="in_progress", msg_type="ptc")
        threads.append(thread_id)
    return computer_id, threads


async def _turn(thread_id: str, user_id: str, stamp: dict) -> None:
    await start_run(run_id=str(uuid.uuid4()), thread_id=thread_id, request_key=str(uuid.uuid4()), turn_index=1,
                    metadata={"msg_type": "ptc", "user_id": user_id, **stamp})


async def _task(thread_id: str, task_id: str, cap: tuple[str, int] | None, *, claude_code: bool = True,
                stored_token: bool = False, conn=None) -> dict:
    return await subagent_runs.start_task_run(task_run_id=str(uuid.uuid4()), thread_id=thread_id, task_id=task_id,
                                              cause="init", claude_code=claude_code, stored_token=stored_token,
                                              cli_cap=cap, conn=conn)


async def test_a_computer_admits_claude_code_runs_until_its_turns_and_runs_reach_its_cap(seed_user):
    user = seed_user["user_id"]
    computer_id, (research, macro) = await _threads(user, "Research", "Macro")
    await _turn(research, user, CLAUDE_CODE_TURN)
    await _turn(macro, user, {})
    cap = (computer_id, 3)

    first = await _task(research, "k1", cap)
    await _task(macro, "k2", cap)
    with pytest.raises(subagent_runs.ClaudeCodeCapError) as refused:
        await _task(research, "k3", cap)
    assert refused.value.cap == 3
    assert first["replay_facts"] == {"claude_code": True}
    assert (await _task(research, "k4", None, claude_code=False))["replay_facts"] is None

    await subagent_runs.finalize_task_run(task_run_id=str(first["task_run_id"]), status="completed")
    await _task(research, "k3", cap)


async def test_a_read_counts_as_admission_does_and_tells_a_task_last_run_on_claude_code(seed_user):
    user = seed_user["user_id"]
    computer_id, (thread,) = await _threads(user, "Research")
    await _turn(thread, user, CLAUDE_CODE_TURN)
    first = await _task(thread, "k1", None)
    await _task(thread, "k2", None, claude_code=False)

    assert await subagent_runs.live_clis(computer_id) == 2
    assert await subagent_runs.ran_on_claude_code(thread, "k1") is True
    assert await subagent_runs.ran_on_claude_code(thread, "k2") is False
    assert await subagent_runs.ran_on_claude_code(thread, "k9") is False

    await subagent_runs.finalize_task_run(task_run_id=str(first["task_run_id"]), status="completed")
    assert await subagent_runs.live_clis(computer_id) == 1


async def test_an_admission_waits_for_one_on_the_same_computer_and_counts_its_run(seed_user, test_db_pool):
    """With room for one more, the second of two admissions on another of
    the computer's workspaces waits for the first to commit, then is refused."""
    user = seed_user["user_id"]
    computer_id, (a, b) = await _threads(user, "A", "B")
    await _turn(a, user, CLAUDE_CODE_TURN)
    cap = (computer_id, 2)

    async with test_db_pool.connection() as first, test_db_pool.connection() as second:
        async with first.transaction():
            await _task(a, "k1", cap, conn=first)
            racing = asyncio.create_task(_task(b, "k2", cap, conn=second))
            await asyncio.sleep(0.5)
            assert not racing.done()
        with pytest.raises(subagent_runs.ClaudeCodeCapError):
            await racing


async def test_a_run_on_the_stored_token_is_found_by_its_run_when_the_token_goes(seed_user):
    """It signs in as the build that launched it does, whichever credential
    the thread's latest turn moved to since, and can outlive that turn."""
    from src.server.database.claude_code_hosted import STORED_TURN, live_stored_task_runs

    user = seed_user["user_id"]
    _, (thread,) = await _threads(user, "Research")
    await _turn(thread, user, CLAUDE_CODE_TURN)

    run = await _task(thread, "k1", None, stored_token=True)
    await _task(thread, "k2", None)

    assert run["replay_facts"] == {"claude_code": True, **STORED_TURN}
    assert await live_stored_task_runs(user) == [(thread, "k1", str(run["task_run_id"]))]


async def test_a_task_run_asked_to_stop_is_found_until_it_settles(seed_user):
    """What a worker that missed the nudge reads to stop it."""
    user = seed_user["user_id"]
    _, (thread,) = await _threads(user, "Research")
    asked = str((await _task(thread, "k1", None))["task_run_id"])
    other = str((await _task(thread, "k2", None))["task_run_id"])

    await subagent_runs.request_task_run_cancel(asked, thread_id=thread)
    assert await subagent_runs.cancel_requested_task_runs([asked, other]) == {asked}

    await subagent_runs.finalize_task_run(task_run_id=asked, status="cancelled")
    assert await subagent_runs.cancel_requested_task_runs([asked, other]) == set()
