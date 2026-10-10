"""Subagent run facts against real Postgres.

A run's facts are learned after its settle CAS, by two writers that can land
together (the archive's lane images, the steering sweep's returns), and replay
keys a turn's stored lines on the facts of the runs it launched, so only a
real database can show that both writes keep theirs and that the facts reach
the launching turn's index.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def launched_run(seed_workspace, patched_get_db_connection):
    """A thread whose second turn launched one subagent run."""
    from src.server.database.conversation import create_thread
    from src.server.database.runs import subagent_runs
    from src.server.database.runs.lifecycle import start_run

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="in_progress",
        msg_type="ptc",
    )
    response_id = str(uuid.uuid4())
    await start_run(
        run_id=response_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        turn_index=1,
        metadata={"msg_type": "ptc", "user_id": seed_workspace["user_id"]},
    )
    task_run_id = str(uuid.uuid4())
    await subagent_runs.start_task_run(
        task_run_id=task_run_id,
        thread_id=thread_id,
        task_id="k1",
        cause="init",
        parent_run_id=response_id,
    )
    await subagent_runs.finalize_task_run(task_run_id=task_run_id, status="completed")
    return thread_id, response_id, task_run_id


async def _run_facts(thread_id):
    """The run facts a replay of the thread reads, with its other rows."""
    from src.server.database.conversation import get_replay_thread_data

    return (await get_replay_thread_data(thread_id)).run_facts


async def _run(thread_id, task_id, parent_run_id, *, predecessor=None):
    from src.server.database.runs import subagent_runs

    task_run_id = str(uuid.uuid4())
    await subagent_runs.start_task_run(
        task_run_id=task_run_id,
        thread_id=thread_id,
        task_id=task_id,
        cause="resume" if predecessor else "init",
        parent_run_id=parent_run_id,
        predecessor_run_id=predecessor,
    )
    await subagent_runs.finalize_task_run(task_run_id=task_run_id, status="completed")
    return task_run_id


async def test_facts_merge_per_key_and_name_the_launching_turn(launched_run):
    from src.server.database import replay_facts

    thread_id, response_id, task_run_id = launched_run
    returned = {"content": "also check AMD", "input_id": "in-1", "reason": "run_ended"}

    imaged, steered = await asyncio.gather(
        replay_facts.append_lane_images(
            thread_id, response_id, "k1", {"/home/workspace/a.png": "https://cdn/a.png"}
        ),
        replay_facts.append_returned_steering(task_run_id, [returned]),
    )
    assert await replay_facts.append_lane_images(
        thread_id, response_id, "k1", {"/home/workspace/b.png": "https://cdn/b.png"}
    )

    assert imaged is True
    assert str(steered["thread_id"]) == thread_id
    assert str(steered["parent_run_id"]) == response_id

    rows = await _run_facts(thread_id)
    assert len(rows) == 1
    assert rows[0]["turn_index"] == 1
    assert str(rows[0]["task_run_id"]) == task_run_id
    assert rows[0]["replay_facts"] == {
        "images": {
            "/home/workspace/a.png": "https://cdn/a.png",
            "/home/workspace/b.png": "https://cdn/b.png",
        },
        "steering_returned": [returned],
    }


async def test_lane_images_reach_the_turns_runs_of_that_task_only(launched_run):
    """A task resumed within the turn has two runs there, both of which the
    lane's events may come from; another task's run and a later turn's run of
    the same task keep their own facts."""
    from src.server.database import replay_facts
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run, start_run

    thread_id, response_id, first = launched_run
    resumed = await _run(thread_id, "k1", response_id, predecessor=first)
    await _run(thread_id, "k2", response_id)
    await finalize_run(
        run_id=response_id, thread_id=thread_id, outcome=RunOutcome(status="completed")
    )
    later_turn = str(uuid.uuid4())
    await start_run(
        run_id=later_turn,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        turn_index=2,
        metadata={"msg_type": "ptc"},
    )
    await _run(thread_id, "k1", later_turn, predecessor=resumed)

    images = {"/home/workspace/a.png": "https://cdn/a.png"}
    assert await replay_facts.append_lane_images(thread_id, response_id, "k1", images)

    rows = await _run_facts(thread_id)
    assert {str(r["task_run_id"]) for r in rows} == {first, resumed}
    assert all(r["replay_facts"] == {"images": images} for r in rows)


async def test_nothing_to_record_writes_nothing(launched_run):
    from src.server.database import replay_facts

    thread_id, response_id, task_run_id = launched_run
    assert await replay_facts.append_returned_steering(task_run_id, []) is None
    assert await replay_facts.append_lane_images(thread_id, response_id, "k1", {}) is False
    assert await _run_facts(thread_id) == []


async def test_an_unknown_run_reports_none(launched_run):
    from src.server.database import replay_facts

    thread_id, _, _ = launched_run
    returned = {"content": "c", "input_id": "in-1", "reason": "run_ended"}
    images = {"/home/workspace/a.png": "https://cdn/a.png"}
    assert await replay_facts.append_returned_steering(str(uuid.uuid4()), [returned]) is None
    assert (
        await replay_facts.append_lane_images(thread_id, str(uuid.uuid4()), "k1", images)
        is False
    )
