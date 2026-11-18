"""A turn a restart's drain stopped while it waited for its computer, against
real Postgres.

The wait's settle is the stream's ``fail_open``, not the executor's finalize,
so only the rows the real START and finalize write show that it leaves the
same restart mark, pending input and resume job a registered turn's restart
stop leaves, that the next process resumes it once, and that a user's or a
suspension's stop stamped before the settle still wins.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# What ``ptc_run`` passes ``fail_open`` when the drain ended the wait.
DRAIN_STOP = {"cancelled_by_user": False, "interrupted_by_restart": True}


@pytest_asyncio.fixture(loop_scope="session")
async def thread_id(seed_workspace, test_db_pool, patched_get_db_connection):
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from src.server.app import setup
    from src.server.database.conversation import create_thread

    tid = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=tid,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="in_progress",
        msg_type="ptc",
    )
    saver = AsyncPostgresSaver(test_db_pool)
    with patch.object(setup, "checkpointer", saver):
        yield tid


async def _waiting_turn(thread_id: str, *, retry_of: str | None = None) -> str:
    """A turn past START whose stream is still acquiring its computer: an
    in_progress row and no checkpoint. With ``retry_of``, the attempt the
    resume's /retry starts for that run's turn."""
    from src.server.database.runs.lifecycle import QuerySpec, get_run, start_run

    run_id = str(uuid.uuid4())
    if retry_of is None:
        turn = {
            "query": QuerySpec(
                query_id=str(uuid.uuid4()), content="hi", query_type="initial"
            )
        }
    else:
        retried = await get_run(retry_of)
        turn = {
            "turn_index": retried["turn_index"],
            "attempt_no": retried["attempt_no"] + 1,
            "retry_of_run_id": retry_of,
        }
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc"},
        **turn,
    )
    return run_id


async def _fail_open(thread_id: str, run_id: str, metadata: dict | None):
    """The waiting path's settle, through the real coordinator."""
    from src.server.services.runs.admission import RunScope
    from src.server.services.runs.coordinator import RunCoordinator, RunHandle

    coordinator = RunCoordinator()
    for tail in (
        "_schedule_replay_refresh",
        "_schedule_transcript_export",
        "_schedule_archive_prune",
        "_nudge_hook_drainer",
    ):
        setattr(coordinator, tail, MagicMock())
    scope = RunScope(user_id=None, burst_slot_id=None)
    scope.attach_run(
        RunHandle(
            run_id=run_id,
            thread_id=thread_id,
            turn_index=0,
            attempt_no=1,
            # START's stamp for a plain send (``executor_input_stamp``).
            pending_input={"pending_input": {"checkpoint_id": None}},
        )
    )
    with patch(
        "src.server.services.runs.coordinator.RunCoordinator.get_instance",
        return_value=coordinator,
    ):
        result = await scope.fail_open(
            "cancelled while acquiring its computer", metadata=metadata
        )
    assert result is not None and result.applied
    return result.run


async def _resume_jobs(db_conn, run_id: str) -> list[dict]:
    from psycopg.rows import dict_row

    async with db_conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM hook_outbox WHERE run_id = %s AND hook_type = 'restart_resume'",
            (run_id,),
        )
        return list(await cur.fetchall())


async def test_a_drain_stopped_wait_is_resumed_once_by_running_its_message_again(
    thread_id, db_conn
):
    from src.server.contracts.status import is_restart_interrupted, is_user_stop
    from src.server.services.report_back.notify_turn import NotOwed
    from src.server.services.report_back.restart import OwedResume, resume_verdict
    from src.server.services.runs.pending_input import read_pending_input

    run_id = await _waiting_turn(thread_id)
    row = await _fail_open(thread_id, run_id, DRAIN_STOP)

    assert row["status"] == "cancelled"
    assert is_restart_interrupted(row) and not is_user_stop(row)
    assert read_pending_input(row) is not None
    # One job, written by the settle's own transaction, for whichever worker
    # of the next process claims it.
    jobs = await _resume_jobs(db_conn, run_id)
    assert len(jobs) == 1
    # Claimed past the minute a resume gives stopped subagents to settle,
    # whose probe reads tuple rows this test pool does not return.
    job = {**jobs[0], "created_at": jobs[0]["created_at"] - timedelta(minutes=2)}
    verdict = await resume_verdict(job)
    assert isinstance(verdict, OwedResume)
    # Its message never reached the graph, so the resume runs it again
    # through /retry, comments and all, rather than a "continue" turn.
    assert verdict.retry_run_id == run_id

    # The resume's /retry starts the thread's next run; the same job claimed
    # again finds that run and posts nothing.
    await _waiting_turn(thread_id, retry_of=run_id)
    again = await resume_verdict(job)
    assert isinstance(again, NotOwed)


@pytest.mark.parametrize("by_suspension", [False, True], ids=["user", "suspension"])
async def test_a_stop_stamped_before_the_drains_settle_is_not_resumed(
    thread_id, db_conn, by_suspension
):
    from src.server.contracts.status import (
        is_restart_interrupted,
        is_suspension_stop,
        is_user_stop,
    )
    from src.server.database.runs.lifecycle import request_run_cancel

    run_id = await _waiting_turn(thread_id)
    await request_run_cancel(
        run_id, thread_id=thread_id, by="suspension" if by_suspension else "user"
    )
    # The drain began before the settle ran, so the settle asks for resume.
    row = await _fail_open(thread_id, run_id, DRAIN_STOP)

    assert row["status"] == "cancelled"
    assert not is_restart_interrupted(row)
    assert is_suspension_stop(row) is by_suspension
    assert is_user_stop(row) is not by_suspension
    assert await _resume_jobs(db_conn, run_id) == []


async def test_a_wait_stopped_with_no_drain_is_a_plain_stop(thread_id, db_conn):
    from src.server.contracts.status import is_restart_interrupted

    run_id = await _waiting_turn(thread_id)
    row = await _fail_open(thread_id, run_id, None)

    assert row["status"] == "cancelled"
    assert not is_restart_interrupted(row)
    assert await _resume_jobs(db_conn, run_id) == []
