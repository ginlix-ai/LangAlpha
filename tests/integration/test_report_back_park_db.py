"""A report parked around its thread's turn ending, against real Postgres.

A finalize releases what is parked on its thread when its release pass runs,
and a job claimed at that moment is skipped, so the claim that parks it has
to see the ending itself. Whether its look waits out a finalize still in
flight is a matter of row locks, which only a real database has.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

MAX = 5
_POST = "src.server.services.report_back.notify_turn.post_notification_turn"


def _app_rows(monkeypatch):
    """Hand the outbox tuple rows by default, as the app's pool does; the test
    pool defaults to dict rows, which would hide a positional read."""
    from src.server.database import pool

    test_connection = pool.get_db_connection

    @asynccontextmanager
    async def _get_db_connection(conn=None):
        if conn is not None:
            yield conn
            return
        async with test_connection() as owned:
            owned.row_factory = tuple_row
            try:
                yield owned
            finally:
                owned.row_factory = dict_row

    monkeypatch.setattr(pool, "get_db_connection", _get_db_connection)


@pytest_asyncio.fixture(loop_scope="session")
async def thread(seed_workspace, patched_get_db_connection, monkeypatch):
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    _app_rows(monkeypatch)
    return SimpleNamespace(
        id=thread_id,
        user_id=seed_workspace["user_id"],
        workspace_id=str(seed_workspace["workspace_id"]),
    )


async def _run(thread) -> str:
    from src.server.database.runs.lifecycle import start_run

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": thread.user_id},
    )
    return run_id


async def _end(thread, run_id: str, status: str = "completed"):
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    return await finalize_run(
        run_id=run_id, thread_id=thread.id, outcome=RunOutcome(status=status)
    )


async def _end_held_open(thread, run_id: str):
    """Start ending ``run_id`` and return once its finalize has run its
    release pass, the terminal CAS done and nothing committed. Awaiting the
    returned callable lets it commit."""
    from src.server.database.runs import lifecycle

    released, go = asyncio.Event(), asyncio.Event()
    settle = lifecycle.settle_run_steers

    async def _held(conn, **kwargs):
        released.set()
        await go.wait()
        await settle(conn, **kwargs)

    with patch.object(lifecycle, "settle_run_steers", _held):
        ending = asyncio.create_task(_end(thread, run_id))
        await asyncio.wait_for(released.wait(), 10)

    async def commit():
        go.set()
        await ending

    return commit


async def _until_blocked_or_done(task: asyncio.Task) -> None:
    """Return once ``task`` is over or its look waits on the held finalize's
    row, so the commit lands after the look has started, never before."""
    from src.server.database.pool import get_db_connection

    for _ in range(200):
        if task.done():
            return
        async with get_db_connection() as conn:
            cur = await conn.execute(
                """
                SELECT 1 FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                  AND query ILIKE '%FOR SHARE%'
                """
            )
            if await cur.fetchone():
                return
        await asyncio.sleep(0.05)
    raise AssertionError("the look neither ended nor waited on the finalize")


async def _id_of(db_conn, idempotency_key: str) -> str:
    cur = await db_conn.execute(
        "SELECT hook_outbox_id::text AS id FROM hook_outbox WHERE idempotency_key = %s",
        (idempotency_key,),
    )
    return (await cur.fetchone())["id"]


async def _claim() -> dict:
    from src.server.database.runs import outbox

    return {
        str(r["hook_outbox_id"]): r
        for r in await outbox.claim_outbox_jobs(limit=50, max_attempts=MAX)
    }


async def _parked(db_conn, job_id: str) -> bool:
    cur = await db_conn.execute(
        """
        SELECT status = 'pending'
               AND next_retry_at = 'infinity'::timestamptz AS parked
        FROM hook_outbox WHERE hook_outbox_id = %s
        """,
        (job_id,),
    )
    return (await cur.fetchone())["parked"]


# ---------------------------------------------------------------------------
# A task's report-back
# ---------------------------------------------------------------------------


async def _report(thread, db_conn, run_id: str) -> str:
    """A task report-back owed to the thread, as the task run's terminal CAS
    enqueues it. No ``task_run_id``, so it is owed without a ledger read."""
    from src.server.database.runs import outbox

    key = f"{run_id}:task:abc123:{uuid.uuid4()}:report_back"
    await outbox.enqueue_compensation_job(
        run_id=run_id,
        thread_id=thread.id,
        hook_type="task_report_back",
        payload={
            "task_id": "abc123",
            "display_id": "Task-abc123",
            "subagent_type": "research",
            "description": "look things up",
            "user_id": thread.user_id,
            "workspace_id": thread.workspace_id,
        },
        ordering_key=thread.id,
        idempotency_key=key,
    )
    return await _id_of(db_conn, key)


async def _execute_report(job: dict) -> None:
    from src.server.services.report_back.subagent import execute_task_report_back

    await execute_task_report_back(job)


async def test_a_park_behind_a_finalize_in_flight_is_released_once_it_commits(
    thread, db_conn
):
    """The report was claimed as its parent turn ended: that finalize's
    release pass found it claimed, and the park that followed read the run
    before the finalize committed. Its look waits the commit out, so the job
    is due again rather than parked for a turn that is already over."""
    run_id = await _run(thread)
    job_id = await _report(thread, db_conn, run_id)
    job = (await _claim())[job_id]

    commit = await _end_held_open(thread, run_id)
    executing = asyncio.create_task(_execute_report(job))
    await _until_blocked_or_done(executing)
    await commit()
    await asyncio.wait_for(executing, 10)

    assert job_id in await _claim()


async def test_a_capped_wait_a_turn_ended_during_releases_the_job(thread, db_conn):
    """The thread was idle when the report looked, a turn started before its
    POST was admitted, and that turn ended as the wait ran out. Its finalize
    found the job claimed and released nothing for it, so the park after the
    cap releases it at once."""
    run_id = await _run(thread)
    job_id = await _report(thread, db_conn, run_id)
    await _end(thread, run_id)
    job = (await _claim())[job_id]

    async def _a_turn_ran_out_the_wait(**kwargs):
        await _end(thread, await _run(thread))
        return ("cap", None)

    with patch(_POST, new=AsyncMock(side_effect=_a_turn_ran_out_the_wait)):
        await _execute_report(job)

    assert job_id in await _claim()


async def test_a_capped_wait_on_an_idle_thread_waits_for_its_next_turn(
    thread, db_conn
):
    """No turn ended during the claim: the thread refused every POST while
    idle (no credit, say). Released at once, the job would spin through its
    attempts to dead; it stays parked until a turn ends."""
    run_id = await _run(thread)
    job_id = await _report(thread, db_conn, run_id)
    await _end(thread, run_id)
    job = (await _claim())[job_id]

    with patch(_POST, new=AsyncMock(return_value=("cap", None))):
        await _execute_report(job)

    assert await _parked(db_conn, job_id)
    assert job_id not in await _claim()

    await _end(thread, await _run(thread))
    assert job_id in await _claim()


# ---------------------------------------------------------------------------
# An alert held after a refused wake
# ---------------------------------------------------------------------------


async def _hit(thread, db_conn) -> str:
    from src.server.database.market_targets import (
        arm_target,
        fire_targets,
        idempotency_key,
    )

    now = datetime.now(timezone.utc)
    target = await arm_target(
        thread_id=thread.id, user_id=thread.user_id, symbol="AAA", market="stock",
        kind="above", value=10.0, pct_from=None, armed_price=None,
        expires_at=now + timedelta(hours=6), warn_at=now + timedelta(hours=5),
        max_armed=10,
    )
    target_id = str(target["target_id"])
    fired = await fire_targets([target_id], 11.0)
    assert [str(r["target_id"]) for r in fired] == [target_id]
    return await _id_of(db_conn, idempotency_key(target_id, "hit"))


async def test_a_hold_behind_a_finalize_in_flight_is_released_once_it_commits(
    thread, db_conn
):
    """A wake refused as a turn ended: its finalize found the alert claimed
    and released nothing, and the hold's look for an ending read the run
    before that finalize committed. The look waits the commit out, so the
    ending buys the alert its one more look."""
    from src.server.services.market_targets.delivery import execute_market_target

    job_id = await _hit(thread, db_conn)
    job = (await _claim())[job_id]
    commits = []

    async def _refused_as_a_turn_ends(**kwargs):
        commits.append(await _end_held_open(thread, await _run(thread)))
        return ("refused", None)

    with (
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=True),
        ),
        patch(_POST, new=AsyncMock(side_effect=_refused_as_a_turn_ends)),
    ):
        holding = asyncio.create_task(execute_market_target(job))
        await _until_blocked_or_done(holding)
        await commits[0]()
        await asyncio.wait_for(holding, 10)

    assert job_id in await _claim()
