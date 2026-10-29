"""Steers on the hook outbox, against real Postgres.

A steer is a ``hook_outbox`` row owned by its thread, not by the run it was
sent into: the gated insert, the hold and confirm of the run that reads it,
the settle at that run's ending or stop, and the claim safety net all decide
its fate in SQL, so only a real database shows the row ends exactly once.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


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
    return SimpleNamespace(id=thread_id, user_id=seed_workspace["user_id"])


async def _run(thread, *, metadata=None, before_commit=None) -> str:
    from src.server.database.runs.lifecycle import start_run

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": thread.user_id, **(metadata or {})},
        before_commit=before_commit,
    )
    return run_id


async def _end(thread, run_id: str, status: str = "completed"):
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    return await finalize_run(
        run_id=run_id, thread_id=thread.id, outcome=RunOutcome(status=status)
    )


async def _steer(
    thread,
    run_id: str,
    text: str,
    *,
    sender: str = "user",
    platform: str | None = "web",
    key: str | None = None,
    origin: dict | None = None,
    origin_stamp: dict | None = None,
) -> dict:
    from src.server.database.runs import outbox
    from src.server.handlers.chat.steering import steer_payload

    request_key = str(uuid.uuid4())
    return await outbox.admit_steer(
        thread_id=thread.id,
        run_id=run_id,
        payload=steer_payload(
            text,
            thread.user_id,
            sender=sender,
            platform=platform,
            request_key=request_key if sender == "user" else None,
            origin=origin,
        ),
        idempotency_key=key or f"steer:{request_key}",
        origin_stamp=origin_stamp,
    )


async def _accepted(thread, run_id: str, text: str, **kw) -> str:
    result = await _steer(thread, run_id, text, **kw)
    assert result["state"] == "accepted", result
    return result["job_id"]


async def _row(db_conn, job_id: str) -> dict:
    """The row's lifecycle, with ``next_retry_at`` read as booleans: psycopg
    cannot load the ``infinity`` every pending steer carries. A steer's due
    time is its payload's ``due_at``; ``parked`` is one without it."""
    cur = await db_conn.execute(
        """
        SELECT status, payload, ordering_key, run_id::text AS run_id, created_at,
               COALESCE(next_retry_at = 'infinity'::timestamptz, false)
                   AS infinity,
               COALESCE(next_retry_at = 'infinity'::timestamptz
                        AND NOT payload ? 'due_at', false) AS parked,
               COALESCE(next_retry_at = 'infinity'::timestamptz
                        AND (payload->>'due_at')::timestamptz <= NOW(), false)
                   AS due
        FROM hook_outbox WHERE hook_outbox_id = %s
        """,
        (job_id,),
    )
    return await cur.fetchone()


async def _set_run(db_conn, run_id: str, status: str, metadata: dict | None = None):
    """End a run the way a lost finalize or an older build leaves it: the row
    terminal, its steers never settled."""
    from src.server.utils.pg_sanitize import SafeJson

    await db_conn.execute(
        """
        UPDATE conversation_responses
        SET status = %s, metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
        WHERE conversation_response_id = %s
        """,
        (status, SafeJson(metadata or {}), run_id),
    )


def _hoc():
    """A Chief of Staff hand-off origin, as dispatch stamps it."""
    return {
        "type": "agent",
        "agent": "chief_of_staff",
        "origin_home_thread_id": str(uuid.uuid4()),
        "origin_dispatch_gen": uuid.uuid4().hex,
    }


# ---------------------------------------------------------------------------
# Admission
# ---------------------------------------------------------------------------


async def test_a_steer_parks_on_the_threads_chain_against_its_live_run(
    thread, db_conn
):
    run_id = await _run(thread)

    first = await _steer(thread, run_id, "first")
    second = await _steer(thread, run_id, "second")

    assert (first["state"], first["position"]) == ("accepted", 1)
    assert (second["state"], second["position"]) == ("accepted", 2)
    row = await _row(db_conn, first["job_id"])
    assert row["status"] == "pending" and row["parked"]
    assert row["run_id"] == run_id
    assert row["ordering_key"] == f"{thread.id}:steer"
    assert row["payload"]["target"] == "thread"
    assert "outcome" not in row["payload"]


async def test_admission_refuses_a_run_that_ended(thread, db_conn):
    run_id = await _run(thread)
    await _end(thread, run_id)

    result = await _steer(thread, run_id, "too late")

    assert result == {"state": "not_running"}
    cur = await db_conn.execute(
        "SELECT COUNT(*) AS n FROM hook_outbox WHERE hook_type = 'steer'"
    )
    assert (await cur.fetchone())["n"] == 0


async def test_admission_refuses_a_run_with_a_cancel_stamp(thread):
    from src.server.database.runs.lifecycle import request_run_cancel

    run_id = await _run(thread)
    assert (await request_run_cancel(run_id, thread_id=thread.id))["state"] == "requested"

    assert await _steer(thread, run_id, "after stop") == {"state": "not_running"}


async def test_a_resent_key_answers_with_the_row_it_wrote(thread, db_conn):
    run_id = await _run(thread)
    key = f"steer:{uuid.uuid4()}"
    first = await _steer(thread, run_id, "once", key=key)

    again = await _steer(thread, run_id, "once", key=key)

    assert again["state"] == "duplicate"
    assert again["job"]["id"] == first["job_id"]
    assert again["job"]["run_id"] == run_id
    cur = await db_conn.execute(
        "SELECT COUNT(*) AS n FROM hook_outbox WHERE idempotency_key = %s", (key,)
    )
    assert (await cur.fetchone())["n"] == 1


async def test_a_hand_off_stamps_the_run_and_keeps_every_gen_it_admitted(thread):
    from src.server.database.runs.lifecycle import get_run, run_has_dispatch_gen

    home = str(uuid.uuid4())
    run_id = await _run(
        thread,
        metadata={"origin_home_thread_id": home, "origin_dispatch_gen": "gen-a"},
    )
    origin = {**_hoc(), "origin_home_thread_id": home, "origin_dispatch_gen": "gen-b"}

    await _accepted(
        thread,
        run_id,
        "also do this",
        sender="chief_of_staff",
        platform=None,
        key="steer:dispatch:gen-b",
        origin=origin,
        origin_stamp={"origin_home_thread_id": home, "origin_dispatch_gen": "gen-b"},
    )

    meta = (await get_run(run_id))["metadata"]
    assert meta["origin_home_thread_id"] == home
    assert meta["origin_dispatch_gen"] == "gen-b"
    assert sorted(meta["dispatch_gens"]) == ["gen-a", "gen-b"]
    assert await run_has_dispatch_gen(run_id, "gen-a")
    assert await run_has_dispatch_gen(run_id, "gen-b")
    assert not await run_has_dispatch_gen(run_id, "gen-c")
    assert not await run_has_dispatch_gen(str(uuid.uuid4()), "gen-a")


async def test_a_hand_off_into_a_run_another_home_owns_is_refused(thread, db_conn):
    run_id = await _run(
        thread,
        metadata={
            "origin_home_thread_id": str(uuid.uuid4()),
            "origin_dispatch_gen": "theirs",
        },
    )
    mine = _hoc()

    result = await _steer(
        thread,
        run_id,
        "hello",
        sender="chief_of_staff",
        platform=None,
        key=f"steer:dispatch:{mine['origin_dispatch_gen']}",
        origin=mine,
        origin_stamp={
            "origin_home_thread_id": mine["origin_home_thread_id"],
            "origin_dispatch_gen": mine["origin_dispatch_gen"],
        },
    )

    assert result == {"state": "owned_elsewhere"}
    cur = await db_conn.execute(
        "SELECT COUNT(*) AS n FROM hook_outbox WHERE hook_type = 'steer'"
    )
    assert (await cur.fetchone())["n"] == 0


# ---------------------------------------------------------------------------
# Task admission
# ---------------------------------------------------------------------------


async def _task_run(
    thread, parent_run_id: str, task_id: str = "k7Xm2p", *, cause: str = "init"
) -> str:
    from src.server.database.runs.subagent_runs import start_task_run

    task_run_id = str(uuid.uuid4())
    await start_task_run(
        task_run_id=task_run_id,
        thread_id=thread.id,
        task_id=task_id,
        cause=cause,
        parent_run_id=parent_run_id,
        launch_tool_call_id=f"call_{uuid.uuid4().hex[:8]}",
    )
    return task_run_id


async def _task_steer(
    thread, task_run_id: str, text: str, *, task_id="k7Xm2p", input_id=None,
    sender="user",
):
    from src.server.database.runs import outbox
    from src.server.services.report_back.task_steer import (
        task_steer_key,
        task_steer_payload,
    )

    input_id = input_id or uuid.uuid4().hex
    return await outbox.admit_task_steer(
        thread_id=thread.id,
        task_id=task_id,
        task_run_id=task_run_id,
        parent_run_id=None,
        payload=task_steer_payload(
            content=text,
            sender=sender,
            task_id=task_id,
            task_run_id=task_run_id,
            input_id=input_id,
            user_id=thread.user_id,
        ),
        idempotency_key=task_steer_key(thread.id, task_id, input_id),
    )


async def test_a_task_steer_hangs_off_the_launching_run_on_the_tasks_chain(
    thread, db_conn
):
    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)

    first = await _task_steer(thread, task_run_id, "narrow it", input_id="in-1")
    again = await _task_steer(thread, task_run_id, "narrow it", input_id="in-1")
    second = await _task_steer(thread, task_run_id, "and this")

    assert (first["state"], first["position"]) == ("accepted", 1)
    assert (second["state"], second["position"]) == ("accepted", 2)
    assert again["state"] == "duplicate"
    assert again["job"]["id"] == first["job_id"]
    row = await _row(db_conn, first["job_id"])
    assert row["run_id"] == parent
    assert row["ordering_key"] == f"{thread.id}:task:k7Xm2p"
    assert row["status"] == "pending" and row["parked"]


async def test_task_admission_refuses_an_ended_or_stopped_task_run(thread):
    from src.server.database.runs.subagent_runs import (
        finalize_task_run,
        request_task_run_cancel,
    )

    parent = await _run(thread)
    ended = await _task_run(thread, parent, task_id="ended1")
    await finalize_task_run(task_run_id=ended, status="completed")
    stopped = await _task_run(thread, parent, task_id="stop01")
    await request_task_run_cancel(stopped, thread_id=thread.id)

    assert (await _task_steer(thread, ended, "x", task_id="ended1")) == {
        "state": "not_running"
    }
    assert (await _task_steer(thread, stopped, "x", task_id="stop01")) == {
        "state": "not_running"
    }


# ---------------------------------------------------------------------------
# Take, hold, confirm
# ---------------------------------------------------------------------------


async def test_take_hold_confirm_delivers_the_row_once(thread, db_conn):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "look at margins")

    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=run_id)
    assert [str(r["hook_outbox_id"]) for r in takeable] == [job_id]

    assert await outbox.hold_jobs([job_id], owner=("run", run_id)) == [job_id]
    held = await _row(db_conn, job_id)
    assert held["payload"]["steered_run_id"] == run_id
    assert held["status"] == "pending" and held["parked"]

    # Taking it twice would put it in the transcript twice.
    assert await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=run_id) == []
    assert await outbox.hold_jobs([job_id], owner=("run", run_id)) == []
    assert await outbox.pending_steer_senders(thread.id, run_id) == []

    assert await outbox.confirm_jobs([job_id], owner=("run", run_id)) == [job_id]
    done = await _row(db_conn, job_id)
    assert done["status"] == "done"
    assert done["payload"]["outcome"] == "delivered"
    assert done["payload"]["steered_run_id"] == run_id
    assert await outbox.confirm_jobs([job_id], owner=("run", run_id)) == []
    delivered = await outbox.list_delivered_steers(run_id)
    assert [d["content"] for d in delivered] == ["look at margins"]


async def test_a_released_hold_is_takeable_again(thread):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "x")
    await outbox.hold_jobs([job_id], owner=("run", run_id))

    assert await outbox.release_jobs([job_id], owner=("run", run_id)) == [job_id]

    assert await outbox.pending_steer_senders(thread.id, run_id) == ["user"]
    assert [
        str(r["hook_outbox_id"])
        for r in await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=run_id)
    ] == [job_id]


async def test_a_stopped_run_takes_nothing(thread, db_conn):
    from src.server.database.runs import outbox

    first = await _run(thread)
    job_id = await _accepted(thread, first, "x")
    await _end(thread, first)
    second = await _run(thread)
    await db_conn.execute(
        "UPDATE conversation_responses SET cancel_requested_at = NOW()"
        " WHERE conversation_response_id = %s",
        (second,),
    )

    assert await outbox.hold_jobs([job_id], owner=("run", second)) == []
    assert "steered_run_id" not in (await _row(db_conn, job_id))["payload"]


# ---------------------------------------------------------------------------
# A missed steer's own turn
# ---------------------------------------------------------------------------


async def test_a_turn_for_one_missed_steer_reads_older_ones_first_and_newer_after(
    thread, db_conn
):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import get_run, start_run

    ended = await _run(thread)
    older = await _accepted(thread, ended, "older")
    claimed = await _accepted(thread, ended, "claimed")
    newer = await _accepted(thread, ended, "newer")
    await _end(thread, ended)

    turn_id = str(uuid.uuid4())
    won: list = []

    async def _before_commit(conn):
        won.append(await outbox.claim_steer_for_turn(conn, claimed, turn_id))

    await start_run(
        run_id=turn_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": thread.user_id},
        before_commit=_before_commit,
    )

    assert won == [True]
    row = await _row(db_conn, claimed)
    assert row["status"] == "done"
    assert row["payload"]["outcome"] == "turn"
    assert row["payload"]["dispatched_run_id"] == turn_id
    # Both rest rows predate the run itself: only the claimed steer's own
    # creation splits them.
    run_created = (await get_run(turn_id))["created_at"]
    assert (await _row(db_conn, newer))["created_at"] < run_created

    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=turn_id)

    assert [(str(r["hook_outbox_id"]), r["before_run"]) for r in takeable] == [
        (older, True),
        (newer, False),
    ]


async def test_a_plain_turn_reads_steers_from_before_it_ahead_of_its_input(thread):
    from src.server.database.runs import outbox

    ended = await _run(thread)
    missed = await _accepted(thread, ended, "missed")
    await _end(thread, ended)
    turn_id = await _run(thread)
    mid_turn = await _accepted(thread, turn_id, "mid-turn")

    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=turn_id)

    assert [(str(r["hook_outbox_id"]), r["before_run"]) for r in takeable] == [
        (missed, True),
        (mid_turn, False),
    ]


async def test_a_turn_opens_with_every_steer_from_before_it_past_one_page(thread):
    from src.server.services.report_back.steer import take_steers_ahead

    ended = await _run(thread)
    missed = [await _accepted(thread, ended, f"missed {i}") for i in range(51)]
    await _end(thread, ended)
    turn_id = await _run(thread)
    await _accepted(thread, turn_id, "mid-turn")

    ahead = await take_steers_ahead(thread.id, turn_id)

    assert [s.job_id for s in ahead] == missed


async def test_a_steer_a_live_run_holds_cannot_become_another_turn(thread):
    from src.server.database.runs import outbox
    from src.server.database.pool import get_db_connection

    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "x")
    await outbox.hold_jobs([job_id], owner=("run", run_id))
    other = str(uuid.uuid4())

    async with get_db_connection() as conn:
        assert not await outbox.claim_steer_for_turn(conn, job_id, other)
        # The holder's own retried START still owns it.
        assert await outbox.claim_steer_for_turn(conn, job_id, run_id)
        assert await outbox.claim_steer_for_turn(conn, job_id, run_id)
        assert not await outbox.claim_steer_for_turn(conn, job_id, other)


# ---------------------------------------------------------------------------
# Settling at the run's ending
# ---------------------------------------------------------------------------


async def _mixed(thread, run_id: str) -> SimpleNamespace:
    """One steer from each kind of sender into ``run_id``."""
    return SimpleNamespace(
        user=await _accepted(thread, run_id, "user"),
        chat=await _accepted(thread, run_id, "from slack", platform="slack"),
        cos=await _accepted(
            thread, run_id, "from home", sender="chief_of_staff", platform=None
        ),
    )


async def test_a_completed_run_leaves_its_steers_due_and_returns_chat_apps(
    thread, db_conn
):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)
    await outbox.hold_jobs([jobs.user], owner=("run", run_id))

    await _end(thread, run_id, "completed")

    for job in (jobs.user, jobs.cos):
        row = await _row(db_conn, job)
        assert row["status"] == "pending" and row["due"], row
        assert "steered_run_id" not in row["payload"]
        assert "outcome" not in row["payload"]
    chat = await _row(db_conn, jobs.chat)
    assert chat["status"] == "done"
    assert chat["payload"]["outcome"] == "returned"
    assert chat["payload"]["returned_run_id"] == run_id
    assert await outbox.list_returned_platform_steers(run_id) == [
        {"content": "from slack", "user_id": thread.user_id}
    ]
    assert await outbox.list_returned_steers(run_id) == []


async def test_a_cancel_nobody_asked_for_settles_as_completed(thread, db_conn):
    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "x")

    await _end(thread, run_id, "cancelled")

    row = await _row(db_conn, job_id)
    assert row["status"] == "pending" and row["due"]


async def test_an_error_keeps_the_users_drops_the_agents_and_returns_chat_apps(
    thread, db_conn
):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)
    await outbox.hold_jobs([jobs.user], owner=("run", run_id))

    await _end(thread, run_id, "error")

    user = await _row(db_conn, jobs.user)
    assert user["status"] == "pending" and user["parked"]
    assert "steered_run_id" not in user["payload"]
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"
    assert (await _row(db_conn, jobs.chat))["payload"]["outcome"] == "returned"
    pending = await outbox.list_pending_steers(thread.id)
    assert [p["content"] for p in pending] == ["user"]


async def test_an_interrupt_keeps_the_steers_parked_for_the_resume(thread, db_conn):
    """The resume is another run, which cannot take a chat-app steer sent into
    this one, so that steer goes back to its gateway."""
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)
    await outbox.hold_jobs([jobs.user], owner=("run", run_id))

    await _end(thread, run_id, "interrupted")

    for job in (jobs.user, jobs.cos):
        row = await _row(db_conn, job)
        assert row["status"] == "pending" and row["parked"], row
        assert "steered_run_id" not in row["payload"]
        assert "outcome" not in row["payload"]
    chat = await _row(db_conn, jobs.chat)
    assert chat["payload"]["outcome"] == "returned"
    assert chat["payload"]["returned_run_id"] == run_id
    assert await outbox.list_returned_platform_steers(run_id) != []


async def test_settle_at_a_user_stop_returns_the_users_and_drops_the_rest(
    thread, db_conn
):
    """The cancel stamp settles first in production; this is the ending's own
    branch, for whatever the stamp did not see."""
    from src.server.database.runs import outbox
    from src.server.database.pool import get_db_connection

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)

    async with get_db_connection() as conn:
        async with conn.transaction():
            await outbox.settle_run_steers(
                conn,
                thread_id=thread.id,
                run_id=run_id,
                final_status="cancelled",
                user_cancel=True,
            )

    user = await _row(db_conn, jobs.user)
    assert user["payload"]["outcome"] == "returned"
    assert user["payload"]["returned_run_id"] == run_id
    assert (await _row(db_conn, jobs.chat))["payload"]["outcome"] == "returned"
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"


async def test_a_steer_a_stop_answered_without_waits_for_the_users_next_move(
    thread, db_conn
):
    """A stop that gave up waiting for the run already answered, so a held
    steer the ending would return would reach no one: it stays queued."""
    from src.server.database.runs import outbox
    from src.server.database.pool import get_db_connection

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)
    held = await _accepted(thread, run_id, "held")
    await outbox.hold_jobs([held], owner=("run", run_id))

    await outbox.keep_unanswered_steers(thread.id, run_id)
    async with get_db_connection() as conn:
        async with conn.transaction():
            await outbox.settle_run_steers(
                conn,
                thread_id=thread.id,
                run_id=run_id,
                final_status="cancelled",
                user_cancel=True,
            )

    row = await _row(db_conn, held)
    assert row["status"] == "pending" and row["parked"], row
    assert "steered_run_id" not in row["payload"]
    assert [p["content"] for p in await outbox.list_pending_steers(thread.id)] == ["held"]
    assert [r["content"] for r in await outbox.list_returned_steers(run_id)] == ["user"]
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"


async def test_settle_at_an_agent_stop_is_an_error(thread, db_conn):
    from src.server.database.runs import outbox
    from src.server.database.pool import get_db_connection

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)

    async with get_db_connection() as conn:
        async with conn.transaction():
            await outbox.settle_run_steers(
                conn,
                thread_id=thread.id,
                run_id=run_id,
                final_status="cancelled",
                user_cancel=False,
                agent_stop=True,
            )

    user = await _row(db_conn, jobs.user)
    assert user["status"] == "pending" and user["parked"]
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"
    assert (await _row(db_conn, jobs.chat))["payload"]["outcome"] == "returned"


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------


async def test_a_user_stop_gives_back_the_users_own_steers(thread, db_conn):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import request_run_cancel

    run_id = await _run(thread)
    web_one = await _accepted(thread, run_id, "web one")
    jobs = await _mixed(thread, run_id)
    market = await _accepted(thread, run_id, "chart", platform="market_view:AAPL")

    intent = await request_run_cancel(run_id, thread_id=thread.id)

    assert intent["state"] == "requested"
    assert [s["content"] for s in intent["returned_steers"]] == [
        "web one",
        "user",
        "chart",
    ]
    assert all(set(s) == {"content", "request_key", "timestamp"}
               for s in intent["returned_steers"])
    for job in (web_one, jobs.user, market, jobs.chat):
        assert (await _row(db_conn, job))["payload"]["outcome"] == "returned"
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"
    # A stop asked again re-reads them.
    assert await outbox.list_returned_steers(run_id) == intent["returned_steers"]
    again = await request_run_cancel(run_id, thread_id=thread.id)
    assert again["state"] == "already_requested"
    assert again["returned_steers"] == intent["returned_steers"]


async def test_an_agent_stop_drops_the_agents_and_keeps_the_users_queued(
    thread, db_conn
):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import get_run, request_run_cancel

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)

    intent = await request_run_cancel(run_id, thread_id=thread.id, by_user=False)

    assert intent["state"] == "requested"
    assert intent["returned_steers"] == []
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"
    assert "outcome" not in (await _row(db_conn, jobs.user))["payload"]
    assert (await get_run(run_id))["metadata"]["stopped_by_agent"] is True

    await _end(thread, run_id, "cancelled")

    user = await _row(db_conn, jobs.user)
    assert user["status"] == "pending" and user["parked"]
    assert (await _row(db_conn, jobs.chat))["payload"]["outcome"] == "returned"
    assert [p["content"] for p in await outbox.list_pending_steers(thread.id)] == [
        "user"
    ]


async def test_cancel_run_steers_by_an_agent_returns_nothing(thread, db_conn):
    from src.server.database.runs import outbox
    from src.server.database.pool import get_db_connection

    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)

    async with get_db_connection() as conn:
        async with conn.transaction():
            returned = await outbox.cancel_run_steers(
                conn, thread_id=thread.id, run_id=run_id, by_user=False
            )

    assert returned == []
    assert "outcome" not in (await _row(db_conn, jobs.user))["payload"]
    assert "outcome" not in (await _row(db_conn, jobs.chat))["payload"]
    assert (await _row(db_conn, jobs.cos))["payload"]["outcome"] == "dropped"


async def test_list_pending_steers_shows_the_users_web_steers_only(thread):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    await _accepted(thread, run_id, "web")
    await _accepted(thread, run_id, "no platform", platform=None)
    await _accepted(thread, run_id, "market", platform="market_view:AAPL")
    await _accepted(thread, run_id, "telegram", platform="telegram")
    await _accepted(thread, run_id, "home", sender="chief_of_staff", platform=None)
    held = await _accepted(thread, run_id, "held")
    await outbox.hold_jobs([held], owner=("run", run_id))

    pending = await outbox.list_pending_steers(thread.id)

    assert [p["content"] for p in pending] == ["web", "no platform", "market"]


async def test_an_agent_stop_after_the_run_ended_stops_the_run_that_took_the_hand_off(
    thread, db_conn
):
    from src.server.database.runs import outbox

    sent_to = await _run(thread)
    taken = await _accepted(
        thread, sent_to, "taken mid-turn", sender="chief_of_staff", platform=None
    )
    parked = await _accepted(
        thread, sent_to, "still parked", sender="chief_of_staff", platform=None
    )
    user = await _accepted(thread, sent_to, "user")
    await _end(thread, sent_to)
    later = await _run(thread)
    assert await outbox.hold_jobs([taken], owner=("run", later)) == [taken]

    stop = await outbox.stop_hand_off_steers(thread.id, sent_to)

    assert stop == [later]
    assert (await _row(db_conn, taken))["payload"]["outcome"] == "dropped"
    assert (await _row(db_conn, parked))["payload"]["outcome"] == "dropped"
    assert "outcome" not in (await _row(db_conn, user))["payload"]


async def test_stop_hand_off_names_the_turn_a_hand_off_became(thread):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import start_run

    sent_to = await _run(thread)
    job_id = await _accepted(
        thread, sent_to, "hand-off", sender="chief_of_staff", platform=None
    )
    await _end(thread, sent_to)
    turn_id = str(uuid.uuid4())

    async def _before_commit(conn):
        assert await outbox.claim_steer_for_turn(conn, job_id, turn_id)

    await start_run(
        run_id=turn_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc"},
        before_commit=_before_commit,
    )

    assert await outbox.stop_hand_off_steers(thread.id, sent_to) == [turn_id]


# ---------------------------------------------------------------------------
# A hand-off followed to the run that took it in
#
# A hand-off steered into a running turn is taken by whichever run reads it
# first. The card follows it there (``get_owned_steers``) and the report of
# the run that took it names it (``list_taken_hand_offs``); both read the one
# taker rule, so each transition is checked through both.
# ---------------------------------------------------------------------------


async def _cos_steer(thread, run_id: str, text: str = "size the market") -> str:
    return await _accepted(
        thread, run_id, text, sender="chief_of_staff", platform=None
    )


async def _followed(thread, steer_id: str):
    """Where the card finds the steer: ``(state, run_id, run status)``, or
    None when it is omitted."""
    from src.server.database.runs.handoff_liveness import get_owned_steers

    steer = (await get_owned_steers([steer_id], thread.user_id)).get(steer_id)
    if steer is None:
        return None
    run = steer["run"]
    assert steer["thread_id"] == thread.id
    if run is None:
        assert steer["run_id"] is None
        return steer["state"], None, None
    # The run's view is the one ``runs`` gives a run asked for by its id.
    assert str(run["conversation_response_id"]) == steer["run_id"]
    assert str(run["conversation_thread_id"]) == thread.id
    return steer["state"], steer["run_id"], run["status"]


async def _reported(thread, *run_ids: str) -> list:
    from src.server.database.runs import outbox

    return await outbox.list_taken_hand_offs(thread.id, list(run_ids))


async def test_a_hand_off_is_queued_until_held_and_taken_once_read(thread):
    from src.server.database.runs import outbox

    sent_to = await _run(thread)
    steer_id = await _cos_steer(thread, sent_to)

    assert await _followed(thread, steer_id) == ("queued", None, None)
    assert await _reported(thread, sent_to) == []

    # Already in the running turn's messages, waiting for its next step.
    assert await outbox.hold_jobs([steer_id], owner=("run", sent_to)) == [steer_id]
    assert await _followed(thread, steer_id) == ("taken", sent_to, "in_progress")
    assert await _reported(thread, sent_to) == [steer_id]

    assert await outbox.confirm_jobs([steer_id], owner=("run", sent_to)) == [steer_id]
    await _end(thread, sent_to)

    assert await _followed(thread, steer_id) == ("taken", sent_to, "completed")
    assert await _reported(thread, sent_to) == [steer_id]


@pytest.mark.parametrize("given_back", ["released", "run-ended-unread"])
async def test_a_hold_given_back_is_queued_again(thread, given_back):
    from src.server.database.runs import outbox

    sent_to = await _run(thread)
    steer_id = await _cos_steer(thread, sent_to)
    await outbox.hold_jobs([steer_id], owner=("run", sent_to))
    assert (await _followed(thread, steer_id))[0] == "taken"

    if given_back == "released":
        assert await outbox.release_jobs([steer_id], owner=("run", sent_to)) == [
            steer_id
        ]
    else:
        await _end(thread, sent_to)

    assert await _followed(thread, steer_id) == ("queued", None, None)
    assert await _reported(thread, sent_to) == []


async def test_a_hand_off_its_run_missed_is_taken_by_the_next_run_to_read_it(thread):
    from src.server.database.runs import outbox

    sent_to = await _run(thread)
    steer_id = await _cos_steer(thread, sent_to)
    await _end(thread, sent_to)
    later = await _run(thread)

    await outbox.hold_jobs([steer_id], owner=("run", later))
    await outbox.confirm_jobs([steer_id], owner=("run", later))

    assert await _followed(thread, steer_id) == ("taken", later, "in_progress")
    assert await _reported(thread, later) == [steer_id]
    assert await _reported(thread, sent_to) == []


async def test_a_hand_off_its_run_missed_is_taken_by_the_turn_it_becomes(thread):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import start_run

    sent_to = await _run(thread)
    steer_id = await _cos_steer(thread, sent_to)
    await _end(thread, sent_to)
    assert await _followed(thread, steer_id) == ("queued", None, None)
    turn_id = str(uuid.uuid4())

    async def _before_commit(conn):
        assert await outbox.claim_steer_for_turn(conn, steer_id, turn_id)

    await start_run(
        run_id=turn_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": thread.user_id},
        before_commit=_before_commit,
    )

    assert await _followed(thread, steer_id) == ("taken", turn_id, "in_progress")
    assert await _reported(thread, turn_id) == [steer_id]
    assert await _reported(thread, sent_to) == []


@pytest.mark.parametrize("ending", ["error", "user-stop", "agent-stop"])
async def test_a_hand_off_no_run_took_is_dropped(thread, ending):
    from src.server.database.runs.lifecycle import request_run_cancel

    sent_to = await _run(thread)
    steer_id = await _cos_steer(thread, sent_to)

    if ending == "error":
        await _end(thread, sent_to, "error")
    else:
        await request_run_cancel(
            sent_to, thread_id=thread.id, by_user=ending == "user-stop"
        )
        await _end(thread, sent_to, "cancelled")

    assert await _followed(thread, steer_id) == ("dropped", None, None)
    assert await _reported(thread, sent_to) == []


async def test_hand_offs_are_named_oldest_first_and_user_steers_never(thread):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    first = await _cos_steer(thread, run_id, "first")
    user = await _accepted(thread, run_id, "the user's own")
    second = await _cos_steer(thread, run_id, "second")
    jobs = [first, user, second]
    assert sorted(await outbox.hold_jobs(jobs, owner=("run", run_id))) == sorted(jobs)
    await outbox.confirm_jobs(jobs, owner=("run", run_id))

    assert await _reported(thread, run_id) == [first, second]
    assert await _followed(thread, user) is None


async def test_a_card_follows_only_its_callers_hand_offs(thread):
    from src.server.database.runs.handoff_liveness import get_owned_steers

    run_id = await _run(thread)
    steer_id = await _cos_steer(thread, run_id)

    assert await get_owned_steers([steer_id], "someone-else") == {}
    asked = [steer_id, "not-a-uuid", str(uuid.uuid4())]
    assert set(await get_owned_steers(asked, thread.user_id)) == {steer_id}


# ---------------------------------------------------------------------------
# The claim safety net
# ---------------------------------------------------------------------------


async def _claimed_ids() -> set:
    from src.server.database.runs import outbox

    return {str(r["hook_outbox_id"]) for r in await outbox.claim_outbox_jobs(limit=50)}


@pytest.mark.parametrize(
    ("status", "metadata", "due"),
    [
        ("completed", {}, True),
        ("cancelled", {}, True),
        ("cancelled", {"cancelled_by_user": True}, False),
        ("error", {}, False),
        ("interrupted", {}, False),
        ("in_progress", {}, False),
    ],
)
async def test_a_parked_steer_falls_due_once_its_run_ended_unsettled(
    thread, db_conn, status, metadata, due
):
    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "x")
    if status != "in_progress":
        await _set_run(db_conn, run_id, status, metadata)

    claimed = await _claimed_ids()

    assert (job_id in claimed) is due
    row = await _row(db_conn, job_id)
    assert row["status"] == ("claimed" if due else "pending")
    if due:
        assert not row["parked"]


async def test_a_task_steer_falls_due_once_its_task_run_ended_unsettled(
    thread, db_conn
):
    from src.server.database.runs import outbox

    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)
    job_id = (await _task_steer(thread, task_run_id, "x"))["job_id"]
    assert job_id not in await _claimed_ids()

    await db_conn.execute(
        "UPDATE subagent_runs SET status = 'completed', finalized_at = NOW()"
        " WHERE task_run_id = %s",
        (task_run_id,),
    )

    assert job_id in await _claimed_ids()
    assert await outbox.fenced_task_steers([job_id], fence=("claimed", 1)) == [job_id]
    assert await outbox.fenced_task_steers([job_id], fence=("claimed", 2)) == []


# ---------------------------------------------------------------------------
# Task fences
# ---------------------------------------------------------------------------


async def test_fenced_task_steers_tells_free_held_and_others_apart(thread):
    from src.server.database.runs import outbox

    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)
    job_id = (await _task_steer(thread, task_run_id, "x"))["job_id"]

    assert await outbox.fenced_task_steers([job_id], fence=("free", None)) == [job_id]
    assert await outbox.fenced_task_steers([job_id], fence=("held", task_run_id)) == []

    assert await outbox.hold_jobs([job_id], owner=("task", task_run_id)) == [job_id]

    assert await outbox.fenced_task_steers([job_id], fence=("free", None)) == []
    assert await outbox.fenced_task_steers(
        [job_id], fence=("held", task_run_id)
    ) == [job_id]
    assert await outbox.fenced_task_steers(
        [job_id], fence=("held", str(uuid.uuid4()))
    ) == []
    # A thread steer is no task steer, whatever its fence.
    thread_job = await _accepted(thread, parent, "thread")
    assert await outbox.fenced_task_steers([thread_job], fence=("free", None)) == []


# ---------------------------------------------------------------------------
# Blue/green: a build without a steer executor never claims a pending steer
# ---------------------------------------------------------------------------

# Copied from main's ``claim_outbox_jobs`` (the build before steers lived on
# the outbox). Across a flip the old color's drainer runs this and nacks a
# ``steer`` toward dead, so no pending steer may ever match it.
_MAIN_CLAIM_DUE_SQL = """
                            (o.status = 'pending'
                             AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW()))
                            OR (o.status = 'claimed'
                                AND o.lease_expires_at <= NOW()
                                AND o.attempts < %s)
"""


async def _seen_by_main(db_conn, thread) -> list:
    cur = await db_conn.execute(
        f"""
        SELECT o.hook_outbox_id::text AS id FROM hook_outbox o
        WHERE o.conversation_thread_id = %s AND o.hook_type = 'steer'
          AND ({_MAIN_CLAIM_DUE_SQL})
        """,
        (thread.id, 5),
    )
    return [r["id"] for r in await cur.fetchall()]


async def _assert_hidden_from_main(db_conn, thread) -> None:
    """Every pending steer on the thread keeps ``infinity``, and main's claim
    selects none of them."""
    cur = await db_conn.execute(
        """
        SELECT COUNT(*) AS n FROM hook_outbox
        WHERE conversation_thread_id = %s AND hook_type = 'steer'
          AND status = 'pending'
          AND next_retry_at IS DISTINCT FROM 'infinity'::timestamptz
        """,
        (thread.id,),
    )
    assert (await cur.fetchone())["n"] == 0
    assert await _seen_by_main(db_conn, thread) == []


async def _backdate_due(db_conn, job_id: str) -> None:
    """Let a nacked steer's backoff run out."""
    await db_conn.execute(
        """
        UPDATE hook_outbox
        SET payload = payload || jsonb_build_object(
            'due_at', NOW() - interval '1 minute')
        WHERE hook_outbox_id = %s
        """,
        (job_id,),
    )


@pytest.mark.parametrize(
    ("status", "metadata"),
    [
        ("completed", {}),
        ("cancelled", {}),
        ("cancelled", {"cancelled_by_user": True}),
        ("cancelled", {"stopped_by_agent": True}),
        ("error", {}),
        ("interrupted", {}),
    ],
)
async def test_no_ending_leaves_a_steer_claimable_by_a_build_without_steers(
    thread, db_conn, status, metadata
):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    run_id = await _run(thread, metadata=metadata)
    jobs = await _mixed(thread, run_id)
    await outbox.hold_jobs([jobs.user], owner=("run", run_id))
    await _assert_hidden_from_main(db_conn, thread)

    await finalize_run(
        run_id=run_id, thread_id=thread.id, outcome=RunOutcome(status=status)
    )

    await _assert_hidden_from_main(db_conn, thread)


async def test_a_released_steer_is_due_to_this_build_alone_through_nack_and_park(
    thread, db_conn
):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "x")
    await _end(thread, run_id, "completed")

    row = await _row(db_conn, job_id)
    assert row["status"] == "pending" and row["infinity"] and row["due"]
    await _assert_hidden_from_main(db_conn, thread)

    # Released: this build claims it, and a claimed steer carries no due_at.
    assert job_id in await _claimed_ids()
    claimed = await _row(db_conn, job_id)
    assert claimed["status"] == "claimed" and "due_at" not in claimed["payload"]

    # Nacked: backing off in its payload, never in next_retry_at.
    assert await outbox.nack_outbox_job(job_id, attempts=1) == "pending"
    backing_off = await _row(db_conn, job_id)
    assert backing_off["status"] == "pending" and backing_off["infinity"]
    assert "due_at" in backing_off["payload"] and not backing_off["due"]
    await _assert_hidden_from_main(db_conn, thread)
    assert job_id not in await _claimed_ids()

    await _backdate_due(db_conn, job_id)
    assert job_id in await _claimed_ids()

    # Parked again by its executor: no due_at, so only the safety net (the
    # thread's latest run completed) makes it due.
    assert await outbox.defer_claimed_job(job_id, attempts=2) == "pending"
    parked = await _row(db_conn, job_id)
    assert parked["status"] == "pending" and parked["parked"]
    await _assert_hidden_from_main(db_conn, thread)

    assert job_id in await _claimed_ids()
    assert await outbox.nack_outbox_job(job_id, attempts=3) == "pending"
    assert (await _row(db_conn, job_id))["infinity"]
    await _assert_hidden_from_main(db_conn, thread)


async def test_a_due_steer_a_live_turn_holds_is_parked_again(thread, db_conn):
    """The hold takes a due row in and drops its due_at, so the drainer does
    not start a turn for a steer a live run already has in its messages."""
    from src.server.database.runs import outbox

    first = await _run(thread)
    job_id = await _accepted(thread, first, "x")
    await _end(thread, first, "completed")
    assert (await _row(db_conn, job_id))["due"]

    second = await _run(thread)
    assert await outbox.hold_jobs([job_id], owner=("run", second)) == [job_id]

    held = await _row(db_conn, job_id)
    assert held["status"] == "pending" and held["parked"]
    assert job_id not in await _claimed_ids()
    await _assert_hidden_from_main(db_conn, thread)


async def test_a_task_steer_moved_to_resume_stays_hidden_from_main(thread, db_conn):
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import finalize_task_run

    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)
    job_id = (await _task_steer(thread, task_run_id, "x"))["job_id"]

    await finalize_task_run(task_run_id=task_run_id, status="completed")

    moved = await _row(db_conn, job_id)
    assert moved["status"] == "pending" and moved["parked"]
    assert moved["ordering_key"] == f"{thread.id}:steer"
    await _assert_hidden_from_main(db_conn, thread)

    await _end(thread, parent, "completed")
    assert job_id in await _claimed_ids()
    assert await outbox.nack_outbox_job(job_id, attempts=1) == "pending"
    await _assert_hidden_from_main(db_conn, thread)


# ---------------------------------------------------------------------------
# A resume hands its steers to the run it admitted
#
# The main turn that resumes a task with steers its last run never read holds
# them, then hands them to the run the resume admits, which confirms them at
# its first model call. Until then that run's ending settles them like any
# steer it holds, so a worker lost before it checkpoints them loses nothing.
# ---------------------------------------------------------------------------


async def _resume_held(thread, *texts: str) -> SimpleNamespace:
    """Task steers their run never read, back on the thread's chain and held
    by a main turn to resume the task with, and the run the resume admitted."""
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import finalize_task_run

    main = await _run(thread)
    first = await _task_run(thread, main)
    jobs = [
        (await _task_steer(thread, first, text))["job_id"] for text in texts or ("x",)
    ]
    await finalize_task_run(task_run_id=first, status="completed")
    resumable = await outbox.list_resumable_task_steers(thread.id, main)
    assert [str(r["hook_outbox_id"]) for r in resumable] == jobs
    assert sorted(await outbox.hold_jobs(jobs, owner=("run", main))) == sorted(jobs)
    resumed = await _task_run(thread, main, cause="resume")
    return SimpleNamespace(main=main, first=first, resumed=resumed, jobs=jobs)


async def _hand_off(thread, held: SimpleNamespace, jobs=None) -> list:
    from src.server.database.runs import outbox

    return await outbox.hand_off_task_resumes(
        held.jobs if jobs is None else jobs,
        run_id=held.main,
        thread_id=thread.id,
        task_id="k7Xm2p",
        task_run_id=held.resumed,
    )


async def test_a_handed_off_resume_is_the_resumed_runs_to_read(thread, db_conn):
    from src.server.database.runs import outbox

    held = await _resume_held(thread, "narrow it")
    (job_id,) = held.jobs

    assert await _hand_off(thread, held) == [job_id]

    row = await _row(db_conn, job_id)
    assert row["ordering_key"] == f"{thread.id}:task:k7Xm2p"
    assert row["status"] == "pending" and row["parked"]
    assert row["payload"]["steered_run_id"] == held.resumed
    assert row["payload"]["task_run_id"] == held.resumed
    assert row["payload"]["resumed_by"] == held.main
    # The run's own take skips it: it already opens with it.
    assert await outbox.list_takeable_jobs(
        thread.id, ["steer"], target="task", task_id="k7Xm2p"
    ) == []
    assert await outbox.hold_jobs([job_id], owner=("task", held.resumed)) == []
    # Nor can the launching turn settle it any more.
    assert await outbox.confirm_jobs([job_id], owner=("run", held.main)) == []

    assert await outbox.confirm_jobs([job_id], owner=("task", held.resumed)) == [
        job_id
    ]
    done = await _row(db_conn, job_id)
    assert done["status"] == "done"
    assert done["payload"]["outcome"] == "resumed"


async def test_a_resumed_run_that_errs_before_reading_gives_the_steer_back_to_resume(
    thread, db_conn
):
    """A worker lost before the resumed run checkpointed its opener: the
    steer goes back to resume the task again, counted as a refusal by the
    turn that resumed it, which gets no second try."""
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import finalize_task_run

    held = await _resume_held(thread, "narrow it")
    (job_id,) = held.jobs
    assert await _hand_off(thread, held) == [job_id]

    await finalize_task_run(task_run_id=held.resumed, status="error")

    row = await _row(db_conn, job_id)
    assert row["ordering_key"] == f"{thread.id}:steer"
    assert row["status"] == "pending" and row["parked"]
    assert "steered_run_id" not in row["payload"]
    assert "resumed_by" not in row["payload"]
    assert row["payload"]["resume_refusals"] == 1
    assert row["payload"]["resume_refused_by"] == held.main
    assert await outbox.list_resumable_task_steers(thread.id, held.main) == []
    resumable = await outbox.list_resumable_task_steers(thread.id, str(uuid.uuid4()))
    assert [str(r["hook_outbox_id"]) for r in resumable] == [job_id]


async def test_a_resumed_run_that_keeps_dying_unread_gives_the_steer_up(
    thread, db_conn
):
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import finalize_task_run

    held = await _resume_held(thread, "narrow it")
    (job_id,) = held.jobs
    await db_conn.execute(
        "UPDATE hook_outbox SET payload = payload || jsonb_build_object("
        "'resume_refusals', %s::int) WHERE hook_outbox_id = %s",
        (outbox.MAX_RESUME_REFUSALS - 1, job_id),
    )
    assert await _hand_off(thread, held) == [job_id]

    await finalize_task_run(task_run_id=held.resumed, status="error")

    row = await _row(db_conn, job_id)
    assert row["status"] == "done"
    assert row["payload"]["outcome"] == "returned"
    assert row["payload"]["returned_run_id"] == held.resumed
    assert await outbox.list_resumable_task_steers(thread.id, str(uuid.uuid4())) == []


async def test_a_stopped_resumed_run_returns_the_users_steer_on_its_own_run(
    thread, db_conn
):
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import finalize_task_run

    held = await _resume_held(thread, "narrow it")
    (job_id,) = held.jobs
    assert await _hand_off(thread, held) == [job_id]

    await finalize_task_run(task_run_id=held.resumed, status="cancelled")

    row = await _row(db_conn, job_id)
    assert row["status"] == "done"
    assert row["payload"]["outcome"] == "returned"
    assert row["payload"]["returned_run_id"] == held.resumed
    returned = await outbox.list_returned_task_steers(
        thread.id, "k7Xm2p", held.resumed
    )
    assert [r["content"] for r in returned] == ["narrow it"]
    assert await outbox.list_returned_task_steers(thread.id, "k7Xm2p", held.first) == []


async def test_a_hand_off_to_a_stopped_run_misses(thread, db_conn):
    from src.server.database.runs.subagent_runs import request_task_run_cancel

    held = await _resume_held(thread, "narrow it")
    (job_id,) = held.jobs
    await request_task_run_cancel(held.resumed, thread_id=thread.id)

    assert await _hand_off(thread, held) == []

    row = await _row(db_conn, job_id)
    assert row["ordering_key"] == f"{thread.id}:steer"
    assert row["payload"]["steered_run_id"] == held.main
    assert "resumed_by" not in row["payload"]


async def test_a_hand_off_moves_all_the_steers_or_none(thread, db_conn):
    """The run opens with every one of them, so it must hold every one."""
    from src.server.database.runs import outbox

    held = await _resume_held(thread, "narrow it", "and this")
    kept, lost = held.jobs
    assert await outbox.release_jobs([lost], owner=("run", held.main)) == [lost]

    assert await _hand_off(thread, held) == []

    for job_id in held.jobs:
        assert (await _row(db_conn, job_id))["ordering_key"] == f"{thread.id}:steer"
    assert (await _row(db_conn, kept))["payload"]["steered_run_id"] == held.main


async def test_a_nacked_report_back_still_backs_off_in_next_retry_at(thread, db_conn):
    """Only steers moved their due time into the payload."""
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    await outbox.enqueue_compensation_job(
        run_id=run_id,
        thread_id=thread.id,
        hook_type="task_report_back",
        payload={"task_id": "k7Xm2p"},
        ordering_key=f"{thread.id}:rb-{uuid.uuid4().hex[:6]}",
        idempotency_key=f"task_report_back:{uuid.uuid4()}",
    )
    cur = await db_conn.execute(
        "SELECT hook_outbox_id::text AS id FROM hook_outbox"
        " WHERE run_id = %s AND hook_type = 'task_report_back'",
        (run_id,),
    )
    job_id = (await cur.fetchone())["id"]
    assert job_id in await _claimed_ids()

    assert await outbox.nack_outbox_job(job_id, attempts=1) == "pending"

    cur = await db_conn.execute(
        """
        SELECT next_retry_at > NOW() AS backing_off, payload
        FROM hook_outbox WHERE hook_outbox_id = %s
        """,
        (job_id,),
    )
    row = await cur.fetchone()
    assert row["backing_off"]
    assert row["payload"] == {"task_id": "k7Xm2p"}


# ---------------------------------------------------------------------------
# Out of attempts: settled per sender, never dead
# ---------------------------------------------------------------------------


async def _claim_at_ceiling(db_conn, job_id: str, *, expired: bool = False) -> int:
    """Put a row where its fifth lease leaves it: claimed, out of attempts."""
    await db_conn.execute(
        """
        UPDATE hook_outbox
        SET status = 'claimed', attempts = 5, next_retry_at = NULL,
            lease_expires_at = NOW() + CASE WHEN %s THEN interval '-1 minute'
                                            ELSE interval '1 minute' END,
            payload = payload - 'due_at'
        WHERE hook_outbox_id = %s
        """,
        (expired, job_id),
    )
    return 5


async def _exhaust(db_conn, job_id: str, path: str):
    from src.server.database.runs import outbox

    attempts = await _claim_at_ceiling(db_conn, job_id, expired=path == "park")
    if path == "nack":
        return await outbox.nack_outbox_job(job_id, attempts=attempts)
    if path == "defer":
        return await outbox.defer_claimed_job(job_id, attempts=attempts)
    parked = await outbox.park_exhausted_jobs()
    return {str(r["hook_outbox_id"]): r["status"] for r in parked}.get(job_id)


_PATHS = ["nack", "defer", "park"]


@pytest.mark.parametrize("path", _PATHS)
async def test_a_users_steer_out_of_attempts_parks_for_the_user(
    thread, db_conn, path
):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    job_id = await _accepted(thread, run_id, "user")

    status = await _exhaust(db_conn, job_id, path)

    assert status == ("pending" if path == "park" else "exhausted")
    row = await _row(db_conn, job_id)
    assert row["status"] == "pending" and row["parked"], row
    assert row["payload"]["exhausted"] is True
    assert "outcome" not in row["payload"]
    assert "steered_run_id" not in row["payload"]
    assert [p["content"] for p in await outbox.list_pending_steers(thread.id)] == [
        "user"
    ]
    # The safety net would hand it straight back to the executor that gave up.
    await _set_run(db_conn, run_id, "completed")
    assert job_id not in await _claimed_ids()
    await _assert_hidden_from_main(db_conn, thread)


@pytest.mark.parametrize("path", _PATHS)
@pytest.mark.parametrize(
    ("kind", "outcome"),
    [("chat", "returned"), ("cos", "dropped")],
)
async def test_a_chat_app_or_agent_steer_out_of_attempts_closes(
    thread, db_conn, path, kind, outcome
):
    run_id = await _run(thread)
    jobs = await _mixed(thread, run_id)
    job_id = getattr(jobs, kind)

    status = await _exhaust(db_conn, job_id, path)

    assert status == ("done" if path == "park" else "exhausted")
    row = await _row(db_conn, job_id)
    assert row["status"] == "done"
    assert row["payload"]["outcome"] == outcome
    if outcome == "returned":
        assert row["payload"]["returned_run_id"] == run_id
    # Only the row out of attempts settled.
    assert "outcome" not in (await _row(db_conn, jobs.user))["payload"]


@pytest.mark.parametrize("path", _PATHS)
@pytest.mark.parametrize(
    ("sender", "outcome"),
    [("user", "returned"), ("chief_of_staff", "dropped")],
)
async def test_a_task_steer_out_of_attempts_closes_as_its_stop_would(
    thread, db_conn, path, sender, outcome
):
    from src.server.database.runs.subagent_runs import get_task_run

    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)
    job_id = (await _task_steer(thread, task_run_id, "narrow it", sender=sender))[
        "job_id"
    ]

    await _exhaust(db_conn, job_id, path)

    row = await _row(db_conn, job_id)
    assert row["status"] == "done"
    assert row["payload"]["outcome"] == outcome
    facts = (await get_task_run(task_run_id)).get("replay_facts") or {}
    returned = [e["content"] for e in facts.get("steering_returned") or []]
    assert returned == (["narrow it"] if outcome == "returned" else [])


@pytest.mark.parametrize("path", _PATHS)
async def test_a_report_back_out_of_attempts_still_goes_dead(thread, db_conn, path):
    from src.server.database.runs import outbox

    run_id = await _run(thread)
    await outbox.enqueue_compensation_job(
        run_id=run_id,
        thread_id=thread.id,
        hook_type="task_report_back",
        payload={"task_id": "k7Xm2p"},
        ordering_key=f"{thread.id}:rb-{uuid.uuid4().hex[:6]}",
        idempotency_key=f"task_report_back:{uuid.uuid4()}",
    )
    cur = await db_conn.execute(
        "SELECT hook_outbox_id::text AS id FROM hook_outbox"
        " WHERE run_id = %s AND hook_type = 'task_report_back'",
        (run_id,),
    )
    job_id = (await cur.fetchone())["id"]

    assert await _exhaust(db_conn, job_id, path) == "dead"
    assert (await _row(db_conn, job_id))["status"] == "dead"


async def test_a_users_exhausted_steer_still_becomes_the_next_turn_once(
    thread, db_conn
):
    """A run's ending makes it due like any parked steer; its one more claim
    either posts the turn or exhausts it again, never dead."""
    from src.server.database.runs import outbox

    first = await _run(thread)
    job_id = await _accepted(thread, first, "user")
    await _exhaust(db_conn, job_id, "nack")
    await _end(thread, first, "error")

    second = await _run(thread)
    await _end(thread, second, "completed")

    assert (await _row(db_conn, job_id))["due"]
    claimed = [r for r in await outbox.claim_outbox_jobs(limit=50)
               if str(r["hook_outbox_id"]) == job_id]
    assert claimed and claimed[0]["attempts"] == 6
    assert await outbox.nack_outbox_job(job_id, attempts=6) == "exhausted"
    row = await _row(db_conn, job_id)
    assert row["status"] == "pending" and row["parked"]
    cur = await db_conn.execute(
        "SELECT COUNT(*) AS n FROM hook_outbox"
        " WHERE hook_type = 'steer' AND status = 'dead'"
    )
    assert (await cur.fetchone())["n"] == 0


# ---------------------------------------------------------------------------
# A stop racing the confirm of a steer the run already holds
# ---------------------------------------------------------------------------


def _fake_reader(monkeypatch, *, run_messages=None, task_messages=None, fail=False):
    """A checkpoint reader that hands back the given messages, as the run's
    newest step or the task namespace's state would carry them."""
    from src.server.services.history import reader as reader_mod

    async def _state(messages):
        if fail:
            raise RuntimeError("checkpoint unreadable")
        return SimpleNamespace(values={"messages": messages or []})

    class _Graph:
        async def aget_state(self, config):
            return await _state(task_messages)

    class _Reader:
        graph = _Graph()

        async def aget_run_state(self, thread_id, run_id):
            return await _state(run_messages)

    monkeypatch.setattr(
        reader_mod.CheckpointHistoryReader, "get_instance", classmethod(lambda cls: _Reader())
    )


def _steering_message(owner_key: str, owner_id: str, job_ids: list):
    from langchain_core.messages import HumanMessage

    return HumanMessage(
        content="steer",
        additional_kwargs={"held_jobs": {owner_key: owner_id, "job_ids": job_ids}},
    )


async def test_a_user_stop_leaves_a_held_steer_to_its_confirm(thread, db_conn):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import request_run_cancel

    run_id = await _run(thread, metadata={"cancelled_by_user": True})
    held = await _accepted(thread, run_id, "held")
    free = await _accepted(thread, run_id, "free")
    await outbox.hold_jobs([held], owner=("run", run_id))

    intent = await request_run_cancel(run_id, thread_id=thread.id)

    assert intent["state"] == "requested"
    assert [s["content"] for s in intent["returned_steers"]] == ["free"]
    assert intent["held_steers"] == 1
    assert "outcome" not in (await _row(db_conn, held))["payload"]
    assert (await _row(db_conn, free))["payload"]["outcome"] == "returned"

    # The model call reading it confirms it after the stamp.
    assert await outbox.confirm_jobs([held], owner=("run", run_id)) == [held]
    await _end(thread, run_id, "cancelled")

    row = await _row(db_conn, held)
    assert row["payload"]["outcome"] == "delivered"
    assert row["payload"]["steered_run_id"] == run_id
    assert [s["content"] for s in await outbox.list_returned_steers(run_id)] == ["free"]
    again = await request_run_cancel(run_id, thread_id=thread.id)
    assert again["held_steers"] == 0


async def test_the_ending_confirms_checkpointed_holds_and_returns_the_rest(
    thread, db_conn, monkeypatch
):
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import request_run_cancel
    from src.server.services.report_back.steer import confirm_checkpointed_holds

    run_id = await _run(thread, metadata={"cancelled_by_user": True})
    read = await _accepted(thread, run_id, "read")
    unread = await _accepted(thread, run_id, "unread")
    await outbox.hold_jobs([read], owner=("run", run_id))
    await outbox.hold_jobs([unread], owner=("run", run_id))
    intent = await request_run_cancel(run_id, thread_id=thread.id)
    assert intent["returned_steers"] == [] and intent["held_steers"] == 2
    # Only the first one's step was checkpointed; another run's stamp is not
    # this run's.
    _fake_reader(
        monkeypatch,
        run_messages=[
            _steering_message("run_id", str(uuid.uuid4()), [unread]),
            _steering_message("run_id", run_id, [read]),
        ],
    )

    assert await confirm_checkpointed_holds(thread.id, run_id) == [read]
    await _end(thread, run_id, "cancelled")

    assert (await _row(db_conn, read))["payload"]["outcome"] == "delivered"
    assert (await _row(db_conn, unread))["payload"]["outcome"] == "returned"
    assert [s["content"] for s in await outbox.list_returned_steers(run_id)] == [
        "unread"
    ]


async def test_an_unreadable_checkpoint_settles_holds_as_before(
    thread, db_conn, monkeypatch
):
    from src.server.database.runs import outbox
    from src.server.services.report_back.steer import confirm_checkpointed_holds

    run_id = await _run(thread, metadata={"cancelled_by_user": True})
    held = await _accepted(thread, run_id, "held")
    await outbox.hold_jobs([held], owner=("run", run_id))
    _fake_reader(monkeypatch, fail=True)

    assert await confirm_checkpointed_holds(thread.id, run_id) == []
    await _end(thread, run_id, "cancelled")

    assert (await _row(db_conn, held))["payload"]["outcome"] == "returned"


async def test_a_task_stop_leaves_its_held_steer_to_the_runs_ending(
    thread, db_conn, monkeypatch
):
    from src.server.database.runs import outbox
    from src.server.database.runs.subagent_runs import (
        finalize_task_run,
        request_task_run_cancel,
    )
    from src.server.services.report_back.steer import (
        confirm_checkpointed_task_holds,
    )

    parent = await _run(thread)
    task_run_id = await _task_run(thread, parent)
    read = (await _task_steer(thread, task_run_id, "read"))["job_id"]
    unread = (await _task_steer(thread, task_run_id, "unread"))["job_id"]
    free = (await _task_steer(thread, task_run_id, "free"))["job_id"]
    await outbox.hold_jobs([read, unread], owner=("task", task_run_id))

    intent = await request_task_run_cancel(task_run_id, thread_id=thread.id)

    assert intent["state"] == "requested"
    assert intent["run"]["held_steers"] == 2
    assert (await _row(db_conn, free))["payload"]["outcome"] == "returned"
    for job in (read, unread):
        assert "outcome" not in (await _row(db_conn, job))["payload"]

    _fake_reader(
        monkeypatch,
        task_messages=[_steering_message("task_run_id", task_run_id, [read])],
    )
    assert await confirm_checkpointed_task_holds(
        thread.id, "k7Xm2p", task_run_id
    ) == [read]
    await finalize_task_run(task_run_id=task_run_id, status="cancelled")

    assert (await _row(db_conn, read))["payload"]["outcome"] == "delivered"
    assert (await _row(db_conn, unread))["payload"]["outcome"] == "returned"
    returned = await outbox.list_returned_task_steers(thread.id, "k7Xm2p", task_run_id)
    assert sorted(r["content"] for r in returned) == ["free", "unread"]


# ---------------------------------------------------------------------------
# A fork rewinds the runs that own steers
# ---------------------------------------------------------------------------


async def _fork(thread, from_turn: int, *, edit: bool) -> str:
    from src.server.database.runs.lifecycle import ForkSpec, start_run

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": thread.user_id},
        fork=ForkSpec(
            from_turn=from_turn,
            checkpoint_id=str(uuid.uuid4()),
            branch_tip=None,
            preserve_query_at_fork=not edit,
        ),
    )
    return run_id


async def _delivered(thread, run_id: str, text: str) -> str:
    from src.server.database.runs import outbox

    job_id = await _accepted(thread, run_id, text)
    await outbox.hold_jobs([job_id], owner=("run", run_id))
    assert await outbox.confirm_jobs([job_id], owner=("run", run_id)) == [job_id]
    return job_id


@pytest.mark.parametrize("edit", [False, True], ids=["regenerate", "edit"])
async def test_a_fork_keeps_the_steers_its_rewound_turn_still_owes(
    thread, db_conn, edit
):
    """The truncation's cascade would delete a steer parked by the rewound
    turn's error, which nobody read; the fork's turn reads it after its
    input. What the rewound turn read mid-turn goes with that turn."""
    from src.server.database.runs import outbox

    errored = await _run(thread)
    read = await _delivered(thread, errored, "read mid-turn")
    parked = await _accepted(thread, errored, "parked by the error")
    await _end(thread, errored, "error")

    fork_id = await _fork(thread, 0, edit=edit)

    row = await _row(db_conn, parked)
    assert row["run_id"] == fork_id
    assert row["status"] == "pending" and row["parked"]
    assert not {"outcome", "steered_run_id"} & row["payload"].keys()
    assert await _row(db_conn, read) is None
    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=fork_id)
    assert [str(r["hook_outbox_id"]) for r in takeable] == [parked]
    if edit:
        assert not takeable[0]["before_run"]


async def test_an_edit_opens_with_the_steers_the_edited_turn_read_ahead_of_its_input(
    thread, db_conn
):
    """A regenerate keeps them in the input checkpoint it starts over from;
    an edit replaces that input, so the rows go ahead of the new one."""
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import get_run

    first = await _run(thread)
    missed = await _accepted(thread, first, "missed")
    await _end(thread, first)
    edited = await _run(thread)
    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=edited)
    assert [(str(r["hook_outbox_id"]), r["before_run"]) for r in takeable] == [
        (missed, True)
    ]
    await outbox.hold_jobs([missed], owner=("run", edited))
    await outbox.confirm_jobs([missed], owner=("run", edited))
    mid_turn = await _delivered(thread, edited, "mid-turn")
    await _end(thread, edited)

    fork_id = await _fork(thread, 1, edit=True)

    assert (await get_run(fork_id))["metadata"]["input_sent_at"]
    row = await _row(db_conn, missed)
    assert row["run_id"] == fork_id and row["status"] == "pending"
    assert "outcome" not in row["payload"]
    assert await _row(db_conn, mid_turn) is None
    takeable = await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=fork_id)
    assert [(str(r["hook_outbox_id"]), r["before_run"]) for r in takeable] == [
        (missed, True)
    ]


async def test_a_regenerate_leaves_what_the_turn_read_to_its_input_checkpoint(
    thread, db_conn
):
    from src.server.database.runs import outbox

    first = await _run(thread)
    missed = await _accepted(thread, first, "missed")
    await _end(thread, first)
    regenerated = await _run(thread)
    await outbox.hold_jobs([missed], owner=("run", regenerated))
    await outbox.confirm_jobs([missed], owner=("run", regenerated))
    await _end(thread, regenerated)

    fork_id = await _fork(thread, 1, edit=False)

    row = await _row(db_conn, missed)
    assert row["status"] == "done" and row["payload"]["outcome"] == "delivered"
    assert row["run_id"] == first
    assert await outbox.list_takeable_jobs(thread.id, ["steer"], run_id=fork_id) == []
