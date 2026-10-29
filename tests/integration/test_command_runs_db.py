"""The background command ledger and its notice jobs against a real Postgres.

Who may read a command, when its notice commits, and which notice a drainer
may claim are each decided by one guarded statement (a SKIP LOCKED lease, a
CAS that enqueues in its own transaction, the outbox's chain gate), so only a
real database shows two workers never share a row and a notice held for the
next turn is never claimed or made due.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio
from psycopg.rows import dict_row

from src.server.database.runs import command_runs as cr
from src.server.database.runs import outbox as ob
from src.server.database.runs.command_runs import Outcome

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# Every hook type but this one, so a claim here can only take this test's jobs.
_OTHER_TYPES = [
    "task_report_back", "report_back", "steer", "watch_clear", "market_target",
    "wakeup", "burst_release", "user_feed", "needs_input_wake",
]


@pytest_asyncio.fixture(loop_scope="session")
async def thread(patched_get_db_connection, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"commands-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    workspace = await create_workspace(user_id=user_id, name="Commands", status="running")
    workspace_id = str(workspace["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id, workspace_id=workspace_id, current_status="completed"
    )
    try:
        yield SimpleNamespace(
            id=thread_id, user_id=user_id, workspace_id=workspace_id,
            computer_id=str(uuid.uuid4()),
        )
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


def _cid() -> str:
    return "bg-" + uuid.uuid4().hex[:12]


def _read(command_id: str, mark=None) -> dict:
    """The row as a finalize's caller judged it."""
    return {"command_id": command_id, "stopped_by": mark}


async def _end(command_id: str, outcome: Outcome, **kw):
    return await cr.finalize(_read(command_id), outcome, **kw)


# An end that reports nothing, for a test that needs only an ended row.
QUIET = Outcome("stopped")


async def _handoff(thread, command_id=None, *, description="Run 10-year backtest"):
    command_id = command_id or _cid()
    return await cr.record_handoff(
        command_id=command_id,
        thread_id=thread.id,
        user_id=thread.user_id,
        workspace_id=thread.workspace_id,
        computer_id=thread.computer_id,
        run_id=str(uuid.uuid4()),
        tool_call_id="toolu_1",
        tool="Bash",
        description=description,
        command_preview="python backtest.py",
        log_path=f".agents/threads/x/commands/{command_id}.log",
        job_ref={
            "command_id": command_id, "state_dir": "/s", "log_path": "/l",
            "trace_path": "/t", "log_relpath": "l",
        },
        started_at=datetime.now(timezone.utc) - timedelta(minutes=23),
    )


async def _jobs(pool, thread_id):
    async with pool.connection() as conn:
        result = await conn.execute(
            """
            SELECT hook_outbox_id, hook_type, payload, ordering_key, status, run_id,
                   next_retry_at = 'infinity'::timestamptz AS waiting,
                   next_retry_at IS NULL AS due
            FROM hook_outbox
            WHERE conversation_thread_id = %s
              AND hook_type IN ('command_report_back', 'command_notice')
            ORDER BY created_at
            """,
            (thread_id,),
        )
        return [dict(row) for row in await result.fetchall()]


async def test_a_handoff_is_due_at_once_and_recorded_once(thread):
    first = await _handoff(thread)
    again = await _handoff(thread, first["command_id"], description="other")
    assert first["status"] == "running" and first["next_check_at"] is not None
    assert again["description"] == "Run 10-year backtest"


async def test_concurrent_claimers_never_share_a_row(thread):
    ids = {(await _handoff(thread))["command_id"] for _ in range(24)}
    batches = await asyncio.gather(*(cr.claim_due(limit=5) for _ in range(8)))
    claimed = [r["command_id"] for b in batches for r in b if r["command_id"] in ids]
    assert len(claimed) == len(set(claimed))
    # Leased rows wait out their interval; then they come due again.
    assert not {r["command_id"] for r in await cr.claim_due(limit=500)} & set(claimed)


async def test_finalize_is_one_cas_that_commits_the_notice_with_it(thread, test_db_pool):
    command_id = (await _handoff(thread))["command_id"]
    row = await _end(
        command_id, Outcome("completed", exit_code=0), tail="x" * 9000,
        trace=[{"server": "fmp", "tool": "quote"}],
    )
    assert row["status"] == "completed" and row["ended_at"] is not None
    assert len(row["output_tail"]) == 4096
    assert await _end(command_id, Outcome("failed", "lost")) is None

    jobs = [j for j in await _jobs(test_db_pool, thread.id) if j["payload"]["command_id"] == command_id]
    assert len(jobs) == 1
    job = jobs[0]
    assert job["hook_type"] == "command_report_back"
    assert job["ordering_key"] == thread.id and job["due"] and job["run_id"] is None
    assert job["payload"] == {
        "command_id": command_id, "tool": "Bash", "description": "Run 10-year backtest",
        "status": "completed", "failure_reason": None, "tool_call_id": "toolu_1",
    }
    assert (await cr.get_command(command_id))["mcp_trace"] == [{"server": "fmp", "tool": "quote"}]
    shown = await cr.get_thread_command(thread.id, command_id, with_trace=True)
    assert shown["mcp_trace"] == [{"server": "fmp", "tool": "quote"}]
    assert "mcp_trace" not in await cr.get_thread_command(thread.id, command_id)


async def test_another_thread_reads_none_of_a_threads_commands(thread):
    """Even a sibling in the same workspace: the command, its trace included,
    is its own thread's."""
    from src.server.database.conversation import create_thread

    sibling = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=sibling, workspace_id=thread.workspace_id,
        current_status="completed",
    )
    command_id = (await _handoff(thread))["command_id"]
    assert await cr.get_thread_command(thread.id, command_id) is not None
    assert await cr.get_thread_command(sibling, command_id) is None
    assert await cr.get_thread_command(sibling, command_id, with_trace=True) is None


async def test_only_the_users_stop_is_told_and_at_the_next_turn(thread, test_db_pool):
    agents, users = (await _handoff(thread))["command_id"], (await _handoff(thread))["command_id"]
    assert await _end(agents, Outcome("stopped", stopped_by="agent"))
    assert await _end(users, Outcome("stopped", stopped_by="user"))
    jobs = {j["payload"]["command_id"]: j for j in await _jobs(test_db_pool, thread.id)}
    assert agents not in jobs
    assert jobs[users]["hook_type"] == "command_notice" and jobs[users]["waiting"]


async def test_a_next_turn_notice_is_never_claimed_or_made_due(thread, test_db_pool):
    now_id = (await _handoff(thread))["command_id"]
    later_id = (await _handoff(thread))["command_id"]
    await _end(later_id, Outcome("failed", "after_turn_limit"))
    await _end(now_id, Outcome("failed", "exit", 1))

    jobs = {j["payload"]["command_id"]: j for j in await _jobs(test_db_pool, thread.id)}
    later = jobs[later_id]
    assert later["hook_type"] == "command_notice"
    assert later["ordering_key"] == f"command_notice:{thread.id}" and later["waiting"]

    # Older on the thread's chain, it still blocks nothing: the 'now' notice
    # behind it is claimed, and it is not.
    claimed = await ob.claim_outbox_jobs(limit=10, excluded_hook_types=_OTHER_TYPES)
    mine = [j["payload"]["command_id"] for j in claimed if str(j["conversation_thread_id"]) == thread.id]
    assert mine == [now_id]

    # A live turn may take it in; a turn that held it and died leaves it
    # parked for the next one, never due.
    steerable = {j["payload"]["command_id"] for j in await ob.list_steerable_report_jobs(thread.id)}
    assert later_id in steerable
    run_id = str(uuid.uuid4())
    await ob.merge_job_payload(str(later["hook_outbox_id"]), {"steered_run_id": run_id})
    async with test_db_pool.connection() as conn:
        await ob.release_deferred_notifications(conn, thread.id)
    after = {j["payload"]["command_id"]: j for j in await _jobs(test_db_pool, thread.id)}[later_id]
    assert after["waiting"] and "steered_run_id" not in after["payload"]
    assert later_id not in {
        j["payload"]["command_id"]
        for j in await ob.list_open_notification_jobs(thread.id, ("task_report_back", "command_report_back"))
    }


async def test_a_notice_is_owed_until_seen_and_while_a_turn_may_come(thread, test_db_pool):
    """The one read the verdict judges by, and the owed predicate the alert
    sweeps share with it."""
    ended, stopped, users = (_cid(), _cid(), _cid())
    for command_id in (ended, stopped, users):
        await _handoff(thread, command_id)
    await _end(ended, Outcome("failed", "after_turn_limit"))
    await _end(stopped, QUIET)
    await _end(users, Outcome("stopped", stopped_by="user"))

    row = await cr.read_notice(ended)
    assert (row["owed"], row["no_turn"]) == (True, False)
    assert str(row["thread_workspace_id"]) == thread.workspace_id
    assert (await cr.read_notice(stopped))["owed"] is False
    assert (await cr.read_notice(users))["owed"] is True
    assert await cr.read_notice("bg-000000000000") is None

    [job] = [j for j in await _jobs(test_db_pool, thread.id) if j["payload"]["command_id"] == ended]

    async def alert_owed() -> bool:
        async with test_db_pool.connection() as conn:
            result = await conn.execute(
                f"SELECT {ob._ALERT_OWED_SQL} AS owed FROM hook_outbox o"
                " WHERE o.hook_outbox_id = %s",
                (job["hook_outbox_id"],),
            )
            return (await result.fetchone())["owed"]

    assert await alert_owed() is True
    await cr.stamp_delivered([ended])
    assert (await cr.read_notice(ended))["owed"] is False
    assert await alert_owed() is False

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW() WHERE conversation_thread_id = %s",
            (thread.id,),
        )
    assert (await cr.read_notice(ended))["no_turn"] is True


async def test_a_seen_end_is_stamped_once_and_never_mid_run_or_quietly_stopped(
    thread, test_db_pool
):
    running, ended, stopped, users = (_cid(), _cid(), _cid(), _cid())
    for command_id in (running, ended, stopped, users):
        await _handoff(thread, command_id)
    await _end(ended, Outcome("failed", "exit", 2, up=False))
    await _end(stopped, QUIET)
    await _end(users, Outcome("stopped", stopped_by="user"))
    assert sorted(await cr.stamp_delivered([running, ended, stopped, users])) == sorted(
        [ended, users]
    )
    assert await cr.stamp_delivered([ended]) == []

    # Reported once seen, or once its notice job closed.
    listed = {r["command_id"]: r["reported"] for r in await cr.list_for_thread(thread.id)}
    assert (listed[ended], listed[stopped]) == (True, False)
    other = (await _handoff(thread))["command_id"]
    await _end(other, Outcome("failed", "exit", 2, up=False))
    assert (await cr.get_command(other))["reported"] is False
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE hook_outbox SET status = 'done' WHERE idempotency_key = %s",
            (cr.report_job_key(other),),
        )
    assert (await cr.get_command(other))["reported"] is True
    assert (await cr.get_thread_command(thread.id, other))["reported"] is True


async def test_the_thread_listing_puts_running_first_and_filters(thread):
    done = (await _handoff(thread))["command_id"]
    await _end(done, Outcome("failed", "exit", 2, up=False))
    running = (await _handoff(thread))["command_id"]
    rows = await cr.list_for_thread(thread.id, limit=None)
    assert rows[0]["status"] == "running"
    assert "mcp_trace" not in rows[0]
    assert {r["command_id"] for r in await cr.list_for_thread(thread.id, statuses=["failed"])} >= {done}
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    since = await cr.list_for_thread(thread.id, since=later, limit=None)
    assert running in {r["command_id"] for r in since}
    assert done not in {r["command_id"] for r in since}


async def test_the_listing_caps_ended_commands_and_never_a_running_one(thread):
    for _ in range(2):
        await _end((await _handoff(thread))["command_id"], QUIET)
    running = {(await _handoff(thread))["command_id"] for _ in range(2)}
    rows = await cr.list_for_thread(thread.id, limit=1)
    assert {r["command_id"] for r in rows[:2]} == running
    assert [r["status"] for r in rows[2:]] == ["stopped"]
    stopped = await cr.list_for_thread(thread.id, statuses=["stopped"], limit=1)
    assert [r["status"] for r in stopped] == ["stopped"]


async def test_running_commands_list_by_thread_or_workspace(thread):
    running = (await _handoff(thread))["command_id"]
    ended = (await _handoff(thread))["command_id"]
    await _end(ended, QUIET)
    for scope in ({"thread_id": thread.id}, {"workspace_id": thread.workspace_id}):
        ids = {r["command_id"] for r in await cr.list_running(**scope)}
        assert running in ids and ended not in ids
    with pytest.raises(ValueError):
        await cr.list_running()
    with pytest.raises(ValueError):
        await cr.list_running(thread_id=thread.id, workspace_id=thread.workspace_id)


async def test_an_archive_marks_only_its_unmarked_running_commands_due_now(
    thread, test_db_pool
):
    """The watcher's next claim takes each one and ends it as the mark says;
    a stop already under way keeps its stopper."""
    fresh, asked, ended = _cid(), _cid(), _cid()
    for command_id in (fresh, asked, ended):
        await _handoff(thread, command_id)
    await _end(ended, QUIET)
    await cr.request_stop(asked, "user")

    assert await cr.request_stops(thread_id=thread.id, by="archive") == [fresh]

    async with test_db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT command_id, stopped_by, next_check_at <= NOW() AS due "
                "FROM command_runs WHERE command_id = ANY(%s)",
                ([fresh, asked, ended],),
            )
            rows = await cur.fetchall()
    by_id = {r["command_id"]: (r["stopped_by"], r["due"]) for r in rows}
    assert by_id[fresh] == ("archive", True)
    assert by_id[asked] == ("user", False)
    assert by_id[ended][0] is None
    assert await cr.request_stops(workspace_id=thread.workspace_id, by="workspace") == []
    with pytest.raises(ValueError):
        await cr.request_stops(by="archive")


async def test_a_workspace_stop_supersedes_the_hours_mark_and_its_finalize(thread):
    """The hour's stop of a job that exited on its own would report it now,
    into a workspace being deleted; its finalize, judged on its own mark,
    loses to the workspace's. A stop someone asked for keeps its stopper."""
    hour, asked, fresh = _cid(), _cid(), _cid()
    for command_id in (hour, asked, fresh):
        await _handoff(thread, command_id)
    judged = await cr.request_stop(hour, "after_turn_limit")
    await cr.request_stop(asked, "agent")

    marked = await cr.request_stops(
        workspace_id=thread.workspace_id, by="workspace", supersede=("after_turn_limit",)
    )

    assert sorted(marked) == sorted([hour, fresh])
    assert await cr.finalize(judged, Outcome.exited(0, up=True)) is None
    assert (await cr.get_command(asked))["stopped_by"] == "agent"


async def test_a_delete_reads_its_running_commands_on_its_own_session(thread, test_db_pool):
    running = (await _handoff(thread))["command_id"]
    async with test_db_pool.connection() as conn:
        rows = await cr.list_running(thread_id=thread.id, conn=conn)
    assert [r["command_id"] for r in rows] == [running]


async def test_an_exit_137_counts_once_with_the_move_that_ends_it(thread, test_db_pool):
    """A retried finalize moves nothing, so it counts nothing; a failed count
    never holds back the end or its notice."""
    from unittest.mock import patch

    from src.server.database.computer import create_computer, update_computer_status

    ref = f"sb-{uuid.uuid4().hex[:8]}"
    computer = await create_computer(
        thread.user_id, kind="daytona", name="box", status="stopped", provider_ref=ref
    )
    computer_id = str(computer["computer_id"])
    try:
        await update_computer_status(computer_id, "running")
        killed = (await _handoff(thread))["command_id"]
        for _ in range(2):
            await _end(killed, Outcome("failed", "exit", 137), oom_kill_on=("daytona", ref))
        async with test_db_pool.connection() as conn:
            result = await conn.execute(
                "SELECT oom_kills FROM computer_runs WHERE computer_id = %s AND ended_at IS NULL",
                (computer_id,),
            )
            assert (await result.fetchone())["oom_kills"] == 1

        # A statement that fails aborts the transaction it runs in, so the
        # count must sit in a savepoint of its own.
        async def failing_count(*args, conn=None):
            await conn.execute("SELECT 1 / 0")

        broken = (await _handoff(thread))["command_id"]
        with patch(
            "src.server.database.computer_runs.record_oom_kill", new=failing_count
        ):
            ended = await _end(
                broken, Outcome("failed", "exit", 137), oom_kill_on=("daytona", ref)
            )
        assert ended is not None and ended["status"] == "failed"
        async with test_db_pool.connection() as conn:
            result = await conn.execute(
                "SELECT status FROM hook_outbox WHERE idempotency_key = %s",
                (f"command:{broken}:report_back",),
            )
            assert (await result.fetchone())["status"] == "pending"
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM computers WHERE computer_id = %s", (computer_id,))


async def test_a_notice_a_turn_holds_is_stamped_delivered_by_its_confirm(thread, test_db_pool):
    """The steered road's stamp reads the held command jobs beside the alerts."""
    from src.server.services.report_back import steer

    command_id = (await _handoff(thread))["command_id"]
    await _end(command_id, Outcome("completed", exit_code=0))
    [job] = [j for j in await _jobs(test_db_pool, thread.id) if j["payload"]["command_id"] == command_id]
    job_id, run_id = str(job["hook_outbox_id"]), str(uuid.uuid4())
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE hook_outbox SET payload = payload || jsonb_build_object('steered_run_id', %s::text)"
            " WHERE hook_outbox_id = %s",
            (run_id, job_id),
        )

    held = await ob.held_notice_jobs([job_id], run_id)
    assert [(str(j["hook_outbox_id"]), j["hook_type"]) for j in held] == [
        (job_id, "command_report_back")
    ]
    assert await ob.held_notice_jobs([job_id], str(uuid.uuid4())) == []

    await steer._stamp_held_notices([job_id], run_id)
    assert (await cr.get_command(command_id))["result_delivered_at"] is not None


async def test_the_after_turn_mark_never_overwrites_a_stop_under_way(thread):
    command_id = (await _handoff(thread))["command_id"]
    assert await cr.request_stop(command_id, "user") is not None
    assert await cr.request_stop(command_id, "after_turn_limit", over=_read(command_id)) is None
    assert (await cr.get_command(command_id))["stopped_by"] == "user"

    fresh = (await _handoff(thread))["command_id"]
    marked = await cr.request_stop(fresh, "after_turn_limit", over=_read(fresh))
    assert marked["stopped_by"] == "after_turn_limit"


async def test_a_stop_that_lands_after_the_read_wins_the_finalize(thread):
    """The user's Stop overwrites the hour's mark mid-stop, and a mark on a
    row judged ended leaves it to its stopper."""
    hour = (await _handoff(thread))["command_id"]
    await cr.request_stop(hour, "after_turn_limit", over=_read(hour))
    await cr.request_stop(hour, "user")
    assert await cr.finalize(
        _read(hour, "after_turn_limit"), Outcome("failed", "after_turn_limit")
    ) is None
    stopped = await cr.finalize(_read(hour, "user"), Outcome("stopped", stopped_by="user"))
    assert stopped["status"] == "stopped" and stopped["stopped_by"] == "user"

    ended = (await _handoff(thread))["command_id"]
    await cr.request_stop(ended, "user")
    assert await _end(ended, Outcome("completed", exit_code=0)) is None
    assert (await cr.get_command(ended))["status"] == "running"


async def test_a_finished_look_sets_the_next_check_unless_a_stop_moved_it(thread):
    plain, marked = (await _handoff(thread))["command_id"], (await _handoff(thread))["command_id"]
    claimed = {
        r["command_id"]: r["next_check_at"]
        for r in await cr.claim_due(limit=500, interval_s=90)
        if r["command_id"] in (plain, marked)
    }
    assert set(claimed) == {plain, marked}
    await cr.request_stop(marked, "user")
    grace = (await cr.get_command(marked))["next_check_at"]

    await cr.schedule_next_check(list(claimed.items()), 15)

    soon = (await cr.get_command(plain))["next_check_at"]
    assert soon < claimed[plain] - timedelta(seconds=60)
    assert (await cr.get_command(marked))["next_check_at"] == grace


async def test_a_next_turn_notice_on_an_archived_thread_is_closed_by_the_sweep(
    thread, test_db_pool
):
    """No executor ever claims one, so the drainer's close sweep is all that
    ends it once no turn will come; one a live turn holds is left to it."""
    held_id = (await _handoff(thread))["command_id"]
    parked_id = (await _handoff(thread))["command_id"]
    for command_id in (held_id, parked_id):
        await _end(command_id, Outcome("failed", "after_turn_limit"))
    jobs = {j["payload"]["command_id"]: j for j in await _jobs(test_db_pool, thread.id)}
    await ob.merge_job_payload(
        str(jobs[held_id]["hook_outbox_id"]), {"steered_run_id": str(uuid.uuid4())}
    )

    # Live thread: nothing closes.
    closed = {str(j["hook_outbox_id"]) for j in await ob.close_unowed_waiting_alerts()}
    assert not closed & {str(j["hook_outbox_id"]) for j in jobs.values()}

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW() WHERE conversation_thread_id = %s",
            (thread.id,),
        )
    closed = {str(j["hook_outbox_id"]) for j in await ob.close_unowed_waiting_alerts()}
    assert str(jobs[parked_id]["hook_outbox_id"]) in closed
    assert str(jobs[held_id]["hook_outbox_id"]) not in closed
    after = {j["payload"]["command_id"]: j for j in await _jobs(test_db_pool, thread.id)}
    assert after[parked_id]["status"] == "done"
    assert after[held_id]["status"] == "pending"


async def test_statuses_are_read_for_this_computer_only(thread):
    running = (await _handoff(thread))["command_id"]
    ended = (await _handoff(thread))["command_id"]
    await _end(ended, Outcome("failed", "lost", up=False))
    got = await cr.statuses_on(thread.computer_id, [running, ended, "bg-nothere"])
    assert got == {running: "running", ended: "failed"}
    assert await cr.statuses_on(str(uuid.uuid4()), [running]) == {}


async def test_an_earlier_attempts_trace_recorded_at_hand_off_comes_before_the_jobs_own(thread):
    command_id = _cid()
    earlier = [{"server": "s", "tool": "first-attempt"}]
    await cr.record_handoff(
        command_id=command_id,
        thread_id=thread.id,
        user_id=thread.user_id,
        workspace_id=thread.workspace_id,
        computer_id=thread.computer_id,
        run_id=None,
        tool_call_id="toolu_1",
        tool="ExecuteCode",
        description="",
        command_preview="import x",
        log_path="l",
        job_ref={
            "command_id": command_id, "state_dir": "/s", "log_path": "/l",
            "trace_path": "/t", "log_relpath": "l",
        },
        started_at=datetime.now(timezone.utc),
        mcp_trace=earlier,
    )
    await _end(command_id, Outcome("completed", exit_code=0), trace=[{"server": "s", "tool": "rerun"}])
    assert (await cr.get_command(command_id))["mcp_trace"] == [
        *earlier, {"server": "s", "tool": "rerun"},
    ]

    # A finalize with no trace of its own keeps what is there, and a row
    # with none takes the job's.
    plain = (await _handoff(thread))["command_id"]
    await _end(plain, Outcome("completed", exit_code=0), trace=[{"tool": "t"}])
    assert (await cr.get_command(plain))["mcp_trace"] == [{"tool": "t"}]


async def test_a_hand_off_on_a_thread_archived_meanwhile_is_written_marked_by_the_archive(
    thread, test_db_pool
):
    """The archive's stop marked the rows it found, and this one was not yet
    written: it comes marked, due at once, so the watcher ends the job."""
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW() WHERE conversation_thread_id = %s",
            (thread.id,),
        )
    row = await _handoff(thread)
    assert row["stopped_by"] == "archive"
    assert row["command_id"] in {r["command_id"] for r in await cr.claim_due(limit=500)}


async def test_an_id_another_thread_holds_is_never_taken_for_this_ones(thread):
    from src.server.database.conversation import create_thread

    other = SimpleNamespace(**{**vars(thread), "id": str(uuid.uuid4())})
    await create_thread(
        conversation_thread_id=other.id, workspace_id=thread.workspace_id,
        current_status="completed",
    )
    command_id = (await _handoff(thread))["command_id"]
    with pytest.raises(RuntimeError):
        await _handoff(other, command_id)
    assert str((await cr.get_command(command_id))["conversation_thread_id"]) == thread.id


async def test_a_plain_stop_goes_over_only_the_hours_mark(thread):
    """An agent told its stop reports nothing is not then told the user's."""
    hour, agents = _cid(), _cid()
    for command_id in (hour, agents):
        await _handoff(thread, command_id)
    await cr.request_stop(hour, "after_turn_limit")
    await cr.request_stop(agents, "agent")

    assert (await cr.request_stop(hour, "user"))["stopped_by"] == "user"
    assert await cr.request_stop(agents, "user") is None
    assert (await cr.get_command(agents))["stopped_by"] == "agent"


async def test_a_running_command_holds_its_computers_folders(thread, test_db_pool):
    from src.server.database.workspace_folders import computer_command_running

    command_id = (await _handoff(thread))["command_id"]
    async with test_db_pool.connection() as conn:
        assert await computer_command_running(conn, thread.computer_id)
        assert not await computer_command_running(conn, str(uuid.uuid4()))
    await _end(command_id, QUIET)
    async with test_db_pool.connection() as conn:
        assert not await computer_command_running(conn, thread.computer_id)


async def test_a_report_an_older_build_dead_lettered_is_made_due_again_while_owed(
    thread, test_db_pool
):
    """Across a cutover the old color's drainer knows no command report and
    kills it at the attempts ceiling; this build brings it back."""
    owed, seen = (await _handoff(thread))["command_id"], (await _handoff(thread))["command_id"]
    for command_id in (owed, seen):
        await _end(command_id, Outcome("completed", exit_code=0))
    await cr.stamp_delivered([seen])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            """
            UPDATE hook_outbox SET status = 'dead', next_retry_at = NULL,
                completed_at = NOW() - interval '20 minutes'
            WHERE conversation_thread_id = %s AND hook_type = 'command_report_back'
            """,
            (thread.id,),
        )

    revived = {
        r["payload"]["command_id"]
        for r in await ob.revive_dead_alerts()
        if str(r["conversation_thread_id"]) == thread.id
    }

    assert revived == {owed}


async def test_a_command_notice_a_week_old_is_closed(thread, test_db_pool):
    old, fresh = (await _handoff(thread))["command_id"], (await _handoff(thread))["command_id"]
    for command_id in (old, fresh):
        await _end(command_id, Outcome("failed", "after_turn_limit"))
    async with test_db_pool.connection() as conn:
        await conn.execute(
            """
            UPDATE hook_outbox SET created_at = NOW() - interval '8 days'
            WHERE conversation_thread_id = %s AND hook_type = 'command_notice'
              AND payload->>'command_id' = %s
            """,
            (thread.id, old),
        )

    closed = {
        r["payload"]["command_id"]
        for r in await ob.close_unowed_waiting_alerts()
        if str(r["conversation_thread_id"]) == thread.id
    }

    assert closed == {old}
