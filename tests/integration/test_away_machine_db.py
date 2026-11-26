"""Work waiting on a user's own machine that never comes back, against real Postgres.

A command whose machine is away records since when (``away_since``), and any
read of the machine clears it, so the watcher can give up a week on. A
notification parked for a computer (``park_for_computer``) is given up once
the computer's tunnel has gone unseen as long: a report-back closed, an alert
held for the thread's next turn. An automation's firing parked for a computer
is released for one worker to start once the tunnel is live. All are guarded
statements every worker may run at once, so only a real database shows what
they match.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

from src.server.contracts.status import (
    COMPUTER_LOST_REASON,
    RESTART_INTERRUPTED_META,
    RESTART_REASON_META,
    RESTART_RESUME_TTL,
)
from src.server.database import automation_executions as ex
from src.server.database import ssh_tunnels as tunnels_db
from src.server.database.runs import command_runs as cr
from src.server.database.runs import outbox as ob
from src.server.database.runs import restart_resume as rr_db
from src.server.database.runs.hook_jobs import RESTART_RESUME_HOOK

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _gateway(pool, *, started: timedelta, seen: timedelta | None) -> None:
    """The gateway's own row: started and last seen that long ago."""
    now = datetime.now(timezone.utc)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO ssh_gateway_state (singleton, started_at, seen_at)"
            " VALUES (TRUE, %(started)s, %(seen)s) ON CONFLICT (singleton)"
            " DO UPDATE SET started_at = %(started)s, seen_at = %(seen)s",
            {"started": now - started, "seen": None if seen is None else now - seen},
        )


@pytest_asyncio.fixture(loop_scope="session")
async def settled_gateway(test_db_pool):
    """A gateway that has run long enough to judge its tunnels."""
    await _gateway(test_db_pool, started=timedelta(hours=1), seen=timedelta(seconds=10))
    try:
        yield
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM ssh_gateway_state")


@pytest_asyncio.fixture(loop_scope="session")
async def machine(patched_get_db_connection, test_db_pool, settled_gateway):
    """A thread whose workspace runs on a machine that connects out through a
    tunnel; ``seen`` sets how long ago the gateway last saw it."""
    from src.server.database.computer import create_computer
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"away-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    workspace = await create_workspace(user_id=user_id, name="Away", status="running")
    workspace_id = str(workspace["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id, workspace_id=workspace_id, current_status="completed"
    )
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO ssh_tunnels (user_id, public_key, fingerprint, paired_at,"
            " connected_at, disconnected_at, seen_at)"
            " VALUES (%s, 'ssh-ed25519 AAAA', %s, NOW(), NOW(), NOW(), NOW())"
            " RETURNING tunnel_id::text AS tunnel_id",
            (user_id, uuid.uuid4().hex),
        )
        tunnel_id = (await cur.fetchone())["tunnel_id"]
    computer = await create_computer(
        user_id,
        kind="ssh",
        name="mini",
        status="running",
        provider_ref="mini-1",
        provider_config={"tunnel": tunnel_id},
    )

    async def seen(ago: timedelta) -> None:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "UPDATE ssh_tunnels SET seen_at = %s, created_at = %s WHERE tunnel_id = %s",
                (datetime.now(timezone.utc) - ago, datetime.now(timezone.utc) - ago, tunnel_id),
            )

    try:
        yield SimpleNamespace(
            thread_id=thread_id, user_id=user_id, workspace_id=workspace_id,
            computer_id=str(computer["computer_id"]), seen=seen,
        )
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM automations WHERE user_id = %s", (user_id,))
            await conn.execute("DELETE FROM computers WHERE user_id = %s", (user_id,))
            await conn.execute("DELETE FROM ssh_tunnels WHERE user_id = %s", (user_id,))
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def _parked(
    pool, machine, hook_type: str, *, payload: dict | None = None,
    ordering_key: str | None = None, thread_id: str | None = None,
) -> str:
    """A job parked for the machine's computer, as ``park_for_computer`` leaves it."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO hook_outbox (conversation_thread_id, hook_type, payload,"
            " ordering_key, idempotency_key, status, next_retry_at)"
            " VALUES (%s, %s, %s::jsonb, %s, %s, 'pending', 'infinity')"
            " RETURNING hook_outbox_id::text AS id",
            (
                thread_id or machine.thread_id, hook_type,
                json.dumps(
                    {**(payload or {}), ob.AWAITS_COMPUTER: machine.computer_id, "waited": 3}
                ),
                ordering_key or f"{hook_type}:{machine.thread_id}", uuid.uuid4().hex,
            ),
        )
        return (await cur.fetchone())["id"]


async def _job(pool, job_id: str) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, payload, next_retry_at = 'infinity'::timestamptz AS waiting"
            " FROM hook_outbox WHERE hook_outbox_id = %s",
            (job_id,),
        )
        return dict(await cur.fetchone())


async def _bind(pool, workspace_id: str, computer_id: str | None) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET computer_id = %s WHERE workspace_id = %s",
            (computer_id, workspace_id),
        )


_AGENT_STEER = {"sender": "analyst", "target": "thread", "message": "Look again"}


async def test_a_machine_back_within_the_week_keeps_its_parked_notifications(
    machine, test_db_pool
):
    await machine.seen(timedelta(days=6))
    job = await _parked(test_db_pool, machine, "task_report_back")

    await ob.close_for_lost_computers()

    assert (await _job(test_db_pool, job))["status"] == "pending"


async def test_a_machine_gone_a_week_has_its_report_backs_closed(machine, test_db_pool):
    await machine.seen(timedelta(days=8))
    job = await _parked(test_db_pool, machine, "task_report_back")

    closed = await ob.close_for_lost_computers()

    assert job in {str(r["hook_outbox_id"]) for r in closed}
    row = await _job(test_db_pool, job)
    assert row["status"] == "done"
    assert ob.AWAITS_COMPUTER not in row["payload"]
    # Closed once: a second sweep finds nothing of it.
    assert job not in {str(r["hook_outbox_id"]) for r in await ob.close_for_lost_computers()}


async def test_a_machine_gone_a_week_has_its_alerts_held_for_the_next_turn(
    machine, test_db_pool
):
    """The notice is still owed to the agent: the thread's next turn takes it
    in, and the machine coming back still brings its wake."""
    await machine.seen(timedelta(days=8))
    job = await _parked(test_db_pool, machine, "market_target")

    held = await ob.close_for_lost_computers()

    assert job in {str(r["hook_outbox_id"]) for r in held}
    row = await _job(test_db_pool, job)
    assert (row["status"], row["waiting"]) == ("pending", True)
    assert row["payload"]["exhausted"] is True
    assert row["payload"][ob.AWAITS_COMPUTER] == machine.computer_id
    assert job not in {str(r["hook_outbox_id"]) for r in await ob.close_for_lost_computers()}


async def test_a_machine_gone_a_week_releases_an_analysts_watch(machine, test_db_pool):
    """An analyst's report-back closed undelivered clears its watch as a dead
    one does, so the card waiting on it reconciles."""
    await machine.seen(timedelta(days=8))
    job = await _parked(
        test_db_pool, machine, "report_back",
        payload={"ptc_thread_id": str(uuid.uuid4()), "dispatch_gen": 2},
        ordering_key=machine.thread_id,
    )

    closed = await ob.close_for_lost_computers()

    assert {str(r["hook_outbox_id"]): r["status"] for r in closed}[job] == "done"
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, ordering_key, payload FROM hook_outbox"
            " WHERE idempotency_key = %s AND hook_type = 'watch_clear'",
            (f"{job}:lost_clear",),
        )
        clear = await cur.fetchone()
    assert (clear["status"], clear["ordering_key"]) == ("pending", machine.thread_id)
    assert clear["payload"]["error_wake"] is True
    assert clear["payload"]["refuse_if_pointer"] is True
    assert clear["payload"]["dispatch_gen"] == 2


async def test_a_machine_gone_a_week_has_its_agents_steers_dropped(machine, test_db_pool):
    """The sender reads an outcome, as for a steer out of attempts, rather
    than a steer closed with none."""
    await machine.seen(timedelta(days=8))
    job = await _parked(
        test_db_pool, machine, "steer", payload=_AGENT_STEER,
        ordering_key=ob.steer_ordering_key(machine.thread_id),
    )

    closed = await ob.close_for_lost_computers()

    assert job in {str(r["hook_outbox_id"]) for r in closed}
    row = await _job(test_db_pool, job)
    assert (row["status"], row["payload"]["outcome"]) == ("done", "dropped")


async def test_the_steer_safety_net_leaves_a_steer_parked_for_its_computer(
    machine, test_db_pool
):
    """A thread with no live run would hand an unowned parked steer to its
    executor; one waiting for its computer waits for that computer's return."""
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO conversation_responses (conversation_thread_id, turn_index,"
            " status, created_at) VALUES (%s, 0, 'in_progress', NOW())",
            (machine.thread_id,),
        )
        await conn.execute(
            "UPDATE conversation_responses SET status = 'completed'"
            " WHERE conversation_thread_id = %s",
            (machine.thread_id,),
        )
    job = await _parked(
        test_db_pool, machine, "steer", payload=_AGENT_STEER,
        ordering_key=ob.steer_ordering_key(machine.thread_id),
    )

    claimed = await ob.claim_outbox_jobs(limit=500)
    assert job not in {str(r["hook_outbox_id"]) for r in claimed}

    # The same steer parked by no computer is the safety net's to take.
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE hook_outbox SET payload = payload - %s WHERE hook_outbox_id = %s",
            (ob.AWAITS_COMPUTER, job),
        )
    claimed = await ob.claim_outbox_jobs(limit=500)
    assert job in {str(r["hook_outbox_id"]) for r in claimed}


async def test_a_machine_gone_a_week_leaves_a_job_whose_workspace_moved_to_the_release(
    machine, test_db_pool
):
    """A workspace moved to a computer that can be reached between the
    release sweep and the close: its report-back is still owed there, so the
    close leaves it for the release rather than closing it undelivered."""
    from src.server.database.computer import create_computer

    await machine.seen(timedelta(days=8))
    report = await _parked(test_db_pool, machine, "task_report_back")
    other = await create_computer(machine.user_id, name="cloud", status="running")
    await _bind(test_db_pool, machine.workspace_id, str(other["computer_id"]))
    try:
        closed = await ob.close_for_lost_computers()

        assert report not in {str(r["hook_outbox_id"]) for r in closed}
        assert (await _job(test_db_pool, report))["status"] == "pending"
    finally:
        await _bind(test_db_pool, machine.workspace_id, None)


async def test_a_job_whose_workspace_moved_off_its_computer_is_released(
    machine, test_db_pool
):
    """The machine is still away, but the thread no longer runs there: its
    executor looks at the computer the workspace is on now."""
    from src.server.database.computer import create_computer

    await _bind(test_db_pool, machine.workspace_id, machine.computer_id)
    report = await _parked(test_db_pool, machine, "task_report_back")
    steer = await _parked(
        test_db_pool, machine, "steer", payload=_AGENT_STEER,
        ordering_key=ob.steer_ordering_key(machine.thread_id),
    )
    try:
        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}
        assert not {report, steer} & released

        other = await create_computer(machine.user_id, name="cloud", status="running")
        await _bind(test_db_pool, machine.workspace_id, str(other["computer_id"]))
        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}

        assert {report, steer} <= released
        row = await _job(test_db_pool, report)
        assert (row["status"], row["waiting"]) == ("pending", False)
        assert ob.AWAITS_COMPUTER not in row["payload"]
        # A steer is due by its due_at, never by next_retry_at.
        row = await _job(test_db_pool, steer)
        assert (row["status"], row["waiting"]) == ("pending", True)
        assert "due_at" in row["payload"]
        assert ob.AWAITS_COMPUTER not in row["payload"]
    finally:
        await _bind(test_db_pool, machine.workspace_id, None)


async def test_an_analysts_report_back_waits_on_the_thread_it_reports_to(
    machine, test_db_pool
):
    """Keyed on the thread it reports to, whose workspace is still on the
    away machine; the analyst's own thread being elsewhere moves nothing."""
    from src.server.database.computer import create_computer
    from src.server.database.conversation import create_thread
    from src.server.database.workspace import create_workspace

    other = await create_computer(machine.user_id, name="cloud", status="running")
    analyst = await create_workspace(user_id=machine.user_id, name="Analyst", status="running")
    analyst_id = str(analyst["workspace_id"])
    analyst_thread = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=analyst_thread, workspace_id=analyst_id,
        current_status="completed",
    )
    await _bind(test_db_pool, analyst_id, str(other["computer_id"]))
    await _bind(test_db_pool, machine.workspace_id, machine.computer_id)
    try:
        job = await _parked(
            test_db_pool, machine, "report_back",
            payload={"ptc_thread_id": analyst_thread},
            ordering_key=machine.thread_id, thread_id=analyst_thread,
        )

        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}

        assert job not in released
        assert (await _job(test_db_pool, job))["waiting"] is True
    finally:
        await _bind(test_db_pool, machine.workspace_id, None)
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (analyst_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (analyst_id,))


async def _handed_off(machine) -> str:
    """A command handed off on the machine's computer; its id."""
    command_id = "bg-" + uuid.uuid4().hex[:12]
    await cr.record_handoff(
        command_id=command_id,
        thread_id=machine.thread_id,
        user_id=machine.user_id,
        workspace_id=machine.workspace_id,
        computer_id=machine.computer_id,
        run_id=str(uuid.uuid4()),
        tool_call_id="toolu_1",
        tool="Bash",
        description="Serve the dashboard",
        command_preview="python serve.py",
        log_path=f".agents/threads/x/commands/{command_id}.log",
        job_ref={
            "command_id": command_id, "state_dir": "/s", "log_path": "/l",
            "trace_path": "/t", "log_relpath": "l",
        },
        started_at=datetime.now(timezone.utc),
    )
    return command_id


async def test_a_command_handed_off_clears_the_away_its_computers_others_kept(
    machine, test_db_pool
):
    """The machine came back long enough to launch a command, unseen by the
    watcher: its older command's week-old away mark would make the next
    outage give up on both at once. The launch is a read of the machine."""
    older = await _handed_off(machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE command_runs SET away_since = NOW() - INTERVAL '8 days'"
            " WHERE command_id = %s",
            (older,),
        )

    await _handed_off(machine)

    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT away_since FROM command_runs WHERE command_id = %s", (older,)
        )
        assert (await cur.fetchone())["away_since"] is None


async def test_a_commands_away_since_is_kept_while_away_and_cleared_by_a_read(machine):
    command_id = await _handed_off(machine)

    async def look(schedule) -> dict:
        (row,) = [r for r in await cr.claim_due(limit=500, interval_s=0) if r["command_id"] == command_id]
        await schedule([(command_id, row["next_check_at"])], 0)
        (after,) = [r for r in await cr.claim_due(limit=500, interval_s=0) if r["command_id"] == command_id]
        return after

    first = await look(cr.schedule_away_check)
    assert first["away_since"] is not None
    again = await look(cr.schedule_away_check)
    assert again["away_since"] == first["away_since"]
    read = await look(cr.schedule_next_check)
    assert read["away_since"] is None


async def test_a_read_clears_the_away_of_its_computers_commands_not_due(machine, test_db_pool):
    """A command not claimed on the pass whose read answered still had its
    stamp, and the machine's next absence counted from the old one."""
    read_id = await _handed_off(machine)
    idle_id = await _handed_off(machine)
    due = {r["command_id"]: r for r in await cr.claim_due(limit=500, interval_s=0)}
    await cr.schedule_away_check(
        [(c, due[c]["next_check_at"]) for c in (read_id, idle_id)], 3600
    )
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "UPDATE command_runs SET next_check_at = NOW() - interval '1 second'"
            " WHERE command_id = %s RETURNING next_check_at",
            (read_id,),
        )
        lease = (await cur.fetchone())["next_check_at"]
    await cr.schedule_next_check([(read_id, lease)], 0)
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT command_id, away_since FROM command_runs WHERE command_id = ANY(%s)",
            ([read_id, idle_id],),
        )
        stamps = {r["command_id"]: r["away_since"] for r in await cur.fetchall()}
    assert stamps == {read_id: None, idle_id: None}


async def test_a_read_whose_lease_was_taken_over_leaves_its_takers_away(
    machine, test_db_pool
):
    """A worker that read the machine, then lost its lease to one that found
    the machine away, cleared the newer stamp with its older read."""
    read_id = await _handed_off(machine)
    idle_id = await _handed_off(machine)
    due = {r["command_id"]: r for r in await cr.claim_due(limit=500, interval_s=0)}
    stale = due[read_id]["next_check_at"]
    await cr.schedule_away_check(
        [(c, due[c]["next_check_at"]) for c in (read_id, idle_id)], 3600
    )
    await cr.schedule_next_check([(read_id, stale)], 0)
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT command_id, away_since FROM command_runs WHERE command_id = ANY(%s)",
            ([read_id, idle_id],),
        )
        stamps = {r["command_id"]: r["away_since"] for r in await cur.fetchall()}
    assert stamps[read_id] is not None and stamps[idle_id] is not None


async def _automation(machine, name: str = "Brief") -> str:
    """A daily automation of the machine's user; its id."""
    from src.server.database.automation import create_automation

    automation = await create_automation(
        user_id=machine.user_id, name=name, instruction="Summarize",
        trigger_type="cron", cron_expression="0 9 * * *",
    )
    return str(automation["automation_id"])


async def _parked_firing(
    pool, machine, *, gives_up_in: timedelta = timedelta(hours=23)
) -> tuple[str, str]:
    """A daily automation and a firing of it parked for the machine's
    computer, as the executor leaves one, its wait up ``gives_up_in`` from
    now; their ids."""
    automation_id = await _automation(machine)
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO automation_executions (automation_id, status, scheduled_at,"
            " started_at, heartbeat_at, wait_reason, awaits_computer, parked_status,"
            " awaits_until, conversation_thread_id)"
            " VALUES (%s, 'waiting', NOW(), NOW(), NOW() + %s,"
            " 'computer_unreachable', %s, 'active', NOW() + %s, %s)"
            " RETURNING automation_execution_id::text AS id",
            (automation_id, gives_up_in, machine.computer_id, gives_up_in, machine.thread_id),
        )
        return automation_id, (await cur.fetchone())["id"]


async def _parked_verdict(execution_id: str) -> dict | None:
    """The sweep's verdict on a parked firing; None when the sweep is not
    shown it, as nothing it could do would change it."""
    rows = [
        r for r in await ex.list_parked_executions(limit=500)
        if str(r["automation_execution_id"]) == execution_id
    ]
    if not rows:
        return None
    (row,) = rows
    return {k: row[k] for k in ("superseded", "reachable", "stand_in", "expired")}


async def test_a_firing_parked_for_a_machine_is_released_once_and_only_once_it_is_back(
    machine, test_db_pool
):
    import asyncio

    _, execution_id = await _parked_firing(test_db_pool, machine)

    # Nothing to do with it yet, so it takes no place in the sweep's page.
    assert await _parked_verdict(execution_id) is None
    # Its heartbeat is quiet, yet no process lost it.
    abandoned = await ex.list_abandoned_executions(60, limit=500)
    assert execution_id not in {str(r["automation_execution_id"]) for r in abandoned}
    assert await ex.release_parked_execution(execution_id) is False

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
    assert (await _parked_verdict(execution_id))["reachable"] is True
    taken = await asyncio.gather(
        *(ex.release_parked_execution(execution_id) for _ in range(4))
    )
    assert sorted(taken) == [False, False, False, True]
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, conversation_thread_id,"
            " heartbeat_at > NOW() - interval '1 minute' AS beating"
            " FROM automation_executions WHERE automation_execution_id = %s",
            (execution_id,),
        )
        row = await cur.fetchone()
    assert (row["status"], row["conversation_thread_id"], row["beating"]) == ("pending", None, True)


async def _home_on_machine(pool, machine) -> str:
    """The user's Home, on the machine's computer; its id."""
    from src.server.database.workspace import get_or_create_home_workspace

    home_id = str((await get_or_create_home_workspace(machine.user_id))["workspace_id"])
    await _bind(pool, home_id, machine.computer_id)
    return home_id


async def test_a_parked_home_firing_its_stand_in_runs_is_released_with_the_machine_away(
    machine, test_db_pool
):
    """Once the user has a cloud computer for Home's stand-in, a Home firing
    need not wait for the machine: the sweep is shown it, though the machine
    is still away, and it is still released only once."""
    from src.server.database.computer import create_computer

    automation_id, execution_id = await _parked_firing(test_db_pool, machine)
    home_id = await _home_on_machine(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automations SET workspace_id = %s WHERE automation_id = %s",
            (home_id, automation_id),
        )
    assert await _parked_verdict(execution_id) is None
    assert await ex.release_parked_execution(execution_id) is False

    await create_computer(machine.user_id, kind="daytona", name="cloud", status="running",
                          provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
    assert (await _parked_verdict(execution_id))["stand_in"] is True
    assert await ex.release_parked_execution(execution_id) is True
    assert await ex.release_parked_execution(execution_id) is False


async def test_a_home_job_its_stand_in_runs_is_released_with_the_machine_away(
    machine, test_db_pool
):
    """A wake parked for the machine Home lives on is due again once the
    user has a computer Home's stand-in can run on; a job of any other
    workspace on the machine keeps waiting for it."""
    from src.server.database.computer import create_computer
    from src.server.database.conversation import create_thread

    home_id = await _home_on_machine(test_db_pool, machine)
    home_thread = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=home_thread, workspace_id=home_id, current_status="completed"
    )
    await _bind(test_db_pool, machine.workspace_id, machine.computer_id)
    home_wake = await _parked(test_db_pool, machine, "wakeup_fire", thread_id=home_thread)
    other_wake = await _parked(test_db_pool, machine, "wakeup_fire")
    try:
        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}
        assert not {home_wake, other_wake} & released

        # One being retired takes no stand-in.
        await create_computer(machine.user_id, kind="daytona", name="old", status="running",
                              provider_ref="sb-old", config={"retire_when_empty": True})
        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}
        assert not {home_wake, other_wake} & released

        await create_computer(machine.user_id, kind="daytona", name="cloud", status="running",
                              provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
        released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}
        assert home_wake in released
        assert other_wake not in released
    finally:
        await _bind(test_db_pool, machine.workspace_id, None)
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM hook_outbox WHERE conversation_thread_id = %s", (home_thread,))
            await conn.execute("DELETE FROM conversation_threads WHERE workspace_id = %s", (home_id,))
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (home_id,))


async def test_home_and_its_stand_in_are_told_apart_by_their_ids_in_sql(test_db_pool):
    from src.server.database.home_workspace import (
        get_home_stand_in_id,
        get_home_workspace_id,
        home_id_sql,
        stand_in_id_sql,
    )

    users = ["user-1", f"away-test-{uuid.uuid4().hex}", "ünïcode@example.com"]
    async with test_db_pool.connection() as conn:
        for user_id in users:
            cur = await conn.execute(
                f"SELECT {home_id_sql('%(u)s')}::text AS home_id,"
                f" {stand_in_id_sql('%(u)s')}::text AS stand_in_id",
                {"u": user_id},
            )
            row = await cur.fetchone()
            assert row["home_id"] == get_home_workspace_id(user_id)
            assert row["stand_in_id"] == get_home_stand_in_id(user_id)


async def test_home_work_waits_while_owed_files_keep_the_stand_in_on_a_failed_computer(
    machine, test_db_pool
):
    """A turn keeps the stand-in on its computer while files Home is owed are
    there, however that computer fares, and runs nowhere while it has failed.
    Parked Home work is released onto the same computer a turn would run on,
    so it stays parked rather than failing every sweep. A hand-back that gives
    up leaves the files there, so it stays until that computer runs again."""
    from src.server.database import home_away
    from src.server.database.computer import create_computer
    from src.server.database.conversation import create_thread
    from src.server.database.home_stand_in import (
        place_home_stand_in,
        read_stand_in,
        stand_in_computer,
    )

    home_id = await _home_on_machine(test_db_pool, machine)
    first = await create_computer(machine.user_id, kind="daytona", name="first",
                                  status="running", provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
    second = await create_computer(machine.user_id, kind="daytona", name="second",
                                   status="running", provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
    first_id = str(first["computer_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET last_activity_at = NOW() WHERE computer_id = %s", (first_id,)
        )
    assert str((await place_home_stand_in(machine.user_id)).computer["computer_id"]) == first_id
    await home_away.mark_away(machine.user_id)
    automation_id, execution_id = await _parked_firing(test_db_pool, machine)
    home_thread = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=home_thread, workspace_id=home_id, current_status="completed"
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automations SET workspace_id = %s WHERE automation_id = %s",
            (home_id, automation_id),
        )
        await conn.execute(
            "UPDATE computers SET status = 'error' WHERE computer_id = %s", (first_id,)
        )
    home_wake = await _parked(test_db_pool, machine, "wakeup_fire", thread_id=home_thread)
    try:
        stand_in = await read_stand_in(machine.user_id)
        assert (stand_in.kept, stand_in.runs) == (True, False)
        assert await stand_in_computer(machine.user_id) is None
        assert await _parked_verdict(execution_id) is None
        assert await ex.release_parked_execution(execution_id) is False
        assert home_wake not in {
            str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()
        }

        # Given up, the files are no longer owed but are still only there.
        await home_away.fail_handback(
            machine.user_id, max_attempts=1, given_up={"failed": True, "cloud_computer_id": first_id}
        )
        assert await stand_in_computer(machine.user_id) is None
        assert await _parked_verdict(execution_id) is None

        # Its computer running again, work goes there, not to the other.
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "UPDATE computers SET status = 'running' WHERE computer_id = %s", (first_id,)
            )
        assert str((await stand_in_computer(machine.user_id))["computer_id"]) == first_id
        assert str((await place_home_stand_in(machine.user_id)).computer["computer_id"]) != str(
            second["computer_id"]
        )
        assert (await _parked_verdict(execution_id))["stand_in"] is True
        assert home_wake in {
            str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()
        }
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM hook_outbox WHERE conversation_thread_id = %s", (home_thread,))
            await conn.execute("DELETE FROM conversation_threads WHERE workspace_id = %s", (home_id,))
            await conn.execute("DELETE FROM home_away WHERE user_id = %s", (machine.user_id,))
            await conn.execute("DELETE FROM automations WHERE user_id = %s", (machine.user_id,))
            await conn.execute("DELETE FROM workspaces WHERE user_id = %s", (machine.user_id,))


async def test_a_parked_firing_a_later_one_took_over_is_never_released(machine, test_db_pool):
    automation_id, execution_id = await _parked_firing(
        test_db_pool, machine, gives_up_in=-timedelta(hours=1)
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
    assert (await _parked_verdict(execution_id))["expired"] is True
    await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )

    assert (await _parked_verdict(execution_id))["superseded"] is True
    assert await ex.release_parked_execution(execution_id) is False


async def test_a_tunnel_reads_down_only_under_a_settled_gateway(machine, test_db_pool):
    """The gateway's outage or restart is the service's, never every user's
    machine gone at once: it fails dials as the gateway's own."""
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is True
    assert (await tunnels_db.get_tunnel(await _tunnel_id(test_db_pool, machine)))["state"] == "down"

    # Its heartbeat stopped: it died, or lost its database.
    await _gateway(test_db_pool, started=timedelta(hours=1), seen=timedelta(minutes=4))
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False
    # It just started: its machines are still connecting back.
    await _gateway(test_db_pool, started=timedelta(minutes=1), seen=timedelta(seconds=1))
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False
    tunnel = await tunnels_db.get_tunnel(await _tunnel_id(test_db_pool, machine))
    assert tunnel["state"] == "unsettled"
    # No gateway ever ran on this build.
    async with test_db_pool.connection() as conn:
        await conn.execute("DELETE FROM ssh_gateway_state")
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False

    # A parked firing is let go to dial, and fails as the gateway's outage.
    _, execution_id = await _parked_firing(test_db_pool, machine)
    assert (await _parked_verdict(execution_id))["reachable"] is True


async def test_a_gateways_start_and_stop_leave_no_tunnel_judged(machine, test_db_pool):
    tunnel_id = await _tunnel_id(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL WHERE tunnel_id = %s",
            (tunnel_id,),
        )
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False

    await tunnels_db.gateway_started()
    # Marked down with the start, but not judged until machines reconnect.
    assert not (await tunnels_db.get_tunnel(tunnel_id))["live"]
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False

    await _gateway(test_db_pool, started=timedelta(hours=1), seen=timedelta(minutes=2))
    assert await tunnels_db.gateway_beat([tunnel_id, str(uuid.uuid4())]) == {tunnel_id}
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is True

    await tunnels_db.gateway_stopped()
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False


async def _tunnel_id(pool, machine) -> str:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tunnel_id::text AS tunnel_id FROM ssh_tunnels WHERE user_id = %s",
            (machine.user_id,),
        )
        return (await cur.fetchone())["tunnel_id"]


async def test_a_firing_parked_for_a_machine_its_workspace_left_is_released(
    machine, test_db_pool
):
    from src.server.database.computer import create_computer

    automation_id, execution_id = await _parked_firing(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automations SET workspace_id = %s WHERE automation_id = %s",
            (machine.workspace_id, automation_id),
        )
        await conn.execute(
            "UPDATE workspaces SET computer_id = %s WHERE workspace_id = %s",
            (machine.computer_id, machine.workspace_id),
        )
    assert await _parked_verdict(execution_id) is None

    cloud = await create_computer(machine.user_id, kind="daytona", name="cloud", status="running")
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET computer_id = %s WHERE workspace_id = %s",
            (cloud["computer_id"], machine.workspace_id),
        )
    assert (await _parked_verdict(execution_id))["reachable"] is True
    assert await ex.release_parked_execution(execution_id) is True


async def test_a_firing_parked_for_its_machine_is_not_ahead_in_the_thread_line(
    machine, test_db_pool
):
    automation_id, parked_id = await _parked_firing(test_db_pool, machine)
    later = await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )
    later_id = str(later)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET status = 'waiting', wait_reason = 'thread_busy'"
            " WHERE automation_execution_id = %s",
            (later_id,),
        )
    assert await ex.has_earlier_waiting_execution(automation_id, later_id) is False
    assert (await _parked_verdict(parked_id))["superseded"] is True


async def test_a_firing_parked_again_keeps_the_deadline_its_first_park_set(
    machine, test_db_pool
):
    """A machine that comes back only to drop again does not start the wait
    over: the deadline is the first park's, and so is the heartbeat."""
    automation_id = await _automation(machine)
    execution_id = await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )

    async def park(wait_seconds: int) -> dict:
        assert await ex.park_execution(
            execution_id, awaits_computer=machine.computer_id, parked_status="active",
            wait_seconds=wait_seconds,
        )
        async with test_db_pool.connection() as conn:
            cur = await conn.execute(
                "SELECT awaits_until, heartbeat_at, status, wait_reason"
                " FROM automation_executions WHERE automation_execution_id = %s",
                (execution_id,),
            )
            return dict(await cur.fetchone())

    first = await park(60 * 60)
    assert first["heartbeat_at"] == first["awaits_until"]
    assert (first["status"], first["wait_reason"]) == ("waiting", "computer_unreachable")
    assert first["awaits_until"] - datetime.now(timezone.utc) > timedelta(minutes=55)

    # Back for a moment: released, started, and parked again on the drop.
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
    assert await ex.release_parked_execution(execution_id) is True
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
    again = await park(7 * 24 * 60 * 60)
    assert again["awaits_until"] == first["awaits_until"]
    assert again["heartbeat_at"] == first["awaits_until"]
    # A firing still parked is not shown to the sweep until its deadline passes.
    assert await _parked_verdict(execution_id) is None

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET awaits_until = NOW() - interval '1 minute'"
            " WHERE automation_execution_id = %s",
            (execution_id,),
        )
    assert (await _parked_verdict(execution_id))["expired"] is True


async def test_a_parked_firing_past_its_deadline_is_not_released(machine, test_db_pool):
    """A deadline that passed after the sweep read the row: the release
    misses, and the firing stays parked for the next sweep to skip."""
    automation_id = await _automation(machine)
    execution_id = await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )
    assert await ex.park_execution(
        execution_id, awaits_computer=machine.computer_id, parked_status="active",
        wait_seconds=60 * 60,
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
        await conn.execute(
            "UPDATE automation_executions SET awaits_until = NOW() - interval '1 second'"
            " WHERE automation_execution_id = %s",
            (execution_id,),
        )
    assert await ex.release_parked_execution(execution_id) is False
    verdict = await _parked_verdict(execution_id)
    assert (verdict["expired"], verdict["reachable"]) == (True, True)


async def test_a_keep_alive_touch_leaves_a_parked_firings_heartbeat(machine, test_db_pool):
    """A touch the firing's keep-alive sent as it parked lands after the park
    and must not date the heartbeat back to now, where a build that predates
    parking would take the row for abandoned."""
    automation_id = await _automation(machine)
    execution_id = await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )
    assert await ex.park_execution(
        execution_id, awaits_computer=machine.computer_id, parked_status="active",
        wait_seconds=60 * 60,
    )
    await ex.touch_execution(execution_id)
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT heartbeat_at, awaits_until FROM automation_executions"
            " WHERE automation_execution_id = %s",
            (execution_id,),
        )
        row = await cur.fetchone()
    assert row["heartbeat_at"] == row["awaits_until"]


async def test_a_firing_that_moved_on_is_not_parked(machine, test_db_pool):
    automation_id = await _automation(machine)
    execution_id = await ex.create_execution(
        automation_id=automation_id, scheduled_at=datetime.now(timezone.utc), server_id="test"
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET status = 'skipped'"
            " WHERE automation_execution_id = %s",
            (execution_id,),
        )
    assert not await ex.park_execution(
        execution_id, awaits_computer=machine.computer_id, parked_status="active",
        wait_seconds=60,
    )


async def test_a_tunnel_seen_live_reads_unsettled_while_a_new_gateway_settles(machine, test_db_pool):
    """A dial this gateway cannot open while it settles is its own start, even
    to a machine the row still shows connected, never the machine away."""
    tunnel_id = await _tunnel_id(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE tunnel_id = %s",
            (tunnel_id,),
        )
    await _gateway(test_db_pool, started=timedelta(minutes=1), seen=timedelta(seconds=1))

    tunnel = await tunnels_db.get_tunnel(tunnel_id)
    assert (tunnel["live"], tunnel["state"]) == (True, "unsettled")
    assert await tunnels_db.gateway_settled() is False


async def test_a_rollback_hands_parked_firings_to_the_previous_builds_sweep(
    machine, test_db_pool
):
    """A parked firing's heartbeat is dated to the end of its wait, which the
    previous build's sweep of abandoned firings would wait out for up to a
    week. Migration 129's rollback release dates it to now, and leaves any
    other firing as it was."""
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[2] / "migrations/versions/129_automation_execution_awaits_computer.py"
    spec = importlib.util.spec_from_file_location("migration_129", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    _, parked_id = await _parked_firing(test_db_pool, machine, gives_up_in=timedelta(days=7))
    other_id = await ex.create_execution(
        automation_id=await _automation(machine, "Other"),
        scheduled_at=datetime.now(timezone.utc), server_id="test",
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET status = 'waiting', wait_reason = 'thread_busy',"
            " heartbeat_at = NOW() + INTERVAL '1 hour' WHERE automation_execution_id = %s",
            (other_id,),
        )
        await conn.execute(migration._RELEASE_PARKED)
        cur = await conn.execute(
            "SELECT automation_execution_id::text AS id, heartbeat_at <= NOW() AS due"
            " FROM automation_executions WHERE automation_execution_id = ANY(%s::uuid[])",
            ([parked_id, other_id],),
        )
        due = {row["id"]: row["due"] for row in await cur.fetchall()}

    assert due == {parked_id: True, other_id: False}


async def test_the_parked_sweeps_skip_spares_a_firing_released_and_waiting_since(
    machine, test_db_pool
):
    """The sweep reads a firing as expired, then another worker releases it
    and its start waits for a busy thread. The sweep's skip, settled from
    its earlier read, finds it no longer parked and leaves it."""
    automation_id, execution_id = await _parked_firing(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET wait_reason = 'thread_busy'"
            " WHERE automation_execution_id = %s",
            (execution_id,),
        )

    def skip(parked: bool):
        return ex.settle_execution(
            execution_id, automation_id=automation_id, from_statuses=("waiting",),
            to="skipped", parked=parked, skip_reason="computer_unreachable",
        )

    assert await skip(parked=True) is None
    assert await skip(parked=False) is not None


async def test_a_wait_from_before_reasons_were_kept_lists_as_a_thread_wait(
    machine, test_db_pool
):
    """A firing already waiting for its thread when the reason column came
    has none, and still shows among the thread's waits, apart from one
    waiting for its computer."""
    _, computer_wait = await _parked_firing(test_db_pool, machine)
    legacy = await ex.create_execution(
        automation_id=await _automation(machine, "Legacy"),
        scheduled_at=datetime.now(timezone.utc), server_id="test",
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE automation_executions SET status = 'waiting', wait_reason = NULL,"
            " conversation_thread_id = %s WHERE automation_execution_id = %s",
            (machine.thread_id, legacy),
        )

    rows, _ = await ex.list_executions(
        machine.user_id, thread_id=machine.thread_id, status="waiting",
        wait_reason="thread_busy",
    )

    listed = {str(r["automation_execution_id"]) for r in rows}
    assert legacy in listed
    assert computer_wait not in listed


async def test_a_tunnel_its_machine_was_paired_again_past_is_not_down(machine, test_db_pool):
    """The machine answers on its new pairing, not this tunnel: nothing waits
    for it, and a dial says to reconnect rather than that it is asleep."""
    tunnel_id = await _tunnel_id(test_db_pool, machine)
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is True

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET superseded_at = NOW() WHERE tunnel_id = %s", (tunnel_id,)
        )

    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False


async def test_a_parked_firing_stays_parked_while_a_new_gateway_settles(
    machine, test_db_pool
):
    """A tunnel a previous gateway saw live is not known to be up on a new
    one: a firing let go then would dial a machine that may still be asleep
    and be skipped for it."""
    _, execution_id = await _parked_firing(test_db_pool, machine)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE ssh_tunnels SET disconnected_at = NULL, seen_at = NOW() WHERE user_id = %s",
            (machine.user_id,),
        )
    await _gateway(test_db_pool, started=timedelta(minutes=1), seen=timedelta(seconds=1))

    assert await ex.release_parked_execution(execution_id) is False

    await _gateway(test_db_pool, started=timedelta(hours=1), seen=timedelta(seconds=1))
    assert await ex.release_parked_execution(execution_id) is True


async def test_the_stand_ins_computer_lists_homes_threads_at_the_stand_ins_folder(
    machine, test_db_pool
):
    """The stand-in's folder serves Home's transcripts, so its computer's
    thread index lists Home's threads there, where the agent finds them."""
    from src.server.database.computer import create_computer
    from src.server.database.conversation import create_thread
    from src.server.database.conversation.threads_read import list_computer_threads
    from src.server.database.home_stand_in import place_home_stand_in

    home_id = await _home_on_machine(test_db_pool, machine)
    cloud = await create_computer(machine.user_id, kind="daytona", name="cloud",
                                  status="running", provider_ref=f"sb-{uuid.uuid4().hex[:8]}")
    cloud_id = str(cloud["computer_id"])
    home_thread = str(uuid.uuid4())
    try:
        await create_thread(
            conversation_thread_id=home_thread, workspace_id=home_id, current_status="completed"
        )
        before, rows = await list_computer_threads(cloud_id)
        assert rows == []

        stand_in = await place_home_stand_in(machine.user_id)
        assert str(stand_in.computer["computer_id"]) == cloud_id
        after, rows = await list_computer_threads(cloud_id)

        assert after != before
        [row] = rows
        assert str(row["conversation_thread_id"]) == home_thread
        assert str(row["workspace_id"]) == home_id
        assert row["dir_name"] == stand_in.row["dir_name"]
        # Home's own computer lists it once, at Home's folder.
        _, on_machine = await list_computer_threads(machine.computer_id)
        assert [str(r["conversation_thread_id"]) for r in on_machine].count(home_thread) == 1
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM conversation_threads WHERE workspace_id = %s", (home_id,))
            await conn.execute("DELETE FROM workspaces WHERE user_id = %s", (machine.user_id,))


async def _run(pool, thread_id: str, turn_index: int, metadata: dict) -> str:
    run_id = str(uuid.uuid4())
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO conversation_responses (conversation_response_id,"
            " conversation_thread_id, turn_index, status, metadata, created_at)"
            " VALUES (%s, %s, %s, 'in_progress', %s::jsonb, NOW())",
            (run_id, thread_id, turn_index, json.dumps(metadata)),
        )
        await conn.execute(
            "UPDATE conversation_responses SET status = 'cancelled' WHERE conversation_response_id = %s",
            (run_id,),
        )
    return run_id


async def _dispatched(pool, thread_id: str, hook_type: str, payload: dict) -> None:
    """A job that posted its turn and is done, as the report-back executor leaves it."""
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO hook_outbox (conversation_thread_id, hook_type, payload,"
            " ordering_key, idempotency_key, status, completed_at)"
            " VALUES (%s, %s, %s::jsonb, %s, %s, 'done', NOW())",
            (thread_id, hook_type, json.dumps(payload), f"{hook_type}:{thread_id}", uuid.uuid4().hex),
        )


async def test_a_restarts_resume_past_its_ttl_is_released_with_the_machine_away(
    machine, test_db_pool
):
    """Its executor declines it then, so its stop settles and says why now,
    not once the machine is back or a week on. One within its TTL waits."""
    await machine.seen(timedelta(hours=2))
    stale = await _parked(
        test_db_pool, machine, RESTART_RESUME_HOOK,
        payload={"run_id": str(uuid.uuid4())}, ordering_key=f"stale:{machine.thread_id}",
    )
    fresh = await _parked(
        test_db_pool, machine, RESTART_RESUME_HOOK,
        payload={"run_id": str(uuid.uuid4())}, ordering_key=f"fresh:{machine.thread_id}",
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE hook_outbox SET created_at = NOW() - make_interval(secs => %s)"
            " WHERE hook_outbox_id = %s",
            (RESTART_RESUME_TTL.total_seconds() + 60, stale),
        )

    released = {str(r["hook_outbox_id"]) for r in await ob.release_for_reachable_computers()}

    assert stale in released and fresh not in released
    assert (await _job(test_db_pool, stale))["waiting"] is False
    assert (await _job(test_db_pool, fresh))["waiting"] is True


async def test_a_machine_gone_a_week_leaves_a_restarts_resume_to_its_executor(
    machine, test_db_pool
):
    """Closed here, its stop would settle outside the executor and its retries."""
    await machine.seen(timedelta(days=8))
    job = await _parked(test_db_pool, machine, RESTART_RESUME_HOOK, payload={"run_id": str(uuid.uuid4())})

    assert job not in {str(r["hook_outbox_id"]) for r in await ob.close_for_lost_computers()}
    assert (await _job(test_db_pool, job))["status"] == "pending"


async def test_a_run_resumes_a_lost_computer_only_when_that_resume_posted_it(
    machine, test_db_pool
):
    """The resume cap tells a lost computer's resume from any other turn that
    loses the computer after it: a report-back's, or the resume of a deploy."""
    lost = {RESTART_INTERRUPTED_META: True, RESTART_REASON_META: COMPUTER_LOST_REASON}
    first = await _run(test_db_pool, machine.thread_id, 0, {RESTART_REASON_META: COMPUTER_LOST_REASON})
    resumed = await _run(test_db_pool, machine.thread_id, 1, lost)
    reported = await _run(test_db_pool, machine.thread_id, 2, lost)
    deployed = await _run(test_db_pool, machine.thread_id, 3, {})
    after_deploy = await _run(test_db_pool, machine.thread_id, 4, lost)
    await _dispatched(test_db_pool, machine.thread_id, RESTART_RESUME_HOOK, {"run_id": first, "dispatched_run_id": resumed})
    await _dispatched(test_db_pool, machine.thread_id, "task_report_back", {"run_id": first, "dispatched_run_id": reported})
    await _dispatched(test_db_pool, machine.thread_id, RESTART_RESUME_HOOK, {"run_id": deployed, "dispatched_run_id": after_deploy})

    async def resumes(run_id: str) -> bool:
        return (await rr_db.read_resume_state(machine.thread_id, run_id))["resumes_lost_computer"]

    assert await resumes(resumed) is True
    assert await resumes(reported) is False
    assert await resumes(after_deploy) is False
