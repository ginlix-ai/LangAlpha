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

from src.server.database import automation_executions as ex
from src.server.database import ssh_tunnels as tunnels_db
from src.server.database.runs import command_runs as cr
from src.server.database.runs import outbox as ob

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


async def test_a_commands_away_since_is_kept_while_away_and_cleared_by_a_read(machine):
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


async def _parked_firing(
    pool, machine, *, waited: timedelta = timedelta(minutes=10), name: str = "Brief",
    **trigger,
) -> tuple[str, str]:
    """An automation (a daily schedule unless ``trigger`` says otherwise) and
    a firing of it parked for the machine's computer, as the executor leaves
    one; their ids."""
    from src.server.database.automation import create_automation

    automation = await create_automation(
        user_id=machine.user_id, name=name, instruction="Summarize",
        **(trigger or {"trigger_type": "cron", "cron_expression": "0 9 * * *"}),
    )
    automation_id = str(automation["automation_id"])
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO automation_executions (automation_id, status, scheduled_at,"
            " started_at, heartbeat_at, wait_reason, awaits_computer, parked_status,"
            " conversation_thread_id)"
            " VALUES (%s, 'waiting', NOW(), NOW() - %s, NOW() - interval '1 hour',"
            " 'computer_unreachable', %s, 'active', %s)"
            " RETURNING automation_execution_id::text AS id",
            (automation_id, waited, machine.computer_id, machine.thread_id),
        )
        return automation_id, (await cur.fetchone())["id"]


async def _parked_verdict(execution_id: str) -> dict | None:
    """The sweep's verdict on a parked firing; None when the sweep is not
    shown it, as nothing it could do would change it."""
    rows = [
        r for r in await ex.list_parked_executions(24 * 60 * 60, 7 * 24 * 60 * 60, limit=500)
        if str(r["automation_execution_id"]) == execution_id
    ]
    if not rows:
        return None
    (row,) = rows
    return {k: row[k] for k in ("superseded", "reachable", "expired")}


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


async def test_a_parked_firing_a_later_one_took_over_is_never_released(machine, test_db_pool):
    automation_id, execution_id = await _parked_firing(
        test_db_pool, machine, waited=timedelta(hours=25)
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
    assert (await tunnels_db.get_tunnel(await _tunnel_id(test_db_pool, machine)))["judged"]

    # Its heartbeat stopped: it died, or lost its database.
    await _gateway(test_db_pool, started=timedelta(hours=1), seen=timedelta(minutes=4))
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False
    # It just started: its machines are still connecting back.
    await _gateway(test_db_pool, started=timedelta(minutes=1), seen=timedelta(seconds=1))
    assert await tunnels_db.computer_tunnel_down(machine.computer_id) is False
    assert not (await tunnels_db.get_tunnel(await _tunnel_id(test_db_pool, machine)))["judged"]
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


@pytest.mark.parametrize(
    "trigger",
    [
        {"trigger_type": "once", "next_run_at": datetime.now(timezone.utc)},
        {"trigger_type": "price", "trigger_config": {"symbol": "NVDA"}},
    ],
    ids=["one-time", "one-shot price alert"],
)
async def test_an_automation_that_runs_once_waits_days_not_a_day(machine, test_db_pool, trigger):
    """Nothing later stands in for it, so a late run beats none."""
    _, day = await _parked_firing(
        test_db_pool, machine, waited=timedelta(hours=25), name="Day", **trigger
    )
    _, week = await _parked_firing(
        test_db_pool, machine, waited=timedelta(days=8), name="Week", **trigger
    )

    assert await _parked_verdict(day) is None
    assert (await _parked_verdict(week))["expired"] is True
