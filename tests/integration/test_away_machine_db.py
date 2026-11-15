"""Work waiting on a user's own machine that never comes back, against real Postgres.

A command whose machine is away records since when (``away_since``), and any
read of the machine clears it, so the watcher can give up a week on. A
notification parked for a computer (``park_for_computer``) is given up once
the computer's tunnel has gone unseen as long: a report-back closed, an alert
held for the thread's next turn. Both are guarded statements every worker may
run at once, so only a real database shows what they match.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

from src.server.database.runs import command_runs as cr
from src.server.database.runs import outbox as ob

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def machine(patched_get_db_connection, test_db_pool):
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
