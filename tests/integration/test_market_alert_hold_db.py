"""A market-target alert whose wake cannot be admitted, against real Postgres.

The alert is held for its thread's next turn instead of dying: every way out
of a claim at the attempts ceiling parks it, the next turn takes it in, and a
turn ending without it buys one more look, never a loop. Dead alerts still
owed come back due, at most twice, and alerts no turn will take in are
closed. The claim gate, the park sweep, the finalize release and both sweeps
decide this in SQL, so only a real database shows each row ends where it
should.
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

# A waiting run's checker asks for the market alert by this sender.
_MARKET_ALERT = {"market_alert": "market_target"}


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


async def _new_thread(workspace) -> SimpleNamespace:
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    return SimpleNamespace(id=thread_id, user_id=workspace["user_id"])


@pytest_asyncio.fixture(loop_scope="session")
async def thread(seed_workspace, patched_get_db_connection, monkeypatch):
    made = await _new_thread(seed_workspace)
    _app_rows(monkeypatch)
    made.workspace = seed_workspace
    return made


async def _arm(thread, symbol: str, *, warn_in: timedelta, expires_in: timedelta) -> str:
    from src.server.database.market_targets import arm_target

    now = datetime.now(timezone.utc)
    row = await arm_target(
        thread_id=thread.id, user_id=thread.user_id, symbol=symbol, market="stock",
        kind="above", value=10.0, pct_from=None, armed_price=None,
        expires_at=now + expires_in, warn_at=now + warn_in, max_armed=10,
    )
    return str(row["target_id"])


async def _job_id(db_conn, target_id: str, notice: str) -> str:
    from src.server.database.market_targets import idempotency_key

    cur = await db_conn.execute(
        "SELECT hook_outbox_id::text AS id FROM hook_outbox WHERE idempotency_key = %s",
        (idempotency_key(target_id, notice),),
    )
    return (await cur.fetchone())["id"]


async def _hit(thread, db_conn, symbol: str) -> SimpleNamespace:
    from src.server.database.market_targets import fire_targets

    target = await _arm(
        thread, symbol, warn_in=timedelta(hours=5), expires_in=timedelta(hours=6)
    )
    assert [str(r["target_id"]) for r in await fire_targets([target], 11.0)] == [target]
    return SimpleNamespace(target=target, job=await _job_id(db_conn, target, "hit"))


async def _reminder(thread, db_conn, symbol: str) -> SimpleNamespace:
    from src.server.database.market_targets import claim_due_warnings

    target = await _arm(
        thread, symbol, warn_in=timedelta(minutes=-1), expires_in=timedelta(minutes=30)
    )
    assert target in {str(r["target_id"]) for r in await claim_due_warnings()}
    return SimpleNamespace(target=target, job=await _job_id(db_conn, target, "expiring"))


async def _row(db_conn, job_id: str) -> dict | None:
    """The row's lifecycle, ``next_retry_at`` read as booleans: psycopg cannot
    load the ``infinity`` a parked row carries."""
    cur = await db_conn.execute(
        """
        SELECT status, payload, attempts, completed_at,
               COALESCE(next_retry_at = 'infinity'::timestamptz, false) AS parked,
               COALESCE(next_retry_at <> 'infinity'::timestamptz
                        AND next_retry_at <= NOW(), false) AS due
        FROM hook_outbox WHERE hook_outbox_id = %s
        """,
        (job_id,),
    )
    return await cur.fetchone()


def _held(row) -> bool:
    return (
        row["status"] == "pending"
        and row["parked"]
        and row["payload"].get("exhausted") is True
        and "steered_run_id" not in row["payload"]
    )


async def _claim() -> dict:
    from src.server.database.runs import outbox

    return {
        str(r["hook_outbox_id"]): r["attempts"]
        for r in await outbox.claim_outbox_jobs(limit=50, max_attempts=MAX)
    }


async def _claim_at_ceiling(db_conn, job_id: str) -> None:
    await db_conn.execute(
        "UPDATE hook_outbox SET attempts = %s WHERE hook_outbox_id = %s", (MAX - 1, job_id)
    )
    assert (await _claim())[job_id] == MAX


async def _hold(db_conn, job_id: str) -> None:
    """Out of attempts and nacked, as the drainer leaves a wake refused
    through its whole retry budget."""
    from src.server.database.runs import outbox

    await _claim_at_ceiling(db_conn, job_id)
    assert await outbox.nack_outbox_job(job_id, attempts=MAX, max_attempts=MAX) == "exhausted"
    assert _held(await _row(db_conn, job_id))


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


def _ids(rows) -> list:
    return [str(r["hook_outbox_id"]) for r in rows]


# ---------------------------------------------------------------------------
# Entering the hold
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("way", ["nack", "defer", "park"])
async def test_every_way_out_of_a_claim_at_the_ceiling_holds_the_alert(
    thread, db_conn, way
):
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    await _claim_at_ceiling(db_conn, hit.job)

    if way == "nack":
        assert await outbox.nack_outbox_job(hit.job, attempts=MAX, max_attempts=MAX) == "exhausted"
    elif way == "defer":
        assert await outbox.defer_claimed_job(hit.job, attempts=MAX, max_attempts=MAX) == "exhausted"
    else:
        # A worker lost mid-claim never nacks; its lease runs out.
        await db_conn.execute(
            "UPDATE hook_outbox SET lease_expires_at = NOW() - interval '1 second'"
            " WHERE hook_outbox_id = %s",
            (hit.job,),
        )
        parked = await outbox.park_exhausted_jobs(max_attempts=MAX)
        assert [(str(r["hook_outbox_id"]), r["status"]) for r in parked] == [
            (hit.job, "pending")
        ]

    row = await _row(db_conn, hit.job)
    assert _held(row)
    assert row["attempts"] == MAX
    # Not handed straight back to the executor that gave up on it, and the
    # park sweep leaves it be.
    assert hit.job not in await _claim()
    assert await outbox.park_exhausted_jobs(max_attempts=MAX) == []
    assert _held(await _row(db_conn, hit.job))


async def test_a_lasting_refusal_holds_the_claimed_alert_at_once(thread, db_conn):
    """The executor against the real ledger, only its POST stubbed: a refusal
    that will not clear holds the job on its first attempt."""
    from src.server.services.market_targets.delivery import execute_market_target

    hit = await _hit(thread, db_conn, "AAA")
    claimed = await _claim()
    assert claimed[hit.job] == 1
    cur = await db_conn.execute(
        "SELECT * FROM hook_outbox WHERE hook_outbox_id = %s", (hit.job,)
    )
    job = await cur.fetchone()

    post = AsyncMock(return_value=("refused", None))
    with (
        patch("src.server.services.report_back.notify_turn.post_notification_turn", new=post),
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=True),
        ),
    ):
        await execute_market_target(job)

    assert post.await_args.kwargs["refusal_fails_fast"] is True
    row = await _row(db_conn, hit.job)
    assert _held(row)
    assert row["attempts"] == 1


async def test_a_hold_whose_lease_was_reclaimed_takes_nothing(thread, db_conn):
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    assert (await _claim())[hit.job] == 1

    assert await outbox.exhaust_alert(hit.job, attempts=0) is False
    assert (await _row(db_conn, hit.job))["status"] == "claimed"
    assert await outbox.exhaust_alert(hit.job, attempts=1) is True


# ---------------------------------------------------------------------------
# The next turn, and a turn that ends without it
# ---------------------------------------------------------------------------


async def test_a_held_alert_blocks_no_later_wake_and_the_next_turn_takes_it_in(
    thread, db_conn
):
    from src.server.database.runs import outbox

    first = await _hit(thread, db_conn, "AAA")
    await _hold(db_conn, first.job)
    later = await _hit(thread, db_conn, "BBB")

    # The later notice's wake is claimable past the held one; its turn would
    # take the held one in too.
    claimed = await _claim()
    assert later.job in claimed
    assert first.job not in claimed

    run_id = await _run(thread)
    assert await outbox.steerable_alert_senders(thread.id, _MARKET_ALERT) == {"market_alert"}
    assert first.job in _ids(await outbox.list_steerable_report_jobs(thread.id))
    assert await outbox.hold_steered_report_jobs([first.job], run_id=run_id) == [first.job]
    assert await outbox.confirm_steered_report_jobs([first.job], run_id=run_id) == [first.job]
    assert (await _row(db_conn, first.job))["status"] == "done"


async def test_a_waiting_run_wakes_only_for_an_alert_the_drainer_cannot_claim(
    thread, db_conn
):
    """A due alert may be claimed between the check and the model call the
    wake trigger leads to, which would then carry no alert. Parked behind
    the live turn, it stays for that call."""
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    assert await outbox.steerable_alert_senders(thread.id, _MARKET_ALERT) == set()

    await _defer(db_conn, hit.job)
    assert await outbox.steerable_alert_senders(thread.id, _MARKET_ALERT) == {"market_alert"}


async def test_a_turn_ending_without_it_buys_one_more_look_not_a_loop(thread, db_conn):
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    await _hold(db_conn, hit.job)

    # A turn that took it in and ended unconfirmed gives it back with its
    # hold dropped; it stays held for good.
    run_id = await _run(thread)
    assert await outbox.hold_steered_report_jobs([hit.job], run_id=run_id) == [hit.job]
    await _end(thread, run_id, "error")
    row = await _row(db_conn, hit.job)
    assert row["status"] == "pending" and row["due"] and not row["parked"]
    assert row["payload"]["exhausted"] is True
    assert "steered_run_id" not in row["payload"]

    # One more look, past the ceiling, since a pending row has none ...
    assert (await _claim())[hit.job] == MAX + 1
    # ... and refused again, held again, with nothing due until another turn
    # ends.
    assert await outbox.nack_outbox_job(hit.job, attempts=MAX + 1, max_attempts=MAX) == "exhausted"
    assert _held(await _row(db_conn, hit.job))
    assert hit.job not in await _claim()

    # A turn running when it looks parks it at the ceiling, held as before.
    await _end(thread, await _run(thread), "completed")
    assert (await _claim())[hit.job] == MAX + 2
    assert await outbox.defer_claimed_job(hit.job, attempts=MAX + 2, max_attempts=MAX) == "exhausted"
    assert _held(await _row(db_conn, hit.job))
    assert hit.job not in await _claim()


async def test_a_turn_that_ended_during_the_claim_releases_the_hold_once(
    thread, db_conn
):
    """The executor against the real ledger: a turn that ran and ended while
    the wake was being posted found the job claimed, so its finalize released
    nothing for it. The hold that follows is released at once, and a later
    claim that met no new ending leaves it held."""
    from src.server.services.market_targets.delivery import execute_market_target

    hit = await _hit(thread, db_conn, "AAA")

    async def _job():
        cur = await db_conn.execute(
            "SELECT * FROM hook_outbox WHERE hook_outbox_id = %s", (hit.job,)
        )
        return await cur.fetchone()

    async def _a_turn_ran_then_refused(**kwargs):
        await _end(thread, await _run(thread), "completed")
        return ("refused", None)

    enabled = patch(
        "src.server.services.features.user_feature_enabled",
        new=AsyncMock(return_value=True),
    )
    assert (await _claim())[hit.job] == 1
    with (
        enabled,
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=AsyncMock(side_effect=_a_turn_ran_then_refused),
        ),
    ):
        await execute_market_target(await _job())

    row = await _row(db_conn, hit.job)
    assert _due(row)
    assert row["payload"]["exhausted"] is True

    assert (await _claim())[hit.job] == 2
    with (
        enabled,
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=AsyncMock(return_value=("refused", None)),
        ),
    ):
        await execute_market_target(await _job())

    assert _held(await _row(db_conn, hit.job))
    assert hit.job not in await _claim()


async def test_a_held_alert_whose_wake_was_admitted_is_stamped_to_it_and_closed(
    thread, db_conn
):
    """Its wake was admitted after all (a POST whose answer was lost): that
    run is the notice's own turn, so the live inbox stamps the notice to it
    and closes the job rather than offering it to every turn and leaving a
    waiting run to wait on it."""
    from src.server.database.market_targets import get_target
    from src.server.database.runs import outbox
    from src.server.database.runs.lifecycle import start_run
    from src.server.services.market_targets.delivery import request_key
    from src.server.services.report_back.steer import SteeredReportInbox

    hit = await _hit(thread, db_conn, "AAA")
    await _hold(db_conn, hit.job)
    wake = str(uuid.uuid4())
    await start_run(
        run_id=wake,
        thread_id=thread.id,
        request_key=request_key(hit.job),
        metadata={"msg_type": "ptc", "user_id": thread.user_id},
    )
    assert await outbox.steerable_alert_senders(thread.id, _MARKET_ALERT) == {"market_alert"}

    cleared = AsyncMock()
    with (
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.report_back.subagent.publish_cleared_wake",
            new=cleared,
        ),
    ):
        assert await SteeredReportInbox().take(thread.id, wake, kinds=("report",)) == []

    row = await _row(db_conn, hit.job)
    assert row["status"] == "done"
    target = await get_target(hit.target)
    assert target["hit_delivered_at"] is not None
    assert str(target["hit_delivered_run_id"]) == wake
    assert await outbox.steerable_alert_senders(thread.id, _MARKET_ALERT) == set()
    cleared.assert_awaited_once_with(thread.id)


# ---------------------------------------------------------------------------
# The revive sweep
# ---------------------------------------------------------------------------


async def _kill(db_conn, job_id: str, *, ago: timedelta = timedelta(minutes=11)) -> None:
    """Dead at the ceiling ``ago``, as a build without the hold, or without a
    ``market_target`` executor, leaves it."""
    await db_conn.execute(
        """
        UPDATE hook_outbox
        SET status = 'dead', attempts = GREATEST(attempts, %s),
            lease_expires_at = NULL, next_retry_at = NOW(),
            completed_at = NOW() - %s
        WHERE hook_outbox_id = %s
        """,
        (MAX, ago, job_id),
    )


def _due(row) -> bool:
    return row["status"] == "pending" and row["due"] and not row["parked"]


async def test_the_revive_sweep_makes_an_owed_dead_alert_due_once_per_death(
    thread, db_conn
):
    from src.server.database.market_targets import cancel_targets, mark_delivered
    from src.server.database.runs import outbox

    owed = await _hit(thread, db_conn, "AAA")
    owed_reminder = await _reminder(thread, db_conn, "BBB")
    delivered = await _hit(thread, db_conn, "CCC")
    await mark_delivered(delivered.target, "hit", None)
    cancelled = await _reminder(thread, db_conn, "DDD")
    await cancel_targets(thread.id, target_id=cancelled.target)
    for job in (owed.job, owed_reminder.job, delivered.job, cancelled.job):
        await _kill(db_conn, job)

    # Every worker runs it at once; each row comes back exactly once.
    first, second = await asyncio.gather(
        outbox.revive_dead_alerts(), outbox.revive_dead_alerts()
    )
    revived = _ids(first) + _ids(second)
    assert sorted(revived) == sorted([owed.job, owed_reminder.job])

    for job in (owed.job, owed_reminder.job):
        row = await _row(db_conn, job)
        # Due for a wake this build never tried, not held for a turn an
        # idle thread may never get.
        assert _due(row)
        assert "exhausted" not in row["payload"]
        assert row["payload"]["revived"] == 1
        assert row["attempts"] == MAX
        assert row["completed_at"] is None
    for job in (delivered.job, cancelled.job):
        assert (await _row(db_conn, job))["status"] == "dead"

    # Its executor claims each in its thread's order, past the ceiling, so
    # one refusal holds it rather than starting a fresh round of retries.
    for job in (owed.job, owed_reminder.job):
        assert (await _claim())[job] == MAX + 1
        assert (
            await outbox.nack_outbox_job(job, attempts=MAX + 1, max_attempts=MAX)
            == "exhausted"
        )
        assert _held(await _row(db_conn, job))


async def test_a_held_alert_revived_keeps_its_exhausted_mark(thread, db_conn):
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    await _hold(db_conn, hit.job)
    await _kill(db_conn, hit.job)

    assert _ids(await outbox.revive_dead_alerts()) == [hit.job]
    row = await _row(db_conn, hit.job)
    assert _due(row)
    assert row["payload"]["exhausted"] is True


async def test_the_revive_sweep_waits_out_a_cool_down_and_looks_back_a_day(
    thread, db_conn
):
    """An old color still draining may kill a row again within minutes; one
    dead past the window is not read at all."""
    from src.server.database.runs import outbox

    fresh = await _hit(thread, db_conn, "AAA")
    await _kill(db_conn, fresh.job, ago=timedelta(minutes=5))
    old = await _hit(thread, db_conn, "BBB")
    await _kill(db_conn, old.job, ago=timedelta(hours=25))
    ready = await _hit(thread, db_conn, "CCC")
    await _kill(db_conn, ready.job, ago=timedelta(minutes=10, seconds=5))

    assert _ids(await outbox.revive_dead_alerts()) == [ready.job]
    assert (await _row(db_conn, fresh.job))["status"] == "dead"
    assert (await _row(db_conn, old.job))["status"] == "dead"

    # Its cool-down over, the fresh one comes back too.
    await _kill(db_conn, fresh.job)
    assert _ids(await outbox.revive_dead_alerts()) == [fresh.job]


async def test_an_alert_killed_again_comes_back_at_most_twice(thread, db_conn):
    from src.server.database.runs import outbox

    hit = await _hit(thread, db_conn, "AAA")
    for revival in (1, 2):
        await _kill(db_conn, hit.job)
        assert _ids(await outbox.revive_dead_alerts()) == [hit.job]
        assert (await _row(db_conn, hit.job))["payload"]["revived"] == revival

    await _kill(db_conn, hit.job)
    assert await outbox.revive_dead_alerts() == []
    row = await _row(db_conn, hit.job)
    assert row["status"] == "dead"
    assert row["payload"]["revived"] == 2


async def test_a_revival_count_of_another_type_does_not_fail_the_sweep(thread, db_conn):
    from src.server.database.runs import outbox

    odd = await _hit(thread, db_conn, "AAA")
    plain = await _hit(thread, db_conn, "BBB")
    for job in (odd.job, plain.job):
        await _kill(db_conn, job)
    await db_conn.execute(
        "UPDATE hook_outbox SET payload = payload || '{\"revived\": true}'::jsonb"
        " WHERE hook_outbox_id = %s",
        (odd.job,),
    )

    assert sorted(_ids(await outbox.revive_dead_alerts())) == sorted([odd.job, plain.job])
    assert (await _row(db_conn, odd.job))["payload"]["revived"] == 1


async def test_the_revive_scan_can_range_on_the_terminal_age_index(thread, db_conn):
    """The plan the sweep's own statement gets with sequential scans off: a
    predicate drifting from the index expression would leave it a seq scan."""
    from src.server.database import pool
    from src.server.database.runs import outbox

    sent = {}

    class _Recording:
        async def execute(self, sql, params=None):
            sent.update(sql=sql, params=params)

        async def fetchall(self):
            return []

    @asynccontextmanager
    async def _recording_connection(conn=None):
        @asynccontextmanager
        async def _cursor(*args, **kwargs):
            yield _Recording()

        yield SimpleNamespace(cursor=_cursor)

    with patch.object(pool, "get_db_connection", _recording_connection):
        assert await outbox.revive_dead_alerts() == []

    async with db_conn.transaction():
        await db_conn.execute("SET LOCAL enable_seqscan = off")
        cur = await db_conn.execute("EXPLAIN " + sent["sql"], sent["params"])
        plan = [r["QUERY PLAN"] for r in await cur.fetchall()]
    assert any("idx_hook_outbox_terminal_age" in line for line in plan), plan


# ---------------------------------------------------------------------------
# Closing alerts no turn will take in
# ---------------------------------------------------------------------------


async def test_the_close_sweep_ends_held_alerts_no_turn_will_take_in(thread, db_conn):
    from src.server.database.market_targets import cancel_targets, fire_targets
    from src.server.database.runs import outbox

    still_owed = await _hit(thread, db_conn, "AAA")
    fired = await _reminder(thread, db_conn, "BBB")
    expired = await _reminder(thread, db_conn, "CCC")
    turn_holds = await _reminder(thread, db_conn, "DDD")
    for job in (still_owed.job, fired.job, expired.job, turn_holds.job):
        await _hold(db_conn, job)

    # Its target hit, so the reminder would only mislead.
    assert len(await fire_targets([fired.target], 11.0)) == 1
    await db_conn.execute(
        "UPDATE market_watch_targets SET expires_at = NOW() - interval '1 second'"
        " WHERE target_id = %s",
        (expired.target,),
    )
    run_id = await _run(thread)
    assert await outbox.hold_steered_report_jobs([turn_holds.job], run_id=run_id) == [
        turn_holds.job
    ]
    await cancel_targets(thread.id, target_id=turn_holds.target)

    first, second = await asyncio.gather(
        outbox.close_unowed_waiting_alerts(), outbox.close_unowed_waiting_alerts()
    )
    closed = _ids(first) + _ids(second)
    assert sorted(closed) == sorted([fired.job, expired.job])
    for job in (fired.job, expired.job):
        row = await _row(db_conn, job)
        assert row["status"] == "done" and row["completed_at"] is not None
    assert _held(await _row(db_conn, still_owed.job))
    # The turn holding one settles it at its confirm or its ending.
    held_by_turn = await _row(db_conn, turn_holds.job)
    assert held_by_turn["status"] == "pending"
    assert held_by_turn["payload"]["steered_run_id"] == run_id


async def test_a_held_alert_on_an_archived_thread_is_closed_and_a_deleted_one_goes(
    thread, db_conn
):
    from src.server.database.runs import outbox

    archived = await _new_thread(thread.workspace)
    archived_hit = await _hit(archived, db_conn, "AAA")
    await _hold(db_conn, archived_hit.job)
    deleted = await _new_thread(thread.workspace)
    deleted_hit = await _hit(deleted, db_conn, "BBB")
    await _hold(db_conn, deleted_hit.job)

    await db_conn.execute(
        "UPDATE conversation_threads SET archived_at = NOW()"
        " WHERE conversation_thread_id = %s",
        (archived.id,),
    )
    assert _ids(await outbox.close_unowed_waiting_alerts()) == [archived_hit.job]
    assert (await _row(db_conn, archived_hit.job))["status"] == "done"

    await db_conn.execute(
        "DELETE FROM conversation_threads WHERE conversation_thread_id = %s",
        (deleted.id,),
    )
    assert await _row(db_conn, deleted_hit.job) is None


async def _defer(db_conn, job_id: str) -> None:
    """Claimed and parked behind a live turn: parked, not held."""
    from src.server.database.runs import outbox

    attempts = (await _claim())[job_id]
    assert (
        await outbox.defer_claimed_job(job_id, attempts=attempts, max_attempts=MAX)
        == "pending"
    )
    row = await _row(db_conn, job_id)
    assert row["status"] == "pending" and row["parked"]
    assert "exhausted" not in row["payload"]


async def test_a_merely_parked_alert_on_an_archived_thread_is_closed_too(
    thread, db_conn
):
    """No turn comes on an archived thread to release one that is parked or
    run one that is due, and its executor would only drop it."""
    from src.server.database.market_targets import fire_targets
    from src.server.database.runs import outbox

    archived = await _new_thread(thread.workspace)
    parked = await _hit(archived, db_conn, "AAA")
    await _defer(db_conn, parked.job)
    # On a live thread, one parked and no longer owed waits for its
    # thread's next ending, whose release lets its executor drop it.
    unowed = await _reminder(thread, db_conn, "BBB")
    await _defer(db_conn, unowed.job)
    assert len(await fire_targets([unowed.target], 11.0)) == 1
    due = await _hit(archived, db_conn, "CCC")

    await db_conn.execute(
        "UPDATE conversation_threads SET archived_at = NOW()"
        " WHERE conversation_thread_id = %s",
        (archived.id,),
    )
    assert sorted(_ids(await outbox.close_unowed_waiting_alerts())) == sorted(
        [parked.job, due.job]
    )
    for job in (parked.job, due.job):
        assert (await _row(db_conn, job))["status"] == "done"
    row = await _row(db_conn, unowed.job)
    assert row["status"] == "pending" and row["parked"]


async def test_every_alert_on_a_deleted_workspace_is_closed_but_one_a_turn_holds(
    thread, db_conn
):
    """A deleted workspace keeps its threads, so nothing cascades to their
    alerts, and no turn will run on one again."""
    from src.server.database.runs import outbox
    from src.server.database.workspace import create_workspace

    workspace = await create_workspace(
        user_id=thread.user_id, name="Deleted Workspace", status="running"
    )
    gone = await _new_thread(workspace)
    held = await _hit(gone, db_conn, "AAA")
    await _hold(db_conn, held.job)
    turn_holds = await _hit(gone, db_conn, "BBB")
    await _hold(db_conn, turn_holds.job)
    run_id = await _run(gone)
    assert await outbox.hold_steered_report_jobs([turn_holds.job], run_id=run_id) == [
        turn_holds.job
    ]
    parked = await _hit(gone, db_conn, "CCC")
    await _defer(db_conn, parked.job)
    live_parked = await _hit(thread, db_conn, "DDD")
    await _defer(db_conn, live_parked.job)
    due = await _hit(gone, db_conn, "EEE")
    live_due = await _hit(thread, db_conn, "FFF")

    await db_conn.execute(
        "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
        (workspace["workspace_id"],),
    )
    first, second = await asyncio.gather(
        outbox.close_unowed_waiting_alerts(), outbox.close_unowed_waiting_alerts()
    )
    assert sorted(_ids(first) + _ids(second)) == sorted([held.job, parked.job, due.job])
    for job in (held.job, parked.job, due.job):
        row = await _row(db_conn, job)
        assert row["status"] == "done" and row["completed_at"] is not None
    # The turn holding one settles it at its confirm or its ending.
    held_by_turn = await _row(db_conn, turn_holds.job)
    assert held_by_turn["status"] == "pending"
    assert held_by_turn["payload"]["steered_run_id"] == run_id
    # The live workspace's alerts are not touched.
    live = await _row(db_conn, live_parked.job)
    assert live["status"] == "pending" and live["parked"]
    assert (await _row(db_conn, live_due.job))["status"] == "pending"
