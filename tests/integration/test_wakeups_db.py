"""Wakeups against real Postgres.

Which slots fire, which are skipped, who may cancel what, and what the order
backstop catches are decided in SQL (one expression per rule, under row
locks), so only a real database shows each row ends where the contract says.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def thread(patched_get_db_connection, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"wakeups-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    workspace = await create_workspace(
        user_id=user_id, name="Wakeups", status="running"
    )
    workspace_id = str(workspace["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=workspace_id,
        current_status="completed",
    )
    try:
        yield SimpleNamespace(
            id=thread_id,
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
        )
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM hook_outbox WHERE conversation_thread_id = %s",
                (thread_id,),
            )
            await conn.execute(
                "DELETE FROM order_attempts WHERE user_id = %s", (user_id,)
            )
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s",
                (workspace_id,),
            )
            await conn.execute(
                "DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


def _slots(n: int) -> tuple[datetime, ...]:
    """``n`` slots, all already due, a minute apart."""
    start = datetime.now(timezone.utc) - timedelta(minutes=n + 5)
    return tuple(start + timedelta(minutes=i) for i in range(n))


async def _arm(thread, **fields):
    from src.server.database.wakeups.arm import WakeupSpec, arm_wakeups

    armed, left_out = await arm_wakeups(
        thread.id, thread.user_id, [WakeupSpec(**fields)]
    )
    assert not left_out
    return armed[0]


async def _cron(thread, n: int):
    return await _arm(
        thread, kind="cron", timezone="UTC", cron="*/5 * * * *", schedule=_slots(n)
    )


async def _notices(db_conn, wakeup_id: str) -> list[dict]:
    cur = await db_conn.execute(
        "SELECT payload FROM hook_outbox WHERE payload->>'wakeup_id' = %s ORDER BY created_at",
        (str(wakeup_id),),
    )
    return [r["payload"] for r in await cur.fetchall()]


async def _row(db_conn, wakeup_id: str) -> dict:
    cur = await db_conn.execute(
        "SELECT * FROM wakeups WHERE wakeup_id = %s", (str(wakeup_id),)
    )
    return await cur.fetchone()


# --- the cap ----------------------------------------------------------------


async def test_the_cap_leaves_out_the_specs_past_it(thread):
    from src.server.database.wakeups.arm import WakeupSpec, arm_wakeups

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    specs = [
        WakeupSpec(kind="at", timezone="UTC", schedule=(future + timedelta(minutes=i),))
        for i in range(4)
    ]
    armed, left_out = await arm_wakeups(thread.id, thread.user_id, specs, cap=3)
    assert len(armed) == 3
    assert left_out == [3]


# --- fire_due_slots ---------------------------------------------------------


async def test_a_series_fires_once_while_unread_and_its_last_slot_always_arrives(
    thread, db_conn
):
    from src.server.database.wakeups.fire import fire_due_slots

    row = await _cron(thread, 4)

    consumed = [
        r
        for r in await fire_due_slots()
        if str(r["wakeup_id"]) == str(row["wakeup_id"])
    ]

    assert [(r["seq"], r["fired"]) for r in consumed] == [
        (1, True),  # nothing unread yet
        (2, False),  # slot 1 is still unread
        (3, False),
        (4, True),  # the last one always arrives
    ]
    final = await _row(db_conn, row["wakeup_id"])
    assert final["status"] == "finished"
    assert final["fires_done"] == 4
    assert final["last_fired_seq"] == 4
    assert final["next_fire_at"] is None
    # Two notices, not four.
    assert [p["seq"] for p in await _notices(db_conn, row["wakeup_id"])] == [1, 4]


async def test_a_slot_is_not_consumed_twice(thread):
    from src.server.database.wakeups.fire import fire_due_slots

    row = await _cron(thread, 2)
    first = [
        r
        for r in await fire_due_slots()
        if str(r["wakeup_id"]) == str(row["wakeup_id"])
    ]
    again = [
        r
        for r in await fire_due_slots()
        if str(r["wakeup_id"]) == str(row["wakeup_id"])
    ]
    assert len(first) == 2
    assert again == []


async def test_reading_a_fire_lets_the_next_slot_fire(thread, db_conn):
    from src.server.database.wakeups.fire import fire_due_slots, mark_delivered

    row = await _arm(
        thread,
        kind="cron",
        timezone="UTC",
        cron="*/5 * * * *",
        schedule=(
            datetime.now(timezone.utc) - timedelta(minutes=10),
            datetime.now(timezone.utc) + timedelta(hours=1),
        ),
    )
    [first] = [
        r
        for r in await fire_due_slots()
        if str(r["wakeup_id"]) == str(row["wakeup_id"])
    ]
    assert (first["seq"], first["fired"]) == (1, True)
    assert await mark_delivered(str(row["wakeup_id"]), "fire", 1, None) is True
    # A second stamp of the same slot is no news.
    assert await mark_delivered(str(row["wakeup_id"]), "fire", 1, None) is False
    assert (await _row(db_conn, row["wakeup_id"]))["delivered_seq"] == 1


# --- cancel -----------------------------------------------------------------


async def test_cancel_outcomes_by_id_and_by_short_id(thread):
    from src.server.database.wakeups import read
    from src.server.database.wakeups.cancel import cancel_wakeups

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    row = await _arm(thread, kind="at", timezone="UTC", schedule=(future,))
    assert (await read.get_thread_wakeup(thread.id, row["short_id"]))["wakeup_id"] == row["wakeup_id"]

    [first] = await cancel_wakeups(thread.id, [row["short_id"]], by="agent")
    assert first.outcome == "cancelled"
    assert first.row["status"] == "cancelled"
    assert first.row["cancelled_by"] == "agent"

    [again] = await cancel_wakeups(thread.id, [str(row["wakeup_id"])], by="agent")
    assert again.outcome == "not_pending"

    [unknown] = await cancel_wakeups(thread.id, ["w_zzzz"], by="agent")
    assert (unknown.outcome, unknown.row) == ("not_pending", None)


async def test_a_fire_past_recall_is_too_late_for_the_agent_but_not_for_a_forced_cancel(
    thread, db_conn
):
    from src.server.database.wakeups.cancel import cancel_wakeups
    from src.server.database.wakeups.fire import fire_due_slots

    row = await _arm(thread, kind="at", timezone="UTC", schedule=_slots(1))
    await fire_due_slots()
    # A running turn is about to show the notice: it holds the job.
    await db_conn.execute(
        'UPDATE hook_outbox SET payload = payload || \'{"steered_run_id": "r_test"}\'::jsonb '
        "WHERE payload->>'wakeup_id' = %s",
        (str(row["wakeup_id"]),),
    )

    [late] = await cancel_wakeups(thread.id, [row["short_id"]], by="agent")
    assert late.outcome == "too_late"
    assert (await _row(db_conn, row["wakeup_id"]))["status"] == "fired"

    [forced] = await cancel_wakeups(thread.id, [row["short_id"]], by="archive")
    assert forced.outcome == "cancelled"


async def test_cancelling_an_unread_fire_in_reach_closes_its_notice(thread, db_conn):
    from src.server.database.wakeups.cancel import cancel_wakeups
    from src.server.database.wakeups.fire import fire_due_slots

    row = await _arm(thread, kind="at", timezone="UTC", schedule=_slots(1))
    await fire_due_slots()

    [out] = await cancel_wakeups(thread.id, None, by="user")

    assert out.outcome == "cancelled"
    cur = await db_conn.execute(
        "SELECT status FROM hook_outbox WHERE payload->>'wakeup_id' = %s",
        (str(row["wakeup_id"]),),
    )
    assert [r["status"] for r in await cur.fetchall()] == ["done"]


@pytest.mark.parametrize("unanswered", [False, True])
async def test_a_wake_post_with_no_answer_keeps_its_notices_past_recall(
    thread, db_conn, unanswered
):
    # The head's wake POST may yet be admitted carrying both notices, after a
    # refusal held the head and its rider, so a cancel cannot call them back.
    from src.server.database.runs import outbox
    from src.server.database.wakeups.cancel import cancel_wakeups
    from src.server.database.wakeups.fire import fire_due_slots
    from src.server.database.wakeups.riders import gather_wakeup_riders

    for _ in range(2):
        await _arm(thread, kind="at", timezone="UTC", schedule=_slots(1))
    await fire_due_slots()
    [head] = [
        r
        for r in await outbox.claim_outbox_jobs(limit=50, max_attempts=5)
        if str(r["conversation_thread_id"]) == thread.id
    ]
    head_id = str(head["hook_outbox_id"])
    assert len(await gather_wakeup_riders(head_id, attempts=head["attempts"])) == 1
    if unanswered:
        await outbox.merge_job_payload(head_id, {"unanswered": True})
    assert await outbox.exhaust_alert(head_id, attempts=head["attempts"])

    outcomes = await cancel_wakeups(thread.id, None, by="user")

    assert [o.outcome for o in outcomes] == ["too_late" if unanswered else "cancelled"] * 2
    cur = await db_conn.execute(
        "SELECT status FROM hook_outbox WHERE conversation_thread_id = %s",
        (thread.id,),
    )
    assert [r["status"] for r in await cur.fetchall()] == (
        ["pending"] * 2 if unanswered else ["done"] * 2
    )


@pytest.mark.parametrize("gone", ["archive", "workspace"])
async def test_the_close_sweep_leaves_an_owed_wakeup_notice_to_its_executor(
    thread, db_conn, gone
):
    # Archive and workspace delete end a thread's wakeups once, after their
    # commit; when that fails the notice's executor is the one left to end
    # the wakeup, so the sweep makes a waiting one due rather than close it.
    from src.server.database.runs import outbox
    from src.server.database.wakeups.cancel import cancel_open
    from src.server.database.wakeups.fire import fire_due_slots, mark_delivered

    owed = await _arm(thread, kind="at", timezone="UTC", schedule=_slots(1))
    delivered = await _arm(thread, kind="at", timezone="UTC", schedule=_slots(1))
    await fire_due_slots()
    jobs = {}
    for _ in range(2):  # one claim per thread at a time; a waiting one blocks none
        [job] = [
            r
            for r in await outbox.claim_outbox_jobs(limit=50, max_attempts=5)
            if str(r["conversation_thread_id"]) == thread.id
        ]
        assert await outbox.exhaust_alert(str(job["hook_outbox_id"]), attempts=job["attempts"])
        jobs[job["payload"]["wakeup_id"]] = job
    assert await mark_delivered(str(delivered["wakeup_id"]), "fire", 1, None)
    if gone == "archive":
        await db_conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW()"
            " WHERE conversation_thread_id = %s",
            (thread.id,),
        )
    else:
        await db_conn.execute(
            "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
            (thread.workspace_id,),
        )

    closed = await outbox.close_unowed_waiting_alerts()

    assert [c["hook_outbox_id"] for c in closed if str(c["conversation_thread_id"]) == thread.id] == [
        jobs[str(delivered["wakeup_id"])]["hook_outbox_id"]
    ]
    [due] = [
        r
        for r in await outbox.claim_outbox_jobs(limit=50, max_attempts=5)
        if str(r["conversation_thread_id"]) == thread.id
    ]
    assert due["payload"]["wakeup_id"] == str(owed["wakeup_id"])
    # What its executor does on this thread: end the wakeup, then drop the notice.
    await cancel_open(thread_id=thread.id, by=gone)
    assert await outbox.ack_outbox_job(str(due["hook_outbox_id"]), attempts=due["attempts"])
    row = await _row(db_conn, owed["wakeup_id"])
    assert (row["status"], row["cancelled_by"]) == ("cancelled", gone)


async def test_cancel_open_takes_exactly_one_scope(thread):
    from src.server.database.wakeups.cancel import cancel_open

    with pytest.raises(ValueError):
        await cancel_open(by="user")
    with pytest.raises(ValueError):
        await cancel_open(by="user", user_id=thread.user_id, thread_id=thread.id)


@pytest.mark.parametrize("scope", ["thread_id", "workspace_id", "user_id"])
async def test_cancel_open_by_scope_closes_every_open_wakeup_in_it(thread, scope):
    from src.server.database.wakeups.cancel import cancel_open

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    rows = [
        await _arm(
            thread, kind="at", timezone="UTC", schedule=(future + timedelta(minutes=i),)
        )
        for i in range(2)
    ]
    value = getattr(thread, scope)

    done = await cancel_open(by="archive", **{scope: value})

    assert {o.wakeup_id for o in done[thread.id]} == {str(r["wakeup_id"]) for r in rows}
    assert {o.outcome for o in done[thread.id]} == {"cancelled"}


# --- the order backstop -----------------------------------------------------


async def _attempt(
    db_conn, thread, *, status: str, filled: float | None, unlisted: bool = False
) -> str:
    cur = await db_conn.execute(
        """
        INSERT INTO order_attempts
            (user_id, thread_id, message_id, tool_call_id, vendor, status,
             vendor_order_id, filled_qty, order_json, unlisted_at)
        VALUES (%s, %s, %s, %s, 'moomoo', %s, %s, %s, '{"qty": 100}'::jsonb,
                CASE WHEN %s THEN NOW() END)
        RETURNING attempt_id::text AS id
        """,
        (
            thread.user_id,
            thread.id,
            uuid.uuid4().hex,
            uuid.uuid4().hex,
            status,
            "7712045",
            filled,
            unlisted,
        ),
    )
    return (await cur.fetchone())["id"]


async def _watch(thread, attempt_id: str, baseline: float):
    return await _arm(
        thread,
        kind="order",
        attempt_id=attempt_id,
        vendor_order_id="7712045",
        baseline_filled_qty=baseline,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
    )


async def test_the_backstop_fires_a_wakeup_whose_order_filled_or_ended(thread, db_conn):
    from src.server.database.wakeups.fire import fire_order_wakeups

    untouched = await _watch(
        thread, await _attempt(db_conn, thread, status="working", filled=0), 0
    )
    moved = await _watch(
        thread, await _attempt(db_conn, thread, status="working", filled=40), 0
    )
    ended = await _watch(
        thread, await _attempt(db_conn, thread, status="cancelled", filled=0), 0
    )

    fired = {str(r["wakeup_id"]): r for r in await fire_order_wakeups(None)}

    assert str(untouched["wakeup_id"]) not in fired
    assert fired[str(moved["wakeup_id"])]["order_outcome"]["outcome"] == "partial"
    assert fired[str(ended["wakeup_id"])]["order_outcome"]["outcome"] == "cancelled"
    assert (await _row(db_conn, untouched["wakeup_id"]))["status"] == "armed"

    # A second pass over the same state fires nothing: the guard is the status.
    assert await fire_order_wakeups(None) == []
    # And one notice each reached the thread.
    assert len(await _notices(db_conn, moved["wakeup_id"])) == 1


@pytest.mark.parametrize(
    "status, filled, unlisted, outcome",
    [
        ("expired", 0, False, "expired"),
        ("expired", 40, False, "partial_expired"),
        # A staged order the vendor's list reported confirmed has no unlisted
        # stamp; one that only left the list keeps it.
        ("handed_off", 0, False, "confirmed"),
        ("handed_off", 0, True, "handed_off"),
    ],
)
async def test_the_backstop_names_how_an_order_ended(
    thread, db_conn, status, filled, unlisted, outcome
):
    from src.server.database.wakeups.fire import fire_order_wakeups

    row = await _watch(
        thread,
        await _attempt(
            db_conn, thread, status=status, filled=filled, unlisted=unlisted
        ),
        0,
    )

    fired = {str(r["wakeup_id"]): r for r in await fire_order_wakeups(None)}

    assert fired[str(row["wakeup_id"])]["order_outcome"]["outcome"] == outcome


async def test_a_fill_already_there_when_the_wakeup_was_set_does_not_fire_it(
    thread, db_conn
):
    from src.server.database.wakeups.fire import fire_order_wakeups

    row = await _watch(
        thread, await _attempt(db_conn, thread, status="working", filled=40), 40
    )
    assert await fire_order_wakeups(None) == []
    assert (await _row(db_conn, row["wakeup_id"]))["status"] == "armed"
