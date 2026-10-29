"""A staged instruction handed off, and the cancel that holds one back, against real Postgres.

The status CHECK is what a missing migration breaks, and the cancel read is a
predicate over a sibling row's action, state, age and target: neither shows
through a stubbed cursor.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
import pytest_asyncio
from psycopg.types.json import Json

from src.server.services.brokerage_orders import AttemptStatus

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# Invented, in IBKR's shape: a decimal-string instruction id.
STAGED_ID = "9000001"
GRACE = 120


@pytest_asyncio.fixture(loop_scope="session")
async def user_id(patched_get_db_connection, test_db_pool):
    user_id = f"handoff-test-{uuid.uuid4().hex[:12]}"
    try:
        yield user_id
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM order_attempts WHERE user_id = %s", (user_id,)
            )


async def _attempt(
    db_conn,
    user_id: str,
    *,
    action: str,
    status: str,
    vendor_order_id: str | None = None,
    target_ref: str | None = None,
    account_ref: str = "",
) -> str:
    cur = await db_conn.execute(
        """
        INSERT INTO order_attempts
            (user_id, message_id, tool_call_id, server, vendor, action, mode,
             account_ref, status, vendor_order_id, order_json, executed_at,
             dispatched_at)
        VALUES (%s, %s, %s, 'ibkr', 'ibkr', %s, 'staged', %s, %s, %s, %s,
                NOW(), NOW())
        RETURNING attempt_id::text AS id
        """,
        (
            user_id,
            uuid.uuid4().hex,
            uuid.uuid4().hex,
            action,
            account_ref,
            status,
            vendor_order_id,
            Json({"target_ref": target_ref} if target_ref else {}),
        ),
    )
    return (await cur.fetchone())["id"]


async def test_a_handed_off_instruction_is_settled_dated_and_final(user_id, db_conn):
    from src.server.database.order_reconciliation import (
        Observation,
        record_observation,
    )

    staged = await _attempt(
        db_conn, user_id, action="stage", status="submitted",
        vendor_order_id=STAGED_ID,
    )

    row = await record_observation(
        staged,
        AttemptStatus.SUBMITTED,
        Observation(status=AttemptStatus.HANDED_OFF, from_absence=True),
    )

    assert row is not None and row["status"] == "handed_off"
    cur = await db_conn.execute(
        "SELECT completed_at FROM order_attempts WHERE attempt_id = %s::uuid",
        (staged,),
    )
    assert (await cur.fetchone())["completed_at"] is not None
    # The delete's answer, landing after, is not allowed over it.
    assert (
        await record_observation(
            staged,
            AttemptStatus.SUBMITTED,
            Observation(status=AttemptStatus.CANCELLED),
        )
        is None
    )


async def test_one_read_finds_our_cancel_answered_or_still_out(user_id, db_conn):
    """Both states from one statement, so a cancel answered mid-read is never missed.

    Read apart, a delete whose answer lands between the two reads is
    ``submitting`` to the first and ``cancelled`` to the second, seen by
    neither, and the instruction is handed off for good.
    """
    from src.server.database.order_reconciliation import own_cancel

    staged = await _attempt(
        db_conn, user_id, action="stage", status="submitted",
        vendor_order_id=STAGED_ID,
    )

    async def found(
        order_id: str = STAGED_ID, account_ref: str = "", awaiting: bool = True
    ) -> str | None:
        cancel = await own_cancel(
            user_id,
            "ibkr",
            order_id,
            account_ref=account_ref,
            exclude_attempt_id=staged,
            awaiting_within=GRACE if awaiting else None,
        )
        return None if cancel is None else cancel["status"]

    # The create itself is no cancel.
    assert await found() is None

    delete = await _attempt(
        db_conn, user_id, action="unstage", status="submitting",
        target_ref=STAGED_ID,
    )
    assert await found() == "submitting"
    # Asked after only by a caller that would hold the order for it.
    assert await found(awaiting=False) is None
    # Scoped to the instruction it names, and to the account it was made in.
    assert await found("9000002") is None
    assert await found(account_ref="other") is None

    await db_conn.execute(
        "UPDATE order_attempts SET updated_at = NOW() - %s::interval"
        " WHERE attempt_id = %s::uuid",
        (timedelta(seconds=GRACE * 2), delete),
    )
    # Past the grace its answer is not coming, and the list is all there is.
    assert await found() is None

    await db_conn.execute(
        "UPDATE order_attempts SET status = 'cancelled', updated_at = NOW()"
        " WHERE attempt_id = %s::uuid",
        (delete,),
    )
    # Answered: confirmed for every caller, whatever its age.
    assert await found() == "cancelled"
    assert await found(awaiting=False) == "cancelled"

    # And it outranks a second delete still out, which holds nothing the
    # first one's answer has not already ended.
    await _attempt(
        db_conn, user_id, action="unstage", status="submitting",
        target_ref=STAGED_ID,
    )
    assert await found() == "cancelled"


async def test_the_first_list_without_it_only_stamps_the_instruction(
    user_id, db_conn
):
    from src.server.database.order_reconciliation import (
        Observation,
        mark_unlisted,
        record_observation,
    )

    staged = await _attempt(
        db_conn, user_id, action="stage", status="submitted",
        vendor_order_id=STAGED_ID,
    )

    async def read() -> dict:
        cur = await db_conn.execute(
            "SELECT status, unlisted_at, updated_at FROM order_attempts"
            " WHERE attempt_id = %s::uuid",
            (staged,),
        )
        return await cur.fetchone()

    before = await read()
    assert await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    stamped = await read()
    assert stamped["status"] == "submitted"
    assert stamped["unlisted_at"] is not None
    # No write the sweep's windows count: the next read is a poll interval on,
    # not an interval after this stamp plus another.
    assert stamped["updated_at"] == before["updated_at"]
    # Once: it dates the first list that lacked the order, not the latest.
    assert not await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    # Held to the state it was read in.
    assert not await mark_unlisted(staged, AttemptStatus.WORKING)

    # A read that names the order again clears it.
    await record_observation(
        staged,
        AttemptStatus.SUBMITTED,
        Observation(status=AttemptStatus.SUBMITTED, vendor_order_id=STAGED_ID),
    )
    assert (await read())["unlisted_at"] is None

    # The end founded on absence keeps it, dating when the order went missing.
    assert await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    await record_observation(
        staged,
        AttemptStatus.SUBMITTED,
        Observation(status=AttemptStatus.HANDED_OFF, from_absence=True),
    )
    ended = await read()
    assert ended["status"] == "handed_off"
    assert ended["unlisted_at"] is not None



async def test_the_stamp_holds_off_a_sibling_for_a_poll_interval_from_the_read(
    user_id, db_conn
):
    """The second list is a poll interval after the first, not after the take.

    The row was taken well before it was read, as a group that waited for a
    slot is. Gated on the take alone, a sibling took it again just after the
    stamp and could end it on a list seconds after the one that stamped it.
    """
    from src.server.database.order_reconciliation import (
        list_stale_attempts,
        mark_unlisted,
    )

    staged = await _attempt(
        db_conn, user_id, action="stage", status="submitted",
        vendor_order_id=STAGED_ID,
    )
    await db_conn.execute(
        "UPDATE order_attempts SET updated_at = NOW() - INTERVAL '1 hour',"
        " swept_at = NOW() - INTERVAL '55 seconds' WHERE attempt_id = %s::uuid",
        (staged,),
    )

    async def take() -> set[str]:
        rows = await list_stale_attempts(
            submitting_grace_seconds=GRACE,
            open_after_seconds=60,
            resweep_after_seconds=48,
            limit=500,
        )
        return {row["attempt_id"] for row in rows if row["user_id"] == user_id}

    assert await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    assert await take() == set()

    # Held off, not dropped: a poll interval after the stamp it comes round.
    await db_conn.execute(
        "UPDATE order_attempts SET swept_at = NOW() - INTERVAL '55 seconds'"
        " WHERE attempt_id = %s::uuid",
        (staged,),
    )
    assert await take() == {staged}

async def test_an_end_holds_to_the_stamp_the_sweep_read(user_id, db_conn):
    """A sighting between the sweep's select and its write voids the end.

    The sighting leaves the status alone, so only the stamp tells the write the
    order was listed since. The end carries the stamp as read back from the
    column, which also pins the round trip: a stamp that did not compare equal
    would leave every instruction open for good.
    """
    from src.server.database.order_reconciliation import (
        Observation,
        mark_unlisted,
        record_observation,
    )

    staged = await _attempt(
        db_conn, user_id, action="stage", status="submitted",
        vendor_order_id=STAGED_ID,
    )

    async def stamp():
        cur = await db_conn.execute(
            "SELECT unlisted_at FROM order_attempts WHERE attempt_id = %s::uuid",
            (staged,),
        )
        return (await cur.fetchone())["unlisted_at"]

    def end(on) -> Observation:
        return Observation(
            status=AttemptStatus.HANDED_OFF, from_absence=True, unlisted_at=on
        )

    assert await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    selected = await stamp()
    # A sibling lists it: the stamp goes, the status stays.
    await record_observation(
        staged,
        AttemptStatus.SUBMITTED,
        Observation(status=AttemptStatus.SUBMITTED, vendor_order_id=STAGED_ID),
    )
    assert await record_observation(staged, AttemptStatus.SUBMITTED, end(selected)) is None
    # Stamped again by a later empty list, the old stamp still names nothing.
    assert await mark_unlisted(staged, AttemptStatus.SUBMITTED)
    assert await record_observation(staged, AttemptStatus.SUBMITTED, end(selected)) is None

    ended = await record_observation(
        staged, AttemptStatus.SUBMITTED, end(await stamp())
    )
    assert ended is not None
    assert ended["status"] == "handed_off"


async def test_the_status_check_is_validated_and_names_every_state(db_conn):
    import re

    cur = await db_conn.execute(
        """
        SELECT pg_get_constraintdef(oid) AS definition, convalidated
          FROM pg_constraint
         WHERE conrelid = 'order_attempts'::regclass
           AND conname = 'order_attempts_status_check'
        """
    )
    check = await cur.fetchone()
    # Added NOT VALID, so only the autocommit block's VALIDATE sets this.
    assert check["convalidated"]
    assert set(re.findall(r"'([a-z_]+)'", check["definition"])) == {
        s.value for s in AttemptStatus
    }

    cur = await db_conn.execute(
        """
        SELECT data_type, is_nullable, column_default
          FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = 'order_attempts'
           AND column_name = 'unlisted_at'
        """
    )
    assert await cur.fetchone() == {
        "data_type": "timestamp with time zone",
        "is_nullable": "YES",
        "column_default": None,
    }
