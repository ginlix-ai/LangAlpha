"""Which proposals the sweep refuses once no answer can reach them, against
real Postgres.

A card waits on an ``interrupted`` run for however long the user takes, so
whether a proposal is abandoned depends on what the thread did after it:
which runs followed, what started them, and how they ended.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _turn(thread_id, query_type, status, metadata=None):
    from src.server.database.runs.lifecycle import (
        QuerySpec,
        RunOutcome,
        finalize_run,
        start_run,
    )

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        query=QuerySpec(query_id=str(uuid.uuid4()), content="", query_type=query_type),
        metadata={"msg_type": "ptc"},
    )
    await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        outcome=RunOutcome(status=status, metadata=metadata or {}),
    )
    return run_id


async def _retry(thread_id, run_id):
    """A /retry of ``run_id``: no query of its own, on the retried turn."""
    from src.server.database.runs.lifecycle import (
        RunOutcome,
        finalize_run,
        get_run,
        start_run,
    )

    retried = await get_run(run_id)
    retry_id = str(uuid.uuid4())
    await start_run(
        run_id=retry_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        turn_index=retried["turn_index"],
        attempt_no=2,
        retry_of_run_id=run_id,
        metadata={"msg_type": "ptc"},
    )
    await finalize_run(
        run_id=retry_id, thread_id=thread_id, outcome=RunOutcome(status="interrupted")
    )


@pytest_asyncio.fixture(loop_scope="session")
async def card(seed_workspace, test_db_pool, patched_get_db_connection):
    """An order proposed on a run that paused for its card, long enough ago
    that only the sweep's predicate decides."""
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="interrupted",
        msg_type="ptc",
    )
    run_id = await _turn(thread_id, "initial", "interrupted")
    user_id = f"abandoned-test-{uuid.uuid4().hex[:12]}"
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            """
            INSERT INTO order_attempts
                (user_id, message_id, tool_call_id, server, vendor, action,
                 status, approval_required, thread_id, conversation_response_id,
                 updated_at)
            VALUES (%s, %s, %s, 'broker', 'ibkr', 'place', 'proposed', TRUE,
                    %s, %s, NOW() - INTERVAL '1 hour')
            RETURNING attempt_id::text AS id
            """,
            (user_id, f"ai-{uuid.uuid4()}", f"call-{uuid.uuid4()}", thread_id, run_id),
        )
        attempt_id = (await cur.fetchone())["id"]
    try:
        yield thread_id, attempt_id
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM order_attempts WHERE user_id = %s", (user_id,))


async def _sweep(db_conn, attempt_id):
    from src.server.database.order_reconciliation import refuse_abandoned_proposals

    await refuse_abandoned_proposals(grace_seconds=600)
    cur = await db_conn.execute(
        "SELECT status, failure->>'code' AS code FROM order_attempts "
        "WHERE attempt_id = %s",
        (attempt_id,),
    )
    row = await cur.fetchone()
    return row["status"], row["code"]


async def test_a_card_nothing_followed_keeps_waiting(card, db_conn):
    _, attempt_id = card

    assert await _sweep(db_conn, attempt_id) == ("proposed", None)


@pytest.mark.parametrize("status", ["completed", "interrupted", "cancelled", "error"])
async def test_a_card_a_later_message_went_past_is_refused(card, db_conn, status):
    """The new message's run answered the order call with a cancellation, so
    no answer reaches the card any more, and the thread still read as waiting
    on an order."""
    thread_id, attempt_id = card
    await _turn(thread_id, "follow_up", status)

    assert await _sweep(db_conn, attempt_id) == ("refused", "never_decided")


@pytest.mark.parametrize("status", ["cancelled", "error"])
async def test_a_card_a_later_message_stopped_short_of_keeps_waiting(
    card, db_conn, status
):
    """Stopped or failed before its first step, the message's run left the
    card the newest thing on the thread, still answerable."""
    thread_id, attempt_id = card
    await _turn(thread_id, "follow_up", status, {"pending_input": {"resend": True}})

    assert await _sweep(db_conn, attempt_id) == ("proposed", None)


@pytest.mark.parametrize("status", ["cancelled", "error"])
async def test_a_card_whose_answer_stopped_short_keeps_waiting(card, db_conn, status):
    """The answer's run ended before its first step, which leaves the card
    the newest thing on the thread: answering it again, or a retry of that
    run, still puts it to the user. A later turn ending does not change that
    for the answer."""
    thread_id, attempt_id = card
    answer = await _turn(thread_id, "resume_feedback", status)
    await _retry(thread_id, answer)

    assert await _sweep(db_conn, attempt_id) == ("proposed", None)
