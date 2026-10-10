"""finalize_run against real Postgres.

The finalize merges its events onto whatever mid-run appenders wrote, writes
the run's replay facts with its status, and rewrites the main lane's
provenance from the lane's own record, so only a real database can show that
an append queued on the row lock reaches the archive, that a subagent lane's
records survive, and that a lost CAS writes nothing.
"""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio

# Session loop: the pool lives there, and the lock test needs a second connection.
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# What a finalize hands back, won or lost.
ROW_KEYS = {
    "conversation_response_id",
    "conversation_thread_id",
    "turn_index",
    "run_seq",
    "status",
    "interrupt_reason",
    "metadata",
}

CHUNK = {"event": "message_chunk", "data": {"agent": "ptc", "content": "m"}}
COMPACT = {"event": "context_window", "data": {"agent": "ptc", "action": "compact"}}


def _provenance(url: str) -> dict:
    return {
        "event": "provenance",
        "source_type": "web",
        "identifier": url,
        "agent": "ptc",
    }


def _finalize(run_id: str, thread_id: str, status: str, **fields):
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    return finalize_run(
        run_id=run_id, thread_id=thread_id, outcome=RunOutcome(status=status, **fields)
    )


def _record(*events: dict, reasoning_ms=None):
    """A producer's record of ``events``' provenance."""
    from src.server.database.runs.lifecycle import ProducerRecord

    return ProducerRecord(
        provenance=[e for e in events if e["event"] == "provenance"],
        reasoning_ms=reasoning_ms,
    )


@pytest_asyncio.fixture(loop_scope="session")
async def open_run(seed_workspace, patched_get_db_connection):
    from src.server.database.conversation import create_thread
    from src.server.database.runs.lifecycle import start_run

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="in_progress",
        msg_type="ptc",
    )
    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc", "user_id": seed_workspace["user_id"]},
    )
    return thread_id, run_id


async def _sse_events(conn, run_id: str):
    cur = await conn.execute(
        "SELECT sse_events FROM conversation_responses WHERE conversation_response_id = %s",
        (run_id,),
    )
    return (await cur.fetchone())["sse_events"]


async def _provenance_ids(conn, run_id: str) -> list[str]:
    cur = await conn.execute(
        "SELECT identifier FROM provenance_records "
        "WHERE conversation_response_id = %s ORDER BY identifier",
        (run_id,),
    )
    return [r["identifier"] for r in await cur.fetchall()]


async def test_events_merge_onto_mid_run_appends_and_encode_once(open_run, db_conn):
    from src.server.database.conversation import append_sse_event
    from src.server.utils import pg_sanitize

    thread_id, run_id = open_run
    early = _provenance("https://early.example")
    assert await append_sse_event(thread_id, early)
    events = [CHUNK, _provenance("https://final.example")]

    encodes = []
    real_dumps = pg_sanitize._safe_dumps

    def counting_dumps(value):
        if value is events:
            encodes.append(value)
        return real_dumps(value)

    with patch.object(pg_sanitize, "_safe_dumps", counting_dumps):
        result = await _finalize(
            run_id, thread_id, "completed", sse_events=events, record=_record(*events)
        )

    assert result.applied
    assert set(result.run) == ROW_KEYS
    assert result.run["status"] == "completed"
    assert len(encodes) == 1
    assert await _sse_events(db_conn, run_id) == [early, *events]
    # The lane's own record, not the merged archive: no mid-run writer
    # appends provenance, so the archive holds nothing the record lacks.
    assert await _provenance_ids(db_conn, run_id) == ["https://final.example"]
    # The outbox jobs are built from the returned row alone.
    cur = await db_conn.execute(
        "SELECT payload FROM hook_outbox WHERE run_id = %s AND hook_type = 'user_feed'",
        (run_id,),
    )
    assert (await cur.fetchone())["payload"]["run_seq"] == result.run["run_seq"]


async def test_append_queued_on_the_row_lock_reaches_the_archive(
    open_run, db_conn, test_db_pool
):
    from src.server.database.conversation import append_sse_event

    thread_id, run_id = open_run
    late = _provenance("https://late.example")

    async with test_db_pool.connection() as holder:
        async with holder.transaction():
            assert await append_sse_event(thread_id, late, conn=holder)
            finalize = asyncio.create_task(
                _finalize(
                    run_id,
                    thread_id,
                    "completed",
                    sse_events=[CHUNK],
                    record=_record(CHUNK),
                )
            )
            await asyncio.sleep(0.2)
            assert not finalize.done()  # queued behind the appender's row lock
    assert (await finalize).applied

    assert await _sse_events(db_conn, run_id) == [late, CHUNK]
    # Provenance is the lane's own record, which the finalize carried.
    assert await _provenance_ids(db_conn, run_id) == []


async def test_lost_cas_writes_nothing_and_returns_the_survivor(open_run, db_conn):
    thread_id, run_id = open_run
    won_events = [CHUNK, _provenance("https://won.example")]
    first = await _finalize(
        run_id, thread_id, "completed", sse_events=won_events, record=_record(*won_events)
    )
    assert first.applied

    lost_events = [COMPACT, _provenance("https://lost.example")]
    second = await _finalize(
        run_id,
        thread_id,
        "error",
        errors=["late"],
        sse_events=lost_events,
        record=_record(*lost_events),
    )

    assert not second.applied
    assert set(second.run) == ROW_KEYS
    assert second.run["status"] == "completed"
    assert await _sse_events(db_conn, run_id) == won_events
    assert await _provenance_ids(db_conn, run_id) == ["https://won.example"]


@pytest.mark.parametrize("appended", [None, COMPACT], ids=["empty", "mid-run-append"])
async def test_finalize_without_events_keeps_the_column(open_run, db_conn, appended):
    from src.server.database.conversation import append_sse_event

    thread_id, run_id = open_run
    if appended is not None:
        assert await append_sse_event(thread_id, appended)

    result = await _finalize(run_id, thread_id, "cancelled")

    assert result.applied
    assert await _sse_events(db_conn, run_id) == (
        [appended] if appended is not None else None
    )
    assert await _provenance_ids(db_conn, run_id) == []


async def _replay_facts(conn, run_id: str):
    cur = await conn.execute(
        "SELECT replay_facts FROM conversation_responses "
        "WHERE conversation_response_id = %s",
        (run_id,),
    )
    return (await cur.fetchone())["replay_facts"]


async def test_replay_facts_land_with_the_status_and_survive_a_lost_cas(
    open_run, db_conn
):
    thread_id, run_id = open_run
    durations = {"msg-1": [1840, 920]}
    first = await _finalize(
        run_id,
        thread_id,
        "completed",
        sse_events=[CHUNK],
        record=_record(reasoning_ms=durations),
    )
    assert first.applied
    assert await _replay_facts(db_conn, run_id) == {"reasoning_ms": durations}

    second = await _finalize(
        run_id,
        thread_id,
        "error",
        errors=["late"],
        record=_record(reasoning_ms={"msg-2": [5]}),
    )
    assert not second.applied
    assert await _replay_facts(db_conn, run_id) == {"reasoning_ms": durations}


@pytest.mark.parametrize("recovered", [False, True], ids=["fail-open", "recovery"])
async def test_finalize_without_facts_leaves_the_column_null(
    open_run, db_conn, recovered
):
    """Recovery and fail-open settles hold no producer: their turns replay
    reasoning durations from the stored events, as a legacy turn does."""
    thread_id, run_id = open_run
    record = _record(CHUNK) if recovered else None
    assert (await _finalize(run_id, thread_id, "cancelled", record=record)).applied
    assert await _replay_facts(db_conn, run_id) is None


async def test_main_lane_provenance_leaves_subagent_lanes(open_run, db_conn):
    """A subagent's archive writes its lane before the parent settles; the
    finalize rewrites only the main lane."""
    from src.server.database.conversation.responses import replace_agent_events

    thread_id, run_id = open_run
    task_record = {
        "event": "provenance",
        "data": {
            "agent": "task:abc",
            "source_type": "web",
            "identifier": "https://task.example",
        },
    }
    assert await replace_agent_events(run_id, [task_record])
    assert await _provenance_ids(db_conn, run_id) == ["https://task.example"]

    main_record = {
        "event": "provenance",
        "data": {
            "agent": "main",
            "source_type": "web",
            "identifier": "https://main.example",
        },
    }
    result = await _finalize(
        run_id,
        thread_id,
        "completed",
        sse_events=[CHUNK, main_record],
        record=_record(main_record),
    )
    assert result.applied
    assert await _provenance_ids(db_conn, run_id) == [
        "https://main.example",
        "https://task.example",
    ]
