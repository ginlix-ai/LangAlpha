"""finalize_run against real Postgres.

The finalize writes the run's replay facts with its status and rewrites the
main lane's provenance from the lane's own record, so only a real database
can show that a subagent lane's records survive and that a lost CAS writes
nothing.
"""

from __future__ import annotations

import uuid

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
    "created_at",
    "usage_settled_at",
}

CHUNK = {"event": "message_chunk", "data": {"agent": "ptc", "content": "m"}}


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


async def _provenance_ids(conn, run_id: str) -> list[str]:
    cur = await conn.execute(
        "SELECT identifier FROM provenance_records "
        "WHERE conversation_response_id = %s ORDER BY identifier",
        (run_id,),
    )
    return [r["identifier"] for r in await cur.fetchall()]


async def test_finalize_settles_the_row(open_run, db_conn):
    thread_id, run_id = open_run
    events = [CHUNK, _provenance("https://final.example")]

    result = await _finalize(run_id, thread_id, "completed", record=_record(*events))

    assert result.applied
    assert set(result.run) == ROW_KEYS
    assert result.run["status"] == "completed"
    assert await _provenance_ids(db_conn, run_id) == ["https://final.example"]
    # The outbox jobs are built from the returned row alone.
    cur = await db_conn.execute(
        "SELECT payload FROM hook_outbox WHERE run_id = %s AND hook_type = 'user_feed'",
        (run_id,),
    )
    assert (await cur.fetchone())["payload"]["run_seq"] == result.run["run_seq"]


async def test_lost_cas_writes_nothing_and_returns_the_survivor(open_run, db_conn):
    thread_id, run_id = open_run
    won = _record(_provenance("https://won.example"))
    first = await _finalize(run_id, thread_id, "completed", record=won)
    assert first.applied

    lost = _record(_provenance("https://lost.example"))
    second = await _finalize(run_id, thread_id, "error", errors=["late"], record=lost)

    assert not second.applied
    assert set(second.run) == ROW_KEYS
    assert second.run["status"] == "completed"
    assert await _provenance_ids(db_conn, run_id) == ["https://won.example"]


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


async def test_a_stopped_runs_late_captured_images_land_in_its_facts(
    open_run, db_conn
):
    """Every ending keeps the images its saved text still named, not only a
    completed one."""
    thread_id, run_id = open_run
    images = {"charts/a.png": "https://cdn.example/a.png"}
    assert (await _finalize(run_id, thread_id, "cancelled", images=images)).applied
    assert await _replay_facts(db_conn, run_id) == {"images": images}


@pytest.mark.parametrize("recovered", [False, True], ids=["fail-open", "recovery"])
async def test_finalize_without_facts_leaves_the_column_null(
    open_run, db_conn, recovered
):
    """Recovery and fail-open settles hold no producer that timed the
    reasoning, so their rows keep no durations."""
    thread_id, run_id = open_run
    record = _record(CHUNK) if recovered else None
    assert (await _finalize(run_id, thread_id, "cancelled", record=record)).applied
    assert await _replay_facts(db_conn, run_id) is None


async def test_main_lane_provenance_leaves_subagent_lanes(open_run, db_conn):
    """A subagent's archive writes its lane before the parent settles; the
    finalize rewrites only the main lane."""
    from src.server.database.conversation.responses import replace_lane_provenance

    thread_id, run_id = open_run
    task_record = {
        "event": "provenance",
        "data": {
            "agent": "task:abc",
            "source_type": "web",
            "identifier": "https://task.example",
        },
    }
    assert await replace_lane_provenance(run_id, [task_record], {"task:abc": "abc"})
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
        record=_record(main_record),
    )
    assert result.applied
    assert await _provenance_ids(db_conn, run_id) == [
        "https://main.example",
        "https://task.example",
    ]
