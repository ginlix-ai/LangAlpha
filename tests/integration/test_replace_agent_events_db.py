"""replace_agent_events against real Postgres.

The strip and append run in SQL under a row lock, so only a real database can
show that a concurrent append survives, that other agents' rows keep their
order, and that the written lanes' provenance is rewritten from the batch.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import pytest_asyncio

# Session loop: the pool lives there, and the lock test needs a second connection.
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

MAIN = {"event": "message_chunk", "data": {"agent": "ptc", "content": "m"}}
STALE_TASK_ROW = {"event": "artifact", "data": {"agent": "task:abc", "artifact_type": "todo"}}
OTHER_TASK_ROW = {"event": "message_chunk", "data": {"agent": "task:def", "content": "d"}}
AGENTLESS = {"event": "metadata", "data": {"note": "turn-level"}}
NULL_AGENT = {"event": "metadata", "data": {"agent": None, "note": "null agent"}}
MAIN_PROVENANCE = {
    "event": "provenance",
    "data": {"agent": "ptc", "source_type": "web", "identifier": "https://a.example"},
}
TASK_PROVENANCE = {
    "event": "provenance",
    "data": {"agent": "task:abc", "source_type": "web", "identifier": "https://b.example"},
}


@pytest_asyncio.fixture(loop_scope="session")
async def make_response_row(seed_workspace, seed_response):
    from src.server.database.conversation import create_thread

    async def _make(sse_events: list) -> str:
        thread_id = str(uuid.uuid4())
        await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=str(seed_workspace["workspace_id"]),
            current_status="completed",
            msg_type="ptc",
        )
        response_id = str(uuid.uuid4())
        await seed_response(
            conversation_response_id=response_id,
            conversation_thread_id=thread_id,
            turn_index=0,
            sse_events=sse_events,
        )
        return response_id

    return _make


@pytest_asyncio.fixture(loop_scope="session")
async def response_row(make_response_row):
    return await make_response_row(
        [MAIN, STALE_TASK_ROW, OTHER_TASK_ROW, MAIN_PROVENANCE]
    )


async def _sse_events(conn, response_id: str) -> list:
    cur = await conn.execute(
        "SELECT sse_events FROM conversation_responses WHERE conversation_response_id = %s",
        (response_id,),
    )
    return (await cur.fetchone())["sse_events"]


async def _provenance_identifiers(conn, response_id: str) -> list[str]:
    cur = await conn.execute(
        "SELECT identifier FROM provenance_records "
        "WHERE conversation_response_id = %s ORDER BY identifier",
        (response_id,),
    )
    return [r["identifier"] for r in await cur.fetchall()]


async def test_strips_only_the_named_agents_and_appends_in_order(
    response_row, db_conn
):
    from src.server.database.conversation.responses import replace_agent_events

    captured = [
        {"event": "message_chunk", "data": {"agent": "task:abc", "content": f"t{i}"}}
        for i in range(4500)  # spans several encode slices
    ]
    captured[7]["data"]["score"] = float("nan")
    captured[8]["data"]["content"] = "before\x00after"

    assert await replace_agent_events(response_row, captured)

    events = await _sse_events(db_conn, response_row)
    assert events[:3] == [MAIN, OTHER_TASK_ROW, MAIN_PROVENANCE]
    assert [e["data"]["content"] for e in events[3:]] == [
        "beforeafter" if i == 8 else f"t{i}" for i in range(4500)
    ]
    assert events[3 + 7]["data"]["score"] is None


async def test_rows_without_a_string_agent_are_never_stripped(
    make_response_row, db_conn
):
    """An appended event with no agent, or a null one, names no rows: the
    turn's own agentless rows survive any batch."""
    from src.server.database.conversation.responses import replace_agent_events

    response_id = await make_response_row([AGENTLESS, NULL_AGENT, STALE_TASK_ROW])
    batch = [AGENTLESS, NULL_AGENT, OTHER_TASK_ROW]

    assert await replace_agent_events(response_id, batch)

    assert await _sse_events(db_conn, response_id) == [
        AGENTLESS, NULL_AGENT, STALE_TASK_ROW, *batch
    ]


async def test_append_waiting_on_the_lock_survives(
    response_row, db_conn, test_db_pool
):
    from src.server.database.conversation import append_sse_event
    from src.server.database.conversation.responses import replace_agent_events

    thread_id = (
        await (
            await db_conn.execute(
                "SELECT conversation_thread_id::text t FROM conversation_responses "
                "WHERE conversation_response_id = %s",
                (response_row,),
            )
        ).fetchone()
    )["t"]
    late = {"event": "context_window", "data": {"agent": "ptc", "action": "compact"}}

    async with test_db_pool.connection() as holder:
        async with holder.transaction():
            await holder.execute(
                "SELECT 1 FROM conversation_responses "
                "WHERE conversation_response_id = %s FOR UPDATE",
                (response_row,),
            )
            replace = asyncio.create_task(
                replace_agent_events(response_row, [STALE_TASK_ROW])
            )
            await asyncio.sleep(0.2)
            assert not replace.done()  # queued behind the lock
            await append_sse_event(thread_id, late, conn=holder)
    assert await replace

    events = await _sse_events(db_conn, response_row)
    assert late in events
    assert events.count(STALE_TASK_ROW) == 1


async def test_a_lane_write_leaves_other_lanes_records(response_row, db_conn):
    """The main lane's records are the finalize's to write; a subagent's
    archive replaces its own lane and nothing else."""
    from src.server.database.conversation.responses import replace_agent_events
    from src.server.database.provenance import (
        MAIN_PROVENANCE_LANE,
        sync_provenance_for_response,
    )

    async with db_conn.transaction():
        await sync_provenance_for_response(
            db_conn,
            conversation_response_id=response_row,
            conversation_thread_id=await _thread_of(db_conn, response_row),
            turn_index=0,
            sse_events=[MAIN_PROVENANCE],
            strict=True,
            lanes=(MAIN_PROVENANCE_LANE,),
        )

    assert await replace_agent_events(response_row, [TASK_PROVENANCE])

    assert await _provenance_identifiers(db_conn, response_row) == [
        "https://a.example",
        "https://b.example",
    ]


async def _thread_of(conn, response_id: str) -> str:
    cur = await conn.execute(
        "SELECT conversation_thread_id FROM conversation_responses "
        "WHERE conversation_response_id = %s",
        (response_id,),
    )
    return str((await cur.fetchone())["conversation_thread_id"])


async def test_a_stripped_agents_provenance_leaves_with_it(
    make_response_row, db_conn
):
    """The strip can take the last provenance entry away; its record goes
    too, rather than outliving the row it was derived from."""
    from src.server.database.conversation.responses import replace_agent_events

    response_id = await make_response_row([MAIN])
    assert await replace_agent_events(response_id, [TASK_PROVENANCE])
    assert await _provenance_identifiers(db_conn, response_id) == [
        "https://b.example"
    ]

    assert await replace_agent_events(response_id, [STALE_TASK_ROW])

    assert await _sse_events(db_conn, response_id) == [MAIN, STALE_TASK_ROW]
    assert await _provenance_identifiers(db_conn, response_id) == []


async def test_missing_row_reports_false(patched_get_db_connection):
    from src.server.database.conversation.responses import replace_agent_events

    assert not await replace_agent_events(str(uuid.uuid4()), [STALE_TASK_ROW])
