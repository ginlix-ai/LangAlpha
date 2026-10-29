"""replace_lane_provenance against real Postgres.

A subagent archive rewrites only its own lanes' provenance rows: the main
lane is the finalize's to write.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

MAIN_PROVENANCE = {
    "event": "provenance",
    "data": {"agent": "main", "source_type": "web", "identifier": "https://a.example"},
}
TASK_PROVENANCE = {
    "event": "provenance",
    "data": {
        "agent": "task:abc",
        "tool_call_id": "tc-b",
        "source_type": "web",
        "identifier": "https://b.example",
        "timestamp": "2026-10-13T10:00:00+00:00",
    },
}
LANES = {"task:abc": "abc"}


@pytest_asyncio.fixture(loop_scope="session")
async def response_row(seed_workspace, seed_response):
    from src.server.database.conversation import create_thread

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
    )
    return response_id


async def _lane_rows(conn, response_id: str) -> list[tuple]:
    cur = await conn.execute(
        "SELECT agent, identifier, source_timestamp IS NOT NULL AS stamped "
        "FROM provenance_records WHERE conversation_response_id = %s "
        "ORDER BY identifier",
        (response_id,),
    )
    return [(r["agent"], r["identifier"], r["stamped"]) for r in await cur.fetchall()]


async def _row(conn, response_id: str) -> dict:
    cur = await conn.execute(
        "SELECT conversation_thread_id::text AS thread_id "
        "FROM conversation_responses WHERE conversation_response_id = %s",
        (response_id,),
    )
    return await cur.fetchone()


async def test_a_lane_write_replaces_its_lane_only(response_row, db_conn):
    from src.server.database.conversation.responses import replace_lane_provenance
    from src.server.database.provenance import (
        MAIN_PROVENANCE_LANE,
        sync_provenance_for_response,
    )

    async with db_conn.transaction():
        await sync_provenance_for_response(
            db_conn,
            conversation_response_id=response_row,
            conversation_thread_id=(await _row(db_conn, response_row))["thread_id"],
            turn_index=0,
            provenance=[MAIN_PROVENANCE],
            strict=True,
            lanes=(MAIN_PROVENANCE_LANE,),
        )

    assert await replace_lane_provenance(response_row, [TASK_PROVENANCE], LANES)
    assert await replace_lane_provenance(response_row, [TASK_PROVENANCE], LANES)

    assert await _lane_rows(db_conn, response_row) == [
        ("main", "https://a.example", False),
        ("task:abc", "https://b.example", True),
    ]

    # A later write with none of the lane's sources saved empties it.
    assert await replace_lane_provenance(response_row, [], LANES)

    assert await _lane_rows(db_conn, response_row) == [
        ("main", "https://a.example", False),
    ]


async def test_missing_row_reports_false(patched_get_db_connection):
    from src.server.database.conversation.responses import replace_lane_provenance

    assert not await replace_lane_provenance(
        str(uuid.uuid4()), [TASK_PROVENANCE], LANES
    )
