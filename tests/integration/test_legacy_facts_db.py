"""Legacy facts against real Postgres.

A legacy row's facts reach its projection only through the replay read, which
must return them with the version of the row they came from; the checkpoint
side is the unit suites' mocked reader. A credit pause settles on the words
its finalize stamped.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage

from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history.replay import legacy
from tests.unit.server.services.history.replay_builders import (
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _turn,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TIP = "cp-tip"
RUN_FACTS = {"v": 1, "reasoning_ms": {"msg_1": [250]}}
# What the turn's archived stream added to its projection, by the projected
# row it follows.
LEGACY = {
    "v": legacy.FACTS_VERSION,
    "merge": {
        "anchors": [["message_chunk", "main", "ai-1", "text"]],
        "signals": [
            {"event": "context_window", "data": {"action": "offload", "kind": "tool_args"}, "at": 0}
        ],
    },
}


@pytest_asyncio.fixture
async def turn(seed_workspace, seed_response, patched_get_db_connection, monkeypatch):
    """A one-turn legacy thread over a checkpoint that projects the turn: its
    completed row keeps the run facts its finalize wrote and the legacy facts
    its archived stream was resolved to."""
    from src.server.database.conversation import create_query, create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    await create_query(
        conversation_query_id=str(uuid.uuid4()),
        conversation_thread_id=thread_id,
        turn_index=0,
        content="question",
        query_type="initial",
    )
    response_id = str(uuid.uuid4())
    await seed_response(response_id, thread_id, 0)

    async def sql(query, params=()):
        async with patched_get_db_connection() as conn:
            cur = await conn.execute(query, params)
            return await cur.fetchone() if cur.description else None

    await sql(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (TIP, thread_id),
    )
    await sql(
        "UPDATE conversation_responses SET replay_facts = %s::jsonb "
        "WHERE conversation_response_id = %s",
        (_json({**RUN_FACTS, LEGACY_KEY: LEGACY}), response_id),
    )
    _cache_probe(monkeypatch)
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=thread_id, turns=[_turn(0, [AIMessage("answer", id="ai-1")])]),
    )

    async def row():
        return await sql(
            "SELECT xmin::text AS xmin "
            "FROM conversation_responses WHERE conversation_response_id = %s",
            (response_id,),
        )

    return SimpleNamespace(thread_id=thread_id, response_id=response_id, sql=sql, row=row)


def _json(value) -> str:
    import json

    return json.dumps(value)


async def _lines(thread_id: str):
    """The thread's turn 0 as a replay projects it now, from its rows."""
    from src.server.database.conversation import get_replay_thread_data
    from src.server.services.history.replay import project_turns

    rows = await get_replay_thread_data(thread_id)
    lines = await project_turns(rows, TIP, turn_indexes=[0])
    return lines[0]


async def test_a_legacy_row_replays_with_the_facts_its_read_returns(turn):
    from src.server.database.conversation import get_replay_responses

    read = (await get_replay_responses([turn.response_id]))[turn.response_id]

    assert read["replay_facts"] == {**RUN_FACTS, LEGACY_KEY: LEGACY}
    assert str(read["xmin"]) == (await turn.row())["xmin"]
    assert [line.event for line in await _lines(turn.thread_id)] == [
        "user_message",
        "message_chunk",
        "context_window",
    ]


async def test_a_credit_pause_settles_on_its_stamped_words(turn, seed_response):
    from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
    from src.server.database.automation_executions import get_settling_run
    from src.server.services.automation_settlement import run_failure_message

    response_id = str(uuid.uuid4())
    await seed_response(response_id, turn.thread_id, 1, status="interrupted")
    await turn.sql(
        "UPDATE conversation_responses SET interrupt_reason = %s, "
        "metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
        "WHERE conversation_response_id = %s",
        (
            INTERRUPT_REASON_CREDIT_PAUSE,
            _json({"credit_pause_message": "Stamped."}),
            response_id,
        ),
    )

    assert run_failure_message(await get_settling_run(response_id)) == "Stamped."
