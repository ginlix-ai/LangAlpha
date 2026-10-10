"""A branch walk that fails in Postgres, as replay and its endpoint see it.

The walk raises rather than falling back to the in-process walk, so replay's
own fallback is what keeps a thread readable: ``auto`` serves the stored
events, and ``checkpoint`` (no fallback by request) fails the request.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState
from src.server.database.conversation.replay_rows import ThreadRows
from src.server.services.history.reader import CheckpointHistoryReader
from src.server.services.history.replay import read_replay_page
from src.server.utils import checkpoint_helpers
from tests.conftest import create_test_app

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

STORED = [{"event": "message_chunk", "data": {"content": "stored answer"}}]


@pytest_asyncio.fixture(loop_scope="session")
async def broken_walk(test_db_pool, monkeypatch):
    """A one-turn thread whose branch walk fails on a renamed column, read
    through a real reader on the real saver. Returns the thread's rows."""
    saver = AsyncPostgresSaver(test_db_pool)
    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", lambda s: {"messages": [AIMessage("answer", id="ai-0")]})
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    thread_id = str(uuid.uuid4())
    await graph.ainvoke(
        {"messages": [HumanMessage("question", id="h-0")]},
        {"configurable": {"thread_id": thread_id}, "metadata": {"turn_index": 0}},
    )
    tip = (await graph.aget_state({"configurable": {"thread_id": thread_id}})).config[
        "configurable"
    ]["checkpoint_id"]
    monkeypatch.setattr(CheckpointHistoryReader, "_instance", CheckpointHistoryReader(saver))
    monkeypatch.setattr(
        checkpoint_helpers,
        "_BRANCH_BOUNDARIES_SQL",
        checkpoint_helpers._BRANCH_BOUNDARIES_SQL.replace(
            "parent_checkpoint_id", "parent_id"
        ),
    )
    thread = {"conversation_thread_id": thread_id, "latest_checkpoint_id": tip}
    queries = [
        {"turn_index": 0, "content": "question", "type": "initial", "created_at": "t0"}
    ]
    responses = {
        0: {
            "conversation_response_id": str(uuid.uuid4()),
            "turn_index": 0,
            "status": "completed",
            "sse_events": STORED,
        }
    }
    return ThreadRows(
        thread_id=thread_id,
        owner_id="test-user-123",
        thread=thread,
        queries=queries,
        responses_by_turn=responses,
        usages=[],
        provenance=[],
        run_facts=[],
    )


async def test_auto_replays_the_stored_events(broken_walk):
    page, source = await read_replay_page(broken_walk)
    assert source == "sse"
    assert any("stored answer" in line.data_json for line in page.lines)


async def test_checkpoint_raises_the_walk_error(broken_walk):
    with pytest.raises(psycopg.errors.UndefinedColumn):
        await read_replay_page(broken_walk, source="checkpoint")


@pytest.mark.parametrize(
    "source, status, replayed_from",
    [("auto", 200, "sse"), ("checkpoint", 500, None)],
)
async def test_the_endpoint_answer(broken_walk, source, status, replayed_from):
    from src.server.app.threads import router

    thread_id = broken_walk.thread_id
    with (
        patch(
            "src.server.app.threads.messaging.get_replay_thread_data",
            new=AsyncMock(return_value=broken_walk),
        ),
        patch(
            "src.server.services.history.snapshot.build_thread_snapshot",
            new=AsyncMock(return_value=None),
        ),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=create_test_app(router)),
            base_url="http://test",
        ) as client:
            resp = await client.get(
                f"/api/v1/threads/{thread_id}/messages/replay",
                params={"source": source},
            )

    assert resp.status_code == status
    if replayed_from:
        assert resp.headers["X-Replay-Source"] == replayed_from
        assert "stored answer" in resp.text
    else:
        assert resp.json()["detail"].startswith(
            'Failed to replay thread: column "parent_id" does not exist'
        )
