"""Edit and regenerate against real Postgres, on a thread with a turn whose
run died before its first checkpoint.

That turn holds a turn number and rows but no boundary on the branch, so
every turn after it sits one place lower among the boundaries than its
number. A fork deletes rows from the turn it replaces onward, and the
turn it replaces is fixed by the checkpoint it forks at: deleting by
position would drop the rows of a turn the branch keeps.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import HTTPException
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

DEAD_TURN = 1


def _graph(saver):
    def reply(state):
        n = len(state["messages"])
        return {"messages": [AIMessage(content=f"reply {n}", id=f"ai-{uuid.uuid4()}")]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("reply", reply)
        .add_edge(START, "reply")
        .compile(checkpointer=saver)
    )


async def _start(thread_id, text, *, fork=None, turn_index=None, query=True):
    from src.server.database.runs.lifecycle import QuerySpec, start_run

    run_id = str(uuid.uuid4())
    row = await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        turn_index=turn_index,
        query=(
            QuerySpec(query_id=str(uuid.uuid4()), content=text, query_type="initial")
            if query
            else None
        ),
        fork=fork,
        metadata={"msg_type": "ptc"},
    )
    return run_id, row["turn_index"]


async def _run(graph, thread_id, run_id, turn_index, text, *, checkpoint_id=None):
    """The graph half of a turn: stamped the way ``build_graph_config`` stamps
    a run, then committed the way the finalize commits it."""
    from src.server.database.runs.lifecycle import finalize_run

    configurable = {"thread_id": thread_id}
    if checkpoint_id:
        configurable["checkpoint_id"] = checkpoint_id
    config = {
        "configurable": configurable,
        "metadata": {"run_id": run_id, "turn_index": turn_index},
    }
    payload = {"messages": [HumanMessage(content=text)]} if text else None
    await graph.ainvoke(payload, config)
    state = await graph.aget_state({"configurable": {"thread_id": thread_id}})
    await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        status="completed",
        checkpoint_id=state.config["configurable"]["checkpoint_id"],
    )


@pytest_asyncio.fixture(loop_scope="session")
async def thread(seed_workspace, test_db_pool, patched_get_db_connection):
    """Turns 0, 2 and 3 ran; turn 1's run failed before the graph wrote
    anything."""
    from src.server.database.conversation import create_thread
    from src.server.database.runs.lifecycle import finalize_run

    saver = AsyncPostgresSaver(test_db_pool)
    graph = _graph(saver)
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    runs = {}
    for text in ("q0", "q1", "q2", "q3"):
        run_id, turn_index = await _start(thread_id, text)
        runs[turn_index] = run_id
        if turn_index == DEAD_TURN:
            await finalize_run(run_id=run_id, thread_id=thread_id, status="error")
        else:
            await _run(graph, thread_id, run_id, turn_index, text)
    with patch(
        "src.server.handlers.checkpoint_handler.get_checkpointer",
        return_value=saver,
    ):
        yield SimpleNamespace(id=thread_id, graph=graph, runs=runs)
    await saver.adelete_thread(thread_id)


async def _turns(thread_id):
    from src.server.database.conversation import get_thread_checkpoint_id
    from src.server.handlers.checkpoint_handler import get_thread_turns

    tip = await get_thread_checkpoint_id(thread_id)
    return (await get_thread_turns(thread_id, branch_tip_checkpoint_id=tip)).turns


async def _fork(thread_id, checkpoint_id, client_turn, *, regenerate):
    """What the send route hands ``start_run`` for a fork request."""
    from src.server.handlers.checkpoint_handler import resolve_fork_turn
    from src.server.services.runs.coordinator import ForkSpec

    return ForkSpec(
        from_turn=await resolve_fork_turn(
            thread_id, checkpoint_id, regenerate=regenerate, requested=client_turn
        ),
        checkpoint_id=checkpoint_id,
        preserve_query_at_fork=regenerate,
    )


async def _rows(conn, table, thread_id):
    cur = await conn.execute(
        f"SELECT turn_index FROM {table} WHERE conversation_thread_id = %s "
        "ORDER BY turn_index",
        (thread_id,),
    )
    return [r["turn_index"] for r in await cur.fetchall()]


async def test_turns_carry_the_turn_number_not_the_position(thread):
    turns = await _turns(thread.id)

    assert [t.turn_index for t in turns] == [0, 2, 3]


async def test_editing_the_latest_turn_keeps_every_turn_before_it(thread, db_conn):
    latest = (await _turns(thread.id))[-1]

    fork = await _fork(
        thread.id, latest.edit_checkpoint_id, latest.turn_index, regenerate=False
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q3 edited",
        checkpoint_id=latest.edit_checkpoint_id,
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    # Every boundary on the new branch still names a turn with rows.
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 3]


async def test_regenerating_the_latest_turn_keeps_its_question(thread, db_conn):
    latest = (await _turns(thread.id))[-1]

    fork = await _fork(
        thread.id, latest.regenerate_checkpoint_id, latest.turn_index, regenerate=True
    )
    _, turn_index = await _start(
        thread.id, "", fork=fork, turn_index=fork.from_turn, query=False
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]


async def test_a_client_count_cannot_move_the_fork(thread, db_conn):
    """A client that counted boundaries names the turn before the one its
    checkpoint replaces; the checkpoint decides."""
    latest = (await _turns(thread.id))[-1]

    edit = await _fork(thread.id, latest.edit_checkpoint_id, 2, regenerate=False)
    regenerate = await _fork(
        thread.id, latest.regenerate_checkpoint_id, 2, regenerate=True
    )

    assert (edit.from_turn, regenerate.from_turn) == (3, 3)
    await _start(thread.id, "q3 edited", fork=edit)
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]


async def test_rewriting_the_failed_message_replaces_it(thread, db_conn):
    """Turn 1 left nothing on the branch, so its message forks where turn 2's
    does; the fork drops turn 1 and every turn after it."""
    _, second, _ = await _turns(thread.id)

    fork = await _fork(thread.id, second.edit_checkpoint_id, DEAD_TURN, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q1 again", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q1 again",
        checkpoint_id=second.edit_checkpoint_id,
    )

    assert turn_index == DEAD_TURN
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1]
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 1]


async def test_a_fork_at_no_turn_of_the_branch_is_refused(thread, db_conn):
    from src.server.handlers.checkpoint_handler import resolve_fork_turn

    latest = (await _turns(thread.id))[-1]

    with pytest.raises(HTTPException) as refused:
        # A regenerate names the turn's own boundary, never its parent.
        await resolve_fork_turn(thread.id, latest.edit_checkpoint_id, regenerate=True)
    assert refused.value.status_code == 409
    with pytest.raises(HTTPException):
        await resolve_fork_turn(thread.id, "not-a-checkpoint", regenerate=False)
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
