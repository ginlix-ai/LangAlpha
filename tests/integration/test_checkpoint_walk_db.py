"""The branch walk on real Postgres agrees with the in-process walk.

The table path restates ``is_turn_boundary`` and ``_resumed_by_later_run`` in
SQL, and the ``alist`` walk other savers take runs them in Python. A boundary
rule added to only one side shows up here as the two paths disagreeing.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import psycopg
import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph
from langgraph.types import Command, interrupt

from ptc_agent.agent.state import DeltaAgentState
from src.server.handlers import checkpoint_handler
from src.server.utils import checkpoint_helpers
from src.server.utils.checkpoint_helpers import (
    CheckpointBranchTipNotFound,
    _walk_via_alist,
    _walk_via_tables,
    walk_current_branch_boundaries,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def saver(test_db_pool):
    return AsyncPostgresSaver(test_db_pool)  # binds the running loop


def _graph(saver, *, asks=True):
    """``ask`` waits on the user and ``reply`` follows. ``asks=False`` is
    another agent holding the same node, one that runs without asking."""

    def ask(state):
        if not asks:
            return {"messages": [AIMessage(content="refused", id="ai-refused")]}
        answer = interrupt({"action_requests": [{"description": "go?"}]})
        return {"messages": [AIMessage(content=f"asked: {answer}", id="ai-asked")]}

    def reply(state):
        return {"messages": [AIMessage(content="reply", id=f"ai-{len(state['messages'])}")]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("ask", ask)
        .add_node("reply", reply)
        .add_edge(START, "ask")
        .add_edge("ask", "reply")
        .compile(checkpointer=saver)
    )


def _cfg(thread_id, *, run_id=None, checkpoint_id=None):
    cfg = {"configurable": {"thread_id": thread_id}}
    if checkpoint_id:
        cfg["configurable"]["checkpoint_id"] = checkpoint_id
    if run_id:
        cfg["metadata"] = {"run_id": run_id}
    return cfg


async def _interrupted_turn(graph, thread_id):
    await graph.ainvoke(
        {"messages": [HumanMessage(content="q0", id="h-0")]},
        _cfg(thread_id, run_id="run-0"),
    )
    state = await graph.aget_state(_cfg(thread_id))
    return state.config["configurable"]["checkpoint_id"], state.interrupts[0].id


async def _both_walks(saver, thread_id, tip=None, strict=False):
    """Walk both paths, assert they agree, and return the boundary ids."""
    tables = await _walk_via_tables(saver, thread_id, tip, strict, "")
    assert tables == await _walk_via_alist(saver, thread_id, tip, strict, "")
    return [b.checkpoint_id for b in tables[0]]


async def test_resume_of_cancelled_calls_is_its_own_boundary(saver):
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    interrupt_tip, interrupt_id = await _interrupted_turn(graph, thread)
    await graph.aupdate_state(
        _cfg(thread),
        {"messages": [AIMessage(content="Not run", id="ai-cancelled")]},
        as_node="ask",
    )
    await graph.ainvoke(Command(resume={interrupt_id: "yes"}), _cfg(thread, run_id="run-1"))

    boundaries = await _both_walks(saver, thread)
    assert len(boundaries) == 2
    assert boundaries[1] == interrupt_tip


async def test_a_retry_at_an_update_the_thread_moved_past_keeps_its_turns(saver):
    """langgraph 1.2.16 opens such a retry with a fork checkpoint, which the
    later-run rule reads past as it does the update beneath it."""
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    interrupt_tip, interrupt_id = await _interrupted_turn(graph, thread)
    await graph.aupdate_state(
        _cfg(thread),
        {"messages": [AIMessage(content="Not run", id="ai-cancelled")]},
        as_node="ask",
    )
    update = (await graph.aget_state(_cfg(thread))).config["configurable"]["checkpoint_id"]
    await graph.ainvoke(Command(resume={interrupt_id: "yes"}), _cfg(thread, run_id="run-1"))
    assert (await _both_walks(saver, thread))[1] == interrupt_tip

    await graph.ainvoke(None, _cfg(thread, run_id="run-2", checkpoint_id=update))

    boundaries = await _both_walks(saver, thread)
    assert len(boundaries) == 2
    assert boundaries[1] == interrupt_tip


async def test_resume_by_an_agent_that_does_not_ask_is_its_own_boundary(saver):
    thread = str(uuid.uuid4())
    interrupt_tip, interrupt_id = await _interrupted_turn(_graph(saver), thread)
    await _graph(saver, asks=False).ainvoke(
        Command(resume={interrupt_id: "yes"}), _cfg(thread, run_id="run-1")
    )

    assert (await _both_walks(saver, thread))[1:] == [interrupt_tip]


async def test_interrupt_continued_by_its_own_run_is_no_boundary(saver):
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    await _interrupted_turn(graph, thread)
    await graph.aupdate_state(
        _cfg(thread, run_id="run-0"),
        {"messages": [AIMessage(content="note", id="ai-note")]},
        as_node="ask",
    )
    await graph.ainvoke(None, _cfg(thread, run_id="run-0"))

    assert len(await _both_walks(saver, thread)) == 1


async def test_new_message_over_an_unanswered_interrupt_is_one_boundary(saver):
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    interrupt_tip, _ = await _interrupted_turn(graph, thread)
    await graph.ainvoke(
        {"messages": [HumanMessage(content="q1", id="h-1")]},
        _cfg(thread, run_id="run-1"),
    )

    boundaries = await _both_walks(saver, thread)
    assert len(boundaries) == 2
    assert interrupt_tip not in boundaries


async def test_answered_interrupt_and_an_interrupt_at_the_tip(saver):
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    _, interrupt_id = await _interrupted_turn(graph, thread)
    assert len(await _both_walks(saver, thread)) == 1  # nothing after it yet

    await graph.ainvoke(Command(resume={interrupt_id: "yes"}), _cfg(thread, run_id="run-1"))
    assert len(await _both_walks(saver, thread)) == 2  # the resume writes __resume__
    boundaries, _ = await _walk_via_tables(saver, thread, None, False, "")
    assert not boundaries[1].is_input
    assert boundaries[1].interrupts == [
        {"id": interrupt_id, "value": {"action_requests": [{"description": "go?"}]}}
    ]


async def test_asks_raised_in_one_step_list_in_the_savers_order(saver):
    """checkpoint-postgres 3.2 loads a step's writes by task path, not task id,
    so the SQL walk has to sort the same way or list parallel asks shuffled."""
    names = ["alpha", "bravo", "charlie", "delta"]

    def asker(name):
        def node(state):
            return {"messages": [AIMessage(content=interrupt(name), id=f"ai-{name}")]}

        return node

    builder = StateGraph(DeltaAgentState)
    for name in names:
        builder.add_node(name, asker(name)).add_edge(START, name)
    graph = builder.compile(checkpointer=saver)
    # Task ids are random per checkpoint, so several threads all but ensure
    # one where task-id order is not task-path order.
    for _ in range(4):
        thread = str(uuid.uuid4())
        await graph.ainvoke(
            {"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg(thread)
        )
        state = await graph.aget_state(_cfg(thread))
        resume = {i.id: "ok" for i in state.interrupts}
        await graph.ainvoke(Command(resume=resume), _cfg(thread))
        await _both_walks(saver, thread)


async def test_an_answered_turn_regenerates_with_its_answer(saver, monkeypatch):
    """The answered turn's regenerate checkpoint holds the answer, so the
    reply reruns and nothing asks. At the resume boundary itself langgraph
    drops the answer and asks again, beside the turn's kept answer row."""
    thread = str(uuid.uuid4())
    graph = _graph(saver)
    interrupt_tip, interrupt_id = await _interrupted_turn(graph, thread)
    await graph.ainvoke(Command(resume={interrupt_id: "yes"}), _cfg(thread, run_id="run-1"))
    await _both_walks(saver, thread)  # the two walks find the same answered step

    monkeypatch.setattr(checkpoint_handler, "get_checkpointer", lambda: saver)
    monkeypatch.setattr(
        checkpoint_handler.queries_db,
        "get_query_turn_indexes",
        AsyncMock(return_value=[0, 1]),
    )
    turns = (await checkpoint_handler.get_thread_turns(thread)).turns
    regenerate_at = turns[1].regenerate_checkpoint_id
    assert regenerate_at != interrupt_tip

    await graph.ainvoke(None, _cfg(thread, run_id="run-2", checkpoint_id=regenerate_at))
    state = await graph.aget_state(_cfg(thread))
    assert not state.interrupts
    assert [m.content for m in state.values["messages"]].count("asked: yes") == 1

    await graph.ainvoke(None, _cfg(thread, run_id="run-3", checkpoint_id=interrupt_tip))
    assert (await graph.aget_state(_cfg(thread))).interrupts


def _two_question_graph(saver):
    """The answered step's next step asks again: an order approved at the
    gate whose tools step then asks the user something."""

    def gate(state):
        answer = interrupt({"action_requests": [{"description": "place it?"}]})
        return {"messages": [AIMessage(content=f"gate: {answer}", id="ai-gate")]}

    def ask(state):
        answer = interrupt({"action_requests": [{"description": "which one?"}]})
        return {"messages": [AIMessage(content=f"asked: {answer}", id="ai-asked")]}

    def reply(state):
        return {"messages": [AIMessage(content="reply", id=f"ai-{len(state['messages'])}")]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("gate", gate)
        .add_node("ask", ask)
        .add_node("reply", reply)
        .add_edge(START, "gate")
        .add_edge("gate", "ask")
        .add_edge("ask", "reply")
        .compile(checkpointer=saver)
    )


async def test_an_answered_step_that_asked_again_keeps_its_boundary(
    saver, monkeypatch
):
    """Turn 1's answered step finished on the checkpoint turn 2's answer is
    written on. A fork there deletes turn 2 and leaves that answer behind,
    so turn 1 regenerates at its own boundary and no regenerate checkpoint
    opens another turn."""
    thread = str(uuid.uuid4())
    graph = _two_question_graph(saver)
    gate_tip, gate_id = await _interrupted_turn(graph, thread)
    await graph.ainvoke(Command(resume={gate_id: "yes"}), _cfg(thread, run_id="run-1"))
    ask_state = await graph.aget_state(_cfg(thread))
    ask_tip = ask_state.config["configurable"]["checkpoint_id"]
    await graph.ainvoke(
        Command(resume={ask_state.interrupts[0].id: "this"}), _cfg(thread, run_id="run-2")
    )
    assert (await _both_walks(saver, thread))[1:] == [gate_tip, ask_tip]

    monkeypatch.setattr(checkpoint_handler, "get_checkpointer", lambda: saver)
    monkeypatch.setattr(
        checkpoint_handler.queries_db,
        "get_query_turn_indexes",
        AsyncMock(return_value=[0, 1, 2]),
    )
    turns = (await checkpoint_handler.get_thread_turns(thread)).turns
    assert [t.regenerate_checkpoint_id for t in turns][1] == gate_tip
    assert turns[2].regenerate_checkpoint_id not in (gate_tip, ask_tip)


async def test_a_fork_walks_only_its_own_branch(saver):
    thread = str(uuid.uuid4())
    graph = _graph(saver, asks=False)
    await graph.ainvoke({"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg(thread))
    first_turn_tip = (await graph.aget_state(_cfg(thread))).config["configurable"]["checkpoint_id"]
    await graph.ainvoke({"messages": [HumanMessage(content="q1", id="h-1")]}, _cfg(thread))
    old_tip = (await graph.aget_state(_cfg(thread))).config["configurable"]["checkpoint_id"]
    await graph.ainvoke(
        {"messages": [HumanMessage(content="q1 edited", id="h-1b")]},
        _cfg(thread, checkpoint_id=first_turn_tip),
    )

    newest = await _both_walks(saver, thread)
    old = await _both_walks(saver, thread, tip=old_tip, strict=True)
    assert len(newest) == len(old) == 2
    assert newest[0] == old[0] and newest[1] != old[1]

    with pytest.raises(CheckpointBranchTipNotFound):
        await _walk_via_tables(saver, thread, "missing", True, "")
    with pytest.raises(CheckpointBranchTipNotFound):
        await _walk_via_alist(saver, thread, "missing", True, "")


async def test_a_failed_sql_walk_raises_rather_than_walking_in_process(
    saver, monkeypatch
):
    """Schema drift (a renamed column, say) fails the read where it happened
    instead of decoding the whole thread on the event loop."""
    thread = str(uuid.uuid4())
    graph = _graph(saver, asks=False)
    await graph.ainvoke({"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg(thread))
    monkeypatch.setattr(
        checkpoint_helpers,
        "_BRANCH_BOUNDARIES_SQL",
        checkpoint_helpers._BRANCH_BOUNDARIES_SQL.replace(
            "parent_checkpoint_id", "parent_id"
        ),
    )

    async def no_alist(*args, **kwargs):
        raise AssertionError("walked in process")

    monkeypatch.setattr(checkpoint_helpers, "_walk_via_alist", no_alist)
    with pytest.raises(psycopg.errors.UndefinedColumn):
        await walk_current_branch_boundaries(saver, thread)
