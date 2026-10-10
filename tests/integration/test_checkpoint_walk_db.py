"""The branch walk on real Postgres agrees with the in-process walk.

The table path restates ``is_turn_boundary`` and ``_resumed_by_later_run`` in
SQL, and the ``alist`` walk other savers take runs them in Python. A boundary
rule added to only one side shows up here as the two paths disagreeing.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph
from langgraph.types import Command, interrupt

from ptc_agent.agent.state import DeltaAgentState
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
