"""An older checkpoint's messages, rebuilt by the Postgres saver past its first page.

The saver rebuilds the ``messages`` DeltaChannel by paging a thread's
checkpoints newest-first, so a checkpoint off the first page is what every
early turn of a long thread is. The page size is shrunk here rather than
writing 1024 checkpoints: which page the target lands on is all that decides
the behaviour. Each test writes its own thread.
"""

from __future__ import annotations

import uuid

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres import aio as postgres_aio
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _reply(state: DeltaAgentState) -> dict:
    question = state["messages"][-1]
    return {"messages": [AIMessage(f"re {question.content}", id=f"a-{question.id}")]}


async def _four_turns(test_db_pool, seen: list[list[str]]):
    """A thread of four turns, plus the second turn's input checkpoint: the
    state the first turn ended on, many checkpoints back from the head."""

    def reply(state: DeltaAgentState) -> dict:
        seen.append([m.id for m in state["messages"]])
        return _reply(state)

    saver = AsyncPostgresSaver(test_db_pool)
    graph = (
        StateGraph(DeltaAgentState)
        .add_node("reply", reply)
        .add_edge(START, "reply")
        .compile(checkpointer=saver)
    )
    thread = {"configurable": {"thread_id": f"delta-paging-{uuid.uuid4()}"}}
    for i in range(4):
        await graph.ainvoke({"messages": [HumanMessage(f"q{i}", id=f"q{i}")]}, thread)
    inputs = [  # newest-first
        cp async for cp in saver.alist(thread) if cp.metadata.get("source") == "input"
    ]
    return graph, inputs[-2].config


@pytest.mark.parametrize("page_size", [1024, 3, 1])
async def test_an_early_turn_keeps_its_messages_off_the_first_page(
    test_db_pool, monkeypatch, page_size
):
    monkeypatch.setattr(postgres_aio, "_DELTA_PAGE_SIZE", page_size)
    graph, second_turn = await _four_turns(test_db_pool, [])

    state = await graph.aget_state(second_turn)

    assert [m.id for m in state.values["messages"]] == ["q0", "a-q0"]


@pytest.mark.parametrize("page_size", [1024, 3, 1])
async def test_an_edit_of_an_early_turn_runs_with_the_history_before_it(
    test_db_pool, monkeypatch, page_size
):
    """An edit or a regenerate runs the graph from an older checkpoint, which
    loads its channels the same way, so the model reads what it read then."""
    monkeypatch.setattr(postgres_aio, "_DELTA_PAGE_SIZE", page_size)
    seen: list[list[str]] = []
    graph, second_turn = await _four_turns(test_db_pool, seen)

    await graph.ainvoke({"messages": [HumanMessage("q1 edited", id="e1")]}, second_turn)

    assert seen[-1] == ["q0", "a-q0", "e1"]
