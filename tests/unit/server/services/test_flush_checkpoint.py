"""`_flush_checkpoint` must exclude `messages` from the re-write (compaction Bug A).

On user-stop the flush reads the committed snapshot and re-applies it via
`aupdate_state`. Re-writing the full message list re-applies every message as a
DeltaChannel delta, and any still-id-less tail message appends as a duplicate.
The flush now strips `messages` from the payload: the committed messages already
carry forward on the channel, and the remaining private keys are last-write-wins
so re-writing them stays idempotent.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from ptc_agent.agent.state import DeltaAgentState
from src.server.services.runs.executor import (
    LocalRunExecutor,
    LocalRunExecution,
    LocalRunStatus,
)


def _manager_with_graph(graph, thread_id: str, run_id: str) -> LocalRunExecutor:
    """A LocalRunExecutor holding just the one task — `_flush_checkpoint`
    only touches `self.executions` and `self.task_lock`, so skip the config-heavy
    __init__ entirely."""
    mgr = LocalRunExecutor.__new__(LocalRunExecutor)
    mgr.executions = {}
    mgr.task_lock = asyncio.Lock()
    mgr.executions[(thread_id, run_id)] = LocalRunExecution(
        thread_id=thread_id,
        run_id=run_id,
        status=LocalRunStatus.RUNNING,
        created_at=datetime.now(),
        graph=graph,
    )
    return mgr


def _the_run_wrote_the_newest_step(run_id: str):
    """The stop's run wrote the thread's newest step, so it has one to flush."""
    from src.server.database.runs.lifecycle import NewestCheckpoint

    return patch(
        "src.server.utils.checkpoint_helpers.newest_checkpoint",
        new=AsyncMock(return_value=NewestCheckpoint("newest", run_id)),
    )


def _build_graph(saver):
    builder = StateGraph(DeltaAgentState)

    def noop(_state: DeltaAgentState) -> dict:
        return {}

    builder.add_node("noop", noop)
    builder.add_edge(START, "noop")
    builder.add_edge("noop", END)
    return builder.compile(checkpointer=saver)


@pytest.mark.asyncio
async def test_flush_excludes_messages_keeps_private_keys():
    """The re-write payload drops `messages` and keeps the private state keys."""
    graph = MagicMock()
    snapshot = MagicMock()
    snapshot.values = {
        "messages": [HumanMessage(content="x", id="m1")],
        "_summarization_event": {"cutoff_index": 3, "anchor_message_id": "m1"},
        "_offloaded_tool_call_ids": {"tc1"},
    }
    graph.aget_state = AsyncMock(return_value=snapshot)
    graph.aupdate_state = AsyncMock(return_value=None)
    graph.channels = _build_graph(InMemorySaver()).channels

    mgr = _manager_with_graph(graph, "t", "r")
    with _the_run_wrote_the_newest_step("r"):
        await mgr._flush_checkpoint("t", "r")

    graph.aupdate_state.assert_awaited_once()
    payload = graph.aupdate_state.await_args.args[1]
    assert "messages" not in payload
    assert payload["_summarization_event"]["cutoff_index"] == 3
    assert payload["_offloaded_tool_call_ids"] == {"tc1"}


@pytest.mark.asyncio
async def test_flush_does_not_duplicate_idless_message():
    """End-to-end on a real DeltaChannel graph: an id-less injected message is
    not duplicated no matter how many times the flush runs."""
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "flush-dup"}}
    await graph.aupdate_state(
        config, {"messages": [HumanMessage(content="hi")]}, as_node="noop"
    )
    before = len((await graph.aget_state(config)).values["messages"])
    assert before == 1

    mgr = _manager_with_graph(graph, "flush-dup", "run1")
    with _the_run_wrote_the_newest_step("run1"):
        await mgr._flush_checkpoint("flush-dup", "run1")
        await mgr._flush_checkpoint("flush-dup", "run1")

    after = len((await graph.aget_state(config)).values["messages"])
    assert after == 1, f"flush must not duplicate the message, {before} -> {after}"



def _card_graph(saver):
    from langchain_core.messages import AIMessage
    from langgraph.types import interrupt

    def ask(_state: DeltaAgentState) -> dict:
        answer = interrupt({"question": "which ticker?"})
        return {"messages": [AIMessage(content=answer, id="ai-answer")]}

    builder = StateGraph(DeltaAgentState)
    builder.add_node("ask", ask)
    builder.add_edge(START, "ask")
    builder.add_edge("ask", END)
    return builder.compile(checkpointer=saver)


@pytest.mark.asyncio
async def test_a_stop_before_its_first_step_leaves_a_waiting_card_answerable():
    """The stopped run wrote nothing, so nothing is flushed over the card it
    found waiting: the user's answer resumes it instead of asking again."""
    from langgraph.types import Command

    graph = _card_graph(InMemorySaver())
    thread = {"configurable": {"thread_id": "card"}}
    await graph.ainvoke(
        {"messages": [HumanMessage(content="ask")]},
        {**thread, "metadata": {"run_id": "paused"}},
    )
    card = (await graph.aget_state(thread)).interrupts[0].id

    mgr = _manager_with_graph(graph, "card", "stopped")
    await mgr._flush_checkpoint("card", "stopped")
    await graph.ainvoke(
        Command(resume={card: "AAPL"}), {**thread, "metadata": {"run_id": "answer"}}
    )

    state = await graph.aget_state(thread)
    assert state.interrupts == ()
    assert state.values["messages"][-1].content == "AAPL"




@pytest.mark.asyncio
async def test_a_stalled_ownership_read_gives_up_at_the_flush_timeout():
    """The subagent teardown waits on the flush, so a checkpointer that stalls
    must hold the stop no longer than the flush's own bound."""

    async def _stalled(*_args, **_kwargs):
        await asyncio.Event().wait()

    graph = MagicMock()
    graph.aupdate_state = AsyncMock()
    mgr = _manager_with_graph(graph, "t", "r")
    with (
        patch("src.server.utils.checkpoint_helpers.newest_checkpoint", new=_stalled),
        patch(
            "src.server.services.runs.executor.get_checkpoint_flush_timeout",
            return_value=0.05,
        ),
    ):
        await asyncio.wait_for(mgr._flush_checkpoint("t", "r"), 2)

    graph.aupdate_state.assert_not_awaited()



@pytest.mark.asyncio
async def test_the_ownership_read_and_the_flush_share_one_deadline():
    """The next send's admission allows the stop one flush timeout, so a slow
    ownership read leaves the flush only what remains of it."""
    from src.server.database.runs.lifecycle import NewestCheckpoint

    async def _slow_owned(*_args, **_kwargs):
        await asyncio.sleep(0.4)
        return NewestCheckpoint("newest", "r")

    async def _stalled(*_args, **_kwargs):
        await asyncio.Event().wait()

    graph = MagicMock()
    graph.aget_state = _stalled
    mgr = _manager_with_graph(graph, "t", "r")
    loop = asyncio.get_running_loop()
    with (
        patch("src.server.utils.checkpoint_helpers.newest_checkpoint", new=_slow_owned),
        patch(
            "src.server.services.runs.executor.get_checkpoint_flush_timeout",
            return_value=0.5,
        ),
        patch(
            "src.server.services.runs.teardown.get_checkpoint_flush_timeout",
            return_value=0.5,
        ),
    ):
        started = loop.time()
        await asyncio.wait_for(mgr._flush_checkpoint("t", "r"), 2)

    assert loop.time() - started < 0.75
