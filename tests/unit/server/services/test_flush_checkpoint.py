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
from unittest.mock import AsyncMock, MagicMock

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

    mgr = _manager_with_graph(graph, "t", "r")
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
    await mgr._flush_checkpoint("flush-dup", "run1")
    await mgr._flush_checkpoint("flush-dup", "run1")

    after = len((await graph.aget_state(config)).values["messages"])
    assert after == 1, f"flush must not duplicate the message, {before} -> {after}"
