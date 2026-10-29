"""Graphs and reads shared by the checkpoint-reader suites: a real
DeltaAgentState graph writes the turns, and ``_history`` reads them back the
way replay does."""

from __future__ import annotations

from types import SimpleNamespace

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState

THREAD = "thread-1"


def _echo_graph(checkpointer):
    """One turn = reply 'echo: <last human>' with a stable per-turn id."""

    def agent(state):
        humans = [m for m in state["messages"] if isinstance(m, HumanMessage)]
        last = humans[-1].content if humans else "?"
        return {
            "messages": [AIMessage(content=f"echo: {last}", id=f"ai-{len(state['messages'])}")]
        }

    return (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=checkpointer)
    )


def _cfg(thread_id=THREAD, *, turn_index=None, run_id=None, checkpoint_id=None):
    cfg = {"configurable": {"thread_id": thread_id}}
    if checkpoint_id:
        cfg["configurable"]["checkpoint_id"] = checkpoint_id
    metadata = {}
    if run_id is not None:
        metadata["run_id"] = run_id
    if turn_index is not None:
        metadata["turn_index"] = turn_index
    if metadata:
        cfg["metadata"] = metadata
    return cfg


async def _history(reader, thread_id=THREAD, tip=None):
    """Every turn on the branch, the interrupts pending at its tip and the
    tip's ui records, read the way replay reads them."""
    anchors, tip_id = await reader.aget_turn_anchors(thread_id, tip)
    if not anchors:
        return SimpleNamespace(turns=[], interrupts=[], ui=[])
    tip_state = await reader.aget_state(thread_id, tip_id)
    return SimpleNamespace(
        turns=await reader.aget_turn_slices(thread_id, anchors),
        interrupts=await reader.aget_tip_interrupts(thread_id, tip_id),
        ui=list(tip_state.values.get("ui") or []),
    )


async def _run_turns(graph, n, start=0):
    for i in range(start, start + n):
        await graph.ainvoke(
            {"messages": [HumanMessage(content=f"q{i}", id=f"h-{i}")]},
            _cfg(turn_index=i, run_id=f"run-{i}"),
        )
