"""Runs started from a stored checkpoint store each message once.

A regenerate replays a turn's input checkpoint, a retry the failed step's
checkpoint, and an edit starts new input at an older one. Each reruns the
tasks scheduled there, so ``messages`` must not also replay the writes those
tasks made the first time, which langgraph (>=1.2.13) ensures by snapshotting
the channel in the new branch's first checkpoint. Inputs arrive as
the id-less dicts the chat path builds, the shape that left a regenerated
turn's message stored twice.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Send, interrupt

from ptc_agent.agent.state import DeltaAgentState

pytestmark = pytest.mark.asyncio


_RECORD = {"type": "ui", "id": "ui-note", "name": "note", "props": {}, "metadata": {}}


def _user(text: str) -> dict:
    return {"messages": [{"role": "user", "content": text}]}


def _said(messages) -> list[str]:
    return [f"{m.type}:{m.content}" for m in messages]


def _chat(saver, seen: list[list[str]]):
    def reply(state):
        seen.append(_said(state["messages"]))
        answered = sum(m.type == "ai" for m in state["messages"])
        return {"messages": [AIMessage(f"a{answered}", id=str(uuid.uuid4()))]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("reply", reply)
        .add_edge(START, "reply")
        .compile(checkpointer=saver)
    )


def _tools(saver, b_does: dict):
    """An agent step that calls two tools as parallel tasks, then answers."""

    def agent(state):
        if state["messages"][-1].type == "tool":
            return {"messages": [AIMessage("done", id=str(uuid.uuid4()))]}
        calls = [
            {"name": name, "args": {}, "id": f"{name}-{uuid.uuid4()}"} for name in "ab"
        ]
        return {"messages": [AIMessage("", tool_calls=calls, id=str(uuid.uuid4()))]}

    def route(state):
        calls = getattr(state["messages"][-1], "tool_calls", None) or []
        return [Send(f"tool_{c['name']}", c) for c in calls] or END

    def tool_a(call):
        return {"messages": [ToolMessage("result a", tool_call_id=call["id"])]}

    async def tool_b(call):
        if b_does.pop("fail", False):
            await asyncio.sleep(0.05)  # tool_a stores its result first
            raise RuntimeError("upstream timeout")
        if b_does.get("ask"):
            interrupt("approve b?")
        return {"messages": [ToolMessage("result b", tool_call_id=call["id"])]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_node("tool_a", tool_a)
        .add_node("tool_b", tool_b)
        .add_edge(START, "agent")
        .add_conditional_edges("agent", route, ["tool_a", "tool_b", END])
        .add_edge("tool_a", "agent")
        .add_edge("tool_b", "agent")
        .compile(checkpointer=saver)
    )


async def _input_checkpoints(graph, thread) -> list:
    """Newest first, the way a thread's turns are walked."""
    return [
        s
        async for s in graph.aget_state_history(thread)
        if s.metadata.get("source") == "input"
    ]


async def _head(graph, thread) -> list:
    return (await graph.aget_state(thread)).values["messages"]


def _results_per_call(messages) -> Counter:
    return Counter(m.tool_call_id for m in messages if m.type == "tool")


async def _fail_with_a_finished_sibling(graph, thread):
    with pytest.raises(RuntimeError):
        await graph.ainvoke(_user("go"), thread)
    tip = await graph.checkpointer.aget_tuple((await graph.aget_state(thread)).config)
    assert any(channel == "messages" for _, channel, _ in tip.pending_writes)


async def test_a_regenerate_stores_the_turns_message_once():
    seen: list[list[str]] = []
    graph = _chat(InMemorySaver(), seen)
    thread = {"configurable": {"thread_id": "regen"}}
    for text in ("q0", "q1"):
        await graph.ainvoke(_user(text), thread)

    await graph.ainvoke(None, (await _input_checkpoints(graph, thread))[0].config)
    await graph.ainvoke(_user("q2"), thread)

    assert seen[-2] == ["human:q0", "ai:a0", "human:q1"]
    assert seen[-1] == ["human:q0", "ai:a0", "human:q1", "ai:a1", "human:q2"]


async def test_a_regenerate_of_the_first_turn_stores_its_message_once():
    """The first turn's input checkpoint is the thread's root, where
    ``messages`` has no version yet for a snapshot to be stored under."""
    graph = _chat(InMemorySaver(), [])
    thread = {"configurable": {"thread_id": "regen-root"}}
    for text in ("q0", "q1"):
        await graph.ainvoke(_user(text), thread)

    await graph.ainvoke(None, (await _input_checkpoints(graph, thread))[-1].config)

    assert _said(await _head(graph, thread)) == ["human:q0", "ai:a0"]


async def test_a_retry_stores_the_finished_sibling_tool_result_once():
    """Two results for one call is a request every provider refuses."""
    b_does = {"fail": True}
    graph = _tools(InMemorySaver(), b_does)
    thread = {"configurable": {"thread_id": "retry"}}
    await _fail_with_a_finished_sibling(graph, thread)

    await graph.ainvoke(None, (await graph.aget_state(thread)).config)

    head = await _head(graph, thread)
    assert set(_results_per_call(head).values()) == {1}
    assert len(_results_per_call(head)) == 2


async def test_a_send_after_a_failed_step_keeps_the_dropped_result_out():
    """A new message drops the failed step; its finished sibling's result
    must not surface in the stored thread the turn never ran with."""
    b_does = {"fail": True}
    graph = _tools(InMemorySaver(), b_does)
    thread = {"configurable": {"thread_id": "send-after-failure"}}
    await _fail_with_a_finished_sibling(graph, thread)
    dropped = {
        call["id"]
        for m in await _head(graph, thread)
        for call in getattr(m, "tool_calls", None) or []
    }

    await graph.ainvoke(_user("again"), thread)

    assert not dropped & set(_results_per_call(await _head(graph, thread)))


async def test_a_retry_after_an_update_on_the_failed_step_stores_each_result_once():
    """Compacting or offloading a failed turn writes a state update on its tip,
    which keeps none of the step's pending writes; the retry runs from it."""
    b_does = {"fail": True}
    graph = _tools(InMemorySaver(), b_does)
    thread = {"configurable": {"thread_id": "update-then-retry"}}
    await _fail_with_a_finished_sibling(graph, thread)
    await graph.aupdate_state((await graph.aget_state(thread)).config, {"ui": [_RECORD]})

    await graph.ainvoke(None, (await graph.aget_state(thread)).config)

    head = await _head(graph, thread)
    assert set(_results_per_call(head).values()) == {1}
    assert len(_results_per_call(head)) == 2


async def test_an_update_that_carries_the_finished_result_keeps_it_once():
    """Another agent's turn answers a waiting step with an update that carries
    the finished sibling's result over (``agent_change``)."""
    graph = _tools(InMemorySaver(), {"ask": True})
    thread = {"configurable": {"thread_id": "carry"}}
    await graph.ainvoke(_user("go"), thread)
    state = await graph.aget_state(thread)
    finished = [
        m for task in state.tasks if not task.interrupts for m in task.result["messages"]
    ]
    calls = next(m for m in reversed(state.values["messages"]) if m.type == "ai").tool_calls
    waiting = next(c for c in calls if c["name"] == "b")
    cancelled = ToolMessage("cancelled", tool_call_id=waiting["id"], id=str(uuid.uuid4()))

    await graph.aupdate_state(thread, {"messages": [*finished, cancelled]}, as_node="tool_b")

    head = await _head(graph, thread)
    assert sorted(m.content for m in head if m.type == "tool") == ["cancelled", "result a"]


async def test_a_resume_from_the_head_keeps_the_finished_sibling_result():
    b_does = {"ask": True}
    graph = _tools(InMemorySaver(), b_does)
    thread = {"configurable": {"thread_id": "resume"}}
    await graph.ainvoke(_user("go"), thread)

    await graph.ainvoke(Command(resume="yes"), thread)

    head = await _head(graph, thread)
    assert set(_results_per_call(head).values()) == {1}
    assert len(_results_per_call(head)) == 2


async def test_ordinary_turns_edits_and_updates_store_no_extra_snapshot():
    """The snapshot costs a full copy of the thread, so only a run or an
    update that drops pending writes takes it."""
    saver = InMemorySaver()
    graph = _chat(saver, [])
    thread = {"configurable": {"thread_id": "no-extra-snapshot"}}
    for text in ("q0", "q1"):
        await graph.ainvoke(_user(text), thread)
    await graph.ainvoke(
        _user("q1 edited"), (await _input_checkpoints(graph, thread))[0].parent_config
    )
    await graph.aupdate_state(thread, {"ui": [_RECORD]})

    snapshots = [
        cp.metadata.get("source")
        async for cp in saver.alist(thread)
        if "messages" in cp.checkpoint["channel_values"]
    ]
    assert snapshots == []
