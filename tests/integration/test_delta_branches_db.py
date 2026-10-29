"""Runs and updates that branch off pending writes store each message once.

A regenerate of a turn resumed from a question replays the checkpoint the
answer was written on, whose pending writes hold the step's tool results; a
retry replays a failed step's checkpoint; a compaction or offload of a failed
turn writes a state update on it. Each reruns the step's tasks, so before
langgraph 1.2.13 snapshotted a new branch's delta channels, its ``messages``
loads also replayed the results those tasks stored the first time: two results for one
tool call, which providers refuse. Inputs are the id-less dicts the chat path
sends. Each test writes its own thread.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, Send, interrupt

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.state import DeltaAgentState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _RecordingModel(GenericFakeChatModel):
    """Answers in turn and keeps every request it was sent."""

    requests: list = []

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.requests.append([f"{m.type}:{m.content}" for m in messages])
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def _user(text: str) -> dict:
    return {"messages": [{"role": "user", "content": text}]}


def _thread() -> dict:
    return {"configurable": {"thread_id": f"delta-branches-{uuid.uuid4()}"}}


def _results_per_call(messages) -> Counter:
    return Counter(m.tool_call_id for m in messages if m.type == "tool")


async def _input_checkpoints(saver, thread) -> list:
    """Newest first."""
    return [
        cp async for cp in saver.alist(thread) if cp.metadata.get("source") == "input"
    ]


def _two_tool_step(saver, fail_once: dict):
    """An agent step that calls two tools as parallel tasks, then answers."""

    def agent(state):
        if state["messages"][-1].type == "tool":
            return {"messages": [AIMessage("done", id=str(uuid.uuid4()))]}
        calls = [{"name": n, "args": {}, "id": f"{n}-{uuid.uuid4()}"} for n in "ab"]
        return {"messages": [AIMessage("", tool_calls=calls, id=str(uuid.uuid4()))]}

    def route(state):
        calls = getattr(state["messages"][-1], "tool_calls", None) or []
        return [Send(f"tool_{c['name']}", c) for c in calls] or END

    def tool_a(call):
        return {"messages": [ToolMessage("result a", tool_call_id=call["id"])]}

    async def tool_b(call):
        if fail_once.pop("b", False):
            await asyncio.sleep(0.05)  # tool_a stores its result first
            raise RuntimeError("upstream timeout")
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


async def _fail_with_a_finished_sibling(graph, saver, thread):
    with pytest.raises(RuntimeError):
        await graph.ainvoke(_user("go"), thread)
    tip = await saver.aget_tuple((await graph.aget_state(thread)).config)
    assert any(channel == "messages" for _, channel, _ in tip.pending_writes)
    return tip


async def test_a_regenerate_of_a_resumed_turn_stores_each_tool_result_once(
    test_db_pool,
):
    """The turn's regenerate replays the checkpoint its answer was written on,
    asks again, and runs both tools again once answered."""
    saver = AsyncPostgresSaver(test_db_pool)

    @tool
    def echo(text: str) -> str:
        """Echo the text."""
        return text

    @tool
    def ask(question: str) -> str:
        """Ask the user."""
        return interrupt(question)

    calls = [
        {"name": "echo", "args": {"text": "papaya"}, "id": f"echo-{uuid.uuid4()}"},
        {"name": "ask", "args": {"question": "fruit?"}, "id": f"ask-{uuid.uuid4()}"},
    ]
    model = _RecordingModel(
        messages=iter(
            [
                AIMessage("", tool_calls=calls, id=str(uuid.uuid4())),
                AIMessage("MANGO", id=str(uuid.uuid4())),
                AIMessage("LYCHEE", id=str(uuid.uuid4())),
            ]
        ),
        requests=[],
    )
    agent = create_agent(
        model, tools=[echo, ask], state_schema=MainAgentState, checkpointer=saver
    )
    thread = _thread()
    await agent.ainvoke(_user("pick a fruit"), thread)
    answered_on = (await agent.aget_state(thread)).config
    await agent.ainvoke(Command(resume="mango"), thread)

    await agent.ainvoke(None, answered_on)
    assert (await agent.aget_state(thread)).interrupts
    await agent.ainvoke(Command(resume="lychee"), thread)

    head = (await agent.aget_state(thread)).values["messages"]
    assert _results_per_call(head) == {c["id"]: 1 for c in calls}
    assert model.requests[-1].count("tool:lychee") == 1
    assert "tool:mango" not in model.requests[-1]


async def test_a_retry_stores_the_finished_sibling_tool_result_once(test_db_pool):
    saver = AsyncPostgresSaver(test_db_pool)
    graph = _two_tool_step(saver, {"b": True})
    thread = _thread()
    tip = await _fail_with_a_finished_sibling(graph, saver, thread)

    await graph.ainvoke(None, tip.config)

    per_call = _results_per_call((await graph.aget_state(thread)).values["messages"])
    assert len(per_call) == 2 and set(per_call.values()) == {1}


async def test_a_retry_after_an_update_on_the_failed_step_stores_each_result_once(
    test_db_pool,
):
    """Compacting or offloading a failed turn writes a state update on its tip
    before the retry runs from it."""
    saver = AsyncPostgresSaver(test_db_pool)
    graph = _two_tool_step(saver, {"b": True})
    thread = _thread()
    tip = await _fail_with_a_finished_sibling(graph, saver, thread)
    record = {"type": "ui", "id": "ui-note", "name": "note", "props": {}, "metadata": {}}
    updated = await graph.aupdate_state(tip.config, {"ui": [record]})

    await graph.ainvoke(None, updated)

    per_call = _results_per_call((await graph.aget_state(thread)).values["messages"])
    assert len(per_call) == 2 and set(per_call.values()) == {1}


async def test_a_regenerate_of_the_first_turn_writes_its_fork_snapshot(test_db_pool):
    """The thread's root has no ``messages`` version yet, so the fork mints
    one; without it the snapshot is not stored and the backport does nothing."""
    saver = AsyncPostgresSaver(test_db_pool)

    def reply(state):
        return {"messages": [AIMessage("a0", id=str(uuid.uuid4()))]}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("reply", reply)
        .add_edge(START, "reply")
        .compile(checkpointer=saver)
    )
    thread = _thread()
    await graph.ainvoke(_user("q0"), thread)
    root = (await _input_checkpoints(saver, thread))[-1]

    await graph.ainvoke(None, root.config)

    fork = [
        cp async for cp in saver.alist(thread) if cp.metadata.get("source") == "fork"
    ]
    assert len(fork) == 1 and "messages" in fork[0].checkpoint["channel_values"]
    head = (await graph.aget_state(thread)).values["messages"]
    assert [f"{m.type}:{m.content}" for m in head] == ["human:q0", "ai:a0"]


async def test_a_regenerate_sends_the_next_model_call_its_message_once(test_db_pool):
    """The plain regenerate the chat UI offers on every turn. Without the
    backport it rests on a race: langgraph stamps the input message's id in
    place, in the list the saver serializes on a worker thread, and only a
    stamp that lands first lets the regenerate resend that id for the reducer
    to merge. The same packages won it in the backend container and lost it on
    a host run against the same Postgres, where the next call saw q1 twice."""
    saver = AsyncPostgresSaver(test_db_pool)
    model = _RecordingModel(
        messages=iter([AIMessage(f"a{i}", id=str(uuid.uuid4())) for i in range(4)]),
        requests=[],
    )
    agent = create_agent(
        model, tools=[], state_schema=MainAgentState, checkpointer=saver
    )
    thread = _thread()
    for text in ("q0", "q1"):
        await agent.ainvoke(_user(text), thread)

    await agent.ainvoke(None, (await _input_checkpoints(saver, thread))[0].config)
    await agent.ainvoke(_user("q2"), thread)

    assert model.requests[-2] == ["human:q0", "ai:a0", "human:q1"]
    assert model.requests[-1] == ["human:q0", "ai:a0", "human:q1", "ai:a2", "human:q2"]
