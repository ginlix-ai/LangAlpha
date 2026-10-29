"""A background subagent must not report into the run that spawned it.

LangGraph merges the ambient runnable config's callbacks into an explicit
``callbacks`` list, so the spawning turn's token tracker would bill every
subagent call a second time and its StreamMessagesHandler would park every
streamed token in a queue nobody drains once that turn ends. A copied
``__pregel_stream`` tees the run's graph updates into that same queue, though
it never bills. This locks both shut against future LangGraph changes.
"""

from __future__ import annotations

import pytest
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_config
from langgraph.graph import END, START, MessagesState, StateGraph

from ptc_agent.agent.middleware.background_subagent.subagent import (
    arun_subagent_streaming,
)


class _CallCounter(AsyncCallbackHandler):
    def __init__(self) -> None:
        self.calls = 0

    async def on_chat_model_start(self, *args, **kwargs) -> None:
        self.calls += 1


def _subagent_graph(saver: InMemorySaver):
    llm = GenericFakeChatModel(messages=iter([AIMessage(content="one two three")]))

    async def model(state: MessagesState):
        return {"messages": [await llm.ainvoke(state["messages"])]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    return builder.compile(checkpointer=saver)


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["task_namespace", "bare"])
async def test_subagent_spawned_inside_a_node_stays_out_of_the_parent_run(shape):
    saver = InMemorySaver()
    subagent = _subagent_graph(saver)
    parent_tracker, child_tracker = _CallCounter(), _CallCounter()
    observed: dict = {}

    async def spawn(state: MessagesState):
        if shape == "task_namespace":
            # The Task path: parent config minus callbacks, the parent's
            # configurable (``__pregel_stream`` included) copied as-is.
            parent = {k: v for k, v in dict(get_config()).items() if k != "callbacks"}
            configurable = parent.get("configurable", {})
            config = {
                **parent,
                "configurable": {**configurable, "checkpoint_ns": "task:abc"},
                "metadata": {"subagent_type": "research"},
                "callbacks": [child_tracker],
            }
        else:
            # No checkpoint coordinates, so the ambient configurable merges in.
            config = {"callbacks": [child_tracker]}
        observed["ambient_before"] = var_child_runnable_config.get()
        result = await arun_subagent_streaming(
            subagent, {"messages": [HumanMessage("go")]}, config, registry=None
        )
        observed["subagent_messages"] = len(result["messages"])
        observed["ambient_after"] = var_child_runnable_config.get()
        return {"messages": [AIMessage(content="spawned")]}

    parent_graph = StateGraph(MessagesState)
    parent_graph.add_node("spawn", spawn)
    parent_graph.add_edge(START, "spawn")
    parent_graph.add_edge("spawn", END)
    parent = parent_graph.compile(checkpointer=saver)

    frames = []
    async for frame in parent.astream(
        {"messages": [HumanMessage("hi")]},
        {"configurable": {"thread_id": "t1"}, "callbacks": [parent_tracker]},
        stream_mode=["messages", "updates", "custom"],
        subgraphs=True,
    ):
        frames.append(frame)

    assert observed["subagent_messages"] == 2
    assert child_tracker.calls == 1
    assert parent_tracker.calls == 0
    # Every frame the parent sees is its own root node's.
    assert frames and all(ns == () for ns, _, _ in frames)
    # The spawning node gets its own ambient config back once the subagent
    # returns.
    assert observed["ambient_after"] is observed["ambient_before"]


@pytest.mark.asyncio
async def test_ambient_config_is_restored_when_the_subagent_fails():
    class Boom(Exception):
        pass

    class _Failing:
        async def astream(self, *args, **kwargs):
            raise Boom
            yield  # pragma: no cover

    ambient = {"callbacks": [_CallCounter()], "configurable": {}}
    token = var_child_runnable_config.set(ambient)
    try:
        with pytest.raises(Boom):
            await arun_subagent_streaming(_Failing(), {}, {}, registry=None)
        assert var_child_runnable_config.get() is ambient
    finally:
        var_child_runnable_config.reset(token)


@pytest.mark.asyncio
async def test_a_follow_up_launched_from_a_resumed_turn_reaches_the_subagent(
    monkeypatch,
):
    """LangGraph marks every task of a turn that answered an interrupt as
    resuming, and a resuming graph applies no input, so a task that already
    has state would skip the follow-up's prompt if it inherited the flag."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    saver = InMemorySaver()

    async def model(state: MessagesState):
        return {"messages": [AIMessage(content=f"saw {len(state['messages'])}")]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile(checkpointer=saver)
    task_conf = {"thread_id": "t1", "__pregel_task_id": "launch"}
    await graph.ainvoke(
        {"messages": [HumanMessage("first")]},
        {"configurable": {**task_conf, "checkpoint_ns": "task:task-1"}},
    )

    results: list[dict] = []

    async def run(subagent, state, config, registry):
        results.append(await subagent.ainvoke(state, config))
        return results[-1]

    monkeypatch.setattr(sa, "arun_subagent_streaming", run)
    registry = MagicMock()
    registry.get_by_tool_call_id.return_value = SimpleNamespace(
        completed=False, task_id="task-1", subagent_type="research", task_run_id=None
    )
    tool = sa._create_task_tool(
        subagent_graphs={"research": graph}, registry=registry, checkpointer=saver
    )
    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-1"

    ambient = {"configurable": {**task_conf, "__pregel_resuming": True}}
    token = current_background_tool_call_id.set("tc-bg")
    config_token = var_child_runnable_config.set(ambient)
    try:
        await tool.coroutine(
            description="d", prompt="follow-up", action="resume",
            task_id="task-1", runtime=runtime,
        )
    finally:
        var_child_runnable_config.reset(config_token)
        current_background_tool_call_id.reset(token)

    assert results[-1]["messages"][-1].content == "saw 3"
