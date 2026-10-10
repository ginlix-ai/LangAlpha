"""The main agent's private fields survive a write through another graph, on
the Postgres saver, which keeps a primitive channel value inside the
checkpoint row: a graph that leaves such a field undeclared writes the next
checkpoint without it. The writers are the history reader's ui-record append
and a manual compaction, whose build leaves the subagent switch out.
"""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph
from langsmith import tracing_context

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.state import DeltaAgentState
from src.server.database.conversation import threads_write
from src.server.services.history.reader import CheckpointHistoryReader

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

THREAD = "5e1f0000-0000-4000-8000-00000000f1e1"
KEPT = {"_subagents_trimmed": True, "_notes_calls_trimmed": 7}


def _config() -> dict[str, Any]:
    return {"configurable": {"thread_id": THREAD}}


def _graph(schema: type, saver: Any) -> Any:
    return (
        StateGraph(schema)
        .add_node("noop", lambda state: {})
        .add_edge(START, "noop")
        .compile(checkpointer=saver)
    )


class _Sets(AgentMiddleware):
    """Sets both fields, as a trim's carries do."""

    def before_agent(self, state: Any, runtime: Any) -> dict[str, Any]:
        return dict(KEPT)


def _agent(saver: Any, *middleware: AgentMiddleware) -> Any:
    """A main-agent build with no subagent switch, as thread maintenance's is."""
    return create_agent(
        GenericFakeChatModel(messages=iter([AIMessage("a", id="a-1")])),
        tools=[],
        middleware=list(middleware),
        checkpointer=saver,
        state_schema=MainAgentState,
    )


async def _seeded(saver: Any) -> Any:
    """A thread whose last turn left both fields set."""
    await saver.adelete_thread(THREAD)
    with tracing_context(enabled=False):
        await _agent(saver, _Sets()).ainvoke(
            {"messages": [HumanMessage("q", id="h-1")]}, _config()
        )


async def _kept(saver: Any) -> dict[str, Any]:
    values = (await _graph(MainAgentState, saver).aget_state(_config())).values
    return {key: values.get(key) for key in KEPT}


async def test_a_graph_without_them_erases_them(test_db_pool) -> None:
    """The hazard the schema closes, so the tests below can see it."""
    saver = AsyncPostgresSaver(test_db_pool)
    await _seeded(saver)
    await _graph(DeltaAgentState, saver).aupdate_state(_config(), {"ui": []})
    assert await _kept(saver) == dict.fromkeys(KEPT)


async def test_a_ui_record_append_keeps_them(test_db_pool, monkeypatch) -> None:
    saver = AsyncPostgresSaver(test_db_pool)
    await _seeded(saver)

    async def advance(thread_id, *, from_checkpoint_id, to_checkpoint_id):
        return True

    monkeypatch.setattr(threads_write, "advance_thread_checkpoint_id", advance)
    await CheckpointHistoryReader(saver).append_ui_record(THREAD, "image_map", {"a": "b"})
    state = await _graph(MainAgentState, saver).aget_state(_config())
    assert [r["name"] for r in state.values["ui"]] == ["image_map"]
    assert await _kept(saver) == KEPT


async def test_a_manual_compaction_keeps_them(test_db_pool, monkeypatch) -> None:
    from src.server.handlers.thread_maintenance import _update_graph_state

    saver = AsyncPostgresSaver(test_db_pool)
    await _seeded(saver)
    graph = _agent(saver)

    async def advance(thread_id, *, from_checkpoint_id, to_checkpoint_id):
        return True

    monkeypatch.setattr(threads_write, "advance_thread_checkpoint_id", advance)
    state = await graph.aget_state(_config())
    await _update_graph_state(
        graph, state, {"_offloaded_tool_call_ids": {"call-1"}}, THREAD, "offload"
    )
    assert (await graph.aget_state(_config())).values["_offloaded_tool_call_ids"] == {"call-1"}
    assert await _kept(saver) == KEPT
