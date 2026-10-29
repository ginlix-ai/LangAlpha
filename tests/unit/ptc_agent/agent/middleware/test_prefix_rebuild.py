"""A deploy's system prompt reaches a thread through a summary that keeps no turns.

Drives a real agent graph with the compaction middleware, the tool surface and
the system pin through a thread whose build changes between turns: a new
static prompt and a reworded tool. The thread keeps sending the system message
and tools array its thinking was produced under, with the reworded tool given
to the model in a message, and marks the kept system message stale. The next
summary at a turn start keeps no turns: its own request still carries the old
system message and tools, which every thinking block it summarizes is checked
against, and the call after it carries the new render. No thinking block may
fail on either.
"""

from __future__ import annotations

import json

import pytest
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summary_request import summary_prompt
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import STATE_SYSTEM_PIN, SystemPinMiddleware
from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    api_drops,
    is_compaction_request,
    kept_breaks,
    reply,
    thinking_count,
)

THRESHOLD = 10_000
CFG = {"configurable": {"thread_id": "t-rebuild", "user_id": "u-1"}}


@tool("get_data")
def get_data_a(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


@tool("get_data")
def get_data_b(query: str, limit: int) -> str:
    """Look something up, at most limit rows."""
    return f"data for {query}"


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


def _agent(saver: InMemorySaver, system: str, data_tool, notes_dir: str | None = None):
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
    )
    return create_agent(
        model=anthropic_model(),
        tools=[data_tool],
        system_prompt=system,
        middleware=[
            compaction.with_scratchpad_notes(notes_dir) if notes_dir else compaction,
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            ToolSurfaceMiddleware(),
            SystemPinMiddleware(),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {"input_tokens": input_tokens, "output_tokens": 50, "total_tokens": input_tokens + 50}
    return message


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "The user asked for data.", "signature": "cmp-1"}],
        id="msg_cmp",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


def _system(payload: dict) -> str:
    return json.dumps([block["text"] for block in payload["system"]])


def _unmarked(tools: list) -> list:
    return [{k: v for k, v in t.items() if k != "cache_control"} for t in tools]


async def _pin(saver: InMemorySaver) -> dict:
    state = await _agent(saver, "x", get_data_a).aget_state(CFG)
    return state.values.get(STATE_SYSTEM_PIN) or {}


@pytest.mark.parametrize("notes_dir", [None, "/workspace/.agents/scratchpad/thread01/note"], ids=["notes off", "notes on"])
@pytest.mark.asyncio
async def test_a_deploy_reaches_the_thread_through_a_summary_that_keeps_no_turns(monkeypatch, notes_dir):
    wire = Wire(
        [
            _measured(reply(1, tool=("get_data", {"query": "a"})), 2_000),
            _measured(reply(2, text="Here it is."), 2_100),
            # After the deploy: still under the threshold.
            _measured(reply(3, tool=("get_data", {"query": "b", "limit": 5})), 2_200),
            _measured(reply(4, text="Here too."), 9_995),
            # The next turn starts over it.
            _compaction,
            _measured(reply(6, tool=("get_data", {"query": "c", "limit": 5})), 1_500),
            _measured(reply(7, text="Done."), 1_600),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.", get_data_a, notes_dir).ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    await _agent(saver, "Prompt B.", get_data_b, notes_dir).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)
    assert (await _pin(saver)).get("stale") is True
    await _agent(saver, "Prompt B.", get_data_b, notes_dir).ainvoke({"messages": [HumanMessage("Get c.")]}, CFG)

    assert len(wire.payloads) == 7
    assert kept_breaks(wire) == []
    assert api_drops(wire) == {}
    first, summary, after = wire.payloads[0], wire.payloads[4], wire.payloads[5]
    # Until the summary the thread sends what its thinking was produced under,
    # the reworded tool given in a message at the turn start after the deploy.
    for payload in wire.payloads[:5]:
        assert _system(payload) == _system(first)
        assert _unmarked(payload["tools"]) == _unmarked(first["tools"])
    assert "limit" in json.dumps(wire.payloads[2]["messages"][-1])
    # The summary's own request is on the old render and summarizes every
    # turn, the thinking in it included; the call after it is on the new one.
    assert is_compaction_request(summary)
    assert summary["extra_body"]["compaction"]["instructions"] == summary_prompt("native", False, notes=notes_dir is not None)
    assert thinking_count(summary) == 4
    assert "Prompt B." in _system(after) and "Prompt A." not in _system(after)
    assert _unmarked(after["tools"])[0]["input_schema"]["required"] == ["query", "limit"]
    assert [m["role"] for m in after["messages"]] == ["assistant", "user"]
    assert all(_system(p) == _system(after) for p in wire.payloads[5:])
    assert (await _pin(saver)).get("stale") is None


@pytest.mark.asyncio
async def test_a_summary_mid_loop_keeps_its_turns_and_leaves_the_rebuild_pending(monkeypatch):
    """Mid-loop no summary can keep no turns, so it keeps its tail as ever and
    the thread stays on the old render."""
    wire = Wire(
        [
            _measured(reply(1, tool=("get_data", {"query": "a"})), 2_000),
            _measured(reply(2, text="Here it is."), 2_100),
            _measured(reply(3, tool=("get_data", {"query": "b", "limit": 5})), 7_000),
            _measured(reply(4, tool=("get_data", {"query": "c", "limit": 5})), 9_995),
            _compaction,
            _measured(reply(6, text="Done."), 4_000),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.", get_data_a).ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    await _agent(saver, "Prompt B.", get_data_b).ainvoke({"messages": [HumanMessage("Get b and c.")]}, CFG)

    assert len(wire.payloads) == 6
    assert kept_breaks(wire) == []
    assert api_drops(wire) == {}
    assert is_compaction_request(wire.payloads[4])
    assert wire.payloads[4]["extra_body"]["compaction"]["instructions"] == summary_prompt("native", True)
    assert all(_system(p) == _system(wire.payloads[0]) for p in wire.payloads)
    assert thinking_count(wire.payloads[5]) > 0
    assert (await _pin(saver)).get("stale") is True
