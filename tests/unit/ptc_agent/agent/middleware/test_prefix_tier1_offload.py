"""Tier 1 is off by default, and when an operator turns it on it never cuts
ahead of Claude thinking.

Drives a real agent graph with the compaction middleware through a turn that
reads one file twice, then turns after the thread sat idle: every thinking
block an earlier call produced must be sent again with a byte-identical
prefix, the stale read included as it was, and no request may carry the
server's context editing, which the API refuses beside a compaction request.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.offloading import select_offloads
from ptc_agent.agent.middleware.compaction.types import OffloadSettings
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.config.agent import CompactionConfig
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    is_compaction_request,
    kept_breaks,
    left_out,
    reply,
    thinking_count,
)

# What an operator turning Tier 1 on sets: every turn start counts as idle,
# and the newest two messages are never cut.
IDLE = OffloadSettings(keep_messages=2, max_length=50, idle_seconds=0)


@tool("Read")
def read(file_path: str) -> str:
    """Read a file."""
    return f"contents of {file_path}"


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


def _agent(saver: InMemorySaver, offload: OffloadSettings, threshold: int = 100_000):
    return create_agent(
        model=anthropic_model(),
        tools=[read],
        system_prompt="You are a test agent.",
        middleware=[
            CompactionMiddleware(
                Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
                token_threshold=threshold,
                keep_tokens=threshold // 3,
                claude_keep_tokens=threshold // 3,
                offload=offload,
            ),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
    )


def _later(monkeypatch, hours: float) -> None:
    """Every turn from here on starts ``hours`` after the model last answered."""
    now = time.time() + hours * 3600
    monkeypatch.setattr(mw_module, "time", SimpleNamespace(time=lambda: now, monotonic=time.monotonic))


def _first_read(payload: dict) -> object:
    return next(
        block["content"]
        for message in payload["messages"]
        if isinstance(message["content"], list)
        for block in message["content"]
        if block.get("type") == "tool_result" and block.get("tool_use_id") == "toolu_01"
    )


# Turns after the first, each reading another file, so the stale read sits
# further back than the newest messages Tier 1 spares by default.
_MORE = 4
_CALLS = 3 + 2 * _MORE + 2


async def _idle_thread(monkeypatch, offload: OffloadSettings) -> Wire:
    more = [
        r
        for k in range(_MORE)
        for r in (reply(4 + 2 * k, tool=("Read", {"file_path": f"file{k}.md"})), reply(5 + 2 * k, text="Read."))
    ]
    wire = Wire(
        [
            reply(1, tool=("Read", {"file_path": "notes.md"})),
            # The first read is stale once the same file is read again.
            reply(2, tool=("Read", {"file_path": "notes.md"})),
            reply(3, text="Read it twice."),
            *more,
            reply(_CALLS - 1, text="Still here."),
            reply(_CALLS, text="And again."),
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": "t-idle", "user_id": "u-1"}}
    saver = InMemorySaver()
    await _agent(saver, offload).ainvoke({"messages": [HumanMessage("Read notes.md.")]}, cfg)
    for k in range(_MORE):
        await _agent(saver, offload).ainvoke({"messages": [HumanMessage(f"Read file{k}.md.")]}, cfg)
    _later(monkeypatch, 3)
    for text in ("Anything new?", "And now?"):
        await _agent(saver, offload).ainvoke({"messages": [HumanMessage(text)]}, cfg)
    return wire


@pytest.mark.asyncio
async def test_tier1_is_off_by_default_so_an_idle_thread_keeps_every_read(monkeypatch):
    wire = await _idle_thread(monkeypatch, OffloadSettings.from_config(CompactionConfig()))

    assert kept_breaks(wire) == []
    assert len(wire.payloads) == _CALLS
    assert left_out(wire) == {}
    assert [thinking_count(p) for p in wire.payloads] == list(range(_CALLS))
    assert all(_first_read(p) == "contents of notes.md" for p in wire.payloads[1:])
    assert not any("context_management" in p for p in wire.payloads)


@pytest.mark.asyncio
async def test_tier1_turned_on_never_cuts_ahead_of_claude_thinking(monkeypatch):
    wire = await _idle_thread(monkeypatch, IDLE)

    assert kept_breaks(wire) == []
    assert left_out(wire) == {}
    assert [thinking_count(p) for p in wire.payloads] == list(range(_CALLS))
    assert not any("context_management" in p for p in wire.payloads)


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {"input_tokens": input_tokens, "output_tokens": 50, "total_tokens": input_tokens + 50}
    return message


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "Summary.", "signature": "cmp-1"}],
        id="msg_cmp1",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


@pytest.mark.asyncio
async def test_a_compaction_request_with_read_bound_carries_no_context_editing(monkeypatch):
    """The API refuses ``context_management`` beside a compaction request, and
    the main agent always binds Read."""
    wire = Wire(
        [
            _measured(reply(1, text="Hello."), 200),
            _measured(reply(2, tool=("Read", {"file_path": "notes.md"})), 7_000),
            _measured(reply(3, tool=("Read", {"file_path": "plan.md"})), 8_500),
            _measured(reply(4, text="Read both."), 9_990),
            _compaction,
            _measured(reply(6, text="Done."), 4_000),
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": "t-compact", "user_id": "u-1"}}
    saver = InMemorySaver()
    for text in ("Hi.", "Read notes.md and plan.md.", "Go on."):
        await _agent(saver, IDLE, threshold=10_000).ainvoke({"messages": [HumanMessage(text)]}, cfg)

    assert any(is_compaction_request(p) for p in wire.payloads)
    assert not any("context_management" in p for p in wire.payloads)
    assert kept_breaks(wire) == []


def test_a_thread_without_checked_thinking_still_hides_a_stale_read():
    calls = [{"name": "Read", "args": {"file_path": "notes.md"}, "id": f"c{i}"} for i in (1, 2)]
    history = [
        HumanMessage("Read notes.md.", id="h1"),
        AIMessage("", id="a1", tool_calls=[calls[0]], response_metadata={"model_name": "gpt-5.5"}),
        ToolMessage("contents", tool_call_id="c1", id="t1"),
        AIMessage("", id="a2", tool_calls=[calls[1]], response_metadata={"model_name": "gpt-5.5"}),
        ToolMessage("contents", tool_call_id="c2", id="t2"),
        AIMessage("Read it twice.", id="a3", response_metadata={"model_name": "gpt-5.5"}),
        HumanMessage("Anything new?", id="h2"),
    ]

    assert select_offloads(history, IDLE, (set(), set())) == (set(), {"c1"})
