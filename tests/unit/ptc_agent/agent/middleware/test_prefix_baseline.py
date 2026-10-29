"""Preserved thinking survives the per-thread baseline and the static prompt.

Drives a real agent graph with the baseline, the tail carrier and the system
pin through threads whose baseline sources move underneath the frozen block,
whose build changes between turns, and which cross a compaction threshold on
Claude. Every thinking block an earlier call produced must be sent again with
a byte-identical prefix, and what a rebuild of the block would have said must
still reach the model, as rows.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import ThreadScratchpad
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.baseline import BaselineContextMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import STATE_SYSTEM_PIN, SystemPinMiddleware
from ptc_agent.agent.middleware.runtime_context.tail import RowCarrierMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    all_breaks,
    anthropic_model,
    kept_breaks,
    reply,
    thinking_count,
)

THREAD = "b1b2c3d4-0000-4000-8000-000000000000"
ROSTER = "- market-data: quotes and fundamentals"
MANIFEST = "## Available Skills\n\n- **pdf**: read and write PDFs"


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


class Sources:
    """What the baseline reads, which a test moves between turns: agent.md
    (an exception stands for a read that failed) and the harness blocks the
    build has (a missing key is a block the build does not read)."""

    def __init__(self, agent_md: str | Exception = "# Notes\nalpha", blocks: dict | None = None):
        self.agent_md = agent_md
        self.blocks = dict(blocks or {})

    def session(self):
        session = MagicMock()
        session.sandbox.normalize_path = MagicMock(side_effect=lambda p: f"/home/workspace/{p}")
        if isinstance(self.agent_md, Exception):
            session.sandbox.aread_file_text = AsyncMock(side_effect=self.agent_md)
        else:
            session.sandbox.aread_file_text = AsyncMock(return_value=self.agent_md)
        session.take_agent_md_writer = MagicMock(return_value=None)
        return session

    def baseline(self) -> BaselineContextMiddleware:
        texts = dict(self.blocks)
        return BaselineContextMiddleware(
            session=self.session(),
            blocks={kind: (lambda _state, k=kind: texts[k]) for kind in texts},
            guidance="lean",
        )


def _agent(saver: InMemorySaver, sources: Sources, system: str = "You are a test agent.", *, compaction=None):
    return create_agent(
        model=anthropic_model(),
        tools=[get_data],
        system_prompt=system,
        middleware=[
            m
            for m in (
                compaction,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                sources.baseline(),
                RowCarrierMiddleware(),
                SystemPinMiddleware(),
                ReasoningCompatibilityMiddleware(),
            )
            if m is not None
        ],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def _cfg(thread: str = THREAD) -> dict:
    return {"configurable": {"thread_id": thread, "user_id": "u-1"}}


def _system(payload: dict) -> str:
    return json.dumps([block["text"] for block in payload["system"]])


def _messages(payload: dict) -> str:
    return json.dumps(payload["messages"])


@pytest.mark.asyncio
async def test_drift_past_the_rebuild_threshold_leaves_the_block_alone(monkeypatch):
    """agent.md moves every turn, so the rows pass the count that used to fold
    them back into the block; the block stays and the newest row is the copy."""
    turns = 22
    wire = Wire([reply(n, text=f"Noted {n}.") for n in range(1, turns + 1)]).install(monkeypatch)
    saver = InMemorySaver()
    for n in range(1, turns + 1):
        sources = Sources(f"# Notes\nentry {n}")
        await _agent(saver, sources).ainvoke({"messages": [HumanMessage(f"Turn {n}.")]}, _cfg())

    assert [thinking_count(p) for p in wire.payloads] == list(range(turns))
    assert all_breaks(wire) == []
    last = wire.payloads[-1]
    assert "entry 1" in _system(last)
    assert f"entry {turns}" in _messages(last)


@pytest.mark.asyncio
async def test_a_source_that_failed_to_read_reaches_the_model_once_it_reads(monkeypatch):
    """The first turn's agent.md read fails, so the block froze without it; the
    next turn reads it and the model gets the whole section as a row."""
    wire = Wire([reply(1, text="Hello."), reply(2, text="Read it."), reply(3, text="Same.")]).install(
        monkeypatch
    )
    saver = InMemorySaver()
    for agent_md, text in (
        (TimeoutError("sandbox did not answer"), "Hi."),
        ("# Notes\nthe purpose", "What is this workspace for?"),
        ("# Notes\nthe purpose", "And now?"),
    ):
        sources = Sources(agent_md, blocks={"mcp_servers": ROSTER})
        await _agent(saver, sources).ainvoke({"messages": [HumanMessage(text)]}, _cfg())

    assert all_breaks(wire) == []
    assert "the purpose" not in _system(wire.payloads[1])
    assert "the purpose" in _messages(wire.payloads[1])
    # Stated once: the third turn reads the same file and files nothing.
    assert _messages(wire.payloads[2]).count("the purpose") == 1


@pytest.mark.asyncio
async def test_a_source_a_deploy_adds_or_retires_is_a_row(monkeypatch):
    """The second turn's build reads the skills manifest the first did not;
    the third no longer reads the MCP roster."""
    wire = Wire([reply(1, text="Hello."), reply(2, text="Skills."), reply(3, text="Servers.")]).install(
        monkeypatch
    )
    saver = InMemorySaver()
    for blocks, text in (
        ({"mcp_servers": ROSTER}, "Hi."),
        ({"mcp_servers": ROSTER, "skills": MANIFEST}, "What can you do?"),
        ({"skills": MANIFEST}, "And servers?"),
    ):
        await _agent(saver, Sources(blocks=blocks)).ainvoke({"messages": [HumanMessage(text)]}, _cfg())

    assert all_breaks(wire) == []
    assert "read and write PDFs" not in _system(wire.payloads[1])
    assert "read and write PDFs" in _messages(wire.payloads[1])
    assert ROSTER in _system(wire.payloads[2])
    assert "<mcp-servers> in your system prompt no longer applies" in _messages(wire.payloads[2])


@pytest.mark.asyncio
async def test_a_static_prompt_a_deploy_changes_stays_pinned_for_the_thread(monkeypatch):
    """The second turn's build renders a new static prompt. The thread keeps
    sending the one its thinking was produced under; a new thread takes the
    new one."""
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            reply(2, text="Done."),
            reply(3, text="Again."),
            reply(4, text="New thread."),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, Sources(), "Prompt v1.").ainvoke({"messages": [HumanMessage("Hi.")]}, _cfg())
    await _agent(saver, Sources(), "Prompt v2.").ainvoke({"messages": [HumanMessage("Again.")]}, _cfg())

    assert all_breaks(wire) == []
    assert "Prompt v1." in _system(wire.payloads[2])

    await _agent(saver, Sources(), "Prompt v2.").ainvoke(
        {"messages": [HumanMessage("Hi.")]}, _cfg("c1b2c3d4-0000-4000-8000-000000000000")
    )
    assert "Prompt v2." in _system(wire.payloads[3])


@pytest.mark.asyncio
async def test_a_call_to_another_model_leaves_the_pin_for_claude():
    """A fallback answering for Claude must not clear the pin the thread's
    Claude thinking was produced under."""
    stored = {"model": "claude-opus-5-5", "content": "Prompt v1.", "stale": True}
    request = MagicMock(
        model=GenericFakeChatModel(messages=iter([])),
        system_message=SystemMessage("Prompt v2."),
        state={STATE_SYSTEM_PIN: stored},
        messages=[HumanMessage("Hi.")],
    )
    answer = AIMessage("Hello.")

    response = await SystemPinMiddleware().awrap_model_call(request, AsyncMock(return_value=answer))

    assert response is answer
    request.override.assert_not_called()


def _unthinking(n: int, text: str) -> AIMessage:
    return AIMessage(
        content=[{"type": "text", "text": text}],
        id=f"msg_{n:02d}",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "end_turn"},
        usage_metadata={"input_tokens": 1000, "output_tokens": 50, "total_tokens": 1050},
    )


@pytest.mark.asyncio
async def test_a_thread_with_no_thinking_in_view_takes_the_new_build(monkeypatch):
    """Nothing binds a prefix no thinking was produced on, so the new static
    prompt and a rebuilt block reach the thread on its next turn."""
    wire = Wire([_unthinking(1, "Hello."), _unthinking(2, "Again.")]).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, Sources(blocks={"mcp_servers": ROSTER}), "Prompt v1.").ainvoke(
        {"messages": [HumanMessage("Hi.")]}, _cfg()
    )
    await _agent(
        saver, Sources(blocks={"mcp_servers": ROSTER, "skills": MANIFEST}), "Prompt v2."
    ).ainvoke({"messages": [HumanMessage("Again.")]}, _cfg())

    assert "Prompt v2." in _system(wire.payloads[1])
    assert "read and write PDFs" in _system(wire.payloads[1])
    assert "read and write PDFs" not in _messages(wire.payloads[1])


# -- compaction --------------------------------------------------------------

THRESHOLD = 10_000


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {
        "input_tokens": input_tokens,
        "output_tokens": 50,
        "total_tokens": input_tokens + 50,
    }
    return message


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "The user asked for help.", "signature": "cmp-1"}],
        id="msg_cmp",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


def _compacting() -> CompactionMiddleware:
    return CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
    )


@pytest.mark.asyncio
async def test_a_compaction_that_keeps_thinking_restates_the_block_and_keeps_the_prefix(monkeypatch):
    """agent.md moves on the second turn and its row is summarized away
    mid-turn three; a deploy changes the static prompt and agent.md again
    before turn four. The kept turns' thinking stays valid, and the first call
    after the summary carries one row restating agent.md as it is, after the
    tool result that follows the kept turns."""
    wire = Wire(
        [
            _measured(reply(1, text="Hello."), 2_000),
            _measured(reply(2, text="Noted."), 7_000),
            _measured(reply(3, tool=("get_data", {"query": "a"})), 7_100),
            _measured(reply(4, tool=("get_data", {"query": "b"})), 7_200),
            _measured(reply(5, tool=("get_data", {"query": "c"})), 9_990),
            _compaction,
            _measured(reply(7, tool=("get_data", {"query": "d"})), 4_000),
            _measured(reply(8, text="Done."), 4_100),
            _measured(reply(9, text="Still done."), 4_200),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    turns = (
        ("# Notes\nfirst purpose", "You are a test agent.", "Hi."),
        ("# Notes\nsecond purpose", "You are a test agent.", "Change the purpose."),
        ("# Notes\nsecond purpose", "You are a test agent.", "Look at NVDA."),
        ("# Notes\nthird purpose", "You are a test agent, deployed again.", "And now?"),
    )
    for agent_md, system, text in turns:
        await _agent(saver, Sources(agent_md), system, compaction=_compacting()).ainvoke(
            {"messages": [HumanMessage(text)]}, _cfg()
        )

    assert len(wire.payloads) == 9
    assert "compaction" in wire.payloads[5].get("extra_body", {})
    assert kept_breaks(wire) == []
    assert {_system(p) for p in wire.payloads} == {_system(wire.payloads[0])}
    after = wire.payloads[7]["messages"]
    assert after[-1]["role"] == "system"
    restated = json.dumps(after[-1])
    assert "summarized" in restated
    assert "second purpose" in restated
    assert "tool_result" in json.dumps(after[-2])
    assert "third purpose" in _messages(wire.payloads[8])

class _Notes:
    """A sandbox whose notes folder holds one note, so a summary carries the
    pointer to it."""

    livefs = None
    transcripts = None

    def normalize_path(self, path: str) -> str:
        return path

    async def aglob(self, pattern: str, path: str = "/"):
        return SimpleNamespace(matches=[{"path": f"{path}/nvda.md"}], error=None)


POINTER = "Your checkpoint notes for this thread are in"


@pytest.mark.asyncio
@pytest.mark.parametrize("moved", [True, False], ids=["agent-md-moved", "nothing-to-restate"])
async def test_the_answer_from_a_summary_reads_its_note_once_and_later_calls_keep_it(monkeypatch, moved):
    """Claude's block goes alone, so the summary's note follows the turns it
    kept. The call answering from the summary is sent it, the note stays in
    history ahead of that answer, and the baseline's restatement after it
    leaves the note out, or is not filed when nothing else is left to
    restate: every call sends the note exactly once, where it first went."""
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)
    scratchpad = ThreadScratchpad.resolve(
        SimpleNamespace(feature_enabled=lambda name: name == "scratchpad"), "/home/workspace/acme", THREAD
    )
    wire = Wire(
        [
            _measured(reply(1, text="Hello."), 2_000),
            _measured(reply(2, text="Noted."), 7_000),
            _measured(reply(3, tool=("get_data", {"query": "a"})), 7_100),
            _measured(reply(4, tool=("get_data", {"query": "b"})), 7_200),
            _measured(reply(5, tool=("get_data", {"query": "c"})), 9_990),
            _compaction,
            _measured(reply(7, tool=("get_data", {"query": "d"})), 4_000),
            _measured(reply(8, text="Done."), 4_100),
            _measured(reply(9, text="Still done."), 4_200),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    first = "# Notes\nfirst purpose" if moved else "# Notes\nsecond purpose"
    turns = (
        (first, "Hi."),
        ("# Notes\nsecond purpose", "Change the purpose."),
        ("# Notes\nsecond purpose", "Look at NVDA."),
        ("# Notes\nsecond purpose", "And now?"),
    )
    for agent_md, text in turns:
        compaction = CompactionMiddleware(
            Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
            token_threshold=THRESHOLD,
            keep_tokens=THRESHOLD // 3,
            claude_keep_tokens=THRESHOLD // 3,
            backend=_Notes(),
        ).with_scratchpad_notes(scratchpad.notes_dir)
        await _agent(saver, Sources(agent_md), compaction=compaction).ainvoke(
            {"messages": [HumanMessage(text)]}, _cfg()
        )

    assert "compaction" in wire.payloads[5].get("extra_body", {})
    answered = wire.payloads[6]["messages"]
    assert POINTER in json.dumps(answered[-1])
    assert "tool_result" in json.dumps(answered[-2])
    assert [_messages(p).count(POINTER) for p in wire.payloads[6:]] == [1, 1, 1]
    after = json.dumps(wire.payloads[7]["messages"][-1])
    if moved:
        assert "second purpose" in after
        assert "summarized" not in after
    else:
        assert "tool_result" in after
    assert kept_breaks(wire) == []
