"""Preserved thinking survives a provider compaction that keeps recent turns.

Drives a real agent graph with the compaction middleware and the notes
reminder through a thread that crosses its threshold on Claude: the provider
summarizes the head, and the tail it keeps holds a notes reminder the agent
answered and a chart image. Every thinking block a kept turn holds must stay
valid on each later call: sent with the prefix it was produced on, the
summarized messages replaced by the compaction block and nothing between it
and the turns it kept.
"""

from __future__ import annotations

import base64
import json
import struct
import zlib

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from types import SimpleNamespace

from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import NotesDueMiddleware, ThreadScratchpad
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    api_drops,
    is_compaction_request,
    kept_breaks,
    left_out,
    reply,
    thinking_count,
)

THREAD = "a1b2c3d4-0000-4000-8000-000000000000"
THRESHOLD = 10_000
WORKSPACE = "/home/workspace/acme"
SCRATCHPAD = ThreadScratchpad.resolve(
    SimpleNamespace(feature_enabled=lambda name: name == "scratchpad"), WORKSPACE, THREAD
)
# What the user pastes in the second turn, which the summary replaces: about
# 4,500 tokens by estimate.
PASTE = " ".join(f"line {i} of the filing" for i in range(550))


def _system(rules: int) -> str:
    """A system prompt of about eight tokens a rule. Only the estimate of
    what a summary keeps counts it: the trigger reads the measurement."""
    return " ".join("Answer plainly and briefly." for _ in range(rules))


def _png(width: int, height: int) -> str:
    """A blank grayscale PNG, as base64: its header is all a size reads."""

    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\x00" * ((width + 1) * height)))
        + chunk(b"IEND", b"")
    )
    return base64.b64encode(png).decode()


# About 1,300 tokens, by estimate and as measured.
IMAGE = _png(1000, 1000)


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


@tool(response_format="content")
def view_chart(name: str) -> list:
    """Show a chart."""
    return [
        {"type": "text", "text": f"Chart {name}"},
        {"type": "image", "base64": IMAGE, "mime_type": "image/png"},
    ]


class _Notes:
    """A sandbox whose notes folder holds one note, so a summary carries the
    pointer to it."""

    livefs = None
    transcripts = None

    def normalize_path(self, path: str) -> str:
        return path

    async def aglob(self, pattern: str, path: str = "/"):
        return SimpleNamespace(matches=[{"path": f"{path}/nvda.md"}], error=None)


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {
        "input_tokens": input_tokens,
        "output_tokens": 50,
        "total_tokens": input_tokens + 50,
    }
    return message


def _compaction(n: int):
    def answer(payload: dict) -> AIMessage:
        return AIMessage(
            content=[{"type": "compaction", "content": f"Summary {n}.", "signature": f"cmp-{n}"}],
            id=f"msg_cmp{n}",
            response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
            usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        )

    return answer


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


def _agent(saver: InMemorySaver, system: str, notes: bool):
    model = anthropic_model()
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
        backend=_Notes() if notes else None,
    ).with_scratchpad_notes(SCRATCHPAD.notes_dir)
    return create_agent(
        model=model,
        tools=[get_data, view_chart],
        system_prompt=system,
        middleware=[
            compaction,
            NotesDueMiddleware(SCRATCHPAD, compaction, model),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
    )


async def _thread(
    monkeypatch, *, system: str, after_summary: list, notes: bool = False, summary: list | None = None
) -> Wire:
    """Four turns: the third crosses the threshold on call 7, and the
    provider summarizes the first two but their last answer. Call 3 answers
    the notes reminder, call 4 reads the chart image. ``after_summary``
    answers the calls after the summary. With ``notes`` the notes folder
    holds a note, which the summary points at. ``summary`` answers the
    compaction request and any summary call after it, the provider's block by
    default."""
    wire = Wire(
        [
            _measured(reply(1, text="Hello."), 200),
            # The paste the summary replaces, measured here.
            _measured(reply(2, text="Read it."), 7_000),
            _measured(reply(3, tool=("get_data", {"query": "a"})), 7_100),
            _measured(reply(4, tool=("view_chart", {"name": "nvda"})), 7_200),
            _measured(reply(5, tool=("get_data", {"query": "b"})), 8_600),
            _measured(reply(6, tool=("get_data", {"query": "c"})), 9_990),
            *(summary or [_compaction(1)]),
            *after_summary,
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": THREAD, "user_id": "u-1"}}
    saver = InMemorySaver()
    for text in ("Hi.", PASTE, "Look at NVDA.", "And now?"):
        await _agent(saver, system, notes).ainvoke({"messages": [HumanMessage(text)]}, cfg)
    assert is_compaction_request(wire.payloads[6])
    assert "notes are in" in json.dumps(wire.payloads[2]["messages"][-1])
    return wire


def _image_sent(payload: dict) -> bool:
    return IMAGE in json.dumps(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("notes", [False, True])
async def test_the_turns_a_summary_keeps_are_sent_as_they_were(monkeypatch, notes):
    wire = await _thread(
        monkeypatch,
        system=_system(190),
        notes=notes,
        after_summary=[
            _measured(reply(8, text="Done."), 4_000),
            _measured(reply(9, tool=("get_data", {"query": "d"})), 4_100),
            _measured(reply(10, text="Still done."), 4_200),
        ],
    )

    assert kept_breaks(wire) == []
    assert len(wire.payloads) == 10
    assert all(_image_sent(p) for p in wire.payloads[7:])
    assert left_out(wire) == {"sig-1": 8}
    assert [thinking_count(p) for p in wire.payloads[7:]] == [5, 6, 7]
    # The block records the tool changes it summarized.
    assert "inline-tools-2026-09-15" in wire.payloads[6]["betas"]


@pytest.mark.asyncio
async def test_an_image_the_kept_turns_cannot_fit_goes_into_the_summary(monkeypatch):
    wire = await _thread(
        monkeypatch,
        system=_system(1_080),
        after_summary=[
            _measured(reply(8, text="Done."), 9_000),
            _measured(reply(9, tool=("get_data", {"query": "d"})), 9_100),
            _measured(reply(10, text="Still done."), 9_200),
        ],
    )

    assert kept_breaks(wire) == []
    assert len(wire.payloads) == 10
    # Summarized up to the answer that read it.
    assert _image_sent(wire.payloads[6])
    assert not any(_image_sent(p) for p in wire.payloads[7:])
    assert left_out(wire) == {"sig-1": 8, "sig-2": 8, "sig-3": 8, "sig-4": 8}
    assert [thinking_count(p) for p in wire.payloads[7:]] == [2, 3, 4]


@pytest.mark.asyncio
async def test_an_image_measured_over_the_threshold_goes_into_the_next_summary(monkeypatch):
    # The provider counts the turns the summary kept at far more than their
    # estimate, which the next call puts down to the image.
    wire = await _thread(
        monkeypatch,
        system=_system(190),
        after_summary=[
            _measured(reply(8, text="Done."), 14_000),
            _compaction(2),
            _measured(reply(10, tool=("get_data", {"query": "d"})), 3_000),
            _measured(reply(11, text="Still done."), 3_100),
        ],
    )

    assert kept_breaks(wire) == []
    assert len(wire.payloads) == 11
    assert is_compaction_request(wire.payloads[8])
    assert _image_sent(wire.payloads[7]) and _image_sent(wire.payloads[8])
    assert not any(_image_sent(p) for p in wire.payloads[9:])
    assert left_out(wire) == {"sig-1": 8, "sig-2": 10, "sig-3": 10, "sig-4": 10}
    assert [thinking_count(p) for p in wire.payloads[7:]] == [5, 3, 3, 4]


@pytest.mark.asyncio
async def test_a_summary_written_on_the_client_leaves_the_kept_thinking_out(monkeypatch):
    """The provider answers the compaction request without a block, so the
    summary is written on the client: a new first message, ahead of every
    kept thinking block, which therefore goes."""
    wire = await _thread(
        monkeypatch,
        system=_system(190),
        summary=[
            _measured(reply(7, text="No summary here."), 9_990),
            # The summarizer falls back to the agent's model.
            _measured(AIMessage("The user said hello and pasted a filing.", id="msg_sum"), 3_000),
        ],
        after_summary=[
            _measured(reply(8, text="Done."), 4_000),
            _measured(reply(9, tool=("get_data", {"query": "d"})), 4_100),
            _measured(reply(10, text="Still done."), 4_200),
        ],
    )

    assert kept_breaks(wire) == []
    assert api_drops(wire) == {}
    assert len(wire.payloads) == 11
    assert "[Context Summary]" in json.dumps(wire.payloads[8]["messages"][0])
    assert [thinking_count(p) for p in wire.payloads[8:]] == [0, 1, 2]
