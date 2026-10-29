"""A summary is streamed so a model still writing is never cut off at its
share of the budget, and so its progress can be shown. Its chunks must never
reach the turn's message stream, where they would read as the answer, while
the callbacks that bill and trace the call still see it.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph

from ptc_agent.agent.middleware.compaction import summarize
from ptc_agent.agent.middleware.compaction.summarize import Summary, awrite_summary

SUMMARY = "The user asked for a DCF of NVDA and the model is half built."


async def _ready(room):
    return [], [HumanMessage("Summarize the conversation.")]


def _server() -> Summary:
    return Summary("server", "server", [])


class _Ends(AsyncCallbackHandler):
    """Stands in for the usage tracker, which reads each call's end."""

    def __init__(self) -> None:
        self.ends = 0

    async def on_llm_end(self, response, **kwargs: Any) -> None:
        self.ends += 1


class _Paced:
    """Writes ``chunks`` pieces ``every`` seconds apart, then goes silent for
    ``then_silent`` seconds before finishing (or forever with None)."""

    def __init__(self, name: str, chunks: int, every: float, then_silent: float | None = 0.0):
        self.model_name = name
        self.chunks, self.every, self.then_silent = chunks, every, then_silent

    async def astream(self, request, config=None):
        for i in range(self.chunks):
            await asyncio.sleep(self.every)
            yield AIMessageChunk(content=f"part {i} ")
        await asyncio.sleep(3600 if self.then_silent is None else self.then_silent)
        yield AIMessageChunk(content="end")


class _Quiet:
    """Opens with the role chunk and nothing in it, as a provider that hides
    its reasoning does, then never writes."""

    model_name = "quiet"

    async def astream(self, request, config=None):
        yield AIMessageChunk(content="")
        await asyncio.sleep(3600)
        yield AIMessageChunk(content="never")


class _Instant:
    model_name = "fallback"

    async def astream(self, request, config=None):
        yield AIMessageChunk(content="the fallback's summary")


@pytest.mark.asyncio
async def test_summary_chunks_never_reach_the_turns_message_stream(monkeypatch):
    monkeypatch.setattr(summarize, "_PROGRESS_EVERY_S", 0.0)
    model = GenericFakeChatModel(messages=iter([AIMessage(SUMMARY)]))

    class State(TypedDict):
        text: str

    async def compact(state: State) -> dict:
        writer = get_stream_writer()
        summary = await awrite_summary(
            model=model,
            fallback=None,
            prepare=_ready,
            server=_server,
            budget=5.0,
            progress=lambda tokens: writer({"summary_tokens": tokens}),
        )
        return {"text": summary.text}

    graph = (
        StateGraph(State)
        .add_node("compact", compact)
        .add_edge(START, "compact")
        .add_edge("compact", END)
        .compile()
    )
    ends = _Ends()
    seen: dict[str, list] = {"messages": [], "custom": [], "values": []}
    async for mode, data in graph.astream(
        {"text": ""}, stream_mode=["messages", "custom", "values"], config={"callbacks": [ends]}
    ):
        seen[mode].append(data)

    assert seen["messages"] == []
    assert seen["values"][-1]["text"] == SUMMARY
    counts = [event["summary_tokens"] for event in seen["custom"]]
    assert counts and counts == sorted(counts)
    assert ends.ends == 1


@pytest.mark.asyncio
async def test_a_model_still_writing_runs_past_its_share():
    # Its share is two thirds of 1.5s; it writes for about 1.2s and finishes.
    writer = _Paced("summary", chunks=12, every=0.1)

    summary = await awrite_summary(
        model=writer, fallback=_Instant(), prepare=_ready, server=_server, budget=1.5
    )

    assert summary.source == "model"
    assert summary.text.startswith("part 0")


@pytest.mark.asyncio
async def test_a_model_that_stops_writing_past_its_share_gives_way(monkeypatch):
    # Its share is two thirds of 2s; it writes until 1.6s, past the share,
    # then hangs. Cut a stall after its last chunk rather than at the end of
    # the budget, the fallback still gets a call.
    monkeypatch.setattr(summarize, "_STALL_S", 0.15)
    stalls = _Paced("summary", chunks=32, every=0.05, then_silent=None)

    summary = await awrite_summary(
        model=stalls, fallback=_Instant(), prepare=_ready, server=_server, budget=2.0
    )

    assert summary.source == "fallback"


@pytest.mark.asyncio
async def test_a_model_that_never_writes_keeps_to_its_share():
    loop = asyncio.get_running_loop()
    started = loop.time()

    summary = await awrite_summary(
        model=_Quiet(), fallback=_Instant(), prepare=_ready, server=_server, budget=0.6
    )

    assert summary.source == "fallback"
    assert loop.time() - started < 0.6
