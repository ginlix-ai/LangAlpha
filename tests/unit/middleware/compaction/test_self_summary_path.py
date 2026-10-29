"""A thread past its threshold, on a model whose provider does not compact.

The agent's model writes the summary from its own request, the prompt sent as
the last user message, whenever it is the model the summarizer would run;
anything short of a finished summary falls back to the summarizer reading the
transcript.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, SystemMessage
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.constants import TAG_NOSTREAM

from ptc_agent.agent.middleware import model_resilience
from ptc_agent.agent.middleware.compaction import compact as compact_module
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.measure import Estimate
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summary_request import (
    finished_summary,
    keeps_turns,
    _NO_TAIL_NOTES,
    _NO_TAIL_STATE,
    summary_prompt,
)
from ptc_agent.agent.middleware.compaction.utils import parse_summary_message, summary_source
from src.llms.provider_compaction import COMPACTION_BETA

_FINISHED = "<summary>\nthe agent's own summary\n</summary>"


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    async def _passthrough(backend, messages, **kwargs):
        return messages

    monkeypatch.setattr(compact_module, "aoffload_base64_content", _passthrough)
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)

NOTES = "/workspace/.agents/scratchpad/thread01/note"



def _model() -> MagicMock:
    """A client of no provider that compacts."""
    model = MagicMock()
    model.model_name = "deepseek-v4-flash"
    model.metadata = {"provider_route": "deepseek"}
    return model


def _summary_model(name: str = "deepseek-v4-flash") -> MagicMock:
    model = MagicMock()
    model.model_name = name
    model.sent = []

    async def astream(request, config=None):
        model.sent.append(request)
        yield AIMessageChunk(content="the summarizer's summary")

    model.astream = astream
    return model


def _history() -> list:
    return [
        HumanMessage("q0", id="h0"),
        AIMessage("r0", id="a0"),
        HumanMessage("q1", id="h1"),
        AIMessage("r1", id="a1"),
        HumanMessage("q2", id="h2"),
        AIMessage("r2", id="a2"),
    ]


class _Handler:
    """The rest of the stack: answers the summary request with ``reply`` and
    anything else as the turn, recording how each was sent."""

    def __init__(self, reply: AIMessage):
        self.reply = reply
        self.requests: list[ModelRequest] = []
        self.tags: list[list[str]] = []
        self.as_sent: list[bool] = []

    async def __call__(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.tags.append(list((var_child_runnable_config.get() or {}).get("tags") or ()))
        self.as_sent.append(model_resilience._as_sent.get())
        if _asks_summary(request):
            return ModelResponse(result=[self.reply])
        return ModelResponse(result=[AIMessage("done", id="turn-answer")])


def _asks_summary(request: ModelRequest) -> bool:
    last = request.messages[-1] if request.messages else None
    return isinstance(last, HumanMessage) and last.content in (
        summary_prompt("self", True),
        summary_prompt("self", False),
        summary_prompt("self", False, notes=True),
    )


async def _run(
    reply: AIMessage | None = None,
    *,
    model=None,
    summary_model=None,
    self_summary: bool = True,
    native: bool = True,
    history: list | None = None,
    keep_tokens: int = 2,
    notes_dir: str | None = None,
):
    model = model or _model()
    summary_model = summary_model or _summary_model()
    mw = CompactionMiddleware(
        Summarizer(summary_model, limit=100_000, counter=len),
        token_threshold=6,
        keep_tokens=keep_tokens,
        native=native,
        self_summary=self_summary,
    )
    if notes_dir:
        mw = mw.with_scratchpad_notes(notes_dir)
    # A token a message for Claude too, so the kept part is the newest two.
    mw._estimate = lambda model: Estimate(len)
    handler = _Handler(reply or AIMessage(_FINISHED))
    history = history or _history()
    state = {"messages": history}
    request = ModelRequest(
        model=model,
        messages=history,
        system_message=SystemMessage("the agent's system prompt"),
        tools=[{"name": "Bash"}],
        state=state,
        runtime=None,
        model_settings={"max_tokens": 1000},
    )
    response = await mw.awrap_model_call(request, handler)
    summary = response.command.update["_summarization_event"]["summary_message"]
    asked = [i for i, r in enumerate(handler.requests) if _asks_summary(r)]
    return SimpleNamespace(
        handler=handler, summary=summary, summary_model=summary_model, asked=asked
    )


@pytest.mark.asyncio
class TestWritesItsOwn:
    async def test_its_own_request_with_the_prompt_last(self):
        run = await _run()
        assert run.asked == [0]
        sent = run.handler.requests[0]
        assert [m.id for m in sent.messages[:-1]] == ["h0", "a0", "h1", "a1"]
        assert sent.system_message.content == "the agent's system prompt"
        assert sent.tools == [{"name": "Bash"}]
        assert sent.model_settings == {"max_tokens": 1000}

    async def test_kept_out_of_the_stream_and_off_the_fallback_models(self):
        run = await _run()
        assert TAG_NOSTREAM in run.handler.tags[0]
        assert run.handler.as_sent[0] is True
        assert TAG_NOSTREAM not in run.handler.tags[-1]

    async def test_stored_without_its_tags(self):
        run = await _run()
        assert parse_summary_message(run.summary) == "the agent's own summary"
        assert summary_source(run.summary) == "self"
        assert run.summary_model.sent == []

    @pytest.mark.parametrize(
        "reply",
        [
            AIMessage("", tool_calls=[{"name": "Bash", "id": "c1", "args": {}}]),
            AIMessage("<summary>\nthe agent's own summ"),
            AIMessage("</summary>"),
        ],
        ids=["tool call", "cut off", "empty"],
    )
    async def test_anything_short_of_a_summary_goes_to_the_summarizer(self, reply):
        run = await _run(reply)
        assert run.asked == [0]
        assert parse_summary_message(run.summary) == "the summarizer's summary"
        assert summary_source(run.summary) == "model"


@pytest.mark.asyncio
class TestLeavesItToTheSummarizer:
    async def test_when_provider_compaction_is_switched_off(self, monkeypatch):
        for var in ("ANTHROPIC_BASE_URL", "ANTHROPIC_API_URL"):
            monkeypatch.delenv(var, raising=False)
        claude = ChatAnthropic(model="claude-test", api_key="x", betas=[COMPACTION_BETA])
        run = await _run(model=claude, summary_model=_summary_model("claude-test"), native=False)
        assert run.asked == []
        assert summary_source(run.summary) == "model"

    async def test_when_compaction_runs_on_another_model(self):
        run = await _run(summary_model=_summary_model("qwen3.8-flash"))
        assert run.asked == []

    async def test_when_switched_off(self):
        run = await _run(self_summary=False)
        assert run.asked == []


class TestFinishedSummary:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("<summary>\nbody\n</summary>", "body"),
            ("body\n</summary>\n", "body"),
            ("body", None),
            ("<summary>\nbody cut mid-wo", None),
            ("<summary>\n</summary>", None),
            ("Here it is.\n<summary>\nbody\n</summary>\nLet me know.", "body"),
            ("<summary>\nbody\nlast line.</summary>", "body\nlast line."),
            ("<summary>\nit quoted `</summary>` and was cut mid-wo", None),
        ],
    )
    def test_closing_line(self, text, expected):
        assert finished_summary(text) == expected


class TestWhatStaysAfterTheSummary:
    """A summary is told whether turns stay word for word after it, by what its
    cut keeps, whatever the reason the cut keeps nothing."""

    def test_only_an_answer_or_a_tool_result_is_a_kept_turn(self):
        from langchain_core.messages import ToolMessage

        assert not keeps_turns([])
        assert not keeps_turns([HumanMessage("the user's newest message")])
        assert keeps_turns([HumanMessage("q"), AIMessage("r")])
        assert keeps_turns([ToolMessage("result", tool_call_id="c1")])

    @pytest.mark.parametrize("kind", ["transcript", "native", "self", "handoff"])
    def test_a_thread_with_notes_is_asked_to_cite_them_only_when_no_turn_stays(self, kind):
        assert summary_prompt(kind, True, notes=True) == summary_prompt(kind, True)
        with_notes, without = summary_prompt(kind, False, notes=True), summary_prompt(kind, False)
        assert "checkpoint notes" in with_notes and "checkpoint notes" not in without
        assert with_notes == without.replace(_NO_TAIL_STATE, f"{_NO_TAIL_STATE}\n{_NO_TAIL_NOTES}")

    @pytest.mark.parametrize(
        ("history", "keep", "notes", "prompt"),
        [
            (_history(), 2, None, summary_prompt("self", True)),
            (_history()[:-1], 1, None, summary_prompt("self", False)),
            (_history(), 2, NOTES, summary_prompt("self", True)),
            (_history()[:-1], 1, NOTES, summary_prompt("self", False, notes=True)),
        ],
        ids=["keeps turns", "keeps only the user's message", "keeps turns, notes on", "keeps nothing, notes on"],
    )
    @pytest.mark.asyncio
    async def test_the_agents_own_summary(self, history, keep, notes, prompt):
        run = await _run(history=history, keep_tokens=keep, notes_dir=notes)

        (asked,) = run.asked
        assert run.handler.requests[asked].messages[-1].content == prompt

    @pytest.mark.parametrize(
        ("history", "keep", "notes", "prompt"),
        [
            (_history(), 2, None, summary_prompt("transcript", True)),
            (_history()[:-1], 1, None, summary_prompt("transcript", False)),
            (_history(), 2, NOTES, summary_prompt("transcript", True)),
            (_history()[:-1], 1, NOTES, summary_prompt("transcript", False, notes=True)),
        ],
        ids=["keeps turns", "keeps only the user's message", "keeps turns, notes on", "keeps nothing, notes on"],
    )
    @pytest.mark.asyncio
    async def test_the_transcript_summary(self, history, keep, notes, prompt):
        run = await _run(history=history, keep_tokens=keep, self_summary=False, notes_dir=notes)

        (sent,) = run.summary_model.sent
        assert sent[0].content.startswith(prompt)
