"""Compaction between turns, asked of the agent's own model node.

Manual /compact and the hand-off at a model switch run a real agent's model
node outside a graph run; the ``CompactionMiddleware`` in its chain answers
the ask instead of calling the model for a turn's answer. The agents here are
real ``create_agent`` builds on fake chat models, with a recorder innermost
that sees every request reaching the model.
"""

from __future__ import annotations

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import var_child_runnable_config

from ptc_agent.agent.middleware import model_resilience
from ptc_agent.agent.middleware.compaction import (
    Compact,
    NoCompaction,
    Compaction,
    HandOff,
    ask_between_turns,
)
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summary_request import summary_prompt
from ptc_agent.agent.middleware.compaction.utils import (
    build_summary_message,
    handed_off,
    parse_summary_message,
    with_summary_text,
)
from src.llms.provider_compaction import SIDECAR_KEY

pytestmark = pytest.mark.asyncio

_ITEM = {"type": "compaction", "encrypted_content": "opaque"}
_BLOCK = {"type": "compaction", "content": "the provider's summary", "signature": "sig-1"}
_NOTES = "\n\nYour notes are in .agents/scratchpad/abc/note/."


class _Sent(AgentMiddleware):
    """Innermost: every request that reaches the model, and whether it was
    made as sent (``model_resilience.as_sent``)."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: list = []
        self.as_sent: list[bool] = []

    async def awrap_model_call(self, request, handler):
        self.requests.append(request)
        self.as_sent.append(model_resilience._as_sent.get())
        return await handler(request)


def _model(*answers: AIMessage) -> GenericFakeChatModel:
    return GenericFakeChatModel(messages=iter(answers))


def _agent(model, *, summary="the summarizer's summary", compaction=True, keep_tokens=2, notes_dir=None):
    sent = _Sent()
    middleware = []
    if compaction:
        summarizer = Summarizer(_model(AIMessage(summary)), limit=100_000, counter=len)
        # Never past the threshold: only an ask summarizes.
        compaction_mw = CompactionMiddleware(summarizer, token_threshold=10**9, keep_tokens=keep_tokens)
        middleware.append(compaction_mw.with_scratchpad_notes(notes_dir) if notes_dir else compaction_mw)
    agent = create_agent(model=model, tools=[], middleware=[*middleware, sent])
    return agent, sent


def _history() -> list:
    return [
        HumanMessage("q0", id="h0"),
        AIMessage("r0", id="a0"),
        HumanMessage("q1", id="h1"),
        AIMessage("r1", id="a1"),
        HumanMessage("q2", id="h2"),
        AIMessage("r2", id="a2"),
    ]


def _summarized(head: HumanMessage) -> dict:
    """A thread whose view opens on ``head``, with h2 and a2 kept after it."""
    history = _history()
    return {
        "messages": history,
        "_summarization_event": {
            "summary_message": head,
            "cutoff_index": 4,
            "anchor_message_id": "h2",
        },
    }


def _opaque(**kwargs) -> HumanMessage:
    """A summary OpenAI compacted into an item only it can read."""
    return build_summary_message(
        "a digest of the compacted part",
        None,
        source="provider",
        native={"items": [_ITEM]},
        origin="openai",
        notes=_NOTES,
        **kwargs,
    )


class TestCompact:
    async def test_the_summary_is_written_without_calling_the_agents_model(self):
        agent, sent = _agent(_model())
        state = {"messages": _history()}

        answer = await ask_between_turns(agent, state, Compact(), thread_id="t-1")

        assert isinstance(answer, Compaction)
        assert sent.requests == []
        assert answer.summary.text == "the summarizer's summary"
        assert answer.summary.source == "model"
        assert [m.id for m in answer.preserved] == ["h2", "a2"]
        assert answer.event["anchor_message_id"] == "h2"
        # Nothing is written: the caller installs the answer.
        assert "_summarization_event" not in state

    async def test_a_view_no_longer_than_what_is_kept_has_nothing_to_compact(self):
        agent, sent = _agent(_model(), keep_tokens=6)

        with pytest.raises(ValueError, match="Nothing to compact yet"):
            await ask_between_turns(
                agent, {"messages": _history()}, Compact(), thread_id="t-1"
            )
        assert sent.requests == []

    async def test_an_agent_without_compaction_has_nothing_to_ask(self):
        agent, sent = _agent(_model(AIMessage("a turn's answer")), compaction=False)

        with pytest.raises(NoCompaction, match="no compaction"):
            await ask_between_turns(
                agent, {"messages": _history()}, Compact(), thread_id="t-1"
            )
        # Told apart before the model would be called as if for a turn.
        assert sent.requests == []

    async def test_the_ask_is_gone_once_answered(self):
        """A turn's call after a call between turns is a turn's again."""
        from ptc_agent.agent.middleware.compaction.between_turns import asked

        agent, _ = _agent(_model())
        await ask_between_turns(agent, {"messages": _history()}, Compact(), thread_id="t-1")

        assert asked() is None


class _Watcher(BaseCallbackHandler):
    """Records the model calls that report to it."""

    def __init__(self) -> None:
        self.started = 0
        self.ended = 0

    def on_chat_model_start(self, serialized, messages, **kwargs):
        self.started += 1

    def on_llm_end(self, response, **kwargs):
        self.ended += 1


class TestHandOff:
    async def test_the_call_reports_to_the_ambient_configs_callbacks(self):
        """Its caller bills it by setting a handler there, as a turn's own
        calls report to the turn's."""
        watcher = _Watcher()
        agent, _ = _agent(_model(AIMessage("Goal: chart AAPL.")))

        token = var_child_runnable_config.set({"callbacks": [watcher]})
        try:
            await ask_between_turns(agent, _summarized(_opaque()), HandOff(), thread_id="t-1")
        finally:
            var_child_runnable_config.reset(token)

        assert watcher.started == 1
        assert watcher.ended == 1

    @pytest.mark.parametrize("notes_dir", [None, "/workspace/.agents/scratchpad/thread01/note"], ids=["notes off", "notes on"])
    async def test_a_summary_no_turn_came_after_is_written_out_as_where_the_work_stands(self, notes_dir):
        head = _opaque()
        agent, sent = _agent(_model(AIMessage("Goal: chart AAPL.")), notes_dir=notes_dir)
        thread = _summarized(head)
        thread["_summarization_event"] = {"summary_message": head, "cutoff_index": len(_history())}

        await ask_between_turns(agent, thread, HandOff(), thread_id="t-1")

        (request,) = sent.requests
        prompt = summary_prompt("handoff", False, notes=notes_dir is not None)
        assert [m.content for m in request.messages] == [head.content, prompt]

    async def test_the_agents_model_writes_out_a_summary_only_its_provider_can_read(self):
        head = _opaque()
        agent, sent = _agent(_model(AIMessage("  Goal: chart AAPL. Progress: data pulled.\n")))

        written = await ask_between_turns(agent, _summarized(head), HandOff(), thread_id="t-1")

        (request,) = sent.requests
        assert request.tool_choice == "none"
        assert [m.content for m in request.messages] == [head.content, summary_prompt("handoff", True)]
        assert request.messages[0] is head
        # Only this model reading that answer can write it: no fallback, no
        # retry without it.
        assert sent.as_sent == [True]
        assert model_resilience._as_sent.get() is False

        text = "Goal: chart AAPL. Progress: data pulled."
        assert parse_summary_message(written) == text
        assert written.content.endswith(_NOTES)
        assert handed_off(written)
        assert written.id == head.id
        assert written.additional_kwargs[SIDECAR_KEY] == head.additional_kwargs[SIDECAR_KEY]
        assert written.additional_kwargs["summarize_complete"]["summary_length"] == len(text)
        # The checkpoint's message is left as it was.
        assert not handed_off(head)

    @pytest.mark.parametrize(
        "head",
        [
            pytest.param(
                lambda: build_summary_message(
                    "the provider's summary",
                    None,
                    source="provider",
                    native={"block": _BLOCK},
                    origin="anthropic",
                ),
                id="readable",
            ),
            pytest.param(
                lambda: build_summary_message("the summarizer's summary", None), id="plain"
            ),
            pytest.param(lambda: with_summary_text(_opaque(), "written out"), id="handed-off"),
        ],
    )
    async def test_a_summary_any_model_can_read_is_left_alone(self, head):
        agent, sent = _agent(_model())

        written = await ask_between_turns(
            agent, _summarized(head()), HandOff(), thread_id="t-1"
        )

        assert written is None
        assert sent.requests == []

    async def test_a_view_opening_on_no_summary_is_left_alone(self):
        agent, sent = _agent(_model())

        written = await ask_between_turns(
            agent, {"messages": _history()}, HandOff(), thread_id="t-1"
        )

        assert written is None
        assert sent.requests == []

    @pytest.mark.parametrize(
        "answer",
        [
            AIMessage(""),
            AIMessage("   "),
            AIMessage(
                "I will look it up first",
                tool_calls=[{"name": "Bash", "id": "c1", "args": {"command": "ls"}}],
            ),
        ],
        ids=["empty", "blank", "tool-call"],
    )
    async def test_no_written_summary_is_an_error(self, answer):
        agent, _ = _agent(_model(answer))

        with pytest.raises(ValueError, match="no hand-off summary"):
            await ask_between_turns(agent, _summarized(_opaque()), HandOff(), thread_id="t-1")
