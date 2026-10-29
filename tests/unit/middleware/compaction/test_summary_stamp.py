"""A compaction's summary rides on the response that answered from it.

The summary message lives only in ``_summarization_event``, which the next
compaction overwrites, so replay reads each summary, and where in the turn it
fired, off that response.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Compaction
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summarize import Summary
from ptc_agent.agent.middleware.compaction.utils import (
    build_summary_message,
    compaction_summary,
)
from ptc_agent.agent.middleware.compaction.measure import context_growth
from ptc_agent.agent.middleware.model_resilience import FALLBACK_KEY

pytestmark = pytest.mark.asyncio


class _Model(GenericFakeChatModel):
    """A model named "m", as its answers are."""

    model_name: str = "m"


def _history() -> list:
    return [
        HumanMessage("go", id="h0"),
        *(AIMessage(f"a{i}", id=f"a{i}", response_metadata={"model_name": "m"}) for i in range(8)),
    ]


def _middleware(summary_message: HumanMessage) -> CompactionMiddleware:
    async def compact(messages, view, cutoff, **_):
        preserved = view[cutoff:]
        return Compaction(
            event={
                "summary_message": summary_message,
                "cutoff_index": cutoff,
                "anchor_message_id": preserved[0].id,
            },
            summary=Summary("condensed", "model", view[:cutoff]),
            original_count=len(view),
            preserved=preserved,
        )

    return CompactionMiddleware(
        SimpleNamespace(counter=len, compact=compact),
        token_threshold=100,
        keep_tokens=2,
        self_summary=False,
    )


async def _answer(
    mw: CompactionMiddleware,
    measured: int,
    *,
    history: list | None = None,
    input_tokens: int = 0,
    metadata: dict | None = None,
) -> AIMessage:
    """The response to one call, reporting ``input_tokens`` from model "m"."""
    answer = AIMessage("done", id="ai-done", response_metadata={"model_name": "m", **(metadata or {})})
    if input_tokens:
        answer.usage_metadata = {
            "input_tokens": input_tokens,
            "output_tokens": 1,
            "total_tokens": input_tokens + 1,
        }

    async def handler(request):
        return ModelResponse(result=[answer])

    history = history or _history()
    request = ModelRequest(
        model=_Model(messages=iter([])),
        messages=history,
        system_message=None,
        tool_choice=None,
        tools=[],
        response_format=None,
        state={"messages": history, "_cached_input_tokens": measured},
        runtime=None,
    )
    await mw.awrap_model_call(request, handler)
    return answer


async def test_the_response_to_a_compacted_call_carries_the_signal_it_followed(
    monkeypatch,
):
    signals: list[dict] = []
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: signals.append)
    summary_message = build_summary_message("condensed", None, original_message_count=9)

    answer = await _answer(_middleware(summary_message), measured=500, input_tokens=600)

    (complete,) = [
        s for s in signals if s["action"] == "summarize" and s["signal"] == "complete"
    ]
    fields = {k: v for k, v in complete.items() if k not in ("type", "action", "signal")}
    assert compaction_summary(answer) == {
        "summary_message_id": summary_message.id,
        **fields,
    }
    assert fields == {
        "summary_length": len("condensed"),
        "original_message_count": 9,
        "summary_text": "condensed",
        "source": "model",
    }


async def test_a_call_that_did_not_compact_carries_no_summary():
    summary_message = build_summary_message("condensed", None)

    answer = await _answer(_middleware(summary_message), measured=50)

    assert compaction_summary(answer) is None


def _since(model: str, metadata: dict | None = None) -> list:
    """An answer from ``model``, and one message after it."""
    return [
        HumanMessage("go", id="h0"),
        AIMessage("a0", id="a0", response_metadata={"model_name": model, **(metadata or {})}),
        HumanMessage("more", id="h1"),
    ]


async def test_a_call_carries_what_its_context_grew_by_since_the_last():
    summary_message = build_summary_message("condensed", None)

    answer = await _answer(
        _middleware(summary_message), measured=50, history=_since("m"), input_tokens=80
    )

    # Measured from the last call's 50 input tokens; estimated as a0 and h1,
    # a token a message.
    assert context_growth(answer, "m") == (30, 2)
    # Another model counts the stretch its own way.
    assert context_growth(answer, "other") is None


async def test_no_growth_is_stamped_across_models_or_a_summary():
    summary_message = build_summary_message("condensed", None)

    other = await _answer(
        _middleware(summary_message), measured=50, history=_since("other"), input_tokens=80
    )
    compacted = await _answer(_middleware(summary_message), measured=500, input_tokens=600)

    assert context_growth(other, "m") is None
    assert context_growth(compacted, "m") is None


async def test_no_growth_is_stamped_across_a_fallback_answer():
    """Both answers can come from one fallback, which counts its own tokens."""
    summary_message = build_summary_message("condensed", None)
    fallback = {FALLBACK_KEY: "fallback-a"}

    after = await _answer(
        _middleware(summary_message), measured=50, history=_since("m", fallback), input_tokens=80
    )
    by = await _answer(
        _middleware(summary_message),
        measured=50,
        history=_since("m", fallback),
        input_tokens=80,
        metadata=fallback,
    )

    assert context_growth(after, "m") is None
    assert context_growth(by, "m") is None
