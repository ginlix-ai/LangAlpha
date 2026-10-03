"""A compaction that has to trim its input keeps the previous summary, and
each model it asks reads what its own window holds.

Trimming keeps the newest messages. After an earlier compaction the list
starts with that compaction's summary, the only copy of everything before it,
so a plain trim dropped it first and the new summary silently forgot the start
of the thread. The summary model is often the Background model, whose window
can be far smaller than the turn model's that falls back behind it, so each is
sent the history its window holds rather than one budget for both.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
    get_buffer_string,
)

from ptc_agent.agent.middleware.compaction.summarize import (
    Summary,
    awrite_summary,
    preparer,
)
from ptc_agent.agent.middleware.compaction.summary_request import trim_for_summary
from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.middleware.compaction.utils import build_summary_message

PRIOR = "Earlier: NVDA gross margin held at 75 percent; the AMD model is pending."


def _chars(messages) -> int:
    """The history as the request writes it out, a character to a token."""
    return sum(len(get_buffer_string([m])) for m in messages)


def _turns(n: int, size: int = 400) -> list:
    return [
        m
        for i in range(1, n + 1)
        for m in (
            HumanMessage(f"q{i} " + "x" * size, id=f"h{i}"),
            AIMessage(f"a{i} " + "y" * size, id=f"a{i}"),
        )
    ]


def _tool_loop(n: int, code: str, result: str) -> list:
    """An OpenAI-shaped tool loop: empty content, the work in the tool call."""
    return [
        m
        for i in range(n)
        for m in (
            AIMessage(
                "",
                id=f"c{i}",
                tool_calls=[{"name": "execute_code", "args": {"code": code}, "id": f"call{i}"}],
            ),
            ToolMessage(result, tool_call_id=f"call{i}", id=f"r{i}"),
        )
    ]


def test_the_previous_summary_is_kept_whole_and_the_rest_trimmed():
    summary = build_summary_message(PRIOR, None)
    history = [summary, *_turns(8)]
    budget = _chars([summary]) + _chars(history[-6:])

    kept = trim_for_summary(history, budget, _chars)

    assert kept[0] is summary
    assert kept[-1] is history[-1]
    assert "h2" not in {m.id for m in kept}
    assert _chars(kept) <= budget


def test_a_summary_from_before_the_stamp_is_kept_whole_too():
    # Summaries once carried only their opening, no lc_source stamp.
    legacy = HumanMessage(f"{CONTEXT_SUMMARY_PREFIX}{PRIOR}", id="old-summary")
    history = [legacy, *_turns(8)]
    budget = _chars([legacy]) + _chars(history[-6:])

    kept = trim_for_summary(history, budget, _chars)

    assert kept[0] is legacy
    assert kept[-1] is history[-1]


def test_a_kept_summary_and_a_tool_loop_fit_the_budget_together():
    # No human message follows the summary, so the human-first trim keeps
    # nothing and the fallback has to budget what remains on its own.
    summary = build_summary_message(PRIOR, None)
    history = [summary, *_tool_loop(20, "x" * 200, "y" * 200)]
    budget = _chars([summary]) + 1_000

    kept = trim_for_summary(history, budget, _chars)

    assert kept[0] is summary
    assert kept[-1] is history[-1]
    assert _chars(kept) <= budget


def test_a_newest_message_over_the_budget_is_cut_to_its_tail():
    # Keeping the summary alone would summarize none of the turns since it.
    summary = build_summary_message(PRIOR, None)
    rows = "\n".join(f"row {i}: " + "z" * 40 for i in range(200))
    history = [summary, *_tool_loop(1, "print(read())", rows)]

    kept = trim_for_summary(history, _chars([summary]) + 1_000, _chars)

    assert kept[0] is summary
    assert [m.id for m in kept[1:]] == ["r0"]
    assert "row 199" in kept[-1].content
    assert "row 0:" not in kept[-1].content


def test_a_summary_leaving_no_room_for_the_newest_result_is_trimmed_with_the_rest():
    # One line cannot be cut down to the room the summary leaves, but it fits
    # the budget alone: the summary gives way rather than the budget.
    summary = build_summary_message(PRIOR, None)
    history = [summary, *_tool_loop(1, "print(rows)", "z" * 1_000)]
    budget = _chars(history[1:]) + 10

    kept = trim_for_summary(history, budget, _chars)

    assert summary not in kept
    assert kept[-1] is history[-1]
    assert _chars(kept) <= budget


class _Model:
    def __init__(self, name: str, window: int, *, fails: bool = False):
        self.model_name = name
        self.profile = {"max_input_tokens": window}
        self.fails = fails
        self.sent: list = []

    async def ainvoke(self, request):
        self.sent.append(request)
        if self.fails:
            raise RuntimeError("context length exceeded")
        return AIMessage("the new summary")


@pytest.mark.asyncio
async def test_each_model_reads_what_its_own_window_holds():
    # The Background model's window is far smaller than the turn model's, so
    # one budget sized to either would overflow the one or starve the other.
    history = _turns(40, size=2_000)
    background = _Model("background", 60_000, fails=True)
    turn_model = _Model("default", 200_000)

    async def render(trimmed):
        return [HumanMessage(get_buffer_string(trimmed))]

    summary = await awrite_summary(
        model=background,
        fallback=turn_model,
        prepare=preparer(history, limit=None, counter=_chars, render=render),
        server=lambda: Summary("server", "server", []),
        budget=30.0,
    )

    assert summary.source == "fallback"
    small, large = (m.sent[0][0].content for m in (background, turn_model))
    # Tokenizers count above tiktoken, so history fills at most 70% of each.
    assert len(small) <= 60_000 * 0.7
    assert len(large) <= 200_000 * 0.7
    assert len(large) > 2 * len(small)
    assert summary.covered[-1] is history[-1]


@pytest.mark.asyncio
async def test_nothing_is_sent_when_not_even_the_newest_message_fits():
    # A request over the limit is never sent, even to a model whose window
    # would take it: the chain goes on to the server summary.
    history = _tool_loop(1, "print(rows)", "z" * 5_000)
    model = _Model("background", 1_000_000)

    async def render(trimmed):
        return [HumanMessage(get_buffer_string(trimmed))]

    summary = await awrite_summary(
        model=model,
        fallback=None,
        prepare=preparer(history, limit=1_000, counter=_chars, render=render),
        server=lambda: Summary("server", "server", []),
        budget=30.0,
    )

    assert summary.source == "server"
    assert model.sent == []
