"""What a summary keeps, sized as the model counts it: measured where a
call measured it, estimated, as Claude counts for Claude, where not."""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import StructuredTool

from ptc_agent.agent.middleware.compaction import measure, utils
from ptc_agent.agent.middleware.compaction import middleware as middleware_module
from ptc_agent.agent.middleware.compaction.compact import Compaction
from ptc_agent.agent.middleware.compaction.measure import Estimate, keep_cutoff
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summarize import Summary
from ptc_agent.agent.middleware.compaction.utils import build_summary_message


def _call(tool_call_id: str) -> dict:
    return {"name": "web_search", "args": {"query": "q"}, "id": tool_call_id}


class TestKeepCutoff:
    """The tail a summary leaves is sized in tokens, never inside a group."""

    @staticmethod
    def _count(messages) -> int:
        """A message's tokens are its text's length."""
        return sum(len(m.text) for m in messages)

    def _history(self) -> list:
        return [
            HumanMessage("x" * 50, id="h0"),
            AIMessage("x" * 50, id="a1"),
            HumanMessage("x" * 10, id="h2"),
            AIMessage("", id="a3", tool_calls=[_call("c1"), _call("c2")]),
            ToolMessage("x" * 30, tool_call_id="c1", id="t4"),
            ToolMessage("x" * 30, tool_call_id="c2", id="t5"),
        ]

    def test_the_newest_messages_that_fit_stay(self):
        assert keep_cutoff(self._history(), tokens=70, estimate=Estimate(self._count)) == 2

    def test_a_cut_inside_a_group_moves_past_the_whole_group(self):
        # 40 fits t5 and half the group, which goes with its call.
        history = self._history()
        cutoff = keep_cutoff(history, tokens=40, estimate=Estimate(self._count))
        assert cutoff == 3
        assert [m.id for m in history[cutoff:]] == ["a3", "t4", "t5"]

    def test_the_newest_group_stays_past_the_budget(self):
        assert keep_cutoff(self._history(), tokens=0, estimate=Estimate(self._count)) == 3

    def test_attachments_count_as_the_target_is_charged(self):
        history = self._history()
        history[1] = AIMessage([{"type": "image", "base64": "AAAA"}], id="a1")
        # Free, a1 fits beside h2's group; at 70 tokens it is past the budget.
        assert keep_cutoff(history, tokens=80, estimate=Estimate(self._count, lambda b: 0)) == 1
        assert keep_cutoff(history, tokens=80, estimate=Estimate(self._count, lambda b: 70)) == 2

    def test_nothing_to_summarize_is_zero(self):
        assert keep_cutoff(self._history(), tokens=10**6, estimate=Estimate(self._count)) == 0


class TestMessageSizes:
    """The tail is sized as its model counts it: a stretch two calls
    measured by what it grew the context by, the rest estimated."""

    @staticmethod
    def _count(messages) -> int:
        return sum(len(m.text) for m in messages)

    def _history(self, h2: int = 30, growth=(100, 50, "m")) -> list:
        stamp = {measure.CONTEXT_GROWTH_KEY: list(growth)} if growth else {}
        return [
            HumanMessage("x" * 10, id="h0"),
            AIMessage("x" * 20, id="a1"),
            HumanMessage("x" * h2, id="h2"),
            AIMessage(
                "x" * 40,
                id="a3",
                response_metadata=stamp,
                usage_metadata={"input_tokens": 500, "output_tokens": 7, "total_tokens": 507},
            ),
            HumanMessage("x" * 60, id="h4"),
        ]

    def _estimate(self, model: str = "m") -> Estimate:
        return Estimate(self._count, model=model)

    def _sizes(self, history, model: str = "m") -> list[int]:
        return list(measure.message_sizes(history, self._estimate(model)))

    def test_a_measured_stretch_counts_as_measured(self):
        # Newest first: h4 and a3 estimated (a3's 7 output tokens are not
        # what the next call is sent), a1 and h2 sharing the 100 measured,
        # h0 estimated.
        assert self._sizes(self._history()) == [60, 40, 60, 40, 10]

    def test_a_stretch_cut_since_shrinks_at_its_measured_rate(self):
        # h2 is half the estimate it was measured at.
        assert self._sizes(self._history(h2=5)) == [60, 40, 10, 40, 10]

    def test_an_unmeasured_stretch_is_estimated(self):
        assert self._sizes(self._history(growth=None)) == [60, 40, 30, 20, 10]

    def test_a_stretch_measured_on_another_model_is_estimated(self):
        assert self._sizes(self._history(), model="other") == [60, 40, 30, 20, 10]

    def test_the_kept_tail_is_sized_by_the_measurement(self):
        # Estimated, h2 fits beside a3 and h4 under 130; measured at twice
        # its estimate, it does not.
        assert keep_cutoff(self._history(growth=None), tokens=130, estimate=self._estimate()) == 2
        assert keep_cutoff(self._history(), tokens=130, estimate=self._estimate()) == 3


class TestClaude:
    """Claude's tokens hold less text, so what no call measured is counted
    as Claude would, and its kept tail is larger."""

    ENGLISH = "The quarterly report shows revenue growth across all segments. " * 40
    CHINESE = "本季度财报显示各业务板块收入均有增长，主要受服务和可穿戴设备推动。" * 40

    @pytest.fixture(autouse=True)
    def _offline_encoder(self, monkeypatch):
        """cl100k_base fetches its BPE file on first use, which the unit
        suite's blocked sockets refuse on a cold cache (CI). A token a
        character keeps the weights each script gets under test."""
        fake = SimpleNamespace(encode=lambda text: [0] * len(text))
        monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: fake)

    def test_english_counts_well_over_tiktoken_and_chinese_about_the_same(self):
        for text, low, high in ((self.ENGLISH, 1.4, 1.8), (self.CHINESE, 0.85, 1.15)):
            ratio = utils.count_tokens_claude([HumanMessage(text)]) / utils.count_tokens_tiktoken(
                [HumanMessage(text)]
            )
            assert low < ratio < high

    def test_digits_are_held_to_about_what_tiktoken_counts(self, monkeypatch):
        """tiktoken packs three digits to a token, which the per-digit term
        would otherwise all but cancel."""
        packed = SimpleNamespace(encode=lambda text: [0] * -(-len(text) // 3))
        monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: packed)
        number = [HumanMessage("1234567890" * 600)]

        assert utils.count_tokens_claude(number) >= 0.95 * utils.count_tokens_tiktoken(number)

    def test_a_claude_model_keeps_its_own_larger_tail(self):
        mw = CompactionMiddleware(
            SimpleNamespace(counter=len), token_threshold=100, keep_tokens=2, claude_keep_tokens=4
        )
        mw._estimate = lambda model: Estimate(len)  # a token a message
        history = [HumanMessage(f"m{i}", id=f"m{i}") for i in range(8)]
        model = lambda name: SimpleNamespace(model_name=name)  # noqa: E731

        assert mw._cutoff(history, model("deepseek-flash")) == 6
        assert mw._cutoff(history, model("us.anthropic.claude-opus-5-5")) == 4


def test_no_tail_takes_more_than_a_third_of_the_threshold():
    claude = SimpleNamespace(model_name="claude-sonnet-5-5")
    other = SimpleNamespace(model_name="deepseek-flash")

    assert measure.keep_tokens_for(claude, 20_000, 32_000, 120_000) == 32_000
    assert measure.keep_tokens_for(other, 20_000, 32_000, 120_000) == 20_000
    # A 64k window's 48k threshold.
    assert measure.keep_tokens_for(claude, 20_000, 32_000, 48_000) == 16_000
    assert measure.keep_tokens_for(other, 20_000, 32_000, 48_000) == 16_000


class TestSentAhead:
    """Where no call measured the context, what the call sends ahead of its
    messages is estimated with them: the system message, the baseline block
    the stack appends to it further in, and the tool schemas, all of which
    the provider counts as input."""

    @staticmethod
    def _count(messages) -> int:
        """A message's tokens are its text's length."""
        return sum(len(m.text) for m in messages)

    def _middleware(self, compactions: list, *, baseline: bool) -> CompactionMiddleware:
        async def compact(messages, view, cutoff, **_):
            compactions.append(cutoff)
            return Compaction(
                event={
                    "summary_message": build_summary_message("condensed", None),
                    "cutoff_index": cutoff,
                    "anchor_message_id": view[cutoff].id,
                },
                summary=Summary("condensed", "model", view[:cutoff]),
                original_count=len(view),
                preserved=view[cutoff:],
            )

        mw = CompactionMiddleware(
            SimpleNamespace(counter=self._count, compact=compact),
            token_threshold=1_000,
            keep_tokens=0,
            self_summary=False,
        )
        mw._cutoff = lambda view, model: len(view) - 1
        return mw.with_baseline(lambda state: "b" * 300) if baseline else mw

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("tools", "baseline", "summarized"),
        [(True, True, True), (False, True, False), (True, False, False)],
        ids=["all_of_it", "no_tools", "no_baseline"],
    )
    async def test_an_unmeasured_turn_start_counts_what_the_call_sends(self, tools, baseline, summarized):
        # 500 of messages and system, 300 of baseline, about 400 of schema:
        # only all of it together is over the 1,000 threshold.
        compactions: list = []

        await self._turn_start(self._middleware(compactions, baseline=baseline), tools=tools)

        assert bool(compactions) is summarized

    @pytest.mark.asyncio
    async def test_what_the_call_sends_ahead_is_built_off_the_loop(self, monkeypatch):
        # The turn start's reading and the summary's fit each build it, every
        # schema converted and serialized; neither on the loop.
        built: list[int] = []
        real = middleware_module.sent_ahead

        def sent_ahead(request, appended=""):
            built.append(threading.get_ident())
            return real(request, appended)

        monkeypatch.setattr(middleware_module, "sent_ahead", sent_ahead)
        compactions: list = []

        await self._turn_start(self._middleware(compactions, baseline=True), tools=True)

        assert compactions and len(built) == 2
        assert threading.get_ident() not in built

    async def _turn_start(self, mw: CompactionMiddleware, *, tools: bool) -> None:
        lookup = StructuredTool.from_function(lambda query: query, name="lookup", description="d" * 300)
        messages = [HumanMessage("x" * 100, id="h0"), AIMessage("x" * 100, id="a1"), HumanMessage("x" * 100, id="h2")]

        async def handler(request):
            return ModelResponse(result=[AIMessage("done", id="done")])

        request = ModelRequest(
            model=GenericFakeChatModel(messages=iter([])),
            messages=messages,
            system_message=SystemMessage("s" * 200),
            tool_choice=None,
            tools=[lookup] if tools else [],
            response_format=None,
            state={"messages": messages},
            runtime=None,
        )
        await mw.awrap_model_call(request, handler)

    @pytest.mark.asyncio
    async def test_a_measured_context_reads_nothing_ahead(self):
        def ahead():
            raise AssertionError("read although the last call measured it")

        state = {"_cached_input_tokens": 900, "_cached_output_tokens": 10}
        view = [HumanMessage("x" * 10, id="h0"), AIMessage("x" * 20, id="a1")]

        reading = await measure.context_tokens(state, view, Estimate(self._count), ahead)
        assert reading.tokens == 910

    def test_the_schemas_are_sent_as_claude_is_sent_them(self):
        lookup = StructuredTool.from_function(lambda query: query, name="lookup", description="Look it up.")
        server = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}
        request = ModelRequest(
            model=None,
            messages=[],
            system_message=SystemMessage("the prompt"),
            tool_choice=None,
            tools=[lookup, server],
            response_format=None,
            state={},
            runtime=None,
        )

        system, baseline, schemas = measure.sent_ahead(request, "the baseline")
        assert (system.text, baseline.text) == ("the prompt", "the baseline")
        sent, kept = json.loads(schemas.text)
        assert (sent["name"], sent["description"], sent["input_schema"]["required"]) == (
            "lookup",
            "Look it up.",
            ["query"],
        )
        assert kept == server
