"""What a summary keeps, sized as the model counts it: measured where a
call measured it, estimated, as Claude counts for Claude, where not."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction import measure, utils
from ptc_agent.agent.middleware.compaction.measure import Estimate, keep_cutoff
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware


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
