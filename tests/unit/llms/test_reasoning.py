"""
Tests for src.llms.reasoning — apply_reasoning_effort() multi-provider mapper.

Covers all provider detection patterns:
- OpenAI: parameters.reasoning.effort
- Anthropic adaptive: output_config.effort (via thinking.type=adaptive or output_config key)
- Anthropic enabled: thinking.budget_tokens
- Gemini 3.x: thinking_level
- Gemini 2.x: thinking_budget (numeric)
- vLLM/Groq/Cerebras: reasoning_effort
- Volcengine/Doubao: extra_body.thinking.type
- Dashscope/Qwen: extra_body.enable_thinking
- Combined extra_body patterns (always run regardless of parameters branch)
- Invalid level passthrough
"""

import copy

import pytest

from src.llms.reasoning import (
    REASONING_LEVELS,
    _ANTHROPIC_BUDGETS,
    _GEMINI_BUDGETS,
    apply_reasoning_effort,
)


# ---------------------------------------------------------------------------
# Invalid / passthrough
# ---------------------------------------------------------------------------


class TestReasoningInvalidLevel:
    def test_invalid_level_returns_unchanged(self):
        params = {"reasoning": {"effort": "medium"}}
        extra = {}
        result_params, result_extra = apply_reasoning_effort("invalid", params, extra)
        assert result_params["reasoning"]["effort"] == "medium"  # Unchanged

    def test_empty_string_returns_unchanged(self):
        params = {"reasoning": {"effort": "medium"}}
        extra = {}
        apply_reasoning_effort("", params, extra)
        assert params["reasoning"]["effort"] == "medium"

    def test_constants(self):
        assert REASONING_LEVELS == ("low", "medium", "high")
        assert "low" in _ANTHROPIC_BUDGETS
        assert "low" in _GEMINI_BUDGETS


# ---------------------------------------------------------------------------
# OpenAI: parameters.reasoning.effort
# ---------------------------------------------------------------------------


class TestOpenAIReasoning:
    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_sets_effort(self, level):
        params = {"reasoning": {"effort": "medium", "summary": "auto"}}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["reasoning"]["effort"] == level
        assert params["reasoning"]["summary"] == "auto"  # Other keys preserved

    def test_non_dict_reasoning_replaced(self):
        """If reasoning is not a dict, replace with dict containing effort."""
        params = {"reasoning": True}
        extra = {}
        apply_reasoning_effort("high", params, extra)
        assert params["reasoning"] == {"effort": "high"}


# ---------------------------------------------------------------------------
# Anthropic adaptive: output_config.effort
# ---------------------------------------------------------------------------


class TestAnthropicAdaptive:
    def test_output_config_present(self):
        """When output_config key exists, sets effort on it."""
        params = {"output_config": {"effort": "medium"}}
        extra = {}
        apply_reasoning_effort("high", params, extra)
        assert params["output_config"]["effort"] == "high"

    def test_thinking_adaptive_type(self):
        """When thinking.type == 'adaptive', sets output_config.effort."""
        params = {"thinking": {"type": "adaptive"}}
        extra = {}
        apply_reasoning_effort("low", params, extra)
        assert params["output_config"]["effort"] == "low"

    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_all_levels(self, level):
        params = {"thinking": {"type": "adaptive"}, "output_config": {"effort": "medium"}}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["output_config"]["effort"] == level


# ---------------------------------------------------------------------------
# Anthropic enabled: thinking.budget_tokens
# ---------------------------------------------------------------------------


class TestAnthropicEnabled:
    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_sets_budget_tokens(self, level):
        params = {"thinking": {"type": "enabled", "budget_tokens": 10000}}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["thinking"]["budget_tokens"] == _ANTHROPIC_BUDGETS[level]

    def test_non_dict_thinking_replaced(self):
        params = {"thinking": True}
        extra = {}
        apply_reasoning_effort("medium", params, extra)
        assert params["thinking"]["type"] == "enabled"
        assert params["thinking"]["budget_tokens"] == _ANTHROPIC_BUDGETS["medium"]


# ---------------------------------------------------------------------------
# Gemini 3.x: thinking_level
# ---------------------------------------------------------------------------


class TestGemini3xThinkingLevel:
    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_sets_level(self, level):
        params = {"thinking_level": "medium"}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["thinking_level"] == level


# ---------------------------------------------------------------------------
# Gemini 2.x: thinking_budget (numeric)
# ---------------------------------------------------------------------------


class TestGemini2xThinkingBudget:
    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_sets_numeric_budget(self, level):
        params = {"thinking_budget": 4096}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["thinking_budget"] == _GEMINI_BUDGETS[level]


# ---------------------------------------------------------------------------
# vLLM / Groq / Cerebras: reasoning_effort
# ---------------------------------------------------------------------------


class TestVLLMReasoningEffort:
    @pytest.mark.parametrize("level", ["low", "medium", "high"])
    def test_sets_effort(self, level):
        params = {"reasoning_effort": "medium"}
        extra = {}
        apply_reasoning_effort(level, params, extra)
        assert params["reasoning_effort"] == level


# ---------------------------------------------------------------------------
# Volcengine / Doubao: extra_body.thinking.type
# ---------------------------------------------------------------------------


class TestVolcengineThinking:
    def test_low_disables(self):
        params = {}
        extra = {"thinking": {"type": "enabled"}}
        apply_reasoning_effort("low", params, extra)
        assert extra["thinking"]["type"] == "disabled"

    @pytest.mark.parametrize("level", ["medium", "high"])
    def test_medium_high_enables(self, level):
        params = {}
        extra = {"thinking": {"type": "disabled"}}
        apply_reasoning_effort(level, params, extra)
        assert extra["thinking"]["type"] == "enabled"

    def test_non_dict_thinking(self):
        params = {}
        extra = {"thinking": True}
        apply_reasoning_effort("low", params, extra)
        assert extra["thinking"]["type"] == "disabled"


# ---------------------------------------------------------------------------
# Dashscope / Qwen: extra_body.enable_thinking
# ---------------------------------------------------------------------------


class TestDashscopeEnableThinking:
    def test_low_disables(self):
        params = {}
        extra = {"enable_thinking": True}
        apply_reasoning_effort("low", params, extra)
        assert extra["enable_thinking"] is False

    @pytest.mark.parametrize("level", ["medium", "high"])
    def test_medium_high_enables(self, level):
        params = {}
        extra = {"enable_thinking": False}
        apply_reasoning_effort(level, params, extra)
        assert extra["enable_thinking"] is True


# ---------------------------------------------------------------------------
# Combined: extra_body patterns run INDEPENDENTLY of parameters branch
# ---------------------------------------------------------------------------


class TestCombinedPatterns:
    def test_openai_plus_volcengine(self):
        """OpenAI reasoning AND volcengine extra_body should both be set."""
        params = {"reasoning": {"effort": "medium"}}
        extra = {"thinking": {"type": "enabled"}}
        apply_reasoning_effort("low", params, extra)
        assert params["reasoning"]["effort"] == "low"
        assert extra["thinking"]["type"] == "disabled"

    def test_anthropic_plus_dashscope(self):
        """Anthropic thinking AND dashscope extra_body should both be set."""
        params = {"thinking": {"type": "enabled", "budget_tokens": 10000}}
        extra = {"enable_thinking": True}
        apply_reasoning_effort("low", params, extra)
        assert params["thinking"]["budget_tokens"] == _ANTHROPIC_BUDGETS["low"]
        assert extra["enable_thinking"] is False

    def test_mutates_in_place(self):
        """apply_reasoning_effort should mutate and return the same objects."""
        params = {"reasoning": {"effort": "low"}}
        extra = {}
        result_params, result_extra = apply_reasoning_effort("high", params, extra)
        assert result_params is params
        assert result_extra is extra

    def test_no_matching_pattern_no_change(self):
        """If no pattern matches, parameters and extra_body stay unchanged."""
        params = {"temperature": 0.7, "max_tokens": 1000}
        extra = {"custom_field": True}
        original_params = copy.deepcopy(params)
        original_extra = copy.deepcopy(extra)
        apply_reasoning_effort("high", params, extra)
        assert params == original_params
        assert extra == original_extra
