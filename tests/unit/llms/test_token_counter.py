"""Tests for token usage tracking and extraction.

Covers extract_token_usage, TokenUsageRecord, TokenUsageTracker, and
global tracker helpers in src/llms/token_counter.py.
"""

from types import SimpleNamespace

import pytest

from src.llms.token_counter import (
    TokenUsageRecord,
    TokenUsageTracker,
    extract_cache_from_details,
    extract_token_usage,
    get_global_tracker,
    reset_global_tracker,
)


# ---------------------------------------------------------------------------
# extract_token_usage
# ---------------------------------------------------------------------------


class TestExtractTokenUsage:
    """Extract token counts from various response formats."""

    def test_usage_metadata_dict(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
            }
        )
        info = extract_token_usage(response)
        assert info["input_tokens"] == 100
        assert info["output_tokens"] == 50
        assert info["total_tokens"] == 150

    def test_usage_metadata_with_cache(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 200,
                "output_tokens": 80,
                "total_tokens": 280,
                "input_token_details": {"cache_read": 120},
            }
        )
        info = extract_token_usage(response)
        assert info["cached_tokens"] == 120

    def test_usage_metadata_with_reasoning(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 200,
                "output_tokens": 80,
                "total_tokens": 280,
                "output_token_details": {"reasoning": 40},
            }
        )
        info = extract_token_usage(response)
        assert info["reasoning_tokens"] == 40

    def test_response_metadata_standard_api(self):
        response = SimpleNamespace(
            response_metadata={
                "token_usage": {
                    "prompt_tokens": 300,
                    "completion_tokens": 100,
                    "total_tokens": 400,
                }
            }
        )
        info = extract_token_usage(response)
        assert info["input_tokens"] == 300
        assert info["output_tokens"] == 100

    def test_anthropic_format(self):
        response = SimpleNamespace(
            response_metadata={
                "usage": {
                    "input_tokens": 500,
                    "output_tokens": 200,
                    "cache_read_input_tokens": 100,
                }
            }
        )
        info = extract_token_usage(response)
        assert info["input_tokens"] == 500
        assert info["cached_tokens"] == 100

    def test_anthropic_cache_creation(self):
        response = SimpleNamespace(
            response_metadata={
                "usage": {
                    "input_tokens": 500,
                    "output_tokens": 200,
                    "cache_creation": {
                        "ephemeral_5m_input_tokens": 1000,
                        "ephemeral_1h_input_tokens": 500,
                    },
                }
            }
        )
        info = extract_token_usage(response)
        assert info["cache_5m_tokens"] == 1000
        assert info["cache_1h_tokens"] == 500

    def test_empty_response(self):
        response = SimpleNamespace()
        info = extract_token_usage(response)
        assert info == {}

    def test_none_cache_values_skipped(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                "input_token_details": {"cache_read": None},
            }
        )
        info = extract_token_usage(response)
        assert "cached_tokens" not in info

    def test_usage_metadata_with_ephemeral_5m(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 28342,
                "output_tokens": 286,
                "total_tokens": 28628,
                "input_token_details": {
                    "cache_read": 20459,
                    "cache_creation": 0,
                    "ephemeral_5m_input_tokens": 7882,
                },
            }
        )
        info = extract_token_usage(response)
        assert info["cached_tokens"] == 20459
        assert info["cache_5m_tokens"] == 7882
        assert "cache_1h_tokens" not in info

    def test_usage_metadata_with_ephemeral_1h(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 500,
                "output_tokens": 50,
                "total_tokens": 550,
                "input_token_details": {
                    "cache_read": 100,
                    "ephemeral_1h_input_tokens": 300,
                },
            }
        )
        info = extract_token_usage(response)
        assert info["cached_tokens"] == 100
        assert info["cache_1h_tokens"] == 300

    def test_usage_metadata_ephemeral_none_skipped(self):
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                "input_token_details": {
                    "cache_read": 0,
                    "ephemeral_5m_input_tokens": None,
                    "ephemeral_1h_input_tokens": None,
                },
            }
        )
        info = extract_token_usage(response)
        assert "cache_5m_tokens" not in info
        assert "cache_1h_tokens" not in info

    def test_anthropic_combined_paths(self):
        """Real-world Anthropic response has both usage_metadata and response_metadata."""
        response = SimpleNamespace(
            usage_metadata={
                "input_tokens": 28342,
                "output_tokens": 286,
                "total_tokens": 28628,
                "input_token_details": {
                    "cache_read": 20459,
                    "cache_creation": 0,
                    "ephemeral_5m_input_tokens": 7882,
                    "ephemeral_1h_input_tokens": 0,
                },
            },
            response_metadata={
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 286,
                    "cache_read_input_tokens": 20459,
                    "cache_creation": {
                        "ephemeral_5m_input_tokens": 7882,
                        "ephemeral_1h_input_tokens": 0,
                    },
                },
                "model_name": "claude-sonnet-4-6",
            },
        )
        info = extract_token_usage(response)
        # input_tokens from Path A (LangChain normalized total)
        assert info["input_tokens"] == 28342
        assert info["output_tokens"] == 286
        assert info["cached_tokens"] == 20459
        assert info["cache_5m_tokens"] == 7882
        assert "cache_1h_tokens" not in info


# ---------------------------------------------------------------------------
# extract_cache_from_details
# ---------------------------------------------------------------------------


class TestExtractCacheFromDetails:
    """Shared helper for extracting cache tokens from input_token_details."""

    def test_flat_format(self):
        details = {
            "cache_read": 20459,
            "ephemeral_5m_input_tokens": 7882,
            "ephemeral_1h_input_tokens": 0,
        }
        result = extract_cache_from_details(details)
        assert result["cached_tokens"] == 20459
        assert result["cache_5m_tokens"] == 7882
        assert "cache_1h_tokens" not in result

    def test_dict_format(self):
        details = {
            "cache_read": 100,
            "cache_creation": {
                "ephemeral_5m_input_tokens": 500,
                "ephemeral_1h_input_tokens": 200,
            },
        }
        result = extract_cache_from_details(details)
        assert result["cached_tokens"] == 100
        assert result["cache_5m_tokens"] == 500
        assert result["cache_1h_tokens"] == 200

    def test_int_format_legacy(self):
        details = {"cache_read": 0, "cache_creation": 1000}
        result = extract_cache_from_details(details)
        assert result["cache_5m_tokens"] == 1000
        assert "cached_tokens" not in result

    def test_int_format_skipped_when_ephemeral_present(self):
        details = {
            "ephemeral_5m_input_tokens": 800,
            "cache_creation": 1000,
        }
        result = extract_cache_from_details(details)
        # Flat ephemeral key takes priority; int cache_creation ignored
        assert result["cache_5m_tokens"] == 800

    def test_none_details(self):
        assert extract_cache_from_details(None) == {}

    def test_empty_details(self):
        assert extract_cache_from_details({}) == {}

    def test_none_values_skipped(self):
        details = {
            "cache_read": None,
            "ephemeral_5m_input_tokens": None,
        }
        result = extract_cache_from_details(details)
        assert result == {}


# ---------------------------------------------------------------------------
# TokenUsageTracker
# ---------------------------------------------------------------------------


class TestTokenUsageTracker:
    """TokenUsageTracker aggregation."""

    def test_add_usage(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
            model="gpt-5",
            operation="test",
        )
        assert len(tracker.records) == 1
        assert tracker.model_totals["gpt-5"]["input_tokens"] == 100
        assert tracker.model_totals["gpt-5"]["call_count"] == 1

    def test_a_model_from_any_manifest_catalog_is_priced(self):
        # Priced from the manifest, whichever provider's catalog holds it.
        from src.llms.llm import LLM

        model = LLM.get_model_config().manifest["models"]["xai"][0]["id"]
        tracker = TokenUsageTracker()
        tracker.add_usage({"input_tokens": 1000, "output_tokens": 1000, "total_tokens": 2000}, model=model)
        assert tracker.estimate_cost()["total_cost"] > 0

    def test_add_empty_usage_skipped(self):
        tracker = TokenUsageTracker()
        tracker.add_usage({}, model="gpt-5")
        # Empty dict is falsy, so add_usage returns early (same as None)
        assert len(tracker.records) == 0

    def test_add_none_usage(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(None, model="gpt-5")
        assert len(tracker.records) == 0

    def test_multiple_calls_same_model(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
            model="gpt-5",
        )
        tracker.add_usage(
            {"input_tokens": 200, "output_tokens": 100, "total_tokens": 300},
            model="gpt-5",
        )
        assert tracker.model_totals["gpt-5"]["input_tokens"] == 300
        assert tracker.model_totals["gpt-5"]["call_count"] == 2

    def test_operation_totals(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {"input_tokens": 50, "output_tokens": 25, "total_tokens": 75},
            operation="plan",
        )
        assert tracker.operation_totals["plan"]["total_tokens"] == 75

    def test_get_summary(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
            model="test-model",
        )
        summary = tracker.get_summary()
        assert summary["total_calls"] == 1
        assert summary["total_input_tokens"] == 100
        assert summary["total_output_tokens"] == 50
        assert summary["average_tokens_per_call"] == 150

    def test_get_summary_with_details(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
            model="m",
            operation="op",
        )
        summary = tracker.get_summary(include_details=True)
        assert "by_model" in summary
        assert "by_operation" in summary

    def test_get_summary_empty(self):
        tracker = TokenUsageTracker()
        summary = tracker.get_summary()
        assert summary["total_calls"] == 0
        assert summary["average_tokens_per_call"] == 0

    def test_reasoning_tokens_aggregation(self):
        tracker = TokenUsageTracker()
        tracker.add_usage(
            {
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                "reasoning_tokens": 30,
            },
            model="gpt-5",
        )
        assert tracker.model_totals["gpt-5"]["reasoning_tokens"] == 30
        summary = tracker.get_summary()
        assert summary["total_reasoning_tokens"] == 30


# ---------------------------------------------------------------------------
# Global tracker helpers
# ---------------------------------------------------------------------------


class TestGlobalTracker:
    """Global tracker singleton helpers."""

    def test_get_creates_singleton(self):
        reset_global_tracker()
        t1 = get_global_tracker()
        t2 = get_global_tracker()
        assert t1 is t2

    def test_reset_creates_new(self):
        t1 = get_global_tracker()
        reset_global_tracker()
        t2 = get_global_tracker()
        assert t1 is not t2
