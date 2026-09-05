"""
Tests for PerCallTokenTracker billing_type attribution
and calculate_cost_from_per_call_records platform_cost logic.
"""

from unittest.mock import patch
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, LLMResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_llm_result(model_name="test-model", input_tokens=10, output_tokens=5):
    """Build an LLMResult that PerCallTokenTracker.on_llm_end can consume."""
    msg = AIMessage(
        content="test",
        response_metadata={"model_name": model_name},
        usage_metadata={
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    )
    gen = ChatGeneration(message=msg)
    return LLMResult(generations=[[gen]])


def _make_per_call_record(
    model_name="test-model",
    input_tokens=100,
    output_tokens=50,
    billing_type="platform",
):
    """Build a per-call record dict (as produced by PerCallTokenTracker)."""
    return {
        "model_name": model_name,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
        "billing_type": billing_type,
        "timestamp": "2025-01-01T00:00:00",
        "run_id": str(uuid4()),
        "parent_run_id": None,
    }


# Minimal pricing stub returned by find_model_pricing
_STUB_PRICING = {
    "input": 1.0,   # $1 per 1M input tokens
    "output": 2.0,  # $2 per 1M output tokens
}


# ===================================================================
# Test 1: PerCallTokenTracker billing_type tests
# ===================================================================

class TestPerCallTokenTrackerBillingType:
    """Verify billing_type flows through the tracker correctly."""

    def _make_tracker(self):
        from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker
        return PerCallTokenTracker()

    # -- on_chat_model_start captures billing_type --

    def test_billing_type_captured_from_on_chat_model_start(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"billing_type": "byok"},
        )

        result = make_llm_result()
        tracker.on_llm_end(result, run_id=run_id)

        records = tracker.get_per_call_records()
        assert len(records) == 1
        assert records[0]["billing_type"] == "byok"

    # -- on_llm_start captures billing_type --

    def test_billing_type_captured_from_on_llm_start(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        tracker.on_llm_start(
            serialized={},
            prompts=["test"],
            run_id=run_id,
            metadata={"billing_type": "oauth"},
        )

        result = make_llm_result()
        tracker.on_llm_end(result, run_id=run_id)

        records = tracker.get_per_call_records()
        assert len(records) == 1
        assert records[0]["billing_type"] == "oauth"

    # -- default billing_type is 'platform' --

    def test_default_billing_type_is_platform_when_no_metadata(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        # No on_llm_start / on_chat_model_start call at all
        result = make_llm_result()
        tracker.on_llm_end(result, run_id=run_id)

        records = tracker.get_per_call_records()
        assert len(records) == 1
        assert records[0]["billing_type"] == "platform"

    def test_default_billing_type_when_metadata_has_no_billing_type(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"some_other_key": "value"},  # no billing_type
        )

        result = make_llm_result()
        tracker.on_llm_end(result, run_id=run_id)

        records = tracker.get_per_call_records()
        assert len(records) == 1
        assert records[0]["billing_type"] == "platform"

    # -- on_llm_error cleans up the per-run attribution --

    def test_on_llm_error_cleans_up_run_attribution(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"billing_type": "byok"},
        )

        assert run_id in tracker._run_attribution

        tracker.on_llm_error(
            error=RuntimeError("test error"),
            run_id=run_id,
        )

        assert run_id not in tracker._run_attribution

    # -- reset() clears the per-run attribution --

    def test_reset_clears_run_attribution(self):
        tracker = self._make_tracker()
        run_id = uuid4()

        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"billing_type": "byok"},
        )

        assert len(tracker._run_attribution) == 1

        tracker.reset()

        assert len(tracker._run_attribution) == 0
        assert len(tracker.per_call_records) == 0
        assert len(tracker.usage_metadata) == 0

    # -- concurrent calls with different billing_types --

    def test_multiple_concurrent_calls_tracked_independently(self):
        tracker = self._make_tracker()
        run_id_1 = uuid4()
        run_id_2 = uuid4()
        run_id_3 = uuid4()

        # Start three calls with different billing types
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id_1,
            metadata={"billing_type": "platform"},
        )
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id_2,
            metadata={"billing_type": "byok"},
        )
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id_3,
            metadata={"billing_type": "oauth"},
        )

        # Complete them in reverse order
        tracker.on_llm_end(make_llm_result(model_name="model-c"), run_id=run_id_3)
        tracker.on_llm_end(make_llm_result(model_name="model-a"), run_id=run_id_1)
        tracker.on_llm_end(make_llm_result(model_name="model-b"), run_id=run_id_2)

        records = tracker.get_per_call_records()
        assert len(records) == 3

        # Build lookup by run_id
        by_run = {r["run_id"]: r for r in records}

        assert by_run[str(run_id_1)]["billing_type"] == "platform"
        assert by_run[str(run_id_1)]["model_name"] == "model-a"

        assert by_run[str(run_id_2)]["billing_type"] == "byok"
        assert by_run[str(run_id_2)]["model_name"] == "model-b"

        assert by_run[str(run_id_3)]["billing_type"] == "oauth"
        assert by_run[str(run_id_3)]["model_name"] == "model-c"

        # All attribution entries consumed
        assert len(tracker._run_attribution) == 0


# ===================================================================
# Test 2: calculate_cost_from_per_call_records platform_cost tests
# ===================================================================

PRICING_MODULE = "src.llms.pricing_utils"


class TestCalculateCostPlatformCost:
    """Verify platform_cost sums only platform-billed records."""

    def _calc(self, records):
        """Call calculate_cost_from_per_call_records with pricing mocked."""
        with patch(
            f"{PRICING_MODULE}.find_model_pricing", return_value=_STUB_PRICING
        ), patch(
            f"{PRICING_MODULE}.resolve_pricing_identity", return_value=("test", "test-model")
        ), patch(
            f"{PRICING_MODULE}.calculate_total_cost",
            side_effect=lambda **kw: {
                "total_cost": (
                    kw.get("input_tokens", 0) * 1e-6
                    + kw.get("output_tokens", 0) * 2e-6
                ),
                "breakdown": {},
            },
        ):
            from src.utils.tracking.core import calculate_cost_from_per_call_records
            return calculate_cost_from_per_call_records(records)

    def test_platform_cost_sums_only_platform_records(self):
        records = [
            _make_per_call_record(input_tokens=1000, output_tokens=500, billing_type="platform"),
            _make_per_call_record(input_tokens=2000, output_tokens=1000, billing_type="platform"),
        ]
        result = self._calc(records)

        # total_cost == platform_cost when all records are platform
        assert result["platform_cost"] == pytest.approx(result["total_cost"])
        assert result["platform_cost"] > 0

    def test_byok_records_excluded_from_platform_cost(self):
        records = [
            _make_per_call_record(input_tokens=1000, output_tokens=500, billing_type="byok"),
        ]
        result = self._calc(records)

        assert result["platform_cost"] == 0.0
        assert result["total_cost"] > 0

    def test_oauth_records_excluded_from_platform_cost(self):
        records = [
            _make_per_call_record(input_tokens=1000, output_tokens=500, billing_type="oauth"),
        ]
        result = self._calc(records)

        assert result["platform_cost"] == 0.0
        assert result["total_cost"] > 0

    def test_mixed_billing_types_correct_platform_vs_total(self):
        records = [
            _make_per_call_record(input_tokens=1000, output_tokens=0, billing_type="platform"),
            _make_per_call_record(input_tokens=1000, output_tokens=0, billing_type="byok"),
            _make_per_call_record(input_tokens=1000, output_tokens=0, billing_type="oauth"),
        ]
        result = self._calc(records)

        # Each record costs 1000 * 1e-6 = 0.001
        expected_per_call = 1000 * 1e-6
        assert result["total_cost"] == pytest.approx(expected_per_call * 3)
        assert result["platform_cost"] == pytest.approx(expected_per_call)

    def test_missing_billing_type_defaults_to_platform(self):
        record = _make_per_call_record(input_tokens=1000, output_tokens=0)
        # Remove billing_type from the record to test the .get() default
        del record["billing_type"]

        result = self._calc([record])

        # The code uses c.get("billing_type") == "platform"  -- missing key
        # returns None, so it does NOT match "platform".
        # But per_call_costs preserves record.get("billing_type", "platform").
        # Let's verify the actual behavior.
        per_call = result["per_call_costs"]
        assert len(per_call) == 1
        # The record-level billing_type gets "platform" default from
        # per_call_costs append: record.get("billing_type", "platform")
        assert per_call[0]["billing_type"] == "platform"
        # And platform_cost filters on per_call_costs, so it should match
        assert result["platform_cost"] == pytest.approx(result["total_cost"])
