"""
Tests for PerCallTokenTracker billing_type attribution,
calculate_cost_from_per_call_records platform_cost logic, and price_call,
the single-call pricing the LangSmith tracer shares with billing.
"""

import logging
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


# ===================================================================
# Test 3: price_call prices one call the way the batch does
# ===================================================================

# Synthetic cards: a reprice of any real model must not move these tests.
_FLAT_CARD = {"input": 3.0, "output": 15.0, "cached_input": 0.3}
_SCHEDULED_CARD = {
    "input": 2.0,
    "output": 8.0,
    "peak_utc": [[0, 12]],
    "off_peak": {"input": 1.0, "output": 4.0},
}
_SEAT_CARD = {"pricing_type": "subscription", "input": 3.0, "output": 15.0}


@pytest.fixture
def cards(monkeypatch):
    """Synthetic manifest: pricing id -> card; ``lookups`` counts each lookup."""
    table = {
        "flat-model": _FLAT_CARD,
        "scheduled-model": _SCHEDULED_CARD,
        "seat-model": _SEAT_CARD,
    }
    lookups: list[str] = []

    def find(pricing_id, provider=None):
        lookups.append(pricing_id)
        return table.get(pricing_id)

    monkeypatch.setattr(f"{PRICING_MODULE}.find_model_pricing", find)
    monkeypatch.setattr(
        f"{PRICING_MODULE}.resolve_pricing_identity",
        lambda name, billing_type="platform": (None, name),
    )
    return lookups


def _priced_record(model, *, billing_type="platform", hour=9):
    record = _make_per_call_record(
        model_name=model, input_tokens=4_000, output_tokens=600, billing_type=billing_type
    )
    record["usage"]["cached_tokens"] = 1_000
    record["started_at"] = f"2026-01-05T{hour:02d}:00:00+00:00"
    record["timestamp"] = f"2026-01-05T{hour:02d}:00:05+00:00"
    return record


class TestPriceCall:
    def test_an_unknown_model_is_unlisted_not_free(self, cards):
        from src.utils.tracking.core import calculate_cost_from_per_call_records, price_call

        record = _priced_record("no-such-model")

        assert price_call(record) == (None, None, False, 0.0, {}, False)
        # Billing still meters the call, at zero.
        billed = calculate_cost_from_per_call_records([record])
        assert billed["per_call_costs"][0]["cost"] == 0.0
        assert billed["by_model"]["no-such-model"]["call_count"] == 1

    def test_a_seat_priced_card_is_a_priced_zero(self, cards):
        from src.utils.tracking.core import price_call

        priced = price_call(_priced_record("seat-model"))

        assert priced.listed
        assert priced.cost == 0.0

    def test_resolver_looks_up_once_and_warns_once_per_platform_miss(self, cards, caplog):
        from src.utils.tracking.core import PricingResolver, price_call

        resolve = PricingResolver()
        with caplog.at_level(logging.DEBUG, logger="src.utils.tracking.core"):
            for _ in range(3):
                price_call(_priced_record("no-such-model"), resolve)
                price_call(_priced_record("flat-model"), resolve)
            price_call(_priced_record("no-such-model", billing_type="byok"), resolve)

        assert cards.count("no-such-model") == 2  # one per billing type
        assert cards.count("flat-model") == 1
        misses = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "No pricing found" in r.getMessage()
        ]
        assert len(misses) == 1

    def test_the_batch_is_the_sum_of_its_calls(self, cards):
        from src.utils.tracking.core import calculate_cost_from_per_call_records, price_call

        records = [
            _priced_record("flat-model"),
            _priced_record("flat-model", billing_type="byok"),
            _priced_record("scheduled-model", hour=9),   # peak
            _priced_record("scheduled-model", hour=18),  # off peak
            _priced_record("no-such-model"),
        ]

        billed = calculate_cost_from_per_call_records(records)
        singles = [price_call(r) for r in records]

        assert billed["total_cost"] > 0
        for row, single in zip(billed["per_call_costs"], singles):
            assert row["cost"] == single.cost
            assert row["breakdown"] == single.breakdown
            assert row.get("window") == single.window
        assert billed["total_cost"] == pytest.approx(sum(s.cost for s in singles))
        # The schedule really split the two calls, so the windows were compared.
        assert singles[2].cost > singles[3].cost


def test_a_response_with_no_response_metadata_still_bills():
    """Used to raise inside the callback, which langchain swallows: the call
    completed having metered nothing."""
    from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

    tracker = PerCallTokenTracker()
    message = AIMessage(
        content="ok",
        usage_metadata={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    )
    message.response_metadata = None
    result = LLMResult(
        generations=[[ChatGeneration(message=message)]],
        llm_output={"model_name": "vendor-model"},
    )

    tracker.on_llm_end(result, run_id=uuid4())

    (record,) = tracker.get_per_call_records()
    assert record["model_name"] == "vendor-model"
    assert record["usage"]["input_tokens"] == 10
