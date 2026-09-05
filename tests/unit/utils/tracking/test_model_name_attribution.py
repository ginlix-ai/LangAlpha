"""Billing keys on our manifest key, not on the name the provider echoes back.

Two failures came out of trusting the echo. langchain concatenates conflicting
strings when it merges streamed metadata, so a provider that repeats model_name on
every finish_reason chunk yields the name written N times, which matches no pricing
and bills as zero. And the echo is usually a bare model_id, which several manifest
keys share while declaring providers whose rates differ.
"""

import logging
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.outputs import ChatGeneration, LLMResult

from src.utils.tracking.per_call_token_tracker import (
    PerCallTokenTracker,
    collapse_repeated_name,
)

PRICING_MODULE = "src.llms.pricing_utils"


def _result(model_name):
    msg = AIMessage(
        content="ok",
        response_metadata={"model_name": model_name},
        usage_metadata={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    )
    return LLMResult(generations=[[ChatGeneration(message=msg)]])


class TestCollapseRepeatedName:
    def test_a_doubled_name_collapses(self):
        assert collapse_repeated_name("glm-5.2glm-5.2") == "glm-5.2"

    def test_a_tripled_name_collapses(self):
        name = "z-ai/glm-5.1-20260406"
        assert collapse_repeated_name(name * 3) == name

    def test_a_clean_name_is_untouched(self):
        assert collapse_repeated_name("glm-5.2") == "glm-5.2"

    def test_a_name_that_merely_repeats_a_fragment_is_untouched(self):
        assert collapse_repeated_name("gpt-5-gpt-5-mini") == "gpt-5-gpt-5-mini"

    def test_a_composite_repeat_count_collapses_all_the_way(self):
        """Taking the longest unit would only ever repair a prime repeat count.

        A four-chunk stream is the common case and would collapse to a still
        unusable doubled name, which fails exactly the way the raw echo does.
        """
        assert collapse_repeated_name("glm-5.2" * 4) == "glm-5.2"
        assert collapse_repeated_name("glm-5.2" * 6) == "glm-5.2"
        assert collapse_repeated_name("glm-5.2" * 9) == "glm-5.2"
        assert collapse_repeated_name("aaaa") == "a"

    def test_an_empty_name_survives(self):
        assert collapse_repeated_name("") == ""

    def test_langchains_chunk_merge_is_what_doubles_the_name(self):
        """Pins the upstream behavior this function exists for, rather than a
        doubled literal we typed ourselves.

        The repair is only correct while langchain keeps concatenating
        conflicting model_name values across streamed chunks. If it ever joins
        the exemption set, collapsing becomes dead code that can only damage a
        legitimate name, and nothing else in this suite would notice.
        """
        chunk = AIMessageChunk(content="", response_metadata={"model_name": "glm-5.2"})
        merged = chunk + chunk + chunk

        assert merged.response_metadata["model_name"] == "glm-5.2" * 3
        assert collapse_repeated_name(merged.response_metadata["model_name"]) == "glm-5.2"


class TestTrackerBillsOnTheManifestKey:
    def test_the_stamped_key_wins_over_the_echo(self):
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"billing_type": "byok", "manifest_model": "qwen3.8-max-intl"},
        )
        tracker.on_llm_end(_result("qwen3.8-max"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "qwen3.8-max-intl"
        assert record["served_model"] == "qwen3.8-max"

    def test_the_stamp_survives_a_doubled_echo(self):
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"manifest_model": "glm-5.1-cn"},
        )
        tracker.on_llm_end(_result("glm-5.1glm-5.1"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "glm-5.1-cn"
        assert record["served_model"] == "glm-5.1glm-5.1"

    def test_an_unstamped_client_falls_back_to_the_collapsed_echo(self):
        """A consumer-supplied client (AgentConfig.create(llm=...)) carries no stamp."""
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_llm_end(_result("glm-5.2glm-5.2"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "glm-5.2"
        assert record["served_model"] == "glm-5.2glm-5.2"

    def test_the_aggregate_buckets_under_the_same_key(self):
        tracker = PerCallTokenTracker()
        for echo in ("glm-5.2", "glm-5.2glm-5.2", "glm-5.2glm-5.2glm-5.2"):
            tracker.on_llm_end(_result(echo), run_id=uuid4())

        assert list(tracker.get_aggregated_usage()) == ["glm-5.2"]
        assert len(tracker.get_per_call_records()) == 3

    def test_a_failed_attempt_leaves_no_stamp_behind(self):
        """Each fallback attempt is its own run_id; a dead one must not leak its key."""
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"manifest_model": "glm-5.1-cn"},
        )
        tracker.on_llm_error(RuntimeError("upstream 500"), run_id=run_id)
        tracker.on_llm_end(_result("glm-5.2"), run_id=run_id)

        assert tracker.get_per_call_records()[0]["model_name"] == "glm-5.2"

    def test_a_non_string_stamp_degrades_to_the_echo_rather_than_poisoning_the_row(self):
        """Run metadata is not only ours: a consumer-supplied client, or any caller
        passing config={"metadata": ...}, can land a non-string here.

        Unguarded it became the billing key, went into the record, and made the
        turn's pricing pass raise on an unhashable dict key, zeroing every model in
        the turn. Dropping it is the same degradation an unstamped client already
        gets.
        """
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={
                "billing_type": "platform",
                "manifest_model": ["glm-5.1-cn"],
                "pricing_model_id": {"id": "glm-5.1"},
            },
        )
        tracker.on_llm_end(_result("glm-5.2"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "glm-5.2"
        assert record["pricing_model_id"] is None
        # Every key that reaches pricing has to be usable as a dict key.
        assert isinstance(record["model_name"], str)

    def test_an_overlong_stamp_is_rejected_rather_than_cut_to_fit(self):
        """The stamp is bounded like the echo, but dropped instead of truncated.

        Both land in a JSON column, so neither may be unbounded. Cutting a manifest
        key to fit would leave a key matching no rate card, billing zero -- the exact
        failure the stamp exists to prevent -- while dropping it degrades to the echo
        and the working fallback behind it.
        """
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={},
            messages=[],
            run_id=run_id,
            metadata={"manifest_model": "glm-" + "5" * 9000, "pricing_provider": "z-ai"},
        )
        tracker.on_llm_end(_result("glm-5.2"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "glm-5.2"
        assert record["pricing_provider"] == "z-ai"

    def test_a_stamp_at_the_bound_is_still_honored(self):
        """The longest identity we ship is well under the cap, so the boundary is
        inclusive and no real key can be near it."""
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        at_limit = "g" * 256
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id,
            metadata={"manifest_model": at_limit},
        )
        tracker.on_llm_end(_result("glm-5.2"), run_id=run_id)

        assert tracker.get_per_call_records()[0]["model_name"] == at_limit

    def test_a_non_string_echo_does_not_cost_the_call_its_metering(self):
        """``response_metadata`` is provider-parsed JSON, not a value we chose.

        A non-string there used to raise inside the callback, and langchain
        swallows callback exceptions, so the call would complete having metered
        nothing at all — the stamped key it did not need the echo for included.
        """
        tracker = PerCallTokenTracker()
        for echo in ({"id": "glm-5.2"}, 42, ["glm-5.2"], None):
            run_id = uuid4()
            tracker.on_chat_model_start(
                serialized={},
                messages=[],
                run_id=run_id,
                metadata={"manifest_model": "glm-5.1-cn"},
            )
            tracker.on_llm_end(_result(echo), run_id=run_id)

        records = tracker.get_per_call_records()
        assert len(records) == 4
        assert {r["model_name"] for r in records} == {"glm-5.1-cn"}
        assert {r["served_model"] for r in records} == {None}

    def test_an_unstamped_non_string_echo_skips_rather_than_raises(self):
        tracker = PerCallTokenTracker()
        tracker.on_llm_end(_result({"id": "glm-5.2"}), run_id=uuid4())
        assert tracker.get_per_call_records() == []

    def test_the_echo_is_bounded_before_it_reaches_a_json_column(self):
        """The chunk merge that motivates the collapse makes the echo's length
        scale with output tokens, and a name that repairs to nothing still lands
        in the row. Collapse runs first: the repetition is longer than the cap,
        so cutting first would strand a name the repair would have recovered.
        """
        tracker = PerCallTokenTracker()
        tracker.on_llm_end(_result("glm-5.2" * 5000), run_id=uuid4())
        tracker.on_llm_end(_result("x" * 9000 + "!"), run_id=uuid4())

        collapsible, unrepairable = tracker.get_per_call_records()
        assert collapsible["model_name"] == "glm-5.2"
        assert len(collapsible["served_model"]) == 256
        assert len(unrepairable["model_name"]) == 256
        assert len(unrepairable["served_model"]) == 256

    def test_the_tracker_stamps_the_start_of_the_request(self):
        """The producer half of the peak hour join. Every consumer test builds
        this field by hand, so dropping it here would price every
        schedule_anchor=request call at peak and make straddle detection report
        zero forever, with both suites still green.
        """
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id, metadata={"manifest_model": "glm-5.1-cn"}
        )
        tracker.on_llm_end(_result("glm-5.1"), run_id=run_id)

        record = tracker.get_per_call_records()[0]
        started = datetime.fromisoformat(record["started_at"])
        ended = datetime.fromisoformat(record["timestamp"])
        assert started.tzinfo is not None and ended.tzinfo is not None
        assert started <= ended

    def test_an_unstamped_client_is_still_anchored_in_time(self):
        """Peak hour pricing applies to a consumer-supplied client too, so the
        start stamp is written even when no billing attribution came with it.
        """
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(serialized={}, messages=[], run_id=run_id, metadata={})
        tracker.on_llm_end(_result("glm-5.2"), run_id=run_id)

        assert tracker.get_per_call_records()[0]["started_at"] is not None

    def test_no_name_anywhere_still_skips_the_record(self):
        tracker = PerCallTokenTracker()
        msg = AIMessage(
            content="ok",
            response_metadata={},
            usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
        )
        result = LLMResult(generations=[[ChatGeneration(message=msg)]])
        tracker.on_llm_end(result, run_id=uuid4())

        assert tracker.get_per_call_records() == []


class TestPricingMissIsAudibleOnPlatformCalls:
    """A miss contributes zero to platform_cost, the figure the turn is billed on."""

    def _run(self, billing_type, caplog):
        from src.utils.tracking.core import calculate_cost_from_per_call_records

        record = {
            "model_name": "not-a-real-model-xyz",
            "served_model": "not-a-real-model-xyz",
            "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
            "billing_type": billing_type,
            "timestamp": "2026-01-01T00:00:00",
            "run_id": str(uuid4()),
            "parent_run_id": None,
        }
        with patch(f"{PRICING_MODULE}.find_model_pricing", return_value=None):
            with caplog.at_level(logging.WARNING, logger="src.utils.tracking.core"):
                result = calculate_cost_from_per_call_records([record])
        return result, caplog

    def test_a_platform_miss_warns(self, caplog):
        result, caplog = self._run("platform", caplog)
        assert result["platform_cost"] == 0.0
        assert any("No pricing found" in r.message for r in caplog.records)

    def test_a_byok_miss_stays_quiet(self, caplog):
        _result_, caplog = self._run("byok", caplog)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_tokens_are_still_recorded_on_a_miss(self, caplog):
        result, _ = self._run("platform", caplog)
        assert result["by_model"]["not-a-real-model-xyz"]["total_tokens"] == 15


class TestAnOffManifestStampStillPrices:
    """A user-defined model's key is its display name, which is in no manifest.

    Its config named the id and provider outright, so those are what the rates
    are looked up under. Without this the whole custom-model path bills zero.
    """

    def _priced_with(self, record_extra):
        from src.utils.tracking.core import calculate_cost_from_per_call_records

        record = {
            "model_name": "my-custom-alias",
            "served_model": "gpt-5.5",
            "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
            "billing_type": "byok",
            "timestamp": "2026-01-01T00:00:00",
            "run_id": str(uuid4()),
            "parent_run_id": None,
            **record_extra,
        }
        seen = {}

        def _capture(model_name, provider=None):
            seen["model_name"], seen["provider"] = model_name, provider
            return None

        with patch(f"{PRICING_MODULE}.find_model_pricing", _capture):
            calculate_cost_from_per_call_records([record])
        return seen

    def test_the_stamped_id_is_what_gets_priced(self):
        seen = self._priced_with(
            {"pricing_model_id": "gpt-5.5", "pricing_provider": "openai"}
        )
        assert seen == {"model_name": "gpt-5.5", "provider": "openai"}

    def test_without_a_stamp_the_display_name_is_all_there_is(self):
        """Pre-stamp records and consumer-supplied clients keep the old behavior."""
        seen = self._priced_with({})
        assert seen == {"model_name": "my-custom-alias", "provider": None}

    def test_a_shadowing_custom_model_bills_on_its_own_route(self):
        """A user may point a built-in's name at a different provider on purpose
        (see resolve_model_source: a custom entry outranks a built-in of the same
        name). Pricing that name off the manifest would charge the model it
        replaced, so the client's own stamp outranks the same-named entry.
        """
        from src.llms.llm import LLM

        key, cfg = next(iter(LLM.get_model_config().llm_config.items()))
        seen = self._priced_with(
            {
                "model_name": key,
                "pricing_model_id": "routed-elsewhere",
                "pricing_provider": "some-variant",
            }
        )
        assert seen == {"model_name": "routed-elsewhere", "provider": "some-variant"}
        assert cfg["model_id"] != "routed-elsewhere"  # the entry it shadows

    def test_a_built_in_prices_the_same_either_way(self):
        """The stamp only changes the shadowed case: for a model we built from the
        manifest, the stamped route is what a lookup by key would have produced.
        """
        from src.llms.pricing_utils import resolve_pricing_identity
        from src.llms.llm import LLM

        key, cfg = next(iter(LLM.get_model_config().llm_config.items()))
        provider, pricing_id = resolve_pricing_identity(key, billing_type="byok")
        seen = self._priced_with(
            {
                "model_name": key,
                "pricing_model_id": pricing_id,
                "pricing_provider": provider,
                "billing_type": "byok",
            }
        )
        assert seen == {"model_name": cfg["model_id"], "provider": cfg["provider"]}


class TestTheStampSurvivesTheWire:
    def test_a_real_invoke_carries_client_metadata_into_the_record(self, monkeypatch):
        """Producer (LLM.get_llm stamps client.metadata) and consumer (this
        tracker) are joined only by langchain's metadata merge. Nothing else in
        the suite proves that join exists, and the whole fix rests on it.
        """
        from langchain_core.language_models.fake_chat_models import (
            FakeMessagesListChatModel,
        )

        # A real invoke would otherwise reach for the tracing backend; the rest of
        # this file never runs the chain, so nothing else disables it. The key is
        # cleared as well as the flags because the tracer's background flush runs
        # at interpreter exit, long after this test's assertions have passed.
        for flag in ("LANGCHAIN_TRACING_V2", "LANGSMITH_TRACING", "LANGCHAIN_TRACING"):
            monkeypatch.setenv(flag, "false")
        for key in ("LANGCHAIN_API_KEY", "LANGSMITH_API_KEY"):
            monkeypatch.delenv(key, raising=False)

        msg = AIMessage(
            content="ok",
            response_metadata={"model_name": "glm-5.1glm-5.1"},
            usage_metadata={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
        )
        llm = FakeMessagesListChatModel(responses=[msg])
        llm.metadata = {
            "billing_type": "byok",
            "manifest_model": "glm-5.1-cn",
            "pricing_model_id": "glm-5.1",
            "pricing_provider": "z-ai",
        }

        tracker = PerCallTokenTracker()
        llm.invoke("hi", config={"callbacks": [tracker]})

        record = tracker.get_per_call_records()[0]
        assert record["model_name"] == "glm-5.1-cn"
        assert record["served_model"] == "glm-5.1glm-5.1"
        assert record["billing_type"] == "byok"
        assert record["pricing_model_id"] == "glm-5.1"


class TestTheReaderGetsASnapshot:
    def test_a_capture_does_not_grow_afterwards(self):
        """The finalize paths capture while background subagent writers are still
        appending. Handing out the live list is what they were changed for, so the
        copy needs an assertion holding it in place."""
        tracker = PerCallTokenTracker()
        tracker.on_llm_end(_result("glm-5.2"), run_id=uuid4())

        snapshot = tracker.get_per_call_records()
        tracker.on_llm_end(_result("glm-5.2"), run_id=uuid4())

        assert len(snapshot) == 1
        assert len(tracker.get_per_call_records()) == 2

    def test_the_attribution_drains_even_when_the_response_is_unusable(self):
        """Four exits in on_llm_end return before the append; a run that takes one
        still ends, so its entry must not outlive it."""
        tracker = PerCallTokenTracker()
        run_id = uuid4()
        tracker.on_chat_model_start(
            serialized={}, messages=[], run_id=run_id,
            metadata={"manifest_model": "glm-5.1-cn"},
        )

        tracker.on_llm_end(LLMResult(generations=[]), run_id=run_id)

        assert tracker._run_attribution == {}
        assert tracker.get_per_call_records() == []
