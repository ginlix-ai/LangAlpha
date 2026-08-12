"""Pricing resolution for the new model entries via the canonical
find_model_pricing() path used by cost tracking.

Variant providers (OAuth) carry no pricing of their own and inherit the
parent provider's entry; region variants keep their own rates.
"""

from src.llms.pricing_utils import find_model_pricing


class TestModelPricingResolution:
    def test_anthropic_direct_pricing(self):
        pricing = find_model_pricing("claude-opus-4-8", provider="anthropic")
        assert pricing is not None
        assert pricing["input"] == 5.0
        assert pricing["output"] == 25.0

    def test_oauth_variant_inherits_parent_pricing(self):
        """claude-oauth has no pricing list; inherits anthropic for the same id."""
        pricing = find_model_pricing("claude-opus-4-8", provider="claude-oauth")
        assert pricing is not None
        assert pricing["input"] == 5.0
        assert pricing["output"] == 25.0

    def test_qwen_cn_pricing(self):
        pricing = find_model_pricing("qwen3.7-max", provider="dashscope")
        assert pricing is not None
        assert pricing["input"] == 1.714

    def test_intl_variant_keeps_own_pricing_not_parent(self):
        """dashscope-intl has its own pricing list, so it must NOT inherit the
        cheaper CN rates from the parent dashscope provider."""
        cn = find_model_pricing("qwen3.7-max", provider="dashscope")
        intl = find_model_pricing("qwen3.7-max", provider="dashscope-intl")
        assert cn["input"] == 1.714
        assert intl["input"] == 2.677
        assert cn["input"] != intl["input"]

    def test_sonnet_5_resolves_and_oauth_inherits(self):
        direct = find_model_pricing("claude-sonnet-5", provider="anthropic")
        oauth = find_model_pricing("claude-sonnet-5", provider="claude-oauth")
        assert direct is not None
        assert direct["input"] > 0 and direct["output"] > 0
        assert oauth == direct

    def test_gpt_5_6_tiered_resolves_and_codex_oauth_inherits(self):
        """The GPT-5.6 series uses short/long-context tiered pricing on `openai`;
        the codex-oauth twins carry no pricing list and inherit the parent."""
        for model_id in ("gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"):
            direct = find_model_pricing(model_id, provider="openai")
            oauth = find_model_pricing(model_id, provider="codex-oauth")
            assert direct is not None, model_id
            # Structural: tiered input + input-dependent output (values not pinned).
            assert direct["input_tiers"][0]["rate"] > 0
            assert direct["input_tiers"][0]["cached_input"] > 0
            assert direct["output_pricing_mode"] == "input_dependent"
            assert direct["output_tiers"][-1]["rate"] > direct["output_tiers"][0]["rate"]
            assert oauth == direct

    def test_gpt_5_6_threshold_billing_structure(self):
        """OpenAI bills GPT-5.6 long context threshold-style ("2x input and
        1.5x output for the full request" past 272K), so the entries must opt
        into threshold input mode and carry per-tier cache-write rates
        (values intentionally not pinned)."""
        for model_id in ("gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"):
            pricing = find_model_pricing(model_id, provider="openai")
            assert pricing["input_pricing_mode"] == "input_dependent", model_id
            for tier in pricing["input_tiers"]:
                assert "cache_5m" in tier, model_id
                assert "cached_input" in tier, model_id

    def test_unknown_model_returns_none(self):
        assert find_model_pricing("does-not-exist", provider="anthropic") is None
