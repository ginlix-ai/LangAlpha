"""Tests for parent_provider pricing fallback in find_model_pricing()."""

from unittest.mock import patch, MagicMock


def _build_manifest(models: dict) -> dict:
    """Build a minimal manifest matching providers.json structure."""
    return {"provider_config": {}, "models": models}


def _build_model_config(manifest: dict, flat_providers: dict):
    """Create a MagicMock ModelConfig with controlled manifest and flat_providers."""
    mc = MagicMock()
    mc.manifest = manifest
    mc._flat_providers = flat_providers

    def _get_provider_info(provider: str) -> dict:
        return flat_providers.get(provider, {})

    mc.get_provider_info = _get_provider_info

    def _get_parent_provider(provider: str) -> str:
        info = flat_providers.get(provider, {})
        return info.get("parent_provider", provider)

    mc.get_parent_provider = _get_parent_provider
    return mc


class TestPricingFallback:
    """Tests for parent_provider pricing fallback behaviour."""

    def test_variant_with_own_pricing_uses_own(self):
        """Provider 'z-ai-cn' has its own model pricing -- should use it, not parent."""
        manifest = _build_manifest(
            models={
                "z-ai-cn": [
                    {
                        "id": "z-model-1",
                        "pricing": {"input": 1.50, "output": 3.00},
                    },
                ],
                "z-ai": [
                    {
                        "id": "z-model-1",
                        "pricing": {"input": 2.00, "output": 5.00},
                    },
                ],
            },
        )
        flat_providers = {
            "z-ai": {"sdk": "openai", "display_name": "Z-AI"},
            "z-ai-cn": {"sdk": "openai", "parent_provider": "z-ai"},
        }
        mc = _build_model_config(manifest, flat_providers)

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            from src.llms.pricing_utils import find_model_pricing

            result = find_model_pricing("z-model-1", provider="z-ai-cn")

        assert result is not None
        assert result["input"] == 1.50
        assert result["output"] == 3.00

    def test_variant_without_pricing_falls_to_parent(self):
        """'dashscope-coding' has NO models listed; should fall back to parent 'dashscope'."""
        manifest = _build_manifest(
            models={
                # No "dashscope-coding" key at all
                "dashscope": [
                    {
                        "id": "qwen-coder-plus",
                        "pricing": {"input": 0.80, "output": 1.60},
                    },
                ],
            },
        )
        flat_providers = {
            "dashscope": {"sdk": "openai", "display_name": "Dashscope"},
            "dashscope-coding": {"sdk": "openai", "parent_provider": "dashscope"},
        }
        mc = _build_model_config(manifest, flat_providers)

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            from src.llms.pricing_utils import find_model_pricing

            result = find_model_pricing("qwen-coder-plus", provider="dashscope-coding")

        assert result is not None
        assert result["input"] == 0.80

    def test_parent_pricing_returns_correct_rates(self):
        """Verify actual pricing values when falling back to parent provider."""
        manifest = _build_manifest(
            models={
                "dashscope": [
                    {
                        "id": "qwen-turbo",
                        "pricing": {
                            "input": 0.30,
                            "output": 0.60,
                            "pricing_type": "token",
                        },
                    },
                ],
            },
        )
        flat_providers = {
            "dashscope": {"sdk": "openai", "display_name": "Dashscope"},
            "dashscope-coding": {"sdk": "openai", "parent_provider": "dashscope"},
        }
        mc = _build_model_config(manifest, flat_providers)

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            from src.llms.pricing_utils import find_model_pricing

            result = find_model_pricing("qwen-turbo", provider="dashscope-coding")

        assert result is not None
        assert result["input"] == 0.30
        assert result["output"] == 0.60
        assert result["pricing_type"] == "token"

    def test_no_parent_no_pricing_falls_to_global(self):
        """Provider with no models and no real parent searches globally."""
        manifest = _build_manifest(
            models={
                # "unknown-provider" has no entry here
                "openai": [
                    {
                        "id": "gpt-4o",
                        "pricing": {"input": 2.50, "output": 10.00},
                    },
                ],
            },
        )
        # get_parent_provider returns itself -- no real parent
        flat_providers = {
            "unknown-provider": {"sdk": "openai"},
            "openai": {"sdk": "openai"},
        }
        mc = _build_model_config(manifest, flat_providers)

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            from src.llms.pricing_utils import find_model_pricing

            # Should fall through to global search and find it under openai
            result = find_model_pricing("gpt-4o", provider="unknown-provider")
            assert result is not None
            assert result["input"] == 2.50

            # Model that doesn't exist anywhere should return None
            result_none = find_model_pricing(
                "nonexistent-model", provider="unknown-provider"
            )
            assert result_none is None

    def test_subscription_pricing_type_skips_cost_calc(self):
        """Models with pricing_type='subscription' contribute zero cost."""
        subscription_pricing = {
            "pricing_type": "subscription",
            "input": 0,
            "output": 0,
        }

        token_usage = {
            "sub-model": {
                "input_tokens": 5000,
                "output_tokens": 2000,
            },
        }

        with (
            patch(
                "src.llms.pricing_utils.find_model_pricing",
                return_value=subscription_pricing,
            ),
            patch(
                "src.llms.pricing_utils.detect_provider_for_model",
                return_value="some-provider",
            ),
        ):
            from src.utils.tracking.core import add_cost_to_token_usage

            result = add_cost_to_token_usage(token_usage)

        assert result["total_cost"] == 0.0
