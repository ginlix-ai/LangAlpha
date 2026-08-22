"""OrcaRouter provider contract — loads the REAL providers.json/models.json.

Mirrors test_manifest_integrity.py: no mocking, exercises the actual manifest
files on disk so a renamed or removed provider entry fails loudly.
"""

from src.llms.llm import LLM, ModelConfig
from src.llms.pricing_utils import find_model_pricing

ORCA_MODELS = [
    "orcarouter/auto",
    "orcarouter/fusion",
    "orcarouter/fusion-flash",
    "orcarouter/fusion-mini",
]


class TestOrcaRouterProvider:
    def test_provider_defined_in_providers_json(self):
        info = ModelConfig().get_provider_info("orcarouter")
        assert info["sdk"] == "openai"
        assert info["base_url"] == "https://api.orcarouter.ai/v1"
        assert info["env_key"] == "ORCAROUTER_API_KEY"
        assert info["access_type"] == "api_key"
        assert info["byok_eligible"] is True
        assert info["display_name"] == "OrcaRouter"

    def test_provider_is_byok_eligible(self):
        eligible = ModelConfig().get_byok_eligible_providers()
        assert "orcarouter" in eligible

    def test_every_orca_model_resolves_to_provider(self):
        mc = ModelConfig()
        for model in ORCA_MODELS:
            cfg = mc.get_model_config(model)
            assert cfg is not None, model
            assert cfg["provider"] == "orcarouter"
            assert cfg.get("visible") is True
            assert "text" in cfg.get("input_modalities", [])

    def test_orca_models_listed_in_configured_models(self):
        from src.llms.llm import get_configured_llm_models

        grouped = get_configured_llm_models()
        assert sorted(grouped.get("orcarouter", [])) == sorted(ORCA_MODELS)

    def test_each_orca_model_has_pricing(self):
        for model in ORCA_MODELS:
            pricing = find_model_pricing(model, provider="orcarouter")
            assert pricing is not None, f"{model} missing pricing"
            assert pricing["input"] > 0
            assert pricing["output"] > 0

    def test_llm_factory_builds_openai_client_for_orca(self):
        from unittest.mock import patch

        mc = ModelConfig()
        with patch.object(LLM, "get_model_config", return_value=mc):
            llm = LLM("orcarouter/auto")
            assert llm.provider == "orcarouter"
            assert llm.sdk == "openai"
            assert llm.base_url == "https://api.orcarouter.ai/v1"
            assert llm.model == "orcarouter/auto"
