"""Tests for GLM SDK dispatch: flatten setdefault semantics, ChatZai construction,
manifest parameter plumb-through, and profile overrides."""

import copy

from src.llms.llm import LLM, ModelConfig, _profile_overrides_from_config
from src.llms.vendor.langchain_zai import ChatZai


def _build_glm_client(model: str = "glm-5.2") -> ChatZai:
    """Construct the GLM client from the real manifest with a fake key (no network)."""
    return LLM(model, api_key="test-key").get_llm()


class TestFlattenExplicitParentProvider:
    """_flatten_providers must respect an explicit parent_provider on a variant."""

    def test_explicit_parent_provider_is_preserved(self):
        """A variant declaring its own parent_provider keeps it (setdefault, not overwrite)."""
        grouped = {
            "brand": {
                "sdk": "glm",
                "env_key": "BRAND_API_KEY",
                "variants": {
                    "brand-cn": {
                        "base_url": "https://cn.example.com/api",
                    },
                    "brand-cn-coding": {
                        "sdk": "anthropic",
                        "base_url": "https://cn.example.com/anthropic",
                        "parent_provider": "brand-cn",
                    },
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        # Explicit declaration wins over the group key
        assert result["brand-cn-coding"]["parent_provider"] == "brand-cn"
        # Variant without a declaration falls back to the group key
        assert result["brand-cn"]["parent_provider"] == "brand"


class TestGlmSdkDispatch:
    """get_llm() with sdk="glm" must build the vendored ChatZai client."""

    def test_glm_model_builds_chatzai(self):
        """glm-5.2 (provider z-ai, sdk glm) dispatches to ChatZai."""
        client = _build_glm_client()
        assert isinstance(client, ChatZai)
        assert client.model_name == "glm-5.2"


class TestGlmParameterPlumbThrough:
    """Manifest parameters and extra_body must reach the constructed client."""

    def test_manifest_max_tokens_reaches_client(self):
        """parameters.max_tokens from models.json lands on the client."""
        model_info = LLM.get_model_config().get_model_config("glm-5.2")
        client = _build_glm_client()
        assert client.max_tokens == model_info["parameters"]["max_tokens"]

    def test_manifest_extra_body_reaches_client(self):
        """extra_body carries thinking.clear_thinking=false for reasoning readback."""
        client = _build_glm_client()
        assert client.extra_body["thinking"]["clear_thinking"] is False
        assert client.extra_body["thinking"]["type"] == "enabled"


class TestGlmProfileOverlay:
    """The manifest overrides the vendored package profile so the two can't drift."""

    def test_profile_matches_manifest_limits(self):
        """profile max_input/max_output tokens come from manifest context/parameters."""
        model_info = LLM.get_model_config().get_model_config("glm-5.2")
        client = _build_glm_client()
        assert client.profile["max_input_tokens"] == model_info["context"]
        assert client.profile["max_output_tokens"] == model_info["parameters"]["max_tokens"]


class TestProfileOverridesFromConfig:
    """_profile_overrides_from_config maps models.json fields onto ModelProfile keys."""

    def test_maps_context_parameters_and_modalities(self):
        """context → max_input_tokens, parameters.max_tokens → max_output_tokens, modality flags."""
        model_info = {
            "context": 200000,
            "parameters": {"max_tokens": 64000},
            "input_modalities": ["text", "image"],
        }

        overrides = _profile_overrides_from_config(model_info)

        assert overrides["max_input_tokens"] == 200000
        assert overrides["max_output_tokens"] == 64000
        assert overrides["text_inputs"] is True
        assert overrides["image_inputs"] is True
        assert overrides["audio_inputs"] is False
        assert overrides["video_inputs"] is False

    def test_missing_fields_produce_no_overrides(self):
        """An entry with none of the mapped fields yields an empty override dict."""
        assert _profile_overrides_from_config({}) == {}


class TestManifestIsolation:
    """LLM instances must never mutate the process-wide manifest singleton."""

    def test_reasoning_effort_does_not_contaminate_manifest(self):
        """A reasoning_effort override mutates the instance copy, not the manifest."""
        manifest_entry = LLM.get_model_config().get_model_config("glm-5.2")
        before = copy.deepcopy(manifest_entry.get("extra_body"))

        LLM("glm-5.2", api_key="test-key", reasoning_effort="low")

        assert manifest_entry.get("extra_body") == before
