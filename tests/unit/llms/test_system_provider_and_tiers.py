"""Tests for system_provider routing, model tiers, and platform provider filtering.

Covers:
- LLM.__init__ system_provider fork (BYOK vs system serving)
- ModelConfig.get_model_metadata() tier field
- ModelConfig._flatten_providers() platform variant SDK skip
- api_keys.py filtering of platform variants from user-facing surfaces
- Manifest validation: models.json entries reference valid providers, have model_id
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from src.llms.llm import ModelConfig, LLM

# ---------------------------------------------------------------------------
# Manifest data (loaded once)
# ---------------------------------------------------------------------------

_MANIFEST_DIR = Path(__file__).resolve().parents[3] / "src" / "llms" / "manifest"

with open(_MANIFEST_DIR / "models.json", encoding='utf-8') as _f:
    _MODELS = json.load(_f)

with open(_MANIFEST_DIR / "providers.json", encoding='utf-8') as _f:
    _PROVIDERS_RAW = json.load(_f)

# Build set of all valid provider names (parent + variants)
_ALL_PROVIDER_NAMES: set[str] = set()
for _name, _cfg in _PROVIDERS_RAW.get("provider_config", {}).items():
    _ALL_PROVIDER_NAMES.add(_name)
    for _vname in _cfg.get("variants", {}):
        _ALL_PROVIDER_NAMES.add(_vname)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_model_config_with(models: dict, providers: dict) -> ModelConfig:
    """Build a ModelConfig with injected models/providers (no disk I/O)."""
    mc = ModelConfig.__new__(ModelConfig)
    mc.llm_config = models
    mc.manifest = {"models": {}, "provider_config": providers}
    mc._flat_providers = ModelConfig._flatten_providers(providers)
    return mc


# ---------------------------------------------------------------------------
# system_provider routing in LLM.__init__
# ---------------------------------------------------------------------------


class TestSystemProviderRouting:
    """LLM.__init__ should fork provider when no BYOK key + system_provider set."""

    def _build_llm_instance(self, api_key=None):
        """Create an LLM instance with a platform system_provider."""
        models = {
            "test-model": {
                "model_id": "test-model",
                "provider": "acme",
                "system_provider": "acme-platform",
                "tier": 2,
                "parameters": {},
            }
        }
        providers = {
            "acme": {
                "sdk": "anthropic",
                "env_key": "ACME_API_KEY",
                "access_type": "api_key",
                "display_name": "Acme",
                "variants": {
                    "acme-platform": {
                        "base_url": "http://proxy:8893",
                        "env_key": "PROXY_API_KEY",
                        "platform": True,
                    }
                },
            }
        }
        mc = _make_model_config_with(models, providers)
        with patch.object(LLM, "get_model_config", return_value=mc):
            return LLM("test-model", api_key=api_key)

    def test_system_provider_used_when_no_api_key(self):
        """No BYOK key -> provider should be system_provider."""
        instance = self._build_llm_instance(api_key=None)
        assert instance.provider == "acme-platform"

    def test_byok_key_bypasses_system_provider(self):
        """BYOK key provided -> provider stays as the model's original provider."""
        instance = self._build_llm_instance(api_key="sk-user-key-123")
        assert instance.provider == "acme"

    def test_missing_system_provider_uses_provider(self):
        """Model without system_provider -> provider unchanged regardless of api_key."""
        models = {
            "test-basic": {
                "model_id": "test-basic",
                "provider": "basic",
                "parameters": {},
            }
        }
        providers = {
            "basic": {
                "sdk": "openai",
                "env_key": "BASIC_API_KEY",
                "display_name": "Basic",
            }
        }
        mc = _make_model_config_with(models, providers)
        with patch.object(LLM, "get_model_config", return_value=mc):
            instance = LLM("test-basic", api_key=None)
        assert instance.provider == "basic"


# ---------------------------------------------------------------------------
# Tier in model metadata
# ---------------------------------------------------------------------------


class TestModelTiers:
    """get_model_metadata() should include tier field."""

    def _build_config(self):
        models = {
            "model-with-tier": {
                "model_id": "m1",
                "provider": "openai",
                "visible": True,
                "tier": 2,
            },
            "model-without-tier": {
                "model_id": "m2",
                "provider": "openai",
                "visible": True,
            },
            "hidden-model": {
                "model_id": "m3",
                "provider": "openai",
                "tier": 1,
                # not visible
            },
        }
        providers = {
            "openai": {
                "sdk": "openai",
                "env_key": "OPENAI_API_KEY",
                "access_type": "api_key",
                "display_name": "OpenAI",
            }
        }
        return _make_model_config_with(models, providers)

    def test_tier_exposed_in_model_metadata(self):
        mc = self._build_config()
        metadata = mc.get_model_metadata()
        assert metadata["model-with-tier"]["tier"] == 2

    def test_tier_absent_when_not_set(self):
        mc = self._build_config()
        metadata = mc.get_model_metadata()
        assert "tier" not in metadata["model-without-tier"]

    def test_hidden_models_excluded_from_metadata(self):
        mc = self._build_config()
        metadata = mc.get_model_metadata()
        assert "hidden-model" not in metadata


# ---------------------------------------------------------------------------
# _flatten_providers: platform variant SDK skip
# ---------------------------------------------------------------------------


class TestFlattenPlatformVariant:
    """Platform variants should be exempt from post-flatten SDK validation."""

    def test_flatten_providers_skips_sdk_validation_for_platform(self):
        """A platform variant without its own sdk should NOT raise ValueError."""
        grouped = {
            "acme": {
                "sdk": "anthropic",
                "env_key": "ACME_API_KEY",
                "display_name": "Acme",
                "variants": {
                    "acme-platform": {
                        "base_url": "http://proxy:8893",
                        "env_key": "PROXY_KEY",
                        "platform": True,
                        # No sdk — inherits from parent via merge, but even
                        # if it didn't, the platform flag should skip validation
                    }
                },
            }
        }
        # Should not raise
        flat = ModelConfig._flatten_providers(grouped)
        assert "acme-platform" in flat
        assert flat["acme-platform"]["platform"] is True

    def test_flatten_providers_still_validates_non_platform(self):
        """Non-platform variants without sdk should still raise."""
        grouped = {
            "broken": {
                "env_key": "X",
                "display_name": "Broken",
                "variants": {
                    "broken-variant": {
                        "base_url": "http://example.com",
                        # no sdk, not platform
                    }
                },
            }
        }
        with pytest.raises(ValueError, match="sdk"):
            ModelConfig._flatten_providers(grouped)


# ---------------------------------------------------------------------------
# api_keys.py: platform variant filtering
# ---------------------------------------------------------------------------


class TestPlatformVariantFiltering:
    """Platform variants must not appear in user-facing API surfaces."""

    def _build_flat_providers(self):
        """Return flat providers dict with a platform variant."""
        return {
            "acme": {
                "sdk": "anthropic",
                "env_key": "ACME_API_KEY",
                "access_type": "api_key",
                "byok_eligible": True,
                "display_name": "Acme",
            },
            "acme-platform": {
                "sdk": "anthropic",
                "base_url": "http://proxy:8893",
                "env_key": "PROXY_API_KEY",
                "access_type": "api_key",
                "platform": True,
                "parent_provider": "acme",
                "display_name": "Acme",
            },
            "beta": {
                "sdk": "openai",
                "env_key": "BETA_API_KEY",
                "access_type": "api_key",
                "byok_eligible": True,
                "display_name": "Beta",
            },
            "beta-oauth": {
                "sdk": "openai",
                "env_key": None,
                "access_type": "oauth",
                "parent_provider": "beta",
                "display_name": "Beta (OAuth)",
            },
        }

    def test_platform_variant_excluded_from_byok_eligible(self):
        """get_byok_eligible_providers() should not include platform variants."""
        providers = {
            "acme": {
                "sdk": "anthropic",
                "env_key": "ACME_API_KEY",
                "access_type": "api_key",
                "byok_eligible": True,
                "display_name": "Acme",
                "variants": {
                    "acme-platform": {
                        "base_url": "http://proxy:8893",
                        "env_key": "PROXY_KEY",
                        "platform": True,
                        "byok_eligible": True,
                    }
                },
            },
            "beta": {
                "sdk": "openai",
                "env_key": "BETA_API_KEY",
                "access_type": "api_key",
                "byok_eligible": True,
                "display_name": "Beta",
            },
        }
        mc = _make_model_config_with({}, providers)
        eligible = mc.get_byok_eligible_providers()
        assert "acme" in eligible
        assert "beta" in eligible
        assert "acme-platform" not in eligible

    def test_platform_variant_excluded_from_provider_info_map(self):
        """_get_provider_info_map() should not include platform variants."""
        import src.server.app.api_keys as api_keys_mod
        from src.server.app.api_keys import _get_provider_info_map

        flat = self._build_flat_providers()
        # Add byok_eligible to platform variant so it would appear
        # in _get_supported_providers if not filtered
        flat["acme-platform"]["byok_eligible"] = True

        mock_config = MagicMock()
        mock_config.flat_providers = flat
        mock_config.get_provider_info = lambda p: flat.get(p, {})
        mock_config.get_byok_eligible_providers = lambda: [
            name for name, cfg in flat.items()
            if cfg.get("byok_eligible", False) and not cfg.get("platform", False)
        ]

        # Reset module-level cache so our mock takes effect
        old_cache = api_keys_mod._BYOK_PROVIDERS_CACHE
        api_keys_mod._BYOK_PROVIDERS_CACHE = None

        with patch("src.llms.llm.ModelConfig", return_value=mock_config):
            info_map = _get_provider_info_map()

        # Restore cache
        api_keys_mod._BYOK_PROVIDERS_CACHE = old_cache

        assert "acme-platform" not in info_map
        assert "acme" in info_map
        assert "beta" in info_map

    def test_platform_variant_filtered_from_provider_catalog(self):
        """_build_provider_catalog should not include platform variants."""
        from src.server.app.api_keys import _build_provider_catalog

        flat = self._build_flat_providers()
        mock_config = MagicMock()
        mock_config.flat_providers = flat
        mock_config.get_display_name = lambda k: flat[k].get("display_name", k.title())

        # Clear the lru_cache so our mock takes effect
        _build_provider_catalog.cache_clear()

        with patch("src.llms.llm.ModelConfig", return_value=mock_config):
            catalog = _build_provider_catalog()

        provider_names = {entry["provider"] for entry in catalog}
        assert "acme-platform" not in provider_names
        assert "acme" in provider_names
        assert "beta" in provider_names

        # Clean up
        _build_provider_catalog.cache_clear()

    def test_platform_variant_excluded_from_allowed_providers(self):
        """_get_allowed_providers should not include platform variants."""
        from src.server.app.api_keys import _get_allowed_providers

        flat = self._build_flat_providers()
        mock_config = MagicMock()
        mock_config.flat_providers = flat

        with patch("src.llms.llm.ModelConfig", return_value=mock_config):
            allowed = _get_allowed_providers([])

        assert "acme-platform" not in allowed
        assert "acme" in allowed
        assert "beta" in allowed
        # OAuth should also be excluded
        assert "beta-oauth" not in allowed


# ---------------------------------------------------------------------------
# Manifest validation: models.json integrity
# ---------------------------------------------------------------------------


_EMBEDDING_KEYS = {"embedding-small", "embedding-large", "embedding-cn"}
_CHAT_MODELS = {k: v for k, v in _MODELS.items() if k not in _EMBEDDING_KEYS}


class TestModelsManifestIntegrity:
    """Every model in models.json must have a valid provider and model_id.

    Non-embedding models must also declare input_modalities.
    """

    @pytest.mark.parametrize("model_key", list(_MODELS.keys()))
    def test_model_has_model_id(self, model_key):
        entry = _MODELS[model_key]
        assert "model_id" in entry and entry["model_id"], (
            f"Model '{model_key}' is missing a model_id"
        )

    @pytest.mark.parametrize("model_key", list(_MODELS.keys()))
    def test_model_provider_exists_in_providers_json(self, model_key):
        entry = _MODELS[model_key]
        provider = entry.get("provider")
        assert provider, f"Model '{model_key}' is missing a provider field"
        assert provider in _ALL_PROVIDER_NAMES, (
            f"Model '{model_key}' references provider '{provider}' "
            f"which is not defined in providers.json"
        )

    @pytest.mark.parametrize("model_key", list(_CHAT_MODELS.keys()))
    def test_chat_model_has_input_modalities(self, model_key):
        entry = _CHAT_MODELS[model_key]
        modalities = entry.get("input_modalities")
        assert modalities and isinstance(modalities, list), (
            f"Chat model '{model_key}' is missing input_modalities"
        )
