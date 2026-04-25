"""
Tests for user preferences validation — custom models and providers.

Covers:
- _validate_custom_models() — name format, collision with system models, provider validation
- _validate_custom_providers() — name format, parent_provider validation, builtin collision
- PUT /api/v1/users/me/preferences with model preferences
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _mock_model_config(system_models=None, byok_providers=None):
    """Create a mock ModelConfig for validation tests."""
    mc = MagicMock()
    system_models = system_models or {}
    byok_providers = byok_providers or ["openai", "anthropic"]

    mc.get_model_config.side_effect = lambda name: system_models.get(name)
    mc.get_byok_eligible_providers.return_value = byok_providers
    # flat_providers property is accessed by _validate_custom_models
    mc.flat_providers = {p: {} for p in byok_providers}
    # llm_config is used by _validate_custom_models to block names that
    # collide with built-in models.
    mc.llm_config = system_models
    return mc


# ---------------------------------------------------------------------------
# _validate_custom_models (unit tests via direct import)
# ---------------------------------------------------------------------------


class TestValidateCustomModels:
    def _validate(self, models, providers=None, mc=None):
        from src.server.app.users import _validate_custom_models

        if mc is None:
            mc = _mock_model_config()
        with patch("src.llms.llm.ModelConfig", return_value=mc):
            _validate_custom_models(models, providers)

    def test_valid_model(self):
        """Valid custom model should pass."""
        mc = _mock_model_config()
        self._validate(
            [{"name": "my-gpt4", "model_id": "gpt-4o", "provider": "openai"}],
            mc=mc,
        )

    def test_missing_name_raises(self):
        with pytest.raises(HTTPException):
            self._validate([{"model_id": "gpt-4o", "provider": "openai"}])

    def test_missing_model_id_raises(self):
        with pytest.raises(HTTPException):
            self._validate([{"name": "my-model", "provider": "openai"}])

    def test_missing_provider_raises(self):
        with pytest.raises(HTTPException):
            self._validate([{"name": "my-model", "model_id": "gpt-4o"}])

    def test_invalid_name_format_raises(self):
        """Name must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$"""
        with pytest.raises(HTTPException):
            self._validate([{"name": "-invalid", "model_id": "gpt-4o", "provider": "openai"}])

    def test_system_model_name_shadow_allowed(self):
        """Custom model name may collide with a built-in — the resolver
        checks custom first, so the user's entry shadows the built-in. This
        is the normal path for routing built-in model names (e.g.
        ``glm-5.1``) through a user's variant-specific key."""
        mc = _mock_model_config(system_models={"gpt-4o": {"model_id": "gpt-4o"}})
        # Should not raise.
        self._validate(
            [{"name": "gpt-4o", "model_id": "gpt-4o", "provider": "openai"}],
            mc=mc,
        )

    def test_system_model_id_reuse_allowed(self):
        """``model_id`` can match a built-in (the upstream endpoint interprets
        it) — only the user-facing ``name`` is reserved."""
        mc = _mock_model_config(system_models={"gpt-4o": {"model_id": "gpt-4o"}})
        self._validate(
            [{"name": "gpt-4o-custom", "model_id": "gpt-4o", "provider": "openai"}],
            mc=mc,
        )

    def test_duplicate_names_raises(self):
        """Duplicate custom model names should be rejected."""
        with pytest.raises(HTTPException, match="duplicate name"):
            self._validate([
                {"name": "my-model", "model_id": "gpt-4o", "provider": "openai"},
                {"name": "my-model", "model_id": "gpt-4", "provider": "openai"},
            ])

    def test_provider_from_custom_providers(self):
        """Provider can be a custom sub-provider name."""
        self._validate(
            [{"name": "my-gpt4", "model_id": "gpt-4o", "provider": "my-openai"}],
            providers=[{"name": "my-openai", "parent_provider": "openai"}],
        )

    def test_unknown_provider_raises(self):
        """Provider must be BYOK-eligible or in custom_providers."""
        with pytest.raises(HTTPException, match="not a known BYOK-eligible"):
            self._validate([
                {"name": "my-model", "model_id": "gpt-4o", "provider": "unknown"},
            ])

    # --- input_modalities validation ---

    def test_valid_input_modalities(self):
        """Valid input_modalities should pass."""
        self._validate([
            {"name": "my-llava", "model_id": "llava", "provider": "openai", "input_modalities": ["text", "image"]},
        ])

    def test_invalid_modality_value_raises(self):
        """Modality values must be in {text, image, pdf}."""
        with pytest.raises(HTTPException, match="invalid modality"):
            self._validate([
                {"name": "my-model", "model_id": "gpt-4o", "provider": "openai", "input_modalities": ["text", "video"]},
            ])

    def test_non_list_modalities_raises(self):
        """input_modalities must be a list."""
        with pytest.raises(HTTPException, match="non-empty list"):
            self._validate([
                {"name": "my-model", "model_id": "gpt-4o", "provider": "openai", "input_modalities": "image"},
            ])

    def test_empty_modalities_raises(self):
        """Empty input_modalities list is invalid."""
        with pytest.raises(HTTPException, match="non-empty list"):
            self._validate([
                {"name": "my-model", "model_id": "gpt-4o", "provider": "openai", "input_modalities": []},
            ])

    def test_omitted_modalities_passes(self):
        """Model without input_modalities should pass (backward compat)."""
        self._validate([
            {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai"},
        ])

    def test_text_auto_prepended(self):
        """If input_modalities is provided without 'text', it should be auto-added."""
        models = [{"name": "my-llava", "model_id": "llava", "provider": "openai", "input_modalities": ["image"]}]
        self._validate(models)
        # After validation, "text" should be prepended
        assert models[0]["input_modalities"] == ["text", "image"]


# ---------------------------------------------------------------------------
# _validate_custom_providers (unit tests via direct import)
# ---------------------------------------------------------------------------


class TestValidateCustomProviders:
    def _validate(self, providers, mc=None):
        from src.server.app.users import _validate_custom_providers

        if mc is None:
            mc = _mock_model_config()
        with patch("src.llms.llm.ModelConfig", return_value=mc):
            _validate_custom_providers(providers)

    def test_valid_provider(self):
        self._validate([{"name": "my-openai", "parent_provider": "openai"}])

    def test_missing_name_raises(self):
        with pytest.raises(HTTPException, match="name is required"):
            self._validate([{"parent_provider": "openai"}])

    def test_missing_parent_provider_raises(self):
        with pytest.raises(HTTPException, match="parent_provider is required"):
            self._validate([{"name": "my-provider"}])

    def test_invalid_parent_provider_raises(self):
        """parent_provider must be a BYOK-eligible builtin."""
        with pytest.raises(HTTPException, match="not a BYOK-eligible"):
            self._validate([{"name": "my-fake", "parent_provider": "not-a-real-provider"}])

    def test_builtin_collision_raises(self):
        """Custom provider name must not collide with builtin."""
        with pytest.raises(HTTPException, match="conflicts with built-in"):
            self._validate([{"name": "openai", "parent_provider": "openai"}])

    def test_duplicate_names_raises(self):
        with pytest.raises(HTTPException, match="duplicate name"):
            self._validate([
                {"name": "my-openai", "parent_provider": "openai"},
                {"name": "my-openai", "parent_provider": "openai"},
            ])

    def test_use_response_api_must_be_bool(self):
        with pytest.raises(HTTPException, match="use_response_api must be a boolean"):
            self._validate([
                {"name": "my-openai", "parent_provider": "openai", "use_response_api": "yes"},
            ])


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/preferences — end-to-end model preferences
# ---------------------------------------------------------------------------

