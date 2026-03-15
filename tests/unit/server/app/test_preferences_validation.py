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

    def test_system_model_collision_raises(self):
        """Custom model name cannot collide with system model."""
        mc = _mock_model_config(system_models={"gpt-4o": {"model_id": "gpt-4o"}})
        with pytest.raises(HTTPException, match="conflicts with a system model"):
            self._validate(
                [{"name": "gpt-4o", "model_id": "gpt-4o", "provider": "openai"}],
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
            self._validate([{"name": "my-deepseek", "parent_provider": "deepseek"}])

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

