"""Tests for upstream library monkey-patches."""

import pytest
from unittest.mock import MagicMock
from pydantic import BaseModel

# Apply the patch before importing _create_usage_metadata
from src.llms.patches import _patch_langchain_anthropic_usage_metadata
_patch_langchain_anthropic_usage_metadata()


class MockCacheCreation(BaseModel):
    ephemeral_5m_input_tokens: int = 0
    ephemeral_1h_input_tokens: int = 0


class MockUsage(BaseModel):
    input_tokens: int = 100
    output_tokens: int = 50
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_creation: MockCacheCreation = None

    model_config = {"arbitrary_types_allowed": True}


class TestPatchLangchainAnthropicUsageMetadata:
    """Test the _create_usage_metadata monkey-patch."""

    def test_none_cache_creation_fields_dont_crash(self):
        """Reproduces the original TypeError: int += None."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        # Simulate what third-party providers return: cache_creation with None values
        usage = MockUsage(
            cache_creation=MockCacheCreation(ephemeral_5m_input_tokens=0, ephemeral_1h_input_tokens=0)
        )
        # Force None values like the Anthropic SDK's lenient parsing does
        object.__setattr__(usage.cache_creation, "ephemeral_5m_input_tokens", None)
        object.__setattr__(usage.cache_creation, "ephemeral_1h_input_tokens", None)

        # This would raise TypeError without the patch
        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_normal_cache_creation_still_works(self):
        """Normal cache_creation values should pass through correctly."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(
            input_tokens=100,
            output_tokens=50,
            cache_creation=MockCacheCreation(ephemeral_5m_input_tokens=10, ephemeral_1h_input_tokens=20),
        )

        result = _create_usage_metadata(usage)
        assert result["output_tokens"] == 50
        input_details = result.get("input_token_details", {})
        assert input_details.get("ephemeral_5m_input_tokens") == 10
        assert input_details.get("ephemeral_1h_input_tokens") == 20

    def test_dict_cache_creation_with_none_fields(self):
        """cache_creation as a plain dict with None values should not crash."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)
        # Simulate cache_creation arriving as a plain dict (some providers do this)
        object.__setattr__(
            usage,
            "cache_creation",
            {"ephemeral_5m_input_tokens": None, "ephemeral_1h_input_tokens": None},
        )

        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_dict_cache_creation_with_valid_values(self):
        """cache_creation as a plain dict with valid int values should work."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)
        object.__setattr__(
            usage,
            "cache_creation",
            {"ephemeral_5m_input_tokens": 10, "ephemeral_1h_input_tokens": 20},
        )

        result = _create_usage_metadata(usage)
        assert result["output_tokens"] == 50
        input_details = result.get("input_token_details", {})
        assert input_details.get("ephemeral_5m_input_tokens") == 10
        assert input_details.get("ephemeral_1h_input_tokens") == 20

    def test_no_cache_creation_still_works(self):
        """Usage without cache_creation should work normally."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)

        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_patch_is_applied(self):
        """Verify the patch was applied to langchain_anthropic."""
        import langchain_anthropic.chat_models as chat_models

        # The patched function should be our wrapper, not the original
        assert "patched" in chat_models._create_usage_metadata.__name__
