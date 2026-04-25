import pytest

from src.llms.llm import ModelConfig, get_input_modalities


class TestGetInputModalities:
    """Tests for ModelConfig.get_input_modalities() and module-level convenience."""

    @pytest.fixture
    def model_config(self):
        return ModelConfig()

    def test_anthropic_model_supports_text_image_pdf(self, model_config):
        result = model_config.get_input_modalities("claude-sonnet-4-6")
        assert "text" in result
        assert "image" in result
        assert "pdf" in result

    def test_openai_model_supports_text_image_pdf(self, model_config):
        result = model_config.get_input_modalities("gpt-5.4-mini")
        assert "text" in result
        assert "image" in result
        assert "pdf" in result

    def test_gemini_supports_video(self, model_config):
        result = model_config.get_input_modalities("gemini-3.1-pro")
        assert "video" in result
        assert "image" in result
        assert "pdf" in result

    def test_gemini_flash_image_no_pdf(self, model_config):
        result = model_config.get_input_modalities("gemini-3.1-flash-image")
        assert "image" in result
        assert "pdf" not in result

    def test_deepseek_text_only(self, model_config):
        result = model_config.get_input_modalities("deepseek-v4-flash")
        assert result == ["text"]

    def test_minimax_text_only(self, model_config):
        result = model_config.get_input_modalities("minimax-m2.7")
        assert result == ["text"]

    def test_moonshot_supports_image_not_pdf(self, model_config):
        result = model_config.get_input_modalities("kimi-k2.5")
        assert "image" in result
        assert "pdf" not in result

    def test_oauth_variant_inherits_modalities(self, model_config):
        """OAuth models should have explicit modalities, not fall back to default."""
        result = model_config.get_input_modalities("claude-opus-4-7-oauth")
        assert "image" in result
        assert "pdf" in result

    def test_codex_oauth_variant(self, model_config):
        result = model_config.get_input_modalities("gpt-5.4-oauth")
        assert "image" in result
        assert "pdf" in result

    def test_doubao_anthropic_variant(self, model_config):
        result = model_config.get_input_modalities("doubao-seed-2.0-pro-anthropic")
        assert "image" in result
        assert "pdf" in result

    def test_dashscope_coding_variant(self, model_config):
        result = model_config.get_input_modalities("qwen3.5-plus-coding")
        assert "image" in result
        assert "pdf" in result

    def test_unknown_model_defaults_to_text(self, model_config):
        result = model_config.get_input_modalities("nonexistent-model-xyz")
        assert result == ["text"]

    def test_every_model_has_valid_modalities(self, model_config):
        """Every model in models.json should resolve to a non-empty modality list."""
        for model_name in model_config.llm_config:
            result = model_config.get_input_modalities(model_name)
            assert isinstance(result, list), f"{model_name}: expected list"
            assert len(result) >= 1, f"{model_name}: empty modalities"
            assert "text" in result, f"{model_name}: missing 'text'"

    def test_module_level_convenience_function(self):
        """Module-level get_input_modalities() should work the same."""
        result = get_input_modalities("claude-sonnet-4-6")
        assert "image" in result
        assert "pdf" in result

    def test_module_level_unknown_model(self):
        result = get_input_modalities("nonexistent-xyz")
        assert result == ["text"]

    def test_custom_modalities_override(self):
        """When custom_modalities is provided, it is returned directly."""
        result = get_input_modalities("nonexistent-xyz", custom_modalities=["text", "image"])
        assert result == ["text", "image"]

    def test_custom_modalities_none_falls_through(self):
        """When custom_modalities is None, falls back to models.json lookup."""
        result = get_input_modalities("claude-sonnet-4-6", custom_modalities=None)
        assert "image" in result
        assert "pdf" in result
