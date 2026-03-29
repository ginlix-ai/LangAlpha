import pytest

from src.llms.llm import ModelConfig


class TestFlattenProviders:
    """Tests for ModelConfig._flatten_providers() static method."""

    def test_pattern_a_variant_inherits_parent_fields(self):
        """Pattern A: variant inherits all parent fields and adds parent_provider."""
        grouped = {
            "z-ai": {
                "sdk": "anthropic",
                "base_url": "https://open.bigmodel.cn/api/paas/v4",
                "env_key": "Z_AI_API_KEY",
                "display_name": "Zhipu AI",
                "variants": {
                    "z-ai-cn": {
                        "base_url": "https://open.bigmodel.cn/api/paas/v4/cn",
                        "env_key": "Z_AI_CN_API_KEY",
                    }
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        assert "z-ai-cn" in result
        entry = result["z-ai-cn"]
        # Inherited from parent
        assert entry["sdk"] == "anthropic"
        assert entry["display_name"] == "Zhipu AI"
        # Overridden by variant
        assert entry["base_url"] == "https://open.bigmodel.cn/api/paas/v4/cn"
        assert entry["env_key"] == "Z_AI_CN_API_KEY"
        # parent_provider added because variant key != group key
        assert entry["parent_provider"] == "z-ai"

    def test_pattern_a_variant_overrides_specific_fields(self):
        """Pattern A: variant overrides should replace parent values."""
        grouped = {
            "openai": {
                "sdk": "openai",
                "base_url": "https://api.openai.com/v1",
                "env_key": "OPENAI_API_KEY",
                "access_type": "api_key",
                "variants": {
                    "codex-oauth": {
                        "sdk": "codex",
                        "base_url": "https://api.openai.com/v1/codex",
                        "env_key": "CODEX_OAUTH_TOKEN",
                        "access_type": "oauth",
                        "use_response_api": True,
                    }
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        entry = result["codex-oauth"]
        assert entry["sdk"] == "codex"
        assert entry["base_url"] == "https://api.openai.com/v1/codex"
        assert entry["env_key"] == "CODEX_OAUTH_TOKEN"
        assert entry["access_type"] == "oauth"
        assert entry["use_response_api"] is True

    def test_pattern_b_self_variant_no_parent_provider(self):
        """Pattern B: self-variant (key == group key) should NOT have parent_provider."""
        grouped = {
            "volcengine": {
                "env_key": "VOLCENGINE_API_KEY",
                "display_name": "Volcengine",
                "variants": {
                    "volcengine": {
                        "sdk": "openai",
                        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
                    },
                    "doubao-anthropic": {
                        "sdk": "anthropic",
                        "base_url": "https://ark.cn-beijing.volces.com/api/v3/anthropic",
                    },
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        entry = result["volcengine"]
        assert entry["sdk"] == "openai"
        assert entry["base_url"] == "https://ark.cn-beijing.volces.com/api/v3"
        assert entry["env_key"] == "VOLCENGINE_API_KEY"
        assert "parent_provider" not in entry

    def test_pattern_b_non_self_variant_gets_parent_provider(self):
        """Pattern B: non-self variant should get parent_provider set to group key."""
        grouped = {
            "volcengine": {
                "env_key": "VOLCENGINE_API_KEY",
                "display_name": "Volcengine",
                "variants": {
                    "volcengine": {
                        "sdk": "openai",
                        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
                    },
                    "doubao-anthropic": {
                        "sdk": "anthropic",
                        "base_url": "https://ark.cn-beijing.volces.com/api/v3/anthropic",
                    },
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        entry = result["doubao-anthropic"]
        assert entry["parent_provider"] == "volcengine"
        assert entry["sdk"] == "anthropic"
        assert entry["env_key"] == "VOLCENGINE_API_KEY"

    def test_pattern_b_sdk_fields_dont_leak(self):
        """Pattern B: fields on self-variant should not leak to other variants."""
        grouped = {
            "dashscope": {
                "env_key": "DASHSCOPE_API_KEY",
                "access_type": "api_key",
                "region": "cn",
                "variants": {
                    "dashscope": {
                        "sdk": "openai",
                        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                        "use_response_api": True,
                    },
                    "dashscope-coding": {
                        "sdk": "anthropic",
                        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/anthropic",
                    },
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        # Self-variant has use_response_api
        assert result["dashscope"].get("use_response_api") is True

        # Other variant should NOT inherit use_response_api from self-variant
        # It only gets shared parent fields + its own variant overrides
        assert "use_response_api" not in result["dashscope-coding"]
        assert result["dashscope-coding"]["sdk"] == "anthropic"
        assert result["dashscope-coding"]["env_key"] == "DASHSCOPE_API_KEY"
        assert result["dashscope-coding"]["region"] == "cn"

    def test_standalone_provider_passes_through(self):
        """A provider with no variants should appear unchanged in output."""
        grouped = {
            "gemini": {
                "sdk": "google",
                "base_url": "https://generativelanguage.googleapis.com/v1beta",
                "env_key": "GEMINI_API_KEY",
                "display_name": "Google Gemini",
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        assert "gemini" in result
        entry = result["gemini"]
        assert entry["sdk"] == "google"
        assert entry["base_url"] == "https://generativelanguage.googleapis.com/v1beta"
        assert entry["env_key"] == "GEMINI_API_KEY"
        assert entry["display_name"] == "Google Gemini"
        assert "parent_provider" not in entry

    def test_access_type_present_in_flattened(self):
        """Flattened entries should preserve access_type correctly per variant."""
        grouped = {
            "openai": {
                "sdk": "openai",
                "base_url": "https://api.openai.com/v1",
                "env_key": "OPENAI_API_KEY",
                "access_type": "api_key",
                "variants": {
                    "codex-oauth": {
                        "sdk": "openai",
                        "base_url": "https://api.openai.com/v1/codex",
                        "env_key": "CODEX_OAUTH_TOKEN",
                        "access_type": "oauth",
                    }
                },
            }
        }

        result = ModelConfig._flatten_providers(grouped)

        # Parent entry keeps api_key access
        assert result["openai"]["access_type"] == "api_key"
        # Variant overrides to oauth
        assert result["codex-oauth"]["access_type"] == "oauth"
        # No entry should have auth_type (it's not a valid field)
        for key, entry in result.items():
            assert "auth_type" not in entry, f"{key} should not have auth_type"

    def test_post_flatten_validation_missing_sdk(self):
        """Pattern B with no self-variant and no sdk on group should raise ValueError."""
        grouped = {
            "broken-provider": {
                "env_key": "BROKEN_API_KEY",
                "display_name": "Broken",
                "variants": {
                    "broken-variant": {
                        "base_url": "https://example.com/api",
                        # no sdk here either
                    }
                },
            }
        }

        with pytest.raises(ValueError, match="sdk"):
            ModelConfig._flatten_providers(grouped)

    def test_get_display_name_prefers_own_over_parent(self):
        """Variant with its own display_name should use it, not parent's."""
        grouped = {
            "parent-brand": {
                "sdk": "openai",
                "display_name": "Parent Brand",
                "env_key": "PARENT_KEY",
                "variants": {
                    "child-variant": {
                        "display_name": "Child Display Name",
                        "access_type": "oauth",
                    }
                },
            }
        }
        flat = ModelConfig._flatten_providers(grouped)

        # Simulate what get_display_name does: prefer own display_name
        child = flat["child-variant"]
        assert child.get("display_name") == "Child Display Name"
        assert child.get("parent_provider") == "parent-brand"
        # The parent's display_name should NOT override the child's
        parent = flat["parent-brand"]
        assert parent.get("display_name") == "Parent Brand"
