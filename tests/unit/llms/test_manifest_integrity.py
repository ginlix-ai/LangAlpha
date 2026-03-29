"""Regression tests that load the REAL providers.json and models.json files.

These verify that the provider config v2 restructure (grouped format with
variants + flattening) didn't break any model-to-provider resolution.
No mocking -- these hit the actual manifest files on disk.
"""

import pytest

from src.llms.llm import ModelConfig
from src.llms.pricing_utils import find_model_pricing


class TestManifestIntegrity:
    @pytest.fixture
    def model_config(self):
        return ModelConfig()

    def test_every_model_resolves_to_valid_provider(self, model_config):
        """Every model in models.json must resolve to a usable provider after flatten.

        For each model entry that declares a ``provider`` field, the flattened
        provider info must:
        - exist (non-empty dict returned by get_provider_info)
        - contain a ``sdk`` key (required to instantiate the LLM client)
        - contain at least one of ``base_url`` or ``env_key`` so the provider
          is reachable (env_key may be None for oauth/dynamic providers, but
          the key itself should still be present in the dict)
        """
        failures: list[str] = []

        for model_name, model_def in model_config.llm_config.items():
            provider = model_def.get("provider")
            if provider is None:
                continue

            info = model_config.get_provider_info(provider)

            if not info:
                failures.append(
                    f"{model_name}: provider '{provider}' resolved to empty/None"
                )
                continue

            if "sdk" not in info:
                failures.append(
                    f"{model_name}: provider '{provider}' missing 'sdk' field"
                )

            has_base_url = "base_url" in info
            has_env_key = "env_key" in info
            if not (has_base_url or has_env_key):
                failures.append(
                    f"{model_name}: provider '{provider}' has neither "
                    "'base_url' nor 'env_key'"
                )

        assert not failures, (
            f"{len(failures)} model(s) failed provider resolution:\n"
            + "\n".join(f"  - {f}" for f in failures)
        )

    def test_dashscope_coding_resolves_pricing_via_parent(self, model_config):
        """dashscope-coding must either have a model with working pricing
        fallback through its parent (dashscope), or at minimum the parent
        resolution itself must work.
        """
        # Find any model that uses the dashscope-coding provider
        dc_model = None
        for model_name, model_def in model_config.llm_config.items():
            if model_def.get("provider") == "dashscope-coding":
                dc_model = (model_name, model_def)
                break

        if dc_model is not None:
            model_name, model_def = dc_model
            model_id = model_def.get("model_id", model_name)
            pricing = find_model_pricing(model_id, provider="dashscope-coding")
            assert pricing is not None, (
                f"find_model_pricing('{model_id}', provider='dashscope-coding') "
                "returned None -- parent fallback to dashscope is broken"
            )
        else:
            # No dashscope-coding model in the manifest right now, but the
            # parent resolution plumbing must still work.
            parent = model_config.get_parent_provider("dashscope-coding")
            assert parent == "dashscope", (
                f"Expected parent provider of 'dashscope-coding' to be "
                f"'dashscope', got '{parent}'"
            )
