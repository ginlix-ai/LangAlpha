"""Claude Code rows in the catalog: listed only where they run, built as a runtime target."""

from unittest.mock import patch

import pytest

from src.llms import claude_code
from src.llms.claude_code import ClaudeCodeTarget, is_claude_code_model
from src.llms.llm import LLM, ModelConfig, create_llm, get_configured_llm_models, image_route
from src.llms.pricing_utils import find_model_pricing
from src.llms.reasoning_lineage import ANTHROPIC_LINEAGE, lineage_for_route

CODE_ROWS = {"claude-haiku-5-5-code", "claude-sonnet-5-5-code", "claude-opus-5-5-code"}


def _listed(runs: bool) -> tuple[set, dict, dict]:
    from src.server.services.llm.provider_catalog import provider_catalog

    provider_catalog.cache_clear()
    try:
        with patch.object(claude_code, "runtime_available", return_value=runs):
            metadata = set(ModelConfig().get_model_metadata())
            grouped = {group: set(names) for group, names in get_configured_llm_models().items()}
            providers = {p["provider"]: p for p in provider_catalog()}
    finally:
        provider_catalog.cache_clear()
    return metadata, grouped, providers


def test_a_deployment_that_does_not_run_claude_code_lists_none_of_it():
    metadata, grouped, providers = _listed(False)
    assert not CODE_ROWS & metadata and not CODE_ROWS & set().union(*grouped.values())
    assert "claude-code" not in grouped and "claude-code" not in providers
    assert "claude-sonnet-5-5-oauth" in metadata, "the subscription rows beside them stay listed"


def test_a_deployment_that_runs_claude_code_lists_it_as_a_provider_of_its_own():
    metadata, grouped, providers = _listed(True)
    assert CODE_ROWS <= metadata and grouped["claude-code"] == CODE_ROWS
    assert not CODE_ROWS & grouped["anthropic"]
    entry = providers["claude-code"]
    assert (entry["brand_key"], entry["group"], entry["group_display_name"]) == ("claude-code", "claude-code", "Claude Code")


def test_claude_code_is_still_priced_and_fitted_as_anthropics_models():
    config = LLM.get_model_config()
    assert config.get_parent_provider("claude-code") == "anthropic"
    assert image_route({"pricing_provider": "claude-code", "manifest_model": "claude-opus-5-5-code"}) == "anthropic"
    assert find_model_pricing("claude-opus-5-5", "claude-code") == find_model_pricing("claude-opus-5-5", "anthropic")


def test_an_unlisted_row_still_resolves_so_a_saved_pick_is_never_read_as_removed():
    with patch.object(claude_code, "runtime_available", return_value=False):
        assert all(LLM.get_model_config().get_model_config(name) for name in CODE_ROWS)
        assert all(is_claude_code_model(name) for name in CODE_ROWS)
    assert not is_claude_code_model("claude-sonnet-5-5-oauth") and not is_claude_code_model({"not": "a name"})


@pytest.mark.parametrize(
    ("flag", "host_mode", "sdk", "key", "runs"),
    [
        (True, "oss", True, "k", True),
        (False, "oss", True, "k", False),
        (True, "platform", True, "k", False),
        (True, "oss", False, "k", False),
        (True, "oss", True, "", False),
    ],
)
def test_claude_code_runs_only_self_hosted_with_the_switch_on_the_extra_installed_and_the_key_set(
    flag, host_mode, sdk, key, runs, monkeypatch
):
    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", key)
    with (
        patch("src.config.features.is_feature_enabled_system", return_value=flag),
        patch("src.config.settings.HOST_MODE", host_mode),
        patch("importlib.util.find_spec", return_value=object() if sdk else None),
    ):
        assert claude_code.runtime_available() is runs


def test_a_claude_code_row_builds_a_target_that_refuses_to_be_called():
    target = create_llm("claude-sonnet-5-5-code", reasoning_effort="xhigh")
    assert isinstance(target, ClaudeCodeTarget)
    assert (target.model, target.effort) == ("claude-sonnet-5-5", "xhigh")
    assert target.metadata["billing_type"] == "oauth"
    assert target.metadata["manifest_model"] == "claude-sonnet-5-5-code"
    with pytest.raises(RuntimeError, match="Claude Code runtime"):
        target.invoke("hi")


def test_claude_codes_replies_replay_to_the_anthropic_api_as_the_subscription_rows_do():
    assert lineage_for_route("claude-code") == lineage_for_route("claude-oauth") == ANTHROPIC_LINEAGE
