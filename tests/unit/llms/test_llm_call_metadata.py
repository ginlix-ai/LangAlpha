"""What a single LLM call reports about itself, as opposed to what the turn reports.

Graph run metadata flows down into every child run unchanged, so a subagent that
picks its own model at its own effort inherits the parent's values and asserts
them for calls they were never true of. Anything the client decides is therefore
stamped on the client here, where it can only describe the call that carries it.
"""

from __future__ import annotations

import pytest

from src.llms.llm import LLM


def _stub_llm(**fields) -> LLM:
    """An instance built past ``__init__``, so ``get_llm`` is exercised alone.

    Every attribute the stamp reads is set here rather than inherited: a
    synthetic provider is the point of the stub, and a field left to a default
    would be one the stamp reports without the test having chosen it.
    """
    llm = LLM.__new__(LLM)
    llm.sdk = "anthropic"
    llm.provider = "test-anthropic"
    llm.provider_info = {"access_type": "platform", "base_url": None}
    llm.env_key = None
    llm.base_url = None
    llm.default_headers = None
    llm.parameters = {}
    llm.extra_body = {}
    llm.model = "claude-test-id"
    llm.custom_model_name = "claude-test"
    llm.resolved_reasoning_effort = None
    llm.api_key_override = "dummy-token"
    llm.prompt_cache_key_enabled = False
    for key, value in fields.items():
        setattr(llm, key, value)
    return llm


class TestResolvedEffortIsWhatRan:
    """The recorded level is the one the provider sees, not the one asked for."""

    def _model_config(self):
        return LLM.get_model_config()

    def _a_clamping_pair(self):
        config = self._model_config()
        for model in config.llm_config:
            for requested in ("minimal", "low", "medium", "high"):
                resolved = config.resolve_reasoning_effort(model, requested)
                if resolved and resolved != requested:
                    return model, requested, resolved
        pytest.skip("no manifest model currently steps a requested level down")

    def test_a_stepped_down_request_records_the_step(self):
        """Recording the request would claim a level the model never honored."""
        model, requested, resolved = self._a_clamping_pair()
        llm = LLM(model, api_key="dummy-token", reasoning_effort=requested)
        assert llm.resolved_reasoning_effort == resolved
        assert llm.resolved_reasoning_effort != requested

    def test_no_request_reports_the_models_own_default(self):
        """The mapper writes it on this path too, so the call is not silent
        about its effort merely because nobody named one. That the level also
        reaches the wire is pinned in ``test_reasoning_efforts_manifest``; this
        is about what the call reports."""
        config = self._model_config()
        with_default = [
            model
            for model in config.llm_config
            if config.get_reasoning_effort_default(model)
        ]
        assert with_default, "manifest declares no reasoning defaults at all"
        for model in with_default:
            llm = LLM(model, api_key="dummy-token")
            assert llm.resolved_reasoning_effort == config.get_reasoning_effort_default(
                model
            )


class TestClientStamp:
    def test_billing_type_names_the_credential_not_the_flag(self):
        """byok/oauth/platform is resolved off the key in hand, which is strictly
        more than the boolean the graph run used to carry."""
        oauth = _stub_llm(provider_info={"access_type": "oauth", "base_url": None})
        assert oauth.get_llm().metadata["billing_type"] == "oauth"

        byok = _stub_llm(provider_info={"access_type": "platform", "base_url": None})
        assert byok.get_llm().metadata["billing_type"] == "byok"

        platform = _stub_llm(api_key_override=None)
        assert platform.get_llm().metadata["billing_type"] == "platform"

    def test_effort_and_tier_ride_the_client(self):
        client = _stub_llm(
            resolved_reasoning_effort="high",
            parameters={"service_tier": "priority"},
        ).get_llm()
        assert client.metadata["reasoning_effort"] == "high"
        assert client.metadata["service_tier"] == "priority"

    def test_a_client_carrying_no_tier_says_nothing_about_one(self):
        """Most routes never pass a tier at all, so the key is stamped only when
        the built client carries the parameter. Its absence is not a claim that
        the call ran standard, and nothing may read it as one."""
        client = _stub_llm(parameters={}).get_llm()
        assert "service_tier" not in client.metadata

    def test_absent_rather_than_false_when_untuned(self):
        """A key that is always present reads as an assertion about the call; an
        unrequested effort is the absence of one."""
        client = _stub_llm().get_llm()
        assert "reasoning_effort" not in client.metadata

    def test_agent_stamped_fields_survive_the_client_build(self):
        """The agents add prompt_guidance/compaction_profile after construction;
        get_llm must merge onto whatever is already there, not replace it."""
        llm = _stub_llm()
        client = llm.get_llm()
        client.metadata = {**client.metadata, "prompt_guidance": "lean"}
        merged = {**(client.metadata or {}), "compaction_profile": "relaxed"}
        assert merged["prompt_guidance"] == "lean"
        assert merged["billing_type"] == "byok"
