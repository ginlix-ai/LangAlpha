"""
Tests for user preferences validation — custom models and providers.

Covers:
- validate_custom_models() — name format, collision with system models, provider validation
- validate_custom_providers() — name format, parent_provider validation, builtin collision
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
    # flat_providers property is accessed by validate_custom_models
    mc.flat_providers = {p: {} for p in byok_providers}
    # llm_config is used by validate_custom_models to block names that
    # collide with built-in models.
    mc.llm_config = system_models
    return mc


# ---------------------------------------------------------------------------
# validate_custom_models (unit tests via direct import)
# ---------------------------------------------------------------------------


class TestValidateCustomModels:
    def _validate(self, models, providers=None, mc=None, stored=None):
        from src.server.services.llm.custom_catalog import validate_custom_models

        if mc is None:
            mc = _mock_model_config()
        # The validators read the LLM singleton, which an earlier test may
        # already hold the real manifest in.
        with patch("src.llms.llm.LLM.get_model_config", return_value=mc):
            validate_custom_models(models, providers, stored)

    @pytest.mark.parametrize("field", ["name", "model_id"])
    def test_a_field_that_is_not_a_string_is_a_400_not_a_500(self, field):
        entry = {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai", field: ["not", "a", "string"]}
        with pytest.raises(HTTPException) as refused:
            self._validate([entry])
        assert refused.value.status_code == 400

    def test_stored_entry_on_a_removed_provider_does_not_block_the_save(self):
        """A save carries the whole list, so the entry would block every change."""
        stale = {"name": "my-doubao", "model_id": "doubao-seed", "provider": "volcengine"}
        new = {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai"}
        self._validate([dict(stale), new], stored={"custom_models": [stale]})

    def test_stored_entry_on_a_provider_gone_platform_only_does_not_block_the_save(self):
        """Re-tiering a provider must not lock out users who saved a model on it."""
        mc = _mock_model_config()
        mc.flat_providers["minimax"] = {"platform": True}
        kept = {"name": "my-minimax", "model_id": "MiniMax-M3", "provider": "minimax"}
        self._validate([dict(kept)], mc=mc, stored={"custom_models": [kept]})
        with pytest.raises(HTTPException, match="minimax"):
            self._validate([kept, {**kept, "name": "other"}], mc=mc, stored={"custom_models": [kept]})

    def test_removing_an_own_provider_does_not_keep_its_models(self):
        relay = {"name": "my-relay", "parent_provider": "openai"}
        model = {"name": "relay-gpt", "model_id": "gpt-4o", "provider": "my-relay"}
        with pytest.raises(HTTPException, match="my-relay"):
            self._validate([model], providers=[], stored={"custom_providers": [relay], "custom_models": [model]})

    def test_new_entry_on_a_removed_provider_is_refused(self):
        stale = {"name": "my-doubao", "model_id": "doubao-seed", "provider": "volcengine"}
        with pytest.raises(HTTPException, match="volcengine"):
            self._validate([stale, {**stale, "name": "other"}], stored={"custom_models": [stale]})

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
        ``glm-5.2``) through a user's variant-specific key."""
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

    def test_a_block_carrying_only_a_surface_is_still_checked(self):
        """The save-time check used to run only when the block declared levels,
        so a block that named nothing but a path was stored unread. Nothing
        writes it today, which is exactly why a typo in it would sit there."""
        with pytest.raises(HTTPException):
            self._validate([{
                "name": "my-gpt", "model_id": "gpt-4o", "provider": "openai",
                "reasoning": {"write": "parmeters.reasoning.effort"},
            }])

    def test_a_wrongly_typed_block_is_a_400_not_a_500(self):
        """`on` as a list survives the path check -- iterating it yields the
        same strings a dict would -- then raises AttributeError in the mapper,
        once per turn, for every turn."""
        with pytest.raises(HTTPException):
            self._validate([{
                "name": "my-gpt", "model_id": "gpt-4o", "provider": "openai",
                "reasoning": {
                    "efforts": ["none", "high"],
                    "write": "parameters.output_config.effort",
                    "on": ["parameters.thinking.type"],
                    "off": {"parameters.thinking.type": "disabled"},
                },
            }])

    def test_an_empty_block_does_not_strand_the_flat_ladder(self):
        """Accepted as declaring a ladder, then read as declaring none: the
        level the user had already picked was cleared on the next save."""
        from src.server.app.users import _clear_unhonored_efforts

        entry = {
            "name": "my-gpt", "model_id": "gpt-4o", "provider": "openai",
            "reasoning": {}, "reasoning_efforts": ["low", "high"],
        }
        self._validate([dict(entry)])
        written = {}
        _clear_unhonored_efforts(
            written, {"custom_models": [entry]}, {"my-gpt": {"reasoning_effort": "high"}}
        )
        assert written == {}

    def test_a_shadow_outlives_the_built_in_it_shadowed(self):
        """Saved while the built-in supplied the ladder its default names. The
        client resends the whole list on any edit, so refusing it once that
        model is retired would lock every custom model the user has."""
        self._validate([{
            "name": "retired-model", "model_id": "retired-model", "provider": "openai",
            "reasoning_effort_default": "high",
        }])

    @pytest.mark.parametrize("context", [128_000, 1_000, 100_000_000])
    def test_a_context_window_in_range_passes(self, context):
        self._validate([{"name": "m", "model_id": "m", "provider": "openai", "context": context}])

    @pytest.mark.parametrize("context", ["128000", 128_000.5, True, 999, 100_000_001, -1])
    def test_a_context_window_compaction_cannot_use_is_refused(self, context):
        with pytest.raises(HTTPException, match="context"):
            self._validate([{"name": "m", "model_id": "m", "provider": "openai", "context": context}])

    def test_a_stored_entry_is_not_refused_by_a_check_it_predates(self):
        """The window and shadowed-ladder checks came after entries were saved.
        A save carries the whole list, so one entry they refuse would block
        every later change; only an entry this save adds or changes is held."""
        mc = _mock_model_config(system_models={
            "shadowed": {"provider": "openai", "model_id": "shadowed", "reasoning": {"efforts": ["low", "high"]}},
        })
        small = {"name": "small", "model_id": "small", "provider": "openai", "context": 500}
        shadow = {"name": "shadowed", "model_id": "shadowed", "provider": "openai", "reasoning_effort_default": "max"}
        stored = {"custom_models": [small, shadow]}
        new = {"name": "new", "model_id": "new", "provider": "openai"}

        self._validate([dict(small), dict(shadow), new], mc=mc, stored=stored)
        with pytest.raises(HTTPException, match="context"):
            self._validate([{**small, "model_id": "small-2"}, dict(shadow)], mc=mc, stored=stored)
        with pytest.raises(HTTPException, match="reasoning_effort_default"):
            self._validate([dict(small), {**shadow, "model_id": "shadowed-2"}], mc=mc, stored=stored)

    def _switch_only_built_in(self):
        mc = _mock_model_config(system_models={
            "switched": {
                "provider": "openai", "model_id": "switched",
                "reasoning": {
                    "efforts": ["none", "high"],
                    "on": {"parameters.thinking.type": "enabled"},
                    "off": {"parameters.thinking.type": "disabled"},
                },
            },
        })
        mc.get_provider_info.side_effect = lambda p: {"openai": {"sdk": "openai"}}.get(p, {})
        return mc

    def test_the_ladder_is_checked_as_merged_with_what_it_borrows(self):
        """A flat ladder borrows the built-in's surface. Three rungs on a
        surface with no ``write`` are one ``on`` patch, which every turn would
        refuse; the entry's own half alone shows nothing wrong."""
        entry = {"name": "switched", "model_id": "switched", "provider": "openai",
                 "reasoning_efforts": ["none", "low", "high"]}
        with pytest.raises(HTTPException, match="same `on` patch"):
            self._validate([entry], mc=self._switch_only_built_in())

    def test_a_shadow_naming_its_own_write_is_not_held_to_the_built_ins_switches(self):
        self._validate([{
            "name": "switched", "model_id": "switched", "provider": "openai",
            "reasoning": {"efforts": ["low", "high"], "write": "parameters.reasoning.effort"},
        }], mc=self._switch_only_built_in())

    def test_text_auto_prepended(self):
        """If input_modalities is provided without 'text', it should be auto-added."""
        models = [{"name": "my-llava", "model_id": "llava", "provider": "openai", "input_modalities": ["image"]}]
        self._validate(models)
        # After validation, "text" should be prepended
        assert models[0]["input_modalities"] == ["text", "image"]


# ---------------------------------------------------------------------------
# validate_custom_providers (unit tests via direct import)
# ---------------------------------------------------------------------------


class TestValidateCustomProviders:
    def _validate(self, providers, mc=None, stored=None):
        from src.server.services.llm.custom_catalog import validate_custom_providers

        if mc is None:
            mc = _mock_model_config()
        # The validators read the LLM singleton, which an earlier test may
        # already hold the real manifest in.
        with patch("src.llms.llm.LLM.get_model_config", return_value=mc):
            validate_custom_providers(providers, stored)

    def test_stored_provider_on_a_removed_parent_does_not_block_the_save(self):
        stale = {"name": "my-ark", "parent_provider": "volcengine"}
        self._validate([dict(stale), {"name": "my-openai", "parent_provider": "openai"}], stored={"custom_providers": [stale]})
        with pytest.raises(HTTPException, match="not a BYOK-eligible"):
            self._validate([stale, {"name": "other-ark", "parent_provider": "volcengine"}], stored={"custom_providers": [stale]})

    def test_stored_provider_on_a_parent_no_longer_byok_eligible_does_not_block_the_save(self):
        mc = _mock_model_config()
        mc.flat_providers["minimax"] = {}
        kept = {"name": "my-minimax", "parent_provider": "minimax"}
        self._validate([dict(kept)], mc=mc, stored={"custom_providers": [kept]})

    def test_valid_provider(self):
        self._validate([{"name": "my-openai", "parent_provider": "openai"}])

    def test_a_stored_provider_whose_name_became_a_built_in_still_saves(self):
        """Users set up ``xai`` by hand before the manifest listed it."""
        mc = _mock_model_config(byok_providers=["openai", "anthropic", "xai"])
        kept = {"name": "xai", "parent_provider": "openai"}
        self._validate([dict(kept)], mc=mc, stored={"custom_providers": [kept]})
        with pytest.raises(HTTPException, match="conflicts with built-in"):
            self._validate([{**kept, "parent_provider": "anthropic"}], mc=mc, stored={"custom_providers": [kept]})

    def test_a_subscriptions_name_is_never_kept(self):
        """Its models would run on the sign-in, so not even a stored one keeps it."""
        mc = _mock_model_config(byok_providers=["openai", "anthropic", "grok-oauth"])
        kept = {"name": "grok-oauth", "parent_provider": "openai"}
        with pytest.raises(HTTPException, match="conflicts with built-in"):
            self._validate([dict(kept)], mc=mc, stored={"custom_providers": [kept]})

    def test_missing_name_raises(self):
        with pytest.raises(HTTPException, match="name is required"):
            self._validate([{"parent_provider": "openai"}])

    @pytest.mark.parametrize("field", ["name", "parent_provider"])
    def test_a_field_that_is_not_a_string_is_a_400_not_a_500(self, field):
        entry = {"name": "my-provider", "parent_provider": "openai", field: {"not": "a string"}}
        with pytest.raises(HTTPException) as refused:
            self._validate([entry])
        assert refused.value.status_code == 400

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
# AgentPreference model — output_format round-trips, extra keys preserved
# ---------------------------------------------------------------------------


class TestAgentPreferenceModel:
    def test_output_format_round_trips(self):
        from src.server.models.user import AgentPreference

        pref = AgentPreference(output_format="html")
        assert pref.model_dump(exclude_unset=True) == {"output_format": "html"}

    def test_output_format_none_preserved_when_set(self):
        """Explicit None must survive model_dump so the JSONB merge can delete the key."""
        from src.server.models.user import AgentPreference

        pref = AgentPreference(output_format=None)
        assert pref.model_dump(exclude_unset=True) == {"output_format": None}

    def test_extra_keys_preserved(self):
        """extra='allow' keeps unknown agent_preference keys for pass-through."""
        from src.server.models.user import AgentPreference

        pref = AgentPreference.model_validate(
            {"output_format": "markdown", "custom_key": "custom_value"}
        )
        dumped = pref.model_dump(exclude_unset=True)
        assert dumped["output_format"] == "markdown"
        assert dumped["custom_key"] == "custom_value"


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/preferences — end-to-end model preferences
# ---------------------------------------------------------------------------

