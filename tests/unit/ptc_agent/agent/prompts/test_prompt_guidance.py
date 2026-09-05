"""Contract for the lean/detailed prompt guidance mechanism.

Locks the resolution order and the invariants that fail silently: an unknown
model must get more scaffolding rather than less, and every template fence must
name a level that exists.

The chain has two owners on purpose. ``resolve_prompt_guidance`` answers from
the deployment pin and the model's own declaration, which is all a build that
never reached the server can know. ``guidance_for`` puts the user's choice on
top of it, and is the only reader of stored preferences.
"""

import re
from pathlib import Path
from unittest.mock import patch

import pytest

from ptc_agent.agent.prompts import (
    guidance_template_vars,
    init_loader,
    resolve_prompt_guidance,
)
from ptc_agent.agent.prompts import guidance
from ptc_agent.agent.prompts.guidance import DEFAULT_GUIDANCE, VALID_GUIDANCE
from src.server.services.llm.user_models import guidance_for


def _pinned(level):
    """Pin the deployment-wide level the way ``config.yaml`` does."""
    return patch(
        "src.config.settings.get_prompt_guidance_default", return_value=level
    )


class TestDeclaredOrder:
    """What the model and the deployment say, with no user in the picture."""

    def test_manifest_lean_model(self):
        assert resolve_prompt_guidance("claude-opus-5") == "lean"

    def test_deployment_pin_beats_the_manifest(self):
        with _pinned("detailed"):
            assert resolve_prompt_guidance("claude-opus-5") == "detailed"

    def test_an_invalid_pin_is_ignored(self):
        with _pinned("yolo"):
            assert resolve_prompt_guidance("claude-opus-5") == "lean"

    def test_a_custom_entry_declares_for_itself(self):
        assert resolve_prompt_guidance("my-model", {"prompt_guidance": "lean"}) == "lean"

    def test_a_custom_entry_shadows_the_builtin_it_is_named_after(self):
        """Custom wins by name everywhere else in resolution, so a custom entry
        silent on the level must not inherit the manifest row's."""
        assert resolve_prompt_guidance("claude-opus-5", {"model_id": "x"}) == "detailed"


class TestUserPreference:
    """``guidance_for`` is the one reader of the stored bag."""

    def test_empty_bag_falls_through_to_the_manifest(self):
        assert guidance_for({}, "claude-opus-5") == "lean"

    def test_user_preference_beats_manifest(self):
        assert guidance_for({"prompt_guidance": "detailed"}, "claude-opus-5") == "detailed"

    def test_user_preference_can_opt_a_small_model_into_lean(self):
        assert guidance_for({"prompt_guidance": "lean"}, "gpt-oss-20b") == "lean"

    def test_user_preference_beats_the_deployment_pin(self):
        with _pinned("detailed"):
            assert guidance_for({"prompt_guidance": "lean"}, "gpt-oss-20b") == "lean"

    @pytest.mark.parametrize("value", ["yolo", "", None, True, 1])
    def test_invalid_preference_falls_through_to_manifest(self, value):
        assert guidance_for({"prompt_guidance": value}, "claude-opus-5") == "lean"

    def test_a_custom_model_declaration_is_reached_through_the_bag(self):
        """A custom model has no manifest row, so its own entry is the only
        thing standing between it and the fail-safe."""
        bag = {"custom_models": [{"name": "my-model", "prompt_guidance": "lean"}]}
        assert guidance_for(bag, "my-model") == "lean"

    def test_a_malformed_custom_row_does_not_raise(self):
        bag = {"custom_models": ["not-a-dict", {"name": "my-model"}]}
        assert guidance_for(bag, "my-model") == DEFAULT_GUIDANCE


class TestPerModelProfiles:
    """A profile for the running model beats the account-wide value.

    The two are separate storage, so the account-wide setting has to keep
    governing every model the user never tuned.
    """

    @staticmethod
    def _tuned(account, profile, model="claude-opus-5"):
        return {
            "prompt_guidance": account,
            "profiles": {model: {"prompt_guidance": profile}},
        }

    def test_profile_beats_the_account_wide_value(self):
        assert guidance_for(self._tuned("lean", "detailed"), "claude-opus-5") == "detailed"

    def test_the_profile_governs_only_its_own_model(self):
        assert guidance_for(self._tuned("lean", "detailed"), "gpt-oss-20b") == "lean"

    def test_a_profile_silent_on_guidance_falls_through_to_account_wide(self):
        bag = {
            "prompt_guidance": "detailed",
            "profiles": {"claude-opus-5": {"reasoning_effort": "high"}},
        }
        assert guidance_for(bag, "claude-opus-5") == "detailed"

    def test_no_model_name_cannot_select_a_profile(self):
        assert guidance_for(self._tuned("detailed", "lean"), None) == "detailed"

    def test_an_invalid_profile_value_discards_the_account_wide_value(self):
        """The user layer answers or abstains as a whole: the value is checked
        after resolution, so a typo in the profile drops the valid account-wide
        setting with it and resolution continues at the manifest.
        """
        assert guidance_for(self._tuned("detailed", "yolo"), "claude-opus-5") == "lean"


class TestFailSafeDefault:
    """Unknown or unannotated models must get more guidance, never less."""

    @pytest.mark.parametrize(
        "model_name",
        [None, "", "gpt-oss-20b", "some-byok-model-not-in-manifest"],
    )
    def test_defaults_to_detailed(self, model_name):
        assert resolve_prompt_guidance(model_name) == "detailed"

    def test_intelligence_score_does_not_drive_guidance(self):
        """`intelligence` is editorial copy for the model picker — a wording
        change there must not move agent behavior."""
        from src.llms import LLM

        config = LLM.get_model_config()
        haiku = config.llm_config["claude-haiku-4-5"]
        assert "prompt_guidance" not in haiku
        assert haiku.get("intelligence") is not None
        assert resolve_prompt_guidance("claude-haiku-4-5") == "detailed"


class TestTemplateVars:
    def test_detailed_level(self):
        assert guidance_template_vars("detailed") == {"guidance": "detailed"}

    def test_lean_level(self):
        assert guidance_template_vars("lean") == {"guidance": "lean"}


class TestFenceExpressions:
    """Fences compare a level by name, so a typo in the *value* is a silent
    no-op — the block vanishes from both tiers and the subset invariant still
    passes, because lean adds nothing. Nothing else catches this."""

    FENCE = re.compile(r"guidance\s*(?:\|\s*default\(\s*[\"'](\w+)[\"']\s*\)\s*)?==\s*[\"'](\w+)[\"']")

    def _fences(self):
        root = Path(guidance.__file__).parent / "templates"
        for path in sorted(root.rglob("*.j2")):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                for default, compared in self.FENCE.findall(line):
                    yield path.name, lineno, default, compared

    def test_the_scan_actually_finds_fences(self):
        """Both checks below pass vacuously if the regex matches nothing —
        which is also what a reworded fence syntax would look like."""
        assert list(self._fences()), (
            "no guidance fences found in templates/ — either the fences are "
            "gone or FENCE no longer matches how they are written"
        )

    def test_every_fence_compares_a_real_level(self):
        bad = [
            f"{name}:{lineno} compares guidance to {compared!r}"
            for name, lineno, _, compared in self._fences()
            if compared not in VALID_GUIDANCE
        ]
        assert not bad, "fence compares against a level that does not exist:\n" + "\n".join(bad)

    def test_every_fence_defaults_to_detailed(self):
        """A render path that forgets the key must get more guidance, never
        less — a bare `guidance == "detailed"` silently resolves lean."""
        bad = [
            f"{name}:{lineno} defaults to {default or 'nothing'!r}"
            for name, lineno, default, _ in self._fences()
            if default != DEFAULT_GUIDANCE
        ]
        assert not bad, (
            'every fence needs `| default("detailed")`:\n' + "\n".join(bad)
        )


class TestLeanIsSubsetOfDetailed:
    """One template body, expansions gated — not two copies that can drift."""

    def test_lean_prompt_is_shorter(self):
        loader = init_loader()
        kwargs = dict(tool_summary="", subagent_summary="")
        detailed = loader.get_system_prompt(**guidance_template_vars("detailed"), **kwargs)
        lean = loader.get_system_prompt(**guidance_template_vars("lean"), **kwargs)
        assert len(lean) < len(detailed)

    def test_default_render_is_detailed(self):
        """A render that forgets to pass the flag must not silently go lean."""
        loader = init_loader()
        kwargs = dict(tool_summary="", subagent_summary="")
        assert loader.get_system_prompt(**kwargs) == loader.get_system_prompt(
            **guidance_template_vars("detailed"), **kwargs
        )


class TestBucketIsInvisibleToThePrompt:
    """Why model-scoped settings never belonged in ``agent_preference``: the
    renderer echoes that bucket verbatim, so a knob stored there would become
    prompt text about itself. Nothing hands the other bucket to this layer any
    more (see ``test_user_profile_cache_key``), and this pins why."""

    def test_model_preference_never_reaches_the_model(self):
        loader = init_loader()
        rendered = loader.render(
            "components/user_profile.md.j2",
            user_profile={
                "model_preference": {"prompt_guidance": "lean"},
                "agent_preference": {"proactive_questions": "auto"},
            },
            sandbox_enabled=True,
        )
        assert "prompt_guidance" not in rendered
        # Ordinary preferences still reach the model.
        assert "proactive_questions" in rendered
