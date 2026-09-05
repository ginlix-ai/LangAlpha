"""Contract for the manifest's reasoning-effort enumeration.

The point of the enum is that a model is offered exactly the levels it honors.
These lock the properties that fail *silently* if broken: a model advertising a
level it will reject, a level quietly downgraded on its way to the provider, and
a graded ladder attached to a model that ignores it.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.llms import LLM
from src.llms.reasoning import REASONING_LEVELS, apply_reasoning_effort

#: The clamp is implemented twice — here and as ``resolveEffort`` in
#: ``web/src/lib/modelTuning.ts``, which labels what a model inherits. Both read
#: this table so a change to one side fails the other's suite instead of quietly
#: making the UI name a level the server does not run.
_CLAMP_CONTRACT = json.loads(
    (Path(__file__).parents[3] / "tests/fixtures/reasoning_clamp.json").read_text()
)


@pytest.fixture(scope="module")
def config():
    return LLM.get_model_config()


class TestDeclaredSetsAreWellFormed:
    def test_every_declared_level_exists(self, config):
        """Read through ``reasoning_block``, not off a fixed key: a typo inside
        the block is dropped by ``canonical_reasoning_efforts`` without a word,
        so a check reading a key no entry carries any more passes on an empty
        set and proves nothing."""
        from src.llms.model_spec import reasoning_block

        ladders = {
            name: reasoning_block(entry).get("efforts")
            for name, entry in config.llm_config.items()
            if isinstance(entry, dict)
        }
        assert any(ladders.values()), "no entry declares a ladder -- check is vacuous"
        bad = {
            name: [lvl for lvl in efforts if lvl not in REASONING_LEVELS]
            for name, efforts in ladders.items()
            if isinstance(efforts, list)
        }
        assert not {k: v for k, v in bad.items() if v}

    def test_accessor_returns_canonical_order(self, config):
        """The UI renders the list as given, so a manifest author writing
        ["high","low"] must not produce a backwards toggle."""
        for name in config.llm_config:
            efforts = config.get_reasoning_efforts(name)
            assert efforts == sorted(efforts, key=REASONING_LEVELS.index)

    def test_default_is_always_offered(self, config):
        """A default outside the model's own set would be unreachable from the
        UI and would be sent for every user who never picked a level."""
        for name in config.llm_config:
            efforts = config.get_reasoning_efforts(name)
            default = config.get_reasoning_effort_default(name)
            if efforts:
                assert default in efforts, name
            else:
                assert default is None, name


class TestUnhonoredLevelsDegrade:
    """The account-wide default is chosen with no model in hand, and stored
    preferences outlive manifest edits, so an unhonored level is normal input."""

    def test_supported_level_is_untouched(self, config):
        assert config.resolve_reasoning_effort("claude-opus-5", "xhigh") == "xhigh"

    def test_model_with_no_control_resolves_to_nothing(self, config):
        """A chat model that declares no ladder, not a model of another kind."""
        assert config.resolve_reasoning_effort("claude-haiku-4-5", "high") is None

    def test_above_the_ceiling_steps_down_one(self, config):
        """max on a low/medium/high model runs high, not that model's middle."""
        assert config.get_reasoning_efforts("gemini-3.1-pro") == [
            "low",
            "medium",
            "high",
        ]
        assert config.resolve_reasoning_effort("gemini-3.1-pro", "max") == "high"

    def test_below_the_floor_takes_the_lowest(self, config):
        """Nothing at or under the request — the model's minimum is as close as it gets."""
        assert config.resolve_reasoning_effort("claude-opus-5", "none") == "low"

    @pytest.mark.parametrize(
        "case", _CLAMP_CONTRACT["cases"], ids=lambda c: f"{c['requested']}->{c['expected']}"
    )
    def test_clamps_into_the_ladder(self, config, case):
        """A binary model's only levels are off and on, so stepping down from a
        thinking level lands on off — the clamp never overshoots the request."""
        with patch.object(
            config, "get_reasoning_efforts", return_value=case["ladder"]
        ):
            assert (
                config.resolve_reasoning_effort("any-model", case["requested"])
                == case["expected"]
            )

    def test_the_canonical_order_matches_the_shared_contract(self):
        """The TypeScript mirror derives its ladder order from this same list."""
        assert list(REASONING_LEVELS) == _CLAMP_CONTRACT["levels"]

    @pytest.mark.parametrize("level", REASONING_LEVELS)
    def test_every_model_survives_every_legacy_value(self, config, level):
        """No stored value may produce a level the model does not honor."""
        for name in config.llm_config:
            efforts = config.get_reasoning_efforts(name)
            if not efforts:
                continue
            assert config.resolve_reasoning_effort(name, level) in efforts, name

    def test_never_overshoots_the_request(self, config):
        """The whole point of stepping down: no model may think harder than asked."""
        for name in config.llm_config:
            efforts = config.get_reasoning_efforts(name)
            if not efforts:
                continue
            for level in REASONING_LEVELS:
                got = config.resolve_reasoning_effort(name, level)
                if REASONING_LEVELS.index(got) > REASONING_LEVELS.index(level):
                    # Only legal when the request sits under the model's floor.
                    assert got == efforts[0], f"{name}: {level} -> {got}"


class TestEveryEntryBuilds:
    """``ModelSpec.from_manifest`` is where a declared surface is checked, and
    it runs per request, so a typo in a rarely-picked model would otherwise
    first surface as a 500 for whoever picked it. Walking the whole catalog
    here is what makes the allowlist a load-time guarantee."""

    def test_every_entry_builds_a_spec(self, config):
        from src.llms.model_spec import ModelSpec

        for name in config.llm_config:
            ModelSpec.from_manifest(config, name)

    def test_every_declared_ladder_has_somewhere_to_write(self, config):
        """The failure the block exists to make loud, checked against the real
        catalog rather than a fixture: buttons with no path behind them."""
        from src.llms.model_spec import ModelSpec

        for name in config.llm_config:
            spec = ModelSpec.from_manifest(config, name)
            assert bool(spec.reasoning_efforts) == bool(spec.reasoning_surface), name


class TestManifestAgreesWithTheMapper:
    """A declared level must actually reach the provider. These two drifting
    apart is the failure the enum exists to prevent."""

    def test_declared_levels_change_the_request(self, config):
        """Adjacent levels must not emit an identical payload — that is exactly
        the lie the enum replaces (a binary switch wearing four buttons)."""
        import copy
        import json

        from src.llms.model_spec import ModelSpec

        for name in config.llm_config:
            efforts = config.get_reasoning_efforts(name)
            if len(efforts) < 2:
                continue
            spec = ModelSpec.from_manifest(config, name)
            seen = {}
            for level in efforts:
                params = copy.deepcopy(spec.parameters)
                extra = copy.deepcopy(spec.extra_body)
                apply_reasoning_effort(level, params, extra, spec.reasoning_surface)
                seen[level] = json.dumps([params, extra], sort_keys=True)
            assert len(set(seen.values())) == len(efforts), (
                f"{name}: levels emit duplicate payloads -> {seen}"
            )

    def test_qwen_tops_out_where_its_endpoint_does(self, config):
        """Dashscope serves `xhigh` and `max` only from Beijing and Singapore.
        Declaring them anywhere else is a 400 on the levels a user is most
        likely to reach for, so the two endpoints that carry them are named
        here rather than derived: adding a third region should stop at this
        test and make its own case."""
        SERVES_THE_TOP = {"dashscope", "dashscope-intl"}
        for name, entry in config.llm_config.items():
            if not isinstance(entry, dict):
                continue
            # Keyed off the provider family, not the surface shape: the surface
            # is shared with OpenAI, which does serve the top of the ladder.
            if not str(entry.get("provider", "")).startswith("dashscope"):
                continue
            top = set(config.get_reasoning_efforts(name)) & {"xhigh", "max"}
            if top:
                assert entry.get("provider") in SERVES_THE_TOP, (
                    f"{name}: declares {sorted(top)} on provider "
                    f"{entry.get('provider')!r}, which is not known to serve them"
                )


class TestWhoWinsWhenTwoThingsNameTheLevel:
    """The manifest used to seed its default level straight into
    ``parameters``, which put a caller's own ``parameters`` override above the
    default and below an explicit request. The mapper writes what that seed
    held, so it has to keep the same two rungs.
    """

    def test_an_override_beats_the_manifest_default(self):
        client = LLM("gpt-5.5", reasoning={"effort": "high"})
        assert client.parameters["reasoning"]["effort"] == "high"

    def test_a_requested_level_beats_an_override(self):
        client = LLM("gpt-5.5", reasoning_effort="low", reasoning={"effort": "high"})
        assert client.parameters["reasoning"]["effort"] == "low"
        assert client.resolved_reasoning_effort == "low"

    def test_the_default_is_still_written_when_nothing_overrides_it(self, config):
        """The level is no longer sitting in `parameters` waiting to be sent, so
        skipping the mapper on this path reports a level the request never
        carried. Walked over the catalog rather than one model: dropping the
        no-request apply leaves every other test in this file green."""
        from src.llms.model_spec import ModelSpec

        checked = 0
        for name in config.llm_config:
            spec = ModelSpec.from_manifest(config, name)
            write = spec.reasoning_surface.get("write")
            if not (write and spec.reasoning_effort_default):
                continue  # no dial, or no ladder to have a default on
            client = LLM(name, api_key="dummy-key")
            lanes = {"parameters": client.parameters, "extra_body": client.extra_body}
            node = lanes[write.split(".")[0]]
            for segment in write.split(".")[1:]:
                node = node[segment]
            assert node == client.resolved_reasoning_effort == spec.reasoning_effort_default, name
            checked += 1
        assert checked > 10, f"only {checked} models exercised the no-request path"
