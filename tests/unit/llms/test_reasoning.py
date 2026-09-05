"""Tests for src.llms.reasoning — the declared-surface effort mapper.

The mapper no longer guesses a vendor from the keys an entry happens to carry.
Each entry names its own surface, so what is worth locking here is
the block's contract: ``write`` takes the level verbatim, ``on`` layers under it,
``off`` replaces both rather than layering over them, and a path outside the
allowlists is refused rather than written somewhere the vendor ignores.
"""

import copy

import pytest

from src.llms.reasoning import (
    OFF_LEVELS,
    PATCH_PATHS,
    REASONING_LEVELS,
    WRITE_PATHS,
    ReasoningSurfaceError,
    apply_reasoning_effort,
    infer_surface,
    validate_surface,
)


def run(level, surface, parameters=None, extra_body=None):
    p, b = copy.deepcopy(parameters or {}), copy.deepcopy(extra_body or {})
    apply_reasoning_effort(level, p, b, surface)
    return p, b


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------


class TestVocabulary:
    def test_levels(self):
        assert REASONING_LEVELS == (
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        )

    def test_levels_are_ordered_weakest_first(self):
        """The UI renders a model's declared subset in this order, and the clamp
        walks down it — an unordered tuple would silently pick the wrong rung."""
        assert REASONING_LEVELS.index("none") < REASONING_LEVELS.index("low")
        assert REASONING_LEVELS.index("low") < REASONING_LEVELS.index("high")
        assert REASONING_LEVELS.index("high") < REASONING_LEVELS.index("max")

    def test_only_none_means_off(self):
        """`low` is a real thinking level on every surface that grades. Binary
        surfaces used to key off it, which is why three of four buttons emitted
        an identical request."""
        assert OFF_LEVELS == frozenset({"none"})


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    @pytest.mark.parametrize("level", REASONING_LEVELS)
    def test_level_goes_out_verbatim(self, level):
        p, _ = run(level, {"write": "parameters.reasoning.effort"})
        assert p == {"reasoning": {"effort": level}}

    @pytest.mark.parametrize("path", WRITE_PATHS)
    def test_every_allowed_path_is_reachable(self, path):
        """A path in the allowlist no branch can reach would be a dead
        declaration that reports a level and sends nothing."""
        p, b = run("high", {"write": path})
        lanes = {"parameters": p, "extra_body": b}
        node = lanes[path.split(".")[0]]
        for segment in path.split(".")[1:]:
            node = node[segment]
        assert node == "high"

    def test_nested_containers_are_created(self):
        p, _ = run("low", {"write": "parameters.reasoning.effort"})
        assert p["reasoning"] == {"effort": "low"}

    def test_sibling_keys_survive(self):
        """`reasoning.summary` and friends are transport config that happens to
        share a container with the dial; the write must not flatten them."""
        p, _ = run(
            "max",
            {"write": "parameters.reasoning.effort"},
            parameters={"reasoning": {"summary": "auto"}, "max_tokens": 8},
        )
        assert p == {"reasoning": {"summary": "auto", "effort": "max"}, "max_tokens": 8}

    def test_non_dict_container_is_replaced(self):
        p, _ = run(
            "low", {"write": "parameters.reasoning.effort"}, parameters={"reasoning": 3}
        )
        assert p == {"reasoning": {"effort": "low"}}


# ---------------------------------------------------------------------------
# on / off
# ---------------------------------------------------------------------------


class TestOnAndOff:
    SURFACE = {
        "write": "parameters.output_config.effort",
        "on": {"parameters.thinking.type": "enabled"},
        "off": {"parameters.thinking.type": "disabled"},
    }

    @pytest.mark.parametrize("level", ["low", "high", "max"])
    def test_on_layers_under_the_write(self, level):
        p, _ = run(level, self.SURFACE)
        assert p == {"thinking": {"type": "enabled"}, "output_config": {"effort": level}}

    def test_off_replaces_the_write_rather_than_layering_over_it(self):
        """On a surface carrying both a switch and a dial, only the switch
        reliably means off; emitting both puts a live effort next to the
        instruction not to think.

        Verified live on 2026-09-02, one turn per rung. deepseek-v4-flash at
        `none` takes `thinking.type: disabled` with no `output_config` at all
        and returns no thinking block; at `high` it takes both and does. GLM
        5.2 at `none` takes `thinking.type: disabled` with no `reasoning_effort`
        and returns no reasoning; at `high` it takes the pair plus
        `clear_thinking: false` and reads 234 characters of reasoning back, so
        the gate still works carried per request rather than seeded.
        """
        p, _ = run("none", self.SURFACE)
        assert p == {"thinking": {"type": "disabled"}}

    def test_off_clears_a_level_the_caller_supplied_at_the_write_path(self):
        """The manifest no longer seeds one, but a caller override lands in the
        same place and is merged in before the mapper runs. Left alone it is
        the payload this branch exists to prevent, arriving by the one route
        the seed's removal did not close."""
        p, _ = run("none", self.SURFACE, parameters={"output_config": {"effort": "high"}})
        assert p["thinking"] == {"type": "disabled"}
        assert "effort" not in p["output_config"]

    def test_off_leaves_the_containers_own_siblings_alone(self):
        """Only the graded key is the contradiction. The rest of that container
        is the entry's transport config, which the switch says nothing about."""
        p, _ = run(
            "none",
            self.SURFACE,
            parameters={"output_config": {"effort": "high", "verbosity": "low"}},
        )
        assert p["output_config"] == {"verbosity": "low"}

    def test_off_without_an_off_patch_is_just_the_level(self):
        """A surface whose vendor accepts `none` as an effort needs no patch."""
        p, _ = run("none", {"write": "parameters.reasoning.effort"})
        assert p == {"reasoning": {"effort": "none"}}

    def test_surface_with_no_write_is_a_bare_switch(self):
        surface = {
            "on": {"parameters.thinking.type": "adaptive"},
            "off": {"parameters.thinking.type": "disabled"},
        }
        assert run("high", surface)[0] == {"thinking": {"type": "adaptive"}}
        assert run("none", surface)[0] == {"thinking": {"type": "disabled"}}

    def test_off_spans_both_lanes(self):
        p, b = run(
            "none",
            {
                "write": "extra_body.reasoning_effort",
                "off": {
                    "extra_body.thinking.type": "disabled",
                    "parameters.thinking.type": "disabled",
                },
            },
        )
        assert p == {"thinking": {"type": "disabled"}}
        assert b == {"thinking": {"type": "disabled"}}


# ---------------------------------------------------------------------------
# Merging with what the caller already supplied
# ---------------------------------------------------------------------------


class TestMergesWithSuppliedParams:
    """A model's `parameters`/`extra_body` carry transport config, and a caller
    may add more through override params. The level is written into that, not
    over it."""

    SWITCHED = {
        "write": "parameters.output_config.effort",
        "on": {"parameters.thinking.type": "enabled"},
        "off": {"parameters.thinking.type": "disabled"},
    }

    def test_write_merges_into_a_supplied_container(self):
        p, _ = run(
            "high",
            {"write": "parameters.reasoning.effort"},
            parameters={"reasoning": {"summary": "auto"}, "max_tokens": 8},
        )
        assert p == {"reasoning": {"summary": "auto", "effort": "high"}, "max_tokens": 8}

    def test_on_merges_and_leaves_unrelated_keys(self):
        p, _ = run(
            "high",
            self.SWITCHED,
            parameters={"output_config": {"verbosity": "low"}, "max_tokens": 8},
        )
        assert p == {
            "output_config": {"verbosity": "low", "effort": "high"},
            "thinking": {"type": "enabled"},
            "max_tokens": 8,
        }

    def test_off_replaces_its_container_rather_than_merging(self):
        """`thinking` is a discriminated union: the disabled variant rejects the
        `budget_tokens` the enabled one requires, so a supplied sibling must not
        survive the switch being turned off."""
        p, _ = run(
            "none",
            self.SWITCHED,
            parameters={"thinking": {"type": "enabled", "budget_tokens": 5000}, "max_tokens": 8},
        )
        assert p == {"thinking": {"type": "disabled"}, "max_tokens": 8}

    def test_off_leaves_keys_it_does_not_name(self):
        p, _ = run("none", self.SWITCHED, parameters={"max_tokens": 8})
        assert p == {"thinking": {"type": "disabled"}, "max_tokens": 8}

    def test_two_off_paths_sharing_a_container_both_land(self):
        """The container is cleared once, before either write, or the second
        path would wipe the first."""
        _, b = run(
            "none",
            {
                "write": "extra_body.reasoning_effort",
                "off": {
                    "extra_body.thinking.type": "disabled",
                    "extra_body.thinking.clear_thinking": False,
                },
            },
            extra_body={"thinking": {"type": "enabled", "stale": 1}},
        )
        assert b == {"thinking": {"type": "disabled", "clear_thinking": False}}


# ---------------------------------------------------------------------------
# Passthrough
# ---------------------------------------------------------------------------


class TestPassthrough:
    def test_no_surface_writes_nothing(self):
        """A model with no effort control must not acquire one."""
        assert run("high", None, parameters={"max_tokens": 8}) == ({"max_tokens": 8}, {})

    def test_level_outside_the_vocabulary_writes_nothing(self):
        assert run("invalid", {"write": "parameters.reasoning.effort"}) == ({}, {})

    def test_empty_level_writes_nothing(self):
        assert run("", {"write": "parameters.reasoning.effort"}) == ({}, {})

    def test_mutates_in_place(self):
        params, extra = {}, {}
        out_p, out_b = apply_reasoning_effort(
            "high", params, extra, {"write": "parameters.reasoning.effort"}
        )
        assert out_p is params and out_b is extra


# ---------------------------------------------------------------------------
# infer_surface
# ---------------------------------------------------------------------------


class TestInferSurface:
    """Entries stored before the block existed name no surface, so the seed
    they carry is the only evidence of where their level goes."""

    @pytest.mark.parametrize("path", WRITE_PATHS)
    def test_every_dial_is_recognized_from_its_seed(self, path):
        lane, *rest = path.split(".")
        seed = "medium"
        for segment in reversed(rest):
            seed = {segment: seed}
        lanes = {"parameters": {}, "extra_body": {}}
        lanes[lane] = seed
        assert infer_surface(lanes["parameters"], lanes["extra_body"]) == {"write": path}

    def test_two_seeded_dials_resolve_to_the_typed_lane(self):
        """WRITE_PATHS is ordered for exactly this: `parameters` holds typed SDK
        fields, so an entry seeded in both lanes writes there rather than
        wherever the paths happen to sort."""
        surface = infer_surface({"reasoning_effort": "low"}, {"reasoning_effort": "low"})
        assert surface == {"write": "parameters.reasoning_effort"}

    def test_a_seed_the_allowlist_does_not_name_infers_nothing(self):
        """A mode switch and a token budget are not dials, so no seed value of
        theirs distinguishes one from a dial's starting point."""
        assert infer_surface({"thinking": {"type": "enabled"}}, {}) == {}
        assert infer_surface({"max_tokens": 8}, {}) == {}


# ---------------------------------------------------------------------------
# validate_surface
# ---------------------------------------------------------------------------


class TestValidateSurface:
    def test_known_paths_pass(self):
        validate_surface("m", {"write": "parameters.reasoning.effort"})
        validate_surface("m", {"off": {"parameters.thinking.type": "disabled"}})

    def test_typo_in_a_write_is_refused(self):
        """The whole point of the allowlist: a misspelled path is structurally
        valid JSON that lands somewhere the vendor answers 200 for and ignores."""
        with pytest.raises(ReasoningSurfaceError, match="not a known write path"):
            validate_surface("m", {"write": "parmeters.reasoning.effort"})

    def test_typo_in_a_patch_is_refused(self):
        with pytest.raises(ReasoningSurfaceError, match="not a known patch path"):
            validate_surface("m", {"off": {"parameters.thinking.mode": "off"}})

    def test_a_mode_switch_is_not_a_write_target(self):
        """`thinking.type` takes a vendor literal, never a level name."""
        assert "parameters.thinking.type" in PATCH_PATHS
        assert "parameters.thinking.type" not in WRITE_PATHS
        with pytest.raises(ReasoningSurfaceError):
            validate_surface("m", {"write": "parameters.thinking.type"})

    def test_a_ladder_with_nowhere_to_write_is_refused(self):
        """The failure the block exists to make loud: levels the UI renders as
        buttons, and no path for the chosen one to be written to."""
        with pytest.raises(ReasoningSurfaceError, match="nowhere to write"):
            validate_surface("m", {"efforts": ["low", "high"], "default": "high"})

    def test_a_dial_is_not_a_patch_target(self):
        """The two allowlists are disjoint: an `off` free to name the entry's
        own graded write is how a switch and a dial end up contradicting each
        other in one payload."""
        assert not set(WRITE_PATHS) & PATCH_PATHS
        with pytest.raises(ReasoningSurfaceError, match="not a known patch path"):
            validate_surface("m", {"off": {"parameters.output_config.effort": "high"}})

    def test_an_on_without_an_off_is_refused(self):
        """Verified against the mapper before it was refused: at `none` this
        emits `thinking.type: enabled` beside `effort: none`, which is the
        surface enabling thinking on the rung that asks for none."""
        with pytest.raises(ReasoningSurfaceError, match="declares no `off`"):
            validate_surface(
                "m",
                {
                    "efforts": ["none", "high"],
                    "write": "parameters.output_config.effort",
                    "on": {"parameters.thinking.type": "enabled"},
                },
            )

    def test_an_unreachable_off_is_refused(self):
        """The clamp never hands the mapper a level outside the ladder, so an
        `off` with no off rung above it is a branch no request can enter."""
        with pytest.raises(ReasoningSurfaceError, match="never be applied"):
            validate_surface(
                "m",
                {
                    "efforts": ["low", "high"],
                    "write": "parameters.output_config.effort",
                    "off": {"parameters.thinking.type": "disabled"},
                },
            )

    @pytest.mark.parametrize(
        "block, wrong",
        [
            ({"efforts": 1, "write": "parameters.reasoning.effort"}, "efforts"),
            ({"write": ["parameters.reasoning.effort"]}, "write"),
            ({"on": ["parameters.thinking.type"]}, "on"),
            ({"off": "parameters.thinking.type"}, "off"),
        ],
    )
    def test_a_wrongly_typed_key_is_refused(self, block, wrong):
        """The block is user input on the preferences path, and every one of
        these otherwise reaches the mapper as an exception raised per turn --
        a 500 on data the save accepted."""
        with pytest.raises(ReasoningSurfaceError, match=f"reasoning.{wrong} must be"):
            validate_surface("m", block)

    def test_an_unhashable_effort_is_refused_before_the_set_math(self):
        """The ladder arrives from a stored preferences bag, so an element can
        be any JSON value. One unhashable one raised out of the ``OFF_LEVELS``
        intersection: a 500 from the function whose job is the 400."""
        with pytest.raises(ReasoningSurfaceError, match="reasoning.efforts must be drawn"):
            validate_surface("m", {"efforts": [{}], "write": "parameters.reasoning.effort"})

    def test_a_bare_switch_may_not_wear_more_than_two_rungs(self):
        """A patch is one payload, so every non-off rung on a surface with no
        graded write emits the same request. Three buttons, two outcomes, and
        nothing downstream can tell -- the shape the block exists to refuse."""
        with pytest.raises(ReasoningSurfaceError, match="all apply the same `on` patch"):
            validate_surface("m", {
                "efforts": ["none", "high", "max"],
                "on": {"parameters.thinking.type": "adaptive"},
                "off": {"parameters.thinking.type": "disabled"},
            })

    def test_a_two_rung_switch_is_still_fine(self):
        """The shape `minimax-m3` actually ships: off, and on."""
        validate_surface("m", {
            "efforts": ["none", "high"],
            "on": {"parameters.thinking.type": "adaptive"},
            "off": {"parameters.thinking.type": "disabled"},
        })

    def test_a_surface_with_no_ladder_skips_the_rung_checks(self):
        """An inferred surface carries no efforts, so there is no rung for the
        cross-field checks to be about."""
        validate_surface("m", {"off": {"parameters.thinking.type": "disabled"}})
        validate_surface("m", {"on": {"parameters.thinking.type": "enabled"}})
