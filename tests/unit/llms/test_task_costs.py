"""Relative task cost per reasoning level, read from manifest/task_costs.json.

The shaping tests build their own small tables, so they pin the rules rather
than the shipped figures. Only the last class reads the real file, and it
asserts relationships the file's structure guarantees, not dollar amounts.
"""

import json
import logging

import pytest

from src.llms import task_costs
from src.llms.llm import ModelConfig
from src.llms.pricing_utils import find_model_pricing
from src.llms.task_costs import (
    ANY_EFFORT,
    parse_task_costs,
    task_cost,
    task_cost_baseline,
)

LADDER = ["low", "medium", "high", "xhigh", "max"]


def _table(
    models: dict, baseline: tuple[str, str] = ("base", "medium"), bounds=None
) -> dict:
    data = {
        "source": {"name": "test"},
        "baseline": {"model": baseline[0], "effort": baseline[1]},
        "models": {"base": {"medium": 0.5}, **models},
    }
    if bounds is not None:
        data["bounds"] = bounds
    return parse_task_costs(data)


@pytest.fixture
def isolated_file(monkeypatch, tmp_path):
    """Point the cached loader at a file under tmp_path, and drop the cache on
    both sides so neither the real file nor the fake one leaks across tests."""
    path = tmp_path / "task_costs.json"
    monkeypatch.setattr(task_costs, "TASK_COSTS_PATH", path)
    task_costs._shipped.cache_clear()
    yield path
    task_costs._shipped.cache_clear()


class TestRelativeValues:
    def test_each_figure_is_divided_by_the_baseline(self):
        table = _table({"m": {"low": 0.25, "high": 1.5}})
        result = task_cost("m", ["low", "high"], table)
        assert result["low"]["value"] == pytest.approx(0.5)
        assert result["high"]["value"] == pytest.approx(3.0)

    def test_the_baseline_reads_as_one(self):
        table = _table({})
        assert task_cost("base", LADDER, table)["medium"] == {
            "value": 1.0,
            "measured": True,
        }

    def test_values_are_not_rounded(self):
        table = _table({"m": {"low": 0.1234567}})
        assert task_cost("m", ["low"], table)["low"]["value"] == 0.1234567 / 0.5


class TestShaping:
    def test_keys_follow_the_models_ladder(self):
        """The file's own key order says nothing; the model's ladder decides."""
        table = _table({"m": {"max": 4.0, "low": 1.0, "medium": 2.0}})
        assert list(task_cost("m", LADDER, table)) == LADDER

    def test_a_gap_takes_the_nearest_measured_level_above_it(self):
        table = _table({"m": {"low": 1.0, "xhigh": 3.0, "max": 4.0}})
        result = task_cost("m", LADDER, table)
        assert result["medium"] == {"value": 6.0, "measured": False}
        assert result["high"] == {"value": 6.0, "measured": False}
        assert result["xhigh"] == {"value": 6.0, "measured": True}
        assert result["low"]["measured"] is True

    def test_a_gap_skips_a_level_the_model_does_not_offer(self):
        """The ceiling comes from an offered level, so a figure for one the
        model does not expose cannot stand in for one it does."""
        table = _table({"m": {"medium": 2.0, "max": 4.0}})
        result = task_cost("m", ["low", "high", "max"], table)
        assert result["low"] == {"value": 8.0, "measured": False}
        assert result["high"] == {"value": 8.0, "measured": False}

    def test_a_gap_with_nothing_measured_above_is_left_out(self):
        table = _table({"m": {"low": 1.0, "medium": 2.0}})
        assert list(task_cost("m", LADDER, table)) == ["low", "medium"]

    def test_no_offered_level_measured_or_bounded_means_no_field(self):
        table = _table({"m": {"low": 1.0}})
        assert task_cost("m", ["high", "max"], table) is None

    def test_levels_the_model_does_not_offer_are_dropped(self):
        table = _table({"m": {"none": 0.5, "low": 1.0, "max": 4.0}})
        result = task_cost("m", ["low", "max"], table)
        assert list(result) == ["low", "max"]

    def test_a_model_measured_once_carries_only_any(self):
        table = _table({"m": {ANY_EFFORT: 2.0}})
        assert task_cost("m", LADDER, table) == {
            ANY_EFFORT: {"value": 4.0, "measured": True}
        }

    def test_a_model_without_levels_uses_any_alone(self):
        table = _table({"m": {ANY_EFFORT: 2.0}, "n": {"low": 1.0, "max": 4.0}})
        assert task_cost("m", [], table) == {
            ANY_EFFORT: {"value": 4.0, "measured": True}
        }
        assert task_cost("n", [], table) is None

    def test_a_model_absent_from_the_file_gets_nothing(self):
        assert task_cost("not-in-the-file", LADDER, _table({})) is None

    def test_an_unusable_figure_reads_as_unmeasured(self, caplog):
        with caplog.at_level(logging.WARNING, logger=task_costs.__name__):
            table = _table(
                {"m": {"low": "cheap", "medium": 0, "high": True, "max": 4.0}}
            )
        result = task_cost("m", ["low", "medium", "high", "max"], table)
        assert all(not result[e]["measured"] for e in ("low", "medium", "high"))
        assert result["max"] == {"value": 8.0, "measured": True}
        assert caplog.text


class TestBounds:
    def test_a_bounded_model_carries_its_sources_figures_times_the_multiple(self):
        table = _table(
            {"m": {"low": 1.0, "max": 4.0}},
            bounds={"fast": {"model": "m", "times": 2}},
        )
        assert task_cost("fast", ["low", "high", "max"], table) == {
            "low": {"value": 4.0, "measured": False},
            "high": {"value": 16.0, "measured": False},
            "max": {"value": 16.0, "measured": False},
        }

    def test_a_measured_figure_wins_over_a_bound(self):
        table = _table(
            {"m": {"max": 4.0}, "fast": {"max": 5.0}},
            bounds={"fast": {"model": "m", "times": 2}},
        )
        assert task_cost("fast", ["max"], table)["max"] == {"value": 10.0, "measured": True}

    @pytest.mark.parametrize(
        "bound",
        [
            {"model": "nowhere", "times": 2},
            {"model": "chained", "times": 2},
            {"model": "m", "times": 0},
            {"model": "m"},
            {"model": ["m"], "times": 2},
            "m",
        ],
        ids=["unknown-model", "chain", "zero-multiple", "no-multiple", "model-not-a-name", "not-an-object"],
    )
    def test_an_unusable_bound_drops_only_itself(self, bound, caplog):
        with caplog.at_level(logging.WARNING, logger=task_costs.__name__):
            table = _table(
                {"m": {"max": 4.0}},
                bounds={"fast": bound, "chained": {"model": "m", "times": 2}},
            )
        assert task_cost("fast", ["max"], table) is None
        assert task_cost("chained", ["max"], table)["max"]["value"] == 16.0
        assert caplog.text


class TestUnusableFile:
    @pytest.mark.parametrize(
        "baseline",
        [("missing", "medium"), ("base", "xhigh"), ("base", None)],
    )
    def test_a_baseline_without_a_figure_drops_everything(self, baseline, caplog):
        with caplog.at_level(logging.WARNING, logger=task_costs.__name__):
            assert _table({"m": {"low": 1.0}}, baseline=baseline) is None
        assert "baseline" in caplog.text

    def test_a_zero_baseline_drops_everything(self):
        assert parse_task_costs(
            {
                "baseline": {"model": "base", "effort": "medium"},
                "models": {"base": {"medium": 0}},
            }
        ) is None

    @pytest.mark.parametrize(
        "data", [[], {"models": {}}, {"baseline": {}, "models": []}, "text"]
    )
    def test_a_file_of_the_wrong_shape_drops_everything(self, data):
        assert parse_task_costs(data) is None

    def test_a_missing_file_is_a_warning(self, isolated_file, caplog):
        with caplog.at_level(logging.WARNING, logger=task_costs.__name__):
            assert task_cost("claude-sonnet-5-5", LADDER) is None
        assert str(isolated_file) in caplog.text

    def test_malformed_json_is_a_warning(self, isolated_file, caplog):
        isolated_file.write_text("{not json")
        with caplog.at_level(logging.WARNING, logger=task_costs.__name__):
            assert task_cost("claude-sonnet-5-5", LADDER) is None
        assert caplog.text

    @pytest.mark.parametrize(
        "contents",
        [
            None,
            "{not json",
            json.dumps(
                {
                    "baseline": {"model": "claude-sonnet-5-5", "effort": "nope"},
                    "models": {"claude-sonnet-5-5": {"medium": 0.5}},
                }
            ),
        ],
        ids=["missing", "malformed", "bad-baseline"],
    )
    def test_the_model_list_still_builds(self, isolated_file, contents):
        if contents is not None:
            isolated_file.write_text(contents)
        metadata = ModelConfig().get_model_metadata()
        assert metadata
        assert not any("task_cost" in entry for entry in metadata.values())


class TestBaseline:
    _USABLE = {
        "baseline": {"model": "base", "effort": "medium"},
        "models": {"base": {"medium": 0.5}},
    }

    def test_a_usable_file_names_its_baseline(self, isolated_file):
        assert parse_task_costs(self._USABLE).baseline == {"model": "base", "effort": "medium"}
        isolated_file.write_text(json.dumps(self._USABLE))
        assert task_cost_baseline() == {"model": "base", "effort": "medium"}

    def test_the_shipped_file_names_a_baseline_that_reads_as_one(self):
        task_costs._shipped.cache_clear()
        base = task_cost_baseline()
        shipped = json.loads(task_costs.TASK_COSTS_PATH.read_text())
        assert base == shipped["baseline"]
        assert task_cost(base["model"], [base["effort"]])[base["effort"]]["value"] == 1.0

    @pytest.mark.parametrize(
        "contents",
        [
            None,
            "{not json",
            json.dumps({"baseline": {"model": "m", "effort": "low"}, "models": {"m": {"low": 0}}}),
            json.dumps({"baseline": {"model": "m", "effort": "nope"}, "models": {"m": {"low": 1.0}}}),
            json.dumps({"baseline": "m", "models": {"m": {"low": 1.0}}}),
            json.dumps([]),
        ],
        ids=["missing", "malformed", "zero-figure", "unknown-level", "wrong-shape", "not-an-object"],
    )
    def test_an_unusable_file_has_no_baseline(self, isolated_file, contents):
        if contents is not None:
            isolated_file.write_text(contents)
        assert task_cost_baseline() is None


class TestShippedFile:
    @pytest.fixture(scope="class")
    def metadata(self) -> dict:
        task_costs._shipped.cache_clear()
        return ModelConfig().get_model_metadata()

    @pytest.fixture(scope="class")
    def shipped(self) -> dict:
        return json.loads(task_costs.TASK_COSTS_PATH.read_text())

    def test_the_baseline_reads_as_one(self, metadata, shipped):
        base = shipped["baseline"]
        assert metadata[base["model"]]["task_cost"][base["effort"]] == {
            "value": 1.0,
            "measured": True,
        }

    def test_an_unmeasured_level_carries_the_level_above(self, metadata, shipped):
        checked = 0
        for key, entry in metadata.items():
            costs = entry.get("task_cost")
            if not costs or ANY_EFFORT in costs or key in shipped.get("bounds", {}):
                continue
            ceiling = None
            for effort in reversed(entry["reasoning_efforts"]):
                if effort not in costs:
                    continue
                if costs[effort]["measured"]:
                    ceiling = costs[effort]["value"]
                else:
                    assert ceiling is not None, (key, effort)
                    assert costs[effort]["value"] == ceiling, (key, effort)
                    checked += 1
        if not checked:
            pytest.skip("the shipped figures measure every level; no gap to fill")

    def test_keys_follow_each_models_ladder(self, metadata):
        for key, entry in metadata.items():
            if "task_cost" not in entry or ANY_EFFORT in entry["task_cost"]:
                continue
            ladder = entry["reasoning_efforts"]
            shown = list(entry["task_cost"])
            assert shown == [e for e in ladder if e in shown], key

    def test_a_model_measured_once_has_no_levels(self, metadata, shipped):
        once = [k for k, v in shipped["models"].items() if list(v) == [ANY_EFFORT]]
        assert once, "Expected at least one model measured without a level"
        for key in once:
            if key in metadata:
                assert list(metadata[key]["task_cost"]) == [ANY_EFFORT], key

    def test_only_keys_in_the_file_carry_the_field(self, metadata, shipped):
        """A regional variant shares a model with a key in the file but is
        priced on its own terms, so it must not borrow its figures. A
        subscription does, as the same model on the same tokens, so it may
        carry the field when its keyed twin is in the file (pinned in
        test_model_rating_metadata)."""
        mc = ModelConfig()
        in_file = {*shipped["models"], *shipped.get("bounds", {})}
        for key, entry in metadata.items():
            if "task_cost" not in entry:
                continue
            if entry["access_type"] == "oauth":
                key = mc._task_cost_key(
                    key, mc.llm_config[key], mc.get_provider_info(entry["provider"])
                )
            assert key in in_file, key

    def test_every_model_with_its_own_source_has_figures(self, shipped):
        """A misspelled key would leave its figures attributed to the main source."""
        assert set(shipped.get("model_sources", {})) <= set(shipped["models"])

    def test_every_key_in_the_file_is_a_visible_model(self, metadata, shipped):
        """A misspelled key would silently show nothing anywhere."""
        keys = [*shipped["models"], *shipped.get("bounds", {})]
        missing = [k for k in keys if k not in metadata]
        assert not missing, missing

    def test_a_bounded_model_is_unmeasured_at_every_level(self, metadata, shipped):
        for key in shipped.get("bounds", {}):
            costs = metadata[key]["task_cost"]
            assert costs and not any(c["measured"] for c in costs.values()), key

    def test_every_bound_covers_each_rate_of_the_bounded_model(self, shipped):
        """The multiple is what makes the figure an upper bound: no rate the
        bounded model bills may exceed it times the measured model's rate, or
        a repricing turns "at most" into an understatement."""
        config = ModelConfig()

        def rates(key):
            info = config.get_model_config(key)
            pricing = find_model_pricing(info["model_id"], provider=info["provider"])
            assert pricing, key
            assert "input_tiers" not in pricing and "output_tiers" not in pricing, (
                f"{key}: a bound over tiered pricing needs a rule for each tier"
            )
            return {k: v for k, v in pricing.items() if isinstance(v, (int, float))}

        for key, bound in shipped.get("bounds", {}).items():
            mine, theirs = rates(key), rates(bound["model"])
            for field, rate in mine.items():
                assert field in theirs, (key, field)
                assert rate <= bound["times"] * theirs[field], (key, field)
