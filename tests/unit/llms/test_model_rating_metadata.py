"""Editorial rating metadata (speed/intelligence/context), list prices and the
task-cost estimate a list price gives."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.llms.llm import ModelConfig
from src.llms import pricing_utils
from src.llms.pricing_utils import _representative_rate, get_list_rates
from src.llms.task_costs import task_cost_baseline, task_cost_estimate

MANIFEST = Path(__file__).resolve().parents[3] / "src/llms/manifest/models.json"


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST.read_text())


@pytest.fixture(scope="module")
def metadata() -> dict:
    # Every visible row, Claude Code's included, which only a deployment that
    # runs Claude Code turns lists.
    with patch("src.llms.claude_code.rows_available", return_value=True):
        return ModelConfig().get_model_metadata()


class TestRatingMetadataSurfacing:
    def test_authored_ratings_passed_through(self, manifest, metadata):
        """Every visible model that authors speed/intelligence/context in
        models.json must surface them verbatim in get_model_metadata()."""
        rated = {
            k: v for k, v in manifest.items()
            if v.get("visible") and any(f in v for f in ("speed", "intelligence", "context"))
        }
        assert rated, "Expected at least one visible model with rating fields in manifest"
        for key, entry in rated.items():
            for field in ("speed", "intelligence", "context"):
                if field in entry:
                    assert metadata[key][field] == entry[field], (
                        f"{key}: metadata {field} should mirror manifest"
                    )

    def test_unrated_models_omit_fields(self, manifest, metadata):
        """Models without authored ratings must not grow them — the flyout
        renders only the rows that are present."""
        for key, entry in metadata.items():
            for field in ("speed", "intelligence", "context"):
                if field in entry:
                    assert field in manifest[key], (
                        f"{key}: {field} surfaced but not authored in manifest"
                    )

    def test_rating_values_in_range(self, metadata):
        for key, entry in metadata.items():
            for field in ("speed", "intelligence"):
                if field in entry:
                    assert entry[field] in range(1, 6), f"{key}: {field} must be 1-5"
            if "context" in entry:
                assert isinstance(entry["context"], int), f"{key}: context must be an int"
                assert entry["context"] > 0, f"{key}: context must be positive"


class TestDisplayNameSurfacing:
    def test_display_name_mirrors_manifest(self, manifest, metadata):
        """The pickers print display_name in place of the key, so an authored
        name must reach the metadata and an absent one must not be invented."""
        assert any("display_name" in e for e in metadata.values())
        for key, entry in metadata.items():
            assert entry.get("display_name") == manifest[key].get("display_name"), key


class TestListPrice:
    def test_unknown_model_has_no_rates(self):
        assert get_list_rates("no-such-model-xyz") is None

    def test_pricing_is_input_and_output(self, metadata):
        priced = {k: v for k, v in metadata.items() if "pricing" in v}
        assert priced, "Expected at least one model with list pricing"
        for key, entry in priced.items():
            assert set(entry["pricing"]) == {"input", "output"}, key
            assert entry["pricing"]["input"] >= 0 and entry["pricing"]["output"] > 0, key

    def test_oauth_variant_inherits_parent_rates(self, manifest):
        """OAuth variants have no own pricing list; the rates must resolve to
        whatever the parent provider's give (None when the parent has no
        per-token pricing either, e.g. subscription-only providers)."""
        mc = ModelConfig()
        oauth = {
            k: v for k, v in manifest.items()
            if v.get("visible") and v.get("provider", "").endswith("oauth")
        }
        if not oauth:
            pytest.skip("no visible oauth variants in manifest")
        inherited = []
        for key, entry in oauth.items():
            parent = mc.get_provider_info(entry["provider"]).get("parent_provider")
            rates = get_list_rates(entry["model_id"], entry["provider"])
            assert rates == get_list_rates(entry["model_id"], parent), (
                f"{key}: oauth rates should match parent provider resolution"
            )
            if rates is not None:
                inherited.append(key)
        assert inherited, "Expected at least one oauth variant inheriting per-token pricing"

    def test_pricing_without_usable_rates_returns_none(self, monkeypatch):
        """When pricing resolves but has neither flat rate nor tiers (e.g. a
        2d_matrix-only entry), there are no rates — distinct from the 'no
        pricing at all' early return."""
        monkeypatch.setattr(
            pricing_utils, "find_model_pricing",
            lambda name, provider=None: {"pricing_mode": "2d_matrix", "matrix": []},
        )
        assert get_list_rates("placeholder-model", "placeholder-provider") is None

    def test_representative_rate_prefers_flat_then_base_tier(self):
        """Flat rate wins when present; otherwise fall back to the first tier's
        rate; None when neither exists."""
        assert _representative_rate({"input": 2.0}, "input", "input_tiers") == 2.0
        assert _representative_rate(
            {"input_tiers": [{"rate": 0.9}, {"rate": 1.8}]}, "input", "input_tiers"
        ) == 0.9
        assert _representative_rate({}, "input", "input_tiers") is None


class TestTaskCostOnSubscriptions:
    @pytest.mark.parametrize(
        ("subscription", "keyed"),
        [("claude-opus-5-5-oauth", "claude-opus-5-5"), ("gpt-6.2-sol-oauth", "gpt-6.2-sol")],
    )
    def test_a_subscription_shows_its_keyed_twins_task_cost(self, metadata, subscription, keyed):
        """A subscription runs the same model on the same tokens as its keyed
        twin, so the picker weighs both alike."""
        assert metadata[subscription]["task_cost"] == metadata[keyed]["task_cost"]

    def test_a_regional_variant_does_not_borrow_its_parents_figure(self, metadata):
        """Priced on its own terms, so the parent's figure would mislead."""
        assert "task_cost" in metadata["glm-5.3"]
        assert "task_cost" not in metadata["glm-5.3-cn"]


class TestTaskCostEstimate:
    """A model no task was measured on is estimated from its list price, as a
    multiple of the baseline's, so its marks band on the measured scale."""

    def test_a_model_priced_like_the_baseline_reads_as_the_baseline(self):
        """The regression: an estimate once banded a price tier on its own
        scale, so a model at the baseline's price took a mark more than the
        baseline itself."""
        assert task_cost_estimate((3.0, 15.0), (3.0, 15.0)) == pytest.approx(1.0)

    def test_blends_three_input_tokens_to_one_output(self):
        assert task_cost_estimate((1.0, 3.0), (2.0, 6.0)) == pytest.approx(0.5)
        # Input weighs three times output: doubling the input rate adds half.
        assert task_cost_estimate((4.0, 6.0), (2.0, 6.0)) == pytest.approx(1.5)

    @pytest.mark.parametrize("rates, baseline", [
        (None, (3.0, 15.0)), ((3.0, 15.0), None), ((3.0, 15.0), (0.0, 0.0)),
    ])
    def test_none_without_both_prices(self, rates, baseline):
        assert task_cost_estimate(rates, baseline) is None

    def test_only_an_unmeasured_model_carries_one(self, metadata):
        """Measured, bound and estimated stay apart: a figure in `task_cost`
        or an estimate beside it, never both."""
        estimated = {k: v for k, v in metadata.items() if "task_cost_estimate" in v}
        assert estimated, "Expected at least one priced model with no task cost"
        for key, entry in estimated.items():
            assert "task_cost" not in entry, key
            assert "pricing" in entry, key
            assert entry["task_cost_estimate"] > 0, key

    def test_it_is_the_list_price_against_the_baselines(self, metadata):
        baseline = task_cost_baseline()
        assert baseline is not None
        base = metadata[baseline["model"]]["pricing"]
        for key, entry in metadata.items():
            if "task_cost_estimate" not in entry:
                continue
            price = entry["pricing"]
            expected = task_cost_estimate(
                (price["input"], price["output"]), (base["input"], base["output"])
            )
            assert entry["task_cost_estimate"] == pytest.approx(expected, rel=1e-3), key
