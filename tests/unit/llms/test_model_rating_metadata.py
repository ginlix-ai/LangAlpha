"""Editorial rating metadata (speed/intelligence/context) and derived price tier."""

import json
from pathlib import Path

import pytest

from src.llms.llm import ModelConfig
from src.llms import pricing_utils
from src.llms.pricing_utils import PRICE_TIER_BANDS, _representative_rate, get_price_tier

MANIFEST = Path(__file__).resolve().parents[3] / "src/llms/manifest/models.json"


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST.read_text())


@pytest.fixture(scope="module")
def metadata() -> dict:
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


class TestPriceTier:
    def test_metadata_price_is_valid_tier(self, metadata):
        priced = {k: v for k, v in metadata.items() if "price" in v}
        assert priced, "Expected at least one model with derived price tier"
        for key, entry in priced.items():
            assert entry["price"] in range(1, 6), f"{key}: price tier must be 1-5"

    def test_unknown_model_returns_none(self):
        assert get_price_tier("no-such-model-xyz") is None

    def test_oauth_variant_inherits_parent_tier(self, manifest):
        """OAuth variants have no own pricing list; the tier must resolve to
        whatever the parent provider's rates give (None when the parent has no
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
            tier = get_price_tier(entry["model_id"], entry["provider"])
            assert tier == get_price_tier(entry["model_id"], parent), (
                f"{key}: oauth tier should match parent provider resolution"
            )
            if tier is not None:
                inherited.append(key)
        assert inherited, "Expected at least one oauth variant inheriting per-token pricing"

    def test_bands_are_descending_and_cover_zero(self):
        lows = [lo for lo, _ in PRICE_TIER_BANDS]
        assert lows == sorted(lows, reverse=True)
        assert lows[-1] == 0.0

    def test_priced_pricing_without_usable_rates_returns_none(self, monkeypatch):
        """When pricing resolves but has neither flat rate nor tiers (e.g. a
        2d_matrix-only entry), the tier must be None — distinct from the
        'no pricing at all' early return."""
        monkeypatch.setattr(
            pricing_utils, "find_model_pricing",
            lambda name, provider=None: {"pricing_mode": "2d_matrix", "matrix": []},
        )
        assert get_price_tier("placeholder-model", "placeholder-provider") is None

    def test_representative_rate_prefers_flat_then_base_tier(self):
        """Flat rate wins when present; otherwise fall back to the first tier's
        rate; None when neither exists."""
        assert _representative_rate({"input": 2.0}, "input", "input_tiers") == 2.0
        assert _representative_rate(
            {"input_tiers": [{"rate": 0.9}, {"rate": 1.8}]}, "input", "input_tiers"
        ) == 0.9
        assert _representative_rate({}, "input", "input_tiers") is None
