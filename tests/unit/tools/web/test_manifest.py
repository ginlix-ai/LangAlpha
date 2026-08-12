"""Tests for src/tools/web/manifest.py — v2 loading, validation, tier resolution.

Per project convention, these test structure and fallback *behavior*, not
specific tier/credit numbers (tunable manifest data).
"""

import pytest

import src.tools.web.manifest as wm
from src.tools.web.manifest import (
    CAPABILITY_FETCH,
    CAPABILITY_SEARCH,
    get_capability,
    get_web_provider_spec,
    get_web_providers,
    providers_with_capability,
    resolve_min_tier,
)


class TestManifestLoading:
    def test_loads_known_providers(self):
        providers = get_web_providers()
        assert {"tavily", "serper", "bocha", "exa", "parallel", "firecrawl", "inhouse"} <= set(
            providers
        )

    def test_search_capable_providers(self):
        assert {"tavily", "serper", "bocha", "exa", "parallel"} == set(
            providers_with_capability(CAPABILITY_SEARCH)
        )

    def test_fetch_capable_providers_include_inhouse(self):
        fetchers = providers_with_capability(CAPABILITY_FETCH)
        assert "inhouse" in fetchers
        assert fetchers["inhouse"].env_key is None

    def test_firecrawl_offers_crawl(self):
        cap = get_web_providers()["firecrawl"].capability("crawl")
        assert cap is not None
        assert cap.tracking_name == "FirecrawlCrawlTool"
        # Tier policy is deployment data, not OSS data: the shipped manifest
        # stays tier-null and gates at the env floor.
        assert cap.min_tier is None
        assert cap.default_level_spec.credits > 0

    def test_every_capability_is_self_describing(self):
        for spec in get_web_providers().values():
            for cap in spec.capabilities.values():
                assert cap.tracking_name
                assert cap.max_batch_size >= 1
                assert cap.level(cap.default_level) is not None
                assert cap.default_level_spec.name == cap.default_level
                for lv in cap.levels:
                    assert lv.display_name
                    assert isinstance(lv.native_params, dict)
                    assert lv.credits >= 0

    def test_tracking_key_qualifies_only_multi_level_capabilities(self):
        """Multi-level caps bill under name:level; single-level caps under the bare name."""
        for spec in get_web_providers().values():
            for cap in spec.capabilities.values():
                for lv in cap.levels:
                    key = cap.tracking_key(lv)
                    if len(cap.levels) > 1:
                        assert key == f"{cap.tracking_name}:{lv.name}"
                    else:
                        assert key == cap.tracking_name

    def test_unknown_provider_is_none(self):
        assert get_web_provider_spec("altavista") is None
        assert get_capability("altavista", CAPABILITY_SEARCH) is None

    def test_unknown_capability_is_none(self):
        assert get_capability("serper", CAPABILITY_FETCH) is None

    def test_levels_arrive_in_manifest_order(self):
        """Tavily's search levels arrive in manifest (fastest → deepest) order."""
        cap = get_capability("tavily", CAPABILITY_SEARCH)
        assert [lv.name for lv in cap.levels] == ["ultra_fast", "fast", "standard", "deep"]

    def test_level_lookup_unknown_returns_none(self):
        cap = get_capability("tavily", CAPABILITY_SEARCH)
        assert cap.level("does-not-exist") is None
        assert cap.level(None) is None

    def test_auxiliary_pricing_present(self):
        aux = wm.get_auxiliary_pricing()
        assert "TavilySearchImages" in aux
        for entry in aux.values():
            assert entry["credits_per_use"] > 0


class TestValidation:
    def _load(self, monkeypatch, bad):
        monkeypatch.setattr(wm, "_load_manifest", lambda: bad)
        wm.get_web_providers.cache_clear()
        try:
            return wm.get_web_providers()
        finally:
            wm.get_web_providers.cache_clear()

    def _provider(self, cap_entry, verb="search"):
        return {
            "providers": {"x": {"capabilities": {verb: cap_entry}}}
        }

    def test_duplicate_level_names_rejected(self, monkeypatch):
        bad = self._provider(
            {
                "tracking_name": "XTool",
                "default_level": "a",
                "levels": [
                    {"name": "a", "credits": 1},
                    {"name": "a", "credits": 2},
                ],
            }
        )
        with pytest.raises(RuntimeError, match="duplicate level names"):
            self._load(monkeypatch, bad)

    def test_default_level_must_exist(self, monkeypatch):
        bad = self._provider(
            {
                "tracking_name": "XTool",
                "default_level": "missing",
                "levels": [{"name": "a", "credits": 1}],
            }
        )
        with pytest.raises(RuntimeError, match="default_level"):
            self._load(monkeypatch, bad)

    def test_max_batch_size_must_be_positive(self, monkeypatch):
        bad = self._provider(
            {
                "tracking_name": "XTool",
                "default_level": "a",
                "max_batch_size": 0,
                "levels": [{"name": "a", "credits": 1}],
            }
        )
        with pytest.raises(RuntimeError, match="max_batch_size"):
            self._load(monkeypatch, bad)

    def test_provider_without_capabilities_rejected(self, monkeypatch):
        bad = {"providers": {"x": {"display_name": "X"}}}
        with pytest.raises(RuntimeError, match="no capabilities"):
            self._load(monkeypatch, bad)

    def test_capability_without_levels_rejected(self, monkeypatch):
        bad = self._provider({"tracking_name": "XTool", "default_level": "a", "levels": []})
        with pytest.raises(RuntimeError, match="no levels"):
            self._load(monkeypatch, bad)


class TestTierResolution:
    def test_null_min_tier_falls_back_to_env_floor(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.SEARCH_PROVIDER_MIN_TIER", 7)
        cap = get_web_providers()["exa"].capability(CAPABILITY_SEARCH)
        if cap.min_tier is None:
            assert resolve_min_tier(cap) == 7
        for lv in cap.levels:
            if lv.min_tier is None:
                assert resolve_min_tier(lv) == 7

    def test_explicit_min_tier_wins_over_floor(self, monkeypatch):
        """An entry's explicit min_tier beats the env floor."""
        monkeypatch.setattr("src.config.settings.SEARCH_PROVIDER_MIN_TIER", 7)
        level = wm.LevelSpec(
            name="deep", display_name="Deep", native_params={}, min_tier=2, credits=16
        )
        assert resolve_min_tier(level) == 2

    def test_explicit_zero_tier_is_respected(self, monkeypatch):
        """min_tier 0 (free) must not be treated as falsy/unset."""
        monkeypatch.setattr("src.config.settings.SEARCH_PROVIDER_MIN_TIER", 7)
        level = wm.LevelSpec(
            name="fast", display_name="Fast", native_params={}, min_tier=0, credits=1
        )
        assert resolve_min_tier(level) == 0
