"""Tests for src/utils/tracking/infrastructure_costs.py — merged pricing table.

Capability pricing comes from src/tools/manifest/web_providers.json (per
level); providers.json `infrastructure_pricing` only carries non-web
leftovers. Tests assert key structure and relative behavior, not specific
credit numbers (tunable manifest data).
"""

from src.tools.web.manifest import (
    CAPABILITY_CRAWL,
    CAPABILITY_FETCH,
    CAPABILITY_SEARCH,
    get_capability,
    providers_with_capability,
)
from src.utils.tracking.infrastructure_costs import (
    INFRASTRUCTURE_PRICING,
    _map_tool_to_service,
    calculate_infrastructure_credits,
    format_infrastructure_usage,
)


def _web_caps():
    return [
        spec.capability(capability)
        for capability in (CAPABILITY_SEARCH, CAPABILITY_FETCH, CAPABILITY_CRAWL)
        for spec in providers_with_capability(capability).values()
    ]


class TestPricingTable:
    def test_every_level_registers_under_its_tracking_key(self):
        for cap in _web_caps():
            for level in cap.levels:
                key = cap.tracking_key(level)
                assert INFRASTRUCTURE_PRICING[key]["credits_per_use"] == level.credits

    def test_bare_key_prices_at_default_depth(self):
        """Legacy/unqualified usage counts bill at the capability's default level."""
        for cap in _web_caps():
            bare = INFRASTRUCTURE_PRICING[cap.tracking_name]
            assert bare["credits_per_use"] == cap.default_level_spec.credits

    def test_auxiliary_tools_priced(self):
        assert INFRASTRUCTURE_PRICING["TavilySearchImages"]["credits_per_use"] > 0


class TestServiceMapping:
    def test_depth_suffix_stripped(self):
        assert _map_tool_to_service("TavilySearchTool:deep") == "tavily_search"
        assert _map_tool_to_service("TavilySearchTool:standard") == "tavily_search"

    def test_bare_key_unchanged(self):
        assert _map_tool_to_service("TavilySearchTool") == "tavily_search"
        assert _map_tool_to_service("SerperSearchTool") == "serper_search"

    def test_unknown_tool_lowercases_base(self):
        assert _map_tool_to_service("FutureTool:deep") == "futuretool"


class TestCalculateCredits:
    def test_qualified_key_bills_that_depth(self):
        tavily = get_capability("tavily", CAPABILITY_SEARCH)
        deep = tavily.level("deep")
        result = calculate_infrastructure_credits({"TavilySearchTool:deep": 3})
        assert result["total_credits"] == 3 * deep.credits
        assert result["services"]["tavily_search"]["usage_count"] == 3

    def test_deep_costs_more_than_standard(self):
        """The per-depth pricing fix: deep and standard bill differently."""
        deep = calculate_infrastructure_credits({"TavilySearchTool:deep": 1})
        standard = calculate_infrastructure_credits({"TavilySearchTool:standard": 1})
        assert deep["total_credits"] > standard["total_credits"]

    def test_mixed_depths_aggregate_per_service(self):
        """Qualified + bare keys share a service entry, summed not overwritten."""
        result = calculate_infrastructure_credits(
            {"TavilySearchTool:deep": 2, "TavilySearchTool": 1}
        )
        entry = result["services"]["tavily_search"]
        assert entry["usage_count"] == 3
        tavily = get_capability("tavily", CAPABILITY_SEARCH)
        expected = (
            2 * tavily.level("deep").credits
            + 1 * tavily.default_level_spec.credits
        )
        assert result["total_credits"] == expected

    def test_unknown_tool_skipped(self):
        result = calculate_infrastructure_credits({"NoSuchTool": 5})
        assert result["total_credits"] == 0.0
        assert result["services"] == {}

    def test_fetch_mixed_levels_bill_qualified_rates(self):
        """Stealth and standard fetch bill at their own manifest rates,
        aggregated under one service entry."""
        firecrawl = get_capability("firecrawl", CAPABILITY_FETCH)
        result = calculate_infrastructure_credits(
            {"FirecrawlFetchTool:stealth": 1, "FirecrawlFetchTool:standard": 2}
        )
        expected = (
            firecrawl.level("stealth").credits
            + 2 * firecrawl.level("standard").credits
        )
        assert result["total_credits"] == expected
        assert result["services"]["firecrawl_fetch"]["usage_count"] == 3

    def test_crawl_bare_key_bills_default_level(self):
        crawl = get_capability("firecrawl", CAPABILITY_CRAWL)
        result = calculate_infrastructure_credits({"FirecrawlCrawlTool": 4})
        assert result["total_credits"] == 4 * crawl.default_level_spec.credits

    def test_mixed_rates_omit_credits_per_use(self):
        """When depths with different rates share a service entry, the
        per-use figure is omitted rather than reporting the last key's rate."""
        result = calculate_infrastructure_credits(
            {"TavilySearchTool:deep": 2, "TavilySearchTool:standard": 1}
        )
        entry = result["services"]["tavily_search"]
        assert "credits_per_use" not in entry
        # Order-independent: reversed insertion omits it too.
        reversed_result = calculate_infrastructure_credits(
            {"TavilySearchTool:standard": 1, "TavilySearchTool:deep": 2}
        )
        assert "credits_per_use" not in reversed_result["services"]["tavily_search"]
        assert reversed_result["total_credits"] == result["total_credits"]

    def test_uniform_rate_keeps_credits_per_use(self):
        """Same-rate keys aggregating into one service keep the figure."""
        tavily = get_capability("tavily", CAPABILITY_SEARCH)
        result = calculate_infrastructure_credits({"TavilySearchTool:deep": 2})
        entry = result["services"]["tavily_search"]
        assert entry["credits_per_use"] == tavily.level("deep").credits


class TestFormatUsage:
    def test_type_is_depth_name(self):
        formatted = format_infrastructure_usage({"TavilySearchTool:deep": 2})
        assert formatted["services"]["tavily_search"] == {"count": 2, "type": "deep"}

    def test_counts_aggregate_across_keys(self):
        formatted = format_infrastructure_usage(
            {"TavilySearchTool:deep": 2, "TavilySearchTool:standard": 1}
        )
        assert formatted["services"]["tavily_search"]["count"] == 3
        # Dominant count's depth wins the type field.
        assert formatted["services"]["tavily_search"]["type"] == "deep"

    def test_dominant_type_is_insertion_order_independent(self):
        a = format_infrastructure_usage(
            {"TavilySearchTool:deep": 2, "TavilySearchTool:standard": 1}
        )
        b = format_infrastructure_usage(
            {"TavilySearchTool:standard": 1, "TavilySearchTool:deep": 2}
        )
        assert (
            a["services"]["tavily_search"]["type"]
            == b["services"]["tavily_search"]["type"]
            == "deep"
        )

    def test_dominant_type_three_way_collision(self):
        formatted = format_infrastructure_usage(
            {
                "TavilySearchTool:deep": 2,
                "TavilySearchTool:standard": 1,
                "TavilySearchTool:fast": 4,
            }
        )
        entry = formatted["services"]["tavily_search"]
        assert entry["count"] == 7
        assert entry["type"] == "fast"

    def test_dominant_type_wins_when_not_last(self):
        """The accumulated total of earlier depths must not outvote the
        depth with the largest individual count (here: 1+2 >= 3 yet the
        3-count depth's type must win)."""
        formatted = format_infrastructure_usage(
            {
                "TavilySearchTool:ultra_fast": 1,
                "TavilySearchTool:fast": 2,
                "TavilySearchTool:standard": 3,
            }
        )
        entry = formatted["services"]["tavily_search"]
        assert entry["count"] == 6
        assert entry["type"] == "standard"

    def test_auxiliary_tools_keep_type_field(self):
        """Auxiliary rows keep their pre-migration JSONB type values."""
        formatted = format_infrastructure_usage({"TavilySearchImages": 1})
        assert formatted["services"]["tavily_images"]["type"] == "advanced"
