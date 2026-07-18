"""
Infrastructure cost calculation and credit conversion utilities.

This module provides functions to:
1. Track infrastructure usage (external paid services)
2. Calculate infrastructure costs based on usage
3. Convert costs to credits for unified billing

Pricing is merged from two manifests at module initialization:
- src/tools/manifest/web_providers.json — web-search providers (per depth
  level, keyed "TrackingName:depth" plus a bare "TrackingName" legacy key),
  fetch capabilities (per fetched URL, same key scheme), and auxiliary
  search tools (images).
- src/llms/manifest/providers.json `infrastructure_pricing` — any remaining
  non-search infrastructure entries. Web-manifest keys win on collision.

Free tools (DuckDuckGo, Arxiv) and internal operations (cache, storage, filesystem)
are not charged and will result in 0 credits.

Usage:
    tool_usage = {"TavilySearchTool": 5}
    result = calculate_infrastructure_credits(tool_usage)
    # Returns: {"total_credits": 80.0, "services": {...}}
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_legacy_pricing_from_manifest() -> Dict[str, Any]:
    """
    Load non-search infrastructure pricing from providers.json.

    An absent or empty `infrastructure_pricing` section is fine (search
    pricing has moved to the search manifest); a missing or unparseable
    file is still a loud startup failure.
    """
    import json

    # Get manifest path relative to this file
    manifest_path = Path(__file__).parent.parent.parent / "llms" / "manifest" / "providers.json"

    if not manifest_path.exists():
        raise RuntimeError(
            f"Infrastructure pricing manifest not found at {manifest_path}. "
            f"Cannot initialize pricing configuration."
        )

    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
    except Exception as e:
        raise RuntimeError(
            f"Failed to load infrastructure pricing from {manifest_path}: {e}"
        )

    return manifest.get("infrastructure_pricing") or {}


def _build_pricing_table() -> Dict[str, Any]:
    """
    Merge web-manifest pricing with any remaining legacy entries.

    Every capability level registers under ``CapabilitySpec.tracking_key``
    (the same authority dispatchers record usage through), plus a bare key
    priced at the capability's default level for legacy/unqualified usage
    counts. Usage counts are in the verb's natural unit (calls, URLs, or
    delivered pages), so ``level.credits`` is always the per-count price.
    The manifest wins over legacy providers.json entries on key collision.
    """
    from src.tools.web.manifest import get_auxiliary_pricing, get_web_providers

    pricing: Dict[str, Any] = dict(_load_legacy_pricing_from_manifest())

    for provider in get_web_providers().values():
        for cap in provider.capabilities.values():
            for level in cap.levels:
                pricing[cap.tracking_key(level)] = {
                    "credits_per_use": level.credits,
                    "search_type": level.name,
                }
            default_level = cap.default_level_spec
            pricing[cap.tracking_name] = {
                "credits_per_use": default_level.credits,
                "search_type": default_level.name,
            }

    pricing.update(get_auxiliary_pricing())

    logger.info(
        f"Loaded infrastructure pricing: {len(pricing)} entries "
        f"(web manifest + legacy providers.json)"
    )
    return pricing


# Load pricing from manifests at module import (single source of truth)
INFRASTRUCTURE_PRICING = _build_pricing_table()

# Service name mapping (tool class names → user-friendly service names)
TOOL_TO_SERVICE_MAPPING = {
    "TavilySearchTool": "tavily_search",
    "TavilySearchImages": "tavily_images",
    "BochaSearchTool": "bocha_search",
    "SerperSearchTool": "serper_search",
    "ExaSearchTool": "exa_search",
    "ParallelSearchTool": "parallel_search",
    "ExaFetchTool": "exa_fetch",
    "ParallelFetchTool": "parallel_fetch",
    "FirecrawlFetchTool": "firecrawl_fetch",
    "TavilyFetchTool": "tavily_fetch",
    "WebFetchTool": "web_fetch",
    "FirecrawlCrawlTool": "firecrawl_crawl",
    "FirecrawlMapTool": "firecrawl_map",
    "TavilyResearchTool": "tavily_research",
    "ExaResearchTool": "exa_research",
    "ParallelResearchTool": "parallel_research",
}


def calculate_infrastructure_credits(
    tool_usage: Dict[str, int],
    pricing_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calculate infrastructure credits from tool usage counts.

    Args:
        tool_usage: Dict mapping tool names to usage counts
            Example: {"TavilySearchTool": 5, "cache_operations": 1000}
        pricing_config: Optional custom pricing config (defaults to INFRASTRUCTURE_PRICING)

    Returns:
        Dict with structure:
        {
            "total_credits": float,
            "services": {
                "tavily_search": {
                    "usage_count": 5,
                    "credits_per_use": 2,
                    "total_credits": 10
                },
                ...
            }
        }
    """
    if pricing_config is None:
        pricing_config = INFRASTRUCTURE_PRICING

    total_credits = 0.0
    services = {}

    for tool_name, count in tool_usage.items():
        if count <= 0:
            continue

        pricing = pricing_config.get(tool_name)
        if not pricing:
            logger.warning(
                f"[InfrastructureCosts] No pricing found for tool: {tool_name}. "
                f"Skipping credit calculation."
            )
            continue

        # Calculate credits based on pricing type
        if "credits_per_use" in pricing:
            # Per-use pricing (e.g., Tavily search)
            credits_per_use = pricing["credits_per_use"]
            tool_credits = count * credits_per_use
        elif "credits_per_1k_ops" in pricing:
            # Per-1k-ops pricing (e.g., cache operations)
            credits_per_1k = pricing["credits_per_1k_ops"]
            tool_credits = (count / 1000.0) * credits_per_1k
        elif "credits_per_op" in pricing:
            # Per-op pricing (e.g., filesystem operations)
            credits_per_op = pricing["credits_per_op"]
            tool_credits = count * credits_per_op
        else:
            logger.warning(
                f"[InfrastructureCosts] Unknown pricing format for {tool_name}. "
                f"Skipping."
            )
            continue

        total_credits += tool_credits

        # Map tool name to service name. Depth-qualified keys share a service
        # with the bare key, so accumulate instead of overwriting.
        service_name = _map_tool_to_service(tool_name)
        prior = services.get(service_name, {})

        # Build service entry
        service_entry = {
            "usage_count": count + prior.get("usage_count", 0),
            "total_credits": round(tool_credits + prior.get("total_credits", 0.0), 6)
        }

        # Add pricing details for transparency. When depth-qualified keys with
        # different rates land on one service, omit the per-use figure rather
        # than report whichever key iterated last.
        if "credits_per_use" in pricing:
            if prior.get("usage_count", 0) == 0:
                service_entry["credits_per_use"] = pricing["credits_per_use"]
            elif prior.get("credits_per_use") == pricing["credits_per_use"]:
                service_entry["credits_per_use"] = pricing["credits_per_use"]
        elif "credits_per_1k_ops" in pricing:
            service_entry["credits_per_1k_ops"] = pricing["credits_per_1k_ops"]
        elif "credits_per_op" in pricing:
            service_entry["credits_per_op"] = pricing["credits_per_op"]

        services[service_name] = service_entry

    return {
        "total_credits": round(total_credits, 6),
        "services": services
    }


def _map_tool_to_service(tool_name: str) -> str:
    """
    Map tool class names to user-friendly service names.

    Depth-qualified tracking keys ("TavilySearchTool:deep") map to the same
    service as their bare base key, preserving analytics continuity.

    Args:
        tool_name: Tool class name (e.g., "TavilySearchTool")

    Returns:
        Service name (e.g., "tavily_search")
    """
    base_name = tool_name.split(":", 1)[0]
    return TOOL_TO_SERVICE_MAPPING.get(base_name, base_name.lower())




def format_infrastructure_usage(tool_usage: Dict[str, int]) -> Dict[str, Any]:
    """
    Format tool usage counts into a structured JSONB format for database storage.

    Args:
        tool_usage: Dict mapping tool names to usage counts

    Returns:
        Structured dict for infrastructure_usage JSONB column:
        {
            "services": {
                "tavily_search": {"count": 5, "type": "advanced"},
                "cache": {"count": 1000},
                ...
            }
        }
    """
    services = {}
    # Per service, the largest single key's count among type-carrying keys.
    # Comparing against the accumulated total instead would let earlier
    # depths' running sum outvote the actually-dominant depth.
    typed_max: Dict[str, int] = {}

    for tool_name, count in tool_usage.items():
        if count <= 0:
            continue

        service_name = _map_tool_to_service(tool_name)
        pricing = INFRASTRUCTURE_PRICING.get(tool_name, {})

        prior = services.get(service_name, {})
        service_entry = {"count": count + prior.get("count", 0)}

        # Add metadata from pricing (the depth name for search providers).
        # On the rare multi-depth collision, the largest individual count's
        # type wins regardless of iteration order.
        if "search_type" in pricing and count > typed_max.get(service_name, 0):
            service_entry["type"] = pricing["search_type"]
            typed_max[service_name] = count
        elif "type" in prior:
            service_entry["type"] = prior["type"]

        services[service_name] = service_entry

    return {"services": services}


# Example usage
if __name__ == "__main__":
    # Example tool usage
    tool_usage = {
        "TavilySearchTool": 5,
        "technical_analysis": 1,
        "cache_operations": 2500,
        "filesystem_operations": 10
    }

    # Calculate credits
    result = calculate_infrastructure_credits(tool_usage)

    print("Infrastructure Credits Calculation:")
    print(f"Total Credits: {result['total_credits']}")
    print("\nBreakdown by Service:")
    for service_name, service_data in result['services'].items():
        print(f"  {service_name}: {service_data}")

    # Format for database storage
    formatted = format_infrastructure_usage(tool_usage)
    print("\nFormatted for database storage:")
    import json
    print(json.dumps(formatted, indent=2))
