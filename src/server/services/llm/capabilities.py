"""Gating the user's search and crawl preferences behind their plan tier.

Nothing here resolves a model, which is why it is not in ``config``.
"""

from __future__ import annotations

from . import logger


async def gate_capabilities(config, user_id: str, model_pref: dict) -> None:
    """Apply the user's search and crawl preferences, each behind its manifest tier.

    Resolve time is the enforcement point, so a stale preference left by a
    downgraded plan is ignored at consumption rather than trusted from storage.
    """
    from src.config.settings import HOST_MODE

    is_platform = HOST_MODE == "platform"
    user_tier: int | None = None

    async def _tier_permits(min_tier: int) -> bool:
        """One polarity for every capability gate: OSS/BYOK deployments are
        never gated; platform users must meet the manifest min tier."""
        nonlocal user_tier
        if not is_platform:
            return True
        if user_tier is None:
            # ChatAuthResult.access_tier is -1 for BYOK/OAuth users, so the
            # true tier is resolved here — lazily, Redis-cached, once per turn.
            from src.server.dependencies.usage_limits import _fetch_platform_tier

            user_tier = await _fetch_platform_tier(user_id)
        return user_tier >= min_tier

    pref_search_provider = model_pref.get("search_provider")
    pref_search_depth = model_pref.get("search_depth")
    if pref_search_provider or pref_search_depth:
        from src.tools.web.manifest import (
            CAPABILITY_SEARCH,
            get_capability,
            resolve_min_tier,
        )

        if pref_search_provider:
            search_cap = (
                get_capability(pref_search_provider, CAPABILITY_SEARCH)
                if isinstance(pref_search_provider, str)
                else None
            )
            if search_cap is None:
                logger.warning(
                    f"[CHAT] Ignoring unknown search_provider preference: {pref_search_provider!r}"
                )
            elif await _tier_permits(resolve_min_tier(search_cap)):
                config.search_api = pref_search_provider
                logger.debug(f"[CHAT] Using search_provider: {pref_search_provider}")
            else:
                logger.debug(
                    f"[CHAT] search_provider pref ignored "
                    f"(tier below {resolve_min_tier(search_cap)})"
                )

        if pref_search_depth:
            # Depth names are provider-scoped: validate against the EFFECTIVE
            # provider (post-provider-resolution), so a stale depth left over
            # from another provider degrades to that provider's default.
            eff_cap = get_capability(config.search_api, CAPABILITY_SEARCH)
            depth_spec = (
                eff_cap.level(pref_search_depth)
                if eff_cap is not None and isinstance(pref_search_depth, str)
                else None
            )
            if depth_spec is None:
                logger.debug(
                    f"[CHAT] search_depth pref {pref_search_depth!r} not offered by "
                    f"provider {config.search_api!r}; using provider default"
                )
            elif await _tier_permits(resolve_min_tier(depth_spec)):
                config.search_depth = depth_spec.name
                logger.debug(f"[CHAT] Using search_depth: {depth_spec.name}")
            else:
                logger.debug(
                    f"[CHAT] search_depth pref ignored (tier below {resolve_min_tier(depth_spec)})"
                )

    # Crawl tools (WebCrawl/WebMap): experimental opt-in via the site_crawl
    # feature; for opted-in users the crawl capability's manifest min_tier
    # still applies (_tier_permits keeps OSS deployments ungated). The tool
    # factory self-skips when the provider key is unset.
    if config.feature_enabled("site_crawl"):
        from src.config.tool_settings import get_crawl_provider
        from src.tools.web.manifest import CAPABILITY_CRAWL, get_capability, resolve_min_tier

        crawl_cap = get_capability(get_crawl_provider(), CAPABILITY_CRAWL)
        if crawl_cap is not None and not await _tier_permits(resolve_min_tier(crawl_cap)):
            config.features["site_crawl"] = False
            logger.debug(f"[CHAT] crawl tools disabled (tier below {resolve_min_tier(crawl_cap)})")


