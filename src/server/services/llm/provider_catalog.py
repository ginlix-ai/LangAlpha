"""What the setup wizard and the key picker know about providers, published
by ``GET /api/v1/models``. Built once per process: the manifest is static."""

from __future__ import annotations

import functools


def _base_url(info: dict) -> str | None:
    from src.config.env import HOST_IP

    return (info.get("base_url") or "").replace("{HOST_IP}", HOST_IP) or None


@functools.lru_cache(maxsize=1)
def provider_catalog() -> list[dict]:
    """Wizard-visible providers.

    A provider is wizard-visible if it has no parent_provider (a root brand),
    or its access_type differs from its parent's. Region variants (same
    access_type, different region) are attached as ``region_variants`` on the
    parent's entry so the wizard can show an inline region toggle.
    """
    from src.config.settings import HOST_MODE
    from src.llms.llm import LLM

    config = LLM.get_model_config()
    flat = config.flat_providers

    # Another API of a connection, saved under by a model served on it.
    routes_by_parent: dict[str, list[str]] = {}
    for key, info in flat.items():
        if not info.get("platform") and config.connection_of(key) != key:
            routes_by_parent.setdefault(config.connection_of(key), []).append(key)

    region_variants_by_parent: dict[str, list[dict]] = {}
    for key, info in flat.items():
        if info.get("platform"):
            continue
        parent_key = info.get("parent_provider")
        if not parent_key:
            continue
        parent_info = flat.get(parent_key, {})
        if (
            info.get("access_type", "api_key") == parent_info.get("access_type", "api_key")
            and info.get("region") != parent_info.get("region")
        ):
            region_variants_by_parent.setdefault(parent_key, []).append({
                "provider": key,
                "display_name": config.get_display_name(key),
                "region": info.get("region", "intl"),
                "sdk": info.get("sdk"),
                "base_url": _base_url(info),
                "use_response_api": info.get("use_response_api", False),
                "models_from": config.models_from(key),
            })

    def _entry(k: str, inf: dict, brand: str) -> dict:
        group = inf.get("group", k)
        entry = {
            "provider": k,
            "display_name": config.get_display_name(k),
            "access_type": inf.get("access_type", "api_key"),
            "brand_key": brand,
            # Eligible unless a provider says otherwise, the same default
            # ``get_byok_eligible_providers`` reads it with. Defaulting to False
            # here reported ten real providers as ineligible, the local three
            # among them, while the resolver was happily accepting keys for them.
            "byok_eligible": inf.get("byok_eligible", True),
            "region": inf.get("region", "intl"),
            "sdk": inf.get("sdk"),
            "base_url": _base_url(inf),
            "use_response_api": inf.get("use_response_api", False),
            "dynamic_models": inf.get("dynamic_models", False),
            # Whose manifest models a key saved here offers (``models_from``):
            # the provider's own, else a plan's brand, or a plan region's plan.
            "models_from": config.models_from(k),
            "group": group,
            "group_display_name": config.get_display_name(group),
            # What kind of provider the group is (closed_source, open_source,
            # inference), which the key picker sorts providers by.
            "category": inf.get("category"),
        }
        rv = region_variants_by_parent.get(k)
        if rv:
            entry["region_variants"] = rv
        if routes_by_parent.get(k):
            entry["routes"] = routes_by_parent[k]
        return entry

    catalog = []
    for key, info in flat.items():
        if info.get("platform"):
            continue
        # Local providers (Ollama, LM Studio, vLLM) are OSS-only
        if info.get("access_type") == "local" and HOST_MODE == "platform":
            continue
        parent_key = info.get("parent_provider")
        if not parent_key:
            catalog.append(_entry(key, info, key))
        elif info.get("access_type", "api_key") != flat.get(parent_key, {}).get("access_type", "api_key"):
            catalog.append(_entry(key, info, parent_key))
    return catalog


@functools.lru_cache(maxsize=1)
def key_candidates() -> dict[str, dict]:
    """Where a turn looks for each provider's stored key, so the client can
    tell which saved models the user's own keys serve without re-deriving the
    walk: ``slugs`` in ``byok_candidates`` order, ``shared`` the ones whose
    key the vendor issues as one with the provider's. A key under another slug
    serves it only when saved with a base URL; a custom model takes any.
    """
    from src.llms.llm import LLM
    from src.server.services.llm.byok import byok_candidates

    mc = LLM.get_model_config()
    out = {}
    for key, info in mc.flat_providers.items():
        if info.get("platform") or info.get("access_type") == "oauth":
            continue
        slugs = byok_candidates(key, mc)
        out[key] = {"slugs": slugs, "shared": [s for s in slugs if mc.shares_api_key(key, s)]}
    return out
