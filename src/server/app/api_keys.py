"""
API Keys and Models Router.

Endpoints:
- GET  /api/v1/users/me/api-keys         — Get BYOK config (masked keys)
- PUT  /api/v1/users/me/api-keys         — Update BYOK config
- DELETE /api/v1/users/me/api-keys/{prov} — Remove one provider key
- POST /api/v1/keys/test                  — Test an API key with a lightweight inference call
- GET  /api/v1/models                     — List available models by provider
- GET  /api/v1/providers/{p}/available-models — What the user's credential can use
"""

import logging
from typing import Dict, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, field_validator

from src.config.env import HOST_IP
from src.server.services.llm import key_probe
from src.server.services.llm.config import deployment_key, deployment_local_provider
from src.server.services.llm.egress import EndpointRefused
from src.server.services.llm.provider_catalog import key_candidates, provider_catalog
from src.server.utils.api import CurrentUserId
from src.utils.cache.throttle import throttle
from src.server.database.api_keys import (
    get_user_api_keys,
    get_base_url_for_provider,
    set_byok_enabled,
    upsert_api_key,
    delete_api_key,
    invalidate_byok_cache,
    key_writes,
    update_base_url,
)
from src.server.services.llm.provider_models import invalidate_available_models

logger = logging.getLogger(__name__)

router = APIRouter(tags=["API Keys"])

# Module-level cache for BYOK-eligible providers (loaded once on first access)
_BYOK_PROVIDERS_CACHE: list[str] | None = None


def _get_supported_providers() -> list[str]:
    """BYOK-eligible connections from the LLM manifest (cached at module level).

    A gateway's route is one API of its connection and takes the gateway's
    key, so it is no connection of its own: listed, a deployment key would
    show as one plan row per API, and a key saved under one would be a second
    key for the same connection.
    """
    global _BYOK_PROVIDERS_CACHE
    if _BYOK_PROVIDERS_CACHE is None:
        from src.llms.llm import ModelConfig

        config = ModelConfig()
        _BYOK_PROVIDERS_CACHE = [p for p in config.get_byok_eligible_providers() if config.connection_of(p) == p]
    return _BYOK_PROVIDERS_CACHE


def _get_provider_info_map() -> dict[str, dict]:
    """Get display names, access_type, and brand_key for BYOK-eligible providers."""
    from src.llms.llm import ModelConfig

    config = ModelConfig()
    info_map = {}
    for p in _get_supported_providers():
        info = config.get_provider_info(p)
        if info.get("platform"):
            continue
        info_map[p] = {
            "display_name": info.get("display_name", p.title()),
            "access_type": info.get("access_type", "api_key"),
            "brand_key": info.get("parent_provider", p),
            "env_key": info.get("env_key"),
            "base_url": info.get("base_url"),
        }
    return info_map


def _mask_key(key: str) -> str:
    """Mask an API key: show first 3 + last 4 chars."""
    if not key or len(key) < 8:
        return "****"
    return f"{key[:3]}...{key[-4:]}"


def _format_response(
    byok_enabled: bool,
    keys: dict,
    base_urls: dict | None = None,
    custom_providers: list | None = None,
) -> dict:
    """Build the public response shape (never exposes full keys).

    ``custom_providers`` is a list of user-defined sub-providers
    (each ``{name, parent_provider}``).  They are appended after the
    built-in providers so the frontend can display them in the same dropdown.
    """
    info_map = _get_provider_info_map()
    base_urls = base_urls or {}
    # In standalone mode (no platform service), server .env keys belong to the
    # user — surface them so the frontend shows the right models.
    providers = []
    # A local server the deployment's default runs on is configured too, at the
    # manifest's address, and named as the server's so it offers no removal.
    served_locally = deployment_local_provider()
    for p in _get_supported_providers():
        raw = keys.get(p)
        pinfo = info_map.get(p, {})
        # Local providers (Ollama, LM Studio, vLLM) are "configured" when a
        # base_url is stored, even without an API key.
        has_base_url = bool(base_urls.get(p))
        has_env = deployment_key(p) is not None or (p == served_locally and not has_base_url)
        providers.append({
            "provider": p,
            "display_name": pinfo.get("display_name", p.title()),
            "access_type": pinfo.get("access_type", "api_key"),
            "brand_key": pinfo.get("brand_key", p),
            "has_key": bool(raw) or has_base_url or has_env,
            "masked_key": _mask_key(raw) if raw else ("(server)" if has_env else None),
            "base_url": base_urls.get(p) or (pinfo.get("base_url") if p == served_locally else None),
            "is_custom": False,
        })

    # Append user-defined sub-providers
    builtin = {row["provider"] for row in providers}
    for cp in custom_providers or []:
        name = cp["name"]
        # One saved before its name became a built-in keeps routing its own
        # models, but the key under the name is the built-in row's: one row.
        if name in builtin:
            continue
        raw = keys.get(name)
        entry = {
            "provider": name,
            "display_name": name,
            "parent_provider": cp["parent_provider"],
            "has_key": bool(raw),
            "masked_key": _mask_key(raw) if raw else None,
            "base_url": base_urls.get(name),
            "is_custom": True,
        }
        if cp.get("use_response_api"):
            entry["use_response_api"] = True
        providers.append(entry)

    providers.extend(_withdrawn_rows(keys, base_urls, listed={p["provider"] for p in providers}))
    return {"byok_enabled": byok_enabled, "providers": providers}


def _withdrawn_rows(keys: dict, base_urls: dict, *, listed: set[str]) -> list[dict]:
    """A key still stored for a provider the manifest no longer has, or for a
    custom provider built on one: nothing can use it, but it is the user's
    secret, so it is listed for them to remove (``DELETE`` takes it). An empty
    manifest is a load failure, not a catalog with nothing in it."""
    from src.llms.llm import ModelConfig

    known = ModelConfig().flat_providers
    if not known:
        return []
    return [
        {
            "provider": slug,
            "display_name": slug,
            "access_type": "api_key",
            "has_key": True,
            "masked_key": _mask_key(raw) if raw else None,
            "base_url": base_urls.get(slug),
            "is_custom": False,
            "withdrawn": True,
        }
        for slug, raw in keys.items()
        if slug not in listed and slug not in known and (raw or base_urls.get(slug))
    ]


# ── BYOK Endpoints ──────────────────────────────────────────────────────


async def _get_custom_providers(user_id: str) -> list:
    """Load user-defined sub-providers from ``custom_providers``.

    Goes through the resolver's merged read because the key moved into
    ``model_preference``: reading ``other_preference`` alone returns empty
    for every migrated user, which drops their custom providers out of this
    response and out of every client that builds a provider list from it.
    """
    from src.server.services.llm.user_models import get_model_preference

    model_pref = await get_model_preference(user_id)
    return model_pref.get("custom_providers") or []


@router.get("/api/v1/users/me/api-keys")
async def get_api_keys(user_id: CurrentUserId):
    """Get user's BYOK configuration (keys are masked)."""
    data = await get_user_api_keys(user_id)
    custom_providers = await _get_custom_providers(user_id)
    return _format_response(
        data["byok_enabled"], data["keys"], data.get("base_urls"),
        custom_providers=custom_providers,
    )


class UpdateApiKeysRequest(BaseModel):
    byok_enabled: Optional[bool] = None
    api_keys: Optional[Dict[str, Optional[str]]] = None
    base_urls: Optional[Dict[str, Optional[str]]] = None

    @field_validator("api_keys")
    @classmethod
    def validate_api_keys(cls, v):
        if v is None:
            return v
        from src.config.settings import HOST_MODE
        for provider, key in v.items():
            if key is not None:
                if len(key) > 4096:
                    raise ValueError(f"API key for {provider} must be under 4096 chars")
                if key and not key.isascii():
                    raise ValueError(f"API key for {provider} must be ASCII")
                if HOST_MODE == "platform" and key and len(key) < 8:
                    raise ValueError(f"API key for {provider} must be at least 8 chars")
        return v

    @field_validator("base_urls")
    @classmethod
    def validate_base_urls(cls, v):
        if v is None:
            return v
        for provider, url in v.items():
            if url is not None:
                if not url.startswith(("http://", "https://")):
                    raise ValueError(f"Base URL for {provider} must start with http:// or https://")
                if len(url) > 512:
                    raise ValueError(f"Base URL for {provider} must be under 512 chars")
        return v


async def _refuse_base_urls(base_urls: Dict[str, Optional[str]]) -> None:
    """Refuse, in platform mode, a base URL the server must not dial."""
    from src.server.services.llm import egress

    if not egress.enforced():
        return
    reasons = await egress.save_refusals(base_urls.values())
    for provider, url in base_urls.items():
        if url and reasons[url]:
            raise HTTPException(
                status_code=400,
                detail=f"Base URL for {provider} cannot be used: {reasons[url]}",
            )


def _get_allowed_providers(custom_providers: list) -> set[str]:
    """Return the set of provider names allowed for API key operations.

    Excludes OAuth-only providers (codex-oauth, claude-oauth) since those
    are authenticated via OAuth flow, not user-supplied API keys.
    Excludes platform variants (system-only proxy routes), and a gateway's
    routes, whose key is the gateway's (``_get_supported_providers``).
    """
    from src.llms.llm import ModelConfig
    config = ModelConfig()
    allowed = {
        name for name, info in config.flat_providers.items()
        if info.get("access_type") != "oauth" and not info.get("platform") and config.connection_of(name) == name
    }
    for cp in custom_providers:
        allowed.add(cp["name"])
    return allowed


@router.put("/api/v1/users/me/api-keys")
async def update_api_keys(body: UpdateApiKeysRequest, user_id: CurrentUserId):
    """
    Update BYOK settings.

    - byok_enabled: toggle the global switch
    - api_keys: { "openai": "sk-..." } to set, { "openai": null } to delete
    """
    custom_providers = await _get_custom_providers(user_id)
    allowed = _get_allowed_providers(custom_providers)

    # Before anything is written, so a refused request leaves nothing applied.
    # Names first: a URL is resolved only for a provider there is, which bounds
    # the lookups one request can ask for.
    for provider in (*(body.api_keys or {}), *(body.base_urls or {})):
        if provider not in allowed:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported provider: {provider}.",
            )
    if body.base_urls:
        await _refuse_base_urls(body.base_urls)

    # All or nothing, so a failed write leaves the keys as they were.
    async with key_writes(user_id) as conn:
        # Toggle BYOK if requested
        if body.byok_enabled is not None:
            await set_byok_enabled(user_id, body.byok_enabled, conn=conn)

        # Upsert / delete individual provider keys
        if body.api_keys:
            from src.llms.llm import LLM as LLMFactory

            mc = LLMFactory.get_model_config()
            for provider, key_value in body.api_keys.items():
                base_url = body.base_urls.get(provider) if body.base_urls else None
                if key_value is None and not base_url:
                    await delete_api_key(user_id, provider, conn=conn)
                else:
                    # Upsert with key (or empty string for local providers with only base_url)
                    await upsert_api_key(user_id, provider, key_value or "", base_url=base_url, conn=conn)
                # One region per plan, whatever the client last saw stored.
                if key_value:
                    for region in mc.plan_regions(provider):
                        await delete_api_key(user_id, region, conn=conn)

        # Update base_urls independently (when key already exists)
        if body.base_urls:
            for provider, url in body.base_urls.items():
                # Skip if already handled in api_keys upsert above
                if body.api_keys and provider in body.api_keys:
                    continue
                # Update base_url on existing key row
                await update_base_url(user_id, provider, url, conn=conn)

    # Invalidate cached BYOK status so the next chat picks up key changes
    await invalidate_byok_cache(user_id)
    await invalidate_available_models(user_id)

    # Auto-complete personalization if keys were added
    from src.server.services.onboarding import maybe_complete_personalization
    await maybe_complete_personalization(user_id)

    # Return updated state
    data = await get_user_api_keys(user_id)
    return _format_response(
        data["byok_enabled"], data["keys"], data.get("base_urls"),
        custom_providers=custom_providers,
    )


@router.delete("/api/v1/users/me/api-keys/{provider}")
async def remove_api_key(provider: str, user_id: CurrentUserId):
    """Remove one provider's API key, including one saved for a provider
    since withdrawn: it is still the user's secret to take back."""
    custom_providers = await _get_custom_providers(user_id)
    allowed = _get_allowed_providers(custom_providers)
    if provider not in allowed and provider not in (await get_user_api_keys(user_id))["keys"]:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported provider: {provider}.",
        )
    await delete_api_key(user_id, provider)
    # As on PUT: the cached BYOK status still lists the deleted key.
    await invalidate_byok_cache(user_id)
    await invalidate_available_models(user_id)
    data = await get_user_api_keys(user_id)
    return _format_response(
        data["byok_enabled"], data["keys"], data.get("base_urls"),
        custom_providers=custom_providers,
    )


# ── Key Test Endpoint ────────────────────────────────────────────────────


class TestApiKeyRequest(BaseModel):
    """Request body for POST /api/v1/keys/test."""

    provider: str
    api_key: str = ""
    base_url: Optional[str] = None

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        return key_probe.probe_key(v)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        return key_probe.probe_url(v)


async def _user_endpoint_transport(base_url: str | None):
    """The transport a request to a user's endpoint goes out on; a URL the
    egress policy refuses is a 400."""
    from src.server.services.llm import egress

    try:
        return await egress.user_endpoint_transport(base_url)
    except EndpointRefused as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/keys/test")
async def test_api_key(body: TestApiKeyRequest, user_id: CurrentUserId):
    """
    Test an API key by listing models from the provider.

    Lists the provider's models with the key, as the provider's model list
    does, and falls back to a one-token call where it serves no list. Supports
    providers using the anthropic, openai, and gemini SDKs. OpenRouter is
    checked against ``/key`` instead and answers with ``credits`` (USD
    ``limit``, ``usage``, ``remaining``, ``is_free_tier``, ``limit_reset``) for
    the key.

    Rate limited to 1 request per 10 seconds per user. Timeout: 5 seconds hard cap.
    """
    from src.llms.llm import LLM as LLMFactory

    # Validate provider against known providers (all flat providers + custom)
    mc = LLMFactory.get_model_config()
    custom_providers = await _get_custom_providers(user_id)
    provider_info = mc.get_provider_info(body.provider)
    custom = next((cp for cp in custom_providers if cp["name"] == body.provider), None)
    if not provider_info and custom is None:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {body.provider}")

    if not await throttle(f"key_test:{user_id}", 1, 10):
        raise HTTPException(
            status_code=429,
            detail="Too many test requests. Please wait 10 seconds.",
            headers={"Retry-After": "10"},
        )

    # A custom sub-provider is tested as its parent.
    if not provider_info:
        provider_info = mc.get_provider_info(custom["parent_provider"])

    transport = await _user_endpoint_transport(body.base_url or provider_info.get("base_url"))
    try:
        return await key_probe.check_key(
            body.provider, provider_info, body.api_key, body.base_url, transport=transport
        )
    except key_probe.ProbeTimedOut:
        raise HTTPException(
            status_code=504,
            detail="API call timed out (5s limit). Check the provider URL and try again.",
        )
    except EndpointRefused as e:
        # A name that passed the check above and then resolved inward at
        # connect time gets the same 400 as one that was inward from the start.
        raise HTTPException(status_code=400, detail=str(e))


# ── Provider Models (dynamic discovery) ─────────────────────────────────


@router.get("/api/v1/providers/{provider}/models")
async def list_provider_models(provider: str, user_id: CurrentUserId):
    """List models available on a local server via its OpenAI-compatible /models endpoint.

    Used by the setup wizard for dynamic_models providers (Ollama, LM Studio, vLLM).
    """
    import httpx

    from src.llms.llm import LLM as LLMFactory
    from src.server.services.llm import provider_models as pm

    mc = LLMFactory.get_model_config()
    provider_info = mc.get_provider_info(provider)
    if not provider_info:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    if not provider_info.get("dynamic_models"):
        raise HTTPException(status_code=400, detail="Provider does not support dynamic model discovery")

    # Resolve base URL (provider default → user override)
    base_url = (provider_info.get("base_url") or "").replace(
        "{HOST_IP}", HOST_IP,
    )

    # Try to read user's stored base_url override — narrow query, no PGP decrypt
    try:
        override = await get_base_url_for_provider(user_id, provider)
        if override:
            base_url = override.replace("{HOST_IP}", HOST_IP)
    except Exception:
        pass  # DB not available — use provider defaults

    if not base_url:
        raise HTTPException(status_code=400, detail="No base URL configured for provider")

    transport = await _user_endpoint_transport(base_url)

    # Minimal auth — local providers generally don't require it
    api_key = None
    try:
        from src.server.database.api_keys import get_key_for_provider
        api_key = await get_key_for_provider(user_id, provider)
    except Exception:
        pass  # No key available — proceed without auth

    try:
        return {"models": await pm.local_server_models(base_url, api_key, transport=transport)}
    except pm.ListFetchError as e:
        if e.timed_out:
            raise HTTPException(status_code=504, detail="Timed out connecting to provider")
        if e.status is not None:
            raise HTTPException(status_code=502, detail=f"Provider returned {e.status}")
        if isinstance(e.__cause__, EndpointRefused):
            raise HTTPException(status_code=400, detail=str(e.__cause__))
        if isinstance(e.__cause__, httpx.ConnectError):
            raise HTTPException(status_code=502, detail="Could not connect to provider — is it running?")
        logger.error("list_provider_models failed for %s: %s", provider, e, exc_info=True)
        raise HTTPException(status_code=502, detail="Internal error while querying provider")


# ── Models a Credential Can Use ─────────────────────────────────────────


@router.get("/api/v1/providers/{provider}/available-models")
async def list_available_models(
    provider: str,
    user_id: CurrentUserId,
    refresh: bool = False,
    q: Optional[str] = Query(None, max_length=200),
):
    """The models the user's key or subscription for ``provider`` can use.

    The provider's live list intersected with the manifest (see
    ``services/llm/provider_models``): ``ready`` manifest keys, ``others``
    (the rest of the list, Not tuned), and ``status: "fallback"`` with the
    manifest list when the provider list could not be read or names no
    manifest model. ``q`` narrows ``others`` by id and name; ``refresh`` skips
    the cache. Local servers stay on ``/providers/{p}/models``.
    """
    from src.server.services.llm import provider_models as pm

    try:
        return await pm.available_models(user_id, provider, refresh=refresh, q=q)
    except pm.UnknownProvider:
        raise HTTPException(
            status_code=404,
            detail={"code": "unknown_provider", "message": f"Unknown provider: {provider}"},
        )
    except pm.UnsupportedProvider:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "unsupported_provider",
                "message": "This provider's models are not listed here.",
            },
        )
    except pm.NotConnected:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_connected",
                "message": "No key or account is connected for this provider.",
            },
        )


# ── A custom endpoint's models ──────────────────────────────────────────

_ENDPOINT_LIST_PER_MINUTE = 12


class EndpointModelsRequest(BaseModel):
    """Request body for POST /api/v1/providers/custom/models."""

    api_format: Literal["openai-responses", "openai-completions", "anthropic", "gemini"]
    base_url: str
    api_key: str = ""

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        return key_probe.probe_key(v)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return key_probe.probe_url(v) or ""


@router.post("/api/v1/providers/custom/models")
async def list_endpoint_models(body: EndpointModelsRequest, user_id: CurrentUserId):
    """The models a custom endpoint lists, before its provider is saved.

    ``recognized`` are manifest models the endpoint serves (offered to add,
    each keeping the manifest's tuning); ``others`` are the chat models the
    manifest does not know, offered as suggestions to describe by hand. Read
    with the key in the body, which is not stored, through the same egress
    check as a key test.
    """
    from src.server.services.llm import provider_models as pm

    if not await throttle(f"endpoint_models:{user_id}", _ENDPOINT_LIST_PER_MINUTE, 60):
        raise HTTPException(
            status_code=429,
            detail="Too many model list requests. Please wait a minute.",
            headers={"Retry-After": "60"},
        )

    transport = await _user_endpoint_transport(body.base_url)
    return await pm.endpoint_models(body.api_format, body.base_url, body.api_key, transport=transport)


@router.get("/api/v1/models")
async def list_models():
    """
    List all configured LLM models grouped by parent provider.

    No auth required — this is public configuration info.
    Includes provider_catalog for wizard provider selection.
    """
    from src.config.settings import get_prompt_guidance_default
    from src.llms.gateway_names import gateway_index
    from src.llms.llm import get_configured_llm_models, LLM
    from src.llms.task_costs import task_cost_baseline
    from src.server.app import setup
    from src.tools.web.manifest import (
        CAPABILITY_SEARCH,
        providers_with_capability,
        resolve_min_tier,
    )

    models = get_configured_llm_models()
    config = LLM.get_model_config()
    from ptc_agent.config.agent import COMPACTION_PROFILES
    from src.server.services.llm.config import system_default_models

    llm_cfg = setup.agent_config.llm if setup.agent_config and setup.agent_config.llm else None
    # Tiers are pre-resolved (env floor folded in) so clients never need to
    # know about SEARCH_PROVIDER_MIN_TIER; native_params stay server-side.
    search_providers = {
        name: {
            "display_name": spec.display_name,
            "min_tier": resolve_min_tier(cap),
            "default_depth": cap.default_level,
            "depths": [
                {
                    "name": lv.name,
                    "display_name": lv.display_name,
                    "min_tier": resolve_min_tier(lv),
                }
                for lv in cap.levels
            ],
        }
        for name, spec in providers_with_capability(CAPABILITY_SEARCH).items()
        if (cap := spec.capability(CAPABILITY_SEARCH)) is not None
    }
    return {
        "models": {
            provider: {
                "display_name": config.get_display_name(provider),
                "models": model_list,
            }
            for provider, model_list in models.items()
        },
        "model_metadata": config.get_model_metadata(),
        "system_defaults": {
            # One source with the default-model reassignment on preference
            # writes, so "threads on the old default" means what the composer showed.
            **system_default_models(),
            # Blank: a summary runs on the model each turn runs.
            "compaction_model": (llm_cfg.compaction or "") if llm_cfg else "",
            # Legacy alias for cached frontend bundles / third-party clients that
            # still read the pre-rename key. Remove once clients have cycled.
            "summarization_model": (llm_cfg.compaction or "") if llm_cfg else "",
            "fetch_model": (llm_cfg.fetch_name or "") if llm_cfg else "",
            "fallback_models": (llm_cfg.fallback or []) if llm_cfg else [],
            # Null for "auto". A pin sits between the account value and the
            # model's own declaration when a turn resolves guidance, so the
            # client cannot name what "Default" will do without it.
            "prompt_guidance": get_prompt_guidance_default(),
        },
        "compaction_profiles": COMPACTION_PROFILES,
        # model_metadata's task_cost values are multiples of this model at this level.
        "task_cost_baseline": task_cost_baseline(),
        "search_providers": search_providers,
        "provider_catalog": provider_catalog(),
        "key_candidates": key_candidates(),
        # A pick from a gateway's list is tuned as the built-in it matched.
        "gateway_index": gateway_index(config),
    }
