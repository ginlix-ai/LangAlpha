"""
API Keys and Models Router.

Endpoints:
- GET  /api/v1/users/me/api-keys         — Get BYOK config (masked keys)
- PUT  /api/v1/users/me/api-keys         — Update BYOK config
- DELETE /api/v1/users/me/api-keys/{prov} — Remove one provider key
- GET  /api/v1/models                     — List available models by provider
"""

import logging
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from src.server.utils.api import CurrentUserId
from src.server.database.api_keys import (
    get_user_api_keys,
    set_byok_enabled,
    upsert_api_key,
    delete_api_key,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["API Keys"])

# Module-level cache for BYOK-eligible providers (loaded once on first access)
_BYOK_PROVIDERS_CACHE: list[str] | None = None


def _get_supported_providers() -> list[str]:
    """Get BYOK-eligible providers from LLM manifest (cached at module level)."""
    global _BYOK_PROVIDERS_CACHE
    if _BYOK_PROVIDERS_CACHE is None:
        from src.llms.llm import ModelConfig

        config = ModelConfig()
        _BYOK_PROVIDERS_CACHE = config.get_byok_eligible_providers()
    return _BYOK_PROVIDERS_CACHE


def _get_provider_display_names() -> dict[str, str]:
    """Get display names for BYOK-eligible providers from manifest."""
    from src.llms.llm import ModelConfig

    config = ModelConfig()
    names = {}
    for p in _get_supported_providers():
        info = config.get_provider_info(p)
        names[p] = info.get("display_name", p.title())
    return names


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
    display_names = _get_provider_display_names()
    base_urls = base_urls or {}
    providers = []
    for p in _get_supported_providers():
        raw = keys.get(p)
        providers.append({
            "provider": p,
            "display_name": display_names.get(p, p.title()),
            "has_key": bool(raw),
            "masked_key": _mask_key(raw) if raw else None,
            "base_url": base_urls.get(p),
            "is_custom": False,
        })

    # Append user-defined sub-providers
    for cp in custom_providers or []:
        name = cp["name"]
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

    return {"byok_enabled": byok_enabled, "providers": providers}


# ── BYOK Endpoints ──────────────────────────────────────────────────────


async def _get_custom_providers(user_id: str) -> list:
    """Load user-defined sub-providers from other_preference.custom_providers."""
    from src.server.database.user import get_user_preferences

    prefs = await get_user_preferences(user_id)
    if not prefs:
        return []
    other = prefs.get("other_preference") or {}
    return other.get("custom_providers") or []


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
        for provider, key in v.items():
            if key is not None:
                if len(key) < 10 or len(key) > 256:
                    raise ValueError(f"API key for {provider} must be 10-256 chars")
                if not key.isascii():
                    raise ValueError(f"API key for {provider} must be ASCII")
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


def _get_allowed_providers(custom_providers: list) -> set[str]:
    """Return the set of provider names allowed for API key operations."""
    allowed = set(_get_supported_providers())
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
    # Toggle BYOK if requested
    if body.byok_enabled is not None:
        await set_byok_enabled(user_id, body.byok_enabled)

    custom_providers = await _get_custom_providers(user_id)
    allowed = _get_allowed_providers(custom_providers)

    # Upsert / delete individual provider keys
    if body.api_keys:
        for provider, key_value in body.api_keys.items():
            if provider not in allowed:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported provider: {provider}.",
                )
            if key_value is None:
                await delete_api_key(user_id, provider)
            else:
                # Include base_url if provided in same request
                base_url = body.base_urls.get(provider) if body.base_urls else None
                await upsert_api_key(user_id, provider, key_value, base_url=base_url)

    # Update base_urls independently (when key already exists)
    if body.base_urls:
        for provider, url in body.base_urls.items():
            if provider not in allowed:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported provider: {provider}.",
                )
            # Skip if already handled in api_keys upsert above
            if body.api_keys and provider in body.api_keys:
                continue
            # Update base_url on existing key row
            from src.server.database.api_keys import update_base_url
            await update_base_url(user_id, provider, url)

    # Return updated state
    data = await get_user_api_keys(user_id)
    return _format_response(
        data["byok_enabled"], data["keys"], data.get("base_urls"),
        custom_providers=custom_providers,
    )


@router.delete("/api/v1/users/me/api-keys/{provider}")
async def remove_api_key(provider: str, user_id: CurrentUserId):
    """Remove one provider's API key."""
    custom_providers = await _get_custom_providers(user_id)
    allowed = _get_allowed_providers(custom_providers)
    if provider not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported provider: {provider}.",
        )
    await delete_api_key(user_id, provider)
    data = await get_user_api_keys(user_id)
    return _format_response(
        data["byok_enabled"], data["keys"], data.get("base_urls"),
        custom_providers=custom_providers,
    )


# ── Models Endpoint ──────────────────────────────────────────────────────


@router.get("/api/v1/models")
async def list_models():
    """
    List all configured LLM models grouped by parent provider.

    No auth required — this is public configuration info.
    """
    from src.llms.llm import get_configured_llm_models, LLM
    from src.server.app import setup

    models = get_configured_llm_models()
    config = LLM.get_model_config()
    llm_cfg = setup.agent_config.llm if setup.agent_config else None
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
            "default_model": llm_cfg.name if llm_cfg else "",
            "flash_model": (llm_cfg.flash or "") if llm_cfg else "",
            "summarization_model": (llm_cfg.summarization or "") if llm_cfg else "",
            "fetch_model": (llm_cfg.fetch or "") if llm_cfg else "",
            "fallback_models": (llm_cfg.fallback or []) if llm_cfg else [],
        },
    }
