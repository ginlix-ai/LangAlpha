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


def _format_response(byok_enabled: bool, keys: dict) -> dict:
    """Build the public response shape (never exposes full keys)."""
    display_names = _get_provider_display_names()
    providers = []
    for p in _get_supported_providers():
        raw = keys.get(p)
        providers.append({
            "provider": p,
            "display_name": display_names.get(p, p.title()),
            "has_key": bool(raw),
            "masked_key": _mask_key(raw) if raw else None,
        })
    return {"byok_enabled": byok_enabled, "providers": providers}


# ── BYOK Endpoints ──────────────────────────────────────────────────────


@router.get("/api/v1/users/me/api-keys")
async def get_api_keys(user_id: CurrentUserId):
    """Get user's BYOK configuration (keys are masked)."""
    data = await get_user_api_keys(user_id)
    return _format_response(data["byok_enabled"], data["keys"])


class UpdateApiKeysRequest(BaseModel):
    byok_enabled: Optional[bool] = None
    api_keys: Optional[Dict[str, Optional[str]]] = None

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

    # Upsert / delete individual provider keys
    if body.api_keys:
        supported = _get_supported_providers()
        for provider, key_value in body.api_keys.items():
            if provider not in supported:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported provider: {provider}. Supported: {supported}",
                )
            if key_value is None:
                await delete_api_key(user_id, provider)
            else:
                await upsert_api_key(user_id, provider, key_value)

    # Return updated state
    data = await get_user_api_keys(user_id)
    return _format_response(data["byok_enabled"], data["keys"])


@router.delete("/api/v1/users/me/api-keys/{provider}")
async def remove_api_key(provider: str, user_id: CurrentUserId):
    """Remove one provider's API key."""
    supported = _get_supported_providers()
    if provider not in supported:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported provider: {provider}. Supported: {supported}",
        )
    await delete_api_key(user_id, provider)
    data = await get_user_api_keys(user_id)
    return _format_response(data["byok_enabled"], data["keys"])


# ── Models Endpoint ──────────────────────────────────────────────────────


@router.get("/api/v1/models")
async def list_models():
    """
    List all configured LLM models grouped by provider.

    No auth required — this is public configuration info.
    """
    from src.llms.llm import get_configured_llm_models

    models = get_configured_llm_models()
    return {"models": models}
