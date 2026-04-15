"""
API Keys and Models Router.

Endpoints:
- GET  /api/v1/users/me/api-keys         — Get BYOK config (masked keys)
- PUT  /api/v1/users/me/api-keys         — Update BYOK config
- DELETE /api/v1/users/me/api-keys/{prov} — Remove one provider key
- POST /api/v1/keys/test                  — Test an API key with a lightweight inference call
- GET  /api/v1/models                     — List available models by provider
"""

import functools
import logging
import os
import re
import time
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from src.config.env import HOST_MODE, HOST_IP
from src.server.utils.api import CurrentUserId
from src.server.database.api_keys import (
    get_user_api_keys,
    get_base_url_for_provider,
    set_byok_enabled,
    upsert_api_key,
    delete_api_key,
    invalidate_byok_cache,
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
    for p in _get_supported_providers():
        raw = keys.get(p)
        pinfo = info_map.get(p, {})
        # Local providers (Ollama, LM Studio, vLLM) are "configured" when a
        # base_url is stored, even without an API key.
        has_base_url = bool(base_urls.get(p))
        env_key = pinfo.get("env_key")
        has_env = HOST_MODE == "oss" and bool(env_key and os.getenv(env_key))
        providers.append({
            "provider": p,
            "display_name": pinfo.get("display_name", p.title()),
            "access_type": pinfo.get("access_type", "api_key"),
            "brand_key": pinfo.get("brand_key", p),
            "has_key": bool(raw) or has_base_url or has_env,
            "masked_key": _mask_key(raw) if raw else ("(server)" if has_env else None),
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
        from src.config.settings import HOST_MODE
        for provider, key in v.items():
            if key is not None:
                if len(key) > 256:
                    raise ValueError(f"API key for {provider} must be under 256 chars")
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


def _get_allowed_providers(custom_providers: list) -> set[str]:
    """Return the set of provider names allowed for API key operations.

    Excludes OAuth-only providers (codex-oauth, claude-oauth) since those
    are authenticated via OAuth flow, not user-supplied API keys.
    Excludes platform variants (system-only proxy routes).
    """
    from src.llms.llm import ModelConfig
    config = ModelConfig()
    allowed = {
        name for name, info in config.flat_providers.items()
        if info.get("access_type") != "oauth" and not info.get("platform")
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
            base_url = body.base_urls.get(provider) if body.base_urls else None
            if key_value is None and not base_url:
                await delete_api_key(user_id, provider)
            else:
                # Upsert with key (or empty string for local providers with only base_url)
                await upsert_api_key(user_id, provider, key_value or "", base_url=base_url)

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

    # Invalidate cached BYOK status so the next chat picks up key changes
    await invalidate_byok_cache(user_id)

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


# ── Key Test Endpoint ────────────────────────────────────────────────────


# Cheapest model per SDK type for key validation.
# These are used for lightweight "say hello" test calls.
_TEST_MODELS: dict[str, str] = {
    "anthropic": "claude-haiku-4-5-20251001",
    "openai": "gpt-4.1-nano",
    "gemini": "gemini-2.0-flash-lite",
}

# Regex to strip potential API key fragments from error messages.
# Matches common key prefixes (sk-, key-, anthropic-, ...) followed by
# 6+ alphanumeric/dash chars.
_KEY_FRAGMENT_RE = re.compile(
    r"(?:sk-|key-|anthropic-|ant-|ANTHROPIC_|OPENAI_|AIza|gsk_|or-)[A-Za-z0-9_-]{6,}", re.ASCII
)


def _sanitize_error(msg: str) -> str:
    """Strip API key fragments and URLs with query params from error messages."""
    # Also strip full URLs that may contain keys in query strings
    msg = re.sub(r"https?://[^\s\"']+", "[URL_REDACTED]", msg)
    return _KEY_FRAGMENT_RE.sub("[REDACTED]", msg)


class TestApiKeyRequest(BaseModel):
    """Request body for POST /api/v1/keys/test."""

    provider: str
    api_key: str = ""
    base_url: Optional[str] = None

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not v:
            return v  # Allow empty for local providers (lm-studio, vllm, ollama)
        if len(v) > 4096:
            raise ValueError("API key must be under 4096 characters")
        if not v.isascii():
            raise ValueError("API key must contain only ASCII characters")
        return v

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if not v.startswith(("http://", "https://")):
                raise ValueError("Base URL must start with http:// or https://")
            if len(v) > 512:
                raise ValueError("Base URL must be under 512 characters")
        return v


async def _test_anthropic_key(
    api_key: str, model: str, base_url: str | None, timeout: float
) -> dict:
    """Test an Anthropic API key. Try /models first, fall back to inference."""
    import httpx

    base = (base_url or "https://api.anthropic.com").rstrip("/")
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Strategy 1: list models
        resp = await client.get(f"{base}/v1/models?limit=5", headers=headers)
        if resp.status_code not in (404, 405, 422):
            resp.raise_for_status()
            data = resp.json()
            models = data.get("data", [])
            return {"models_found": len(models), "model": models[0].get("id", "") if models else ""}

        # Strategy 2: minimal inference
        resp2 = await client.post(
            f"{base}/v1/messages",
            json={"model": model, "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]},
            headers={**headers, "content-type": "application/json"},
        )
        if resp2.status_code not in (404, 405):
            resp2.raise_for_status()
            return {"models_found": None, "model": model}

        # Both 404 — server reachable but no known endpoints responded
        raise ValueError("Server reachable but could not verify key — no compatible API endpoint found")


async def _test_openai_key(
    api_key: str, model: str, base_url: str | None, timeout: float
) -> dict:
    """Test an OpenAI-compatible API key. Try /models first, fall back to inference."""
    import httpx

    base = (base_url or "https://api.openai.com/v1").rstrip("/")
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Strategy 1: list models
        resp = await client.get(f"{base}/models", headers=headers)
        if resp.status_code not in (404, 405):
            resp.raise_for_status()
            data = resp.json()
            models = data.get("data", [])
            return {"models_found": len(models), "model": models[0].get("id", "") if models else ""}

        # Strategy 2: minimal inference (some providers have non-standard paths)
        resp2 = await client.post(
            f"{base}/chat/completions",
            json={"model": model, "max_tokens": 1, "messages": [{"role": "user", "content": "hi"}]},
            headers={**headers, "content-type": "application/json"},
        )
        if resp2.status_code not in (404, 405):
            resp2.raise_for_status()
            return {"models_found": None, "model": model}

        # Both paths 404 — server reachable but no known endpoints responded
        raise ValueError("Server reachable but could not verify key — no compatible API endpoint found")


async def _test_gemini_key(
    api_key: str, model: str, base_url: str | None, timeout: float
) -> dict:
    """Test a Gemini API key. Try /models first, fall back to inference."""
    import httpx

    base = (base_url or "https://generativelanguage.googleapis.com").rstrip("/")
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Strategy 1: list models
        resp = await client.get(f"{base}/v1beta/models?key={api_key}&pageSize=5")
        if resp.status_code not in (404, 405):
            resp.raise_for_status()
            data = resp.json()
            models = data.get("models", [])
            return {
                "models_found": len(models),
                "model": models[0].get("name", "").replace("models/", "") if models else "",
            }

        # Strategy 2: minimal inference
        resp2 = await client.post(
            f"{base}/v1beta/models/{model}:generateContent?key={api_key}",
            json={"contents": [{"parts": [{"text": "hi"}]}], "generationConfig": {"maxOutputTokens": 1}},
            headers={"content-type": "application/json"},
        )
        if resp2.status_code not in (404, 405):
            resp2.raise_for_status()
            return {"models_found": None, "model": model}

        # Both 404 — server reachable but no known endpoints responded
        raise ValueError("Server reachable but could not verify key — no compatible API endpoint found")


# Maps SDK type -> (test function, test model)
_SDK_TEST_DISPATCH: dict[str, tuple] = {
    "anthropic": (_test_anthropic_key, _TEST_MODELS["anthropic"]),
    "openai": (_test_openai_key, _TEST_MODELS["openai"]),
    "gemini": (_test_gemini_key, _TEST_MODELS["gemini"]),
}


_SSRF_BLOCKED_HOSTS = frozenset({"localhost", "metadata.google.internal", "metadata.goog", "instance-data"})


async def _check_ssrf(base_url: str) -> None:
    """Block private/internal IPs in platform mode. No-op in OSS mode."""
    if HOST_MODE != "platform" or not base_url:
        return
    from urllib.parse import urlparse
    import ipaddress
    import socket
    import asyncio

    hostname = urlparse(base_url).hostname or ""

    if hostname.rstrip(".").lower() in _SSRF_BLOCKED_HOSTS or hostname.endswith(".internal"):
        raise HTTPException(
            status_code=400,
            detail="Base URL cannot point to a private or internal address.",
        )

    try:
        loop = asyncio.get_running_loop()
        infos = await loop.run_in_executor(
            None, socket.getaddrinfo, hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM,
        )
        for family, _type, _proto, _canonname, sockaddr in infos:
            addr = ipaddress.ip_address(sockaddr[0])
            if addr.is_private or addr.is_loopback or addr.is_link_local:
                raise HTTPException(
                    status_code=400,
                    detail="Base URL cannot point to a private or internal address.",
                )
    except socket.gaierror:
        pass  # DNS resolution failed — allow; the actual HTTP call will fail later


@router.post("/api/v1/keys/test")
async def test_api_key(body: TestApiKeyRequest, user_id: CurrentUserId):
    """
    Test an API key by listing models from the provider.

    Calls the provider's /models endpoint to verify the key is valid and
    returns how many models are accessible. Supports providers using the
    anthropic, openai, and gemini SDKs.

    Rate limited to 1 request per 10 seconds per user (Redis SET NX EX 10).
    Timeout: 5 seconds hard cap.
    """
    import httpx

    # Validate provider against known providers (all flat providers + custom)
    from src.llms.llm import LLM as LLMFactory

    mc = LLMFactory.get_model_config()
    custom_providers = await _get_custom_providers(user_id)
    custom_names = {cp["name"] for cp in custom_providers}
    provider_info = mc.get_provider_info(body.provider)
    if not provider_info and body.provider not in custom_names:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {body.provider}")

    # Rate limit: 1 per 10s per user
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            rate_key = f"key_test:{user_id}"
            was_set = await cache.client.set(rate_key, "1", nx=True, ex=10)
            if not was_set:
                raise HTTPException(
                    status_code=429,
                    detail="Too many test requests. Please wait 10 seconds.",
                    headers={"Retry-After": "10"},
                )
    except HTTPException:
        raise
    except Exception:
        pass  # Redis unavailable: fail-open

    # For custom sub-providers, resolve through parent
    if not provider_info:
        for cp in custom_providers:
            if cp["name"] == body.provider:
                parent = cp["parent_provider"]
                provider_info = mc.get_provider_info(parent)
                break

    sdk = provider_info.get("sdk", "openai") if provider_info else "openai"

    dispatch = _SDK_TEST_DISPATCH.get(sdk)
    if not dispatch:
        # Fall back to openai-compatible for unknown SDKs (deepseek, qwq, codex)
        dispatch = _SDK_TEST_DISPATCH["openai"]

    test_fn, default_model = dispatch

    # Prefer a model from this specific provider's model list
    provider_models = mc.manifest.get("models", {}).get(body.provider, [])
    if provider_models:
        default_model = provider_models[0].get("id", default_model)

    base_url = body.base_url or provider_info.get("base_url") if provider_info else body.base_url

    await _check_ssrf(base_url)

    start = time.monotonic()
    try:
        result = await test_fn(body.api_key, default_model, base_url, timeout=5.0)
        latency_ms = int((time.monotonic() - start) * 1000)
        return {
            "success": True,
            "models_found": result.get("models_found", 0),
            "model": result.get("model", ""),
            "latency_ms": latency_ms,
        }
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="API call timed out (5s limit). Check the provider URL and try again.",
        )
    except httpx.HTTPStatusError as e:
        latency_ms = int((time.monotonic() - start) * 1000)
        status = e.response.status_code
        try:
            err_body = e.response.json()
            msg = err_body.get("error", {}).get("message", "") if isinstance(err_body.get("error"), dict) else str(err_body.get("error", ""))
        except Exception:
            msg = e.response.text[:200]
        msg = _sanitize_error(msg) if msg else f"HTTP {status}"
        return {"success": False, "error": msg, "latency_ms": latency_ms}
    except Exception as e:
        latency_ms = int((time.monotonic() - start) * 1000)
        msg = _sanitize_error(str(e))
        return {"success": False, "error": msg, "latency_ms": latency_ms}


# ── Provider Models (dynamic discovery) ─────────────────────────────────


@router.get("/api/v1/providers/{provider}/models")
async def list_provider_models(provider: str, user_id: CurrentUserId):
    """List models available on a remote provider via its OpenAI-compatible /models endpoint.

    Used by the setup wizard for dynamic_models providers (Ollama, LM Studio, vLLM).
    All three expose an OpenAI-compatible ``GET /v1/models`` regardless of which SDK
    is used for inference, so we always use that format here.
    """
    import httpx

    from src.llms.llm import LLM as LLMFactory

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

    await _check_ssrf(base_url)

    # All local providers (Ollama, LM Studio, vLLM) expose OpenAI-compatible
    # GET /v1/models. The base_url may or may not already include /v1.
    base = base_url.rstrip("/")
    if base.endswith("/v1"):
        url = f"{base}/models"
    else:
        url = f"{base}/v1/models"

    # Minimal auth — local providers generally don't require it
    headers: dict[str, str] = {}
    try:
        from src.server.database.api_keys import get_key_for_provider
        api_key = await get_key_for_provider(user_id, provider)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
    except Exception:
        pass  # No key available — proceed without auth

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            models = data.get("data", [])
            return {
                "models": [
                    {"id": m.get("id", ""), "name": m.get("id", "")}
                    for m in models
                    if m.get("id")
                ],
            }
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Timed out connecting to provider")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Provider returned {e.response.status_code}")
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Could not connect to provider — is it running?")
    except Exception as e:
        logger.error("list_provider_models failed for %s: %s", provider, e, exc_info=True)
        raise HTTPException(status_code=502, detail="Internal error while querying provider")


# ── Models Endpoint ──────────────────────────────────────────────────────


@functools.lru_cache(maxsize=1)
def _build_provider_catalog() -> list[dict]:
    """Build wizard-visible provider catalog from flat provider config.

    A provider is wizard-visible if:
    - It has no parent_provider (root brand), OR
    - Its access_type differs from its parent's access_type

    Region variants (same access_type, different region) are attached as
    ``region_variants`` on the parent catalog entry so the wizard can show
    an inline region toggle.
    """
    from src.llms.llm import ModelConfig

    config = ModelConfig()
    flat = config.flat_providers

    # First pass: collect region variants per parent
    # region_variant_key -> {provider, region, base_url, sdk, ...}
    region_variants_by_parent: dict[str, list[dict]] = {}
    for key, info in flat.items():
        if info.get("platform"):
            continue
        parent_key = info.get("parent_provider")
        if not parent_key:
            continue
        access_type = info.get("access_type", "api_key")
        parent_info = flat.get(parent_key, {})
        parent_access_type = parent_info.get("access_type", "api_key")
        # Same access_type + different region = region variant
        if access_type == parent_access_type and info.get("region") != parent_info.get("region"):
            region_variants_by_parent.setdefault(parent_key, []).append({
                "provider": key,
                "display_name": config.get_display_name(key),
                "region": info.get("region", "intl"),
                "sdk": info.get("sdk"),
                "base_url": (info.get("base_url") or "").replace("{HOST_IP}", HOST_IP) or None,
                "use_response_api": info.get("use_response_api", False),
            })

    catalog = []
    for key, info in flat.items():
        if info.get("platform"):
            continue
        # Local providers (Ollama, LM Studio, vLLM) are OSS-only
        if info.get("access_type") == "local" and HOST_MODE == "platform":
            continue
        parent_key = info.get("parent_provider")
        access_type = info.get("access_type", "api_key")

        def _entry(k: str, inf: dict, brand: str) -> dict:
            entry = {
                "provider": k,
                "display_name": config.get_display_name(k),
                "access_type": inf.get("access_type", "api_key"),
                "brand_key": brand,
                "byok_eligible": inf.get("byok_eligible", False),
                "region": inf.get("region", "intl"),
                "sdk": inf.get("sdk"),
                "base_url": (inf.get("base_url") or "").replace("{HOST_IP}", HOST_IP) or None,
                "use_response_api": inf.get("use_response_api", False),
                "dynamic_models": inf.get("dynamic_models", False),
            }
            # Attach region variants if this is a root brand
            rv = region_variants_by_parent.get(k)
            if rv:
                entry["region_variants"] = rv
            return entry

        # Root brand: always visible
        if not parent_key:
            catalog.append(_entry(key, info, key))
            continue

        # Variant: visible only if access_type differs from parent
        parent_info = flat.get(parent_key, {})
        parent_access_type = parent_info.get("access_type", "api_key")
        if access_type != parent_access_type:
            catalog.append(_entry(key, info, parent_key))

    return catalog


@router.get("/api/v1/models")
async def list_models():
    """
    List all configured LLM models grouped by parent provider.

    No auth required — this is public configuration info.
    Includes provider_catalog for wizard provider selection.
    """
    from src.llms.llm import get_configured_llm_models, LLM
    from src.server.app import setup

    models = get_configured_llm_models()
    config = LLM.get_model_config()
    llm_cfg = setup.agent_config.llm if setup.agent_config and setup.agent_config.llm else None
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
        "provider_catalog": _build_provider_catalog(),
    }
