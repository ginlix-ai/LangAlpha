"""The credential a turn would use, the cached list it reads, and the entry
points the routes call. The only layer that reads the database, the OAuth
stores and Redis."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from src.llms.model_spec import wire_api
from src.server.services.oauth_providers import SUBSCRIPTIONS

from .catalog import GatewayCatalog, fetch_section
from .matching import ModelsResult, build_result, filter_others, match_endpoint_models
from .wire import (
    CODEX,
    ENDPOINT_REFUSED,
    OPENAI,
    UNAUTHORIZED,
    ListFetchError,
    build_list_request,
    fetch_list,
    list_shape,
    normalize,
)

logger = logging.getLogger(__name__)

CACHE_TTL_S = 6 * 3600
# Bumped when the cached shape changes, so an old entry reads as a miss.
_CACHE_SCHEMA = 2
_REFRESH_COOLDOWN_S = 10

# Fallback reasons a subscription gives before any list is read.
NEEDS_REAUTH = "needs_reauth"
REFRESH_UNAVAILABLE = "refresh_unavailable"
NO_API_ACCESS = "no_api_access"  # the subscription's plan has no API access


class UnknownProvider(Exception):
    pass


class UnsupportedProvider(Exception):
    """A provider whose models are not listed here: a local server (its own
    ``/providers/{p}/models`` route) or a platform-only route."""


class NotConnected(Exception):
    """No credential a turn would use for this provider."""


# ---------------------------------------------------------------------------
# The credential a turn would use
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Credential:
    api_key: str | None = field(repr=False)
    holder: str
    base_url: str | None = None  # saved with the key; None is the manifest's
    account_id: str | None = None
    plan_type: str | None = None
    token_headers: dict[str, str] = field(default_factory=dict, repr=False, compare=False)

    def fingerprint(self, provider: str, extra: Iterable[str] = ()) -> str:
        if provider in SUBSCRIPTIONS:
            # The access token rotates hourly; the account and plan decide the list.
            parts = [provider, self.account_id or "", self.plan_type or "", *extra]
        else:
            parts = [provider, self.holder, self.api_key or "", self.base_url or "", *extra]
        return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:24]


class _OAuthUnavailable(Exception):
    def __init__(self, reason: str):
        self.reason = reason


async def _resolve_credential(user_id: str, provider: str, mc) -> Credential:
    """The credential a turn on ``provider`` would use, or ``NotConnected``."""
    subscription = SUBSCRIPTIONS.get(provider)
    if subscription is not None:
        from src.server.services.oauth_health import (
            OAuthPlanNoApiAccess,
            OAuthReauthRequired,
            OAuthRefreshUnavailable,
            valid_token,
        )

        try:
            token = await valid_token(user_id, provider)
        except OAuthReauthRequired as e:
            raise _OAuthUnavailable(NEEDS_REAUTH) from e
        except OAuthRefreshUnavailable as e:
            raise _OAuthUnavailable(REFRESH_UNAVAILABLE) from e
        except OAuthPlanNoApiAccess as e:
            raise _OAuthUnavailable(NO_API_ACCESS) from e
        if not token or not token.access_token:
            raise NotConnected(provider)
        return Credential(
            api_key=token.access_token,
            holder=provider,
            account_id=token.account_id or None,
            plan_type=token.plan_type or None,
            token_headers=subscription.request_headers(token),
        )

    from src.server.services.llm.byok import walk_byok_candidates
    from src.server.services.llm.config import deployment_key

    byok_config, holder = await walk_byok_candidates(user_id, provider, mc)
    if byok_config and byok_config.get("api_key"):
        return Credential(
            api_key=byok_config["api_key"], holder=holder, base_url=byok_config.get("base_url") or None,
        )
    server_key = deployment_key(provider)
    if server_key:
        return Credential(api_key=server_key, holder=provider)
    raise NotConnected(provider)


# ---------------------------------------------------------------------------
# Cache: one Redis hash per user, a field per provider holding the fetched
# list and the fingerprint of the credential that fetched it.
# ---------------------------------------------------------------------------


def available_models_key(user_id: str) -> str:
    return f"provider_models:{user_id}"


def _redis():
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    return cache.client if cache.enabled and cache.client else None


async def _cache_read(user_id: str, provider: str, fingerprint: str) -> dict | None:
    client = _redis()
    if client is None:
        return None
    try:
        raw = await client.hget(available_models_key(user_id), provider)
        if raw is None:
            return None
        entry = json.loads(raw)
    except Exception:
        return None
    if (
        entry.get("schema") != _CACHE_SCHEMA
        or entry.get("fingerprint") != fingerprint
        or time.time() - entry.get("stored", 0) > CACHE_TTL_S
    ):
        return None
    return entry


async def _cache_write(user_id: str, provider: str, entry: dict) -> None:
    client = _redis()
    if client is None:
        return
    key = available_models_key(user_id)
    try:
        await client.hset(key, provider, json.dumps({**entry, "schema": _CACHE_SCHEMA, "stored": time.time()}))
        await client.expire(key, CACHE_TTL_S)
    except Exception:
        logger.debug("[provider_models] cache write failed", exc_info=True)


async def _refresh_allowed(user_id: str, provider: str) -> bool:
    """One forced refresh per user and provider every few seconds, so the
    button cannot be turned into a stream of calls to the provider."""
    from src.utils.cache.throttle import throttle

    return await throttle(f"provider_models:refresh:{user_id}:{provider}", 1, _REFRESH_COOLDOWN_S)


#: How long a catalog that could not be read is taken as unreadable.
_CATALOG_RETRY_S = 300


async def _gateway_catalog(info: dict) -> GatewayCatalog | None:
    """A gateway's catalog (``model_catalog``), its section shared by every
    user for as long as a list is kept; None for any other provider."""
    config = info.get("model_catalog")
    if not config:
        return None
    url_key = hashlib.sha256(config["url"].encode()).hexdigest()[:16]
    key = f"model_catalog:{url_key}:{config['provider']}"
    client = _redis()
    if client is not None:
        try:
            raw = await client.get(key)
            if raw is not None:
                return GatewayCatalog(config, json.loads(raw))
        except Exception:
            logger.debug("[provider_models] catalog cache read failed", exc_info=True)
    section = await fetch_section(config)
    if client is not None:
        # A catalog that could not be read is kept briefly too, so an outage
        # costs one wait per few minutes rather than one per list.
        try:
            await client.set(key, json.dumps(section), ex=CACHE_TTL_S if section is not None else _CATALOG_RETRY_S)
        except Exception:
            logger.debug("[provider_models] catalog cache write failed", exc_info=True)
    return GatewayCatalog(config, section)


async def invalidate_available_models(user_id: str) -> None:
    """Drop every cached list of the user's. Called when a key or account is
    saved or removed: a key under one slug can serve its siblings' lists."""
    client = _redis()
    if client is None:
        return
    try:
        await client.delete(available_models_key(user_id))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


async def available_models(user_id: str, provider: str, *, refresh: bool = False, q: str | None = None) -> ModelsResult:
    """What the user's credential for ``provider`` can use; see the package doc.

    Raises ``UnknownProvider``, ``UnsupportedProvider`` or ``NotConnected``.
    """
    from src.llms.llm import LLM
    from src.server.services.llm import egress

    mc = LLM.get_model_config()
    info = mc.get_provider_info(provider)
    if not info:
        raise UnknownProvider(provider)
    if info.get("platform") or info.get("dynamic_models") or info.get("access_type") == "local":
        raise UnsupportedProvider(provider)

    shape = list_shape(info)
    try:
        cred = await _resolve_credential(user_id, provider, mc)
    except _OAuthUnavailable as e:
        return filter_others({**build_result(provider, shape, None, mc, reason=e.reason), "cached": False}, q)

    match_kwargs = {"holder": cred.holder, "base_url_saved": bool(cred.base_url)}
    if shape == CODEX:
        from src.llms.extension.codex import codex_client_version

        extra = [codex_client_version()]
    else:
        extra = []
    fingerprint = cred.fingerprint(provider, extra)

    if not refresh or not await _refresh_allowed(user_id, provider):
        entry = await _cache_read(user_id, provider, fingerprint)
        if entry is not None:
            result = build_result(
                provider, entry["shape"], entry["models"], mc, fetched_at=entry["fetched_at"],
                catalog=await _gateway_catalog(info), **match_kwargs,
            )
            return filter_others({**result, "cached": True}, q)

    req = build_list_request(info, api_key=cred.api_key, base_url=cred.base_url, token_headers=cred.token_headers)
    started = time.monotonic()
    # A gateway's catalog is read beside its list: neither waits on the other.
    catalog_read = asyncio.create_task(_gateway_catalog(info))
    try:
        # A list on a base URL the user saved goes out as a key test does.
        try:
            transport = await egress.user_endpoint_transport(cred.base_url)
        except egress.EndpointRefused as e:
            raise ListFetchError(ENDPOINT_REFUSED) from e
        models = normalize(req.shape, await fetch_list(req, transport=transport))
    except ListFetchError as e:
        catalog = await catalog_read
        logger.warning(
            "[provider_models] list fetch failed provider=%s user_id=%s reason=%s status=%s",
            provider, user_id, e.reason, e.status,
        )
        reason = e.reason
        # A token that just passed the refresh check, refused for the plan
        # rather than the sign-in (live-checked on xAI's 403). The message
        # tells the two apart: xAI also answers a stale token with a 403.
        subscription = SUBSCRIPTIONS.get(provider)
        if subscription is not None and subscription.plan_refused(e.status, e.detail):
            reason = NO_API_ACCESS
        elif subscription is not None and subscription.token_refused(e.status, e.detail):
            # Revoked before its stated expiry: spent now, so the next list or
            # turn refreshes it instead of offering it again until then.
            await _expire_refused_token(user_id, provider)
        # A refused key runs none of a gateway's models, so its catalog does
        # not stand in for the list then.
        result = build_result(
            provider, req.shape, None, mc, reason=reason,
            catalog=catalog if reason != UNAUTHORIZED else None, **match_kwargs,
        )
        return filter_others({**result, "cached": False}, q)
    except BaseException:
        catalog_read.cancel()
        raise
    catalog = await catalog_read

    fetched_at = datetime.now(timezone.utc).isoformat()
    await _cache_write(
        user_id, provider,
        {"fingerprint": fingerprint, "shape": req.shape, "models": models, "fetched_at": fetched_at},
    )
    result = build_result(provider, req.shape, models, mc, fetched_at=fetched_at, catalog=catalog, **match_kwargs)
    if result["status"] == "fallback":
        logger.warning(
            "[provider_models] falling back to the manifest provider=%s user_id=%s reason=%s listed=%d",
            provider, user_id, result["reason"], len(models),
        )
    logger.debug(
        "[provider_models] listed provider=%s models=%d ready=%d in %dms",
        provider, len(models), len(result["ready"]), int((time.monotonic() - started) * 1000),
    )
    return filter_others({**result, "cached": False}, q)


async def _expire_refused_token(user_id: str, provider: str) -> None:
    from src.server.database.oauth_tokens import expire_oauth_access_token

    try:
        await expire_oauth_access_token(user_id, provider)
    except Exception:
        logger.warning("[provider_models] could not expire a refused token provider=%s", provider, exc_info=True)


#: A custom endpoint's API format -> the SDK family whose list shape it answers in.
FORMAT_SDKS = {
    "openai-responses": "openai",
    "openai-completions": "openai",
    "anthropic": "anthropic",
    "gemini": "gemini",
}


async def endpoint_models(api_format: str, base_url: str, api_key: str, *, transport: Any = None) -> dict:
    """What a custom endpoint lists, read with the user's key before anything is saved.

    ``status: "unavailable"`` with a ``reason`` when the list could not be
    read: the endpoint may serve no list at all, and the user can still name a
    model by hand.
    """
    from src.llms.llm import LLM as LLMFactory

    req = build_list_request({"sdk": FORMAT_SDKS[api_format]}, api_key=api_key, base_url=base_url)
    try:
        # A 200 that is not a list (z.ai's Anthropic route answers one) fails here.
        models = normalize(req.shape, await fetch_list(req, transport=transport))
    except ListFetchError as e:
        return {"status": "unavailable", "reason": e.reason, "recognized": [], "others": []}
    mc = LLMFactory.get_model_config()
    route_api = wire_api(mc.get_provider_info(FORMAT_SDKS[api_format]), api_format == "openai-responses")
    return {"status": "ok", **match_endpoint_models(models, mc, route_api=route_api)}


async def local_server_models(base_url: str, api_key: str | None, *, transport: Any = None) -> list[dict]:
    """``[{id, name}]`` a local server (Ollama, LM Studio, vLLM) lists, unfiltered.

    Each serves an OpenAI-shaped ``GET /v1/models`` whatever SDK runs its
    inference, and its base URL may or may not already end in ``/v1``.
    Raises ``ListFetchError``.
    """
    base = base_url.rstrip("/")
    if not base.endswith("/v1"):
        base = f"{base}/v1"
    req = build_list_request({"sdk": OPENAI}, api_key=api_key, base_url=base)
    payload = await fetch_list(req, transport=transport, timeout=10.0)
    data = payload.get("data") if isinstance(payload, dict) else None
    return [
        {"id": m["id"], "name": m["id"]}
        for m in data or []
        if isinstance(m, dict) and m.get("id")
    ]
