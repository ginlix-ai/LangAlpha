"""LLM configuration resolution for the chat handler.

Resolves the effective LLM model, BYOK / OAuth client injection,
reasoning-effort overrides, and user custom-model / custom-provider
lookups.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ptc_agent.config.agent import CredentialSource

from ._common import logger
from .model_availability import (
    _MODEL_PREF_KEYS,
    _cleanup_stale_model_preferences,
    _raise_byok_key_required,
    _raise_model_removed,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODE_MODEL_MAP = {
    "ptc": ("name", "preferred_model"),
    "flash": ("flash", "preferred_flash_model"),
}


@dataclass(frozen=True)
class ResolvedClient:
    """A resolved LLM client plus its model and credential provenance.

    ``model_source`` (a ``ModelSource``) and ``credential_source`` are
    orthogonal — a BYOK user on a system-catalog model yields SYSTEM + BYOK.
    """

    client: Any | None
    model_source: Any | None  # ModelSource
    credential_source: CredentialSource


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _candidate_slugs(provider: str, mc) -> list[str]:
    """Return [provider → parent → sibling variants] in BYOK priority order.

    Shared by ``_walk_byok_candidates`` (key lookup) and STEP-0 prefetch so the
    two can't drift. Excludes platform-only siblings (BYOK keys never live there).
    """
    parent = mc.get_parent_provider(provider)
    candidates: list[str] = [provider]
    # Nested variants (e.g. z-ai-cn-coding under z-ai-cn) parent to the
    # provider itself, not the root — walk them before the parent's family.
    for child in mc.get_child_variants(provider):
        if child not in candidates:
            candidates.append(child)
    if parent and parent != provider:
        candidates.append(parent)
    root = parent if parent else provider
    for sibling in mc.get_child_variants(root):
        if sibling not in candidates:
            candidates.append(sibling)
    return candidates


async def _walk_byok_candidates(
    user_id,
    provider,
    mc,
    *,
    _byok_cache=None,
):
    """Walk [provider → parent → sibling variants] for a stored BYOK key.

    Returns ``(byok_config, holding_slug)`` — the first candidate (in priority
    order) that has a key, or ``(None, None)``. Honors a request-scoped
    ``_byok_cache`` (``dict[str, dict | None] | None``): a slug mapping to a
    dict is a confirmed key, a slug mapping to ``None`` is confirmed-absent, and
    a slug absent from the cache is NOT prefetched — it MUST fall back to a
    direct ``get_byok_configs_for_providers`` lookup so a cache miss is never a
    silent false "no key". Direct-fetch results are written back into the cache.
    """
    from src.server.database.api_keys import get_byok_configs_for_providers

    candidates = _candidate_slugs(provider, mc)

    if _byok_cache is None:
        # Back-compat path: no request-scoped cache, batch-fetch all candidates.
        configs = await get_byok_configs_for_providers(user_id, candidates)
    else:
        # Tri-state cache: a slug absent from the cache was never prefetched, so
        # it must be fetched directly — only a recorded ``None`` counts as a
        # confirmed absence.
        missing = [c for c in candidates if c not in _byok_cache]
        if missing:
            fetched = await get_byok_configs_for_providers(user_id, missing)
            for slug in missing:
                _byok_cache[slug] = fetched.get(slug)
        configs = {c: _byok_cache.get(c) for c in candidates}

    for candidate in candidates:  # keep the provider → parent → sibling priority
        byok_config = configs.get(candidate)
        if byok_config:
            return byok_config, candidate

    return None, None


def _inherit_custom_provider_sdk(custom_config, parent_provider, provider_def, mc):
    """Make a custom provider inherit its manifest parent's SDK/headers (#221).

    A user-defined custom provider slug isn't in the manifest, so
    ``from_custom_config`` derives ``sdk`` from an empty ``provider_info`` and
    defaults to ``"openai"`` — which 404s an Anthropic-shaped endpoint
    (``/chat/completions`` vs ``/v1/messages``). Rewriting ``provider`` to the
    manifest parent fixes the SDK and ``default_headers``.

    Skip the rewrite for openai parents: the default already yields
    ``sdk="openai"``, and inheriting the manifest openai entry would force
    ``use_response_api`` / ``prompt_cache_key`` onto OpenAI-compatible gateways
    (vLLM/LiteLLM/OpenRouter) that only speak ``/chat/completions``. The custom
    provider's own ``use_response_api`` opt-in is honoured either way.
    """
    updates: dict = {}
    if mc.get_provider_info(parent_provider).get("sdk") not in (None, "openai"):
        updates["provider"] = parent_provider
    if provider_def.get("use_response_api"):
        updates["_use_response_api"] = True
    return {**custom_config, **updates} if updates else custom_config


async def _resolve_custom_model_byok(
    user_id: str,
    model_name: str,
    custom_config: dict,
    mc,
    _pref_cache: dict | None = None,
    _byok_cache: dict | None = None,
):
    """
    Resolve BYOK key + base_url for a user-defined custom model.

    Key lookup order:
    1. Model name as a custom sub-provider (model and provider share a name).
    2. Custom model's provider field as a custom sub-provider.
    3. System provider fan-out: the provider's own slug, then its parent, then
       every non-platform sibling variant of the parent. The sibling step
       handles the mirror case where the custom model is tagged with the
       parent slug (e.g. ``moonshot``) but the user only configured a variant
       (e.g. ``moonshot-coding``) so the key lives under that variant.
       Platform-only variants are excluded (BYOK keys are never stored there).
    """
    from src.server.database.api_keys import get_byok_config_for_provider

    provider = custom_config["provider"]

    # 1. Model name is itself a custom sub-provider with a key
    cp_by_name = await get_custom_provider_config(user_id, model_name, _pref_cache=_pref_cache)
    if cp_by_name:
        byok_config = await get_byok_config_for_provider(user_id, model_name)
        if byok_config:
            parent = cp_by_name["parent_provider"]
            base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
            custom_config = _inherit_custom_provider_sdk(custom_config, parent, cp_by_name, mc)
            return byok_config, base_url, custom_config

    # 2. Provider field is a custom sub-provider
    cp_by_provider = await get_custom_provider_config(user_id, provider, _pref_cache=_pref_cache)
    if cp_by_provider:
        byok_config = await get_byok_config_for_provider(user_id, provider)
        if byok_config:
            parent = cp_by_provider["parent_provider"]
            base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
            custom_config = _inherit_custom_provider_sdk(custom_config, parent, cp_by_provider, mc)
            return byok_config, base_url, custom_config

    # 3. System provider — walk [provider → parent → sibling variants] for a
    #    stored key. The sibling step covers the mirror case where a custom
    #    model is tagged with the parent slug but the user only configured a
    #    variant (e.g. coding-plan) so the key lives under the variant.
    byok_config, holding = await _walk_byok_candidates(
        user_id, provider, mc, _byok_cache=_byok_cache,
    )
    if byok_config:
        base_url = byok_config.get("base_url") or mc.get_provider_info(holding).get("base_url")
        # Rewrite ``provider`` to the candidate that actually held the key.
        # ``create_llm_from_custom`` reads SDK / default_headers /
        # use_response_api from the provider field, so if a custom model
        # tagged ``dashscope`` resolves via its ``dashscope-coding``
        # sibling, we need the SDK to match the coding-plan endpoint —
        # otherwise we'd build a Qwen client pointed at an
        # Anthropic-shaped URL and fail every request.
        if holding != provider:
            custom_config = {**custom_config, "provider": holding}
        return byok_config, base_url, custom_config

    return None, None, custom_config


async def resolve_byok_llm_client(
    user_id: str,
    model_name: str,
    is_byok: bool,
    reasoning_effort: str | None = None,
    _pref_cache: dict | None = None,
    cache_key: str | None = None,
    _byok_cache: dict | None = None,
):
    """
    If BYOK is active, build an LLM client for ``model_name``. Returns None
    if BYOK isn't applicable or no key is configured. ``resolve_llm_config``
    converts a None result into a user-facing ``byok_key_required``
    HTTPException for custom models on the main-model path — this function
    stays at debug log level so the user sees one error, not two.

    - System model: walk [provider → parent → sibling variants] for the BYOK
      key (coding-plan variants store it under their own slug), but build
      against the MODEL'S OWN provider endpoint, never the candidate that
      merely held the key.
    - Custom model (custom shadows built-in when names collide): walk the
      custom/provider/variant key chain via ``_resolve_custom_model_byok``.
    - Unknown name but matches a user's ``custom_providers`` slug:
      synthesize a custom model entry and route through the user's key.

    ``classify_model`` is O(1) with ``_pref_cache`` populated, so callers
    don't need to pre-classify — pass the cache and this function does its
    own lookup. ``_byok_cache`` is a request-scoped tri-state cache threaded
    into ``_walk_byok_candidates`` (see its contract).
    """
    if not is_byok:
        return None

    from src.llms.llm import LLM as LLMFactory, create_llm, create_llm_from_custom

    mc = LLMFactory.get_model_config()
    source, config_entry = await classify_model(
        user_id, model_name, _pref_cache=_pref_cache,
    )

    # Custom model — custom entry wins. If the name also matches a built-in,
    # we intentionally ignore the system side: the user asked for their
    # variant's key to handle this name.
    if source == ModelSource.CUSTOM:
        byok_config, base_url, custom_config = await _resolve_custom_model_byok(
            user_id, model_name, config_entry, mc,
            _pref_cache=_pref_cache, _byok_cache=_byok_cache,
        )
        if not byok_config:
            # ``resolve_llm_config`` converts this None into an HTTPException
            # for the main-model path, and logs its own warning for custom
            # fallback models. Keep this at debug so the chat-level error
            # (with CTA) is the single user-visible signal.
            logger.debug(
                f"[CHAT] No BYOK key found for custom model={model_name} "
                f"provider={custom_config['provider']}."
            )
            return None
        logger.info(
            f"[CHAT] Using BYOK key for custom model={model_name} "
            f"provider={custom_config['provider']} base_url={base_url or 'SDK default'}"
        )
        return create_llm_from_custom(
            custom_config,
            api_key=byok_config["api_key"],
            base_url=base_url,
            cache_key=cache_key,
        )

    # Unknown name — last-chance check for a custom-provider slug. Covers the
    # edge case where a user typed their custom provider slug as the model name.
    if source == ModelSource.UNKNOWN:
        cp_config = await get_custom_provider_config(
            user_id, model_name, _pref_cache=_pref_cache,
        )
        if not cp_config:
            return None
        synthetic_cm = {
            "name": model_name,
            "model_id": model_name,
            "provider": cp_config["parent_provider"],
        }
        byok_config, base_url, custom_config = await _resolve_custom_model_byok(
            user_id, model_name, synthetic_cm, mc,
            _pref_cache=_pref_cache, _byok_cache=_byok_cache,
        )
        if not byok_config:
            return None
        return create_llm_from_custom(
            custom_config,
            api_key=byok_config["api_key"],
            base_url=base_url,
            cache_key=cache_key,
        )

    # System model — the BYOK key may live under the model's own provider
    # slug (coding-plan variants store it there), its parent, or a sibling
    # variant. Walk all three; but pin SDK + base_url to the MODEL'S OWN
    # provider, never the candidate that merely held the key.
    provider = config_entry["provider"]
    byok_config, holding = await _walk_byok_candidates(
        user_id, provider, mc, _byok_cache=_byok_cache,
    )
    if not byok_config:
        return None
    # base_url precedence: a user custom base_url on the holding slug wins;
    # otherwise the MODEL'S OWN provider endpoint (NOT the parent's, NOT the
    # candidate's). This is the coding-variant fix: a dashscope-coding model
    # (anthropic SDK) whose key lives under parent `dashscope` (openai SDK)
    # must still build against the anthropic coding endpoint.
    base_url = byok_config.get("base_url") or mc.get_provider_info(provider).get("base_url")
    logger.debug(
        f"[CHAT] Resolved BYOK client for system model={model_name} "
        f"provider={provider} key_held_by={holding} base_url={base_url or 'SDK default'}"
    )
    return create_llm(
        model_name,
        api_key=byok_config["api_key"],
        base_url=base_url,
        reasoning_effort=reasoning_effort,
        cache_key=cache_key,
    )


async def resolve_oauth_llm_client(
    user_id: str,
    model_name: str,
    reasoning_effort: str | None = None,
    service_tier: str | None = None,
    cache_key: str | None = None,
):
    """Resolve OAuth-connected LLM client. Independent of BYOK toggle."""
    from src.llms.llm import LLM as LLMFactory, create_llm

    mc = LLMFactory.get_model_config()
    model_info = mc.get_model_config(model_name)
    if not model_info:
        return None

    provider = model_info["provider"]
    provider_info = mc.get_provider_info(provider)
    if provider_info.get("access_type") != "oauth":
        return None

    # Dispatch to the correct OAuth service by provider
    if provider == "claude-oauth":
        from src.server.services.claude_oauth import get_valid_token
    else:
        from src.server.services.codex_oauth import get_valid_token

    token_data = await get_valid_token(user_id)
    if not token_data:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Model '{model_name}' requires a connected {provider} account.",
                "type": "oauth_required",
                "link": {"url": "/setup/method", "label": "Connect account"},
            },
        )

    access_token = token_data["access_token"]
    if not access_token or not isinstance(access_token, str):
        logger.error(
            f"[CHAT] OAuth token is empty or not a string for provider={provider}: type={type(access_token)}"
        )
        return None

    # Provider-specific headers
    headers = {}
    if provider == "claude-oauth":
        logger.debug(f"[CHAT] Resolved Claude OAuth client for model={model_name}")
    else:
        # Codex: set ChatGPT-Account-Id header
        account_id = token_data.get("account_id", "")
        logger.debug(f"[CHAT] Resolved Codex OAuth client for model={model_name}")
        if account_id:
            headers["ChatGPT-Account-Id"] = account_id

    return create_llm(
        model_name,
        api_key=access_token,
        default_headers=headers if headers else None,
        reasoning_effort=reasoning_effort,
        cache_key=cache_key,
        **({"service_tier": service_tier} if service_tier and provider != "claude-oauth" else {}),
    )


async def resolve_model_client(
    user_id,
    model_name,
    *,
    is_byok,
    cache_key=None,
    reasoning_effort=None,
    service_tier=None,
    allow_platform_fallback=False,
    _pref_cache=None,
    _byok_cache=None,
) -> ResolvedClient:
    """Resolve a client for ``model_name`` and report which credential built it.

    Tries OAuth first (always, independent of ``is_byok``), then BYOK (if
    enabled), then a platform-keyed client (only for SYSTEM models when
    ``allow_platform_fallback`` is set). ``model_source`` classifies the model;
    ``credential_source`` records which credential produced the client — the two
    are orthogonal. An OAuth-required HTTPException is allowed to propagate.
    """
    source, _ = await classify_model(user_id, model_name, _pref_cache=_pref_cache)

    client = await resolve_oauth_llm_client(
        user_id, model_name, reasoning_effort,
        service_tier=service_tier, cache_key=cache_key,
    )
    if client:
        return ResolvedClient(client, source, CredentialSource.OAUTH)

    if is_byok:
        client = await resolve_byok_llm_client(
            user_id, model_name, is_byok, reasoning_effort,
            _pref_cache=_pref_cache, cache_key=cache_key, _byok_cache=_byok_cache,
        )
        if client:
            return ResolvedClient(client, source, CredentialSource.BYOK)

    # Platform fallback — only for SYSTEM-catalog models. Reached when OAuth and
    # BYOK both miss. service_tier is OAuth-only (matches the main-branch
    # reasoning path), so it is intentionally NOT passed here.
    if allow_platform_fallback and source == ModelSource.SYSTEM:
        from src.llms.llm import create_llm

        client = create_llm(
            model_name, reasoning_effort=reasoning_effort, cache_key=cache_key,
        )
        return ResolvedClient(client, source, CredentialSource.PLATFORM)

    return ResolvedClient(None, source, CredentialSource.NONE)


async def get_model_preference(user_id: str) -> dict:
    """Return model preferences from other_preference (not agent_preference, which is dumped to agent context)."""
    from src.server.database.user import get_user_preferences

    prefs = await get_user_preferences(user_id)
    if not prefs:
        return {}
    return prefs.get("other_preference") or {}


async def get_custom_model_config(user_id: str, model_name: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined custom model by name from other_preference.custom_models."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    for cm in model_pref.get("custom_models") or []:
        if cm.get("name") == model_name:
            return cm
    return None


async def get_custom_provider_config(user_id: str, provider: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined sub-provider config (name, parent_provider, use_response_api, etc.)."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    for cp in model_pref.get("custom_providers") or []:
        if cp.get("name") == provider:
            return cp
    return None


# ---------------------------------------------------------------------------
# Central model classification — single entry point used by every call site
# that needs to answer "what is this model?". System vs custom is a flat
# namespace guaranteed by ``_validate_custom_models`` (users.py), so this
# function does at most one in-memory dict hit and one pref-cache scan.
# ---------------------------------------------------------------------------


class ModelSource(StrEnum):
    SYSTEM = "system"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


async def classify_model(
    user_id: str,
    model_name: str,
    _pref_cache: dict | None = None,
) -> tuple[str, dict]:
    """Classify ``model_name`` as system / custom / unknown.

    Returns a ``(source, config)`` pair where ``config`` is:
      - the user's ``custom_models`` entry for custom models
      - the entry from ``models.json`` for system models
      - ``{}`` for unknown

    Custom is checked first. When a user's ``custom_models`` entry shadows a
    built-in of the same name, the custom entry wins — lets users route a
    built-in model name (e.g. ``glm-5.1``) through a variant's own key.
    ``_pref_cache`` keeps the chat hot path free of extra DB reads.
    """
    from src.llms.llm import LLM as LLMFactory

    custom_cm = await get_custom_model_config(user_id, model_name, _pref_cache=_pref_cache)
    if custom_cm:
        return ModelSource.CUSTOM, custom_cm

    mc = LLMFactory.get_model_config()
    system_info = mc.get_model_config(model_name)
    if system_info:
        return ModelSource.SYSTEM, system_info

    return ModelSource.UNKNOWN, {}


# ---------------------------------------------------------------------------
# LLM roles — compaction / fetch / per-subagent. A dumb record + builder so the
# resolution loop is a flat iteration instead of bespoke per-role branches.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LLMRole:
    """A subsidiary model slot to resolve a client for.

    ``key`` is the ``subsidiary_llm_clients`` key ("compaction" | "fetch" |
    "subagent:<name>"). ``fallback_to_main`` controls whether a keyless role
    inherits a copy of the main client at materialization time. Priority
    service tier is main-only, so ``service_tier`` stays None for roles.
    """

    key: str
    model: str | None
    fallback_to_main: bool = True
    service_tier: str | None = None


def role_registry(config, enabled_subagents, subagent_defs) -> list[LLMRole]:
    """Build the ordered list of model roles to resolve for this request."""
    roles = [
        LLMRole("compaction", config.llm.compaction),
        LLMRole("fetch", config.llm.fetch),
    ]
    for name in enabled_subagents:
        defn = subagent_defs.get(name)
        if defn is not None and getattr(defn, "model", None):
            roles.append(LLMRole(f"subagent:{name}", defn.model))
    return [r for r in roles if r.model]


def _build_roles(config, enabled_subagents: list[str]) -> list[LLMRole]:
    """Build the role list once, resolving subagent defs through the registry.

    Shared by the BYOK prefetch (so subagent model slugs land in the batch) and
    the role-client resolver (so the registry is built once, not twice).
    """
    try:
        from ptc_agent.agent.subagents.registry import SubagentRegistry

        registry = SubagentRegistry(user_definitions=config.subagents.definitions)
        subagent_defs = {name: registry.get(name) for name in enabled_subagents}
    except Exception:
        logger.error(
            "[CHAT] Failed to build subagent registry; skipping subagent roles",
            exc_info=True,
        )
        subagent_defs = {}
    return role_registry(config, enabled_subagents, subagent_defs)


async def _prefetch_byok_cache(
    user_id: str, config, effective_model: str, model_pref: dict,
    roles: list[LLMRole],
) -> dict[str, dict | None]:
    """Best-effort batch BYOK prefetch → tri-state cache for ``_walk_byok_candidates``.

    Gathers candidate provider slugs across the main model, every role model
    (compaction / fetch / subagent), and every fallback model, issues ONE
    ``get_byok_configs_for_providers`` query, and seeds the cache. Pure perf:
    every walk is cache-miss-safe (a slug absent from the cache falls back to a
    direct lookup), so this must never become a correctness dependency. On any
    error returns ``{}`` — each walk then does its own direct lookup (correct,
    just unoptimized).
    """
    from src.llms.llm import LLM as LLMFactory

    try:
        mc = LLMFactory.get_model_config()
        candidate_models = [effective_model]
        candidate_models += [r.model for r in roles if r.model]
        candidate_models += list(config.llm.fallback or [])
        all_slugs: set[str] = set()
        for m in candidate_models:
            src_, cfg = await classify_model(user_id, m, _pref_cache=model_pref)
            # SYSTEM (models.json) and CUSTOM (user config) both carry a
            # "provider" slug; UNKNOWN carries nothing.
            prov = cfg.get("provider") if src_ != ModelSource.UNKNOWN else None
            if prov:
                all_slugs.update(_candidate_slugs(prov, mc))
        if not all_slugs:
            return {}
        from src.server.database.api_keys import get_byok_configs_for_providers

        slugs = list(all_slugs)
        configs = await get_byok_configs_for_providers(user_id, slugs)
        return {slug: configs.get(slug) for slug in slugs}
    except Exception:
        logger.warning(
            "[CHAT] BYOK batch prefetch failed; falling back to per-walk lookups",
            exc_info=True,
        )
        return {}


async def _resolve_role_clients(
    config, user_id: str, roles: list[LLMRole], model_pref: dict,
    byok_cache: dict, *, is_byok: bool, cache_key: str | None,
) -> None:
    """Resolve compaction/fetch/subagent role clients onto ``config`` in place.

    Assumes ``config`` is already a copy and ``config.credential_source`` /
    ``config.llm_client`` are set. Roles never get an eager platform client
    (``allow_platform_fallback=False``); a keyless role for an OAUTH/BYOK user
    is seeded with a copy of the main client (BYOK-pure), while PLATFORM/NONE
    users store nothing so the cheap name-based path stays. Each role's I/O
    (OAuth check + BYOK walk) runs concurrently; writes happen after the gather
    so the SSE hot path waits one round-trip, not N.
    """

    async def _resolve_one(role: LLMRole):
        try:
            return role, await resolve_model_client(
                user_id, role.model, is_byok=is_byok, cache_key=cache_key,
                allow_platform_fallback=False, service_tier=None,
                _pref_cache=model_pref, _byok_cache=byok_cache,
            )
        except Exception:
            logger.error(
                "[CHAT] Failed to resolve role %s model %s, skipping",
                role.key, role.model, exc_info=True,
            )
            return role, None

    for role, rc in await asyncio.gather(*(_resolve_one(r) for r in roles)):
        if rc is None:
            continue
        if rc.client is not None:
            config.subsidiary_llm_clients[role.key] = rc.client
        elif rc.model_source is not None and rc.model_source != ModelSource.SYSTEM:
            logger.warning(
                "[CHAT] Role '%s' model '%s' is a custom model without a usable "
                "BYOK key — falling back to default.",
                role.key, role.model,
            )

    # BYOK-pure write-time materialization. Gate purely on the credential
    # SIGNAL: by the primitive's invariant OAUTH/BYOK ⟹ llm_client is set, so a
    # main-client copy is safe. PLATFORM/NONE users store nothing → cheap name
    # path stays (a non-BYOK reasoning user's PLATFORM client is NOT copied).
    # The explicit None check makes the invariant a guard, not an assumption.
    if (
        config.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK)
        and config.llm_client is not None
    ):
        for role in roles:
            if role.fallback_to_main and role.key not in config.subsidiary_llm_clients:
                config.subsidiary_llm_clients[role.key] = config.llm_client.model_copy()
                logger.info(
                    "[CHAT] Role '%s' has no own key; falling back to the user's "
                    "main client (cost shifts to main-model rate).",
                    role.key,
                )


async def _resolve_fallback_clients(
    config, user_id: str, model_pref: dict, byok_cache: dict,
    *, is_byok: bool, cache_key: str | None,
) -> None:
    """Resolve ``config.llm.fallback`` names into client instances in place.

    Platform fallback is ON so a SYSTEM fallback name without a user key still
    yields a platform client (no model silently dropped). Custom/unknown
    fallbacks without a usable key warn + skip.
    """
    fallback_models = config.llm.fallback or []
    if not fallback_models:
        return

    async def _resolve_one(model_name: str):
        try:
            return model_name, await resolve_model_client(
                user_id, model_name, is_byok=is_byok, cache_key=cache_key,
                allow_platform_fallback=True,
                _pref_cache=model_pref, _byok_cache=byok_cache,
            )
        except Exception:
            logger.error(
                "[CHAT] Failed to resolve fallback model %s, skipping",
                model_name, exc_info=True,
            )
            return model_name, None

    merged_fallbacks = []
    byok_count = 0
    # Resolve concurrently; append in declared order to preserve fallback priority.
    for model_name, fc in await asyncio.gather(
        *(_resolve_one(m) for m in fallback_models)
    ):
        if fc is None:
            continue
        if fc.client is not None:
            merged_fallbacks.append(fc.client)
            if fc.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK):
                byok_count += 1
        elif fc.model_source is not None and fc.model_source != ModelSource.SYSTEM:
            logger.warning(
                "[CHAT] Fallback model '%s' is a custom model without a "
                "usable BYOK key — skipping. Add a key in Settings to enable.",
                model_name,
            )
        # else: SYSTEM with no client (shouldn't happen with platform fallback
        # on) — guard by skipping.

    if merged_fallbacks:
        config.fallback_llm_clients = merged_fallbacks
        if byok_count:
            logger.debug(
                f"[CHAT] Resolved {byok_count}/{len(fallback_models)} fallback models via OAuth/BYOK"
            )


async def resolve_llm_config(
    base_config,
    user_id: str,
    request_model: str | None,
    is_byok: bool | None = None,
    mode: str = "ptc",
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
    thread_id: str | None = None,
    *,
    enabled_subagents: list[str] | None = None,
):
    """
    Resolve final LLM config with priority:
    per-request model > user preferred model > default.
    Then inject BYOK/OAuth client if active, and apply reasoning effort.

    Mode determines which config field and preference key to use
    (see _MODE_MODEL_MAP). Easy to extend for new modes.

    ``is_byok=None`` self-resolves via ``is_byok_active`` (guards future entry
    points; all current callers pass it explicitly). ``enabled_subagents``
    threads the request's active subagent list so per-subagent model roles get
    their own credential resolution; ``None`` falls back to the config default.
    """
    from ptc_agent.config import LLMConfig

    if is_byok is None:
        from src.server.database.api_keys import is_byok_active

        is_byok = await is_byok_active(user_id)

    model_field, pref_key = _MODE_MODEL_MAP[mode]
    config = base_config

    def _cow():
        """Copy-on-write: deep-copy base_config the first time we mutate it."""
        nonlocal config
        if config is base_config:
            config = config.model_copy(deep=True)

    model_pref = await get_model_preference(user_id)
    _enabled_subagents = (
        enabled_subagents
        if enabled_subagents is not None
        else list(config.subagents.enabled)
    )

    # Bootstrap LLMConfig when agent_config.yaml has llm: null.
    # The user must have configured a model via the UI or per-request param.
    if config.llm is None:
        resolved_name = request_model or model_pref.get(pref_key)
        if not resolved_name:
            raise ValueError(
                "No model configured. Set llm in agent_config.yaml or select a model in Settings."
            )
        _cow()
        config.llm = LLMConfig(
            name=resolved_name if mode == "ptc" else "placeholder",
            flash=resolved_name if mode == "flash" else model_pref.get("preferred_flash_model"),
            compaction=(
                model_pref.get("compaction_model")
                or model_pref.get("summarization_model")
                or model_pref.get("preferred_flash_model")
            ),
            fetch=model_pref.get("fetch_model"),
            fallback=model_pref.get("fallback_models"),
        )
        config.llm_client = None
        logger.debug(f"[CHAT] No system default LLM; bootstrapped from user preferences: {resolved_name}")
    elif request_model:
        _cow()
        setattr(config.llm, model_field, request_model)
        config.llm_client = None
        logger.debug(f"[CHAT] Using per-request LLM model: {request_model}")
    else:
        preferred = model_pref.get(pref_key)
        if preferred:
            _cow()
            setattr(config.llm, model_field, preferred)
            config.llm_client = None
            logger.debug(f"[CHAT] Using {pref_key}: {preferred}")
        else:
            logger.debug(
                f"[CHAT] No {pref_key} set, using system default: {getattr(config.llm, model_field, None) or config.llm.name}"
            )

    # Apply other model overrides from user preferences.
    # Both "compaction_model" (new) and "summarization_model" (legacy) map to
    # the renamed ``compaction`` config field; legacy key is read so existing
    # rows with the legacy key keep working. Order matters: legacy is
    # applied first so the new key wins when both are present.
    _other_model_keys = [
        ("summarization_model", "compaction"),
        ("compaction_model", "compaction"),
        ("fetch_model", "fetch"),
    ]
    for pref_key_other, config_field in _other_model_keys:
        user_val = model_pref.get(pref_key_other)
        if user_val:
            _cow()
            setattr(config.llm, config_field, user_val)

    user_fallback = model_pref.get("fallback_models")
    if user_fallback is not None:
        _cow()
        config.llm.fallback = user_fallback

    # Per-user search provider + depth, gated per the search manifest. This
    # resolve-time check is the enforcement point, so a stale pref from a
    # downgraded plan is ignored at consumption. ChatAuthResult.access_tier
    # is -1 for BYOK/OAuth users, so the true tier is resolved here
    # (Redis-cached) — once, lazily, shared by both checks.
    pref_search_provider = model_pref.get("search_provider")
    pref_search_depth = model_pref.get("search_depth")
    if pref_search_provider or pref_search_depth:
        from src.config.settings import HOST_MODE
        from src.tools.search_manifest import (
            get_search_provider_spec,
            resolve_depth_tier,
            resolve_provider_tier,
        )

        is_platform = HOST_MODE == "platform"
        _user_tier: int | None = None

        async def _platform_tier() -> int:
            nonlocal _user_tier
            if _user_tier is None:
                from src.server.dependencies.usage_limits import _fetch_platform_tier

                _user_tier = await _fetch_platform_tier(user_id)
            return _user_tier

        if pref_search_provider:
            spec = (
                get_search_provider_spec(pref_search_provider)
                if isinstance(pref_search_provider, str)
                else None
            )
            if spec is None:
                logger.warning(
                    f"[CHAT] Ignoring unknown search_provider preference: {pref_search_provider!r}"
                )
            elif not is_platform or await _platform_tier() >= resolve_provider_tier(spec):
                _cow()
                config.search_api = spec.name
                logger.debug(f"[CHAT] Using search_provider: {spec.name}")
            else:
                logger.debug(
                    f"[CHAT] search_provider pref ignored (tier below {resolve_provider_tier(spec)})"
                )

        if pref_search_depth:
            # Depth names are provider-scoped: validate against the EFFECTIVE
            # provider (post-provider-resolution), so a stale depth left over
            # from another provider degrades to that provider's default.
            eff_spec = get_search_provider_spec(config.search_api)
            depth_spec = (
                eff_spec.depth(pref_search_depth)
                if eff_spec is not None and isinstance(pref_search_depth, str)
                else None
            )
            if depth_spec is None:
                logger.debug(
                    f"[CHAT] search_depth pref {pref_search_depth!r} not offered by "
                    f"provider {config.search_api!r}; using provider default"
                )
            elif not is_platform or await _platform_tier() >= resolve_depth_tier(depth_spec):
                _cow()
                config.search_depth = depth_spec.name
                logger.debug(f"[CHAT] Using search_depth: {depth_spec.name}")
            else:
                logger.debug(
                    f"[CHAT] search_depth pref ignored (tier below {resolve_depth_tier(depth_spec)})"
                )

    # Compaction profile: a named preset (aggressive/moderate/extended/relaxed)
    # that bundles token_threshold, truncate_args_trigger_messages, and
    # keep_messages. Unknown/missing values fall through to the YAML-configured
    # defaults.
    from ptc_agent.config.agent import COMPACTION_PROFILES

    compaction_profile = model_pref.get("compaction_profile")
    preset = (
        COMPACTION_PROFILES.get(compaction_profile)
        if isinstance(compaction_profile, str)
        else None
    )
    if preset:
        _cow()
        for field, value in preset.items():
            setattr(config.compaction, field, value)

    # Resolve the effective model from whichever field we just set
    effective_model = getattr(config.llm, model_field, None) or config.llm.name

    # Classify via the single entry point. System and custom share a flat
    # namespace (enforced by ``_validate_custom_models``), so one call answers
    # the question for the entire downstream flow.
    source, resolved_config = await classify_model(
        user_id, effective_model, _pref_cache=model_pref
    )
    is_custom = source == ModelSource.CUSTOM
    custom_cm = resolved_config if is_custom else None
    # ``is_custom_provider`` only matters when the model name didn't classify
    # as a known custom model — catches the case where the user typed a
    # custom *provider* slug as the model preference.
    if source == ModelSource.UNKNOWN:
        is_custom_provider = (
            await get_custom_provider_config(user_id, effective_model, _pref_cache=model_pref) is not None
        )
    else:
        is_custom_provider = False

    # Custom model/provider requires BYOK. No silent fallback — raise a clear error
    # so the frontend can show a CTA linking to Settings.
    if (is_custom or is_custom_provider) and not is_byok:
        _raise_byok_key_required(effective_model)

    # Stale-model recovery. Scrub prefs if the user's saved name is the
    # culprit; raise a user-facing CTA either way. YAML-default UNKNOWN
    # falls through so the downstream error surfaces the config bug.
    if source == ModelSource.UNKNOWN and not is_custom_provider:
        # Only the five scalar keys feed ``effective_model`` — fallback_models
        # is resolved separately in ``_resolve_fallback_clients`` and never
        # flows through here, so it's intentionally excluded from this
        # attribution check (the scrub in ``_cleanup_stale_model_preferences``
        # still filters fallback_models once it fires).
        from_pref = any(
            model_pref.get(k) == effective_model for k in _MODEL_PREF_KEYS
        )
        from_request = request_model == effective_model

        if from_pref:
            removed = await _cleanup_stale_model_preferences(user_id)
            _raise_model_removed(effective_model, removed)
        elif from_request:
            _raise_model_removed(effective_model, [])

    # Thread custom model input_modalities onto config
    if custom_cm and custom_cm.get("input_modalities"):
        _cow()
        config.input_modalities = custom_cm["input_modalities"]

    # Resolve reasoning effort: per-request > user pref > None (use model default)
    effective_reasoning = reasoning_effort
    if not effective_reasoning:
        effective_reasoning = model_pref.get("reasoning_effort")

    # Resolve fast mode: per-request > user pref > None
    effective_fast = fast_mode
    if effective_fast is None:
        effective_fast = model_pref.get("fast_mode")
    effective_service_tier = "priority" if effective_fast else None

    # Build the role list once (shared by the prefetch and the resolver below).
    roles = _build_roles(config, _enabled_subagents)

    # STEP 0 — best-effort batch BYOK prefetch (pure perf; see helper docstring).
    byok_cache: dict[str, dict | None] = (
        await _prefetch_byok_cache(user_id, config, effective_model, model_pref, roles)
        if is_byok
        else {}
    )

    # Main model — single primitive call. OAuth-first → BYOK → platform fallback
    # (only when allow_platform_fallback and the model is SYSTEM). The
    # ``bool(effective_reasoning)`` gate preserves the old behavior exactly: a
    # non-credentialed reasoning request gets an eager platform client (tagged
    # PLATFORM); a non-reasoning non-credentialed request leaves llm_client=None
    # (NONE) for the lazy OSS path. An OAuth-required HTTPException propagates.
    main = await resolve_model_client(
        user_id, effective_model, is_byok=is_byok, cache_key=thread_id,
        reasoning_effort=effective_reasoning, service_tier=effective_service_tier,
        allow_platform_fallback=bool(effective_reasoning),
        _pref_cache=model_pref, _byok_cache=byok_cache,
    )
    _cow()
    # Always store credential_source (even NONE) — single source of truth
    # downstream (credit gate, materialization gate, billing signal).
    config.credential_source = main.credential_source
    if main.client is not None:
        config.llm_client = main.client
    elif is_custom or is_custom_provider:
        # Custom model selected but no usable key — fail loud with a CTA.
        _raise_byok_key_required(effective_model)

    # Stash on config so the lazy ``AgentConfig.get_llm_client()`` path forwards
    # it to ``create_llm`` when no client was pre-built.
    if thread_id and config.cache_key != thread_id:
        config.cache_key = thread_id

    # Subsidiary role clients (compaction / fetch / subagent:*) + BYOK-pure
    # materialization, and fallback models. Both resolve through the primitive
    # and write disjoint config fields (subsidiary_llm_clients vs
    # fallback_llm_clients), so they run concurrently.
    await asyncio.gather(
        _resolve_role_clients(
            config, user_id, roles, model_pref, byok_cache,
            is_byok=is_byok, cache_key=thread_id,
        ),
        _resolve_fallback_clients(
            config, user_id, model_pref, byok_cache,
            is_byok=is_byok, cache_key=thread_id,
        ),
    )

    return config
