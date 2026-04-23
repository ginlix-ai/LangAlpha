"""LLM configuration resolution for the chat handler.

Resolves the effective LLM model, BYOK / OAuth client injection,
reasoning-effort overrides, and user custom-model / custom-provider
lookups.
"""

from __future__ import annotations

from enum import StrEnum
from typing import NoReturn

from ._common import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODE_MODEL_MAP = {
    "ptc": ("name", "preferred_model"),
    "flash": ("flash", "preferred_flash_model"),
}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


async def _resolve_custom_model_byok(
    user_id: str,
    model_name: str,
    custom_config: dict,
    mc,
    _pref_cache: dict | None = None,
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
            base_url = byok_config.get("base_url") or mc.get_provider_info(cp_by_name["parent_provider"]).get("base_url")
            if cp_by_name.get("use_response_api"):
                custom_config = {**custom_config, "_use_response_api": True}
            return byok_config, base_url, custom_config

    # 2. Provider field is a custom sub-provider
    cp_by_provider = await get_custom_provider_config(user_id, provider, _pref_cache=_pref_cache)
    if cp_by_provider:
        byok_config = await get_byok_config_for_provider(user_id, provider)
        if byok_config:
            base_url = byok_config.get("base_url") or mc.get_provider_info(cp_by_provider["parent_provider"]).get("base_url")
            if cp_by_provider.get("use_response_api"):
                custom_config = {**custom_config, "_use_response_api": True}
            return byok_config, base_url, custom_config

    # 3. System provider — try the provider's own slug first (variants like
    #    ``moonshot-coding`` store their key under their own slug), then the
    #    parent, then any sibling variants of the parent. The last step covers
    #    the mirror case where a custom model is tagged with the parent slug
    #    but the user only configured a variant (e.g. coding-plan) so the key
    #    lives under the variant.
    #
    #    Single batch query instead of a per-candidate round-trip: typical
    #    candidate list is 2-4 entries, and the chat hot path can't afford
    #    N round-trips per request.
    from src.server.database.api_keys import get_byok_configs_for_providers

    parent = mc.get_parent_provider(provider)
    candidates: list[str] = [provider]
    if parent and parent != provider:
        candidates.append(parent)
    root = parent if parent else provider
    for sibling in mc.get_child_variants(root):
        if sibling not in candidates:
            candidates.append(sibling)

    configs = await get_byok_configs_for_providers(user_id, candidates)
    for candidate in candidates:  # keep the provider → parent → sibling priority
        byok_config = configs.get(candidate)
        if byok_config:
            base_url = byok_config.get("base_url") or mc.get_provider_info(candidate).get("base_url")
            # Rewrite ``provider`` to the candidate that actually held the key.
            # ``create_llm_from_custom`` reads SDK / default_headers /
            # use_response_api from the provider field, so if a custom model
            # tagged ``dashscope`` resolves via its ``dashscope-coding``
            # sibling, we need the SDK to match the coding-plan endpoint —
            # otherwise we'd build a Qwen client pointed at an
            # Anthropic-shaped URL and fail every request.
            if candidate != provider:
                custom_config = {**custom_config, "provider": candidate}
            return byok_config, base_url, custom_config

    return None, None, custom_config


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _raise_byok_key_required(model_name: str) -> None:
    """Raise a user-facing HTTPException pointing the user to Settings.

    Used when a custom model is selected but no usable API key can be found
    (BYOK disabled, or BYOK enabled but no key stored). Mirrors the
    ``oauth_required`` error shape so the chat UI renders a single banner with
    a clickable CTA.
    """
    from fastapi import HTTPException

    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"API key required for custom model '{model_name}'. "
                "Enable BYOK and add the key in Settings."
            ),
            "type": "byok_key_required",
            "link": {"url": "/settings?tab=model", "label": "Open Settings"},
        },
    )


# Preference keys that hold a single model name. Used by the stale-pref
# scrubber when a saved model vanishes from the manifest.
_MODEL_PREF_KEYS = (
    "preferred_model",
    "preferred_flash_model",
    "fetch_model",
    "compaction_model",
    "summarization_model",
)


async def _cleanup_stale_model_preferences(user_id: str) -> list[tuple[str, str]]:
    """Drop stale model names from the user's prefs. Returns ``[(key, name), ...]``."""
    from src.llms.llm import LLM as LLMFactory
    from src.server.database.user import (
        invalidate_user_prefs_cache,
        upsert_user_preferences,
    )

    # Bust cache + re-read so a concurrent Settings save isn't clobbered.
    await invalidate_user_prefs_cache(user_id)
    pref = await get_model_preference(user_id)

    mc = LLMFactory.get_model_config()
    custom_models = {cm.get("name") for cm in (pref.get("custom_models") or [])}
    custom_providers = {cp.get("name") for cp in (pref.get("custom_providers") or [])}

    def resolvable(name: str | None) -> bool:
        if not name:
            return True  # empty = not set; nothing to scrub
        return (
            name in custom_models
            or name in custom_providers
            or mc.get_model_config(name) is not None
        )

    # Values: ``None`` for scalar deletes, ``list[str]`` (or ``None``) for
    # fallback_models. Merge-upsert interprets ``None`` as key deletion.
    updates: dict[str, list[str] | None] = {}
    removed: list[tuple[str, str]] = []

    for key in _MODEL_PREF_KEYS:
        val = pref.get(key)
        if val and not resolvable(val):
            updates[key] = None
            removed.append((key, val))

    fallback = pref.get("fallback_models")
    if isinstance(fallback, list):
        kept: list[str] = []
        for m in fallback:
            if resolvable(m):
                kept.append(m)
            else:
                removed.append(("fallback_models", m))
        if len(kept) != len(fallback):
            # Empty list → delete the key entirely so it doesn't linger as ``[]``
            updates["fallback_models"] = kept or None

    if updates:
        # Residual race window: between the re-read above and this upsert, a
        # Settings save could still land and get overwritten by our ``None``
        # delete. Narrow (single DB read → single DB write) and self-healing
        # (the user saves again and it sticks). Not worth a CTE or advisory
        # lock for the size of the hole.
        await upsert_user_preferences(user_id=user_id, other_preference=updates)
        await invalidate_user_prefs_cache(user_id)
        logger.info(
            f"[CHAT] Scrubbed stale model prefs for user={user_id}: {removed}"
        )

    return removed


def _raise_model_removed(
    model_name: str, removed: list[tuple[str, str]]
) -> NoReturn:
    """Raise a 400 with a CTA banner payload when a saved model no longer resolves."""
    from fastapi import HTTPException

    other = sorted({name for _, name in removed if name != model_name})
    extra = f" Also cleared: {', '.join(other)}." if other else ""

    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"Model '{model_name}' is no longer available. "
                "Your saved preference has been cleared — open Settings to pick a current model."
                + extra
            ),
            "type": "model_removed",
            "link": {"url": "/settings?tab=model", "label": "Open Settings"},
        },
    )


async def resolve_byok_llm_client(
    user_id: str,
    model_name: str,
    is_byok: bool,
    reasoning_effort: str | None = None,
    _pref_cache: dict | None = None,
):
    """
    If BYOK is active, build an LLM client for ``model_name``. Returns None
    if BYOK isn't applicable or no key is configured. ``resolve_llm_config``
    converts a None result into a user-facing ``byok_key_required``
    HTTPException for custom models on the main-model path — this function
    stays at debug log level so the user sees one error, not two.

    - System model: look up BYOK key under the model's parent provider,
      resolving variants to the parent's endpoint.
    - Custom model (custom shadows built-in when names collide): walk the
      custom/provider/variant key chain via ``_resolve_custom_model_byok``.
    - Unknown name but matches a user's ``custom_providers`` slug:
      synthesize a custom model entry and route through the user's key.

    ``classify_model`` is O(1) with ``_pref_cache`` populated, so callers
    don't need to pre-classify — pass the cache and this function does its
    own lookup.
    """
    if not is_byok:
        return None

    from src.server.database.api_keys import get_byok_config_for_provider
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
            user_id, model_name, config_entry, mc, _pref_cache=_pref_cache,
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
            user_id, model_name, synthetic_cm, mc, _pref_cache=_pref_cache,
        )
        if not byok_config:
            return None
        return create_llm_from_custom(
            custom_config,
            api_key=byok_config["api_key"],
            base_url=base_url,
        )

    # System model — BYOK key lives under the parent provider.
    provider = config_entry["provider"]
    parent = mc.get_parent_provider(provider)
    byok_config = await get_byok_config_for_provider(user_id, parent)
    if not byok_config:
        return None

    base_url = byok_config.get("base_url")
    if not base_url:
        base_url = mc.get_provider_info(parent).get("base_url")

    logger.debug(
        f"[CHAT] Resolved BYOK client for model={model_name} parent={parent} base_url={base_url or 'SDK default'}"
    )
    return create_llm(
        model_name,
        api_key=byok_config["api_key"],
        base_url=base_url,
        reasoning_effort=reasoning_effort,
    )


async def resolve_oauth_llm_client(
    user_id: str,
    model_name: str,
    reasoning_effort: str | None = None,
    service_tier: str | None = None,
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
        **({"service_tier": service_tier} if service_tier and provider != "claude-oauth" else {}),
    )


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


async def resolve_llm_config(
    base_config,
    user_id: str,
    request_model: str | None,
    is_byok: bool,
    mode: str = "ptc",
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
):
    """
    Resolve final LLM config with priority:
    per-request model > user preferred model > default.
    Then inject BYOK/OAuth client if active, and apply reasoning effort.

    Mode determines which config field and preference key to use
    (see _MODE_MODEL_MAP). Easy to extend for new modes.
    """
    from ptc_agent.config import LLMConfig

    model_field, pref_key = _MODE_MODEL_MAP[mode]
    config = base_config
    model_pref = await get_model_preference(user_id)

    # Bootstrap LLMConfig when agent_config.yaml has llm: null.
    # The user must have configured a model via the UI or per-request param.
    if config.llm is None:
        resolved_name = request_model or model_pref.get(pref_key)
        if not resolved_name:
            raise ValueError(
                "No model configured. Set llm in agent_config.yaml or select a model in Settings."
            )
        config = config.model_copy(deep=True)
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
        config = config.model_copy(deep=True)
        setattr(config.llm, model_field, request_model)
        config.llm_client = None
        logger.debug(f"[CHAT] Using per-request LLM model: {request_model}")
    else:
        preferred = model_pref.get(pref_key)
        if preferred:
            config = config.model_copy(deep=True)
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
    # rows in the platform-service DB keep working. Order matters: legacy is
    # applied first so the new key wins when both are present.
    _other_model_keys = [
        ("summarization_model", "compaction"),
        ("compaction_model", "compaction"),
        ("fetch_model", "fetch"),
    ]
    for pref_key_other, config_field in _other_model_keys:
        user_val = model_pref.get(pref_key_other)
        if user_val:
            if config is base_config:
                config = config.model_copy(deep=True)
            setattr(config.llm, config_field, user_val)

    user_fallback = model_pref.get("fallback_models")
    if user_fallback is not None:
        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm.fallback = user_fallback

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
        if config is base_config:
            config = config.model_copy(deep=True)
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
        # is tried by ``_resolve_one_with_fallbacks`` on a separate path and
        # never flows through here, so it's intentionally excluded from this
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
        if config is base_config:
            config = config.model_copy(deep=True)
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

    # Try OAuth-connected providers first (independent of BYOK toggle)
    oauth_client = await resolve_oauth_llm_client(
        user_id, effective_model, effective_reasoning,
        service_tier=effective_service_tier,
    )
    if oauth_client:
        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm_client = oauth_client
    # Then try BYOK
    elif is_byok:
        byok_client = await resolve_byok_llm_client(
            user_id, effective_model, is_byok, effective_reasoning,
            _pref_cache=model_pref,
        )
        if byok_client:
            if config is base_config:
                config = config.model_copy(deep=True)
            config.llm_client = byok_client
        elif is_custom or is_custom_provider:
            # Custom model selected but no key resolvable — fail loud with a CTA
            # instead of letting downstream create_llm() crash with a generic
            # "Model X not found in models.json" error.
            _raise_byok_key_required(effective_model)
    # Default path (system key) — apply reasoning_effort if set
    elif effective_reasoning:
        from src.llms.llm import create_llm

        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm_client = create_llm(
            effective_model, reasoning_effort=effective_reasoning
        )
        logger.debug(
            f"[CHAT] Applied reasoning_effort={effective_reasoning} to {effective_model}"
        )

    # Resolve OAuth/BYOK for subsidiary + fallback models in parallel.
    # Each model tries OAuth first, then BYOK if OAuth fails.
    import asyncio

    async def _resolve_one(model_name: str):
        """Resolve one subsidiary/fallback model. Returns (client, source).

        ``source`` tells the fallback merge loop whether a missed OAuth/BYOK
        resolve can fall back to a platform-keyed client (only valid for
        system models). On exception the source is ``None`` — callers should
        treat that as "already logged, skip".
        """
        try:
            source, _ = await classify_model(
                user_id, model_name, _pref_cache=model_pref,
            )
            client = await resolve_oauth_llm_client(user_id, model_name)
            if not client and is_byok:
                client = await resolve_byok_llm_client(
                    user_id, model_name, is_byok,
                    _pref_cache=model_pref,
                )
            return client, source
        except Exception:
            logger.error("[CHAT] Failed to resolve model %s, skipping", model_name, exc_info=True)
            return None, None

    subsidiary_pairs = [(role, m) for role, m in [("compaction", config.llm.compaction), ("fetch", config.llm.fetch)] if m]
    fallback_models = config.llm.fallback or []

    all_models = [m for _, m in subsidiary_pairs] + list(fallback_models)
    if all_models:
        results = await asyncio.gather(*[_resolve_one(m) for m in all_models])

        sub_count = len(subsidiary_pairs)
        for i, (role, sub_name) in enumerate(subsidiary_pairs):
            client, source = results[i]
            if client:
                if config is base_config:
                    config = config.model_copy(deep=True)
                config.subsidiary_llm_clients[role] = client
                continue
            # Mirror the fallback-loop warning so silently-dropped subsidiaries
            # (e.g. user picked a custom compaction model without a BYOK key)
            # surface in the logs. Main model stays hard-raised at the preflight;
            # subsidiaries degrade: compaction falls back to the main llm_client,
            # fetch re-constructs from the name via the factory.
            if source is not None and source != ModelSource.SYSTEM:
                logger.warning(
                    "[CHAT] Subsidiary role '%s' model '%s' is a custom model "
                    "without a usable BYOK key — falling back to default. "
                    "Add a key in Settings to enable.",
                    role,
                    sub_name,
                )

        # Merge resolved OAuth/BYOK clients with platform fallbacks.
        # For each fallback model: use the pre-resolved client if available,
        # otherwise create a platform-keyed client so no model is silently dropped.
        # Custom fallback models without a usable key are skipped with a
        # visible warning — we reuse the source already computed by
        # ``_resolve_one`` instead of re-classifying.
        from src.llms.llm import create_llm as _create_llm

        fallback_results = results[sub_count:]
        merged_fallbacks = []
        byok_count = 0
        for i, model_name in enumerate(fallback_models):
            client, source = fallback_results[i]
            if client:
                merged_fallbacks.append(client)
                byok_count += 1
                continue
            if source is None:
                # ``_resolve_one`` caught an exception and already logged it.
                continue
            if source != ModelSource.SYSTEM:
                # Custom (or unknown) fallback without a usable key — can't
                # build a platform client for a non-system name.
                logger.warning(
                    "[CHAT] Fallback model '%s' is a custom model without a "
                    "usable BYOK key — skipping. Add a key in Settings to enable.",
                    model_name,
                )
                continue
            try:
                merged_fallbacks.append(_create_llm(model_name))
            except Exception:
                logger.warning("[CHAT] Failed to create platform fallback for %s, skipping", model_name)

        if merged_fallbacks:
            if config is base_config:
                config = config.model_copy(deep=True)
            config.fallback_llm_clients = merged_fallbacks
            if byok_count:
                logger.debug(
                    f"[CHAT] Resolved {byok_count}/{len(fallback_models)} fallback models via OAuth/BYOK"
                )

    return config
