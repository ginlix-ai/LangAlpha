"""LLM configuration resolution for the chat handler.

Resolves the effective LLM model, BYOK / OAuth client injection,
reasoning-effort overrides, and user custom-model / custom-provider
lookups.
"""

from __future__ import annotations

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
    1. model name as a custom sub-provider (model and provider share a name)
    2. custom model's provider field as a custom sub-provider
    3. parent of the custom model's provider (system provider)
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

    # 3. System/parent provider
    parent = mc.get_parent_provider(provider)
    byok_config = await get_byok_config_for_provider(user_id, parent)
    if byok_config:
        base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
        return byok_config, base_url, custom_config

    return None, None, custom_config


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def resolve_byok_llm_client(
    user_id: str,
    model_name: str,
    is_byok: bool,
    reasoning_effort: str | None = None,
    _pref_cache: dict | None = None,
):
    """
    If BYOK is active, look up the user's key for the model's **parent** provider
    and return a fresh LLM client.  Returns None if BYOK isn't applicable.

    Auto-reroutes from sub-providers (e.g., platform variants) to the parent provider's
    official endpoint (or user's custom base_url if set).
    """
    if not is_byok:
        return None

    from src.server.database.api_keys import get_byok_config_for_provider
    from src.llms.llm import LLM as LLMFactory, create_llm, create_llm_from_custom

    mc = LLMFactory.get_model_config()
    model_info = mc.get_model_config(model_name)

    if not model_info:
        # Fall back to user's custom models
        custom_config = await get_custom_model_config(user_id, model_name, _pref_cache=_pref_cache)

        if not custom_config:
            # Check if model_name is a BYOK custom provider name
            # (user selected a custom provider directly as their model)
            cp_config = await get_custom_provider_config(user_id, model_name, _pref_cache=_pref_cache)
            if cp_config:
                custom_config = {
                    "name": model_name,
                    "model_id": model_name,
                    "provider": cp_config["parent_provider"],
                }
            else:
                return None

        byok_config, base_url, custom_config = await _resolve_custom_model_byok(
            user_id, model_name, custom_config, mc, _pref_cache=_pref_cache,
        )
        if not byok_config:
            logger.warning(
                f"[CHAT] No BYOK key found for custom model={model_name} "
                f"provider={custom_config['provider']}. Falling back to system default."
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

    provider = model_info["provider"]
    parent = mc.get_parent_provider(provider)

    # Look up BYOK key for parent provider (resolves platform variants to parent)
    byok_config = await get_byok_config_for_provider(user_id, parent)
    if not byok_config:
        return None

    # Resolve base_url: user custom > parent provider's official > None (SDK default)
    base_url = byok_config.get("base_url")
    if not base_url:
        parent_info = mc.get_provider_info(parent)
        base_url = parent_info.get("base_url")  # None for anthropic = SDK default

    logger.debug(
        f"[CHAT] Resolved BYOK client for model={model_name} parent={parent} base_url={base_url or 'SDK default'}"
    )
    # Always pass base_url (even None) to override the sub-provider's URL via sentinel
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
            summarization=model_pref.get("summarization_model") or model_pref.get("preferred_flash_model"),
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

    # Apply other model overrides from user preferences
    _other_model_keys = [
        ("summarization_model", "summarization"),
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

    # Resolve the effective model from whichever field we just set
    effective_model = getattr(config.llm, model_field, None) or config.llm.name

    # If effective model is a custom model but BYOK is off, fall back to system default
    from src.llms.llm import LLM as LLMFactory

    mc = LLMFactory.get_model_config()
    is_system_model = mc.get_model_config(effective_model) is not None
    if not is_system_model:
        custom_cm = await get_custom_model_config(user_id, effective_model, _pref_cache=model_pref)
        is_custom = custom_cm is not None
        is_custom_provider = not is_custom and await get_custom_provider_config(user_id, effective_model, _pref_cache=model_pref) is not None
        if (is_custom or is_custom_provider) and not is_byok:
            # Custom model/provider requires BYOK — revert to system default
            default_model = (getattr(base_config.llm, model_field, None) or base_config.llm.name) if base_config.llm else None
            if not default_model:
                raise ValueError(
                    f"Custom model {effective_model} requires BYOK but no system default model is configured to fall back to."
                )
            logger.warning(
                f"[CHAT] Custom model {effective_model} selected but BYOK disabled, "
                f"falling back to system default: {default_model}"
            )
            effective_model = default_model
            config = base_config
            custom_cm = None  # Reverted to system model

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
        try:
            client = await resolve_oauth_llm_client(user_id, model_name)
            if not client and is_byok:
                client = await resolve_byok_llm_client(
                    user_id, model_name, is_byok, _pref_cache=model_pref,
                )
            return client
        except Exception:
            logger.error("[CHAT] Failed to resolve model %s, skipping", model_name, exc_info=True)
            return None

    subsidiary_pairs = [(role, m) for role, m in [("summarization", config.llm.summarization), ("fetch", config.llm.fetch)] if m]
    fallback_models = config.llm.fallback or []

    all_models = [m for _, m in subsidiary_pairs] + list(fallback_models)
    if all_models:
        results = await asyncio.gather(*[_resolve_one(m) for m in all_models])

        sub_count = len(subsidiary_pairs)
        for i, (role, _) in enumerate(subsidiary_pairs):
            if results[i]:
                if config is base_config:
                    config = config.model_copy(deep=True)
                config.subsidiary_llm_clients[role] = results[i]

        # Merge resolved OAuth/BYOK clients with platform fallbacks.
        # For each fallback model: use the pre-resolved client if available,
        # otherwise create a platform-keyed client so no model is silently dropped.
        from src.llms.llm import create_llm as _create_llm

        fallback_results = results[sub_count:]
        merged_fallbacks = []
        byok_count = 0
        for i, model_name in enumerate(fallback_models):
            if fallback_results[i]:
                merged_fallbacks.append(fallback_results[i])
                byok_count += 1
            else:
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
