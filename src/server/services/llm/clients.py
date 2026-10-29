"""Building an LLM client for a model, and recording which credential built it.

The OAuth / BYOK / platform ladder, and the provider walk that finds a stored
key under a slug's own family. Separate from ``config`` because choosing a
model for a turn and building a client for a model are different questions.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from ptc_agent.config.agent import CredentialSource
from src.llms.model_spec import (
    canonical_reasoning_efforts,
    reasoning_block,
    summary_reasoning_effort,
)
from src.llms.preferences import resolve_tuning, resolve_turn_tuning

from . import logger, user_models
from .availability import (
    raise_byok_key_required,
    raise_oauth_plan_no_api_access,
    raise_oauth_reauth_required,
    raise_oauth_refresh_unavailable,
)
from .byok import byok_candidates, call_endpoint, walk_byok_candidates
from .user_models import ModelSource

#: Providers that accept a request-time service tier. The rule had no owner:
#: an ``access_type == "oauth"`` gate, a ``claude-oauth`` exclusion and four
#: routes that hard-coded ``None`` between them meant one route could emit the
#: setting while the rest silently dropped it. Nothing in the manifest declares
#: tier support, so a provider is assumed not to accept one until it is added
#: here. Every route asks this function, so widening is a one-line change.
_SERVICE_TIER_PROVIDERS = frozenset({"codex-oauth"})


def supports_service_tier(provider: str | None) -> bool:
    """Whether ``provider`` accepts a request-time service tier."""
    return provider in _SERVICE_TIER_PROVIDERS


def _tier_kwargs(service_tier: str | None, provider: str | None) -> dict[str, str]:
    """``service_tier=`` for a provider that accepts one, nothing otherwise."""
    return (
        {"service_tier": service_tier}
        if service_tier and supports_service_tier(provider)
        else {}
    )


@dataclass(frozen=True)
class ResolvedClient:
    """A resolved LLM client plus its model and credential provenance.

    ``model_source`` (a ``ModelSource``) and ``credential_source`` are
    orthogonal — a BYOK user on a system-catalog model yields SYSTEM + BYOK.
    """

    client: Any | None
    model_source: Any | None  # ModelSource
    credential_source: CredentialSource


def is_own_key_turn(config: Any) -> bool:
    """Did the user's own credential pay the vendor for this turn?

    The billing question, and deliberately not the same one as ``is_byok``,
    which asks whether to attempt the BYOK ladder. For an OAuth-only user the
    two answers differ, and reading the ladder's answer as the billing one
    meters an own-key turn as though the platform had funded it.

    Taken off the RESOLVED credential rather than off a flag a caller passed
    down, because the caller that knows which ladder to try is not always the
    one that knows who ends up paying.
    """
    return getattr(config, "credential_source", None) in (
        CredentialSource.OAUTH,
        CredentialSource.BYOK,
    )


def may_use_platform_key(config: Any, *, users_choice: bool) -> bool:
    """May a client this turn builds run on the platform's key?

    A turn moves onto the platform's key only for a model the user picked, or
    when the platform already pays for it, so a refused key or subscription
    never quietly becomes the same model on credits.
    """
    return users_choice or not is_own_key_turn(config)


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
       Unlike a system model's walk, this one crosses a separate credential
       (``own_api_key``): the client is built against the endpoint of the slug
       that held the key, so the key still goes to the service that issued it.
    """
    from src.server.database.api_keys import get_byok_config_for_provider

    provider = custom_config["provider"]

    # 1. Model name is itself a custom sub-provider with a key
    cp_by_name = await user_models.get_custom_provider_config(user_id, model_name, _pref_cache=_pref_cache)
    if cp_by_name:
        byok_config = await get_byok_config_for_provider(user_id, model_name)
        if byok_config:
            parent = cp_by_name["parent_provider"]
            base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
            custom_config = user_models.route_custom_entry(custom_config, cp_by_name, mc)
            return byok_config, base_url, custom_config

    # 2. Provider field is a custom sub-provider
    cp_by_provider = await user_models.get_custom_provider_config(user_id, provider, _pref_cache=_pref_cache)
    if cp_by_provider:
        byok_config = await get_byok_config_for_provider(user_id, provider)
        if byok_config:
            parent = cp_by_provider["parent_provider"]
            base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
            custom_config = user_models.route_custom_entry(custom_config, cp_by_provider, mc)
            return byok_config, base_url, custom_config

    # 3. System provider — walk [provider → parent → sibling variants] for a
    #    stored key. The sibling step covers the mirror case where a custom
    #    model is tagged with the parent slug but the user only configured a
    #    variant (e.g. coding-plan) so the key lives under the variant.
    byok_config, holding = await walk_byok_candidates(
        user_id, provider, mc, _byok_cache=_byok_cache, any_credential=True,
    )
    if byok_config:
        # ``create_llm_from_custom`` reads SDK / default_headers /
        # use_response_api from the provider field, so it names the endpoint
        # called: a GLM client pointed at an Anthropic-shaped URL fails every
        # request.
        endpoint = call_endpoint(provider, holding, mc, custom=True)
        base_url = byok_config.get("base_url") or mc.get_provider_info(endpoint).get("base_url")
        if endpoint != provider:
            custom_config = {**custom_config, "provider": endpoint}
        return byok_config, base_url, custom_config

    return None, None, custom_config


async def _custom_provider_model(
    user_id: str, model_name: str, _pref_cache: dict | None = None
) -> dict | None:
    """``model_name`` as a custom model on the user's custom provider of that
    name, or None when it names none: a provider slug picked as the model runs
    on that provider's key, so its key decides whether it runs at all."""
    cp_config = await user_models.get_custom_provider_config(
        user_id, model_name, _pref_cache=_pref_cache,
    )
    if not cp_config:
        return None
    return {
        "name": model_name,
        "model_id": model_name,
        "provider": cp_config["parent_provider"],
    }


async def _egress_kwargs(model_name: str, base_url: str | None, params: dict | None = None) -> dict:
    """``egress_transport=`` for a client on the user's own key that goes where
    the user pointed it, in platform mode.

    Only the user's own endpoints are held: a base URL saved with the key, an
    endpoint or connection in a custom model's parameters, a local provider's
    default. One the policy refuses is refused here, before the client exists,
    so the user is told why instead of a call failing. So are Vertex keys in the
    parameters, even beside an operator's endpoint, which they would replace.
    """
    from . import egress

    if not egress.enforced():
        return {}
    urls = egress.user_endpoints(base_url, params)
    await egress.refuse_model_endpoints(model_name, urls, params)
    if not urls and not egress.connection_params(params):
        return {}
    return {"egress_transport": egress.sdk_transport}


async def resolve_byok_llm_client(
    user_id: str,
    model_name: str,
    is_byok: bool,
    reasoning_effort: str | None = None,
    _pref_cache: dict | None = None,
    cache_key: str | None = None,
    _byok_cache: dict | None = None,
    service_tier: str | None = None,
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
    into ``walk_byok_candidates`` (see its contract). ``service_tier`` is
    forwarded only to providers ``supports_service_tier`` accepts.
    """
    if not is_byok:
        return None

    from src.llms.llm import LLM as LLMFactory, create_llm, create_llm_from_custom

    mc = LLMFactory.get_model_config()
    source, config_entry = await user_models.classify_model(
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
            reasoning_effort=reasoning_effort,
            **_tier_kwargs(service_tier, custom_config["provider"]),
            **await _egress_kwargs(model_name, base_url, custom_config.get("parameters")),
        )

    # Unknown name — last-chance check for a custom-provider slug. Covers the
    # edge case where a user typed their custom provider slug as the model name.
    if source == ModelSource.UNKNOWN:
        synthetic_cm = await _custom_provider_model(user_id, model_name, _pref_cache=_pref_cache)
        if synthetic_cm is None:
            return None
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
            reasoning_effort=reasoning_effort,
            **_tier_kwargs(service_tier, custom_config["provider"]),
            **await _egress_kwargs(model_name, base_url, custom_config.get("parameters")),
        )

    # System model — the BYOK key may live under the model's own provider
    # slug (coding-plan variants store it there), its parent, or a sibling
    # variant that shares its credential. Walk all three; but pin SDK +
    # base_url to the MODEL'S OWN provider, never the candidate that merely
    # held the key.
    provider = config_entry["provider"]
    byok_config, holding = await walk_byok_candidates(
        user_id, provider, mc, _byok_cache=_byok_cache,
    )
    if not byok_config:
        return None
    # base_url precedence: a user custom base_url on the holding slug wins;
    # otherwise the endpoint ``call_endpoint`` picks.
    endpoint = call_endpoint(provider, holding, mc, custom=False)
    base_url = byok_config.get("base_url") or mc.get_provider_info(endpoint).get("base_url")
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
        **_tier_kwargs(service_tier, provider),
        **await _egress_kwargs(model_name, base_url),
    )


async def resolve_oauth_llm_client(
    user_id: str,
    model_name: str,
    reasoning_effort: str | None = None,
    service_tier: str | None = None,
    cache_key: str | None = None,
    *,
    custom_entry: dict | None = None,
):
    """Resolve OAuth-connected LLM client. Independent of BYOK toggle.

    ``custom_entry`` is the user's custom model of this name, which shadows the
    manifest as everywhere else. One saved on a subscription provider (an
    untuned model the subscription lists) runs on its token like a built-in.
    """
    from fastapi import HTTPException

    from src.llms.llm import LLM as LLMFactory, create_llm, create_llm_from_custom
    from src.server.services.oauth_health import (
        OAuthPlanNoApiAccess,
        OAuthReauthRequired,
        OAuthRefreshUnavailable,
        valid_token,
    )
    from src.server.services.oauth_providers import SUBSCRIPTIONS

    mc = LLMFactory.get_model_config()
    model_info = custom_entry if custom_entry is not None else mc.get_model_config(model_name)
    if not model_info:
        return None

    provider = model_info["provider"]
    subscription = SUBSCRIPTIONS.get(provider)
    if subscription is None:
        return None

    try:
        token = await valid_token(user_id, provider)
    except OAuthReauthRequired as e:
        raise_oauth_reauth_required(subscription, model_name, e.code)
    except OAuthPlanNoApiAccess:
        raise_oauth_plan_no_api_access(subscription)
    except OAuthRefreshUnavailable:
        raise_oauth_refresh_unavailable(subscription)
    if token is None:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Model '{model_name}' requires a connected {provider} account.",
                "type": "oauth_required",
                "link": {"url": "/setup/method", "label": "Connect account"},
            },
        )

    # Plan-gated OAuth models (manifest `oauth_plans`): reject early with a
    # clear message instead of an opaque upstream error. Unknown plan_type
    # fails open — the upstream backend remains the authority.
    allowed_plans = model_info.get("oauth_plans")
    plan_type = (token.plan_type or "").lower()
    if allowed_plans and plan_type and plan_type not in {p.lower() for p in allowed_plans}:
        raise HTTPException(
            status_code=400,
            detail={
                "message": (
                    f"Model '{model_name}' is not available on your connected "
                    f"'{plan_type}' plan (available on: {', '.join(allowed_plans)})."
                ),
                "type": "oauth_plan_unsupported",
            },
        )

    if not token.access_token:
        logger.error(f"[CHAT] OAuth token is empty for provider={provider}")
        return None

    headers = subscription.request_headers(token)
    logger.debug(f"[CHAT] Resolved {subscription.account_name} OAuth client for model={model_name}")
    if custom_entry is not None:
        from . import egress

        params = custom_entry.get("parameters") or {}
        # The token is the user's sign-in, for the provider that issued it:
        # parameters that move the endpoint would send it elsewhere.
        if egress.routing_params(params):
            raise HTTPException(
                status_code=400,
                detail={
                    "message": (
                        f"Model '{model_name}' runs on your {subscription.account_name} "
                        "sign-in and cannot set its own endpoint. Remove it from its settings."
                    ),
                    "type": "oauth_custom_endpoint",
                },
            )
        # A custom model's headers ride its parameters, over any it saved.
        extra = (
            {"default_headers": {**(params.get("default_headers") or {}), **headers}}
            if headers else {}
        )
        return create_llm_from_custom(
            custom_entry,
            api_key=token.access_token,
            cache_key=cache_key,
            reasoning_effort=reasoning_effort,
            **_tier_kwargs(service_tier, provider),
            **await _egress_kwargs(model_name, None, params),
            **extra,
        )
    return create_llm(
        model_name,
        api_key=token.access_token,
        default_headers=headers or None,
        reasoning_effort=reasoning_effort,
        cache_key=cache_key,
        **_tier_kwargs(service_tier, provider),
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

    ``service_tier`` is offered to all three routes; each drops it unless
    ``supports_service_tier`` says its provider takes one.
    """
    source, entry = await user_models.classify_model(user_id, model_name, _pref_cache=_pref_cache)

    client = await resolve_oauth_llm_client(
        user_id, model_name, reasoning_effort,
        service_tier=service_tier, cache_key=cache_key,
        custom_entry=entry if source == ModelSource.CUSTOM else None,
    )
    if client:
        return ResolvedClient(client, source, CredentialSource.OAUTH)

    if is_byok:
        client = await resolve_byok_llm_client(
            user_id, model_name, is_byok, reasoning_effort,
            _pref_cache=_pref_cache, cache_key=cache_key, _byok_cache=_byok_cache,
            service_tier=service_tier,
        )
        if client:
            return ResolvedClient(client, source, CredentialSource.BYOK)

    # Platform fallback — only for SYSTEM-catalog models. Reached when OAuth and
    # BYOK both miss.
    if allow_platform_fallback and source == ModelSource.SYSTEM:
        from src.llms.llm import MissingProviderKeyError, create_llm

        try:
            client = create_llm(
                model_name, reasoning_effort=reasoning_effort, cache_key=cache_key,
                **_tier_kwargs(service_tier, entry.get("provider")),
            )
        except MissingProviderKeyError as exc:
            # Built eagerly only because an effort was asked for. Without a
            # key the turn takes the lazy path a turn with no effort takes,
            # and fails there as a run the user can see and retry, not as a
            # 500 before it starts.
            logger.warning("[CHAT] No platform client for %s: %s", model_name, exc)
            return ResolvedClient(None, source, CredentialSource.NONE)
        return ResolvedClient(client, source, CredentialSource.PLATFORM)

    return ResolvedClient(None, source, CredentialSource.NONE)


# ---------------------------------------------------------------------------
# LLM roles — compaction / fetch / per-subagent. A dumb record + builder so the
# resolution loop is a flat iteration instead of bespoke per-role branches.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LLMRole:
    """A subsidiary model slot to resolve a client for.

    ``key`` is the ``subsidiary_llm_clients`` key ("compaction" |
    "compaction_fallback" | "fetch" | "subagent:<name>"). ``fallback_to_main``
    controls whether a keyless role inherits a copy of the main client at
    materialization time.

    ``reasoning_effort`` is the role model's own resolved level, because a
    level set on a model is a fact about that model wherever it runs. The
    compaction role is the exception: a summary runs at the summary level
    (``summary_reasoning_effort``), whatever the model is set to for
    conversations, and so does the model a summary falls back to.
    ``service_tier`` stays unset by decision rather than by
    omission: priority routing is bought per call, so honoring it here would
    multiply one fast turn across compaction, fetch and every enabled
    subagent. The resolver reads it off the role so that decision has one
    place to change.

    ``effort_follows`` marks ``reasoning_effort`` as a level a subagent sets
    for itself. Unlike a model's own level it belongs to the subagent, so it
    still applies when the role falls back to the main model. ``own_client``
    is a role that must always be built as a client (see ``_needs_own_client``).
    """

    key: str
    model: str | None
    fallback_to_main: bool = True
    service_tier: str | None = None
    reasoning_effort: str | None = None
    effort_follows: bool = False
    own_client: bool = False


def _summary_effort(model_pref: dict, model: str | None) -> str | None:
    """The level a summary runs ``model`` at, by its own effort ladder."""
    return summary_reasoning_effort(canonical_reasoning_efforts(
        reasoning_block(user_models.model_entry(model_pref, model)).get("efforts")
    ))


def role_registry(config, model_pref, turn_model: str | None = None) -> list[LLMRole]:
    """The compaction and fetch roles to resolve for this request, in order.

    A subagent's roles come from ``subagents.subagent_roles``. Each role
    carries the tuning its own model resolves to, compaction's effort aside
    (see ``LLMRole``). The turn-level override is deliberately not applied: it
    names the model the composer picked, not whichever model a role pins.
    """
    compaction = config.llm.compaction_name
    # A summary written by a model picked for it falls back to the turn's
    # model; one on the turn's own model, to the model the turn falls back to,
    # since the main client would be the model that just failed.
    if turn_model and compaction != turn_model:
        summary_fallback = turn_model
    else:
        summary_fallback = next(iter(config.llm.fallbacks_for(compaction)), None)
    roles = [
        LLMRole(
            "compaction", compaction,
            reasoning_effort=_summary_effort(model_pref, compaction),
        ),
        LLMRole(
            "compaction_fallback", summary_fallback, fallback_to_main=False,
            reasoning_effort=_summary_effort(model_pref, summary_fallback),
        ),
        LLMRole(
            "fetch", config.llm.fetch_name,
            reasoning_effort=resolve_tuning(model_pref, config.llm.fetch_name).reasoning_effort,
        ),
    ]
    return [r for r in roles if r.model]


async def _prefetch_byok_cache(
    user_id: str, config, effective_model: str, model_pref: dict,
    roles: list[LLMRole],
) -> dict[str, dict | None]:
    """Best-effort batch BYOK prefetch → tri-state cache for ``walk_byok_candidates``.

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
            src_, cfg = await user_models.classify_model(user_id, m, _pref_cache=model_pref)
            # SYSTEM (models.json) and CUSTOM (user config) both carry a
            # "provider" slug; UNKNOWN carries nothing.
            prov = cfg.get("provider") if src_ != ModelSource.UNKNOWN else None
            if prov:
                all_slugs.update(byok_candidates(prov, mc))
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


def _tier_for(fast_mode: bool | None) -> str | None:
    """The service tier a fast turn buys. One place, so "fast" means one thing."""
    return "priority" if fast_mode else None


def _stamp(config, client, prompt_guidance: str | None) -> None:
    """Put the turn-shaping facts the factory cannot see on a built client.

    Every client a turn can reach passes through here exactly once, at the
    point it is built. Stamping at prompt-build time instead reached whichever
    single client the prompt builder happened to hold.
    """
    from src.llms.llm import stamp_call_metadata

    stamp_call_metadata(
        client,
        prompt_guidance=prompt_guidance,
        compaction_profile=config.compaction.profile,
    )


async def _resolve_role_clients(
    config, user_id: str, roles: list[LLMRole], model_pref: dict,
    byok_cache: dict, *, is_byok: bool, cache_key: str | None,
    main_model: str | None = None,
) -> None:
    """Resolve compaction/fetch/subagent role clients onto ``config`` in place.

    Assumes ``config`` is already a copy and ``config.credential_source`` /
    ``config.llm_client`` are set. A keyless role for an OAUTH/BYOK user is
    seeded with a copy of the main client (BYOK-pure), while PLATFORM/NONE users
    store nothing so the cheap name-based path stays -- unless the role is
    tuned or a subagent's, which ``_needs_own_client`` decides. Each role's I/O
    (OAuth check + BYOK walk) runs concurrently; writes happen after the gather
    so the SSE hot path waits one round-trip, not N. A keyless compaction role
    gets ``main_model`` at the summary level instead, so a summary keeps its
    level, and a keyless role with a set effort gets it at that effort.
    """
    # A role is never the user's pick of a stand-in, so it stays on the key
    # the turn runs on.
    platform_ok = may_use_platform_key(config, users_choice=False)

    def _needs_own_client(role: LLMRole) -> bool:
        """Whether this role has to be materialized rather than left to its name.

        A client is what carries tuning here; a bare model name carries none, so
        the cheap name path rebuilds from manifest defaults and drops the role's
        effort. A subagent's name never reaches the manifest at all: the agent
        factory hands a bare string to the chat-model initializer, which knows
        neither the catalog's providers nor its keys. OAUTH/BYOK users are
        excluded because the block below already gives them the main client,
        and a platform client would move a role off the user's own key.
        """
        return (role.reasoning_effort is not None or role.own_client) and platform_ok

    async def _main_client_at(model: str, effort: str | None):
        """``model`` at ``effort``, on the key the turn itself runs on, or None."""
        try:
            rc = await resolve_model_client(
                user_id, model, is_byok=is_byok, cache_key=cache_key,
                allow_platform_fallback=platform_ok,
                reasoning_effort=effort,
                _pref_cache=model_pref, _byok_cache=byok_cache,
            )
        except Exception:
            logger.warning(
                "[CHAT] No client for %s at %s", model, effort, exc_info=True
            )
            return None
        if rc.client is not None:
            _stamp(config, rc.client, config.prompt_guidance)
        return rc.client

    async def _resolve_one(role: LLMRole):
        try:
            return role, await resolve_model_client(
                user_id, role.model, is_byok=is_byok, cache_key=cache_key,
                allow_platform_fallback=_needs_own_client(role),
                service_tier=role.service_tier,
                reasoning_effort=role.reasoning_effort,
                _pref_cache=model_pref, _byok_cache=byok_cache,
            )
        except Exception:
            logger.error(
                "[CHAT] Failed to resolve role %s model %s, skipping",
                role.key, role.model, exc_info=True,
            )
            return role, None

    # Every role runs the model it names until the main-client fallback below
    # says otherwise, so this is also the answer for the name-based path, where
    # there is no client to carry it.
    for role in roles:
        config.role_prompt_guidance[role.key] = user_models.guidance_for(model_pref, role.model)

    for role, rc in await asyncio.gather(*(_resolve_one(r) for r in roles)):
        if rc is None:
            continue
        if rc.client is not None:
            _stamp(config, rc.client, config.role_prompt_guidance[role.key])
            config.subsidiary_llm_clients[role.key] = rc.client
        elif rc.model_source == ModelSource.CUSTOM:
            logger.warning(
                "[CHAT] Role '%s' model '%s' is a custom model without a usable "
                "BYOK key — falling back to default.",
                role.key, role.model,
            )
        elif rc.model_source == ModelSource.UNKNOWN:
            # ``resolve_llm_config`` already dropped a missing saved model, so
            # this is deployment config or a custom-provider slug with no key.
            logger.warning(
                "[CHAT] Role '%s' model '%s' is in neither the model manifest "
                "nor the user's custom models.",
                role.key, role.model,
            )

    # A subagent's set effort is the user's choice for that subagent, whatever
    # serves it, so a role whose own model got no client runs the main model
    # at that effort before anything coarser. A model's own saved level never
    # moves this way: it is a fact about that model. One client per level
    # serves every role at it; ``client_for_role`` hands each a copy.
    if main_model:
        pending = [
            role for role in roles
            if role.effort_follows
            and role.model != main_model
            and role.key not in config.subsidiary_llm_clients
        ]
        efforts = list(dict.fromkeys(role.reasoning_effort for role in pending))
        at_effort = dict(zip(efforts, await asyncio.gather(
            *(_main_client_at(main_model, effort) for effort in efforts)
        )))
        for role in pending:
            client = at_effort[role.reasoning_effort]
            if client is not None:
                config.subsidiary_llm_clients[role.key] = client
                if config.prompt_guidance:
                    config.role_prompt_guidance[role.key] = config.prompt_guidance

    # BYOK-pure write-time materialization. Gate purely on the credential
    # SIGNAL: by the primitive's invariant OAUTH/BYOK ⟹ llm_client is set, so a
    # main-client copy is safe. PLATFORM/NONE users store nothing → cheap name
    # path stays (a non-BYOK reasoning user's PLATFORM client is NOT copied).
    # The explicit None check makes the invariant a guard, not an assumption.
    if is_own_key_turn(config) and config.llm_client is not None:
        for role in roles:
            if role.fallback_to_main and role.key not in config.subsidiary_llm_clients:
                summary_client = (
                    await _main_client_at(main_model, _summary_effort(model_pref, main_model))
                    if role.key == "compaction" and main_model
                    else None
                )
                config.subsidiary_llm_clients[role.key] = (
                    summary_client or config.llm_client.model_copy()
                )
                # The copy runs the MAIN model, so the level resolved for the
                # role's own name stops being the answer for it.
                if config.prompt_guidance:
                    config.role_prompt_guidance[role.key] = config.prompt_guidance
                logger.info(
                    "[CHAT] Role '%s' has no own key; falling back to the user's "
                    "main client (cost shifts to main-model rate).",
                    role.key,
                )


async def _resolve_fallback_clients(
    config, user_id: str, model_pref: dict, byok_cache: dict,
    *, is_byok: bool, cache_key: str | None, users_fallbacks: bool,
    reasoning_effort: str | None = None, fast_mode: bool | None = None,
) -> None:
    """Resolve ``config.llm.fallback`` names into client instances in place.

    A turn never moves onto a payer the user did not choose. Fallbacks the
    user saved (``users_fallbacks``) are their choice, each running as they
    picked it, on credits included. The deployment's own list is not: on a
    turn the user's key or subscription pays for, a default fallback they
    hold no credential for is skipped rather than built on the platform's
    key (``may_use_platform_key``). On any other turn a SYSTEM fallback
    without a user key yields a platform client. Custom/unknown fallbacks
    without a usable key warn + skip.

    A fallback takes the turn-level overrides, unlike a role: it stands in for
    the model the request named, so "think harder this turn" still applies.
    Everything under that resolves on the fallback's own name, so the primary's
    speed is not charged to a model whose profile did not ask for it.
    """
    config.fallback_users_choice = users_fallbacks
    fallback_models = config.llm.fallback or []
    if not fallback_models:
        config.fallback_llm_clients = []
        config.fallback_llm_names = []
        return

    platform_ok = may_use_platform_key(config, users_choice=users_fallbacks)

    async def _resolve_one(model_name: str):
        try:
            tuning = resolve_turn_tuning(
                model_pref, model_name,
                reasoning_effort=reasoning_effort, fast_mode=fast_mode,
            )
            return model_name, await resolve_model_client(
                user_id, model_name, is_byok=is_byok, cache_key=cache_key,
                allow_platform_fallback=platform_ok,
                service_tier=_tier_for(tuning.fast_mode),
                reasoning_effort=tuning.reasoning_effort,
                _pref_cache=model_pref, _byok_cache=byok_cache,
            )
        except Exception:
            logger.error(
                "[CHAT] Failed to resolve fallback model %s, skipping",
                model_name, exc_info=True,
            )
            return model_name, None

    merged_fallbacks = []
    merged_fallback_names = []
    byok_count = 0
    # Resolve concurrently; append in declared order to preserve fallback priority.
    for model_name, fc in await asyncio.gather(
        *(_resolve_one(m) for m in fallback_models)
    ):
        if fc is None:
            continue
        if fc.client is not None:
            # The system prompt is rendered once, from the primary's guidance,
            # and a mid-turn swap to this client does not re-render it. Stamping
            # the fallback's own level would claim scaffolding the turn never
            # applied, which is worse than naming the level it really ran.
            _stamp(config, fc.client, config.prompt_guidance)
            merged_fallbacks.append(fc.client)
            merged_fallback_names.append(model_name)
            if fc.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK):
                byok_count += 1
        elif fc.model_source == ModelSource.CUSTOM:
            logger.warning(
                "[CHAT] Fallback model '%s' is a custom model without a "
                "usable BYOK key — skipping. Add a key in Settings to enable.",
                model_name,
            )
        elif fc.model_source == ModelSource.UNKNOWN:
            logger.warning(
                "[CHAT] Fallback model '%s' is in neither the model manifest nor "
                "the user's custom models — skipping.",
                model_name,
            )
        elif not platform_ok:
            logger.info(
                "[CHAT] Default fallback model '%s' has no key or subscription "
                "of the user's and would run on the deployment's key; skipping.",
                model_name,
            )
        # else: SYSTEM with no client on a platform turn (a missing platform
        # key) — guard by skipping.

    # Empty is authoritative too: every candidate may have been rejected.
    # Leaving None would let agent construction retry the original names.
    config.fallback_llm_clients = merged_fallbacks
    config.fallback_llm_names = merged_fallback_names
    if byok_count:
        logger.debug(
            f"[CHAT] Resolved {byok_count}/{len(fallback_models)} fallback models via OAuth/BYOK"
        )


async def resolve_clients(
    config, user_id: str, effective_model: str, model_pref: dict,
    subagent_roles: list[LLMRole], *, is_byok: bool, thread_id: str | None,
    reasoning_effort: str | None, fast_mode: bool | None, user_defined: bool,
    users_fallbacks: bool,
) -> None:
    """Build every client this turn can reach, tuned and stamped as it is built.

    Main model, subsidiary roles and fallbacks all go through one primitive, so
    a trace reads the guidance and compaction facts off the client that made
    the call rather than off whichever one a prompt builder happened to hold.

    ``reasoning_effort`` and ``fast_mode`` are the request's own overrides, not
    a settled answer: which model each client runs differs, and a level is
    resolved against the model that will run it. Resolving once for the main
    model and reusing it is how a fallback came to be charged the primary's
    speed.

    ``subagent_roles`` are the roles of the subagents this build compiles;
    the role list is built here because both things that read it, the BYOK
    prefetch and the role resolver, are below this line. ``users_fallbacks``
    says the fallback list is one the user saved (``select_model``).
    """
    main_tuning = resolve_turn_tuning(
        model_pref, effective_model,
        reasoning_effort=reasoning_effort, fast_mode=fast_mode,
    )
    roles = [*role_registry(config, model_pref, effective_model), *subagent_roles]

    # STEP 0 — best-effort batch BYOK prefetch (pure perf; see helper docstring).
    byok_cache: dict[str, dict | None] = (
        await _prefetch_byok_cache(user_id, config, effective_model, model_pref, roles)
        if is_byok
        else {}
    )

    # Main model — single primitive call. OAuth-first → BYOK → platform fallback
    # (only when allow_platform_fallback and the model is SYSTEM). The
    # ``bool(reasoning_effort)`` gate preserves the old behavior exactly: a
    # non-credentialed reasoning request gets an eager platform client (tagged
    # PLATFORM); a non-reasoning non-credentialed request leaves llm_client=None
    # (NONE) for the lazy OSS path. An OAuth-required HTTPException propagates.
    main = await resolve_model_client(
        user_id, effective_model, is_byok=is_byok, cache_key=thread_id,
        reasoning_effort=main_tuning.reasoning_effort,
        service_tier=_tier_for(main_tuning.fast_mode),
        allow_platform_fallback=bool(main_tuning.reasoning_effort),
        _pref_cache=model_pref, _byok_cache=byok_cache,
    )
    # Always store credential_source (even NONE) — single source of truth
    # downstream (credit gate, materialization gate, billing signal).
    config.credential_source = main.credential_source
    if main.client is not None:
        _stamp(config, main.client, config.prompt_guidance)
        config.llm_client = main.client
    elif user_defined:
        # Custom model selected but no usable key — fail loud with a CTA.
        raise_byok_key_required(effective_model)

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
            is_byok=is_byok, cache_key=thread_id, main_model=effective_model,
        ),
        _resolve_fallback_clients(
            config, user_id, model_pref, byok_cache,
            is_byok=is_byok, cache_key=thread_id, users_fallbacks=users_fallbacks,
            reasoning_effort=reasoning_effort, fast_mode=fast_mode,
        ),
    )
