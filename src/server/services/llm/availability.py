"""Model-availability error responses + stale-preference scrubbing.

When a selected model can't be served — a custom model with no usable key, a
saved preference whose model has vanished from the manifest, a model the
user's plan does not cover on the platform's key, or a subscription whose
sign-in cannot serve the turn — the chat handler needs to
fail loudly with a CTA banner rather than silently downgrade. This module owns
those user-facing 400s and the pref-scrub that backs the ``model_removed``
case. A missing model saved for a role (the Background model on a primary
turn, compaction, fetch, a fallback), or any saved model the plan does not
cover, is not a refusal: the turn reads the prefs as if the slot were unset,
and nothing is deleted.
``resolve_llm_config`` invokes them; they are not part of the resolution
engine itself.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, NoReturn

from . import user_models
from .capabilities import TierPermits

if TYPE_CHECKING:
    from src.server.services.oauth_providers import SubscriptionProvider

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")



def on_subscription(entry: Mapping | None) -> bool:
    """Whether a custom model entry runs on one of the user's subscriptions.

    "Show all" saves a model a subscription lists but the manifest does not
    tune as a custom model on that subscription's provider. It is served by
    the subscription's sign-in, like a built-in of that provider, so no API
    key is needed for it. No custom provider can take a subscription's
    name (``validate_custom_providers``), so the slug says which it is.
    """
    from src.server.services.oauth_providers import SUBSCRIPTIONS

    return bool(entry) and entry.get("provider") in SUBSCRIPTIONS


def raise_byok_key_required(model_name: str) -> None:
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


# A subscription that cannot serve the turn. Each names the account the way
# the user signed in to it, and says whether reconnecting can help.

def raise_oauth_reauth_required(
    subscription: SubscriptionProvider, model_name: str | None, code: str | None
) -> NoReturn:
    """``model_name`` is None where no model has been resolved yet (the
    no-provider gate), so the sentence names the account alone."""
    from fastapi import HTTPException

    use = f"use '{model_name}'" if model_name else "use its models"
    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"Your {subscription.account_name} sign-in has expired. Reconnect "
                f"it to {use}."
            ),
            "type": "oauth_reauth_required",
            "provider": subscription.id,
            "reason": code,
            "link": {"url": "/settings?tab=model", "label": "Reconnect"},
        },
    )


def raise_oauth_plan_no_api_access(subscription: SubscriptionProvider) -> NoReturn:
    """Not a sign-in problem, so no reconnect: a key is what works."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"Your {subscription.account_name} plan doesn't include API access, "
                "which LangAlpha needs. Use a SpaceXSI API key instead."
            ),
            "type": "oauth_plan_no_api_access",
            "provider": subscription.id,
            "link": {"url": "/settings?tab=model", "label": "Add an API key"},
        },
    )


def raise_oauth_refresh_unavailable(subscription: SubscriptionProvider) -> NoReturn:
    from fastapi import HTTPException

    raise HTTPException(
        status_code=503,
        detail={
            "message": (
                f"Your {subscription.account_name} sign-in could not be renewed just "
                "now. Try again in a moment."
            ),
            "type": "oauth_refresh_unavailable",
            "provider": subscription.id,
        },
    )


# Preference keys that hold a single model name. Used by the stale-pref
# scrubber when a saved model vanishes from the manifest.
_MODEL_PREF_KEYS = (
    "preferred_model",
    "preferred_background_model",
    "fetch_model",
    "compaction_model",
    "summarization_model",
)


def _resolver(pref: dict) -> Callable[[str | None], bool] | None:
    """Whether a saved model name still resolves: the one meaning of "stale"
    for both the per-turn filter and the scrub, so they never disagree.

    ``None`` when the manifest is empty. That is a load failure, not a catalog
    of zero models, and judging against it would call every saved name dead —
    for the scrub an irreversible delete of the whole preference set, since it
    goes straight to the merge-upsert with no copy kept.
    """
    from src.llms.llm import LLM as LLMFactory
    from src.llms.preferences import custom_model_names

    mc = LLMFactory.get_model_config()
    if not mc.llm_config:
        return None

    custom_models = custom_model_names(pref)
    providers = pref.get("custom_providers")
    custom_providers = {
        cp.get("name") for cp in (providers if isinstance(providers, list) else [])
        if isinstance(cp, dict)
    }

    # Runs on every turn, over values nothing validated on write: a name that
    # is not a string cannot be served, so it reads as dead instead of raising.
    def resolvable(name: object) -> bool:
        if name is None or name == "":
            return True  # not set; nothing to scrub
        return isinstance(name, str) and (
            name in custom_models
            or name in custom_providers
            or mc.get_model_config(name) is not None
        )

    return resolvable


def model_resolves(model_pref: dict, name: str) -> bool:
    """Whether ``name`` names a model this user can still select.

    True when the manifest failed to load, for the reason ``_resolver`` gives:
    judging against nothing would call every name dead.
    """
    resolvable = _resolver(model_pref)
    return True if resolvable is None else resolvable(name)


def raise_model_unavailable(model_name: str) -> NoReturn:
    """Raise the 400 for a model a client asked to keep that this user cannot run."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=400,
        detail={
            "message": f"Model '{model_name}' is not available. Pick one from the model list.",
            "type": "model_unavailable",
        },
    )


def drop_dead_models(
    model_pref: dict, keys: Iterable[str]
) -> tuple[dict, list[tuple[str, str]]]:
    """``model_pref`` as if the dead names under ``keys`` and in
    ``fallback_models`` had never been saved, plus ``[(key, name), ...]`` dropped.

    The one definition of what a dead slot is: the scrub persists this for
    every key, plus the dead ``profiles`` it alone sweeps.
    """
    resolvable = _resolver(model_pref)
    if resolvable is None:
        return model_pref, []

    live = dict(model_pref)
    dropped: list[tuple[str, str]] = []
    for key in keys:
        val = live.get(key)
        if val and not resolvable(val):
            del live[key]
            dropped.append((key, val))

    fallback = live.get("fallback_models")
    if isinstance(fallback, list):
        kept = [m for m in fallback if resolvable(m)]
        if len(kept) != len(fallback):
            dropped.extend(("fallback_models", m) for m in fallback if not resolvable(m))
            # Emptied stays empty: a user whose every pick is gone gets no
            # fallback, as when each dead one was skipped, rather than the
            # deployment's list on a key they never chose.
            live["fallback_models"] = kept

    return live, dropped


async def model_on_plan(
    user_id: str,
    model: str,
    *,
    model_pref: dict,
    permits: TierPermits,
    is_byok: bool,
    byok_cache: dict[str, dict | None],
) -> bool:
    """Whether the user may run ``model``, in the model list's order.

    A provider the user holds a key or a subscription for is never gated by
    plan, so a free-plan user who pins a model to their own key keeps it. A
    model with no tier is one no plan includes, which the list offers only to
    such a user.
    """
    from src.server.dependencies.usage_limits import platform_gating_active

    if not platform_gating_active():
        return True
    from src.llms.llm import LLM as LLMFactory

    from .clients import _resolve_custom_model_byok, walk_byok_candidates

    mc = LLMFactory.get_model_config()
    source, entry = await user_models.classify_model(user_id, model, _pref_cache=model_pref)
    if source == user_models.ModelSource.CUSTOM:
        if on_subscription(entry):
            # As a built-in of a subscription provider below.
            return True
        # Runs on the user's own key. With none, a role left without a
        # client is built by this name from the manifest, on the platform's
        # key, so a custom model under a built-in's name is judged as that one.
        shadowed = mc.get_model_config(model)
        if shadowed is None:
            return True
        if is_byok:
            key, _, _ = await _resolve_custom_model_byok(
                user_id, model, entry, mc, _pref_cache=model_pref, _byok_cache=byok_cache,
            )
            if key:
                return True
        tier = shadowed.get("tier")
        return isinstance(tier, int) and await permits(tier)
    if source != user_models.ModelSource.SYSTEM:
        return True
    tier = entry.get("tier")
    if isinstance(tier, int) and await permits(tier):
        return True

    provider = entry.get("provider")
    if not provider:
        return False
    if mc.get_provider_info(provider).get("access_type") == "oauth":
        # Served on the user's subscription or not at all; with none, building
        # its client refuses the turn or leaves the role without one.
        return True
    if not is_byok:
        return False
    found, _ = await walk_byok_candidates(user_id, provider, mc, _byok_cache=byok_cache)
    return found is not None


async def drop_off_plan_models(
    user_id: str,
    model_pref: dict,
    *,
    permits: TierPermits,
    is_byok: bool,
    byok_cache: dict[str, dict | None],
    exempt: Mapping[str, frozenset[str]] | None = None,
) -> tuple[dict, list[tuple[str, str]]]:
    """``model_pref`` as if every saved model ``model_on_plan`` refuses had never
    been saved, plus ``[(key, name), ...]`` dropped.

    A downgrade leaves the paid models it covered in the user's slots, where
    they would keep running on the platform's key. A saved model reads as
    unset, as a dead one does; a model the turn names is refused instead,
    before this runs. Nothing is deleted, since the plan may come back.
    ``exempt`` names, per key (``fallback_models`` included), the
    deployment's own models that key may hold: the operator's call.
    """
    exempt = exempt or {}
    fallback = model_pref.get("fallback_models")
    fallback = fallback if isinstance(fallback, list) else []
    saved = [
        *((key, model_pref.get(key)) for key in _MODEL_PREF_KEYS),
        *(("fallback_models", m) for m in fallback),
    ]

    # Nothing validates these on write, and the slot a turn runs is left to
    # this check by the dead-model filter, so only a string is ever hashed.
    def judged(key: str, value: object) -> bool:
        return isinstance(value, str) and bool(value) and value not in exempt.get(key, ())

    off = {
        name
        for name in {value for key, value in saved if judged(key, value)}
        if not await model_on_plan(
            user_id, name, model_pref=model_pref, permits=permits,
            is_byok=is_byok, byok_cache=byok_cache,
        )
    }
    if not off:
        return model_pref, []

    def is_off(key: str, value: object) -> bool:
        return judged(key, value) and value in off

    view = dict(model_pref)
    dropped: list[tuple[str, str]] = []
    for key in _MODEL_PREF_KEYS:
        if is_off(key, view.get(key)):
            dropped.append((key, view.pop(key)))
    if any(is_off("fallback_models", m) for m in fallback):
        view["fallback_models"] = [m for m in fallback if not is_off("fallback_models", m)]
        dropped.extend(("fallback_models", m) for m in fallback if is_off("fallback_models", m))
    return view, dropped


def raise_model_not_on_plan(model_name: str) -> NoReturn:
    """Raise the 400 for a model the user chose that would run on the
    platform's key above their plan."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"Model '{model_name}' is not included in your plan. Pick another "
                "model, or add your own API key for it in Settings."
            ),
            "type": "model_not_on_plan",
            "link": {"url": "/settings?tab=model", "label": "Open Settings"},
        },
    )


async def cleanup_stale_model_preferences(user_id: str) -> list[tuple[str, str]]:
    """Drop stale model names from the user's prefs. Returns ``[(key, name), ...]``."""
    from src.server.database.user import (
        invalidate_user_prefs_cache,
        upsert_user_preferences,
    )

    # Bust cache + re-read so a concurrent Settings save isn't clobbered.
    await invalidate_user_prefs_cache(user_id)
    pref = await user_models.get_model_preference(user_id)

    resolvable = _resolver(pref)
    if resolvable is None:
        logger.warning(
            f"[CHAT] Skipping stale-pref scrub for user={user_id}: model manifest is empty"
        )
        return []

    # Values: ``None`` for scalar deletes, ``list[str]`` for fallback_models,
    # and a ``{model: None}`` map for profiles. Merge-upsert
    # interprets ``None`` as key deletion at either level.
    live, removed = drop_dead_models(pref, _MODEL_PREF_KEYS)
    updates: dict[str, list[str] | dict[str, None] | None] = {
        key: live.get(key) for key, _ in removed
    }
    if "fallback_models" in updates:
        # Emptied, the list is saved as ``[]`` rather than deleted: deleting it
        # would hand the user the deployment's list from the next turn on,
        # fallbacks they never picked, where this turn's filter gave them none.
        updates["fallback_models"] = updates["fallback_models"] or []

    # A profile keyed on a model that no longer exists is unreachable config.
    # ``None`` per model is the merge's own per-model delete, so the surviving
    # profiles are never rewritten.
    profiles = pref.get("profiles")
    if isinstance(profiles, dict):
        dead = {name: None for name in profiles if not resolvable(name)}
        if dead:
            updates["profiles"] = dead
            removed.extend(("profiles", name) for name in dead)

    if updates:
        # Each ``None`` here also clears the key's pre-move copy, which the
        # merge-upsert does for every writer; without it a stale name reappears
        # from underneath on the next read, to be raised on and cleared again
        # every turn.
        #
        # Residual race window: between the re-read above and this upsert, a
        # Settings save could still land and get overwritten by our ``None``
        # delete. Narrow (single DB read → single DB write) and self-healing
        # (the user saves again and it sticks). Not worth a CTE or advisory
        # lock for the size of the hole.
        await upsert_user_preferences(
            user_id=user_id,
            model_preference=updates,
        )
        await invalidate_user_prefs_cache(user_id)
        logger.info(
            f"[CHAT] Scrubbed stale model prefs for user={user_id}: {removed}"
        )

    return removed


def raise_model_removed(
    model_name: str, removed: list[tuple[str, str]]
) -> NoReturn:
    """Raise a 400 with a CTA banner payload when a saved model no longer resolves."""
    from fastapi import HTTPException

    other = sorted({name for _, name in removed if name != model_name})
    extra = f" Also cleared: {', '.join(other)}." if other else ""
    # Only claim a clear when one happened. The caller passes an empty list for
    # a model named in the request rather than saved, and the scrub also returns
    # empty when it declines to run, so asserting the clear unconditionally
    # tells the user their preference is gone while it is still stored.
    cleared = (
        "Your saved preference has been cleared — open Settings to pick a current model."
        if removed
        else "Open Settings to pick a current model."
    )

    raise HTTPException(
        status_code=400,
        detail={
            "message": (
                f"Model '{model_name}' is no longer available. " + cleared + extra
            ),
            "type": "model_removed",
            "link": {"url": "/settings?tab=model", "label": "Open Settings"},
        },
    )
