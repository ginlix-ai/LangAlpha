"""Model-availability error responses + stale-preference scrubbing.

When a selected model can't be served — a custom model with no usable key, or a
saved preference whose model has vanished from the manifest — the chat handler
needs to fail loudly with a CTA banner rather than silently downgrade. This
module owns those user-facing 400s and the pref-scrub that backs the
``model_removed`` case. ``resolve_llm_config`` invokes them; they are not part
of the resolution engine itself.
"""

from __future__ import annotations

import logging
from typing import NoReturn

from . import user_models

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")



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


# Preference keys that hold a single model name. Used by the stale-pref
# scrubber when a saved model vanishes from the manifest.
_MODEL_PREF_KEYS = (
    "preferred_model",
    "preferred_flash_model",
    "fetch_model",
    "compaction_model",
    "summarization_model",
)


async def cleanup_stale_model_preferences(user_id: str) -> list[tuple[str, str]]:
    """Drop stale model names from the user's prefs. Returns ``[(key, name), ...]``."""
    from src.llms.llm import LLM as LLMFactory
    from src.llms.preferences import custom_model_names
    from src.server.database.user import (
        invalidate_user_prefs_cache,
        upsert_user_preferences,
    )


    # Bust cache + re-read so a concurrent Settings save isn't clobbered.
    await invalidate_user_prefs_cache(user_id)
    pref = await user_models.get_model_preference(user_id)

    mc = LLMFactory.get_model_config()
    # An empty manifest is a load failure, not a catalog of zero models. Every
    # name would then read as stale and this function would delete the user's
    # entire model preference set — irreversibly, since the deletes go straight
    # to the merge-upsert with no copy kept.
    if not mc.llm_config:
        logger.warning(
            f"[CHAT] Skipping stale-pref scrub for user={user_id}: model manifest is empty"
        )
        return []

    custom_models = custom_model_names(pref)
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
    # fallback_models, and a ``{model: None}`` map for profiles. Merge-upsert
    # interprets ``None`` as key deletion at either level.
    updates: dict[str, list[str] | dict[str, None] | None] = {}
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
