"""Model-availability error responses + stale-preference scrubbing.

When a selected model can't be served — a custom model with no usable key, or a
saved preference whose model has vanished from the manifest — the chat handler
needs to fail loudly with a CTA banner rather than silently downgrade. This
module owns those user-facing 400s and the pref-scrub that backs the
``model_removed`` case. ``resolve_llm_config`` invokes them; they are not part
of the resolution engine itself.
"""

from __future__ import annotations

from typing import NoReturn

from ._common import logger


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

    from .llm_config import get_model_preference

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
