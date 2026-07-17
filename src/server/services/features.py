"""Per-user feature-flag resolution and overrides.

User overrides live under ``user_preferences.other_preference.feature_overrides``
— the server-managed JSONB bag that is deliberately excluded from the agent's
sandbox view, so the LLM can never edit a flag. Reads ride the existing
Redis-cached preferences fetch; writes merge-upsert the sub-dict and
invalidate that cache. Plan-gated features resolve against the platform
access tier (Redis-cached, fetched only when a plan-gated feature exists).
"""

from typing import Any, Literal

from pydantic import BaseModel

from src.config import settings
from src.config.features import (
    FEATURES,
    USER_CONTROLLABLE_GATES,
    effective_flags,
    has_plan_gated_features,
    system_flag,
)
from src.server.database.user import (
    get_user_preferences,
    invalidate_user_prefs_cache,
    upsert_user_preferences,
)


class FeatureState(BaseModel):
    """Resolved state of one cataloged feature, shaped for the API.

    The service owns this shape (it builds it); the router imports it for its
    response envelope so the wire contract is declared once.
    """

    key: str
    label: str
    description: str
    tradeoffs: str | None = None
    enabled: bool
    gate: Literal["none", "opt_in", "opt_out", "plan"]
    min_tier: int | None = None
    user_override: bool | None = None


class UnknownFeatureError(KeyError):
    """The feature key is not in the code catalog."""


class FeatureNotOverridableError(PermissionError):
    """The feature's gate does not accept per-user overrides."""


async def _get_overrides(user_id: str) -> dict[str, Any]:
    prefs = await get_user_preferences(user_id)
    other = (prefs or {}).get("other_preference") or {}
    overrides = other.get("feature_overrides")
    return dict(overrides) if isinstance(overrides, dict) else {}


async def _access_tier(user_id: str) -> int | None:
    """Platform access tier for plan gates; None when nothing needs it.

    Skipped entirely unless a plan-gated feature exists and we're in platform
    mode — ``effective_flags`` bypasses plan gates outside platform mode.
    """
    if not has_plan_gated_features() or settings.HOST_MODE != "platform":
        return None
    from src.server.dependencies.usage_limits import get_platform_access_tier

    return await get_platform_access_tier(user_id)


async def effective_flags_for_user(
    user_id: str, overrides: dict[str, Any] | None = None
) -> dict[str, bool]:
    """Resolved feature map for one user; ``overrides`` skips the prefs read
    when the caller already holds the user's ``feature_overrides``."""
    if overrides is None:
        overrides = await _get_overrides(user_id)
    return effective_flags(overrides, access_tier=await _access_tier(user_id))


async def user_feature_enabled(user_id: str, key: str) -> bool:
    """Effective flag for one user; unknown keys resolve False."""
    return (await effective_flags_for_user(user_id)).get(key, False)


async def list_user_features(user_id: str) -> list[FeatureState]:
    """Resolved state of every cataloged feature, shaped for the API."""
    overrides = await _get_overrides(user_id)
    resolved = await effective_flags_for_user(user_id, overrides)
    features = []
    for key, spec in FEATURES.items():
        flag = system_flag(key)
        override = overrides.get(key)
        user_controllable = flag.gate in USER_CONTROLLABLE_GATES
        features.append(
            FeatureState(
                key=key,
                label=spec.label,
                description=spec.description,
                tradeoffs=spec.tradeoffs,
                enabled=resolved[key],
                gate=flag.gate.value,
                min_tier=flag.min_tier,
                user_override=(
                    override
                    if user_controllable and isinstance(override, bool)
                    else None
                ),
            )
        )
    return features


async def set_feature_override(
    user_id: str, key: str, enabled: bool | None
) -> list[FeatureState]:
    """Set (or clear, with ``None``) a user's override and return the new state.

    Only ``opt_in``/``opt_out`` gates accept overrides. The whole
    ``feature_overrides`` sub-dict is rewritten because the JSONB merge in
    ``upsert_user_preferences`` is shallow; an emptied dict deletes the key
    outright (``None`` value = per-key delete in the DB layer).
    """
    if key not in FEATURES:
        raise UnknownFeatureError(key)
    if system_flag(key).gate not in USER_CONTROLLABLE_GATES:
        raise FeatureNotOverridableError(key)

    # Read-modify-write of the whole feature_overrides bag: concurrent PUTs on
    # different keys can drop each other. Accepted — this is a single user's
    # settings toggle, so last-writer-wins is fine at the expected rate.
    overrides = await _get_overrides(user_id)
    if enabled is None:
        overrides.pop(key, None)
    else:
        overrides[key] = enabled

    await upsert_user_preferences(
        user_id, other_preference={"feature_overrides": overrides or None}
    )
    await invalidate_user_prefs_cache(user_id)
    return await list_user_features(user_id)
