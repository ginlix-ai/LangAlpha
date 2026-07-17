"""Feature-flag registry: code-declared catalog, deployment overrides, user opt-ins.

Each feature carries two orthogonal controls:

- ``enabled`` — the kill switch. ``false`` turns the feature off for everyone
  and hides every surface; the gate is irrelevant.
- ``gate`` — the access model while enabled:
    * ``none``    — available to everyone
    * ``opt_in``  — off until the user opts in (Settings toggle)
    * ``opt_out`` — on unless the user opts out (Settings toggle)
    * ``plan``    — requires a platform plan whose access tier meets the
      feature's ``min_tier`` (OSS/self-hosted deployments bypass plan gates,
      matching the search-provider tier-gating convention)

Three definition layers, resolved in order:

1. **Catalog** (this module): every feature the codebase knows about, with its
   hard defaults and the frontend-facing label/description. A feature must be
   declared here to exist anywhere else.
2. **Deployment posture** (``config.yaml`` ``features:`` section): per-deploy
   override of ``enabled`` / ``gate`` / ``min_tier``; unset fields inherit
   the catalog.
3. **User override** (``user_preferences.other_preference.feature_overrides``,
   server-side): honored only for ``opt_in``/``opt_out`` gates.

Server code resolves per-user via ``src.server.services.features``; this
module stays user-agnostic so the agent library can consult defaults.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, NamedTuple

from src.config import settings
from src.config.settings import get_infrastructure_config


class FeatureGate(StrEnum):
    NONE = "none"
    OPT_IN = "opt_in"
    OPT_OUT = "opt_out"
    PLAN = "plan"


# Gates whose features accept a per-user override (the Settings toggle).
USER_CONTROLLABLE_GATES = frozenset({FeatureGate.OPT_IN, FeatureGate.OPT_OUT})


@dataclass(frozen=True)
class FeatureSpec:
    """A code-declared feature and its hard defaults."""

    key: str
    label: str
    description: str
    enabled: bool = False
    gate: FeatureGate = FeatureGate.NONE
    min_tier: int | None = None  # plan gate only
    tradeoffs: str | None = None  # honest cost of opting in, shown in Settings


FEATURES: dict[str, FeatureSpec] = {
    "market_watch": FeatureSpec(
        key="market_watch",
        label="Market watch",
        description=(
            "Adds a Watch toggle to chat: keep a per-thread list of symbols "
            "whose live quotes stream into the conversation while you work, "
            "so the agent reasons from current intraday prices instead of "
            "stale lookups."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        tradeoffs=(
            "Quotes are re-injected into the model's context as they update, "
            "which can occasionally distract the agent from its main task and "
            "invalidate the prompt cache — expect slightly slower, costlier "
            "turns while a watch list is active."
        ),
    ),
}


class SystemFlag(NamedTuple):
    enabled: bool
    gate: FeatureGate
    min_tier: int | None


def get_feature_specs() -> dict[str, FeatureSpec]:
    return dict(FEATURES)


def system_flag(key: str) -> SystemFlag:
    """Deployment-level state of a feature: catalog defaults overlaid with the
    config.yaml ``features:`` entry (unset fields inherit the catalog)."""
    spec = FEATURES[key]
    override = get_infrastructure_config().features.get(key)
    enabled = spec.enabled
    gate = spec.gate
    min_tier = spec.min_tier
    if override is not None:
        if override.enabled is not None:
            enabled = override.enabled
        if override.gate is not None:
            gate = FeatureGate(override.gate)
        if override.min_tier is not None:
            min_tier = override.min_tier
    return SystemFlag(enabled=enabled, gate=gate, min_tier=min_tier)


def is_feature_enabled_system(key: str) -> bool:
    """Kill-switch check: is the feature on at all in this deployment?

    Gate-agnostic — an ``opt_in`` feature nobody enabled yet is still "on"
    here. Used where no user context exists but the feature merely needs to
    exist (skill registry listings, sandbox skill sync).
    """
    return system_flag(key).enabled


def has_plan_gated_features() -> bool:
    """Whether any cataloged feature currently resolves to a plan gate —
    lets callers skip the platform tier fetch entirely when none does."""
    return any(system_flag(key).gate is FeatureGate.PLAN for key in FEATURES)


def effective_flags(
    user_overrides: Mapping[str, object] | None,
    *,
    access_tier: int | None = None,
    platform_mode: bool | None = None,
) -> dict[str, bool]:
    """Resolve every cataloged feature for one user.

    ``user_overrides`` is the raw ``feature_overrides`` mapping from the
    user's preferences; non-bool values and overrides on non-opt gates are
    ignored rather than errored so a stale or hand-edited row can't break
    turns. Plan gates fail closed in platform mode when ``access_tier`` is
    unknown or below ``min_tier``; non-platform deployments bypass them.
    """
    if platform_mode is None:
        platform_mode = settings.HOST_MODE == "platform"

    resolved: dict[str, bool] = {}
    for key in FEATURES:
        flag = system_flag(key)
        if not flag.enabled:
            resolved[key] = False
            continue
        override = (user_overrides or {}).get(key)
        override = override if isinstance(override, bool) else None
        if flag.gate is FeatureGate.NONE:
            resolved[key] = True
        elif flag.gate is FeatureGate.OPT_IN:
            resolved[key] = override is True
        elif flag.gate is FeatureGate.OPT_OUT:
            resolved[key] = override is not False
        else:  # FeatureGate.PLAN
            resolved[key] = not platform_mode or (
                access_tier is not None
                and flag.min_tier is not None
                and access_tier >= flag.min_tier
            )
    return resolved


def default_feature_enabled(key: str) -> bool:
    """No-user-context effective default (opt_in and platform plan gates
    resolve False). Fallback for entry points that skip per-user resolution."""
    return effective_flags(None).get(key, False)
