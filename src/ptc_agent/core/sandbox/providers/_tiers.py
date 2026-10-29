"""Resource-tier arithmetic, shared by the sandbox providers.

One rule both providers need, stated once: a workspace whose tier was removed
from config must still start, and an elevated tier whose size could not be
provisioned must never be handed back at base size. The two providers enforce
the second half differently (one refuses a base snapshot, the other deletes an
under-provisioned container), but the choice they enforce it on is the same.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, NamedTuple

import structlog

from ptc_agent.config.core import ResourceTier

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class TierChoice:
    """A tier name resolved against the configured presets."""

    name: str
    preset: ResourceTier | None
    is_default: bool

    @property
    def elevated(self) -> bool:
        """Whether this tier is one the user is charged above base for."""
        return not self.is_default and self.preset is not None


def resolve_tier(
    tier: str | None,
    *,
    default_tier: str,
    resource_tiers: dict[str, ResourceTier],
) -> TierChoice:
    """Resolve a tier name, keeping an orphaned name startable.

    A persisted tier that was later removed from config resolves with no
    preset and a warning rather than an error: raising would make that
    workspace unrecoverable, which is a worse outcome than running it at base
    size. The default tier resolves uniformly with the rest so its configured
    size takes effect instead of falling through to a platform default.
    """
    name = tier or default_tier
    preset = resource_tiers.get(name)
    is_default = name == default_tier
    if preset is None and not is_default:
        logger.warning(
            "Unknown resource tier; falling back to base size",
            tier=name,
            known_tiers=sorted(resource_tiers),
        )
    return TierChoice(name=name, preset=preset, is_default=is_default)


class TierSizing(NamedTuple):
    """The container limits one tier name resolves to."""

    tier: str
    cpus: float
    memory_bytes: int
    disk_gib: int | None
    elevated: bool


def parse_memory(limit_str: str) -> int:
    """Convert a human-friendly memory string (e.g. ``"4g"``) to bytes."""
    limit_str = limit_str.strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([kmgt])?b?", limit_str)
    if not match:
        raise ValueError(f"Cannot parse memory limit: {limit_str!r}")
    value = float(match.group(1))
    suffix = match.group(2) or ""
    multipliers = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    return int(value * multipliers[suffix])


def applied_cpus(host_config: dict[str, Any]) -> float | None:
    """vCPU count the daemon actually applied, from whichever knob it recorded.

    ``NanoCpus`` and ``CpuQuota``/``CpuPeriod`` are one limit written two ways and
    are mutually exclusive on create, so reading both is what lets the check pass
    on a daemon that stores the other one.
    """
    nano_cpus = host_config.get("NanoCpus")
    if nano_cpus:
        return int(nano_cpus) / 1e9
    quota, period = host_config.get("CpuQuota"), host_config.get("CpuPeriod")
    if quota and period:
        return int(quota) / int(period)
    return None


def container_sizing(
    choice: TierChoice, *, floor_cpus: float, floor_memory_limit: str
) -> TierSizing:
    """Container limits for a resolved tier, floored at the operator's own size.

    ``cpu_count``/``memory_limit`` act as a floor rather than a default: they
    are what a self-hosted deployment already sized every container at, so a
    tier raises them and never shrinks a box its operator configured.
    """
    floor_memory = parse_memory(floor_memory_limit)
    if choice.preset is None:
        return TierSizing(
            tier=choice.name,
            cpus=floor_cpus,
            memory_bytes=floor_memory,
            disk_gib=None,
            elevated=False,
        )
    return TierSizing(
        tier=choice.name,
        cpus=max(floor_cpus, float(choice.preset.cpu)),
        memory_bytes=max(floor_memory, choice.preset.memory * 1024**3),
        disk_gib=choice.preset.disk,
        elevated=choice.elevated,
    )
