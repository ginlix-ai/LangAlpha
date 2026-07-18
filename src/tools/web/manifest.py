"""Web provider manifest v2: provider × capability × level.

One manifest for all web verbs (search / fetch / crawl / map). A *level* is
one depth/mode a capability offers — same struct and validation across
verbs, ``credits`` billing in the verb's natural unit and ``min_tier``
gating.

Data-only module (stdlib + src.config) — it must never import search.py,
langchain, or provider packages, so the resolve-time gate and write
validation can import it cheaply.
"""

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Tuple, Union

logger = logging.getLogger(__name__)

_MANIFEST_PATH = Path(__file__).parent.parent / "manifest" / "web_providers.json"

CAPABILITY_SEARCH = "search"
CAPABILITY_FETCH = "fetch"
CAPABILITY_CRAWL = "crawl"
CAPABILITY_MAP = "map"
CAPABILITY_RESEARCH = "research"


@dataclass(frozen=True)
class LevelSpec:
    """One level a capability offers (ordered fastest → deepest).

    ``credits`` is priced in the verb's natural unit: per call for search
    and map, per URL for fetch, per delivered page for crawl.
    """

    name: str
    display_name: str
    native_params: Dict[str, Any]
    min_tier: Optional[int]
    credits: float


@dataclass(frozen=True)
class CapabilitySpec:
    """One capability (search/fetch/crawl/map) a provider offers."""

    verb: str
    tracking_name: str
    min_tier: Optional[int]
    default_level: str
    levels: Tuple[LevelSpec, ...]
    max_batch_size: int = 1

    def level(self, name: Optional[str]) -> Optional[LevelSpec]:
        """Look up a level by name; None if the capability doesn't offer it."""
        for lv in self.levels:
            if lv.name == name:
                return lv
        return None

    @property
    def default_level_spec(self) -> LevelSpec:
        spec = self.level(self.default_level)
        if spec is None:  # guaranteed present by get_web_providers validation
            raise RuntimeError(
                f"Capability {self.verb!r} default_level {self.default_level!r} missing"
            )
        return spec

    def tracking_key(self, level: LevelSpec) -> str:
        """Billing key for a usage row: level-qualified when the capability
        offers multiple levels, bare otherwise.

        Single authority for the rule — usage recording and the pricing table
        must key identically, and adding a second level re-keys the rows.
        """
        if len(self.levels) > 1:
            return f"{self.tracking_name}:{level.name}"
        return self.tracking_name


@dataclass(frozen=True)
class WebProviderSpec:
    """A provider entry from the manifest."""

    name: str
    display_name: str
    env_key: Optional[str]
    capabilities: Mapping[str, CapabilitySpec]

    def capability(self, verb: str) -> Optional[CapabilitySpec]:
        return self.capabilities.get(verb)


@lru_cache(maxsize=1)
def _load_manifest() -> Dict[str, Any]:
    if not _MANIFEST_PATH.exists():
        raise RuntimeError(f"Web provider manifest not found at {_MANIFEST_PATH}")
    try:
        with open(_MANIFEST_PATH) as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load web provider manifest {_MANIFEST_PATH}: {e}")


def _parse_capability(provider: str, verb: str, entry: Dict[str, Any]) -> CapabilitySpec:
    levels = tuple(
        LevelSpec(
            name=lv["name"],
            display_name=lv.get("display_name", lv["name"]),
            native_params=lv.get("native_params", {}),
            min_tier=lv.get("min_tier"),
            credits=lv["credits"],
        )
        for lv in entry.get("levels", [])
    )
    cap = CapabilitySpec(
        verb=verb,
        tracking_name=entry["tracking_name"],
        min_tier=entry.get("min_tier"),
        default_level=entry.get("default_level", levels[0].name if levels else ""),
        levels=levels,
        max_batch_size=entry.get("max_batch_size", 1),
    )

    where = f"Web provider {provider!r} capability {verb!r}"
    if not levels:
        raise RuntimeError(f"{where} declares no levels")
    names = [lv.name for lv in levels]
    if len(names) != len(set(names)):
        raise RuntimeError(f"{where} has duplicate level names")
    if cap.level(cap.default_level) is None:
        raise RuntimeError(
            f"{where} default_level {cap.default_level!r} is not one of its levels {names}"
        )
    if cap.max_batch_size < 1:
        raise RuntimeError(f"{where} max_batch_size must be >= 1")
    return cap


@lru_cache(maxsize=1)
def get_web_providers() -> Mapping[str, WebProviderSpec]:
    """Load and validate all provider specs, keyed by provider name.

    Read-only mapping: the lru_cache shares one object across all callers.
    """
    manifest = _load_manifest()
    providers: Dict[str, WebProviderSpec] = {}

    for name, entry in manifest.get("providers", {}).items():
        caps = {
            verb: _parse_capability(name, verb, cap_entry)
            for verb, cap_entry in entry.get("capabilities", {}).items()
        }
        if not caps:
            raise RuntimeError(f"Web provider {name!r} declares no capabilities")
        providers[name] = WebProviderSpec(
            name=name,
            display_name=entry.get("display_name", name),
            env_key=entry.get("env_key"),
            capabilities=MappingProxyType(caps),
        )

    if not providers:
        raise RuntimeError(f"No web providers defined in {_MANIFEST_PATH}")
    return MappingProxyType(providers)


def get_web_provider_spec(name: str) -> Optional[WebProviderSpec]:
    """Spec for a provider name, or None if unknown."""
    return get_web_providers().get(name)


def get_capability(provider: str, verb: str) -> Optional[CapabilitySpec]:
    """A provider's capability spec, or None if provider/verb unknown."""
    spec = get_web_providers().get(provider)
    return spec.capability(verb) if spec else None


def providers_with_capability(verb: str) -> Mapping[str, WebProviderSpec]:
    """Providers offering a verb, in manifest order."""
    return {
        name: spec for name, spec in get_web_providers().items() if spec.capability(verb)
    }


@lru_cache(maxsize=1)
def get_auxiliary_pricing() -> Mapping[str, Dict[str, Any]]:
    """Pricing entries for tools that aren't level-selectable capabilities
    (image search, research) — consumed by the infrastructure billing table."""
    return MappingProxyType(_load_manifest().get("auxiliary_tools", {}))


def _tier_floor() -> int:
    # Read at call time so tests monkeypatching src.config.settings see effect.
    from src.config import settings

    return settings.SEARCH_PROVIDER_MIN_TIER


def resolve_min_tier(spec: Union[CapabilitySpec, LevelSpec]) -> int:
    """Effective min tier for a capability or level (env floor when unset)."""
    return spec.min_tier if spec.min_tier is not None else _tier_floor()
