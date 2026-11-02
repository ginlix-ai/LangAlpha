"""Which names the setup step may hold, and how the app labels them."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from ptc_agent.config.core import KEYED_KINDS, DaytonaConfig, E2BConfig
from src.tools.web.manifest import CAPABILITY_SEARCH, get_web_providers

#: The saved sandbox choice, exported under the name config already reads.
PROVIDER_SETTING = "SANDBOX_PROVIDER"
#: Comma-joined install groups, which the local provider reads from the environment.
INSTALL_GROUPS_SETTING = "LOCAL_INSTALL_GROUPS"
#: Set once the user finishes the step. Never exported.
COMPLETED_SETTING = "SETUP_COMPLETED"

#: Key of each kind reached with an API key.
SANDBOX_KEYS = KEYED_KINDS
SANDBOX_NAMESPACES = {
    "daytona": DaytonaConfig.ENV_FIELDS["secret_namespace"],
    "e2b": E2BConfig.ENV_FIELDS["secret_namespace"],
}

FMP_KEY = "FMP_API_KEY"


@dataclass(frozen=True)
class KeySpec:
    name: str
    group: str
    provider: str


@lru_cache(maxsize=1)
def key_specs() -> tuple[KeySpec, ...]:
    """The keys the app may hold, in the order it lists them.

    Web providers come from their manifest, so a new one needs no change here.
    A key that only fetches is its own group, since the agent's search does not
    run on it.
    """
    web = tuple(
        KeySpec(
            name=spec.env_key,
            group="search" if spec.capability(CAPABILITY_SEARCH) else "fetch",
            provider=name,
        )
        for name, spec in get_web_providers().items()
        if spec.env_key
    )
    sandbox = tuple(
        KeySpec(name=key, group="sandbox", provider=kind)
        for kind, key in SANDBOX_KEYS.items()
    )
    return (KeySpec(name=FMP_KEY, group="data", provider="fmp"), *web, *sandbox)


def key_names() -> frozenset[str]:
    return frozenset(spec.name for spec in key_specs())
