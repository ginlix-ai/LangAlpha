"""Data routing ruleset: which provider serves which market-data cell.

The ruleset is *generated* by an entitlement probe run against a deployment's
own tokens and loaded at startup by that deployment's provider registry. It
refines the hand-written provider chain cell by cell; where it has no cell,
the configured order stands.
"""

from .ruleset import (
    PROBED_SURFACES,
    RULESET_VERSION,
    Cell,
    CellKey,
    ProbedProvider,
    ProviderInfo,
    Ruleset,
    RoutingTable,
    Surface,
    load_ruleset,
    order_providers,
)

__all__ = [
    "PROBED_SURFACES",
    "RULESET_VERSION",
    "Cell",
    "CellKey",
    "ProbedProvider",
    "ProviderInfo",
    "Ruleset",
    "RoutingTable",
    "Surface",
    "load_ruleset",
    "order_providers",
]
