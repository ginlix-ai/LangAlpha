"""Entitlement probe: measures what each configured provider actually serves.

Writes ``data_routing.yaml`` (the ruleset the provider chain consults): per
market, asset class and surface, the providers whose data passed, in order.
A provider with missing, stale or misanchored data is excluded from its cell.
"""

PROBE_VERSION = 1

__all__ = ["PROBE_VERSION"]
