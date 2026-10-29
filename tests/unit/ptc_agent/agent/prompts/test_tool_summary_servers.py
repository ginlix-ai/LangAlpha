"""The MCP tool summary narrowed to a server set, with preferred servers first and none dropped."""

from __future__ import annotations

import re

from ptc_agent.agent.prompts import RegistryTools, format_registry_tools

ORDER = ("srv_a", "srv_b", "srv_c", "srv_d")
REGISTRY = RegistryTools(
    tools_by_server={n: [{"name": "lookup", "description": "Look up."}] for n in ORDER},
    server_configs={},
)
_HEADER = re.compile(r"^(srv_\w+):", re.MULTILINE)


def _split(summary: str) -> tuple[str, list[str]]:
    """The text before the first server, and the servers in listed order."""
    first = _HEADER.search(summary)
    lead = summary[: first.start()] if first else summary
    return lead, _HEADER.findall(summary)


def test_servers_none_lists_every_server_in_registry_order():
    lead, listed = _split(format_registry_tools(REGISTRY))
    assert listed == list(ORDER)
    assert lead.strip() == ""


def test_only_the_given_servers_are_listed():
    _, listed = _split(format_registry_tools(REGISTRY, servers={"srv_d", "srv_b"}))
    assert listed == ["srv_b", "srv_d"]


def test_preferred_lead_under_a_line_naming_them_and_the_rest_follow():
    """Unreachable and repeated names are dropped from the lead, never added
    to the listing: a preference orders the guide, it grants nothing."""
    summary = format_registry_tools(
        REGISTRY,
        servers={"srv_a", "srv_c", "srv_d"},
        preferred=["srv_d", "srv_b", "srv_d", "srv_a"],
    )
    lead, listed = _split(summary)
    assert listed == ["srv_d", "srv_a", "srv_c"]
    assert re.findall(r"srv_\w+", lead) == ["srv_d", "srv_a"]


def test_a_preference_with_nothing_reachable_changes_nothing():
    allowed = {"srv_a", "srv_c"}
    assert format_registry_tools(
        REGISTRY, servers=allowed, preferred=["srv_b"]
    ) == format_registry_tools(REGISTRY, servers=allowed)
