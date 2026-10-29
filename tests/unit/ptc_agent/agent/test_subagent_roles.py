"""Which roles get a subagent is the definition's to say, and the registry applies it.

The Chief of Staff hands new analysis to a workspace's analyst, so the
analyst-only equity-analyst is in neither its compiled roster nor the summary
its prompt advertises, which both come from the same lookup.
"""

from ptc_agent.agent.subagents import BUILTIN_SUBAGENTS, SubagentRegistry
from ptc_agent.config.agent import SubagentConfig

ALL = list(BUILTIN_SUBAGENTS)


def _roster(registry: SubagentRegistry, role: str | None) -> list[str]:
    return [defn.name for defn in registry.get_enabled(ALL, role=role)]


def test_the_chief_of_staff_roster_leaves_out_the_equity_analyst():
    assert _roster(SubagentRegistry(), "chief_of_staff") == [
        name for name in ALL if name != "equity-analyst"
    ]


def test_the_analyst_roster_keeps_it():
    assert _roster(SubagentRegistry(), "analyst") == ALL


def test_a_lookup_with_no_role_keeps_every_enabled_subagent():
    assert _roster(SubagentRegistry(), None) == ALL


def test_a_yaml_override_keeps_the_builtins_roles():
    """The YAML cannot say which roles get a subagent, so reshaping the
    equity-analyst must not hand it to the Chief of Staff."""
    registry = SubagentRegistry(
        user_definitions={"equity-analyst": SubagentConfig(description="mine")}
    )
    assert registry.get("equity-analyst").description == "mine"
    assert "equity-analyst" not in _roster(registry, "chief_of_staff")


def test_a_new_yaml_subagent_serves_every_role():
    registry = SubagentRegistry(
        user_definitions={"macro": SubagentConfig(description="macro desk")}
    )
    for role in ("analyst", "chief_of_staff"):
        assert [d.name for d in registry.get_enabled(["macro"], role=role)] == ["macro"]
