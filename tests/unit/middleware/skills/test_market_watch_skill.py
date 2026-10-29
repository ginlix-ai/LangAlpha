"""Registry wiring for the market-watch skill (tool-less)."""

import ptc_agent.agent.middleware.skills.registry as registry
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_skill,
    get_skill_registry,
    list_skills,
    skills_outside_sandbox,
)


def test_registered_as_a_tool_less_skill():
    assert "market-watch" in SKILL_REGISTRY
    skill = SKILL_REGISTRY["market-watch"]
    assert skill.name == "market-watch"
    assert skill.tool_names == ()
    assert skill.skill_md_path == "skills/market-watch/SKILL.md"
    assert not skill.hidden
    assert skill.command == "market-watch"
    assert skill.feature == "market_watch"


def test_resolves_when_enabled():
    skill = get_skill("market-watch")
    assert skill is not None
    assert skill.tool_names == ()
    assert skill.command == "market-watch"
    assert "market-watch" in get_skill_registry()


def test_synced_to_sandbox_and_command_mapped():
    assert "market-watch" not in skills_outside_sandbox()
    assert get_command_to_skill_map().get("market-watch") == "market-watch"


def test_feature_off_hides_the_skill_everywhere(monkeypatch):
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)

    assert get_skill("market-watch") is None
    assert "market-watch" not in get_skill_registry()
    assert "market-watch" in skills_outside_sandbox()
    assert "market-watch" not in get_command_to_skill_map()
    assert "market-watch" not in {s["name"] for s in list_skills()}


def test_feature_off_leaves_unflagged_skills_untouched(monkeypatch):
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)

    assert get_skill("chart-annotation") is not None
    assert "pdf" in SKILL_REGISTRY
    assert "pdf" not in skills_outside_sandbox()
    assert "chart-annotation" in {s["name"] for s in list_skills()}


def test_injected_resolver_overrides_system_gate(monkeypatch):
    # The agent-build path: an injected per-user resolver decides the flagged
    # skill regardless of the system gate. Unflagged skills stay untouched.
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: True)
    off = get_skill_registry(feature_resolver=lambda key: False)
    assert "market-watch" not in off
    assert "chart-annotation" in off  # feature is None → resolver never consulted

    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)
    on = get_skill_registry(feature_resolver=lambda key: True)
    assert "market-watch" in on
