"""Registry wiring for the market-watch skill (PTC-only, tool-less)."""

import ptc_agent.agent.middleware.skills.registry as registry
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_sandbox_skill_names,
    get_skill,
    get_skill_registry,
    list_skills,
)


def test_registered_as_a_tool_less_ptc_skill():
    assert "market-watch" in SKILL_REGISTRY
    skill = SKILL_REGISTRY["market-watch"]
    assert skill.name == "market-watch"
    assert skill.tools == []
    assert skill.skill_md_path == "skills/market-watch/SKILL.md"
    assert skill.exposure == "ptc"
    assert skill.command == "market-watch"
    assert skill.feature == "market_watch"


def test_resolves_in_ptc_mode_only():
    ptc = get_skill("market-watch", mode="ptc")
    assert ptc is not None
    assert ptc.tools == []
    assert ptc.command == "market-watch"

    # Flash has no sandbox/filesystem, so the skill is PTC-exposed only.
    assert get_skill("market-watch", mode="flash") is None
    assert "market-watch" in get_skill_registry("ptc")
    assert "market-watch" not in get_skill_registry("flash")


def test_synced_to_sandbox_and_command_mapped():
    assert "market-watch" in get_sandbox_skill_names()
    assert get_command_to_skill_map("ptc").get("market-watch") == "market-watch"


def test_feature_off_hides_the_skill_everywhere(monkeypatch):
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)

    assert get_skill("market-watch") is None
    assert get_skill("market-watch", mode="ptc") is None
    assert "market-watch" not in get_skill_registry()
    assert "market-watch" not in get_skill_registry("ptc")
    assert "market-watch" not in get_sandbox_skill_names()
    assert "market-watch" not in get_command_to_skill_map("ptc")
    assert "market-watch" not in {s["name"] for s in list_skills("ptc")}


def test_feature_off_leaves_unflagged_skills_untouched(monkeypatch):
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)

    assert get_skill("chart-annotation", mode="ptc") is not None
    assert "pdf" in get_sandbox_skill_names()
    assert "chart-annotation" in {s["name"] for s in list_skills("ptc")}


def test_injected_resolver_overrides_system_gate(monkeypatch):
    # The agent-build path: an injected per-user resolver decides the flagged
    # skill regardless of the system gate. Unflagged skills stay untouched.
    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: True)
    off = get_skill_registry("ptc", feature_resolver=lambda key: False)
    assert "market-watch" not in off
    assert "chart-annotation" in off  # feature is None → resolver never consulted

    monkeypatch.setattr(registry, "is_feature_enabled_system", lambda key: False)
    on = get_skill_registry("ptc", feature_resolver=lambda key: True)
    assert "market-watch" in on
