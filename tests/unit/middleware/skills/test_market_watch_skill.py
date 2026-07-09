"""Registry wiring for the market-watch skill (PTC-only, tool-less)."""

from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_sandbox_skill_names,
    get_skill,
    get_skill_registry,
)


def test_registered_as_a_tool_less_ptc_skill():
    assert "market-watch" in SKILL_REGISTRY
    skill = SKILL_REGISTRY["market-watch"]
    assert skill.name == "market-watch"
    assert skill.tools == []
    assert skill.skill_md_path == "skills/market-watch/SKILL.md"
    assert skill.exposure == "ptc"
    assert skill.command == "market-watch"


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
