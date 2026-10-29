"""Tests for the HTML-output skills (html-report, ui-design) in the registry."""

import re

import pytest
import yaml

from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_skill,
    get_skill_registry,
    list_skills,
    skills_outside_sandbox,
)

# New HTML-output skills under test.
NEW_SKILLS = ("html-report", "ui-design")

# Expected slash-command shortcut per skill (None = no shortcut).
# html-report is user-invocable (like /dashboard); ui-design is a pure design reference.
EXPECTED_COMMANDS = {"html-report": "html-report", "ui-design": None}

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_skill_registered(name):
    """Both new skills are present in the registry as plain, tool-less skills."""
    assert name in SKILL_REGISTRY
    skill = SKILL_REGISTRY[name]
    assert skill.name == name
    assert skill.tool_names == ()
    assert skill.command == EXPECTED_COMMANDS[name]
    assert skill.skill_md_path == f"skills/{name}/SKILL.md"


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_included_in_sandbox_sync_set(name):
    """The sandbox sync picks up the new skills automatically."""
    assert name not in skills_outside_sandbox()


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_enabled_in_registry(name):
    assert name in get_skill_registry()
    assert get_skill(name) is not None


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_listed(name):
    """Non-hidden skills appear in the listing."""
    assert name in {entry["name"] for entry in list_skills()}


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_skill_md_exists_and_frontmatter_parses(name, shipped_skill_md):
    """SKILL.md is on disk with valid YAML frontmatter whose name matches the directory."""
    skill_md = shipped_skill_md(name)

    content = skill_md.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(content)
    assert match, f"no YAML frontmatter in {skill_md}"

    frontmatter = yaml.safe_load(match.group(1))
    assert isinstance(frontmatter, dict)
    assert frontmatter.get("name") == name
    assert str(frontmatter.get("description", "")).strip()


def test_command_shortcuts():
    """html-report is invocable via the /html-report shortcut; ui-design has none."""
    commands = get_command_to_skill_map()
    assert commands.get("html-report") == "html-report"
    assert "ui-design" not in commands.values()


def _shipped_names() -> list[str]:
    from ptc_agent.config.plugins import bundled_skill_dirs

    shipped = {p.parent.name for root in bundled_skill_dirs() for p in root.glob("*/SKILL.md")}
    return sorted(name for name in SKILL_REGISTRY if name in shipped)


@pytest.mark.parametrize("name", _shipped_names())
def test_registry_description_mirrors_frontmatter(name, shipped_skill_md):
    """The registry description is what discovery shows the model; the frontmatter is what
    the agent reads once the skill loads. The two are one string, verbatim."""
    frontmatter = yaml.safe_load(_FRONTMATTER_RE.match(shipped_skill_md(name).read_text(encoding="utf-8")).group(1))
    assert frontmatter["description"] == SKILL_REGISTRY[name].description, name
