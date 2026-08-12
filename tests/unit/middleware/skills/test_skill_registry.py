"""Tests for the HTML-output skills (html-report, ui-design) in the registry."""

import re
from pathlib import Path

import pytest
import yaml

from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_sandbox_skill_names,
    get_skill,
    get_skill_registry,
    list_skills,
)

# New HTML-output skills under test.
NEW_SKILLS = ("html-report", "ui-design")

# Expected slash-command shortcut per skill (None = no shortcut).
# html-report is user-invocable (like /dashboard); ui-design is a pure design reference.
EXPECTED_COMMANDS = {"html-report": "html-report", "ui-design": None}

# Repo root: tests/unit/middleware/skills/ -> repo root is four parents up.
REPO_ROOT = Path(__file__).resolve().parents[4]

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_skill_registered(name):
    """Both new skills are present in the registry as plain, tool-less PTC skills."""
    assert name in SKILL_REGISTRY
    skill = SKILL_REGISTRY[name]
    assert skill.name == name
    assert skill.tools == []
    assert skill.command == EXPECTED_COMMANDS[name]
    assert skill.skill_md_path == f"skills/{name}/SKILL.md"


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_exposure_is_ptc(name):
    """Flash has no sandbox/filesystem, so these are PTC-only."""
    assert SKILL_REGISTRY[name].exposure == "ptc"


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_included_in_sandbox_sync_set(name):
    """get_sandbox_skill_names() picks up the new PTC skills automatically."""
    assert name in get_sandbox_skill_names()


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_exposed_to_ptc_not_flash(name):
    """Mode filtering: visible to PTC, hidden from Flash."""
    assert name in get_skill_registry("ptc")
    assert name not in get_skill_registry("flash")
    assert get_skill(name, mode="ptc") is not None
    assert get_skill(name, mode="flash") is None


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_listed_for_ptc(name):
    """Non-hidden skills appear in the PTC listing."""
    ptc_names = {entry["name"] for entry in list_skills("ptc")}
    assert name in ptc_names


@pytest.mark.parametrize("name", NEW_SKILLS)
def test_skill_md_exists_and_frontmatter_parses(name):
    """SKILL.md is on disk with valid YAML frontmatter whose name matches the directory."""
    skill_md = REPO_ROOT / SKILL_REGISTRY[name].skill_md_path
    assert skill_md.is_file(), f"missing {skill_md}"

    content = skill_md.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(content)
    assert match, f"no YAML frontmatter in {skill_md}"

    frontmatter = yaml.safe_load(match.group(1))
    assert isinstance(frontmatter, dict)
    assert frontmatter.get("name") == name
    assert str(frontmatter.get("description", "")).strip()


def test_command_shortcuts():
    """html-report is invocable via the /html-report shortcut; ui-design has none."""
    commands = get_command_to_skill_map("ptc")
    assert commands.get("html-report") == "html-report"
    assert "ui-design" not in commands.values()
