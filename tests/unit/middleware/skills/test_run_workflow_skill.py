"""Tests for the run-workflow skill that gates the RunWorkflow factory tool.

RunWorkflow is a per-thread factory tool (created in agent.py), so the skill
gates it by name via SkillDefinition.tool_names rather than a tool object.
"""

import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

import ptc_agent.agent.middleware.skills.middleware as middleware_module
import ptc_agent.agent.middleware.skills.registry as registry
from ptc_agent.agent.middleware.skills.middleware import SkillsMiddleware
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_sandbox_skill_names,
    get_skill,
    get_skill_registry,
    list_skills,
)

SKILL_NAME = "run-workflow"
TOOL_NAME = "RunWorkflow"

# Repo root: tests/unit/middleware/skills/ -> repo root is four parents up.
REPO_ROOT = Path(__file__).resolve().parents[4]

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


class _FakeRequest:
    """Minimal stand-in for ModelRequest as consumed by _filter_tools."""

    def __init__(self, tools, state, system_message=None):
        self.tools = tools
        self.state = state
        self.system_message = system_message

    def override(self, **kwargs):
        new = _FakeRequest(self.tools, self.state, self.system_message)
        for key, value in kwargs.items():
            setattr(new, key, value)
        return new


def _tool_names(request):
    return [t.name for t in request.tools]


def test_skill_registered_as_name_gated_ptc_skill():
    skill = SKILL_REGISTRY[SKILL_NAME]
    assert skill.name == SKILL_NAME
    assert skill.tools == []
    assert skill.tool_names == (TOOL_NAME,)
    assert skill.get_tool_names() == [TOOL_NAME]
    assert skill.skill_md_path == f"skills/{SKILL_NAME}/SKILL.md"
    assert skill.exposure == "ptc"
    assert SKILL_NAME in get_skill_registry("ptc")
    assert SKILL_NAME not in get_skill_registry("flash")
    assert SKILL_NAME in get_sandbox_skill_names()


def test_skill_md_frontmatter_matches_registry():
    """SKILL.md exists and its frontmatter description stays in sync with the registry."""
    skill = SKILL_REGISTRY[SKILL_NAME]
    skill_md = REPO_ROOT / skill.skill_md_path
    assert skill_md.is_file(), f"missing {skill_md}"

    match = _FRONTMATTER_RE.match(skill_md.read_text(encoding="utf-8"))
    assert match, f"no YAML frontmatter in {skill_md}"
    frontmatter = yaml.safe_load(match.group(1))
    assert frontmatter["name"] == SKILL_NAME
    assert frontmatter["description"] == skill.description


def test_tool_hidden_until_skill_loaded():
    middleware = SkillsMiddleware(mode="ptc")
    assert middleware._tool_to_skills[TOOL_NAME] == {SKILL_NAME}

    run_workflow = SimpleNamespace(name=TOOL_NAME)
    plain = SimpleNamespace(name="execute_code")

    hidden = middleware._filter_tools(
        _FakeRequest(tools=[run_workflow, plain], state={})
    )
    assert _tool_names(hidden) == ["execute_code"]

    visible = middleware._filter_tools(
        _FakeRequest(
            tools=[run_workflow, plain],
            state={"loaded_skills": [SKILL_NAME]},
        )
    )
    assert _tool_names(visible) == [TOOL_NAME, "execute_code"]


def test_manifest_advertises_skill_and_gated_tool():
    middleware = SkillsMiddleware(mode="ptc")
    manifest = middleware._build_combined_manifest({})
    assert f"**{SKILL_NAME}**" in manifest
    assert f"(tools: {TOOL_NAME})" in manifest


def test_read_of_skill_md_autoloads():
    middleware = SkillsMiddleware(mode="ptc")
    matched = middleware._match_skill_from_read(
        "Read", {"file_path": f".agents/skills/{SKILL_NAME}/SKILL.md"}
    )
    assert matched == SKILL_NAME


def test_build_gate_drop_ungates_nothing():
    """agent.py pops the skill on a build that registers no RunWorkflow tool
    (the recursion gate) — no gate entry remains."""
    build_registry = get_skill_registry("ptc")
    build_registry.pop(SKILL_NAME, None)
    middleware = SkillsMiddleware(skill_registry=build_registry, mode="ptc")
    assert TOOL_NAME not in middleware._tool_to_skills
    manifest = middleware._build_combined_manifest({})
    assert f"**{SKILL_NAME}**" not in manifest


def test_kill_switch_drops_the_skill_from_every_accessor(monkeypatch):
    """The deployment switch must reach sandbox sync too: leaving the skill in
    that set keeps uploading its SKILL.md, which the agent then reads."""
    monkeypatch.setattr(
        registry,
        "get_workflow_orchestration_config",
        lambda: SimpleNamespace(enabled=False),
    )

    assert get_skill(SKILL_NAME) is None
    assert get_skill(SKILL_NAME, mode="ptc") is None
    assert SKILL_NAME not in get_skill_registry("ptc")
    assert SKILL_NAME not in get_sandbox_skill_names()
    assert SKILL_NAME not in {s["name"] for s in list_skills("ptc")}
    assert "pdf" in get_sandbox_skill_names()


@pytest.mark.asyncio
async def test_gated_skill_is_not_rediscovered_from_the_sandbox(monkeypatch):
    """The sandbox still carries the SKILL.md of a skill this build gated off;
    filesystem discovery must not advertise it back as a user-installed skill."""

    async def _fake_discover(backend, source, known):
        return [{"name": SKILL_NAME, "description": "placeholder", "confirmed": True}]

    monkeypatch.setattr(middleware_module, "adiscover_skills", _fake_discover)

    build_registry = get_skill_registry("ptc")
    build_registry.pop(SKILL_NAME, None)
    middleware = SkillsMiddleware(
        skill_registry=build_registry,
        mode="ptc",
        backend=MagicMock(),
        sources=[".agents/skills/"],
    )

    update = await middleware.abefore_agent({}, MagicMock(), config=None)
    assert update["discovered_skills"] == []
    assert f"**{SKILL_NAME}**" not in (middleware._build_combined_manifest(update) or "")
