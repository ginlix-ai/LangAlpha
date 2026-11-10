"""The chart layer tool is bound without loading a skill; its skill is guidance only.

Pins the shape that replaced skill gating: no registry skill hides the tools
from a model request, only main-agent prompts name them, and loading the now
tool-less skill does not promise tools it never unlocked.
"""

from unittest.mock import patch

import pytest

from ptc_agent.agent.middleware.skills.content import (
    SkillRequest,
    build_skill_content,
)
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    list_skills,
)
from ptc_agent.agent.prompts import get_loader
from tests.unit.ptc_agent.agent.prompts.test_lean_subset_invariant import (
    SUBAGENTS,
    _render_pair,
)

ANNOTATION_TOOLS = {"manage_chart_layers"}


def test_no_skill_gates_the_layer_tool():
    # _filter_tools hides exactly these names until a skill loads.
    gated = {name for skill in SKILL_REGISTRY.values() for name in skill.tool_names}
    assert not ANNOTATION_TOOLS & gated


def test_the_skill_lists_the_tools_its_switch_removes():
    from src.tools.chart_annotation import CHART_LAYER_TOOLS

    skill = SKILL_REGISTRY["chart-annotation"]
    assert set(skill.switched_tools) == {t.name for t in CHART_LAYER_TOOLS}
    (listed,) = [s for s in list_skills() if s["name"] == "chart-annotation"]
    assert set(listed["tools"]) == ANNOTATION_TOOLS


@pytest.mark.parametrize("enabled", [True, False])
def test_main_prompts_point_at_chart_layers_only_when_bound(enabled):
    loader = get_loader()
    ptc = loader.get_system_prompt(
        subagent_summary="STUB", chart_annotation_enabled=enabled
    )
    assert ("chart layer" in ptc) is enabled
    assert "draw_chart_annotation" not in ptc


@pytest.mark.parametrize("subagent", SUBAGENTS)
def test_subagent_prompts_never_mention_chart_layers(subagent):
    # Subagents never bind it, though the prompt default is on.
    for prompt in _render_pair(subagent):
        assert "chart layer" not in prompt


def test_loading_a_guidance_only_skill_lists_no_tools():
    with patch(
        "ptc_agent.agent.middleware.skills.content.load_skill_content",
        return_value="GUIDE BODY",
    ):
        result = build_skill_content(
            [SkillRequest(name="chart-annotation")], registry=SKILL_REGISTRY
        )

    assert "GUIDE BODY" in result.content
    assert "Available tools" not in result.content
