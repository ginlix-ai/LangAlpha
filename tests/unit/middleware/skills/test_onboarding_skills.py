"""The onboarding kickoff's skills: the hidden ``onboarding`` skill and the
user-visible ``portfolio-diagnosis`` its health-check pick hands off to.

The web's kickoff names ``onboarding`` alone, with an instruction. The agent
imports, proposes three first tasks, and reads the diagnosis body itself when
the user picks the health check, so that body has to ship where the onboarding
body says it is.
"""

import re

import pytest
import yaml

from ptc_agent.agent.middleware.skills.content import (
    SkillRequest,
    build_skill_content,
    resolve_skill_dir,
)
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    get_command_to_skill_map,
    get_skill,
    list_skills,
    skills_outside_sandbox,
)

# The user wrote nothing: the app opened the conversation for them. Import
# from the brokerage, then propose three, one of them built on the day's
# market brief that rides the message.
PROPOSE = (
    "Onboarding kickoff. Action: propose. Language: en-US. Brokerage: moomoo. "
    "Market brief: attached. This is the user's first conversation, right after "
    "onboarding. Their investing profile is in your system prompt; do not ask "
    "for it again."
)

# With nothing connected: the brokerages the user said they use, by display
# name (one typed, not listed), and how many screenshots ride the message.
SCREENSHOTS = (
    "Onboarding kickoff. Action: propose. Language: zh-CN. Brokerage: none. "
    "Brokerages used: Fidelity, 华泰证券. Screenshots: 2. Market brief: attached. "
    "This is the user's first conversation, right after onboarding. Their "
    "investing profile is in your system prompt; do not ask for it again."
)

# The user wrote their own first task, and no brief was generated today.
FIRST_TASK = (
    "Onboarding kickoff. Action: first_task. Language: en-US. Brokerage: none. "
    "This is the user's first conversation, right after onboarding. Their "
    "investing profile is in your system prompt; do not ask for it again."
)

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _body(shipped_skill_md) -> str:
    return shipped_skill_md("onboarding").read_text(encoding="utf-8")


def test_portfolio_diagnosis_is_a_user_visible_skill():
    skill = SKILL_REGISTRY["portfolio-diagnosis"]
    assert skill.hidden is False
    assert skill.tool_names == ()
    assert skill.skill_md_path == "skills/portfolio-diagnosis/SKILL.md"
    assert get_command_to_skill_map()["portfolio-diagnosis"] == "portfolio-diagnosis"
    assert "portfolio-diagnosis" in {s["name"] for s in list_skills()}
    assert "portfolio-diagnosis" not in skills_outside_sandbox()


def test_onboarding_stays_hidden_but_resolves_by_name():
    skill = SKILL_REGISTRY["onboarding"]
    assert skill.hidden is True
    assert skill.command is None
    assert "onboarding" not in {s["name"] for s in list_skills()}
    assert get_skill("onboarding") is skill


def test_onboarding_description_matches_its_frontmatter(shipped_skill_md):
    """The registry's copy drives discovery and the frontmatter drives the
    sandbox manifest, so the two must say the same thing."""
    match = _FRONTMATTER_RE.match(_body(shipped_skill_md))
    assert match
    frontmatter = yaml.safe_load(match.group(1))
    assert frontmatter["description"] == SKILL_REGISTRY["onboarding"].description


@pytest.mark.parametrize("name", ["onboarding", "portfolio-diagnosis"])
def test_skill_dir_resolves_to_the_shipped_bundle(name, shipped_skill_md):
    assert resolve_skill_dir(name) == shipped_skill_md(name).parent


@pytest.mark.parametrize("instruction", [PROPOSE, SCREENSHOTS, FIRST_TASK])
def test_kickoff_loads_the_onboarding_body_alone_with_its_instruction(instruction):
    result = build_skill_content([SkillRequest("onboarding", instruction)])

    assert result is not None
    assert result.loaded_skill_names == ["onboarding"]
    assert "# Onboarding" in result.content
    # The diagnosis body is no longer sent beside it: the agent reads it only
    # when the user picks the health check.
    assert "# Portfolio Diagnosis" not in result.content
    # One skill in the message, so the instruction rides in the bare form,
    # whole, with every field the agent reads.
    assert f"[Instruction: {instruction}]" in result.content


def test_onboarding_points_at_the_diagnosis_skill_that_ships(shipped_skill_md):
    body = _body(shipped_skill_md)
    assert ".agents/skills/portfolio-diagnosis/SKILL.md" in body
    assert shipped_skill_md("portfolio-diagnosis").is_file()


@pytest.mark.parametrize(
    "instruction, fields",
    [
        (PROPOSE, ["Action", "Language", "Brokerage", "Market brief"]),
        (
            SCREENSHOTS,
            ["Action", "Language", "Brokerage", "Brokerages used", "Screenshots", "Market brief"],
        ),
        (FIRST_TASK, ["Action", "Language", "Brokerage"]),
    ],
)
def test_the_skill_names_every_field_the_kickoff_sends(
    instruction, fields, shipped_skill_md
):
    """The kickoff is ``Field: value.`` pairs. A field the body never names is
    one the agent has to guess the meaning of."""
    assert re.findall(r"(?:^|\. )([A-Z][a-z]+(?: [a-z]+)*): ", instruction) == fields
    body = _body(shipped_skill_md)
    for field in fields:
        assert f"`{field}`" in body, field


def test_the_skill_names_every_action_and_the_brief_block(shipped_skill_md):
    """Both actions the web sends, and the block the brief arrives in."""
    body = _body(shipped_skill_md)
    for action in ("propose", "first_task"):
        assert f"`{action}`" in body, action
    assert "`<market-brief>`" in body


def test_the_proposal_names_the_question_tool_as_it_is_bound(shipped_skill_md):
    """The proposal step spells out the tool's arguments; they must be the
    ones the bound tool takes."""
    from ptc_agent.agent.middleware.ask_user import AskUserMiddleware

    (tool,) = AskUserMiddleware().tools
    body = _body(shipped_skill_md)
    assert f"`{tool.name}`" in body
    for arg in ("question", "options", "allow_multiple"):
        assert arg in tool.args, arg
        assert f"`{arg}`" in body, arg
