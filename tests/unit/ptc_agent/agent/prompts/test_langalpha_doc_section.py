"""Where the system prompt names langalpha-doc's pages.

`<langalpha_doc>` lists the cases for opening the skill with the page for
each, so the main agent's prompt names every page there and nowhere else. A
turn without the skill (skills off for the build, or the user switched the
skill off) has no page to read, so its prompt names none. A subagent's prompt
has no such section and keeps the pointer each of its sections carries.
"""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.agent.prompts import get_loader, guidance_template_vars
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)

PREFIX = ".agents/skills/langalpha-doc/"
# Page -> the build flags its case needs.
PAGES = {
    "references/history.md": {"files_mounted": True, "workspace_folder": "Research"},
    "references/files.md": {},
    "references/drive.md": {"drive_enabled": True},
    "references/computer.md": {},
    "references/skills.md": {},
    "references/plugins.md": {},
    "references/chat.md": {"channels_enabled": True},
    "SKILL.md": {},
}
ALL_ON = {k: v for flags in PAGES.values() for k, v in flags.items()}


def _prompt(**kwargs) -> str:
    return get_loader().get_system_prompt(current_time="2026-10-08 09:00 ET", **kwargs)


def _section(prompt: str) -> str:
    return prompt[prompt.index("<langalpha_doc>") : prompt.index("</langalpha_doc>")]


def _built_prompt(*, skills: bool = True, disabled: frozenset[str] = frozenset()) -> str:
    """The system prompt ``create_agent`` hands langchain's builder."""
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    config.skills.enabled = skills
    config.disabled_skills = disabled
    llm = GenericFakeChatModel(messages=iter([]))
    captured: list[dict] = []

    def fake_create_agent(_model, **built):
        captured.append(built)
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=fake_create_agent),
    ):
        PTCAgent(config).create_agent(
            llm=llm, thread_id="t-1", sandbox=MagicMock(), mcp_registry=MagicMock()
        )
    return str(captured[0]["system_prompt"])


def test_a_turn_s_own_prompt_carries_the_section():
    # Through the build a turn uses: the section is gated on a flag only that
    # build can pass, and a template test passing it by hand stays green
    # while no agent ever sees the section.
    assert "<langalpha_doc>" in _built_prompt()


@pytest.mark.parametrize(
    "without", [{"skills": False}, {"disabled": frozenset({"langalpha-doc"})}]
)
def test_a_turn_without_the_skill_is_pointed_at_none_of_its_pages(without):
    # The skill's files are not on the computer then, so a pointer at one
    # sends the agent to read a file that is not there.
    prompt = _built_prompt(**without)

    assert "<langalpha_doc>" not in prompt
    assert PREFIX not in prompt


@pytest.mark.parametrize("guidance", ["lean", "detailed"])
@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
def test_the_section_names_every_page_and_nothing_else_does(guidance, role):
    prompt = _prompt(
        langalpha_doc_enabled=True, role=role, **ALL_ON, **guidance_template_vars(guidance)
    )
    section = _section(prompt)

    for page in PAGES:
        assert f"`{PREFIX}{page}`" in section
        assert prompt.count(PREFIX + page) == 1


@pytest.mark.parametrize("page", ["references/drive.md", "references/chat.md"])
def test_a_case_the_build_lacks_is_left_out(page):
    assert PREFIX + page not in _prompt(langalpha_doc_enabled=True)


def test_the_save_command_is_named_only_where_bash_runs_it():
    assert "`la skills save <name>`" in _section(_prompt(langalpha_doc_enabled=True))
    assert "la skills save" not in _prompt(langalpha_doc_enabled=True, bash_enabled=False)


def test_a_subagent_keeps_the_pages_its_own_sections_name():
    prompt = get_loader().get_subagent_base_prompt(
        sections={"drive_system": True, "workspace_context": True},
        drive_enabled=True,
        files_mounted=True,
        workspace_folder="Research",
    )

    assert "<langalpha_doc>" not in prompt
    assert f"`{PREFIX}references/drive.md`" in prompt
    assert f"`{PREFIX}references/history.md`" in prompt
