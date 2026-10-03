"""Where the subagent switch sits in the main agent, and that it changes no prompt or tool."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.utils.function_calling import convert_to_openai_tool

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


def _build(**kwargs) -> dict:
    """The arguments ``create_agent`` hands langchain's builder."""
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    llm = GenericFakeChatModel(messages=iter([]))
    captured: list[dict] = []

    def fake_create_agent(_model, **built):
        captured.append(built)
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch(
            "ptc_agent.agent.middleware.compaction.compact.get_llm_by_type",
            return_value=llm,
        ),
        patch("ptc_agent.agent.agent.create_agent", side_effect=fake_create_agent),
    ):
        PTCAgent(config).create_agent(llm=llm, thread_id="t-1", **kwargs)
    return captured[0]


def _names(built: dict) -> list[str]:
    return [type(m).__name__ for m in built["middleware"]]


@pytest.fixture
def machine():
    return {"sandbox": MagicMock(), "mcp_registry": MagicMock()}


def test_it_sits_after_steering_and_outside_the_task_interceptor(machine):
    names = _names(_build(subagent_switch=AsyncMock(), **machine))

    at = names.index("SubagentSwitchMiddleware")
    assert names[at - 1] == "SteeringMiddleware"
    assert names[at + 1] == "BackgroundSubagentMiddleware"
    assert names.count("SubagentSwitchMiddleware") == 1


def test_no_reader_builds_without_it(machine):
    assert "SubagentSwitchMiddleware" not in _names(_build(**machine))


def test_a_build_without_subagents_has_nothing_to_hold(machine):
    names = _names(
        _build(subagent_switch=AsyncMock(), disable_subagents=True, **machine)
    )
    assert "SubagentSwitchMiddleware" not in names


def test_the_prompt_and_the_tools_are_the_same_either_way(machine):
    without = _build(**machine)
    with_switch = _build(subagent_switch=AsyncMock(return_value=False), **machine)

    assert with_switch["system_prompt"] == without["system_prompt"]
    assert [convert_to_openai_tool(t) for t in with_switch["tools"]] == [
        convert_to_openai_tool(t) for t in without["tools"]
    ]
