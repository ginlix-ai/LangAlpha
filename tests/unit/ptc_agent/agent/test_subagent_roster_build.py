"""The Task tool reaches exactly the roster a build is given: no implicit general-purpose subagent outlives a user turning theirs off."""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.config.agent import AgentConfig, LLMConfig, SubagentsConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


def _task_graphs(roster: list[str]) -> dict:
    """The graphs the main agent's Task tool dispatches to, for ``roster``."""
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        subagents=SubagentsConfig(enabled=roster),
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
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=fake_create_agent),
    ):
        PTCAgent(config).create_agent(
            llm=llm, thread_id="t-1",
            sandbox=MagicMock(), mcp_registry=MagicMock(),
        )
    (task,) = [m for m in captured[0]["middleware"] if isinstance(m, SubAgentMiddleware)]
    return task.subagent_graphs


@pytest.mark.parametrize("roster", [[], ["research"]])
def test_a_roster_without_general_purpose_compiles_none(roster):
    assert sorted(_task_graphs(roster)) == roster
