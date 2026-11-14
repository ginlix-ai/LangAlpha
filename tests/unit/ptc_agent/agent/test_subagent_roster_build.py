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
    return _task_middleware(roster).subagent_graphs


def _task_middleware(roster: list[str], task_models=None) -> SubAgentMiddleware:
    """The main agent's Task middleware, for ``roster``."""
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        subagents=SubagentsConfig(enabled=roster),
    )
    config.task_models = task_models
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
    return task


@pytest.mark.parametrize("roster", [[], ["research"]])
def test_a_roster_without_general_purpose_compiles_none(roster):
    assert sorted(_task_graphs(roster)) == roster


def test_a_task_on_a_model_its_call_names_gets_a_graph_built_for_it_once_per_turn():
    """Compiled from its definition on that model's client, with that model's
    guidance, and kept for the turn's next task of that type on that model."""
    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.subagents.compiler import SubagentCompiler
    from ptc_agent.config.agent import TaskModel

    client = GenericFakeChatModel(messages=iter([]))
    on = {
        name: TaskModel(name=name, display_name=name, client=client, guidance="lean")
        for name in ("other-model", "third-model")
    }
    task = _task_middleware(["research"], task_models=MagicMock())
    built: list[tuple] = []
    real = sa.create_agent

    def spy(model, **kwargs):
        built.append((model, kwargs["name"]))
        return real(model, **kwargs)

    with (
        patch.object(sa, "create_agent", side_effect=spy),
        patch.object(SubagentCompiler, "compile", autospec=True, side_effect=SubagentCompiler.compile) as compile_,
    ):
        first = task._graph_on("research", on["other-model"])
        again = task._graph_on("research", on["other-model"])
        another = task._graph_on("research", on["third-model"])

    assert first is again
    assert first is not task.subagent_graphs["research"]
    assert another is not first
    assert built == [(client, "research"), (client, "research")]
    assert compile_.call_args_list[0].kwargs == {"client": client, "guidance": "lean"}


def test_a_task_placed_on_another_computer_gets_a_graph_of_its_own():
    """Two Tasks on one Claude Code model that resolved at once can be placed
    apart, and a graph keeps the computer it was built for."""
    from ptc_agent.config.agent import Admitted, TaskModel

    client = GenericFakeChatModel(messages=iter([]))
    placed = [
        TaskModel(name="cc-row", display_name="cc-row", client=client, guidance="lean", claude_code=Admitted(cli_computer=c))
        for c in ("c-a", "c-b", "c-a")
    ]
    task = _task_middleware(["research"], task_models=MagicMock())

    a, b, a_again = (task._graph_on("research", on) for on in placed)

    assert a is not b
    assert a is a_again
