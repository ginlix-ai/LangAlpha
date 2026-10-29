"""The time-up tool stop is the outermost tool wrapper in every stack a turn runs.

Whatever wraps the stop has already acted on a call by the time the refusal
comes back (the Task interceptor spawned the subagent, the file-op stream
reported the write), so in each stack the first middleware that wraps tool
calls has to be the stop. A subagent's result capture is the one exception: it
acts only on what the call returns, and has to stream the refusal as well.
"""

from unittest.mock import MagicMock, patch

import pytest
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.agent.middleware.background_subagent import (
    SubagentToolResultCaptureMiddleware,
)
from ptc_agent.agent.middleware.runtime_context import TimeUpToolStopMiddleware
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


def _config(**llm) -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(name="test-model", **llm),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


def _fake_graph(captured: list[list]):
    def build(_model, **built):
        captured.append(list(built["middleware"]))
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    return build


def _wraps_tools(middleware: object) -> bool:
    cls = type(middleware)
    return (
        cls.awrap_tool_call is not AgentMiddleware.awrap_tool_call
        or cls.wrap_tool_call is not AgentMiddleware.wrap_tool_call
    )


def _first_tool_wrapper(stack: list) -> object:
    return next(
        m
        for m in stack
        if _wraps_tools(m) and not isinstance(m, SubagentToolResultCaptureMiddleware)
    )


@pytest.fixture(scope="module")
def ptc_stacks() -> tuple[list, list]:
    """The main agent's middleware and the one every subagent is built on."""
    from ptc_agent.agent.agent import PTCAgent

    llm = GenericFakeChatModel(messages=iter([]))
    main: list[list] = []
    handed: list[list] = []
    real = SubAgentMiddleware

    def spy(*args, **kwargs):
        handed.append(list(kwargs["default_middleware"]))
        return real(*args, **kwargs)

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch(
            "ptc_agent.agent.middleware.compaction.compact.create_llm",
            return_value=llm,
        ),
        patch("ptc_agent.agent.agent.create_agent", side_effect=_fake_graph(main)),
        patch("ptc_agent.agent.subagents.assembly.SubAgentMiddleware", side_effect=spy),
    ):
        PTCAgent(_config()).create_agent(
            llm=llm, thread_id="t-1", sandbox=MagicMock(), mcp_registry=MagicMock()
        )
    return main[0], handed[0]


def test_the_main_stack_refuses_before_any_tool_wrapper(ptc_stacks):
    main, _ = ptc_stacks
    assert isinstance(_first_tool_wrapper(main), TimeUpToolStopMiddleware)
    assert sum(isinstance(m, TimeUpToolStopMiddleware) for m in main) == 1


def test_the_subagent_stack_refuses_before_any_tool_wrapper(ptc_stacks):
    _, subagent = ptc_stacks
    assert isinstance(_first_tool_wrapper(subagent), TimeUpToolStopMiddleware)
    assert sum(isinstance(m, TimeUpToolStopMiddleware) for m in subagent) == 1


def test_the_stops_it_has_to_beat_are_tool_wrappers(ptc_stacks):
    """Guards the check above against a wrapper it cannot see: the three
    middlewares the refusal used to reach first all wrap tool calls."""
    main, subagent = ptc_stacks
    names = {type(m).__name__ for m in [*main, *subagent] if _wraps_tools(m)}
    assert {
        "BackgroundSubagentMiddleware",
        "FileOperationMiddleware",
        "RepeatCallGuardMiddleware",
    } <= names
