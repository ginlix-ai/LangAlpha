"""Where the subagent switch sits in the main agent, and that it changes no prompt or tool."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.utils.function_calling import convert_to_openai_tool

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.agent.middleware import SubAgentMiddleware
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


def test_a_build_without_it_still_declares_its_state(machine):
    """Thread maintenance builds without a reader; a write through that
    build must not erase the last notice's value (``main_state``)."""
    from ptc_agent.agent.main_state import MainAgentState

    assert _build(**machine)["state_schema"] is MainAgentState


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


def _scratchpad_build(machine) -> tuple[dict, list]:
    """The main build with the scratchpad flag on, and the stack subagents get."""
    from ptc_agent.core.paths import WorkspaceLayout

    machine["sandbox"].workspace.return_value = WorkspaceLayout("/home/workspace", "acme")
    handed: list = []
    real = SubAgentMiddleware

    def spy(*args, **kwargs):
        handed.append(kwargs["default_middleware"])
        return real(*args, **kwargs)

    with (
        patch.object(AgentConfig, "feature_enabled", lambda self, k: k == "scratchpad"),
        patch("ptc_agent.agent.agent.SubAgentMiddleware", side_effect=spy),
    ):
        built = _build(**machine)
    return built, handed


def test_the_notes_prompt_sits_inside_compaction_on_the_main_stack_only(machine):
    from ptc_agent.agent.middleware import CompactionMiddleware

    built, handed = _scratchpad_build(machine)
    names = _names(built)

    order = [
        names.index(n)
        for n in (
            "CompactionMiddleware",
            "BaselineContextMiddleware",
            "NotesDueMiddleware",
            "TailEnvelopeMiddleware",
        )
    ]
    assert order == sorted(order)

    main = next(m for m in built["middleware"] if isinstance(m, CompactionMiddleware))
    assert main._notes_dir.endswith("/.agents/scratchpad/t-1/note")

    assert len(handed) == 1
    sub_names = [type(m).__name__ for m in handed[0]]
    assert "NotesDueMiddleware" not in sub_names
    sub = next(m for m in handed[0] if isinstance(m, CompactionMiddleware))
    assert sub._notes_dir is None


def test_without_the_scratchpad_its_rows_stay_out_of_the_calls(machine):
    names = _names(_build(**machine))

    assert "NotesDueMiddleware" not in names
    at = names.index("NotesOffMiddleware")
    assert names[at - 1] == "BaselineContextMiddleware"
    assert names[at + 1] == "TailEnvelopeMiddleware"


def _compactions(built: dict, handed: list | None = None):
    from ptc_agent.agent.middleware import CompactionMiddleware

    stacks = [built["middleware"], *(handed or [])]
    return [[m for m in stack if isinstance(m, CompactionMiddleware)] for stack in stacks]


def test_the_window_trims_the_main_agent_only(machine):
    """A subagent's run is bounded, and its transcript reads its namespace's
    whole history, so only the main stack's compaction gets the window."""
    from ptc_agent.agent.middleware.subagent_switch import subagents_window_carry

    real = SubAgentMiddleware
    handed: list = []

    def spy(*args, **kwargs):
        handed.append(kwargs["default_middleware"])
        return real(*args, **kwargs)

    coverage = AsyncMock(return_value=True)
    with patch("ptc_agent.agent.agent.SubAgentMiddleware", side_effect=spy):
        built = _build(subagent_switch=AsyncMock(), window_coverage=coverage, **machine)
    (main,), (sub,) = _compactions(built, handed)
    assert main._coverage is coverage
    assert subagents_window_carry in main._carries and len(main._carries) == 2
    assert sub._coverage is None


def test_the_window_needs_only_the_server(machine):
    """A build without the switch's middleware (a notification turn) trims
    too, the carry keeping a field ``MainAgentState`` declares; without the
    server to ask there is nothing to trim against."""
    coverage = AsyncMock(return_value=True)
    for kwargs in (
        {"subagent_switch": AsyncMock(), "disable_subagents": True, "window_coverage": coverage},
        {"window_coverage": coverage},
    ):
        ((main,),) = _compactions(_build(**kwargs, **machine))
        assert main._coverage is coverage, kwargs
        assert len(main._carries) == 2, kwargs
    ((main,),) = _compactions(_build(subagent_switch=AsyncMock(), **machine))
    assert main._coverage is None
