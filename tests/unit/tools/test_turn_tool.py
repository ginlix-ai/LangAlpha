"""A tool built every turn infers its input schema once, and the schema is
the one ``@tool`` would infer."""

from typing import Annotated
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.tools import InjectedToolCallId, StructuredTool, tool
from langchain_core.utils.function_calling import convert_to_openai_tool

from src.tools.turn_tool import _SCHEMAS, turn_tool


def _factory(make, label: str):
    async def lookup(
        symbol: Annotated[str, "Ticker to look up"],
        limit: int = 5,
        tool_call_id: Annotated[str, InjectedToolCallId] = "",
    ) -> str:
        """Look a symbol up."""
        return f"{label}:{symbol}:{limit}"

    return make("Lookup", response_format="content")(lookup)


def test_the_schema_matches_tool_and_is_built_once():
    first, second = _factory(turn_tool, "a"), _factory(turn_tool, "b")
    plain = _factory(tool, "a")
    assert first.args_schema is second.args_schema
    assert plain.args_schema is not _factory(tool, "a").args_schema
    assert convert_to_openai_tool(first) == convert_to_openai_tool(plain)
    assert first.tool_call_schema.model_json_schema() == plain.tool_call_schema.model_json_schema()
    assert first.description == plain.description


def test_an_argument_described_by_the_turn_gets_its_own_schema():
    def factory(reach: str):
        async def send(files: Annotated[list[str], f"Files from {reach}"]) -> str:
            """Send files."""
            return ""

        return turn_tool("send")(send)

    home, anywhere = factory("Home"), factory("any computer")
    assert home.args["files"]["description"] == "Files from Home"
    assert anywhere.args["files"]["description"] == "Files from any computer"
    assert factory("Home").args_schema is not home.args_schema
    assert factory("Home").args["files"]["description"] == "Files from Home"


@pytest.mark.asyncio
async def test_each_turns_tool_runs_its_own_closure():
    first, second = _factory(turn_tool, "a"), _factory(turn_tool, "b")
    call = {"name": "Lookup", "args": {"symbol": "NVDA"}, "id": "c1", "type": "tool_call"}
    assert (await first.ainvoke(call)).content == "a:NVDA:5"
    assert (await second.ainvoke(call)).content == "b:NVDA:5"


def _agent_tools(role: str) -> list[StructuredTool]:
    """Every tool one build of the agent in a workspace makes through
    ``from_function``: the main agent's own Bash and ExecuteCode take one."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import (
        DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig,
    )
    from ptc_agent.core.project_context import ProjectContext

    config = AgentConfig(
        llm=LLMConfig(name="claude-haiku-5-5"), security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    model = FakeListChatModel(responses=["x"])
    made: list[StructuredTool] = []
    build = StructuredTool.from_function.__func__

    def recorded(cls, *args, **kwargs):
        made.append(build(cls, *args, **kwargs))
        return made[-1]

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=model),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=model),
        patch.object(StructuredTool, "from_function", classmethod(recorded)),
    ):
        PTCAgent(config).create_agent(
            llm=model, thread_id="t-1", user_id="u-1", mcp_registry=MagicMock(),
            sandbox=MagicMock(), role=role, project=ProjectContext(workspace_id="w-1", dir_name="alpha"),
        )
    return made


@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
def test_every_turn_tool_has_the_schema_langchain_infers(role):
    """On a turn that reuses the cached schemas, each tool's input is what
    LangChain infers from that turn's own function: a LangChain upgrade that
    infers differently, or a signature the cache key misses, fails here."""
    _agent_tools(role)
    cached = {id(schema) for _, schema in _SCHEMAS.values()}
    reused = [t for t in _agent_tools(role) if id(t.args_schema) in cached]
    assert {t.name for t in reused} >= {"Read", "Write", "Edit", "Glob", "Grep", "Bash", "ExecuteCode"}
    for built in reused:
        inferred = StructuredTool.from_function(
            func=built.func, coroutine=built.coroutine, name=built.name, description=built.description,
        )
        assert convert_to_openai_tool(built) == convert_to_openai_tool(inferred), built.name
        assert built.args == inferred.args, built.name
        assert list(built.args_schema.model_fields) == list(inferred.args_schema.model_fields), built.name
