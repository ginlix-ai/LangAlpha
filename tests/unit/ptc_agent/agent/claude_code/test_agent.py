"""A Claude Code turn takes the system prompt and the tool node of the agent a turn builds."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import START, MessagesState, StateGraph

from ptc_agent.agent.claude_code.agent import ClaudeCodeGraph, claude_code_graph
from ptc_agent.agent.claude_code.runtime import TURN_KEY
from src.llms.llm import create_llm


@tool("Bash")
def bash(command: str) -> str:
    """Run a shell command."""
    return f"ran: {command}"


class _Tagged(AgentMiddleware):
    """A tool-call middleware, as langalpha's stack wraps every call."""

    async def awrap_tool_call(self, request, handler):
        result = await handler(request)
        result.content = f"tagged {result.content}"
        return result


class _Opens(AgentMiddleware):
    """A turn-entry hook, as the context middleware write the turn's rows."""

    async def abefore_agent(self, state, runtime):
        return {"messages": [HumanMessage("row")]}


_OPENS = _Opens()


def _agent():
    return create_agent(
        FakeListChatModel(responses=["x"]), tools=[bash], middleware=[_Tagged(), _OPENS]
    ).with_config({"recursion_limit": 9999})


def _graph(**kw):
    target = create_llm("claude-haiku-5-5-code", reasoning_effort="low")
    return claude_code_graph(
        _agent(), system_prompt="the prompt", target=target, checkpointer=None,
        **{"user_id": "user-1", "thread_id": "thread-1", **kw},
    )


def test_the_turn_runs_the_agents_prompt_and_tools():
    graph = _graph()
    turn = graph.config["configurable"][TURN_KEY]
    assert turn is graph.turn
    assert graph.config["recursion_limit"] == 9999
    assert (turn.identity, turn.thread_key) == ("user-1", "thread-1")
    assert (turn.spec.system_prompt, turn.spec.model, turn.spec.effort) == ("the prompt", "claude-haiku-5-5", "low")
    assert [t.name for t in turn.spec.tools] == ["Bash"]
    assert turn.spec.billing["billing_type"] == "oauth" and "lc_versions" not in turn.spec.billing


@pytest.mark.asyncio
async def test_tool_calls_go_through_the_agents_tool_middleware():
    built = []
    with patch("ptc_agent.agent.claude_code.agent.build_claude_code_graph", side_effect=lambda node, **_: built.append(node) or MagicMock()):
        _graph()
    g = StateGraph(MessagesState)
    g.add_node("tools", built[0])
    g.add_edge(START, "tools")
    call = AIMessage("", tool_calls=[{"name": "Bash", "args": {"command": "ls"}, "id": "c1", "type": "tool_call"}])
    out = await g.compile().ainvoke({"messages": [call]})
    assert [m.content for m in out["messages"] if isinstance(m, ToolMessage)] == ["tagged ran: ls"]


def test_a_turn_without_a_user_or_a_thread_is_refused():
    with pytest.raises(RuntimeError, match="for a user, on a thread"):
        _graph(user_id=None)


def test_the_turn_opens_with_the_agents_entry_hooks_by_name():
    built = []
    with patch("ptc_agent.agent.claude_code.agent.build_claude_code_graph", side_effect=lambda node, **kw: built.append(kw) or MagicMock()):
        _graph(entry=[_OPENS])
    [(name, hook)] = built[0]["entry"]
    assert name == "_Opens.before_agent"
    g = StateGraph(MessagesState)
    g.add_node(name, hook)
    g.add_edge(START, name)
    out = asyncio.run(g.compile().ainvoke({"messages": [HumanMessage("ask")]}))
    assert [m.content for m in out["messages"]] == ["ask", "row"]


@pytest.mark.asyncio
async def test_the_turn_closes_when_its_stream_ends_however_it_ends():
    closed = []

    class _Turn:
        async def close(self):
            closed.append(True)

    class _Compiled:
        checkpointer = "saver"

        async def astream(self, *args, **kwargs):
            yield 1
            raise RuntimeError("the run failed")

    graph = ClaudeCodeGraph(_Compiled(), _Turn())
    assert graph.checkpointer == "saver"
    with pytest.raises(RuntimeError):
        async for _ in graph.astream({}):
            pass
    assert closed == [True]


@pytest.mark.asyncio
async def test_a_stream_closed_early_finishes_the_graph_before_the_cli_goes():
    """The producer closes the stream at its time limit; the steps still
    running must end before the CLI they talk to is closed."""
    order = []

    class _Turn:
        async def close(self):
            order.append("cli closed")

    class _Compiled:
        async def astream(self, *args, **kwargs):
            try:
                yield 1
                yield 2
            finally:
                order.append("graph closed")

    stream = ClaudeCodeGraph(_Compiled(), _Turn()).astream({})
    assert await anext(stream) == 1
    await stream.aclose()
    assert order == ["graph closed", "cli closed"]


def _built(**kwargs) -> ClaudeCodeGraph:
    """The real build of a Claude Code turn."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    config = AgentConfig(
        llm=LLMConfig(name="claude-haiku-5-5-code"), security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    target = create_llm("claude-haiku-5-5-code")
    with patch.object(AgentConfig, "get_llm_client", return_value=target):
        graph = PTCAgent(config).create_agent(
            llm=target, thread_id="t-1", user_id="u-1", sandbox=MagicMock(), mcp_registry=MagicMock(), **kwargs
        )
    assert isinstance(graph, ClaudeCodeGraph)
    return graph


def test_a_claude_code_turn_opens_with_the_skills_turn_and_baseline_hooks():
    """Claude Code takes the hooks that write the turn's rows, inject
    requested skills and freeze the baseline, in the stack's order."""
    graph = _built()
    edges = {a: b for a, b in graph.graph.builder.edges}
    path, node = [], "__start__"
    while node in edges:
        node = edges[node]
        path.append(node)
    assert path == ["SkillsMiddleware.before_agent", "TurnContextMiddleware.before_agent",
                    "BaselineContextMiddleware.before_agent", "model"]
    assert graph.turn._baseline.__self__.__class__.__name__ == "BaselineContextMiddleware"
    assert "AskUserQuestion" not in {t.name for t in graph.turn.spec.tools}
    assert not {"Task", "RunWorkflow"} & {t.name for t in graph.turn.spec.tools}


def test_a_claude_code_turn_is_offered_no_tool_that_places_an_order():
    """An order is put to the user before it runs, which a Claude Code turn cannot do yet."""
    from langchain_core.tools import StructuredTool

    from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY

    def place_order(symbol: str) -> str:
        """Place an order."""
        return "placed"

    def get_quote(symbol: str) -> str:
        """Get a quote."""
        return "quote"

    order = StructuredTool.from_function(func=place_order, name="mcp__broker__place_order")
    order.metadata = {METADATA_KEY: {"server": "broker", "tool": "place_order", "approval": True,
                                     "order": {"action": "place", "mode": "paper"}}}
    quote = StructuredTool.from_function(func=get_quote, name="mcp__broker__get_quote")
    quote.metadata = {METADATA_KEY: {"server": "broker", "tool": "get_quote", "approval": False}}
    toolset = MagicMock(tools=[order, quote])
    graph = _built(direct_mcp=toolset, order_ledger=MagicMock())
    names = {t.name for t in graph.turn.spec.tools}
    assert "mcp__broker__get_quote" in names and "mcp__broker__place_order" not in names
