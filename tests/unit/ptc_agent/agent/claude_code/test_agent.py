"""A Claude Code turn takes the system prompt and the tool node of the agent a turn builds."""

import asyncio
import secrets
import uuid
from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import START, MessagesState, StateGraph

from ptc_agent.agent.claude_code.agent import ClaudeCodeGraph, claude_code_graph, waits_for_user
from ptc_agent.agent.claude_code.compaction import NO_RULES
from ptc_agent.agent.claude_code.launcher import _THREAD_KEY, LocalLauncher
from ptc_agent.agent.claude_code.runtime import TURN_KEY
from ptc_agent.agent.claude_code.tools_bridge import tool_specs
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
        **{"user_id": "user-1", "launcher": LocalLauncher, "compaction": NO_RULES, **kw},
    )


#: A thread's turn: the run's config, as the producer streams the graph with.
_RUN = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}


def test_the_turn_runs_the_agents_prompt_and_tools():
    graph = _graph()
    turn = graph.turn(_RUN)
    assert TURN_KEY not in graph.config.get("configurable", {})
    assert graph.config["recursion_limit"] == 9999
    assert (turn.identity, turn.thread_key) == ("user-1", "thread-1")
    assert isinstance(turn.launcher, LocalLauncher)
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
    with pytest.raises(RuntimeError, match="for a user, on a thread"):
        _graph().turn({"configurable": {}})


@pytest.mark.asyncio
async def test_each_run_gets_a_turn_of_its_own_for_its_thread_and_namespace():
    """One compiled graph serves every run of its agent, as a subagent's
    serves each of its tasks: each run, streamed or invoked, gets its own
    CLI, closed with the run."""
    seen, closed = [], []

    class _Turn:
        def __init__(self, **run):
            self.run = run

        async def close(self):
            closed.append(self.run)

    class _Compiled:
        async def astream(self, input, config, **kwargs):
            seen.append(config["configurable"][TURN_KEY])
            yield 1

        async def ainvoke(self, input, config, **kwargs):
            seen.append(config["configurable"][TURN_KEY])
            return 2

    graph = ClaudeCodeGraph(_Compiled(), _Turn)
    for namespace in ("", "task:k3Xq_-", "task:k3Xq_-"):
        async for _ in graph.astream({}, {"configurable": {"thread_id": "t-1", "checkpoint_ns": namespace}}):
            pass
    assert await graph.ainvoke({}, {"configurable": {"thread_id": "t-1", "checkpoint_ns": "task:a1b2c3"}}) == 2
    runs = [{"thread_key": "t-1", "checkpoint_ns": ns} for ns in ("", "task:k3Xq_-", "task:k3Xq_-", "task:a1b2c3")]
    assert [t.run for t in seen] == runs and closed == runs
    assert len({id(t) for t in seen}) == 4


def test_a_runs_directory_is_its_threads_or_its_tasks_by_a_plain_id_every_launcher_accepts():
    """A resumed task finds its directory again, and no two CLIs on a thread share one."""
    thread, task = str(uuid.uuid4()), secrets.token_urlsafe(4)[:6]
    graph = _graph()

    def key(namespace):
        return graph.turn({"configurable": {"thread_id": thread, "checkpoint_ns": namespace}}).run_key

    assert key("") == thread
    assert key(f"task:{task}") == key(f"task:{task}|1") == f"{thread}-{task}"
    assert _THREAD_KEY.fullmatch(key(f"task:{task}"))


def test_each_turn_gets_a_launcher_of_its_own():
    """A launcher serves one run at a time and holds that run's credential."""
    graph = _graph()
    assert graph.turn(_RUN).launcher is not graph.turn(_RUN).launcher


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

    graph = ClaudeCodeGraph(_Compiled(), lambda **_: _Turn())
    assert graph.checkpointer == "saver"
    with pytest.raises(RuntimeError):
        async for _ in graph.astream({}, _RUN):
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

    stream = ClaudeCodeGraph(_Compiled(), lambda **_: _Turn()).astream({}, _RUN)
    assert await anext(stream) == 1
    await stream.aclose()
    assert order == ["graph closed", "cli closed"]


def _built(*, saved: str | None = None, **kwargs) -> ClaudeCodeGraph:
    """The real build of a Claude Code turn, let to sign in with ``saved``."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    config = AgentConfig(
        llm=LLMConfig(name="claude-haiku-5-5-code"), security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    config.claude_code_saved = saved
    target = create_llm("claude-haiku-5-5-code")
    kwargs = {"sandbox": MagicMock(), "claude_code_launcher": lambda sandbox, hosted, saved: LocalLauncher(), **kwargs}
    with patch.object(AgentConfig, "get_llm_client", return_value=target):
        graph = PTCAgent(config).create_agent(
            llm=target, thread_id="t-1", user_id="u-1", mcp_registry=MagicMock(), **kwargs
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
                    "BaselineContextMiddleware.before_agent", "resume"]
    assert graph.turn(_RUN)._baseline.__self__.__class__.__name__ == "BaselineContextMiddleware"
    assert "AskUserQuestion" in {t.name for t in graph.turn(_RUN).spec.tools}
    assert {"Task", "TaskOutput"} <= {t.name for t in graph.turn(_RUN).spec.tools}


def test_a_claude_code_turn_can_answer_comments_on_a_file():
    """A comment send tells the agent to answer with this tool."""
    assert "reply_to_comment" in {t.name for t in _built().turn(_RUN).spec.tools}


def test_a_claude_code_turn_waits_for_the_user_on_a_call_that_places_an_order():
    """An order is put to the user before it runs, so its call waits for the user's answer."""
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
    names = {t.name for t in graph.turn(_RUN).spec.tools}
    assert {"mcp__broker__get_quote", "mcp__broker__place_order"} <= names
    assert graph.turn(_RUN).spec.waits("mcp__broker__place_order", {"symbol": "AAPL"})
    assert not graph.turn(_RUN).spec.waits("mcp__broker__get_quote", {"symbol": "AAPL"})
    edges = {a: b for a, b in graph.graph.builder.edges}
    path, node = ["CreditGateMiddleware.before_model"], "CreditGateMiddleware.before_model"
    while node in edges:
        node = edges[node]
        path.append(node)
    assert path[0] == "CreditGateMiddleware.before_model" and path[-1] == "held"
    assert "OrderGovernanceMiddleware.after_model" in path


@pytest.mark.asyncio
async def test_a_claude_code_turn_refuses_the_order_cards_a_later_run_cut_off():
    """A message sent over an order card cuts its call off, and nothing will
    answer that card now, so its attempt is refused; other calls are not orders."""
    from unittest.mock import AsyncMock

    from langchain_core.tools import StructuredTool

    from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY

    order = StructuredTool.from_function(func=lambda symbol: "placed", name="mcp__broker__place_order", description="t")
    order.metadata = {METADATA_KEY: {"server": "broker", "tool": "place_order", "approval": True,
                                     "order": {"action": "place", "mode": "paper"}}}
    ledger = MagicMock(abandon=AsyncMock())
    built = []
    with patch("ptc_agent.agent.claude_code.agent.build_claude_code_graph", side_effect=lambda node, **kw: built.append(kw) or MagicMock()):
        _built(direct_mcp=MagicMock(tools=[order]), order_ledger=ledger)
    calls = [{"id": "c1", "name": "mcp__broker__place_order", "args": {"symbol": "AAPL"}},
             {"id": "c2", "name": "Bash", "args": {"command": "ls"}}]
    await built[-1]["abandon"](AIMessage("", id="m1", tool_calls=calls), calls)
    ledger.abandon.assert_awaited_once_with("m1", "c1")


def test_a_chief_of_staff_call_waits_for_the_user_only_with_an_action_that_puts_a_card_to_them():
    waits = waits_for_user(frozenset())
    assert waits("AskUserQuestion", {}) and waits("delegate_to_analyst", {})
    assert waits("manage_workspaces", {"action": "create"}) and not waits("manage_workspaces", {"action": "list"})
    assert waits("manage_threads", {"action": "delete"}) and not waits("manage_threads", {"action": "list"})
    assert not waits("manage_workspaces", {"action": ["create"]}) and not waits("manage_threads", {"action": {"x": 1}})
    assert waits("delegate_to_analyst", {"action": ["create"]})
    assert not waits("Bash", {})


def test_a_claude_code_turn_takes_reports_steers_and_task_resumes_in():
    """A command's end, a wakeup or an alert reaches it while it runs, and a
    task left a message after its run ended resumes as a turn opens; a turn
    that runs no subagents has no task to resume."""
    assert _built().turn(_RUN).steering._steering.kinds == ("report", "steer", "resume")
    assert _built(disable_subagents=True).turn(_RUN).steering._steering.kinds == ("report", "steer")


def test_a_claude_code_turn_compacts_and_moves_its_session_by_the_main_agents_threshold_and_floor():
    """And its summaries point at the transcript through the agent's own file tools."""
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.config.agent import CompactionConfig

    rules = _built().turn(_RUN).compaction
    settings = CompactionConfig()
    assert (rules.floor_tokens, rules.threshold) == (settings.rebuild_floor_tokens, settings.token_threshold)
    assert isinstance(rules.backend, SandboxBackend)


#: The CLI cuts an MCP tool's description past this many characters and adds
#: "… [truncated]" (its default ``CLAUDE_CODE_MAX_MCP_DESCRIPTION_LENGTH``).
CLI_DESCRIPTION_CAP = 4096


@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
def test_every_tool_description_reaches_claude_whole(role):
    """A description past the CLI's cap would read differently than on any other turn."""
    specs = tool_specs(_built(role=role).turn(_RUN).spec.tools)
    assert len(specs) > 20
    assert {s["name"]: len(s["description"]) for s in specs if len(s["description"]) > CLI_DESCRIPTION_CAP} == {}


def test_the_turn_runs_where_the_callers_launcher_puts_it_for_the_turns_sandbox():
    sandbox, launcher = MagicMock(), LocalLauncher()
    seen = []
    graph = _built(sandbox=sandbox, claude_code_launcher=lambda s, hosted, saved: seen.append(s) or launcher)
    turn = graph.turn(_RUN)
    assert seen == [sandbox] and turn.launcher is launcher


def test_a_claude_code_turn_built_with_no_launcher_is_refused():
    with pytest.raises(RuntimeError, match="claude_code_launcher"):
        _built(claude_code_launcher=None)


@pytest.mark.asyncio
async def test_a_run_never_takes_the_claude_session_or_approvals_it_is_handed_with_its_parents_state():
    """A subagent's run is handed its parent's whole state; it takes only
    what its agent takes as input, as ``create_agent``'s graph does, so the
    parent's private channels and Claude session stay the parent's."""
    seen = []

    class _Turn:
        async def close(self):
            pass

    class _Compiled:
        async def astream(self, input, config, **kwargs):
            seen.append(input)
            yield 1

        async def ainvoke(self, input, config, **kwargs):
            seen.append(input)

    parent = {"messages": [], "todos": [], "claude_session": {"session_id": "parent"}, "runtime_baseline": {"epoch": 3},
              "_claude_code_rules": {}, "preapproved_calls": ["c1"]}
    graph = ClaudeCodeGraph(_Compiled(), lambda **_: _Turn(), frozenset({"messages", "todos"}))
    [_ async for _ in graph.astream(parent, _RUN)]
    await graph.ainvoke(parent, _RUN)
    assert seen == [{"messages": [], "todos": []}] * 2
    assert _graph()._takes == frozenset(create_agent(FakeListChatModel(responses=["x"]), tools=[bash]).builder.input_schema.__annotations__)


def test_a_subagent_of_a_claude_code_turn_runs_each_run_in_a_cli_of_its_own():
    """Its runs take its own prompt, tools and stack, open with the hooks that
    write its rows, take in what is sent into its task, stop only at the
    credit gate, report the calls they make to its card, and wait a
    subagent's time for a slot on an own computer."""
    from ptc_agent.agent.claude_code import agent as cc_agent
    from ptc_agent.agent.claude_code.mid_turn import TaskSteering
    from ptc_agent.agent.middleware.background_subagent.event_capture import SubagentEventCaptureMiddleware

    graphs, builds, launched = [], [], []
    real_graph, real_build = cc_agent.claude_code_graph, cc_agent.build_claude_code_graph

    def graph(*args, **kw):
        graphs.append((kw, real_graph(*args, **kw)))
        return graphs[-1][1]

    def build(node, **kw):
        builds.append(kw)
        return real_build(node, **kw)

    with patch.object(cc_agent, "claude_code_graph", side_effect=graph), \
         patch.object(cc_agent, "build_claude_code_graph", side_effect=build):
        main = _built(claude_code_launcher=lambda sandbox, hosted, saved, **kw: launched.append(kw) or LocalLauncher())
    [(kw, sub, built)] = [(kw, g, b) for (kw, g), b in zip(graphs, builds, strict=True) if g is not main]
    turn = sub.turn({"configurable": {"thread_id": "t-1", "checkpoint_ns": "task:k1"}})
    assert turn.run_key == "t-1-k1" and launched == [{"slot_wait_s": 30}]
    assert isinstance(turn.steering, TaskSteering)
    assert "Task" not in {t.name for t in turn.spec.tools} and turn.spec.system_prompt != main.turn(_RUN).spec.system_prompt
    assert [n for n, _ in built["entry"]][:2] == ["SkillsMiddleware.before_agent", "TurnContextMiddleware.before_agent"]
    assert [n for n, _ in built["pause"]] == ["CreditGateMiddleware.before_model"]
    assert isinstance(built["on_reply"].__self__, SubagentEventCaptureMiddleware)
    assert builds[-1]["on_reply"] is None


@pytest.mark.parametrize("saved", ["stored_token", None])
def test_a_subagent_is_admitted_on_the_credential_its_turn_was_let_to_sign_in_with(saved):
    """Its CLI signs in as the build that launches it does, so its run is
    stopped with the turns when the token it used goes."""
    from ptc_agent.agent.middleware.background_subagent.middleware import BackgroundSubagentMiddleware

    made, real = [], BackgroundSubagentMiddleware.__init__

    def init(self, *args, **kwargs):
        real(self, *args, **kwargs)
        made.append(self)

    with patch.object(BackgroundSubagentMiddleware, "__init__", init):
        _built(saved=saved)
    [middleware] = made
    assert middleware.claude_code_types
    assert middleware.claude_code_stored is (saved is not None)


def test_a_claude_code_pin_on_a_turn_of_another_model_runs_each_run_in_a_cli_of_its_own(monkeypatch):
    """As a Claude Code turn's subagent does, on what admission let the pins
    sign in with (``claude_code_pins``), counted by its computer, its main
    agent untouched; and a build that could not launch it is refused."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.agent.claude_code import agent as cc_agent
    from ptc_agent.agent.middleware.background_subagent.middleware import BackgroundSubagentMiddleware
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    config = AgentConfig(
        llm=LLMConfig(name="claude-haiku-5-5"), security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    config.subsidiary_llm_clients["subagent:general-purpose"] = create_llm("claude-haiku-5-5-code")
    config.claude_code_pins = ("stored_token", "stored_token")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    main_model = FakeListChatModel(responses=["x"])
    graphs, launched, made = [], [], []
    real_graph, real_init = cc_agent.claude_code_graph, BackgroundSubagentMiddleware.__init__

    def graph(*args, **kw):
        graphs.append(real_graph(*args, **kw))
        return graphs[-1]

    def init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        made.append(self)

    def build(**kw):
        with patch.object(AgentConfig, "get_llm_client", return_value=main_model):
            return PTCAgent(config).create_agent(
                llm=main_model, thread_id="t-1", user_id="u-1", mcp_registry=MagicMock(), sandbox=MagicMock(), **kw,
            )

    with patch.object(cc_agent, "claude_code_graph", side_effect=graph), \
         patch.object(BackgroundSubagentMiddleware, "__init__", init):
        main = build(claude_code_launcher=lambda sandbox, hosted, saved, **kw: launched.append((hosted, saved, kw)) or LocalLauncher())
    assert not isinstance(main, ClaudeCodeGraph)
    [sub] = graphs
    sub.turn({"configurable": {"thread_id": "t-1", "checkpoint_ns": "task:k1"}})
    assert launched == [("stored_token", "stored_token", {"slot_wait_s": 30})]
    [middleware] = made
    assert middleware.claude_code_types == {"general-purpose"} and middleware.claude_code_stored
    with pytest.raises(RuntimeError, match="claude_code_launcher"):
        build(claude_code_launcher=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("claude_replied", "on_claude_code", "refused"),
    [
        pytest.param(False, True, True, id="another-model-replied"),
        pytest.param(True, True, False, id="claude-replied"),
        pytest.param(False, False, False, id="not-on-claude-code"),
    ],
)
async def test_a_task_another_model_last_answered_is_not_resumed_on_claude_code(
    claude_replied, on_claude_code, refused,
):
    """Read off the task's replayed history, two runs deep in a real saver,
    since its messages are a delta channel. Claude's session would hold none
    of that reply, and no later turn changes that, so the steers waiting on
    the resume are settled."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from langgraph.checkpoint.memory import InMemorySaver

    from ptc_agent.agent.main_state import MainAgentState
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        OTHER_MODEL_TASK_REFUSAL,
        BackgroundSubagentMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask, BackgroundTaskRegistry
    from ptc_agent.agent.middleware.background_subagent.spawn import TaskRunRefused
    from src.llms.claude_code import MESSAGE_ID_KEY

    saver = InMemorySaver()
    replies = iter([{MESSAGE_ID_KEY: "msg_1"}, {MESSAGE_ID_KEY: "msg_2"} if claude_replied else {}])
    task_graph = (
        StateGraph(MainAgentState)
        .add_node("agent", lambda s: {"messages": [AIMessage("done", id=str(uuid.uuid4()), response_metadata=next(replies))]})
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    config = {"configurable": {"thread_id": "t-1", "checkpoint_ns": "task:k1", "__pregel_task_id": "parent-task"}}
    for prompt in ("go", "and again"):
        await task_graph.ainvoke({"messages": [HumanMessage(prompt, id=str(uuid.uuid4()))]}, config)

    registry = BackgroundTaskRegistry("t-1")
    registry.run_ledger = SimpleNamespace(start_task_run=AsyncMock(return_value="tr-3"))
    mw = BackgroundSubagentMiddleware(subagent_types=["research"], registry=registry, checkpointer=saver)
    mw.claude_code_types = frozenset({"research"} if on_claude_code else ())
    task = BackgroundTask(tool_call_id="c", task_id="k1", description="d", prompt="p", subagent_type="research")

    async def resume():
        return await mw.admit_task_run(
            task, cause="resume", description="d", launch_tool_call_id="c", parent_run_id="run-A", acquire_fence=False,
        )

    if not refused:
        assert await resume() == "tr-3"
        return
    with pytest.raises(TaskRunRefused) as caught:
        await resume()
    assert (caught.value.reason, caught.value.final, caught.value.busy) == (OTHER_MODEL_TASK_REFUSAL, True, False)
    registry.run_ledger.start_task_run.assert_not_awaited()
