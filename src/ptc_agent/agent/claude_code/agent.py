"""A Claude Code turn, built from the agent a langalpha turn would run.

The agent is built as for any turn, and Claude Code takes from it the system
prompt, the tool node with every tool-call middleware around it, and the
hooks a turn opens with, which write the turn's rows and freeze the baseline.
Claude Code runs the model loop, so nothing that wraps a model call runs on
these turns, and the turn's rows and baseline reach Claude in the turn's own
message instead (``runtime.turn_prompt``). Where the CLI runs, and on which
token, is the launcher's, which the caller picks (the server's
``turn_launcher``): on the user's own computer it runs there
(``SSHLauncher``), anywhere else on the backend's own machine
(``LocalLauncher``).

A call that may wait for the user (``waits_for_user``) is held until the
model-side hooks a langalpha turn runs before its calls (``_HOLDS``: the
credit gate, an order or a hand-off put to the user) have run, and the rows a
langalpha turn writes before a model call (``_OPENING_ROWS``) open the turn.

The pieces are read off the compiled agent's builder by the names
``create_agent`` gives them (``tools``, ``<middleware>.before_agent``), since it
is the one place the stack is composed; ``test_agent.py`` pins that shape.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from langchain_core.messages import ToolMessage

from ptc_agent.agent.claude_code.cli import RuntimeSpec
from ptc_agent.agent.claude_code.compaction import CompactionRules
from ptc_agent.agent.claude_code.launcher import Launcher
from ptc_agent.agent.claude_code.runtime import TURN_KEY, Baseline, ClaudeTurn, build_claude_code_graph
from ptc_agent.agent.middleware import ask_user
from ptc_agent.agent.middleware.credit_gate import CreditGateMiddleware
from ptc_agent.agent.middleware.order_governance import OrderGovernanceMiddleware, places_order
from ptc_agent.agent.middleware.steering import SteeringMiddleware
from ptc_agent.agent.middleware.wakeups import WakeupCancelNoticeMiddleware
from src.llms.claude_code import ClaudeCodeTarget
from src.tools.secretary._commands import puts_card
from src.tools.secretary.approvals import StandingApprovalMiddleware
from src.tools.wakeups import quiet

#: The middleware whose model hooks a held call passes before it runs, as a
#: langalpha turn's calls pass them after its model step. A call they stop
#: for has to be one ``waits_for_user`` holds, or the credit gate's.
_HOLDS = (CreditGateMiddleware, OrderGovernanceMiddleware, StandingApprovalMiddleware)
#: The middleware whose model-call hooks write rows the turn opens with.
_OPENING_ROWS = (WakeupCancelNoticeMiddleware,)


def waits_for_user(orders: frozenset[str]):
    """Which calls may wait for the user: a question, an order (``orders``
    names the tools that place one) or a Chief of Staff card."""

    def waits(name: str, args: Mapping[str, Any]) -> bool:
        return name == ask_user.TOOL_NAME or name in orders or puts_card(name, args)

    return waits


def _ends_turn(message: ToolMessage) -> bool:
    return quiet.quiet_result(message) is not None


def _quiet_call(name: str, args: Mapping[str, Any]) -> bool:
    return name == quiet.TOOL_NAME and args.get("quiet") is not None


class ClaudeCodeGraph:
    """The turn graph and the run's ``ClaudeTurn``, used as any turn's graph is.

    Its stream closes the CLI however the run ends, in the run's own task, and
    only after the graph's own stream, so no step is still using the CLI when
    it goes; everything else is the compiled graph's.
    """

    def __init__(self, graph: Any, turn: ClaudeTurn) -> None:
        self.graph = graph
        self.turn = turn

    async def astream(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        stream = self.graph.astream(*args, **kwargs)
        try:
            async for event in stream:
                yield event
        finally:
            try:
                await stream.aclose()
            finally:
                await self.turn.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.graph, name)


def claude_code_graph(
    agent: Any, *, system_prompt: str, target: ClaudeCodeTarget, user_id: str | None,
    thread_id: str | None, checkpointer: Any, launcher: Launcher, entry: Sequence[Any] = (),
    compaction: CompactionRules, middleware: Sequence[Any] = (), baseline: Baseline | None = None,
    steering: SteeringMiddleware | None = None, alert_senders: frozenset[str] = frozenset(),
) -> ClaudeCodeGraph:
    """The turn graph for ``agent``, the compiled agent ``create_agent`` built.

    ``entry`` are the middleware whose turn-entry hooks run first, in order,
    then the opening rows' and the holds' hooks of ``middleware``, the agent's
    stack, in the order ``create_agent`` runs them. ``baseline`` renders the thread's frozen baseline, and ``steering`` is the
    agent's own, which the turn takes in what is sent into the thread with
    between tool calls and as each query ends, coming back for a notice of
    ``alert_senders`` as a langalpha turn does. The user is who
    Claude Code is told the device belongs to and the thread names its
    working directory, so a turn without either is refused. ``launcher`` is
    where the turn's CLI runs, and ``compaction`` when Claude's session
    compacts and moves to this build's prompt and tools.
    """
    if not (user_id and thread_id):
        raise RuntimeError("a Claude Code turn runs for a user, on a thread")
    nodes = agent.builder.nodes
    tool_node = nodes["tools"].runnable
    orders = frozenset(t.name for t in tool_node.tools_by_name.values() if places_order(t))
    spec = RuntimeSpec(
        system_prompt=system_prompt,
        model=target.model,
        tools=list(tool_node.tools_by_name.values()),
        billing={k: v for k, v in (target.metadata or {}).items() if isinstance(v, str)},
        effort=target.effort,
        waits=waits_for_user(orders),
        ends_turn=_ends_turn,
        ends_turn_call=_quiet_call,
    )
    turn = ClaudeTurn(spec, launcher, identity=user_id, thread_key=thread_id, baseline=baseline, compaction=compaction,
                      steering=steering, alert_senders=alert_senders)

    def hooks(of: Sequence[Any], hook: str) -> list[tuple[str, Any]]:
        return [(name, nodes[name].runnable) for m in of if (name := f"{m.name}.{hook}") in nodes]

    holds = [m for m in middleware if isinstance(m, _HOLDS)]
    graph = build_claude_code_graph(
        tool_node,
        entry=hooks(entry, "before_agent") + hooks([m for m in middleware if isinstance(m, _OPENING_ROWS)], "before_model"),
        pause=hooks(holds, "before_model") + hooks(holds[::-1], "after_model"),
        checkpointer=checkpointer,
    ).with_config(
        {"recursion_limit": agent.config["recursion_limit"], "configurable": {TURN_KEY: turn}}
    )
    return ClaudeCodeGraph(graph, turn)
