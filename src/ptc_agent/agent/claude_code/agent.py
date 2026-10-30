"""A Claude Code turn, built from the agent a langalpha turn would run.

The agent is built as for any turn, and Claude Code takes from it the system
prompt, the tool node with every tool-call middleware around it, and the
hooks a turn opens with, which write the turn's rows and freeze the baseline.
Claude Code runs the model loop, so nothing that wraps a model call runs on
these turns, and the turn's rows and baseline reach Claude in the turn's own
message instead (``runtime.turn_prompt``). This phase runs the CLI on the
backend's own machine with the setup token the user saved for LangAlpha
(``LocalLauncher``), which only a self-hosted deployment offers
(``src.llms.claude_code.runtime_available``).

The pieces are read off the compiled agent's builder by the names
``create_agent`` gives them (``tools``, ``<middleware>.before_agent``), since it
is the one place the stack is composed; ``test_agent.py`` pins that shape.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.runtime import (
    TURN_KEY,
    Baseline,
    ClaudeTurn,
    RuntimeSpec,
    build_claude_code_graph,
)
from src.llms.claude_code import ClaudeCodeTarget


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
    thread_id: str | None, checkpointer: Any, entry: Sequence[Any] = (), baseline: Baseline | None = None,
) -> ClaudeCodeGraph:
    """The turn graph for ``agent``, the compiled agent ``create_agent`` built.

    ``entry`` are the middleware whose turn-entry hooks run first, in order,
    and ``baseline`` renders the thread's frozen baseline. The user is who
    Claude Code is told the device belongs to and the thread names its
    working directory, so a turn without either is refused.
    """
    if not (user_id and thread_id):
        raise RuntimeError("a Claude Code turn runs for a user, on a thread")
    nodes = agent.builder.nodes
    tool_node = nodes["tools"].runnable
    spec = RuntimeSpec(
        system_prompt=system_prompt,
        model=target.model,
        tools=list(tool_node.tools_by_name.values()),
        billing={k: v for k, v in (target.metadata or {}).items() if isinstance(v, str)},
        effort=target.effort,
    )
    from src.server.services.llm.claude_code import SavedToken

    turn = ClaudeTurn(
        spec, LocalLauncher(), identity=user_id, thread_key=thread_id, baseline=baseline, token=SavedToken(user_id),
    )
    hooks = [(name, nodes[name].runnable) for m in entry if (name := f"{m.name}.before_agent") in nodes]
    graph = build_claude_code_graph(tool_node, entry=hooks, checkpointer=checkpointer).with_config(
        {"recursion_limit": agent.config["recursion_limit"], "configurable": {TURN_KEY: turn}}
    )
    return ClaudeCodeGraph(graph, turn)
