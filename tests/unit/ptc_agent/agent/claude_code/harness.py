"""What the runtime's end-to-end tests share: one stub tool, the graph and a turn around it."""

from __future__ import annotations

from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.runtime import TURN_KEY, ClaudeTurn, RuntimeSpec, build_claude_code_graph


@tool("Bash")
async def bash(command: str) -> str:
    """Run a shell command."""
    return f"ran: {command}"


def spec(model: str) -> RuntimeSpec:
    billing = {"billing_type": "oauth", "manifest_model": f"{model}-code", "pricing_model_id": model, "pricing_provider": "anthropic"}
    return RuntimeSpec(system_prompt="You are a test agent. Keep replies short.", model=model, tools=[bash], billing=billing)


def new_graph(entry=()):
    return build_claude_code_graph(ToolNode([bash]), entry=entry, checkpointer=InMemorySaver())


async def run_turn(spec: RuntimeSpec, launcher: LocalLauncher, graph, thread: str, messages: list,
                   callbacks: list | None = None, baseline=None, token=None) -> dict:
    turn = ClaudeTurn(spec, launcher, identity="test-user", thread_key=thread, baseline=baseline, token=token)
    config = {"configurable": {"thread_id": thread, TURN_KEY: turn}, "callbacks": callbacks or []}
    try:
        await graph.ainvoke({"messages": messages}, config)
    finally:
        await turn.close()
    return (await graph.aget_state({"configurable": {"thread_id": thread}})).values
