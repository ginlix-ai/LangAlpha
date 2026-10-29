"""A real tool call survives LangChain's input parsing with its arguments
intact: ``cancel`` as a JSON string and ``triggers`` as a list reach the
tool as the model sent them, so the tool's own refusals can answer them."""

from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode

from src.tools.wakeups import tool as tool_mod
from src.tools.wakeups.models import WakeupArtifact


@pytest.mark.asyncio
async def test_tool_node_hands_the_tool_the_arguments_unparsed(monkeypatch):
    seen = AsyncMock(
        return_value=(
            "Ended without a reply.",
            WakeupArtifact(quiet=False, changed=False),
            False,
        )
    )
    monkeypatch.setattr(tool_mod, "_execute", seen)
    triggers = [
        {"label": "NVDA breakdown", "price": {"symbol": "NVDA", "below": 198}},
        {
            "title": "not a field",
            "at": "2026-10-21T08:30",
        },  # strict schema would reject this
    ]
    call = {
        "name": "schedule_wakeup",
        "id": "call_1",
        "args": {"triggers": triggers, "cancel": '["w_3f2a", "w_91c0"]'},
    }
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([tool_mod.build_schedule_wakeup_tool()]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    config = {"configurable": {"thread_id": "t_test", "timezone": "UTC"}}
    out = await graph.compile().ainvoke(
        {"messages": [AIMessage(content="", tool_calls=[call])]}, config
    )

    args, state, configurable = seen.await_args.args
    assert args == {"triggers": triggers, "cancel": '["w_3f2a", "w_91c0"]'}
    assert state["messages"][0].tool_calls[0]["id"] == "call_1"
    assert configurable["thread_id"] == "t_test"

    message = out["messages"][-1]
    assert isinstance(message, ToolMessage)
    assert (message.tool_call_id, message.name, message.status) == (
        "call_1",
        "schedule_wakeup",
        "success",
    )
    assert message.artifact == {"quiet": False, "changed": False}
