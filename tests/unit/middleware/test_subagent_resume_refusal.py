"""A Task naming a subagent the build did not compile is refused before anything starts, and only a resume gets the refusal that says its type is gone."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent import subagent as sa
from ptc_agent.agent.middleware.background_subagent.middleware import (
    current_background_tool_call_id,
)


def _graphs() -> dict[str, MagicMock]:
    return {"research": MagicMock(), "macro-desk": MagicMock()}


def _runtime() -> MagicMock:
    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-1"
    return runtime


@pytest.mark.parametrize("resuming", [True, False])
def test_the_refusal_names_the_missing_type_and_every_available_one(resuming):
    graphs = _graphs()

    text = sa._unknown_type_refusal("gone-desk", graphs, resuming=resuming)

    assert "gone-desk" in text
    assert all(f"`{name}`" in text for name in graphs)


def test_a_resume_is_refused_differently_from_a_launch():
    """A resumed task outlives the roster it started on, so its refusal says
    the type is gone rather than that it never existed."""
    graphs = _graphs()

    assert sa._unknown_type_refusal("gone-desk", graphs, resuming=True) != (
        sa._unknown_type_refusal("gone-desk", graphs, resuming=False)
    )


async def _call(tool, path: str, **kwargs):
    return await tool.coroutine(**kwargs) if path == "async" else tool.func(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["sync", "async"])
@pytest.mark.parametrize(("action", "resuming"), [("resume", True), ("update", False), ("init", False)])
async def test_the_task_tool_refuses_a_type_off_the_roster(path, action, resuming):
    graphs = _graphs()
    tool = sa._create_task_tool(subagent_graphs=graphs, registry=None, checkpointer=None)

    reply = await _call(
        tool, path, description="d", prompt="p", subagent_type="gone-desk", action=action,
        task_id=None if action == "init" else "task-1", runtime=_runtime(),
    )

    assert isinstance(reply, ToolMessage)
    assert (reply.status, reply.tool_call_id) == ("error", "tc-1")
    assert reply.content == sa._unknown_type_refusal("gone-desk", graphs, resuming=resuming)
    assert all(not graph.mock_calls for graph in graphs.values())


@pytest.mark.asyncio
async def test_a_resume_infers_the_tasks_own_type_and_refuses_it_once_gone():
    """No type named: the background task's own is the one checked, so a
    subagent deleted since the task started is what the refusal names."""
    graphs = _graphs()
    registry = MagicMock()
    registry.get_by_tool_call_id.return_value = SimpleNamespace(
        completed=False, task_id="task-1", subagent_type="gone-desk",
    )
    tool = sa._create_task_tool(subagent_graphs=graphs, registry=registry, checkpointer=None)

    token = current_background_tool_call_id.set("tc-bg")
    try:
        reply = await tool.coroutine(
            description="d", prompt="p", action="resume", task_id="task-1", runtime=_runtime(),
        )
    finally:
        current_background_tool_call_id.reset(token)

    assert reply.status == "error"
    assert reply.content == sa._unknown_type_refusal("gone-desk", graphs, resuming=True)
