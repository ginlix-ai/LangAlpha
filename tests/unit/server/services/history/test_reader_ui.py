"""The reader's ui channel: a task's terminal snapshot, and records a node
pushes mid-turn."""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from langgraph.graph.ui import ui_message_reducer

from ptc_agent.agent.state import DeltaAgentState
from src.server.services.history.reader import CheckpointHistoryReader

from tests.unit.server.services.history.reader_graphs import (
    THREAD,
    _echo_graph,
    _history,
    _run_turns,
)

pytestmark = pytest.mark.asyncio


async def test_task_ui_snapshot_readable_through_reader():
    # Cross-layer contract: the workflow driver persists its terminal ui
    # snapshot middleware-side (through the run's own checkpointer, so the
    # writer-guard fence applies), and the server reader materializes it. The
    # snapshot may land while the launching turn is still executing — a
    # root-ns append there forks a dead branch — so the writer must (a)
    # surface via aget_task_history, (b) upsert by record id, and (c) leave
    # the root chain untouched.
    from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
        persist_task_ui_record,
    )

    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)
    reader = CheckpointHistoryReader(saver)
    root_cfg = {"configurable": {"thread_id": THREAD}}
    tip_before = await saver.aget_tuple(root_cfg)

    await persist_task_ui_record(
        saver,
        THREAD,
        "wf1",
        "workflow_run",
        {"task_id": "wf1", "frames": [{"phase": "run_started"}]},
        record_id="workflow-run-r1",
    )
    await persist_task_ui_record(
        saver,
        THREAD,
        "wf1",
        "workflow_run",
        {
            "task_id": "wf1",
            "frames": [{"phase": "run_started"}, {"phase": "run_completed"}],
        },
        record_id="workflow-run-r1",
    )

    task_history = await reader.aget_task_history(THREAD, "wf1")
    records = [
        r for r in task_history.new_ui_records if r["id"] == "workflow-run-r1"
    ]
    assert len(records) == 1  # same-id rewrite upserts, never duplicates
    assert [f["phase"] for f in records[0]["props"]["frames"]] == [
        "run_started",
        "run_completed",
    ]

    # Root chain untouched: same tip, no root ui records, no new boundary.
    tip_after = await saver.aget_tuple(root_cfg)
    assert (
        tip_after.config["configurable"]["checkpoint_id"]
        == tip_before.config["configurable"]["checkpoint_id"]
    )
    history = await _history(reader)
    assert history.ui == []
    assert len(history.turns) == 1


async def test_new_ui_records_attributed_to_their_turn():
    # push_ui_message inside a node (the resilience middleware's fallback
    # path) checkpoints the record on the ui channel; the reader id-diffs the
    # channel per turn so replay can project the notice into the right turn.
    from langgraph.graph.ui import push_ui_message

    saver = InMemorySaver()

    def agent(state):
        humans = [m for m in state["messages"] if isinstance(m, HumanMessage)]
        if len(humans) == 2:  # second turn only
            push_ui_message(
                name="model_fallback",
                props={"from_model": "primary", "to_model": "backup"},
                id="ui-fb-1",
            )
        return {
            "messages": [AIMessage(content="ok", id=f"ai-{len(state['messages'])}")]
        }

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await _run_turns(graph, 2)

    reader = CheckpointHistoryReader(saver)
    history = await _history(reader)
    assert [len(t.new_ui_records) for t in history.turns] == [0, 1]
    record = history.turns[1].new_ui_records[0]
    assert record["name"] == "model_fallback"
    assert record["props"] == {"from_model": "primary", "to_model": "backup"}


async def test_ui_reducer_upsert_and_unknown_remove_guard():
    a1 = {"type": "ui", "id": "u1", "name": "n", "props": {"v": 1}, "metadata": {}}
    a2 = {"type": "ui", "id": "u1", "name": "n", "props": {"v": 2}, "metadata": {}}
    merged = ui_message_reducer([a1], [a2])
    assert merged == [a2]  # same id upserts, no duplicate

    with pytest.raises(ValueError):
        ui_message_reducer([a1], [{"type": "remove-ui", "id": "missing"}])
