"""The reader's ui channel: records appended after a turn, a task's terminal
snapshot, and records a node pushes mid-turn."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from langgraph.graph.ui import ui_message_reducer
from langgraph.types import Command, interrupt

from ptc_agent.agent.state import DeltaAgentState
from src.server.services.history.reader import CheckpointHistoryReader

from tests.unit.server.services.history.reader_graphs import (
    THREAD,
    _cfg,
    _echo_graph,
    _history,
    _run_turns,
)

pytestmark = pytest.mark.asyncio


async def test_append_ui_record_no_new_boundary():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 2)

    reader = CheckpointHistoryReader(saver)
    await reader.append_ui_record(THREAD, "image_capture", {"path_to_url": {"a.png": "https://x/a"}})
    await reader.append_ui_record(THREAD, "image_capture", {"path_to_url": {"b.png": "https://x/b"}})

    history = await _history(reader)
    assert len(history.turns) == 2  # updates are not input boundaries
    assert [r["name"] for r in history.ui] == ["image_capture", "image_capture"]
    assert history.ui[0]["props"] == {"path_to_url": {"a.png": "https://x/a"}}
    assert history.ui[1]["props"] == {"path_to_url": {"b.png": "https://x/b"}}
    assert all(r["id"] for r in history.ui)

    # A ui append after the last turn must not shift the turn's message slice.
    assert [m.content for m in history.turns[1].messages] == ["q1", "echo: q1"]


async def test_append_ui_record_skipped_on_interrupted_tip():
    # An interrupted turn's tip carries a pending __interrupt__ write. Appending
    # a ui record there via aupdate_state attributes the write to the reader
    # graph's node and clears the pending interrupt, silently breaking HITL
    # resume (the image-capture Hook B fallback fires at interrupt time, so this
    # is reachable when a subagent emits a sandbox image on an interrupted turn).
    # The append must skip while the tip is interrupted.
    saver = InMemorySaver()

    def agent(state):
        humans = [m for m in state["messages"] if isinstance(m, HumanMessage)]
        if len(humans) == 1:
            answer = interrupt({"action_requests": [{"description": "go?"}]})
            return {"messages": [AIMessage(content=f"resumed: {answer}", id="ai-r")]}
        return {"messages": [AIMessage(content="ok", id=f"ai-{len(state['messages'])}")]}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await graph.ainvoke({"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg())

    reader = CheckpointHistoryReader(saver)
    assert (await graph.aget_state(_cfg())).next == ("agent",)

    await reader.append_ui_record(
        THREAD, "image_capture", {"path_to_url": {"a.png": "https://x/a"}}
    )

    # Interrupt survives the append: the pending task is intact and resume runs.
    assert (await graph.aget_state(_cfg())).next == ("agent",)
    result = await graph.ainvoke(Command(resume="yes"), _cfg())
    assert any(
        isinstance(m, AIMessage) and m.content == "resumed: yes"
        for m in result["messages"]
    )
    # The skipped record did not land.
    history = await _history(reader)
    assert history.ui == []


async def test_append_ui_record_skipped_when_interrupt_check_fails(monkeypatch):
    reader = CheckpointHistoryReader(InMemorySaver())
    interrupt_check = AsyncMock(side_effect=RuntimeError("checkpoint unavailable"))
    update = AsyncMock()
    monkeypatch.setattr(reader._checkpointer, "aget_tuple", interrupt_check)
    monkeypatch.setattr(reader._updater, "aupdate_state", update)

    await reader.append_ui_record(
        THREAD, "image_capture", {"path_to_url": {"a.png": "https://x/a"}}
    )

    interrupt_check.assert_awaited_once()
    update.assert_not_awaited()


async def test_append_ui_record_advances_recorded_branch_tip(monkeypatch):
    # The image-capture hook appends after turn end, creating a checkpoint
    # beyond the recorded branch tip where the replay walk cannot see it. The
    # append must CAS the recorded tip from the checkpoint it built on onto
    # the new one. (The workflow terminal snapshot does NOT come through here
    # — it writes into task:{id} via persist_task_ui_record, below.)
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)
    reader = CheckpointHistoryReader(saver)

    advance = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "src.server.database.conversation.threads_write."
        "advance_thread_checkpoint_id",
        advance,
    )
    root_cfg = {"configurable": {"thread_id": THREAD}}
    tip_before = await saver.aget_tuple(root_cfg)

    await reader.append_ui_record(
        THREAD, "image_capture", {"path_to_url": {"chart.png": "https://x/chart.png"}}
    )

    tip_after = await saver.aget_tuple(root_cfg)
    assert advance.await_args.args[0] == THREAD
    kwargs = advance.await_args.kwargs
    assert (
        kwargs["from_checkpoint_id"]
        == tip_before.config["configurable"]["checkpoint_id"]
    )
    assert (
        kwargs["to_checkpoint_id"]
        == tip_after.config["configurable"]["checkpoint_id"]
    )
    assert kwargs["to_checkpoint_id"] != kwargs["from_checkpoint_id"]


async def test_append_ui_record_skipped_on_a_thread_with_no_checkpoint(monkeypatch):
    # With nothing to anchor to, the write would land on whatever checkpoint
    # a starting turn writes first, and a CAS from no pointer would publish it.
    saver = InMemorySaver()
    advance = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "src.server.database.conversation.threads_write."
        "advance_thread_checkpoint_id",
        advance,
    )

    await CheckpointHistoryReader(saver).append_ui_record(THREAD, "image_capture", {})

    assert await saver.aget_tuple({"configurable": {"thread_id": THREAD}}) is None
    advance.assert_not_awaited()


async def test_append_ui_record_anchors_to_the_tip_it_read(monkeypatch):
    # A turn starting on another worker between the tip read and the append
    # must not re-parent the record onto that turn's uncommitted checkpoint:
    # the CAS guard is the tip that was read, so an unanchored write would
    # still pass it and publish partial state as the thread's commit pointer.
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)
    reader = CheckpointHistoryReader(saver)
    root_cfg = {"configurable": {"thread_id": THREAD}}
    stale_tip = await saver.aget_tuple(root_cfg)
    stale_id = stale_tip.config["configurable"]["checkpoint_id"]

    # The concurrent turn lands. It has not finalized, so the recorded pointer
    # still names the turn-1 tip — which is exactly why the CAS would pass.
    await _run_turns(graph, 1, start=1)
    moved = await saver.aget_tuple(root_cfg)
    moved_id = moved.config["configurable"]["checkpoint_id"]
    assert moved_id != stale_id

    class _StaleTip:
        """Models the read/write window: the reader holds the older tip."""

        async def aget_tuple(self, config):
            return stale_tip

    monkeypatch.setattr(reader, "_checkpointer", _StaleTip())
    advance = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "src.server.database.conversation.threads_write."
        "advance_thread_checkpoint_id",
        advance,
    )

    await reader.append_ui_record(THREAD, "image_capture", {"path_to_url": {}})

    kwargs = advance.await_args.kwargs
    assert kwargs["from_checkpoint_id"] == stale_id
    written = await saver.aget_tuple(
        {
            "configurable": {
                "thread_id": THREAD,
                "checkpoint_id": kwargs["to_checkpoint_id"],
            }
        }
    )
    # The guard and the write agree on one parent — the whole point.
    assert written.parent_config["configurable"]["checkpoint_id"] == stale_id


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
