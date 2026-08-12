"""CheckpointHistoryReader slicing against an in-memory checkpointer.

Turns are created by a real DeltaAgentState graph (so ``messages`` goes through
the DeltaChannel write path), then read back through the reader's no-op graph —
the same recipe production uses against Postgres.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.graph.ui import ui_message_reducer
from langgraph.types import Command, interrupt

from ptc_agent.agent.middleware.compaction.types import CompactionState
from ptc_agent.agent.state import DeltaAgentState
from src.server.services.history.reader import (
    CheckpointHistoryReader,
    TaskHistory,
    _ReaderState,
)
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

pytestmark = pytest.mark.asyncio

THREAD = "thread-1"

# Background subagents are invoked from inside a parent tool context, whose
# config carries the pregel task id — that is what makes langgraph honor the
# explicit checkpoint_ns instead of resetting it to root. Tests must mirror it.
CONFIG_KEY_TASK_ID = "__pregel_task_id"


def _echo_graph(checkpointer):
    """One turn = reply 'echo: <last human>' with a stable per-turn id."""

    def agent(state):
        humans = [m for m in state["messages"] if isinstance(m, HumanMessage)]
        last = humans[-1].content if humans else "?"
        return {
            "messages": [AIMessage(content=f"echo: {last}", id=f"ai-{len(state['messages'])}")]
        }

    return (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=checkpointer)
    )


def _cfg(thread_id=THREAD, *, turn_index=None, run_id=None, checkpoint_id=None):
    cfg = {"configurable": {"thread_id": thread_id}}
    if checkpoint_id:
        cfg["configurable"]["checkpoint_id"] = checkpoint_id
    metadata = {}
    if run_id is not None:
        metadata["run_id"] = run_id
    if turn_index is not None:
        metadata["turn_index"] = turn_index
    if metadata:
        cfg["metadata"] = metadata
    return cfg


async def _run_turns(graph, n, start=0):
    for i in range(start, start + n):
        await graph.ainvoke(
            {"messages": [HumanMessage(content=f"q{i}", id=f"h-{i}")]},
            _cfg(turn_index=i, run_id=f"run-{i}"),
        )


async def test_turn_slicing_and_metadata():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)

    assert len(history.turns) == 3
    for i, turn in enumerate(history.turns):
        assert turn.turn_ordinal == i
        assert turn.user_message is not None
        assert turn.user_message.content == f"q{i}"
        # The slice starts at the turn's own input (the input checkpoint's
        # state predates it) and ends before the next turn's input.
        contents = [m.content for m in turn.messages]
        assert contents == [f"q{i}", f"echo: q{i}"]
        assert turn.run_id == f"run-{i}"
        assert turn.turn_index == i
    assert history.interrupts == []


async def test_edit_branch_follows_requested_tip():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    original_tip = (await graph.aget_state(_cfg())).config["configurable"]["checkpoint_id"]
    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD, original_tip)

    # Production edit forks from the checkpoint BEFORE the turn's input
    # (checkpoint_handler's edit_checkpoint_id = the input checkpoint's parent),
    # so the stale input boundary is off the new branch.
    turn2_input = history.turns[2].input_checkpoint_id
    input_tuple = await saver.aget_tuple(
        {"configurable": {"thread_id": THREAD, "checkpoint_id": turn2_input}}
    )
    edit_from = input_tuple.parent_config["configurable"]["checkpoint_id"]

    await graph.ainvoke(
        {"messages": [HumanMessage(content="q2-edited", id="h-2b")]},
        _cfg(turn_index=2, run_id="run-2b", checkpoint_id=edit_from),
    )
    new_tip = (await graph.aget_state(_cfg())).config["configurable"]["checkpoint_id"]
    assert new_tip != original_tip

    branched = await reader.aget_thread_history(THREAD, new_tip)
    assert [t.user_message.content for t in branched.turns] == ["q0", "q1", "q2-edited"]
    assert branched.turns[2].messages[-1].content == "echo: q2-edited"

    original = await reader.aget_thread_history(THREAD, original_tip)
    assert [t.user_message.content for t in original.turns] == ["q0", "q1", "q2"]


async def test_missing_requested_tip_fails_instead_of_reading_newest():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)

    reader = CheckpointHistoryReader(saver)
    with pytest.raises(CheckpointBranchTipNotFound, match="missing-tip"):
        await reader.aget_thread_history(THREAD, "missing-tip")


async def test_regenerate_branch_reuses_input_boundary():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 2)

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)

    # Production regenerate re-runs FROM the input checkpoint (input=None
    # replays its pending input writes) — same boundary, new branch below it.
    await graph.ainvoke(
        None, _cfg(checkpoint_id=history.turns[1].input_checkpoint_id)
    )
    regenerated = await reader.aget_thread_history(THREAD)
    assert len(regenerated.turns) == 2
    assert regenerated.turns[1].user_message.content == "q1"
    assert [m.content for m in regenerated.turns[1].messages] == ["q1", "echo: q1"]


async def test_compaction_id_diff_slicing():
    saver = InMemorySaver()

    def agent(state):
        msgs = state["messages"]
        if len(msgs) > 2:
            return {
                "messages": [
                    RemoveMessage(id=REMOVE_ALL_MESSAGES),
                    AIMessage(content="summary of the past", id="sum-1"),
                    AIMessage(content="fresh reply", id="ai-fresh"),
                ]
            }
        return {"messages": [AIMessage(content="first reply", id="ai-first")]}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await graph.ainvoke({"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg())
    await graph.ainvoke({"messages": [HumanMessage(content="q1", id="h-1")]}, _cfg())

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)

    assert len(history.turns) == 2
    assert [m.id for m in history.turns[0].messages] == ["h-0", "ai-first"]
    # Post-compaction turn: id-diff yields the summary + reply, never a
    # count-based mis-slice. REMOVE_ALL also swallowed the turn's own input, so
    # there is no HumanMessage left to attribute (replay sources user text from
    # DB query rows, not from here).
    assert [m.id for m in history.turns[1].messages] == ["sum-1", "ai-fresh"]
    assert history.turns[1].user_message is None


async def test_hitl_resume_is_its_own_turn_boundary():
    saver = InMemorySaver()

    def agent(state):
        answer = interrupt({"action_requests": [{"description": "proceed?"}]})
        return {"messages": [AIMessage(content=f"resumed: {answer}", id="ai-r")]}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await graph.ainvoke(
        {"messages": [HumanMessage(content="q0", id="h-0")]},
        _cfg(turn_index=0, run_id="run-0"),
    )

    reader = CheckpointHistoryReader(saver)
    pending = await reader.aget_thread_history(THREAD)
    # A pending interrupt (__interrupt__ writes, no __resume__) is not a boundary.
    assert len(pending.turns) == 1
    assert len(pending.interrupts) == 1
    assert pending.interrupts[0]["value"] == {
        "action_requests": [{"description": "proceed?"}]
    }
    # Pending, not answered — it must not double as an ending interrupt.
    assert pending.turns[0].ending_interrupts == []

    await graph.ainvoke(Command(resume="yes"), _cfg())
    resumed = await reader.aget_thread_history(THREAD)
    # The resume is a boundary of its own — it persists a resume_feedback
    # query row, so boundaries must stay 1:1 with persisted turns.
    assert len(resumed.turns) == 2
    assert [m.content for m in resumed.turns[0].messages] == ["q0"]
    assert [m.content for m in resumed.turns[1].messages] == ["resumed: yes"]
    assert resumed.turns[1].user_message is None
    # The resume checkpoint's metadata belongs to the interrupted run — the
    # resume turn must not inherit its run_id/turn_index.
    assert resumed.turns[1].run_id is None
    assert resumed.turns[1].turn_index is None
    assert resumed.interrupts == []
    # Once answered, the interrupt attributes to the turn that raised it —
    # read from the resume boundary's __interrupt__ writes.
    assert [i["value"] for i in resumed.turns[0].ending_interrupts] == [
        {"action_requests": [{"description": "proceed?"}]}
    ]
    assert resumed.turns[0].ending_interrupts[0]["id"] is not None
    assert resumed.turns[1].ending_interrupts == []


async def test_task_namespace_transcript():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)

    # Background subagents run their own graph against the parent thread_id
    # with an explicit checkpoint_ns — mirror that write path.
    summary_message = HumanMessage(content="task summary", id="task-summary")
    summarization_event = {
        "cutoff_index": 1,
        "summary_message": summary_message,
        "file_path": None,
    }
    task_ui = {
        "type": "ui",
        "id": "task-ui-1",
        "name": "model_fallback",
        "props": {"from_model": "primary", "to_model": "fallback"},
        "metadata": {},
    }

    sub_graph = (
        StateGraph(_ReaderState)
        .add_node(
            "agent",
            lambda s: {
                "messages": [AIMessage(content="subagent reply", id="sub-ai-1")],
                "_summarization_event": summarization_event,
                "_offloaded_tool_call_ids": {"tc-1", "tc-2"},
                "_offloaded_read_result_ids": {"read-1"},
                "ui": [task_ui],
            },
        )
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await sub_graph.ainvoke(
        {"messages": [HumanMessage(content="sub prompt", id="sub-h-1")]},
        {
            "configurable": {
                "thread_id": THREAD,
                "checkpoint_ns": "task:tsk1",
                CONFIG_KEY_TASK_ID: "parent-task",
            }
        },
    )

    reader = CheckpointHistoryReader(saver)
    task_history = await reader.aget_task_history(THREAD, "tsk1")
    assert isinstance(task_history, TaskHistory)
    assert [m.content for m in task_history.messages] == [
        "sub prompt",
        "subagent reply",
    ]
    assert task_history.new_summarization_event == summarization_event
    assert task_history.newly_offloaded_args == 2
    assert task_history.newly_offloaded_reads == 1
    assert task_history.new_ui_records == [task_ui]

    # The subagent namespace does not leak turn boundaries into the main thread.
    history = await reader.aget_thread_history(THREAD)
    assert len(history.turns) == 1


async def test_project_task_transcript_serves_unprojected_lanes(monkeypatch):
    # On-demand transcript for tasks replay never projects as lanes (workflow
    # children have no Task-tool launch artifact): the endpoint's projection
    # must emit the same SSE-shaped items as the lane projector, tagged with
    # the task's agent id.
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)
    sub_graph = (
        StateGraph(_ReaderState)
        .add_node(
            "agent",
            lambda s: {
                "messages": [AIMessage(content="child reply", id="sub-ai-1")]
            },
        )
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await sub_graph.ainvoke(
        {"messages": [HumanMessage(content="child prompt", id="sub-h-1")]},
        {
            "configurable": {
                "thread_id": THREAD,
                "checkpoint_ns": "task:wf1",
                CONFIG_KEY_TASK_ID: "parent-task",
            }
        },
    )
    reader = CheckpointHistoryReader(saver)
    monkeypatch.setattr(
        CheckpointHistoryReader, "get_instance", classmethod(lambda cls: reader)
    )
    from src.server.services.history.replay import task_lane
    from src.server.services.history.replay.task_lane import (
        project_task_transcript,
    )

    monkeypatch.setattr(
        task_lane,
        "resolve_task_details",
        AsyncMock(return_value={"wf1": {"status": "completed"}}),
    )

    items = await project_task_transcript(THREAD, "wf1")

    assert items
    assert all(i["data"]["agent"] == "task:wf1" for i in items)
    events = [i["event"] for i in items]
    assert "user_message" in events  # the spawn instruction (run boundary)
    assert "message_chunk" in events  # the child's reply
    assert "artifact" not in events

    # Unknown task: honest empty, not an error.
    monkeypatch.setattr(
        task_lane,
        "resolve_task_details",
        AsyncMock(return_value={"nosuch": {"status": "completed"}}),
    )
    assert await project_task_transcript(THREAD, "nosuch") == []


async def test_project_task_transcript_gates_on_a_settled_task(monkeypatch):
    # The endpoint promises "the same items replay emits", and replay leaves a
    # running task's segment to its live stream. The gate has to sit here, not
    # in the browser: a checkpoint read of a live task freezes a partial
    # transcript the live writes never reconcile.
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)
    sub_graph = (
        StateGraph(_ReaderState)
        .add_node(
            "agent",
            lambda s: {"messages": [AIMessage(content="partial", id="sub-ai-1")]},
        )
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await sub_graph.ainvoke(
        {"messages": [HumanMessage(content="child prompt", id="sub-h-1")]},
        {
            "configurable": {
                "thread_id": THREAD,
                "checkpoint_ns": "task:wf1",
                CONFIG_KEY_TASK_ID: "parent-task",
            }
        },
    )
    reader = CheckpointHistoryReader(saver)
    monkeypatch.setattr(
        CheckpointHistoryReader, "get_instance", classmethod(lambda cls: reader)
    )
    from src.server.services.history.replay import task_lane

    monkeypatch.setattr(
        task_lane,
        "resolve_task_details",
        AsyncMock(return_value={"wf1": {"status": "running"}}),
    )
    assert await task_lane.project_task_transcript(THREAD, "wf1") == []

    # An unresolvable status counts as running, same as the client's rule.
    monkeypatch.setattr(
        task_lane,
        "resolve_task_details",
        AsyncMock(side_effect=RuntimeError("ledger down")),
    )
    assert await task_lane.project_task_transcript(THREAD, "wf1") == []


async def test_append_ui_record_no_new_boundary():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 2)

    reader = CheckpointHistoryReader(saver)
    await reader.append_ui_record(THREAD, "image_capture", {"path_to_url": {"a.png": "https://x/a"}})
    await reader.append_ui_record(THREAD, "image_capture", {"path_to_url": {"b.png": "https://x/b"}})

    history = await reader.aget_thread_history(THREAD)
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
    history = await reader.aget_thread_history(THREAD)
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
    history = await reader.aget_thread_history(THREAD)
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
    history = await reader.aget_thread_history(THREAD)
    assert [len(t.new_ui_records) for t in history.turns] == [0, 1]
    record = history.turns[1].new_ui_records[0]
    assert record["name"] == "model_fallback"
    assert record["props"] == {"from_model": "primary", "to_model": "backup"}


async def test_tail_checkpoint_ids_and_anchors():
    # Each turn's tail = its own last checkpoint (the tip the persist path
    # records) — the projection-cache key. Anchors derive the same tails from
    # the light walk without materializing state, and must agree with the
    # full history read.
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)
    anchors, tip_id = await reader.aget_turn_anchors(THREAD)

    tails = [t.tail_checkpoint_id for t in history.turns]
    assert all(tails) and len(set(tails)) == 3
    assert [a.tail_checkpoint_id for a in anchors] == tails
    assert anchors[-1].tail_checkpoint_id == tip_id
    assert [a.turn_index for a in anchors] == [0, 1, 2]
    # A non-last turn's tail is the next input boundary's parent — strictly
    # between the two boundaries (checkpoint ids are time-ordered).
    for i in range(2):
        assert history.turns[i].input_checkpoint_id < tails[i]
        assert tails[i] < history.turns[i + 1].input_checkpoint_id

    # The tail is what the persist path records: run one more turn and the
    # previous tip becomes... unchanged history for turns 0-2.
    await _run_turns(graph, 1, start=3)
    anchors_after, _ = await reader.aget_turn_anchors(THREAD)
    assert [a.tail_checkpoint_id for a in anchors_after[:3]] == tails


async def test_resume_boundary_tail_is_the_interrupt_checkpoint():
    # Interrupt and resume writes ride the SAME checkpoint, so the interrupted
    # turn's tail (the tip at interrupt-persist time) IS the resume boundary's
    # checkpoint — not its parent.
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
    interrupt_tip = (await graph.aget_state(_cfg())).config["configurable"][
        "checkpoint_id"
    ]
    await graph.ainvoke(Command(resume="yes"), _cfg())

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)
    assert len(history.turns) == 2
    assert history.turns[1].input_checkpoint_id == interrupt_tip
    assert history.turns[0].tail_checkpoint_id == interrupt_tip
    anchors, tip_id = await reader.aget_turn_anchors(THREAD)
    assert anchors[0].tail_checkpoint_id == interrupt_tip
    assert anchors[1].tail_checkpoint_id == tip_id


async def test_empty_thread():
    reader = CheckpointHistoryReader(InMemorySaver())
    history = await reader.aget_thread_history("no-such-thread")
    assert history.turns == []
    assert await reader.acount_input_boundaries("no-such-thread") == 0


async def test_ui_reducer_upsert_and_unknown_remove_guard():
    a1 = {"type": "ui", "id": "u1", "name": "n", "props": {"v": 1}, "metadata": {}}
    a2 = {"type": "ui", "id": "u1", "name": "n", "props": {"v": 2}, "metadata": {}}
    merged = ui_message_reducer([a1], [a2])
    assert merged == [a2]  # same id upserts, no duplicate

    with pytest.raises(ValueError):
        ui_message_reducer([a1], [{"type": "remove-ui", "id": "missing"}])


async def test_summarization_event_attributed_to_its_turn():
    # Auto-compaction stores its summary in the _summarization_event state key
    # (never the messages channel). The reader must materialize that private
    # channel and attribute the event to exactly the turn it landed in.
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    saver = InMemorySaver()
    summary_message = build_summary_message(
        "prior context condensed", None, original_message_count=8
    )

    def agent(state):
        update = {
            "messages": [
                AIMessage(content="reply", id=f"ai-{len(state['messages'])}")
            ]
        }
        if len(state["messages"]) > 1:  # compaction fires during turn 2
            update["_summarization_event"] = {
                "cutoff_index": 1,
                "summary_message": summary_message,
                "file_path": None,
            }
            update["_offloaded_tool_call_ids"] = {"tc-1", "tc-2"}
        return update

    graph = (
        StateGraph(CompactionState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await _run_turns(graph, 3)

    reader = CheckpointHistoryReader(saver)
    history = await reader.aget_thread_history(THREAD)

    assert [t.new_summarization_event is not None for t in history.turns] == [
        False,
        True,
        False,  # turn 3 still carries the event in state, but it isn't new
    ]
    event = history.turns[1].new_summarization_event
    assert event["summary_message"].id == summary_message.id
    # Offload-set growth is attributed the same way: new ids count on the
    # turn that offloaded them, zero once the set stops growing.
    assert [t.newly_offloaded_args for t in history.turns] == [0, 2, 0]
    assert [t.newly_offloaded_reads for t in history.turns] == [0, 0, 0]
