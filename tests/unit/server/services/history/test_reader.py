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
from langgraph.types import Command, interrupt

from ptc_agent.agent.middleware.compaction.types import CompactionState
from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.transcript.classify import is_run_boundary_message
from src.server.services.history.reader import CheckpointHistoryReader, TaskHistory
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound
from tests.unit.server.services.history.reader_graphs import (
    THREAD,
    _cfg,
    _echo_graph,
    _history,
    _run_turns,
)

pytestmark = pytest.mark.asyncio

# Background subagents are invoked from inside a parent tool context, whose
# config carries the pregel task id — that is what makes langgraph honor the
# explicit checkpoint_ns instead of resetting it to root. Tests must mirror it.
CONFIG_KEY_TASK_ID = "__pregel_task_id"


def _user(turn):
    """The turn's own input: the first run boundary among what it added."""
    return next((m for m in turn.messages if is_run_boundary_message(m)), None)


async def test_turn_slicing_and_metadata():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    reader = CheckpointHistoryReader(saver)
    history = await _history(reader)

    assert len(history.turns) == 3
    for i, turn in enumerate(history.turns):
        assert turn.anchor.turn_ordinal == i
        assert _user(turn) is not None
        assert _user(turn).content == f"q{i}"
        # The slice starts at the turn's own input (the input checkpoint's
        # state predates it) and ends before the next turn's input.
        contents = [m.content for m in turn.messages]
        assert contents == [f"q{i}", f"echo: q{i}"]
        assert turn.anchor.run_id == f"run-{i}"
        assert turn.anchor.turn_index == i
    assert history.interrupts == []


async def test_edit_branch_follows_requested_tip():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    original_tip = (await graph.aget_state(_cfg())).config["configurable"]["checkpoint_id"]
    reader = CheckpointHistoryReader(saver)
    history = await _history(reader, tip=original_tip)

    # Production edit forks from the checkpoint BEFORE the turn's input
    # (checkpoint_handler's edit_checkpoint_id = the input checkpoint's parent),
    # so the stale input boundary is off the new branch.
    turn2_input = history.turns[2].anchor.input_checkpoint_id
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

    branched = await _history(reader, tip=new_tip)
    assert [_user(t).content for t in branched.turns] == ["q0", "q1", "q2-edited"]
    assert branched.turns[2].messages[-1].content == "echo: q2-edited"

    original = await _history(reader, tip=original_tip)
    assert [_user(t).content for t in original.turns] == ["q0", "q1", "q2"]


async def test_missing_requested_tip_fails_instead_of_reading_newest():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 1)

    reader = CheckpointHistoryReader(saver)
    with pytest.raises(CheckpointBranchTipNotFound, match="missing-tip"):
        await _history(reader, tip="missing-tip")


async def test_regenerate_branch_reuses_input_boundary():
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 2)

    reader = CheckpointHistoryReader(saver)
    history = await _history(reader)

    # Production regenerate re-runs FROM the input checkpoint (input=None
    # replays its pending input writes) — same boundary, new branch below it.
    await graph.ainvoke(
        None, _cfg(checkpoint_id=history.turns[1].anchor.input_checkpoint_id)
    )
    regenerated = await _history(reader)
    assert len(regenerated.turns) == 2
    assert _user(regenerated.turns[1]).content == "q1"
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
    history = await _history(reader)

    assert len(history.turns) == 2
    assert [m.id for m in history.turns[0].messages] == ["h-0", "ai-first"]
    # Post-compaction turn: id-diff yields the summary + reply, never a
    # count-based mis-slice. REMOVE_ALL also swallowed the turn's own input, so
    # there is no HumanMessage left to attribute (replay sources user text from
    # DB query rows, not from here).
    assert [m.id for m in history.turns[1].messages] == ["sum-1", "ai-fresh"]
    assert _user(history.turns[1]) is None


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
    pending = await _history(reader)
    # A pending interrupt (__interrupt__ writes, no __resume__) is not a boundary.
    assert len(pending.turns) == 1
    assert len(pending.interrupts) == 1
    assert pending.interrupts[0]["value"] == {
        "action_requests": [{"description": "proceed?"}]
    }
    # Pending, not answered — it must not double as an ending interrupt.
    assert pending.turns[0].anchor.ending_interrupts == []

    await graph.ainvoke(Command(resume="yes"), _cfg())
    resumed = await _history(reader)
    # The resume is a boundary of its own — it persists a resume_feedback
    # query row, so boundaries must stay 1:1 with persisted turns.
    assert len(resumed.turns) == 2
    assert [m.content for m in resumed.turns[0].messages] == ["q0"]
    assert [m.content for m in resumed.turns[1].messages] == ["resumed: yes"]
    assert _user(resumed.turns[1]) is None
    # The resume checkpoint's metadata belongs to the interrupted run — the
    # resume turn must not inherit its run_id/turn_index.
    assert resumed.turns[1].anchor.run_id is None
    assert resumed.turns[1].anchor.turn_index is None
    assert resumed.interrupts == []
    # Once answered, the interrupt attributes to the turn that raised it —
    # read from the resume boundary's __interrupt__ writes.
    assert [i["value"] for i in resumed.turns[0].anchor.ending_interrupts] == [
        {"action_requests": [{"description": "proceed?"}]}
    ]
    assert resumed.turns[0].anchor.ending_interrupts[0]["id"] is not None
    assert resumed.turns[1].anchor.ending_interrupts == []


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
        StateGraph(MainAgentState)
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
    history = await _history(reader)
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
        StateGraph(MainAgentState)
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
    from src.server.services.history.replay import run_lane as task_lane
    from src.server.services.history.replay.run_lane import (
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
        StateGraph(MainAgentState)
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
    from src.server.services.history.replay import run_lane as task_lane

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


async def test_tail_checkpoint_ids_and_anchors():
    # Each turn's tail = its own last checkpoint (the tip the persist path
    # records) — the projection-cache key. Anchors derive the same tails from
    # the light walk without materializing state, and must agree with the
    # full history read.
    saver = InMemorySaver()
    graph = _echo_graph(saver)
    await _run_turns(graph, 3)

    reader = CheckpointHistoryReader(saver)
    history = await _history(reader)
    anchors, tip_id = await reader.aget_turn_anchors(THREAD)

    tails = [t.anchor.tail_checkpoint_id for t in history.turns]
    assert all(tails) and len(set(tails)) == 3
    assert [a.tail_checkpoint_id for a in anchors] == tails
    assert anchors[-1].tail_checkpoint_id == tip_id
    assert [a.turn_index for a in anchors] == [0, 1, 2]
    # A non-last turn's tail is the next input boundary's parent — strictly
    # between the two boundaries (checkpoint ids are time-ordered).
    for i in range(2):
        assert history.turns[i].anchor.input_checkpoint_id < tails[i]
        assert tails[i] < history.turns[i + 1].anchor.input_checkpoint_id

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
    history = await _history(reader)
    assert len(history.turns) == 2
    assert history.turns[1].anchor.input_checkpoint_id == interrupt_tip
    assert history.turns[0].anchor.tail_checkpoint_id == interrupt_tip
    anchors, tip_id = await reader.aget_turn_anchors(THREAD)
    assert anchors[0].tail_checkpoint_id == interrupt_tip
    assert anchors[1].tail_checkpoint_id == tip_id


def _ask_then_reply_graph(checkpointer, *, asks=True):
    """``ask`` waits on the user, ``reply`` follows. ``asks=False`` is another
    agent holding the same node, one that runs without asking."""

    def ask(state):
        if not asks:
            return {"messages": [AIMessage(content="refused", id="ai-refused")]}
        answer = interrupt({"action_requests": [{"description": "go?"}]})
        return {"messages": [AIMessage(content=f"asked: {answer}", id="ai-asked")]}

    def reply(state):
        return {"messages": [AIMessage(content="reply", id=f"ai-{len(state['messages'])}")]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("ask", ask)
        .add_node("reply", reply)
        .add_edge(START, "ask")
        .add_edge("ask", "reply")
        .compile(checkpointer=checkpointer)
    )


async def _interrupted_turn(graph):
    await graph.ainvoke(
        {"messages": [HumanMessage(content="q0", id="h-0")]},
        _cfg(turn_index=0, run_id="run-0"),
    )
    state = await graph.aget_state(_cfg())
    return state.config["configurable"]["checkpoint_id"], state.interrupts[0].id


async def test_resume_of_cancelled_calls_is_its_own_turn_boundary():
    # A thread that changed agents while it waited has its waiting calls
    # cancelled by an update, so nothing re-runs interrupt() to write
    # __resume__; the resume still persists a turn of its own.
    saver = InMemorySaver()
    graph = _ask_then_reply_graph(saver)
    interrupt_tip, interrupt_id = await _interrupted_turn(graph)

    await graph.aupdate_state(
        _cfg(),
        {"messages": [AIMessage(content="Not run", id="ai-cancelled")]},
        as_node="ask",
    )
    await graph.ainvoke(
        Command(resume={interrupt_id: "yes"}), _cfg(turn_index=1, run_id="run-1")
    )

    history = await _history(CheckpointHistoryReader(saver))
    assert len(history.turns) == 2
    assert history.turns[1].anchor.input_checkpoint_id == interrupt_tip
    assert [m.content for m in history.turns[1].messages] == ["Not run", "reply"]
    assert [i["id"] for i in history.turns[0].anchor.ending_interrupts] == [interrupt_id]


async def test_resume_by_an_agent_that_does_not_ask_is_its_own_turn_boundary():
    saver = InMemorySaver()
    interrupt_tip, interrupt_id = await _interrupted_turn(
        _ask_then_reply_graph(saver)
    )

    await _ask_then_reply_graph(saver, asks=False).ainvoke(
        Command(resume={interrupt_id: "yes"}), _cfg(turn_index=1, run_id="run-1")
    )

    history = await _history(CheckpointHistoryReader(saver))
    assert len(history.turns) == 2
    assert history.turns[1].anchor.input_checkpoint_id == interrupt_tip
    assert [m.content for m in history.turns[1].messages] == ["refused", "reply"]


async def test_interrupted_checkpoint_continued_by_its_own_run_is_no_boundary():
    # The background-task orchestrator injects into the same run and carries
    # on; that run's turn is still one turn.
    saver = InMemorySaver()
    graph = _ask_then_reply_graph(saver)
    await _interrupted_turn(graph)

    await graph.aupdate_state(
        _cfg(run_id="run-0"),
        {"messages": [AIMessage(content="note", id="ai-note")]},
        as_node="ask",
    )
    await graph.ainvoke(None, _cfg(run_id="run-0"))

    history = await _history(CheckpointHistoryReader(saver))
    assert len(history.turns) == 1


async def test_new_message_over_an_unanswered_interrupt_adds_one_boundary():
    saver = InMemorySaver()
    graph = _ask_then_reply_graph(saver)
    await _interrupted_turn(graph)

    await graph.ainvoke(
        {"messages": [HumanMessage(content="q1", id="h-1")]},
        _cfg(turn_index=1, run_id="run-1"),
    )

    history = await _history(CheckpointHistoryReader(saver))
    assert [t.anchor.turn_index for t in history.turns] == [0, 1]


async def test_empty_thread():
    reader = CheckpointHistoryReader(InMemorySaver())
    assert await reader.aget_turn_anchors("no-such-thread") == ([], None)



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
    history = await _history(reader)

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


async def test_turn_slices_from_anchors_match_the_full_walk():
    """Reading chosen turns from their anchors yields exactly those turns as
    a read of every turn builds them, compaction and answered interrupts
    included."""
    saver = InMemorySaver()
    state = {"n": 0}

    def agent(st):
        state["n"] += 1
        if state["n"] == 2:
            answer = interrupt({"action_requests": [{"description": "ok?"}]})
            return {"messages": [AIMessage(content=f"after {answer}", id="ai-hitl")]}
        if state["n"] == 4:
            return {
                "messages": [
                    RemoveMessage(id=REMOVE_ALL_MESSAGES),
                    AIMessage(content="kept", id="ai-kept"),
                ]
            }
        return {"messages": [AIMessage(content=f"r{state['n']}", id=f"ai-{state['n']}")]}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    await graph.ainvoke({"messages": [HumanMessage(content="q0", id="h-0")]}, _cfg(turn_index=0))
    await graph.ainvoke({"messages": [HumanMessage(content="q1", id="h-1")]}, _cfg(turn_index=1))
    await graph.ainvoke(Command(resume="yes"), _cfg())
    await graph.ainvoke({"messages": [HumanMessage(content="q3", id="h-3")]}, _cfg(turn_index=3))

    reader = CheckpointHistoryReader(saver)
    anchors, _tip = await reader.aget_turn_anchors(THREAD)
    assert len(anchors) == 4

    def shape(t):
        return (
            t.anchor.turn_ordinal,
            t.anchor.input_checkpoint_id,
            t.anchor.end_checkpoint_id,
            t.anchor.tail_checkpoint_id,
            t.anchor.turn_index,
            [m.id for m in t.messages],
            [i["value"] for i in t.anchor.ending_interrupts],
            t.new_summarization_event,
            t.newly_offloaded_args,
        )

    every = await reader.aget_turn_slices(THREAD, anchors)
    # The resume reruns the asking node, and the last turn's REMOVE_ALL
    # takes its own input with everything before it.
    assert [[m.id for m in t.messages] for t in every] == [
        ["h-0", "ai-1"],
        ["h-1"],
        ["ai-3"],
        ["ai-kept"],
    ]
    assert [i["value"] for i in every[1].anchor.ending_interrupts] == [
        {"action_requests": [{"description": "ok?"}]}
    ]
    picked = await reader.aget_turn_slices(THREAD, [anchors[1], anchors[3]])
    assert [shape(t) for t in picked] == [shape(every[1]), shape(every[3])]


async def test_task_runs_slice_by_their_own_boundaries():
    """Each run is the diff between its own boundary and the next run's, so a
    later run that compacts the namespace does not erase an earlier run, and
    each run carries only the signals that landed during it."""
    saver = InMemorySaver()
    calls = {"n": 0}

    def agent(st):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"messages": [AIMessage(content="first answer", id="sub-ai-1")]}
        return {
            "messages": [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                AIMessage(content="second answer", id="sub-ai-2"),
            ],
            "_offloaded_tool_call_ids": {"tc-9"},
        }

    sub_graph = (
        StateGraph(MainAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    for n, run_id in ((1, "run-a"), (2, "run-b")):
        await sub_graph.ainvoke(
            {"messages": [HumanMessage(content=f"prompt {n}", id=f"sub-h-{n}")]},
            {
                "configurable": {
                    "thread_id": THREAD,
                    "checkpoint_ns": "task:tsk1",
                    CONFIG_KEY_TASK_ID: "parent-task",
                },
                "metadata": {"task_run_id": run_id},
            },
        )

    reader = CheckpointHistoryReader(saver)
    runs = await reader.aget_task_runs(THREAD, "tsk1")
    assert [(r.ordinal, r.task_run_id) for r in runs] == [(0, "run-a"), (1, "run-b")]
    assert runs[0].end_checkpoint_id == runs[1].input_checkpoint_id

    slices = await reader.aget_run_slices(THREAD, "tsk1", runs)
    assert [m.id for m in slices[0].messages] == ["sub-h-1", "sub-ai-1"]
    assert [m.id for m in slices[1].messages] == ["sub-ai-2"]
    assert slices[0].newly_offloaded_args == 0
    assert slices[1].newly_offloaded_args == 1
    # The namespace tip alone has lost the first run entirely.
    tip = await reader.aget_task_history(THREAD, "tsk1")
    assert "sub-ai-1" not in [m.id for m in tip.messages]
