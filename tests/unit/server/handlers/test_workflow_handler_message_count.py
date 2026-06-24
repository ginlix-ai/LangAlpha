"""Tests for DeltaChannel-safe message_count in workflow_handler.

Under DeltaChannel, the latest checkpoint is usually a NON-snapshot step:
``messages`` is absent from raw ``channel_values`` (a sentinel), so the old
``len(channel_values["messages"])`` would report 0. ``_delta_safe_message_count``
reconstructs the true count cheaply via the checkpointer (no graph, no sandbox),
folding in the head checkpoint's own pending writes (which the delta history
excludes — Codex P3).

The scenario is built with an in-memory ``StateGraph`` whose ``messages`` field
uses ``DeltaChannel`` + the vendored reducer, so ``InMemorySaver`` exercises the
real ``aget_delta_channel_history`` path.
"""

from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    RemoveMessage,
)
from langgraph.channels import DeltaChannel
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from typing_extensions import TypedDict

from ptc_agent.agent.state import messages_delta_reducer


class _DeltaState(TypedDict):
    messages: Annotated[
        list[AnyMessage],
        DeltaChannel(messages_delta_reducer, snapshot_frequency=50),
    ]


def _node(state):
    # Append an id-less AI message (id-less is the worst case for delta replay).
    return {"messages": [AIMessage(content="reply")]}


def _build_delta_graph():
    g = StateGraph(_DeltaState)
    g.add_node("n", _node)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    saver = InMemorySaver()
    return g.compile(checkpointer=saver), saver


class _Snap1State(TypedDict):
    # snapshot_frequency=1 → every step is a snapshot, so the head checkpoint
    # stores a real `_DeltaSnapshot` blob in channel_values (not a sentinel).
    messages: Annotated[
        list[AnyMessage],
        DeltaChannel(messages_delta_reducer, snapshot_frequency=1),
    ]


def _build_snapshot_graph():
    g = StateGraph(_Snap1State)
    g.add_node("n", _node)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    saver = InMemorySaver()
    return g.compile(checkpointer=saver), saver


async def _run_turns(graph, cfg, n):
    for i in range(n):
        await graph.ainvoke({"messages": [HumanMessage(content=f"q{i}")]}, cfg)


@pytest.mark.asyncio
async def test_delta_count_nonsnapshot_step_reconstructs_true_count():
    """Latest step is a non-snapshot delta step (messages absent from
    channel_values) → the helper reconstructs the TRUE count, not 0."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    graph, saver = _build_delta_graph()
    cfg = {"configurable": {"thread_id": "t1"}}
    await _run_turns(graph, cfg, 3)  # 3 Human + 3 AI = 6 messages

    tup = await saver.aget_tuple(cfg)
    # Precondition: this is genuinely a non-snapshot step.
    assert "messages" not in tup.checkpoint.get("channel_values", {})

    true_count = len(graph.get_state(cfg).values["messages"])
    assert true_count == 6

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=saver,
    ):
        count = await _delta_safe_message_count(tup)

    assert count == true_count == 6


@pytest.mark.asyncio
async def test_delta_count_includes_head_pending_writes():
    """Codex P3: a write stored ON the head checkpoint is excluded from the
    delta history, so the helper must fold in ``pending_writes`` or undercount."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    graph, saver = _build_delta_graph()
    cfg = {"configurable": {"thread_id": "t1"}}
    await _run_turns(graph, cfg, 3)  # 6 messages so far

    head = await saver.aget_tuple(cfg)
    # Attach a pending message write directly ON the head checkpoint, mirroring
    # how aupdate_state records the write after the checkpoint row.
    await saver.aput_writes(
        head.config,
        [("messages", [HumanMessage(content="head-extra")])],
        task_id="task-x",
    )
    head2 = await saver.aget_tuple(head.config)
    # Precondition: the head genuinely carries a pending messages write.
    assert any(w[1] == "messages" for w in (head2.pending_writes or []))

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=saver,
    ):
        count = await _delta_safe_message_count(head2)

    # 6 base + 1 head-pending = 7. Without the P3 fold-in this would be 6.
    assert count == 7


@pytest.mark.asyncio
async def test_delta_count_after_remove_all_head_write_reports_reset_count():
    """After ``/offload`` or ``/compact``, the head checkpoint carries a
    ``messages`` pending write that STARTS with ``RemoveMessage(REMOVE_ALL)``.

    The fold-in replays that head write through the reducer on top of the
    delta-history seed, so the reset must be honored — the count is the
    post-compaction size, not seed + relist. Without honoring the reset this
    would report 7 (6 base + 1) instead of 1."""
    from langchain_core.messages import RemoveMessage
    from langgraph.graph.message import REMOVE_ALL_MESSAGES

    from src.server.handlers.workflow_handler import _delta_safe_message_count

    graph, saver = _build_delta_graph()
    cfg = {"configurable": {"thread_id": "reset"}}
    await _run_turns(graph, cfg, 3)  # 6 messages so far

    head = await saver.aget_tuple(cfg)
    # Mirror compact.py:326 — REMOVE_ALL then a fresh re-list (here, one kept msg).
    await saver.aput_writes(
        head.config,
        [
            (
                "messages",
                [
                    RemoveMessage(id=REMOVE_ALL_MESSAGES),
                    HumanMessage(content="kept", id="k1"),
                ],
            )
        ],
        task_id="reset-x",
    )
    head2 = await saver.aget_tuple(head.config)

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=saver,
    ):
        count = await _delta_safe_message_count(head2)

    assert count == 1


@pytest.mark.asyncio
async def test_real_snapshot_step_counts_deltasnapshot_not_field_count():
    """Codex P2: on a REAL snapshot step, ``channel_values['messages']`` is a
    ``_DeltaSnapshot`` NamedTuple, not a plain list. The naive
    ``len(channel_values['messages'])`` returns 1 (the NamedTuple field count);
    the helper must rehydrate the blob and report the true message count."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    graph, saver = _build_snapshot_graph()  # snapshot every step
    cfg = {"configurable": {"thread_id": "snap"}}
    await _run_turns(graph, cfg, 1)  # 1 Human + 1 AI = 2 messages

    tup = await saver.aget_tuple(cfg)
    mv = tup.checkpoint.get("channel_values", {}).get("messages")
    # Precondition: a real snapshot stores a _DeltaSnapshot, and naive len() is 1.
    assert mv is not None and not isinstance(mv, list)
    assert len(mv) == 1  # the bug the fix must avoid

    true_count = len(graph.get_state(cfg).values["messages"])
    assert true_count == 2

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=saver,
    ):
        count = await _delta_safe_message_count(tup)

    assert count == true_count == 2


@pytest.mark.asyncio
async def test_legacy_plain_list_path_uses_direct_len():
    """When ``messages`` is a plain list in channel_values (legacy add_messages
    thread) with no head pending writes, the helper seeds from the list and never
    consults the (beta) delta history."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    msgs = [HumanMessage(content="a"), AIMessage(content="b"), HumanMessage(content="c")]
    tup = MagicMock()
    tup.checkpoint = {"channel_values": {"messages": msgs}}
    tup.pending_writes = []

    checkpointer = MagicMock()
    checkpointer.aget_delta_channel_history = AsyncMock()

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=checkpointer,
    ):
        count = await _delta_safe_message_count(tup)

    assert count == 3
    # Complete-value path must not consult the (beta) delta history.
    checkpointer.aget_delta_channel_history.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_plain_list_folds_head_pending_writes():
    """Codex finding: the legacy plain-list branch must also fold in head pending
    writes, like the delta/snapshot branches. A legacy thread mid-``/offload``
    carries a ``RemoveMessage(REMOVE_ALL_MESSAGES)`` head write; the naive
    ``len(messages_value)`` would report the pre-reset count instead of the
    post-reset one."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    # Legacy seed of 3 messages...
    msgs = [
        HumanMessage(content="a", id="1"),
        AIMessage(content="b", id="2"),
        HumanMessage(content="c", id="3"),
    ]
    tup = MagicMock()
    tup.checkpoint = {"channel_values": {"messages": msgs}}
    # ...with an offload-shape head write: reset all, keep one.
    tup.pending_writes = [
        (
            "task1",
            "messages",
            [RemoveMessage(id=REMOVE_ALL_MESSAGES), HumanMessage(content="kept", id="k")],
        )
    ]

    checkpointer = MagicMock()
    checkpointer.aget_delta_channel_history = AsyncMock()

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=checkpointer,
    ):
        count = await _delta_safe_message_count(tup)

    # Reset + 1 kept = 1. Without the fold-in this would be the stale 3.
    assert count == 1
    checkpointer.aget_delta_channel_history.assert_not_awaited()


@pytest.mark.asyncio
async def test_delta_count_returns_none_on_error():
    """Any failure in the delta-reconstruction branch falls back to None so the
    status endpoint can never break on beta-internal churn. message_count is
    purely informational (no consumer branches on it)."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    # messages absent → delta branch; checkpointer raises.
    tup = MagicMock()
    tup.checkpoint = {"channel_values": {}}
    tup.config = {"configurable": {"thread_id": "t1", "checkpoint_id": "x"}}
    tup.pending_writes = []

    checkpointer = MagicMock()
    checkpointer.aget_delta_channel_history = AsyncMock(side_effect=RuntimeError("boom"))

    with patch(
        "src.server.handlers.workflow_handler.get_checkpointer",
        return_value=checkpointer,
    ):
        count = await _delta_safe_message_count(tup)

    assert count is None


@pytest.mark.asyncio
async def test_empty_tuple_returns_none():
    """No checkpoint tuple → None (no crash)."""
    from src.server.handlers.workflow_handler import _delta_safe_message_count

    assert await _delta_safe_message_count(None) is None
    empty = MagicMock()
    empty.checkpoint = None
    assert await _delta_safe_message_count(empty) is None


@pytest.mark.asyncio
async def test_get_workflow_status_reports_true_count_on_delta_step():
    """End-to-end: get_workflow_status surfaces the reconstructed count (not 0)
    in progress.message_count for a non-snapshot delta thread."""
    from src.server.handlers import workflow_handler

    graph, saver = _build_delta_graph()
    cfg = {"configurable": {"thread_id": "t1"}}
    await _run_turns(graph, cfg, 3)  # 6 messages
    tup = await saver.aget_tuple(cfg)
    assert "messages" not in tup.checkpoint.get("channel_values", {})

    tracker = MagicMock()
    tracker.get_status = AsyncMock(return_value=None)
    tracker.mark_completed = AsyncMock()

    manager = MagicMock()
    manager.get_workflow_status = AsyncMock(return_value={"status": "not_found"})

    patches = [
        patch.object(
            workflow_handler, "get_checkpoint_tuple", AsyncMock(return_value=tup)
        ),
        patch.object(workflow_handler, "get_checkpointer", return_value=saver),
        patch(
            "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
            return_value=tracker,
        ),
        patch(
            "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.database.conversation.get_thread_by_id",
            new=AsyncMock(return_value=None),
        ),
    ]
    for p in patches:
        p.start()
    try:
        result = await workflow_handler.get_workflow_status("t1")
    finally:
        for p in patches:
            p.stop()

    assert result["progress"] is not None
    assert result["progress"]["message_count"] == 6
