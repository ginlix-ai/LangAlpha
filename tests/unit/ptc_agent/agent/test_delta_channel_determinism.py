"""Determinism guard for `DeltaChannel`-backed `messages` — THE P1 regression test.

Under `DeltaChannel`, non-snapshot steps persist the *raw* write (a sentinel in
`channel_values`, the messages stored as a checkpoint write) BEFORE the reducer
runs. A message that entered the channel *without an id* would be persisted
id-less; if the reducer minted a fresh `uuid4()` for it on every reconstruction,
two reads of the same checkpoint would disagree on ids and a full-list write-back
(the hard-stop checkpoint flush in `background_task_manager._flush_checkpoint`, or
a post-`/offload` write) would freeze one id into history while the original
id-less write kept re-minting → duplicate messages.

The clean pattern that prevents this (matching deepagents 0.6.11) has two parts,
and NO app-side id-stamping shim:

* **Upstream stamp.** langgraph's `ensure_message_ids` (>=1.2.2) stamps id-less
  `BaseMessage`/dict/list writes inside `PregelLoop.put_writes` — BEFORE the
  checkpointer persists the raw delta write — so every message lands on disk with
  a stable id, for any saver (plain `InMemorySaver` included).
* **Non-minting reducer.** The vendored `ptc_agent.agent.state.messages_delta_reducer`
  appends an id-less message as-is and never re-rolls its id, so even a message
  that somehow slipped past the upstream stamp reconstructs deterministically (id
  stays None) rather than duplicating.

langalpha emits no id-less `Overwrite` writes — the one deepagents path that did
(`PatchToolCallsMiddleware`'s dangling-tool repair) was changed in 0.6.11 to write
a plain list that `ensure_message_ids` covers — so the earlier id-stamping
checkpointer shim is no longer needed. These tests pin the property on a plain
saver and the >=1.2.2 floor in pyproject.toml: if a langgraph bump regresses the
upstream stamp, they fail.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    RemoveMessage,
)
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from ptc_agent.agent.state import DeltaAgentState

# --- repro harness ---------------------------------------------------------

# Each superstep writes >1 id-less message; this reliably exercises any
# id-stamping-order non-determinism (a single id-less write per step only trips
# it ~10% of the time, which would be a flaky demonstration).
_NODE_MESSAGES_PER_STEP = 2
_TURNS = 6
# 1 HumanMessage input + N node messages per turn.
_EXPECTED_COUNT = _TURNS * (1 + _NODE_MESSAGES_PER_STEP)


def _build_graph(saver):
    """A real ``StateGraph`` whose ``messages`` field is the ``DeltaChannel``.

    The single node returns id-less ``AIMessage``s, mirroring the many id-less
    app messages langalpha writes (orchestrator/steering/subagent-return/etc.).
    """
    builder = StateGraph(DeltaAgentState)

    def node(_state: DeltaAgentState) -> dict[str, list[AnyMessage]]:
        return {
            "messages": [
                AIMessage(f"from-node-{k}") for k in range(_NODE_MESSAGES_PER_STEP)
            ]
        }

    builder.add_node("respond", node)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)
    return builder.compile(checkpointer=saver)


def _drive_turns(graph, config) -> None:
    """Drive `_TURNS` turns of id-less ``HumanMessage`` input through the graph."""
    for i in range(_TURNS):
        graph.invoke({"messages": [HumanMessage(f"turn {i}")]}, config)


def _drive_turns_dicts(graph, config) -> None:
    """Drive turns with dict-form user input — the REAL chat path.

    ``normalize_request_messages`` feeds ``{"role", "content"}`` dicts into the
    graph, not ``BaseMessage`` objects. langgraph's ``ensure_message_ids`` stamps
    the dict's ``id`` in place (a separate branch from the BaseMessage one), so
    this exercises the dict half of the upstream stamp.
    """
    for i in range(_TURNS):
        graph.invoke({"messages": [{"role": "user", "content": f"turn {i}"}]}, config)


def _reconstruct_ids(graph, config) -> list[str]:
    """Reconstruct the thread's messages and return their ids."""
    return [m.id for m in graph.get_state(config).values["messages"]]


def _simulate_hard_stop_flush(graph, config) -> None:
    """Replicate ``background_task_manager._flush_checkpoint`` exactly.

    The app does ``aget_state`` then ``aupdate_state(config, snapshot.values)`` —
    a full-list write-back of the reconstructed state. We use the sync
    equivalents here.
    """
    values = graph.get_state(config).values
    graph.update_state(config, values)


# --- precondition: the delta step that makes the property meaningful --------


def test_head_checkpoint_omits_messages_on_non_snapshot_step():
    """Head ``channel_values`` omits ``messages`` (sentinel / non-snapshot step).

    With ``snapshot_frequency=50`` (DeltaAgentState's default) a thread of
    ~18 messages never hits a snapshot step, so the latest checkpoint blob is a
    sentinel and the raw ``channel_values`` dict has no ``messages`` key. This is
    the precondition that makes the determinism property meaningful — without it,
    the full list would be stored every step (the ``add_messages`` behaviour) and
    there would be nothing to re-mint.
    """
    saver = InMemorySaver()
    graph = _build_graph(saver)
    config = {"configurable": {"thread_id": "head-sentinel"}}
    _drive_turns(graph, config)

    tup = saver.get_tuple(config)
    channel_values = tup.checkpoint["channel_values"]
    assert "messages" not in channel_values, (
        "expected a non-snapshot delta step (sentinel), but the head checkpoint "
        f"stored messages directly: {list(channel_values)}"
    )


# --- the P1 guard: deterministic reconstruction, flush is a no-op -----------


def test_reconstruction_is_deterministic_and_flush_noop():
    """Two reconstructions agree on ids and the hard-stop flush does not duplicate.

    Covers both the ``BaseMessage`` path and the dict chat path (the REAL path —
    ``normalize_request_messages`` feeds ``{"role","content"}`` dicts). On a plain
    saver with NO shim, ``ensure_message_ids`` stamps every id-less write before
    persistence, so reconstruction is deterministic and the full-list write-back
    is a no-op. A mismatch means the langgraph >=1.2.2 upstream stamp regressed.
    """
    for label, drive in (("basemsg", _drive_turns), ("dict", _drive_turns_dicts)):
        saver = InMemorySaver()  # plain saver — upstream stamp is the only mechanism
        graph = _build_graph(saver)
        config = {"configurable": {"thread_id": f"determinism-{label}"}}
        drive(graph, config)

        ids_a = _reconstruct_ids(graph, config)
        ids_b = _reconstruct_ids(graph, config)
        assert len(ids_a) == _EXPECTED_COUNT
        assert None not in ids_a, f"every {label} write must be stamped upstream"
        assert ids_a == ids_b, (
            f"{label} reconstruction must be deterministic; a mismatch means the "
            "langgraph >=1.2.2 id-stamp regressed"
        )
        before = len(graph.get_state(config).values["messages"])
        _simulate_hard_stop_flush(graph, config)
        after = len(graph.get_state(config).values["messages"])
        assert after == before == _EXPECTED_COUNT, (
            f"{label} hard-stop flush must be a no-op, got {before} -> {after}"
        )


def test_remove_message_by_id_still_removes_across_reconstruction():
    """Stable ids let ``RemoveMessage(id=...)`` reliably target a message.

    Stable ids are the precondition that lets compaction/offload remove messages
    by id across turns. With upstream stamping every message has a stable id, so
    the removal write hits across reconstruction.
    """
    saver = InMemorySaver()
    graph = _build_graph(saver)
    config = {"configurable": {"thread_id": "remove-by-id"}}
    _drive_turns(graph, config)

    messages = graph.get_state(config).values["messages"]
    target_id = messages[0].id
    assert target_id is not None

    graph.update_state(config, {"messages": [RemoveMessage(id=target_id)]})

    after = graph.get_state(config).values["messages"]
    after_ids = [m.id for m in after]
    assert len(after) == _EXPECTED_COUNT - 1
    assert target_id not in after_ids, (
        "RemoveMessage-by-id must remove the targeted message; a miss means ids "
        "were not stable across reconstruction"
    )


@pytest.mark.asyncio
async def test_async_path_is_deterministic_and_flush_noop():
    """The PRODUCTION path is async (``aput_writes``); pin it independently.

    Every sync test drives ``put_writes``; the prod Postgres saver and the
    in-memory dev path under the ASGI server checkpoint via ``aput_writes`` — a
    separate method body that could regress independently. Drive the delta graph
    with ``ainvoke`` (dict chat input, the worst case): reconstruction must be
    deterministic and the hard-stop flush a no-op.
    """
    saver = InMemorySaver()
    graph = _build_graph(saver)
    config = {"configurable": {"thread_id": "async-determinism"}}
    for i in range(_TURNS):
        await graph.ainvoke(
            {"messages": [{"role": "user", "content": f"turn {i}"}]}, config
        )

    ids_a = [m.id for m in (await graph.aget_state(config)).values["messages"]]
    ids_b = [m.id for m in (await graph.aget_state(config)).values["messages"]]
    assert len(ids_a) == _EXPECTED_COUNT
    assert None not in ids_a, "async path must stamp every write upstream"
    assert ids_a == ids_b, (
        "async (aput_writes) reconstruction must be deterministic"
    )

    snap = await graph.aget_state(config)
    before = len(snap.values["messages"])
    await graph.aupdate_state(config, snap.values)
    after = len((await graph.aget_state(config)).values["messages"])
    assert after == before == _EXPECTED_COUNT, (
        f"async hard-stop flush must be a no-op, got {before} -> {after}"
    )
