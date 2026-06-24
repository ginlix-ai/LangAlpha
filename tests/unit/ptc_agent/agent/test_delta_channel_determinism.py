"""Determinism guard for `DeltaChannel`-backed `messages` — THE P1 regression test.

Under `DeltaChannel`, non-snapshot steps persist the *raw* write (a sentinel in
`channel_values`, the actual messages stored as a checkpoint write) BEFORE the
reducer runs. Any message that enters the channel *without an id* is therefore
persisted id-less, and the reducer re-mints a fresh `uuid4()` for it on *every*
reconstruction — so two reconstructions of the same checkpoint disagree on ids,
and a full-list write-back (the hard-stop checkpoint flush in
`background_task_manager._flush_checkpoint`, or a post-`/offload` write) freezes
one id into history while the original id-less write keeps re-minting → duplicate
messages.

Two layers stamp ids before persistence; this test pins what each covers on the
pinned langgraph (>=1.2.2):

* **Upstream (langgraph >=1.2.2)** stamps id-less `BaseMessage`/dict writes inside
  `PregelLoop.put_writes` (`ensure_message_ids`, keyed on `DeltaChannel`), BEFORE
  the checkpointer sees them — so a plain `InMemorySaver` is already deterministic
  for the `HumanMessage`/dict chat path. `test_upstream_stamps_plain_id_less_writes`
  guards that, and the >=1.2.2 floor in pyproject.toml: if a bump regresses the
  upstream stamp, it fails.
* **Our `IdStampingCheckpointerMixin`** (`src.server.utils.checkpointer`) is the
  *residual* cover for the case upstream misses: `ensure_message_ids` handles only
  `BaseMessage`/dict/list, NOT the `Overwrite` wrapper. In deepagents' default
  stack the one middleware that writes an *id-less* message inside `Overwrite([...])`
  is `PatchToolCallsMiddleware`, repairing a dangling tool call after a mid-tool
  cancel (exactly the hard-stop path this branch touches). `filesystem` eviction
  keeps the original id and `summarization`'s id-less writes are plain lists, so
  both are already covered by the upstream stamp.
  An id-less message inside an `Overwrite` survives upstream un-stamped and
  re-mints once a later turn folds it into reducer `state`. The mixin unwraps the
  `Overwrite` and stamps it. `test_overwrite_id_less_without_shim_*` reproduces the
  gap on a plain saver; `test_shim_fixes_overwrite_id_less_path` proves the mixin
  closes it, keeping reconstruction deterministic and the flush a no-op.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Overwrite

from ptc_agent.agent.state import DeltaAgentState
from src.server.utils.checkpointer import IdStampingCheckpointerMixin


class ShimmedSaver(IdStampingCheckpointerMixin, InMemorySaver):
    """In-memory saver WITH the P1 id-stamping fix (mixin first in MRO)."""


# A plain ``InMemorySaver`` is the "no shim" control that reproduces the bug.


# --- repro harness ---------------------------------------------------------

# Each superstep writes >1 id-less message; this reliably exercises the
# id-minting-order non-determinism (a single id-less write per step only trips
# it ~10% of the time, which would be a flaky bug demonstration).
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
    graph, not ``BaseMessage`` objects. Dicts skipped the BaseMessage id-stamp
    (the P1 hole Codex found), so this is the path the shim must cover.
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


# --- precondition + upstream-fix guard -------------------------------------


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


def test_upstream_stamps_plain_id_less_writes():
    """Upstream (langgraph >=1.2.2) makes a PLAIN saver deterministic for the
    common id-less write path — so the mixin is redundant *there*.

    `ensure_message_ids` runs in `PregelLoop.put_writes` before the checkpointer,
    stamping id-less `BaseMessage` and dict writes for any saver. A plain
    `InMemorySaver` with NO shim therefore reconstructs deterministically and its
    hard-stop flush is a no-op, for both the `HumanMessage` and dict chat paths.
    This pins that behaviour and the >=1.2.2 floor in pyproject.toml: if a langgraph
    bump drops the upstream stamp this fails — and the Overwrite tests below show
    the mixin is still load-bearing for the case upstream never covered.
    """
    for label, drive in (("basemsg", _drive_turns), ("dict", _drive_turns_dicts)):
        saver = InMemorySaver()  # NO shim — upstream is the only thing stamping
        graph = _build_graph(saver)
        config = {"configurable": {"thread_id": f"upstream-{label}"}}
        drive(graph, config)

        ids_a = _reconstruct_ids(graph, config)
        ids_b = _reconstruct_ids(graph, config)
        assert len(ids_a) == _EXPECTED_COUNT
        assert None not in ids_a, f"upstream must stamp every {label} write"
        assert ids_a == ids_b, (
            f"langgraph >=1.2.2 must reconstruct the {label} path deterministically "
            "without the shim; a mismatch means the upstream id-stamp regressed"
        )
        before = len(graph.get_state(config).values["messages"])
        _simulate_hard_stop_flush(graph, config)
        after = len(graph.get_state(config).values["messages"])
        assert after == before == _EXPECTED_COUNT, (
            f"upstream-stamped {label} flush must be a no-op, got {before} -> {after}"
        )


# --- residual gap: Overwrite-wrapped id-less messages (upstream misses these) --
#
# `ensure_message_ids` only handles BaseMessage/dict/list, never the `Overwrite`
# wrapper. In deepagents' default stack the only id-less Overwrite write to
# `messages` is patch_tool_calls' dangling-tool repair after a mid-tool cancel
# (filesystem eviction keeps the id; summarization's id-less writes are plain
# lists, both upstream-covered). An id-less message inside an Overwrite is
# persisted un-stamped and re-mints once a later turn folds it into reducer
# `state` — the live bug-vs-fix discriminator that justifies keeping the mixin.


def _drive_overwrite_then_turn(graph, config) -> None:
    """`_TURNS` normal turns, an `Overwrite([id-kept, id-less ToolMessage])` repair
    (the `patch_tool_calls` shape), then one more turn so the reducer folds the
    id-less message into `state`."""
    _drive_turns(graph, config)
    graph.update_state(
        config,
        {
            "messages": Overwrite(
                [
                    AIMessage("base", id="keep"),
                    ToolMessage(content="cancelled", tool_call_id="c1"),
                ]
            )
        },
    )
    graph.invoke({"messages": [HumanMessage("after-repair")]}, config)


def test_overwrite_id_less_without_shim_is_nondeterministic_and_dups():
    """Control: upstream alone does NOT cover the Overwrite path → P1 is live.

    On a plain saver the single id-less ToolMessage inside the Overwrite re-mints a
    different uuid on every reconstruction (so the mismatch is reliable, not
    probabilistic), and the hard-stop flush write-back duplicates it. Proves the
    fix-half below is meaningful — if this ever goes stable, upstream started
    covering Overwrite and the mixin's unwrap may have become redundant.
    """
    saver = InMemorySaver()  # upstream-only, no mixin
    graph = _build_graph(saver)
    config = {"configurable": {"thread_id": "overwrite-bug"}}
    _drive_overwrite_then_turn(graph, config)

    ids_a = _reconstruct_ids(graph, config)
    ids_b = _reconstruct_ids(graph, config)
    assert ids_a != ids_b, (
        "Overwrite-wrapped id-less message must re-mint without the shim (P1)"
    )
    before = len(graph.get_state(config).values["messages"])
    _simulate_hard_stop_flush(graph, config)
    after = len(graph.get_state(config).values["messages"])
    assert after > before, (
        f"hard-stop flush must duplicate the re-minted message, got {before} -> {after}"
    )


def test_shim_fixes_overwrite_id_less_path():
    """With the mixin: the Overwrite-wrapped id-less message is stamped before
    persistence, so reconstruction is deterministic and the flush is a no-op."""
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "overwrite-fix"}}
    _drive_overwrite_then_turn(graph, config)

    ids_a = _reconstruct_ids(graph, config)
    ids_b = _reconstruct_ids(graph, config)
    assert None not in ids_a, "the mixin must stamp the Overwrite-injected message"
    assert ids_a == ids_b, "mixin must stabilise Overwrite-injected ids"
    before = len(graph.get_state(config).values["messages"])
    _simulate_hard_stop_flush(graph, config)
    after = len(graph.get_state(config).values["messages"])
    assert after == before, (
        f"Overwrite flush must be a no-op under the shim, got {before} -> {after}"
    )


# --- fix half: ShimmedSaver (IdStampingCheckpointerMixin + InMemorySaver) ----


def test_shim_reconstruction_is_deterministic():
    """With the shim: two reconstructions of the same checkpoint give same ids."""
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-determinism"}}
    _drive_turns(graph, config)

    ids_a = _reconstruct_ids(graph, config)
    ids_b = _reconstruct_ids(graph, config)
    assert len(ids_a) == _EXPECTED_COUNT
    assert ids_a == ids_b, "id-stamping shim must make reconstruction deterministic"


def test_shim_hard_stop_flush_is_noop_on_count():
    """With the shim: the hard-stop flush write-back does not duplicate messages."""
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-flush"}}
    _drive_turns(graph, config)

    before = len(graph.get_state(config).values["messages"])
    assert before == _EXPECTED_COUNT
    _simulate_hard_stop_flush(graph, config)
    after = len(graph.get_state(config).values["messages"])
    assert after == before, (
        f"hard-stop flush must be a no-op under the shim, got {before} -> {after}"
    )


def test_shim_remove_message_by_id_still_removes_across_reconstruction():
    """With the shim: ids are stable, so `RemoveMessage(id=...)` reliably hits.

    This is a positive correctness guard for the shim, not a bug-vs-fix
    discriminator: stable ids are the precondition that lets compaction/offload
    target messages by id across turns. (Measured: in-memory this particular
    assertion happens to also pass without the shim, because the removal write
    and its replay land in the same reconstruction pass; the discriminating
    P1 symptoms are the determinism + flush-no-op tests above. The genuine
    by-id-miss surfaces when a previously-frozen id is targeted on the Postgres
    replay path.)
    """
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-remove"}}
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


# --- the dict-input chat path (not just BaseMessage) -------------------------
#
# The harness above drives ``HumanMessage`` objects; the REAL chat path feeds
# ``{"role", "content"}`` dicts (``normalize_request_messages``). On langgraph
# >=1.2.2 upstream stamps these too (asserted by the plain-saver path in
# `test_upstream_stamps_plain_id_less_writes`), and the shim stamps them
# redundantly. These pin that the shim's dict coverage keeps the chat path
# deterministic — belt-and-suspenders if the upstream stamp ever regresses.


def test_shim_dict_input_reconstruction_is_deterministic():
    """With the shim: the dict chat path reconstructs deterministically.

    Regression for the P1 hole — the shim originally stamped only BaseMessage
    writes, but the chat path feeds dicts. The dict-aware shim gives them a
    stable id so two reconstructions of the same checkpoint agree.
    """
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-dict-determinism"}}
    _drive_turns_dicts(graph, config)

    ids_a = _reconstruct_ids(graph, config)
    ids_b = _reconstruct_ids(graph, config)
    assert len(ids_a) == _EXPECTED_COUNT
    assert ids_a == ids_b, (
        "dict-input reconstruction must be deterministic under the shim"
    )


def test_shim_dict_input_hard_stop_flush_is_noop():
    """With the shim: the hard-stop flush on a dict-input thread is a no-op."""
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-dict-flush"}}
    _drive_turns_dicts(graph, config)

    before = len(graph.get_state(config).values["messages"])
    assert before == _EXPECTED_COUNT
    _simulate_hard_stop_flush(graph, config)
    after = len(graph.get_state(config).values["messages"])
    assert after == before, (
        f"hard-stop flush on the dict chat path must not duplicate, got "
        f"{before} -> {after}"
    )


def test_stamp_message_ids_covers_dicts_and_basemessages():
    """Direct unit test of the shim's stamping rules.

    id-less ``BaseMessage``s and id-less dict messages get stamped; already-id'd
    messages, ``RemoveMessage``, and non-``messages`` channels are left alone.
    """
    from src.server.utils.checkpointer import _stamp_message_ids

    bare = AIMessage(content="bare")          # id-less BaseMessage
    keep = AIMessage(content="x", id="keep")  # already has an id
    rm = RemoveMessage(id="r")                # must stay untouched
    dict_msg = {"role": "user", "content": "hi"}  # id-less dict (chat path)
    single = AIMessage(content="solo")        # single (non-list) write value
    other = AIMessage(content="b")            # non-messages channel

    writes = [
        ("messages", [bare, keep, rm, dict_msg]),
        ("messages", single),
        ("other", [other]),
    ]
    _stamp_message_ids(writes)

    assert bare.id is not None
    assert keep.id == "keep"
    assert rm.id == "r"
    assert dict_msg.get("id")          # dict stamped (the P1 fix)
    assert single.id is not None       # single non-list value stamped
    assert other.id is None            # non-messages channel untouched


@pytest.mark.asyncio
async def test_shim_async_path_is_deterministic_and_flush_noop():
    """The PRODUCTION path is async (``aput_writes``); every test above drives the
    SYNC graph, so only ``put_writes`` runs.

    ``IdStampingAsyncPostgresSaver`` (prod) and the in-memory dev path under the
    ASGI server both checkpoint via ``aput_writes`` — a SEPARATE method body that
    could regress independently of ``put_writes``. This drives the delta graph
    with ``ainvoke`` (dict chat input, the worst case) so the async stamp override
    is exercised end-to-end: reconstruction must be deterministic and the
    hard-stop flush a no-op.
    """
    graph = _build_graph(ShimmedSaver())
    config = {"configurable": {"thread_id": "fix-async"}}
    for i in range(_TURNS):
        await graph.ainvoke(
            {"messages": [{"role": "user", "content": f"turn {i}"}]}, config
        )

    ids_a = [m.id for m in (await graph.aget_state(config)).values["messages"]]
    ids_b = [m.id for m in (await graph.aget_state(config)).values["messages"]]
    assert len(ids_a) == _EXPECTED_COUNT
    assert ids_a == ids_b, (
        "async (aput_writes) reconstruction must be deterministic under the shim"
    )

    snap = await graph.aget_state(config)
    before = len(snap.values["messages"])
    await graph.aupdate_state(config, snap.values)
    after = len((await graph.aget_state(config)).values["messages"])
    assert after == before == _EXPECTED_COUNT, (
        f"async hard-stop flush must be a no-op under the shim, got {before} -> {after}"
    )


def test_stamp_message_ids_overwrite_single_and_empty():
    """``Overwrite`` wrapping a SINGLE (non-list) message, and an empty
    ``Overwrite([])`` reset.

    The covered unwrap test always wraps a *list*; a regression that only handled
    ``Overwrite(list)`` would pass it. ``Overwrite`` can in principle wrap a single
    value too, so pin both defensively: the single value inside the wrapper gets
    stamped, and an empty reset stays an untouched, preserved ``Overwrite``.
    """
    from langgraph.types import Overwrite

    from src.server.utils.checkpointer import _stamp_message_ids

    single_msg = AIMessage(content="solo")            # id-less single BaseMessage
    single_dict = {"role": "user", "content": "x"}    # id-less single dict
    r1 = _stamp_message_ids(
        [("messages", Overwrite(single_msg)), ("messages", Overwrite(single_dict))]
    )
    assert single_msg.id is not None
    assert single_dict.get("id")
    assert isinstance(r1[0][1], Overwrite)
    assert isinstance(r1[1][1], Overwrite)

    # Empty Overwrite([]) (a reset to no messages) is a structural no-op: nothing
    # to stamp, and the wrapper must survive so DeltaChannel still resets.
    r2 = _stamp_message_ids([("messages", Overwrite([]))])
    assert isinstance(r2[0][1], Overwrite)
    assert r2[0][1].value == []


def test_stamp_message_ids_unwraps_overwrite():
    """The shim must stamp id-less messages wrapped in ``Overwrite``.

    deepagents' ``PatchToolCallsMiddleware`` (default stack) repairs a dangling
    tool call on resume-after-cancel by writing ``{"messages": Overwrite([...
    id-less ToolMessage ...])}``. Before the unwrap, the wrapper was neither list
    nor tuple, so the shim skipped it and the filler persisted id-less → re-minted
    on every delta replay (non-deterministic ids on the hard-stop resume path).
    This pins the unwrap so the message is stamped while the ``Overwrite`` wrapper
    (reset semantics) is preserved.
    """
    from langchain_core.messages import ToolMessage
    from langgraph.types import Overwrite

    from src.server.utils.checkpointer import _stamp_message_ids

    patched = ToolMessage(content="cancelled", tool_call_id="call_1")  # id-less
    keep = AIMessage(content="x", id="keep")
    dict_msg = {"role": "user", "content": "hi"}  # id-less dict inside Overwrite
    writes = [("messages", Overwrite([patched, keep, dict_msg]))]

    result = _stamp_message_ids(writes)

    assert patched.id is not None      # id-less BaseMessage inside Overwrite stamped
    assert keep.id == "keep"           # already-id'd left alone
    assert dict_msg.get("id")          # id-less dict inside Overwrite stamped
    # wrapper preserved so DeltaChannel still treats it as a reset/base
    assert isinstance(result[0][1], Overwrite)
