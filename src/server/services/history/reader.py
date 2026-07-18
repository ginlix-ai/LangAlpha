"""Checkpoint history reader — materialize thread transcripts from checkpoints.

``messages`` is a ``DeltaChannel`` (see ``ptc_agent.agent.state``), so a raw
``checkpointer.aget_tuple`` cannot materialize it: deltas live in
``checkpoint_writes`` and are only replayed by a compiled graph. This module
compiles a no-op ``StateGraph(_ReaderState)`` against the server
checkpointer purely to read state. Background subagents checkpoint under the
parent ``thread_id`` with ``checkpoint_ns="task:{task_id}"``; resolving that
namespace requires a child compiled graph registered as a node literally named
``task``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Annotated, Any

from langchain.agents.middleware.types import PrivateStateAttr
from langchain_core.messages import AnyMessage, HumanMessage
from langgraph.graph import START, StateGraph
from typing_extensions import NotRequired

from ptc_agent.agent.middleware.compaction.types import CompactionEvent
from ptc_agent.agent.state import DeltaAgentState
from src.server.utils.checkpoint_helpers import walk_current_branch_boundaries

logger = logging.getLogger(__name__)

# LangGraph records a pending write on this channel when a node pauses via
# interrupt(). We match the stored channel name directly rather than importing
# the constant, which langgraph made private in v1.0 (deprecated import); the
# on-disk channel name is a stable storage-format detail.
_INTERRUPT_CHANNEL = "__interrupt__"

# Concurrent aget_state reads per history materialization. Each read holds a
# checkpointer-pool connection (pool max defaults to 25), so this stays well
# below the pool to keep long-thread replays from starving live runs.
_STATE_READ_CONCURRENCY = 8


class _ReaderState(DeltaAgentState):
    """DeltaAgentState plus middleware-private channels replay materializes.

    ``aget_state`` only surfaces channels the reading graph declares. Live
    agents get ``_summarization_event`` from the compaction middleware's state
    schema; the reader graph must declare it itself (same annotation, so
    channel semantics match the checkpointed writes).
    """

    _summarization_event: Annotated[
        NotRequired[CompactionEvent | None], PrivateStateAttr
    ]
    _offloaded_tool_call_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]
    _offloaded_read_result_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]


def _silence_pending_sends_noise() -> None:
    """Suppress langgraph's 'unknown node name … in pending sends' warnings.

    The reader graph is a no-op shell, so historical checkpoints referencing
    real agent node names in pending sends trigger this warning on every
    materialization. Live graphs contain all their nodes, so the warning never
    fires for them; filtering the message on the emitting logger is safe.
    """
    algo_logger = logging.getLogger("langgraph.pregel._algo")
    marker = "in pending sends"
    if any(getattr(f, "_history_reader_filter", False) for f in algo_logger.filters):
        return

    def _filter(record: logging.LogRecord) -> bool:
        return marker not in record.getMessage()

    _filter._history_reader_filter = True  # type: ignore[attr-defined]
    algo_logger.addFilter(_filter)


@dataclass
class TurnSlice:
    """Messages of one conversational turn on the current checkpoint branch."""

    turn_ordinal: int
    input_checkpoint_id: str
    end_checkpoint_id: str
    user_message: HumanMessage | None
    messages: list[AnyMessage] = field(default_factory=list)
    run_id: str | None = None
    turn_index: int | None = None
    # The ``_summarization_event`` that landed during this turn (end-state
    # event differing from start-state), or None. Compaction's summary message
    # lives in this state key — never in the messages channel — so replay
    # re-emits the summarize signal from here.
    new_summarization_event: dict[str, Any] | None = None
    # Per-turn growth of the compaction offload sets — replay re-emits the
    # offload signals from these counts (one aggregated event per kind).
    newly_offloaded_args: int = 0
    newly_offloaded_reads: int = 0
    # Answered interrupts this turn raised, read from the FOLLOWING resume
    # boundary's ``__interrupt__`` writes. Pending (unanswered) interrupts at
    # the branch tip surface via ``ThreadHistory.interrupts`` instead.
    ending_interrupts: list[dict[str, Any]] = field(default_factory=list)
    # ``ui``-channel records that landed during this turn (id-diff between the
    # boundary states) — e.g. model_fallback notices pushed by middleware.
    new_ui_records: list[dict[str, Any]] = field(default_factory=list)
    # The turn's own last checkpoint — the projection-cache key (see TurnAnchor).
    tail_checkpoint_id: str | None = None


@dataclass
class ThreadHistory:
    thread_id: str
    turns: list[TurnSlice] = field(default_factory=list)
    interrupts: list[dict[str, Any]] = field(default_factory=list)
    ui: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TaskHistory:
    """Materialized state for one background-task checkpoint namespace."""

    messages: list[AnyMessage] = field(default_factory=list)
    new_summarization_event: dict[str, Any] | None = None
    newly_offloaded_args: int = 0
    newly_offloaded_reads: int = 0
    new_ui_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class _InputBoundary:
    checkpoint_id: str
    metadata: dict[str, Any]
    is_resume: bool = False
    # ``__interrupt__`` pending writes riding a resume boundary — the answered
    # interrupts of the turn this boundary resumes (interrupt and resume writes
    # attach to the same checkpoint).
    interrupts: list[dict[str, Any]] = field(default_factory=list)
    parent_checkpoint_id: str | None = None


@dataclass
class TurnAnchor:
    """A turn's identity on the current branch, without any state reads.

    ``tail_checkpoint_id`` is the turn's own last checkpoint — the id the
    branch tip held when the turn persisted — which keys the projection
    cache: it exists at persist time (unlike the next turn's boundary) and
    survives forks of later turns.
    """

    turn_ordinal: int
    input_checkpoint_id: str
    tail_checkpoint_id: str | None
    turn_index: Any | None = None
    run_id: str | None = None


def _tail_of_turn(
    boundaries: list[_InputBoundary], i: int, tip_id: str
) -> str | None:
    """Turn *i*'s last checkpoint: the tip for the last turn; otherwise the
    next boundary's parent — except a resume boundary IS the interrupted
    turn's tip (interrupt and resume writes ride the same checkpoint)."""
    if i == len(boundaries) - 1:
        return tip_id
    nxt = boundaries[i + 1]
    return nxt.checkpoint_id if nxt.is_resume else nxt.parent_checkpoint_id


def _interrupt_writes(cp_tuple: Any) -> list[dict[str, Any]]:
    """``{"id", "value"}`` records from a checkpoint's ``__interrupt__`` writes."""
    interrupts: list[dict[str, Any]] = []
    for _task_id, channel, value in (cp_tuple.pending_writes or []) if cp_tuple else []:
        if channel != _INTERRUPT_CHANNEL:
            continue
        values = value if isinstance(value, (list, tuple)) else [value]
        for intr in values:
            interrupts.append(
                {
                    "id": getattr(intr, "id", None),
                    "value": getattr(intr, "value", None),
                }
            )
    return interrupts


def _new_summarization_event(start_state: Any, end_state: Any) -> dict[str, Any] | None:
    """The turn's freshly landed ``_summarization_event``, or None.

    Events are identified by their summary message id (uuid-stamped at build);
    an end-state event matching the start state predates this turn.
    """

    def _identity(state: Any) -> tuple[Any, Any] | None:
        event = state.values.get("_summarization_event")
        if not isinstance(event, dict):
            return None
        summary_message = event.get("summary_message")
        return (getattr(summary_message, "id", None), event.get("cutoff_index"))

    end_event = end_state.values.get("_summarization_event")
    if isinstance(end_event, dict) and _identity(end_state) != _identity(start_state):
        return end_event
    return None


def _set_growth(start_state: Any, end_state: Any, key: str) -> int:
    """How many ids ``key``'s set gained between the two states."""
    start = start_state.values.get(key) or set()
    end = end_state.values.get(key) or set()
    return len(set(end) - set(start))


def _new_ui_records(start_state: Any, end_state: Any) -> list[dict[str, Any]]:
    """``ui``-channel records the end state has that the start state lacks."""
    start_ids = {
        r.get("id")
        for r in (start_state.values.get("ui") or [])
        if isinstance(r, dict)
    }
    return [
        r
        for r in (end_state.values.get("ui") or [])
        if isinstance(r, dict) and r.get("id") not in start_ids
    ]


class CheckpointHistoryReader:
    """Read-only materializer for thread + subagent transcripts."""

    _instance: CheckpointHistoryReader | None = None

    def __init__(self, checkpointer: Any):
        _silence_pending_sends_noise()
        self._checkpointer = checkpointer
        # Child graph resolves checkpoint_ns="task:{id}" (recast to node name
        # "task"). It MUST be compiled with the checkpointer: subgraph
        # aget_state materializes DeltaChannel history via the child's OWN
        # `self.checkpointer` (langgraph `_aprepare_state_snapshot` ignores the
        # config-passed one for delta replay), so a plain-compiled child
        # silently returns un-replayed (empty) messages.
        child = (
            StateGraph(_ReaderState)
            .add_node("noop", lambda state: {})
            .add_edge(START, "noop")
            .compile(checkpointer=checkpointer)
        )
        self._graph = (
            StateGraph(_ReaderState)
            .add_node("task", child)
            .add_edge(START, "task")
            .compile(checkpointer=checkpointer)
        )
        # Separate single-node graph for ui-record appends: with exactly one
        # node, aupdate_state auto-attributes the write (no as_node needed),
        # and the update checkpoint carries source="update" — never a turn
        # boundary.
        self._updater = (
            StateGraph(_ReaderState)
            .add_node("noop", lambda state: {})
            .add_edge(START, "noop")
            .compile(checkpointer=checkpointer)
        )

    @classmethod
    def get_instance(cls) -> CheckpointHistoryReader:
        if cls._instance is None:
            from src.server.app import setup

            if not setup.checkpointer:
                raise RuntimeError("Checkpointer not initialized")
            cls._instance = cls(setup.checkpointer)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    async def aget_thread_history(
        self, thread_id: str, branch_tip_checkpoint_id: str | None = None
    ) -> ThreadHistory:
        """Materialize per-turn message slices for the current branch.

        Turn boundaries are ``source=input`` checkpoints plus HITL resume
        points (``__resume__`` pending writes) — each persists its own query
        row, so boundaries stay 1:1 with turns. A boundary checkpoint's state
        is the thread BEFORE its turn applies (the input/resume rides pending
        writes), so turn *i* = id-diff between boundary *i* and boundary *i+1*
        (or the tip). Id-diff, not count-diff, so compaction/REMOVE_ALL is
        safe.
        """
        boundaries, tip_id = await self._current_branch_input_boundaries(
            thread_id, branch_tip_checkpoint_id
        )
        history = ThreadHistory(thread_id=thread_id)
        if not boundaries or tip_id is None:
            return history

        # Materialize each boundary state once, plus the branch tip.
        *boundary_states, tip_state = await self._aget_states_at(
            thread_id, [b.checkpoint_id for b in boundaries] + [tip_id]
        )
        history.turns = self._build_turn_slices(
            boundaries, boundary_states, tip_id, tip_state
        )
        history.interrupts = await self._extract_interrupts(thread_id, tip_id)
        history.ui = list(tip_state.values.get("ui", []) or [])
        return history

    async def aget_recent_history(
        self,
        thread_id: str,
        n_turns: int,
        branch_tip_checkpoint_id: str | None = None,
    ) -> ThreadHistory:
        """Materialize only the last ``n_turns`` turns (windowed replay).

        Same id-diff slicing as ``aget_thread_history``, but materializes just
        the tail boundaries + tip — ``n_turns + 1`` state reads instead of one
        per turn — so initial-load latency is bounded by the window, not by
        thread length. ``turn_ordinal`` stays absolute.
        """
        boundaries, tip_id = await self._current_branch_input_boundaries(
            thread_id, branch_tip_checkpoint_id
        )
        history = ThreadHistory(thread_id=thread_id)
        if not boundaries or tip_id is None:
            return history

        n = max(1, min(n_turns, len(boundaries)))
        kept = boundaries[-n:]
        *kept_states, tip_state = await self._aget_states_at(
            thread_id, [b.checkpoint_id for b in kept] + [tip_id]
        )
        history.turns = self._build_turn_slices(
            kept, kept_states, tip_id, tip_state, ordinal_offset=len(boundaries) - n
        )
        history.interrupts = await self._extract_interrupts(thread_id, tip_id)
        history.ui = list(tip_state.values.get("ui", []) or [])
        return history

    @staticmethod
    def _build_turn_slices(
        boundaries: list[_InputBoundary],
        states: list[Any],
        tip_id: str,
        tip_state: Any,
        ordinal_offset: int = 0,
    ) -> list[TurnSlice]:
        """Id-diff each boundary against the next (or the tip) into a TurnSlice.

        ``boundaries``/``states`` are a contiguous run; the last boundary's end
        is the tip (callers pass the run that ends at the branch tip).
        """
        turns: list[TurnSlice] = []
        for i, boundary in enumerate(boundaries):
            start_msgs: list[AnyMessage] = states[i].values.get("messages", [])
            is_last = i == len(boundaries) - 1
            end_state = tip_state if is_last else states[i + 1]
            end_id = tip_id if is_last else boundaries[i + 1].checkpoint_id
            end_msgs: list[AnyMessage] = end_state.values.get("messages", [])

            start_ids = {m.id for m in start_msgs if m.id is not None}
            slice_msgs = [m for m in end_msgs if m.id not in start_ids]

            user_message = next(
                (m for m in slice_msgs if isinstance(m, HumanMessage)), None
            )
            # A resume checkpoint's metadata belongs to the interrupted run,
            # not the resume turn — don't propagate its run_id/turn_index.
            metadata = {} if boundary.is_resume else (boundary.metadata or {})
            turns.append(
                TurnSlice(
                    turn_ordinal=ordinal_offset + i,
                    input_checkpoint_id=boundary.checkpoint_id,
                    end_checkpoint_id=end_id,
                    user_message=user_message,
                    messages=slice_msgs,
                    run_id=metadata.get("run_id"),
                    turn_index=metadata.get("turn_index"),
                    new_summarization_event=_new_summarization_event(
                        states[i], end_state
                    ),
                    newly_offloaded_args=_set_growth(
                        states[i], end_state, "_offloaded_tool_call_ids"
                    ),
                    newly_offloaded_reads=_set_growth(
                        states[i], end_state, "_offloaded_read_result_ids"
                    ),
                    ending_interrupts=(
                        [] if is_last else boundaries[i + 1].interrupts
                    ),
                    new_ui_records=_new_ui_records(states[i], end_state),
                    tail_checkpoint_id=_tail_of_turn(boundaries, i, tip_id),
                )
            )
        return turns

    async def append_ui_record(
        self, thread_id: str, name: str, props: dict[str, Any]
    ) -> None:
        """Append a ``UIMessage``-shaped record to the thread's ``ui`` channel.

        The id is pre-stamped: ``ui_message_reducer`` upserts by id, so a
        stable unique id keeps re-writes idempotent.

        Skips the write when the thread tip is interrupted: ``aupdate_state``
        attributes the write to this reader graph's node and clears the real
        agent graph's pending interrupt, silently breaking HITL resume. The
        record is only a legacy fallback (new turns carry rewritten image
        paths in the checkpointed message itself), so dropping it on a live
        interrupt is safe.
        """
        tip_interrupted = await self._tip_is_interrupted(thread_id)
        if tip_interrupted is not False:
            logger.debug(
                "[CheckpointHistoryReader] skip ui record %r on interrupted or "
                "unknown tip for thread_id=%s",
                name,
                thread_id,
            )
            return
        record = {
            "type": "ui",
            "id": f"ui-{uuid.uuid4().hex[:12]}",
            "name": name,
            "props": props,
            "metadata": {},
        }
        await self._updater.aupdate_state(
            {"configurable": {"thread_id": thread_id}}, {"ui": [record]}
        )

    async def _tip_is_interrupted(self, thread_id: str) -> bool | None:
        """Interrupt state of the latest checkpoint (None when unreadable).

        Reads the raw tuple rather than ``aget_state().next``: this reader's
        graph nodes differ from the real agent graph, so it can't replan the
        foreign pending task and ``.next`` reads empty. The stored
        ``__interrupt__`` pending write is graph-agnostic. Read failures return
        None so the optional legacy UI write fails closed and cannot clear an
        interrupt whose state could not be verified.
        """
        try:
            tup = await self._checkpointer.aget_tuple(
                {"configurable": {"thread_id": thread_id}}
            )
        except Exception as e:
            logger.warning(
                "[CheckpointHistoryReader] interrupt check failed for "
                "thread_id=%s: %s",
                thread_id,
                e,
            )
            return None
        if tup is None:
            return False
        return any(
            w[1] == _INTERRUPT_CHANNEL for w in (tup.pending_writes or ())
        )

    async def aget_task_history(
        self, thread_id: str, task_id: str
    ) -> TaskHistory:
        """Materialize replay-relevant state from a ``task:{task_id}`` namespace."""
        snapshot = await self._graph.aget_state(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": f"task:{task_id}",
                }
            }
        )
        values = snapshot.values
        summarization_event = values.get("_summarization_event")
        return TaskHistory(
            messages=list(values.get("messages", []) or []),
            new_summarization_event=(
                summarization_event
                if isinstance(summarization_event, dict)
                else None
            ),
            newly_offloaded_args=len(
                set(values.get("_offloaded_tool_call_ids") or ())
            ),
            newly_offloaded_reads=len(
                set(values.get("_offloaded_read_result_ids") or ())
            ),
            new_ui_records=[
                record
                for record in (values.get("ui") or [])
                if isinstance(record, dict)
            ],
        )

    async def aget_task_messages(
        self, thread_id: str, task_id: str
    ) -> list[AnyMessage]:
        """Compatibility wrapper returning only a task namespace's transcript."""
        return (await self.aget_task_history(thread_id, task_id)).messages

    async def aget_task_run_stamps(
        self, thread_id: str, task_id: str
    ) -> list[str | None]:
        """Per-run ``task_run_id`` stamps for a task namespace, oldest-first.

        Each spawn/resume writes a ``source=input`` boundary whose metadata
        carries the execution's ledger identity (stamped at spawn via the
        child-graph config) — stamp k belongs to run segment k of the
        namespace transcript. None entries are pre-ledger runs.
        """
        boundaries, _tip = await walk_current_branch_boundaries(
            self._checkpointer,
            thread_id,
            checkpoint_ns=f"task:{task_id}",
        )
        return [(b.metadata or {}).get("task_run_id") for b in boundaries]

    async def aget_turn_anchors(
        self, thread_id: str, branch_tip_checkpoint_id: str | None = None
    ) -> tuple[list[TurnAnchor], str | None]:
        """Turn identities on the current branch — light walk, no state reads.

        Lets the projection cache pair and key turns without materializing
        any checkpoint state.
        """
        boundaries, tip_id = await self._current_branch_input_boundaries(
            thread_id, branch_tip_checkpoint_id
        )
        if not boundaries or tip_id is None:
            return [], tip_id
        anchors: list[TurnAnchor] = []
        for i, boundary in enumerate(boundaries):
            metadata = {} if boundary.is_resume else (boundary.metadata or {})
            anchors.append(
                TurnAnchor(
                    turn_ordinal=i,
                    input_checkpoint_id=boundary.checkpoint_id,
                    tail_checkpoint_id=_tail_of_turn(boundaries, i, tip_id),
                    turn_index=metadata.get("turn_index"),
                    run_id=metadata.get("run_id"),
                )
            )
        return anchors, tip_id

    async def aget_tip_interrupts(
        self, thread_id: str, tip_checkpoint_id: str
    ) -> list[dict[str, Any]]:
        """Pending (unanswered) interrupts at the branch tip."""
        return await self._extract_interrupts(thread_id, tip_checkpoint_id)

    async def acount_input_boundaries(
        self, thread_id: str, branch_tip_checkpoint_id: str | None = None
    ) -> int:
        """Number of turn boundaries on the current branch (auto-mode coverage)."""
        boundaries, _ = await self._current_branch_input_boundaries(
            thread_id, branch_tip_checkpoint_id
        )
        return len(boundaries)

    async def _aget_state_at(self, thread_id: str, checkpoint_id: str):
        return await self._graph.aget_state(
            {"configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint_id}}
        )

    async def _aget_states_at(
        self, thread_id: str, checkpoint_ids: list[str]
    ) -> list[Any]:
        """Materialize several checkpoint states concurrently, order-preserving."""
        semaphore = asyncio.Semaphore(_STATE_READ_CONCURRENCY)

        async def _one(checkpoint_id: str) -> Any:
            async with semaphore:
                return await self._aget_state_at(thread_id, checkpoint_id)

        return list(await asyncio.gather(*(_one(cid) for cid in checkpoint_ids)))

    async def _current_branch_input_boundaries(
        self, thread_id: str, branch_tip_checkpoint_id: str | None
    ) -> tuple[list[_InputBoundary], str | None]:
        """Turn boundaries on the current branch, as ``_InputBoundary`` records.

        Wraps the canonical ``walk_current_branch_boundaries`` — every boundary
        is a ``source=input`` checkpoint or (by construction of the walk) a HITL
        resume, so ``is_resume`` is simply "not input".
        """
        boundaries, tip_id = await walk_current_branch_boundaries(
            self._checkpointer,
            thread_id,
            branch_tip_checkpoint_id,
            strict_branch_tip=branch_tip_checkpoint_id is not None,
        )
        records = [
            _InputBoundary(
                checkpoint_id=cp.config["configurable"]["checkpoint_id"],
                metadata=dict(cp.metadata or {}),
                is_resume=(cp.metadata or {}).get("source") != "input",
                interrupts=_interrupt_writes(cp),
                parent_checkpoint_id=(
                    (cp.parent_config or {})
                    .get("configurable", {})
                    .get("checkpoint_id")
                ),
            )
            for cp in boundaries
        ]
        return records, tip_id

    async def _extract_interrupts(
        self, thread_id: str, tip_checkpoint_id: str
    ) -> list[dict[str, Any]]:
        """Pending interrupts at the branch tip, from raw checkpoint writes.

        StateSnapshot.tasks can't reconstruct interrupts here — the reader
        graph doesn't contain the real agent node names — but the interrupt
        values themselves sit in the tip's ``__interrupt__`` pending writes.
        """
        cp_tuple = await self._checkpointer.aget_tuple(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": tip_checkpoint_id,
                }
            }
        )
        return _interrupt_writes(cp_tuple)
