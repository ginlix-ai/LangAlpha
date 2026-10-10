"""Checkpoint history reader — materialize thread transcripts from checkpoints.

``messages`` is a ``DeltaChannel`` (see ``ptc_agent.agent.state``), so a raw
``checkpointer.aget_tuple`` cannot materialize it: deltas live in
``checkpoint_writes`` and are only replayed by a compiled graph. This module
compiles a no-op ``StateGraph(MainAgentState)`` against the server checkpointer
purely to read state — in the ``task_namespace_graph`` shape, since background
subagents checkpoint under the parent ``thread_id`` with
``checkpoint_ns="task:{task_id}"``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any

from langgraph.graph import START, StateGraph

from ptc_agent.agent.main_state import MainAgentState
from src.server.services.history import slices
from src.server.utils.checkpoint_helpers import (
    INTERRUPT_CHANNEL,
    Boundary,
    interrupt_records,
    update_at_commit,
    walk_current_branch_boundaries,
)

logger = logging.getLogger(__name__)


def _has_pending_interrupt(tup: Any) -> bool:
    return tup is not None and any(
        w[1] == INTERRUPT_CHANNEL for w in (tup.pending_writes or ())
    )

# Concurrent aget_state reads per history materialization. Each read holds a
# checkpointer-pool connection (pool max defaults to 25), so this stays well
# below the pool to keep long-thread replays from starving live runs.
_STATE_READ_CONCURRENCY = 8


def _silence_pending_sends_noise() -> None:
    """Suppress langgraph's 'unknown node name … in pending sends' warnings.

    The reader graph is a no-op shell, so historical checkpoints referencing
    real agent node names in pending sends trigger this warning on every
    materialization. Live graphs contain all their nodes, so the warning never
    fires for them; filtering the message on the emitting logger is safe.
    """
    # The warning goes out on pregel's shared logger, not one named for its
    # module; a filter on "langgraph.pregel._algo" never saw a record.
    try:
        from langgraph.pregel._log import logger as algo_logger
    except ImportError:
        algo_logger = logging.getLogger("langgraph")
    marker = "in pending sends"
    if any(getattr(f, "_history_reader_filter", False) for f in algo_logger.filters):
        return

    def _filter(record: logging.LogRecord) -> bool:
        return marker not in record.getMessage()

    _filter._history_reader_filter = True  # type: ignore[attr-defined]
    algo_logger.addFilter(_filter)


@dataclass
class TaskHistory(slices.SpanDelta):
    """A background task's whole namespace: what it added from nothing."""

    #: The namespace checkpoint these were read at.
    checkpoint_id: str | None = None


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
    # Where the turn's message diff ends: the next boundary, or the tip for
    # the last turn. A boundary's state is the thread before its turn
    # applies, so the next boundary's state is this turn's result.
    end_checkpoint_id: str | None = None
    # Answered interrupts this turn raised, from the next resume boundary.
    # Pending ones at the branch tip come from ``aget_tip_interrupts``.
    ending_interrupts: list[dict[str, Any]] = field(default_factory=list)
    # A HITL resume opens on the interrupt's answer, never a message of its
    # own: it carries on the turn before it.
    is_resume: bool = False
    # The state before the turn, where an edit of its message forks; a
    # resume has no message of its own to edit.
    parent_checkpoint_id: str | None = None


def pair_turns(anchors: list[TurnAnchor], persisted: list[int]) -> list[int | None]:
    """Each turn's persisted turn_index, given the thread's persisted ones
    ascending.

    A turn's input checkpoint carries the turn_index its run was admitted
    under. A resume's checkpoint belongs to the run it answers, and threads
    from before the stamp carry none, so those take the next persisted turn
    after the one before: a turn whose run died before its first checkpoint
    has rows and no boundary, so position alone would name every later turn
    one too low. None when no persisted turn is left to name.
    """
    numbers: list[int | None] = []
    previous: int | None = None
    for anchor in anchors:
        number = anchor.turn_index
        if number is None:
            at = 0 if previous is None else bisect_right(persisted, previous)
            number = persisted[at] if at < len(persisted) else None
        numbers.append(number)
        if number is not None:
            previous = number
    return numbers


@dataclass
class TaskRun:
    """One run of a background task: the span its namespace wrote between
    its own input boundary and the next run's (or the namespace tip)."""

    task_id: str
    ordinal: int
    input_checkpoint_id: str
    end_checkpoint_id: str
    # The run's own last checkpoint, which validates a stored run slice.
    tail_checkpoint_id: str
    # The ledger's run id stamped on the boundary; None before the ledger.
    task_run_id: str | None = None


def _tail_of_turn(boundaries: list[Boundary], i: int, tip_id: str) -> str | None:
    """Turn *i*'s last checkpoint: the tip for the last turn; otherwise the
    next boundary's parent — except a resume boundary IS the interrupted
    turn's tip (interrupt and resume writes ride the same checkpoint)."""
    if i == len(boundaries) - 1:
        return tip_id
    nxt = boundaries[i + 1]
    return nxt.parent_checkpoint_id if nxt.is_input else nxt.checkpoint_id


def turn_anchors(boundaries: list[Boundary], tip_id: str) -> list[TurnAnchor]:
    """The branch's turns, one per boundary of its walk."""
    anchors: list[TurnAnchor] = []
    for i, boundary in enumerate(boundaries):
        is_last = i == len(boundaries) - 1
        # A resume checkpoint's metadata belongs to the interrupted run,
        # not the resume turn — don't propagate its run_id/turn_index.
        metadata = boundary.metadata if boundary.is_input else {}
        anchors.append(
            TurnAnchor(
                turn_ordinal=i,
                input_checkpoint_id=boundary.checkpoint_id,
                tail_checkpoint_id=_tail_of_turn(boundaries, i, tip_id),
                turn_index=metadata.get("turn_index"),
                run_id=metadata.get("run_id"),
                end_checkpoint_id=(
                    tip_id if is_last else boundaries[i + 1].checkpoint_id
                ),
                ending_interrupts=[] if is_last else boundaries[i + 1].interrupts,
                is_resume=not boundary.is_input,
                parent_checkpoint_id=boundary.parent_checkpoint_id,
            )
        )
    return anchors


class CheckpointHistoryReader:
    """Read-only materializer for thread + subagent transcripts."""

    _instance: CheckpointHistoryReader | None = None

    def __init__(self, checkpointer: Any):
        # Imported here so this module never sits on the agent middleware
        # package's order-sensitive import graph; the reader is built once.
        from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
            task_namespace_graph,
        )

        _silence_pending_sends_noise()
        self._checkpointer = checkpointer
        # The main agent's own schema, for two reasons: ``aget_state`` only
        # surfaces channels the reading graph declares, and the ui-record
        # append writes through this graph, which would erase any primitive
        # field it left undeclared (see ``main_state``). Same shape the
        # snapshot writer uses, so what it wrote into task:{id} is what this
        # reads back. Public: checkpoint storage maintenance decodes channels
        # through the same declaration.
        self.graph = task_namespace_graph(MainAgentState, checkpointer)
        # Separate single-node graph for ui-record appends: with exactly one
        # node, aupdate_state auto-attributes the write (no as_node needed),
        # and the update checkpoint carries source="update" — never a turn
        # boundary.
        self._updater = (
            StateGraph(MainAgentState)
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

    @property
    def serde(self) -> Any:
        """The checkpointer's serializer, which stored slices share."""
        return self._checkpointer.serde

    async def aget_turn_slices(
        self, thread_id: str, anchors: list[TurnAnchor]
    ) -> list[slices.TurnSlice]:
        """Materialize only these turns: each reads its boundary and the
        state its diff ends at, shared between adjacent turns, so the cost
        follows the turns asked for rather than the thread's length."""
        ids = list(
            dict.fromkeys(
                cid
                for a in anchors
                for cid in (a.input_checkpoint_id, a.end_checkpoint_id)
                if cid
            )
        )
        states = dict(zip(ids, await self._aget_states_at(thread_id, ids)))
        return [
            slices.turn_slice(
                a,
                states[a.input_checkpoint_id].values,
                states[a.end_checkpoint_id].values,
            )
            for a in anchors
        ]

    async def aget_task_runs(self, thread_id: str, task_id: str) -> list[TaskRun]:
        """A task namespace's runs, oldest first, from a light walk.

        Each spawn or resume opens a run at a ``source=input`` boundary
        stamped with its ledger run id; any other boundary resumes the run
        before it.
        """
        boundaries, tip_id = await walk_current_branch_boundaries(
            self._checkpointer, thread_id, checkpoint_ns=f"task:{task_id}"
        )
        if not boundaries or tip_id is None:
            return []
        starts = [b for i, b in enumerate(boundaries) if i == 0 or b.is_input]
        runs: list[TaskRun] = []
        for k, start in enumerate(starts):
            nxt = starts[k + 1] if k + 1 < len(starts) else None
            stamp = start.metadata.get("task_run_id")
            runs.append(
                TaskRun(
                    task_id=task_id,
                    ordinal=k,
                    input_checkpoint_id=start.checkpoint_id,
                    end_checkpoint_id=nxt.checkpoint_id if nxt else tip_id,
                    tail_checkpoint_id=(
                        (nxt.parent_checkpoint_id or nxt.checkpoint_id)
                        if nxt
                        else tip_id
                    ),
                    task_run_id=str(stamp) if stamp else None,
                )
            )
        return runs

    async def aget_run_slices(
        self, thread_id: str, task_id: str, runs: list[TaskRun]
    ) -> list[slices.SpanDelta]:
        """What each run added to its namespace, two state reads per run,
        shared between adjacent runs. Diffing each run's own boundaries keeps
        early runs whole after the namespace compacts them away."""
        ids = list(
            dict.fromkeys(
                cid for r in runs for cid in (r.input_checkpoint_id, r.end_checkpoint_id)
            )
        )
        semaphore = asyncio.Semaphore(_STATE_READ_CONCURRENCY)

        async def _one(checkpoint_id: str) -> Any:
            async with semaphore:
                return await self.graph.aget_state(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": f"task:{task_id}",
                            "checkpoint_id": checkpoint_id,
                        }
                    }
                )

        states = dict(zip(ids, await asyncio.gather(*(_one(c) for c in ids))))
        return [
            slices.delta(
                states[r.input_checkpoint_id].values, states[r.end_checkpoint_id].values
            )
            for r in runs
        ]

    async def append_ui_record(
        self, thread_id: str, name: str, props: dict[str, Any]
    ) -> None:
        """Append a ``UIMessage``-shaped record to the thread's root ``ui`` channel.

        Skips the write when the thread tip is interrupted: ``aupdate_state``
        attributes the write to this reader graph's node and clears the real
        agent graph's pending interrupt, silently breaking HITL resume. The
        record is only a legacy fallback (new turns carry rewritten image
        paths in the checkpointed message itself), so dropping it on a live
        interrupt is safe.

        The image-capture hook runs after the turn finalized, so the append
        sits beyond the recorded branch tip where the replay walk cannot see
        it: it is written at the tip read here and the recorded tip
        CAS-advanced onto it (``update_at_commit``). A concurrent turn or
        branch switch that moved the tip first leaves a dead-branch record,
        and a thread with no checkpoint gets none.
        """
        try:
            tip = await self._checkpointer.aget_tuple(
                {"configurable": {"thread_id": thread_id}}
            )
        except Exception as e:
            # Fail closed: an unverifiable tip could hide a live interrupt.
            logger.warning(
                "[CheckpointHistoryReader] tip read failed, dropping ui "
                "record %r for thread_id=%s: %s",
                name,
                thread_id,
                e,
            )
            return
        if _has_pending_interrupt(tip):
            logger.debug(
                "[CheckpointHistoryReader] skip ui record %r on interrupted "
                "tip for thread_id=%s",
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
        built_on = (
            (tip.config.get("configurable") or {}).get("checkpoint_id") if tip else None
        )
        await update_at_commit(self._updater, thread_id, built_on, {"ui": [record]})

    async def aget_state(self, thread_id: str, checkpoint_id: str | None = None):
        """The thread's state at ``checkpoint_id``, or at its latest checkpoint."""
        configurable = {"thread_id": thread_id}
        if checkpoint_id:
            configurable["checkpoint_id"] = checkpoint_id
        return await self.graph.aget_state({"configurable": configurable})

    async def aget_task_history(
        self, thread_id: str, task_id: str
    ) -> TaskHistory:
        """Materialize replay-relevant state from a ``task:{task_id}`` namespace."""
        snapshot = await self.graph.aget_state(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": f"task:{task_id}",
                }
            }
        )
        return TaskHistory(
            checkpoint_id=(snapshot.config or {})
            .get("configurable", {})
            .get("checkpoint_id"),
            **vars(slices.delta({}, snapshot.values)),
        )

    async def alatest_checkpoint_id(
        self, thread_id: str, checkpoint_ns: str = ""
    ) -> str | None:
        """The newest checkpoint written in ``checkpoint_ns``, without
        materializing its state: for a task, the one ``aget_task_history``
        would read at now; for the thread, past its stamp while a turn runs."""
        tip = await self._checkpointer.aget_tuple(
            {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}
        )
        if tip is None:
            return None
        return (tip.config.get("configurable") or {}).get("checkpoint_id")

    async def aget_turn_anchors(
        self, thread_id: str, branch_tip_checkpoint_id: str | None = None
    ) -> tuple[list[TurnAnchor], str | None]:
        """Turn identities on the current branch — light walk, no state reads."""
        boundaries, tip_id = await walk_current_branch_boundaries(
            self._checkpointer,
            thread_id,
            branch_tip_checkpoint_id,
            strict_branch_tip=branch_tip_checkpoint_id is not None,
        )
        if not boundaries or tip_id is None:
            return [], tip_id
        return turn_anchors(boundaries, tip_id), tip_id

    async def aget_tip_interrupts(
        self, thread_id: str, tip_checkpoint_id: str
    ) -> list[dict[str, Any]]:
        """Pending (unanswered) interrupts at the branch tip."""
        return await self._extract_interrupts(thread_id, tip_checkpoint_id)

    async def _aget_state_at(self, thread_id: str, checkpoint_id: str):
        return await self.graph.aget_state(
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
        return interrupt_records(cp_tuple.pending_writes if cp_tuple else None)
