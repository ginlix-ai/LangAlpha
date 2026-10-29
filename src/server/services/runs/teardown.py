"""Stop-teardown pipeline for a user-stopped run.

Owns the deterministic sequence a user stop triggers: flush the LangGraph
checkpoint, cancel the stopped run's orphan collectors, kill and claim its
subagents, and drain their captured events for the finalize to record.
Executor-local state (the task table, the orphan-collector registry) stays
in ``LocalRunExecutor`` and is lent in via arguments.
"""

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from src.config.settings import (
    get_checkpoint_flush_timeout,
    get_stop_drain_timeout,
)
from src.server.services.runs.subagent_archive import (
    SubagentArchiveReadError,
    iter_subagent_events_full,
    record_to_persist_event,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StopTeardown:
    """What a stop teardown hands its run's finalize, filled in place.

    ``events`` are the killed subagents' drained events, whose provenance the
    finalize records if the run ends cancelled. ``claimed`` are the settled subagents the kill claimed
    before evicting them with their usage: the teardown runs before anyone
    knows whether the user or the system ended the run, and only the finalize
    decides whether that usage is billed. The run holds this before the
    teardown starts, because a second cancel can start the finalize while the
    kill is still unwinding, and the claim must still reach the bill.
    """

    events: list[dict] = field(default_factory=list)
    claimed: list = field(default_factory=list)


async def flush_checkpoint(graph: Any, thread_id: str) -> None:
    """Force a checkpoint write for the current thread state on user stop.

    Persists state up to the last completed step so the next message
    resumes from it. The in-flight step is discarded and re-run on resume.
    """
    config = {"configurable": {"thread_id": thread_id}}

    try:
        graph_any: Any = graph

        snapshot = await asyncio.wait_for(
            graph_any.aget_state(config), timeout=get_checkpoint_flush_timeout()
        )
        values = getattr(snapshot, "values", None)
        if not values:
            return

        # Exclude `messages` from the re-write. The committed messages are
        # already in this snapshot and carry forward on the DeltaChannel, so
        # re-writing the full list only re-applies every message as a delta —
        # and any still-id-less tail message appends as a duplicate (the
        # reducer keys dedup on id). The remaining keys (private compaction /
        # offload state) are last-write-wins, so re-writing them is idempotent.
        flush_values = {k: v for k, v in values.items() if k != "messages"}
        if not flush_values:
            return

        await asyncio.wait_for(
            graph_any.aupdate_state(config, flush_values),
            timeout=get_checkpoint_flush_timeout(),
        )
        logger.info(f"[StopTeardown] Flushed checkpoint for {thread_id}")
    except asyncio.TimeoutError:
        logger.warning(
            f"[StopTeardown] Checkpoint flush timed out for {thread_id}"
        )
    except Exception as e:
        logger.warning(
            f"[StopTeardown] Failed to flush checkpoint for {thread_id}: {e}"
        )


async def teardown_subagents_on_stop(
    thread_id: str,
    run_id: str,
    stop: StopTeardown,
    *,
    orphan_collectors: dict[asyncio.Task, str],
    drain: Callable[[str, list], Awaitable[list[dict]]],
) -> None:
    """Single-owner subagent teardown on a stop, scoped to the stopped run.

    Order (decision 1A, amended): list this run's tasks, cancel this run's
    orphan collectors, kill and claim its subagents
    (``cancel_run_tasks(force, claim_for=run_id)``), then drain the killed
    subagents' events (bounded). The kill MUST precede the drain: the
    drain's high-water is read at drain start, so a pre-kill snapshot misses
    frames the task emits between snapshot and kill, which the live stream
    already delivered. The task list is snapshotted first because the kill
    drops the registry entries; the drain reads only the held task objects
    and their Redis streams. Everything is keyed by ``spawned_run_id``: a
    prior turn's orphan collector persists to ITS OWN response, so stopping
    the current run must neither kill it nor archive its tasks' events here.
    """
    from src.server.services.background_registry_store import BackgroundRegistryStore

    registry_store = BackgroundRegistryStore.get_instance()
    registry = await registry_store.get_registry(thread_id)

    # --- 2. Snapshot this run's task objects before the kill drops them
    # from the registry. ---
    tasks: list = []
    if registry is not None:
        try:
            tasks = [
                t
                for t in await registry.get_all_tasks()
                if getattr(t, "spawned_run_id", None) == run_id
            ]
        except Exception:
            tasks = []

    # --- 3. Cancel THIS run's orphan collectors (normally none: a stopped
    # run never reached collection) so they can't mutate the response.
    # Prior turns' collectors keep running — they own other responses. ---
    collectors = [t for t, owner in orphan_collectors.items() if owner == run_id]
    for collector in collectors:
        if not collector.done():
            collector.cancel()
    if collectors:
        with suppress(Exception):
            await asyncio.gather(*collectors, return_exceptions=True)
        for collector in collectors:
            orphan_collectors.pop(collector, None)

    # --- 4. Kill and claim this run's subagents; the registry and prior-turn
    # tasks survive for their own collectors. cancel_run_tasks awaits the
    # unwinding writers (bounded), so the captured streams are final
    # before the drain reads its high-water. The claim is published with no
    # await after the kill's lock section, so a later kill that reads it once
    # it returns finds each task either here or in its own claim. ---
    with suppress(Exception):
        killed = await registry_store.cancel_run_tasks(
            thread_id, run_id, force=True, claim_for=run_id
        )
        stop.claimed.extend(killed.claimed)

    # --- 5. Drain killed-subagent events (best-effort, hard timeout) ---
    drain_timeout = get_stop_drain_timeout()
    if tasks:
        try:
            stop.events.extend(
                await asyncio.wait_for(
                    drain(thread_id, tasks),
                    timeout=drain_timeout,
                )
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"[StopTeardown] Subagent drain exceeded "
                f"{drain_timeout}s for thread_id={thread_id}; "
                "proceeding without drained events"
            )
        except Exception as exc:
            logger.warning(
                f"[StopTeardown] Subagent drain failed for "
                f"thread_id={thread_id}: {exc}"
            )


async def drain_killed_subagent_events(
    thread_id: str, tasks: list
) -> list[dict]:
    """Best-effort bounded snapshot of a killed run's subagent events.

    Runs AFTER the kill (ordering at the teardown call site) so the
    high-water read at drain start covers everything the tasks ever
    emitted; the passed task objects are held by the caller — the registry
    entries are already gone. The caller bounds it with
    ``asyncio.wait_for``. Starts no new agent work — it only reads and
    closes. A lane withheld here is archived after the finalize, once its
    writers settle (``subagent_collection.end_run_subagents``).
    """
    merged: list[dict] = []
    for task in tasks:
        merged.extend(await killed_lane(thread_id, task))
    return merged


async def killed_lane(
    thread_id: str, task, *, owner: str | None = None
) -> list[dict]:
    """A killed subagent's captured events, or ``[]``.

    Reads the task's Redis capture stream via ``iter_subagent_events_full``.
    ``owner`` fences the read against a resume the way the collector's replay
    is fenced: a task whose claim leaves ``owner`` mid-read belongs to the
    resuming run's archive.
    """
    if owner is not None and task.collector_response_id != owner:
        return []
    expected = getattr(task, "captured_event_count", 0)
    if expected <= 0:
        return []
    # A writer still unwinding can append past this snapshot: the terminal
    # steering sweep runs in the writer's teardown and is exempt from the
    # seal, so only a settled writer guarantees the count is final.
    # Withhold the task otherwise — its stream keeps the terminal-retention
    # TTL and the lane rebuilds per read until it is archived.
    if task.live_writers:
        logger.warning(
            f"[StopTeardown] Writers still unwinding for task "
            f"{getattr(task, 'task_id', '?')}; withholding snapshot"
        )
        return []
    task_events: list[dict] = []
    run_id = getattr(task, "spawned_run_id", None)
    # Always the stamped arm in practice: both callers take only
    # spawned_run_id == run_id tasks and run_id is a non-empty uuid, so
    # agreement with the collector's epoch rule comes from that upstream
    # filter, not from re-deriving the rule here. The collector's
    # unstamped arm serves cross-worker hydrated shells — a population
    # the filter excludes; the (None,) arm is defensive only.
    allowed = (run_id,) if run_id else (None,)
    eligible = 0
    try:
        async for record in iter_subagent_events_full(thread_id, task):
            if owner is not None and task.collector_response_id != owner:
                return []  # resumed mid-read: the resume owns the archive
            if record.get("run") not in allowed:
                continue  # another round's record (cross-worker resume)
            eligible += 1
            task_events.append(record_to_persist_event(record, thread_id))
    except SubagentArchiveReadError as exc:
        logger.warning(
            f"[StopTeardown] Archive read failed for task "
            f"{getattr(task, 'task_id', '?')}; withholding snapshot: {exc}"
        )
        return []
    if eligible != expected:
        # Mismatch against this round's attempted appends (XRANGE
        # failure reads as zero rows; a torn spill leaves a prefix;
        # foreign-epoch records never count): append none of it. A
        # partial snapshot would record part of the lane as all of it;
        # the capture stream keeps its terminal-retention TTL as the last
        # complete-able copy.
        logger.warning(
            f"[StopTeardown] Incomplete recovery for task "
            f"{getattr(task, 'task_id', '?')} (recovered={eligible}, "
            f"expected={expected}); withholding snapshot"
        )
        return []
    return task_events
