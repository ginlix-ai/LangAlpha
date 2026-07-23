"""Stop-teardown pipeline for a user-stopped run.

Owns the deterministic sequence a user stop triggers: flush the LangGraph
checkpoint, cancel the stopped run's orphan collectors, kill its subagents,
and drain their captured events for the finalize to persist. Executor-local
state (the task table, the orphan-collector registry) stays in
``LocalRunExecutor`` and is lent in via arguments.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Any, Awaitable, Callable

from src.config.settings import (
    get_checkpoint_flush_timeout,
    get_stop_drain_timeout,
)
from src.server.services.runs import subagent_collection

logger = logging.getLogger(__name__)


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
    *,
    orphan_collectors: dict[asyncio.Task, str],
    drain: Callable[[str, list], Awaitable[list[dict]]],
) -> list[dict]:
    """Single-owner subagent teardown on a user stop — scoped to the
    stopped run.

    Order (decision 1A, amended): list this run's tasks → cancel this
    run's orphan collectors → cancel_run_tasks(force) → drain killed-
    subagent events (bounded) → return the merged events for the caller
    to stash for the finalize to persist. The kill MUST precede the
    drain: the drain's high-water is read at drain start, so a pre-kill
    snapshot misses frames the task emits between snapshot and kill —
    output the live stream already delivered. The task list is
    snapshotted first because cancel_run_tasks drops the registry
    entries; the drain reads only the held task objects and their Redis
    streams. Everything is keyed by ``spawned_run_id``: a prior turn's
    orphan collector persists to ITS OWN response, so stopping the
    current run must neither kill it nor archive its tasks' events here.
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

    # --- 4. Kill this run's subagents; the registry and prior-turn tasks
    # survive for their own collectors. cancel_run_tasks awaits the
    # unwinding writers (bounded), so the captured streams are final
    # before the drain reads its high-water. ---
    with suppress(Exception):
        await registry_store.cancel_run_tasks(thread_id, run_id, force=True)

    # --- 5. Drain killed-subagent events (best-effort, hard timeout) ---
    merged_subagent_events: list[dict] = []
    drain_timeout = get_stop_drain_timeout()
    if tasks:
        try:
            merged_subagent_events = await asyncio.wait_for(
                drain(thread_id, tasks),
                timeout=drain_timeout,
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

    return merged_subagent_events


async def drain_killed_subagent_events(
    thread_id: str, tasks: list
) -> list[dict]:
    """Best-effort bounded snapshot of a killed run's subagent events.

    Reads each task's Redis capture stream via ``iter_subagent_events_full``
    and appends a synthetic "stopped" close per task. Runs AFTER the kill
    (ordering at the teardown call site) so the high-water read at drain
    start covers everything the tasks ever emitted; the passed task
    objects are held by the caller — the registry entries are already
    gone. The caller bounds it with ``asyncio.wait_for``. Starts no new
    agent work — it only reads and closes.
    """
    merged: list[dict] = []
    for task in tasks:
        expected = getattr(task, "captured_event_count", 0)
        if expected <= 0:
            continue
        # A writer still unwinding can append past this drain's snapshot:
        # the terminal steering sweep runs in the writer's teardown and
        # is exempt from the seal, so only a settled writer guarantees
        # the count is final. Withhold the task otherwise — its stream
        # keeps the terminal-retention TTL and the lane rebuilds per
        # read; the guard drain is the backstop for a writer that never
        # dies.
        if any(
            w is not None and not w.done()
            for w in (
                getattr(task, "asyncio_task", None),
                getattr(task, "handler_task", None),
            )
        ):
            logger.warning(
                f"[StopTeardown] Writers still unwinding for task "
                f"{getattr(task, 'task_id', '?')}; withholding snapshot"
            )
            continue
        # Track reasoning blocks left open at the kill point so we can close
        # them, mirroring the main agent's finalize_stopped_events. Keyed by
        # the subagent's own (agent, message id) so the synthetic close
        # matches the unpaired start exactly.
        open_reasoning: dict[tuple[str, str], None] = {}
        task_events: list[dict] = []
        run_id = getattr(task, "spawned_run_id", None)
        # Same epoch rule as the collector: a run-stamped task's records
        # are always stamped, so unstamped ones are a foreign pre-stamp
        # writer's — never this round's.
        allowed = (run_id,) if run_id else (None,)
        eligible = 0
        async for record in subagent_collection.iter_subagent_events_full(
            thread_id, task
        ):
            if record.get("run") not in allowed:
                continue  # another round's record (cross-worker resume)
            eligible += 1
            enriched = subagent_collection.record_to_persist_event(
                record, thread_id
            )
            task_events.append(enriched)
            data = enriched.get("data") or {}
            if data.get("content_type") == "reasoning_signal":
                rk = (data.get("agent", ""), data.get("id", ""))
                if data.get("content") == "start":
                    open_reasoning[rk] = None
                elif data.get("content") == "complete":
                    open_reasoning.pop(rk, None)
        if eligible != expected:
            # Mismatch against this round's attempted appends (XRANGE
            # failure reads as zero rows; a torn spill leaves a prefix;
            # foreign-epoch records never count): append none of it. A
            # transcript-class row is the replay cache gate's archive
            # evidence — a partial snapshot would clear the gate and
            # cache the loss. The lane stays uncacheable
            # (rebuild-per-read) and the capture stream keeps its
            # terminal-retention TTL as the last complete-able copy.
            logger.warning(
                f"[StopTeardown] Incomplete recovery for task "
                f"{getattr(task, 'task_id', '?')} (recovered={eligible}, "
                f"expected={expected}); withholding snapshot"
            )
            continue
        merged.extend(task_events)
        # Close any reasoning block still open when the subagent was killed,
        # else replay renders the card stuck "thinking" indefinitely.
        for r_agent, r_id in open_reasoning:
            merged.append(
                {
                    "event": "message_chunk",
                    "data": {
                        "thread_id": thread_id,
                        "agent": r_agent,
                        "id": r_id,
                        "role": "assistant",
                        "content": "complete",
                        "content_type": "reasoning_signal",
                    },
                }
            )
        # Mark the killed subagent's stream "stopped" for replay.
        agent_id = f"task:{getattr(task, 'task_id', '')}"
        merged.append(
            {
                "event": "message_chunk",
                "data": {
                    "thread_id": thread_id,
                    "agent": agent_id,
                    "id": f"{agent_id}:stopped",
                    "role": "assistant",
                    "finish_reason": "stopped",
                },
            }
        )
    return merged
