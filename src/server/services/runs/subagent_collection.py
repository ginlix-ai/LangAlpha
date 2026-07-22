"""Post-terminal subagent collection for root workflow runs.

Owns the read/retire side of the per-task capture streams: replaying a
settled subagent's events into the turn archive, billing its usage, and
retiring its Redis keys — all fenced on the collector claim
(``collector_response_id``), because a resume can steal a task back at any
await boundary. Process-local executor state (the orphan-collector registry,
the task table) stays in ``LocalRunExecutor`` and is lent in through
narrow callables.
"""

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Callable, Optional

from src.config.settings import (
    get_sse_drain_timeout,
    get_subagent_collector_timeout,
    get_subagent_orphan_collector_timeout,
)
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


async def iter_subagent_events_full(
    thread_id: str, task
) -> AsyncIterator[dict]:
    """Yield every captured record for a subagent in seq order."""
    if task is None or not thread_id:
        return

    high_water = int(getattr(task, "captured_event_seq", 0) or 0)
    if high_water <= 0:
        return

    try:
        cache = get_cache_client()
    except Exception as exc:
        logger.warning(
            "[SubagentCollector] Failed to obtain cache client for "
            f"task {getattr(task, 'task_id', '?')}: {exc}"
        )
        return
    if cache is None or not getattr(cache, "enabled", False) or cache.client is None:
        return

    sa_stream_key = f"subagent:stream:{thread_id}:{task.task_id}"
    try:
        entries = await cache.client.xrange(sa_stream_key, min="-", max="+")
    except Exception as exc:
        logger.warning(
            f"[SubagentCollector] XRANGE failed for {sa_stream_key}: {exc}"
        )
        return

    yielded = 0
    for entry_id, fields in entries or []:
        try:
            seq_part = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id
            seq = int(seq_part.split("-", 1)[0])
        except (ValueError, AttributeError):
            continue
        if seq <= 0 or seq > high_water:
            continue
        raw = fields.get(b"record")
        if raw is None:
            continue
        try:
            payload = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            record = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        yielded += 1
        yield record

    expected = high_water
    if yielded < expected:
        logger.warning(
            "subagent_history_truncated",
            extra={
                "thread_id": thread_id,
                "task_id": getattr(task, "task_id", None),
                "expected": expected,
                "recovered": yielded,
                "missing": expected - yielded,
                "redis_write_failed": bool(getattr(task, "redis_write_failed", False)),
            },
        )


def record_to_persist_event(record: dict, thread_id: str) -> dict:
    """Convert a captured-event record to persistence shape ``{event, data}``."""
    data = dict(record.get("data") or {})
    data["thread_id"] = thread_id
    out: dict = {
        "event": record.get("event"),
        "data": data,
    }
    ts = record.get("ts")
    if ts is not None:
        out["ts"] = ts
    return out


# Settled task streams are RETAINED for a bounded window instead of
# deleted: consumer counts are process-local, so a mux/SSE reader on
# another worker is invisible to the drain-wait above — a delete here
# would cut its backlog mid-drain. The stream ends in the producer's
# sentinel, so late attachers still close promptly. Resume's reset
# (_reset_task_for_resume) still hard-DELETEs — that delete IS the
# epoch bump and must not linger.
TASK_STREAM_RETENTION_SECONDS = 900

# Deletes the task's captured-events key only while the durable meta hash
# still names the caller's run as the writer's owner. The local claim and
# spill lock are process-local, so a stale collector on ANOTHER worker
# could otherwise delete state a cross-worker resume has already reset and
# refilled; the resume restamps ``spawned_run_id`` in the meta (under the
# namespace lock, before its writer spawns), so this owner check refuses
# every post-restamp delete. Missing hash or empty owner (legacy/unfenced
# writers) allows the delete.
_RETIRE_TASK_KEYS_IF_OWNED_LUA = """
local owner = redis.call('HGET', KEYS[1], 'spawned_run_id')
if owner == false or owner == '' or owner == ARGV[1] then
    return redis.call('DEL', KEYS[2])
end
return -1
"""


async def delete_task_keys_if_owned(
    cache,
    thread_id: str,
    task_id: str,
    response_id: str,
    task_run_id: Optional[str] = None,
) -> None:
    if not getattr(cache, "enabled", False) or cache.client is None:
        return
    try:
        await cache.client.eval(
            _RETIRE_TASK_KEYS_IF_OWNED_LUA,
            2,
            f"subagent:meta:{thread_id}:{task_id}",
            f"subagent:events:{thread_id}:{task_id}",
            response_id,
        )
        if task_run_id:
            # Run-scoped retire: the archive owns the transcript now, so
            # the collected run's v2 stream drops from attach-grace to
            # the retention window. Keyed by THAT run's id, it can never
            # touch a successor's stream — the stale-collector hazard the
            # v1 keys need the Lua ownership guard for doesn't exist.
            await cache.client.expire(
                f"subagent:stream:{thread_id}:{task_run_id}",
                TASK_STREAM_RETENTION_SECONDS,
            )
    except Exception:
        logger.warning(
            f"[SubagentCleanup] owned-retire failed for "
            f"thread_id={thread_id} task_id={task_id}",
            exc_info=True,
        )


async def replay_owned_task_events(
    thread_id: str, task, response_id: str, out: list[dict]
) -> bool:
    """Append a task's captured events, fenced against a mid-replay steal.

    Same-process: the claim is re-checked per yielded record — the XRANGE
    await inside the iterator is a steal window, and the reclaim strictly
    precedes any round-2 write, so the yield-time check always catches
    stolen records. Cross-worker: the claim is process-local, so each
    record's ``run`` stamp (the writer's spawned_run_id at capture time)
    is the durable fence — a resumed round's records carry the resuming
    run's id and are dropped here even when the stale collector's local
    claim still looks intact. Unstamped records (pre-stamp writers) pass.

    Returns True only when the stream yielded exactly this round's
    attempted appends (``captured_event_count`` at entry). Only records
    passing the run filter count — a cross-worker resume resets the
    shared stream and writes its own records, which must not pad the
    tally for a round whose capture is gone. On any mismatch — XRANGE
    failure reads as zero rows, a torn spill leaves a prefix, a late
    terminal append lands past the entry snapshot — or a mid-replay
    steal, nothing is appended: a partial archive would clear the replay
    cache gate and freeze an incomplete transcript, and the caller must
    not retire the streams it would have been rebuilt from.
    """
    expected = getattr(task, "captured_event_count", 0)
    # Unstamped records are acceptable only from a task whose own writer
    # predates run stamping (no spawned_run_id): a modern task's run id
    # is set at registration — before any append — so every one of its
    # records is stamped, and an unstamped record on its stream can only
    # be a foreign pre-stamp writer's (rolling-deploy resume): epoch
    # unknowable, never archive it.
    allowed = (
        (None, response_id)
        if not getattr(task, "spawned_run_id", None)
        else (response_id,)
    )
    buffered: list[dict] = []
    eligible = 0
    async for record in iter_subagent_events_full(thread_id, task):
        if task.collector_response_id != response_id:
            return False  # stolen mid-replay: the resume owns the archive
        if record.get("run") not in allowed:
            continue  # another round's record (cross-worker resume)
        eligible += 1
        buffered.append(record_to_persist_event(record, thread_id))
    if eligible != expected:
        logger.error(
            f"[SubagentCollector] Incomplete stream recovery for task "
            f"{getattr(task, 'task_id', '?')} (recovered={eligible}, "
            f"expected={expected}); withholding partial archive"
        )
        return False
    out.extend(buffered)
    return True


# -- shared collector mechanics -----------------------------------------
# Both collectors (turn + orphan continuation) run the same fence-checked
# machinery over a different waiting policy: the turn collector waits a
# fixed deadline and hands leftovers to an orphan continuation; the orphan
# collector waits on idle-progress and releases leftovers' claims. Every
# helper re-checks the collector claim because a resume can steal a task
# back at any await boundary.


def _mark_settled(task, writer: asyncio.Task) -> None:
    """Adopt a done writer's result onto its (already fence-checked) task."""
    if task.completed:
        return
    task.completed = True
    try:
        task.result = writer.result()
    except Exception as e:
        task.error = str(e)
        task.result = {"success": False, "error": str(e)}


def _settle_finished(tasks: list, response_id: str) -> None:
    for task in tasks:
        if task.collector_response_id != response_id:
            continue
        if not task.completed and task.asyncio_task and task.asyncio_task.done():
            _mark_settled(task, task.asyncio_task)


def _owned_pending(tasks: list, response_id: str) -> dict[asyncio.Task, Any]:
    """Ownership filter alongside liveness: a resume can steal a task back
    at any await boundary (clears collector_response_id and installs a
    fresh writer) — a stolen task's new writer must never be awaited,
    marked, or cleaned under this collector."""
    return {
        t.asyncio_task: t for t in tasks
        if t.is_pending and t.asyncio_task
        and t.collector_response_id == response_id
    }


async def _replay_settled(
    thread_id: str,
    tasks: list,
    response_id: str,
    pending: dict,
    out: list[dict],
) -> bool:
    """Replay every owned, settled task's events; False on any withheld
    archive (``is_pending``/``completed`` are mutually exclusive, so the
    pending check only guards the registered-but-unstarted shape)."""
    ok = True
    for task in tasks:
        if (
            task.collector_response_id == response_id
            and task.completed
            and task.captured_event_count > 0
            and task not in pending.values()
        ):
            if not await replay_owned_task_events(
                thread_id, task, response_id, out
            ):
                ok = False
    return ok


async def _adopt_settled_batch(
    done: set,
    pending: dict,
    thread_id: str,
    response_id: str,
    out: list[dict],
    *,
    log_label: str | None = None,
) -> bool:
    """Pop finished writers, adopt their results, replay their events.

    Replay re-checks the claim per task: the prior task's replay awaits,
    and a steal in that window would archive round-2 events into round 1.
    """
    ok = True
    settled_now = []
    for writer in done:
        task = pending.pop(writer)
        if task.collector_response_id != response_id:
            continue  # stolen between settle and this wake
        _mark_settled(task, writer)
        settled_now.append(task)
    for task in settled_now:
        if task.collector_response_id != response_id:
            continue
        if task.captured_event_count > 0:
            if not await replay_owned_task_events(
                thread_id, task, response_id, out
            ):
                ok = False
        if log_label:
            logger.info(
                f"[{log_label}] {task.display_id} completed, "
                f"persisting events for thread_id={thread_id}"
            )
    return ok


async def _persist_if_any(
    main_chunks: list[dict],
    all_events: list[dict],
    response_id: str,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    sandbox,
    persist_ok: bool,
) -> bool:
    if not all_events:
        return persist_ok
    return (
        await persist_collected_events(
            main_chunks, all_events, response_id,
            thread_id, workspace_id, user_id, sandbox=sandbox,
        )
        and persist_ok
    )


async def _finish_collected(
    thread_id: str,
    response_id: str,
    collected_tasks: list,
    workspace_id: str,
    user_id: str,
    is_byok: bool,
    persist_ok: bool,
    *,
    publish_wake: bool,
) -> None:
    """Usage billing + optional settled wake + drain/retire, in that order.

    Usage and report-back are ownership-filtered upstream: a stolen task's
    usage is billed by its new owner, and its report-back must be claimed
    under the new response id.
    """
    await persist_subagent_usage(
        response_id, collected_tasks, thread_id, workspace_id, user_id,
        is_byok=is_byok,
    )
    if publish_wake:
        await publish_settled_wake(thread_id)
    await await_drain_and_cleanup_tasks(
        collected_tasks, thread_id, response_id,
        retire_streams=persist_ok,
    )


async def collect_subagent_results_for_turn(
    thread_id: str,
    response_id: str,
    original_chunks: list[dict[str, Any]],
    tasks: list,
    workspace_id: str,
    user_id: str,
    timeout: float | None = None,
    is_byok: bool = False,
    sandbox=None,
    *,
    track_orphan_collector: Callable[[str, str, asyncio.Task], None],
) -> None:
    if timeout is None:
        timeout = get_subagent_collector_timeout()

    try:
        _settle_finished(tasks, response_id)

        subagent_agent_ids = {f"task:{t.task_id}" for t in tasks}
        main_chunks = [
            c for c in original_chunks
            if c.get("data", {}).get("agent", "") not in subagent_agent_ids
        ]

        pending = _owned_pending(tasks, response_id)

        all_subagent_events: list[dict] = []
        # Tracks whether the LATEST archive write landed. Cleanup retires
        # the Redis capture streams, which after a failed persist are the
        # only remaining source of the transcript — never retire on False.
        persist_ok = await _replay_settled(
            thread_id, tasks, response_id, pending, all_subagent_events
        )
        persist_ok = await _persist_if_any(
            main_chunks, all_subagent_events, response_id,
            thread_id, workspace_id, user_id, sandbox, persist_ok,
        )

        if not pending:
            await _finish_collected(
                thread_id, response_id, tasks, workspace_id, user_id,
                is_byok, persist_ok, publish_wake=False,
            )
            return

        deadline = time.time() + timeout

        while pending:
            remaining_timeout = deadline - time.time()
            if remaining_timeout <= 0:
                logger.warning(
                    f"[SubagentCollector] Turn collector timeout for {thread_id}, "
                    f"{len(pending)} tasks still pending"
                )
                break

            done, _ = await asyncio.wait(
                pending.keys(),
                timeout=remaining_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                break

            if not await _adopt_settled_batch(
                done, pending, thread_id, response_id, all_subagent_events
            ):
                persist_ok = False
            persist_ok = await _persist_if_any(
                main_chunks, all_subagent_events, response_id,
                thread_id, workspace_id, user_id, sandbox, persist_ok,
            )

        if pending:
            orphaned_tasks = list(pending.values())
            logger.info(
                f"[SubagentCollector] Spawning orphan collector for "
                f"{len(orphaned_tasks)} timed-out task(s), thread_id={thread_id}"
            )
            orphan_task = asyncio.create_task(
                collect_orphaned_subagent_results(
                    thread_id=thread_id,
                    response_id=response_id,
                    main_chunks=main_chunks,
                    prior_subagent_events=list(all_subagent_events),
                    tasks=orphaned_tasks,
                    workspace_id=workspace_id,
                    user_id=user_id,
                    is_byok=is_byok,
                    sandbox=sandbox,
                ),
                name=f"subagent-orphan-collector-{thread_id}",
            )
            track_orphan_collector(thread_id, response_id, orphan_task)

        collected_tasks = [
            t for t in tasks
            if t.collector_response_id == response_id
            and t not in pending.values()
        ]
        await _finish_collected(
            thread_id, response_id, collected_tasks, workspace_id, user_id,
            is_byok, persist_ok, publish_wake=not pending,
        )

    except Exception as e:
        logger.error(
            f"[SubagentCollector] Turn collector failed for {thread_id}: {e}",
            exc_info=True,
        )


async def publish_settled_wake(thread_id: str) -> None:
    """Settled-watch reconciliation. Report-back jobs are born on the run
    ledger's terminal CAS, not by the collectors — this only publishes
    the cleared wake once the batch has fully settled with no open job.
    Never raises (the helper swallows its own errors)."""
    from src.server.services.report_back.subagent import (
        publish_cleared_wake_if_no_open_job,
    )

    await publish_cleared_wake_if_no_open_job(thread_id)


async def await_drain_and_cleanup_tasks(
    tasks: list,
    thread_id: str,
    response_id: str,
    timeout: float | None = None,
    *,
    retire_streams: bool = True,
) -> None:
    """Post-collection teardown, fenced on the collector claim: every
    mutation and delete re-checks ``collector_response_id`` because a
    resume can steal the entry back (clears the claim, installs a live
    writer) at any await boundary — an unfenced pass here would null the
    new writer's handles, nuke its fresh Redis keys, and evict the entry
    out from under the resuming run's tail drain.

    ``retire_streams=False`` (after a failed archive persist) skips only
    the Redis key retirement — the streams keep their terminal-retention
    TTL as the transcript's last copy — while heavy refs and registry
    entries are still released; the in-memory entry is process-local and
    holding it recovers nothing, it only leaks."""
    if timeout is None:
        timeout = get_sse_drain_timeout()

    async def _wait_one(event: "asyncio.Event") -> None:
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(*[_wait_one(t.sse_drain_complete) for t in tasks])

    if not retire_streams:
        logger.error(
            f"[SubagentCleanup] Archive persist failed for "
            f"response_id={response_id}; retaining capture streams for "
            "their terminal-retention TTL"
        )

    try:
        cache = get_cache_client()
    except Exception as exc:
        cache = None
        logger.warning(
            f"[SubagentCleanup] Cache client unavailable during cleanup "
            f"for thread_id={thread_id}: {exc}"
        )

    # Look up the per-thread registry once so we can evict each task's
    # dict entry after its cleanup completes. Without this, _tasks grows
    # unboundedly across turns on a long-lived thread (every subagent
    # ever spawned stays referenced forever).
    from src.server.services.background_registry_store import BackgroundRegistryStore
    bg_registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)

    for task in tasks:
        if task.collector_response_id != response_id:
            continue  # stolen by a resume — the new owner cleans up
        task.per_call_records = []
        task.tool_usage = {}
        task.asyncio_task = None
        task.handler_task = None
        if cache is not None and retire_streams:
            # Serialized on the spill lock: the resume's reset-deletes and
            # the resumed writer's spills take the same lock, so a delete
            # issued here can never be in flight when round-2 data lands
            # (a pooled-connection delete can otherwise overtake the reset
            # and erase the fresh key). Claim re-checked INSIDE the lock:
            # if the steal won the lock first, these keys already hold the
            # new run's events.
            async with task.redis_spill_lock:
                if task.collector_response_id == response_id:
                    await delete_task_keys_if_owned(
                        cache,
                        thread_id,
                        task.task_id,
                        response_id,
                        task_run_id=task.task_run_id,
                    )
        logger.info(
            "task_heavy_refs_released",
            extra={
                "thread_id": thread_id,
                "task_id": task.task_id,
                "tool_call_id": task.tool_call_id,
                "captured_event_count": getattr(task, "captured_event_count", 0),
                "captured_event_bytes": getattr(task, "captured_event_bytes", 0),
                "redis_write_failed": getattr(task, "redis_write_failed", False),
            },
        )

        if bg_registry is not None:
            try:
                # Claim re-checked under the registry lock: eviction is
                # the one mutation that can't be undone by the new owner.
                await bg_registry.remove_task_if_owned(
                    task.tool_call_id, response_id
                )
            except Exception as exc:
                logger.warning(
                    f"[SubagentCleanup] remove_task failed for "
                    f"thread_id={thread_id} task_id={task.task_id}: {exc}"
                )


async def collect_orphaned_subagent_results(
    thread_id: str,
    response_id: str,
    main_chunks: list[dict[str, Any]],
    prior_subagent_events: list[dict],
    tasks: list,
    workspace_id: str,
    user_id: str,
    is_byok: bool = False,
    sandbox=None,
) -> None:
    idle_timeout = get_subagent_orphan_collector_timeout()
    poll_interval = min(30.0, idle_timeout)

    try:
        all_subagent_events = list(prior_subagent_events)

        _settle_finished(tasks, response_id)
        pending = _owned_pending(tasks, response_id)

        persist_ok = await _replay_settled(
            thread_id, tasks, response_id, pending, all_subagent_events
        )

        if not pending:
            persist_ok = await _persist_if_any(
                main_chunks, all_subagent_events, response_id,
                thread_id, workspace_id, user_id, sandbox, persist_ok,
            )
            owned_tasks = [
                t for t in tasks if t.collector_response_id == response_id
            ]
            await _finish_collected(
                thread_id, response_id, owned_tasks, workspace_id, user_id,
                is_byok, persist_ok, publish_wake=False,
            )
            logger.info(
                f"[OrphanCollector] All tasks already completed for "
                f"thread_id={thread_id}"
            )
            return

        logger.info(
            f"[OrphanCollector] Waiting for {len(pending)} task(s) with "
            f"{idle_timeout}s idle timeout, thread_id={thread_id}"
        )

        last_activity: dict[asyncio.Task, tuple[float, int]] = {
            at: (t.last_updated_at, t.captured_event_count)
            for at, t in pending.items()
        }
        last_progress_time = time.time()

        while pending:
            if time.time() - last_progress_time > idle_timeout:
                logger.warning(
                    f"[OrphanCollector] Idle timeout ({idle_timeout}s) for "
                    f"thread_id={thread_id}, {len(pending)} tasks still pending"
                )
                break

            done, _ = await asyncio.wait(
                pending.keys(),
                timeout=poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if done:
                last_progress_time = time.time()
                for writer in done:
                    last_activity.pop(writer, None)

                if not await _adopt_settled_batch(
                    done, pending, thread_id, response_id,
                    all_subagent_events, log_label="OrphanCollector",
                ):
                    persist_ok = False
                persist_ok = await _persist_if_any(
                    main_chunks, all_subagent_events, response_id,
                    thread_id, workspace_id, user_id, sandbox, persist_ok,
                )
            else:
                for asyncio_task, task in pending.items():
                    prev_update, prev_events = last_activity.get(
                        asyncio_task, (0.0, 0)
                    )
                    cur_update = task.last_updated_at
                    cur_events = task.captured_event_count
                    if cur_update > prev_update or cur_events > prev_events:
                        last_progress_time = time.time()
                        last_activity[asyncio_task] = (cur_update, cur_events)

        if pending:
            for asyncio_task, task in pending.items():
                if task.collector_response_id == response_id:
                    task.collector_response_id = None
                logger.warning(
                    f"[OrphanCollector] Giving up on idle task "
                    f"{task.display_id} for thread_id={thread_id} "
                    f"(no progress for {idle_timeout}s)"
                )

        collected_tasks = [
            t for t in tasks
            if t.collector_response_id == response_id
            and t not in pending.values()
        ]
        if collected_tasks:
            await _finish_collected(
                thread_id, response_id, collected_tasks, workspace_id,
                user_id, is_byok, persist_ok, publish_wake=not pending,
            )

    except Exception as e:
        logger.error(
            f"[OrphanCollector] Failed for thread_id={thread_id}: {e}",
            exc_info=True,
        )
        for task in tasks:
            if task.collector_response_id == response_id:
                task.collector_response_id = None


async def spawn_subagent_collector(
    thread_id: str,
    run_id: str,
    metadata: dict,
    workspace_id: Optional[str],
    user_id: Optional[str],
    *,
    collect_for_turn: Callable[..., Any],
) -> None:
    """Claim this run's subagents and collect their events post-terminal."""
    response_id = run_id  # 1:1 contract

    from src.server.services.background_registry_store import BackgroundRegistryStore
    bg_store = BackgroundRegistryStore.get_instance()
    bg_registry = await bg_store.get_registry(thread_id)
    if not bg_registry:
        return
    tasks_to_collect = []
    # Hold the registry lock during claim so two concurrent collectors
    # (e.g., orphan from prior turn + current turn) can't both observe
    # collector_response_id is None for the same task and double-claim.
    async with bg_registry._lock:
        for t in bg_registry._tasks.values():
            if t.collector_response_id:
                continue
            # Filter by spawned_run_id: only claim subagents spawned
            # by THIS turn. None matches as a compat shim for tasks
            # registered before run_id stamping shipped.
            if t.spawned_run_id is not None and t.spawned_run_id != run_id:
                continue
            if (
                t.is_pending
                or t.captured_event_count > 0
                or t.per_call_records
                or t.tool_usage
            ):
                t.collector_response_id = response_id
                tasks_to_collect.append(t)
    if tasks_to_collect and workspace_id and user_id:
        handler = metadata.get("handler")
        sse_events = handler.get_sse_events() if handler else []
        asyncio.create_task(
            collect_for_turn(
                thread_id=thread_id,
                response_id=response_id,
                original_chunks=sse_events or [],
                tasks=tasks_to_collect,
                workspace_id=workspace_id,
                user_id=user_id,
                is_byok=metadata.get("is_byok", False),
                sandbox=metadata.get("sandbox"),
            ),
            name=f"subagent-collector-{thread_id}-{run_id}-post-tail",
        )


async def persist_collected_events(
    main_chunks: list[dict],
    subagent_events: list[dict],
    response_id: str,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    sandbox=None,
) -> bool:
    """Clean and persist main + subagent events to DB.

    Returns True once the archive write landed; callers must not retire
    the Redis capture streams on False — they are the only remaining
    source of the captured transcript.
    """
    import copy

    cleaned = []
    for event in subagent_events:
        e = copy.deepcopy(event)
        e.pop("ts", None)
        cleaned.append(e)

    if sandbox:
        try:
            from src.server.services.persistence.image_capture import (
                capture_and_rewrite_images,
            )

            await capture_and_rewrite_images(
                cleaned, sandbox, thread_id=thread_id,
            )
        except Exception:
            logger.warning(
                "[IMAGE_CAPTURE] Hook B failed", exc_info=True,
            )

    # Direct DB update — we know the response_id, no need to go through
    # the persistence-service singleton (which would key by run_id and
    # might not match a subagent collector running across turns).
    from src.server.database import conversation as qr_db

    replaced_agents = {
        str((e.get("data") or {}).get("agent", "")) for e in cleaned
    }
    # One bounded retry: the replay cache gate holds the turn uncacheable
    # until these rows land, so a transiently failed write must not leave
    # the turn rebuilding on every read for its lifetime. Each attempt
    # rebases inside one row-locked transaction — concurrent atomic
    # appends (compact/offload context_window) serialize on the lock
    # instead of being erased, and successive batch writes strip their
    # own earlier task rows rather than duplicate them.
    for attempt in (1, 2):
        try:
            if await qr_db.rebase_sse_events(
                response_id,
                drop_agents=replaced_agents,
                append_events=cleaned,
                fallback_base=main_chunks,
            ):
                logger.info(
                    f"[SubagentCollector] Updated sse_events for "
                    f"response_id={response_id} (+{len(cleaned)} events)"
                )
                return True
            raise RuntimeError(f"no response row for {response_id}")
        except Exception as e:
            if attempt == 1:
                await asyncio.sleep(2.0)
                continue
            logger.error(
                f"[SubagentCollector] Failed to update sse_events "
                f"response_id={response_id}: {e}",
                exc_info=True,
            )
    return False


async def persist_subagent_usage(
    response_id: str,
    tasks: list,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    is_byok: bool = False,
) -> None:
    """Persist each subagent's token usage as a separate row with msg_type='task'."""
    from src.server.services.persistence.usage import UsagePersistenceService
    from src.server.services.background_registry_store import BackgroundRegistryStore

    # Snapshot-and-clear usage under the registry lock, gated on still
    # owning the task (collector_response_id == response_id). A resume
    # clears that field, so a stale collector that re-claimed the same task
    # at turn-N end skips here while turn-N+1's collector bills the merged
    # usage exactly once — no double-persist across the resume window.
    bg_registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)

    def _claim_owned_usage() -> list[tuple[Any, list, dict]]:
        out: list[tuple[Any, list, dict]] = []
        for task in tasks:
            if task.collector_response_id != response_id:
                continue
            if not (task.per_call_records or task.tool_usage):
                continue
            records = task.per_call_records
            tool_usage = task.tool_usage
            task.per_call_records = []
            task.tool_usage = {}
            out.append((task, records, tool_usage))
        return out

    if bg_registry is not None:
        async with bg_registry._lock:
            claimed = _claim_owned_usage()
    else:
        # Registry gone (thread teardown) — tasks still carry their claim,
        # and the claim body has no awaits, so it's atomic without the lock.
        claimed = _claim_owned_usage()

    if not claimed:
        return

    persisted_count = 0
    persisted_records = 0

    for task, records, tool_usage in claimed:
        try:
            usage_service = UsagePersistenceService(
                thread_id=thread_id,
                workspace_id=workspace_id,
                user_id=user_id,
            )
            await usage_service.track_llm_usage(records)

            if tool_usage:
                usage_service.record_tool_usage_batch(tool_usage)

            # track_llm_usage([]) initializes _token_usage to a zeroed
            # dict, so tool-only tasks still get stamped; None only on its
            # internal cost-calculation error path, where skipping is the
            # documented is_byok fallback contract.
            if usage_service._token_usage is not None:
                usage_service._token_usage["task_id"] = task.task_id
                usage_service._token_usage["agent_id"] = task.agent_id
                usage_service._token_usage["subagent_type"] = task.subagent_type

            await usage_service.persist_usage(
                response_id=response_id,
                msg_type="task",
                status="completed",
                is_byok=is_byok,
            )
            persisted_count += 1
            persisted_records += len(records)

        except Exception as e:
            logger.error(
                f"[SubagentUsage] Failed to persist usage for task {task.task_id} "
                f"in thread_id={thread_id}: {e}",
                exc_info=True,
            )

    if persisted_count:
        logger.info(
            f"[SubagentUsage] Persisted {persisted_count} subagent usage row(s) "
            f"({persisted_records} LLM calls) for response_id={response_id} "
            f"thread_id={thread_id}"
        )
