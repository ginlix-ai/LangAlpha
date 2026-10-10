"""Post-terminal subagent collection and billing for root workflow runs.

Owns the read/retire side of the per-task capture streams: replaying a
settled subagent's events into the turn archive, billing its usage, and
retiring its Redis keys, all fenced on the collector claim
(``collector_response_id``) because a resume can steal a task back at any
await boundary. A run that ends without a collector has its subagents
claimed by its kill, their lanes archived here once their writers settle, and
billed here when the user stopped it. Process-local executor state (the
orphan-collector registry, the task table) stays in ``LocalRunExecutor`` and
is lent in through narrow callables.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Optional

from src.config.settings import (
    get_sse_drain_timeout,
    get_subagent_collector_timeout,
    get_subagent_orphan_collector_timeout,
)
from src.server.services.runs.subagent_archive import (
    ArchiveBuffer,
    SubagentArchiveReadError,
    iter_subagent_events_full,
    persist_collected_events,
    record_to_persist_event,
)
from src.server.services.runs.teardown import killed_lane
from src.server.services.runs.subagent_usage import persist_subagent_usage
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


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
    try:
        async for record in iter_subagent_events_full(thread_id, task):
            if task.collector_response_id != response_id:
                return False  # stolen mid-replay: the resume owns the archive
            if record.get("run") not in allowed:
                continue  # another round's record (cross-worker resume)
            eligible += 1
            buffered.append(record_to_persist_event(record, thread_id))
    except SubagentArchiveReadError as exc:
        logger.error(
            f"[SubagentCollector] Archive read failed for task "
            f"{getattr(task, 'task_id', '?')}; withholding partial archive: {exc}"
        )
        return False
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
    task.adopt_writer_outcome(writer)


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


async def _replay_into(buf: ArchiveBuffer, task) -> None:
    replayed = await replay_owned_task_events(
        buf.thread_id, task, buf.response_id, buf.unwritten
    )
    buf.withheld |= not replayed


async def _replay_settled(tasks: list, pending: dict, buf: ArchiveBuffer) -> None:
    """Replay every owned, settled task's events into ``buf``
    (``is_pending``/``completed`` are mutually exclusive, so the pending
    check only guards the registered-but-unstarted shape)."""
    for task in tasks:
        if (
            task.collector_response_id == buf.response_id
            and task.completed
            and task.captured_event_count > 0
            and task not in pending.values()
        ):
            await _replay_into(buf, task)


async def _claim_owner_children(
    thread_id: str,
    parent,
    response_id: str,
    tasks: list,
    pending: dict,
) -> list:
    """Claim a settled task's owner-children into this collection.

    Workflow children register while the driver runs — after the turn's
    terminal claim sweep — so no sweep ever saw the late ones; the parent's
    settle is the first moment the full child set exists. The registry claims
    them under its own lock (same gate as the sweep); here we fold live
    writers into the wait set and hand already-settled children back for the
    caller's replay loop. One level suffices: children are plain subagents
    and never own children of their own.
    """
    from src.server.services.background_registry_store import BackgroundRegistryStore

    bg_registry = await BackgroundRegistryStore.get_instance().get_registry(
        thread_id
    )
    if bg_registry is None:
        return []
    claimed = await bg_registry.claim_owner_children(parent.task_id, response_id)
    settled = []
    for child in claimed:
        tasks.append(child)
        writer = child.asyncio_task
        if writer is not None and writer.done():
            _mark_settled(child, writer)
            settled.append(child)
        elif child.is_pending and writer is not None:
            pending[writer] = child
    return settled


async def _claim_settled_parents(
    thread_id: str, tasks: list, response_id: str, pending: dict
) -> None:
    """Entry-path mirror of the adopt-time claim: a parent can settle
    between the turn's terminal claim sweep and collection start, entering
    already completed — the wait loop never adopts it, so its owner-children
    must be claimed here. Snapshot: the claim appends children to ``tasks``.
    """
    for task in [
        t for t in tasks
        if t.collector_response_id == response_id and t.completed
    ]:
        await _claim_owner_children(
            thread_id, task, response_id, tasks, pending
        )


async def _adopt_settled_batch(
    done: set,
    pending: dict,
    buf: ArchiveBuffer,
    *,
    tasks: list,
    log_label: str | None = None,
) -> None:
    """Pop finished writers, adopt their results, replay their events.

    Replay re-checks the claim per task: the prior task's replay awaits,
    and a steal in that window would archive round-2 events into round 1.
    """
    thread_id, response_id = buf.thread_id, buf.response_id
    settled_now = []
    for writer in done:
        task = pending.pop(writer)
        if task.collector_response_id != response_id:
            continue  # stolen between settle and this wake
        _mark_settled(task, writer)
        settled_now.append(task)
    for task in list(settled_now):
        settled_now.extend(
            await _claim_owner_children(
                thread_id, task, response_id, tasks, pending
            )
        )
    for task in settled_now:
        if task.collector_response_id != response_id:
            continue
        if task.captured_event_count > 0:
            await _replay_into(buf, task)
        if log_label:
            logger.info(
                f"[{log_label}] {task.display_id} completed, "
                f"persisting events for thread_id={thread_id}"
            )


async def _finish_collected(
    buf: ArchiveBuffer,
    collected_tasks: list,
    is_byok: bool,
    *,
    publish_wake: bool,
) -> None:
    """Usage billing + optional settled wake + drain/retire, in that order.

    Usage and report-back are ownership-filtered upstream: a stolen task's
    usage is billed by its new owner, and its report-back must be claimed
    under the new response id.
    """
    await persist_subagent_usage(
        buf.response_id, collected_tasks, buf.thread_id, buf.workspace_id,
        buf.user_id, is_byok=is_byok,
    )
    if publish_wake:
        await publish_settled_wake(buf.thread_id)
    await await_drain_and_cleanup_tasks(
        collected_tasks, buf.thread_id, buf.response_id,
        retire_streams=buf.complete,
    )


async def collect_subagent_results_for_turn(
    thread_id: str,
    response_id: str,
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

        pending = _owned_pending(tasks, response_id)
        await _claim_settled_parents(thread_id, tasks, response_id, pending)

        buf = ArchiveBuffer(response_id, thread_id, workspace_id, user_id, sandbox)
        await _replay_settled(tasks, pending, buf)
        await buf.flush()

        if not pending:
            await _finish_collected(buf, tasks, is_byok, publish_wake=False)
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

            await _adopt_settled_batch(done, pending, buf, tasks=tasks)
            await buf.flush()

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
                    unwritten=buf.unwritten[:],
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
            buf, collected_tasks, is_byok, publish_wake=not pending,
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

    ``retire_streams=False`` (a replay withheld, or a write not landed)
    skips only the Redis key retirement, so the streams keep their
    terminal-retention TTL as the transcript's last copy. Heavy refs and
    registry entries are still released: the in-memory entry is
    process-local, and holding it recovers nothing, it only leaks."""
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
            f"[SubagentCleanup] Archive incomplete for "
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
    unwritten: list[dict],
    tasks: list,
    workspace_id: str,
    user_id: str,
    is_byok: bool = False,
    sandbox=None,
) -> None:
    idle_timeout = get_subagent_orphan_collector_timeout()
    poll_interval = min(30.0, idle_timeout)

    try:
        # ``unwritten``: the turn collector's events that have not landed yet.
        buf = ArchiveBuffer(
            response_id, thread_id, workspace_id, user_id, sandbox, unwritten
        )

        _settle_finished(tasks, response_id)
        pending = _owned_pending(tasks, response_id)
        await _claim_settled_parents(thread_id, tasks, response_id, pending)

        await _replay_settled(tasks, pending, buf)

        if not pending:
            await buf.flush()
            owned_tasks = [
                t for t in tasks if t.collector_response_id == response_id
            ]
            await _finish_collected(buf, owned_tasks, is_byok, publish_wake=False)
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

                await _adopt_settled_batch(
                    done, pending, buf, tasks=tasks, log_label="OrphanCollector",
                )
                await buf.flush()
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
                buf, collected_tasks, is_byok, publish_wake=not pending,
            )

    except Exception as e:
        logger.error(
            f"[OrphanCollector] Failed for thread_id={thread_id}: {e}",
            exc_info=True,
        )
        for task in tasks:
            if task.collector_response_id == response_id:
                task.collector_response_id = None


# Strong refs for post-terminal collector tasks: a bare create_task is
# eligible for GC mid-flight, which would silently skip transcript archival
# and usage persistence. Distinct from the executor's orphan-collector
# registry — these must never be cancelled by the stop path.
_collector_tasks: set[asyncio.Task] = set()


def _retain_collector(task: asyncio.Task) -> None:
    _collector_tasks.add(task)

    def _discard(t: asyncio.Task) -> None:
        _collector_tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.error(
                f"[SubagentCollector] post-terminal collector failed: "
                f"{t.exception()!r}"
            )

    task.add_done_callback(_discard)


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
    tasks_to_collect = await bg_registry.claim_run_subagents(run_id, response_id)
    if tasks_to_collect and workspace_id and user_id:
        _retain_collector(
            asyncio.create_task(
                collect_for_turn(
                    thread_id=thread_id,
                    response_id=response_id,
                    tasks=tasks_to_collect,
                    workspace_id=workspace_id,
                    user_id=user_id,
                    is_byok=metadata.get("is_byok", False),
                    sandbox=metadata.get("sandbox"),
                ),
                name=f"subagent-collector-{thread_id}-{run_id}-post-tail",
            )
        )


async def end_run_subagents(
    thread_id: str,
    run_id: str,
    workspace_id: Optional[str],
    user_id: Optional[str],
    *,
    claimed_at_stop: list,
    carried_events: list[dict],
    bill: bool,
    is_byok: bool = False,
    sandbox=None,
) -> None:
    """Kill the subagents of a run no collector takes, archive their lanes,
    and bill each of them once when the ending bills.

    ``claimed_at_stop`` is what a stop teardown's kill claimed, and
    ``carried_events`` what its drain put in the finalize. This kill claims
    whatever is still registered, which is all of them when no teardown ran
    (a stop only the finalize learns of, a failure). Every claimed lane the
    finalize did not carry is archived once its writers settle, so none is
    left to expire with its capture stream. Process-local claims suffice: a
    subagent's writer and usage live on the worker running the turn that
    spawned or resumed it, which is where that turn finalizes.
    """
    from src.server.services.background_registry_store import BackgroundRegistryStore

    killed_claimed: list = []
    try:
        killed = await BackgroundRegistryStore.get_instance().cancel_run_tasks(
            thread_id, run_id, force=True, claim_for=run_id
        )
        killed_claimed = killed.claimed
    finally:
        # Read only now: a teardown that a second cancel left running claims
        # into ``claimed_at_stop`` while this kill waits on the same writers,
        # and whichever kill's lock section comes second finds them claimed.
        # Settled even when the kill raised: the claim keeps every later
        # collector off these tasks, so this is the only owner they have.
        claimed = [*claimed_at_stop, *killed_claimed]
        if claimed and workspace_id:
            carried = {(e.get("data") or {}).get("agent") for e in carried_events}
            _archive_after_unwind(
                thread_id,
                run_id,
                [t for t in claimed if f"task:{t.task_id}" not in carried],
                workspace_id,
                sandbox,
            )
            if bill and user_id:
                await _bill_after_unwind(
                    run_id, claimed, thread_id, workspace_id, user_id, is_byok
                )


def _archive_after_unwind(
    thread_id: str, response_id: str, tasks: list, workspace_id: str, sandbox
) -> None:
    """Archive claimed tasks' lanes in the background once their writers settle.

    A writer still unwinding can append past any earlier read, which is why
    the stop drain withholds its lane. The read is fenced on the claim: a
    resume that takes a task back meanwhile archives that lane under its own
    run.
    """
    if not tasks:
        return

    async def archive() -> None:
        writers = [writer for task in tasks for writer in task.live_writers]
        if writers:
            await asyncio.wait(writers, timeout=SUBAGENT_TAIL_TIMEOUT)
        events: list[dict] = []
        for task in tasks:
            events.extend(await killed_lane(thread_id, task, owner=response_id))
        if events:
            await persist_collected_events(
                events, response_id, thread_id, workspace_id, sandbox=sandbox
            )

    _retain_collector(
        asyncio.create_task(
            archive(), name=f"subagent-archive-{thread_id}-{response_id}"
        )
    )


# How long a run's subagent writers may outlive it. Legit tail subagents (deep
# research) run 15+ min, so a writer alive past this is hung: the guard drain
# discards the run's session under it, and the stop bill stops waiting for it.
SUBAGENT_TAIL_TIMEOUT = 1800.0


async def _bill_after_unwind(
    response_id: str,
    tasks: list,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    is_byok: bool,
) -> None:
    """Bill claimed tasks once their writers can no longer add to the usage.

    A writer merges its usage onto the task as it settles, so a task still
    unwinding past the kill's bounded wait has not merged yet. Waiting for it
    happens off the caller's path, in one retained task for the whole batch.
    """
    writers = [writer for task in tasks for writer in task.live_writers]
    if not writers:
        await persist_subagent_usage(
            response_id, tasks, thread_id, workspace_id, user_id, is_byok=is_byok
        )
        return

    async def bill_when_settled() -> None:
        _, hung = await asyncio.wait(writers, timeout=SUBAGENT_TAIL_TIMEOUT)
        if hung:
            logger.warning(
                f"[SubagentUsage] {len(hung)} writer(s) of response_id="
                f"{response_id} thread_id={thread_id} still running after "
                f"{SUBAGENT_TAIL_TIMEOUT:.0f}s; billing without their usage"
            )
        await persist_subagent_usage(
            response_id, tasks, thread_id, workspace_id, user_id, is_byok=is_byok
        )

    _retain_collector(
        asyncio.create_task(
            bill_when_settled(),
            name=f"subagent-usage-{thread_id}-{response_id}-unwind",
        )
    )
