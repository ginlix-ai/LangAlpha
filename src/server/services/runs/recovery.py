"""Recovery scanner v4 Phase 2.2 — guarded finalize of orphaned runs.

Finds in_progress runs whose owner is provably dead — the run's N(thread,
root) advisory lock is acquirable, and an owner holds that lock from before
START to after finalize — classifies them exactly as the owner would have
(durable cancel intent → cancelled; a durable run-matching ``__interrupt__``
at the checkpoint tip → interrupted; anything else → error worker_lost),
salvages the run's Redis stream into the sse_events archive, and finalizes
through the same CAS as every other path. Concurrent scanners on sibling
workers are safe without leadership: the lock probe serializes them per run
and the CAS is single-winner regardless.

Runs periodically only when the WriterGuard fence is active. Without the
fence there is no liveness oracle, so scanning is startup-only (this
process restarting is the proof of death) — exactly Phase 1's sweep.
"""

import asyncio
import json
import logging
import random
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import get_recovery_scan_interval
from src.server.database.runs import subagent_runs as sr_db
from src.server.database.runs import lifecycle as tl_db

logger = logging.getLogger(__name__)

# Salvage reads at most this many stream entries; a run that produced more
# archives a truncated prefix (recovery_quality says so) rather than letting
# one huge dead run stall the scan.
SALVAGE_MAX_EVENTS = 5000

# Graceful-stop grace period: a scan pass in flight gets this long to finish
# its current run's commit-to-emission section before being cancelled.
STOP_GRACE = 30.0


class RecoveryScanner:
    _instance: Optional["RecoveryScanner"] = None

    def __init__(self) -> None:
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "RecoveryScanner":
        if cls._instance is None:
            cls._instance = RecoveryScanner()
        return cls._instance

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        """Start the periodic scan loop (guard-fenced deployments only)."""
        from src.server.services import writer_guard as wg

        if self._loop_task is not None and not self._loop_task.done():
            return
        if not wg.guard_enabled():
            logger.info(
                "[RecoveryScanner] fence inactive; recovery is startup-only"
            )
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="recovery-scanner"
        )
        logger.info(
            f"[RecoveryScanner] started (interval={get_recovery_scan_interval()}s)"
        )

    async def stop(self) -> None:
        """Cooperative stop: a recovery in flight finishes its committed
        CAS through terminal emission (cancelling between them would strand
        a terminal row with no run_end); only a pass overrunning STOP_GRACE
        gets cancelled."""
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            logger.warning(
                "[RecoveryScanner] scan pass exceeded stop grace; cancelling"
            )
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass
        except Exception:
            pass
        self._loop_task = None
        # Fresh unset event: a later lifespan in this process runs its
        # startup scan_once() BEFORE start() — a still-set event would
        # silently skip every run in that pass.
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        interval = get_recovery_scan_interval()
        while not self._stop_event.is_set():
            # Jitter desynchronizes sibling workers' scans so they don't
            # probe the same runs in lockstep every cycle.
            jitter = interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[RecoveryScanner] scan failed", exc_info=True)

    # ----------------------------------------------------------------- scan

    async def scan_once(self, *, assume_dead: bool = False) -> int:
        """One pass over open runs. ``assume_dead`` (startup, single worker,
        no fence) finalizes without a lock probe — this process restarting
        is the proof no executor exists."""
        from src.server.services import writer_guard as wg

        try:
            open_runs = await tl_db.list_open_runs()
        except Exception:
            logger.error("[RecoveryScanner] open-run query failed", exc_info=True)
            open_runs = []
        try:
            open_task_runs = await sr_db.list_open_task_runs()
        except Exception:
            logger.error(
                "[RecoveryScanner] open-task-run query failed", exc_info=True
            )
            open_task_runs = []

        # Before the early-return: an idle deployment is exactly where
        # cascade-orphaned task rows sit unnoticed, so the heal must not be
        # conditional on there being open runs to recover.
        await self.heal_task_chains()

        if not open_runs and not open_task_runs:
            return 0

        if wg.guard_enabled():
            try:
                async with wg.get_writer_pool().connection() as lock_conn:
                    # The pool's reset callback re-runs unlock_all on the way
                    # out, backstopping any probe lock this scan leaks.
                    recovered = (
                        await self._scan(open_runs, lock_conn) if open_runs else 0
                    )
                    if open_task_runs:
                        recovered += await self._scan_task_runs(
                            open_task_runs, lock_conn
                        )
                    return recovered
            except Exception:
                logger.error(
                    "[RecoveryScanner] guarded scan session failed", exc_info=True
                )
                return 0
        if not assume_dead:
            # No fence, no liveness oracle: a periodic scan here would reap
            # LIVE runs of this very process.
            return 0
        recovered = await self._scan(open_runs, None) if open_runs else 0
        if open_task_runs:
            recovered += await self._scan_task_runs(open_task_runs, None)
        return recovered

    async def _scan(
        self, open_runs: List[Dict[str, Any]], lock_conn
    ) -> int:
        from src.server.services import writer_guard as wg

        recovered = 0
        for run in open_runs:
            if self._stop_event.is_set():
                # Shutting down: current run finished cleanly; leave the
                # rest to sibling workers or the next startup scan.
                break
            run_id = str(run["conversation_response_id"])
            thread_id = str(run["conversation_thread_id"])
            root_key = None
            if lock_conn is not None:
                root_key = wg.namespace_key(thread_id, wg.ROOT_NS)
                try:
                    cur = await lock_conn.execute(
                        "SELECT pg_try_advisory_lock(%s)", (root_key,)
                    )
                    acquired = (await cur.fetchone())[0]
                except Exception:
                    logger.error(
                        f"[RecoveryScanner] lock probe failed for {run_id}",
                        exc_info=True,
                    )
                    continue
                if not acquired:
                    logger.debug(
                        f"[RecoveryScanner] run {run_id} fenced by a live "
                        "owner; skipping"
                    )
                    continue
            inner = asyncio.ensure_future(
                self._recover_run(run, run_id, thread_id)
            )
            try:
                # Shield: a last-resort cancel (STOP_GRACE overrun, process
                # teardown) must not split the commit-to-emission section —
                # a committed CAS with no run_end strands reconnected
                # clients until stream TTL.
                if await asyncio.shield(inner):
                    recovered += 1
            except asyncio.CancelledError:
                try:
                    await inner
                except BaseException:
                    pass
                raise
            except Exception:
                logger.error(
                    f"[RecoveryScanner] recovery failed for run {run_id}",
                    exc_info=True,
                )
            finally:
                if lock_conn is not None and root_key is not None:
                    try:
                        await lock_conn.execute(
                            "SELECT pg_advisory_unlock(%s)", (root_key,)
                        )
                    except Exception:
                        pass
        return recovered

    async def _scan_task_runs(
        self, open_task_runs: List[Dict[str, Any]], lock_conn
    ) -> int:
        """Minimal task-run orphan recovery (M3): an in_progress subagent_runs
        row whose N(thread, task:id) fence is acquirable has no live writer —
        the row is born under that fence and the fence outlives finalize.
        Classification is deliberately thin (durable cancel intent →
        cancelled, else error worker_lost); task HITL is descoped, so no
        interrupted branch exists here.
        """
        from src.server.services import writer_guard as wg
        from src.server.services.subagent_run_coordinator import SubagentRunCoordinator

        recovered = 0
        for run in open_task_runs:
            if self._stop_event.is_set():
                break
            task_run_id = str(run["task_run_id"])
            thread_id = str(run["thread_id"])
            task_id = str(run["task_id"])
            ns_key = None
            if lock_conn is not None:
                ns_key = wg.namespace_key(thread_id, f"task:{task_id}")
                try:
                    cur = await lock_conn.execute(
                        "SELECT pg_try_advisory_lock(%s)", (ns_key,)
                    )
                    acquired = (await cur.fetchone())[0]
                except Exception:
                    logger.error(
                        f"[RecoveryScanner] task-ns probe failed for "
                        f"{task_run_id}",
                        exc_info=True,
                    )
                    continue
                if not acquired:
                    continue  # live writer holds the fence
            try:
                # Retention stamp FIRST: a terminal row is never revisited,
                # so a failed stamp after finalize would leak the dead
                # worker's no-TTL keys forever. Deferring the finalize keeps
                # the row open and this pass's work retried by the next scan
                # — recovery is not time-critical, immortal keys are.
                if not await self._stamp_task_retention(
                    thread_id, task_id, task_run_id
                ):
                    continue
                status = (
                    "cancelled" if run.get("cancel_requested_at") else "error"
                )
                result = await SubagentRunCoordinator(thread_id).finalize_task_run(
                    task_run_id,
                    status,
                    task_id=task_id,
                    failure=(
                        None
                        if status == "cancelled"
                        else {
                            "error": (
                                "worker_lost: no live executor holds this "
                                "task's namespace fence"
                            )
                        }
                    ),
                )
                if result["applied"]:
                    recovered += 1
                    logger.warning(
                        f"[RecoveryScanner] recovered task run {task_run_id} "
                        f"(thread={thread_id} task={task_id}) -> "
                        f"{result['run']['status']}"
                    )
            except Exception:
                logger.error(
                    f"[RecoveryScanner] task-run recovery failed for "
                    f"{task_run_id}",
                    exc_info=True,
                )
            finally:
                if lock_conn is not None and ns_key is not None:
                    try:
                        await lock_conn.execute(
                            "SELECT pg_advisory_unlock(%s)", (ns_key,)
                        )
                    except Exception:
                        pass
        return recovered

    async def _stamp_task_retention(
        self, thread_id: str, task_id: str, task_run_id: str
    ) -> bool:
        """Start the attach-grace expiry clock on a recovered task's event keys.

        Active task streams carry no TTL, and the only other stamp sites
        (the run wrapper's finally, the post-turn collector) live on the
        dead worker. Runs BEFORE the ledger finalize: False defers the
        finalize so the still-open row retries the stamp next scan. A
        disabled cache has no keys to stamp and never blocks recovery.
        """
        try:
            from ptc_agent.agent.middleware.background_subagent.redis_stream import (
                stamp_task_retention,
            )

            await stamp_task_retention(thread_id, task_id, task_run_id)
            return True
        except Exception:
            logger.warning(
                f"[RecoveryScanner] retention stamp failed for task "
                f"{task_id} run {task_run_id} (thread={thread_id}); "
                "finalize deferred to the next scan",
                exc_info=True,
            )
            return False

    async def _recover_run(
        self, run: Dict[str, Any], run_id: str, thread_id: str
    ) -> bool:
        status, interrupt_reason, errors, checkpoint_id = await self._classify(
            run, run_id, thread_id
        )
        sse_events, quality = await self._salvage_stream(thread_id, run_id)

        from src.server.services.runs.coordinator import RunCoordinator, RunOutcome

        # The funnel owns the entire post-CAS tail — projection refresh,
        # drainer nudge, and stream closure through the run_end gate. A lost
        # CAS still closes the stream with the survivor's outcome (the owner
        # may have died between its commit and its emission, and no later
        # scan revisits a terminal row).
        result = await RunCoordinator.get_instance().finalize_detached_run(
            thread_id,
            run_id,
            RunOutcome(
                status=status,
                interrupt_reason=interrupt_reason,
                metadata={"recovery": "scanner", "recovery_quality": quality},
                errors=errors,
                sse_events=sse_events,
            ),
            checkpoint_id=checkpoint_id,
            error_frame={
                "thread_id": thread_id,
                "content": "the worker running this turn was lost",
                "error_type": "worker_lost",
                "error": "worker_lost",
            },
        )
        if result.applied:
            logger.warning(
                f"[RecoveryScanner] recovered run {run_id} (thread={thread_id}) "
                f"-> {(result.run or {}).get('status', status)} "
                f"(quality={quality})"
            )
        return result.applied

    # --------------------------------------------------------- classification

    async def _classify(
        self, run: Dict[str, Any], run_id: str, thread_id: str
    ) -> Tuple[str, Optional[str], Optional[List[str]], Optional[str]]:
        """(status, interrupt_reason, errors, checkpoint_id) for a dead run.

        `interrupted` demands a pending ``__interrupt__`` on a checkpoint
        CREATED BY this run (CheckpointMetadata.run_id, stamped from the
        workflow's graph_config) — a predecessor's stale pending interrupt
        must not lend false resumability to a run that died pre-graph (I8).
        """
        tip_id: Optional[str] = None
        tip_matches = False
        pending_interrupts: List[Any] = []
        try:
            from src.server.app import setup

            saver = setup.checkpointer
            cp = (
                await saver.aget_tuple(
                    {"configurable": {"thread_id": thread_id}}
                )
                if saver
                else None
            )
            if cp is not None:
                tip_matches = (cp.metadata or {}).get("run_id") == run_id
                if tip_matches:
                    tip_id = cp.config["configurable"].get("checkpoint_id")
                    for _task, channel, value in cp.pending_writes or ():
                        if channel != "__interrupt__":
                            continue
                        pending_interrupts.extend(
                            value if isinstance(value, list) else [value]
                        )
        except Exception:
            logger.error(
                f"[RecoveryScanner] checkpoint read failed for {thread_id}; "
                f"classifying without it",
                exc_info=True,
            )

        if run.get("cancel_requested_at"):
            # The CAS adopts cancelled from the durable intent regardless;
            # requesting it just keeps the log honest.
            return "cancelled", None, None, tip_id

        if pending_interrupts:
            from src.server.contracts.status import (
                classify_interrupt_reason,
            )

            return (
                "interrupted",
                classify_interrupt_reason(pending_interrupts),
                None,
                tip_id,
            )

        return (
            "error",
            None,
            ["worker_lost: no live executor holds this run's writer fence"],
            tip_id,
        )

    # --------------------------------------------------------------- salvage

    async def _salvage_stream(
        self, thread_id: str, run_id: str
    ) -> Tuple[Optional[List[Dict[str, Any]]], str]:
        """Parse the run's Redis stream into the persisted sse_events shape.

        Best-effort archive (I7): the dead run's events would otherwise
        evaporate at stream TTL. Uses the same accumulator as live
        persistence so chunk merging matches owner-persisted turns.
        """
        try:
            from src.server.services.runs.sse_producer import StreamEventAccumulator
            from src.server.services.runs.stream_writer import stream_key
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not (cache.enabled and cache.client):
                return None, "redis_unavailable"
            # +1 proves truncation instead of inferring it from an exact
            # SALVAGE_MAX_EVENTS-length stream.
            entries = await cache.client.xrange(
                stream_key(thread_id, run_id), count=SALVAGE_MAX_EVENTS + 1
            )
        except Exception:
            logger.warning(
                f"[RecoveryScanner] stream salvage read failed for {run_id}",
                exc_info=True,
            )
            return None, "redis_unavailable"

        if not entries:
            return None, "empty_stream"
        truncated = len(entries) > SALVAGE_MAX_EVENTS
        if truncated:
            entries = entries[:SALVAGE_MAX_EVENTS]

        acc = StreamEventAccumulator()
        parsed = 0
        for _entry_id, fields in entries:
            wire = fields.get(b"event") or fields.get("event")
            frame = self._parse_wire_frame(wire)
            if frame is not None:
                acc.add(frame[0], frame[1])
                parsed += 1
        events = acc.get_events()
        if not events:
            return None, "unparseable_stream"
        if truncated:
            quality = "salvaged_truncated"
        elif parsed != len(entries):
            quality = "salvaged_partial"
        else:
            quality = "salvaged"
        logger.info(
            f"[RecoveryScanner] salvaged {parsed}/{len(entries)} stream "
            f"entries for run {run_id}"
        )
        return events, quality

    @staticmethod
    def _parse_wire_frame(wire: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
        if wire is None:
            return None
        try:
            text = (
                wire.decode("utf-8", errors="replace")
                if isinstance(wire, (bytes, bytearray))
                else str(wire)
            )
            event_type: Optional[str] = None
            data: Optional[Dict[str, Any]] = None
            for line in text.splitlines():
                if event_type is None and line.startswith("event: "):
                    event_type = line[7:].strip()
                elif data is None and line.startswith("data: "):
                    parsed = json.loads(line[6:])
                    if isinstance(parsed, dict):
                        data = parsed
            if event_type and data is not None:
                return event_type, data
        except Exception:
            return None
        return None

    # ------------------------------------------------------------ legacy heal

    async def heal_thread_projections(self) -> int:
        """One-time startup heal of stale live-spelling thread projections
        (SQL lives in the database layer)."""
        try:
            healed = await tl_db.heal_stale_thread_projections()
            if healed:
                logger.warning(
                    f"[RecoveryScanner] healed {healed} stale thread "
                    f"projection(s)"
                )
            return healed
        except Exception:
            logger.error(
                "[RecoveryScanner] thread projection heal failed", exc_info=True
            )
            return 0

    async def heal_task_chains(self) -> Dict[str, int]:
        """Per-cycle heal of task rows orphaned by a response-row cascade.

        The truncation paths repair their own thread transactionally; this
        catches damage from before that existed and from any path that ever
        escapes the guard. An anti-join over a small table, so running it
        every cycle costs nothing on a healthy ledger.
        """
        try:
            healed = await sr_db.repair_dangling_task_chains()
            if healed["rewound"] or healed["deleted"]:
                logger.warning(
                    f"[RecoveryScanner] healed task chains: "
                    f"rewound={healed['rewound']} deleted={healed['deleted']}"
                )
            return healed
        except Exception:
            logger.error("[RecoveryScanner] task chain heal failed", exc_info=True)
            return {"rewound": 0, "deleted": 0}
