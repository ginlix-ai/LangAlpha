"""Per-thread subagent run ledger service — the injected `run_ledger`.

Server-side façade the background-subagent middleware calls through the
registry (`registry.run_ledger`, same injection pattern as
`result_resolver`). Owns the three things the SQL layer doesn't: predecessor
resolution for resume chains, the v2 per-run stream's lane_open/run_end
anchors (STREAM_CONTRACT_V2.md), and the best-effort final checkpoint pin.
Admission failures surface as TaskRunRejected (defined next to the registry
so ptc_agent code can catch it without importing server modules).
"""

import json
import logging
import uuid
from typing import Any, Dict, Optional

from ptc_agent.agent.middleware.background_subagent.registry import TaskRunRejected

from src.server.database import subagent_runs as sr_db

logger = logging.getLogger(__name__)


def v2_stream_key(thread_id: str, task_run_id: str) -> str:
    """Immutable per-run stream: `subagent:stream:{thread}:{task_run_id}`.

    Shares the v1 prefix but cannot collide — v1 keys end in the 6-char
    task_id, v2 in a UUID. A resume creates a new run and a new stream;
    nothing resets or re-incarnates a key readers may hold cursors into.
    """
    return f"subagent:stream:{thread_id}:{task_run_id}"


class SubagentRunLedger:
    """Admission-authoritative task-run ledger for one thread."""

    def __init__(self, thread_id: str) -> None:
        self.thread_id = thread_id

    # ---------------------------------------------------------------- start

    async def start_task_run(
        self,
        *,
        task_id: str,
        cause: str,
        description: str = "",
        subagent_type: str = "general-purpose",
        parent_run_id: Optional[str] = None,
        launch_tool_call_id: Optional[str] = None,
    ) -> str:
        """Bear the run row (in_progress) and open its v2 stream; returns the
        new task_run_id.

        Called under the task's namespace guard, BEFORE the writer spawns.
        Any constraint conflict raises TaskRunRejected — the spawn/resume
        must not proceed; a ledger that tolerates conflicting writers is
        worse than none. Infra failures raise too (fail closed): a run we
        cannot record is a run we do not start.
        """
        task_run_id = str(uuid.uuid4())
        predecessor_run_id: Optional[str] = None
        start_checkpoint_id: Optional[str] = None

        if cause == "resume":
            task_row = await sr_db.get_task(self.thread_id, task_id)
            latest = task_row.get("latest_run_id") if task_row else None
            if latest is not None:
                pred = await sr_db.get_task_run(str(latest))
                if pred is not None:
                    predecessor_run_id = str(pred["task_run_id"])
                    # Run N+1's start pin = run N's final pin — this is what
                    # partitions namespace-wide private channels across
                    # resumes without a checkpointer read at spawn time.
                    start_checkpoint_id = pred.get("final_checkpoint_id")
            # latest is None for a pre-ledger task resumed after the ledger
            # shipped: the upsert adopts it with an unchained first run.

        try:
            run_row = await sr_db.start_task_run(
                task_run_id=task_run_id,
                thread_id=self.thread_id,
                task_id=task_id,
                cause=cause,
                description=description,
                subagent_type=subagent_type,
                parent_run_id=parent_run_id,
                launch_tool_call_id=launch_tool_call_id,
                predecessor_run_id=predecessor_run_id,
                start_checkpoint_id=start_checkpoint_id,
            )
        except sr_db.DuplicateLaunchError as e:
            raise TaskRunRejected(
                f"this Task call already spawned run "
                f"{e.existing_run.get('task_run_id')} (checkpoint re-execution)",
                existing=e.existing_run,
            ) from e
        except sr_db.TaskRunSlotBusyError as e:
            raise TaskRunRejected(
                f"task {task_id} already has a live run "
                f"({(e.active_run or {}).get('task_run_id', 'unknown')})",
                existing=e.active_run,
            ) from e
        except sr_db.PredecessorClaimedError as e:
            raise TaskRunRejected(
                f"task {task_id} was already resumed "
                f"(run {(e.successor or {}).get('task_run_id', 'unknown')})",
                existing=e.successor,
            ) from e

        try:
            await self._append_v2_frame(
                task_run_id,
                lane=f"task:{task_id}",
                frame_type="lane_open",
                payload={
                    "task_run_id": task_run_id,
                    "task_id": task_id,
                    "cause": cause,
                    "launch_tool_call_id": launch_tool_call_id,
                    "description": description,
                    "subagent_type": subagent_type,
                },
                required=True,
            )
        except Exception as e:
            # An anchorless stream must not start: without lane_open no
            # reader can causally order this run. Settle the just-born row
            # here rather than strand it for the scanner to reap.
            try:
                await sr_db.finalize_task_run_idempotent(
                    task_run_id=task_run_id,
                    status="error",
                    failure={
                        "error": f"lane_open append failed: {e}",
                        "error_type": "transport_lost",
                    },
                    final_checkpoint_id=None,
                )
            except Exception:
                logger.warning(
                    f"[subagent_ledger] could not settle run {task_run_id} "
                    f"after lane_open failure; scanner will reap it",
                    exc_info=True,
                )
            raise TaskRunRejected(
                "subagent event transport unavailable (lane_open failed)"
            ) from e

        # Push-style discovery: lane_open lives inside the stream it
        # announces, so attached consumers learn of the run here. Best
        # effort — ledger reconciliation is the backstop.
        from src.server.services.thread_control_stream import (
            announce_task_run_started,
        )

        await announce_task_run_started(
            self.thread_id,
            task_run_id=task_run_id,
            task_id=task_id,
            cause=cause,
            parent_run_id=parent_run_id,
        )
        return str(run_row["task_run_id"])

    # ------------------------------------------------------------- finalize

    async def finalize_task_run(
        self,
        task_run_id: str,
        status: str,
        *,
        task_id: Optional[str] = None,
        failure: Optional[Dict[str, Any]] = None,
        defer_run_end: bool = False,
    ) -> Dict[str, Any]:
        """One CAS to terminal, then the cursor-bearing run_end append
        (commit-then-signal). Idempotent: losing the CAS returns the
        survivor row and appends nothing.

        ``defer_run_end=True`` is for the run wrapper, whose steering sweep
        must land between the CAS and run_end — it appends via
        ``append_run_end`` after the sweep. Recovery paths keep the default
        immediate append (they have no sweep)."""
        final_checkpoint_id = (
            await self._read_task_checkpoint_tip(task_id) if task_id else None
        )
        result = await sr_db.finalize_task_run_idempotent(
            task_run_id=task_run_id,
            status=status,
            failure=failure,
            final_checkpoint_id=final_checkpoint_id,
        )
        if result["applied"]:
            run = result["run"]
            if not defer_run_end:
                await self._append_v2_frame(
                    task_run_id,
                    lane=f"task:{run['task_id']}",
                    frame_type="run_end",
                    payload={"outcome": run["status"]},
                    terminal=True,
                )
            if run["status"] == "completed" and run.get("parent_run_id"):
                # The report-back job committed with the CAS; wake the
                # drainer so a tail completion notifies promptly instead of
                # riding the poll interval. Best-effort — the job is durable.
                try:
                    from src.server.services.hook_outbox import HookOutboxDrainer

                    HookOutboxDrainer.get_instance().nudge()
                except Exception:
                    pass
        return result

    async def append_run_end(
        self, task_run_id: str, *, task_id: str, outcome: str
    ) -> None:
        """Cursor-bearing terminal frame for the wrapper's deferred path —
        appended after the steering sweep so steering_returned frames
        precede it and nothing follows it. Idempotent by last-frame
        inspection, so it is safe to race a recovery finalizer."""
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not (getattr(cache, "enabled", False) and cache.client):
                return
            key = v2_stream_key(self.thread_id, task_run_id)
            last = await cache.client.xrevrange(key, count=1)
            if last and last[0][1].get(b"type") == b"run_end":
                return
        except Exception:
            # An unreadable tail falls through to the append: a duplicate
            # run_end is inert (readers close on the first), a missing one
            # costs every reader the reconciliation backstop.
            logger.warning(
                f"[subagent_ledger] run_end idempotence read failed for "
                f"task_run={task_run_id}; appending anyway",
                exc_info=True,
            )
        await self._append_v2_frame(
            task_run_id,
            lane=f"task:{task_id}",
            frame_type="run_end",
            payload={"outcome": outcome},
            terminal=True,
        )

    async def mark_result_delivered(self, task_run_id: str) -> bool:
        return await sr_db.mark_result_delivered(task_run_id)

    async def get_latest_run(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Latest-chain run row for a task, or None (pre-ledger/unknown).

        The duck-typed read TaskOutput uses to answer honestly about a task
        with no live registry entry — running elsewhere, failed, cancelled,
        or completed-with-archive.
        """
        task_row = await sr_db.get_task(self.thread_id, task_id)
        latest = task_row.get("latest_run_id") if task_row else None
        if latest is None:
            return None
        return await sr_db.get_task_run(str(latest))

    async def request_task_run_cancel(self, task_run_id: str) -> Dict[str, Any]:
        """Durable cancel intent, thread-scoped. Stamped before the local
        writer is signalled so a worker that dies mid-unwind recovers as
        `cancelled`, not `worker_lost`. Idempotent; a row that settled first
        makes this a no-op (terminal is immutable)."""
        return await sr_db.request_task_run_cancel(
            task_run_id, thread_id=self.thread_id
        )

    # ------------------------------------------------------------- internals

    async def _read_task_checkpoint_tip(self, task_id: str) -> Optional[str]:
        """Best-effort final pin: the task namespace's checkpoint tip."""
        try:
            from src.server.app import setup

            saver = setup.checkpointer
            if saver is None:
                return None
            cp = await saver.aget_tuple(
                {
                    "configurable": {
                        "thread_id": self.thread_id,
                        "checkpoint_ns": f"task:{task_id}",
                    }
                }
            )
            if cp is not None:
                return cp.config["configurable"].get("checkpoint_id")
        except Exception:
            logger.warning(
                f"[subagent_ledger] checkpoint tip read failed for "
                f"task={task_id} thread={self.thread_id}",
                exc_info=True,
            )
        return None

    async def _append_v2_frame(
        self,
        task_run_id: str,
        *,
        lane: str,
        frame_type: str,
        payload: Dict[str, Any],
        terminal: bool = False,
        required: bool = False,
    ) -> None:
        """Contract-grade v2 append (STREAM_CONTRACT_V2.md): active streams
        carry no TTL — the attach-grace clock starts only at a terminal
        append. ``required`` propagates failure to the caller (lane_open:
        an anchorless stream must not start); other frames stay best-effort
        with the ledger row as the durable truth. seq is the XADD id.
        """
        try:
            from src.config.settings import get_redis_ttl_workflow_events
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not (getattr(cache, "enabled", False) and cache.client):
                # No Redis, no stream transport contract — a no-cache
                # deployment runs tasks without streams, so even required
                # frames are skipped rather than refused.
                return
            key = v2_stream_key(self.thread_id, task_run_id)
            await cache.client.xadd(
                key,
                {
                    b"run_id": task_run_id.encode(),
                    b"lane": lane.encode(),
                    b"type": frame_type.encode(),
                    b"payload": json.dumps(
                        payload, ensure_ascii=False, default=str
                    ).encode(),
                },
            )
            if terminal:
                await cache.client.expire(key, get_redis_ttl_workflow_events())
        except Exception:
            if required:
                raise
            logger.warning(
                f"[subagent_ledger] v2 {frame_type} append failed for "
                f"task_run={task_run_id}",
                exc_info=True,
            )
