"""Idle compaction: the summary a thread idle after a completed turn gets
while its prompt cache is still warm (``TurnStartRebuilds.idle``).

A completed turn's outbox job arms one firing per thread in
``idle_compactions``, ``lead_seconds`` before the cache entry the summary
would read expires, from what the turn's final answer left in the checkpoint
(``IdleClock``). Every worker's sweeper claims the firings that come due by
deleting their rows, so no two workers fire one, and none is retried. A
worker runs at most ``MAX_FIRINGS`` at once: each holds a sandbox session
and a model call for up to the compaction timeout, and a deploy makes many
threads due together. A slot is claimed again as soon as its firing ends,
and a firing whose deadline passed unclaimed is dropped without a call. The firing itself is the caller's (``start``), since
it builds the thread's agent.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Optional

from ptc_agent.agent.middleware.compaction import idle
from ptc_agent.agent.middleware.compaction.idle import IDLE_DEADLINE_MARGIN_S
from ptc_agent.agent.middleware.compaction.measure import measured_tokens
from ptc_agent.agent.middleware.compaction.rebuild import IDLE_TTL_SECONDS, LAST_ANSWER_KEY, cold_threshold
from ptc_agent.agent.middleware.compaction.types import IdleClock
from ptc_agent.config.agent import CompactionConfig
from src.server.database import idle_compactions as idle_db
from src.server.database.runs.hook_jobs import IDLE_COMPACT_HOOK

logger = logging.getLogger(__name__)

#: How long before its computer's idle stop a firing must start: the summary
#: may take the whole compaction timeout.
STOP_MARGIN_S = 300
MAX_FIRINGS = 2
INTERVAL = 15.0
# Spread so the workers' passes do not land on the database together.
JITTER = 3.0
STOP_GRACE = 10.0

Fire = Callable[[dict[str, Any]], Awaitable[None]]


def log_idle(event: str, thread_id: str, reason: str, kind: str | None = None, **fields: Any) -> None:
    """One line for each arm, skip, discard and install, in the shape of
    compaction's other decisions, so each can be counted from the logs."""
    extra = "".join(f" {key}={value}" for key, value in fields.items())
    logger.info(
        "[Compaction] idle=%s reason=%s kind=%s thread_id=%s agent=main%s",
        event,
        reason,
        kind or "-",
        thread_id,
        extra,
    )


@dataclass(frozen=True)
class Firing:
    #: ``I1``, a rebuild keeping no turns, read off the last answer's entry;
    #: ``I2``, a summary keeping the tail, read off the entry at its cut.
    kind: Literal["I1", "I2"]
    fire_at: datetime
    #: Past this the claim is dropped without a call: the entry would expire
    #: under the summary, which would write the history at the write rate.
    deadline: datetime


def _at(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def plan_firing(
    clock: IdleClock,
    answer: Mapping[str, Any],
    *,
    tokens: int,
    answered_at: float,
    stop_at: datetime | None,
    lead: int,
) -> Firing | str:
    """When the thread's idle compaction fires, or why it is not armed.

    Only under the threshold (a thread at it summarizes at its next call
    anyway), and from the least size either summary runs at, as the turn
    resolved them for the user (``answer``, ``rebuild.answer_reading``):
    ``floor_tokens`` for a rebuild, which ``cold_threshold`` allows too, and
    ``idle_tokens`` (``idle_compact.threshold_tokens``, else
    ``cold_compact_tokens``) for one keeping the tail, none where the user
    turned the summary after a break off. A firing comes ``lead`` seconds before its entry
    expires and ``STOP_MARGIN_S`` before the computer's idle stop
    (``stop_at``, None for one never stopped), and only while that leaves
    the thread idle for two thirds of the usual gap: a closer slot is a
    pause to read the answer, whose next message would find the cache warm,
    or a computer stopping soon after it. A summary keeping the tail is
    tried first; past that floor, a rebuild, whose entry is the newer one.
    """
    ttl = IDLE_TTL_SECONDS
    threshold = answer.get("threshold") or 0
    if tokens >= threshold:
        return "over_threshold"
    floor = answer.get("floor_tokens") or 0
    cold = tokens >= cold_threshold(threshold, answer.get("cold_tokens") or 0)
    entries: list[tuple[Literal["I1", "I2"], float]] = []
    tail = clock.get("tail_requested_at")
    if tokens >= cold_threshold(threshold, answer.get("idle_tokens") or 0) and tail is not None:
        entries.append(("I2", float(tail)))
    if 0 < floor <= tokens or cold:
        entries.append(("I1", float(clock["requested_at"])))
    if not entries:
        return "under_floor"
    stop = stop_at.timestamp() - STOP_MARGIN_S if stop_at is not None else math.inf
    for kind, entry in entries:
        fire_at = min(entry + ttl - lead, stop)
        if fire_at - answered_at >= (ttl - lead) * 2 / 3:
            return Firing(kind, _at(fire_at), _at(entry + ttl - IDLE_DEADLINE_MARGIN_S))
    return "idle_stop" if stop < entries[0][1] + ttl - lead else "reading_pause"


async def arm_after_turn(job: dict[str, Any]) -> None:
    """The ``idle_compact`` outbox job: arm the thread's firing after its
    completed turn, or drop the one an earlier turn armed when this one
    leaves nothing to fire. Only for the thread's latest run: a job that runs
    late leaves the later turn's row alone."""
    from src.server.app import setup

    payload = job.get("payload") or {}
    thread_id = str(payload["thread_id"])
    settings = setup.agent_config.compaction if setup.agent_config is not None else None
    if settings is None or not idle.enabled():
        return
    reason, run_seq = await _arm(thread_id, payload, settings)
    if reason is None:
        return
    if reason != "superseded":
        await idle_db.disarm(thread_id, run_seq)
    # Every turn no Claude answer ended has none: not news.
    if reason != "no_clock":
        log_idle("skip", thread_id, reason)


async def _arm(thread_id: str, payload: Mapping[str, Any], settings: CompactionConfig) -> tuple[str | None, int]:
    """Arm the firing and return no reason, else say why not; with the
    arming run's ``run_seq``, which bounds what a disarm may drop."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.database.conversation import threads_read
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services.workspace_manager import WorkspaceManager
    from src.server.utils.checkpoint_helpers import (
        build_checkpoint_config,
        get_checkpointer,
        interrupt_records,
    )

    run_id = str(payload["run_id"])
    latest = await tl_db.get_latest_attempt(thread_id)
    if latest is None or str(latest.get("conversation_response_id")) != run_id:
        return "superseded", 0
    run_seq = int(latest["run_seq"])
    pointer = (await threads_read.get_thread_branch_read(thread_id)).checkpoint_id
    saved = await get_checkpointer().aget_tuple(build_checkpoint_config(thread_id, pointer)) if pointer else None
    if saved is None:
        return "no_checkpoint", run_seq
    if interrupt_records(saved.pending_writes):
        return "waiting_on_user", run_seq
    values = saved.checkpoint.get("channel_values") or {}
    clock = values.get("_idle_clock")
    answer = values.get(LAST_ANSWER_KEY)
    if not clock or not answer:
        return "no_clock", run_seq
    started = latest.get("created_at")
    if isinstance(started, datetime) and clock["requested_at"] < started.timestamp():
        return "stale_clock", run_seq
    workspace_id = payload.get("workspace_id")
    if not workspace_id:
        return "no_workspace", run_seq
    computer = await get_computer_for_workspace(workspace_id)
    if computer is None or computer.get("status") != "running":
        return "computer_stopped", run_seq
    window = WorkspaceManager.get_instance().idle_stop_window(computer)
    firing = plan_firing(
        clock,
        answer,
        tokens=measured_tokens(values) or 0,
        answered_at=values.get("_last_model_response_at") or clock["requested_at"],
        stop_at=window.stop_at if window is not None else None,
        lead=settings.idle_compact.lead_seconds,
    )
    if isinstance(firing, str):
        return firing, run_seq
    await idle_db.arm(
        thread_id,
        kind=firing.kind,
        fire_at=firing.fire_at,
        deadline=firing.deadline,
        base_checkpoint_id=pointer,
        armed_by_run_id=run_id,
        armed_run_seq=run_seq,
        user_id=str(payload["user_id"]),
    )
    log_idle("arm", thread_id, "completed", firing.kind, fire_at=firing.fire_at.isoformat())
    return None, run_seq


def register_outbox_executors() -> None:
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(IDLE_COMPACT_HOOK, arm_after_turn)


class IdleCompactionSweeper:
    """Every worker's claim on the firings that came due (see the module)."""

    _instance: Optional["IdleCompactionSweeper"] = None

    @classmethod
    def get_instance(cls) -> "IdleCompactionSweeper":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        # Set by a stop and by each firing's end, whose slot is claimed again.
        self._wake = asyncio.Event()
        self._fire: Fire | None = None
        # This worker's own firings, for its cap: execution context only.
        self._firing: set[asyncio.Task] = set()

    async def start(self, fire: Fire) -> None:
        self._fire = fire
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._loop(), name="idle-compaction-sweeper")

    async def stop(self) -> None:
        """Let a pass under way end, logging what it claims, then cancel the
        firings, each of which logs its own end."""
        self._stopping.set()
        self._wake.set()
        loop, self._task = self._task, None
        if loop is not None:
            await asyncio.wait({loop}, timeout=STOP_GRACE)
            loop.cancel()
        firing = list(self._firing)
        for task in firing:
            task.cancel()
        if firing:
            await asyncio.wait(firing, timeout=STOP_GRACE)

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=INTERVAL + random.uniform(0, JITTER))
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stopping.is_set():
                return
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[Compaction] idle sweep failed", exc_info=True)

    async def sweep_once(self) -> int:
        """Claim as many due firings as this worker has room for, and start
        each in a task of its own; how many were claimed."""
        for row in await idle_db.drop_expired():
            log_idle("discard", row["thread_id"], "past_deadline", row["kind"])
        room = MAX_FIRINGS - len(self._firing)
        if room <= 0 or self._fire is None:
            return 0
        rows = await idle_db.claim_due(room)
        if self._stopping.is_set():
            # Claimed while stopping: dropped, as a firing is never retried.
            for row in rows:
                log_idle("discard", row["thread_id"], "shutdown", row["kind"])
            return 0
        for row in rows:
            task = asyncio.create_task(self._fire(row), name=f"idle-compaction-{row['thread_id'][:8]}")
            self._firing.add(task)
            task.add_done_callback(self._done)
        return len(rows)

    def _done(self, task: asyncio.Task) -> None:
        self._firing.discard(task)
        self._wake.set()
        if not task.cancelled() and task.exception() is not None:
            logger.error("[Compaction] idle firing failed", exc_info=task.exception())


async def start_sweeper(fire: Fire) -> bool:
    """Start this worker's sweeper with the ``idle_compaction`` feature on, and
    say whether it did: off, nothing armed while it was on fires either."""
    if not idle.enabled():
        return False
    await IdleCompactionSweeper.get_instance().start(fire)
    return True
