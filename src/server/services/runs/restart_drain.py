"""Graceful restart: hand this process's running work to the next process.

A drain starts when ``DRAIN_FILE`` is created after the process started,
which asks it to hand its work over before it is replaced, or when the process
gets SIGTERM, and every graceful shutdown runs one before it tears anything
down. It stops this worker's background subagents with a restart stop, then
cancels its running turns the way a shutdown does, marked so each finalize
records the turn for resume (``report_back.restart``), and records the
subagents a finished turn left running. It also stops the loops that start
turns, and a turn admitted after the drain began is cancelled the same way
the moment it registers, so all of it resumes on a process that is not
draining.

Draining is a property of the process, not shared state: every worker of a
process polls the same ``DRAIN_FILE``, and each drains only what it runs.
What the next process needs is all in Postgres: the run rows, the subagent
runs and the outbox job each finalize writes.
"""

import asyncio
import logging
import os
import signal
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

#: /health advertises this while the process can drain and resume.
CAPABILITY = "restart_resume_v1"
DRAIN_FILE = "/tmp/langalpha-drain"
_POLL_SECONDS = 1.0

# A ``DRAIN_FILE`` older than the process asked an earlier one to drain.
_STARTED_AT = time.time()
_draining = False
_drain_task: Optional[asyncio.Task] = None
_watch_task: Optional[asyncio.Task] = None


class Draining(RuntimeError):
    """A firing refused its turn because this process is draining; its
    executor hands it to the next process (``automation_executor``)."""


def draining() -> bool:
    """Whether this process is draining. Reads ``DRAIN_FILE`` too, since
    the watcher sees it only on its next poll, and a turn admitted in between
    must still be left to the next process."""
    return _draining or _drain_requested()


def _drain_requested() -> bool:
    try:
        return os.stat(DRAIN_FILE).st_mtime >= _STARTED_AT
    except OSError:
        return False


def begin(reason: str) -> asyncio.Task:
    """Start the drain once; every later call returns the same task."""
    global _draining, _drain_task
    _draining = True
    if _drain_task is None:
        _drain_task = asyncio.get_running_loop().create_task(
            _drain(reason), name="restart-drain"
        )
    return _drain_task


def start(loop: asyncio.AbstractEventLoop) -> None:
    """Watch for ``DRAIN_FILE``, and drain on SIGTERM ahead of the server's
    own shutdown, which waits out open streams before the lifespan runs."""
    global _watch_task
    if _watch_task is None:
        _watch_task = loop.create_task(_watch(), name="restart-drain-watch")
    if threading.current_thread() is not threading.main_thread():
        return
    previous = signal.getsignal(signal.SIGTERM)

    def on_sigterm(signum, frame) -> None:
        loop.call_soon_threadsafe(begin, "SIGTERM")
        if callable(previous):
            previous(signum, frame)

    signal.signal(signal.SIGTERM, on_sigterm)


async def stop() -> None:
    """Drain before the shutdown tears anything down, then stop watching."""
    global _watch_task
    if _watch_task is not None:
        _watch_task.cancel()
        await asyncio.gather(_watch_task, return_exceptions=True)
        _watch_task = None
    await begin("shutdown")


async def _watch() -> None:
    while not _drain_requested():
        await asyncio.sleep(_POLL_SECONDS)
    begin("drain requested")


async def _drain(reason: str) -> None:
    from ptc_agent.agent.middleware.background_subagent.registry import (
        CANCEL_UNWIND_TIMEOUT,
    )
    from src.server.database.runs.restart_resume import enqueue_for_stopped_tasks
    from src.server.services.background_registry_store import BackgroundRegistryStore
    from src.server.services.runs.executor import LocalRunExecutor

    logger.warning(f"[RestartDrain] Draining ({reason}): stopping work for the next process to resume")
    starters = asyncio.create_task(_stop_turn_starters())
    # Subagents first, and settled before their turns are cancelled: a turn's
    # own stop would otherwise kill them as the user's stop.
    threads: set[str] = set()
    try:
        writers, threads = await BackgroundRegistryStore.get_instance().stop_all_for_restart()
        if writers:
            await asyncio.wait(writers, timeout=CANCEL_UNWIND_TIMEOUT)
    except Exception:
        logger.warning("[RestartDrain] Stopping subagents failed", exc_info=True)
    executor = LocalRunExecutor.get_instance()
    runs = await executor.uncancelled_runs()
    for thread_id, run_id in runs:
        try:
            await executor.signal_cancel(
                thread_id, run_id, user_initiated=False, restart=True
            )
        except Exception:
            logger.warning(f"[RestartDrain] Cancelling run {run_id} failed", exc_info=True)
    for thread_id in threads:
        try:
            await enqueue_for_stopped_tasks(thread_id)
        except Exception:
            logger.warning(
                f"[RestartDrain] Recording stopped subagents on thread {thread_id} failed",
                exc_info=True,
            )
    logger.warning(
        f"[RestartDrain] Cancelled {len(runs)} turn(s) and stopped subagents "
        f"on {len(threads)} thread(s) for resume"
    )
    await starters


async def _stop_turn_starters() -> None:
    """Stop the loops that start turns here, so new ones start on a process
    that is not draining; one that slips through is cancelled as it starts."""
    from src.server.services.automation_scheduler import AutomationScheduler
    from src.server.services.hook_outbox import HookOutboxDrainer
    from src.server.services.price_monitor import PriceMonitorService

    for name, stop_loop in (
        ("HookOutboxDrainer", HookOutboxDrainer.get_instance().stop),
        ("AutomationScheduler", AutomationScheduler.get_instance().shutdown),
        ("PriceMonitorService", PriceMonitorService.get_instance().stop),
    ):
        try:
            await stop_loop()
        except Exception:
            logger.warning(f"[RestartDrain] Stopping {name} failed", exc_info=True)
