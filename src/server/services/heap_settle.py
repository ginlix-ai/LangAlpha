"""Freezing what a worker's turns leave behind for good, once it is idle.

Startup freezes what importing the app left. A worker's turns then load much
more that never dies: modules imported on first use (hundreds for one model
SDK), pydantic schemas built the first time a model is used, caches. A worker
some hours old carries 200k to 300k such objects, and every full collection
walks them all while the event loop is stopped, which is most of each pause.
Freezing takes them out of the collector.

Only while no run's task, computer wait or subagent writer is still going on
this worker: a frozen object is never examined again, so a live turn's
reference cycles frozen with the rest would outlive it. A turn still being
set up before its run registers can be caught; it then counts as a turn run
since, so the next settle frees what it left. Idle, the collect before the
freeze frees the last turns' garbage. Some of what is alive then still dies
later, a finished run's record a day on, and a frozen object that dies inside
a reference cycle is never freed; so each freeze thaws the last one first,
and the collect frees what died since. Rarer code paths keep loading for
hours, so it freezes again after more turns, at most once per interval.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import time

from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs.executor import LocalRunExecutor

logger = logging.getLogger(__name__)

_CHECK_S = 60.0
_INTERVAL_S = 1800.0


async def run() -> None:
    """Freeze at the first idle check after turns ran, then at most once per
    ``_INTERVAL_S`` after more did."""
    executor = LocalRunExecutor.get_instance()
    tasks = BackgroundRegistryStore.get_instance()
    frozen_at_runs = executor.runs_started
    frozen_at: float | None = None
    while True:
        await asyncio.sleep(_CHECK_S)
        if executor.runs_started == frozen_at_runs:
            continue
        if frozen_at is not None and time.monotonic() - frozen_at < _INTERVAL_S:
            continue
        # The check and the freeze run with no await between them, so no
        # turn can start in between.
        if executor.has_live_runs() or tasks.has_live_tasks():
            continue
        before = gc.get_freeze_count()
        started = time.perf_counter()
        gc.unfreeze()
        gc.collect()
        gc.freeze()
        frozen_at_runs, frozen_at = executor.runs_started, time.monotonic()
        logger.info(
            f"Froze {gc.get_freeze_count()} objects out of the GC, {before} before, "
            f"after {frozen_at_runs} turns, in {(time.perf_counter() - started) * 1000:.0f} ms"
        )
