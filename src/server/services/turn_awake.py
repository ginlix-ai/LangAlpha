"""Holding a turn's computer out of idle sleep while the turn runs.

A Mac its owner left alone sleeps after a few minutes, and a turn spends
minutes at a time between calls on the machine, thinking or waiting on a
subagent: the Mac sleeps under it and the next call finds it gone. Commands
hold the Mac themselves while they run (the job wrapper's ``caffeinate``);
this covers the stretches between them. Each hold is timed and renewed well
before it runs out, so a turn that ends, or a server that dies, leaves the Mac
to sleep again within ``HOLD_S``. A machine that never sleeps on its own says
so at the first hold, and this turn does not ask it again; the next turn asks
afresh, so nothing about the machine outlives the turn's own task.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any, Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

#: How long one hold keeps the machine awake.
HOLD_S = 300
#: How often a running turn renews it: twice inside a hold, so one renewal
#: lost to a slow link does not let the machine sleep.
RENEW_S = 120


async def _hold(runtime_of: Callable[[], Optional[Any]]) -> None:
    while True:
        runtime = runtime_of()
        if runtime is not None:
            try:
                if not await runtime.hold_awake(HOLD_S):
                    return
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - the next renewal tries again
                logger.debug(f"[TURN_AWAKE] hold not renewed: {type(e).__name__}: {e}")
        await asyncio.sleep(RENEW_S)


async def held_awake(
    runtime_of: Callable[[], Optional[Any]], stream: AsyncIterator[Any]
) -> AsyncIterator[Any]:
    """Drive ``stream`` while the machine ``runtime_of`` names is held awake.

    ``runtime_of`` is read at each renewal, since a reconnect mid-turn
    replaces the session's runtime."""
    task = asyncio.create_task(_hold(runtime_of), name="turn-awake")
    try:
        async with contextlib.aclosing(stream):
            async for event in stream:
                yield event
    finally:
        await cancel_and_join(task)
