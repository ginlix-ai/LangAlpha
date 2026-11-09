"""A computer's Claude Code CLIs held to the number its owner set.

The SSH transport starts any CLI its connections have a slot for, so the
computer's own count (``hosted.computer_clis``) is held here as each CLI
starts, among the CLIs this worker runs there.
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncIterator

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT
from ptc_agent.core.sandbox.providers._ssh_transport import SlotWaitTimeout

from .hosted import computer_clis


#: How often a CLI waiting for one reads the count anew, so one the owner
#: raised lets it in with no CLI ending.
POLL_S = 1.0


class _Held:
    """The CLIs this worker runs on one computer, and what wakes one waiting."""

    def __init__(self) -> None:
        self.n = 0
        self.freed = asyncio.Event()


#: Per event loop, so a count never crosses loops. Execution context only.
_held: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, _Held]] = weakref.WeakKeyDictionary()


@contextlib.asynccontextmanager
async def computer_cli_slot(computer_id: str, wait_s: float) -> AsyncIterator[None]:
    """Hold one of the CLIs the computer ``computer_id`` runs at once, waiting
    up to ``wait_s`` in all, reading the count included, for one to free. The
    count is read as this one starts and again after each wait, so one
    lowered meanwhile holds the CLIs already waiting.

    Raises:
        SlotWaitTimeout: None freed in time, naming the count last read.
    """
    deadline = asyncio.get_running_loop().time() + wait_s
    async with _in_worker(computer_id, deadline):
        yield


async def _count(computer_id: str, deadline: float, last: int) -> int:
    """The computer's count as it stands now, read by ``deadline``.

    Raises:
        SlotWaitTimeout: The read ran past it, naming ``last``, the count last read.
    """
    try:
        async with asyncio.timeout_at(deadline):
            return await computer_clis(computer_id)
    except TimeoutError:
        raise SlotWaitTimeout("cli", last) from None


@contextlib.asynccontextmanager
async def _in_worker(computer_id: str, deadline: float) -> AsyncIterator[None]:
    """One of the computer's CLIs in this worker, by ``deadline``, its count
    read again on each wake, as one here frees or every ``POLL_S``."""
    loop = asyncio.get_running_loop()
    held = _held.setdefault(loop, {}).setdefault(computer_id, _Held())
    clis = await _count(computer_id, deadline, CLIS_DEFAULT)
    while held.n >= clis:
        if loop.time() >= deadline:
            raise SlotWaitTimeout("cli", clis)
        held.freed.clear()
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout_at(min(loop.time() + POLL_S, deadline)):
                await held.freed.wait()
        clis = await _count(computer_id, deadline, clis)
    held.n += 1
    try:
        yield
    finally:
        held.n -= 1
        held.freed.set()
