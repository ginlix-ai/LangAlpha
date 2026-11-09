"""A computer's Claude Code CLIs held to the number its owner set.

The SSH transport starts any CLI its connections have a slot for, so the
computer's own count (``hosted.computer_clis``) is held here as each CLI
starts: first within this worker, then across every worker, one Redis slot
per CLI (``slot_guard``), renewed while it runs and reaped once its worker
stops renewing it, so a worker that dies frees its CLIs' slots. Without Redis
each worker still holds its own CLIs to the count.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import weakref
from collections.abc import AsyncIterator
from typing import Any

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT
from ptc_agent.core.sandbox.providers._ssh_transport import SlotWaitTimeout
from src.server.utils.slot_guard import SlotAdmission, acquire_slot_member, release_slot_member, renew_slot_member
from src.utils.cache.redis_cache import get_cache_client

from .hosted import computer_clis

logger = logging.getLogger(__name__)

#: How long a slot its worker stopped renewing is held, how often a running
#: CLI renews its own, and how often a CLI waiting for one asks again and
#: reads the count anew, so one the owner raised lets it in with no CLI ending.
STALE_S = 120
RENEW_S = 30
POLL_S = 1.0


class _Held:
    """The CLIs this worker runs on one computer, and what wakes one waiting."""

    def __init__(self) -> None:
        self.n = 0
        self.freed = asyncio.Event()


#: Per event loop, so a count never crosses loops. Execution context only:
#: the count across workers is Redis's.
_held: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, _Held]] = weakref.WeakKeyDictionary()


def slots_key(computer_id: str) -> str:
    return f"claude_code:cli_slots:{computer_id}"


def _redis() -> Any | None:
    cache = get_cache_client()
    return cache.client if cache.enabled and cache.client else None


@contextlib.asynccontextmanager
async def computer_cli_slot(computer_id: str, wait_s: float) -> AsyncIterator[None]:
    """Hold one of the CLIs the computer ``computer_id`` runs at once, in this
    worker and then across every worker, waiting up to ``wait_s`` in all,
    reading the count included, for one to free. The count is read as this
    one starts and again before each try after a wait, so one lowered
    meanwhile holds the CLIs already waiting. Its renewal ends before its
    slot is freed, so none still under way lands after and holds the slot
    again.

    Raises:
        SlotWaitTimeout: None freed in time, naming the count last read.
    """
    deadline = asyncio.get_running_loop().time() + wait_s
    async with _in_worker(computer_id, deadline) as clis:
        client = _redis()
        key = slots_key(computer_id)
        slot_id = await _take(client, computer_id, key, clis, deadline) if client is not None else None
        renewing = asyncio.ensure_future(_renew(client, key, slot_id)) if slot_id else None
        try:
            yield
        finally:
            if renewing is not None:
                renewing.cancel()
                await asyncio.wait({renewing})
                try:
                    await release_slot_member(client, key, slot_id)
                except Exception as e:  # noqa: BLE001 - the slot is reaped once it goes stale
                    logger.warning(f"[claude_code] could not free a CLI slot of computer_id={computer_id}: {e}")


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
async def _in_worker(computer_id: str, deadline: float) -> AsyncIterator[int]:
    """One of the computer's CLIs in this worker, by ``deadline``, its count
    read again on each wake, as one here frees or every ``POLL_S``; it yields
    the count it was let in on."""
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
        yield clis
    finally:
        held.n -= 1
        held.freed.set()


async def _take(client: Any, computer_id: str, key: str, clis: int, deadline: float) -> str | None:
    """A slot's id once one is free by ``deadline``, of the computer's count
    ``clis``, read again before each try after the first; None where Redis
    fails, or answers no admission by then, before it ever answered, which
    holds nothing. Once it has found the slots full, a failure is a try like
    any other: the slots were full when last read, so the CLI waits on.

    Raises:
        SlotWaitTimeout: None freed in time.
    """
    loop = asyncio.get_running_loop()
    full = warned = False
    while True:
        try:
            async with asyncio.timeout_at(deadline):
                admitted = await _admit(client, key, clis)
        except Exception as e:  # noqa: BLE001 - coordination only; each worker still holds its own CLIs
            if not full:
                if isinstance(e, TimeoutError):
                    logger.warning("[claude_code] CLI slots gave no answer within the CLI's wait, holding only this worker's")
                else:
                    logger.warning(f"[claude_code] CLI slots unavailable, holding only this worker's: {e}")
                return None
            if not warned:
                logger.warning(f"[claude_code] CLI slots unavailable after finding them full, waiting on: {e or type(e).__name__}")
                warned = True
        else:
            if admitted.allowed:
                return admitted.slot_id
            full = True
        left = deadline - loop.time()
        if left <= 0:
            raise SlotWaitTimeout("cli", clis)
        await asyncio.sleep(min(POLL_S, left))
        clis = await _count(computer_id, deadline, clis)


async def _admit(client: Any, key: str, clis: int) -> SlotAdmission:
    """One admission. A wait cancelled while Redis admits it ends at once and
    frees the slot once it lands, so no slot is held that nothing will
    release."""
    admitting = asyncio.ensure_future(acquire_slot_member(client, key, limit=clis, stale_after=STALE_S))
    try:
        return await asyncio.shield(admitting)
    except asyncio.CancelledError:
        freeing = asyncio.ensure_future(_free_once_landed(client, key, admitting))
        _freeing.add(freeing)
        freeing.add_done_callback(_freeing.discard)
        raise


#: Admissions a cancelled wait left to land, kept until they free their slot.
_freeing: set[asyncio.Task[None]] = set()


async def _free_once_landed(client: Any, key: str, admitting: asyncio.Future[SlotAdmission]) -> None:
    with contextlib.suppress(Exception):
        landed = await admitting
        if landed.allowed:
            await release_slot_member(client, key, landed.slot_id)


async def _renew(client: Any, key: str, slot_id: str) -> None:
    while True:
        await asyncio.sleep(RENEW_S)
        try:
            await renew_slot_member(client, key, slot_id, stale_after=STALE_S)
        except Exception as e:  # noqa: BLE001 - one missed renewal leaves the slot until it goes stale
            logger.warning(f"[claude_code] could not renew a CLI slot: {e}")
