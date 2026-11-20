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

from ptc_agent.core.claude_code_clis import CLIS_MAX, CliCount, CliQueue
from ptc_agent.core.sandbox.providers._ssh_transport import SlotWaitTimeout
from src.llms.claude_code import CliWait
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


class _WaitNotice:
    """Tells a CLI's ``CliQueue`` that it waits, once, as it first finds the
    slots full, then how long it waited; from what the count read had, so
    nothing between finding them full and waiting for one awaits."""

    def __init__(self, computer_id: str, queue: CliQueue | None) -> None:
        self.computer_id = computer_id
        self.queue = queue
        self.since: float | None = None

    def full(self, running: int, count: CliCount) -> None:
        if self.queue is None or self.since is not None:
            return
        self.since = asyncio.get_running_loop().time()
        wait = CliWait(computer_id=self.computer_id, name=count.name, running=running, cap=count.cap,
                       settable=count.settable, max=CLIS_MAX)
        try:
            self.queue.waiting(wait)
        except Exception:  # noqa: BLE001 - a notice is never worth failing a turn's start for
            logger.debug("[claude_code] queue notice not sent", exc_info=True)

    def ended(self, *, admitted: bool) -> None:
        """Tell the queue the wait ended, once, whether or not it got a slot,
        so a wait that timed out or was cancelled takes its line down too."""
        if self.queue is None or self.since is None:
            return
        waited = asyncio.get_running_loop().time() - self.since
        self.since = None
        logger.info(f"[claude_code] a CLI waited {waited:.0f}s for a slot on computer_id={self.computer_id}"
                    f"{'' if admitted else ' and gave up'}")
        try:
            (self.queue.admitted if admitted else self.queue.gave_up)(waited)
        except Exception:  # noqa: BLE001
            logger.debug("[claude_code] queue notice not sent", exc_info=True)


@contextlib.asynccontextmanager
async def computer_cli_slot(computer_id: str, wait_s: float, queue: CliQueue | None = None) -> AsyncIterator[None]:
    """Hold one of the CLIs the computer ``computer_id`` runs at once, in this
    worker and then across every worker, waiting up to ``wait_s`` in all,
    reading the count included, for one to free. The count is read as this
    one starts and again before each try after a wait, so one lowered
    meanwhile holds the CLIs already waiting, and one raised lets them in.
    ``queue`` is told once the slots are found full and once the wait ends,
    with a slot or without one. Its renewal ends before its slot is freed,
    so none still under way lands after and holds the slot again.

    Raises:
        SlotWaitTimeout: None freed in time, naming the count last read.
        TimeoutError: The count was never read in time (``_count``).
    """
    deadline = asyncio.get_running_loop().time() + wait_s
    told = _WaitNotice(computer_id, queue)
    try:
        async with _in_worker(computer_id, deadline, told) as count:
            client = _redis()
            key = slots_key(computer_id)
            slot_id = await _take(client, computer_id, key, count, deadline, told) if client is not None else None
            told.ended(admitted=True)
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
    finally:
        told.ended(admitted=False)


def _timed_out(computer_id: str, count: CliCount) -> SlotWaitTimeout:
    """A wait's end without a slot, at the count last read: the computer and
    whether its owner sets that count, for the refusal's offer to raise it."""
    return SlotWaitTimeout("cli", count.cap, computer_id=computer_id, settable=count.settable)


async def _count(computer_id: str, deadline: float, last: CliCount | None) -> CliCount:
    """The computer's count as it stands now, read by ``deadline``.

    Raises:
        SlotWaitTimeout: The read ran past it, naming ``last``, the count last read.
        TimeoutError: The first read (``last`` None) ran past it: no count was
            ever seen, so the computer is not known to be full, and the start
            fails as this service's own failure rather than as a full computer.
    """
    try:
        async with asyncio.timeout_at(deadline):
            return await computer_clis(computer_id)
    except TimeoutError:
        if last is None:
            raise TimeoutError(f"the CLI count of computer_id={computer_id} gave no answer within the CLI's wait") from None
        raise _timed_out(computer_id, last) from None


@contextlib.asynccontextmanager
async def _in_worker(computer_id: str, deadline: float, told: _WaitNotice) -> AsyncIterator[CliCount]:
    """One of the computer's CLIs in this worker, by ``deadline``, its count
    read again on each wake, as one here frees or every ``POLL_S``; it yields
    the count it was let in on."""
    loop = asyncio.get_running_loop()
    held = _held.setdefault(loop, {}).setdefault(computer_id, _Held())
    count = await _count(computer_id, deadline, None)
    while held.n >= count.cap:
        told.full(held.n, count)
        if loop.time() >= deadline:
            raise _timed_out(computer_id, count)
        held.freed.clear()
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout_at(min(loop.time() + POLL_S, deadline)):
                await held.freed.wait()
        count = await _count(computer_id, deadline, count)
    held.n += 1
    try:
        yield count
    finally:
        held.n -= 1
        held.freed.set()


async def _take(client: Any, computer_id: str, key: str, count: CliCount, deadline: float, told: _WaitNotice) -> str | None:
    """A slot's id once one is free by ``deadline``, of the computer's count
    ``count``, read again before each try after the first; None where Redis
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
                admitted = await _admit(client, key, count.cap)
        except Exception as e:  # noqa: BLE001 - coordination only; each worker still holds its own CLIs
            if full and isinstance(e, TimeoutError):
                # The wait ran out mid-try, the slots full when last read.
                raise _timed_out(computer_id, count) from None
            if not full:
                if isinstance(e, TimeoutError):
                    logger.warning("[claude_code] CLI slots gave no answer within the CLI's wait, holding only this worker's")
                else:
                    logger.warning(f"[claude_code] CLI slots unavailable, holding only this worker's: {e}")
                return None
            if not warned:
                logger.warning(f"[claude_code] CLI slots unavailable after finding them full, waiting on: {str(e) or type(e).__name__}")
                warned = True
        else:
            if admitted.allowed:
                return admitted.slot_id
            full = True
            told.full(admitted.current, count)
        left = deadline - loop.time()
        if left <= 0:
            raise _timed_out(computer_id, count)
        await asyncio.sleep(min(POLL_S, left))
        count = await _count(computer_id, deadline, count)


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
