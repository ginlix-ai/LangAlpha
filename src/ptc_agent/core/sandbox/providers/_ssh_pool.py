"""The pooled SSH connections of each event loop (``pool``).

One connection per login, reused by every runtime that reaches the machine,
and one more once Claude Code CLIs fill the first's CLI slots. The pool is
execution context only: whether a computer runs is read from the machine on
each call, so every worker agrees.
"""

from __future__ import annotations

import asyncio
import functools
import time
import weakref
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget, connect
from ptc_agent.core.sandbox.providers._ssh_transport import Link

logger = structlog.get_logger(__name__)

# A connection nothing has used for this long is closed, unless a channel on
# it is still open (a preview socket, say).
_IDLE_CLOSE_S = 600
_SWEEP_S = 60
# Channel opens refused in a row after which a CLI connection is replaced:
# sshd holds sessions of it that we no longer count.
_REFUSALS_REPLACED = 2


class SSHPool:
    """Pooled connections of one event loop, keyed by target identity."""

    def __init__(self) -> None:
        self._links: dict[tuple[Any, ...], Link] = {}
        self._dialing: dict[tuple[Any, ...], asyncio.Future[Link]] = {}
        # Each login's second connection, for CLIs past the first's slots,
        # and what keeps its dial one at a time.
        self._spilled: dict[tuple[Any, ...], Link] = {}
        self._spill_locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        # What each login's connections set as a CLI slot frees on any of them.
        self._cli_freed: dict[tuple[Any, ...], asyncio.Event] = {}
        self._sweeper: asyncio.Task[None] | None = None

    async def link(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        """The target's live connection, dialing it once however many ask.

        Raises:
            SSHConnectError: The machine could not be reached or refused us.
        """
        key = target.pool_key
        link = self._links.get(key)
        if link is not None and link.usable:
            return link
        pending = self._dialing.get(key)
        if pending is None:
            pending = asyncio.ensure_future(self._dial(target, load_key))
            self._dialing[key] = pending
            pending.add_done_callback(lambda _: self._dialing.pop(key, None))
        return await asyncio.shield(pending)

    async def _dial(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        link = await self._connected(target, load_key)
        link.spill = functools.partial(self._spill, target, load_key)
        old = self._links.get(target.pool_key)
        if old is not None:
            old.close()
        self._links[target.pool_key] = link
        self._start_sweeper()
        logger.debug("SSH connection opened", target=target.label)
        return link

    async def _connected(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        conn = await connect(target, await load_key())
        link = Link(target=target, conn=conn)
        try:
            await link.hello()
        except BaseException:
            link.close()
            raise
        link.cli_freed = self._cli_freed.setdefault(target.pool_key, asyncio.Event())
        return link

    async def _spill(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        """The login's second connection, for the CLIs past the first's
        slots: dialed once, and again once it is lost or keeps refusing
        channels, closed then once its CLIs are done, as a shed first
        connection is (``shed``). One per login, so a login runs at most
        two connections' CLI slots. A new one wakes the CLIs waiting for a
        slot (``cli_freed``), so one whose own dial failed takes one there.

        Raises:
            Exception: What dialing raised: the machine unreachable or
                refusing us (SSHConnectError), its HELLO too slow, the key
                not loading.
        """
        key = target.pool_key
        async with self._spill_locks.setdefault(key, asyncio.Lock()):
            link = self._spilled.get(key)
            if link is not None and link.usable and link.refusals < _REFUSALS_REPLACED:
                return link
            if link is not None:
                del self._spilled[key]
                link.drain()
            link = self._spilled[key] = await self._connected(target, load_key)
            link.cli_freed.set()
            self._start_sweeper()
            logger.debug("SSH connection opened for more CLIs", target=target.label)
            return link

    def evict(self, link: Link) -> None:
        """Drop *link* after a transport error, so the next call dials again."""
        if self._links.get(link.target.pool_key) is link:
            del self._links[link.target.pool_key]
        link.close()
        self._forget(link.target.pool_key)

    def shed(self, link: Link) -> None:
        """Dial anew for the next call, closing *link* once its calls are done.

        For a connection that works but will not open channels: closing it
        at once would cut off the commands still running on it.
        """
        if self._links.get(link.target.pool_key) is link:
            del self._links[link.target.pool_key]
        link.drain()
        self._forget(link.target.pool_key)

    def peek(self, target: SSHTarget) -> Link | None:
        """The target's connection if one is open, without dialing."""
        link = self._links.get(target.pool_key)
        return link if link is not None and link.usable else None

    def retire(self, target: SSHTarget, *, only_if_idle: bool = False) -> None:
        """Close the target's connections, as a stop does.

        Other computers on the same login share them, so ``only_if_idle``
        leaves one that is in use, or holds a forward, to the idle sweep.
        """
        spilled = self._spilled.get(target.pool_key)
        if spilled is not None and (not only_if_idle or spilled.idle()):
            del self._spilled[target.pool_key]
            spilled.close()
        link = self._links.get(target.pool_key)
        # A forward is a preview another computer on the login may be showing
        # between requests; the sweep closes it once nothing has used it.
        if link is not None and not (only_if_idle and (not link.idle() or link.forwards)):
            del self._links[target.pool_key]
            link.close()
        self._forget(target.pool_key)

    def _forget(self, key: tuple[Any, ...]) -> None:
        """Drop what a login's connections share (``_cli_freed``,
        ``_spill_locks``) once no connection of it is pooled or being dialed
        and no dial of its second one holds the lock. A CLI still waiting on
        a connection gone from the pool keeps that connection's event, which
        its CLIs ending still set."""
        lock = self._spill_locks.get(key)
        if key in self._links or key in self._spilled or key in self._dialing or (lock is not None and lock.locked()):
            return
        self._spill_locks.pop(key, None)
        self._cli_freed.pop(key, None)

    def _start_sweeper(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.ensure_future(self._sweep())

    async def _sweep(self) -> None:
        while self._links or self._spilled:
            await asyncio.sleep(_SWEEP_S)
            cutoff = time.monotonic() - _IDLE_CLOSE_S
            for links in (self._links, self._spilled):
                for key, link in list(links.items()):
                    if not link.usable:
                        del links[key]
                    elif link.idle() and link.last_used <= cutoff:
                        del links[key]
                        link.close()
                        logger.debug("Closed idle SSH connection", target=link.target.label)
                    else:
                        continue
                    self._forget(key)


_pools: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, SSHPool] = (
    weakref.WeakKeyDictionary()
)


def pool() -> SSHPool:
    """The running loop's pool. Connections never cross event loops."""
    loop = asyncio.get_running_loop()
    found = _pools.get(loop)
    if found is None:
        found = _pools[loop] = SSHPool()
    return found
