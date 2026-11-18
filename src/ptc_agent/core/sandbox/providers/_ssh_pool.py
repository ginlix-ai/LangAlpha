"""The pooled SSH connections of each event loop (``pool``).

One connection per login, reused by every runtime that reaches the machine,
and more, up to ``_MAX_CONNECTIONS``, once Claude Code CLIs or long commands
fill the first's slots. The pool is execution context only: whether a
computer runs is read from the machine on each call, so every worker agrees.
"""

from __future__ import annotations

import asyncio
import functools
import time
import weakref
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import structlog

from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget, answered_by_helper, connect
from ptc_agent.core.sandbox.providers._ssh_transport import Link

logger = structlog.get_logger(__name__)

# A connection nothing has used for this long is closed, unless a channel on
# it is still open (a preview socket, say).
_IDLE_CLOSE_S = 600
_SWEEP_S = 60
# The most connections a login runs, the first included: room for a
# computer's most CLIs (``CLIS_MAX``) on sshd's sessions and a command for
# each, which a helper's one connection holds alone.
_MAX_CONNECTIONS = 8


class SSHPool:
    """Pooled connections of one event loop, keyed by target identity."""

    def __init__(self) -> None:
        self._links: dict[tuple[Any, ...], Link] = {}
        self._dialing: dict[tuple[Any, ...], asyncio.Future[Link]] = {}
        # Each login's connections past the first, for CLIs and commands the
        # first has no room for, and what keeps their dials one at a time.
        self._more: dict[tuple[Any, ...], list[Link]] = {}
        self._spill_locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        # What each login's connections set as a slot frees on any of them.
        self._freed: dict[tuple[Any, ...], asyncio.Event] = {}
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
        link = Link(target=target, conn=conn, helper=answered_by_helper(conn))
        try:
            await link.hello()
        except BaseException:
            link.close()
            raise
        link.freed = self._freed.setdefault(target.pool_key, asyncio.Event())
        link.peers = functools.partial(self._peers, target.pool_key)
        return link

    def _peers(self, key: tuple[Any, ...]) -> list[Link]:
        return list(self._more.get(key, ()))

    async def _spill(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]], want: Literal["cli", "command"]
    ) -> Link | None:
        """A connection of the login past the first with room for a CLI or a
        command (``want``): one dialed already, else a new one while the
        login runs fewer than ``_MAX_CONNECTIONS``; None at the most, so the
        caller waits on the ones up. One lost or that keeps refusing
        channels is let go, closed once its calls are done, as a shed first
        connection is (``shed``), and counted until then. A new one wakes the CLIs and commands
        waiting for a slot (``freed``), so one whose own dial failed takes
        one there.

        Raises:
            Exception: What dialing raised: the machine unreachable or
                refusing us (SSHConnectError), its HELLO too slow, the key
                not loading.
        """
        key = target.pool_key
        async with self._spill_locks.setdefault(key, asyncio.Lock()):
            # One draining stays counted until it closes: it is still a
            # connection sshd and the machine carry.
            live = []
            for link in self._more.pop(key, ()):
                if not link.accepting:
                    link.drain()
                if link.usable:
                    live.append(link)
            if live:
                self._more[key] = live
            for link in live:
                # A CLI's channel takes a command slot too.
                if link.accepting and not link.slots.locked() and not (
                    want == "cli" and link.cli_slots.locked()
                ):
                    return link
            if 1 + len(live) >= _MAX_CONNECTIONS:
                return None
            link = await self._connected(target, load_key)
            self._more.setdefault(key, []).append(link)
            link.freed.set()
            self._start_sweeper()
            logger.debug("SSH connection opened for more CLIs and commands", target=target.label)
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
        at once would cut off the commands still running on it. Until it
        closes it stays among the login's further connections, taking no new
        calls but counted toward ``_MAX_CONNECTIONS``.
        """
        key = link.target.pool_key
        if self._links.get(key) is link:
            del self._links[key]
        link.drain()
        more = self._more.setdefault(key, [])
        if link.usable and link not in more:
            more.append(link)
        if not more:
            del self._more[key]
        self._forget(key)

    def peek(self, target: SSHTarget) -> Link | None:
        """The target's connection if one is open, without dialing."""
        link = self._links.get(target.pool_key)
        return link if link is not None and link.usable else None

    def retire(self, target: SSHTarget, *, only_if_idle: bool = False) -> None:
        """Close the target's connections, as a stop does.

        Other computers on the same login share them, so ``only_if_idle``
        leaves one that is in use, or holds a forward, to the idle sweep.
        """
        more = self._more.pop(target.pool_key, [])
        for link in more:
            if not only_if_idle or link.idle():
                link.close()
        if kept := [link for link in more if link.usable]:
            self._more[target.pool_key] = kept
        link = self._links.get(target.pool_key)
        # A forward is a preview another computer on the login may be showing
        # between requests; the sweep closes it once nothing has used it.
        if link is not None and not (only_if_idle and (not link.idle() or link.forwards)):
            del self._links[target.pool_key]
            link.close()
        self._forget(target.pool_key)

    def _forget(self, key: tuple[Any, ...]) -> None:
        """Drop what a login's connections share (``_freed``,
        ``_spill_locks``) once no connection of it is pooled or being dialed
        and no dial of its second one holds the lock. A CLI still waiting on
        a connection gone from the pool keeps that connection's event, which
        its CLIs ending still set."""
        lock = self._spill_locks.get(key)
        if key in self._links or key in self._more or key in self._dialing or (lock is not None and lock.locked()):
            return
        self._spill_locks.pop(key, None)
        self._freed.pop(key, None)

    def _start_sweeper(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.ensure_future(self._sweep())

    async def _sweep(self) -> None:
        while self._links or self._more:
            await asyncio.sleep(_SWEEP_S)
            cutoff = time.monotonic() - _IDLE_CLOSE_S
            for key, link in list(self._links.items()):
                if not link.usable:
                    del self._links[key]
                elif link.idle() and link.last_used <= cutoff:
                    del self._links[key]
                    link.close()
                    logger.debug("Closed idle SSH connection", target=link.target.label)
                else:
                    continue
                self._forget(key)
            for key, more in list(self._more.items()):
                kept = []
                for link in more:
                    if link.usable and link.idle() and link.last_used <= cutoff:
                        link.close()
                        logger.debug("Closed idle SSH connection", target=link.target.label)
                    elif link.usable:
                        kept.append(link)
                if len(kept) != len(more):
                    if kept:
                        self._more[key] = kept
                    else:
                        del self._more[key]
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
