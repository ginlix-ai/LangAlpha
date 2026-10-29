"""Per-path write locks, keyed by scope and absolute path.

The registry serialises writers inside one process. Across uvicorn workers the
same path is guarded by whatever :func:`install_cross_process_lock` supplied;
the server installs a Redis lease at startup, and with nothing installed (one
worker, the CLI, tests) exclusion stays process-local. Nothing here knows
about a sandbox; the caller supplies the scope that separates one sandbox's
paths from another's.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

# A factory returning an async context manager that holds the cross-process
# lease for ``(scope, path)`` while entered. Taken by the outermost holder only,
# so a re-entrant edit never waits on its own lease.
CrossProcessLockFactory = Callable[[str, str], AbstractAsyncContextManager[None]]

_CROSS_PROCESS_LOCK: CrossProcessLockFactory | None = None


def install_cross_process_lock(factory: CrossProcessLockFactory | None) -> None:
    """Set (or clear, with None) the cross-process lease taken around each write."""
    global _CROSS_PROCESS_LOCK
    _CROSS_PROCESS_LOCK = factory


# How many per-path locks the registry keeps. Sized to cover the files one
# computer's concurrent turns touch at once; past it, idle locks are dropped and
# rebuilt on the next write, which costs one allocation and loses nothing.
_MAX_TRACKED_PATH_LOCKS = 512


class _PathLockEntry:
    """One path's lock, its owning task, and the count of tasks holding or waiting."""

    __slots__ = ("lock", "loop", "owner", "users")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.lock = asyncio.Lock()
        self.loop = loop
        self.owner: asyncio.Task[Any] | None = None
        self.users = 0


class _PathLockRegistry:
    """Bounded LRU of write locks keyed by ``(sandbox id, absolute path)``.

    Eviction only ever drops an entry with no holder and no waiter, so a lock
    that is currently serialising writers is never pulled out from under them;
    if every entry is in use the map stays over its bound until they finish.
    """

    def __init__(self, capacity: int = _MAX_TRACKED_PATH_LOCKS) -> None:
        self._capacity = capacity
        self._entries: OrderedDict[tuple[str, str], _PathLockEntry] = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    def checkout(self, key: tuple[str, str]) -> _PathLockEntry:
        loop = asyncio.get_running_loop()
        entry = self._entries.get(key)
        if entry is None or (entry.users == 0 and entry.loop is not loop):
            # A lock left behind by a finished event loop cannot serialise
            # anything on this one, and nobody holds it, so replacing it is free.
            entry = _PathLockEntry(loop)
            self._entries[key] = entry
        entry.users += 1
        self._entries.move_to_end(key)
        self._evict()
        return entry

    def checkin(self, entry: _PathLockEntry) -> None:
        entry.users = max(0, entry.users - 1)
        if entry.users == 0:
            self._evict()

    def _evict(self) -> None:
        if len(self._entries) <= self._capacity:
            return
        for key in list(self._entries):
            if len(self._entries) <= self._capacity:
                return
            if self._entries[key].users == 0:
                del self._entries[key]


_PATH_LOCKS = _PathLockRegistry()


@asynccontextmanager
async def path_write_lock(scope: str, normalized_path: str) -> AsyncIterator[None]:
    """Serialise writers of one absolute path inside this process.

    One sandbox already serves concurrent threads and subagent fan-out, so a
    read-modify-write edit can interleave with another writer and lose an
    update.

    Re-entrant per task, because a read-modify-write edit holds the lock across
    its own nested write, which would otherwise deadlock on it.

    The process-local lock is taken first so one process sends one contender
    per path to the cross-process lease rather than every waiting task.
    """
    key = (scope, normalized_path)
    task = asyncio.current_task()
    entry = _PATH_LOCKS.checkout(key)

    if entry.owner is not None and entry.owner is task:
        try:
            yield
        finally:
            _PATH_LOCKS.checkin(entry)
        return

    try:
        await entry.lock.acquire()
    except BaseException:
        # Cancelling a turn while it waits here is routine; without this the
        # entry keeps a phantom user and can never be evicted again.
        _PATH_LOCKS.checkin(entry)
        raise

    entry.owner = task
    try:
        factory = _CROSS_PROCESS_LOCK
        if factory is None:
            yield
        else:
            async with factory(scope, normalized_path):
                yield
    finally:
        entry.owner = None
        entry.lock.release()
        _PATH_LOCKS.checkin(entry)
