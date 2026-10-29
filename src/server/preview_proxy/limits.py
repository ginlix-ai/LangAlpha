"""Per-sandbox bounds on what one worker's preview proxy holds open."""

from __future__ import annotations

import asyncio


class SandboxSlots:
    """At most *limit* of something at once for each sandbox, on this worker.

    A local resource bound, not status: it keeps one sandbox from taking the
    upstream pool, or the memory, every other user's previews share. A
    sandbox's entry exists only while something holds or waits for a slot.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._slots: dict[str, asyncio.Semaphore] = {}
        self._users: dict[str, int] = {}

    async def acquire(self, key: str, *, wait: float = 0) -> bool:
        """Take a slot, waiting up to *wait* seconds; False when none came free."""
        slot = self._slots.get(key)
        if slot is None:
            slot = self._slots[key] = asyncio.Semaphore(self._limit)
        self._users[key] = self._users.get(key, 0) + 1
        try:
            if wait <= 0:
                if slot.locked():
                    self._leave(key)
                    return False
                await slot.acquire()
            else:
                async with asyncio.timeout(wait):
                    await slot.acquire()
        except TimeoutError:
            self._leave(key)
            return False
        except BaseException:
            self._leave(key)
            raise
        return True

    def release(self, key: str) -> None:
        self._slots[key].release()
        self._leave(key)

    def _leave(self, key: str) -> None:
        left = self._users[key] - 1
        if left:
            self._users[key] = left
        else:
            del self._users[key]
            del self._slots[key]
