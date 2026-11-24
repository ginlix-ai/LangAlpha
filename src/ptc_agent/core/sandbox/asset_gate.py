"""Whether what a workspace's commands need on a computer has settled.

A turn starts once its first prompt can be built, which the computer's synced
files (tool modules, internal packages, skills and their links) are not needed
for. The host brings those up to date off the turn's path and holds this gate
while it does, and whatever the agent then does on the computer waits here.
A sync that writes to the computer replaces files every workspace on it
imports, so for the length of its writes it holds them all.

Execution context of one process: the host that holds the gate and the turn
that waits on it share the sandbox object. Another worker's turn has its own
object, and compares the computer's manifest for itself.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

#: The longest a call waits. Past it the call runs on whatever the computer
#: holds, as it does when the sync fails. Longer than the host lets one
#: catch-up run, so a wait ends by the release; only catch-ups queued one
#: behind another can outlast it.
MAX_WAIT_S = 240.0

#: The key of a hold on the whole computer. Workspace ids are UUIDs, and the
#: empty key is a caller that names no workspace.
_COMPUTER = "*"


class AssetGate:
    """Holds per workspace, and on the whole computer, which every workspace
    waits on; a workspace neither holds is settled."""

    def __init__(self) -> None:
        self._holds: dict[str, set[asyncio.Event]] = {}

    def hold(self, workspace_id: str | None) -> Callable[[], None]:
        """Hold ``workspace_id``'s calls until the returned release is called.
        The release is idempotent."""
        return self._hold(workspace_id or "")

    def hold_computer(self) -> Callable[[], None]:
        """Hold every workspace's calls until the returned release is called."""
        return self._hold(_COMPUTER)

    def _hold(self, key: str) -> Callable[[], None]:
        held = asyncio.Event()
        self._holds.setdefault(key, set()).add(held)

        def release() -> None:
            held.set()
            holds = self._holds.get(key)
            if holds is None:
                return
            holds.discard(held)
            if not holds:
                del self._holds[key]

        return release

    def _held(self, workspace_id: str | None) -> set[asyncio.Event]:
        empty: set[asyncio.Event] = set()
        return self._holds.get(workspace_id or "", empty) | self._holds.get(
            _COMPUTER, empty
        )

    def settled(self, workspace_id: str | None) -> bool:
        return not self._held(workspace_id)

    async def wait(
        self, workspace_id: str | None, *, timeout: float | None = MAX_WAIT_S
    ) -> float:
        """Wait until nothing holds ``workspace_id``, and say how long that
        took in seconds; 0.0 when it was settled already. Never raises: a
        hold that outlives ``timeout`` is waited for no longer. With no
        ``timeout`` the wait ends only by the releases."""
        return await self._wait(lambda: self._held(workspace_id), timeout)

    async def wait_all(self, *, timeout: float | None = MAX_WAIT_S) -> float:
        """``wait`` for a caller at the computer's root, which is in no one
        workspace and reads every folder: until nothing there is held."""
        return await self._wait(
            lambda: set().union(*self._holds.values()), timeout
        )

    async def _wait(
        self, held: Callable[[], set[asyncio.Event]], timeout: float | None
    ) -> float:
        if not held():
            return 0.0
        began = time.monotonic()
        deadline = None if timeout is None else began + timeout
        # A hold taken while this waits (a second catch-up queued behind the
        # first) is waited for too, so the loop reads the set again.
        while holds := held():
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            waits = [asyncio.ensure_future(event.wait()) for event in holds]
            try:
                await asyncio.wait(waits, timeout=remaining)
            finally:
                for waiting in waits:
                    waiting.cancel()
        return time.monotonic() - began
