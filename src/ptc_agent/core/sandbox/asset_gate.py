"""Whether what a workspace's commands need on a computer has settled.

A turn starts once its first prompt can be built, which the computer's synced
files (tool modules, internal packages, skills and their links) are not needed
for. The host brings those up to date off the turn's path and holds this gate
while it does, and whatever the agent then does on the computer waits here.

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


class AssetGate:
    """Holds per workspace; a workspace nothing holds is settled."""

    def __init__(self) -> None:
        self._holds: dict[str, set[asyncio.Event]] = {}

    def hold(self, workspace_id: str | None) -> Callable[[], None]:
        """Hold ``workspace_id``'s calls until the returned release is called.
        The release is idempotent."""
        key = workspace_id or ""
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

    def settled(self, workspace_id: str | None) -> bool:
        return not self._holds.get(workspace_id or "")

    async def wait(
        self, workspace_id: str | None, *, timeout: float | None = MAX_WAIT_S
    ) -> float:
        """Wait until nothing holds ``workspace_id``, and say how long that
        took in seconds; 0.0 when it was settled already. Never raises: a
        hold that outlives ``timeout`` is waited for no longer. With no
        ``timeout`` the wait ends only by the releases."""
        key = workspace_id or ""
        if not self._holds.get(key):
            return 0.0
        began = time.monotonic()
        deadline = None if timeout is None else began + timeout
        # A hold taken while this waits (a second catch-up queued behind the
        # first) is waited for too, so the loop reads the set again.
        while holds := self._holds.get(key):
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            waits = [asyncio.ensure_future(held.wait()) for held in holds]
            try:
                await asyncio.wait(waits, timeout=remaining)
            finally:
                for waiting in waits:
                    waiting.cancel()
        return time.monotonic() - began
