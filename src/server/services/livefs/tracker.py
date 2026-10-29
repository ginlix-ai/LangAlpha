"""This worker's view of one machine's file mount.

Asking the sandbox is an exec, so a turn asks only when something is due.
At a turn's start the view takes what every worker recorded (``load``): the
last start's answer on the token row and the folders links were laid for,
so a worker that saw nothing of the computer trusts another's answer rather
than asking again. Serving is due on another sandbox, on a sandbox object
never handed the mount (a reconnect's, until the rows say it serves), with
the token gone, or once a failure's backoff is over; a token that only runs
low still has half an hour in it, so it is renewed in the background
(``renew_soon``) while turns and commands go on with it. The links are due
for a workspace not linked on this sandbox, serving or not, and go in off
the turn's path once the daemon answered a serve (``link_soon``), which a
command in that workspace waits out (``linked``).

Commands never read the rows: they go on with what their turn's start read.
The daemon stays the truth, so a command that meets it dead restarts it
whatever the rows say. Backoffs, the tasks in flight and the lock are this
worker's own.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from src.server.services.livefs.tokens import lapsed, runs_low

if TYPE_CHECKING:
    from src.server.services.livefs.mount import LinkState, MountState, MountView

logger = logging.getLogger(__name__)

# Seconds until a failed mount is asked for again. No FUSE or no root is fixed
# for the sandbox's life. Installing libfuse, on a sandbox built before its
# image carried it, takes seconds, as does a folder settle holding the
# computer or an asset sync replacing the daemon's code. The usual other
# causes (a server the sandbox cannot reach) do not change between turns.
_RETRY_S = {
    MountError.UNSUPPORTED: math.inf,
    MountError.INSTALLING: 20,
    MountError.BUSY: 20,
    MountError.STALE_CODE: 20,
}
_FAILED_RETRY_S = 600
# Seconds until links the daemon did not answer for are asked again: a settle
# held the folders, a sync was replacing its code, or the exec lost its answer.
_LINK_RETRY_S = 20
# Seconds until folders whose links failed are linked again: what stood in
# the way on disk (a path the owner cannot write) stays until code moves it.
_LINK_FAILED_RETRY_S = 600
# Seconds until a renewal that renewed nothing is tried again. Mostly another
# worker was renewing, and its token is adopted on the next try.
_RENEW_RETRY_S = 15
# Seconds until a missing file under the mount asks the sandbox again
# whether the mount itself is gone (``probe_due``).
_PROBE_EVERY_S = 60


def sandbox_id(sandbox: Any) -> str | None:
    return str(getattr(sandbox, "sandbox_id", "") or "") or None


def _folder(workspace_id: str | None) -> str | None:
    return None if workspace_id is None else str(workspace_id)


class MountTracker:
    """What this worker last saw of one machine's mount, and when to ask again."""

    def __init__(self) -> None:
        # The sandbox everything below was seen on.
        self._sandbox_id: str | None = None
        # The last ``serve`` answer, and when to ask again after a failed one.
        self._seen: MountState | None = None
        self._retry_at = 0.0
        # The folders seen linked (None in it: the computer's own); None
        # until a link answers. A failed link holds off asking again until
        # ``_link_retry_at`` for the folders it failed for, or every folder
        # (None) when the daemon answered for none.
        self._linked: frozenset[str | None] | None = None
        self._link_retry_at = 0.0
        self._link_held: frozenset[str | None] | None = None
        # Workspaces ``unlinked`` since the running link read the folders,
        # which its answer does not bring back.
        self._dropped: set[str] = set()
        # When a command's missing file under the mount last had this worker
        # ask the sandbox whether the mount itself was gone.
        self._probed_at = -math.inf
        # Parallel commands (subagents) share one refresh.
        self._lock = asyncio.Lock()
        self._renewing: asyncio.Task | None = None
        self._renew_at = 0.0
        self._linking: asyncio.Task | None = None
        # The latest link asked for while one runs, with its sandbox, and the
        # workspaces every ask since was for.
        self._relink: tuple[Any, Callable[[], Awaitable[Any]]] | None = None
        self._relink_for: set[str | None] = set()

    def _reset_view(self) -> None:
        # The backoffs and the probe time answer for what was seen, so they
        # go with it.
        self._seen, self._linked, self._link_held = None, None, None
        self._retry_at = self._link_retry_at = self._renew_at = 0.0
        self._probed_at = -math.inf

    def _on(self, sandbox: Any) -> None:
        """Point the view at ``sandbox``: what was seen on another tells
        nothing about it."""
        if sandbox_id(sandbox) != self._sandbox_id:
            self._sandbox_id = sandbox_id(sandbox)
            self._reset_view()

    def load(self, sandbox: Any, view: MountView) -> bool:
        """Take what the rows say of ``sandbox`` at a turn's start over what
        this worker saw: another may have served, linked, renewed or stopped
        it since. This worker's backoffs stand only while the rows do not say
        it serves. Answers whether they say it serves with a token not gone,
        which hands a sandbox object never handed it the mount."""
        self._on(sandbox)
        seen = self._seen
        if view.serving:
            if seen is None or not seen.mounted or seen.expires_at != view.expires_at:
                self._seen = view.state()
            self._retry_at = 0.0
        elif seen is not None and seen.mounted:
            # Found down, stopped or replaced since: a serve asks the sandbox.
            self._seen = view.state()
            self._retry_at = 0.0
        self._linked = view.linked
        return view.serving and not lapsed(view.expires_at)

    def saw(self, sandbox: Any, state: MountState) -> None:
        """Record what ``serve`` answered on ``sandbox``."""
        now = time.monotonic()
        self._on(sandbox)
        if state.error == MountError.BUSY:
            # A settle kept the folders past the wait: nothing was asked, so
            # what was seen stands, and a serving mount serves on.
            self._retry_at = now + _RETRY_S[MountError.BUSY]
            return
        self._seen = state
        if state.mounted:
            self._retry_at = 0.0
        else:
            self._retry_at = now + _RETRY_S.get(state.error, _FAILED_RETRY_S)

    def saw_links(self, sandbox: Any, state: LinkState | None) -> None:
        """Record what ``link`` answered on ``sandbox`` (None: it raised),
        unless the machine was forgotten or moved to another sandbox while it
        ran."""
        dropped, self._dropped = self._dropped, set()
        if self._seen is None or sandbox_id(sandbox) != self._sandbox_id:
            return
        now = time.monotonic()
        if state is None or state.linked is None:
            self._link_retry_at, self._link_held = now + _LINK_RETRY_S, None
            return
        # The daemon laid the computer's whole folder list, so a workspace
        # it left out has left this computer.
        self._linked = state.linked - dropped
        self._link_held = state.failed
        self._link_retry_at = now + _LINK_FAILED_RETRY_S if state.failed else 0.0

    def unlinked(self, workspace_id: str) -> None:
        """Count ``workspace_id`` not linked until a link answers again: its
        folder here may be gone, or made anew without the links."""
        if self._linked is not None:
            self._linked = self._linked - {str(workspace_id)}
        if self.linking():
            self._dropped.add(str(workspace_id))

    def forget(self) -> None:
        # A renewal still running lands on nothing: it checks what it renewed
        # is still what is seen. A link still running would lay links on a
        # machine leaving service.
        if self._linking is not None:
            self._linking.cancel()
            self._linking = None
        self._relink, self._relink_for = None, set()
        self._dropped = set()
        self._sandbox_id = None
        self._reset_view()

    def serving(self, sandbox: Any) -> bool:
        """Whether this worker saw the mount serve on ``sandbox``, handed to
        it."""
        seen = self._seen
        return (
            seen is not None
            and seen.mounted
            and self._sandbox_id == sandbox_id(sandbox)
            and getattr(sandbox, "livefs", None) is not None
        )

    def serves(self, sandbox: Any, workspace_id: str | None) -> bool:
        """Whether the mount serves ``workspace_id``'s folder on ``sandbox``
        (None: the computer's root)."""
        linked = self._linked
        return (
            self.serving(sandbox)
            and linked is not None
            and _folder(workspace_id) in linked
        )

    def serve_due(self, sandbox: Any) -> bool:
        """Whether a turn on ``sandbox`` asks it to serve."""
        if self._sandbox_id != sandbox_id(sandbox):
            return True
        if time.monotonic() < self._retry_at:
            return False
        seen = self._seen
        # Down, a token gone, or up but this sandbox object (a reconnect's)
        # never handed the mount: only a read of the rows saying it serves,
        # or a serve, hands it one.
        return (
            seen is None
            or not seen.mounted
            or lapsed(seen.expires_at)
            or getattr(sandbox, "livefs", None) is None
        )

    def link_owed(self, sandbox: Any, workspace_id: str | None) -> bool:
        """Whether a link for ``workspace_id`` (None: the computer's own)
        is owed on ``sandbox``: once the mount serves there, or the daemon
        answered why not. A serve that never reached it (it raised, or a
        stop refused the token) says nothing of the sandbox. Unserved, the
        link dangles, so code that builds the path is refused rather than
        writing files the store never sees, which a later link sets aside."""
        seen, linked = self._seen, self._linked
        if (
            seen is None
            or not (seen.mounted or seen.error)
            or self._sandbox_id != sandbox_id(sandbox)
            or (linked is not None and _folder(workspace_id) in linked)
        ):
            return False
        held = self._link_held
        # A folder the failed link never read (one that joined since) is
        # not held off by it.
        return (
            time.monotonic() >= self._link_retry_at
            or (held is not None and _folder(workspace_id) not in held)
        )

    def linking(self) -> bool:
        """Whether a link handed to ``link_soon`` is still running."""
        return self._linking is not None and not self._linking.done()

    def link_soon(
        self,
        sandbox: Any,
        workspace_id: str | None,
        link: Callable[[], Awaitable[LinkState]],
    ) -> None:
        """Lay the links off the turn's path when owed. One asked for while
        another runs goes after it, folded with any others asked meanwhile
        into the latest, so a workspace that joined after the running one
        read the folders is not lost to that read."""
        if self.linking():
            self._relink = (sandbox, link)
            self._relink_for.add(workspace_id)
            return
        if self.link_owed(sandbox, workspace_id):
            self._linking = asyncio.get_running_loop().create_task(
                self._link(sandbox, link)
            )

    async def _link(
        self, sandbox: Any, link: Callable[[], Awaitable[LinkState]]
    ) -> None:
        while True:
            self._dropped = set()
            try:
                state = await link()
            except Exception:
                logger.warning("livefs link failed", exc_info=True)
                state = None
            self.saw_links(sandbox, state)
            relink, wanted = self._relink, self._relink_for
            self._relink, self._relink_for = None, set()
            if relink is None:
                return
            sandbox, link = relink
            # The one that ran may have linked what the rest were due for.
            if not any(self.link_owed(sandbox, w) for w in wanted):
                return

    async def linked(self) -> None:
        """Wait out the links still going in. A cancelled wait leaves them
        running, as other commands wait on them too."""
        while (task := self._linking) is not None and not task.done():
            await asyncio.wait([task])

    def renewal_owed(self, sandbox: Any = None) -> bool:
        """Whether ``renew_soon`` would start a renewal. With ``sandbox``,
        only while the mount serves there."""
        seen = self._seen
        if seen is None or not seen.mounted or not runs_low(seen.expires_at):
            return False
        if sandbox is not None and not self.serving(sandbox):
            return False
        return self._renewing is None and time.monotonic() >= self._renew_at

    def renew_soon(
        self, renew: Callable[[], Awaitable[MountState | None]], sandbox: Any = None
    ) -> None:
        """Renew a token running low in the background, one renewal at a
        time; nothing waits on it."""
        if self.renewal_owed(sandbox):
            self._renewing = asyncio.get_running_loop().create_task(
                self._renew(self._seen, renew)
            )

    async def _renew(
        self, seen: MountState, renew: Callable[[], Awaitable[MountState | None]]
    ) -> None:
        try:
            renewed = await renew()
        except Exception:
            logger.warning("livefs token renewal failed", exc_info=True)
            renewed = None
        finally:
            if self._renewing is asyncio.current_task():
                self._renewing = None
        if self._seen is not seen:
            # Forgotten, or a serve saw the mount since: that stands.
            return
        if renewed is not None and not renewed.mounted:
            # The daemon took the token but cannot serve with it. The next
            # command asks it to serve again, which asks the server, and hands
            # the file tools the store while it stays down.
            self._seen = renewed
            self._retry_at = 0.0
        elif renewed is not None and not runs_low(renewed.expires_at):
            self._seen = replace(seen, expires_at=renewed.expires_at)
        else:
            self._renew_at = time.monotonic() + _RENEW_RETRY_S

    def _refresh_owed(self) -> bool:
        # Nothing seen is a machine forgotten (stopped, or its session
        # dropped) under a command still running, which must not mint for it.
        seen = self._seen
        return (
            seen is not None
            and (not seen.mounted or lapsed(seen.expires_at))
            and time.monotonic() >= self._retry_at
        )

    async def keep_token(
        self,
        refresh: Callable[[], Awaitable[Any]],
        renew: Callable[[], Awaitable[MountState | None]],
    ) -> None:
        """Before a command: replace a token that is gone, or bring back a
        mount a renewal found down (``refresh``, a serve), which the command
        waits on since it cannot run without either, and renew a token
        running low in the background, since it still carries the command."""
        if self._refresh_owed() and (renewing := self._renewing) is not None:
            # A turn started replacing the token gone; that replaces it.
            await asyncio.wait([renewing])
        if not self._refresh_owed():
            self.renew_soon(renew)
            return
        async with self._lock:
            if not self._refresh_owed():
                return
            await refresh()
            if self._refresh_owed():
                # Whatever held the refresh off, each command does not wait
                # on it again: they run on the current token until the retry.
                self._retry_at = time.monotonic() + _RETRY_S[MountError.BUSY]

    def probe_due(self) -> bool:
        """Whether a command's missing file under the mount should ask the
        sandbox whether the mount is gone. A missing file is usually just
        missing, so at most once a minute on each worker."""
        now = time.monotonic()
        if now < self._probed_at + _PROBE_EVERY_S:
            return False
        self._probed_at = now
        return True

    async def restart(self, refresh: Callable[[], Awaitable[MountState]]) -> MountState:
        """Bring a mount found dead back up, never alongside a token refresh."""
        async with self._lock:
            return await refresh()
