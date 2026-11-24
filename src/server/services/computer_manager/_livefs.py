"""Seam: each machine's file mount, brought up when due and revoked with the machine.

One file of the ComputerManager split; see the package __init__. What this
worker saw of the mount is ``MachineState.livefs``, a ``MountTracker``, which
takes what every worker recorded at each turn's start (``_load_livefs``)."""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar

from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError

if TYPE_CHECKING:
    from src.server.services.livefs.drive_mount import DriveMount
    from src.server.services.livefs.mount import Handle, LinkState, MountState

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class LivefsMixin:
    def _file_calls(self, computer_id: Any, user_id: str | None, sandbox: Any) -> None:
        """Hand ``sandbox`` the filing of who each command runs for, beside
        the mount or the history it is handed: a save through the mount
        reads it, and so does a call the ``la`` CLI makes, which names its
        thread to the server only through it, whatever mounts the machine.
        With it, what each command's paid relay calls cost, where any are
        metered, read for the computer's owner."""
        from src.server.services.egress.platform_accounts import platform_accounts_configured
        from src.server.services.livefs import outcomes
        from src.server.services.metered_charges import command_charges

        computer_id = str(computer_id)
        sandbox.call_filer = lambda call_id, context: outcomes.open_call(
            computer_id, call_id, context
        )
        sandbox.paid_calls = (
            functools.partial(command_charges, str(user_id))
            if user_id and platform_accounts_configured()
            else None
        )
        try:
            self._hand_drive(computer_id, user_id, sandbox)
        except Exception:
            # The Drive's view costs the Drive, never the acquire.
            logger.warning("drive handle not set for computer %s", computer_id, exc_info=True)

    def _hand_drive(self, computer_id: Any, user_id: str | None, sandbox: Any) -> DriveMount:
        """``sandbox``'s view of the Drive mount, which the tools ask before
        a command or file call names the Drive: kept while the computer and
        owner are the same, made anew for another."""
        from src.server.services.livefs.drive_mount import DriveMount

        handle = getattr(sandbox, "drive", None)
        if isinstance(handle, DriveMount) and handle.owned_by(computer_id, user_id):
            return handle
        handle = DriveMount(
            sandbox,
            str(computer_id),
            user_id,
            self._livefs_provider(sandbox),
            self._machine(str(computer_id)).drive,
        )
        sandbox.drive = handle
        return handle

    def _livefs_provider(self, sandbox: Any) -> str:
        provider = getattr(getattr(sandbox, "config", None), "sandbox", None)
        return getattr(provider, "provider", None) or self.config.sandbox.provider

    async def _serve_livefs(
        self,
        computer_id: Any,
        user_id: str | None,
        sandbox: Any,
        *,
        adopt: bool = True,
    ) -> MountState:
        """Make the machine's file mount serve, record it, and hand the file
        tools the mount (``sandbox.livefs``) while it does. ``adopt`` takes a
        start another worker recorded since this one read the rows.

        Never raises: without the mount the file tools still reach these files
        through the store, so a mount failure costs code access, not a turn.
        """
        from src.server.services.livefs import mount as livefs

        state = await livefs.serve(
            sandbox,
            computer_id=str(computer_id),
            user_id=user_id,
            provider=self._livefs_provider(sandbox),
            adopt=adopt,
        )
        self._machine(str(computer_id)).livefs.saw(sandbox, state)
        # A settle holding the folders asked nothing, so whatever the sandbox
        # was handed stands.
        if sandbox is not None and state.error != MountError.BUSY:
            if state.mounted:
                sandbox.livefs = self._livefs_handle(computer_id, user_id, sandbox)
            else:
                self._take_livefs_back(computer_id, sandbox)
        if state.mounted:
            # A turn's path keeps a token that only runs low; this replaces it.
            self._renew_livefs_soon(computer_id, user_id, sandbox)
            if state.started:
                # A daemon started afresh most often means the sandbox
                # restarted, taking the Drive's daemon with it.
                self._drive_soon(computer_id, user_id, sandbox, force=True)
        return state

    def _livefs_link(
        self, computer_id: Any, user_id: str | None, sandbox: Any
    ) -> Callable[[], Awaitable[LinkState]]:
        from src.server.services.livefs import mount as livefs

        return lambda: livefs.link(
            sandbox, computer_id=str(computer_id), user_id=user_id
        )

    def _link_livefs_soon(
        self,
        computer_id: Any,
        user_id: str | None,
        sandbox: Any,
        workspace_id: str | None = None,
    ) -> None:
        """Link the machine's folders in off the turn's path, once the
        daemon answered a serve, mounted or not, and ``workspace_id``'s
        folder (None: any) is not seen linked. A command in that workspace
        waits for it while the mount serves (``Handle.ready``)."""
        if not sandbox.mounts_livefs:
            # Links into a mount that never runs would only dangle, and laying
            # them takes root the machine need not grant.
            return
        tracker = self._machine(str(computer_id)).livefs
        tracker.link_soon(sandbox, workspace_id, self._livefs_link(computer_id, user_id, sandbox))
        self._keep_drive(computer_id, user_id, sandbox)

    def _keep_drive(self, computer_id: Any, user_id: str | None, sandbox: Any) -> None:
        """``_drive_soon`` while the file mount serves, whose token the Drive
        uses."""
        if self._machine(str(computer_id)).livefs.serving(sandbox):
            self._drive_soon(computer_id, user_id, sandbox)

    def _drive_soon(
        self, computer_id: Any, user_id: str | None, sandbox: Any, *, force: bool = False
    ) -> None:
        """Bring the Drive mount to what is due beside a serving file mount,
        off the turn's path: up and linked with the feature on, down with it
        off. Its failures stay its own (``livefs.drive_mount.converge``)."""
        try:
            self._hand_drive(computer_id, user_id, sandbox).converge_soon(force=force)
        except Exception:
            logger.warning("drive mount not scheduled for computer %s", computer_id, exc_info=True)

    def _livefs_handle(self, computer_id: Any, user_id: str | None, sandbox: Any) -> Handle:
        from src.server.services.livefs import mount as livefs

        return livefs.Handle(
            str(computer_id),
            self._machine(str(computer_id)).livefs,
            sandbox,
            serve=lambda adopt=True: self._serve_livefs(
                computer_id, user_id, sandbox, adopt=adopt
            ),
            link=self._livefs_link(computer_id, user_id, sandbox),
            renew=lambda: self._renew_livefs(computer_id, user_id, sandbox),
        )

    async def _load_livefs(
        self, computer_id: Any, user_id: str | None, sandbox: Any
    ) -> None:
        """Take what every worker recorded of the mount on ``sandbox``: one
        indexed read, which a turn's start makes and its commands never do.
        Rows saying it serves there hand a sandbox object never handed it
        (a reconnect's) the mount without an exec, and rows saying it does
        not take it back, since an attach can end before any serve and the
        turn's prompt says whether the files are mounted. Unread, nothing
        changes and the tracker goes on with what this worker saw."""
        from src.server.services.livefs import mount as livefs

        self._file_calls(computer_id, user_id, sandbox)
        if not sandbox.mounts_livefs:
            sandbox.livefs = None
            await self._load_history(computer_id, sandbox)
            return
        view = await livefs.view(
            sandbox,
            computer_id=str(computer_id),
            provider=self._livefs_provider(sandbox),
        )
        if view is None:
            return
        serving = self._machine(str(computer_id)).livefs.load(sandbox, view)
        if not view.serving:
            self._take_livefs_back(computer_id, sandbox)
        elif serving and user_id and getattr(sandbox, "livefs", None) is None:
            sandbox.livefs = self._livefs_handle(computer_id, user_id, sandbox)

    async def _load_history(self, computer_id: Any, sandbox: Any) -> None:
        """Past threads for the file tools on a machine the mount never
        serves, over the folders it holds now. Unread, the tools go on with
        the folders the last turn saw, or with the thread index alone: the
        prompt says the transcripts are there for as long as the machine's
        kind does, so a turn never goes without them."""
        from src.server.services.livefs.history_files import (
            ComputerHistory,
            computer_history,
        )

        try:
            sandbox.history = await computer_history(str(computer_id))
        except Exception:
            logger.warning(
                "transcripts for the file tools not loaded on computer %s",
                computer_id,
                exc_info=True,
            )
            if sandbox.history is None:
                sandbox.history = ComputerHistory(str(computer_id), ())

    def _livefs_owed(self, computer_id: Any, workspace_id: str, sandbox: Any) -> bool:
        """Whether a turn in ``workspace_id`` has anything to ask of the
        mount, by what its start read (``_load_livefs``)."""
        tracker = self._machine(str(computer_id)).livefs
        return (
            tracker.serve_due(sandbox)
            or tracker.link_owed(sandbox, workspace_id)
            or tracker.renewal_owed(sandbox)
        )

    def _take_livefs_back(self, computer_id: Any, sandbox: Any) -> None:
        """Take the mount from ``sandbox``, unless a start it was handed ahead
        of is still to answer: what a turn was told then stands for as long
        as it may prove true, and the hand-off's end settles it."""
        if not self._machine(str(computer_id)).livefs.starting():
            sandbox.livefs = None

    def _livefs_start_trusted(
        self, computer_id: Any, user_id: str | None, sandbox: Any
    ) -> bool:
        """Whether the mount on ``sandbox`` is due a start a turn takes to
        serve without waiting for it: the machine mounts, and no start this
        worker asked of it failed. After a failure the turn waits for the
        next start, so its prompt says what is so. One worker's memory of
        one sandbox, kept through a stop: the notice corrects a prompt
        another worker got wrong (``FileMountNoticeMiddleware``)."""
        tracker = self._machine(str(computer_id)).livefs
        return bool(
            user_id
            and sandbox.mounts_livefs
            and tracker.serve_due(sandbox)
            and not tracker.serving(sandbox)
            and not tracker.failed(sandbox)
        )

    def _hand_livefs_ahead(
        self,
        computer_id: Any,
        user_id: str | None,
        sandbox: Any,
        start: asyncio.Future[Any],
    ) -> None:
        """Hand ``sandbox`` the mount ahead of ``start``, the work a turn
        handed over that starts it. The turn's prompt and its history's
        pointers at the transcripts then take the mount to serve, and
        whatever reads through it waits for the start (``Handle.ready``). A
        start that ends without a mount takes it back."""
        tracker = self._machine(str(computer_id)).livefs
        tracker.start_handed(sandbox, start)
        sandbox.livefs = self._livefs_handle(computer_id, user_id, sandbox)

        def settle(_start: asyncio.Future[Any]) -> None:
            if not tracker.serving(sandbox):
                self._take_livefs_back(computer_id, sandbox)

        # After the tracker's own callback, which ends the start.
        start.add_done_callback(settle)

    async def _keep_livefs(
        self, computer_id: Any, user_id: str | None, sandbox: Any, workspace_id: str
    ) -> None:
        """An acquire's share of the mount. A mount not serving is started
        and waited for here; a turn's acquire hands this over where it trusts
        the start (``_livefs_start_trusted``). A token running low or gone
        and a folder not linked yet are seen to off its path, and a command
        waits for what it needs of them (``Handle.ready``)."""
        if sandbox is None or not user_id:
            return
        tracker = self._machine(str(computer_id)).livefs
        if tracker.serve_due(sandbox) and not tracker.serving(sandbox):
            await self._serve_livefs(computer_id, user_id, sandbox)
        self._renew_livefs_soon(computer_id, user_id, sandbox)
        self._link_livefs_soon(computer_id, user_id, sandbox, workspace_id)

    async def _livefs_beside(
        self,
        computer_id: Any,
        user_id: str | None,
        sandbox: Any,
        sync: Awaitable[_T],
        *,
        workspace_id: str | None = None,
    ) -> _T:
        """Run an asset sync for code about to run on ``sandbox``, with the
        file mount's start beside it unless the rows say it serves.

        The links wait for the layout the sync may move, since files moved
        under a link would move through the mount. A start the sync may have
        stood in the way of is asked once more after it: the turn's prompt
        says whether the files are mounted for the rest of the conversation.
        """
        if sandbox is None:
            return await sync
        self._file_calls(computer_id, user_id, sandbox)
        if not user_id:
            return await sync
        tracker = self._machine(str(computer_id)).livefs
        if not tracker.serving(sandbox):
            await self._load_livefs(computer_id, user_id, sandbox)
        if not tracker.serve_due(sandbox):
            result = await sync
            self._renew_livefs_soon(computer_id, user_id, sandbox)
            self._link_livefs_soon(computer_id, user_id, sandbox, workspace_id)
            return result
        times: dict[str, float] = {}

        async def timed(name: str, step: Awaitable[MountState]) -> MountState:
            began = time.monotonic()
            try:
                return await step
            finally:
                times[name] = (time.monotonic() - began) * 1000

        serving = asyncio.ensure_future(
            timed("livefs_start", self._serve_livefs(computer_id, user_id, sandbox))
        )
        try:
            result = await sync
        except BaseException:
            serving.cancel()
            raise
        state = await serving
        if not state.settled:
            await timed(
                "livefs_retry", self._serve_livefs(computer_id, user_id, sandbox)
            )
        self._link_livefs_soon(computer_id, user_id, sandbox, workspace_id)
        logger.info(
            "[SYNC_DETAIL] computer_id=%s workspace_id=%s %s",
            computer_id,
            workspace_id,
            " ".join(f"{k}={v:.0f}ms" for k, v in times.items()),
        )
        return result

    async def _renew_livefs(
        self, computer_id: Any, user_id: str | None, sandbox: Any
    ) -> MountState | None:
        from src.server.services.livefs import mount as livefs

        return await livefs.renew(
            sandbox,
            computer_id=str(computer_id),
            user_id=user_id,
            provider=self._livefs_provider(sandbox),
        )

    def _renew_livefs_soon(
        self, computer_id: Any, user_id: str | None, sandbox: Any
    ) -> None:
        """Renew the mount's token in the background once it runs low; the
        turn goes on with the current one."""
        if not user_id:
            return
        self._machine(str(computer_id)).livefs.renew_soon(
            lambda: self._renew_livefs(computer_id, user_id, sandbox), sandbox
        )

    async def _revoke_livefs(self, computer_id: Any) -> None:
        """A machine leaving service stops serving its files at once, rather
        than when the token it holds would have run out."""
        from src.server.services.livefs import mount as livefs

        await livefs.revoke(computer_id)
        from src.server.services.livefs import drive_mount

        await drive_mount.forget(str(computer_id))
        machine = self._machine_if_known(str(computer_id))
        if machine is not None:
            machine.livefs.forget()
