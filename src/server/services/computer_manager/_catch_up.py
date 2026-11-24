"""Seam: what a computer still owes a workspace once its turn has started.

A chat turn's first model call needs the computer up and its files in place.
It does not need the asset sync (tool modules, internal packages, shared
skills), the tool overlay, the skill links, the browser settings, the
computer's tokens or the file mount's start, which only what the agent then
runs on the computer reads. An acquire made for a turn hands those to a
catch-up here and returns; the sandbox's ``AssetGate`` holds the turn's calls
on the computer until the catch-up ends. Every other
acquire awaits the same work (``_run_or_catch_up``).

One file of the ComputerManager split; see the package __init__.

The gate and the task are this worker's execution context. Truth stays on the
computer (the asset manifest, the union ledger), so another worker's acquire
compares for itself, as does this one's after a restart; its sync waits for
one running here to end first (``_sync_turn.py``). A sync this worker
took on and did not see finish stays on its books (``MachineState.sync_owed``)
and the next acquire that syncs runs it again.
"""

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ptc_agent.core.sandbox.attach_only import for_every_acquire
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from ptc_agent.core.sandbox.runtime import SandboxGoneError

from src.server.services.computer_manager._types import ComputerBinding

logger = logging.getLogger(__name__)

#: How long one catch-up may run before it is stopped and its gate opened.
#: Several times the slowest sync seen (85 s, on a machine of a user's own).
CATCH_UP_DEADLINE_S = 180.0


def _no_mark(_phase: str) -> None:
    """For work run off an acquire's path, whose timeline a ``mark`` writes."""


class CatchUpMixin:
    def _catches_up_later(
        self, binding: ComputerBinding, session: Any, asked: bool
    ) -> bool:
        """Whether this acquire may return ahead of its asset sync.

        Only for a caller that asked, and only on a computer whose row says
        its layout is current: a layout migration moves the folders the turn
        is about to read, so a computer that may owe one syncs first. So does
        one a catch-up was cut short on: slower than the deadline, its sync
        only finishes on an acquire's own path, which has none.
        """
        return (
            asked
            and binding.layout_version == CURRENT_LAYOUT_VERSION
            and session.sandbox is not None
            and not self._machine(binding.computer_id).catch_up_cut
        )

    def _sync_owed(self, computer_id: str, workspace_id: str) -> bool:
        """Whether this worker took on a sync for ``workspace_id`` that it did
        not see finish, with no catch-up of the workspace's still to run.

        Read where an acquire already syncs, never to get past the sync
        cooldown: a sync that keeps failing stays owed, and would then run
        on every request for the workspace's files."""
        machine = self._machine(computer_id)
        return workspace_id in machine.sync_owed and not machine.catch_ups.get(
            workspace_id
        )

    async def _run_or_catch_up(
        self,
        binding: ComputerBinding,
        session: Any,
        work: Callable[[], Awaitable[Any]],
        *,
        later: bool,
        what: str,
        attach_key: tuple[str, str | None] | None = None,
    ) -> None:
        """Await ``work``, or hand it to a catch-up when ``later``.

        ``attach_key`` is an attach recorded on the strength of ``work``
        (``_claim_attached``). It is withdrawn unless ``work`` returns True,
        however the work ends: here when awaited, by the catch-up when handed
        over. Nothing awaits between the call and either, so the caller's
        record is covered from the moment it passes the key.
        """
        if later:
            self._catch_up_soon(binding, session, work, what=what, attach_key=attach_key)
            return
        stands = False
        try:
            stands = await work()
        finally:
            if attach_key is not None and stands is not True:
                self._projects_attached.pop(attach_key, None)

    async def _sync_or_catch_up(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        session: Any,
        *,
        later: bool,
        what: str,
        link_source: str | None,
        skills_signature: str | None = None,
        attach_key: tuple[str, str | None] | None = None,
        mark: Callable[[str], None] = _no_mark,
    ) -> None:
        """``_sync_and_link`` for an acquire: awaited, or handed to a catch-up
        when ``later``.

        Owed from here until this run of it finishes. A sync cut short (a
        stop that then gave up, the deadline, a failure) changed none of what
        the next acquire compares, so the record is what has the next acquire
        that syncs run it again (``_sync_owed``).
        """
        owed = object()
        self._machine(binding.computer_id).sync_owed[binding.workspace_id] = owed
        await self._run_or_catch_up(
            binding,
            session,
            functools.partial(
                self._sync_and_link,
                binding,
                user_id,
                session,
                owed=owed,
                link_source=link_source,
                skills_signature=skills_signature,
                attach_key=attach_key,
                later=later,
                mark=_no_mark if later else mark,
            ),
            later=later,
            what=what,
            attach_key=attach_key,
        )
        if later:
            mark("catch_up_later")

    async def _sync_and_link(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        session: Any,
        *,
        owed: object,
        link_source: str | None,
        skills_signature: str | None = None,
        attach_key: tuple[str, str | None] | None = None,
        later: bool = False,
        mark: Callable[[str], None] = _no_mark,
    ) -> bool:
        """The asset sync, the skill pass when ``link_source`` names it, and
        the stamp of the skills that sync delivered. Says whether the sync
        ran to its end, which settles the workspace's owed record when
        ``owed`` is still the one it holds: a sync taken on since is owed
        until its own run ends.

        ``later`` is a catch-up's run (``_sync_sandbox_assets``).
        ``attach_key`` is an attach that stands on the sync and the pass: the
        pass withdraws it by its own rule, the caller for a sync that did not
        finish."""
        synced = await self._sync_sandbox_assets(
            binding, user_id, session.sandbox, reusing_sandbox=True, later=later
        )
        mark("asset_sync")
        records = self._machine(binding.computer_id).sync_owed
        if synced is not None and records.get(binding.workspace_id) is owed:
            del records[binding.workspace_id]
        if link_source is not None:
            await self._reconcile_skills(
                binding.workspace_id,
                user_id,
                session.sandbox,
                source=link_source,
                attach_key=attach_key,
            )
            mark("skill_reconcile")
        # Only a sync that ran to its end: a failed one leaves the signature
        # for the next acquire to try again instead of trusting the cooldown.
        # The pass is not asked, since a workspace it cannot finish for would
        # then sync on every acquire.
        if skills_signature is not None and synced is not None:
            session.skills_signature = skills_signature
        return synced is not None

    async def _restore_and_claim(
        self, binding: ComputerBinding, session: Any
    ) -> tuple[str, str | None] | None:
        """The workspace's folder and its files, then the attach recorded on
        the strength of work still to come. Returns the attach's key for
        ``_run_or_catch_up``; None when the folder is not restored, or the
        attach was recorded before this acquire and so is not its own.

        Unrestored, the next acquisition retries before trusting this
        workspace. Restored, the key goes in before the next await: the
        bring-up just queued may drop the workspace and discard its key,
        which a later add would undo."""
        await self._ensure_workspace_dirs(
            binding.workspace_id, session.sandbox, binding.dir_name
        )
        if await self._maybe_restore_files(binding, session.sandbox):
            return self._claim_attached(binding, session)
        # A key from before a stop on this sandbox would have the next
        # acquisition trust a folder this check never saw.
        self._projects_attached.pop(
            (binding.workspace_id, self._session_sandbox_id(session)), None
        )
        return None

    async def _attach_overlay_and_skills(
        self,
        binding: ComputerBinding,
        session: Any,
        *,
        user_id: str | None,
        links: bool,
        later: bool = False,
    ) -> bool:
        """An attach's tool overlay, then its skill links when this acquire
        recorded the attach (``links``). Says whether that attach stands: the
        overlay is there, which MCP calls refuse to run without, and the pass
        finished. ``later`` is a catch-up's run, whose overlay build moves no
        folders (``_sync_sandbox_assets``).

        A folder reaches the shared skills only through links, and a machine
        start or a sibling's lifecycle links no folder but its own. The pass
        comes after the overlay, whose asset refresh can deliver skills it has
        to link, and runs once per sandbox per process."""
        if not await self._ensure_project_tool_overlay(
            binding, session, user_id=user_id, later=later
        ):
            return False
        return not links or await self._reconcile_skills(
            binding.workspace_id, user_id, session.sandbox, source="attach_project"
        )

    def _catch_up_soon(
        self,
        binding: ComputerBinding,
        session: Any,
        work: Callable[[], Awaitable[Any]],
        *,
        what: str,
        attach_key: tuple[str, str | None] | None = None,
    ) -> asyncio.Task:
        """Run ``work`` off the acquire's path, holding ``binding``'s
        workspace's calls on the computer until it ends, however it ends.
        Returns its task, which ends however the work does.

        Catch-ups for one workspace run in the order they were handed over.
        A failure opens the gate like a success: the call then runs on the
        files the computer has, as it does when a sync fails on an acquire's
        own path, and a later acquire tries again (``_sync_owed``).
        """
        sandbox = session.sandbox
        computer_id = binding.computer_id
        workspace_id = binding.workspace_id
        machine = self._machine(computer_id)
        release = sandbox.assets.hold(workspace_id)
        queue = machine.catch_ups.setdefault(workspace_id, [])
        earlier = queue[-1] if queue else None
        handed_over = time.monotonic()

        async def _run() -> Any:
            for_every_acquire()
            outcome = "done"
            began: float | None = None
            try:
                if earlier is not None:
                    await asyncio.wait({earlier})
                began = time.monotonic()
                deadline = asyncio.timeout(CATCH_UP_DEADLINE_S)
                try:
                    async with deadline:
                        return await work()
                except TimeoutError:
                    # A timeout the work raised itself is a failure like any
                    # other: only the deadline says this computer is too slow.
                    if not deadline.expired():
                        raise
                    outcome = "timed_out"
                    machine.catch_up_cut = True
                    logger.warning(
                        f"Asset catch-up ({what}) for workspace {workspace_id} "
                        f"stopped after {CATCH_UP_DEADLINE_S:.0f}s; its calls run "
                        "on what the computer holds"
                    )
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except SandboxGoneError as e:
                outcome = "sandbox_gone"
                logger.warning(
                    f"Sandbox gone during asset catch-up ({what}) for workspace "
                    f"{workspace_id}: {e}"
                )
                # The next acquire rebuilds it; the turn's own call on the
                # computer meets the loss as any call mid-turn does.
                if self._cached_session(computer_id) is session:
                    await self._clear_session(computer_id, evict_session=session)
            except Exception as e:  # noqa: BLE001 - never the turn's failure
                outcome = "failed"
                logger.warning(
                    f"Asset catch-up ({what}) failed for workspace {workspace_id}: "
                    f"{type(e).__name__}: {e}"
                )
            finally:
                now = time.monotonic()
                # Cancelled while it still waited its turn: it never ran.
                started = now if began is None else began
                logger.info(
                    f"[CATCH_UP] what={what} kind={binding.kind} "
                    f"computer={computer_id[:8]} workspace={workspace_id[:8]} "
                    f"outcome={outcome} ran={(now - started) * 1000:.0f}ms "
                    f"queued={(started - handed_over) * 1000:.0f}ms"
                )

        task = asyncio.create_task(_run())
        queue.append(task)

        def _done(finished: asyncio.Task) -> None:
            # Here and not in the task: one cancelled before it first ran
            # never reached its own code.
            release()
            stands = (
                not finished.cancelled()
                and finished.exception() is None
                and finished.result() is True
            )
            if attach_key is not None and not stands:
                self._projects_attached.pop(attach_key, None)
            if finished in queue:
                queue.remove(finished)
            if not queue and machine.catch_ups.get(workspace_id) is queue:
                del machine.catch_ups[workspace_id]

        task.add_done_callback(_done)
        return task

    async def _stop_catch_ups(
        self, computer_id: str, *, workspace_id: str | None = None
    ) -> None:
        """Cancel the catch-ups this worker runs on ``computer_id``, or only
        ``workspace_id``'s, and wait until they have let go of it: before a
        stop backs up and takes down the disk they write, and before a delete
        backs up and removes the folder. What they left undone the next
        acquire finds owed."""
        machine = self._machine_if_known(computer_id)
        if machine is None:
            return
        tasks = [
            task
            for task in machine.running_catch_ups()
            if workspace_id is None or task in machine.catch_ups.get(workspace_id, ())
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)
