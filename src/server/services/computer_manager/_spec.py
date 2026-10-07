"""Seam: changing a computer's spec tier, which means rebuilding its sandbox.

One file of the ComputerManager split; see the package __init__.

Every change, from the computer route or the deprecated workspace route, is
claimed on the computer row, run under the machine's locks with a heartbeat
on the claim, and settled on the row before those locks are released. The
settle carries the tier revert of a failed change, so no tier writer can land
between the failure and the revert.
"""

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, AsyncIterator, Dict, Mapping, Optional

from fastapi import HTTPException

from ptc_agent.core.sandbox.runtime import SandboxGoneError
from ptc_agent.core.session import Session, SessionManager

from src.server.database.computer import (
    clear_computer_disk,
    computer_capacity_lock,
    get_computer,
    set_computer_resource_tier as db_set_computer_resource_tier,
    try_claim_computer_for_start,
    update_computer_status,
)
from src.server.database.computer_spec_change import (
    claim_computer_spec_change,
    heartbeat_computer_spec_change,
    settle_computer_spec_change,
)
from src.server.database.sql_fences import SPEC_CHANGE_HEARTBEAT_SECONDS
from src.server.database.workspace import (
    get_live_workspace_ids_for_computer,
    get_workspace as db_get_workspace,
)
from src.server.database.workspace_file import get_live_project_sizes_for_computer
from src.server.models.computer import ComputerStatus
from src.server.services.computer_errors import (
    ComputerBusyError,
    DiskTooSmallError,
    MachineBusyError,
    SpecChangeLostError,
)
from src.server.services.computer_manager._types import ComputerBinding
from src.server.services.persistence.transfer import PULL_MAX_INFLIGHT_BYTES
from src.server.services.spec_change import (
    SPEC_CHANGE_IN_PROGRESS,
    spec_change_error,
    spec_change_from_row,
)
from src.server.services.workspace_status_pubsub import (
    publish_computer_status_change,
)

logger = logging.getLogger(__name__)

# Disk a restored workspace needs besides its own files, measured on fresh
# sandboxes of every tier. The image lives in read-only layers the tier's disk
# does not pay for, so the writable layer starts at a few MiB. A restore then
# stages up to PULL_MAX_INFLIGHT_BYTES of pack chunks beside the files it has
# already written, and the agent needs room to work once it runs. Each
# project restores lazily on its own first start, under its own lock, so two
# projects opened together stage at once: the machine's allowance carries one
# staging window per project.
_SANDBOX_BASELINE_BYTES = 16 * 1024**2
_WORKING_HEADROOM_BYTES = 512 * 1024**2
_DISK_SYSTEM_RESERVE_BYTES = (
    _SANDBOX_BASELINE_BYTES + PULL_MAX_INFLIGHT_BYTES + _WORKING_HEADROOM_BYTES
)
_GIB = 1024**3


def _spec_refusal(
    computer: Mapping[str, Any],
    tier: str,
    tiers: Mapping[str, Any],
    *,
    claim_id: str | None = None,
) -> bool:
    """Raise the refusal for a change that cannot run now; True when nothing changes.

    Shared by the lock-free precheck and the re-check under the locks, so the
    two can disagree only about the row they read, never about the rules.
    ``claim_id`` is the re-check's own claim, whose record says whether it
    took over a dead change.
    """
    if tier not in tiers:
        raise ValueError(f"Unknown resource tier: {tier}")
    record = computer.get("spec_change") or {}
    took_over = (
        claim_id is not None
        and str(record.get("claim_id")) == claim_id
        and bool(record.get("took_over"))
    )
    if (
        tier == (computer.get("resource_tier") or "standard")
        and not computer.get("spec_change_stale")
        and not took_over
    ):
        # A dead change may have stopped anywhere between persisting its
        # target and the recreate, so the tier the row reads says nothing
        # about the machine's size. The precheck sees it stale; the run sees
        # its own claim marked took_over, and rebuilds.
        return True
    status = computer.get("status")
    if computer.get("provider_ref") and status not in (
        ComputerStatus.RUNNING,
        ComputerStatus.STOPPED,
    ):
        # A transient state may have an in-flight op holding the sandbox;
        # replacing it underneath that op would race it.
        raise ComputerBusyError(
            f"Cannot change spec while this computer is {status!r}; "
            "wait for the current operation to finish"
        )
    return False


def _claim_id(row: Mapping[str, Any]) -> str:
    return str(row["spec_change"]["claim_id"])


@dataclass
class _Replacement:
    """Where the machine's row goes if a replacement leaves before it binds."""

    release_to: str


class ComputerSpecMixin:
    """Spec-tier changes for ComputerManager."""

    spec_change_heartbeat_s: float = SPEC_CHANGE_HEARTBEAT_SECONDS

    @asynccontextmanager
    async def _spec_change_heartbeat(
        self, computer_id: str, claim_id: str
    ) -> AsyncIterator[None]:
        """Keep a live change's record newer than the stale window while it runs.

        Stops once the row no longer answers to the claim: it has been taken
        over, and the ownership check ahead of the next destructive step is
        what stops the runner.
        """

        async def renew() -> None:
            while True:
                await asyncio.sleep(self.spec_change_heartbeat_s)
                try:
                    if not await heartbeat_computer_spec_change(
                        computer_id, claim_id=claim_id
                    ):
                        logger.warning(
                            "Spec change %s of computer %s no longer holds the row",
                            claim_id,
                            computer_id,
                        )
                        return
                except Exception as e:
                    logger.warning(
                        "Could not renew spec change %s of computer %s: %s",
                        claim_id,
                        computer_id,
                        e,
                    )

        task = asyncio.create_task(renew())
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _assert_spec_claim_owned(self, computer_id: str, claim_id: str) -> None:
        """Fenced no-op ahead of a step the runner cannot take back.

        The machine lock serialises runners on one worker and the claim across
        workers, but a claim whose heartbeats stopped landing for a whole
        stale window is taken over while its runner still holds the lock.
        From then on the row is the other runner's and this one's settle
        matches nothing, so the only safe move is to stop before mutating.
        """
        if not await heartbeat_computer_spec_change(computer_id, claim_id=claim_id):
            raise SpecChangeLostError(
                "Another spec change took this computer over; "
                "this one changed nothing"
            )

    def _resource_tiers(self, computer: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._provider_settings(
            computer.get("kind") or self.config.sandbox.provider,
            computer.get("provider_config"),
        ).resource_tiers

    async def _refuse_if_turn_active(
        self, computer_id: str, workspace_id: str | None = None
    ) -> None:
        """Never replace the sandbox under a live agent turn.

        execute_code keeps running in the sandbox without holding the machine
        lock, so a recreate would abort it with SandboxGoneError. The sandbox is
        shared, so a turn on any project on the machine is enough to refuse.
        """
        if await self._machine_has_active_tasks(computer_id, workspace_id=workspace_id):
            raise MachineBusyError(
                "Cannot change spec while an agent turn is running; "
                "wait for the current turn to finish"
            )

    async def precheck_computer_spec(
        self, computer: Dict[str, Any], tier: str
    ) -> bool:
        """Refuse a spec change that would fail at once; True when there is nothing to change.

        Lock-free and advisory: an accepted change runs after its request has
        returned, so a refusal the client can act on (unknown tier, a live turn,
        a machine mid-operation) is answered here instead of surfacing minutes
        later as a failed outcome. The run re-checks all of it under its locks.
        """
        running = spec_change_from_row(computer)
        if running is not None and running.state == "in_progress":
            # Ahead of the same-tier shortcut: a running change persists its
            # target tier early, so a repeat would otherwise read as done.
            raise ComputerBusyError(SPEC_CHANGE_IN_PROGRESS)
        if _spec_refusal(computer, tier, self._resource_tiers(computer)):
            return True
        if computer.get("status") == ComputerStatus.RUNNING:
            await self._refuse_if_turn_active(str(computer["computer_id"]))
        return False

    async def claim_computer_spec(
        self, computer_id: str, tier: str
    ) -> Optional[Dict[str, Any]]:
        """Record a change as in progress; None when a fresh one already holds the row."""
        return await claim_computer_spec_change(computer_id, target_tier=tier)

    async def run_accepted_spec_change(
        self, computer_id: str, tier: str, *, user_id: str, claim_id: str
    ) -> None:
        """Run a change accepted behind a 202; the outcome on the row is its only report."""
        try:
            await self._run_spec_change(
                computer_id, tier, claim_id=claim_id, user_id=user_id
            )
        except (RuntimeError, HTTPException) as exc:
            logger.warning("Spec change of computer %s refused: %s", computer_id, exc)
        except Exception:
            logger.exception("Spec change of computer %s failed", computer_id)

    async def set_workspace_spec(
        self,
        workspace_id: str,
        tier: str,
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Project-addressed entry for the deprecated workspace spec route."""
        binding = await self.resolve_binding(workspace_id)
        await self.set_computer_spec(
            binding.computer_id, tier, user_id=user_id, workspace_id=workspace_id
        )
        return await db_get_workspace(workspace_id) or {}

    async def set_computer_spec(
        self,
        computer_id: str,
        tier: str,
        *,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> Dict[str, Any]:
        """Change a computer's resource tier and wait for the recreate.

        Hosted Daytona can't resize a snapshot sandbox or override its resources,
        so sizing lives in per-tier snapshots and a spec change means recreate,
        not resize. The sandbox is the machine's, so this recreates it for every
        project on it:

        - **running**: back every project's files up to the DB, tear the live
          sandbox down, and recreate from the target tier's snapshot
          (``_recover_sandbox`` restores the files and applies always-on);
        - **stopped**: destroy the sandbox so the next start recreates it at the
          new tier, after a strict backup of the stopped copy;
        - **never-started**: just persist the tier, still under the machine lock
          so a change racing the initial create serializes behind it.

        Claimed and settled on the row like an accepted change, so a failed one
        reverts the tier and reports its outcome the same way. A downgrade whose
        backed-up files won't fit the smaller disk is refused before teardown.

        Raises:
            ValueError: Computer not found or ``tier`` is unknown.
            ComputerBusyError: Another spec change or operation holds the machine.
            DiskTooSmallError: Downgrade refused, files exceed the target disk.
            MachineBusyError: A turn is running on the machine.
            BackupIncomplete: The backup before teardown could not save every file.
            SpecChangeLostError: Another worker took the change over mid-run.
        """
        computer = await get_computer(computer_id)
        if not computer:
            raise ValueError(f"Computer {computer_id} not found")
        if await self.precheck_computer_spec(computer, tier):
            return computer
        claimed = await claim_computer_spec_change(computer_id, target_tier=tier)
        if claimed is None:
            raise ComputerBusyError(SPEC_CHANGE_IN_PROGRESS)
        return await self._run_spec_change(
            computer_id,
            tier,
            claim_id=_claim_id(claimed),
            user_id=user_id,
            workspace_id=workspace_id,
        )

    async def _run_spec_change(
        self,
        computer_id: str,
        tier: str,
        *,
        claim_id: str,
        user_id: str | None,
        workspace_id: str | None = None,
    ) -> Dict[str, Any]:
        """Run a claimed change under the machine's locks and settle it before they release.

        Settled whatever happens, cancellation included: shutdown drains these
        tasks, and without an outcome the client would read the change as in
        progress until it went stale.
        """
        settled = False

        async def settle(exc: BaseException | None) -> None:
            nonlocal settled
            if not settled:
                settled = True
                await asyncio.shield(
                    self._settle_spec_change(computer_id, claim_id, exc)
                )

        try:
            async with (
                self._spec_change_heartbeat(computer_id, claim_id),
                self._observed_lock(computer_id, "computer.spec"),
            ):
                async with AsyncExitStack() as stack:
                    try:
                        await self._hold_spec_locks(stack, computer_id, tier)
                        result = await self._set_computer_spec_locked(
                            computer_id,
                            tier,
                            claim_id=claim_id,
                            user_id=user_id,
                            workspace_id=workspace_id,
                        )
                    except BaseException as exc:
                        await settle(exc)
                        raise
                    await settle(None)
                    return result
        except BaseException as exc:
            # Cancelled while waiting for the local lock: nothing ran.
            await settle(exc)
            raise

    async def _settle_spec_change(
        self, computer_id: str, claim_id: str, exc: BaseException | None
    ) -> None:
        error = spec_change_error(exc) if exc is not None else None
        state = "failed" if error else "succeeded"
        try:
            row = await settle_computer_spec_change(
                computer_id,
                claim_id=claim_id,
                error=error.model_dump(mode="json") if error else None,
            )
        except Exception:
            logger.exception("Could not record the spec change outcome of %s", computer_id)
            return
        logger.info(
            "Spec change of computer %s settled %s%s",
            computer_id,
            state,
            f" ({error.code})" if error else "",
        )
        if row is not None:
            await publish_computer_status_change(
                computer_id, row["status"], extra={"spec_change": state}
            )

    async def _hold_spec_locks(
        self, stack: AsyncExitStack, computer_id: str, tier: str
    ) -> None:
        """Take the quota lock (re-checking the plan under it), then the machine lock."""
        computer = await get_computer(computer_id)
        if not computer:
            raise ValueError(f"Computer {computer_id} not found")
        owner_id = str(computer["user_id"])
        from src.server.dependencies.usage_limits import (
            assert_spec_allowed,
            platform_gating_active,
        )

        if platform_gating_active():
            await stack.enter_async_context(computer_capacity_lock(owner_id))
            computer = await get_computer(computer_id)
            if not computer:
                raise ValueError(f"Computer {computer_id} not found")
            await assert_spec_allowed(
                owner_id,
                tier,
                current_tier=computer.get("resource_tier") or "standard",
            )
        acquired = await stack.enter_async_context(
            self._machine_decision_lock(computer_id)
        )
        if not acquired:
            raise ComputerBusyError("Computer is busy; retry the spec change")

    async def _set_computer_spec_locked(
        self,
        computer_id: str,
        tier: str,
        *,
        claim_id: str,
        user_id: str | None,
        workspace_id: str | None,
    ) -> Dict[str, Any]:
        # Read under the cross-worker lock: status and provider_ref may have
        # moved since the request's read (a concurrent start, stop, cleanup).
        computer = await get_computer(computer_id)
        if not computer:
            raise ValueError(f"Computer {computer_id} not found")
        tiers = self._resource_tiers(computer)
        if _spec_refusal(computer, tier, tiers, claim_id=claim_id):
            return computer

        # A disk shrink risks silently dropping files that don't fit the smaller
        # sandbox (restore is best-effort), so the guard travels as the disk the
        # backup has to fit, and None where the move cannot shrink it. Treat an
        # unknown current tier as a possible downgrade (data-safe).
        current_tier = tiers.get(computer.get("resource_tier") or "standard")
        target_disk = tiers[tier].disk
        may_shrink = current_tier is None or target_disk < current_tier.disk
        await self._apply_spec_change(
            computer,
            tier=tier,
            claim_id=claim_id,
            disk_guard=target_disk if may_shrink else None,
            user_id=user_id,
            workspace_id=workspace_id,
        )
        return await get_computer(computer_id) or computer

    async def _apply_spec_change(
        self,
        computer: Dict[str, Any],
        *,
        tier: str,
        claim_id: str,
        disk_guard: int | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> None:
        """Persist the tier and make the machine's sandbox match it.

        Runs under the machine lock on a row read under it; the settle reverts
        the tier if this raises. Every step that mutates the row or the
        machine first proves the claim is still this runner's."""
        computer_id = str(computer["computer_id"])
        status = computer.get("status")
        sandbox_id = computer.get("provider_ref")

        await self._assert_spec_claim_owned(computer_id, claim_id)
        # Persisted inside the lock, fenced on the status and sandbox it was
        # decided on, so the create/recover paths read the right size and the
        # new tier is not visible before this owns the critical section.
        updated = await db_set_computer_resource_tier(
            computer_id,
            tier,
            expected_status=status,
            expected_provider_ref=sandbox_id,
        )
        if updated is None:
            raise ComputerBusyError(
                "Computer changed while preparing the spec change; retry"
            )
        # The binding is what the recreate sizes from, so it carries the tier
        # just persisted: built from the row read above it would rebuild the
        # sandbox at the size the change is moving away from.
        binding = replace(
            self._binding_from_computer(workspace_id or "", computer),
            resource_tier=tier,
        )

        if not sandbox_id:
            # Never started: the tier is persisted, so the next create/start
            # builds it at the new size.
            logger.info(
                f"Computer {computer_id} has no sandbox; persisted tier {tier!r} only"
            )
        elif status == ComputerStatus.RUNNING:
            await self._replace_running_sandbox(
                binding,
                sandbox_id,
                tier=tier,
                claim_id=claim_id,
                disk_guard=disk_guard,
                user_id=user_id,
                workspace_id=workspace_id,
            )
        else:
            # _spec_refusal leaves only stopped for a machine with a sandbox.
            await self._replace_stopped_sandbox(
                replace(binding, resource_tier=computer.get("resource_tier") or "standard"),
                sandbox_id,
                claim_id=claim_id,
                disk_guard=disk_guard,
                origin_workspace_id=computer.get("origin_workspace_id"),
                user_id=user_id or computer.get("user_id"),
            )

    @asynccontextmanager
    async def _replacement_claim(
        self,
        computer_id: str,
        *,
        from_status: str,
        sandbox_id: str,
        release_on_success: bool = False,
    ) -> AsyncIterator[_Replacement]:
        """Hold the machine at 'starting' for a sandbox replacement; always hand it back.

        Claimed, an acquire on any worker sees 'starting' and waits rather than
        admitting a turn onto the sandbox being replaced. Only this holder can
        release the row, and the start path claims from 'stopped', never from
        'starting', so an exit that skipped the release would strand the
        machine unstartable. The release goes back to ``from_status`` while the
        old sandbox is intact; the caller flips it to 'stopped' as teardown
        begins, from where the next start self-heals. CancelledError is a
        BaseException and is caught too: a shutdown mid-replacement releases the
        row like any failure.
        """
        if not await try_claim_computer_for_start(
            computer_id,
            from_status=from_status,
            expected_provider_ref=sandbox_id,
            require_provider_ref=True,
        ):
            raise ComputerBusyError(
                "This computer changed while preparing the spec change; retry"
            )
        claim = _Replacement(release_to=from_status)
        try:
            yield claim
        except BaseException:
            await update_computer_status(
                computer_id, claim.release_to, expected=ComputerStatus.STARTING
            )
            raise
        if release_on_success:
            await update_computer_status(
                computer_id, claim.release_to, expected=ComputerStatus.STARTING
            )

    async def _replace_stopped_sandbox(
        self, binding: ComputerBinding, sandbox_id: str, *, claim_id: str,
        disk_guard: int | None, origin_workspace_id: Any, user_id: str | None,
    ) -> None:
        """A best-effort stop backup never authorizes deletion of the remaining copy.

        The row keeps naming a sandbox this path already destroyed: nothing
        clears ``provider_ref``, and the next start's restore fence keys on
        it, exactly as after a running replacement that failed past teardown.
        A second change while still stopped therefore finds the sandbox gone,
        which leaves nothing to back up: what was on it is in the DB or
        nowhere, and the start rebuilds from the manifest as it does for any
        sandbox that has gone. The disk guard still applies to the DB copy.
        """
        computer_id = binding.computer_id
        async with self._replacement_claim(
            computer_id,
            from_status=ComputerStatus.STOPPED,
            sandbox_id=sandbox_id,
            release_on_success=True,
        ):
            # No host policy: a resize backs up from this sandbox, and calling
            # an unreachable host gone would skip that and record the new tier
            # over a sandbox the host may still bring back at the old size.
            session = Session(
                computer_id, self._core_config_for(binding),
                computer_id=computer_id, resource_tier=binding.resource_tier,
            )
            gone = False
            try:
                try:
                    await session.initialize(sandbox_id=sandbox_id)
                except SandboxGoneError as e:
                    logger.warning(
                        f"Sandbox {sandbox_id} of stopped computer {computer_id} "
                        f"is already gone ({e}); nothing to back up, the next "
                        "start rebuilds at the new tier"
                    )
                    gone = True
                else:
                    await self._sync_machine_assets(
                        computer_id, user_id, session.sandbox, reusing_sandbox=True,
                        origin_workspace_id=origin_workspace_id,
                    )
                    await self._backup_machine_files_to_db(
                        computer_id, strict=True, expected_sandbox_id=sandbox_id,
                        session=session,
                    )
                if disk_guard is not None:
                    await self._assert_machine_disk_fits(computer_id, disk_guard)
            finally:
                # On any failure, keep the original sandbox stopped and intact.
                await session.stop()
            if not gone:
                await self._assert_spec_claim_owned(computer_id, claim_id)
                await self._destroy_sandbox(sandbox_id, binding=binding)
            # The ref still names this sandbox, so its reading would pass for
            # the machine's at the new tier until the next start measures.
            await clear_computer_disk(computer_id, sandbox_id=sandbox_id)

    async def _project_addressed(self, binding: ComputerBinding) -> ComputerBinding:
        """The same machine binding, addressed by one of its own projects.

        Nothing about a tier belongs to one project, but the recreate is
        project-addressed: binding a project is the write that publishes the
        machine's new sandbox ref, and its folder is where the new sandbox is
        laid out. The oldest live project stands in for a caller that named
        none, the one a consolidated machine's root already belongs to.
        Unchanged when the machine has no live project left.
        """
        workspace_id = binding.workspace_id or ""
        if not workspace_id:
            live = await get_live_workspace_ids_for_computer(binding.computer_id)
            if not live:
                return binding
            workspace_id = live[0]
        return replace(
            binding,
            workspace_id=workspace_id,
            dir_name=await self._workspace_folder(workspace_id),
        )

    async def _assert_machine_is_replaceable(
        self,
        binding: ComputerBinding,
        locked_sandbox_id: str,
        workspace_id: str | None,
    ) -> Session:
        """Refuse active work and attach the exact sandbox that must be backed up."""
        computer_id = binding.computer_id
        await self._refuse_if_turn_active(computer_id, workspace_id)
        # A request can land on a worker that has never served this machine.
        # Reconnect under the cross-worker decision lock held by the caller so
        # the strict backup does not depend on another process's cache.
        session = self._cached_session(computer_id) or SessionManager.get_cached_session(
            computer_id
        )
        if session is not None and (
            not getattr(session, "_initialized", False)
            or getattr(session, "sandbox", None) is None
            or self._session_sandbox_id(session) != locked_sandbox_id
        ):
            await self._retire_session(
                computer_id,
                session,
                reason="spec change must attach the locked sandbox",
            )
            session = None
        if session is None:
            session = self._session_handle(binding, self._core_config_for(binding))
            await session.initialize(sandbox_id=locked_sandbox_id)
            if (
                getattr(session, "sandbox", None) is None
                or self._session_sandbox_id(session) != locked_sandbox_id
            ):
                raise RuntimeError(
                    "Computer changed while attaching it for the spec backup"
                )
        self._put_session(computer_id, session, workspace_id=binding.workspace_id)
        return session

    async def _replace_running_sandbox(
        self,
        binding: ComputerBinding,
        locked_sandbox_id: str,
        *,
        tier: str,
        claim_id: str,
        disk_guard: int | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> None:
        """Swap a live machine's sandbox for one built at the persisted tier."""
        computer_id = binding.computer_id
        # Resolved here and not with the machine binding: the other branches
        # need no project, and a folder read that fails must not stop a machine
        # that has no sandbox to lay out from changing tier.
        binding = await self._project_addressed(binding)
        if not binding.workspace_id:
            # Backup, bind and restore are all project-addressed, and a machine
            # with no live project has nothing to rebuild for. Refuse before any
            # teardown, where recreating would delete the sandbox and bind
            # nothing back.
            raise RuntimeError(
                "This computer has no live project to rebuild its sandbox for"
            )
        session = await self._assert_machine_is_replaceable(
            binding, locked_sandbox_id, workspace_id
        )

        logger.info(f"Recreating computer {computer_id} at tier {tier!r}")
        # Fence the machine BEFORE the backup: while the row reads running, a
        # turn on any worker is admitted onto the old sandbox, and whatever it
        # writes after its project was mirrored is lost with that sandbox.
        async with self._replacement_claim(
            computer_id,
            from_status=ComputerStatus.RUNNING,
            sandbox_id=locked_sandbox_id,
        ) as claim:
            # A turn admitted between the first check and the claim is already
            # past the gate the claim closes.
            await self._refuse_if_turn_active(computer_id, workspace_id)
            # Strict: the sandbox is about to be destroyed, so a missed backup
            # here is data loss, not a degraded sync. Pinned to the locked
            # sandbox id: a session bound to an already-superseded sandbox
            # would snapshot the wrong filesystem over the DB copy.
            await self._backup_machine_files_to_db(
                computer_id,
                workspace_id=workspace_id,
                strict=True,
                expected_sandbox_id=locked_sandbox_id,
                session=session,
            )
            if disk_guard is not None:
                # Post-backup (fresh DB sizes) and pre-teardown, so a refusal
                # leaves the live sandbox as it was.
                await self._assert_machine_disk_fits(computer_id, disk_guard)

            await self._assert_spec_claim_owned(computer_id, claim_id)
            # Teardown starts: the files are backed up and the old sandbox is
            # no longer something to hand back, so a failure from here leaves
            # the row stopped and the next start rebuilds (claim -> restart ->
            # SandboxGone -> recover). 'error' would be terminal.
            claim.release_to = ComputerStatus.STOPPED
            # Force-evict the SessionManager entry: cleanup_session skips its
            # own pop when session.cleanup() raised, so the recover below must
            # not return the stale broken session.
            await self._clear_session(computer_id)
            SessionManager.remove_session(computer_id)
            # The teardown's own delete may still be settling, or may never have
            # been sent if cleanup raised first. The new sandbox is built only
            # once the old one is confirmed gone, so the two never run, and
            # bill, side by side.
            await self._destroy_sandbox(locked_sandbox_id, binding=binding)
            # A recover that fails leaves the ref on the destroyed sandbox.
            await clear_computer_disk(computer_id, sandbox_id=locked_sandbox_id)
            await self._recover_sandbox(
                binding, user_id, self._core_config_for(binding)
            )

    async def _destroy_sandbox(
        self, sandbox_id: str, *, binding: Any = None, timeout: float = 60.0
    ) -> None:
        """Delete a sandbox by id and return once the provider no longer has it.

        A provider that changes state asynchronously refuses a delete while a
        stop or an earlier delete of the same sandbox is still settling
        (Daytona answers 409 "state change in progress"), so a refused delete
        is retried until one is accepted, and only the provider reporting the
        sandbox absent ends the wait; callers may block for up to ``timeout``.
        Raising when that never happens keeps a caller from building a
        replacement beside a sandbox still billed. A sandbox already gone
        returns normally. ``binding`` points it at the machine's own backend.
        """
        provider = self._provider_for(binding)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        accepted = False
        refused = 0
        last: Exception | None = None
        try:
            while True:
                try:
                    runtime = await provider.get(sandbox_id)
                    if not accepted:
                        await runtime.delete()
                        accepted = True
                        if refused:
                            logger.info(
                                "Delete of sandbox %s accepted after %d refusal(s): %s",
                                sandbox_id,
                                refused,
                                last,
                            )
                except Exception as e:
                    if self._is_sandbox_gone(e, binding):
                        return
                    if not accepted:
                        refused += 1
                    last = e
                if loop.time() >= deadline:
                    # The provider's text stays in the log: the deprecated
                    # workspace route answers a RuntimeError with its message.
                    logger.warning(
                        "Sandbox %s still exists %.0fs after its delete was "
                        "requested: %s",
                        sandbox_id,
                        timeout,
                        last,
                    )
                    raise RuntimeError(
                        "The old sandbox could not be removed; retry the spec change"
                    ) from last
                await asyncio.sleep(1.0)
        finally:
            await provider.close()

    async def _assert_machine_disk_fits(
        self, computer_id: str, target_disk_gib: int
    ) -> None:
        """Refuse a downgrade whose backed-up files would not fit the target disk.

        File restore is per-file best effort, so a sandbox recreated on a
        smaller disk silently drops whatever overflows. One disk holds every
        project on the machine, so the sum is the guard: checking only the
        project that asked is how a two-project machine passes a downgrade its
        combined files cannot fit. Call before teardown.
        """
        sizes = await get_live_project_sizes_for_computer(computer_id)
        # Each project's restore stages at most its own size, never a whole
        # window, and the base reserve already holds one window.
        staged = sorted(min(PULL_MAX_INFLIGHT_BYTES, s) for s in sizes)
        reserve = _DISK_SYSTEM_RESERVE_BYTES + sum(staged[:-1])
        usable = max(0, target_disk_gib * _GIB - reserve)
        total = sum(sizes)
        if total > usable:
            raise DiskTooSmallError(
                f"Cannot downgrade: files on this computer "
                f"({total / _GIB:.1f} GiB) exceed the {target_disk_gib} GiB "
                f"tier's usable space (~{usable / _GIB:.1f} GiB). "
                "Free up space first."
            )
