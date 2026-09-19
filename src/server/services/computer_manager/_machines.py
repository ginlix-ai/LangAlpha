"""Seam: the computer-addressed surface, and a machine with no project on it.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from ptc_agent.core.session import Session, SessionManager


from src.server.database.computer import (
    computer_advisory_key,
    create_computer,
    default_computer_name,
    get_computer,
    get_computer_by_provider_ref,
    get_computer_for_workspace,
    get_primary_computer,
    try_bind_computer_provider_ref,
    try_claim_computer_for_start,
    update_computer_activity,
    update_computer_status,
)
from src.server.database.sql_fences import workspace_dir_name
from src.server.database.workspace import (
    bind_workspace_to_computer,
    get_live_workspace_ids_for_computer,
    get_workspace as db_get_workspace,
    WorkspaceDirNameTaken,
)
from src.server.models.computer import CLAIMABLE_FOR_START, ComputerStatus
from src.server.services.user_skills import sandbox_skill_sync_params

from src.server.services.computer_manager._types import (
    _MACHINE_DECISION_LOCK_TIMEOUT_MS,
)

logger = logging.getLogger(__name__)


class MachineLifecycleMixin:
    async def create_computer_for_user(
        self,
        user_id: str,
        *,
        name: Optional[str] = None,
        resource_tier: Optional[str] = None,
        is_primary: bool = False,
    ) -> Dict[str, Any]:
        """Creation stays stopped and unprovisioned so billing begins only on start."""
        return await create_computer(
            user_id,
            kind=self.config.sandbox.provider,
            name=name or default_computer_name(self.config.sandbox.provider),
            is_primary=is_primary,
            status="stopped",
            resource_tier=resource_tier or "standard",
            root_dir=self.config.filesystem.working_directory,
        )

    async def ensure_primary_computer(
        self,
        user_id: str,
        *,
        name: Optional[str] = None,
        resource_tier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reuse the primary computer to avoid paying for a sandbox per project.

        Creation is stopped until first start. A concurrent primary insert can
        return a non-primary loser; tombstone that row rather than hand it out."""
        computer = await get_primary_computer(user_id)
        if computer is not None:
            return computer

        computer = await self.create_computer_for_user(
            user_id, name=name, resource_tier=resource_tier, is_primary=True
        )
        if computer.get("is_primary"):
            return computer

        computer_id = str(computer["computer_id"])
        logger.info(
            f"User {user_id} gained a primary computer while {computer_id} was "
            "being inserted; tombstoning it and using theirs"
        )
        await update_computer_status(computer_id, "deleted")
        winner = await get_primary_computer(user_id)
        if winner is None:
            # A racing delete can remove the winner; never return the tombstoned loser.
            raise RuntimeError(f"User {user_id} has no primary computer")
        return winner

    async def _adopt_workspace_onto_computer(
        self, workspace_id: str, *, workspace: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Give a project left unbound by migration 046 a machine, once, here.

        Two projects naming one sandbox are two projects on one machine, so an
        existing computer for that sandbox is adopted rather than duplicated. The
        bind is a CAS on ``computer_id IS NULL``, which is what makes two
        concurrent requests settle on one machine instead of two."""
        if workspace is None:
            workspace = await db_get_workspace(workspace_id)
        if not workspace:
            return None
        status = workspace.get("status")
        if status in ("flash", "deleted"):
            # A flash workspace has no sandbox lifecycle to give it.
            return None

        user_id = workspace.get("user_id")
        sandbox_id = workspace.get("sandbox_id")
        computer: Optional[Dict[str, Any]] = None
        adopt_ref = str(sandbox_id) if sandbox_id else None
        if adopt_ref:
            computer = await get_computer_by_provider_ref(
                self.config.sandbox.provider, adopt_ref
            )
            if computer is not None and str(computer.get("user_id")) != str(user_id):
                # A sandbox is one user's files and secrets. A row naming
                # another user's sandbox gets a machine of its own and
                # restores from its mirror rather than joining that machine.
                logger.warning(
                    f"Workspace {workspace_id} names sandbox {adopt_ref}, which "
                    f"belongs to computer {computer['computer_id']} of another "
                    "user; provisioning a separate machine"
                )
                computer = None
                adopt_ref = None
        minted_here = computer is None
        if computer is None:
            computer = await create_computer(
                user_id,
                kind=self.config.sandbox.provider,
                name=workspace.get("name")
                or default_computer_name(self.config.sandbox.provider),
                status=status
                if status in ("running", "stopped") and adopt_ref
                else "stopped",
                provider_ref=adopt_ref,
                resource_tier=workspace.get("resource_tier") or "standard",
                is_always_on=bool(workspace.get("is_always_on")),
                platform_secret_version=int(
                    workspace.get("platform_secret_version") or 0
                ),
                root_dir=self.config.filesystem.working_directory,
                origin_workspace_id=workspace_id if adopt_ref else None,
            )
            if adopt_ref and str(computer.get("origin_workspace_id")) != str(
                workspace_id
            ):
                minted_here = False  # a concurrent adopter's row; not ours to tombstone
        computer_id = str(computer["computer_id"])

        for attempt in range(3):
            try:
                bound = await bind_workspace_to_computer(
                    workspace_id,
                    computer_id,
                    expected_computer_id=None,
                    dir_name=workspace.get("dir_name")
                    or workspace_dir_name(
                        workspace.get("name"), workspace_id, hex_chars=4 + 4 * attempt
                    ),
                )
                break
            except WorkspaceDirNameTaken:
                logger.info(
                    f"Folder for workspace {workspace_id} is taken on computer "
                    f"{computer_id}; re-slugging"
                )
        else:
            bound = None

        if bound is None:
            if minted_here:
                # The row this call inserted names no project now and would
                # otherwise sit in the user's computer list, startable and
                # billable, with nothing ever stopping it.
                await update_computer_status(computer_id, "deleted")
            # Another request bound it first; that machine is the answer. A
            # project whose folder never settled has no machine at all.
            return await get_computer_for_workspace(workspace_id)
        logger.info(
            f"Adopted workspace {workspace_id} onto computer {computer_id} "
            "(migration 046 left it unbound)"
        )
        # The bind is what assigns the folder, and it returns the project row it
        # wrote, so the machine row can carry the folder out of here without a
        # second question about it.
        return {**computer, "dir_name": bound.get("dir_name")}

    async def _provision_first_sandbox(
        self, workspace: Dict[str, Any], user_id: str | None
    ) -> Session:
        """Instant creation defers provisioning until first start.

        Recovery supplies entitled sizing and restores mirrored files for duplicates;
        a new project has no files to restore."""
        workspace_id = str(workspace["workspace_id"])
        binding = await self.resolve_binding(workspace_id, workspace=workspace)
        session = await self._recover_sandbox(
            binding, user_id, self._core_config_for(binding)
        )
        await self._seed_agent_md(
            session.sandbox,
            workspace.get("name") or "",
            workspace.get("dir_name"),
        )
        return session

    @asynccontextmanager
    async def _machine_decision_lock(self, computer_id: str):
        """Hold C(computer) without a transaction across provider round trips.

        The session lock uses statement_timeout so contention cannot pin a request
        forever; False means the wait expired."""
        from src.server.database.pool import get_db_connection

        key = computer_advisory_key(computer_id)
        async with get_db_connection() as conn:
            await conn.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(_MACHINE_DECISION_LOCK_TIMEOUT_MS),),
            )
            try:
                try:
                    await conn.execute("SELECT pg_advisory_lock(%s)", (key,))
                except Exception as e:
                    logger.warning(
                        f"Could not take the decision lock on computer "
                        f"{computer_id}: {e}; leaving the machine as it is"
                    )
                    yield False
                    return
                try:
                    yield True
                finally:
                    try:
                        await conn.execute("SELECT pg_advisory_unlock(%s)", (key,))
                    except Exception as e:
                        logger.warning(
                            f"Could not release the decision lock on computer "
                            f"{computer_id}: {e}"
                        )
            finally:
                # On every exit, the lock-failure return included: the pool
                # does not reset session settings, so the next borrower would
                # inherit the timeout.
                try:
                    await conn.execute(
                        "SELECT set_config('statement_timeout', '0', false)"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not clear statement_timeout after the decision "
                        f"lock on computer {computer_id}: {e}"
                    )

    async def _retire_machine_if_empty(
        self, computer_id: str, workspace_id: str, workspace: Dict[str, Any]
    ) -> bool:
        """Check emptiness under the machine advisory key after project tombstoning.

        Otherwise concurrent last-project deletes could both see a sibling and
        leave the machine alive. Keep empty primaries for future projects to reuse
        their sandbox and disk; only final retirement tombstones the machine."""
        try:
            async with self._machine_decision_lock(computer_id) as locked:
                computer = await get_computer(computer_id)
                if computer is None:
                    logger.info(
                        f"Computer {computer_id} is already retired; nothing "
                        "left to tear down"
                    )
                    return False
                if locked and await self._machine_is_finished(computer_id, computer):
                    await self._teardown_machine(
                        self._binding_from_computer(workspace_id, computer)
                    )
                    await update_computer_status(computer_id, ComputerStatus.DELETED)
                    return True
                # The shared machine survives; remove only the departing project's folder.
                await self._remove_workspace_folder(workspace_id, workspace, computer)
                return False
        except Exception as e:
            # The project is already unlinked; do not fail its completed delete.
            # Start paths can reconcile a machine left running behind its live row.
            logger.warning(
                f"Could not settle computer {computer_id} after deleting "
                f"workspace {workspace_id}: {e}"
            )
            return False

    @staticmethod
    async def _machine_is_finished(computer_id: str, computer: Dict[str, Any]) -> bool:
        """The caller must hold the machine advisory key to serialize delete decisions."""
        remaining = await get_live_workspace_ids_for_computer(computer_id)
        if remaining:
            logger.info(
                f"Computer {computer_id} still carries {len(remaining)} "
                "live workspace(s); leaving it in place"
            )
            return False
        if computer.get("is_primary"):
            logger.info(
                f"Computer {computer_id} is the user's primary and is kept "
                "with no project on it; the next one joins this machine"
            )
            return False
        return True

    async def _claim_machine_for_stop(
        self, computer_id: str
    ) -> Optional[Dict[str, Any]]:
        """Whoever wins running -> stopping owns the teardown; losing is not an error.

        The computer is the lifecycle authority, so a project shadow that lags
        behind it cannot veto a stop the machine still needs, and cannot make the
        idle reaper raise on every cycle while the sandbox keeps billing.
        """
        computer = await get_computer(computer_id)
        if computer is None:
            raise ValueError(f"Computer {computer_id} not found")
        status = computer["status"]
        if status in (ComputerStatus.STOPPING, ComputerStatus.STOPPED):
            logger.info(
                f"Computer {computer_id} is already {status!r}; nothing to stop"
            )
            return None
        if status != ComputerStatus.RUNNING:
            raise RuntimeError(
                f"Cannot stop computer in '{status}' state. "
                "Only running computers can be stopped."
            )

        claimed = await update_computer_status(
            computer_id, ComputerStatus.STOPPING, expected=ComputerStatus.RUNNING
        )
        if claimed is None:
            logger.info(
                f"Computer {computer_id} left 'running' while its stop was being "
                "claimed; leaving the transition to whoever moved it"
            )
        return claimed

    async def _settle_machine_stop(self, computer_id: str, status: str) -> None:
        """CAS from 'stopping' so a teardown's tail cannot stomp a peer's transition."""
        if (
            await update_computer_status(computer_id, status, expected="stopping")
            is None
        ):
            logger.info(
                f"Computer {computer_id} was moved out of 'stopping' by another "
                f"worker; leaving it rather than writing {status!r}"
            )

    async def _retain_machine_stop_claim(self, computer_id: str) -> bool:
        """Fence provider stop against a row another worker has recovered."""
        return (
            await update_computer_status(
                computer_id,
                ComputerStatus.STOPPING,
                expected=ComputerStatus.STOPPING,
            )
            is not None
        )

    async def _machine_ready_to_archive(self, computer_id: str) -> Dict[str, Any]:
        """Archive moves no status, so the machine's own row is the only gate."""
        computer = await get_computer(computer_id)
        if computer is None:
            raise ValueError(f"Computer {computer_id} not found")
        if computer["status"] != "stopped":
            raise RuntimeError(
                f"Cannot archive computer in '{computer['status']}' state. "
                "Only stopped computers can be archived."
            )
        if not computer.get("provider_ref"):
            raise RuntimeError("No sandbox associated with this computer")
        return computer

    # Bare computers have no folder, file mirror or project MCP set. Use machine
    # statements directly so subscribers see the same lifecycle frames without
    # inventing a placeholder project that would pollute listings.

    def _machine_session_if_live(self, computer: Dict[str, Any]) -> Optional[Session]:
        session = self._cached_session(str(computer["computer_id"]))
        if session is None or not session._initialized or session.sandbox is None:
            return None
        if session.sandbox.has_failed():
            return None
        if self._session_sandbox_id(session) != computer.get("provider_ref"):
            return None
        return session

    async def _sync_machine_assets(
        self,
        computer_id: str,
        user_id: str | None,
        sandbox: Any,
        *,
        reusing_sandbox: bool,
        origin_workspace_id: Any = None,
    ) -> None:
        """Project tokens, folders, vault secrets and MCP wait for a project to join.

        The layout owner is the exception: a bare start still runs the v3 to v4
        move, and leaving it no target would strand the root files outside every
        project's folder for the machine's life."""
        if sandbox is None:
            return
        skill_dirs = (
            self.config.skills.local_skill_dirs_with_sandbox()
            if self.config.skills.enabled
            else None
        )
        user_skill_params = await sandbox_skill_sync_params(
            user_id, self.config.skills.sandbox_skills_base
        )
        result = await sandbox.sync_sandbox_assets(
            skill_dirs=skill_dirs,
            reusing_sandbox=reusing_sandbox,
            user_id=user_id,
            root_owner_dir_name=await self._root_owner_folder(
                computer_id, origin_workspace_id
            ),
            **user_skill_params,
        )
        await self._stamp_layout_version(computer_id, result)

    async def _build_machine_session(
        self,
        computer: Dict[str, Any],
        *,
        user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
    ) -> Session:
        """New sandboxes require the provider-ref CAS to elect one provisioner.

        Reconnections already own that binding, so status updates preserve identity
        and the certified secret generation."""
        computer_id = str(computer["computer_id"])
        user_id = user_id or computer.get("user_id")
        binding = self._binding_from_computer("", computer)
        # A bare start is a (re)provision like any other: the persisted tier and
        # always-on are what the plan granted once, not what it grants now.
        tier = await self._entitled_tier(binding, user_id)
        always_on = await self._entitled_always_on(binding, user_id)
        binding = replace(binding, resource_tier=tier, is_always_on=always_on)
        core_config = self._core_config_for(binding)
        previous_ref = binding.provider_ref
        reconnected = previous_ref is not None
        tier_lapsed = reconnected and (computer.get("resource_tier") or tier) != tier

        def _handle() -> Session:
            return SessionManager.get_session(
                computer_id,
                core_config,
                label=computer_id,
                computer_id=computer_id,
                resource_tier=binding.resource_tier,
            )

        session = _handle()
        try:
            if tier_lapsed:
                # The sandbox was sized for a tier the plan no longer grants;
                # it is rebuilt at the entitled size from the backed-up files.
                logger.info(
                    f"Computer {computer_id} lost its tier entitlement; "
                    f"replacing sandbox {previous_ref} at tier {tier!r}"
                )
                try:
                    await self._destroy_sandbox(previous_ref, binding=binding)
                except Exception as e:
                    logger.warning(
                        f"Could not destroy lapsed-tier sandbox {previous_ref} "
                        f"for computer {computer_id}: {e}"
                    )
                reconnected = False
            if reconnected:
                try:
                    await session.initialize(
                        sandbox_id=previous_ref,
                        on_state_observed=on_state_observed,
                    )
                except SandboxGoneError as e:
                    logger.warning(
                        f"Sandbox {previous_ref} is gone for computer "
                        f"{computer_id} ({e}); building a fresh one"
                    )
                    await self._clear_session(computer_id, evict_session=session)
                    session = _handle()
                    reconnected = False
            if not reconnected:
                await session.initialize(
                    user_id=user_id,
                    tier=tier,
                    auto_stop_minutes=0 if always_on else None,
                )

            sandbox_id = self._session_sandbox_id(session)
            runtime = getattr(session.sandbox, "runtime", None)
            if not sandbox_id or runtime is None:
                raise RuntimeError(
                    f"Computer {computer_id} came up without a sandbox identity"
                )
            if reconnected and bool(computer.get("is_always_on")) != always_on:
                await self._apply_autostop_for_always_on(
                    sandbox_id, enabled=always_on, runtime=runtime, binding=binding
                )

            await self._sync_machine_assets(
                computer_id,
                user_id,
                session.sandbox,
                reusing_sandbox=reconnected,
                origin_workspace_id=computer.get("origin_workspace_id"),
            )

            if reconnected:
                if (
                    await update_computer_status(
                        computer_id,
                        ComputerStatus.RUNNING,
                        expected=("starting", "running"),
                    )
                    is None
                ):
                    # The row left the states a reconnect may publish from, so
                    # this handle is not the machine's current lifecycle and
                    # must not be cached as it.
                    raise SandboxTransientError(
                        f"Computer {computer_id} was moved out of its start "
                        f"while sandbox {sandbox_id} was reconnecting"
                    )
            else:
                from src.server.services.platform_secret_rollout import (
                    certify_platform_secrets,
                )

                # Certify before binding so no row can name an uncertified sandbox.
                secret_version = await certify_platform_secrets(
                    core_config, runtime=runtime
                )
                bound = await try_bind_computer_provider_ref(
                    computer_id,
                    provider_ref=sandbox_id,
                    expected_previous_provider_ref=previous_ref,
                    platform_secret_version=secret_version,
                )
                if bound is None:
                    # The losing sandbox is unreferenced and billed; the unwind must destroy it.
                    raise SandboxTransientError(
                        f"Computer {computer_id} was bound to a different "
                        f"sandbox while {sandbox_id} was being provisioned"
                    )

            self._put_session(computer_id, session)
            self._record_sync(computer_id)
            await update_computer_activity(computer_id)
            logger.info(
                f"Computer {computer_id} is running on sandbox {sandbox_id} "
                f"({'reconnected' if reconnected else 'built'})"
            )
            return session
        except (Exception, asyncio.CancelledError):
            if reconnected:
                # The existing sandbox still belongs to the row; destroying it would lose its disk.
                await self._retire_session(
                    computer_id, session, reason="machine start failed"
                )
            else:
                await self._clear_session(computer_id, evict_session=session)
            raise

    async def _revert_machine_start(self, computer_id: str) -> None:
        """CAS only from starting so failure cannot stop a machine another worker moved."""
        try:
            await update_computer_status(
                computer_id, ComputerStatus.STOPPED, expected="starting"
            )
        except Exception:
            logger.exception(
                f"Failed to revert computer {computer_id} after a failed start"
            )

    async def _start_machine(
        self,
        computer_id: str,
        *,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> Optional[Session]:
        """Another start owner publishes progress, so losing the claim returns None.

        Ownership of a transition is whatever the row's CAS says. A machine mid
        transition is left to its owner even on this worker, because the machine
        lock above already serialises a same-worker re-entry behind it, and
        reap_stuck_starting_workspaces is what covers an owner that died."""
        async with self._observed_lock(computer_id, "computer.start"):
            computer = await get_computer(computer_id)
            if computer is None:
                raise ValueError(f"Computer {computer_id} not found")
            status = computer["status"]
            if status == ComputerStatus.DELETED:
                raise RuntimeError(f"Computer {computer_id} has been deleted")
            if status == ComputerStatus.ERROR:
                raise RuntimeError(
                    f"Computer {computer_id} is in error state. "
                    "Please delete and recreate."
                )

            if status in CLAIMABLE_FOR_START:
                claimed = await try_claim_computer_for_start(
                    computer_id, from_status=status
                )
                if claimed is None:
                    return None
                computer = claimed
            elif status == ComputerStatus.RUNNING:
                live = self._machine_session_if_live(computer)
                if live is not None:
                    return live
                # A running row with no local handle needs a reconnect, which
                # _build_machine_session re-CASes from 'running'.
            else:
                logger.info(
                    f"Computer {computer_id} is {status!r}; leaving the "
                    "transition to whoever claimed it"
                )
                return None

            try:
                return await self._build_machine_session(
                    computer, user_id=user_id, on_state_observed=on_state_observed
                )
            except (Exception, asyncio.CancelledError):
                # CancelledError is a BaseException: a client dropping mid
                # provision would otherwise leave the machine at 'starting'
                # for every project on it until the reaper runs.
                await asyncio.shield(self._revert_machine_start(computer_id))
                raise

    # The router must check ownership; these methods act on any supplied computer.

    async def get_session_for_computer(
        self,
        computer_id: str,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> Session:
        """The machine's own session, with no project's lifecycle borrowed for it.

        A project on this machine materialises at its first use through
        _ensure_project_attached, the same way every sibling already does."""
        session = await self._start_machine(
            computer_id, user_id=user_id, on_state_observed=on_state_observed
        )
        if session is None:
            raise RuntimeError(
                f"Computer {computer_id} is being started by another worker"
            )
        return session

    async def start_computer(self, computer_id: str) -> Optional[Dict[str, Any]]:
        """Starting a computer must not require a project folder."""
        await self._start_machine(computer_id)
        return await get_computer(computer_id)

    async def stop_computer(self, computer_id: str) -> Optional[Dict[str, Any]]:
        return await self._stop_machine(computer_id)

    async def archive_computer(self, computer_id: str) -> Optional[Dict[str, Any]]:
        return await self._archive_machine(computer_id)
