"""Seam: the sandbox behind a machine, its file mirror and its layout stamps.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, Dict, Optional

from ptc_agent.config import AgentConfig
from ptc_agent.core.paths import WorkspaceLayout
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.sandbox import assets as sandbox_assets
from ptc_agent.core.sandbox.migration import LayoutMigrationError
from ptc_agent.core.sandbox.runtime import SandboxGoneError
from ptc_agent.core.session import Session, SessionManager

from src.server.database.computer import (
    DEFAULT_ROOT_DIR,
    get_computer,
    stamp_computer_layout_version,
    stamp_computer_mcp_config_version,
    try_claim_computer_for_start,
    update_computer_status,
)
from src.server.database.workspace import (
    flag_sibling_restores_pending,
    get_workspace_dir_names_for_computer,
    get_workspace as db_get_workspace,
    get_workspace_dir_name as db_get_workspace_dir_name,
    get_workspace_identity as db_get_workspace_identity,
    get_restore_owed_workspace_ids_for_computer,
    SandboxIdentityLostError,
    update_workspace_activity,
)
from src.server.database.workspace_folders import (
    WorkspaceFolderMoving,
    is_top_level,
    workspace_folder_in_use,
)
from src.server.models.computer import ComputerStatus
from src.server.services.persistence.file import (
    FilePersistenceService,
    RestoreGuardUnavailable,
    RestoreIdentityLost,
)
from src.server.services.persistence.sync_result import BackupIncomplete
from src.server.services.workspace_layout import (
    held_workspace_layout,
    layout_from_binding,
    WorkspaceLayoutUnavailable,
)
from src.server.services.user_skills import sandbox_skill_sync_params

from src.server.services.computer_manager._types import (
    ComputerBinding,
    _PROJECTS_ATTACHED_CAP,
    _SEEDED_AGENT_MD,
)

logger = logging.getLogger(__name__)

# Sibling folders one restore pass fills at once: a recreated sandbox is one
# small machine, already busy with the turn that recreated it.
_SIBLING_RESTORES_AT_ONCE = 3


class ProvisioningMixin:
    async def _teardown_machine(self, binding: ComputerBinding) -> None:
        """Only tear down an unused machine whose files are already mirrored.

        cleanup_session deletes its attached sandbox, so stale handles must be
        retired intact and only the durable machine reference may be destroyed."""
        # Only the computer ref authorizes deletion; a drifted workspace shadow
        # may name someone else's sandbox. NULL means nothing to destroy.
        computer_id = binding.computer_id
        durable_sandbox_id = binding.provider_ref
        await self._revoke_livefs(computer_id)

        session = self._cached_session(computer_id)
        attached_sandbox_id = self._session_sandbox_id(session)
        machine = self._machine_if_known(computer_id)
        if machine is not None:
            machine.pending_lazy_sync = False
            machine.last_sync_at = None
        self._cancel_mcp_discovery(computer_id)

        if session is not None and attached_sandbox_id != durable_sandbox_id:
            # Cleanup would delete a stale handle's sandbox, which this machine does not own.
            await self._retire_session(
                computer_id,
                session,
                reason="stale handle; teardown deletes the machine's own sandbox",
            )
        else:
            self._drop_session(computer_id)
            try:
                await SessionManager.cleanup_session(computer_id)
            except Exception as e:
                # The sandbox keeps billing until something deletes it, so a
                # failed cleanup falls through to the detached teardown.
                logger.warning(f"Error cleaning up from SessionManager: {e}")
                attached_sandbox_id = None

        if durable_sandbox_id and attached_sandbox_id != durable_sandbox_id:
            await self._detached_sandbox_teardown(
                durable_sandbox_id, delete=True, binding=binding
            )

    def _forget_project(self, workspace_id: str) -> None:
        """The computer owns the session, metadata and lock; they outlive a project.

        What this worker knows of the project's folder goes with it: a folder
        made again, on a computer the project left and came back to, holds
        neither its files nor the mount's links until they are laid again. For
        the links, the delete or move that took the project off the computer
        already dropped its rows for every worker (``drop_workspace_links``);
        this drops this worker's copy now, which a link it has in flight does
        not bring back."""
        self._projects_attached = {
            key for key in self._projects_attached if key[0] != workspace_id
        }
        self._session_computer.pop(workspace_id, None)
        for machine in self._machines.values():
            machine.livefs.unlinked(workspace_id)

    @staticmethod
    def _warn_unshadowed(
        workspace_id: str, computer: Dict[str, Any], transition: str
    ) -> None:
        """Shadow drift is nonfatal: the computer is authoritative.

        Project fences may reject the shadow write; the next project write reconciles."""
        shadowed = {str(w) for w in (computer.get("shadowed_workspace_ids") or ())}
        if str(workspace_id) not in shadowed:
            logger.warning(
                f"Computer {computer.get('computer_id')} {transition} left "
                f"workspace {workspace_id} unshadowed (moved {sorted(shadowed)})"
            )

    async def _sync_sandbox_assets(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        sandbox: Any,
        reusing_sandbox: bool = False,
        force_refresh: bool = False,
    ) -> Any:
        """``_sync_project_assets`` for code about to run on ``sandbox``: the
        file mount comes up beside the sync when due."""
        return await self._livefs_beside(
            binding.computer_id,
            user_id,
            sandbox,
            self._sync_project_assets(
                binding, user_id, sandbox, reusing_sandbox, force_refresh
            ),
            workspace_id=binding.workspace_id,
        )

    async def _sync_project_assets(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        sandbox: Any,
        reusing_sandbox: bool = False,
        force_refresh: bool = False,
    ) -> Any:
        """Upload this project's assets, returning the asset leg's result or None.

        None means that leg failed: the acquisition path only logs it, since a
        stale tool module is not a reason to refuse a turn, but the refresh
        route answers on it. Layout failures always propagate because the
        project paths cannot be used until its files have moved.

        The folder is not the binding's: an acquisition reads that before a
        settle on another worker can move it. An overlay built through the old
        folder recreates it and stamps this claim current in the tool ledger,
        so the folder that landed keeps its stale tools with no sync owed.
        """
        if not sandbox:
            return None
        workspace_id = binding.workspace_id

        skill_dirs = (
            self.config.skills.local_skill_dirs_with_sandbox()
            if self.config.skills.enabled
            else None
        )

        # Include tokens and user data in the manifest hash to skip unchanged uploads.
        _sync_t0 = time.time()

        async def _mint_and_sync_assets() -> Any:
            tokens = {}
            if reusing_sandbox and user_id:
                tokens = await self._mint_sandbox_tokens(
                    user_id, workspace_id, binding.computer_id
                )

            user_skill_params = await sandbox_skill_sync_params(
                user_id,
                self.config.skills.sandbox_skills_base,
                workspace_id=workspace_id,
            )
            vault_owner, vault_secrets, vault_fingerprint = await self._vault_snapshot(
                workspace_id, user_id
            )
            try:
                workspace_dirs = await get_workspace_dir_names_for_computer(binding.computer_id)
            except Exception as exc:
                raise LayoutMigrationError("Cannot identify sibling folders safely") from exc
            view = self._machine(binding.computer_id).tool_views.get(workspace_id)
            async with self._held_workspace_folder(workspace_id) as dir_name:
                with sandbox_assets.asset_sync_context(
                    mcp_registry=view.mcp_registry if view is not None else None,
                    mcp_servers=view.mcp_servers if view is not None else None,
                    vault_secrets=vault_secrets,
                ):
                    result = await sandbox.sync_sandbox_assets(
                        skill_dirs=skill_dirs,
                        reusing_sandbox=reusing_sandbox,
                        force_refresh=force_refresh,
                        tokens=tokens or None,
                        user_id=user_id,
                        project=ProjectContext(workspace_id, dir_name or ""),
                        root_owner_dir_name=await self._layout_root_owner_dir(
                            replace(binding, dir_name=dir_name)
                        ),
                        workspace_dir_names=workspace_dirs,
                        **user_skill_params,
                    )
            if vault_owner is not None:
                # The sync published a decrypt taken before it waited on the
                # folder and the lock; a push may have landed newer meanwhile.
                vault_secrets = await self._settle_vault(
                    vault_owner,
                    sandbox,
                    vault_secrets,
                    vault_fingerprint,
                    published=True,
                )
            sandbox.vault_secrets = vault_secrets
            await self._stamp_layout_version(binding.computer_id, result)
            await self._stamp_mcp_config_version(binding.computer_id)
            return result

        assets: Any = None
        try:
            assets = await _mint_and_sync_assets()
        except LayoutMigrationError:
            raise
        except WorkspaceFolderMoving:
            logger.info(f"Asset sync for {workspace_id} waits for its folder to land")
        except Exception as e:
            logger.warning(f"Asset sync failed for {workspace_id}: {e}")
        total = (time.time() - _sync_t0) * 1000
        logger.info(f"[SYNC_DETAIL] workspace_id={workspace_id} total={total:.0f}ms")
        return assets

    async def refresh_project_assets(
        self, workspace_id: str, user_id: str | None, sandbox: Any
    ) -> Any:
        """Force one project's assets back in sync on a machine already running.

        The refresh route's half of the acquisition-path sync, and the same
        call: the folder and the v3 root owner it owes a layout migration are
        derived from the binding, which is the manager's to resolve. A route
        that rebuilt either would drift from the path that provisions. No
        command runs next, so the file mount waits for the next turn.
        """
        binding = await self.resolve_binding(workspace_id)
        return await self._sync_project_assets(
            binding,
            user_id,
            sandbox,
            reusing_sandbox=True,
            force_refresh=True,
        )

    async def _workspace_folder(self, workspace_id: str) -> Optional[str]:
        """Read the assigned folder afresh so layout migration cannot use stale state.

        Raises rather than answering None on a failed read: None spells "this
        project owns the whole machine", which is true only of a project bound
        to no computer, and on the backup path that spelling makes one
        project's scan claim its siblings' files."""
        try:
            return await db_get_workspace_dir_name(workspace_id)
        except Exception as e:
            raise WorkspaceLayoutUnavailable(
                f"Could not read the folder for workspace {workspace_id}: {e}"
            ) from e

    @asynccontextmanager
    async def _held_workspace_folder(self, workspace_id: str) -> AsyncIterator[Optional[str]]:
        """The folder to write generated content into, which no settle moves until exit."""
        async with held_workspace_layout(workspace_id, DEFAULT_ROOT_DIR) as layout:
            yield layout.dir_name if layout is not None else None

    async def _project_layout(
        self,
        workspace_id: str,
        computer_id: Optional[str],
        *,
        dir_name: Optional[str] = None,
        root: Optional[str] = None,
    ) -> WorkspaceLayout:
        """The folder this project owns on its machine, as the persistence layer wants it.

        Built from the binding rather than resolved inside the persistence layer
        so a project bound to a machine but naming no folder raises here, where
        the caller still owns the sandbox, instead of widening to the machine
        root mid-scan."""
        if dir_name is None:
            dir_name = await self._workspace_folder(workspace_id)
        if root is None:
            binding = await self.resolve_binding(workspace_id)
            if computer_id is not None and binding.computer_id != computer_id:
                raise WorkspaceLayoutUnavailable(
                    f"Workspace {workspace_id} moved to another computer"
                )
            root = binding.root_dir or self.config.filesystem.working_directory
        return layout_from_binding(
            workspace_id,
            {"dir_name": dir_name, "computer_id": computer_id},
            root=root,
        )

    async def _root_owner_folder(
        self, computer_id: str, origin_workspace_id: Any
    ) -> Optional[str]:
        """Cache per machine: which project owns a machine's root never changes.

        A failed folder read is not cached, or one bad read would disarm the
        v3 to v4 target for the rest of the process."""
        machine = self._machine(computer_id)
        if machine.root_owner_read:
            return machine.root_owner_dir
        if not origin_workspace_id:
            machine.root_owner_dir = None
            machine.root_owner_read = True
            return None
        owner = await self._workspace_folder(str(origin_workspace_id))
        if owner:
            machine.root_owner_dir = owner
            machine.root_owner_read = True
        return owner

    async def _layout_root_owner_dir(self, binding: ComputerBinding) -> Optional[str]:
        """On a shared machine the v3 root belongs to the machine's first project.

        The v3 to v4 move sweeps every loose root entry into one folder, so on a
        machine consolidation folded onto an older sandbox the target has to be
        the project those files came from, not whichever sibling happens to
        start first. A tombstoned origin still wins: an orphaned folder is
        recoverable, a merge into a living sibling is not.
        """
        machine_id = binding.computer_id
        machine = self._machine(machine_id)
        if machine.root_owner_read:
            return machine.root_owner_dir or binding.dir_name
        try:
            computer = await get_computer(machine_id)
        except Exception as e:
            # Answering with the requester's folder would sweep the origin
            # project's root files into a sibling, and the recorded version
            # then keeps a retry from putting them back. Fail the sync instead.
            logger.warning(f"Root owner read failed for computer {machine_id}: {e}")
            raise
        if computer is None:
            return binding.dir_name
        owner = await self._root_owner_folder(
            machine_id, computer.get("origin_workspace_id")
        )
        return owner or binding.dir_name

    async def _stamp_layout_version(
        self,
        computer_id: Optional[str],
        result: Any,
    ) -> None:
        """A backfilled machine's row says 0, meaning unobserved, and only this
        stamp replaces it."""
        version = getattr(result, "layout_version", None)
        if version is None or computer_id is None:
            return
        try:
            await stamp_computer_layout_version(computer_id, int(version))
        except Exception as e:
            logger.warning(f"Layout version stamp failed for {computer_id}: {e}")

    async def _stamp_mcp_config_version(self, computer_id: Optional[str]) -> None:
        """Stamp after rebuilding shared _internal/tools/, so it reflects disk state.

        The DB layer determines the generation for machines with multiple workspaces."""
        if computer_id is None:
            return
        try:
            await stamp_computer_mcp_config_version(computer_id)
        except Exception as e:
            logger.warning(f"MCP config version stamp failed for {computer_id}: {e}")

    @staticmethod
    async def _seed_agent_md(
        sandbox: Any, name: str, dir_name: str | None = None
    ) -> None:
        """Keep the name in the row so renames cannot stale the template.

        Provisioning has no bound turn, so an explicit project folder is required:
        a bare agent.md would land at the computer root instead of its v4 folder."""
        if not sandbox:
            return

        path = f"{dir_name}/agent.md" if dir_name else "agent.md"
        content = _SEEDED_AGENT_MD
        try:
            # Duplicates and rebuilt machines may have restored agent.md; never overwrite it.
            if await sandbox.aread_file_text(path) is not None:
                return
            # awrite_file_text normalizes this relative path.
            written = await sandbox.awrite_file_text(path, content)
            if written:
                logger.info(f"Seeded agent.md for workspace '{name}'")
            else:
                logger.warning(f"Failed to seed agent.md for workspace '{name}'")
        except Exception as e:
            logger.warning(f"Failed to seed agent.md: {e}")

    async def _provision_sandbox_session(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        *,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        ws_version: int | None,
        kick_discovery: bool,
        post_init: "Callable[[Session], Any]",
        core_config: Any = None,
        expected_previous_sandbox_id: str | None = None,
    ) -> tuple[Session, Any]:
        """Publish only after post_init and the guarded identity write succeed.

        Earlier publication looks stale and can trigger retirement mid-restore,
        removing the cleanup handle and orphaning a billed sandbox. Discovery also
        requires publication for its liveness gate. ws_version=None forces MCP
        resolution; failures and cancellation must destroy the partial provision."""
        if core_config is None:
            core_config = self.config.to_core_config()
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id
        dir_name = binding.dir_name

        sandbox_tokens = await self._mint_sandbox_tokens(
            user_id or "", workspace_id, computer_id
        )
        session = self._session_handle(binding, core_config)
        try:
            await session.initialize(
                sandbox_tokens=sandbox_tokens,
                user_id=user_id,
                workspace_id=workspace_id,
                tier=tier,
                auto_stop_minutes=auto_stop_minutes,
                dir_name=dir_name,
            )

            # Install before asset sync so codegen includes user-server wrappers.
            resolved_mcp = await self._apply_session_mcp(
                binding, user_id, session, ws_version=ws_version
            )
            self._freeze_tool_view(binding.computer_id, workspace_id, session)

            await self._sync_sandbox_assets(
                binding,
                user_id,
                session.sandbox,
                reusing_sandbox=False,
            )

            await post_init(session)

            # Reconcile after post_init restores the skill directories and ledger.
            await self._reconcile_skills(
                binding.workspace_id, user_id, session.sandbox, source="provision"
            )

            sandbox_id = (
                getattr(session.sandbox, "sandbox_id", None)
                if session.sandbox
                else None
            )
            if not sandbox_id or not session.sandbox or not session.sandbox.runtime:
                raise RuntimeError("Fresh sandbox is missing its runtime identity")

            from src.server.services.platform_secret_rollout import (
                certify_platform_secrets,
            )

            # Certify before binding so no workspace can name an uncertified sandbox.
            secret_version = await certify_platform_secrets(
                core_config, runtime=session.sandbox.runtime
            )
            workspace = await self._bind_machine_identity(
                binding,
                sandbox_id=sandbox_id,
                expected_previous_sandbox_id=expected_previous_sandbox_id,
                platform_secret_version=secret_version,
            )
            if workspace is None:
                # The losing sandbox is unreferenced and billed: destroy it and retry attach.
                # Never retry the bind write, which could elect two winners and leak one.
                logger.warning(
                    f"Lost the sandbox-identity race for {workspace_id}; "
                    f"discarding our sandbox {sandbox_id}",
                    extra={"workspace_id": workspace_id, "sandbox_id": sandbox_id},
                )
                raise SandboxIdentityLostError(workspace_id, sandbox_id)

            # Only the bound sandbox can be published without appearing stale.
            self._put_session(computer_id, session, workspace_id=workspace_id)

            # post_init could not clear completeness before this sandbox was bound.
            # Now a marker permits clearing it; absent a marker, retry the restore.
            await self._maybe_restore_files(binding, session.sandbox)

            if kick_discovery and resolved_mcp is not None:
                view = self.tool_view(session, workspace_id)
                self._kick_mcp_discovery(
                    binding,
                    user_id,
                    session,
                    self._servers_needing_discovery(
                        session,
                        resolved_mcp,
                        workspace_id=workspace_id,
                    ),
                    view.mcp_config_version or 0,
                )

            self._record_sync(computer_id, workspace_id)
            return session, workspace
        except (Exception, asyncio.CancelledError):
            # Cancellation is a BaseException and must also destroy partial provisions;
            # a disconnect can otherwise leave a billed sandbox no row will ever name.
            await self._clear_session(computer_id, evict_session=session)
            raise

    async def _recover_sandbox(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        core_config: Any,
    ) -> Session:
        """Recheck tier and always-on entitlements when recreating a sandbox.

        Hosted Daytona cannot resize a snapshot sandbox, so entitled sizing and
        auto-stop must be applied at creation. Sizing comes off the machine: a
        project shadow that lags a resize would rebuild at the old size."""
        workspace_id = binding.workspace_id
        workspace = await db_get_workspace(workspace_id)
        # Warm-path recovery may lack user_id. Use the NOT NULL row owner or
        # provisioning resolves the owner's MCP/OAuth tier as empty.
        user_id = user_id or (workspace or {}).get("user_id")
        # The sandbox being replaced is gone or about to be, so the mirror is
        # what the new one restores from and what its tier has to fit.
        tier = await self._entitled_tier(binding, user_id, mirror_only=True)
        always_on = await self._entitled_always_on(binding, user_id)
        auto_stop_minutes = 0 if always_on else None

        # The machine's ref is the identity the bind CAS fences on.
        previous_sandbox_id = binding.provider_ref
        # Only this project is restored below; the siblings rejoin when next
        # opened. Until then their prune authority must not carry over from
        # the sandbox being replaced, or a sweep of a folder that exists but
        # was never restored prunes every row it did not find.
        try:
            await flag_sibling_restores_pending(
                binding.computer_id,
                except_workspace_id=workspace_id,
                expected_provider_ref=previous_sandbox_id,
            )
        except Exception as e:
            raise RestoreGuardUnavailable(workspace_id) from e

        async def _post_init(session: Session) -> None:
            if session.sandbox:
                await self._restore_files(
                    binding,
                    session.sandbox,
                    expected_sandbox_id=previous_sandbox_id,
                )

        # A new session needs fresh MCP resolution and background schema discovery.
        session, _ = await self._provision_sandbox_session(
            binding,
            user_id,
            tier=tier,
            auto_stop_minutes=auto_stop_minutes,
            ws_version=None,
            kick_discovery=True,
            post_init=_post_init,
            core_config=core_config,
            expected_previous_sandbox_id=previous_sandbox_id,
        )
        # The new sandbox is bound: past here nothing may fail the recover, or
        # a spec change settles failed and reverts the tier over a machine
        # already built at the target size.
        try:
            await update_workspace_activity(workspace_id)
        except Exception as e:
            logger.warning(
                f"Recovered workspace {workspace_id} but could not stamp activity: {e}"
            )
        return session

    async def backup_project_files(
        self,
        workspace_id: str,
        *,
        computer_id: str | None = None,
        strict: bool = False,
        expected_sandbox_id: str | None = None,
        session: Session | None = None,
        layout: WorkspaceLayout | None = None,
    ) -> bool:
        """Always fence backups: sync_to_db overwrites the durable file copy.

        Returns whether the mirror is current for this project. A skip, a stale
        session, or an unsaved file is False, so a caller counting folders
        never reports a backup that did not run as complete.

        A superseded session would destroy the good copy and miss live files.
        strict=True must abort destructive callers on incomplete backup;
        expected_sandbox_id and layout's root avoid reads when the caller already
        holds them. The folder is always read again, under the folder hold.
        The machine is resolved here for a caller that holds only the project
        (the post-turn mirror), because the session and the fence are the
        machine's: requiring it of every caller made that one fail on its
        signature and stop mirroring turns at all."""
        binding = None
        if computer_id is None:
            binding = await self.resolve_binding(workspace_id)
            computer_id = binding.computer_id
        # Restart has not cached its session yet; callers holding it must pass it.
        session = session or self._cached_session(computer_id)
        if not session or not getattr(session, "sandbox", None):
            if strict:
                raise RuntimeError(
                    f"No attached session to back up workspace {workspace_id} from"
                )
            # Session-less workers are normal; log skipped backup because later teardown
            # may lose unsynced files even for non-strict callers.
            logger.warning(
                f"Skipping file backup for {workspace_id}: no attached session "
                "on this worker"
            )
            return False

        # A lazy start names its sandbox at once and attaches the runtime only
        # when the start lands, so the id check below passes on a handle nothing
        # can run on yet. The start never takes the machine lock, so a caller
        # holding it can wait, and a failed start surfaces as its own error.
        try:
            await session.sandbox.ensure_sandbox_ready()
        except Exception as e:
            message = f"Sandbox for workspace {workspace_id} is not ready to back up: {e}"
            if strict:
                raise BackupIncomplete(
                    f"{message}; aborting before sandbox teardown"
                ) from e
            logger.warning(message)
            return False

        if expected_sandbox_id is None:
            identity = await db_get_workspace_identity(workspace_id)
            expected_sandbox_id = (identity or {}).get("sandbox_id")

        local_sandbox_id = self._session_sandbox_id(session)
        if local_sandbox_id != expected_sandbox_id:
            message = (
                f"Refusing to back up workspace {workspace_id} from a stale "
                f"session (attached={local_sandbox_id}, "
                f"durable={expected_sandbox_id})"
            )
            if strict:
                raise BackupIncomplete(message)
            logger.warning(message)
            return False

        root = layout.root if layout is not None else (
            binding.root_dir if binding is not None else None
        )
        try:
            # A settle moves only a folder it can hold, so the folder read under
            # this hold is the one the scan walks. Moved mid-scan, it would read
            # as missing, which counts as mirrored. A move a settle could not
            # finish raises: until the next one, the content may be in any of
            # three folders.
            async with self._held_workspace_folder(workspace_id) as dir_name:
                result = await FilePersistenceService.sync_to_db(
                    workspace_id,
                    session.sandbox,
                    layout=await self._project_layout(
                        workspace_id, computer_id, dir_name=dir_name or "", root=root
                    ),
                )
        except WorkspaceFolderMoving as e:
            message = f"Folder of workspace {workspace_id} is moving; not backed up this pass"
            if strict:
                raise BackupIncomplete(f"{message}; aborting before sandbox teardown") from e
            logger.warning(message)
            return False
        except Exception as e:
            if strict:
                raise BackupIncomplete(
                    f"File backup failed for {workspace_id}; aborting before "
                    f"sandbox teardown: {e}"
                ) from e
            logger.warning(f"File backup failed for {workspace_id}: {e}")
            return False

        # ``sync_to_db`` is per-file best-effort: it reports failures instead
        # of raising, so an exception is not the only way a backup can be
        # incomplete. A strict caller is about to delete the sandbox, and every
        # unsaved file has its only copy in it, whatever the reason. A
        # ``too_large`` file is refused by every later sync as well, so the
        # workspace has to stay up holding it until an operator gives it a
        # transfer path that fits. The message names paths because a count
        # gives the user nothing to act on.
        if result.unsaved:
            message = (
                f"File backup for {workspace_id} left {len(result.unsaved)} "
                f"file(s) unsaved: {result.describe_unsaved()}"
            )
            if strict:
                raise BackupIncomplete(
                    f"{message}; aborting before sandbox teardown", result.unsaved
                )
            logger.warning(message)
            return False

        if result.root_missing:
            logger.info(
                f"Workspace {workspace_id} has no folder on this sandbox; "
                f"its mirror is already the record"
            )
            return True

        logger.debug(f"File backup completed for {workspace_id}: {result}")
        return True

    async def _detached_sandbox_teardown(
        self,
        sandbox_id: str,
        *,
        delete: bool,
        binding: ComputerBinding,
    ) -> None:
        """A session-less worker must stop the durable sandbox, not just its DB row."""
        action = "delete" if delete else "stop"
        computer_id = binding.computer_id
        try:
            async with self._detached_runtime(sandbox_id, binding=binding) as runtime:
                await (runtime.delete() if delete else runtime.stop())
            logger.info(
                f"Tore down detached sandbox {sandbox_id} of computer "
                f"{computer_id} ({action})",
                extra={
                    "computer_id": computer_id,
                    "sandbox_id": sandbox_id,
                    "action": action,
                },
            )
        except Exception as exc:
            if not self._is_sandbox_gone(exc, binding):
                raise
            logger.info(
                f"Detached sandbox {sandbox_id} of computer {computer_id} is "
                f"already gone; nothing to tear down",
                extra={"computer_id": computer_id, "sandbox_id": sandbox_id},
            )

    async def _restore_files(
        self,
        binding: ComputerBinding,
        sandbox: Any,
        *,
        expected_sandbox_id: Any,
        strict: bool = False,
    ) -> None:
        """Fence restore flags with the binding the subsequent identity CAS replaces."""
        workspace_id = binding.workspace_id
        try:
            result = await FilePersistenceService.restore_to_sandbox(
                workspace_id,
                sandbox,
                expected_sandbox_id=expected_sandbox_id,
                layout=await self._project_layout(
                    workspace_id,
                    binding.computer_id,
                    dir_name=binding.dir_name,
                    root=binding.root_dir,
                ),
            )
            errors = result.get("errors", 0) if isinstance(result, dict) else 0
            if errors:
                if strict:
                    raise RuntimeError(f"Workspace {workspace_id}: {errors} files failed to restore")
                # Per-file restore failures do not raise; log data loss such as downgraded
                # disk overflow instead of silently accepting an incomplete restore.
                logger.warning(
                    f"Restored {result['restored']} files to sandbox for "
                    f"{workspace_id}, but {errors} failed to restore "
                    f"(possible data loss after a disk downgrade)"
                )
            else:
                logger.info(
                    f"Restored {result['restored']} files to sandbox for {workspace_id}"
                )
        except RestoreIdentityLost:
            # The later identity CAS would lose too; discard this sandbox before filling it.
            logger.info(
                f"Skipping restore for {workspace_id}: another sandbox was "
                f"bound while this one was being provisioned"
            )
            raise
        except RestoreGuardUnavailable:
            # Without a completeness guard, binding the empty sandbox lets the next
            # backup prune the full manifest. Unwind provisioning and retry start instead.
            raise
        except Exception as e:
            if strict:
                raise
            logger.warning(f"File restore failed for {workspace_id}: {e}")

    async def _maybe_restore_files(
        self,
        binding: ComputerBinding,
        sandbox: Any,
        *,
        urgent: bool = True,
    ) -> bool:
        """Completeness-guard failures must propagate to prevent destructive backups.

        The folder is read again under the folder hold, as a backup's is: a
        restore into a folder a settle moved meanwhile recreates the old one and
        marks itself complete there, and a backup of the new one then prunes."""
        workspace_id = binding.workspace_id
        try:
            async with self._held_workspace_folder(workspace_id) as dir_name:
                await FilePersistenceService.maybe_restore(
                    workspace_id,
                    sandbox,
                    layout=await self._project_layout(
                        workspace_id,
                        binding.computer_id,
                        dir_name=dir_name or "",
                        root=binding.root_dir,
                    ),
                )
        except WorkspaceFolderMoving:
            # The next acquisition restores once it lands.
            return False
        except RestoreGuardUnavailable:
            raise
        except Exception as e:
            logger.warning(f"File restore check failed for {workspace_id}: {e}")
            return False
        self._queue_bring_up(
            binding.computer_id, workspace_id, sandbox, binding.root_dir, urgent=urgent
        )
        return True

    async def _ensure_workspace_dirs(
        self, workspace_id: str, sandbox: Any, dir_name: Optional[str]
    ) -> None:
        make_dirs = getattr(sandbox, "_ensure_workspace_dirs", None)
        if make_dirs is None:
            return
        try:
            await make_dirs(dir_name)
        except Exception as e:
            logger.warning(f"Folder setup failed for {workspace_id}: {e}")

    async def restore_sibling_folders(self, session: Any, workspace_id: str) -> None:
        """Restore the other workspaces on this one's computer that a recreate left empty.

        A recreate restores only the workspace that asked for it, and each
        sibling rejoins on its own next open. Home's agent reads and edits its
        siblings' folders before then, where an empty folder reads as no work
        and an edit is overwritten when that open restores it. Only a sibling
        still flagged is touched, only on the sandbox the caller holds, and a
        failure is that sibling's alone: it restores on its own next open.
        """
        sandbox = getattr(session, "sandbox", None)
        if sandbox is None:
            return
        try:
            binding = await self.resolve_binding(workspace_id)
            owed = await get_restore_owed_workspace_ids_for_computer(
                binding.computer_id
            )
        except Exception as e:
            logger.warning(f"Could not list sibling restores for {workspace_id}: {e}")
            return
        sandbox_id = self._session_sandbox_id(session)
        gate = asyncio.Semaphore(_SIBLING_RESTORES_AT_ONCE)

        async def restore(sibling_id: str) -> None:
            async with gate:
                try:
                    # Under the hold a settle respects, as the sibling's own
                    # attach is, so a folder moving now is left for that open.
                    async with workspace_folder_in_use(sibling_id):
                        dir_name = await db_get_workspace_dir_name(sibling_id)
                        if not dir_name or not is_top_level(dir_name):
                            return
                        await self._ensure_workspace_dirs(sibling_id, sandbox, dir_name)
                        await self._maybe_restore_files(
                            replace(binding, workspace_id=sibling_id, dir_name=dir_name),
                            sandbox,
                            urgent=False,
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not restore sibling {sibling_id} for {workspace_id}; "
                        f"it restores on its own next open: {e}"
                    )

        # Not recorded as attached: that would skip the sibling's own attach,
        # which also reconciles its skills and tool overlay.
        await asyncio.gather(
            *(
                restore(sibling_id)
                for sibling_id in owed
                if sibling_id != workspace_id
                and (sibling_id, sandbox_id) not in self._projects_attached
            )
        )

    async def _ensure_project_attached(
        self, binding: ComputerBinding, session: Any, *, user_id: str | None = None
    ) -> None:
        """Machine startup prepares only its starter project, not every sibling.

        Every project on a machine needs the same things regardless of which
        one provisioned it: a folder, its files, links to the shared skills,
        and its own tool overlay. Recheck the generated configuration on every
        acquire so a deleted or damaged file is repaired even on a warm
        computer."""
        sandbox = getattr(session, "sandbox", None)
        if sandbox is None:
            return
        workspace_id = binding.workspace_id
        key = (workspace_id, self._session_sandbox_id(session))
        recorded = False
        if key not in self._projects_attached:
            # Restore files before rebuilding the generated tool configuration.
            await self._ensure_workspace_dirs(workspace_id, sandbox, binding.dir_name)
            # Unrestored, the next acquisition retries before trusting this
            # workspace. Restored, the key goes in before the next await: the
            # bring-up just queued may drop the workspace and discard its key,
            # which a later add would undo.
            if await self._maybe_restore_files(binding, sandbox):
                if len(self._projects_attached) >= _PROJECTS_ATTACHED_CAP:
                    self._projects_attached.clear()
                self._projects_attached.add(key)
                recorded = True
        # Before the overlay can end the attach, which does not end the turn:
        # a restart the reconnect found is recorded and a serving mount handed.
        await self._load_livefs(binding.computer_id, user_id, sandbox)
        if not await self._ensure_project_tool_overlay(
            binding, session, user_id=user_id
        ):
            # Retry on the next acquire; MCP calls refuse an absent config.
            if recorded:
                self._projects_attached.discard(key)
            return
        if recorded and not await self._reconcile_skills(
            workspace_id, user_id, sandbox, source="attach_project"
        ):
            # A folder reaches the shared skills only through links, and a
            # machine start or a sibling's lifecycle links no folder but its
            # own. After the overlay, whose asset refresh can deliver skills
            # the pass has to link. A full pass, once per sandbox per process:
            # one that did not finish leaves the attachment unrecorded, so the
            # next acquisition retries it before trusting this project.
            self._projects_attached.discard(key)
        if not self._livefs_owed(binding.computer_id, workspace_id, sandbox):
            return
        if not user_id:
            workspace = await db_get_workspace(workspace_id)
            user_id = (workspace or {}).get("user_id")
        await self._keep_livefs(binding.computer_id, user_id, sandbox, workspace_id)

    async def _ensure_project_tool_overlay(
        self,
        binding: ComputerBinding,
        session: Any,
        *,
        user_id: str | None = None,
    ) -> bool:
        """Repair this project's config from its own resolved connector set."""
        sandbox = session.sandbox
        workspace_id = binding.workspace_id
        dir_name = binding.dir_name
        try:
            if not await sandbox.workspace_overlay_missing(
                workspace_id=workspace_id, dir_name=dir_name
            ):
                return True
        except Exception as e:
            logger.warning(f"Tool overlay check failed for {workspace_id}: {e}")
            return False

        if not user_id:
            workspace = await db_get_workspace(workspace_id)
            user_id = (workspace or {}).get("user_id")
        try:
            # ws_version=None forces the resolve: the version compare cannot
            # answer for a workspace whose composite was never installed.
            await self._apply_session_mcp(binding, user_id, session, ws_version=None)
            view = self._workspace_tool_view(
                binding.computer_id, workspace_id, session
            ) or self._freeze_tool_view(binding.computer_id, workspace_id, session)
            if view is None:
                # The resolve failed and the session still carries a sibling's
                # set. Syncing now would write that set into this project's
                # overlay, which is worse than leaving it for the next acquire.
                logger.warning(
                    f"Skipping tool overlay for {workspace_id}: "
                    f"its MCP config did not resolve"
                )
                return False
            result = await self._sync_sandbox_assets(
                binding,
                user_id,
                sandbox,
                reusing_sandbox=True,
                force_refresh=True,
            )
            if result is None or await sandbox.workspace_overlay_missing(
                workspace_id=workspace_id, dir_name=dir_name
            ):
                return False
            logger.info(
                f"Built the tool overlay for joining project {workspace_id} "
                f"(folder {dir_name})"
            )
            return True
        except LayoutMigrationError:
            raise
        except Exception as e:
            logger.warning(f"Tool overlay build failed for {workspace_id}: {e}")
            return False

    @staticmethod
    def _compute_sandbox_config_hash(
        config: AgentConfig, binding: Optional[ComputerBinding] = None
    ) -> str:
        """New hash fields invalidate existing sandboxes and trigger migration.

        Machine settings override deployment settings. Preserve the pre-computer
        hash for rows matching the deployment with no overrides, avoiding fleet-wide
        recreation."""
        data = {
            "provider": (binding.kind if binding else None) or config.sandbox.provider,
            "working_dir": (
                (binding.root_dir if binding else None)
                or config.filesystem.working_directory
            ),
        }
        if binding is not None and binding.provider_config:
            data["provider_config"] = binding.provider_config
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:8]

    def _sandbox_config_stamp(
        self, binding: Optional[ComputerBinding] = None
    ) -> Dict[str, Any]:
        """Persist actual settings beside the hash for diagnosis."""
        return {
            "sandbox_config_hash": self._compute_sandbox_config_hash(
                self.config, binding
            ),
            "sandbox_provider": (
                (binding.kind if binding else None) or self.config.sandbox.provider
            ),
            "sandbox_working_dir": (
                (binding.root_dir if binding else None)
                or self.config.filesystem.working_directory
            ),
        }

    @staticmethod
    async def _update_workspace_config_fields(
        workspace_id: str, fields: Dict[str, Any], *, raise_on_error: bool = False
    ) -> None:
        """Critical stamps must request raise_on_error so callers can retry."""
        from psycopg.types.json import Json

        from src.server.database.pool import get_db_connection

        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE workspaces
                        SET config = COALESCE(config, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE workspace_id = %s
                        """,
                        (Json(fields), workspace_id),
                    )
        except Exception as e:
            logger.warning(f"Failed to update config for workspace {workspace_id}: {e}")
            if raise_on_error:
                raise

    async def _maybe_migrate_sandbox(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        session: Session,
        workspace: Dict[str, Any],
        *,
        expected_hash: str | None = None,
        transition_already_owned: bool = False,
    ) -> Session | None:
        """Restart callers already own starting and must not reclaim from running.

        transition_already_owned bypasses that impossible replacement claim.
        Reuse the resolved binding and expected_hash to keep migration tied to the
        machine already read by the caller."""
        workspace_id = binding.workspace_id
        if expected_hash is None:
            expected_hash = self._compute_sandbox_config_hash(self.config, binding)

        ws_config = workspace.get("config") or {}
        stored_hash = ws_config.get("sandbox_config_hash")
        if stored_hash == expected_hash:
            return None

        # Reconnect populates working_dir via fetch_working_dir.
        if not session.sandbox:
            return None
        actual_wd = session.sandbox.working_dir
        expected_wd = binding.root_dir or self.config.filesystem.working_directory
        if actual_wd == expected_wd:
            # Another recreation may already have corrected the directory.
            await self._update_workspace_config_fields(
                workspace_id, self._sandbox_config_stamp(binding)
            )
            return None

        if await self._machine_has_active_tasks(
            binding.computer_id, workspace_id=workspace_id
        ):
            logger.info(
                f"Deferring sandbox migration for computer {binding.computer_id}: "
                "a bound workspace still has active work"
            )
            return None

        logger.info(
            f"Migrating workspace {workspace_id} sandbox: {actual_wd} -> {expected_wd}"
        )

        old_sandbox_id = self._session_sandbox_id(session) or workspace.get(
            "sandbox_id"
        )

        # Teardown requires a complete backup. Opportunistic migration can abort
        # and keep the old directory without failing the request.
        try:
            await self._backup_machine_files_to_db(
                binding.computer_id,
                workspace_id=workspace_id,
                strict=True,
                expected_sandbox_id=old_sandbox_id,
                session=session,
            )
        except Exception:
            logger.error(
                f"Migration aborted for {workspace_id}: pre-migration backup "
                f"incomplete",
                exc_info=True,
            )
            return None

        # Claim before deletion so another worker cannot attach to running/<old id>
        # and provision a duplicate. A lost claim leaves the current session alone.
        if not old_sandbox_id:
            logger.warning(
                f"Skipping migration for {workspace_id}: no sandbox to replace"
            )
            return None
        # The machine owns the transition, so the claim is its CAS: claiming on
        # the project would let a lagging shadow replace a machine that has
        # meanwhile stopped.
        if not transition_already_owned and not await try_claim_computer_for_start(
            binding.computer_id,
            from_status="running",
            expected_provider_ref=old_sandbox_id,
            require_provider_ref=True,
        ):
            logger.warning(
                f"Skipping migration for {workspace_id}: could not claim the "
                f"replacement (sandbox {old_sandbox_id})"
            )
            return None

        # A sibling can enter while the strict backup is running. Once this
        # claim lands, new admissions are blocked; recheck before deleting the
        # shared runtime and release our running->starting claim if work won the
        # race.
        if await self._machine_has_active_tasks(
            binding.computer_id, workspace_id=workspace_id
        ):
            logger.info(
                f"Deferring sandbox migration for computer {binding.computer_id}: "
                "a bound workspace became active during backup"
            )
            if not transition_already_owned:
                await update_computer_status(
                    binding.computer_id,
                    ComputerStatus.RUNNING,
                    expected=ComputerStatus.STARTING,
                )
            return None

        # After claiming, every failure must release starting; new starts can claim
        # only stopped, so an uncompensated exit strands the workspace.
        try:
            self._drop_session(binding.computer_id)
            try:
                await SessionManager.cleanup_session(binding.computer_id)
            except Exception as e:
                # Cleanup may fail before removing its cache entry; force eviction so
                # recovery cannot reuse the stale session.
                SessionManager.remove_session(binding.computer_id)
                if isinstance(e, SandboxGoneError) or self._is_sandbox_gone(e, binding):
                    logger.warning(
                        f"Old sandbox {old_sandbox_id} for {workspace_id} is "
                        f"already gone ({e}); continuing migration"
                    )
                else:
                    raise

            core_config = self._core_config_for(binding)
            new_session = await self._recover_sandbox(binding, user_id, core_config)
        except (Exception, asyncio.CancelledError):
            # stopped permits recovery from the backup; error would block starts.
            # Catch CancelledError explicitly: it is a BaseException and would otherwise
            # strand starting on disconnect or shutdown.
            await update_computer_status(
                binding.computer_id,
                ComputerStatus.STOPPED,
                expected=ComputerStatus.STARTING,
            )
            raise

        # Retry the stamp once: an unstamped workspace re-migrates on every reconnect,
        # wasting resources and risking data loss.
        stamp = self._sandbox_config_stamp(binding)
        for attempt in range(2):
            try:
                await self._update_workspace_config_fields(
                    workspace_id, stamp, raise_on_error=True
                )
                break
            except Exception:
                if attempt == 0:
                    logger.warning(f"Retrying config stamp for {workspace_id}")
                else:
                    logger.error(
                        f"Failed to stamp sandbox config for {workspace_id} "
                        f"after 2 attempts. Workspace may re-migrate on next reconnect.",
                        exc_info=True,
                    )

        logger.info(f"Migration complete for workspace {workspace_id}")
        return new_session
