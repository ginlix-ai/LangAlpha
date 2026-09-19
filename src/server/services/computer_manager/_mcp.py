"""Seam: what a live session carries besides files: MCP config, secrets, skills.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import logging
import os
import time
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Any

import httpx

from ptc_agent.core.mcp_sanitize import is_untrusted_server
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.session import Session

from src.server.services.mcp_tool_split import build_direct_entries
from src.server.services.egress.session_binding import (
    maybe_remint_egress_jwt,
    RelayBind,
    sync_egress_relay,
)

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

from src.server.database.computer import get_computer_for_workspace
from src.server.database.workspace import (
    get_workspace as db_get_workspace,
    get_workspace_identity as db_get_workspace_identity,
)
from src.server.services.computer_manager._types import (
    ComputerBinding,
    WorkspaceToolView,
)
from src.server.services.user_skills.reconcile import reconcile_workspace_skills


logger = logging.getLogger(__name__)


class McpSecretsMixin:
    async def _vault_payloads(
        self, workspace_id: str, user_id: str | None
    ) -> tuple[str | None, dict[str, dict[str, str]]]:
        from ptc_agent.core.sandbox.assets import ROOT_VAULT_CLAIM
        from src.server.database.user_vault_secrets import get_user_secrets_decrypted
        from src.server.database.vault_secrets import get_effective_secrets

        if user_id is None:
            workspace = await db_get_workspace(workspace_id)
            user_id = (workspace or {}).get("user_id")

        effective, user_tier = await asyncio.gather(
            get_effective_secrets(workspace_id, user_id),
            get_user_secrets_decrypted(user_id)
            if user_id
            else asyncio.sleep(0, result={}),
        )
        claim = ProjectContext(workspace_id, "").claim
        return user_id, {
            ROOT_VAULT_CLAIM: dict(user_tier),
            claim: dict(effective),
        }

    async def _apply_session_platform_secret(
        self,
        binding: ComputerBinding,
        session: "Session",
        *,
        ws_version: int | None,
    ) -> None:
        """Only hot-resync here; PlatformSecretSweeper owns destructive scrubbing.

        No busy guard or eviction is needed for this non-destructive work. Failures
        propagate, and slow-path acquisitions retry using the session generation."""
        from ptc_agent.core.sandbox.platform_secrets import (
            platform_secrets_active,
        )

        core_config = self.config.to_core_config()
        if not platform_secrets_active(core_config):
            return
        sandbox = session.sandbox
        runtime = getattr(sandbox, "runtime", None) if sandbox else None
        if runtime is None:
            return

        from src.server.services.platform_secret_rollout import (
            resync_computer_platform_secret,
        )

        applied = await resync_computer_platform_secret(
            core_config,
            runtime,
            computer_id=binding.computer_id,
            sandbox_id=getattr(sandbox, "sandbox_id", None),
            db_version=ws_version or 0,
            applied_generation=session.platform_secret_version,
        )
        if applied is not None:
            session.platform_secret_version = applied

    async def push_vault_secrets(
        self,
        workspace_id: str,
        sandbox: "PTCSandbox | None" = None,
        user_id: str | None = None,
    ) -> None:
        """Pass the sandbox during startup, before the session enters the cache.

        Two files on the computer: the root vault carries the owner's user tier,
        which every server shared across the machine's workspaces reads, and the
        workspace's own file carries its effective set (get_effective_secrets
        owns that precedence: workspace secrets shadow user secrets), which its
        workspace-local servers and the ``vault`` helper read. One file for all
        would let each workspace's push overwrite its siblings' credentials."""
        if sandbox is None:
            # Mutation fan-outs report no failure; a stale handle would silently update
            # the wrong sandbox. Decline and let the version bump drive convergence.
            identity = await db_get_workspace_identity(workspace_id)
            session = self.get_session_if_ready(
                workspace_id, expected_sandbox_id=(identity or {}).get("sandbox_id")
            )
            if session is None:
                return
            sandbox = session.sandbox

        from ptc_agent.core.sandbox.assets import publish_vault_secrets

        user_id, payloads = await self._vault_payloads(workspace_id, user_id)
        await publish_vault_secrets(sandbox, payloads)
        claim = ProjectContext(workspace_id, "").claim
        secrets = payloads[claim]
        user_tier = payloads[next(iter(payloads))]
        sandbox.vault_secrets = dict(secrets)
        logger.debug(
            f"[vault] Pushed {len(secrets)} workspace and {len(user_tier)} "
            "user-tier secret(s) to sandbox",
            extra={"workspace_id": workspace_id},
        )

    @staticmethod
    async def _mint_sandbox_tokens(
        user_id: str, workspace_id: str, computer_id: str | None = None
    ) -> dict:
        """Tokens belong to the machine because its sandbox serves all projects.

        Keep workspace_id while the platform still requires it. Mint failures
        degrade to FMP-only mode."""
        auth_url = os.getenv("AUTH_SERVICE_URL", "")
        service_token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
        ginlix_data_url = os.getenv("GINLIX_DATA_URL", "")

        if not ginlix_data_url or not auth_url or not service_token:
            return {}

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{auth_url}/api/auth/data-tokens",
                    json={
                        "user_id": user_id,
                        "workspace_id": workspace_id,
                        "computer_id": computer_id,
                    },
                    headers={"X-Service-Token": service_token},
                    timeout=10,
                )
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.warning(
                f"Failed to mint sandbox tokens, ginlix-data features disabled: {e}",
                extra={"workspace_id": workspace_id},
            )
            return {}

    # Cache resolution on the session so create_agent performs no per-turn DB
    # resolution. Check mcp_config_version on the post-cooldown read; discovery
    # must stay off the turn and outside the workspace lock.

    async def _apply_session_mcp(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        session: Session,
        *,
        ws_version: int | None,
    ) -> Any | None:
        """Keep discovery off this path; only resolve and build the composite here.

        ws_version piggybacks the workspace read. None means no composite change,
        so callers skip discovery."""
        sandbox = session.sandbox
        if sandbox is None:
            return None
        workspace_id = binding.workspace_id
        prior = self._workspace_tool_view(binding.computer_id, workspace_id, session)
        if prior is None:
            prior = self._freeze_tool_view(binding.computer_id, workspace_id, session)

        # An unchanged workspace entry adds no resolution reads. Config versions
        # are per workspace, so a sibling's matching number says nothing here.
        if (
            prior is not None
            and prior.mcp_config_version is not None
            and ws_version is not None
            and prior.mcp_config_version == ws_version
            and prior.mcp_tool_summary is not None
        ):
            # The relay JWT ages independently of the config version.
            relay_session = SimpleNamespace(
                sandbox=session.sandbox,
                config=session.config,
                egress_binding=prior.egress_binding,
            )
            await maybe_remint_egress_jwt(
                workspace_id, binding.computer_id, relay_session
            )
            if relay_session.egress_binding is not prior.egress_binding:
                self._store_tool_view(
                    binding.computer_id,
                    replace(prior, egress_binding=relay_session.egress_binding),
                )
            return None

        from src.server.services.mcp_config import resolve_mcp_config

        try:
            resolved = await resolve_mcp_config(
                self.config, user_id or "", workspace_id
            )
        except Exception as e:
            logger.warning(
                "[ASSET_SYNC] MCP resolve failed for %s: %s, keeping prior set",
                workspace_id,
                e,
            )
            return None

        # Bind before copying configs so codegen receives grant annotations. Relay
        # failures leave OAuth clients unbound without blocking non-OAuth tools.
        relay_session = SimpleNamespace(
            sandbox=session.sandbox,
            config=session.config,
            egress_binding=prior.egress_binding if prior is not None else None,
        )
        egress_bind = RelayBind.APPLIED
        try:
            egress_bind = await sync_egress_relay(
                workspace_id, binding.computer_id, user_id, relay_session, resolved
            )
        except Exception as e:
            egress_bind = RelayBind.REFUSED
            logger.warning("[EGRESS] relay binding failed for %s: %s", workspace_id, e)

        view = await self._install_session_composite(
            session,
            resolved,
            user_id=user_id,
            workspace_id=workspace_id,
            egress_binding=relay_session.egress_binding,
        )
        if egress_bind is RelayBind.SUPERSEDED:
            # Returning None skips asset sync. Keep the workspace's prior snapshot,
            # if any, and bypass cooldown so the next acquire re-resolves. Never let
            # this stale result replace either the prior workspace view or a sibling.
            logger.info(
                "[EGRESS] resolve for %s superseded by a newer config version; "
                "keeping the prior workspace view and withholding the version stamp",
                workspace_id,
            )
            if prior is not None:
                self._mirror_tool_view_on_session(session, prior)
            else:
                session.mcp_registry = session._builtin_mcp_registry
                session.mcp_tool_summary = None
                session.direct_mcp_tools = {}
                session.egress_binding = None
                session.mcp_config_version = None
                session.mcp_config_workspace_id = None
                session.mcp_settled_servers = set()
                if session.sandbox is not None:
                    session.sandbox.mcp_registry = session._builtin_mcp_registry
            self._machine(binding.computer_id).resolve_superseded.add(workspace_id)
            return None
        if egress_bind is not RelayBind.APPLIED:
            # Withhold the stamp so failed credential pushes retry on acquire; JWT
            # remint only re-sends the stale in-memory map. Keep non-OAuth tools usable.
            view = replace(view, mcp_config_version=None)
        self._store_tool_view(binding.computer_id, view)
        self._mirror_tool_view_on_session(session, view)
        return resolved

    async def _install_session_composite(
        self,
        session: Session,
        resolved: Any,
        *,
        workspace_id: str,
        user_id: str | None = None,
        egress_binding: Any = None,
    ) -> WorkspaceToolView:
        """Codegen and per-turn prompts must share the same effective registry.

        CoreConfig is a per-workspace deep copy, safe to replace with the effective
        server set. Without user servers, preserve the built-in registry identity.
        workspace_id names the project this composite is for: the session is cached
        per machine, so its own label is whichever sibling built it."""
        from ptc_agent.core.mcp_registry import build_composite_registry
        from ptc_agent.agent.prompts.formatter import (
            build_tool_summary_from_registry,
        )

        # ToolSnapshotIndex requires current fingerprints and USER-tier snapshots
        # for inherited servers; workspace OAuth snapshots can outlive disconnects.
        untrusted_servers = [s for s in resolved.servers if is_untrusted_server(s)]
        tool_schemas: dict[str, list[dict]] = {}
        settled: set[str] = set()
        if untrusted_servers:
            from src.server.database.mcp_tool_schemas import (
                get_tool_schemas,
                get_user_tool_schemas,
            )
            from src.server.services.mcp_discovery import ToolSnapshotIndex

            user_rows: list[dict] = []
            if user_id and any(s.source == "user" for s in untrusted_servers):
                user_rows = await get_user_tool_schemas(user_id)
            snapshots = ToolSnapshotIndex(
                workspace_rows=await get_tool_schemas(workspace_id),
                user_rows=user_rows,
            )
            # Filter consent here: shared discovery snapshots must describe all vendor
            # capabilities, with consent-independent schema_digest values. Remove only
            # declined groups so newly added vendor tools remain available.
            tool_schemas, direct_mcp_tools = build_direct_entries(
                untrusted_servers,
                snapshots,
                denied=resolved.denied_tools_by_name,
                plans=resolved.binding_plans_by_name,
            )
            # Only a usable snapshot lets a server answer for itself.
            settled = set(tool_schemas)
        else:
            direct_mcp_tools = {}

        # Rebuild from built-ins, never from an earlier composite.
        builtin_registry = session._builtin_mcp_registry or session.mcp_registry
        composite = build_composite_registry(
            builtin_registry,
            untrusted_servers,
            tool_schemas,
            resolved.disabled_builtin_names,
        )

        try:
            tool_exposure = self.config.mcp.tool_exposure_mode
        except Exception:
            tool_exposure = "summary"
        tool_summary = build_tool_summary_from_registry(composite, mode=tool_exposure)
        view = WorkspaceToolView(
            workspace_id=workspace_id,
            session=session,
            mcp_registry=composite,
            mcp_tool_summary=tool_summary,
            direct_mcp_tools=MappingProxyType(dict(direct_mcp_tools)),
            egress_binding=egress_binding,
            mcp_config_version=resolved.version,
            mcp_servers=tuple(resolved.servers),
            mcp_settled_servers=frozenset(settled),
        )
        self._mirror_tool_view_on_session(session, view)
        return view

    @staticmethod
    def _mirror_tool_view_on_session(session: Session, view: WorkspaceToolView) -> None:
        """Keep legacy readers usable while turns consume the frozen view."""
        session.mcp_registry = view.mcp_registry
        session.mcp_tool_summary = view.mcp_tool_summary
        session.direct_mcp_tools = dict(view.direct_mcp_tools)
        session.egress_binding = view.egress_binding
        session.mcp_config_version = view.mcp_config_version
        session.mcp_config_workspace_id = view.workspace_id
        session.mcp_settled_servers = set(view.mcp_settled_servers)
        if session.sandbox is not None:
            session.sandbox.mcp_registry = view.mcp_registry

    def _servers_needing_discovery(
        self,
        session: Session,
        resolved: Any,
        *,
        workspace_id: str | None = None,
    ) -> list[Any]:
        """Use fingerprint-valid snapshots, not tool counts, to decide discovery.

        Zero-tool or fully sanitized servers are settled. Pending, failed or
        config-invalidated snapshots must be probed again."""
        from src.server.services.mcp_config import State

        view = (
            self._workspace_tool_view(
                str(session.computer_id or ""), workspace_id, session
            )
            if workspace_id
            else None
        )
        settled = (
            view.mcp_settled_servers
            if view is not None
            else session.mcp_settled_servers
        )
        return [
            e.config
            for e in resolved.entries
            if e.state is State.ACTIVE
            and is_untrusted_server(e.config)
            # OAuth discovery is host-side; the sandbox has no vendor token.
            and not e.host_side_oauth
            and e.name not in settled
        ]

    def _kick_mcp_discovery(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        session: Session,
        servers: list[Any],
        version: int,
    ) -> None:
        """Keep the up-to-30s stdio cold start off the turn and workspace lock.

        Mid-turn composite swaps are safe: create_agent reads at turn start, so new
        tools appear at most one turn later."""
        if not servers:
            return
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id

        def _session_live() -> bool:
            # Do not probe torn-down sandboxes or write schemas for retired sessions.
            if self._cached_session(computer_id) is not session:
                return False
            sandbox = session.sandbox
            if sandbox is None:
                return False
            is_ready = getattr(sandbox, "is_ready", None)
            return is_ready() if callable(is_ready) else True

        async def _run() -> None:
            try:
                if not _session_live():
                    return
                from src.server.services.mcp_discovery import discover_and_cache

                _t_disc = time.time()
                await discover_and_cache(workspace_id, session.sandbox, servers)
                logger.info(
                    "[ASSET_SYNC] workspace_id=%s mcp_discovery=%.0fms servers=%d",
                    workspace_id,
                    (time.time() - _t_disc) * 1000,
                    len(servers),
                )
                # Publish discoveries immediately, but never overwrite a newer
                # config or session. The version is a per-workspace counter, so
                # the workspace has to match as well: two siblings both at
                # version 1 must not install over each other.
                current = self._workspace_tool_view(computer_id, workspace_id, session)
                if (
                    current is not None
                    and current.mcp_config_version == version
                    and _session_live()
                ):
                    from src.server.services.mcp_config import (
                        resolve_mcp_config,
                    )

                    resolved = await resolve_mcp_config(
                        self.config, user_id or "", workspace_id
                    )
                    if resolved.version == version and _session_live():
                        updated = await self._install_session_composite(
                            session,
                            resolved,
                            user_id=user_id,
                            workspace_id=workspace_id,
                            egress_binding=current.egress_binding,
                        )
                        self._store_tool_view(computer_id, updated)
                        await self._sync_sandbox_assets(
                            binding,
                            user_id,
                            session.sandbox,
                            reusing_sandbox=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "[ASSET_SYNC] background MCP discovery failed for %s: %s",
                    workspace_id,
                    e,
                )

        task = asyncio.create_task(_run())
        self._mcp_discovery_tasks.add(task)
        self._machine(computer_id).discovery_tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._mcp_discovery_tasks.discard(t)
            machine = self._machine_if_known(computer_id)
            if machine is not None:
                machine.discovery_tasks.discard(t)

        task.add_done_callback(_on_done)

    def _cancel_mcp_discovery(self, computer_id: str) -> None:
        """Cancel before teardown to prevent dead-sandbox probes and orphan schemas."""
        machine = self._machine_if_known(computer_id)
        if machine is None:
            return
        for task in list(machine.discovery_tasks):
            task.cancel()
        # The flat registry keeps each task alive until its done callback runs,
        # so a cancelled probe is never collected mid-cancellation.
        machine.discovery_tasks.clear()

    def get_applied_mcp_config_version(
        self, workspace_id: str, *, expected_sandbox_id: str | None
    ) -> int | None:
        """Only report versions from a ready, identity-checked session.

        The UI treats this as applied state; a superseded session cannot vouch for
        what the live sandbox has loaded."""
        session = self.get_session_if_ready(
            workspace_id, expected_sandbox_id=expected_sandbox_id
        )
        if session is None:
            return None
        computer_id = self._live_session_computer(workspace_id) or str(
            session.computer_id or ""
        )
        view = self._workspace_tool_view(computer_id, workspace_id, session)
        if view is None:
            view = self._freeze_tool_view(computer_id, workspace_id, session)
        return view.mcp_config_version if view is not None else None

    async def _reconcile_skills(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        sandbox: Any,
        *,
        source: str,
    ) -> None:
        """Reconcile after asset sync and restore so skills see the final disk state.

        The service uses the cross-worker SKILL_SYNC advisory lock and never raises;
        anonymous sessions have no skill rows."""
        if not user_id or sandbox is None:
            return
        await reconcile_workspace_skills(
            sandbox,
            user_id=user_id,
            workspace_id=binding.workspace_id,
            source=source,
        )

    # Strong refs prevent task GC; never consult this set as state.
    _skill_reconcile_tasks: set[asyncio.Task] = set()

    @classmethod
    def schedule_skill_reconcile(
        cls, workspace_id: str, user_id: str, *, source: str
    ) -> None:
        """Best-effort delivery falls back to cold acquire or post-turn reconciliation.

        That fallback covers uninitialized managers, stopped sandboxes and sessions
        held by another worker."""
        try:
            manager = cls.get_instance()
        except Exception:
            return

        async def _run() -> None:
            try:
                await manager.reconcile_skills_if_running(
                    workspace_id, user_id, source=source
                )
            except Exception as e:
                logger.warning(
                    f"[skill_sync] proactive reconcile failed for {workspace_id}: {e}"
                )

        task = asyncio.create_task(_run())
        cls._skill_reconcile_tasks.add(task)
        task.add_done_callback(cls._skill_reconcile_tasks.discard)

    async def reconcile_skills_if_running(
        self, workspace_id: str, user_id: str, *, source: str
    ) -> None:
        """Never wake a sandbox for a skill mutation.

        Stopped or other-worker sessions converge at cold acquire or post-turn sync;
        only this worker's ready, identity-checked session can be updated here."""
        ws = await db_get_workspace(workspace_id)
        if not ws or not ws.get("sandbox_id"):
            return
        session = self.get_session_if_ready(
            workspace_id, expected_sandbox_id=str(ws["sandbox_id"])
        )
        if session is None:
            return
        await reconcile_workspace_skills(
            session.sandbox,
            user_id=user_id,
            workspace_id=workspace_id,
            source=source,
        )

    async def proactively_apply_mcp_config(
        self, workspace_id: str, user_id: str | None = None
    ) -> None:
        """Apply mutations before the next turn, even when the sandbox needs waking.

        A DB version bump alone waits for acquisition, and a warm session can skip
        it for 30s unless cooldown is cleared. Failures fall back to next-message
        application."""
        computer = await get_computer_for_workspace(workspace_id)
        if computer is not None:
            self._invalidate_workspace_tool_view(
                str(computer["computer_id"]), workspace_id
            )
        try:
            await self._acquire_session(workspace_id, user_id=user_id)
        except Exception as e:
            logger.warning(
                "[ASSET_SYNC] proactive MCP apply failed for %s: %s, "
                "falling back to next-message apply",
                workspace_id,
                e,
            )

    async def refresh_session_mcp(
        self, workspace_id: str, user_id: str | None = None
    ) -> None:
        """Manual /discover changes schema snapshots without bumping mcp_config_version.

        Invalidate the session stamp so application reloads snapshots and wrappers."""
        # Lock-free invalidation costs at most a redundant resolve: proactive apply
        # re-enters the lock, so none is missed. Locking here would contend with chat.
        computer_id = self._live_session_computer(workspace_id)
        session = self._cached_session(computer_id) if computer_id else None
        if session is not None:
            self._invalidate_workspace_tool_view(
                computer_id or "", workspace_id, clear_version=True
            )
        await self.proactively_apply_mcp_config(workspace_id, user_id)
