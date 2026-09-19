"""Seam: the computer-keyed session cache, its locks and staleness checks.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import logging
import time
from dataclasses import replace
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from ptc_agent.core.session import Session, SessionManager

from src.observability import (
    safe_add,
    safe_record,
    session_acquire_phase_duration_ms,
    session_path_counter,
)
from src.observability.tracing import hash_id as _obs_hash_id
from src.observability.tracing import safe_aspan

from src.server.database.computer import get_computer_for_workspace
from src.server.database.workspace import (
    get_workspace as db_get_workspace,
    get_workspace_identity as db_get_workspace_identity,
)

from src.server.services.computer_manager._types import (
    ComputerBinding,
    MachineState,
    SessionMetadata,
    WorkspaceNotOnComputer,
    WorkspaceToolView,
    _WORKSPACE_TOOL_VIEWS_CAP,
)

logger = logging.getLogger(__name__)


class SessionCacheMixin:
    _SYNC_COOLDOWN_SECONDS = 30

    @staticmethod
    def _binding_from_computer(
        workspace_id: str, computer: Dict[str, Any]
    ) -> ComputerBinding:
        return ComputerBinding(
            workspace_id=workspace_id,
            computer_id=str(computer["computer_id"]),
            dir_name=computer.get("dir_name"),
            kind=computer.get("kind"),
            root_dir=computer.get("root_dir"),
            provider_ref=(
                str(computer["provider_ref"]) if computer.get("provider_ref") else None
            ),
            resource_tier=computer.get("resource_tier"),
            is_always_on=bool(computer.get("is_always_on")),
            provider_config=computer.get("provider_config") or None,
        )

    async def resolve_binding(
        self, workspace_id: str, *, workspace: Optional[Dict[str, Any]] = None
    ) -> ComputerBinding:
        """Resolve the machine once, here, so nothing below has two addressing modes.

        Migration 046 leaves a row unbound when it had no sandbox or shared one
        with a sibling, so a miss is adopted onto a machine rather than handed
        down as a second lifecycle to maintain.
        """
        computer = await get_computer_for_workspace(workspace_id)
        if computer is None:
            computer = await self._adopt_workspace_onto_computer(
                workspace_id, workspace=workspace
            )
        if computer is None:
            raise WorkspaceNotOnComputer(workspace_id)
        return self._binding_from_computer(workspace_id, computer)

    async def binding_for_computer(self, computer_id: str) -> ComputerBinding:
        """A machine operation names no project, so the binding carries none."""
        from src.server.database.computer import get_computer

        computer = await get_computer(computer_id)
        if computer is None:
            raise ValueError(f"Computer {computer_id} not found")
        return self._binding_from_computer("", computer)

    async def workspace_row(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """The project row, for callers that need its columns and not the machine's."""
        return await db_get_workspace(workspace_id)

    # ---- the per-machine record ------------------------------------------

    def _machine(self, computer_id: str) -> MachineState:
        """This worker's record for one machine, created on first touch.

        setdefault with no await between the miss and the insert is what keeps
        two coroutines from taking different locks for the same machine."""
        return self._machines.setdefault(computer_id, MachineState())

    def _machine_if_known(self, computer_id: str) -> Optional[MachineState]:
        """For readers that must not mint a record for a machine they only ask about."""
        return self._machines.get(computer_id)

    def _pending_lazy_start(self, computer_id: str) -> bool:
        """Whether this worker owns an unpromoted lazy start of the machine.

        The owner holds the row's transition, promoting on success and reverting
        on failure, so no other path may retire its session or reap its row."""
        machine = self._machines.get(computer_id)
        return machine is not None and machine.pending_lazy_sync

    def _forget_machine(self, computer_id: str) -> None:
        """Drop the record, lock included, once the machine itself is gone.

        Only safe where no caller can still be holding the lock: a live holder
        would let the next caller build a second lock and run alongside it."""
        self._machines.pop(computer_id, None)

    # ---- the cache -------------------------------------------------------

    def _cached_session(self, computer_id: str) -> Optional[Session]:
        machine = self._machines.get(computer_id)
        return machine.session if machine is not None else None

    def _put_session(
        self, computer_id: str, session: Session, *, workspace_id: str | None = None
    ) -> None:
        machine = self._machine(computer_id)
        machine.session = session
        if machine.meta is None:
            machine.meta = SessionMetadata(
                workspace_id=workspace_id, computer_id=computer_id
            )
        machine.meta.sandbox_id = self._session_sandbox_id(session)
        machine.meta.touch()
        if workspace_id:
            self._session_computer[workspace_id] = computer_id

    def _freeze_tool_view(
        self, computer_id: str, workspace_id: str, session: Session
    ) -> Optional[WorkspaceToolView]:
        """Return this workspace's installed snapshot, with a legacy fallback."""
        machine = self._machine(computer_id)
        existing = machine.tool_views.get(workspace_id)
        if existing is not None and existing.session is session:
            machine.tool_views.move_to_end(workspace_id)
            return existing
        if session.mcp_config_workspace_id != workspace_id:
            return None
        view = WorkspaceToolView.from_session(workspace_id, session)
        self._store_tool_view(computer_id, view)
        return view

    def _store_tool_view(
        self, computer_id: str, view: WorkspaceToolView
    ) -> WorkspaceToolView:
        machine = self._machine(computer_id)
        machine.tool_views[view.workspace_id] = view
        machine.tool_views.move_to_end(view.workspace_id)
        while len(machine.tool_views) > _WORKSPACE_TOOL_VIEWS_CAP:
            machine.tool_views.popitem(last=False)
        return view

    def _workspace_tool_view(
        self, computer_id: str, workspace_id: str, session: Session
    ) -> WorkspaceToolView | None:
        machine = self._machine_if_known(computer_id)
        view = machine.tool_views.get(workspace_id) if machine is not None else None
        if view is None or view.session is not session:
            return None
        machine.tool_views.move_to_end(workspace_id)
        return view

    def tool_view(self, session: Session, workspace_id: str) -> WorkspaceToolView:
        """The configuration a turn on ``workspace_id`` builds from."""
        computer_id = str(session.computer_id or "")
        view = self._workspace_tool_view(computer_id, workspace_id, session)
        if view is not None:
            return view
        view = self._freeze_tool_view(computer_id, workspace_id, session)
        if view is not None:
            return view
        logger.warning(
            f"Workspace {workspace_id} has no tool configuration of its own on "
            f"computer {computer_id}; the turn runs with the built-in servers only"
        )
        return WorkspaceToolView.builtin_only(workspace_id, session)

    def _drop_session(self, computer_id: str) -> None:
        machine = self._machines.get(computer_id)
        if machine is not None:
            machine.forget_session()
        for workspace_id, cid in list(self._session_computer.items()):
            if cid == computer_id:
                del self._session_computer[workspace_id]

    def _touch_session_meta(self, computer_id: str) -> None:
        """Warm hits install no session, so activity must be counted separately."""
        machine = self._machines.get(computer_id)
        if machine is not None and machine.meta is not None:
            machine.meta.touch()

    def live_session_stats(self) -> List[Dict[str, Any]]:
        return [
            {
                "workspace_id": machine.meta.workspace_id,
                "computer_id": machine.meta.computer_id,
                "created_at": machine.meta.created_at.isoformat(),
                "last_active": machine.meta.last_active.isoformat(),
                "request_count": machine.meta.request_count,
                "sandbox_id": machine.meta.sandbox_id,
            }
            for machine in self._machines.values()
            if machine.meta is not None
        ]

    def _session_handle(self, binding: ComputerBinding, core_config: Any) -> Session:
        """Sibling projects share one sandbox handle, keyed by the machine.

        The label is for logs only, so it names the project that built the
        handle; the machine is the identity."""
        return SessionManager.get_session(
            binding.computer_id,
            core_config,
            label=binding.workspace_id or binding.computer_id,
            computer_id=binding.computer_id,
            resource_tier=binding.resource_tier,
        )

    # ---- locks -----------------------------------------------------------

    def _machine_lock(self, computer_id: str) -> asyncio.Lock:
        """One lock per machine, so unrelated machines never block each other."""
        return self._machine(computer_id).lock

    @asynccontextmanager
    async def _acquire_machine_lock(self, computer_id: str, timeout: float = 60.0):
        lock = self._machine_lock(computer_id)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Timeout acquiring lock for computer {computer_id} after {timeout}s"
            )
        try:
            yield
        finally:
            lock.release()

    @asynccontextmanager
    async def _observed_lock(self, computer_id: str, span_name: str, **extra_attrs):
        attrs = {"computer_id": _obs_hash_id(computer_id), **extra_attrs}
        async with safe_aspan(span_name, attrs):
            async with self._acquire_machine_lock(computer_id):
                yield

    def _sync_cooldown_ok(
        self, computer_id: str, workspace_id: str | None = None
    ) -> bool:
        machine = self._machines.get(computer_id)
        if machine is None:
            return False
        if workspace_id is None:
            last = machine.last_sync_at
        else:
            view = machine.tool_views.get(workspace_id)
            last = view.built_at if view is not None else None
        if last is None:
            return False
        return (time.monotonic() - last) < self._SYNC_COOLDOWN_SECONDS

    def _record_sync(self, computer_id: str, workspace_id: str | None = None) -> None:
        machine = self._machine(computer_id)
        now = time.monotonic()
        if workspace_id is None:
            machine.last_sync_at = now
            return
        view = machine.tool_views.get(workspace_id)
        if view is not None:
            self._store_tool_view(computer_id, replace(view, built_at=now))

    def _invalidate_workspace_tool_view(
        self, computer_id: str, workspace_id: str, *, clear_version: bool = False
    ) -> None:
        machine = self._machine_if_known(computer_id)
        if machine is None:
            return
        view = machine.tool_views.get(workspace_id)
        if view is None:
            return
        self._store_tool_view(
            computer_id,
            replace(
                view,
                built_at=0.0,
                mcp_config_version=None if clear_version else view.mcp_config_version,
            ),
        )

    # ---- eviction --------------------------------------------------------

    async def _clear_session(
        self,
        computer_id: str,
        *,
        evict_session: "Session | None" = None,
    ) -> None:
        """Guard eviction against replacements installed during cleanup_session.

        Cleanup awaits resource release, allowing another request to replace the
        cache slot. Pass evict_session outside the machine lock to preserve it;
        callers holding that lock may omit the guard. A preserved replacement
        keeps its own flags: they describe the session in the slot, not the one
        this call meant to evict."""
        # Cancel discovery before teardown to avoid dead-sandbox probes and orphan schemas.
        self._cancel_mcp_discovery(computer_id)
        try:
            await SessionManager.cleanup_session(computer_id)
        except Exception as e:
            logger.warning(
                "Error during session cleanup (continuing)",
                extra={"computer_id": computer_id, "error": str(e)},
            )
        if evict_session is None or self._cached_session(computer_id) is evict_session:
            self._drop_session(computer_id)

    async def _take_valid_cached_session(
        self, binding: ComputerBinding, mark: Callable[[str], None]
    ) -> "Session | None":
        """Validate cached handles against Postgres before any warm return.

        Spawn-isolated workers have no shared invalidation, so the narrow identity
        read is required. Retirement must preserve sandboxes another worker may own;
        readiness and sync policy remain with the caller."""
        session = self._cached_session(binding.computer_id)
        if session is None:
            return None

        workspace_id = binding.workspace_id
        logger.debug(
            f"Found cached session for {workspace_id}, "
            f"initialized={session._initialized}, "
            f"has_sandbox={session.sandbox is not None}"
        )
        if not session._initialized or not session.sandbox:
            return None

        _t0 = time.time()
        identity = await db_get_workspace_identity(workspace_id)
        identity_ms = (time.time() - _t0) * 1000
        stale_reason = self._identity_is_stale(binding, session, identity)
        mark("identity_check")
        # Warm returns skip the caller's phase-metric emission, so measure here.
        safe_record(
            session_acquire_phase_duration_ms,
            identity_ms,
            {"phase": "identity_check", "session_path": "warm"},
        )
        if stale_reason is None:
            self._touch_session_meta(binding.computer_id)
            self._session_computer[workspace_id] = binding.computer_id
            return session

        # The plain %-formatter drops extra fields; the reason must be in the message.
        logger.warning(
            f"Cached session for {workspace_id} is stale "
            f"({stale_reason}); retiring and re-attaching",
            extra={"workspace_id": workspace_id, "reason": stale_reason},
        )
        await self._retire_session(binding.computer_id, session, reason=stale_reason)
        safe_add(session_path_counter, 1, {"path": "stale_reattach"})
        return None

    async def retire_session_if_present(
        self, workspace_id: str, *, reason: str
    ) -> bool:
        """Invalidate only this worker: another worker may still own the sandbox.

        Resolves the machine rather than consulting a local index, so a project
        this worker has never served still reaches its machine's session."""
        computer = await get_computer_for_workspace(workspace_id)
        if computer is None:
            return False
        return await self.retire_computer_session_if_present(
            str(computer["computer_id"]), reason=reason
        )

    async def retire_computer_session_if_present(
        self, computer_id: str, *, reason: str
    ) -> bool:
        if self._cached_session(computer_id) is None:
            return False
        async with self._acquire_machine_lock(computer_id):
            session = self._cached_session(computer_id)
            if session is None:
                return False
            await self._retire_session(computer_id, session, reason=reason)
        return True

    async def _retire_session(
        self, computer_id: str, session: "Session", *, reason: str
    ) -> None:
        """Evict both caches without closing resources still held by active work.

        Leaving SessionManager populated makes reattach skip initialization and
        reuse the stale handle. PTCSandbox.close() closes the shared SDK HTTP client,
        so resources must survive until the last graph or subagent holder drops them."""
        self._cancel_mcp_discovery(computer_id)
        if self._cached_session(computer_id) is session:
            self._drop_session(computer_id)
        if SessionManager.get_cached_session(computer_id) is session:
            SessionManager.remove_session(computer_id)
        logger.info(
            f"Retired cached session for computer {computer_id} "
            f"(sandbox {self._session_sandbox_id(session)} left intact): {reason}",
            extra={
                "computer_id": computer_id,
                "reason": reason,
                "sandbox_id": self._session_sandbox_id(session),
            },
        )

    @staticmethod
    def _session_sandbox_id(session: "Session | None") -> str | None:
        sandbox = getattr(session, "sandbox", None) if session is not None else None
        sandbox_id = getattr(sandbox, "sandbox_id", None) if sandbox else None
        return str(sandbox_id) if sandbox_id else None

    # A flash workspace never reaches acquisition, so 'running' is the whole set.
    _SESSION_SERVING_STATUSES = frozenset({"running"})

    def _identity_is_stale(
        self,
        binding: ComputerBinding,
        session: "Session",
        identity: Dict[str, Any] | None,
    ) -> str | None:
        """Both status and identity must match the durable binding.

        A half-known binding is stale, or deleted sandboxes keep serving 404s.
        Replacement claims and stops retain sandbox_id while tearing down its
        sandbox, so matching ids alone cannot authorize a warm return."""
        if identity is None:
            return "workspace row is gone"
        status = identity.get("status")
        if status not in self._SESSION_SERVING_STATUSES:
            # Ownership is pending membership, not readiness: Phase 2 can finish init
            # before promotion. Retiring that owner drops the promotion/revert gate
            # and strands starting until the reaper. Other workers must reject it.
            initializing_here = status == "starting" and self._pending_lazy_start(
                binding.computer_id
            )
            if not initializing_here:
                return f"workspace status is {status!r}"
        db_sandbox_id = identity.get("sandbox_id")
        local_sandbox_id = self._session_sandbox_id(session)
        if db_sandbox_id != local_sandbox_id:
            return (
                f"sandbox identity moved (db={db_sandbox_id}, local={local_sandbox_id})"
            )
        return self._computer_identity_is_stale(binding, identity)

    def _computer_identity_is_stale(
        self, binding: ComputerBinding, identity: Dict[str, Any]
    ) -> str | None:
        """A successful computer CAS can leave a rejected workspace shadow behind.

        The losing sandbox may already be deleted; conflicting ids require
        recreation rather than guessing which table to trust."""
        computer_status = identity.get("computer_status")
        if computer_status is not None and computer_status != "running":
            initializing_here = (
                computer_status == "starting"
                and self._pending_lazy_start(binding.computer_id)
            )
            if not initializing_here:
                return f"computer status is {computer_status!r}"
        provider_ref = identity.get("provider_ref")
        workspace_ref = identity.get("sandbox_id")
        # A missing project ref is adopted on attach; only conflicting ids are split.
        if (
            provider_ref is not None
            and workspace_ref is not None
            and provider_ref != workspace_ref
        ):
            return (
                f"computer/workspace binding is split "
                f"(computer={provider_ref}, workspace={workspace_ref})"
            )
        return None

    # ---- the synchronous, route-facing accessors -------------------------

    def _live_session_computer(self, workspace_id: str) -> Optional[str]:
        """Which machine this worker is already serving that project on.

        Live sessions only, because these two accessors are synchronous and a
        miss means the same thing to every caller as no session at all."""
        return self._session_computer.get(workspace_id)

    def has_ready_session(self, workspace_id: str) -> bool:
        computer_id = self._live_session_computer(workspace_id)
        if computer_id is None:
            return False
        session = self._cached_session(computer_id)
        if session is None or not session._initialized or not session.sandbox:
            return False
        return session.sandbox.is_ready()

    def get_session_if_ready(
        self, workspace_id: str, *, expected_sandbox_id: str | None
    ) -> "Session | None":
        """Public UUID-only requests must never wake a sandbox.

        Combine readiness and lookup to avoid TOCTOU, and require expected_sandbox_id
        so share links cannot serve replaced sandboxes. On mismatch, decline and
        let routes use persistence; only async acquisition retires stale handles."""
        computer_id = self._live_session_computer(workspace_id)
        if computer_id is None:
            return None
        session = self._cached_session(computer_id)
        if session is None or not session._initialized or not session.sandbox:
            return None
        if not session.sandbox.is_ready():
            return None
        if self._session_sandbox_id(session) != expected_sandbox_id:
            return None
        return session
