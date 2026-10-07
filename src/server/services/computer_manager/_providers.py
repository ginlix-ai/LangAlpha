"""Seam: resolving a binding to a sandbox provider and its core config.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.runtime import (
    HostUnavailablePolicy,
    SandboxGoneError,
    SandboxHostLostError,
    SandboxRuntime,
)

from src.server.database.computer import (
    DEFAULT_ROOT_DIR,
    clear_computer_host_unavailable,
    get_computer,
    record_computer_disk,
    stamp_computer_host_unavailable,
)
from src.server.models.computer import ComputerStatus
from src.server.services.computer_disk import (
    STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
    disk_breakdown_key,
    disk_command,
    measured_within,
    parse_disk_output,
)
from src.server.services.computer_manager._types import ComputerBinding
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


class HostOutageClock(HostUnavailablePolicy):
    """Rebuild a computer once its sandbox's host has refused starts too long.

    The clock lives on the computer row, so every worker and every restart
    measures the same outage. A handle only calls a sandbox replaced once the
    row has named it: the ref the handle was opened on, or one the row
    confirmed since. A sandbox still being built is named nowhere yet.
    """

    def __init__(
        self, computer_id: str, threshold: timedelta, sandbox_id: str | None = None
    ):
        self.computer_id = computer_id
        self.threshold = threshold
        self._named = {sandbox_id} if sandbox_id else set()

    async def rebuild_now(self, sandbox_id: str) -> Optional[datetime]:
        clock = await stamp_computer_host_unavailable(self.computer_id, sandbox_id)
        if clock is None:
            return None
        self._named.add(sandbox_id)
        since, unavailable_for = clock
        if unavailable_for < self.threshold:
            return None
        logger.warning(
            f"Host of sandbox {sandbox_id} has refused starts of computer "
            f"{self.computer_id} for {unavailable_for}; rebuilding it from the "
            "backup and keeping the old sandbox for salvage",
            extra={
                "computer_id": self.computer_id,
                "sandbox_id": sandbox_id,
                "unavailable_seconds": int(unavailable_for.total_seconds()),
            },
        )
        return since

    def bound(self, sandbox_id: str) -> None:
        """A sandbox this handle built and published is the row's from then on,
        so a later replacement of it reads as one."""
        self._named.add(sandbox_id)

    async def reconnected(self, sandbox_id: str) -> Optional[bool]:
        current = await clear_computer_host_unavailable(self.computer_id, sandbox_id)
        if current == sandbox_id:
            self._named.add(sandbox_id)
            return True
        if current is None or sandbox_id not in self._named:
            return None
        return False


def host_outage_of(exc: BaseException) -> Optional[datetime]:
    """The outage that authorized replacing a sandbox, which its rebuild binds on."""
    return exc.outage_since if isinstance(exc, SandboxHostLostError) else None


class ProviderMixin:
    def _provider_settings(self, kind: str, overrides: Optional[Dict[str, Any]]):
        """Validate overrides into a fresh model to prevent cross-machine aliasing.

        Providers hold config by reference; computers.provider_config lets machines
        on the same deployment use different settings for a provider kind."""
        base = getattr(self.config.to_core_config().sandbox, kind, None)
        if base is None:
            raise ValueError(f"Unknown sandbox provider: {kind!r}")
        if not overrides:
            return base
        unknown = sorted(set(overrides) - set(type(base).model_fields))
        if unknown:
            raise ValueError(
                f"provider_config for kind {kind!r} names unknown settings: "
                f"{', '.join(unknown)}"
            )
        return type(base).model_validate({**base.model_dump(), **overrides})

    def _provider_for(self, binding: Optional[ComputerBinding]):
        """Use the persisted backend so one process can serve multiple providers.

        Unbound workspaces fall back to the deployment configuration."""
        from ptc_agent.core.sandbox.providers import build_provider, create_provider

        if binding is None or not binding.kind:
            return create_provider(self.config.to_core_config())
        return build_provider(
            binding.kind,
            self._provider_settings(binding.kind, binding.provider_config),
            working_dir=(
                binding.root_dir or self.config.filesystem.working_directory
            ),
        )

    def _host_unavailable_policy(
        self, computer_id: str, core_config: Any, sandbox_id: str | None = None
    ) -> Optional[HostUnavailablePolicy]:
        """The threshold comes from the config the session's provider is built
        from, overrides included. None when the backend sets none, which leaves
        the computer waiting for its host. *sandbox_id* is the ref the session
        is opened on."""
        sandbox = core_config.sandbox
        minutes = getattr(
            getattr(sandbox, str(sandbox.provider), None),
            "host_unavailable_rebuild_after_minutes",
            0,
        )
        if not isinstance(minutes, int) or minutes <= 0:
            return None
        return HostOutageClock(
            computer_id, timedelta(minutes=minutes), sandbox_id=sandbox_id
        )

    async def provider_for_workspace(self, workspace_id: str):
        """The caller must close this session-independent provider client."""
        return self._provider_for(await self.resolve_binding(workspace_id))

    async def provider_kind_for_workspace(self, workspace_id: str) -> Optional[str]:
        binding = await self.resolve_binding(workspace_id)
        return binding.kind

    def _core_config_for(self, binding: Optional[ComputerBinding]):
        """Rebuild FilesystemConfig when the root changes.

        Its model_post_init derives allowed and denied directories; copying or
        assigning the root would leave those lists stale."""
        core_config = self.config.to_core_config()
        if binding is None:
            return core_config
        if binding.kind:
            core_config.sandbox.provider = binding.kind
            setattr(
                core_config.sandbox,
                binding.kind,
                self._provider_settings(binding.kind, binding.provider_config),
            )
        root = binding.root_dir
        if root and root != core_config.filesystem.working_directory:
            from ptc_agent.config.core import FilesystemConfig

            core_config.filesystem = FilesystemConfig(
                **{
                    **core_config.filesystem.model_dump(exclude_unset=True),
                    "working_directory": root,
                },
            )
        return core_config

    @asynccontextmanager
    async def _detached_runtime(
        self, sandbox_id: str, *, binding: Optional[ComputerBinding] = None
    ):
        """Lifecycle operations must reach the durable sandbox without a local session.

        Own and close the provider here to avoid leaking its SDK HTTP client.
        binding selects the machine backend, otherwise the deployment is used."""
        provider = self._provider_for(binding)
        try:
            yield await provider.get(sandbox_id)
        finally:
            await provider.close()

    def measures_disk(self, computer: Dict[str, Any]) -> bool:
        """A local computer without a storage quota reports the host's disk, not its own."""
        if computer.get("kind") != "docker":
            return True
        try:
            settings = self._provider_settings(
                "docker", computer.get("provider_config") or None
            )
        except ValueError:
            return False
        return bool(getattr(settings, "storage_quota_enabled", False))

    async def refresh_computer_disk(
        self,
        computer: Dict[str, Any] | str,
        *,
        sandbox_id: str | None = None,
        breakdown: bool = False,
        min_age_s: float | None = None,
        breakdown_max_age_s: float | None = None,
        timeout: float | None = None,
    ) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, int]]]:
        """Measure a computer's disk and store the reading, best effort.

        Answers the row as it now stands and the folder sizes (empty unless
        ``breakdown``), or sizes None when nothing was measured: a machine that
        does not measure, one measured within ``min_age_s``, or a failure,
        which leaves the last reading in place and never raises. ``computer``
        may be an id, read here. Without ``sandbox_id`` only a running machine
        is measured, on its current sandbox; a caller that names one (a stop
        about to take it away) is measuring that sandbox whatever the row says.
        With ``breakdown_max_age_s`` a breakdown of that sandbox taken within
        it, by any worker, is answered again without an exec, beside the
        row's reading. ``timeout`` bounds everything, the row read and a
        detached connect included, since a stop waits on this under the
        machine lock; it defaults to the exec's own budget plus a connect.
        """
        if timeout is None:
            timeout = disk_command("/", breakdown=breakdown)[1] + 5
        computer_id = computer if isinstance(computer, str) else computer.get("computer_id")
        row = None if isinstance(computer, str) else computer
        try:
            async with asyncio.timeout(timeout):
                if row is None:
                    row = await get_computer(str(computer_id))
                if row is None or not self.measures_disk(row):
                    return row, None
                if sandbox_id is None:
                    if row.get("status") != ComputerStatus.RUNNING:
                        return row, None
                    sandbox_id = row.get("provider_ref")
                if not sandbox_id:
                    return row, None
                if min_age_s is not None and measured_within(row, min_age_s):
                    return row, None
                if breakdown and breakdown_max_age_s is not None:
                    sizes = await self._recent_breakdown(
                        str(computer_id), str(sandbox_id), breakdown_max_age_s
                    )
                    if sizes is not None:
                        return row, sizes
                return await self._measure_disk(row, str(sandbox_id), breakdown)
        except Exception as e:  # noqa: BLE001 - a reading is never worth failing its caller
            logger.warning(f"Disk reading failed for computer {computer_id}: {e}")
            return row, None

    @asynccontextmanager
    async def _computer_runtime(
        self, computer: Dict[str, Any], sandbox_id: str
    ) -> AsyncIterator[SandboxRuntime]:
        """This worker's session runtime when it holds that sandbox, else a detached one."""
        session = self._cached_session(str(computer["computer_id"]))
        runtime = getattr(getattr(session, "sandbox", None), "runtime", None)
        if runtime is not None and self._session_sandbox_id(session) == sandbox_id:
            yield runtime
            return
        binding = self._binding_from_computer("", computer)
        async with self._detached_runtime(sandbox_id, binding=binding) as detached:
            yield detached

    async def _measure_disk(
        self, computer: Dict[str, Any], sandbox_id: str, breakdown: bool
    ) -> tuple[Dict[str, Any], Dict[str, int]]:
        async with self._computer_runtime(computer, sandbox_id) as runtime:
            return await self._read_and_record_disk(
                computer, runtime, breakdown, sandbox_id=sandbox_id
            )

    async def _recent_breakdown(
        self, computer_id: str, sandbox_id: str, max_age_s: float
    ) -> Optional[Dict[str, int]]:
        """The last breakdown of this sandbox if it is young enough, else None.

        Redis, not process memory: the worker answering the storage panel is
        rarely the one that measured. A cache miss or outage only costs an exec.
        """
        cached = await get_cache_client().get(disk_breakdown_key(computer_id))
        if not isinstance(cached, dict) or cached.get("sandbox_id") != sandbox_id:
            return None
        try:
            measured_at = datetime.fromisoformat(cached["measured_at"])
            sizes = {str(k): int(v) for k, v in cached["sizes"].items()}
        except (KeyError, TypeError, ValueError):
            return None
        if (datetime.now(timezone.utc) - measured_at).total_seconds() >= max_age_s:
            return None
        return sizes

    async def _remember_breakdown(
        self, computer_id: str, sandbox_id: str, sizes: Dict[str, int]
    ) -> None:
        await get_cache_client().set(
            disk_breakdown_key(computer_id),
            {
                "sandbox_id": sandbox_id,
                "measured_at": datetime.now(timezone.utc).isoformat(),
                "sizes": sizes,
            },
            ttl=STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
        )

    async def _read_and_record_disk(
        self,
        computer: Dict[str, Any],
        runtime: SandboxRuntime,
        breakdown: bool,
        *,
        sandbox_id: str,
    ) -> tuple[Dict[str, Any], Dict[str, int]]:
        root_dir = computer.get("root_dir") or DEFAULT_ROOT_DIR
        command, exec_timeout = disk_command(root_dir, breakdown=breakdown)
        # df runs first, so the reading is as old as the exec's start.
        observed_at = datetime.now(timezone.utc)
        result = await runtime.exec(command, timeout=exec_timeout)
        reading, sizes = parse_disk_output(
            result.stdout or "", root_dir, breakdown=breakdown
        )
        if breakdown:
            await self._remember_breakdown(
                str(computer["computer_id"]), sandbox_id, sizes
            )
        if reading is None:
            logger.warning(
                "Disk reading on computer %s returned no parsable df line",
                computer.get("computer_id"),
            )
            return computer, sizes
        updated = await record_computer_disk(
            str(computer["computer_id"]),
            sandbox_id=sandbox_id,
            observed_at=observed_at,
            total_bytes=reading.total_bytes,
            used_bytes=reading.used_bytes,
            free_bytes=reading.free_bytes,
        )
        if updated is not None:
            return updated, sizes
        # Lost the write. To a newer reading of the same sandbox, the folders
        # still describe the machine; to a replacement, they describe one that
        # is gone, so they are dropped and nothing is answered as measured.
        current = await get_computer(str(computer["computer_id"])) or computer
        if current.get("provider_ref") != sandbox_id:
            return current, None
        return current, sizes

    def _is_sandbox_gone(
        self, exc: Exception, binding: Optional[ComputerBinding] = None
    ) -> bool:
        """Reuse classifiers because constructing providers opens SDK clients.

        This synchronous method cannot await close, so a provider per failed teardown
        would leak clients. Cache per backend because exception shapes differ."""
        from ptc_agent.core.sandbox.runtime import SandboxFailureKind

        if isinstance(exc, SandboxGoneError):
            return True
        key = (
            binding.provider_identity
            if binding is not None and binding.kind
            else (None, "{}")
        )
        classifier = self._error_classifiers.get(key)
        if classifier is None:
            classifier = self._provider_for(binding if key[0] else None)
            self._error_classifiers[key] = classifier
        return classifier.classify_error(exc) is SandboxFailureKind.SANDBOX_GONE
