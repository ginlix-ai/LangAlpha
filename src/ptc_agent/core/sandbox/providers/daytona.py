"""Daytona sandbox provider — wraps the Daytona SDK."""

import asyncio
import contextlib
import contextvars
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from daytona import (
    AsyncDaytona,
    CreateSandboxFromSnapshotParams,
    CreateSnapshotParams,
    DaytonaConfig as SDKDaytonaConfig,
    DaytonaNotFoundError,
    Image,
    Resources,
)

from ptc_agent.config.core import DaytonaConfig, PlatformSecretDefinition
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox._defaults import SANDBOX_FALLBACK_CPU
from ptc_agent.core.sandbox.runtime import (
    MetricSample,
    SandboxFailureKind,
    SandboxProvider,
    SandboxRateLimitedError,
)
from ptc_agent.core.sandbox.platform_secrets import (
    ReconciledPlatformSecret,
    ResolvedPlatformSecret,
)
from ptc_agent.core.sandbox.providers._image_recipe import (
    BASE_IMAGE,
    Env,
    Run,
    Step,
    config_hash,
    image_identity,
    image_steps,
)
from ptc_agent.core.sandbox.providers._metrics import normalize_samples
from ptc_agent.core.sandbox.providers._tiers import TierChoice, resolve_tier
from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime
from ptc_agent.core.sandbox.providers.daytona_secrets import (
    DaytonaSecretReconciler,
    daytona_error_code as _daytona_error_code,
    daytona_error_status as _daytona_error_status,
    is_daytona_host_unavailable,
    is_transient_daytona_error,
)

logger = structlog.get_logger(__name__)

# A snapshot in one of these is on its way to active (or, removing, to gone).
_SNAPSHOT_IN_PROGRESS = frozenset(
    {"pending", "pulling", "building", "snapshotting", "removing"}
)
_SNAPSHOT_FAILED = frozenset({"build_failed", "error"})


def _state_value(state: Any) -> str:
    return state.value if hasattr(state, "value") else str(state)


def _detach_event_dispatcher(client: AsyncDaytona) -> None:
    """Start the SDK's event-socket tasks outside the caller's context.

    The client is built lazily during a turn, and its dispatcher reconnects
    from ``subscribe`` calls made during later turns. A task copies the context
    it is created in, so the socket's long-lived tasks would otherwise pin a
    turn's request-scoped state for the client's whole life. The dispatcher
    is SDK-private; if it moves, this warns rather than fail.
    """
    dispatcher = getattr(client, "_event_dispatcher", None)
    ensure_connected = getattr(dispatcher, "ensure_connected", None)
    if ensure_connected is None:
        logger.warning(
            "Daytona event dispatcher hook not found; its socket tasks will "
            "pin the context of the turn that starts them"
        )
        return
    dispatcher.ensure_connected = lambda: contextvars.Context().run(ensure_connected)


class DaytonaProvider(SandboxProvider):
    """Provider that manages sandboxes via the Daytona SDK."""

    label = "Daytona"
    platform_secret_namespace_env = DaytonaConfig.ENV_FIELDS["secret_namespace"]
    scrubs_platform_secrets = True
    reports_metrics = True

    def __init__(
        self,
        config: DaytonaConfig,
        working_dir: str | None = None,
        *,
        # Daytona holds a Secret's routing itself, so the catalog is not needed.
        platform_secrets: Sequence[PlatformSecretDefinition] = (),
    ) -> None:
        self._config = config
        self._working_dir = working_dir or DEFAULT_SANDBOX_ROOT
        sdk_config = SDKDaytonaConfig(api_key=config.api_key, api_url=config.base_url)
        self._client = contextvars.Context().run(AsyncDaytona, sdk_config)
        _detach_event_dispatcher(self._client)

    # -- SandboxProvider interface --

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        platform_secret_bindings: dict[str, str] | None = None,
        mcp_packages: list[str] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        **kwargs: Any,
    ) -> DaytonaRuntime:
        """Create a new Daytona sandbox, optionally from a snapshot.

        Args:
            env_vars: Environment variables injected at creation time.
            platform_secret_bindings: Environment-variable to organization
                Secret-name mappings mounted by Daytona.
            mcp_packages: NPM packages for MCP servers (needed for snapshot).
            tier: Resource tier name. Hosted Daytona can't resize a snapshot
                sandbox or override its resources at create time, so a tier's
                cpu/mem/disk is baked into a tier-specific snapshot. ``None``
                resolves to the configured default tier, whose size is applied
                the same way. An unknown (removed-from-config) tier falls back to
                a base-sized sandbox so the workspace stays recoverable.
            auto_stop_minutes: Auto-stop interval override in minutes (0 disables,
                for always-on). ``None`` uses the configured default.
            **kwargs: Extra keyword arguments (reserved for future use).

        Returns:
            A DaytonaRuntime wrapping the new sandbox.
        """
        choice, resources = self._resolve_tier(tier)
        snapshot_name = await self._ensure_snapshot(
            mcp_packages=mcp_packages or [],
            tier=choice.name,
            resources=resources,
        )

        # An elevated (non-default) tier's size lives only in its snapshot. If
        # that snapshot's BUILD failed, creating from the base (snapshot=None)
        # would silently under-provision a billed tier — fail loudly so the caller
        # (e.g. set_workspace_spec) reverts the tier instead of charging for a
        # size the user never received. Guarded on snapshot_enabled so a globally
        # snapshots-disabled deployment still creates a base-sized sandbox rather
        # than raising (it never expected a sized snapshot in the first place).
        # The default tier is exempt: base-sized ~= default, so a missing default
        # snapshot degrades gracefully instead of hard-failing sandbox creation.
        if choice.elevated and snapshot_name is None and self._config.snapshot_enabled:
            raise RuntimeError(
                f"Could not provision the {choice.name!r} tier snapshot; "
                "refusing to create a base-sized sandbox for an elevated tier"
            )

        auto_stop = (
            auto_stop_minutes
            if auto_stop_minutes is not None
            else self._config.auto_stop_interval // 60
        )
        params = CreateSandboxFromSnapshotParams(
            snapshot=snapshot_name if snapshot_name else None,
            env_vars=env_vars or None,
            secrets=platform_secret_bindings or None,
            auto_stop_interval=auto_stop,
            auto_archive_interval=self._config.auto_archive_interval // 60,
            auto_delete_interval=self._config.auto_delete_interval // 60,
        )

        sdk_sandbox = await self._client.create(params)
        return DaytonaRuntime(
            sdk_sandbox,
            snapshot_name=snapshot_name,
            default_working_dir=self._working_dir,
        )

    async def get(self, sandbox_id: str) -> DaytonaRuntime:
        sdk_sandbox = await self._client.get(sandbox_id)
        return DaytonaRuntime(
            sdk_sandbox,
            default_working_dir=self._working_dir,
        )

    async def metrics_since(
        self,
        sandbox_id: str,
        since: datetime,
        *,
        until: datetime | None = None,
    ) -> list[MetricSample]:
        # ``get`` is an API lookup that leaves a stopped sandbox stopped.
        # History is the telemetry backend's aggregated buckets, kept 3 days.
        # That backend has a rate limit of its own, far below the control
        # plane's (measured 2026-10-23: about 40 reads at once, then about 1/s),
        # and refuses with a bare DaytonaError whose 429 is only on its cause.
        try:
            sdk_sandbox = await self._client.get(sandbox_id)
            raw = await sdk_sandbox.get_metrics(since, until or datetime.now(UTC))
        except Exception as e:
            if _daytona_error_status(e) == 429:
                raise SandboxRateLimitedError(str(e).split("\n", 1)[0]) from e
            raise
        return normalize_samples(raw, since, until)

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception as e:
            logger.debug("Failed to close Daytona client", error=str(e))

    async def reconcile_platform_secrets(
        self, secrets: Sequence[ResolvedPlatformSecret]
    ) -> tuple[ReconciledPlatformSecret, ...]:
        """Create or update every required organization-scoped Secret."""

        return await DaytonaSecretReconciler(self._client).reconcile(secrets)

    def is_transient_error(self, exc: Exception) -> bool:
        """Classify whether *exc* is a transient Daytona SDK error."""
        return is_transient_daytona_error(exc)

    def is_host_unavailable(self, exc: Exception) -> bool:
        return is_daytona_host_unavailable(exc)

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        """Classify a Daytona failure from its structured error metadata.

        ``DaytonaError`` carries ``status_code`` and a machine-readable
        ``error_code``; both are needed because a missing *file* and a missing
        *sandbox* are both HTTP 404. Measured against the live API (SDK 0.200.1):
        a per-path miss reports ``FILE_NOT_FOUND``, a dead sandbox reports
        ``NOT_FOUND``, and a bulk-download against a deleted sandbox reports
        *neither* — it arrives with no status at all, which is why status-less
        failures must stay ``UNKNOWN`` instead of collapsing into "absent".

        Absence is checked before ``is_transient_daytona_error`` for the reason
        the base classifier gives: that helper scans the message, and the server
        echoes the path into it ("file not found: /home/workspace/x.log"), so a
        missing file named ``session_timeout.log`` read as a timeout and turned
        an ordinary 404 into a permanent 503. The 404 → ``SANDBOX_GONE`` check
        deliberately stays *after* the scan: gone authorizes destroying and
        replacing the sandbox, so a transient-looking failure must not reach it.
        """
        error_code = _daytona_error_code(exc)
        if error_code == "FILE_NOT_FOUND":
            return SandboxFailureKind.PATH_ABSENT

        if is_transient_daytona_error(exc):
            return SandboxFailureKind.TRANSIENT

        status = _daytona_error_status(exc)
        if status == 404:
            return SandboxFailureKind.SANDBOX_GONE
        if status == 429 or (status is not None and 500 <= status <= 599):
            return SandboxFailureKind.TRANSIENT
        return SandboxFailureKind.UNKNOWN

    # -- Snapshot management --

    def _resolve_tier(self, tier: str | None) -> tuple[TierChoice, Resources | None]:
        """The tier ``create`` uses and the size baked into its snapshot.

        Resolved for every tier uniformly (including the default) so the
        configured default-tier cpu/mem/disk actually take effect instead of
        falling through to the Daytona platform default.
        """
        choice = resolve_tier(
            tier,
            default_tier=self._config.default_tier,
            resource_tiers=self._config.resource_tiers,
        )
        if choice.preset is None:
            return choice, None
        preset = choice.preset
        return choice, Resources(cpu=preset.cpu, memory=preset.memory, disk=preset.disk)

    def snapshot_name(self, mcp_packages: list[str], *, tier: str | None = None) -> str:
        """The snapshot ``create`` starts a sandbox of ``tier`` from."""
        choice, resources = self._resolve_tier(tier)
        return self._snapshot_name(mcp_packages, choice.name, resources)

    async def snapshot_state(self, name: str) -> str | None:
        """Daytona's state for the snapshot (``active``, ``building``, ...), None if absent."""
        try:
            snapshot = await self._client.snapshot.get(name)
        except DaytonaNotFoundError:
            return None
        return _state_value(snapshot.state)

    async def build_snapshot(
        self,
        mcp_packages: list[str],
        *,
        tier: str | None = None,
        on_log: Callable[[str], None] | None = None,
    ) -> str:
        """Build ``tier``'s snapshot and wait until it is active; raises if the build fails.

        A snapshot whose last build failed holds the name, so it is removed
        first, and the build starts once Daytona has let the name go.
        """
        choice, resources = self._resolve_tier(tier)
        name = self._snapshot_name(mcp_packages, choice.name, resources)
        if await self.snapshot_state(name) in _SNAPSHOT_FAILED:
            # A server's first create removes a failed snapshot too, and may get there first.
            with contextlib.suppress(DaytonaNotFoundError):
                await self._client.snapshot.delete(name)
            for _ in range(60):
                if await self.snapshot_state(name) is None:
                    break
                await asyncio.sleep(2)
            else:
                raise RuntimeError(
                    f"{name} is still being removed; rerun once it is gone"
                )
        await self._create_snapshot(name, mcp_packages, resources, on_log=on_log)
        return name

    async def _settled_snapshot_state(self, name: str) -> str | None:
        """The snapshot's state once no build is under way, or after five minutes.

        A build started elsewhere (another worker's first create, or
        ``make daytona-snapshots``) passes through several states on its way to
        active, and a sandbox created from it before then fails.
        """
        state: str | None = "pending"
        for _ in range(60):
            with contextlib.suppress(Exception):
                state = await self.snapshot_state(name)
            if state not in _SNAPSHOT_IN_PROGRESS:
                return state
            await asyncio.sleep(5)
        return state

    async def activate_snapshot(self, name: str) -> str:
        """Make an inactive snapshot usable again; returns the state Daytona reports for it."""
        return _state_value((await self._client.snapshot.activate(name)).state)

    def _snapshot_name(
        self, mcp_packages: list[str] | None, tier: str, resources: Resources | None
    ) -> str:
        config_hash = self._get_snapshot_hash(mcp_packages, resources=resources)
        base_name = self._config.snapshot_name or "ptc-base"
        if resources is not None:
            return f"{base_name}-{tier}-{config_hash}"
        return f"{base_name}-{config_hash}"

    async def _create_snapshot(
        self,
        name: str,
        mcp_packages: list[str] | None,
        resources: Resources | None,
        *,
        on_log: Callable[[str], None] | None = None,
    ) -> None:
        image = self._create_snapshot_image(mcp_packages, resources=resources)
        await self._client.snapshot.create(
            CreateSnapshotParams(name=name, image=image, resources=resources),
            on_logs=on_log or (lambda log: logger.debug("Snapshot build", log=log)),
        )

    def _image_steps(
        self, mcp_packages: list[str] | None, resources: Resources | None
    ) -> list[Step]:
        return image_steps(
            working_dir=self._working_dir,
            mcp_packages=mcp_packages,
            cpu=resources.cpu if resources is not None else SANDBOX_FALLBACK_CPU,
        )

    def _get_snapshot_hash(
        self,
        mcp_packages: list[str] | None = None,
        *,
        resources: Resources | None = None,
    ) -> str:
        """Generate an 8-char hash for snapshot versioning (see ``image_identity``)."""
        steps = self._image_steps(mcp_packages, resources)
        return config_hash(image_identity(steps, resources))

    def _create_snapshot_image(
        self,
        mcp_packages: list[str] | None = None,
        *,
        resources: Resources | None = None,
    ) -> Image:
        """Translate the shared image recipe into Daytona's Image DSL."""
        image = Image.base(BASE_IMAGE)
        for step in self._image_steps(mcp_packages, resources):
            if isinstance(step, Run):
                image = image.run_commands(*step.commands)
            elif isinstance(step, Env):
                image = image.env(dict(step.values))
            else:
                image = image.workdir(step.path)

        logger.info(
            "Created snapshot image definition",
            mcp_packages=sorted(mcp_packages or []),
            thread_cpu=resources.cpu if resources is not None else SANDBOX_FALLBACK_CPU,
        )
        return image

    async def _ensure_snapshot(
        self,
        mcp_packages: list[str] | None = None,
        *,
        tier: str | None = None,
        resources: Resources | None = None,
    ) -> str | None:
        """Ensure a snapshot exists for the current configuration.

        When ``resources`` is given (a non-default tier), they're baked into a
        tier-specific snapshot — the only way to size a snapshot-born sandbox on
        hosted Daytona, which supports neither runtime resize nor per-sandbox
        resource overrides.

        Returns:
            Snapshot name if available, None otherwise.
        """
        if not self._config.snapshot_enabled:
            logger.debug("Snapshot feature disabled in config")
            return None

        snapshot_name = self._snapshot_name(
            mcp_packages, tier or self._config.default_tier, resources
        )
        logger.info("Checking for snapshot", snapshot_name=snapshot_name)

        # Check if snapshot exists and is usable
        try:
            snapshots_result = await self._client.snapshot.list()
            snapshots = (
                snapshots_result.items
                if hasattr(snapshots_result, "items")
                else snapshots_result
            )

            snapshot_obj = None
            for s in snapshots:
                if hasattr(s, "name") and s.name == snapshot_name:
                    snapshot_obj = s
                    break

            if snapshot_obj:
                state = _state_value(snapshot_obj.state)
                if state == "build_failed":
                    logger.warning(
                        "Found failed snapshot, will recreate",
                        snapshot_name=snapshot_name,
                        error=snapshot_obj.error_reason,
                    )
                    try:
                        await self._client.snapshot.delete(snapshot_obj)
                        logger.info(
                            "Deleted failed snapshot",
                            snapshot_name=snapshot_name,
                        )
                        await asyncio.sleep(2)
                    except Exception as del_err:
                        logger.warning(
                            "Could not delete failed snapshot",
                            error=str(del_err),
                        )
                    snapshot_exists = False
                elif state == "active":
                    snapshot_exists = True
                elif state in _SNAPSHOT_IN_PROGRESS:
                    logger.info(
                        "Snapshot is still building, waiting...",
                        snapshot_name=snapshot_name,
                        state=state,
                    )
                    state = await self._settled_snapshot_state(snapshot_name)
                    if state in _SNAPSHOT_IN_PROGRESS:
                        logger.warning(
                            "Snapshot build timed out", snapshot_name=snapshot_name
                        )
                        return None
                    snapshot_exists = state == "active"
                else:
                    logger.warning(f"Snapshot in unexpected state: {state}")
                    snapshot_exists = False
            else:
                snapshot_exists = False

        except Exception as e:
            logger.warning("Error listing snapshots", error=str(e))
            snapshot_exists = False

        # Create snapshot if it doesn't exist
        if not snapshot_exists and self._config.snapshot_auto_create:
            logger.info("Creating snapshot", snapshot_name=snapshot_name)
            try:
                await self._create_snapshot(snapshot_name, mcp_packages, resources)
                logger.info(
                    "Snapshot created successfully",
                    snapshot_name=snapshot_name,
                )
                return snapshot_name
            except Exception as e:
                error_str = str(e)
                if "already exists" in error_str.lower():
                    # Another build holds the name, and a sandbox can start
                    # from it only once that build is done.
                    state = await self._settled_snapshot_state(snapshot_name)
                    if (
                        state is None
                        or state in _SNAPSHOT_IN_PROGRESS
                        or state in _SNAPSHOT_FAILED
                    ):
                        logger.error(
                            "Snapshot another build holds is not usable",
                            snapshot_name=snapshot_name,
                            state=state,
                        )
                        return None
                    logger.info(
                        "Snapshot already exists, will use it",
                        snapshot_name=snapshot_name,
                    )
                    return snapshot_name
                logger.error("Failed to create snapshot", error=error_str)
                return None

        if snapshot_exists:
            logger.info("Using existing snapshot", snapshot_name=snapshot_name)
            return snapshot_name

        logger.warning("Snapshot not found and auto_create disabled")
        return None
