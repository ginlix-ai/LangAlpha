"""Daytona sandbox provider — wraps the Daytona SDK."""

import asyncio
import contextvars
import hashlib
import json
import shlex
from collections.abc import Sequence
from typing import Any

import structlog
from daytona import (
    AsyncDaytona,
    CreateSandboxFromSnapshotParams,
    CreateSnapshotParams,
    DaytonaConfig as SDKDaytonaConfig,
    Image,
    Resources,
)

from ptc_agent.config.core import DaytonaConfig
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox._defaults import (
    DEFAULT_DEPENDENCIES,
    SANDBOX_FALLBACK_CPU,
    SANDBOX_IMAGE_ENV,
    SANDBOX_NODE_VERSION,
    SANDBOX_PLAYWRIGHT_VERSION,
    SNAPSHOT_PYTHON_VERSION,
    sandbox_thread_env,
)
from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxProvider,
)
from ptc_agent.core.sandbox.platform_secrets import (
    ReconciledPlatformSecret,
    ResolvedPlatformSecret,
)
from ptc_agent.core.sandbox.providers._tiers import resolve_tier
from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime
from ptc_agent.core.sandbox.providers.daytona_secrets import (
    DaytonaSecretReconciler,
    daytona_error_code as _daytona_error_code,
    daytona_error_status as _daytona_error_status,
    is_daytona_host_unavailable,
    is_transient_daytona_error,
)

logger = structlog.get_logger(__name__)


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

    SNAPSHOT_PYTHON_VERSION = SNAPSHOT_PYTHON_VERSION
    DEFAULT_DEPENDENCIES = DEFAULT_DEPENDENCIES

    def __init__(self, config: DaytonaConfig, working_dir: str | None = None) -> None:
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
        # Resolved for every tier uniformly (including the default) so the
        # configured default-tier cpu/mem/disk actually take effect instead of
        # falling through to the Daytona platform default.
        choice = resolve_tier(
            tier,
            default_tier=self._config.default_tier,
            resource_tiers=self._config.resource_tiers,
        )
        resources = (
            Resources(
                cpu=choice.preset.cpu,
                memory=choice.preset.memory,
                disk=choice.preset.disk,
            )
            if choice.preset is not None
            else None
        )

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

    def _get_snapshot_hash(
        self,
        mcp_packages: list[str] | None = None,
        *,
        resources: Resources | None = None,
    ) -> str:
        """Generate an 8-char hash for snapshot versioning.

        ``resources`` (cpu/memory/disk) are folded into the hash so a tier's size
        is part of its snapshot identity. Hosted Daytona bakes size into the
        snapshot at build time, so retuning a tier's cpu/mem/disk must yield a new
        hash — otherwise the stale-sized snapshot would be reused silently.
        """
        config_data = {
            "base_image": "ubuntu:24.04",
            "working_dir": self._working_dir,
            "python_version": self.SNAPSHOT_PYTHON_VERSION,
            "dependencies": self.DEFAULT_DEPENDENCIES,
            "resources": (
                {
                    "cpu": resources.cpu,
                    "memory": resources.memory,
                    "disk": resources.disk,
                }
                if resources is not None
                else None
            ),
            # The apt-list strings below (e.g. "nodejs", "playwright") are static
            # labels — they stay identical when the pinned Node version or the baked
            # Playwright browser layout changes, so hash those explicitly to force a
            # rebuild of existing snapshots when either changes.
            #
            # The image env is hashed whole rather than by hand-picked key: an
            # env-only edit changes no other hashed input, so without this the
            # name is unchanged, the existing snapshot is reused verbatim, and
            # the new variable is simply never built.
            "image_env": SANDBOX_IMAGE_ENV,
            "node_version": SANDBOX_NODE_VERSION,
            "mcp_packages": sorted(mcp_packages or []),
            # Bump when the image changes in a way no other key here records
            # (a reordered layer, a folded-in cache clean, a build-time guard).
            "image_revision": 2,
            # Per-tier BLAS/OpenMP caps. Derived from `resources` above, so this
            # is redundant for a sized tier, but it is the only input that moves
            # when the unsized fallback changes.
            "thread_env": sandbox_thread_env(
                resources.cpu if resources is not None else SANDBOX_FALLBACK_CPU
            ),
            # Both language ports of Playwright ride this one pin; drifting it
            # changes the baked browser revision.
            "playwright_version": SANDBOX_PLAYWRIGHT_VERSION,
            "no_recommends": True,
            "apt_packages": [
                "ca-certificates",
                "curl",
                "nodejs",
                "ripgrep",
                "uv",
                "uvx",
                "jq",
                "git",
                "unzip",
                "libreoffice-writer",
                "libreoffice-calc",
                "libreoffice-impress",
                "libreoffice-draw",
                "gcc",
                "poppler-utils",
                "pandoc",
                "qpdf",
                "fuse3",
                "fonts-noto-cjk",
                "fonts-dejavu-core",
                "fonts-opensymbol",
                "gh",
                "polymarket",
                "playwright",
                "docker-ce",
                "docker-ce-cli",
                "containerd.io",
            ],
        }
        config_str = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:8]

    def _create_snapshot_image(
        self,
        mcp_packages: list[str] | None = None,
        *,
        resources: Resources | None = None,
    ) -> Image:
        """Build the declarative Image definition for a snapshot.

        ``resources`` sizes the baked-in BLAS/OpenMP thread caps. A snapshot is
        already per tier, so the tier's cpu count is fixed for every sandbox born
        from this image and the image is the right place to carry it.

        Every cache is emptied in the same command that filled it: the SDK emits
        one ``RUN`` per string, so a trailing cleanup command only adds a whiteout
        and leaves the bytes in the earlier layer.
        """
        dependencies = self.DEFAULT_DEPENDENCIES
        pkgs = mcp_packages or []
        cpu = resources.cpu if resources is not None else SANDBOX_FALLBACK_CPU
        browsers_path = SANDBOX_IMAGE_ENV["PLAYWRIGHT_BROWSERS_PATH"]

        base_image = Image.base("ubuntu:24.04").run_commands(
            "echo 'debconf debconf/frontend select Noninteractive'"
            " | debconf-set-selections",
            "apt-get update && apt-get install -y"
            " python3 python3-pip python3-venv"
            " gcc gfortran build-essential"
            " && apt-get clean && rm -rf /var/lib/apt/lists/*",
            "ln -sf /usr/bin/python3 /usr/bin/python",
            "ln -sf /usr/bin/pip3 /usr/bin/pip",
            "rm -f /usr/lib/python*/EXTERNALLY-MANAGED",
        )

        image = (
            base_image.run_commands(
                # --no-install-recommends, then name what conversion actually
                # needs. The `libreoffice` metapackage's recommends were most of
                # the office stack's footprint: a JRE the headless converter never
                # calls, Mesa/LLVM Vulkan drivers, and the full Noto font set.
                "apt-get update"
                " && apt-get install -y --no-install-recommends"
                " ca-certificates curl ripgrep jq git unzip gcc"
                " poppler-utils pandoc qpdf fuse3"
                " libreoffice-writer libreoffice-calc libreoffice-impress"
                " libreoffice-draw"
                " fonts-noto-cjk fonts-dejavu-core fonts-opensymbol"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*",
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
                # Relocate BOTH uv and uvx — the MCP command allowlist permits
                # `uvx`, so it must be on PATH too (mirrors Dockerfile.sandbox).
                "mv /root/.local/bin/uv /root/.local/bin/uvx /usr/local/bin/",
                # Node.js: pinned direct binary (matches Dockerfile.sandbox;
                # avoids apt-mirror flakiness and unpinned-version drift).
                'NODE_ARCH=$([ "$(dpkg --print-architecture)" = "arm64" ]'
                " && echo arm64 || echo x64)"
                f" && curl -fsSL https://nodejs.org/dist/v{SANDBOX_NODE_VERSION}/"
                f"node-v{SANDBOX_NODE_VERSION}-linux-${{NODE_ARCH}}.tar.xz"
                " -o /tmp/node.tar.xz"
                " && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1"
                " && rm /tmp/node.tar.xz",
                *[f"npm install -g {shlex.quote(pkg)}" for pkg in pkgs],
                # Same pins as Dockerfile.sandbox: the pptx skill and its checks
                # target 4.0.1, and playwright rides the shared version so the npm
                # side resolves the same browser revision as the Python side.
                "npm install -g docx pptxgenjs@4.0.1"
                f" playwright@{SANDBOX_PLAYWRIGHT_VERSION}"
                " && npm cache clean --force",
                "GH_ARCH=$(dpkg --print-architecture)"
                " && curl -fsSL https://github.com/cli/cli/releases/download/"
                "v2.87.3/gh_2.87.3_linux_${GH_ARCH}.tar.gz -o /tmp/gh.tar.gz"
                " && tar -xzf /tmp/gh.tar.gz -C /tmp"
                " && mv /tmp/gh_2.87.3_linux_${GH_ARCH}/bin/gh /usr/local/bin/gh"
                " && rm -rf /tmp/gh.tar.gz /tmp/gh_2.87.3_linux_${GH_ARCH}",
                "POLY_ARCH=$(uname -m)"
                " && curl -fsSL https://github.com/Polymarket/polymarket-cli/"
                "releases/download/v0.1.4/"
                "polymarket-v0.1.4-${POLY_ARCH}-unknown-linux-gnu.tar.gz"
                " -o /tmp/polymarket.tar.gz"
                " && tar -xzf /tmp/polymarket.tar.gz -C /tmp"
                " && mv /tmp/polymarket /usr/local/bin/polymarket"
                " && rm -rf /tmp/polymarket.tar.gz",
                # --with-deps runs its own apt-get update, so the emptied lists
                # above are not a problem; clean again on the way out.
                f"PLAYWRIGHT_BROWSERS_PATH={browsers_path}"
                " npx playwright install --with-deps chromium"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*"
                " && npm cache clean --force && rm -rf /root/.npm/_npx",
                # -- Docker Engine (for interactive-dashboard complex tier) --
                "install -m 0755 -d /etc/apt/keyrings"
                " && curl -fsSL https://download.docker.com/linux/ubuntu/gpg"
                " -o /etc/apt/keyrings/docker.asc"
                " && chmod a+r /etc/apt/keyrings/docker.asc",
                'echo "deb [arch=$(dpkg --print-architecture)'
                " signed-by=/etc/apt/keyrings/docker.asc]"
                " https://download.docker.com/linux/ubuntu"
                ' $(. /etc/os-release && echo $VERSION_CODENAME) stable"'
                " > /etc/apt/sources.list.d/docker.list",
                # docker-ce keeps its recommends: the compose plugin is one of
                # them and the interactive-dashboard tier uses it.
                "apt-get update"
                " && apt-get install -y docker-ce docker-ce-cli containerd.io"
                " && apt-get clean && rm -rf /var/lib/apt/lists/*",
            )
            # Same values Dockerfile.sandbox sets with ENV, and the same ones
            # PTCSandbox injects per sandbox at create time. Both layers are
            # load-bearing; see SANDBOX_IMAGE_ENV.
            .env(SANDBOX_IMAGE_ENV)
            .env(sandbox_thread_env(cpu))
            .run_commands(
                # yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14.
                # Override resolves the conflict (tested, yfinance works with 0.14+).
                "echo 'curl_cffi>=0.14' > /tmp/overrides.txt",
                "uv pip install --system --override /tmp/overrides.txt "
                + " ".join(dependencies)
                + " && rm /tmp/overrides.txt"
                + " && uv cache clean && rm -rf /root/.cache/pip",
                # Scrapling browser setup (Camoufox for StealthyFetcher). Its
                # Chromium is the one npx already placed in the shared
                # PLAYWRIGHT_BROWSERS_PATH, because both pins are the same version.
                "scrapling install || true",
                # A version drift between the two Playwright pins is invisible at
                # build time and doubles the browser payload, so fail here instead.
                'n=$(ls -d "'
                + browsers_path
                + '"/chromium-*/ 2>/dev/null | wc -l); [ "$n" -eq 1 ]',
            )
            .run_commands(
                'python -c "'
                "import matplotlib as mpl; "
                "mpl_dir = mpl.get_configdir(); "
                "import os; os.makedirs(mpl_dir, exist_ok=True); "
                "open(os.path.join(mpl_dir, 'matplotlibrc'), 'w').write("
                "'font.sans-serif: Noto Sans CJK SC, DejaVu Sans\\n'); "
                "import matplotlib.font_manager; "
                "matplotlib.font_manager._load_fontmanager(try_read_cache=False)"
                '"',
            )
            .workdir(self._working_dir)
        )

        logger.info(
            "Created snapshot image definition",
            python_version=self.SNAPSHOT_PYTHON_VERSION,
            dependencies=dependencies,
            mcp_packages=pkgs,
            thread_cpu=cpu,
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

        config_hash = self._get_snapshot_hash(mcp_packages, resources=resources)
        base_name = self._config.snapshot_name or "ptc-base"
        if resources is not None:
            snapshot_name = f"{base_name}-{tier}-{config_hash}"
        else:
            snapshot_name = f"{base_name}-{config_hash}"

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
                state = (
                    snapshot_obj.state.value
                    if hasattr(snapshot_obj.state, "value")
                    else str(snapshot_obj.state)
                )
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
                elif state == "building":
                    logger.info(
                        "Snapshot is still building, waiting...",
                        snapshot_name=snapshot_name,
                    )
                    # Wait for build to complete (poll up to 5 min)
                    build_resolved = False
                    for _ in range(60):
                        await asyncio.sleep(5)
                        try:
                            refreshed = await self._client.snapshot.list()
                            items = (
                                refreshed.items
                                if hasattr(refreshed, "items")
                                else refreshed
                            )
                            for s2 in items:
                                if hasattr(s2, "name") and s2.name == snapshot_name:
                                    s2_state = (
                                        s2.state.value
                                        if hasattr(s2.state, "value")
                                        else str(s2.state)
                                    )
                                    if s2_state == "active":
                                        logger.info("Snapshot build completed")
                                        snapshot_exists = True
                                        build_resolved = True
                                        break
                                    elif s2_state == "build_failed":
                                        logger.warning("Snapshot build failed")
                                        snapshot_exists = False
                                        build_resolved = True
                                        break
                        except Exception:
                            pass
                        if build_resolved:
                            break
                    else:
                        logger.warning("Snapshot build timed out")
                        snapshot_exists = False
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
            image = self._create_snapshot_image(mcp_packages, resources=resources)

            try:
                await self._client.snapshot.create(
                    CreateSnapshotParams(
                        name=snapshot_name, image=image, resources=resources
                    ),
                    on_logs=lambda log: logger.debug("Snapshot build", log=log),
                )
                logger.info(
                    "Snapshot created successfully",
                    snapshot_name=snapshot_name,
                )
                return snapshot_name
            except Exception as e:
                error_str = str(e)
                if "already exists" in error_str.lower():
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
