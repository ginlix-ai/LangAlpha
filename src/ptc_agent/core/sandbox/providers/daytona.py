"""Daytona sandbox provider — wraps the Daytona SDK."""

import asyncio
import hashlib
import json
from typing import Any

import structlog
from daytona_sdk import AsyncDaytona
from daytona_sdk import DaytonaConfig as SDKDaytonaConfig
from daytona_sdk import FileUpload
from daytona_sdk.common.daytona import (
    CreateSandboxFromSnapshotParams,
    Image,
)
from daytona_sdk.common.process import CodeRunParams, SessionExecuteRequest
from daytona_sdk.common.snapshot import CreateSnapshotParams

from ptc_agent.config.core import DaytonaConfig
from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES, SNAPSHOT_PYTHON_VERSION
from ptc_agent.core.sandbox.runtime import (
    Artifact,
    CodeRunResult,
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
    SessionCommandResult,
)

logger = structlog.get_logger(__name__)

# Mapping from Daytona SDK state strings to RuntimeState enum.
_STATE_MAP: dict[str, RuntimeState] = {
    "started": RuntimeState.RUNNING,
    "running": RuntimeState.RUNNING,
    "stopped": RuntimeState.STOPPED,
    "starting": RuntimeState.STARTING,
    "stopping": RuntimeState.STOPPING,
    "archived": RuntimeState.ARCHIVED,
    "error": RuntimeState.ERROR,
}


class DaytonaRuntime(SandboxRuntime):
    """Runtime that delegates to a Daytona SDK sandbox object."""

    def __init__(
        self,
        sdk_sandbox: Any,
        *,
        snapshot_name: str | None = None,
        default_working_dir: str = "/home/workspace",
    ) -> None:
        self._sandbox = sdk_sandbox
        self._working_dir: str | None = None
        self._default_working_dir = default_working_dir
        self.snapshot_name: str | None = snapshot_name

    # -- Properties --

    @property
    def id(self) -> str:
        return self._sandbox.id

    @property
    def proxy_domain(self) -> str | None:
        from urllib.parse import urlparse

        url = getattr(self._sandbox, "toolbox_proxy_url", None)
        if not url:
            return None
        return urlparse(url).hostname

    @property
    def working_dir(self) -> str:
        """Return cached working dir, or Daytona default if not yet fetched."""
        return self._working_dir or self._default_working_dir

    async def fetch_working_dir(self) -> str:
        """Fetch and cache the sandbox working directory (must be awaited).

        When a snapshot is in use, prefers the configured default_working_dir
        (/home/workspace) over the SDK result, which may return the Daytona
        user's home (/home/daytona).  Without a snapshot the SDK-reported
        directory is authoritative.
        """
        if self._working_dir is None:
            if self.snapshot_name and self._default_working_dir:
                self._working_dir = self._default_working_dir
            else:
                self._working_dir = await self._sandbox.get_work_dir()
        return self._working_dir

    # -- Lifecycle --

    async def start(self, timeout: int = 120) -> None:
        await self._sandbox.start(timeout=timeout)

    async def stop(self, timeout: int = 120) -> None:
        await self._sandbox.stop(timeout=timeout)

    async def delete(self) -> None:
        await self._sandbox.delete()

    async def get_state(self) -> RuntimeState:
        state = getattr(self._sandbox, "state", None)
        if state is None:
            return RuntimeState.ERROR
        state_value = state.value if hasattr(state, "value") else str(state)
        return _STATE_MAP.get(state_value, RuntimeState.ERROR)

    # -- Execution --

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        result = await self._sandbox.process.exec(command, timeout=timeout)
        # SDK exec returns an object with .result (combined stdout+stderr)
        # and .exit_code. There is no separate stderr field.
        stdout = ""
        if hasattr(result, "result"):
            stdout = result.result or ""
        elif hasattr(result, "output"):
            stdout = result.output or ""
        else:
            stdout = str(result) if result else ""
        exit_code = getattr(result, "exit_code", 0)
        return ExecResult(stdout=stdout, stderr="", exit_code=exit_code)

    async def code_run(
        self,
        code: str,
        env: dict[str, str] | None = None,
        timeout: int = 300,
    ) -> CodeRunResult:
        params = CodeRunParams(env=env or {})
        result = await self._sandbox.process.code_run(
            code, params=params, timeout=timeout
        )

        # Parse stdout
        if hasattr(result, "result"):
            stdout = result.result or ""
        elif hasattr(result, "stdout"):
            stdout = result.stdout or ""
        else:
            stdout = ""

        # Parse stderr
        if hasattr(result, "stderr"):
            stderr = result.stderr or ""
        elif hasattr(result, "artifacts") and hasattr(
            result.artifacts, "stderr"
        ):
            stderr = result.artifacts.stderr or ""
        else:
            stderr = ""

        exit_code = getattr(result, "exit_code", None)
        if exit_code is None:
            exit_code = 0

        # Parse chart artifacts
        artifacts: list[Artifact] = []
        if (
            hasattr(result, "artifacts")
            and result.artifacts
            and hasattr(result.artifacts, "charts")
            and result.artifacts.charts
        ):
            for chart in result.artifacts.charts:
                chart_type = (
                    chart.type.value
                    if hasattr(chart.type, "value")
                    else str(chart.type)
                )
                artifacts.append(
                    Artifact(
                        type=chart_type,
                        data=chart.png if hasattr(chart, "png") else "",
                        name=chart.title if hasattr(chart, "title") else None,
                    )
                )

        return CodeRunResult(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            artifacts=artifacts,
        )

    # -- File I/O --

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        await self._sandbox.fs.upload_file(content, dest_path)

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        batch = [
            FileUpload(source=src, destination=dst) for src, dst in files
        ]
        await self._sandbox.fs.upload_files(batch)

    async def download_file(self, path: str) -> bytes:
        return await self._sandbox.fs.download_file(path)

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        result = await self._sandbox.fs.list_files(directory)
        # SDK returns a list of file-info objects; normalize to dicts.
        if result and hasattr(result[0], "__dict__"):
            return [vars(f) for f in result]
        return result

    # -- Sessions (background processes) --

    async def create_session(self, session_id: str) -> None:
        await self._sandbox.process.create_session(session_id)

    async def session_execute(
        self,
        session_id: str,
        command: str,
        *,
        run_async: bool = False,
        timeout: int | None = None,
    ) -> SessionCommandResult:
        req = SessionExecuteRequest(command=command, run_async=run_async)
        result = await self._sandbox.process.execute_session_command(
            session_id, req, timeout=timeout
        )
        return SessionCommandResult(
            cmd_id=result.cmd_id,
            exit_code=result.exit_code,
            stdout=result.stdout or "",
            stderr=result.stderr or "",
        )


    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        cmd, logs = await asyncio.gather(
            self._sandbox.process.get_session_command(session_id, command_id),
            self._sandbox.process.get_session_command_logs(
                session_id, command_id
            ),
        )
        return SessionCommandResult(
            cmd_id=command_id,
            exit_code=cmd.exit_code,
            stdout=logs.stdout or "",
            stderr=logs.stderr or "",
        )

    async def delete_session(self, session_id: str) -> None:
        await self._sandbox.process.delete_session(session_id)

    # -- Preview URLs --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """Get a signed preview URL for a service running on the given port.

        Daytona returns the base URL and token separately. For iframe use the
        token must be embedded as a query parameter since iframes cannot set
        custom headers.
        """
        result = await self._sandbox.create_signed_preview_url(port, expires_in)
        url = result.url
        # Embed token in URL if not already present (required for iframe access)
        if result.token and "token=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}token={result.token}"
        return PreviewInfo(url=url, token=result.token)

    async def get_preview_link(self, port: int) -> PreviewInfo:
        """Get a standard (non-signed) preview URL with header-based auth token."""
        result = await self._sandbox.get_preview_link(port)
        return PreviewInfo(
            url=result.url,
            token=result.token,
            auth_headers={"X-Daytona-Preview-Token": result.token},
        )

    # -- Capabilities & metadata --

    @property
    def capabilities(self) -> set[str]:
        return {"exec", "code_run", "file_io", "archive", "snapshot", "preview_url", "sessions"}

    async def archive(self) -> None:
        await self._sandbox.archive()

    async def get_metadata(self) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "id": self.id,
            "working_dir": self.working_dir,
        }
        for attr in ("cpu", "memory", "disk", "gpu", "created_at", "auto_stop_interval"):
            val = getattr(self._sandbox, attr, None)
            if val is not None:
                meta[attr] = val
        state = getattr(self._sandbox, "state", None)
        if state is not None:
            meta["state"] = (
                state.value if hasattr(state, "value") else str(state)
            )
        return meta

    @property
    def raw(self) -> Any:
        """Access the underlying Daytona SDK sandbox object.

        Escape hatch for callers that need SDK-specific functionality
        not yet surfaced through the runtime interface.
        """
        return self._sandbox


class DaytonaProvider(SandboxProvider):
    """Provider that manages sandboxes via the Daytona SDK."""

    SNAPSHOT_PYTHON_VERSION = SNAPSHOT_PYTHON_VERSION
    DEFAULT_DEPENDENCIES = DEFAULT_DEPENDENCIES

    def __init__(self, config: DaytonaConfig, working_dir: str | None = None) -> None:
        self._config = config
        self._working_dir = working_dir or "/home/workspace"
        sdk_config = SDKDaytonaConfig(
            api_key=config.api_key, api_url=config.base_url
        )
        self._client = AsyncDaytona(sdk_config)

    # -- SandboxProvider interface --

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        mcp_packages: list[str] | None = None,
        **kwargs: Any,
    ) -> DaytonaRuntime:
        """Create a new Daytona sandbox, optionally from a snapshot.

        Args:
            env_vars: Environment variables injected at creation time.
            mcp_packages: NPM packages for MCP servers (needed for snapshot).
            **kwargs: Extra keyword arguments (reserved for future use).

        Returns:
            A DaytonaRuntime wrapping the new sandbox.
        """
        snapshot_name = await self._ensure_snapshot(
            mcp_packages=mcp_packages or []
        )

        params = CreateSandboxFromSnapshotParams(
            snapshot=snapshot_name if snapshot_name else None,
            env_vars=env_vars or None,
            auto_stop_interval=self._config.auto_stop_interval // 60,
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
            sdk_sandbox, default_working_dir=self._working_dir,
        )

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception as e:
            logger.debug("Failed to close Daytona client", error=str(e))

    def is_transient_error(self, exc: Exception) -> bool:
        """Classify whether *exc* is a transient Daytona SDK error."""
        message = str(exc).lower()

        # A closed HTTP client means the command never reached the server.
        # The SDK wraps these as "Failed to execute command: Session is closed"
        # so we must check BEFORE the execution-error guard.
        client_dead_markers = ("session is closed", "client is closed")
        if any(marker in message for marker in client_dead_markers):
            return True

        # Execution errors are not transient — the command ran and the server
        # responded. Don't let "timeout" in the server message trigger
        # transient handling.
        if message.startswith("failed to execute command"):
            return False
        transient_markers = (
            "remote end closed connection",
            "remotedisconnected",
            "connection aborted",
            "connection reset",
            "broken pipe",
            "timed out",
            "timeout",
            "service unavailable",
            "no ip address found",
            "400",
            "502",
            "503",
            "504",
        )
        return any(marker in message for marker in transient_markers)

    # -- Snapshot management --

    def _get_snapshot_hash(
        self, mcp_packages: list[str] | None = None
    ) -> str:
        """Generate an 8-char hash for snapshot versioning."""
        config_data = {
            "base_image": "ubuntu:24.04",
            "working_dir": self._working_dir,
            "python_version": self.SNAPSHOT_PYTHON_VERSION,
            "dependencies": self.DEFAULT_DEPENDENCIES,
            "mcp_packages": sorted(mcp_packages or []),
            "apt_packages": [
                "curl",
                "nodejs",
                "ripgrep",
                "uv",
                "jq",
                "git",
                "unzip",
                "libreoffice",
                "gcc",
                "poppler-utils",
                "pandoc",
                "qpdf",
                "fonts-noto-cjk",
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
        self, mcp_packages: list[str] | None = None
    ) -> Image:
        """Build the declarative Image definition for a snapshot."""
        dependencies = self.DEFAULT_DEPENDENCIES
        pkgs = mcp_packages or []

        base_image = Image.base("ubuntu:24.04").run_commands(
            "echo 'debconf debconf/frontend select Noninteractive'"
            " | debconf-set-selections",
            "apt-get update && apt-get install -y"
            " python3 python3-pip python3-venv"
            " gcc gfortran build-essential",
            "ln -sf /usr/bin/python3 /usr/bin/python",
            "ln -sf /usr/bin/pip3 /usr/bin/pip",
            "rm -f /usr/lib/python*/EXTERNALLY-MANAGED",
        )

        image = (
            base_image.run_commands(
                "apt-get update",
                "apt-get install -y curl ripgrep jq git unzip"
                " libreoffice gcc poppler-utils pandoc qpdf"
                " fonts-noto-cjk",
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
                "mv /root/.local/bin/uv /usr/local/bin/uv",
                "curl -fsSL https://deb.nodesource.com/setup_24.x | bash -",
                "apt-get install -y nodejs",
                *[f"npm install -g {pkg}" for pkg in pkgs],
                "npm install -g docx pptxgenjs",
                "curl -fsSL https://github.com/cli/cli/releases/download/"
                "v2.87.3/gh_2.87.3_linux_amd64.tar.gz -o /tmp/gh.tar.gz"
                " && tar -xzf /tmp/gh.tar.gz -C /tmp"
                " && mv /tmp/gh_2.87.3_linux_amd64/bin/gh /usr/local/bin/gh"
                " && rm -rf /tmp/gh.tar.gz /tmp/gh_2.87.3_linux_amd64",
                "curl -fsSL https://github.com/Polymarket/polymarket-cli/"
                "releases/download/v0.1.4/"
                "polymarket-v0.1.4-x86_64-unknown-linux-gnu.tar.gz"
                " -o /tmp/polymarket.tar.gz"
                " && tar -xzf /tmp/polymarket.tar.gz -C /tmp"
                " && mv /tmp/polymarket /usr/local/bin/polymarket"
                " && rm -rf /tmp/polymarket.tar.gz",
                "npm install -g playwright"
                " && PLAYWRIGHT_BROWSERS_PATH=/usr/local/ms-playwright"
                " npx playwright install --with-deps chromium",
                # -- Docker Engine (for interactive-dashboard complex tier) --
                "install -m 0755 -d /etc/apt/keyrings"
                " && curl -fsSL https://download.docker.com/linux/ubuntu/gpg"
                " -o /etc/apt/keyrings/docker.asc"
                " && chmod a+r /etc/apt/keyrings/docker.asc",
                'echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc]'
                " https://download.docker.com/linux/ubuntu"
                ' $(. /etc/os-release && echo $VERSION_CODENAME) stable"'
                " > /etc/apt/sources.list.d/docker.list",
                "apt-get update"
                " && apt-get install -y docker-ce docker-ce-cli containerd.io",
                "apt-get clean",
                "rm -rf /var/lib/apt/lists/*",
            )
            .run_commands(
                # yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14.
                # Override resolves the conflict (tested, yfinance works with 0.14+).
                "echo 'curl_cffi>=0.14' > /tmp/overrides.txt",
                "uv pip install --system --override /tmp/overrides.txt "
                + " ".join(dependencies),
                "rm /tmp/overrides.txt",
                # Scrapling browser setup (Camoufox for StealthyFetcher)
                "scrapling install || true",
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
        )
        return image

    async def _ensure_snapshot(
        self, mcp_packages: list[str] | None = None
    ) -> str | None:
        """Ensure a snapshot exists for the current configuration.

        Returns:
            Snapshot name if available, None otherwise.
        """
        if not self._config.snapshot_enabled:
            logger.debug("Snapshot feature disabled in config")
            return None

        config_hash = self._get_snapshot_hash(mcp_packages)
        base_name = self._config.snapshot_name or "ptc-base"
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
                    logger.warning(
                        f"Snapshot in unexpected state: {state}"
                    )
                    snapshot_exists = False
            else:
                snapshot_exists = False

        except Exception as e:
            logger.warning("Error listing snapshots", error=str(e))
            snapshot_exists = False

        # Create snapshot if it doesn't exist
        if not snapshot_exists and self._config.snapshot_auto_create:
            logger.info("Creating snapshot", snapshot_name=snapshot_name)
            image = self._create_snapshot_image(mcp_packages)

            try:
                await self._client.snapshot.create(
                    CreateSnapshotParams(name=snapshot_name, image=image),
                    on_logs=lambda log: logger.debug(
                        "Snapshot build", log=log
                    ),
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
            logger.info(
                "Using existing snapshot", snapshot_name=snapshot_name
            )
            return snapshot_name

        logger.warning("Snapshot not found and auto_create disabled")
        return None
