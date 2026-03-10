"""PTC Sandbox - Manages Daytona sandbox for Programmatic Tool Calling execution."""

import asyncio
import base64
import hashlib
import json
import shlex
import textwrap
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any

import structlog
from daytona_sdk import AsyncDaytona, DaytonaConfig, FileUpload
from daytona_sdk.common.daytona import (
    CreateSandboxFromSnapshotParams,
    Image,
)
from daytona_sdk.common.snapshot import CreateSnapshotParams

from ptc_agent.config.core import CoreConfig

from .mcp_registry import MCPRegistry
from .tool_generator import ToolFunctionGenerator

logger = structlog.get_logger(__name__)


class SandboxTransientError(RuntimeError):
    """Transient sandbox transport error.

    Raised when an operation fails due to transient transport issues and cannot be
    safely retried automatically.
    """


class _DaytonaRetryPolicy(Enum):
    SAFE = "safe"
    UNSAFE = "unsafe"


@dataclass
class ChartData:
    """Captured chart from matplotlib execution."""

    type: str
    title: str
    png_base64: str | None = None
    elements: list[Any] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of code execution in sandbox."""

    success: bool
    stdout: str
    stderr: str
    duration: float
    files_created: list[str]
    files_modified: list[str]
    execution_id: str
    code_hash: str
    charts: list[ChartData] = field(default_factory=list)


@dataclass
class SyncResult:
    """Result of a unified sandbox asset sync operation."""

    refreshed_modules: list[str]
    forced: bool


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_dict(d: dict[str, str]) -> str:
    """Deterministic SHA-256 hash of a string→string dict."""
    payload = "\n".join(f"{k}:{v}" for k, v in sorted(d.items()))
    return hashlib.sha256(payload.encode()).hexdigest()


def _resolve_local_path(local_path: str, config_dir: Path | None) -> str | None:
    """Resolve a relative file path, trying *config_dir* first, then CWD."""
    p = Path(local_path)
    if not p.is_absolute() and config_dir:
        candidate = (config_dir / local_path).resolve()
        if candidate.exists():
            return str(candidate)
    if p.exists():
        return str(p)
    return None


class PTCSandbox:
    """Manages Daytona sandbox for Programmatic Tool Calling (PTC) execution."""

    SNAPSHOT_PYTHON_VERSION = (
        "3.12"  # Intentionally pinned for stability/compatibility.
    )

    UNIFIED_MANIFEST_PATH = "/home/daytona/_internal/.sandbox_manifest.json"
    TOKEN_FRESHNESS_SECONDS = 25 * 60  # 25 min (access token TTL is 30 min)

    # Default Python dependencies installed in sandbox
    DEFAULT_DEPENDENCIES = [
        # Core
        "mcp",
        "fastmcp",
        "pandas",
        "requests",
        "aiohttp",
        "httpx[http2]",
        # Data science
        "numpy",
        "scipy",
        "scikit-learn",
        "statsmodels",
        # Financial data
        "yfinance",
        # Visualization
        "matplotlib",
        "seaborn",
        "plotly",
        # Image analysis
        "pillow",
        "opencv-python-headless",
        "scikit-image",
        # File formats
        "openpyxl",
        "xlrd",
        "python-docx",
        "pypdf",
        "beautifulsoup4",
        "lxml",
        "pyyaml",
        # Office skill dependencies
        "defusedxml",
        "pdfplumber",
        "reportlab",
        "markitdown[pptx]",
        # Utilities
        "tqdm",
        "tabulate",
    ]

    def __init__(
        self, config: CoreConfig, mcp_registry: MCPRegistry | None = None
    ) -> None:
        """Initialize PTC sandbox.

        Args:
            config: Configuration object
            mcp_registry: MCP registry with connected servers (can be None for reconnect)
        """
        self.config = config
        self.mcp_registry = mcp_registry

        # Initialize Daytona with proper config
        daytona_config = DaytonaConfig(
            api_key=config.daytona.api_key, api_url=config.daytona.base_url
        )
        self.daytona_client = AsyncDaytona(daytona_config)

        # External Daytona SDK sandbox object - Any type is required since it's from external SDK
        self.sandbox: Any | None = None
        self.sandbox_id: str | None = None
        self.tool_generator = ToolFunctionGenerator()
        self.execution_count = 0
        self.bash_execution_count = 0

        self._reconnect_lock = asyncio.Lock()
        self._tool_refresh_lock = asyncio.Lock()
        self._reconnect_inflight: asyncio.Future[None] | None = None

        # Track per-thread code dirs that have been created (avoids repeated mkdir)
        self._thread_dirs_created: set[str] = set()

        # Lazy initialization support
        self._ready_event: asyncio.Event | None = None
        self._init_task: asyncio.Task[None] | None = None
        self._init_error: Exception | None = None

        # Cached skills manifest (populated after sync_sandbox_assets)
        self._skills_manifest: dict[str, Any] | None = None

        # Track whether disabled tool modules have been pruned (only needed once)
        self._disabled_modules_pruned = False

        logger.info("Initialized PTCSandbox")

    async def _wait_ready(self) -> None:
        """Wait for sandbox to be ready. Call at start of methods needing sandbox."""
        if self._ready_event is None:
            # Not using lazy init - sandbox should already be ready
            if self.sandbox is None:
                raise RuntimeError("Sandbox not initialized")
            return

        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            raise RuntimeError("Sandbox initialization timed out after 300s")

        if self._init_error:
            raise RuntimeError(f"Sandbox initialization failed: {self._init_error}")

    def is_ready(self) -> bool:
        """Check if sandbox is ready without blocking.

        Returns:
            True if sandbox is ready for operations, False if still initializing.
        """
        if self._ready_event is None:
            # Not using lazy init - check if sandbox exists
            return self.sandbox is not None

        # Using lazy init - check if event is set and no error
        return self._ready_event.is_set() and self._init_error is None

    @property
    def skills_manifest(self) -> dict[str, Any] | None:
        """Cached skills manifest from the last ``sync_sandbox_assets`` call.

        Contains ``"version"``, ``"files"``, and ``"skills"`` (parsed metadata).
        Returns None if ``sync_sandbox_assets`` has not been called yet.
        """
        return self._skills_manifest

    def start_lazy_init(self, sandbox_id: str) -> None:
        """Start sandbox initialization in background (non-blocking).

        Call this instead of reconnect() for lazy initialization.
        Methods will automatically wait for init to complete.
        """
        if self._init_task is not None:
            return  # Already started

        self._ready_event = asyncio.Event()
        self._init_task = asyncio.create_task(self._lazy_reconnect(sandbox_id))

    async def _lazy_reconnect(self, sandbox_id: str) -> None:
        """Background task for lazy reconnection."""
        try:
            logger.info("Starting lazy sandbox init", sandbox_id=sandbox_id)
            await self.reconnect(sandbox_id)
            logger.info("Lazy sandbox init complete", sandbox_id=sandbox_id)
        except Exception as e:
            logger.error("Lazy sandbox init failed", error=str(e))
            self._init_error = e
        finally:
            if self._ready_event:
                self._ready_event.set()

    def _get_mcp_packages(self) -> list[str]:
        """Extract MCP package names from enabled stdio servers.

        Returns:
            List of MCP package names to install globally
        """
        mcp_packages = []
        for server in self.config.mcp.servers:
            if not server.enabled:
                continue
            if server.transport == "stdio" and server.command == "npx":
                # Extract package name from npx arguments
                # Format: ["npx", "-y", "package-name", ...]
                if len(server.args) >= 2 and server.args[0] == "-y":
                    mcp_packages.append(server.args[1])
        return mcp_packages

    def _normalize_search_path(self, path: str) -> str:
        """Normalize search path to absolute sandbox path.

        Converts relative/virtual paths to absolute paths for search operations.

        Args:
            path: Path to normalize (".", relative, or absolute)

        Returns:
            Absolute sandbox path
        """
        if path == ".":
            return self.config.filesystem.working_directory
        if not path.startswith("/"):
            return f"{self.config.filesystem.working_directory}/{path}"
        return path

    def _create_snapshot_image(self) -> Image:
        """Create image definition for snapshot with Node.js and MCP servers.

        Returns:
            Image definition with base dependencies and configuration
        """
        # Use class-level default dependencies
        dependencies = self.DEFAULT_DEPENDENCIES

        # Get MCP server npm packages from config (only enabled servers)
        mcp_packages = self._get_mcp_packages()

        # Build image using declarative builder
        # Note: Directories are created in _setup_workspace(), not in snapshot
        if self.config.daytona.python_version != self.SNAPSHOT_PYTHON_VERSION:
            logger.debug(
                "Ignoring configured python version for snapshots",
                configured=self.config.daytona.python_version,
                pinned=self.SNAPSHOT_PYTHON_VERSION,
            )
        base_image = Image.base("ubuntu:24.04").run_commands(
            "echo 'debconf debconf/frontend select Noninteractive'"
            " | debconf-set-selections",
            "apt-get update && apt-get install -y"
            " python3 python3-pip python3-venv"
            " gcc gfortran build-essential",
            # Symlink python/pip so bare commands work (Ubuntu only ships python3/pip3)
            "ln -sf /usr/bin/python3 /usr/bin/python",
            "ln -sf /usr/bin/pip3 /usr/bin/pip",
            # Remove PEP 668 marker so pip works freely (standard for containers)
            "rm -f /usr/lib/python*/EXTERNALLY-MANAGED",
        )

        image = (
            base_image.run_commands(
                # Install system dependencies including ripgrep for fast search
                "apt-get update",
                "apt-get install -y curl ripgrep jq git unzip"
                " libreoffice gcc poppler-utils pandoc qpdf"
                " fonts-noto-cjk",
                # Install uv for fast Python package management
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
                "mv /root/.local/bin/uv /usr/local/bin/uv",
                # Install Node.js 20.x LTS
                "curl -fsSL https://deb.nodesource.com/setup_20.x | bash -",
                "apt-get install -y nodejs",
                # Install MCP server packages globally
                *[f"npm install -g {pkg}" for pkg in mcp_packages],
                # Office skill npm dependencies (docx creation, pptx creation)
                "npm install -g docx pptxgenjs",
                # GitHub CLI (direct binary — apt repo unreliable in sandbox builds)
                "curl -fsSL https://github.com/cli/cli/releases/download/"
                "v2.87.3/gh_2.87.3_linux_amd64.tar.gz -o /tmp/gh.tar.gz"
                " && tar -xzf /tmp/gh.tar.gz -C /tmp"
                " && mv /tmp/gh_2.87.3_linux_amd64/bin/gh /usr/local/bin/gh"
                " && rm -rf /tmp/gh.tar.gz /tmp/gh_2.87.3_linux_amd64",
                # Polymarket CLI (direct binary)
                "curl -fsSL https://github.com/Polymarket/polymarket-cli/"
                "releases/download/v0.1.4/"
                "polymarket-v0.1.4-x86_64-unknown-linux-gnu.tar.gz"
                " -o /tmp/polymarket.tar.gz"
                " && tar -xzf /tmp/polymarket.tar.gz -C /tmp"
                " && mv /tmp/polymarket /usr/local/bin/polymarket"
                " && rm -rf /tmp/polymarket.tar.gz",
                # Playwright CLI with Chromium browser
                "npm install -g playwright"
                " && npx playwright install --with-deps chromium",
                # Clean up apt cache and matplotlib font cache
                "apt-get clean",
                "rm -rf /var/lib/apt/lists/*",
            )
            .pip_install(*dependencies)  # Unpack list as individual arguments
            .run_commands(
                # Set Noto Sans CJK as matplotlib fallback font and rebuild cache
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
            .workdir("/home/daytona")
        )

        logger.info(
            "Created snapshot image definition",
            python_version=self.SNAPSHOT_PYTHON_VERSION,
            dependencies=dependencies,
            mcp_packages=mcp_packages,
        )

        return image

    def _get_snapshot_hash(self) -> str:
        """Generate hash for snapshot versioning based on configuration.

        Returns:
            8-character hash of snapshot configuration
        """
        # Get MCP server npm packages from config (only enabled servers)
        mcp_packages = self._get_mcp_packages()

        # Include configuration that affects the snapshot in the hash
        config_data = {
            "base_image": "ubuntu:24.04",
            "python_version": self.SNAPSHOT_PYTHON_VERSION,
            "dependencies": self.DEFAULT_DEPENDENCIES,
            "mcp_packages": sorted(mcp_packages),  # Include MCP packages in hash
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
            ],  # Include apt/curl-installed packages in hash
        }

        config_str = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:8]

    async def _ensure_snapshot(self) -> str | None:
        """Ensure snapshot exists, create if needed.

        Returns:
            Snapshot name if available, None otherwise
        """
        if not self.config.daytona.snapshot_enabled:
            logger.debug("Snapshot feature disabled in config")
            return None

        # Generate versioned snapshot name with config hash
        config_hash = self._get_snapshot_hash()
        base_name = self.config.daytona.snapshot_name or "ptc-base"
        snapshot_name = f"{base_name}-{config_hash}"

        logger.info("Checking for snapshot", snapshot_name=snapshot_name)

        # Check if snapshot exists and is usable
        try:
            snapshots_result = await self._daytona_call(
                self.daytona_client.snapshot.list,
                retry_policy=_DaytonaRetryPolicy.SAFE,
                allow_reconnect=False,
            )
            snapshots = (
                snapshots_result.items
                if hasattr(snapshots_result, "items")
                else snapshots_result
            )

            # Only consider active or building snapshots as existing
            # Failed snapshots should be recreated
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
                    # Delete failed snapshot
                    try:
                        await self._daytona_call(
                            self.daytona_client.snapshot.delete,
                            snapshot_obj,
                            retry_policy=_DaytonaRetryPolicy.SAFE,
                            allow_reconnect=False,
                        )
                        logger.info(
                            "Deleted failed snapshot", snapshot_name=snapshot_name
                        )
                        # Give the deletion a moment to complete
                        await asyncio.sleep(2)
                    except OSError as del_err:
                        logger.warning(
                            "Could not delete failed snapshot", error=str(del_err)
                        )
                    snapshot_exists = False
                elif state in ["active", "building"]:
                    snapshot_exists = True
                else:
                    logger.warning(f"Snapshot in unexpected state: {state}")
                    snapshot_exists = False
            else:
                snapshot_exists = False

        except OSError as e:
            logger.warning("Error listing snapshots", error=str(e))
            snapshot_exists = False

        # Create snapshot if it doesn't exist
        if not snapshot_exists and self.config.daytona.snapshot_auto_create:
            logger.info("Creating snapshot", snapshot_name=snapshot_name)
            image = self._create_snapshot_image()

            try:
                await self._daytona_call(
                    self.daytona_client.snapshot.create,
                    CreateSnapshotParams(
                        name=snapshot_name,
                        image=image,
                    ),
                    on_logs=lambda log: logger.debug("Snapshot build", log=log),
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                    allow_reconnect=False,
                )
                logger.info(
                    "Snapshot created successfully", snapshot_name=snapshot_name
                )
                return snapshot_name
            except OSError as e:
                error_str = str(e)
                # Check if snapshot already exists (race condition or list failed)
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

    def _build_sandbox_env_vars(self) -> dict[str, str]:
        """Build environment variables to inject at sandbox creation time.

        Resolves MCP server env vars (${VAR} placeholders from host) and
        GitHub bot credentials so they're available to all sandbox processes.
        """
        import os

        env_vars: dict[str, str] = {}

        # MCP server env vars (resolve ${VAR} placeholders from host)
        for server in self.config.mcp.servers:
            if not server.enabled:
                continue
            if hasattr(server, "env") and server.env:
                for key, value in server.env.items():
                    if key == "INTERNAL_SERVICE_TOKEN":
                        continue  # Never inject platform token into sandbox
                    if value.startswith("${") and value.endswith("}"):
                        var_name = value[2:-1]
                        resolved_value = os.getenv(var_name)
                        if resolved_value:
                            env_vars[key] = resolved_value
                    else:
                        env_vars[key] = value

        # GitHub bot env vars
        from src.config.settings import get_nested_config

        if get_nested_config("github.enabled", False):
            token_env = get_nested_config("github.token_env", "GITHUB_BOT_TOKEN")
            token = os.getenv(token_env)
            if token:
                env_vars["GITHUB_TOKEN"] = token
                bot_name = get_nested_config("github.bot_name", "langalpha-bot")
                bot_email = get_nested_config("github.bot_email", "bot@ginlix.ai")
                env_vars["GIT_AUTHOR_NAME"] = bot_name
                env_vars["GIT_AUTHOR_EMAIL"] = bot_email
                env_vars["GIT_COMMITTER_NAME"] = bot_name
                env_vars["GIT_COMMITTER_EMAIL"] = bot_email

        return env_vars

    async def setup_sandbox_workspace(self) -> str | None:
        """Create sandbox and setup workspace directories.

        Can run concurrently with MCP registry connection since it doesn't
        require the registry.

        Returns:
            snapshot_name if used, None otherwise
        """
        logger.info("Setting up sandbox workspace")

        # Build env vars once — injected at sandbox creation time so they're
        # available to all processes (Python, bash, MCP servers)
        sandbox_env = self._build_sandbox_env_vars()

        # Try to use snapshot if enabled
        snapshot_name = await self._ensure_snapshot()

        if snapshot_name:
            # Create sandbox from snapshot (FAST!)
            logger.info("Creating sandbox from snapshot", snapshot_name=snapshot_name)
            try:
                self.sandbox = await self._daytona_call(
                    self.daytona_client.create,
                    CreateSandboxFromSnapshotParams(
                        snapshot=snapshot_name,
                        env_vars=sandbox_env or None,
                        auto_stop_interval=self.config.daytona.auto_stop_interval // 60,
                        auto_archive_interval=self.config.daytona.auto_archive_interval
                        // 60,
                        auto_delete_interval=self.config.daytona.auto_delete_interval
                        // 60,
                    ),
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                    allow_reconnect=False,
                )
                logger.info(
                    "Sandbox created from snapshot", snapshot_name=snapshot_name
                )
            except OSError as e:
                logger.warning(
                    "Failed to create from snapshot, falling back to default",
                    error=str(e),
                )
                snapshot_name = None

        if not snapshot_name:
            # Fallback to default creation
            logger.info("Creating sandbox from default image")
            self.sandbox = await self._daytona_call(
                self.daytona_client.create,
                CreateSandboxFromSnapshotParams(
                    env_vars=sandbox_env or None,
                    auto_stop_interval=self.config.daytona.auto_stop_interval // 60,
                    auto_archive_interval=self.config.daytona.auto_archive_interval
                    // 60,
                    auto_delete_interval=self.config.daytona.auto_delete_interval // 60,
                ),
                retry_policy=_DaytonaRetryPolicy.SAFE,
                allow_reconnect=False,
            )
            assert self.sandbox is not None

            sandbox = self.sandbox
            self.sandbox_id = self._extract_sandbox_id(sandbox)
            logger.info("Daytona sandbox created", sandbox_id=self.sandbox_id)

            # Set up workspace structure
            await self._setup_workspace()

            # Install dependencies
            await self._install_dependencies()
        else:
            # Snapshot-based creation
            assert self.sandbox is not None
            sandbox = self.sandbox
            self.sandbox_id = self._extract_sandbox_id(sandbox)
            logger.info(
                "Sandbox ready from snapshot",
                sandbox_id=self.sandbox_id,
                snapshot=snapshot_name,
            )
            # Ensure workspace directories exist (results, data, etc.)
            await self._setup_workspace()

        logger.info("Sandbox workspace ready", sandbox_id=self.sandbox_id)
        return snapshot_name

    async def setup_tools_and_mcp(
        self,
        snapshot_name: str | None,
        *,
        tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Install tool modules and start MCP servers.

        Requires MCP registry to be connected first.

        Args:
            snapshot_name: Snapshot name from setup_sandbox_workspace(), or None
            tokens: Pre-minted OAuth tokens (written to initial manifest).
            user_id: User ID for token tracking in manifest.
            workspace_id: Workspace ID for token tracking in manifest.
        """
        logger.info("Setting up tools and MCP servers")

        # Upload MCP server files, internal packages, and tokens in parallel (disjoint paths).
        # Tokens must be on disk before _start_internal_mcp_servers reads them.
        parallel = [
            self._upload_mcp_server_files_impl(),  # → mcp_servers/
            self._upload_internal_packages(),  # → _internal/src/
        ]
        if tokens:
            parallel.append(
                self.upload_token_file(tokens)
            )  # → _internal/.mcp_tokens.json
        await asyncio.gather(*parallel)

        # Generate and install tool modules after mcp_servers (intent: derived from MCP definitions)
        await self._install_tool_modules()

        # Start internal MCP servers (when using snapshot with Node.js)
        if snapshot_name:
            # Node.js and MCP packages are available in snapshot
            await self._start_internal_mcp_servers()
        else:
            logger.warning(
                "Skipping internal MCP servers - not using snapshot. "
                "MCP tools will not work without snapshot."
            )

        # Write initial unified manifest so subsequent syncs can diff against it
        try:
            manifest = await self._compute_sandbox_manifest(
                tokens=tokens, user_id=user_id, workspace_id=workspace_id
            )
            await self._write_unified_manifest(manifest)
        except Exception as e:
            logger.warning("Failed to write initial unified manifest", error=str(e))

        logger.info("Tools and MCP servers ready", sandbox_id=self.sandbox_id)

    TOKEN_FILE_PATH = "/home/daytona/_internal/.mcp_tokens.json"

    async def upload_token_file(self, tokens: dict) -> None:
        """Write scoped auth tokens to a file in the sandbox.

        Tokens are written as a JSON file (not env vars) because refresh tokens
        rotate on each use and the MCP server needs to update them in-place.
        Tokens carry deterministic prefixes (gxsa_, gxsr_) so the host-side
        LeakDetectionMiddleware can pattern-match them without knowing exact values.
        """
        import os

        if not tokens or not self.sandbox:
            return

        token_data = json.dumps(
            {
                "access_token": tokens["access_token"],
                "refresh_token": tokens["refresh_token"],
                "client_id": tokens["client_id"],
                "auth_service_url": os.getenv("AUTH_SERVICE_URL", ""),
                "ginlix_data_url": os.getenv("GINLIX_DATA_URL", ""),
            }
        )

        try:
            await self._daytona_call(
                self.sandbox.fs.upload_file,
                token_data.encode("utf-8"),
                self.TOKEN_FILE_PATH,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            logger.info("Uploaded sandbox token file", path=self.TOKEN_FILE_PATH)
        except Exception as e:
            logger.warning("Failed to upload sandbox token file", error=str(e))

    async def ensure_sandbox_ready(self) -> None:
        await self._wait_ready()

        work_dir = await self._daytona_call(
            self.sandbox.get_work_dir,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )
        self._work_dir = work_dir

    async def refresh_tools(self, **kwargs: Any) -> dict[str, Any]:
        """Force-rebuild all sandbox tool modules and packages.

        Delegates to :meth:`sync_sandbox_assets` with ``force_refresh=True``.
        Accepts the same keyword arguments as ``sync_sandbox_assets``.
        """
        kwargs.setdefault("force_refresh", True)
        kwargs.setdefault("reusing_sandbox", True)
        result = await self.sync_sandbox_assets(**kwargs)
        return {"success": True, "refreshed_modules": result.refreshed_modules}

    async def setup(self) -> None:
        """Set up the sandbox environment.

        For async initialization, use setup_sandbox_workspace() and
        setup_tools_and_mcp() separately via Session.initialize().
        """
        snapshot_name = await self.setup_sandbox_workspace()
        await self.setup_tools_and_mcp(snapshot_name)
        logger.info("Sandbox setup complete", sandbox_id=self.sandbox_id)

    async def reconnect(self, sandbox_id: str) -> None:
        """Reconnect to a stopped sandbox.

        This is a fast path for session persistence - it starts a stopped
        sandbox and skips all setup work (file uploads, tool modules, etc.)
        since they're already present from the first session.

        Args:
            sandbox_id: The ID of an existing Daytona sandbox

        Raises:
            RuntimeError: If sandbox cannot be found or is in invalid state
        """
        logger.info("Reconnecting to stopped sandbox", sandbox_id=sandbox_id)

        # Get the existing sandbox from Daytona with error handling
        try:
            self.sandbox = await self._daytona_call(
                self.daytona_client.get,
                sandbox_id,
                retry_policy=_DaytonaRetryPolicy.SAFE,
                allow_reconnect=False,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to find sandbox {sandbox_id}. It may have been deleted. "
                f"Original error: {e}"
            ) from e

        assert self.sandbox is not None
        sandbox = self.sandbox
        self.sandbox_id = sandbox_id

        # Check sandbox state before attempting to start
        state = getattr(sandbox, "state", None)
        if state:
            state_value = state.value if hasattr(state, "value") else str(state)
            if state_value == "started":
                logger.info(
                    "Sandbox already started, skipping start", sandbox_id=sandbox_id
                )
            elif state_value == "stopped":
                logger.info(
                    "Starting stopped sandbox", sandbox_id=sandbox_id, state=state_value
                )
                await self._daytona_call(
                    sandbox.start,
                    timeout=60,
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
            elif state_value == "starting":
                # Sandbox is already transitioning — wait for it to reach 'started'.
                logger.info(
                    "Sandbox is starting, waiting for ready",
                    sandbox_id=sandbox_id,
                )
                for _ in range(40):  # Max ~20 seconds
                    await asyncio.sleep(0.5)
                    sandbox = await self._daytona_call(
                        self.daytona_client.get,
                        sandbox_id,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                        allow_reconnect=False,
                    )
                    state = getattr(sandbox, "state", None)
                    state_value = state.value if hasattr(state, "value") else str(state)
                    if state_value == "started":
                        break
                self.sandbox = sandbox
                if state_value != "started":
                    raise RuntimeError(
                        f"Sandbox still in state '{state_value}' after waiting. "
                        f"Expected 'started'."
                    )
            elif state_value == "stopping":
                # Wait for sandbox to finish stopping, then start it.
                # Re-fetch from Daytona API each iteration (cached object is stale).
                logger.info(
                    "Sandbox is stopping, waiting before start",
                    sandbox_id=sandbox_id,
                )
                for _ in range(20):  # Max ~10 seconds
                    await asyncio.sleep(0.5)
                    sandbox = await self._daytona_call(
                        self.daytona_client.get,
                        sandbox_id,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                        allow_reconnect=False,
                    )
                    state = getattr(sandbox, "state", None)
                    state_value = state.value if hasattr(state, "value") else str(state)
                    if state_value == "stopped":
                        break
                # Update to fresh object
                self.sandbox = sandbox
                if state_value == "stopped":
                    logger.info(
                        "Sandbox finished stopping, starting it",
                        sandbox_id=sandbox_id,
                    )
                    await self._daytona_call(
                        sandbox.start,
                        timeout=60,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
                else:
                    raise RuntimeError(
                        f"Sandbox still in state '{state_value}' after waiting. "
                        f"Expected 'stopped'."
                    )
            elif state_value == "archived":
                logger.info(
                    "Starting archived sandbox (restore may take longer)",
                    sandbox_id=sandbox_id,
                )
                await self._daytona_call(
                    sandbox.start,
                    timeout=300,
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
            elif state_value == "archiving":
                # Wait for sandbox to finish archiving, then start it.
                logger.info(
                    "Sandbox is archiving, waiting before start",
                    sandbox_id=sandbox_id,
                )
                for _ in range(60):  # Max ~30 seconds
                    await asyncio.sleep(0.5)
                    sandbox = await self._daytona_call(
                        self.daytona_client.get,
                        sandbox_id,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                        allow_reconnect=False,
                    )
                    state = getattr(sandbox, "state", None)
                    state_value = state.value if hasattr(state, "value") else str(state)
                    if state_value == "archived":
                        break
                self.sandbox = sandbox
                if state_value == "archived":
                    logger.info(
                        "Sandbox finished archiving, starting it",
                        sandbox_id=sandbox_id,
                    )
                    await self._daytona_call(
                        sandbox.start,
                        timeout=300,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
                else:
                    raise RuntimeError(
                        f"Sandbox still in state '{state_value}' after waiting. "
                        f"Expected 'archived'."
                    )
            elif state_value == "error":
                # Sandbox hit an internal error — attempt recovery via start().
                logger.warning(
                    "Sandbox in error state, attempting recovery start",
                    sandbox_id=sandbox_id,
                )
                await self._daytona_call(
                    sandbox.start,
                    timeout=120,
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
            else:
                raise RuntimeError(
                    f"Cannot reconnect to sandbox in state: {state_value}. "
                    f"Expected 'stopped', 'started', or 'error'."
                )
        else:
            # No state attribute, assume we need to start
            logger.info("Starting sandbox (state unknown)", sandbox_id=sandbox_id)
            await self._daytona_call(
                sandbox.start,
                timeout=60,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

        # Get work directory reference
        self._work_dir = await self._daytona_call(
            sandbox.get_work_dir,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )
        logger.info(f"Sandbox working directory: {self._work_dir}")

        # SKIP: _setup_workspace() - directories already exist
        # SKIP: _upload_mcp_server_files() - files already uploaded
        # SKIP: _install_tool_modules() - tool modules already installed

        # Initialize MCP server sessions (needed for tool execution)
        self.mcp_server_sessions: dict[str, Any] = {}
        await self._start_internal_mcp_servers()

        logger.info(
            "Sandbox started from stopped state",
            sandbox_id=self.sandbox_id,
        )

    async def stop_sandbox(self) -> None:
        """Stop the sandbox without deleting it.

        Used for session persistence - stops the sandbox so it can be
        restarted quickly on the next session, rather than deleting it.
        """
        if not self.sandbox:
            return

        # Check state before stopping to avoid errors when already stopped
        try:
            state = getattr(self.sandbox, "state", None)
            if state:
                state_value = state.value if hasattr(state, "value") else str(state)
                if state_value == "stopped":
                    logger.info("Sandbox already stopped", sandbox_id=self.sandbox_id)
                    return
        except Exception as e:
            # If state check fails, log and continue with stop attempt
            logger.debug("Could not check sandbox state", error=str(e))

        try:
            logger.info("Stopping sandbox", sandbox_id=self.sandbox_id)
            await self._daytona_call(
                self.sandbox.stop,
                timeout=60,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            logger.info("Sandbox stopped", sandbox_id=self.sandbox_id)
        except Exception as e:
            # Log warning but don't raise - sandbox may already be stopped or unavailable
            logger.warning(
                "Failed to stop sandbox",
                sandbox_id=self.sandbox_id,
                error=str(e),
            )

    async def _setup_workspace(self) -> None:
        """Create workspace directory structure."""
        logger.info("Setting up workspace structure")

        # Get the working directory
        assert self.sandbox is not None
        work_dir = await self._daytona_call(
            self.sandbox.get_work_dir,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )
        logger.info(f"Sandbox working directory: {work_dir}")

        # Store work_dir for use by other methods
        self._work_dir = work_dir

        # Use absolute paths to ensure directories are created correctly
        directories = [
            f"{work_dir}/tools",
            f"{work_dir}/tools/docs",
            f"{work_dir}/results",
            f"{work_dir}/data",
            f"{work_dir}/code",
            f"{work_dir}/work",
            f"{work_dir}/.agent/threads",
            f"{work_dir}/_internal/src",
        ]

        # Create all directories in parallel for faster setup
        async def create_directory(directory: str) -> None:
            try:
                assert self.sandbox is not None
                await self._daytona_call(
                    self.sandbox.process.exec,
                    f"mkdir -p {directory}",
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
                logger.info(f"Created directory: {directory}")
            except OSError as e:
                logger.warning(f"Error creating directory {directory}: {e}")

        await asyncio.gather(*[create_directory(d) for d in directories])

    async def _upload_internal_packages(self) -> None:
        """Upload internal Python packages for sandbox execution.

        Currently uploads the `src.data_client` package so code executed inside the
        sandbox can import `src.data_client` without depending on the full repo.
        """
        work_dir = getattr(self, "_work_dir", "/home/daytona")
        internal_root = Path(f"{work_dir}/_internal/src")

        # Resolve local paths relative to config file directory if available.
        config_dir = getattr(self.config, "config_file_dir", None)
        repo_root = config_dir or Path.cwd()

        local_src_dir = (repo_root / "src").resolve()
        local_src_init = local_src_dir / "__init__.py"
        local_data_client_dir = (local_src_dir / "data_client").resolve()

        if not local_src_init.exists() or not local_data_client_dir.exists():
            logger.warning(
                "Skipping internal package upload - local src/data_client not found",
                src_init=str(local_src_init),
                data_client_dir=str(local_data_client_dir),
            )
            return

        assert self.sandbox is not None
        sandbox = self.sandbox

        files: list[tuple[Path, Path]] = []
        files.append((local_src_init, Path("__init__.py")))
        for file_path in local_data_client_dir.rglob("*.py"):
            if "__pycache__" in file_path.parts:
                continue
            rel = file_path.relative_to(local_src_dir)
            files.append((file_path, rel))

        # Collect unique parent dirs → single mkdir command
        parent_dirs = {str(Path(str(internal_root / rel)).parent) for _, rel in files}
        mkdir_cmd = "mkdir -p " + " ".join(shlex.quote(d) for d in sorted(parent_dirs))
        await self._daytona_call(
            sandbox.process.exec,
            mkdir_cmd,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )

        # Batch upload — FileUpload(source=str) streams from local path
        batch = [
            FileUpload(
                source=str(local_path),
                destination=str(internal_root / rel_path),
            )
            for local_path, rel_path in files
        ]
        await self._daytona_call(
            sandbox.fs.upload_files,
            batch,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )
        logger.info(
            "Uploaded internal packages to sandbox",
            uploaded_files=len(files),
            sandbox_root=str(internal_root),
        )

    # ── Unified manifest helpers ────────────────────────────────────────

    def _compute_tool_schema_hash(self) -> str:
        """Hash the current MCP tool schemas from the live registry.

        Captures tool names + input schemas so that adding/removing/modifying
        a tool on a running MCP server is detected even if the .py file is unchanged.
        """
        if not self.mcp_registry:
            return ""
        all_tools = self.mcp_registry.get_all_tools()
        parts: list[str] = []
        for server_name in sorted(all_tools):
            for tool in sorted(all_tools[server_name], key=lambda t: t.name):
                parts.append(
                    f"{server_name}:{tool.name}:{json.dumps(tool.input_schema, sort_keys=True)}"
                )
        return hashlib.sha256("\n".join(parts).encode()).hexdigest()

    async def _compute_skills_module(self, skill_roots: list[str]) -> dict[str, Any]:
        """Compute a skills module manifest with content-based SHA-256 hashing.

        Unlike the legacy ``_compute_skills_manifest`` (size+mtime), this hashes
        actual file contents so the manifest is deterministic and portable.
        """

        def build() -> dict[str, Any]:
            from ptc_agent.agent.middleware.skills import (
                get_sandbox_skill_names,
                SKILL_REGISTRY,
            )
            from ptc_agent.agent.middleware.skills.discovery import (
                parse_skill_metadata,
            )

            sandbox_skill_names = get_sandbox_skill_names()
            all_registry_names = set(SKILL_REGISTRY.keys())

            files: dict[str, str] = {}  # rel_path → sha256
            skills_metadata: dict[str, dict[str, Any]] = {}
            seen_skill_names: set[str] = set()

            for root_str in skill_roots:
                root = Path(root_str).expanduser()
                if not root.exists():
                    continue

                for skill_dir in root.iterdir():
                    if not skill_dir.is_dir():
                        continue
                    if not (skill_dir / "SKILL.md").exists():
                        continue

                    skill_name = skill_dir.name
                    # Skip flash-only skills (not needed in sandbox)
                    if (
                        skill_name not in sandbox_skill_names
                        and skill_name in all_registry_names
                    ):
                        continue

                    # Later sources override earlier ones
                    if skill_name in seen_skill_names:
                        prefix = f"{skill_name}/"
                        files = {
                            k: v for k, v in files.items() if not k.startswith(prefix)
                        }
                    seen_skill_names.add(skill_name)

                    for fp in skill_dir.rglob("*"):
                        if not fp.is_file():
                            continue
                        if "__pycache__" in fp.parts or fp.name == "LICENSE.txt":
                            continue
                        rel = f"{skill_name}/{fp.relative_to(skill_dir)}"
                        files[rel] = _sha256_file(fp)

                    # Parse SKILL.md frontmatter
                    try:
                        content = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
                    except (OSError, UnicodeDecodeError):
                        continue
                    sandbox_path = f"/home/daytona/skills/{skill_name}/SKILL.md"
                    meta = parse_skill_metadata(content, sandbox_path, skill_name)
                    skills_metadata[skill_name] = dict(meta)

            version = _hash_dict(files)
            return {"version": version, "files": files, "skills": skills_metadata}

        return await asyncio.to_thread(build)

    async def _compute_sandbox_manifest(
        self,
        *,
        skill_roots: list[str] | None = None,
        tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> dict[str, Any]:
        """Compute the unified local manifest for all sandbox asset modules."""
        modules: dict[str, Any] = {}
        config_dir = getattr(self.config, "config_file_dir", None)

        # ── Module: mcp_servers ──
        mcp_files: dict[str, str] = {}  # filename → sha256
        for server in self.config.mcp.servers:
            if not server.enabled:
                continue
            if server.transport != "stdio" or server.command != "uv":
                continue
            if (
                len(server.args) < 3
                or server.args[0] != "run"
                or server.args[1] != "python"
            ):
                continue
            resolved = _resolve_local_path(server.args[2], config_dir)
            if resolved:
                mcp_files[Path(resolved).name] = _sha256_file(Path(resolved))
        mcp_version = _hash_dict(mcp_files)
        modules["mcp_servers"] = {"version": mcp_version, "files": mcp_files}

        # ── Module: data_client ──
        dc_files: dict[str, str] = {}
        repo_root = config_dir or Path.cwd()
        src_dir = (repo_root / "src").resolve()
        src_init = src_dir / "__init__.py"
        dc_dir = (src_dir / "data_client").resolve()
        if src_init.exists() and dc_dir.exists():
            dc_files["__init__.py"] = _sha256_file(src_init)
            for fp in dc_dir.rglob("*.py"):
                if "__pycache__" in fp.parts:
                    continue
                rel = str(fp.relative_to(src_dir))
                dc_files[rel] = _sha256_file(fp)
        dc_version = _hash_dict(dc_files)
        modules["data_client"] = {"version": dc_version, "files": dc_files}

        # ── Module: tool_modules (derived) ──
        tool_schema_hash = self._compute_tool_schema_hash()
        source_versions = {
            "mcp_servers": mcp_version,
            "tool_schemas": tool_schema_hash,
        }
        tm_version = _hash_dict(source_versions)
        modules["tool_modules"] = {
            "version": tm_version,
            "source_versions": source_versions,
        }

        # ── Module: skills ──
        if skill_roots:
            modules["skills"] = await self._compute_skills_module(skill_roots)

        # ── Module: tokens ──
        if tokens:
            # Version captures the config identity; freshness is checked via minted_at.
            token_config_parts = {
                "user_id": user_id or "",
                "workspace_id": workspace_id or "",
                "client_id": tokens.get("client_id", ""),
            }
            modules["tokens"] = {
                "version": _hash_dict(token_config_parts),
                "minted_at": time.time(),
                "user_id": user_id or "",
                "workspace_id": workspace_id or "",
            }

        return {"schema_version": 1, "modules": modules}

    # ── Unified manifest I/O ─────────────────────────────────────────

    async def _read_unified_manifest(self) -> dict[str, Any] | None:
        """Read the unified manifest from the sandbox.

        Uses the Daytona SDK directly (bypasses path validation for
        ``_internal/``).  Returns None if missing, corrupt, or wrong
        ``schema_version`` (triggers full refresh in the caller).
        """
        assert self.sandbox is not None
        try:
            raw = await self._daytona_call(
                self.sandbox.fs.download_file,
                self.UNIFIED_MANIFEST_PATH,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            if raw:
                text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                parsed = json.loads(text)
                if isinstance(parsed, dict) and parsed.get("schema_version") == 1:
                    return parsed
        except Exception:
            pass  # Missing file, decode error, or JSON error → full refresh
        return None

    async def _write_unified_manifest(self, manifest: dict[str, Any]) -> None:
        """Write the unified manifest directly via Daytona SDK.

        Bypasses path validation since ``_internal/`` is a protected directory
        that the agent cannot access, but the system needs to write to.
        """
        assert self.sandbox is not None
        await self._daytona_call(
            self.sandbox.fs.upload_file,
            json.dumps(manifest, sort_keys=True).encode("utf-8"),
            self.UNIFIED_MANIFEST_PATH,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )

    async def _cleanup_legacy_manifests(self) -> None:
        """Remove old per-module manifest files after migration to unified manifest."""
        work_dir = getattr(self, "_work_dir", "/home/daytona")
        legacy_paths = [
            f"{work_dir}/mcp_servers/.mcp_manifest.json",
            f"{work_dir}/skills/.skills_manifest.json",
        ]
        assert self.sandbox is not None
        try:
            rm_cmd = "rm -f " + " ".join(shlex.quote(p) for p in legacy_paths)
            await self._daytona_call(
                self.sandbox.process.exec,
                rm_cmd,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
        except Exception:
            pass  # Best-effort cleanup

    async def _upload_mcp_server_files_impl(self) -> None:
        """Upload MCP server .py files to sandbox (pure upload, no manifest check)."""
        work_dir = getattr(self, "_work_dir", "/home/daytona")
        mcp_servers_dir = f"{work_dir}/mcp_servers"
        config_dir = getattr(self.config, "config_file_dir", None)

        files_to_upload: list[tuple[str, str, str]] = []
        expected_files: set[str] = set()

        for server in self.config.mcp.servers:
            if not server.enabled:
                continue
            if server.transport == "stdio" and server.command == "uv":
                if (
                    len(server.args) >= 3
                    and server.args[0] == "run"
                    and server.args[1] == "python"
                ):
                    resolved = _resolve_local_path(server.args[2], config_dir)
                    if resolved:
                        filename = Path(resolved).name
                        sandbox_path = f"{mcp_servers_dir}/{filename}"
                        expected_files.add(filename)
                        files_to_upload.append((server.name, resolved, sandbox_path))
                    else:
                        searched = [server.args[2]]
                        if config_dir:
                            searched.append(str(config_dir / server.args[2]))
                        logger.warning(
                            f"MCP server file not found: {server.args[2]}",
                            server=server.name,
                            searched_paths=searched,
                        )

        assert self.sandbox is not None
        sandbox = self.sandbox

        await self._daytona_call(
            sandbox.process.exec,
            f"mkdir -p {mcp_servers_dir}",
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )

        # Prune stale files — single rm command instead of N
        existing_entries = await self.als_directory(mcp_servers_dir)
        if existing_entries:
            files_to_remove = [
                entry["path"]
                for entry in existing_entries
                if not entry.get("is_dir", False)
                and entry.get("name") not in expected_files
                and entry.get("name")
                not in (".mcp_manifest.json", ".sandbox_manifest.json")
            ]
            if files_to_remove:
                rm_cmd = "rm -f " + " ".join(shlex.quote(p) for p in files_to_remove)
                await self._daytona_call(
                    sandbox.process.exec,
                    rm_cmd,
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
                logger.info(
                    "Pruned MCP server files",
                    removed=len(files_to_remove),
                    sandbox_root=mcp_servers_dir,
                )

        # Batch upload — single HTTP request via upload_files
        if files_to_upload:
            batch = [
                FileUpload(source=local, destination=remote)
                for _, local, remote in files_to_upload
            ]
            await self._daytona_call(
                sandbox.fs.upload_files,
                batch,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            for name, local, remote in files_to_upload:
                logger.info(
                    "Uploaded MCP server file",
                    server=name,
                    local_path=local,
                    sandbox_path=remote,
                )

    # ── Unified sync entry point ─────────────────────────────────────

    async def sync_sandbox_assets(
        self,
        *,
        skill_dirs: list[tuple[str, str]] | None = None,
        reusing_sandbox: bool = False,
        force_refresh: bool = False,
        tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
        on_progress: Callable[[str], None] | None = None,
    ) -> SyncResult:
        """Sync all sandbox assets using a single unified manifest.

        Replaces the previous ``sync_tools()`` and ``sync_skills()`` methods
        with a single entry point that tracks MCP servers, data client, tool
        modules, skills, and tokens in one manifest file.

        Args:
            skill_dirs: Ordered list of (local_path, sandbox_path) for skills.
            reusing_sandbox: Whether reconnecting to an existing sandbox.
            force_refresh: Force re-upload of all modules regardless of manifest.
            tokens: Pre-minted OAuth tokens (from workspace_manager).
            user_id: User ID for token tracking.
            workspace_id: Workspace ID for token tracking.
            on_progress: Optional callback for reporting progress.

        Returns:
            SyncResult with list of refreshed module names.
        """
        await self._wait_ready()

        async with self._tool_refresh_lock:
            await self.ensure_sandbox_ready()

            # Steps 0+1+2: all three are independent — parallelize
            # _prune_disabled_tool_modules → sandbox rm (disjoint from manifest paths)
            # _compute_sandbox_manifest → local CPU/disk only
            # _read_unified_manifest → sandbox HTTP GET
            skill_roots = [d for d, _ in skill_dirs] if skill_dirs else None
            _, local_manifest, remote_manifest = await asyncio.gather(
                self._prune_disabled_tool_modules(),
                self._compute_sandbox_manifest(
                    skill_roots=skill_roots,
                    tokens=tokens,
                    user_id=user_id,
                    workspace_id=workspace_id,
                ),
                self._read_unified_manifest(),
            )

            # 3. Determine which modules changed (pure CPU)
            if force_refresh or remote_manifest is None or not reusing_sandbox:
                changed_modules = set(local_manifest["modules"].keys())
            else:
                changed_modules: set[str] = set()
                for mod_name, mod_data in local_manifest["modules"].items():
                    remote_mod = remote_manifest.get("modules", {}).get(mod_name)
                    if mod_name == "tokens":
                        if self._token_needs_refresh(
                            remote_mod, tokens, user_id, workspace_id
                        ):
                            changed_modules.add("tokens")
                    elif (
                        remote_mod is None
                        or remote_mod.get("version") != mod_data["version"]
                    ):
                        changed_modules.add(mod_name)

            if not changed_modules:
                if "skills" in local_manifest["modules"]:
                    self._skills_manifest = local_manifest["modules"]["skills"]
                return SyncResult(refreshed_modules=[], forced=False)

            refreshed: list[str] = []

            # 4. Upload changed modules
            # Intent-based ordering: tool_modules after mcp_servers (derived from
            # MCP definitions). All other modules write to disjoint sandbox paths
            # and are safe to run in parallel.

            async def _do_skills_upload() -> None:
                """Skills sub-chain: collect → prune → upload (internally sequential)."""
                local_skill_names = await self._collect_local_skill_names(
                    [d for d, _ in skill_dirs]  # type: ignore[union-attr]
                )
                sandbox_base = skill_dirs[-1][1].rstrip("/")  # type: ignore[index]
                await self._prune_remote_skills(sandbox_base, local_skill_names)
                skills_mod = local_manifest["modules"].get("skills", {})
                if skills_mod.get("files"):
                    await self._upload_skills(
                        skill_dirs,
                        manifest=skills_mod,  # type: ignore[arg-type]
                    )

            # Group 1: independent uploads in parallel
            parallel_uploads: list[tuple[str, Any]] = []
            if "mcp_servers" in changed_modules:
                if on_progress:
                    on_progress("Syncing MCP server files...")
                parallel_uploads.append(
                    ("mcp_servers", self._upload_mcp_server_files_impl())
                )
            if "data_client" in changed_modules:
                if on_progress:
                    on_progress("Syncing data client...")
                parallel_uploads.append(
                    ("data_client", self._upload_internal_packages())
                )
            if "skills" in changed_modules and skill_dirs:
                if on_progress:
                    on_progress("Syncing skills...")
                parallel_uploads.append(("skills", _do_skills_upload()))
            if "tokens" in changed_modules and tokens:
                if on_progress:
                    on_progress("Uploading tokens...")
                parallel_uploads.append(("tokens", self.upload_token_file(tokens)))

            if parallel_uploads:
                await asyncio.gather(*[coro for _, coro in parallel_uploads])
                refreshed.extend(name for name, _ in parallel_uploads)

            # Group 2: tool_modules AFTER mcp_servers (intent: derived from MCP definitions)
            if "tool_modules" in changed_modules:
                if on_progress:
                    on_progress("Regenerating tool modules...")
                await self._install_tool_modules()
                refreshed.append("tool_modules")
                try:
                    await self._start_internal_mcp_servers()
                except Exception as e:
                    logger.warning("Failed to refresh MCP servers", error=str(e))

            # Cache skills metadata
            if "skills" in local_manifest["modules"]:
                self._skills_manifest = local_manifest["modules"]["skills"]

            # Steps 5+6: independent — parallelize
            await asyncio.gather(
                self._write_unified_manifest(local_manifest),
                self._cleanup_legacy_manifests(),
            )

            logger.info(
                "Sandbox asset sync complete",
                refreshed=refreshed,
                forced=force_refresh,
            )
            return SyncResult(refreshed_modules=refreshed, forced=force_refresh)

    @staticmethod
    def _token_needs_refresh(
        remote_token_mod: dict[str, Any] | None,
        tokens: dict | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> bool:
        """Check whether tokens need to be re-uploaded based on freshness."""
        if not tokens:
            return False
        if remote_token_mod is None:
            return True
        # Re-mint if user or workspace changed
        if remote_token_mod.get("user_id") != (user_id or ""):
            return True
        if remote_token_mod.get("workspace_id") != (workspace_id or ""):
            return True
        # Re-mint if older than freshness threshold
        minted_at = remote_token_mod.get("minted_at", 0)
        age = time.time() - minted_at
        if age > PTCSandbox.TOKEN_FRESHNESS_SECONDS:
            return True
        return False

    async def _prune_disabled_tool_modules(self) -> None:
        if not self.sandbox or self._disabled_modules_pruned:
            return

        sandbox = self.sandbox
        disabled = [
            server.name for server in self.config.mcp.servers if not server.enabled
        ]
        if not disabled:
            self._disabled_modules_pruned = True
            return

        work_dir = getattr(self, "_work_dir", "/home/daytona")
        paths: list[str] = []
        for name in disabled:
            paths.append(f"{work_dir}/tools/{name}.py")
            paths.append(f"{work_dir}/tools/docs/{name}")

        async def remove_one(path: str) -> None:
            await self._daytona_call(
                sandbox.process.exec,
                f"rm -rf {shlex.quote(path)}",
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

        await asyncio.gather(*[remove_one(path) for path in paths])
        self._disabled_modules_pruned = True
        logger.debug("Pruned disabled tool modules", removed=len(paths))

    SKILLS_MANIFEST_FILENAME = (
        ".skills_manifest.json"  # Legacy; used by _prune_remote_skills
    )

    def _is_transient_daytona_error(self, e: Exception) -> bool:
        message = str(e).lower()
        # Execution errors are not transient — the command ran and the server
        # responded. Don't let "timeout" in the server message trigger
        # transient handling, which would produce a misleading
        # "Sandbox disconnected" error.
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

    @staticmethod
    def _classify_execution_error(
        e: Exception,
        duration: float,
        timeout_limit: float,
        timeout_message: str,
    ) -> tuple[bool, str, str]:
        """Classify a sandbox execution exception as timeout or generic error.

        Returns:
            (is_timeout, error_detail, stderr_msg)
        """
        error_detail = f"{type(e).__name__}: {e!s}" if str(e) else type(e).__name__
        error_lower = str(e).lower()
        is_timeout = (
            duration >= timeout_limit * 0.95
            or "timed out" in error_lower
            or "timeout" in error_lower
        )
        if is_timeout:
            stderr_msg = timeout_message
        else:
            stderr_msg = f"Sandbox execution error: {error_detail}"
        return is_timeout, error_detail, stderr_msg

    async def _ensure_sandbox_connected(self) -> None:
        if self.sandbox_id is None:
            raise SandboxTransientError(
                "Sandbox disconnected and no sandbox_id is available"
            )

        # Coalesce concurrent reconnect attempts.
        async with self._reconnect_lock:
            if (
                self._reconnect_inflight is not None
                and not self._reconnect_inflight.done()
            ):
                await self._reconnect_inflight
                return

            loop = asyncio.get_running_loop()
            self._reconnect_inflight = loop.create_future()
            inflight = self._reconnect_inflight

            try:
                await self.reconnect(self.sandbox_id)
                inflight.set_result(None)
            except Exception as e:
                inflight.set_exception(e)
                raise
            finally:
                self._reconnect_inflight = None

    async def _daytona_call(
        self,
        func: Callable[..., Any],
        *args: Any,
        retry_policy: _DaytonaRetryPolicy,
        allow_reconnect: bool = True,
        retries: int = 5,
        initial_delay_s: float = 0.25,
        total_timeout: float = 120.0,
        **kwargs: Any,
    ) -> Any:
        deadline = time.monotonic() + total_timeout
        delay_s = initial_delay_s
        reconnected = False

        for attempt in range(1, retries + 1):
            if time.monotonic() > deadline:
                raise SandboxTransientError(
                    f"Daytona call timed out after {total_timeout}s"
                )

            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if not self._is_transient_daytona_error(e):
                    raise

                if allow_reconnect and not reconnected:
                    try:
                        await self._ensure_sandbox_connected()
                        reconnected = True
                    except Exception as reconnect_error:
                        logger.debug(
                            "Reconnect attempt failed during retry",
                            error=str(reconnect_error),
                        )

                if retry_policy == _DaytonaRetryPolicy.UNSAFE:
                    logger.warning(
                        "Sandbox disconnected during unsafe operation; not retrying automatically",
                        func=getattr(func, "__name__", str(func)),
                        attempt=attempt,
                        error=str(e),
                    )
                    message = (
                        "Sandbox disconnected during command execution; sandbox reconnected. Please retry."
                        if reconnected
                        else "Sandbox disconnected during command execution; please retry after recovery."
                    )
                    raise SandboxTransientError(message) from e

                if attempt == retries:
                    raise SandboxTransientError(
                        "Transient sandbox transport error; operation failed after retries"
                    ) from e

                logger.debug(
                    "Retrying Daytona SDK call after transient error",
                    func=getattr(func, "__name__", str(func)),
                    attempt=attempt,
                    error=str(e),
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SandboxTransientError(
                        f"Daytona call timed out after {total_timeout}s"
                    ) from e
                await asyncio.sleep(min(delay_s, remaining))
                delay_s *= 2

        raise SandboxTransientError("Transient sandbox transport error")

    async def _collect_local_skill_names(
        self, local_skill_roots: list[str]
    ) -> set[str]:
        def build() -> set[str]:
            from ptc_agent.agent.middleware.skills import (
                get_sandbox_skill_names,
                SKILL_REGISTRY,
            )

            sandbox_skill_names = get_sandbox_skill_names()
            all_registry_names = set(SKILL_REGISTRY.keys())

            names: set[str] = set()
            for root_str in local_skill_roots:
                root = Path(root_str).expanduser()
                if not root.exists():
                    continue
                for skill_dir in root.iterdir():
                    if not skill_dir.is_dir():
                        continue
                    if not (skill_dir / "SKILL.md").exists():
                        continue
                    skill_name = skill_dir.name
                    # Skip flash-only skills so they get pruned from sandbox
                    if (
                        skill_name not in sandbox_skill_names
                        and skill_name in all_registry_names
                    ):
                        continue
                    names.add(skill_name)
            return names

        return await asyncio.to_thread(build)

    async def _prune_remote_skills(
        self, sandbox_base: str, local_skill_names: set[str]
    ) -> None:
        assert self.sandbox is not None
        sandbox = self.sandbox
        entries = await self.als_directory(sandbox_base)
        if not entries:
            return

        paths_to_remove: list[str] = []
        for entry in entries:
            name = entry.get("name")
            if not name:
                continue
            if name == self.SKILLS_MANIFEST_FILENAME:
                if not local_skill_names:
                    paths_to_remove.append(entry["path"])
                continue
            if entry.get("is_dir", False):
                if name not in local_skill_names:
                    paths_to_remove.append(entry["path"])

        if not paths_to_remove:
            return

        async def remove_one(path: str) -> None:
            await self._daytona_call(
                sandbox.process.exec,
                f"rm -rf {shlex.quote(path)}",
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

        await asyncio.gather(*[remove_one(path) for path in paths_to_remove])
        logger.info(
            "Pruned skills from sandbox",
            removed=len(paths_to_remove),
            sandbox_root=sandbox_base,
        )

    async def _upload_skills(
        self,
        local_skills_dirs: list[tuple[str, str]],
        *,
        manifest: dict[str, Any] | None = None,
    ) -> None:
        """Upload skill files from local filesystem to sandbox.

        Uses a two-pass approach to fix override precedence:
        - Pass 1 (local I/O only): Walk all sources, later sources overwrite earlier
          ones for the same skill_name — each skill appears exactly once.
        - Pass 2 (sandbox I/O): Single rm, single mkdir, parallel per-skill batch uploads.

        Args:
            local_skills_dirs: List of (local_path, sandbox_path) tuples.
                Example: [("~/.ptc-agent/skills", "/home/daytona/skills")]
            manifest: Pre-computed skills manifest. If None, computed from local_skills_dirs.
        """
        assert self.sandbox is not None
        sandbox = self.sandbox

        if manifest is None:
            local_roots = [local_dir for local_dir, _ in local_skills_dirs]
            manifest = await self._compute_skills_module(local_roots)

        if not manifest.get("files"):
            logger.debug("No skills found; skipping upload")
            return

        # Skills eligible for sandbox upload (exposure "ptc" or "both")
        from ptc_agent.agent.middleware.skills import (
            get_sandbox_skill_names,
            SKILL_REGISTRY,
        )

        sandbox_skill_names = get_sandbox_skill_names()
        all_registry_names = set(SKILL_REGISTRY.keys())

        # ── Pass 1: Planning (local I/O only) ──
        # For each skill, collect files from the *last* source that provides it.
        # Key: skill_name → (sandbox_skill_dir, list of (local_file, sandbox_dest))
        @dataclass
        class _SkillPlan:
            sandbox_dir: str
            files: list[tuple[Path, str]] = field(default_factory=list)
            subdirs: set[str] = field(default_factory=set)

        final_skills: dict[str, _SkillPlan] = {}

        def _list_skill_dirs(local_root: Path) -> list[Path]:
            dirs: list[Path] = []
            for entry in local_root.iterdir():
                if not entry.is_dir():
                    continue
                if not (entry / "SKILL.md").exists():
                    continue
                dirs.append(entry)
            return dirs

        def _list_skill_files(skill_dir: Path) -> list[Path]:
            return [
                p
                for p in skill_dir.rglob("*")
                if p.is_file()
                and "__pycache__" not in p.parts
                and p.name != "LICENSE.txt"
            ]

        def _plan_all() -> None:
            for local_dir, sandbox_dir in local_skills_dirs:
                local_path = Path(local_dir).expanduser()
                if not local_path.exists():
                    continue

                for skill_dir in _list_skill_dirs(local_path):
                    skill_name = skill_dir.name
                    if skill_name in ("", ".", ".."):
                        continue
                    if (
                        skill_name not in sandbox_skill_names
                        and skill_name in all_registry_names
                    ):
                        continue

                    sandbox_skill_dir = f"{sandbox_dir.rstrip('/')}/{skill_name}"
                    plan = _SkillPlan(sandbox_dir=sandbox_skill_dir)

                    for fp in _list_skill_files(skill_dir):
                        rel = fp.relative_to(skill_dir)
                        dest = f"{sandbox_skill_dir}/{rel}"
                        plan.files.append((fp, dest))
                        if len(rel.parts) > 1:
                            plan.subdirs.add(f"{sandbox_skill_dir}/{rel.parent}")

                    # Later source overwrites earlier for same skill_name
                    final_skills[skill_name] = plan

        await asyncio.to_thread(_plan_all)

        if not final_skills:
            logger.debug("No skills to upload after planning")
            return

        # ── Pass 2: Execute (minimal sandbox I/O) ──
        # 1. Single rm for clean slate (all skill dirs that will be uploaded)
        rm_targets = [plan.sandbox_dir for plan in final_skills.values()]
        if rm_targets:
            rm_cmd = "rm -rf " + " ".join(shlex.quote(d) for d in rm_targets)
            await self._daytona_call(
                sandbox.process.exec,
                rm_cmd,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

        # 2. Single mkdir for all skill dirs + subdirs
        mkdir_targets: set[str] = set()
        for plan in final_skills.values():
            mkdir_targets.add(plan.sandbox_dir)
            mkdir_targets.update(plan.subdirs)
        if mkdir_targets:
            mkdir_cmd = "mkdir -p " + " ".join(
                shlex.quote(d) for d in sorted(mkdir_targets)
            )
            await self._daytona_call(
                sandbox.process.exec,
                mkdir_cmd,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

        # 3. Parallel per-skill batch uploads — no race since planning collapsed duplicates
        upload_coros = []
        for plan in final_skills.values():
            if plan.files:
                batch = [
                    FileUpload(source=str(fp), destination=dest)
                    for fp, dest in plan.files
                ]
                upload_coros.append(
                    self._daytona_call(
                        sandbox.fs.upload_files,
                        batch,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
                )
        if upload_coros:
            await asyncio.gather(*upload_coros)

        logger.info(
            "Uploaded skills to sandbox",
            skill_count=len(final_skills),
            file_count=len(manifest.get("files", {})),
        )

    async def _install_dependencies(self) -> None:
        """Install required Python packages in sandbox."""
        logger.info("Installing dependencies")

        dependencies = [
            "mcp",
            "pandas",
            "requests",
            "aiohttp",
            "httpx[http2]",
        ]

        install_cmd = f"uv pip install -q {' '.join(dependencies)}"

        try:
            assert self.sandbox is not None
            _result = await self._daytona_call(
                self.sandbox.process.exec,
                install_cmd,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            logger.info("Dependencies installed")
        except OSError as e:
            logger.error(f"Failed to install dependencies: {e}")
            raise

    async def _install_tool_modules(self) -> None:
        """Generate and install tool modules from MCP servers."""
        logger.info("Installing tool modules")

        # Get work directory (set by _setup_workspace)
        work_dir = getattr(self, "_work_dir", "/home/daytona")

        # Collect all files to upload (content generation is CPU-bound, fast)
        uploads: list[tuple[bytes, str, tuple[str, dict[str, str]] | None]] = []

        # 1. MCP client module
        enabled_servers = [
            server for server in self.config.mcp.servers if server.enabled
        ]
        mcp_client_code = self.tool_generator.generate_mcp_client_code(enabled_servers)
        mcp_client_path = f"{work_dir}/tools/mcp_client.py"
        uploads.append(
            (
                mcp_client_code.encode("utf-8"),
                mcp_client_path,
                ("MCP client module installed", {"path": mcp_client_path}),
            )
        )

        # 2. Tool modules and documentation
        assert self.mcp_registry is not None
        tools_by_server = self.mcp_registry.get_all_tools()

        assert self.sandbox is not None

        # Prune stale doc dirs (best-effort)
        docs_root = f"{work_dir}/tools/docs"
        try:
            existing = await self.als_directory(docs_root)
            if existing:
                stale = [
                    entry["path"]
                    for entry in existing
                    if entry.get("is_dir") and entry.get("name") not in tools_by_server
                ]
                if stale:
                    rm_cmd = "rm -rf " + " ".join(shlex.quote(p) for p in stale)
                    await self._daytona_call(
                        self.sandbox.process.exec,
                        rm_cmd,
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
        except Exception:
            pass  # docs dir may not exist yet on fresh sandbox

        for server_name, tools in tools_by_server.items():
            # Generate Python module
            module_code = self.tool_generator.generate_tool_module(server_name, tools)
            module_path = f"{work_dir}/tools/{server_name}.py"
            uploads.append(
                (
                    module_code.encode("utf-8"),
                    module_path,
                    (
                        "Tool module installed",
                        {
                            "server": server_name,
                            "path": module_path,
                            "tool_count": str(len(tools)),
                        },
                    ),
                )
            )

            # Generate documentation for each tool
            for tool in tools:
                doc = self.tool_generator.generate_tool_documentation(tool)
                doc_path = f"{work_dir}/tools/docs/{server_name}/{tool.name}.md"
                upload_item: tuple[bytes, str, tuple[str, dict[str, str]] | None] = (
                    doc.encode("utf-8"),
                    doc_path,
                    None,
                )
                uploads.append(upload_item)

        # 3. __init__.py for tools package
        init_content = '"""Auto-generated tool modules from MCP servers."""\n'
        init_path = f"{work_dir}/tools/__init__.py"
        init_item: tuple[bytes, str, tuple[str, dict[str, str]] | None] = (
            init_content.encode("utf-8"),
            init_path,
            None,
        )
        uploads.append(init_item)

        # Batch mkdir — all dirs in one command
        all_dirs = [f"{work_dir}/tools"] + [
            f"{work_dir}/tools/docs/{name}" for name in tools_by_server
        ]
        mkdir_cmd = "mkdir -p " + " ".join(shlex.quote(d) for d in all_dirs)
        await self._daytona_call(
            self.sandbox.process.exec,
            mkdir_cmd,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )

        # Batch upload — single HTTP request for all generated content
        batch = [
            FileUpload(source=content, destination=path) for content, path, _ in uploads
        ]
        await self._daytona_call(
            self.sandbox.fs.upload_files,
            batch,
            retry_policy=_DaytonaRetryPolicy.SAFE,
        )
        # Log after batch
        for _, _, log_info in uploads:
            if log_info:
                msg, kwargs = log_info
                logger.info(msg, **kwargs)

        logger.info("Tool modules installation complete")

    async def _start_internal_mcp_servers(self) -> None:
        """Start MCP servers as background processes inside sandbox."""
        logger.info("Starting internal MCP servers")

        # Track server sessions for lifecycle management
        self.mcp_server_sessions = {}

        for server in self.config.mcp.servers:
            if not server.enabled:
                continue
            if server.transport != "stdio":
                logger.warning(
                    f"Skipping non-stdio server {server.name}",
                    transport=server.transport,
                )
                continue

            try:
                # Build the command to start the MCP server
                if server.command == "npx":
                    # npx -y package-name [args...]
                    cmd_parts = [server.command, *server.args]
                    cmd = " ".join(cmd_parts)
                else:
                    # Custom command
                    cmd = f"{server.command} {' '.join(server.args)}"

                # Add environment variables if specified
                env_vars = []
                if hasattr(server, "env") and server.env:
                    for key, value in server.env.items():
                        # Environment variables might have ${VAR} syntax, resolve them
                        # For now, we'll pass them as-is and they'll need to be set in sandbox
                        env_vars.append(f"{key}={value}")

                # Create PTY session for the MCP server
                session_name = f"mcp-{server.name}"

                logger.info(
                    "Creating MCP server session",
                    server=server.name,
                    session=session_name,
                    command=cmd,
                )

                # Create session (but don't start the server yet, we'll do that when needed)
                # For now, just track that this server should be available
                self.mcp_server_sessions[server.name] = {
                    "session_name": session_name,
                    "command": cmd,
                    "env": env_vars,
                    "started": False,
                }

                logger.info(
                    "MCP server session configured",
                    server=server.name,
                    session=session_name,
                )

            except OSError as e:
                logger.error(
                    "Failed to configure MCP server session",
                    server=server.name,
                    error=str(e),
                )

        logger.info(
            "Internal MCP server configuration complete",
            servers=list(self.mcp_server_sessions.keys()),
        )

    def _detect_missing_imports(self, stderr: str) -> list[str]:
        """Extract missing module names from ImportError/ModuleNotFoundError.

        Args:
            stderr: Standard error output from code execution

        Returns:
            List of missing package names (base package only, e.g., 'foo' from 'foo.bar')
        """
        import re

        patterns = [
            r"ModuleNotFoundError: No module named ['\"]([^'\"]+)['\"]",
            r"ImportError: No module named ['\"]([^'\"]+)['\"]",
        ]

        matches = []
        for pattern in patterns:
            matches.extend(re.findall(pattern, stderr))

        # Handle submodule imports (e.g., "foo.bar" -> "foo")
        # Also deduplicate
        base_packages = list({m.split(".")[0] for m in matches})

        if base_packages:
            logger.info(
                "Detected missing imports",
                packages=base_packages,
            )

        return base_packages

    async def _install_package(self, package: str) -> bool:
        """Install a Python package in the sandbox.

        Args:
            package: Package name to install

        Returns:
            True if installation succeeded, False otherwise
        """
        try:
            logger.info(f"Auto-installing missing package: {package}")
            assert self.sandbox is not None
            result = await self._daytona_call(
                self.sandbox.process.exec,
                f"uv pip install -q {package}",
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            exit_code = getattr(result, "exit_code", 1)
            if exit_code == 0:
                logger.info(f"Successfully installed package: {package}")
                return True
            logger.warning(
                f"Failed to install package: {package}, exit_code={exit_code}"
            )
            return False
        except OSError as e:
            logger.warning(f"Failed to install {package}: {e}")
            return False

    async def execute(
        self,
        code: str,
        timeout: int | None = None,
        *,
        auto_install: bool = True,
        max_retries: int = 2,
        thread_id: str | None = None,
    ) -> ExecutionResult:
        """Execute Python code in the sandbox with optional auto-install for missing dependencies.

        Args:
            code: Python code to execute
            timeout: Optional timeout in seconds
            auto_install: Whether to automatically install missing packages on ImportError (default: True)
            max_retries: Maximum number of retries after auto-installing packages (default: 2)
            thread_id: Optional thread ID (first 8 chars) for thread-scoped code storage

        Returns:
            ExecutionResult with execution details
        """
        await self._wait_ready()

        self.execution_count += 1
        execution_id = f"exec_{self.execution_count:04d}"
        code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]

        logger.info(
            "Executing code",
            execution_id=execution_id,
            code_hash=code_hash,
            code_length=len(code),
            auto_install=auto_install,
            thread_id=thread_id,
        )

        timeout_val = timeout or self.config.security.max_execution_time
        start_time = time.time()

        try:
            # Write code to thread dir or fallback to code/
            if thread_id:
                code_path = f".agent/threads/{thread_id}/code/{execution_id}.py"
                # Ensure per-thread code dir exists (lazy, once per thread)
                if thread_id not in self._thread_dirs_created:
                    await self._daytona_call(
                        self.sandbox.process.exec,
                        f"mkdir -p {self.normalize_path(f'.agent/threads/{thread_id}/code')}",
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
                    self._thread_dirs_created.add(thread_id)
            else:
                code_path = f"code/{execution_id}.py"
            try:
                await self._daytona_call(
                    self.sandbox.fs.upload_file,
                    code.encode("utf-8"),
                    self.normalize_path(code_path),
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
            except Exception as upload_err:
                logger.warning(
                    "Failed to save code file to sandbox (non-fatal)",
                    code_path=code_path,
                    error=str(upload_err),
                )

            # Get list of files before execution
            files_before = await self._list_result_files()

            # Execute code
            # Set PYTHONPATH so code can import from tools/ and _internal/
            # MCP + GitHub env vars are injected at sandbox creation time
            work_dir = await self._daytona_call(
                self.sandbox.get_work_dir,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

            internal_dir = f"{work_dir}/_internal"
            exec_env = {"PYTHONPATH": f"{work_dir}:{internal_dir}/src:{internal_dir}"}

            # Use code_run() for native artifact support (captures matplotlib charts)
            from daytona_sdk.common.process import CodeRunParams

            result = await self._daytona_call(
                self.sandbox.process.code_run,
                code,
                params=CodeRunParams(env=exec_env),
                timeout=timeout_val,
                retry_policy=_DaytonaRetryPolicy.UNSAFE,
                total_timeout=timeout_val + 30,
            )

            # Get stdout/stderr and exit code from Daytona ExecuteResponse
            # The result object has: exit_code, result (stdout), artifacts
            if hasattr(result, "result"):
                # Daytona SDK ExecuteResponse.result contains the stdout
                stdout = result.result or ""
            elif hasattr(result, "stdout"):
                stdout = result.stdout or ""
            else:
                stdout = ""

            # Get stderr - check multiple possible locations
            if hasattr(result, "stderr"):
                stderr = result.stderr or ""
            elif hasattr(result, "artifacts") and hasattr(result.artifacts, "stderr"):
                stderr = result.artifacts.stderr or ""
            else:
                stderr = ""

            exit_code = getattr(result, "exit_code", 1)

            # Determine success based on exit code
            success = exit_code == 0

            # Extract charts from artifacts (matplotlib captures)
            charts = []
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
                    charts.append(
                        ChartData(
                            type=chart_type,
                            title=chart.title if hasattr(chart, "title") else "",
                            png_base64=chart.png if hasattr(chart, "png") else None,
                            elements=chart.elements
                            if hasattr(chart, "elements")
                            else [],
                        )
                    )
                logger.info(f"Captured {len(charts)} chart(s) from artifacts")

            # Get files after execution
            files_after = await self._list_result_files()

            # Determine file changes
            files_created = [f for f in files_after if f not in files_before]
            files_modified: list[str] = []  # TODO: Implement modification tracking

            duration = time.time() - start_time

            execution_result = ExecutionResult(
                success=success,
                stdout=stdout,
                stderr=stderr,
                duration=duration,
                files_created=files_created,
                files_modified=files_modified,
                execution_id=execution_id,
                code_hash=code_hash,
                charts=charts,
            )

            # Auto-install missing packages and retry if enabled
            if not success and auto_install and max_retries > 0:
                missing_packages = self._detect_missing_imports(stderr)
                if missing_packages:
                    logger.info(
                        "Attempting auto-install and retry",
                        execution_id=execution_id,
                        missing_packages=missing_packages,
                        retries_remaining=max_retries,
                    )

                    # Install missing packages
                    for package in missing_packages:
                        await self._install_package(package)

                    # Retry execution with decremented retry count
                    return await self.execute(
                        code=code,
                        timeout=timeout,
                        auto_install=auto_install,
                        max_retries=max_retries - 1,
                        thread_id=thread_id,
                    )

            logger.info(
                "Code execution completed",
                execution_id=execution_id,
                success=success,
                duration=duration,
                files_created=len(files_created),
                charts_captured=len(charts),
            )

            return execution_result

        except Exception as e:
            duration = time.time() - start_time
            is_timeout, error_detail, stderr_msg = self._classify_execution_error(
                e,
                duration,
                timeout_val,
                f"Execution timed out after {duration:.0f}s (limit: {timeout_val}s). "
                "The script was killed before completion — no output was captured. "
                "Split into smaller steps or optimize the script to run faster.",
            )

            logger.error(
                "Code execution failed",
                execution_id=execution_id,
                error=error_detail,
                duration=duration,
                is_timeout=is_timeout,
            )

            return ExecutionResult(
                success=False,
                stdout="",
                stderr=stderr_msg,
                duration=duration,
                files_created=[],
                files_modified=[],
                execution_id=execution_id,
                code_hash=code_hash,
                charts=[],
            )

    async def execute_bash_command(
        self,
        command: str,
        working_dir: str = "/home/daytona",
        timeout: int = 60,
        *,
        background: bool = False,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a bash command in the sandbox.

        Args:
            command: Bash command to execute
            working_dir: Working directory for command execution (default: /home/daytona)
            timeout: Maximum execution time in seconds (default: 60)
            background: Run command in background (not fully implemented yet)
            thread_id: Optional thread ID (first 8 chars) for thread-scoped script storage

        Returns:
            Dictionary with success, stdout, stderr, exit_code, bash_id, command_hash
        """
        await self._wait_ready()
        start_time = time.time()

        try:
            # Generate bash execution ID for tracking
            self.bash_execution_count += 1
            bash_id = f"bash_{self.bash_execution_count:04d}"
            command_hash = hashlib.sha256(command.encode()).hexdigest()[:16]
            from datetime import UTC, datetime

            timestamp = datetime.now(tz=UTC).isoformat()

            logger.info(
                "Executing bash command",
                bash_id=bash_id,
                command_hash=command_hash,
                command=command[:100],
                working_dir=working_dir,
            )

            # Build the full bash command with working directory
            # Use cd to change directory, then execute command
            full_command = f"cd {working_dir} && {command}"

            # Audit: save .sh script for traceability (non-fatal)
            script_content = textwrap.dedent(f"""\
                #!/bin/bash
                # Bash Execution Log
                # ID: {bash_id}
                # Working Directory: {working_dir}
                # Timestamp: {timestamp}
                # Command Hash: {command_hash}

                set -e
                {full_command}
            """)

            if thread_id:
                script_relative_path = f".agent/threads/{thread_id}/code/{bash_id}.sh"
                if thread_id not in self._thread_dirs_created:
                    await self._daytona_call(
                        self.sandbox.process.exec,
                        f"mkdir -p {self.normalize_path(f'.agent/threads/{thread_id}/code')}",
                        retry_policy=_DaytonaRetryPolicy.SAFE,
                    )
                    self._thread_dirs_created.add(thread_id)
            else:
                script_relative_path = f"code/{bash_id}.sh"

            try:
                assert self.sandbox is not None
                await self._daytona_call(
                    self.sandbox.fs.upload_file,
                    script_content.encode("utf-8"),
                    self.normalize_path(script_relative_path),
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
            except Exception as upload_err:
                logger.warning(
                    "Failed to save bash script to sandbox (non-fatal)",
                    bash_id=bash_id,
                    error=str(upload_err),
                )

            # Execute directly via process.exec — no file upload dependency
            assert self.sandbox is not None
            exec_result = await self._daytona_call(
                self.sandbox.process.exec,
                full_command,
                timeout=timeout,
                retry_policy=_DaytonaRetryPolicy.UNSAFE,
                total_timeout=timeout + 30,
            )

            exit_code = (
                exec_result.exit_code if hasattr(exec_result, "exit_code") else 0
            )
            stdout = (
                (exec_result.result or "")
                if hasattr(exec_result, "result")
                else str(exec_result)
            )

            if exit_code == 0:
                return {
                    "success": True,
                    "stdout": stdout,
                    "stderr": "",
                    "exit_code": 0,
                    "bash_id": bash_id,
                    "command_hash": command_hash,
                }

            return {
                "success": False,
                "stdout": stdout,
                "stderr": "",  # Daytona process.exec returns combined output only
                "exit_code": exit_code,
                "bash_id": bash_id,
                "command_hash": command_hash,
            }

        except Exception as e:
            duration = time.time() - start_time
            is_timeout, error_detail, stderr_msg = self._classify_execution_error(
                e,
                duration,
                timeout,
                f"Command timed out after {duration:.0f}s (limit: {timeout}s). "
                "The command was killed before completion. "
                "Split into smaller steps or increase the timeout.",
            )

            logger.error(
                f"Failed to execute bash command: {e}",
                exc_info=True,
                extra={"is_timeout": is_timeout},
            )
            return {
                "success": False,
                "stdout": "",
                "stderr": stderr_msg,
                "exit_code": -1,
                "bash_id": getattr(self, "_last_bash_id", None),
                "command_hash": None,
            }

    async def _list_result_files(self) -> list[str]:
        """List files in the results directory.

        Returns:
            List of file paths relative to workspace (e.g., "results/file.csv")
        """
        try:
            assert self.sandbox is not None
            file_infos = await self._daytona_call(
                self.sandbox.fs.list_files,
                "results",
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            if not file_infos:
                return []
            # Return paths relative to workspace, not just filenames
            return [
                f"results/{str(f.name) if hasattr(f, 'name') else str(f)}"
                for f in file_infos
            ]
        except (OSError, AttributeError) as e:
            logger.warning(f"Error listing result files: {e}")
            return []

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        """Download raw bytes from sandbox.

        This path is safe to retry automatically.

        Returns:
            Bytes if downloaded, or None if missing.

        Raises:
            SandboxTransientError: If a transient sandbox transport error persists.
        """
        await self._wait_ready()

        try:
            return await self._daytona_call(
                self.sandbox.fs.download_file,
                filepath,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
        except SandboxTransientError:
            raise
        except Exception as e:
            logger.debug(
                "Failed to download file bytes", filepath=filepath, error=str(e)
            )
            return None

    async def aread_file_text(self, filepath: str) -> str | None:
        """Read a UTF-8 text file from the sandbox.

        This path is safe to retry automatically.
        """
        content_bytes = await self.adownload_file_bytes(filepath)
        if not content_bytes:
            return None
        try:
            return content_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            logger.debug(
                "Failed to decode file as utf-8", filepath=filepath, error=str(e)
            )
            return None

    async def aupload_file_bytes(self, filepath: str, content: bytes) -> bool:
        """Upload raw bytes to the sandbox.

        This path is safe to retry automatically because uploads overwrite the target.

        Raises:
            SandboxTransientError: If a transient sandbox transport error persists.
        """
        await self._wait_ready()

        # Normalize the path to ensure it's in the correct format for Daytona SDK
        normalized_path = self.normalize_path(filepath)

        if self.config.filesystem.enable_path_validation and not self.validate_path(
            normalized_path
        ):
            logger.error(f"Access denied: {filepath} is not in allowed directories")
            return False

        try:
            assert self.sandbox is not None
            # Use normalized path for upload - Daytona SDK expects absolute paths
            await self._daytona_call(
                self.sandbox.fs.upload_file,
                content,
                normalized_path,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            return True
        except SandboxTransientError:
            raise
        except Exception as e:
            logger.debug(
                "Failed to upload file bytes",
                filepath=filepath,
                normalized_path=normalized_path,
                error=str(e),
            )
            return False

    async def awrite_file_text(self, filepath: str, content: str) -> bool:
        """Write UTF-8 text to a sandbox file (overwrites).

        This path is safe to retry automatically.
        """
        try:
            return await self.aupload_file_bytes(filepath, content.encode("utf-8"))
        except UnicodeEncodeError as e:
            logger.debug(
                "Failed to encode file as utf-8", filepath=filepath, error=str(e)
            )
            return False

    async def aread_file_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        """Read a specific range of lines from a UTF-8 text file.

        Args:
            file_path: Path to the file.
            offset: Line offset (0-indexed).
            limit: Maximum number of lines.
        """
        content = await self.aread_file_text(file_path)
        if content is None:
            return None

        lines = content.splitlines()
        start = max(0, offset)
        end = start + limit
        return "\n".join(lines[start:end])

    @staticmethod
    def _extract_sandbox_id(sandbox: object) -> str:
        """Extract a stable ID string from a sandbox object."""
        return sandbox.id if hasattr(sandbox, "id") else str(id(sandbox))

    def normalize_path(self, path: str) -> str:
        """Normalize virtual path to absolute sandbox path (input normalization).

        Converts agent's virtual paths to real sandbox paths:
            "/" or "." or "" -> {working_directory}
            "/results/file.txt" -> {working_directory}/results/file.txt
            "data/file.txt" -> {working_directory}/data/file.txt
            "{working_directory}/file.txt" -> unchanged
            "/tmp/file.txt" -> unchanged

        Args:
            path: Virtual or relative path from agent

        Returns:
            Absolute sandbox path
        """
        # Use configured working_directory as the prefix for path normalization
        work_dir = self.config.filesystem.working_directory

        if path in (None, "", ".", "/"):
            return work_dir

        path = path.strip()

        # Already in allowed directories - keep as is (just normalize . and ..)
        for allowed_dir in self.config.filesystem.allowed_directories:
            if path.startswith(allowed_dir):
                return str(Path(path))

        # Virtual absolute path: /foo -> /home/daytona/foo
        if path.startswith("/"):
            return str(Path(f"{work_dir}{path}"))

        # Relative path: foo -> /home/daytona/foo
        return str(Path(f"{work_dir}/{path}"))

    def virtualize_path(self, path: str) -> str:
        """Convert real sandbox path to virtual path (output normalization).

        Strips working_directory prefix from paths returned to agent:
            {working_directory}/results/file.txt -> /results/file.txt
            {working_directory}/tools/docs/foo.md -> /tools/docs/foo.md
            /tmp/file.txt -> /tmp/file.txt (unchanged)

        Args:
            path: Absolute sandbox path

        Returns:
            Virtual path for agent consumption
        """
        # Use configured working_directory as the prefix to strip
        work_dir = self.config.filesystem.working_directory

        if path.startswith(work_dir + "/"):
            return path[len(work_dir) :]  # Strip prefix, keep leading /
        if path == work_dir:
            return "/"

        return path  # /tmp or other paths unchanged

    def validate_path(self, filepath: str) -> bool:
        """Validate if a path is within allowed directories.

        Args:
            filepath: Path to validate (virtual or absolute)

        Returns:
            True if path is allowed, False otherwise
        """
        if not self.config.filesystem.enable_path_validation:
            return True

        # Normalize the path first (handles virtual paths like /results/...)
        normalized_path = self.normalize_path(filepath)

        # Denylist takes priority over allowlist
        for denied_dir in self.config.filesystem.denied_directories:
            if normalized_path == denied_dir or normalized_path.startswith(
                denied_dir + "/"
            ):
                return False

        # Check against allowed directories
        for allowed_dir in self.config.filesystem.allowed_directories:
            # Exact match or path within allowed directory
            if normalized_path == allowed_dir or normalized_path.startswith(
                allowed_dir + "/"
            ):
                return True

        logger.warning(
            "Path validation failed",
            path=filepath,
            normalized_path=normalized_path,
            allowed_dirs=self.config.filesystem.allowed_directories,
        )
        return False

    def validate_and_normalize_path(self, path: str) -> tuple[str, str | None]:
        """Normalize path and validate access.

        Combines path normalization and validation into a single operation.

        Args:
            path: Virtual or relative path from agent

        Returns:
            Tuple of (normalized_path, error_message_or_none)
        """
        normalized = self.normalize_path(path)
        if self.config.filesystem.enable_path_validation and not self.validate_path(
            normalized
        ):
            return normalized, f"Access denied: {path} is not in allowed directories"
        return normalized, None

    async def als_directory(self, directory: str = ".") -> list[dict[str, Any]]:
        """List contents of a directory.

        Returns entries as dicts with at least: name, path, is_dir.
        """
        await self._wait_ready()

        try:
            if self.config.filesystem.enable_path_validation and not self.validate_path(
                directory
            ):
                logger.error(
                    f"Access denied: {directory} is not in allowed directories"
                )
                return []

            assert self.sandbox is not None
            file_infos = await self._daytona_call(
                self.sandbox.fs.list_files,
                directory,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            if not file_infos:
                return []

            results: list[dict[str, Any]] = []
            for entry in file_infos:
                name = str(entry.name) if hasattr(entry, "name") else str(entry)
                is_dir = bool(getattr(entry, "is_dir", False))
                entry_path = f"{directory}/{name}" if directory != "." else name
                results.append({"name": name, "path": entry_path, "is_dir": is_dir})
            return results
        except Exception as e:
            logger.debug("Error listing directory", directory=directory, error=str(e))
            return []

    async def acreate_directory(self, dirpath: str) -> bool:
        """Create a directory in the sandbox."""
        await self._wait_ready()

        try:
            if self.config.filesystem.enable_path_validation and not self.validate_path(
                dirpath
            ):
                logger.error(f"Access denied: {dirpath} is not in allowed directories")
                return False

            assert self.sandbox is not None
            await self._daytona_call(
                self.sandbox.process.exec,
                f"mkdir -p {shlex.quote(dirpath)}",
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )
            return True
        except Exception as e:
            logger.debug("Failed to create directory", dirpath=dirpath, error=str(e))
            return False

    async def aedit_file_text(
        self,
        filepath: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        """Async edit for tools; safe to retry underlying I/O.

        This does not retry the logical edit itself; it only makes file I/O resilient.
        """
        await self._wait_ready()

        try:
            if self.config.filesystem.enable_path_validation and not self.validate_path(
                filepath
            ):
                return {
                    "success": False,
                    "error": f"Access denied: {filepath} is not in allowed directories",
                }

            content = await self.aread_file_text(filepath)
            if content is None:
                return {"success": False, "error": "File not found"}

            if old_string == new_string:
                return {
                    "success": False,
                    "error": "old_string and new_string must be different",
                }

            if old_string not in content:
                return {
                    "success": False,
                    "error": f"old_string not found in file: {filepath}",
                }

            if not replace_all:
                occurrences = content.count(old_string)
                if occurrences > 1:
                    return {
                        "success": False,
                        "error": "old_string found multiple times and requires more code context to uniquely identify the intended match",
                    }

            updated = (
                content.replace(old_string, new_string)
                if replace_all
                else content.replace(old_string, new_string, 1)
            )

            if updated == content:
                return {"success": False, "error": "Edit produced no changes"}

            write_ok = await self.awrite_file_text(filepath, updated)
            if not write_ok:
                return {"success": False, "error": "Failed to write updated file"}

            return {
                "success": True,
                "message": "File edited successfully",
            }

        except Exception as e:
            logger.debug("Async edit_file failed", filepath=filepath, error=str(e))
            return {"success": False, "error": f"Edit operation failed: {e!s}"}

    def _validate_path_allow_denied(self, path: str) -> bool:
        """Validate path against allowlist only (ignores denied_directories).

        Intended for user-initiated inspection flows where we want to keep
        internal directories hidden by default, but still allow explicit access.
        """

        normalized_path = self._normalize_search_path(path)
        for allowed_dir in self.config.filesystem.allowed_directories:
            if normalized_path == allowed_dir or normalized_path.startswith(
                allowed_dir + "/"
            ):
                return True
        return False

    async def aglob_files(
        self, pattern: str, path: str = ".", *, allow_denied: bool = False
    ) -> list[str]:
        """Async glob; safe to retry automatically."""
        await self._wait_ready()

        try:
            if self.config.filesystem.enable_path_validation:
                is_allowed = (
                    self._validate_path_allow_denied(path)
                    if allow_denied
                    else self.validate_path(path)
                )
                if not is_allowed:
                    logger.error(f"Access denied: {path} is not in allowed directories")
                    return []

            search_path = self._normalize_search_path(path)

            if "**" not in pattern and "/" not in pattern:
                pattern = f"**/{pattern}"

            glob_code = textwrap.dedent(f"""\
                import glob
                import os

                pattern = {pattern!r}
                search_path = {search_path!r}

                full_pattern = os.path.join(search_path, pattern)
                matches = glob.glob(full_pattern, recursive=True, include_hidden=True)
                files = [f for f in matches if os.path.isfile(f)]

                try:
                    files_with_mtime = [(f, os.path.getmtime(f)) for f in files]
                    sorted_files = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
                    for f, _ in sorted_files:
                        print(f)  # noqa: T201
                except OSError:
                    for f in files:
                        print(f)  # noqa: T201
            """)

            encoded_code = base64.b64encode(glob_code.encode()).decode()
            cmd = f"python3 -c \"import base64; exec(base64.b64decode('{encoded_code}').decode())\""

            assert self.sandbox is not None
            result = await self._daytona_call(
                self.sandbox.process.exec,
                cmd,
                timeout=30,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

            output = result.result.strip() if getattr(result, "result", None) else ""
            if not output:
                return []
            return output.split("\n")

        except Exception as e:
            logger.warning(
                "Async glob failed", pattern=pattern, path=path, error=str(e)
            )
            return []

    async def agrep_content(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 - matches ripgrep's --type flag
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        """Async ripgrep; safe to retry automatically."""
        await self._wait_ready()

        try:
            if self.config.filesystem.enable_path_validation and not self.validate_path(
                path
            ):
                logger.error(f"Access denied: {path} is not in allowed directories")
                return []

            cmd = ["rg"]
            if output_mode == "files_with_matches":
                cmd.append("-l")
            elif output_mode == "count":
                cmd.append("-c")

            if case_insensitive:
                cmd.append("-i")

            if output_mode == "content" and show_line_numbers:
                cmd.append("-n")

            if lines_before:
                cmd.extend(["-B", str(lines_before)])
            if lines_after:
                cmd.extend(["-A", str(lines_after)])
            if lines_context:
                cmd.extend(["-C", str(lines_context)])

            if multiline:
                cmd.extend(["-U", "--multiline-dotall"])

            if glob:
                cmd.extend(["--glob", glob])
            if type:
                cmd.extend(["--type", type])

            cmd.append(pattern)
            search_path = self._normalize_search_path(path)
            cmd.append(search_path)

            cmd_str = " ".join(f'"{c}"' if " " in c else c for c in cmd)
            assert self.sandbox is not None
            result = await self._daytona_call(
                self.sandbox.process.exec,
                cmd_str,
                timeout=60,
                retry_policy=_DaytonaRetryPolicy.SAFE,
            )

            output = result.result.strip() if getattr(result, "result", None) else ""
            if not output:
                return []

            if output_mode == "count":
                count_results: list[tuple[str, int]] = []
                for line in output.split("\n"):
                    if ":" in line:
                        parts = line.rsplit(":", 1)
                        if len(parts) == 2:
                            try:
                                count_results.append((parts[0], int(parts[1])))
                            except ValueError:
                                count_results.append((line, 0))
                    else:
                        count_results.append((line, 0))

                if offset > 0:
                    count_results = count_results[offset:]
                if head_limit:
                    count_results = count_results[:head_limit]
                return count_results

            results_strs = output.split("\n")
            if offset > 0:
                results_strs = results_strs[offset:]
            if head_limit:
                results_strs = results_strs[:head_limit]
            return results_strs

        except Exception as e:
            logger.debug("Async grep failed", pattern=pattern, path=path, error=str(e))
            return []

    async def cleanup(self) -> None:
        """Clean up and destroy the sandbox."""
        logger.info("Cleaning up sandbox", sandbox_id=self.sandbox_id)

        if self.sandbox:
            try:
                await self._daytona_call(
                    self.sandbox.delete,
                    retry_policy=_DaytonaRetryPolicy.SAFE,
                )
                logger.info("Sandbox deleted", sandbox_id=self.sandbox_id)
            except OSError as e:
                logger.error(f"Error deleting sandbox: {e}")

        self.sandbox = None
        self.sandbox_id = None

        try:
            await self.daytona_client.close()
        except Exception as e:
            logger.debug("Failed to close Daytona client", error=str(e))

    async def __aenter__(self) -> "PTCSandbox":
        """Async context manager entry."""
        await self.setup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.cleanup()
