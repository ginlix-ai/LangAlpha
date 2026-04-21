"""PTC Sandbox - Manages sandbox for Programmatic Tool Calling execution."""

import asyncio
import base64
import hashlib
import json
import shlex
import textwrap
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any

import structlog

from ptc_agent.config.core import CoreConfig
from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES, SNAPSHOT_PYTHON_VERSION
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION, run_layout_migrations
from ptc_agent.core.sandbox.providers import create_provider
from ptc_agent.core.sandbox.retry import RetryPolicy, async_retry_with_backoff
from ptc_agent.core.sandbox.runtime import (
    PreviewInfo,
    RuntimeState,
    SandboxGoneError,
    SandboxRuntime,
    SandboxTransientError,
    SessionCommandResult,
)

from ..mcp_registry import MCPRegistry
from ..tool_generator import ToolFunctionGenerator

logger = structlog.get_logger(__name__)

# Lock entry fields excluded from skills manifest hash — these timestamps
# change on every computation and would force needless re-uploads.
_LOCK_VOLATILE_KEYS: frozenset[str] = frozenset({"installedAt", "updatedAt"})


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


def _entry_name(entry) -> str:
    """Extract name from a file entry — handles both dict and object forms."""
    if isinstance(entry, dict):
        return str(entry.get("name", entry))
    return str(getattr(entry, "name", entry))


def _entry_is_dir(entry) -> bool:
    """Extract is_dir from a file entry — handles both dict and object forms."""
    if isinstance(entry, dict):
        return bool(entry.get("is_dir", False))
    return bool(getattr(entry, "is_dir", False))


def _get_sandbox_eligible_skills() -> tuple[set[str], set[str]]:
    """Return (sandbox_skill_names, all_registry_names) for flash-only filtering.

    Skills present in SKILL_REGISTRY but not in sandbox_skill_names are
    flash-only and should be skipped during sandbox operations.
    """
    from ptc_agent.agent.middleware.skills import SKILL_REGISTRY, get_sandbox_skill_names

    return get_sandbox_skill_names(), set(SKILL_REGISTRY.keys())


class PTCSandbox:
    """Manages sandbox for Programmatic Tool Calling (PTC) execution."""

    SNAPSHOT_PYTHON_VERSION = SNAPSHOT_PYTHON_VERSION
    DEFAULT_DEPENDENCIES = DEFAULT_DEPENDENCIES

    TOKEN_FRESHNESS_SECONDS = 25 * 60  # 25 min (access token TTL is 30 min)

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

        # Provider-based sandbox management
        self.provider = create_provider(config)
        self.runtime: SandboxRuntime | None = None
        self.sandbox_id: str | None = None
        self.tool_generator = ToolFunctionGenerator()
        self.execution_count = 0
        self.bash_execution_count = 0

        # Working directory — initialized from config, updated by fetch_working_dir()
        # after sandbox creation/reconnect.
        self._work_dir: str = config.filesystem.working_directory

        self._reconnect_lock = asyncio.Lock()
        self._tool_refresh_lock = asyncio.Lock()
        self._download_semaphore = asyncio.Semaphore(4)

        # Track per-thread code dirs that have been created (avoids repeated mkdir)
        self._thread_dirs_created: set[str] = set()

        # Per-command sessions for background Bash commands (cmd_id → session_id)
        self._bg_sessions: dict[str, str] = {}
        # Per-port sessions for preview servers (port → (session_id, cmd_id))
        self._preview_sessions: dict[int, tuple[str, str]] = {}
        # Per-port locks to serialize start_and_get_preview_url (avoids races)
        self._preview_locks: dict[int, asyncio.Lock] = {}

        # Lazy initialization support
        self._ready_event: asyncio.Event | None = None
        self._init_task: asyncio.Task[None] | None = None
        self._init_error: Exception | None = None

        # Cached skills manifest (populated after sync_sandbox_assets)
        self._skills_manifest: dict[str, Any] | None = None

        # Track whether disabled tool modules have been pruned (only needed once)
        self._disabled_modules_pruned = False

        # Cached standard preview link info per port (avoids repeated Daytona API calls)
        self._preview_link_cache: dict[int, PreviewInfo] = {}

        logger.debug("Initialized PTCSandbox")

    @property
    def working_dir(self) -> str:
        """The sandbox working directory (available from construction, updated after setup)."""
        return self._work_dir

    @property
    def _unified_manifest_path(self) -> str:
        return f"{self._work_dir}/_internal/.sandbox_manifest.json"

    @property
    def _token_file_path(self) -> str:
        return f"{self._work_dir}/_internal/.mcp_tokens.json"

    async def _wait_ready(self) -> None:
        """Wait for sandbox to be ready. Call at start of methods needing sandbox."""
        if self._ready_event is None:
            # Not using lazy init - sandbox should already be ready
            if self.runtime is None:
                raise RuntimeError("Sandbox not initialized")
            return

        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            raise RuntimeError("Sandbox initialization timed out after 300s")

        if self._init_error:
            raise self._init_error

    def is_ready(self) -> bool:
        """Check if sandbox is ready without blocking.

        Returns:
            True if sandbox is ready for operations, False if still initializing.
        """
        if self._ready_event is None:
            # Not using lazy init - check if runtime exists
            return self.runtime is not None

        # Using lazy init - check if event is set and no error
        return self._ready_event.is_set() and self._init_error is None

    def has_failed(self) -> bool:
        """Check if lazy initialization completed with an error."""
        if self._ready_event is None:
            return False
        return self._ready_event.is_set() and self._init_error is not None

    @property
    def init_error(self) -> Exception | None:
        """The error from lazy initialization, if any."""
        return self._init_error

    @property
    def skills_manifest(self) -> dict[str, Any] | None:
        """Cached skills manifest from the last ``sync_sandbox_assets`` call.

        Contains ``"version"``, ``"files"``, and ``"skills"`` (parsed metadata).
        Returns None if ``sync_sandbox_assets`` has not been called yet.
        """
        return self._skills_manifest

    def start_lazy_init(
        self,
        sandbox_id: str,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> None:
        """Start sandbox initialization in background (non-blocking).

        Call this instead of reconnect() for lazy initialization.
        Methods will automatically wait for init to complete.

        ``on_state_observed`` is forwarded to reconnect so callers can
        learn the pre-start sandbox state asynchronously.
        """
        if self._init_task is not None:
            return  # Already started

        self._ready_event = asyncio.Event()
        self._init_task = asyncio.create_task(
            self._lazy_reconnect(sandbox_id, on_state_observed=on_state_observed)
        )

    async def _lazy_reconnect(
        self,
        sandbox_id: str,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> None:
        """Background task for lazy reconnection."""
        try:
            logger.debug("Starting lazy sandbox init", sandbox_id=sandbox_id)
            await self.reconnect(sandbox_id, on_state_observed=on_state_observed)
            logger.debug("Lazy sandbox init complete", sandbox_id=sandbox_id)
        except asyncio.CancelledError:
            # CancelledError is BaseException, not Exception — must be
            # caught explicitly so _init_error is set.  Without this,
            # _ready_event.set() in the finally block signals "ready"
            # with no error, and concurrent _wait_ready() callers
            # proceed with a None runtime.
            logger.debug("Lazy sandbox init cancelled", sandbox_id=sandbox_id)
            self._init_error = RuntimeError("Sandbox init was cancelled")
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
            return self._work_dir
        if not path.startswith("/"):
            return f"{self._work_dir}/{path}"
        return path

    def _build_sandbox_env_vars(self) -> dict[str, str]:
        """Build environment variables to inject at sandbox creation time.

        Resolves MCP server env vars (${VAR} placeholders from host) and
        GitHub bot credentials so they're available to all sandbox processes.
        """
        import os

        env_vars: dict[str, str] = {
            # Playwright browsers are installed to /usr/local/ms-playwright
            # in the snapshot image; tell the Python package where to find them.
            "PLAYWRIGHT_BROWSERS_PATH": "/usr/local/ms-playwright",
        }

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

        # Create sandbox via provider (handles snapshot logic internally)
        mcp_packages = self._get_mcp_packages()
        self.runtime = await self._runtime_call(
            self.provider.create,
            env_vars=sandbox_env or None,
            mcp_packages=mcp_packages,
            retry_policy=RetryPolicy.SAFE,
            allow_reconnect=False,
        )

        assert self.runtime is not None
        self.sandbox_id = self.runtime.id
        logger.info("Sandbox created", sandbox_id=self.sandbox_id)

        # Set up workspace structure
        await self._setup_workspace()

        # Surface snapshot name from provider metadata for MCP server init
        snapshot_name = getattr(self.runtime, "snapshot_name", None)

        # When no snapshot is available (disabled, creation failed, etc.) the
        # sandbox is a bare image without application packages — install them.
        if not snapshot_name:
            await self._install_dependencies()

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

    async def upload_token_file(self, tokens: dict) -> None:
        """Write scoped auth tokens to a file in the sandbox.

        Tokens are written as a JSON file (not env vars) because refresh tokens
        rotate on each use and the MCP server needs to update them in-place.
        Tokens carry deterministic prefixes (gxsa_, gxsr_) so the host-side
        LeakDetectionMiddleware can pattern-match them without knowing exact values.
        """
        import os

        if not tokens or not self.runtime:
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
            await self._runtime_call(
                self.runtime.upload_file,
                token_data.encode("utf-8"),
                self._token_file_path,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.debug("Uploaded sandbox token file", path=self._token_file_path)
        except Exception as e:
            logger.warning("Failed to upload sandbox token file", error=str(e))

    async def upload_vault_secrets(self, secrets: dict[str, str]) -> None:
        """Write (or remove) vault secrets JSON in the sandbox.

        Called by the vault API on every CRUD mutation.  Also caches the
        secrets dict on ``self`` so the server can pass them to
        ``LeakDetectionMiddleware`` without an extra DB call.
        """
        self.vault_secrets: dict[str, str] = secrets

        if not self.runtime:
            return

        vault_path = f"{self._work_dir}/_internal/.vault_secrets.json"

        if not secrets:
            # Remove the file so vault.list_names() returns []
            try:
                await self._runtime_call(
                    self.runtime.exec,
                    f"rm -f {vault_path}",
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception as e:
                logger.warning("Failed to remove vault secrets file", error=str(e))
            return

        try:
            await self._runtime_call(
                self.runtime.upload_file,
                json.dumps(secrets).encode("utf-8"),
                vault_path,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.info("Uploaded vault secrets file", path=vault_path)
        except Exception as e:
            logger.warning("Failed to upload vault secrets file", error=str(e))

    async def ensure_sandbox_ready(self) -> None:
        await self._wait_ready()

        assert self.runtime is not None
        # Only set from config default when _work_dir is unset (fresh sandbox).
        # reconnect() may have already fetched the real dir from the sandbox
        # API — don't clobber it with the config default which can differ
        # (e.g. /home/workspace vs /home/daytona after a config change).
        if not self._work_dir:
            self._work_dir = self.runtime.working_dir

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

    async def reconnect(
        self,
        sandbox_id: str,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> None:
        """Reconnect to a stopped sandbox.

        This is a fast path for session persistence - it starts a stopped
        sandbox and skips all setup work (file uploads, tool modules, etc.)
        since they're already present from the first session.

        Args:
            sandbox_id: The ID of an existing sandbox
            on_state_observed: Optional sync callback invoked once with the
                initial state string (``"archived"``, ``"running"``,
                ``"stopped"``, ...) right after the first ``get_state``
                call. Lets upstream callers (e.g. the chat SSE generator)
                react to the pre-start state without a second SDK probe.
                Callback must not raise — exceptions are swallowed.

        Raises:
            SandboxGoneError: If sandbox cannot be found or is in an unrecoverable state
        """
        logger.debug("Reconnecting to stopped sandbox", sandbox_id=sandbox_id)

        _t0 = time.time()
        _rc_phases: dict[str, float] = {}

        def _mark_rc(name: str) -> None:
            nonlocal _t0
            now = time.time()
            _rc_phases[name] = (now - _t0) * 1000
            _t0 = now

        # Clear stale state — sessions and preview links don't survive stop/start
        self._bg_sessions.clear()
        self._preview_sessions.clear()
        self._preview_link_cache.clear()

        # Get the existing sandbox via provider
        try:
            self.runtime = await self._runtime_call(
                self.provider.get,
                sandbox_id,
                retry_policy=RetryPolicy.SAFE,
                allow_reconnect=False,
            )
        except Exception as e:
            raise SandboxGoneError(sandbox_id, f"not found: {e}") from e
        _mark_rc("provider_get")

        assert self.runtime is not None
        self.sandbox_id = sandbox_id

        # Check sandbox state before attempting to start
        state = await self.runtime.get_state()
        state_value = state.value
        _mark_rc("get_state")

        if on_state_observed is not None:
            try:
                on_state_observed(state_value)
            except Exception:
                logger.debug(
                    "on_state_observed callback raised; ignoring",
                    sandbox_id=sandbox_id,
                )

        if state_value == "running":
            logger.debug(
                "Sandbox already started, skipping start", sandbox_id=sandbox_id
            )
        elif state_value == "stopped":
            logger.debug(
                "Starting stopped sandbox", sandbox_id=sandbox_id, state=state_value
            )
            await self._runtime_call(
                self.runtime.start,
                timeout=60,
                retry_policy=RetryPolicy.SAFE,
            )
            _mark_rc("start")
        elif state_value == "starting":
            # Sandbox is already transitioning — wait for it to reach 'running'.
            logger.debug(
                "Sandbox is starting, waiting for ready",
                sandbox_id=sandbox_id,
            )
            for _ in range(40):  # Max ~20 seconds
                await asyncio.sleep(0.5)
                self.runtime = await self._runtime_call(
                    self.provider.get,
                    sandbox_id,
                    retry_policy=RetryPolicy.SAFE,
                    allow_reconnect=False,
                )
                state = await self.runtime.get_state()
                state_value = state.value
                if state_value == "running":
                    break
            if state_value != "running":
                raise SandboxGoneError(
                    sandbox_id,
                    f"stuck in state '{state_value}', expected 'running'",
                )
            _mark_rc("wait_starting")
        elif state_value == "stopping":
            # Wait for sandbox to finish stopping, then start it.
            logger.info(
                "Sandbox is stopping, waiting before start",
                sandbox_id=sandbox_id,
            )
            for _ in range(20):  # Max ~10 seconds
                await asyncio.sleep(0.5)
                self.runtime = await self._runtime_call(
                    self.provider.get,
                    sandbox_id,
                    retry_policy=RetryPolicy.SAFE,
                    allow_reconnect=False,
                )
                state = await self.runtime.get_state()
                state_value = state.value
                if state_value == "stopped":
                    break
            if state_value == "stopped":
                logger.info(
                    "Sandbox finished stopping, starting it",
                    sandbox_id=sandbox_id,
                )
                await self._runtime_call(
                    self.runtime.start,
                    timeout=60,
                    retry_policy=RetryPolicy.SAFE,
                )
            else:
                raise SandboxGoneError(
                    sandbox_id,
                    f"stuck in state '{state_value}', expected 'stopped'",
                )
            _mark_rc("wait_stopping")
        elif state_value == "archived":
            logger.info(
                "Starting archived sandbox (restore may take longer)",
                sandbox_id=sandbox_id,
            )
            await self._runtime_call(
                self.runtime.start,
                timeout=300,
                retry_policy=RetryPolicy.SAFE,
            )
            _mark_rc("start_archived")
        elif state_value == "error":
            # Sandbox hit an internal error — attempt recovery via start().
            logger.warning(
                "Sandbox in error state, attempting recovery start",
                sandbox_id=sandbox_id,
            )
            await self._runtime_call(
                self.runtime.start,
                timeout=120,
                retry_policy=RetryPolicy.SAFE,
            )
            _mark_rc("start_error_recovery")
        else:
            raise SandboxGoneError(
                sandbox_id,
                f"unrecoverable state: {state_value}",
            )

        # Fetch the actual working dir from the sandbox. The config default
        # may differ from the real dir (e.g. /home/workspace vs /home/daytona)
        # when the sandbox was created under a previous config.
        self._work_dir = await self.runtime.fetch_working_dir()
        _mark_rc("fetch_workdir")

        total = sum(_rc_phases.values())
        phases = " ".join(f"{k}={v:.0f}ms" for k, v in _rc_phases.items())
        logger.info(
            f"[RECONNECT] sandbox_id={sandbox_id} state={state_value} "
            f"total={total:.0f}ms ({phases})"
        )

        # SKIP: _setup_workspace() - directories already exist
        # SKIP: _upload_mcp_server_files() - files already uploaded
        # SKIP: _install_tool_modules() - tool modules already installed

        # Initialize MCP server sessions (needed for tool execution)
        self.mcp_server_sessions: dict[str, Any] = {}
        await self._start_internal_mcp_servers()

        logger.debug(
            "Sandbox started from stopped state",
            sandbox_id=self.sandbox_id,
        )

    async def _cancel_init_task(self) -> None:
        """Cancel any in-flight lazy init task and wait for it to finish."""
        if self._init_task is not None and not self._init_task.done():
            self._init_task.cancel()
            try:
                await self._init_task
            except (asyncio.CancelledError, Exception):
                pass
        self._init_task = None

    async def stop_sandbox(self) -> None:
        """Stop the sandbox without deleting it.

        Used for session persistence - stops the sandbox so it can be
        restarted quickly on the next session, rather than deleting it.
        """
        await self._cancel_init_task()

        if not self.runtime:
            return

        # Check state before stopping to avoid errors when already stopped
        try:
            state = await self.runtime.get_state()
            if state == RuntimeState.STOPPED:
                logger.info("Sandbox already stopped", sandbox_id=self.sandbox_id)
                return
        except Exception as e:
            # If state check fails, log and continue with stop attempt
            logger.debug("Could not check sandbox state", error=str(e))

        try:
            logger.info("Stopping sandbox", sandbox_id=self.sandbox_id)
            await self._runtime_call(
                self.runtime.stop,
                timeout=60,
                retry_policy=RetryPolicy.SAFE,
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
        assert self.runtime is not None
        work_dir = await self.runtime.fetch_working_dir()
        logger.info(f"Sandbox working directory: {work_dir}")

        # Store work_dir for use by other methods
        self._work_dir = work_dir

        # Use absolute paths to ensure directories are created correctly
        directories = [
            f"{work_dir}/tools",
            f"{work_dir}/tools/docs",
            f"{work_dir}/results",
            f"{work_dir}/data",
            f"{work_dir}/.system/code",
            f"{work_dir}/work",
            f"{work_dir}/.agents/threads",
            f"{work_dir}/.agents/skills",
            f"{work_dir}/_internal/src",
        ]

        # Create all directories in parallel for faster setup
        async def create_directory(directory: str) -> None:
            try:
                assert self.runtime is not None
                await self._runtime_call(
                    self.runtime.exec,
                    f"mkdir -p {shlex.quote(directory)}",
                    retry_policy=RetryPolicy.SAFE,
                )
                logger.info(f"Created directory: {directory}")
            except Exception as e:
                logger.warning(f"Error creating directory {directory}: {e}")

        await asyncio.gather(*[create_directory(d) for d in directories])

    async def _upload_internal_packages(self) -> None:
        """Upload internal Python packages for sandbox execution.

        Currently uploads the `src.data_client` package so code executed inside the
        sandbox can import `src.data_client` without depending on the full repo.
        """
        work_dir = self._work_dir
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

        assert self.runtime is not None
        sandbox = self.runtime

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
        await self._runtime_call(
            sandbox.exec,
            mkdir_cmd,
            retry_policy=RetryPolicy.SAFE,
        )

        # Batch upload — source is a local file path string
        batch: list[tuple[str, str]] = [
            (str(local_path), str(internal_root / rel_path))
            for local_path, rel_path in files
        ]
        await self._runtime_call(
            sandbox.upload_files,
            batch,
            retry_policy=RetryPolicy.SAFE,
        )
        logger.debug(
            "Uploaded internal packages to sandbox",
            uploaded_files=len(files),
            sandbox_root=str(internal_root),
        )

        # Upload vault helper module so `from vault import get` is always
        # importable, even if no secrets exist yet.
        try:
            from ptc_agent.core.sandbox.vault_helper import VAULT_MODULE_SOURCE

            vault_dest = str(internal_root / "vault.py")
            await self._runtime_call(
                sandbox.upload_file,
                VAULT_MODULE_SOURCE.encode("utf-8"),
                vault_dest,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.debug("Uploaded vault helper module", path=vault_dest)
        except Exception as e:
            logger.warning("Failed to upload vault helper module", error=str(e))

    async def _upload_user_data_files(self, files: dict[str, str]) -> None:
        """Upload pre-formatted user data markdown files to sandbox."""
        work_dir = self._work_dir
        user_data_path = f"{work_dir}/.agents/user"

        assert self.runtime is not None
        await self._runtime_call(
            self.runtime.exec,
            f"mkdir -p {user_data_path}",
            retry_policy=RetryPolicy.SAFE,
        )
        batch: list[tuple[bytes, str]] = [
            (content.encode("utf-8"), f"{user_data_path}/{name}")
            for name, content in files.items()
        ]
        await self._runtime_call(
            self.runtime.upload_files,
            batch,
            retry_policy=RetryPolicy.SAFE,
        )
        logger.debug("Uploaded user data files", file_count=len(files))

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

        skills_base = f"{self._work_dir}/.agents/skills"

        def build() -> dict[str, Any]:
            from ptc_agent.agent.middleware.skills.discovery import (
                parse_skill_metadata,
            )

            sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

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
                    sandbox_path = f"{skills_base}/{skill_name}/SKILL.md"
                    meta = parse_skill_metadata(content, sandbox_path, skill_name)
                    skills_metadata[skill_name] = dict(meta)

                    # Build lock entry for platform skill
                    from ptc_agent.agent.middleware.skills.lock import build_lock_entry

                    content_hash = f"sha256:{_sha256_file(skill_dir / 'SKILL.md')}"
                    lock_entry = build_lock_entry(
                        meta,
                        owner="platform",
                        source="platform",
                        source_type="platform",
                        content_hash=content_hash,
                    )
                    skills_metadata[skill_name]["lock_entry"] = dict(lock_entry)

            version = _hash_dict(files)

            # Include lock entries in version hash so manifest detects ownership changes.
            # Exclude volatile timestamp fields (installedAt, updatedAt) — they change
            # on every manifest computation and would force a full skills re-upload
            # on every workspace restart even when no skill files changed.
            lock_hash_parts = []
            for name in sorted(skills_metadata):
                entry = skills_metadata[name].get("lock_entry")
                if entry:
                    stable = {k: v for k, v in entry.items() if k not in _LOCK_VOLATILE_KEYS}
                    lock_hash_parts.append(f"{name}:{json.dumps(stable, sort_keys=True)}")
            if lock_hash_parts:
                lock_payload = "\n".join(lock_hash_parts)
                combined = f"{version}\n{lock_payload}"
                version = hashlib.sha256(combined.encode()).hexdigest()

            return {"version": version, "files": files, "skills": skills_metadata}

        return await asyncio.to_thread(build)

    async def _compute_sandbox_manifest(
        self,
        *,
        skill_roots: list[str] | None = None,
        tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
        user_data_hash: str | None = None,
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

        # ── Module: user_data ──
        if user_data_hash:
            modules["user_data"] = {"version": user_data_hash}

        return {
            "schema_version": 1,
            "layout_version": CURRENT_LAYOUT_VERSION,
            "modules": modules,
        }

    # ── Unified manifest I/O ─────────────────────────────────────────

    async def _read_unified_manifest(self) -> dict[str, Any] | None:
        """Read the unified manifest from the sandbox.

        Bypasses path validation for ``_internal/``.
        Returns None if missing, corrupt, or wrong ``schema_version``
        (triggers full refresh in the caller).
        """
        assert self.runtime is not None
        try:
            raw = await self._runtime_call(
                self.runtime.download_file,
                self._unified_manifest_path,
                retry_policy=RetryPolicy.SAFE,
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
        """Write the unified manifest to the sandbox.

        Bypasses path validation since ``_internal/`` is a protected directory
        that the agent cannot access, but the system needs to write to.
        """
        assert self.runtime is not None
        await self._runtime_call(
            self.runtime.upload_file,
            json.dumps(manifest, sort_keys=True).encode("utf-8"),
            self._unified_manifest_path,
            retry_policy=RetryPolicy.SAFE,
        )

    async def _cleanup_legacy_manifests(self) -> None:
        """Remove old per-module manifest files after migration to unified manifest."""
        work_dir = self._work_dir
        legacy_paths = [
            f"{work_dir}/mcp_servers/.mcp_manifest.json",
            f"{work_dir}/skills/.skills_manifest.json",
            f"{work_dir}/.agents/skills/.skills_manifest.json",
        ]
        assert self.runtime is not None
        try:
            rm_cmd = "rm -f " + " ".join(shlex.quote(p) for p in legacy_paths)
            await self._runtime_call(
                self.runtime.exec,
                rm_cmd,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception:
            pass  # Best-effort cleanup

    async def _upload_mcp_server_files_impl(self) -> None:
        """Upload MCP server .py files to sandbox (pure upload, no manifest check)."""
        work_dir = self._work_dir
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

        assert self.runtime is not None
        sandbox = self.runtime

        await self._runtime_call(
            sandbox.exec,
            f"mkdir -p {mcp_servers_dir}",
            retry_policy=RetryPolicy.SAFE,
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
                await self._runtime_call(
                    sandbox.exec,
                    rm_cmd,
                    retry_policy=RetryPolicy.SAFE,
                )
                logger.info(
                    "Pruned MCP server files",
                    removed=len(files_to_remove),
                    sandbox_root=mcp_servers_dir,
                )

        # Batch upload — single HTTP request via upload_files
        if files_to_upload:
            batch = [
                (local, remote)
                for _, local, remote in files_to_upload
            ]
            await self._runtime_call(
                sandbox.upload_files,
                batch,
                retry_policy=RetryPolicy.SAFE,
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
        user_data_files: dict[str, str] | None = None,
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

            _t0 = time.time()
            _sync_phases: dict[str, float] = {}

            def _mark_sync(name: str) -> None:
                nonlocal _t0
                now = time.time()
                _sync_phases[name] = (now - _t0) * 1000
                _t0 = now

            # Steps 0+1+2: all three are independent — parallelize
            # _prune_disabled_tool_modules → sandbox rm (disjoint from manifest paths)
            # _compute_sandbox_manifest → local CPU/disk only
            # _read_unified_manifest → sandbox HTTP GET
            skill_roots = [d for d, _ in skill_dirs] if skill_dirs else None
            # Compute user_data hash from pre-formatted content (if provided)
            ud_hash: str | None = None
            if user_data_files:
                combined = "\n".join(
                    f"{k}:{v}" for k, v in sorted(user_data_files.items())
                )
                ud_hash = hashlib.sha256(combined.encode()).hexdigest()

            _, local_manifest, remote_manifest = await asyncio.gather(
                self._prune_disabled_tool_modules(),
                self._compute_sandbox_manifest(
                    skill_roots=skill_roots,
                    tokens=tokens,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    user_data_hash=ud_hash,
                ),
                self._read_unified_manifest(),
            )
            _mark_sync("manifest")

            # 2b. Run layout migrations if needed (zero cost when current)
            remote_layout = (remote_manifest or {}).get("layout_version", 1)
            await run_layout_migrations(
                self.runtime, self._work_dir, remote_layout
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

                # Download existing lock file once (shared by prune + upload)
                existing_lock = await self._download_skills_lock(sandbox_base)

                await self._prune_remote_skills(
                    sandbox_base, local_skill_names, existing_lock=existing_lock
                )
                skills_mod = local_manifest["modules"].get("skills", {})
                if skills_mod.get("files"):
                    merged_lock = await self._upload_skills(
                        skill_dirs,
                        manifest=skills_mod,  # type: ignore[arg-type]
                        existing_lock=existing_lock,
                    )
                    # Build complete skills cache from merged lock data
                    if merged_lock:
                        self._build_complete_skills_cache(
                            skills_mod, merged_lock, sandbox_base
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
            if "user_data" in changed_modules and user_data_files:
                if on_progress:
                    on_progress("Syncing user data...")
                parallel_uploads.append(
                    ("user_data", self._upload_user_data_files(user_data_files))
                )

            if parallel_uploads:
                await asyncio.gather(*[coro for _, coro in parallel_uploads])
                refreshed.extend(name for name, _ in parallel_uploads)
            _mark_sync("uploads")

            # Group 2: tool_modules AFTER mcp_servers (intent: derived from MCP definitions)
            if "tool_modules" in changed_modules:
                if on_progress:
                    on_progress("Regenerating tool modules...")
                await self._install_tool_modules()
                refreshed.append("tool_modules")
                _mark_sync("tool_modules")
                try:
                    await self._start_internal_mcp_servers()
                except Exception as e:
                    logger.warning("Failed to refresh MCP servers", error=str(e))
                _mark_sync("mcp_start")

            # Cache skills metadata (only if not already set by _build_complete_skills_cache,
            # which includes user-installed skills from the lock file)
            if self._skills_manifest is None and "skills" in local_manifest["modules"]:
                self._skills_manifest = local_manifest["modules"]["skills"]

            # Steps 5+6: independent — parallelize
            await asyncio.gather(
                self._write_unified_manifest(local_manifest),
                self._cleanup_legacy_manifests(),
            )
            _mark_sync("finalize")

            total = sum(_sync_phases.values())
            phases = " ".join(f"{k}={v:.0f}ms" for k, v in _sync_phases.items())
            logger.info(
                f"[ASSET_SYNC] total={total:.0f}ms ({phases}) "
                f"changed={','.join(sorted(refreshed)) or 'none'}"
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
        if not self.runtime or self._disabled_modules_pruned:
            return

        sandbox = self.runtime
        disabled = [
            server.name for server in self.config.mcp.servers if not server.enabled
        ]
        if not disabled:
            self._disabled_modules_pruned = True
            return

        work_dir = self._work_dir
        paths: list[str] = []
        for name in disabled:
            paths.append(f"{work_dir}/tools/{name}.py")
            paths.append(f"{work_dir}/tools/docs/{name}")

        async def remove_one(path: str) -> None:
            await self._runtime_call(
                sandbox.exec,
                f"rm -rf {shlex.quote(path)}",
                retry_policy=RetryPolicy.SAFE,
            )

        await asyncio.gather(*[remove_one(path) for path in paths])
        self._disabled_modules_pruned = True
        logger.debug("Pruned disabled tool modules", removed=len(paths))

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

        # Serialize concurrent reconnect attempts. asyncio.Lock is held
        # across internal awaits, so a second caller that acquires the lock
        # runs after the first's reconnect has fully resolved (success or
        # exception propagated). No explicit coalescing primitive needed.
        async with self._reconnect_lock:
            # Always recreate the provider. This callback only fires after
            # a transient error, so the existing client may be dead or stale.
            try:
                await self.provider.close()
            except Exception:
                pass
            self.provider = create_provider(self.config)
            await self.reconnect(self.sandbox_id)

    async def _runtime_call(
        self,
        func: Callable[..., Any],
        *args: Any,
        retry_policy: RetryPolicy,
        allow_reconnect: bool = True,
        retries: int = 5,
        initial_delay_s: float = 0.25,
        total_timeout: float = 120.0,
        **kwargs: Any,
    ) -> Any:
        on_transient = self._ensure_sandbox_connected if allow_reconnect else None
        return await async_retry_with_backoff(
            func,
            *args,
            retry_policy=retry_policy,
            is_transient=self.provider.is_transient_error,
            on_transient=on_transient,
            retries=retries,
            initial_delay_s=initial_delay_s,
            total_timeout=total_timeout,
            **kwargs,
        )

    async def _collect_local_skill_names(
        self, local_skill_roots: list[str]
    ) -> set[str]:
        def build() -> set[str]:
            sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

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

    async def _download_skills_lock(
        self, sandbox_skills_base: str
    ) -> dict[str, Any] | None:
        """Download and parse the existing skills-lock.json from sandbox.

        Returns parsed skill entries dict, or None if missing/corrupt.
        """
        from ptc_agent.agent.middleware.skills.lock import LOCK_FILENAME, parse_skills_lock

        lock_path = f"{sandbox_skills_base}/{LOCK_FILENAME}"
        assert self.runtime is not None
        try:
            raw = await self._runtime_call(
                self.runtime.download_file,
                lock_path,
                retry_policy=RetryPolicy.SAFE,
            )
            if raw:
                text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                return parse_skills_lock(text)
        except Exception:
            logger.debug("No existing skills-lock.json (fresh sandbox or error)")
        return None

    def _build_complete_skills_cache(
        self,
        skills_mod: dict[str, Any],
        merged_lock: dict[str, Any],
        sandbox_skills_base: str,
    ) -> None:
        """Merge user-installed skills from lock file into the skills manifest cache.

        This ensures known_skills in agent.py includes both platform and
        user-installed skills, eliminating per-message downloads.
        """
        from ptc_agent.agent.middleware.skills.lock import lock_entry_to_skill_metadata

        all_skills = dict(skills_mod.get("skills", {}))

        lock_skills = merged_lock.get("skills", {})
        for name, entry in lock_skills.items():
            if entry.get("owner") == "user" and name not in all_skills:
                skill_path = f"{sandbox_skills_base}/{name}/SKILL.md"
                meta = lock_entry_to_skill_metadata(entry, skill_path)
                all_skills[name] = dict(meta)

        self._skills_manifest = {**skills_mod, "skills": all_skills}

    async def sync_skills_lock(self) -> None:
        """Reconcile skills-lock.json with the actual filesystem state.

        Bidirectional sync in a single sandbox exec (1 API call):
        - **Remove** lock entries whose skill directories no longer exist
        - **Add** lock entries for skill directories not yet in the lock
          (parses SKILL.md frontmatter to populate name/description/metadata)

        Fast path: if no lock file exists and no skill directories exist,
        exits immediately.  If lock is perfectly in sync, no write occurs.

        Intended to be called post-completion alongside file backup.
        Self-healing in discovery.py serves as a fallback if this fails.
        """
        if not self.runtime:
            return
        skills_base = f"{self._work_dir}/.agents/skills"
        lock_path = f"{skills_base}/skills-lock.json"

        # Single inline Python script that runs entirely in the sandbox.
        # Reads dirs + lock file, diffs, parses SKILL.md for new entries,
        # writes updated lock — all in one exec round trip.
        # Uses json.dumps() for path interpolation (not shlex.quote) because
        # values appear as Python string literals inside python3 -c.
        script = textwrap.dedent(f"""\
            python3 -c '
import json, os, re, hashlib, sys
from datetime import datetime, timezone

SKILLS_BASE = {json.dumps(skills_base)}
LOCK_PATH = {json.dumps(lock_path)}

# 1. List skill dirs (only dirs containing SKILL.md)
dirs = set()
if os.path.isdir(SKILLS_BASE):
    for name in os.listdir(SKILLS_BASE):
        p = os.path.join(SKILLS_BASE, name)
        if os.path.isdir(p) and os.path.isfile(os.path.join(p, "SKILL.md")):
            dirs.add(name)

# 2. Read existing lock
lock_data = {{"version": 1, "skills": {{}}}}
if os.path.isfile(LOCK_PATH):
    try:
        with open(LOCK_PATH) as f:
            lock_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
skills = lock_data.get("skills", {{}})

# 3. Compute diff
locked_names = set(skills.keys())
to_remove = locked_names - dirs
to_add = dirs - locked_names

if not to_remove and not to_add:
    print(json.dumps({{"status": "noop", "removed": 0, "added": 0}}))
    sys.exit(0)

# 4. Remove stale entries
for name in to_remove:
    del skills[name]

# 5. Add new entries by parsing SKILL.md frontmatter
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
for name in sorted(to_add):
    skill_md = os.path.join(SKILLS_BASE, name, "SKILL.md")
    desc = ""
    confirmed = False
    meta = {{}}
    license_val = None
    allowed_tools = []
    try:
        with open(skill_md, errors="replace") as f:
            content = f.read(1048576)  # 1MB cap
        content = content.replace("\\r\\n", "\\n")
        m = re.match(r"^---\\s*\\n(.*?)\\n---\\s*(?:\\n|$)", content, re.DOTALL)
        if m:
            # Minimal YAML-like parser for simple key: value frontmatter
            # Avoids PyYAML dependency in sandbox
            for line in m.group(1).splitlines():
                line = line.strip()
                if ":" in line:
                    k, _, v = line.partition(":")
                    k, v = k.strip(), v.strip()
                    if k == "description":
                        desc = v.strip("\\"\\x27")
                        confirmed = True
                    elif k == "license":
                        license_val = v.strip("\\"\\x27") or None
            confirmed = confirmed and bool(name)
        content_hash = "sha256:" + hashlib.sha256(content.encode()).hexdigest()
    except Exception:
        content_hash = ""

    skills[name] = {{
        "name": name,
        "description": desc,
        "owner": "user",
        "source": "local",
        "sourceType": "local",
        "computedHash": content_hash,
        "confirmed": confirmed,
        "license": license_val,
        "metadata": meta,
        "allowed_tools": allowed_tools,
        "installedAt": now,
        "updatedAt": now,
    }}

# 6. Write updated lock atomically
lock_data["skills"] = skills
os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
tmp = LOCK_PATH + ".tmp"
try:
    with open(tmp, "w") as f:
        json.dump(lock_data, f, sort_keys=True, indent=2, ensure_ascii=False)
        f.write("\\n")
    os.replace(tmp, LOCK_PATH)
    print(json.dumps({{"status": "ok", "removed": len(to_remove), "added": len(to_add)}}))
except OSError as e:
    try:
        os.unlink(tmp)
    except OSError:
        pass
    print(json.dumps({{"status": "error", "error": str(e)}}))
    sys.exit(1)
'
        """)

        try:
            result = await self._runtime_call(
                self.runtime.exec,
                script.strip(),
                retry_policy=RetryPolicy.SAFE,
            )
            stdout = (getattr(result, "stdout", "") or "").strip()
            if stdout:
                try:
                    info = json.loads(stdout)
                    if info.get("status") == "ok":
                        logger.info(
                            "Skills lock synced",
                            removed=info.get("removed", 0),
                            added=info.get("added", 0),
                            skills_base=skills_base,
                        )
                    elif info.get("status") == "noop":
                        logger.debug("Skills lock already in sync")
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.debug("Skills lock sync failed (non-critical)", error=str(e))

    async def _prune_remote_skills(
        self,
        sandbox_base: str,
        local_skill_names: set[str],
        *,
        existing_lock: dict[str, Any] | None = None,
    ) -> None:
        """Prune stale platform skills from sandbox, protecting user-installed ones.

        Safe default: if lock is unavailable or a skill has no lock entry,
        it is preserved to prevent data loss on transient failures.
        """
        assert self.runtime is not None
        sandbox = self.runtime
        entries = await self.als_directory(sandbox_base)
        if not entries:
            return

        paths_to_remove: list[str] = []
        for entry in entries:
            name = entry.get("name")
            if not name:
                continue
            if not entry.get("is_dir", False):
                continue
            if name in local_skill_names:
                continue  # Current platform skill — will be re-uploaded

            # Unknown skill — check lock for ownership
            if existing_lock is None:
                # Lock unavailable — safe default: preserve everything
                continue
            lock_entry = existing_lock.get(name)
            if lock_entry is None:
                # Not in lock — unknown origin, preserve (safe default)
                continue
            if lock_entry.get("owner") == "user":
                # User-installed — never prune
                logger.debug("Preserving user-installed skill", skill=name)
                continue
            # Platform skill no longer in local set — stale, prune it
            paths_to_remove.append(entry["path"])

        if not paths_to_remove:
            return

        async def remove_one(path: str) -> None:
            await self._runtime_call(
                sandbox.exec,
                f"rm -rf {shlex.quote(path)}",
                retry_policy=RetryPolicy.SAFE,
            )

        await asyncio.gather(*[remove_one(path) for path in paths_to_remove])
        logger.info(
            "Pruned stale platform skills from sandbox",
            removed=len(paths_to_remove),
            sandbox_root=sandbox_base,
        )

    async def _upload_skills(
        self,
        local_skills_dirs: list[tuple[str, str]],
        *,
        manifest: dict[str, Any] | None = None,
        existing_lock: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Upload skill files from local filesystem to sandbox.

        Uses a two-pass approach to fix override precedence:
        - Pass 1 (local I/O only): Walk all sources, later sources overwrite earlier
          ones for the same skill_name — each skill appears exactly once.
        - Pass 2 (sandbox I/O): Single rm, single mkdir, parallel per-skill batch uploads.

        Args:
            local_skills_dirs: List of (local_path, sandbox_path) tuples.
                Example: [("~/.ptc-agent/skills", "{working_directory}/skills")]
            manifest: Pre-computed skills manifest. If None, computed from local_skills_dirs.
            existing_lock: Previously downloaded lock entries, or None for fresh sandbox.

        Returns:
            Merged lock file dict if lock entries were written, else None.
        """
        assert self.runtime is not None
        sandbox = self.runtime

        if manifest is None:
            local_roots = [local_dir for local_dir, _ in local_skills_dirs]
            manifest = await self._compute_skills_module(local_roots)

        if not manifest.get("files"):
            logger.debug("No skills found; skipping upload")
            return

        # Skills eligible for sandbox upload (exposure "ptc" or "both")
        sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

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
            await self._runtime_call(
                sandbox.exec,
                rm_cmd,
                retry_policy=RetryPolicy.SAFE,
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
            await self._runtime_call(
                sandbox.exec,
                mkdir_cmd,
                retry_policy=RetryPolicy.SAFE,
            )

        # 3. Parallel per-skill batch uploads — no race since planning collapsed duplicates
        upload_coros = []
        for plan in final_skills.values():
            if plan.files:
                batch = [
                    (str(fp), dest)
                    for fp, dest in plan.files
                ]
                upload_coros.append(
                    self._runtime_call(
                        sandbox.upload_files,
                        batch,
                        retry_policy=RetryPolicy.SAFE,
                    )
                )
        if upload_coros:
            await asyncio.gather(*upload_coros)

        logger.debug(
            "Uploaded skills to sandbox",
            skill_count=len(final_skills),
            file_count=len(manifest.get("files", {})),
        )

        # --- Lock file merge + write ---
        # Build platform lock entries from the manifest
        platform_entries = {}
        skills_metadata = manifest.get("skills", {})
        for skill_name, skill_meta in skills_metadata.items():
            lock_entry = skill_meta.get("lock_entry")
            if lock_entry:
                platform_entries[skill_name] = lock_entry

        if platform_entries or existing_lock:
            from ptc_agent.agent.middleware.skills.lock import (
                LOCK_FILENAME,
                merge_lock_files,
                serialize_skills_lock,
            )

            merged = merge_lock_files(platform_entries, existing_lock)
            lock_content = serialize_skills_lock(merged)

            # Write lock file to sandbox
            sandbox_base = local_skills_dirs[-1][1].rstrip("/")
            lock_path = f"{sandbox_base}/{LOCK_FILENAME}"
            await self._runtime_call(
                sandbox.upload_file,
                lock_content.encode("utf-8"),
                lock_path,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.debug(
                "Skills lock file written",
                path=lock_path,
                platform_count=len(platform_entries),
                user_count=sum(
                    1
                    for e in merged["skills"].values()
                    if e.get("owner") == "user"
                ),
            )
            return dict(merged)

        return None

    async def _install_dependencies(self) -> None:
        """Install required Python packages in sandbox (no-snapshot fallback)."""
        logger.info("Installing dependencies (no snapshot)")

        # yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14.
        # Override resolves the conflict (tested, yfinance works with 0.14+).
        install_cmd = (
            "echo 'curl_cffi>=0.14' > /tmp/_overrides.txt && "
            f"uv pip install -q --override /tmp/_overrides.txt {' '.join(DEFAULT_DEPENDENCIES)} && "
            "rm -f /tmp/_overrides.txt"
        )

        try:
            assert self.runtime is not None
            result = await self._runtime_call(
                self.runtime.exec,
                install_cmd,
                retry_policy=RetryPolicy.SAFE,
            )
            if result.exit_code != 0:
                logger.warning(
                    "Dependency install exited with non-zero code",
                    exit_code=result.exit_code,
                    output=result.stdout[:500],
                )
            else:
                logger.info("Dependencies installed")
        except OSError as e:
            logger.error(f"Failed to install dependencies: {e}")
            raise

        # Install Scrapling browsers (Camoufox for StealthyFetcher)
        try:
            result = await self._runtime_call(
                self.runtime.exec,
                "scrapling install",
                retry_policy=RetryPolicy.SAFE,
            )
            if result.exit_code != 0:
                logger.warning(
                    "Scrapling browser install failed",
                    output=result.stdout[:300] if result.stdout else "",
                )
            else:
                logger.info("Scrapling browsers installed")
        except Exception as e:
            logger.warning(f"Scrapling browser install skipped: {e}")

    async def _install_tool_modules(self) -> None:
        """Generate and install tool modules from MCP servers."""
        logger.debug("Installing tool modules")

        # Get work directory (set by _setup_workspace)
        work_dir = self._work_dir

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

        assert self.runtime is not None

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
                    await self._runtime_call(
                        self.runtime.exec,
                        rm_cmd,
                        retry_policy=RetryPolicy.SAFE,
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
        await self._runtime_call(
            self.runtime.exec,
            mkdir_cmd,
            retry_policy=RetryPolicy.SAFE,
        )

        # Batch upload — single HTTP request for all generated content
        batch = [
            (content, path) for content, path, _ in uploads
        ]
        await self._runtime_call(
            self.runtime.upload_files,
            batch,
            retry_policy=RetryPolicy.SAFE,
        )
        # Log after batch
        for _, _, log_info in uploads:
            if log_info:
                msg, kwargs = log_info
                logger.debug(msg, **kwargs)

        server_count = len(tools_by_server)
        tool_count = sum(len(t) for t in tools_by_server.values())
        logger.info(
            "Tool modules installed",
            servers=server_count,
            tools=tool_count,
        )

    async def _start_internal_mcp_servers(self) -> None:
        """Start MCP servers as background processes inside sandbox."""
        logger.debug("Starting internal MCP servers")

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

                logger.debug(
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

                logger.debug(
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

        logger.debug(
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
            assert self.runtime is not None
            result = await self._runtime_call(
                self.runtime.exec,
                f"uv pip install -q {package}",
                retry_policy=RetryPolicy.SAFE,
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

        logger.debug(
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
                code_path = f".agents/threads/{thread_id}/code/{execution_id}.py"
                # Ensure per-thread code dir exists (lazy, once per thread)
                if thread_id not in self._thread_dirs_created:
                    await self._runtime_call(
                        self.runtime.exec,
                        f"mkdir -p {self.normalize_path(f'.agents/threads/{thread_id}/code')}",
                        retry_policy=RetryPolicy.SAFE,
                    )
                    self._thread_dirs_created.add(thread_id)
            else:
                code_path = f".system/code/{execution_id}.py"
            try:
                await self._runtime_call(
                    self.runtime.upload_file,
                    code.encode("utf-8"),
                    self.normalize_path(code_path),
                    retry_policy=RetryPolicy.SAFE,
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
            work_dir = await self.runtime.fetch_working_dir()

            internal_dir = f"{work_dir}/_internal"
            exec_env = {"PYTHONPATH": f"{work_dir}:{internal_dir}/src:{internal_dir}"}

            # Use code_run() for native artifact support (captures matplotlib charts)
            result = await self._runtime_call(
                self.runtime.code_run,
                code,
                env=exec_env,
                timeout=timeout_val,
                retry_policy=RetryPolicy.UNSAFE,
                total_timeout=timeout_val + 30,
            )

            stdout = result.stdout
            stderr = result.stderr
            exit_code = result.exit_code
            success = exit_code == 0

            # Extract charts from artifacts
            charts = []
            for artifact in result.artifacts:
                charts.append(
                    ChartData(
                        type=artifact.type,
                        title=artifact.name or "",
                        png_base64=artifact.data if artifact.data else None,
                        elements=[],
                    )
                )
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

    @property
    def proxy_domain(self) -> str | None:
        """Hostname of the sandbox proxy, or None if unavailable."""
        if self.runtime is None:
            return None
        return self.runtime.proxy_domain

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """Get a signed preview URL for a service running on the given port.

        Args:
            port: Port number (3000-9999) the service is listening on.
            expires_in: URL expiry in seconds (default: 3600 = 1 hour).

        Returns:
            PreviewInfo with url and token.
        """
        await self._wait_ready()
        assert self.runtime is not None
        return await self._runtime_call(
            self.runtime.get_preview_url,
            port,
            expires_in,
            retry_policy=RetryPolicy.SAFE,
        )

    async def get_preview_link(self, port: int) -> PreviewInfo:
        """Get a standard preview URL with header-based auth token.

        Results are cached per-port since the standard URL doesn't change
        while the sandbox is running. Cache is cleared on sandbox restart.
        """
        cached = self._preview_link_cache.get(port)
        if cached is not None:
            return cached
        await self._wait_ready()
        assert self.runtime is not None
        result = await self._runtime_call(
            self.runtime.get_preview_link,
            port,
            retry_policy=RetryPolicy.SAFE,
        )
        self._preview_link_cache[port] = result
        return result

    async def start_preview_server(self, command: str, port: int) -> str:
        """Start a command in a dedicated per-port session for preview URL serving.

        Each port gets its own Daytona session so blocking server commands
        (e.g. ``python -m http.server``) don't interfere with each other.
        If a session for this port already exists the old one is deleted first.

        Returns:
            The command ID from the session.
        """
        await self._wait_ready()
        assert self.runtime is not None

        session_id = f"preview-{port}"

        # Tear down stale session for this port if one exists
        if port in self._preview_sessions:
            old_sid, _old_cmd = self._preview_sessions[port]
            try:
                await self._runtime_call(
                    self.runtime.delete_session,
                    old_sid,
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception:
                logger.debug("Stale preview session cleanup failed", port=port)
            del self._preview_sessions[port]

        try:
            await self._runtime_call(
                self.runtime.create_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as e:
            if "already exists" in str(e).lower():
                # Stale session from a previous server process — delete and
                # recreate to avoid inheriting a running command from the old
                # session (same pattern as _create_bg_session).
                try:
                    await self._runtime_call(
                        self.runtime.delete_session,
                        session_id,
                        retry_policy=RetryPolicy.SAFE,
                    )
                    await self._runtime_call(
                        self.runtime.create_session,
                        session_id,
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    logger.debug(
                        "Stale preview session cleanup failed, reusing",
                        session_id=session_id,
                    )
            else:
                raise

        result = await self._runtime_call(
            self.runtime.session_execute,
            session_id,
            command,
            run_async=True,
            retry_policy=RetryPolicy.UNSAFE,
            total_timeout=30,
        )
        self._preview_sessions[port] = (session_id, result.cmd_id)
        logger.info(
            "Preview server started",
            cmd_id=result.cmd_id,
            session_id=session_id,
            port=port,
        )
        return result.cmd_id

    async def _is_preview_reachable(self, port: int, *, timeout: float = 3.0) -> bool:
        """Check if a preview port is reachable via the Daytona proxy.

        Uses the preview link (proxy URL + auth headers) to verify the server
        is accessible from outside the sandbox — not just locally.  A server
        binding to 127.0.0.1 passes an in-sandbox ``/dev/tcp`` check but
        returns 502 through the proxy.
        """
        import httpx

        try:
            link = await self.get_preview_link(port)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.head(
                    link.url,
                    headers=link.auth_headers,
                    follow_redirects=True,
                )
                # 4xx means the server IS running (path not found, etc.)
                # 5xx (especially 502) means the proxy can't reach the backend
                return 200 <= resp.status_code < 500 and resp.status_code != 502
        except Exception:
            return False

    async def start_and_get_preview_url(
        self,
        command: str,
        port: int,
        *,
        expires_in: int = 3600,
        startup_timeout: float = 10.0,
    ) -> PreviewInfo:
        """Start a server command in background and return a signed preview URL.

        Combines start_preview_server + port readiness poll + get_preview_url.
        If the port is already reachable through the Daytona proxy the server
        start is skipped entirely, making this method safe to call repeatedly.

        Polls for up to ``startup_timeout`` seconds to confirm the port is
        actually listening before generating the URL.  If the port never
        becomes reachable the URL is still returned — the frontend
        health-check polling handles dead-server detection.

        If the server command fails (e.g. port already in use), the preview
        URL is still generated — the existing server keeps serving.
        """
        await self._wait_ready()
        assert self.runtime is not None

        if port not in self._preview_locks:
            self._preview_locks[port] = asyncio.Lock()
        async with self._preview_locks[port]:
            # Quick probe: is the server already reachable through the proxy?
            # This catches the common case where the server is already running
            # and avoids an unnecessary (destructive) session teardown + restart.
            # We check the proxy — not an in-sandbox /dev/tcp — because a server
            # binding to 127.0.0.1 would pass the in-sandbox check but return 502
            # through the proxy.
            if await self._is_preview_reachable(port):
                logger.info("Preview already reachable via proxy, skipping server start", port=port)
                return await self.get_preview_url(port, expires_in=expires_in)

            try:
                await self.start_preview_server(command, port)
            except Exception as e:
                logger.warning("Failed to start preview server", command=command, error=str(e))

            # Poll until the port is listening.
            # Uses bash built-in /dev/tcp (no external tools like nc needed) via
            # a single lightweight runtime.exec call with an internal retry loop.
            max_attempts = max(int(startup_timeout / 0.5), 1)
            try:
                result = await self._runtime_call(
                    self.runtime.exec,
                    f"bash -c 'for i in $(seq 1 {max_attempts}); do"
                    f" (echo > /dev/tcp/localhost/{port}) 2>/dev/null && echo READY && exit 0;"
                    f" sleep 0.5; done; echo TIMEOUT'",
                    timeout=int(startup_timeout) + 5,
                    retry_policy=RetryPolicy.SAFE,
                )
                if "READY" in result.stdout:
                    logger.info("Preview server port ready", port=port)
                else:
                    logger.warning(
                        "Preview server port not reachable after startup timeout",
                        port=port,
                        startup_timeout=startup_timeout,
                    )
            except Exception:
                logger.warning(
                    "Port readiness check failed, proceeding anyway",
                    port=port,
                    exc_info=True,
                )

            return await self.get_preview_url(port, expires_in=expires_in)

    _MAX_BG_SESSIONS = 20

    async def _evict_finished_bg_sessions(self) -> None:
        """Evict finished background sessions to stay under the cap."""
        assert self.runtime is not None
        # Collect finished sessions (skip sentinel keys)
        finished: list[str] = []
        for cmd_id, sid in list(self._bg_sessions.items()):
            if cmd_id.startswith("_pending:"):
                continue
            try:
                result = await self._runtime_call(
                    self.runtime.session_command_logs,
                    sid, cmd_id,
                    retry_policy=RetryPolicy.SAFE,
                )
                if result.exit_code is not None:
                    finished.append(cmd_id)
            except Exception:
                # Can't check status (e.g. sandbox restarted) — treat as
                # finished to avoid zombie entries that permanently block the cap.
                finished.append(cmd_id)
        # Delete finished sessions
        for cmd_id in finished:
            sid = self._bg_sessions.pop(cmd_id, None)
            if sid:
                try:
                    await self._runtime_call(
                        self.runtime.delete_session, sid,
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    logger.debug("Evict bg session failed", session_id=sid)

    async def _create_bg_session(self, label: str) -> str:
        """Create a dedicated session for a background command.

        Each background command gets its own Daytona session so blocking
        commands don't prevent subsequent ones from executing.
        Evicts finished sessions when the cap is reached.
        """
        await self._wait_ready()
        assert self.runtime is not None

        # Evict finished sessions if at or above the cap
        active_count = sum(1 for k in self._bg_sessions if not k.startswith("_pending:"))
        if active_count >= self._MAX_BG_SESSIONS:
            await self._evict_finished_bg_sessions()

        session_id = f"bg-{label}"
        try:
            await self._runtime_call(
                self.runtime.create_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as e:
            if "already exists" in str(e).lower():
                # Stale session from a previous run — delete and recreate
                # to avoid inheriting env/state from the old session
                try:
                    await self._runtime_call(
                        self.runtime.delete_session,
                        session_id,
                        retry_policy=RetryPolicy.SAFE,
                    )
                    await self._runtime_call(
                        self.runtime.create_session,
                        session_id,
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    logger.debug("Stale bg session cleanup failed, reusing", session_id=session_id)
            else:
                raise
        return session_id

    async def get_background_command_status(self, cmd_id: str) -> dict[str, Any]:
        """Get status and logs for a background command.

        Args:
            cmd_id: Command ID returned when the background command was started.

        Returns:
            Dict with keys: success, is_running, exit_code, stdout, stderr, cmd_id.
        """
        await self._wait_ready()
        assert self.runtime is not None

        session_id = self._bg_sessions.get(cmd_id)
        if not session_id:
            return {
                "success": False,
                "is_running": False,
                "exit_code": None,
                "stdout": "",
                "stderr": "No background session found for this command",
                "cmd_id": cmd_id,
            }

        result: SessionCommandResult = await self._runtime_call(
            self.runtime.session_command_logs,
            session_id,
            cmd_id,
            retry_policy=RetryPolicy.SAFE,
        )
        is_running = result.exit_code is None

        # Auto-clean: if the command finished (e.g. killed via pkill), tear
        # down the orphaned session so it doesn't leak on the Daytona side.
        if not is_running:
            sid = self._bg_sessions.pop(cmd_id, None)
            if sid:
                try:
                    await self._runtime_call(
                        self.runtime.delete_session, sid,
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    logger.debug("Auto-clean bg session failed", session_id=sid)

        return {
            "success": not is_running and result.exit_code == 0,
            "is_running": is_running,
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "cmd_id": cmd_id,
        }

    async def stop_background_command(self, cmd_id: str) -> bool:
        """Stop a background command by deleting its session.

        Returns True if the session was found and deleted.
        """
        session_id = self._bg_sessions.get(cmd_id)
        if not session_id:
            return False
        await self._wait_ready()
        assert self.runtime is not None
        try:
            await self._runtime_call(
                self.runtime.delete_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception:
            logger.warning("Failed to delete bg session", session_id=session_id)
            self._bg_sessions.pop(cmd_id, None)
            return False
        self._bg_sessions.pop(cmd_id, None)
        return True

    async def get_preview_server_logs(self, port: int) -> dict[str, Any]:
        """Get logs for the preview server running on the given port.

        Returns:
            Dict with keys: success, is_running, stdout, stderr, port.
        """
        entry = self._preview_sessions.get(port)
        if not entry:
            return {
                "success": False,
                "is_running": False,
                "stdout": "",
                "stderr": f"No preview session for port {port}",
                "port": port,
            }
        session_id, cmd_id = entry
        await self._wait_ready()
        assert self.runtime is not None
        try:
            result: SessionCommandResult = await self._runtime_call(
                self.runtime.session_command_logs,
                session_id,
                cmd_id,
                retry_policy=RetryPolicy.SAFE,
            )
            is_running = result.exit_code is None
            return {
                "success": True,
                "is_running": is_running,
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "port": port,
            }
        except Exception as e:
            return {
                "success": False,
                "is_running": False,
                "stdout": "",
                "stderr": f"Failed to get logs: {e!s}",
                "port": port,
            }

    async def stop_preview_server(self, port: int) -> bool:
        """Stop the preview server on the given port by deleting its session.

        Returns True if the session was found and deleted.
        """
        entry = self._preview_sessions.get(port)
        if not entry:
            return False
        session_id, _cmd_id = entry
        await self._wait_ready()
        assert self.runtime is not None
        try:
            await self._runtime_call(
                self.runtime.delete_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.info("Preview server stopped", port=port, session_id=session_id)
        except Exception:
            logger.debug("Failed to delete preview session", session_id=session_id)
        self._preview_sessions.pop(port, None)
        return True

    async def execute_bash_command(
        self,
        command: str,
        working_dir: str | None = None,
        timeout: int = 60,
        *,
        background: bool = False,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a bash command in the sandbox.

        Args:
            command: Bash command to execute
            working_dir: Working directory for command execution (default: sandbox working dir)
            timeout: Maximum execution time in seconds (default: 60)
            background: Run command in background
            thread_id: Optional thread ID (first 8 chars) for thread-scoped script storage

        Returns:
            Dictionary with success, stdout, stderr, exit_code, bash_id, command_hash
        """
        if working_dir is None:
            working_dir = self._work_dir
        await self._wait_ready()
        start_time = time.time()

        try:
            # Generate bash execution ID for tracking
            self.bash_execution_count += 1
            bash_id = f"bash_{self.bash_execution_count:04d}"
            command_hash = hashlib.sha256(command.encode()).hexdigest()[:16]
            from datetime import UTC, datetime

            timestamp = datetime.now(tz=UTC).isoformat()

            logger.debug(
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
                script_relative_path = f".agents/threads/{thread_id}/code/{bash_id}.sh"
                if thread_id not in self._thread_dirs_created:
                    await self._runtime_call(
                        self.runtime.exec,
                        f"mkdir -p {self.normalize_path(f'.agents/threads/{thread_id}/code')}",
                        retry_policy=RetryPolicy.SAFE,
                    )
                    self._thread_dirs_created.add(thread_id)
            else:
                script_relative_path = f".system/code/{bash_id}.sh"

            try:
                assert self.runtime is not None
                await self._runtime_call(
                    self.runtime.upload_file,
                    script_content.encode("utf-8"),
                    self.normalize_path(script_relative_path),
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception as upload_err:
                logger.warning(
                    "Failed to save bash script to sandbox (non-fatal)",
                    bash_id=bash_id,
                    error=str(upload_err),
                )

            # Background execution via dedicated Daytona session per command
            if background:
                session_id = await self._create_bg_session(bash_id)
                assert self.runtime is not None
                # Track immediately so cleanup() can find it if execute fails
                sentinel_key = f"_pending:{session_id}"
                self._bg_sessions[sentinel_key] = session_id
                try:
                    result = await self._runtime_call(
                        self.runtime.session_execute,
                        session_id,
                        full_command,
                        run_async=True,
                        retry_policy=RetryPolicy.UNSAFE,
                        total_timeout=30,
                    )
                except Exception:
                    # Clean up the session to avoid leaking on the Daytona side
                    try:
                        await self._runtime_call(
                            self.runtime.delete_session,
                            session_id,
                            retry_policy=RetryPolicy.SAFE,
                        )
                    except Exception:
                        logger.debug("Failed to clean up bg session after execute failure", session_id=session_id)
                    self._bg_sessions.pop(sentinel_key, None)
                    raise
                # Replace sentinel with real cmd_id key
                self._bg_sessions.pop(sentinel_key, None)
                self._bg_sessions[result.cmd_id] = session_id
                logger.debug(
                    "Background command started",
                    bash_id=bash_id,
                    cmd_id=result.cmd_id,
                    session_id=session_id,
                )
                return {
                    "success": True,
                    "stdout": (
                        f"Background command started (command_id: {result.cmd_id})\n"
                        f"Use BashOutput tool with command_id=\"{result.cmd_id}\" to check output and status."
                    ),
                    "stderr": "",
                    "exit_code": 0,
                    "bash_id": bash_id,
                    "command_hash": command_hash,
                }

            # Execute directly via process.exec — no file upload dependency
            assert self.runtime is not None
            exec_result = await self._runtime_call(
                self.runtime.exec,
                full_command,
                timeout=timeout,
                retry_policy=RetryPolicy.UNSAFE,
                total_timeout=timeout + 30,
            )

            exit_code = exec_result.exit_code
            stdout = exec_result.stdout

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
                "stderr": "",  # runtime.exec() returns combined output in stdout only
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
                "bash_id": locals().get("bash_id"),
                "command_hash": None,
            }

    async def _list_result_files(self) -> list[str]:
        """List files in the results directory.

        Returns:
            List of file paths relative to workspace (e.g., "results/file.csv")
        """
        try:
            assert self.runtime is not None
            file_infos = await self._runtime_call(
                self.runtime.list_files,
                "results",
                retry_policy=RetryPolicy.SAFE,
            )
            if not file_infos:
                return []
            # Return paths relative to workspace, not just filenames
            return [
                f"results/{_entry_name(f)}"
                for f in file_infos
            ]
        except (OSError, AttributeError) as e:
            logger.warning(f"Error listing result files: {e}")
            return []

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        """Download raw bytes from sandbox.

        This path is safe to retry automatically. Concurrency is bounded by a
        semaphore to limit event-loop pressure from concurrent downloads.

        Returns:
            Bytes if downloaded, or None if missing.

        Raises:
            SandboxTransientError: If a transient sandbox transport error persists.
        """
        await self._wait_ready()

        try:
            async with self._download_semaphore:
                return await self._runtime_call(
                    self.runtime.download_file,
                    filepath,
                    retry_policy=RetryPolicy.SAFE,
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

        # Normalize the path to ensure it's absolute for the sandbox runtime
        normalized_path = self.normalize_path(filepath)

        if self.config.filesystem.enable_path_validation and not self.validate_path(
            normalized_path
        ):
            logger.error(f"Access denied: {filepath} is not in allowed directories")
            return False

        try:
            assert self.runtime is not None
            # Use normalized path for upload - runtime expects absolute paths
            await self._runtime_call(
                self.runtime.upload_file,
                content,
                normalized_path,
                retry_policy=RetryPolicy.SAFE,
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

        Uses sed via process.exec to extract lines server-side, avoiding
        full-file download through the multipart parser hot path.

        Args:
            file_path: Path to the file.
            offset: Line offset (0-indexed).
            limit: Maximum number of lines.
        """
        await self._wait_ready()
        normalized = self.normalize_path(file_path)
        start = max(0, offset)
        start_line = start + 1  # sed is 1-indexed
        end_line = start + limit
        cmd = f"sed -n '{start_line},{end_line}p' {shlex.quote(normalized)}"

        try:
            result = await self._runtime_call(
                self.runtime.exec,
                cmd,
                timeout=30,
                retry_policy=RetryPolicy.SAFE,
            )
            if result.exit_code != 0:
                return await self._aread_file_range_fallback(file_path, offset, limit)
            return result.stdout or ""
        except SandboxTransientError:
            raise
        except Exception as e:
            logger.debug("Failed to read file range", filepath=file_path, error=str(e))
            return await self._aread_file_range_fallback(file_path, offset, limit)

    async def _aread_file_range_fallback(
        self, file_path: str, offset: int, limit: int
    ) -> str | None:
        """Fallback: download full file and slice (original behavior)."""
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
        # Use live working directory (updated by fetch_working_dir)
        work_dir = self._work_dir

        if path in (None, "", ".", "/"):
            return work_dir

        path = path.strip()

        # Already in allowed directories - keep as is (just normalize . and ..)
        for allowed_dir in self.config.filesystem.allowed_directories:
            if path.startswith(allowed_dir):
                return str(Path(path))

        # Virtual absolute path: /foo -> {working_directory}/foo
        if path.startswith("/"):
            return str(Path(f"{work_dir}{path}"))

        # Relative path: foo -> {working_directory}/foo
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
        # Use live working directory (updated by fetch_working_dir)
        work_dir = self._work_dir

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

            assert self.runtime is not None
            file_infos = await self._runtime_call(
                self.runtime.list_files,
                directory,
                retry_policy=RetryPolicy.SAFE,
            )
            if not file_infos:
                return []

            results: list[dict[str, Any]] = []
            for entry in file_infos:
                name = _entry_name(entry)
                is_dir = _entry_is_dir(entry)
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

            assert self.runtime is not None
            await self._runtime_call(
                self.runtime.exec,
                f"mkdir -p {shlex.quote(dirpath)}",
                retry_policy=RetryPolicy.SAFE,
            )
            return True
        except Exception as e:
            logger.debug("Failed to create directory", dirpath=dirpath, error=str(e))
            return False

    async def acreate_directories(self, dirpaths: Iterable[str]) -> bool:
        """Create multiple directories in a single ``mkdir -p`` exec call.

        Much faster than N separate ``acreate_directory`` calls for bulk
        setup (e.g. file restore), collapsing N round-trips into one.
        ``mkdir -p`` is idempotent. Returns False if any validation or
        exec fails; callers can fall back to per-dir creates.
        """
        paths = [p for p in dirpaths if p]
        if not paths:
            return True

        await self._wait_ready()

        if self.config.filesystem.enable_path_validation:
            for p in paths:
                if not self.validate_path(p):
                    logger.error(f"Access denied: {p} is not in allowed directories")
                    return False

        try:
            assert self.runtime is not None
            quoted = " ".join(shlex.quote(p) for p in paths)
            await self._runtime_call(
                self.runtime.exec,
                f"mkdir -p {quoted}",
                retry_policy=RetryPolicy.SAFE,
            )
            return True
        except Exception as e:
            logger.debug(
                "Failed to bulk-create directories",
                count=len(paths),
                error=str(e),
            )
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

            assert self.runtime is not None
            result = await self._runtime_call(
                self.runtime.exec,
                cmd,
                timeout=30,
                retry_policy=RetryPolicy.SAFE,
            )

            output = result.stdout.strip() if result.stdout else ""
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

            cmd_str = " ".join(shlex.quote(c) for c in cmd)
            assert self.runtime is not None
            result = await self._runtime_call(
                self.runtime.exec,
                cmd_str,
                timeout=60,
                retry_policy=RetryPolicy.SAFE,
            )

            output = result.stdout.strip() if result.stdout else ""
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
        await self._cancel_init_task()

        logger.info("Cleaning up sandbox", sandbox_id=self.sandbox_id)

        try:
            if self.runtime:
                # Clean up all managed sessions (preview + background)
                all_sessions = [sid for sid, _ in self._preview_sessions.values()] + list(self._bg_sessions.values())
                for sid in dict.fromkeys(all_sessions):  # deduplicate
                    try:
                        await self._runtime_call(
                            self.runtime.delete_session, sid,
                            retry_policy=RetryPolicy.SAFE,
                        )
                    except Exception:
                        logger.debug("Failed to delete session", session_id=sid)
                self._preview_sessions.clear()
                self._bg_sessions.clear()

                try:
                    await self._runtime_call(
                        self.runtime.delete,
                        retry_policy=RetryPolicy.SAFE,
                    )
                    logger.info("Sandbox deleted", sandbox_id=self.sandbox_id)
                except Exception as e:
                    logger.error(f"Error deleting sandbox: {e}")
        finally:
            self.runtime = None
            self.sandbox_id = None
            await self.close()

    async def close(self) -> None:
        """Release provider resources (HTTP client, etc.)."""
        try:
            await self.provider.close()
        except Exception as e:
            logger.debug("Failed to close provider", error=str(e))

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
