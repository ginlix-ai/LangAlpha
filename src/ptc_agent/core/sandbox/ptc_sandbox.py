"""PTC Sandbox - Manages sandbox for Programmatic Tool Calling execution."""

import asyncio
import json
import shlex
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from types import TracebackType
from typing import Any

import structlog


from ptc_agent.config.core import CoreConfig
from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES, SNAPSHOT_PYTHON_VERSION
from ptc_agent.core.sandbox.platform_secrets import build_platform_secret_bindings
from ptc_agent.core.sandbox.providers import create_provider
from ptc_agent.core.sandbox.retry import RetryPolicy, async_retry_with_backoff
from ptc_agent.core.sandbox.runtime import (
    PreviewInfo,
    RuntimeState,
    SandboxGoneError,
    SandboxRuntime,
    SandboxTransientError,
)

from ..mcp_registry import MCPRegistry
from ..mcp_sanitize import (
    is_user_server,
)
from ..tool_generator import ToolFunctionGenerator

from ptc_agent.core.sandbox._shared import (
    ExecutionResult,
    SyncResult,
    _internal_package_files,
)
from ptc_agent.core.sandbox import assets as _assets
from ptc_agent.core.sandbox import execution as _execution
from ptc_agent.core.sandbox import files as _files
from ptc_agent.core.sandbox import mcp_setup as _mcp_setup
from ptc_agent.core.sandbox import sessions as _sessions

logger = structlog.get_logger(__name__)


class PTCSandbox:
    """Manages sandbox for Programmatic Tool Calling (PTC) execution."""

    SNAPSHOT_PYTHON_VERSION = SNAPSHOT_PYTHON_VERSION
    DEFAULT_DEPENDENCIES = DEFAULT_DEPENDENCIES

    # Fallback only, for a mint response that carries no expires_in. The live
    # threshold derives from it (see _token_needs_refresh): the TTL is set
    # platform-side, so hardcoding it here lets the two drift apart silently.
    TOKEN_FRESHNESS_SECONDS = 10 * 60
    # Re-upload once the sandbox's copy is this far through the token's life.
    TOKEN_REFRESH_FRACTION = 0.6

    def __init__(
        self, config: CoreConfig, mcp_registry: MCPRegistry | None = None
    ) -> None:
        """``mcp_registry`` may be None when reconnecting to an existing sandbox."""
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
        # Per-command MCP provenance trace paths for background Bash (cmd_id →
        # trace_path). Harvested when get_background_command_status observes the
        # command finished, so a backgrounded script's MCP calls are recorded
        # too (the foreground/ExecuteCode path harvests inline).
        self._bg_trace_paths: dict[str, str] = {}
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

    # ── Effective vs built-in server views ───────────────────────────────
    #
    # ``self.config.mcp.servers`` holds the per-workspace EFFECTIVE set the
    # WorkspaceManager installs at session build: built-ins (minus disables) plus
    # the workspace's user (``source='workspace'``) servers. Most host-side
    # operations must see ONLY the built-ins — user stdio servers are fetched by
    # npx/uvx at call time inside the sandbox, never pre-installed/pre-started on
    # the host path, and their secrets resolve vault-only (never host os.environ).
    # A zero-user-server workspace has effective == built-ins, so both views are
    # the same objects and byte-identical to pre-change behavior (regression #1).

    def _builtin_servers(self) -> list:
        """Built-in servers only (``source != 'workspace'``) from the effective set."""
        return [s for s in self.config.mcp.servers if not is_user_server(s)]

    def _user_servers(self) -> list:
        """User-configured servers (``source == 'workspace'``) from the effective set."""
        return [s for s in self.config.mcp.servers if is_user_server(s)]

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
        # Built-ins only: user-server npx/uvx packages are fetched at call time
        # inside the sandbox, never pre-installed globally on the host path.
        mcp_packages = []
        for server in self._builtin_servers():
            if not server.enabled:
                continue
            if server.transport == "stdio" and server.command == "npx":
                # Extract package name from npx arguments
                # Format: ["npx", "-y", "package-name", ...]
                if len(server.args) >= 2 and server.args[0] == "-y":
                    mcp_packages.append(server.args[1])
        return mcp_packages


    def _build_sandbox_env_vars(
        self,
        platform_secret_bindings: dict[str, str],
    ) -> dict[str, str]:
        """Build environment variables to inject at sandbox creation time.

        Resolves MCP server env vars (${VAR} placeholders from host) and
        GitHub bot credentials so they're available to all sandbox processes.
        Vars named in ``platform_secret_bindings`` are excluded — Daytona
        mounts those as managed Secrets.
        """
        import os

        env_vars: dict[str, str] = {
            # Playwright browsers are installed to /usr/local/ms-playwright
            # in the snapshot image; tell the Python package where to find them.
            "PLAYWRIGHT_BROWSERS_PATH": "/usr/local/ms-playwright",
            # Tell the in-sandbox ginlix-data client the exact path the host
            # uploads the token file to. It must not key off $HOME — Daytona
            # runs as root ($HOME=/root) while the working dir is /home/workspace.
            "GINLIX_TOKEN_FILE": self._token_file_path,
        }

        # MCP server env vars (resolve ${VAR} placeholders from host).
        # Built-ins ONLY: a user server's env must never be injected into the
        # sandbox os.environ — its ${vault:NAME} refs resolve vault-only at
        # call time, so injecting host values here would leak platform creds.
        for server in self._builtin_servers():
            if not server.enabled:
                continue
            if hasattr(server, "env") and server.env:
                for key, value in server.env.items():
                    if key == "INTERNAL_SERVICE_TOKEN":
                        continue  # Never inject platform token into sandbox
                    if key in platform_secret_bindings:
                        # Daytona injects an opaque placeholder for managed
                        # platform Secrets. Never put the real host value in
                        # ordinary sandbox environment variables as fallback.
                        continue
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

    async def setup_sandbox_workspace(
        self,
        *,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
    ) -> str | None:
        """Create sandbox and setup workspace directories.

        Can run concurrently with MCP registry connection since it doesn't
        require the registry.

        Args:
            tier: Resource tier to size the new sandbox at (provider-resolved).
            auto_stop_minutes: Auto-stop interval override in minutes (0 for
                always-on); ``None`` uses the provider default.

        Returns:
            snapshot_name if used, None otherwise
        """
        logger.info("Setting up sandbox workspace")

        # Build env vars once — injected at sandbox creation time so they're
        # available to all processes (Python, bash, MCP servers)
        platform_secret_bindings = build_platform_secret_bindings(self.config)
        sandbox_env = self._build_sandbox_env_vars(platform_secret_bindings)

        # Create sandbox via provider (handles snapshot logic internally)
        mcp_packages = self._get_mcp_packages()
        self.runtime = await self._runtime_call(
            self.provider.create,
            env_vars=sandbox_env or None,
            platform_secret_bindings=platform_secret_bindings or None,
            mcp_packages=mcp_packages,
            tier=tier,
            auto_stop_minutes=auto_stop_minutes,
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

        # Sandboxes are remote, so these must stay publicly reachable even when
        # the host reaches the same services over a private address.
        public_base = os.getenv("PUBLIC_API_BASE_URL", "").rstrip("/")

        token_data = json.dumps(
            {
                "access_token": tokens["access_token"],
                "refresh_token": tokens["refresh_token"],
                "client_id": tokens["client_id"],
                "auth_service_url": public_base or os.getenv("AUTH_SERVICE_URL", ""),
                "ginlix_data_url": public_base or os.getenv("GINLIX_DATA_URL", ""),
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
        self._bg_trace_paths.clear()
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
            metadata = await self.runtime.get_metadata()
            if metadata.get("recoverable") is False:
                raise SandboxGoneError(
                    sandbox_id,
                    "archived sandbox is no longer recoverable",
                )
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
            f"{work_dir}/.system/trace",
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

        # Sweep orphaned MCP trace files from a prior/reused session
        # (best-effort). Normal cleanup is per-execution in _collect_mcp_trace;
        # this bounds leakage when a run dies before its trace is collected.
        try:
            assert self.runtime is not None
            await self._runtime_call(
                self.runtime.exec,
                f"rm -f {shlex.quote(f'{work_dir}/.system/trace')}/*.jsonl",
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as e:
            logger.debug(f"MCP trace dir sweep skipped: {e}")

    async def _upload_internal_packages(self) -> None:
        """Mirror the ``_SANDBOX_INTERNAL_PACKAGES`` set into ``_internal/src/``.

        All-or-nothing: the whole set ships together, gated by the single
        ``internal_packages`` manifest module.
        """
        work_dir = self._work_dir
        internal_root = Path(f"{work_dir}/_internal/src")

        # Resolve local paths relative to config file directory if available.
        config_dir = getattr(self.config, "config_file_dir", None)
        repo_root = config_dir or Path.cwd()
        local_src_dir = (repo_root / "src").resolve()

        files = _internal_package_files(local_src_dir)
        if not files:
            logger.warning(
                "Skipping internal package upload - local src/__init__.py not found",
                src_dir=str(local_src_dir),
            )
            return

        assert self.runtime is not None
        sandbox = self.runtime

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

    # ── Unified manifest helpers ────────────────────────────────────────


    # ── Unified manifest I/O ─────────────────────────────────────────


    # ── Unified sync entry point ─────────────────────────────────────


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
        # Re-mint once the remote copy is most of the way through its life.
        ttl = tokens.get("expires_in") or 0
        threshold = (
            ttl * PTCSandbox.TOKEN_REFRESH_FRACTION
            if ttl > 0
            else PTCSandbox.TOKEN_FRESHNESS_SECONDS
        )
        minted_at = remote_token_mod.get("minted_at", 0)
        age = time.time() - minted_at
        if age > threshold:
            return True
        return False


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


    # Bound concurrent discovery so a burst of servers can't exhaust the
    # sandbox; one hung server still can't starve others (each runs isolated).
    _DISCOVERY_CONCURRENCY = 4
    # CLI exec ceiling — the in-sandbox client has its own 30s stdio cold-start
    # select timeout; give a little headroom for npx/uvx fetch + JSON write.
    _DISCOVERY_EXEC_TIMEOUT_S = 90


    @property
    def proxy_domain(self) -> str | None:
        """Hostname of the sandbox proxy, or None if unavailable."""
        if self.runtime is None:
            return None
        return self.runtime.proxy_domain


    _MAX_BG_SESSIONS = 20


    @staticmethod
    def _extract_sandbox_id(sandbox: object) -> str:
        """Extract a stable ID string from a sandbox object."""
        return sandbox.id if hasattr(sandbox, "id") else str(id(sandbox))


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
                self._bg_trace_paths.clear()

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

    # ------------------------------------------------------------------
    # Delegators — implementations live in the sibling function modules
    # (assets, mcp_setup, execution, sessions, files); each function takes
    # this sandbox as its explicit first argument.
    # ------------------------------------------------------------------

    # -- assets --

    def _compute_tool_schema_hash(self) -> str:
        return _assets._compute_tool_schema_hash(self)

    def _compute_user_mcp_config_hash(self) -> str:
        return _assets._compute_user_mcp_config_hash(self)

    async def _compute_skills_module(self, skill_roots: list[str]) -> dict[str, Any]:
        return await _assets._compute_skills_module(self, skill_roots)

    async def _compute_sandbox_manifest(
        self,
        *,
        skill_roots: list[str] | None = None,
        tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> dict[str, Any]:
        return await _assets._compute_sandbox_manifest(self, skill_roots=skill_roots, tokens=tokens, user_id=user_id, workspace_id=workspace_id)

    async def _read_unified_manifest(self) -> dict[str, Any] | None:
        return await _assets._read_unified_manifest(self)

    async def _write_unified_manifest(self, manifest: dict[str, Any]) -> None:
        return await _assets._write_unified_manifest(self, manifest)

    async def _cleanup_legacy_manifests(self) -> None:
        return await _assets._cleanup_legacy_manifests(self)

    async def _upload_mcp_server_files_impl(self) -> None:
        return await _assets._upload_mcp_server_files_impl(self)

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
        return await _assets.sync_sandbox_assets(self, skill_dirs=skill_dirs, reusing_sandbox=reusing_sandbox, force_refresh=force_refresh, tokens=tokens, user_id=user_id, workspace_id=workspace_id, on_progress=on_progress)

    async def _prune_disabled_tool_modules(self) -> None:
        return await _assets._prune_disabled_tool_modules(self)

    async def _collect_local_skill_names(
        self, local_skill_roots: list[str]
    ) -> set[str]:
        return await _assets._collect_local_skill_names(self, local_skill_roots)

    async def _download_skills_lock(
        self, sandbox_skills_base: str
    ) -> dict[str, Any] | None:
        return await _assets._download_skills_lock(self, sandbox_skills_base)

    def _build_complete_skills_cache(
        self,
        skills_mod: dict[str, Any],
        merged_lock: dict[str, Any],
        sandbox_skills_base: str,
    ) -> None:
        return _assets._build_complete_skills_cache(self, skills_mod, merged_lock, sandbox_skills_base)

    async def sync_skills_lock(self) -> None:
        return await _assets.sync_skills_lock(self)

    async def _prune_remote_skills(
        self,
        sandbox_base: str,
        local_skill_names: set[str],
        *,
        existing_lock: dict[str, Any] | None = None,
    ) -> None:
        return await _assets._prune_remote_skills(self, sandbox_base, local_skill_names, existing_lock=existing_lock)

    async def _upload_skills(
        self,
        local_skills_dirs: list[tuple[str, str]],
        *,
        manifest: dict[str, Any] | None = None,
        existing_lock: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        return await _assets._upload_skills(self, local_skills_dirs, manifest=manifest, existing_lock=existing_lock)


    # -- mcp_setup --

    async def _install_dependencies(self) -> None:
        return await _mcp_setup._install_dependencies(self)

    async def _upload_discovery_client(
        self, extra_servers: list[Any] | None = None
    ) -> str:
        return await _mcp_setup._upload_discovery_client(self, extra_servers)

    async def _install_tool_modules(self) -> None:
        return await _mcp_setup._install_tool_modules(self)

    async def discover_user_mcp_schemas(
        self, servers: list[Any]
    ) -> dict[str, dict[str, Any]]:
        return await _mcp_setup.discover_user_mcp_schemas(self, servers)

    async def _start_internal_mcp_servers(self) -> None:
        return await _mcp_setup._start_internal_mcp_servers(self)

    def _detect_missing_imports(self, stderr: str) -> list[str]:
        return _mcp_setup._detect_missing_imports(self, stderr)

    async def _install_package(self, package: str) -> bool:
        return await _mcp_setup._install_package(self, package)


    # -- execution --

    async def _collect_mcp_trace(self, trace_path: str) -> list[dict]:
        return await _execution._collect_mcp_trace(self, trace_path)

    async def execute(
        self,
        code: str,
        timeout: int | None = None,
        *,
        auto_install: bool = True,
        max_retries: int = 2,
        thread_id: str | None = None,
        _carry_mcp_trace: list[dict] | None = None,
    ) -> ExecutionResult:
        return await _execution.execute(self, code, timeout, auto_install=auto_install, max_retries=max_retries, thread_id=thread_id, _carry_mcp_trace=_carry_mcp_trace)

    async def execute_bash_command(
        self,
        command: str,
        working_dir: str | None = None,
        timeout: int = 60,
        *,
        background: bool = False,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        return await _execution.execute_bash_command(self, command, working_dir, timeout, background=background, thread_id=thread_id)

    def _build_trace_env_command(
        self, bash_id: str, full_command: str
    ) -> tuple[str, str]:
        return _execution._build_trace_env_command(self, bash_id, full_command)

    async def _list_result_files(self) -> list[str]:
        return await _execution._list_result_files(self)


    # -- sessions --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        return await _sessions.get_preview_url(self, port, expires_in)

    async def get_preview_link(self, port: int) -> PreviewInfo:
        return await _sessions.get_preview_link(self, port)

    async def start_preview_server(self, command: str, port: int) -> str:
        return await _sessions.start_preview_server(self, command, port)

    async def _is_preview_reachable(self, port: int, *, timeout: float = 3.0) -> bool:
        return await _sessions._is_preview_reachable(self, port, timeout=timeout)

    async def start_and_get_preview_url(
        self,
        command: str,
        port: int,
        *,
        expires_in: int = 3600,
        startup_timeout: float = 10.0,
    ) -> PreviewInfo:
        return await _sessions.start_and_get_preview_url(self, command, port, expires_in=expires_in, startup_timeout=startup_timeout)

    async def _evict_finished_bg_sessions(self) -> None:
        return await _sessions._evict_finished_bg_sessions(self)

    async def _create_bg_session(self, label: str) -> str:
        return await _sessions._create_bg_session(self, label)

    async def get_background_command_status(self, cmd_id: str) -> dict[str, Any]:
        return await _sessions.get_background_command_status(self, cmd_id)

    async def stop_background_command(self, cmd_id: str) -> bool:
        return await _sessions.stop_background_command(self, cmd_id)

    async def get_preview_server_logs(self, port: int) -> dict[str, Any]:
        return await _sessions.get_preview_server_logs(self, port)

    async def stop_preview_server(self, port: int) -> bool:
        return await _sessions.stop_preview_server(self, port)


    # -- files --

    def _normalize_search_path(self, path: str) -> str:
        return _files._normalize_search_path(self, path)

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        return await _files.adownload_file_bytes(self, filepath)

    async def aread_file_text(self, filepath: str) -> str | None:
        return await _files.aread_file_text(self, filepath)

    async def aupload_file_bytes(self, filepath: str, content: bytes) -> bool:
        return await _files.aupload_file_bytes(self, filepath, content)

    async def awrite_file_text(self, filepath: str, content: str) -> bool:
        return await _files.awrite_file_text(self, filepath, content)

    async def aread_file_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        return await _files.aread_file_range(self, file_path, offset, limit)

    async def _aread_file_range_fallback(
        self, file_path: str, offset: int, limit: int
    ) -> str | None:
        return await _files._aread_file_range_fallback(self, file_path, offset, limit)

    def normalize_path(self, path: str) -> str:
        return _files.normalize_path(self, path)

    def virtualize_path(self, path: str) -> str:
        return _files.virtualize_path(self, path)

    def validate_path(self, filepath: str) -> bool:
        return _files.validate_path(self, filepath)

    def validate_and_normalize_path(self, path: str) -> tuple[str, str | None]:
        return _files.validate_and_normalize_path(self, path)

    async def als_directory(self, directory: str = ".") -> list[dict[str, Any]]:
        return await _files.als_directory(self, directory)

    async def acreate_directory(self, dirpath: str) -> bool:
        return await _files.acreate_directory(self, dirpath)

    async def acreate_directories(self, dirpaths: Iterable[str]) -> bool:
        return await _files.acreate_directories(self, dirpaths)

    async def aedit_file_text(
        self,
        filepath: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        return await _files.aedit_file_text(self, filepath, old_string, new_string, replace_all=replace_all)

    def _validate_path_allow_denied(self, path: str) -> bool:
        return _files._validate_path_allow_denied(self, path)

    async def aglob_files(
        self, pattern: str, path: str = ".", *, allow_denied: bool = False
    ) -> list[str]:
        return await _files.aglob_files(self, pattern, path, allow_denied=allow_denied)

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
        return await _files.agrep_content(self, pattern, path, output_mode, glob, type, case_insensitive=case_insensitive, show_line_numbers=show_line_numbers, lines_after=lines_after, lines_before=lines_before, lines_context=lines_context, multiline=multiline, head_limit=head_limit, offset=offset)
