"""PTC Sandbox - Manages sandbox for Programmatic Tool Calling execution."""

import asyncio
import shlex
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from pathlib import Path
from types import TracebackType
from typing import Any

import structlog


from ptc_agent.config.core import CoreConfig
from ptc_agent.core.sandbox._defaults import (
    DEFAULT_DEPENDENCIES,
    SANDBOX_IMAGE_ENV,
)
from ptc_agent.core.sandbox.platform_secrets import (
    build_platform_secret_bindings,
    is_credential_env_name,
    platform_secrets_required,
)
from ptc_agent.core.sandbox.providers import create_provider
from ptc_agent.core.sandbox.providers._image_recipe import builtin_mcp_packages
from ptc_agent.core.sandbox.jobs import (
    Ended,
    Failed,
    JobOutcome,
    JobRef,
    JobSpec,
    Running,
    StoppedJob,
)
from ptc_agent.core.sandbox.livefs_mount import HistoryFiles, MountHandle, TranscriptReach
from ptc_agent.core.sandbox.retry import RetryPolicy, async_retry_with_backoff
from ptc_agent.core.sandbox.runtime import (
    HostUnavailablePolicy,
    PreviewInfo,
    RuntimeState,
    SandboxGoneError,
    SandboxRuntime,
    SandboxTransientError,
)
from ptc_agent.core.sandbox.vault_helper import VAULT_MODULE_SOURCE

from ..mcp_registry import MCPRegistry
from ..mcp_sanitize import (
    is_untrusted_server,
)
from ..paths import SandboxLayout, WorkspaceLayout
from ..project_context import ProjectContext, current_project
from ..tool_generator import ToolFunctionGenerator

from ptc_agent.core.sandbox._shared import (
    REPLACE_UNLESS_NEWER_SCRIPT,
    SyncResult,
    _internal_package_files,
    publish_private_file,
)
from ptc_agent.core.sandbox import assets as _assets
from ptc_agent.core.sandbox import execution as _execution
from ptc_agent.core.sandbox import files as _files
from ptc_agent.core.sandbox import path_resolution as _paths
from ptc_agent.core.sandbox import mcp_setup as _mcp_setup
from ptc_agent.core.sandbox import reconnect as _reconnect
from ptc_agent.core.sandbox import tool_overlay as _tool_overlay
from ptc_agent.core.sandbox import sessions as _sessions

logger = structlog.get_logger(__name__)


class PTCSandbox:
    """Manages sandbox for Programmatic Tool Calling (PTC) execution."""

    DEFAULT_DEPENDENCIES = DEFAULT_DEPENDENCIES

    def __init__(
        self, config: CoreConfig, mcp_registry: MCPRegistry | None = None,
        *, resource_tier: str | None = None,
        host_unavailable_policy: HostUnavailablePolicy | None = None,
    ) -> None:
        """``mcp_registry`` may be None when reconnecting to an existing sandbox.

        ``host_unavailable_policy`` decides when a start refused by a recovering
        host stops being worth waiting for; without one it never is."""
        self.config = config
        self.resource_tier = resource_tier
        self.host_unavailable_policy = host_unavailable_policy
        self.mcp_registry = mcp_registry

        # Provider-based sandbox management
        self.provider = create_provider(config)
        self.runtime: SandboxRuntime | None = None
        self.sandbox_id: str | None = None
        self.tool_generator = ToolFunctionGenerator()
        # Set by the host while the file mount serves this sandbox; None
        # means the store-backed paths are not on its filesystem.
        self.livefs: MountHandle | None = None
        # Set by the host on a machine the mount never serves, whose past
        # threads the file tools then read in process.
        self.history: HistoryFiles | None = None
        # A reconnect found the mount gone: it booted the sandbox, or nothing
        # answered at the mount's path. The host records that before it next
        # reads what was recorded of the mount, which still says it serves.
        self.livefs_lost = False

        # Working directory — initialized from config, updated by fetch_working_dir()
        # after sandbox creation/reconnect.
        self._work_dir: str = config.filesystem.working_directory

        # The reconnect in flight, which every caller arriving meanwhile awaits
        # rather than repeating it: against a host that does not answer, each
        # one queued behind it would pay its own connect timeout in turn.
        self._reconnect_task: asyncio.Task[None] | None = None
        self._reconnects_running = 0
        self._tool_refresh_lock = asyncio.Lock()
        self._download_semaphore = asyncio.Semaphore(4)

        # Per-port sessions for preview servers (port → (session_id, cmd_id))
        self._preview_sessions: dict[int, tuple[str, str]] = {}
        # The workspace that started each locally tracked preview. A computer
        # serves several workspaces, so a sibling must not inherit or replace
        # the process merely because it asked for the same port.
        self._preview_owners: dict[int, str] = {}
        # Per-port locks to serialize start_and_get_preview_url (avoids races)
        self._preview_locks: dict[int, asyncio.Lock] = {}

        # Lazy initialization support
        self._ready_event: asyncio.Event | None = None
        self._init_task: asyncio.Task[None] | None = None
        self._init_error: Exception | None = None
        # Every failed open evicts its session through cleanup, so cleanup keeps
        # a sandbox no reconnect has completed on: an errored one may hold the
        # only copy of files its backup lacks. A later call that reaches the
        # sandbox outside a reconnect clears it, as does a removal.
        self._reconnect_incomplete = False
        # Set once a rebuild elsewhere is authorized: the host may come back,
        # and with it files the backup lacks, so cleanup never deletes this one.
        self._kept_for_salvage = False
        # A reconnect after a transient found the sandbox gone. The handle then
        # reads as failed, so the next acquisition recovers instead of reusing it.
        self._lost: SandboxGoneError | None = None

        # Cached skills manifest (populated after sync_sandbox_assets)
        self._skills_manifest: dict[str, Any] | None = None

        # Which workspaces have had their disabled tool modules pruned. One
        # sandbox serves every workspace on the computer, so this is a set of
        # claims and not a flag.
        self._disabled_modules_pruned: set[str] = set()

        # Cached standard preview link info per port (avoids repeated Daytona API calls)
        self._preview_link_cache: dict[int, PreviewInfo] = {}

        # The owner's vault as last published to this sandbox, kept so the
        # leak redactor needs no database read of its own.
        self.vault_secrets: dict[str, str] = {}

        logger.debug("Initialized PTCSandbox")

    @property
    def working_dir(self) -> str:
        """The sandbox working directory (available from construction, updated after setup)."""
        return self._work_dir

    @property
    def layout(self) -> SandboxLayout:
        """Where everything lives under the current working dir.

        Rebuilt per access on purpose: ``_work_dir`` is only final after
        ``fetch_working_dir``, so a cached layout would pin the config default.
        """
        return SandboxLayout(self._work_dir)

    def workspace(self, project: "ProjectContext | None" = None) -> WorkspaceLayout:
        """This computer's layout for one workspace folder.

        The only place a workspace root is built. Both halves are read live:
        the computer root off ``_work_dir``, which is final only after
        ``fetch_working_dir``, and the folder off the turn's project. A layout
        handed around instead goes stale on a reconnect that moves the root,
        and a caller that resolves the folder itself is one more place a
        missing project silently means the whole computer.
        """
        ctx = project if project is not None else current_project()
        return SandboxLayout(self._work_dir).for_workspace(
            ctx.dir_name if ctx is not None else None
        )

    @property
    def channel(self) -> str:
        """How a call reaches this computer's machine, as tool traces name it:
        the provider's kind, unless the provider tells its ways in apart."""
        return self.provider.channel or self.config.sandbox.provider

    @property
    def mounts_livefs(self) -> bool:
        """Whether this computer's provider can serve the file mount at all."""
        return type(self.provider).mounts_livefs

    @property
    def files_as_command_user(self) -> bool:
        """Whether a file call here runs as the user commands run as."""
        return type(self.provider).files_as_command_user

    @property
    def recreatable(self) -> bool:
        """Whether the server rebuilds this computer, and so lays its files
        unasked; on one its owner brought, a missing folder is theirs to restore."""
        return type(self.provider).recreatable

    @property
    def transcripts(self) -> TranscriptReach | None:
        """Where this agent's transcripts reach its tools, by the provider's
        kind: the mount wherever one may serve, the in-process routes on a
        machine none ever does. None while that one is not there."""
        if self.mounts_livefs:
            return None if self.livefs is None else TranscriptReach(self.livefs, shell=True)
        return None if self.history is None else TranscriptReach(self.history, shell=False)

    @property
    def _unified_manifest_path(self) -> str:
        return self.layout.manifest

    @property
    def _token_file_path(self) -> str:
        return self.layout.mcp_tokens

    @property
    def _egress_relay_file_path(self) -> str:
        return self.layout.egress_relay

    # ── Effective vs built-in server views ───────────────────────────────
    #
    # ``self.config.mcp.servers`` holds the per-workspace EFFECTIVE set the
    # WorkspaceManager installs at session build: built-ins (minus disables) plus
    # the user's own (``source="user"``, untrusted) servers. Most host-side
    # operations must see ONLY the built-ins — user stdio servers are fetched by
    # npx/uvx at call time inside the sandbox, never pre-installed/pre-started on
    # the host path, and their secrets resolve vault-only (never host os.environ).
    # A zero-user-server workspace has effective == built-ins, so both views are
    # the same objects and byte-identical to pre-change behavior (regression #1).

    def _builtin_servers(self) -> list:
        """Built-in servers only (trusted ``source``) from the effective set."""
        return [s for s in self.config.mcp.servers if not is_untrusted_server(s)]

    def _user_servers(self) -> list:
        """The user's own (``source="user"``, untrusted) servers from the effective set."""
        return [s for s in self.config.mcp.servers if is_untrusted_server(s)]

    async def wait_ready(self) -> None:
        """``_wait_ready`` for a caller outside the sandbox package: one handed
        a sandbox a lazy reconnect is still starting."""
        await self._wait_ready()

    async def _wait_ready(self) -> None:
        """Wait for sandbox to be ready. Call at start of methods needing sandbox.

        Raises typed sandbox errors, never a bare ``RuntimeError``: this runs
        *before* the ``try`` that normalizes each operation's failures, so an
        untyped error here escapes every classifier downstream — the HTTP layer
        turns "sandbox isn't up yet" into a 500 instead of a 503, and the chat
        funnel stops recognizing it as a sandbox condition at all.

        The wording matters too. The file panel selects its "starting" card by
        looking for that word in the detail, so only the genuinely-in-flight case
        may claim it.
        """
        if self._ready_event is None:
            # Not using lazy init - sandbox should already be ready
            if self.runtime is None:
                raise SandboxTransientError("Sandbox is not initialized")
            return

        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            raise SandboxTransientError(
                "Sandbox is still starting: initialization timed out after 300s"
            )

        if self._init_error:
            raise self._init_error

    def is_ready(self) -> bool:
        """Check if sandbox is ready without blocking.

        Returns:
            True if sandbox is ready for operations, False if still initializing.
        """
        if self._lost is not None:
            return False
        if self._ready_event is None:
            # Not using lazy init - check if runtime exists
            return self.runtime is not None

        # Using lazy init - check if event is set and no error
        return self._ready_event.is_set() and self._init_error is None

    def has_failed(self) -> bool:
        """Check if lazy initialization completed with an error, or the sandbox was lost since."""
        if self._lost is not None:
            return True
        if self._ready_event is None:
            return False
        return self._ready_event.is_set() and self._init_error is not None

    @property
    def init_error(self) -> Exception | None:
        """The error from lazy initialization, or the loss of the sandbox since."""
        return self._lost or self._init_error

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

        # Bind the intended identity synchronously. ``reconnect`` only sets
        # ``sandbox_id`` after ``provider.get`` returns, so without this a
        # still-initializing sandbox has no identity for callers to validate
        # against the workspace row — and a stale handle would be handed out on
        # the "still initializing" fast path unchecked.
        self.sandbox_id = sandbox_id
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
            # Typed, because _wait_ready re-raises this verbatim — see its
            # docstring on why a bare RuntimeError escapes every classifier.
            self._init_error = SandboxTransientError("Sandbox init was cancelled")
        except Exception as e:
            logger.error("Lazy sandbox init failed", error=str(e))
            self._init_error = e
        finally:
            if self._ready_event:
                self._ready_event.set()

    def _build_sandbox_env_vars(
        self,
        platform_secret_bindings: dict[str, str],
    ) -> dict[str, str]:
        """Build environment variables to inject at sandbox creation time.

        The deployment's own (``_deployment_env_vars``) go only to a machine
        it trusts. One it does not (``trusted_host`` False) gets none: whoever
        owns it can read every process's environment, and config load has
        already replaced each placeholder with the host's value, so no server
        env is sent at all.
        """
        env_vars: dict[str, str] = {
            # Paths baked into the sandbox image, repeated here so a sandbox born
            # on a snapshot built before one of them was added still gets it.
            **SANDBOX_IMAGE_ENV,
            # Tell the in-sandbox ginlix-data client the exact path the host
            # uploads the token file to. It must not key off $HOME: Daytona
            # runs as root ($HOME=/root) while the working dir is /home/workspace.
            "GINLIX_TOKEN_FILE": self._token_file_path,
        }
        if type(self.provider).trusted_host(self.config.sandbox):
            env_vars.update(self._deployment_env_vars(platform_secret_bindings))
        return env_vars

    async def converge_env(self) -> bool:
        """Bring a machine that outlives every create to the env a new one gets;
        True when that changed it.

        Only where the provider asks (``converges_env``): the env is written at
        create, so a change since (a key added, the trust switch turned on or
        off) reaches such a machine at its next open or never.
        """
        if self.runtime is None or not type(self.provider).converges_env:
            return False
        return await self.runtime.replace_env(
            self._build_sandbox_env_vars(build_platform_secret_bindings(self.config))
        )

    def _deployment_env_vars(
        self, platform_secret_bindings: dict[str, str]
    ) -> dict[str, str]:
        """Resolve MCP server env vars (${VAR} placeholders from host) so
        they're available to all sandbox processes.
        Vars named in ``platform_secret_bindings`` are excluded: the provider
        delivers those as managed Secrets.

        Where platform secrets are required, a credential-named built-in var
        with no binding is dropped with an error rather than failing the
        build: dropping cannot leak the value, and a non-secret caught by the
        name check costs one var instead of every sandbox create.
        """
        import os

        withhold_credentials = platform_secrets_required(self.config)
        # The host's credentials by value: config loading expands ${VAR}, so an
        # override such as FMP_KEY: ${FMP_API_KEY} arrives here as the value
        # alone, and only the value says it is the same credential.
        host_credentials = (
            {v for k, v in os.environ.items() if v and is_credential_env_name(k)}
            if withhold_credentials
            else set()
        )
        withheld: set[str] = set()
        withheld_from: set[str] = set()
        env_vars: dict[str, str] = {}

        # MCP server env vars (resolve ${VAR} placeholders from host).
        # Built-ins ONLY: a user server's env must never be injected into the
        # sandbox os.environ — its ${vault:NAME} refs resolve vault-only at
        # call time, so injecting host values here would leak platform creds.
        for server in self._builtin_servers():
            if not server.enabled:
                continue
            # Only a stdio server has a process here to inherit its env. A
            # remote one keeps a bundled base's env when config switches its
            # transport, and injecting that would hand its vendor key to
            # every sandbox the remote server exists to keep it out of.
            if server.transport != "stdio":
                continue
            if hasattr(server, "env") and server.env:
                for key, value in server.env.items():
                    if key == "INTERNAL_SERVICE_TOKEN":
                        continue  # Never inject platform token into sandbox
                    if key in platform_secret_bindings:
                        # The provider puts an opaque placeholder there for a
                        # managed platform Secret. Never put the real host
                        # value in ordinary sandbox env as a fallback.
                        continue
                    resolved_value = value
                    if value.startswith("${") and value.endswith("}"):
                        resolved_value = os.getenv(value[2:-1]) or ""
                        if not resolved_value:
                            continue
                    if (
                        resolved_value
                        and withhold_credentials
                        and (
                            is_credential_env_name(key)
                            or resolved_value in host_credentials
                        )
                    ):
                        withheld.add(key)
                        withheld_from.add(server.name)
                        continue
                    env_vars[key] = resolved_value
        if withheld:
            # Names only: the values are the platform's credentials.
            logger.error(
                "Withheld credential env from the sandbox: no platform Secret "
                "is bound to it, and hosted sandboxes never get one as "
                "plaintext",
                env_vars=sorted(withheld),
                servers=sorted(withheld_from),
            )

        return env_vars

    async def setup_sandbox_workspace(
        self,
        *,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        dir_name: str | None = None,
    ) -> None:
        """Create sandbox and setup workspace directories.

        Can run concurrently with MCP registry connection since it doesn't
        require the registry.

        Args:
            tier: Resource tier to size the new sandbox at (provider-resolved).
            auto_stop_minutes: Auto-stop interval override in minutes (0 for
                always-on); ``None`` uses the provider default.
            dir_name: Folder the asking workspace owns on this computer.
        """
        logger.info("Setting up sandbox workspace")

        # Build env vars once — injected at sandbox creation time so they're
        # available to all processes (Python, bash, MCP servers)
        platform_secret_bindings = build_platform_secret_bindings(self.config)
        sandbox_env = self._build_sandbox_env_vars(platform_secret_bindings)

        # Create sandbox via provider (handles snapshot logic internally)
        mcp_packages = builtin_mcp_packages(self.config)
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
        await self._setup_workspace(dir_name)

        # A sandbox on a bare image (snapshots disabled, a failed snapshot
        # build, an operator's Docker image) lacks the application packages.
        if not self.runtime.image_prebuilt:
            logger.warning(
                "Sandbox image is not prebuilt; installing dependencies. The MCP "
                "runtime packages may be missing and MCP tools may not work."
            )
            await self._install_dependencies()

        logger.info("Sandbox workspace ready", sandbox_id=self.sandbox_id)

    async def setup_tools_and_mcp(self) -> None:
        """Install tool modules and start MCP servers.

        Requires MCP registry to be connected first.
        """
        logger.info("Setting up tools and MCP servers")

        # Disjoint paths, so in parallel. These run here rather than in the
        # first sync so they overlap the registry connect; the manifest below
        # records them, and that sync skips whatever it finds recorded at the
        # same version. No prune either: a new sandbox has no stale server
        # files.
        await asyncio.gather(
            self._upload_mcp_server_files_impl(),  # → mcp_servers/
            self._upload_internal_packages(),  # → _internal/src/
        )

        # No tool install and no supervisor start here. Which workspace folder
        # the overlay belongs to is not knowable on this path (a cold create
        # runs before any turn), and installing it anyway plants a claim for
        # the computer root that no workspace ever releases. The supervisor
        # imports the client that install writes, so starting it now can only
        # die on the import. ``sync_sandbox_assets`` always follows this, is
        # the caller that knows the folder, and starts the supervisor after
        # the install.

        # The initial manifest records only what is now on disk, so the first
        # sync diffs against it like any other. Tool modules are not: the sync
        # installs them for the folder it knows.
        try:
            manifest = await self._compute_sandbox_manifest()
            manifest["modules"].pop("tool_modules", None)
            await self._write_unified_manifest(manifest)
        except Exception as e:
            logger.warning("Failed to write initial unified manifest", error=str(e))

        logger.info("Tools and MCP servers ready", sandbox_id=self.sandbox_id)

    async def upload_token_file(self, payload: dict[str, Any]) -> bool:
        """Write the market-data token the in-sandbox client reads.

        A file, not an env var, so a fresh token reaches running MCP servers,
        which re-read it when it changes. The server writes every token; the
        sandbox only reads. True only once the write is confirmed.
        """
        if not self.runtime:
            return False
        return await publish_private_file(
            self.runtime,
            self._token_file_path,
            payload,
            what="sandbox token file",
            script=REPLACE_UNLESS_NEWER_SCRIPT,
            args=(str(payload["iat_ms"]),),
            call=self._safe_call,
        )

    async def upload_egress_relay_credentials(self, payload: dict | None) -> bool:
        """Write (or remove) the egress-relay credential file in the sandbox.

        Carries the relay base URL, the sandbox's relay JWT, and the
        server→grant map. The generated MCP client reads it at call time, so a
        re-upload (JWT remint, grant change) needs no sandbox convergence.

        The write is atomic (``publish_private_file``), which is what lets two
        uvicorn workers push the same workspace concurrently without a
        cross-worker lock: the last rename wins, both relay JWTs are
        independently valid, and the grant map converges on the next push.

        Returns ``True`` only on **confirmed** success. The caller must not
        record a fresh binding on a ``False`` return, or the process would
        believe it published a token it did not and skip reminting until that
        phantom token nears expiry.
        """
        if not self.runtime:
            return False

        path = self._egress_relay_file_path

        if not payload:
            try:
                result = await self._runtime_call(
                    self.runtime.exec,
                    f"rm -f {shlex.quote(path)}",
                    retry_policy=RetryPolicy.SAFE,
                )
                return getattr(result, "exit_code", 0) == 0
            except Exception as e:
                logger.warning("Failed to remove egress relay file", error=str(e))
                return False

        return await publish_private_file(
            self.runtime,
            path,
            payload,
            what="egress relay credentials",
            call=self._safe_call,
        )

    def _safe_call(self, func: Callable[..., Any], *args: Any) -> Any:
        return self._runtime_call(func, *args, retry_policy=RetryPolicy.SAFE)

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
        await self.setup_sandbox_workspace()
        await self.setup_tools_and_mcp()
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
        await _reconnect.reconnect(self, sandbox_id, on_state_observed)

    async def _cancel_background_tasks(self) -> None:
        """Cancel an in-flight lazy init or shared reconnect, and wait for each.

        A reconnect is shielded from the callers that gave up on it, so this
        is the one thing that stops it; left to finish after a stop, it would
        start the machine again behind a computer recorded as stopped.
        """
        for task in (self._init_task, self._reconnect_task):
            if task is None or task.done() or task is asyncio.current_task():
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._init_task = None
        self._reconnect_task = None

    async def stop_sandbox(self) -> None:
        """Stop the sandbox without deleting it.

        Used for session persistence - stops the sandbox so it can be
        restarted quickly on the next session, rather than deleting it.
        """
        await self._cancel_background_tasks()

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

    async def _setup_workspace(self, dir_name: str | None = None) -> None:
        """Create workspace directory structure."""
        logger.info("Setting up workspace structure")

        # Get the working directory
        assert self.runtime is not None
        work_dir = await self.runtime.fetch_working_dir()
        logger.info(f"Sandbox working directory: {work_dir}")

        # Store work_dir for use by other methods
        self._work_dir = work_dir

        # Both tiers: the computer's own runtime directories, in parallel,
        # then the folder of whichever workspace is asking.
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

        await asyncio.gather(*[create_directory(d) for d in self.layout.setup_dirs])
        await self._ensure_workspace_dirs(dir_name)

        # Sweep orphaned MCP trace files from a prior/reused session
        # (best-effort). Normally a job's trace is deleted when its end is
        # read (``jobs``); this bounds leakage when nothing ever reads it.
        try:
            assert self.runtime is not None
            await self._runtime_call(
                self.runtime.exec,
                f"rm -f {shlex.quote(self.layout.system_trace)}/*.jsonl",
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as e:
            logger.debug(f"MCP trace dir sweep skipped: {e}")

    async def _ensure_workspace_dirs(self, dir_name: str | None = None) -> None:
        """Create the asking workspace's own directories, in one shell.

        A computer gains folders long after its sandbox was built, and the
        reconnect path skips setup because the computer's directories are
        already there, so this is the only place workspace N's folder comes
        from. Out of a turn and with no folder named, the tier folds onto the
        root, which is what a computer holding one workspace looks like.
        """
        if dir_name is None:
            project = current_project()
            dir_name = project.dir_name if project else None
        workspace = self.layout.for_workspace(dir_name)
        quoted = " ".join(shlex.quote(d) for d in workspace.setup_dirs)
        try:
            assert self.runtime is not None
            await self._runtime_call(
                self.runtime.exec,
                f"mkdir -p {quoted}",
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as e:
            logger.warning(f"Error creating workspace directories: {e}")

    async def _upload_internal_packages(self) -> None:
        """Mirror the ``_SANDBOX_INTERNAL_PACKAGES`` set into ``_internal/src/``.

        All-or-nothing: the whole set ships together, gated by the single
        ``internal_packages`` manifest module.
        """
        internal_root = Path(self.layout.internal_src)

        # Resolve local paths relative to config file directory if available.
        config_dir = getattr(self.config, "config_file_dir", None)
        repo_root = config_dir or Path.cwd()
        local_src_dir = (repo_root / "src").resolve()

        files = _internal_package_files(local_src_dir, livefs=self.mounts_livefs)
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

        batch: list[tuple[bytes | str, str]] = [
            (str(local_path), str(internal_root / rel_path))
            for local_path, rel_path in files
        ]
        # The vault helper ships even with no secrets, so `from vault import
        # get` always imports. Its source is stamped with this set, so it goes
        # up in the same batch: a failed upload must not be recorded as applied.
        batch.append(
            (VAULT_MODULE_SOURCE.encode("utf-8"), str(internal_root / "vault.py"))
        )
        await self._runtime_call(
            sandbox.upload_files,
            batch,
            retry_policy=RetryPolicy.SAFE,
        )
        logger.debug(
            "Uploaded internal packages to sandbox",
            uploaded_files=len(batch),
            sandbox_root=str(internal_root),
        )

    # ── Unified manifest helpers ────────────────────────────────────────


    # ── Unified manifest I/O ─────────────────────────────────────────


    # ── Unified sync entry point ─────────────────────────────────────


    async def _ensure_sandbox_connected(self) -> None:
        await _reconnect.ensure_connected(self)

    async def _reset_provider(self) -> None:
        try:
            await self.provider.close()
        except Exception:
            pass
        self.provider = create_provider(self.config)

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
        result = await async_retry_with_backoff(
            func,
            *args,
            retry_policy=retry_policy,
            is_transient=self.provider.is_transient_error,
            on_transient=on_transient,
            rebind=self._make_rebinder(self.runtime) if allow_reconnect else None,
            retries=retries,
            initial_delay_s=initial_delay_s,
            total_timeout=total_timeout,
            **kwargs,
        )
        _reconnect.call_served(self)
        return result

    def _make_rebinder(
        self, entry_runtime: SandboxRuntime | None
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Build the retry-loop hook that re-resolves a method onto a new runtime.

        Callers pass already-bound methods (``sandbox.runtime.download_file``), so
        after ``_ensure_sandbox_connected`` swaps ``self.runtime`` every remaining
        attempt ran against the old runtime — whose provider had just been closed,
        making retries 2-5 guaranteed failures.

        Narrow by construction: rebinding happens only for a method bound to the
        exact runtime this call started with. ``provider.create``/``provider.get``
        are bound to the provider, so they are never touched.
        """

        def rebind(func: Callable[..., Any]) -> Callable[..., Any]:
            current = self.runtime
            if entry_runtime is None or current is None or current is entry_runtime:
                return func
            if getattr(func, "__self__", None) is not entry_runtime:
                return func
            replacement = getattr(current, getattr(func, "__name__", ""), None)
            return replacement if callable(replacement) else func

        return rebind


    # Bound concurrent discovery so a burst of servers can't exhaust the
    # sandbox; one hung server still can't starve others (each runs isolated).
    _DISCOVERY_CONCURRENCY = 4
    # CLI exec ceiling: the in-sandbox client gives a stdio server 60s to start
    # reading and 10s per handshake reply after that; the rest is headroom for
    # tools/list and the JSON write.
    _DISCOVERY_EXEC_TIMEOUT_S = 90


    @property
    def proxy_domain(self) -> str | None:
        """Hostname of the sandbox proxy, or None if unavailable."""
        if self.runtime is None:
            return None
        return self.runtime.proxy_domain


    @staticmethod
    def _extract_sandbox_id(sandbox: object) -> str:
        """Extract a stable ID string from a sandbox object."""
        return sandbox.id if hasattr(sandbox, "id") else str(id(sandbox))


    def release_for_removal(self) -> None:
        """Let cleanup delete the sandbox though no reconnect completed on it.

        That keep is for evicting a failed open, whose sandbox may hold files
        its backup lacks, and a computer is removed only once no workspace is
        left on it. A sandbox kept for salvage stays kept.
        """
        self._reconnect_incomplete = False

    async def cleanup(self) -> None:
        """Clean up and destroy the sandbox."""
        await self._cancel_background_tasks()

        logger.info("Cleaning up sandbox", sandbox_id=self.sandbox_id)

        try:
            if self.runtime:
                # Clean up preview sessions; deleting the sandbox ends the rest
                for sid in dict.fromkeys(sid for sid, _ in self._preview_sessions.values()):
                    try:
                        await self._runtime_call(
                            self.runtime.delete_session, sid,
                            retry_policy=RetryPolicy.SAFE,
                        )
                    except Exception:
                        logger.debug("Failed to delete session", session_id=sid)
                self._preview_sessions.clear()
                self._preview_owners.clear()

                if self._kept_for_salvage:
                    logger.info(
                        "Keeping sandbox left for salvage", sandbox_id=self.sandbox_id
                    )
                elif self._reconnect_incomplete:
                    logger.info(
                        "Keeping sandbox no reconnect completed on",
                        sandbox_id=self.sandbox_id,
                    )
                else:
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

    async def _compute_skills_module(
        self,
        skill_roots: list[str],
        *,
        managed_root: str | None = None,
        disabled: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        return await _assets._compute_skills_module(self, skill_roots, managed_root=managed_root, disabled=disabled)

    async def _compute_sandbox_manifest(
        self,
        *,
        skill_roots: list[str] | None = None,
        managed_skill_root: str | None = None,
        disabled_skills: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        return await _assets._compute_sandbox_manifest(self, skill_roots=skill_roots, managed_skill_root=managed_skill_root, disabled_skills=disabled_skills)

    async def _read_unified_manifest(self) -> dict[str, Any] | None:
        return await _assets._read_unified_manifest(self)

    async def _write_unified_manifest(self, manifest: dict[str, Any]) -> None:
        return await _assets._write_unified_manifest(self, manifest)

    async def _cleanup_legacy_manifests(self) -> None:
        return await _assets._cleanup_legacy_manifests(self)

    async def _upload_mcp_server_files_impl(self) -> set[str]:
        return await _assets._upload_mcp_server_files_impl(self)

    async def _prune_mcp_server_files(self, expected: set[str]) -> None:
        return await _assets._prune_mcp_server_files(self, expected)

    async def sync_sandbox_assets(
        self,
        *,
        skill_dirs: list[str] | None = None,
        user_skill_dir: str | None = None,
        disabled_skills: frozenset[str] = frozenset(),
        reusing_sandbox: bool = False,
        force_refresh: bool = False,
        project: "ProjectContext | None" = None,
        root_owner_dir_name: str | None = None,
        workspace_dir_names: tuple[str, ...] = (),
        on_progress: Callable[[str], None] | None = None,
    ) -> SyncResult:
        result = await _assets.sync_sandbox_assets(
            self,
            skill_dirs=skill_dirs,
            user_skill_dir=user_skill_dir,
            disabled_skills=disabled_skills,
            reusing_sandbox=reusing_sandbox,
            force_refresh=force_refresh,
            project=project,
            root_owner_dir_name=root_owner_dir_name,
            workspace_dir_names=workspace_dir_names,
            on_progress=on_progress,
        )
        # After the migration, never before: a fresh folder lets v3 to v4 move
        # a directory rather than merge into one this just created.
        await self._ensure_workspace_dirs(project.dir_name if project else None)
        return result

    async def _prune_disabled_tool_modules(
        self, *, project: "ProjectContext | None" = None
    ) -> None:
        return await _assets._prune_disabled_tool_modules(self, project=project)

    async def _download_skills_lock(self) -> dict[str, Any] | None:
        return await _assets._download_skills_lock(self)

    def _build_complete_skills_cache(
        self, skills_mod: dict[str, Any], merged_lock: dict[str, Any]
    ) -> None:
        return _assets._build_complete_skills_cache(self, skills_mod, merged_lock)

    async def _prune_remote_skills(
        self,
        local_skill_names: set[str],
        *,
        existing_lock: dict[str, Any] | None = None,
    ) -> None:
        return await _assets._prune_remote_skills(self, local_skill_names, existing_lock=existing_lock)

    async def _upload_skills(
        self,
        local_skills_dirs: list[str],
        *,
        manifest: dict[str, Any] | None = None,
        existing_lock: dict[str, Any] | None = None,
        disabled: frozenset[str] = frozenset(),
    ) -> tuple[dict[str, Any] | None, set[str]]:
        return await _assets._upload_skills(self, local_skills_dirs, manifest=manifest, existing_lock=existing_lock, disabled=disabled)


    # -- mcp_setup --

    async def _install_dependencies(self) -> None:
        return await _mcp_setup._install_dependencies(self)

    async def _upload_discovery_client(
        self, extra_servers: list[Any] | None = None
    ) -> str:
        return await _mcp_setup._upload_discovery_client(self, extra_servers)

    async def _install_tool_modules(
        self, *, project: ProjectContext, tool_version: str | None = None
    ) -> None:
        return await _tool_overlay.install_tool_modules(
            self, project=project, tool_version=tool_version
        )

    async def workspace_overlay_missing(
        self, *, workspace_id: str, dir_name: str | None
    ) -> bool:
        return await _tool_overlay.overlay_claim_missing(
            self, ProjectContext(workspace_id or "", dir_name or "")
        )

    async def discover_user_mcp_schemas(
        self, servers: list[Any]
    ) -> dict[str, dict[str, Any]]:
        return await _mcp_setup.discover_user_mcp_schemas(self, servers)

    async def check_user_mcp_server(
        self,
        server: Any,
        vault: Mapping[str, str],
        *,
        budget_s: float,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        return await _mcp_setup.check_user_mcp_server(
            self, server, vault, budget_s=budget_s, deadline=deadline
        )

    async def _start_internal_mcp_servers(self, *, replace: bool = False) -> bool | None:
        return await _mcp_setup._start_internal_mcp_servers(self, replace=replace)

    async def _install_package(self, package: str) -> bool:
        return await _mcp_setup._install_package(self, package)


    # -- execution --

    async def execute_bash_command(
        self,
        command: str,
        working_dir: str | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        return await _execution.execute_bash_command(self, command, working_dir, timeout)

    async def start_bash_job(
        self,
        command: str,
        *,
        working_dir: str | None = None,
        wait_s: float | None,
        thread_id: str | None = None,
        call_id: str | None = None,
        allowed_servers: frozenset[str] | None = None,
    ) -> JobOutcome:
        return await _execution.start_bash_job(
            self,
            command,
            working_dir=working_dir,
            wait_s=wait_s,
            thread_id=thread_id,
            call_id=call_id,
            allowed_servers=allowed_servers,
        )

    async def run_foreground(self, spec: JobSpec, timeout: int) -> Ended | Failed:
        return await _execution.run_foreground(self, spec, timeout)

    async def start_code_job(
        self,
        code: str,
        *,
        wait_s: float | None,
        thread_id: str | None = None,
        call_id: str | None = None,
        allowed_servers: frozenset[str] | None = None,
    ) -> JobOutcome:
        return await _execution.start_code_job(
            self,
            code,
            wait_s=wait_s,
            thread_id=thread_id,
            call_id=call_id,
            allowed_servers=allowed_servers,
        )

    async def continue_job(self, outcome: Running, wait_s: float | None) -> JobOutcome:
        return await _execution.continue_job(self, outcome, wait_s)

    async def read_ended_job(self, outcome: Running) -> JobOutcome:
        return await _execution.read_ended_job(self, outcome)

    async def stop_job(self, ref: JobRef, *, discard: bool = False) -> bool:
        return await _execution.stop_job(self, ref, discard=discard)

    async def stop_job_with_trace(self, outcome: Running) -> StoppedJob:
        return await _execution.stop_job_with_trace(self, outcome)

    # -- sessions --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        return await _sessions.get_preview_url(self, port, expires_in)

    async def get_preview_link(self, port: int, *, wake: bool = True) -> PreviewInfo:
        return await _sessions.get_preview_link(self, port, wake=wake)

    async def start_preview_server(
        self, command: str, port: int, *, owner: str | None = None
    ) -> str:
        return await _sessions.start_preview_server(
            self, command, port, owner=owner
        )

    async def _is_preview_reachable(self, port: int, *, timeout: float = 3.0) -> bool:
        return await _sessions._is_preview_reachable(self, port, timeout=timeout)

    async def start_and_get_preview_url(
        self,
        command: str,
        port: int,
        *,
        expires_in: int = 3600,
        startup_timeout: float = 10.0,
        owner: str | None = None,
    ) -> PreviewInfo:
        return await _sessions.start_and_get_preview_url(
            self,
            command,
            port,
            expires_in=expires_in,
            startup_timeout=startup_timeout,
            owner=owner,
        )

    async def get_preview_server_logs(self, port: int) -> dict[str, Any]:
        return await _sessions.get_preview_server_logs(self, port)

    async def stop_preview_server(self, port: int) -> bool:
        return await _sessions.stop_preview_server(self, port)


    # -- files --

    def _normalize_search_path(self, path: str) -> str:
        return _paths._normalize_search_path(self, path)

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        return await _files.adownload_file_bytes(self, filepath)

    async def adownload_file_bytes_capped(
        self, filepath: str, cap: Callable[[bytes], int]
    ) -> bytes | None:
        return await _files.adownload_file_bytes_capped(self, filepath, cap)

    async def astream_file_bytes(self, filepath: str) -> AsyncIterator[bytes] | None:
        return await _files.astream_file_bytes(self, filepath)

    async def aread_file_text(self, filepath: str) -> str | None:
        return await _files.aread_file_text(self, filepath)

    async def aupload_file_bytes(
        self, filepath: str, content: bytes, *, verify: bool = False
    ) -> bool:
        return await _files.aupload_file_bytes(self, filepath, content, verify=verify)

    async def awrite_file_text(self, filepath: str, content: str) -> bool:
        return await _files.awrite_file_text(self, filepath, content)

    def path_write_lock(self, normalized_path: str):
        return _files.path_write_lock(self, normalized_path)

    async def aread_file_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        return await _files.aread_file_range(self, file_path, offset, limit)

    async def _aread_file_range_fallback(
        self, file_path: str, offset: int, limit: int
    ) -> str | None:
        return await _files._aread_file_range_fallback(self, file_path, offset, limit)

    def normalize_path(
        self, path: str, project: "ProjectContext | None" = None
    ) -> str:
        return _paths.normalize_path(self, path, project)

    def virtualize_path(
        self, path: str, project: "ProjectContext | None" = None
    ) -> str:
        return _paths.virtualize_path(self, path, project)

    def validate_path(
        self, filepath: str, project: "ProjectContext | None" = None
    ) -> bool:
        return _paths.validate_path(self, filepath, project)

    def validate_and_normalize_path(
        self, path: str, project: "ProjectContext | None" = None
    ) -> tuple[str, str | None]:
        return _paths.validate_and_normalize_path(self, path, project)

    async def als_directory(
        self, directory: str = ".", *, allow_denied: bool = False
    ) -> list[dict[str, Any]]:
        return await _files.als_directory(self, directory, allow_denied=allow_denied)

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
        return _paths._validate_path_allow_denied(self, path)

    async def aglob_files(
        self,
        pattern: str,
        path: str = ".",
        *,
        allow_denied: bool = False,
        hide_history: bool = False,
    ) -> list[str]:
        return await _files.aglob_files(
            self, pattern, path, allow_denied=allow_denied, hide_history=hide_history
        )

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
        folder: str | None = None,
    ) -> Any:
        return await _files.agrep_content(self, pattern, path, output_mode, glob, type, case_insensitive=case_insensitive, show_line_numbers=show_line_numbers, lines_after=lines_after, lines_before=lines_before, lines_context=lines_context, multiline=multiline, head_limit=head_limit, offset=offset, folder=folder)
