"""Abstract runtime and provider interfaces for sandbox backends."""

import base64
import shlex
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.platform_secrets import (
        ReconciledPlatformSecret,
        ResolvedPlatformSecret,
    )


class SandboxTransientError(RuntimeError):
    """Transient sandbox transport error.

    Raised when an operation fails due to transient transport issues and cannot be
    safely retried automatically.
    """


class SandboxGoneError(RuntimeError):
    """The sandbox no longer exists (deleted, expired, or in an unrecoverable state).

    Callers should create a fresh sandbox and restore files from backup.
    """

    def __init__(self, sandbox_id: str, message: str = ""):
        self.sandbox_id = sandbox_id
        full_msg = f"Sandbox {sandbox_id} is gone"
        if message:
            full_msg += f": {message}"
        super().__init__(full_msg)


class SandboxHostUnavailableError(SandboxTransientError):
    """The provider refused to start the sandbox while its host recovers.

    Carries the provider's own wording, which tells the person to retry in a
    few moments; the sandbox and its files are still there.
    """

    def __init__(self, sandbox_id: str, message: str):
        self.sandbox_id = sandbox_id
        super().__init__(message)


class SandboxHostLostError(SandboxGoneError):
    """The sandbox's host has been unavailable long enough to rebuild elsewhere.

    Gone to every handler, so the start rebuilds from the backed-up files, but
    the sandbox may come back with files the backup lacks, so it is never
    deleted. ``outage_since`` is the outage that authorized the rebuild: the
    host can come back while it runs, and a start that resumes the sandbox
    meanwhile ends that outage, which is what refuses the replacement.
    """

    def __init__(
        self, sandbox_id: str, reason: str, *, outage_since: datetime | None = None
    ):
        self.outage_since = outage_since
        super().__init__(sandbox_id, reason)


class HostUnavailablePolicy(ABC):
    """How long a sandbox waits for its recovering host before it is rebuilt.

    The caller keeps the outage's clock, because the outage outlives any one
    handle on the sandbox. A refusal starts it and the sandbox coming up ends
    it; a clock left running would cut the next outage short.
    """

    @abstractmethod
    async def rebuild_now(self, sandbox_id: str) -> datetime | None:
        """The host refused to start *sandbox_id*.

        When the outage that authorizes rebuilding instead began, or None to
        keep waiting for the host.
        """

    @abstractmethod
    async def reconnected(self, sandbox_id: str) -> bool | None:
        """*sandbox_id* came up, so its host is back.

        True while the machine still names it. False when the machine named it
        and names another by now: a rebuild during the outage replaced it, and
        it must go unused. None when the machine has not named it yet, as for a
        sandbox still being built to replace another.
        """


class SandboxFailureKind(str, Enum):
    """What a failed sandbox operation actually means.

    Exists so callers never have to infer "the file isn't there" from "the call
    didn't work". ``UNKNOWN`` is a first-class outcome, not a fallback bucket:
    a status-less transport failure is genuinely undecidable from the exception
    alone and must be confirmed against the runtime rather than guessed at.
    """

    PATH_ABSENT = "path_absent"  # positively identified per-path not-found
    SANDBOX_GONE = "sandbox_gone"  # the sandbox itself is not there
    TRANSIENT = "transient"  # transport-level, may succeed on retry
    UNKNOWN = "unknown"  # undecidable — never treat as absence


class RuntimeState(str, Enum):
    """Possible states of a sandbox runtime."""

    RUNNING = "running"
    STOPPED = "stopped"
    STARTING = "starting"
    STOPPING = "stopping"
    ARCHIVED = "archived"
    ERROR = "error"


@dataclass
class ExecResult:
    """Result of a shell command execution."""

    stdout: str
    stderr: str
    exit_code: int


@dataclass
class PreviewInfo:
    """Preview URL info for a service running in the sandbox."""

    url: str
    token: str
    auth_headers: dict[str, str] = field(default_factory=dict)


@dataclass
class SessionCommandResult:
    """Result of a command executed in a background session."""

    cmd_id: str
    exit_code: int | None  # None = still running
    stdout: str
    stderr: str


@dataclass
class SessionState:
    """A session on the machine, and whether a command in it is still running."""

    session_id: str
    running: bool


@dataclass
class Artifact:
    """An artifact produced by code execution (e.g. a chart image)."""

    type: str  # MIME type, e.g. "image/png"
    data: str  # base64-encoded content
    name: str | None = None


@dataclass
class CodeRunResult:
    """Result of a code execution with optional artifacts."""

    stdout: str
    stderr: str
    exit_code: int
    artifacts: list[Artifact] = field(default_factory=list)


# One ranged read of a streamed download. Large enough that a file costs few
# execs, small enough that a stream holds little memory at any moment.
STREAM_CHUNK_BYTES = 8 * 1024 * 1024


class SandboxRuntime(ABC):
    """Primitive operations that vary per sandbox provider.

    Each provider (Daytona, Docker, etc.) implements this interface to expose
    a uniform execution surface to PTCSandbox.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """Unique identifier for this runtime instance."""
        ...

    @property
    @abstractmethod
    def working_dir(self) -> str:
        """Default working directory inside the sandbox (sync, may return cached/default)."""
        ...

    @property
    def proxy_domain(self) -> str | None:
        """Hostname of the sandbox proxy (e.g. 'sandbox-abc123.proxy.example.com'). None if unsupported."""
        return None

    async def fetch_working_dir(self) -> str:
        """Fetch and cache the working directory (async). Override if working_dir requires I/O."""
        return self.working_dir

    # -- Lifecycle --

    @abstractmethod
    async def start(self, timeout: int = 120) -> None:
        """Start the runtime."""
        ...

    async def recover_from_error(self, timeout: int = 120) -> None:
        """Bring an errored runtime back.

        Providers without a recovery verb restart it. One that can tell a
        runtime will never come back raises SandboxGoneError instead.
        """
        await self.start(timeout=timeout)

    @abstractmethod
    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        """Stop the runtime, forcefully when the provider supports it."""
        ...

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the runtime."""
        ...

    @abstractmethod
    async def get_state(self) -> RuntimeState:
        """Return the current lifecycle state."""
        ...

    async def update_env(
        self, env: dict[str, str], *, unset: Sequence[str] = ()
    ) -> None:
        """Set/unset sandbox-level env vars; providers without support raise."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support sandbox env updates"
        )

    async def update_secrets(self, secrets: dict[str, str]) -> None:
        """Attach provider-managed secret bindings; providers without support raise."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support platform secrets"
        )

    async def refresh_state(self) -> RuntimeState:
        """Refresh and return lifecycle state; defaults to ``get_state``."""

        return await self.get_state()

    # -- Execution --

    @abstractmethod
    async def exec(
        self, command: str, timeout: int = 60
    ) -> ExecResult:
        """Run a shell command and return the result, from the sandbox's own
        working directory. A caller that needs another one says so in the
        command, because one computer holds several workspace folders and the
        turn, not the machine, decides which of them is its."""
        ...

    async def exec_as_root(
        self, command: str, timeout: int = 60, env: dict[str, str] | None = None
    ) -> ExecResult:
        """``exec`` as root, for work the sandbox's own user must not be able
        to do, such as mounting. ``env`` reaches that command alone, so a
        secret in it is in no file and no command line. A runtime whose
        commands already run as root and take no environment keeps this
        default, which refuses one."""
        if env:
            raise NotImplementedError(f"{type(self).__name__} takes no exec environment")
        return await self.exec(command, timeout)

    @abstractmethod
    async def code_run(
        self,
        code: str,
        env: dict[str, str] | None = None,
        timeout: int = 300,
    ) -> CodeRunResult:
        """Execute code (Python) and return the result with artifacts.

        No working directory: a turn's Python lands in its own workspace via
        the ``PTC_TURN_CWD`` entry in *env*, which the shipped
        ``sitecustomize.py`` reads at interpreter startup. One provider's
        ``code_run`` accepts no directory at all, and prepending a ``chdir``
        to the submitted source costs a ``from __future__`` import and every
        traceback line number.
        """
        ...

    # -- File I/O --

    @abstractmethod
    async def upload_file(self, content: bytes, dest_path: str) -> None:
        """Upload a single file to the sandbox."""
        ...

    @abstractmethod
    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        """Upload multiple files in one operation.

        Each tuple is (source, destination_path) where source is either
        bytes content or a local file path string.
        """
        ...

    @abstractmethod
    async def download_file(self, path: str) -> bytes:
        """Download a file from the sandbox."""
        ...

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        """Yield a file's bytes in order without ever holding the whole file.

        The default reads one range per exec, which every runtime can run and
        which keeps each exec's output far below any provider's output limit.
        A provider with a native streaming download should override it.
        """
        quoted = shlex.quote(path)
        offset = 0
        while True:
            result = await self.exec(
                f"test -f {quoted} && tail -c +{offset + 1} {quoted} 2>/dev/null"
                f" | head -c {STREAM_CHUNK_BYTES} | base64",
                timeout=120,
            )
            if result.exit_code != 0:
                if offset == 0:
                    raise FileNotFoundError(f"File not found or unreadable: {path}")
                raise RuntimeError(f"Ranged read of {path} failed at byte {offset}")
            chunk = base64.b64decode(result.stdout)
            if chunk:
                yield chunk
            if len(chunk) < STREAM_CHUNK_BYTES:
                return
            offset += len(chunk)

    @abstractmethod
    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        """List files in a directory."""
        ...

    # -- Capabilities & metadata --

    @property
    def capabilities(self) -> set[str]:
        """Set of capability strings supported by this runtime."""
        return {"exec", "code_run", "file_io"}

    async def archive(self) -> None:
        """Archive the runtime for later restoration.

        Not all providers support this; the default raises NotImplementedError.
        """
        raise NotImplementedError

    async def set_autostop_interval(self, minutes: int) -> None:
        """Set the idle auto-stop interval in minutes (0 disables auto-stop).

        Not all providers support this; the default raises NotImplementedError.
        """
        raise NotImplementedError

    # -- Sessions (background processes) --

    async def create_session(self, session_id: str) -> None:
        """Create a named session for background command execution."""
        raise NotImplementedError("Sessions not supported by this runtime")

    async def session_execute(
        self,
        session_id: str,
        command: str,
        *,
        run_async: bool = False,
        timeout: int | None = None,
    ) -> SessionCommandResult:
        """Execute a command in a session. Use run_async=True for background execution."""
        raise NotImplementedError("Sessions not supported by this runtime")

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        """Get stdout/stderr logs of a session command."""
        raise NotImplementedError("Sessions not supported by this runtime")

    async def session_logs(self, session_id: str) -> SessionCommandResult | None:
        """Logs and exit code of the session's latest command, or None when the
        session does not exist."""
        raise NotImplementedError("Sessions not supported by this runtime")

    async def list_sessions(self) -> list[SessionState]:
        """Every session on the machine."""
        raise NotImplementedError("Sessions not supported by this runtime")

    async def delete_session(self, session_id: str) -> None:
        """Delete a session. Default is no-op for providers without session support."""

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """Get a signed preview URL for a service running on the given port.

        Not all providers support this; the default raises NotImplementedError.
        """
        raise NotImplementedError("Preview URLs not supported by this runtime")

    async def get_preview_link(self, port: int) -> PreviewInfo:
        """Get a standard (non-signed) preview URL for a service running on the given port.

        Returns PreviewInfo with ``auth_headers`` populated for authenticated
        requests. Unlike signed URLs, this token resets on sandbox restart.
        Used for health checks.
        """
        raise NotImplementedError("Preview links not supported by this runtime")

    async def get_metadata(self) -> dict[str, Any]:
        """Return provider-specific metadata about the runtime."""
        return {"id": self.id, "working_dir": self.working_dir}


class SandboxProvider(ABC):
    """Factory that creates and reconnects to sandbox runtime instances."""

    async def reconcile_platform_secrets(
        self, secrets: "Sequence[ResolvedPlatformSecret]"
    ) -> "list[ReconciledPlatformSecret]":
        """Create/update provider-managed Secrets; capability-absent by default."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support platform secrets"
        )

    @abstractmethod
    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        platform_secret_bindings: dict[str, str] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        **kwargs: Any,
    ) -> SandboxRuntime:
        """Create a new sandbox runtime.

        Args:
            env_vars: Environment variables injected at creation time.
            platform_secret_bindings: Provider-specific secret mounts. Providers
                without managed-secret support may ignore this mapping.
            tier: Resource tier name (provider-resolved; may be ignored by
                providers without tiered sizing).
            auto_stop_minutes: Auto-stop interval override in minutes (0 disables).
        """
        ...

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxRuntime:
        """Reconnect to an existing sandbox runtime by ID."""
        ...

    async def prepare_reconnect(
        self, runtime: SandboxRuntime, *, tier: str | None = None
    ) -> None:
        """Providers with mutable limits can repair an existing runtime before use."""

    @abstractmethod
    async def close(self) -> None:
        """Release provider resources (HTTP clients, etc.)."""
        ...

    def is_transient_error(self, exc: Exception) -> bool:
        """Return True if *exc* is a transient error that may be retried.

        Providers should override to classify provider-specific errors.
        """
        return False

    def is_host_unavailable(self, exc: Exception) -> bool:
        """True when a start failed because the sandbox's host is recovering.

        Only a backend that runs sandboxes on hosts it can lose reports this.
        """
        return False

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        """Classify *exc* into a failure kind for callers to act on.

        Providers should override with SDK-specific structured signals (status
        codes, machine-readable error codes) — never message matching, which
        breaks silently when a vendor rewords a string. Beyond the two cases
        below the default cannot tell, so everything else is honestly
        ``UNKNOWN``.

        ``FileNotFoundError`` is the one absence signal a provider can raise
        without an SDK of its own, and it is checked before
        ``is_transient_error`` because that is a message scan: a path like
        ``/data/connection.log`` would otherwise read as a connection fault.
        Without it, a provider that cannot say "this path is missing" sends
        every genuine miss to the liveness probe, which finds the sandbox alive
        and reports a transient — turning an ordinary 404 into a 503.
        """
        if isinstance(exc, FileNotFoundError):
            return SandboxFailureKind.PATH_ABSENT
        if self.is_transient_error(exc):
            return SandboxFailureKind.TRANSIENT
        return SandboxFailureKind.UNKNOWN
