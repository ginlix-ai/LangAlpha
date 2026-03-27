"""Abstract runtime and provider interfaces for sandbox backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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

    @abstractmethod
    async def stop(self, timeout: int = 60) -> None:
        """Stop the runtime."""
        ...

    @abstractmethod
    async def delete(self) -> None:
        """Permanently delete the runtime."""
        ...

    @abstractmethod
    async def get_state(self) -> RuntimeState:
        """Return the current lifecycle state."""
        ...

    # -- Execution --

    @abstractmethod
    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        """Run a shell command and return the result."""
        ...

    @abstractmethod
    async def code_run(
        self,
        code: str,
        env: dict[str, str] | None = None,
        timeout: int = 300,
    ) -> CodeRunResult:
        """Execute code (Python) and return the result with artifacts."""
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

    @abstractmethod
    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> SandboxRuntime:
        """Create a new sandbox runtime."""
        ...

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxRuntime:
        """Reconnect to an existing sandbox runtime by ID."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release provider resources (HTTP clients, etc.)."""
        ...

    def is_transient_error(self, exc: Exception) -> bool:
        """Return True if *exc* is a transient error that may be retried.

        Providers should override to classify provider-specific errors.
        """
        return False
