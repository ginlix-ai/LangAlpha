"""The runtime half of the Daytona provider: one SDK sandbox behind ``SandboxRuntime``."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import structlog
from daytona import (
    DaytonaNotFoundError,
    FileUpload,
    SessionExecuteRequest,
)

from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxGoneError,
    SandboxRuntime,
    SessionCommandResult,
    SessionState,
)

logger = structlog.get_logger(__name__)

# Mapping from Daytona SDK state strings to RuntimeState enum.
_STATE_MAP: dict[str, RuntimeState] = {
    "started": RuntimeState.RUNNING,
    "running": RuntimeState.RUNNING,
    "stopped": RuntimeState.STOPPED,
    "starting": RuntimeState.STARTING,
    # An archive being brought back. A second start during it answers 409
    # "state change in progress", so it has to wait like any other boot.
    "restoring": RuntimeState.STARTING,
    "stopping": RuntimeState.STOPPING,
    "archived": RuntimeState.ARCHIVED,
    # Its own state, not an error: a stop of it answers 409, and a start that
    # lands after the container is gone errors the sandbox beyond recovery.
    "archiving": RuntimeState.ARCHIVING,
    "error": RuntimeState.ERROR,
}


def _raw_state(sandbox: Any) -> str | None:
    state = getattr(sandbox, "state", None)
    if state is None:
        return None
    return state.value if hasattr(state, "value") else str(state)


# Override the SDK's 30-min default so a hung toolbox connection surfaces
# as a transient error within the _runtime_call retry envelope.
_FS_TIMEOUT_S = 60
# A streamed download is paced by the client that receives it, so it gets far
# longer than a whole-file read; a client that stops reading ends it sooner.
_STREAM_TIMEOUT_S = 60 * 60
# Session command ids a runtime keeps; past it the oldest go, and a poll of
# one of those reads its status first again.
_COMMAND_IDS_KEPT = 256


class DaytonaRuntime(SandboxRuntime):
    """Runtime that delegates to a Daytona SDK sandbox object."""

    def __init__(
        self,
        sdk_sandbox: Any,
        *,
        snapshot_name: str | None = None,
        default_working_dir: str = DEFAULT_SANDBOX_ROOT,
    ) -> None:
        self._sandbox = sdk_sandbox
        self._working_dir: str | None = None
        self._default_working_dir = default_working_dir
        self.snapshot_name: str | None = snapshot_name
        # Session id to the id of the last command this runtime started or saw
        # there. Both are random and never reused, and the status read beside
        # it on every poll decides whether it still names the latest command.
        self._command_ids: dict[str, str] = {}

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
    def image_prebuilt(self) -> bool:
        return self.snapshot_name is not None

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

    async def recover_from_error(self, timeout: int = 120) -> None:
        # Every state the map does not list reads as error, and ``recoverable``
        # is False on any healthy sandbox, including one an earlier attempt of
        # this call already recovered. Only a raw ``error`` carries a verdict,
        # and Daytona refuses both start and recover on one it will not recover.
        state = _raw_state(self._sandbox)
        if state == "started":
            return
        recoverable = getattr(self._sandbox, "recoverable", None)
        if state != "error" or recoverable is None:
            await self.start(timeout=timeout)
        elif recoverable:
            await self._sandbox.recover(timeout=timeout)
        else:
            # The reason stays out of the message, which classifiers scan for words.
            logger.warning(
                "Daytona will not recover errored sandbox",
                sandbox_id=self.id,
                reason=getattr(self._sandbox, "error_reason", None),
            )
            raise SandboxGoneError(self.id, "errored and not recoverable")

    async def stop(self, timeout: int = 120, *, force: bool = False) -> None:
        await self._sandbox.stop(timeout=timeout, force=force)

    async def delete(self) -> None:
        await self._sandbox.delete()

    updates_env = True

    async def update_env(
        self, env: dict[str, str], *, unset: Sequence[str] = ()
    ) -> None:
        """Update the daemon environment inherited by newly spawned processes."""
        await self._sandbox.update_env(env, unset=list(unset))

    async def update_secrets(self, secrets: dict[str, str]) -> None:
        """Replace the complete set of Daytona Secret mounts."""
        await self._sandbox.update_secrets(secrets)

    async def get_state(self) -> RuntimeState:
        return _STATE_MAP.get(_raw_state(self._sandbox), RuntimeState.ERROR)

    async def refresh_state(self) -> RuntimeState:
        await self._sandbox.refresh_data()
        return await self.get_state()

    # -- Execution --

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        # Commands here already run as root, so exec_as_root keeps the default.
        result = await self._sandbox.process.exec(command, env=env, timeout=timeout)
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

    # -- File I/O --

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        await self._sandbox.fs.upload_file(content, dest_path, timeout=_FS_TIMEOUT_S)

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        batch = [FileUpload(source=src, destination=dst) for src, dst in files]
        await self._sandbox.fs.upload_files(batch, timeout=_FS_TIMEOUT_S)

    async def download_file(self, path: str) -> bytes:
        # SDK's download_file uses *args dispatch; pass timeout positionally.
        return await self._sandbox.fs.download_file(path, _FS_TIMEOUT_S)

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        stream = await self._sandbox.fs.download_file_stream(
            path, timeout=_STREAM_TIMEOUT_S
        )
        try:
            async for chunk in stream:
                yield chunk
        finally:
            # Closes the SDK's connection when the reader stops early.
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        result = await self._sandbox.fs.list_files(directory)
        # SDK returns a list of file-info objects; normalize to dicts.
        if result and hasattr(result[0], "__dict__"):
            return [vars(f) for f in result]
        return result

    # -- Sessions (background processes) --

    async def create_session(self, session_id: str) -> None:
        await self._sandbox.process.create_session(session_id)

    async def session_execute(self, session_id: str, command: str) -> str:
        req = SessionExecuteRequest(command=command, run_async=True)
        result = await self._sandbox.process.execute_session_command(session_id, req)
        self._remember_command(session_id, result.cmd_id)
        return result.cmd_id

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        cmd, logs = await asyncio.gather(
            self._sandbox.process.get_session_command(session_id, command_id),
            self._sandbox.process.get_session_command_logs(session_id, command_id),
        )
        return SessionCommandResult(
            cmd_id=command_id,
            exit_code=cmd.exit_code,
            stdout=logs.stdout or "",
            stderr=logs.stderr or "",
        )

    def _remember_command(self, session_id: str, command_id: str | None) -> None:
        if not command_id:
            return
        self._command_ids.pop(session_id, None)
        self._command_ids[session_id] = command_id
        if len(self._command_ids) > _COMMAND_IDS_KEPT:
            del self._command_ids[next(iter(self._command_ids))]

    async def session_logs(self, session_id: str) -> SessionCommandResult | None:
        process = self._sandbox.process
        known = self._command_ids.get(session_id)
        # A command's logs are read by its id, which the status read names.
        # When this runtime knows it already, the two reads go out together,
        # and the status still decides which command's logs are returned.
        reads = [process.get_session(session_id)]
        if known is not None:
            reads.append(process.get_session_command_logs(session_id, known))
        session, *early = await asyncio.gather(*reads, return_exceptions=True)
        if isinstance(session, DaytonaNotFoundError):
            self._command_ids.pop(session_id, None)
            return None
        if isinstance(session, BaseException):
            raise session
        if not session.commands:
            return None
        command = session.commands[-1]
        self._remember_command(session_id, command.id)
        logs = early[0] if early and command.id == known else None
        # Logs read alongside a finished status may predate the last output
        # its command wrote, so those are read again, after it.
        if logs is None or isinstance(logs, BaseException) or command.exit_code is not None:
            try:
                logs = await process.get_session_command_logs(session_id, command.id)
            except DaytonaNotFoundError:
                # Deleted since its status was read, by whoever read it first.
                self._command_ids.pop(session_id, None)
                return None
        return SessionCommandResult(
            cmd_id=command.id,
            exit_code=command.exit_code,
            stdout=logs.stdout or "",
            stderr=logs.stderr or "",
        )

    async def list_sessions(self) -> list[SessionState]:
        sessions = await self._sandbox.process.list_sessions()
        # A session with no command yet is one a launch is about to use, so
        # it counts as running and eviction leaves it alone.
        return [
            SessionState(
                session_id=s.session_id,
                running=not s.commands or any(c.exit_code is None for c in s.commands),
            )
            for s in sessions
        ]

    async def delete_session(self, session_id: str) -> None:
        self._command_ids.pop(session_id, None)
        try:
            await self._sandbox.process.delete_session(session_id)
        except DaytonaNotFoundError as exc:
            raise FileNotFoundError(f"No session {session_id}") from exc

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

    archives = True

    async def archive(self) -> None:
        await self._sandbox.archive()

    autostops = True

    async def set_autostop_interval(self, minutes: int) -> None:
        """Set the idle auto-stop interval in minutes (0 disables auto-stop)."""
        await self._sandbox.set_autostop_interval(minutes)

    async def get_metadata(self) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "id": self.id,
            "working_dir": self.working_dir,
        }
        for attr in (
            "cpu",
            "memory",
            "disk",
            "gpu",
            "created_at",
            "auto_stop_interval",
        ):
            val = getattr(self._sandbox, attr, None)
            if val is not None:
                meta[attr] = val
        state = getattr(self._sandbox, "state", None)
        if state is not None:
            meta["state"] = state.value if hasattr(state, "value") else str(state)
        return meta

    @property
    def raw(self) -> Any:
        """Access the underlying Daytona SDK sandbox object.

        Escape hatch for callers that need SDK-specific functionality
        not yet surfaced through the runtime interface.
        """
        return self._sandbox
