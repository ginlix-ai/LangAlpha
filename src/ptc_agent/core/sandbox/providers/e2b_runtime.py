"""The runtime half of the E2B provider: one E2B sandbox behind ``SandboxRuntime``.

An E2B sandbox has a deadline rather than an idle timer: it pauses when its
timeout runs out whatever it is doing. The runtime turns that into the idle
auto-stop the rest of the system expects by pushing the deadline out as work
arrives (``_keep_alive``), so a sandbox pauses once nothing has touched it for
the window.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import Any

import structlog
from e2b import (
    AsyncSandbox,
    CommandExitException,
    CommandResult,
    FileNotFoundException,
    SandboxInfo,
)
from e2b.sandbox.filesystem.filesystem import FileType, WriteEntry

from ptc_agent.config.core import PlatformSecretDefinition
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT, SandboxLayout
from ptc_agent.core.sandbox.providers._exec_sessions import ExecSessionRuntime
from ptc_agent.core.sandbox.providers.e2b_secrets import egress_rules
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxTransientError,
)

logger = structlog.get_logger(__name__)

_STATE_MAP: dict[str, RuntimeState] = {
    "running": RuntimeState.RUNNING,
    "paused": RuntimeState.STOPPED,
}

# Every command and file operation runs as root, as on Daytona: the image is
# built as root, HOME is /root, and packages install into the system Python.
_USER = "root"
# The SDK's own default, stated so a hung envd connection surfaces as a
# transient error within the PTCSandbox retry envelope.
_FS_TIMEOUT_S = 60
# A streamed download is paced by the client that receives it.
_STREAM_TIMEOUT_S = 60 * 60
# Pausing snapshots memory at about 4 s per GiB, so the largest tier needs more
# than a plain request gets.
_PAUSE_TIMEOUT_S = 180
# The stop's one exec that ends the computer's jobs before the pause: a TERM
# grace of 3 s, and the round trip.
_END_JOBS_TIMEOUT_S = 30
# Headroom past a command's own timeout, so the deadline never lands mid-command.
_COMMAND_MARGIN_S = 120
# How long a state read stays good enough to attach on without reading again.
_STATE_FRESH_S = 10.0
# The header E2B requires on every request to a port of a sandbox created
# without public traffic.
TRAFFIC_TOKEN_HEADER = "e2b-traffic-access-token"


def _state_of(info: SandboxInfo) -> RuntimeState:
    raw = getattr(info.state, "value", info.state)
    return _STATE_MAP.get(str(raw), RuntimeState.ERROR)


def _seconds_left(info: SandboxInfo) -> float:
    end = info.end_at if info.end_at.tzinfo else info.end_at.replace(tzinfo=UTC)
    return (end - datetime.now(UTC)).total_seconds()


def _window(minutes: int | None, *, default_s: int, max_s: int) -> int:
    """Seconds a sandbox may sit idle, capped at the plan's longest run.

    ``None`` takes ``default_s``, and 0 asks for as long as E2B allows, since
    E2B has no idle timer to turn off.
    """
    seconds = default_s if minutes is None else (minutes * 60 or max_s)
    return max(60, min(seconds, max_s))


# envd reports a process a signal ended as exit -1, naming the signal the way
# Go does ("signal: killed"). A shell reports 128 + its number, which is what
# the other providers return, so an OOM kill reads 137 on every one of them.
# Linux numbers, the sandbox's, not this host's (SIGBUS is 10 on macOS).
_SIGNAL_NUMBERS = {
    "hangup": 1,
    "interrupt": 2,
    "aborted": 6,
    "bus error": 7,
    "killed": 9,
    "segmentation fault": 11,
    "terminated": 15,
}


def _exit_status(exit_code: int, error: str | None) -> int:
    """The shell's exit status for an envd result."""
    if exit_code != -1 or not error or not error.startswith("signal: "):
        return exit_code
    name = error.removeprefix("signal: ").removesuffix(" (core dumped)")
    number = _SIGNAL_NUMBERS.get(name)
    return exit_code if number is None else 128 + number


class E2BRuntime(ExecSessionRuntime):
    """Runtime over one E2B sandbox, attached lazily and never resumed implicitly.

    ``AsyncSandbox.connect`` resumes a paused sandbox, so the runtime only
    connects when the sandbox reads as running. A stopped computer then stays
    stopped until something calls ``start``, which is how the reconnect path
    decides when to bring it back.
    """

    # A paused sandbox keeps its processes (its memory is snapshotted), and a
    # cold boot ends them all, which is exactly when the kernel's boot id changes.
    session_boot_key_sh = "$(cat /proc/sys/kernel/random/boot_id)"

    def __init__(
        self,
        client: Any,
        sandbox_id: str,
        *,
        sandbox: AsyncSandbox | None = None,
        info: SandboxInfo | None = None,
        default_working_dir: str = DEFAULT_SANDBOX_ROOT,
        window_s: int,
        max_timeout_s: int,
        secret_routes: Mapping[str, PlatformSecretDefinition] | None = None,
        public_traffic: bool = True,
    ) -> None:
        """Wrap a sandbox just created (``sandbox``) or just looked up (``info``).

        ``public_traffic`` is the deployment's setting, not the sandbox's: a
        sandbox created while it was on stays public, and its URLs are still
        proxied once it is off, so no raw sandbox host reaches a browser.
        """
        self._client = client
        self._public_traffic = public_traffic
        self._secret_routes = secret_routes or {}
        self._id = sandbox_id
        self._state = _state_of(info) if info is not None else RuntimeState.RUNNING
        self._sandbox = sandbox
        self._info = info
        self._state_read_at = time.monotonic() if info is not None else 0.0
        self._working_dir = default_working_dir
        self._window_s = window_s
        self._max_timeout_s = max_timeout_s
        # Monotonic time until which this runtime knows the sandbox stays up.
        # Process-local by design: it only saves round trips, and a stale value
        # costs one extra deadline read, never a wrong answer.
        self._alive_until = time.monotonic() + window_s if sandbox else 0.0
        self._attach_lock = asyncio.Lock()
        self._renew_lock = asyncio.Lock()

    # -- Properties --

    @property
    def id(self) -> str:
        return self._id

    @property
    def working_dir(self) -> str:
        return self._working_dir

    @property
    def image_prebuilt(self) -> bool:
        # A create starts only from the tier's template, never a bare image.
        return True

    # -- Lifecycle --

    async def start(self, timeout: int = 120) -> None:
        sandbox = await self._client.AsyncSandbox.connect(
            self._id, timeout=self._window_s, request_timeout=timeout
        )
        self._sandbox = sandbox
        self._state = RuntimeState.RUNNING
        # connect only ever extends the deadline, to at least the window.
        self._alive_until = time.monotonic() + self._window_s

    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        await self._end_jobs()
        await self._client.AsyncSandbox.pause(
            self._id, request_timeout=max(timeout, _PAUSE_TIMEOUT_S)
        )
        self._sandbox = None
        self._state = RuntimeState.STOPPED
        self._alive_until = 0.0

    async def _end_jobs(self) -> None:
        """End the computer's detached jobs before a pause, which keeps every
        process: a job would otherwise run on once the computer starts again,
        long after its command was reported lost. Best effort, and only on a
        sandbox already running (an exec never resumes one); what this misses,
        the bring-up after the next start ends."""
        from ptc_agent.core.sandbox import jobs

        if self._sandbox is None and self._state is not RuntimeState.RUNNING:
            return
        try:
            async with asyncio.timeout(_END_JOBS_TIMEOUT_S):
                ended = await jobs.end_jobs(
                    self.exec, SandboxLayout(self._working_dir).system_jobs
                )
        except Exception as e:  # noqa: BLE001 - the pause must go on
            logger.warning(
                "Could not end jobs before pausing E2B sandbox",
                sandbox_id=self._id,
                error=str(e) or type(e).__name__,
            )
            return
        if ended:
            logger.info("Ended jobs before pausing E2B sandbox", sandbox_id=self._id, jobs=ended)

    async def delete(self) -> None:
        # False means it was already gone, which is the outcome asked for.
        await self._client.AsyncSandbox.kill(self._id)
        self._sandbox = None

    async def update_secrets(self, secrets: dict[str, str]) -> None:
        """Replace this sandbox's egress rules with the ones *secrets* bind.

        E2B's update replaces the whole egress configuration, and nothing else
        sets one on our sandboxes, so the rules are the complete state. Whether
        ports take public traffic is not part of it: E2B fixes that at create
        and the update leaves it as it was. The env stays as created: E2B
        cannot change it on a running sandbox, and the marker it holds never
        changes.
        """
        await self._client.AsyncSandbox.update_network(
            self._id, {"rules": egress_rules(secrets, self._secret_routes)}
        )

    async def get_state(self) -> RuntimeState:
        return self._state

    async def refresh_state(self) -> RuntimeState:
        info = await self._client.AsyncSandbox.get_info(self._id)
        self._info = info
        self._state = _state_of(info)
        self._state_read_at = time.monotonic()
        if self._state is not RuntimeState.RUNNING:
            self._sandbox = None
        return self._state

    async def _handle(self) -> AsyncSandbox:
        """The connected sandbox, attaching only to one that is running."""
        if self._sandbox is not None:
            return self._sandbox
        async with self._attach_lock:
            if self._sandbox is not None:
                return self._sandbox
            state = self._state
            if time.monotonic() - self._state_read_at > _STATE_FRESH_S:
                state = await self.refresh_state()
            if state is not RuntimeState.RUNNING:
                raise self._not_running()
            self._sandbox = await self._client.AsyncSandbox.connect(
                self._id, timeout=self._window_s
            )
            self._alive_until = time.monotonic() + self._window_s
            return self._sandbox

    def _not_running(self) -> SandboxTransientError:
        # Transient, so a caller with a reconnect path starts it.
        return SandboxTransientError(
            f"E2B sandbox {self._id} is {self._state.value}, not running"
        )

    async def _keep_alive(self, busy_for_s: int = 0) -> None:
        """Push the pause deadline past this call, never pulling it in.

        ``set_timeout`` sets the deadline outright, so the current one is read
        first: a longer window set through another runtime must survive this
        runtime's shorter default. Renewal waits until the deadline has
        slipped a few minutes, which keeps an active sandbox to about one
        read and one write per few minutes and bounds how early it pauses.
        """
        want = min(max(self._window_s, busy_for_s + _COMMAND_MARGIN_S), self._max_timeout_s)
        slack = min(300, want // 4)
        if self._alive_until - time.monotonic() >= want - slack:
            return
        async with self._renew_lock:
            now = time.monotonic()
            if self._alive_until - now >= want - slack:
                return
            try:
                state = await self.refresh_state()
            except Exception as e:
                # The operation that follows reports any real failure itself.
                logger.debug(
                    "Could not read E2B sandbox deadline",
                    sandbox_id=self._id,
                    error=str(e),
                )
                return
            if state is not RuntimeState.RUNNING:
                # Paused under a handle this runtime still held (its deadline
                # ran out, or another worker stopped it). refresh_state dropped
                # the handle, so the reconnect path, not this call, decides to
                # bring it back.
                raise self._not_running()
            left = _seconds_left(self._info)
            if left < want - slack:
                try:
                    await self._client.AsyncSandbox.set_timeout(self._id, want)
                    left = want
                except Exception as e:
                    logger.debug(
                        "Could not renew E2B sandbox deadline",
                        sandbox_id=self._id,
                        error=str(e),
                    )
            self._alive_until = now + left

    async def _ready(self, busy_for_s: int = 0) -> AsyncSandbox:
        """The connected sandbox, kept up past work expected to take ``busy_for_s``."""
        sandbox = await self._handle()
        await self._keep_alive(busy_for_s)
        return sandbox

    # -- Execution --

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        # Commands here already run as root, so exec_as_root keeps the default.
        # The streams merge in the shell rather than by joining two buffers,
        # which keeps them in the order they were written.
        sandbox = await self._ready(timeout)
        result = await self._run(
            sandbox, f"exec 2>&1\n{command}", timeout=timeout, envs=env
        )
        return ExecResult(
            stdout=result.stdout + result.stderr, stderr="", exit_code=result.exit_code
        )

    @staticmethod
    async def _run(
        sandbox: AsyncSandbox,
        command: str,
        *,
        timeout: int,
        envs: dict[str, str] | None = None,
    ) -> CommandResult:
        try:
            return await sandbox.commands.run(
                command, envs=envs or None, user=_USER, timeout=timeout
            )
        except CommandExitException as exc:
            # A nonzero exit is a result here, not an error.
            exc.exit_code = _exit_status(exc.exit_code, exc.error)
            return exc

    # -- File I/O --

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        sandbox = await self._ready()
        await sandbox.files.write(
            dest_path, content, user=_USER, request_timeout=_FS_TIMEOUT_S
        )

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        if not files:
            return
        sandbox = await self._ready()
        entries: list[WriteEntry] = []
        for source, dest in files:
            if isinstance(source, str):
                with open(source, "rb") as f:
                    source = f.read()
            entries.append(WriteEntry(path=dest, data=source))
        await sandbox.files.write_files(
            entries, user=_USER, request_timeout=_FS_TIMEOUT_S
        )

    async def download_file(self, path: str) -> bytes:
        sandbox = await self._ready()
        try:
            data = await sandbox.files.read(
                path, format="bytes", user=_USER, request_timeout=_FS_TIMEOUT_S
            )
        except FileNotFoundException as exc:
            raise FileNotFoundError(path) from exc
        return bytes(data)

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        sandbox = await self._ready()
        try:
            stream = await sandbox.files.read(
                path, format="stream", user=_USER, request_timeout=_STREAM_TIMEOUT_S
            )
        except FileNotFoundException as exc:
            raise FileNotFoundError(path) from exc
        try:
            async for chunk in stream:
                yield bytes(chunk)
        finally:
            # Closes the connection when the reader stops early.
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        sandbox = await self._ready()
        try:
            entries = await sandbox.files.list(
                directory, depth=1, user=_USER, request_timeout=_FS_TIMEOUT_S
            )
        except FileNotFoundException as exc:
            raise FileNotFoundError(directory) from exc
        return [
            {
                "name": entry.name,
                "is_dir": entry.type == FileType.DIR,
                "size": entry.size,
                "mode": entry.mode,
                "permissions": entry.permissions,
                "owner": entry.owner,
                "group": entry.group,
                "mod_time": entry.modified_time.isoformat()
                if entry.modified_time
                else None,
            }
            for entry in entries
        ]

    # -- Preview URLs --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """A port's URL, which does not expire, with the header a private one needs.

        A private sandbox answers only requests that carry its traffic token,
        which a browser cannot add, so its URL is never ``browser_ready``. The
        token does not change across a pause, so the headers stay good for the
        sandbox's life.
        """
        sandbox = await self._handle()
        token = sandbox.traffic_access_token
        return PreviewInfo(
            url=f"https://{sandbox.get_host(port)}",
            token=None,
            auth_headers={TRAFFIC_TOKEN_HEADER: token} if token else {},
            browser_ready=self._public_traffic and not token,
        )

    async def get_preview_link(self, port: int) -> PreviewInfo:
        return await self.get_preview_url(port)

    async def note_activity(self) -> None:
        # Requests to the sandbox's URL do not move E2B's deadline.
        await self._keep_alive()

    # -- Capabilities & metadata --

    async def archive(self) -> None:
        raise NotImplementedError("E2B provider does not support archive")

    autostops = True

    async def set_autostop_interval(self, minutes: int) -> None:
        """Set how long the sandbox may sit idle; 0 keeps it up as long as E2B allows.

        Applied to the deadline outright, so shortening the window takes effect
        now rather than when the longer deadline runs out.
        """
        self._window_s = _window(
            minutes, default_s=self._window_s, max_s=self._max_timeout_s
        )
        if self._state is RuntimeState.RUNNING:
            await self._client.AsyncSandbox.set_timeout(self._id, self._window_s)
            self._alive_until = time.monotonic() + self._window_s

    async def get_metadata(self) -> dict[str, Any]:
        info = self._info
        if info is None:
            await self.refresh_state()
            info = self._info
        meta: dict[str, Any] = {
            "id": self._id,
            "working_dir": self._working_dir,
            "state": self._state.value,
            "provider": "e2b",
            "auto_stop_interval": self._window_s // 60,
        }
        if info is not None:
            meta["cpu"] = info.cpu_count
            meta["memory"] = round(info.memory_mb / 1024, 2)
            meta["created_at"] = info.started_at.isoformat()
            if info.name:
                meta["template"] = info.name
        return meta
