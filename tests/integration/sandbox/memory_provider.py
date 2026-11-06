"""In-memory SandboxRuntime + SandboxProvider for provider-agnostic integration tests.

This module provides a fully functional sandbox backend that operates entirely
in-process using an in-memory filesystem and subprocess-based Python execution.
It implements the exact same SandboxRuntime/SandboxProvider contracts that
DaytonaRuntime/DaytonaProvider implement, allowing integration tests to exercise
the full PTCSandbox lifecycle without any external infrastructure.

Design principles:
- Every SandboxRuntime abstract method is implemented with real behavior
- File I/O is backed by a dict[str, bytes] in-memory filesystem
- exec() runs commands via asyncio subprocess (real shell)
- State machine (start/stop/delete/archive) is fully functional
- No mocks — this IS the provider, just an in-process one
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import os
import signal
import tempfile
import uuid
from typing import Any

from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
    SandboxTransientError,
    SessionCommandResult,
    SessionState,
    StreamProcess,
)


async def _decoded(source: asyncio.StreamReader, sink: asyncio.StreamReader) -> None:
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    try:
        while chunk := await source.read(65536):
            sink.feed_data(decoder.decode(chunk).encode())
        sink.feed_data(decoder.decode(b"", final=True).encode())
    finally:
        sink.feed_eof()


class _LocalStream(StreamProcess):
    """A local subprocess behind the stream contract, a session of its own so
    a signal reaches its whole process group, its output read as UTF-8 as
    every provider's is."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        assert proc.stdout is not None and proc.stderr is not None
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self._pumps = [
            asyncio.ensure_future(_decoded(proc.stdout, self.stdout)),
            asyncio.ensure_future(_decoded(proc.stderr, self.stderr)),
        ]

    @property
    def failure(self) -> str | None:
        return None

    @property
    def returncode(self) -> int | None:
        code = self._proc.returncode
        # asyncio reports signal N as -N; the contract follows the shell.
        return None if code is None else (128 - code if code < 0 else code)

    async def write(self, data: bytes) -> None:
        if self._proc.returncode is not None:
            raise ConnectionError("The process has ended")
        assert self._proc.stdin is not None
        self._proc.stdin.write(data)
        await self._proc.stdin.drain()

    async def write_eof(self) -> bool:
        assert self._proc.stdin is not None
        self._proc.stdin.close()
        return True

    async def wait(self) -> int:
        await self._proc.wait()
        await asyncio.wait(self._pumps)
        return self.returncode  # type: ignore[return-value]

    async def terminate(self) -> None:
        self._signal(signal.SIGTERM)

    async def kill(self) -> None:
        self._signal(signal.SIGKILL)

    def _signal(self, number: int) -> None:
        if self._proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self._proc.pid, number)

    async def close(self) -> None:
        try:
            await self.kill()
            await self._proc.wait()
        finally:
            for pump in self._pumps:
                pump.cancel()
            await asyncio.gather(*self._pumps, return_exceptions=True)


class MemoryRuntime(SandboxRuntime):
    """In-memory sandbox runtime backed by a temp directory and real subprocesses.

    Uses a real temp directory for file I/O so that exec can access
    the files they create. This gives us realistic behavior while remaining
    fully self-contained.
    """

    def __init__(
        self,
        runtime_id: str,
        work_dir: str,
        env_vars: dict[str, str] | None = None,
    ) -> None:
        self._id = runtime_id
        self._work_dir = work_dir
        self._state = RuntimeState.RUNNING
        self._env_vars = dict(env_vars or {})
        self._deleted = False

        # Create the working directory on disk
        os.makedirs(self._work_dir, exist_ok=True)

    @property
    def id(self) -> str:
        return self._id

    @property
    def working_dir(self) -> str:
        return self._work_dir

    async def fetch_working_dir(self) -> str:
        return self._work_dir

    # -- Lifecycle --

    async def start(self, timeout: int = 120) -> None:
        if self._deleted:
            raise RuntimeError("Cannot start a deleted runtime")
        if self._state == RuntimeState.RUNNING:
            return
        self._state = RuntimeState.RUNNING

    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        if self._deleted:
            raise RuntimeError("Cannot stop a deleted runtime")
        self._state = RuntimeState.STOPPED

    async def delete(self) -> None:
        self._state = RuntimeState.STOPPED
        self._deleted = True

    async def get_state(self) -> RuntimeState:
        if self._deleted:
            return RuntimeState.ERROR
        return self._state

    # -- Execution --

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        self._check_running()
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._work_dir,
                env={**os.environ, **self._env_vars, **(env or {})},
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            stdout = (stdout_bytes or b"").decode("utf-8", errors="replace")
            stderr = (stderr_bytes or b"").decode("utf-8", errors="replace")
            # Match DaytonaRuntime behavior: combine into stdout, empty stderr
            combined = stdout + stderr if stderr else stdout
            return ExecResult(
                stdout=combined, stderr="", exit_code=proc.returncode or 0
            )
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="timeout", exit_code=-1)
        except Exception as e:
            return ExecResult(stdout="", stderr=str(e), exit_code=-1)

    async def open_stream(self, command: str) -> StreamProcess:
        self._check_running()
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._work_dir,
            env={**os.environ, **self._env_vars},
            start_new_session=True,
        )
        return _LocalStream(proc)

    # -- File I/O --

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        self._check_running()
        full_path = self._resolve(dest_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        self._check_running()
        for source, dest in files:
            if isinstance(source, str):
                # source is a local file path
                with open(source, "rb") as f:
                    content = f.read()
            else:
                content = source
            await self.upload_file(content, dest)

    async def download_file(self, path: str) -> bytes:
        self._check_running()
        full_path = self._resolve(path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(full_path, "rb") as f:
            return f.read()

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        self._check_running()
        full_path = self._resolve(directory)
        if not os.path.isdir(full_path):
            return []
        entries = []
        for name in sorted(os.listdir(full_path)):
            entry_path = os.path.join(full_path, name)
            entries.append(
                _FileEntry(
                    name=name,
                    is_dir=os.path.isdir(entry_path),
                )
            )
        return entries

    # -- Sessions (not supported in-process) --

    async def create_session(self, session_id: str) -> None:
        raise NotImplementedError("MemoryRuntime has no sessions")

    async def session_execute(self, session_id: str, command: str) -> str:
        raise NotImplementedError("MemoryRuntime has no sessions")

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        raise NotImplementedError("MemoryRuntime has no sessions")

    async def session_logs(self, session_id: str) -> SessionCommandResult | None:
        raise NotImplementedError("MemoryRuntime has no sessions")

    async def list_sessions(self) -> list[SessionState]:
        raise NotImplementedError("MemoryRuntime has no sessions")

    async def delete_session(self, session_id: str) -> None:
        raise NotImplementedError("MemoryRuntime has no sessions")

    # -- Capabilities & metadata --

    archives = True

    async def archive(self) -> None:
        if self._deleted:
            raise RuntimeError("Cannot archive a deleted runtime")
        self._state = RuntimeState.ARCHIVED

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "working_dir": self._work_dir,
            "state": self._state.value,
            "env_vars_count": len(self._env_vars),
        }

    # -- Internal --

    def _check_running(self) -> None:
        if self._deleted:
            raise RuntimeError("Runtime has been deleted")
        if self._state != RuntimeState.RUNNING:
            raise SandboxTransientError(
                f"Runtime is not running (state={self._state.value})"
            )

    def _resolve(self, path: str) -> str:
        """Resolve a path relative to the working directory."""
        if os.path.isabs(path):
            return path
        return os.path.join(self._work_dir, path)


class _FileEntry:
    """Mimics the Daytona SDK file entry object with .name and .is_dir attributes."""

    def __init__(self, name: str, is_dir: bool) -> None:
        self.name = name
        self.is_dir = is_dir


class MemoryProvider(SandboxProvider):
    """In-memory provider that creates MemoryRuntime instances."""

    def __init__(self, base_dir: str | None = None) -> None:
        self._base_dir = base_dir or tempfile.mkdtemp(prefix="sandbox-test-")
        self._runtimes: dict[str, MemoryRuntime] = {}
        self._closed = False

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> SandboxRuntime:
        if self._closed:
            raise RuntimeError("Provider is closed")
        runtime_id = f"mem-{uuid.uuid4().hex[:12]}"
        work_dir = os.path.join(self._base_dir, runtime_id)
        runtime = MemoryRuntime(runtime_id, work_dir, env_vars)
        self._runtimes[runtime_id] = runtime
        return runtime

    async def get(self, sandbox_id: str) -> SandboxRuntime:
        if self._closed:
            raise RuntimeError("Provider is closed")
        runtime = self._runtimes.get(sandbox_id)
        if runtime is None or runtime._deleted:
            raise RuntimeError(f"Runtime not found: {sandbox_id}")
        return runtime

    async def close(self) -> None:
        self._closed = True

    def is_transient_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return "transient" in msg or "connection" in msg

