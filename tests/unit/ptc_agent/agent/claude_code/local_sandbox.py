"""A sandbox runtime on this machine, so ``sandbox_launcher`` runs here through
the same code a cloud computer's runtime takes: exec merges a command's
streams under sh as Daytona's and E2B's do, uploads land as files, and a
stream is a local process. ``eof=False`` stands in for a provider that cannot
close a process's stdin."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from pathlib import Path

from ptc_agent.core.sandbox.runtime import ExecResult, StreamProcess


class LocalStream(StreamProcess):
    def __init__(self, proc: asyncio.subprocess.Process, eof: bool) -> None:
        self._proc = proc
        self._eof = eof
        self.stdout = proc.stdout
        self.stderr = proc.stderr
        self.writes: list[bytes] = []

    @property
    def returncode(self) -> int | None:
        code = self._proc.returncode
        return 128 - code if code is not None and code < 0 else code

    @property
    def failure(self) -> str | None:
        return None

    async def write(self, data: bytes) -> None:
        if self._proc.returncode is not None:
            raise ConnectionError("the process ended")
        self.writes.append(data)
        self._proc.stdin.write(data)
        await self._proc.stdin.drain()

    async def write_eof(self) -> bool:
        if not self._eof:
            return False
        with contextlib.suppress(Exception):
            self._proc.stdin.close()
        return True

    async def wait(self) -> int:
        await self._proc.wait()
        return self.returncode

    def _signal(self, sig: int) -> None:
        if self._proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self._proc.send_signal(sig)

    async def terminate(self) -> None:
        self._signal(signal.SIGTERM)

    async def kill(self) -> None:
        self._signal(signal.SIGKILL)

    async def close(self) -> None:
        await self.kill()
        await self._proc.wait()


class LocalSandboxRuntime:
    """The part of a ``SandboxRuntime`` the launcher uses, on this machine, rooted at ``root``."""

    def __init__(self, root: Path, *, eof: bool = True, env: dict[str, str] | None = None) -> None:
        self.root = root
        self.eof = eof
        self.env = {"PATH": os.environ["PATH"], "HOME": str(root.parent), "LANG": "C", **(env or {})}
        self.uploads: list[str] = []
        self.streams: list[LocalStream] = []
        self.scripts: list[str] = []

    @property
    def working_dir(self) -> str:
        return str(self.root)

    async def exec(self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None) -> ExecResult:
        self.scripts.append(command)
        proc = await asyncio.create_subprocess_exec(
            "sh", "-c", f"exec 2>&1\n{command}", cwd=self.root, env={**self.env, **(env or {})},
            stdout=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(stdout=out.decode(errors="replace"), stderr="", exit_code=proc.returncode)

    exec_as_root = exec

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        for source, dest in files:
            data = Path(source).read_bytes() if isinstance(source, str) else source
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(data)
            self.uploads.append(dest)

    async def open_stream(self, command: str) -> LocalStream:
        pipe = asyncio.subprocess.PIPE
        proc = await asyncio.create_subprocess_exec(
            "bash", "-c", command, cwd=self.root, env=self.env, stdin=pipe, stdout=pipe,
            stderr=pipe, start_new_session=True,
        )
        stream = LocalStream(proc, self.eof)
        self.streams.append(stream)
        return stream
