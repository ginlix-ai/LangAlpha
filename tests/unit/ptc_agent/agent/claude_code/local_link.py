"""A Link over a local stand-in connection, so the scripts a user's computer
runs (the CLI installer, the token's writes and reads) run here through the
same Link code, only the connection under it local."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from collections.abc import AsyncIterator
from types import SimpleNamespace

from ptc_agent.core.sandbox.providers._ssh_transport import Link


class LocalProcess:
    """An SSH channel's process, as Link reads it, for a local subprocess."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self.proc = proc
        self.stdin, self.stdout, self.stderr = proc.stdin, proc.stdout, proc.stderr
        self.exit_signal = None

    @property
    def exit_status(self) -> int | None:
        return self.proc.returncode

    def is_closing(self) -> bool:
        return self.proc.returncode is not None

    def send_signal(self, sig: str) -> None:
        if self.proc.returncode is None:
            self.proc.send_signal(getattr(signal, f"SIG{sig}"))

    def close(self) -> None:
        if self.proc.returncode is None:
            self.proc.kill()

    async def wait_closed(self) -> None:
        await self.proc.wait()


class LocalConn:
    """The asyncssh connection a Link rides, on this machine, with ``env`` as sshd would give a command."""

    def __init__(self, path: str, env: dict[str, str] | None = None) -> None:
        self.env = {"PATH": path, "HOME": os.environ.get("HOME", "/tmp"), "LANG": "C", **(env or {})}
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed

    def abort(self) -> None:
        self.closed = True

    async def create_process(self, command: str, encoding: None = None) -> LocalProcess:
        pipe = asyncio.subprocess.PIPE
        return LocalProcess(await asyncio.create_subprocess_shell(command, stdin=pipe, stdout=pipe, stderr=pipe, env=self.env))

    async def start_sftp_client(self) -> LocalSftp:
        return LocalSftp()


class LocalSftp:
    sent: list[str] = []

    @contextlib.asynccontextmanager
    async def open(self, path: str, mode: str) -> AsyncIterator[SimpleNamespace]:
        LocalSftp.sent.append(path)
        with open(path, mode) as f:
            yield SimpleNamespace(write=_async(f.write))

    async def remove(self, path: str) -> None:
        os.unlink(path)

    def exit(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


def _async(call):
    async def run(*args):
        return call(*args)

    return run


def local_link(path: str | None = None, **env: str) -> Link:
    return Link(SimpleNamespace(label="local", host="local"), LocalConn(path or os.environ["PATH"], env))
