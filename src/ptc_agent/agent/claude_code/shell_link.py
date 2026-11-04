"""What a Claude Code turn's launcher runs on: a computer of the user's own
over its pooled SSH connection (``Link``), or a cloud computer's sandbox
(``SandboxLink``). ``SSHLauncher``, its transport and ``cli_install`` use
these and nothing else of either."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Protocol

from ptc_agent.core.sandbox.providers._ssh_transport import ScriptResult


class ShellProcess(Protocol):
    """A process started on a link, in the shape of an SSH channel's: byte
    streams, an exit status, or the signal that ended it."""

    stdin: Any
    stdout: Any
    stderr: Any

    @property
    def exit_status(self) -> int | None: ...

    @property
    def exit_signal(self) -> Any: ...

    def is_closing(self) -> bool: ...

    def close(self) -> None: ...

    async def wait_closed(self) -> None: ...

    def send_signal(self, sig: str) -> None: ...


class ShellLink(Protocol):
    """A machine's scripts and long-lived processes, named ``host`` in what a
    failure says. ``cli_hold`` holds what a CLI needs there for a turn and
    yields what starts it. Where the link has slots, ``kill`` runs a script
    that signals one even when every other slot is taken, and ``long`` one
    that may take minutes, so it holds no slot a short script waits on."""

    host: str

    @property
    def usable(self) -> bool: ...

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]: ...

    async def run(self, script: str, *, timeout: float, kill: bool = False, long: bool = False) -> ScriptResult: ...

    def cli_hold(self, wait_s: float) -> contextlib.AbstractAsyncContextManager[Callable[[str], Awaitable[ShellProcess]]]: ...

    async def upload(self, local: str, remote: str, *, timeout: float) -> None: ...

    async def write_files(self, files: list[tuple[str, bytes]]) -> None: ...
