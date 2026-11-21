"""The runtime's own long-lived processes, the bridge and the proxy: how one is
started apart from the command that needs it, how it binds, and how it serves
for as long as the browser's stream is there to serve."""

from __future__ import annotations

import asyncio
import socket
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from .protocol import STREAM_PORT, state_file

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]

_PROBE_TIMEOUT_S = 0.3
_POLL_S = 0.05
_READ_BYTES = 256 * 1024
_WATCH_INTERVAL_S = 5.0
# Long enough for a daemon restart and for a first start on a loaded machine.
_STREAM_GONE_S = 45.0


def listening(port: int, wait_s: float = 0.0) -> bool:
    """Whether something accepts on loopback *port*, waiting up to *wait_s* for it."""
    deadline = time.monotonic() + wait_s
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=_PROBE_TIMEOUT_S).close()
            return True
        except OSError:
            if time.monotonic() >= deadline:
                return False
        time.sleep(_POLL_S)


def detach(*argv: str, log: str | None = None) -> None:
    """Run this package with *argv* on its own, its output to the state file *log*.

    Its own session with no inherited stream, or a Daytona exec waits for it
    until the call's timeout.
    """
    command = [sys.executable, "-I", str(Path(__file__).resolve().parent), *argv]
    if log is None:
        _spawn(command, subprocess.DEVNULL)
        return
    path = state_file(log)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as out:
        _spawn(command, out)


def _spawn(command: list[str], out: object) -> None:
    subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=out,
        stderr=out,
        cwd="/",
        start_new_session=True,
        close_fds=True,
    )


def ensure(command: str, port: int, log: str, wait_s: float = 0.0) -> bool:
    """Whether this package's *command* listens on *port*: started once when it
    does not, then waited for up to *wait_s*. Two racing starts are fine: only
    one can listen, and the other exits having written nothing."""
    if listening(port):
        return True
    detach(command, log=log)
    return wait_s > 0 and listening(port, wait_s)


def bind(host: str, port: int, backlog: int) -> socket.socket | None:
    """A listening socket, or None when another is already up on *port*."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        # Two starts can both bind a reusable address, and only one can then listen.
        listener.bind((host, port))
        listener.listen(backlog)
    except OSError:
        listener.close()
        return None
    listener.setblocking(False)
    return listener


async def serve_while_stream(
    listener: socket.socket,
    handle: Handler,
    limit: int,
    tick: Callable[[bool], Awaitable[None]],
) -> None:
    """Serve *handle* until the stream has been gone long enough that a restart
    is not what it is waiting out, so it ends with the browser however the
    browser ended. *tick* runs at each look, told whether the stream is up."""
    server = await asyncio.start_server(handle, sock=listener, limit=limit)
    last_seen = time.monotonic()
    async with server:
        while time.monotonic() - last_seen < _STREAM_GONE_S:
            await asyncio.sleep(_WATCH_INTERVAL_S)
            up = listening(STREAM_PORT)
            if up:
                last_seen = time.monotonic()
            await tick(up)


async def answer(writer: asyncio.StreamWriter, status: bytes) -> None:
    """Answer with *status* and nothing else, and close."""
    writer.write(b"HTTP/1.1 " + status + b"\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
    try:
        await writer.drain()
    except OSError:
        pass
    writer.close()


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, half_close: bool = False) -> None:
    """Copy *reader* to *writer* until it ends. With *half_close* its end only
    ends the writing side, and the caller closes both; else the writer closes."""
    try:
        while data := await reader.read(_READ_BYTES):
            writer.write(data)
            await writer.drain()
        if half_close and writer.can_write_eof():
            writer.write_eof()
    except (OSError, asyncio.CancelledError):
        writer.close()
        return
    if not half_close:
        writer.close()


def split_head(head: bytes) -> list[bytes] | None:
    """A request head's lines, or None for one a parser could read two ways."""
    if not head.endswith(b"\r\n\r\n"):
        return None
    lines = head[: -len(b"\r\n\r\n")].split(b"\r\n")
    if any(b"\r" in line or b"\n" in line for line in lines):
        return None
    return lines
