"""Naming MCP connection failures the SDK only reports as "Connection closed".

A stdio server that dies before the handshake reaches us as a CONNECTION_CLOSED
error buried in TaskGroup wrappers, with the child's real traceback nowhere in
the exception. These two pieces recover it: a stderr tail captured off the
subprocess, and a classifier that turns the pair into a one-line diagnosis.
"""

import os
import threading
from collections import deque

from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED


def _contains_connection_closed(error: BaseException) -> bool:
    if isinstance(error, MCPError) and error.code == CONNECTION_CLOSED:
        return True
    if isinstance(error, BaseExceptionGroup):
        return any(_contains_connection_closed(sub) for sub in error.exceptions)
    return _contains_connection_closed(error.__cause__) if error.__cause__ else False


def classify_startup_failure(error: BaseException, stderr_tail: str) -> str | None:
    """Name the failure when a stdio server process dies before the handshake.

    The SDK surfaces a crashed child as CONNECTION_CLOSED buried in TaskGroup
    wrappers; the child's actual traceback exists only in our stderr capture.
    Returns a one-line human diagnosis, or None for other failure shapes.
    """
    if not _contains_connection_closed(error):
        return None
    if "No module named 'mcp." in stderr_tail or (
        "ImportError" in stderr_tail and "from mcp." in stderr_tail
    ):
        return (
            "server process crashed importing an MCP SDK module its runtime "
            "does not provide — its environment pins an incompatible mcp "
            "version; launch it isolated (uvx/npx) with pinned versions"
        )
    if stderr_tail:
        return (
            "server process exited before completing the MCP handshake — "
            "see stderr_tail for the crash output"
        )
    return (
        "server process exited before completing the MCP handshake, "
        "with no stderr output"
    )


class StderrTail:
    """Bounded in-memory tail of an MCP subprocess's stderr.

    ``errlog`` reaches ``subprocess.Popen`` as the child's stderr, so it must
    be a real file descriptor: a pipe drained by a daemon thread into a
    bounded deque. Steady-state server chatter never reaches our logs; on a
    connection failure :meth:`tail` recovers the crash output that would
    otherwise surface only as an opaque "Connection closed".
    """

    def __init__(self, max_lines: int = 80) -> None:
        self._lines: deque[str] = deque(maxlen=max_lines)
        read_fd, write_fd = os.pipe()
        self.writer = os.fdopen(write_fd, "w")
        self._reader = os.fdopen(read_fd, "r", errors="replace")
        # The drain thread lives for the connection's whole lifetime (not just
        # connect): it copies subprocess stderr into the deque until EOF.
        self._thread = threading.Thread(target=self._drain, daemon=True)
        self._thread.start()

    def _drain(self) -> None:
        # The thread owns the read end: EOF arrives once every write end
        # (ours and the exited subprocess's dup) is closed.
        with self._reader:
            for line in self._reader:
                self._lines.append(line.rstrip("\n"))

    def tail(self, *, drain: bool = False) -> str:
        """Snapshot of the captured lines.

        Pass ``drain=True`` on the failure path to close the writer and join
        the drain thread first, so the read can't race the daemon thread still
        appending the subprocess's dying output into the bounded deque.
        """
        if drain:
            self.close()
            self._thread.join(timeout=0.25)
        return "\n".join(list(self._lines))

    def close(self) -> None:
        try:
            self.writer.close()
        except OSError:
            pass
