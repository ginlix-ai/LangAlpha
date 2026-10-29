"""Naming MCP connection failures the SDK reports only generically.

A stdio server that dies before the handshake reaches us as a CONNECTION_CLOSED
error buried in TaskGroup wrappers, with the child's real traceback nowhere in
the exception. These two pieces recover it: a stderr tail captured off the
subprocess, and a classifier that turns the pair into a one-line diagnosis.

A remote (HTTP/SSE) server's refusal has the same problem one layer up, and
:func:`classify_remote_failure` names it from the HTTP status instead.
"""

import os
import ssl
import threading
from collections import deque
from typing import NamedTuple

import httpx2
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


class RemoteFailure(NamedTuple):
    reason: str
    transient: bool


def _find(
    error: BaseException,
    kinds: tuple[type[BaseException], ...],
    *,
    implicit: bool = False,
) -> BaseException | None:
    if isinstance(error, kinds):
        return error
    if isinstance(error, BaseExceptionGroup):
        for sub in error.exceptions:
            if (found := _find(sub, kinds, implicit=implicit)) is not None:
                return found
    after = error.__cause__ or (error.__context__ if implicit else None)
    return _find(after, kinds, implicit=implicit) if after else None


def _leaf(error: BaseException) -> BaseException:
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    return error


def classify_remote_failure(error: BaseException, http_status: int | None) -> RemoteFailure:
    """Name a failed HTTP/SSE connection and say whether trying again can help.

    Streamable HTTP folds every non-2xx answer into one generic "Server returned
    an error response", so the status comes from the transport's response hook,
    not the error. A 4xx is an auth or configuration fault that a retry only
    repeats, and so are a URL with no scheme and a certificate that fails
    verification; a 5xx, a 408 or 429, a refused or dropped connection, or a
    timeout is what a server in the middle of a restart looks like.
    """
    if http_status is not None and http_status >= 400:
        if http_status >= 500 or http_status in (408, 429):
            return RemoteFailure(f"server unavailable (HTTP {http_status})", True)
        if http_status in (401, 403):
            return RemoteFailure(
                f"server refused the credentials (HTTP {http_status}); "
                "check the headers configured for it",
                False,
            )
        if http_status == 404:
            return RemoteFailure("server answered HTTP 404; check its URL", False)
        return RemoteFailure(f"server rejected the connection (HTTP {http_status})", False)
    # An unset ${VAR} standing for the whole URL leaves it with no scheme.
    if (bad_url := _find(error, (httpx2.UnsupportedProtocol,))) is not None:
        return RemoteFailure(
            f"server URL is unusable ({bad_url}); check it and the variables in it", False
        )
    # httpcore raises its ConnectError while handling the ssl error, so the
    # verdict is only the ConnectError's context; alone it reads as refused.
    if (bad_cert := _find(error, (ssl.SSLCertVerificationError,), implicit=True)) is not None:
        return RemoteFailure(
            f"server's TLS certificate failed verification ({bad_cert})", False
        )
    # TimeoutError and ConnectionRefusedError are OSErrors.
    unreachable = _find(error, (httpx2.TransportError, OSError))
    if unreachable is not None:
        return RemoteFailure(
            f"server unreachable ({type(unreachable).__name__}: {unreachable})", True
        )
    if _contains_connection_closed(error):
        return RemoteFailure("server closed the connection before the handshake finished", True)
    leaf = _leaf(error)
    return RemoteFailure(f"{type(leaf).__name__}: {leaf}", False)


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
