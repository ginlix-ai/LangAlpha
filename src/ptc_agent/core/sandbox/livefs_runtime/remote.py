"""The server's livefs endpoint, as the daemon calls it."""

from __future__ import annotations

import errno
import json
import os
import random
import select
import threading
import time
import urllib.parse
from typing import TYPE_CHECKING, Any

from .protocol import CALL_HEADER, PREFIX, Refusal, code_of

if TYPE_CHECKING:
    import http.client

REQUEST_TIMEOUT_S = 20

#: Sent on every request, as the sandbox's market-data client does: http.client
#: sends no User-Agent, and edge bot mitigation can challenge a request without
#: one, which the daemon would only see as the mount going away.
USER_AGENT = "langalpha-sandbox/1.0"

#: Kept-alive connections at most, one per libfuse worker thread (libfuse 3
#: runs 10 by default); a burst past it closes the extra ones after use.
POOL_SIZE = 10

#: A connection idle longer is closed rather than reused: the far side has
#: likely dropped it, and the request on it would fail and be sent again.
IDLE_S = 60.0

#: A save or removal is sent at most once, so one sent on a connection the far
#: side is just closing is lost. It takes a kept-alive connection only when
#: that answered moments ago, as the command's own lookups before it did.
WRITE_REUSE_S = 2.0

#: The pace the daemon keeps, under the server's limit per computer: a walk
#: over many files waits its turn rather than meeting a refusal.
RATE_PER_S = 20.0
BURST = 100

#: How long a request the server throttled keeps being retried before the
#: program sees EAGAIN. Each wait honours Retry-After, up to a second.
THROTTLE_BUDGET_S = 3.0

#: The errno each refusal reaches the program as.
ERRNO = {
    Refusal.NOT_FOUND: errno.ENOENT,
    Refusal.IS_DIRECTORY: errno.EISDIR,
    Refusal.NOT_DIRECTORY: errno.ENOTDIR,
    Refusal.EXISTS: errno.EEXIST,
    Refusal.CHANGED: errno.ESTALE,
    Refusal.READ_ONLY: errno.EACCES,
    Refusal.INVALID: errno.EINVAL,
    Refusal.PRECONDITION_REQUIRED: errno.EINVAL,
    Refusal.TOO_LARGE: errno.EFBIG,
    Refusal.UNAVAILABLE: errno.EIO,
}


def fail(code: int) -> OSError:
    return OSError(code, os.strerror(code))


def error_for(status: int, data: bytes) -> OSError:
    if status == 401:
        return fail(errno.EACCES)
    if status == 429:
        return fail(errno.EAGAIN)
    return fail(ERRNO.get(code_of(data), errno.EIO))


class _Conn:
    __slots__ = ("http", "base", "prefix", "idle_since")

    def __init__(self, conn: http.client.HTTPConnection, base: str, prefix: str) -> None:
        self.http = conn
        self.base = base
        self.prefix = prefix
        self.idle_since = time.monotonic()

    def dropped(self) -> bool:
        """Whether the far side closed it while idle: an idle kept-alive
        socket that reads as ready holds an EOF, or a TLS close."""
        sock = self.http.sock
        if sock is None:
            return False
        try:
            return bool(select.select([sock], [], [], 0)[0])
        except (OSError, ValueError):
            return True


class _Failed(Exception):
    """A request that got no whole answer."""

    def __init__(self, *, timeout: bool) -> None:
        super().__init__()
        self.timeout = timeout


class Remote:
    """The server endpoint, over a pool of kept-alive connections.

    A pool rather than one connection per thread: libfuse calls in on its own
    C threads, and Python gives each such callback a fresh thread state, so a
    thread-local is empty again on the next call.

    The config file is re-read when it changes, which is how a rewritten
    token takes effect without a restart.
    """

    def __init__(
        self,
        config_path: str,
        timeout: float = REQUEST_TIMEOUT_S,
        *,
        rate: float = RATE_PER_S,
        burst: int = BURST,
    ) -> None:
        self._config_path = config_path
        self._timeout = timeout
        self._config: dict = {}
        self._mtime: int | None = None
        self._lock = threading.Lock()
        self._idle: list[_Conn] = []
        self._tls: Any = None
        self._rate = rate
        self._burst = float(burst)
        self._tokens = float(burst)
        self._stamp = time.monotonic()

    def _load(self) -> dict:
        mtime = os.stat(self._config_path).st_mtime_ns
        retired: list[_Conn] = []
        with self._lock:
            if mtime != self._mtime:
                with open(self._config_path) as f:
                    config = json.load(f)
                if config.get("base_url") != self._config.get("base_url"):
                    retired, self._idle = self._idle, []
                self._config = config
                self._mtime = mtime
            config = self._config
        for conn in retired:
            conn.http.close()
        return config

    # --- connections ----------------------------------------------------

    def _open(self, base: str) -> _Conn:
        import http.client

        parts = urllib.parse.urlsplit(base)
        if parts.scheme == "https":
            with self._lock:
                if self._tls is None:
                    import ssl

                    # Loading the CA store costs about as much CPU as the
                    # handshake itself, so every connection shares one.
                    self._tls = ssl.create_default_context()
                tls = self._tls
            conn = http.client.HTTPSConnection(
                parts.hostname, parts.port, timeout=self._timeout, context=tls
            )
        else:
            conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=self._timeout)
        return _Conn(conn, base, parts.path.rstrip("/"))

    def _lease(self, base: str, within: float = IDLE_S) -> _Conn:
        """The connection used last, when it is still good and answered no
        longer than ``within`` ago (LIFO keeps the ones in use warm and lets
        the rest age out)."""
        now = time.monotonic()
        closing: list[_Conn] = []
        found = None
        with self._lock:
            while self._idle and now - self._idle[0].idle_since > IDLE_S:
                closing.append(self._idle.pop(0))
            while self._idle and now - self._idle[-1].idle_since <= within:
                conn = self._idle.pop()
                if conn.base == base and not conn.dropped():
                    found = conn
                    break
                closing.append(conn)
        for conn in closing:
            conn.http.close()
        return found or self._open(base)

    def _give_back(self, conn: _Conn) -> None:
        conn.idle_since = time.monotonic()
        with self._lock:
            # A response that closed its connection left it without a socket.
            if (
                conn.http.sock is not None
                and conn.base == self._config.get("base_url")
                and len(self._idle) < POOL_SIZE
            ):
                self._idle.append(conn)
                return
        conn.http.close()

    # --- pacing ---------------------------------------------------------

    def _pace(self) -> None:
        """Take a token, waiting for one to come due. Each caller reserves
        the next, so a burst is spread in arrival order."""
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._burst, self._tokens + (now - self._stamp) * self._rate)
            self._stamp = now
            self._tokens -= 1
            wait = -self._tokens / self._rate if self._tokens < 0 else 0.0
        if wait > 0:
            time.sleep(wait)

    @staticmethod
    def _throttled_for(resp: http.client.HTTPResponse) -> float:
        try:
            after = float(resp.getheader("Retry-After", "1"))
        except (TypeError, ValueError):
            after = 1.0
        return min(max(after, 0.0), 1.0) + random.uniform(0.0, 0.2)

    # --- requests -------------------------------------------------------

    @staticmethod
    def _exchange(
        conn: _Conn, method: str, url: str, body: bytes | None, headers: dict
    ) -> tuple[http.client.HTTPResponse, bytes]:
        import http.client

        try:
            conn.http.request(method, url, body=body, headers=headers)
            resp = conn.http.getresponse()
            # Read whole, so the connection is free for the next request.
            return resp, resp.read()
        except TimeoutError:
            raise _Failed(timeout=True) from None
        except (OSError, http.client.HTTPException):
            raise _Failed(timeout=False) from None

    def request(
        self,
        method: str,
        action: str,
        params: dict,
        *,
        body: bytes | None = None,
        headers: dict | None = None,
        call: str | None = None,
        attempts: int = 2,
    ) -> tuple[int, http.client.HTTPResponse, bytes]:
        """Send one request, sending it again only where that cannot apply it twice.

        A read that failed short of a timeout is sent once more, on a new
        connection. A save or removal never is, whatever failed: once its
        bytes left, the server may have applied it with the answer lost, and
        a copy would meet its own precondition as a conflict, or remove a
        file made since. A 401 or 429 comes before any handler runs, so every
        method is sent again after one.
        """
        tries = 0
        throttled = 0.0
        fresh = False
        while True:
            try:
                config = self._load()
            except (OSError, ValueError):
                raise fail(errno.EIO) from None
            base = config["base_url"]
            # Before the lease, so a wait here does not age the connection.
            self._pace()
            if fresh:
                # The pooled ones may have been closed by the same restart
                # of the far side.
                conn = self._open(base)
            else:
                conn = self._lease(base, IDLE_S if method == "GET" else WRITE_REUSE_S)
            sent = {
                "Authorization": f"Bearer {config['token']}",
                "User-Agent": USER_AGENT,
                **(headers or {}),
            }
            if call:
                sent[CALL_HEADER] = call
            url = f"{conn.prefix}{PREFIX}/{action}?{urllib.parse.urlencode(params)}"
            try:
                resp, data = self._exchange(conn, method, url, body, sent)
            except _Failed as failed:
                conn.http.close()
                tries += 1
                if method != "GET" or failed.timeout or tries >= attempts:
                    raise fail(errno.EIO) from None
                fresh = True
                continue
            self._give_back(conn)
            if resp.status == 401 and tries < attempts - 1:
                tries += 1
                with self._lock:
                    self._mtime = None
                continue
            # Refused before any handler runs, so a retried save cannot land twice.
            if resp.status == 429 and throttled < THROTTLE_BUDGET_S:
                wait = self._throttled_for(resp)
                time.sleep(wait)
                throttled += wait
                continue
            return resp.status, resp, data
