"""Ranged reads of a large file's bytes, and the PUT of one part of a save,
over kept-alive connections: the one HTTP path for both of a version's
routes (a signed link on the store, or the server's own ``content`` route),
which differ only in the URL and headers they send.

The rules every ranged read keeps, whatever the route: it is sent again
once, on a new connection, when it failed before a byte arrived and did not
time out (the far side closed a kept-alive connection while it was idle);
and a 200 where a range was asked is the whole file, of which only the
range is read and the connection then closed.
"""

from __future__ import annotations

import errno
import http.client
import select
import threading
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass

from .remote import IDLE_S, fail, tls_context

TIMEOUT_S = 60.0
#: A refusal body larger than this is not read: the connection is closed.
REFUSAL_MAX = 1 << 20

_Key = tuple[str, str, int | None]


class Connections:
    """Kept-alive connections per host, at most ``keep`` idle."""

    def __init__(self, timeout: float = TIMEOUT_S, keep: int = 32) -> None:
        self._timeout = timeout
        self._keep = keep
        self._mu = threading.Lock()
        self._idle: dict[_Key, list[tuple[http.client.HTTPConnection, float]]] = {}

    def _new(self, key: _Key) -> http.client.HTTPConnection:
        scheme, host, port = key
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=self._timeout, context=tls_context())
        return http.client.HTTPConnection(host, port, timeout=self._timeout)

    def take(self, key: _Key) -> http.client.HTTPConnection:
        """The connection used last, when it went idle within ``IDLE_S`` and
        the far side has not closed it: one idle longer may have been dropped
        by a NAT on the way without a word, and a request on it waits out the
        whole timeout."""
        now = time.monotonic()
        stale, found = [], None
        with self._mu:
            pool = self._idle.get(key) or []
            while pool and now - pool[0][1] > IDLE_S:
                stale.append(pool.pop(0)[0])
            while pool and found is None:
                conn = pool.pop()[0]
                if _closed(conn):
                    stale.append(conn)
                else:
                    found = conn
        for conn in stale:
            conn.close()
        return found or self._new(key)

    def give_back(self, key: _Key, conn: http.client.HTTPConnection, resp: http.client.HTTPResponse) -> None:
        if resp.will_close or conn.sock is None:
            conn.close()
            return
        with self._mu:
            pool = self._idle.setdefault(key, [])
            if sum(len(p) for p in self._idle.values()) < self._keep:
                pool.append((conn, time.monotonic()))
                return
        conn.close()


def _closed(conn: http.client.HTTPConnection) -> bool:
    """An idle kept-alive socket that reads as ready holds an EOF or a TLS close."""
    sock = conn.sock
    if sock is None:
        return True
    try:
        return bool(select.select([sock], [], [], 0)[0])
    except (OSError, ValueError):
        return True


@dataclass(frozen=True)
class Answer:
    """What a ranged read was answered with when it was not the range: the
    status, and the body of a refusal small enough to read."""

    status: int
    body: bytes = b""
    retry_after: str | None = None


def _is_range(header: str | None, start: int, end: int) -> bool:
    """Whether a 206's ``Content-Range`` names exactly ``start`` to ``end``."""
    unit, _, rest = (header or "").strip().partition(" ")
    span = rest.partition("/")[0]
    return unit.lower() == "bytes" and span == f"{start}-{end - 1}"


def _split(url: str) -> tuple[_Key, str]:
    parts = urllib.parse.urlsplit(url)
    return (parts.scheme, parts.hostname or "", parts.port), parts.path + (f"?{parts.query}" if parts.query else "")


def get_range(conns: Connections, url: str, headers: dict, start: int, end: int,
              sink: Callable[[memoryview], None], buf: memoryview) -> Answer | None:
    """Bytes ``start`` to ``end`` of ``url`` handed to ``sink`` as they
    arrive; None once they all did. Any other answer is returned unread past
    ``REFUSAL_MAX``. A 200 from a later start is the whole file: not read,
    answered as ``Answer(200)``."""
    key, target = _split(url)
    sent = {**headers, "Range": f"bytes={start}-{end - 1}"}
    for attempt in (1, 2):
        conn = conns.take(key)
        got = 0
        try:
            conn.request("GET", target, headers=sent)
            resp = conn.getresponse()
            if resp.status not in (200, 206) or (resp.status == 200 and start):
                if resp.status in (200, 206) or (resp.length or 0) > REFUSAL_MAX:
                    conn.close()
                    return Answer(resp.status, b"", resp.getheader("Retry-After"))
                # No length said (chunked): read no more than the cap either.
                body = resp.read(REFUSAL_MAX + 1)
                if len(body) > REFUSAL_MAX:
                    conn.close()
                    return Answer(resp.status, b"", resp.getheader("Retry-After"))
                conns.give_back(key, conn, resp)
                return Answer(resp.status, body, resp.getheader("Retry-After"))
            if resp.status == 206 and not _is_range(resp.getheader("Content-Range"), start, end):
                # As many bytes from elsewhere in the file are not these.
                conn.close()
                raise fail(errno.EIO)
            want = end - start
            while got < want:
                n = resp.readinto(buf[: min(len(buf), want - got)])
                if not n:
                    raise http.client.IncompleteRead(b"", want - got)
                sink(buf[:n])
                got += n
            if not resp.isclosed():
                conn.close()  # the rest of the answer (a whole file) not read
            else:
                conns.give_back(key, conn, resp)
            return None
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            if got or attempt == 2 or isinstance(exc, TimeoutError):
                raise fail(errno.EIO) from None
        except BaseException:
            # The sink gave up part way: the rest of the answer is unread.
            conn.close()
            raise
    raise fail(errno.EIO)


def put(conns: Connections, url: str, headers: dict, body: bytes) -> int:
    """PUT ``body`` to a signed address: exactly its bytes, the headers the
    server named, no credentials. The status, or 0 when nothing answered. A
    PUT is not sent again here; the caller decides."""
    key, target = _split(url)
    conn = conns.take(key)
    try:
        conn.request("PUT", target, body=body, headers={**headers, "Content-Length": str(len(body))})
        resp = conn.getresponse()
    except (OSError, http.client.HTTPException):
        conn.close()
        return 0
    try:
        resp.read()
    except (OSError, http.client.HTTPException):
        # Answered, then cut short: the status stands; the connection goes.
        conn.close()
        return resp.status
    conns.give_back(key, conn, resp)
    return resp.status
