"""Where one version of a large file is read by range: the route the server
names for it (``source``), a signed link on the store read directly, or the
mount endpoint's own ``content`` route. Both go through ``drive_ranges``;
this module only picks the URL and headers, and asks again for a link that
nears its end or that the store refuses.

Versions are named by sha256, never by path, so a read never gets another
version's bytes: once the server no longer holds a version its reads fail
with ESTALE (``Gone``).
"""

from __future__ import annotations

import errno
import threading
import time
from collections import Counter
from collections.abc import Callable

from .drive_client import DriveClient, Refused
from .drive_ranges import Connections, get_range
from .drive_tree import Space
from .remote import fail

#: A fetch takes contiguous missing blocks up to this many bytes in one
#: request from the store: the block is what the cache holds and evicts,
#: the request what pays a round trip.
REQUEST_MAX = 8 << 20
#: The same through the server's own route. Each request is one stream from
#: the store through the server, a few MB/s, so throughput comes from how
#: many run at once: 4 MiB read fastest, at about a third of the server's
#: rate per computer.
API_REQUEST_MAX = 4 << 20
#: A link is asked for again this long before it runs out.
LINK_MARGIN_S = 60.0
#: How long the server's answer that a version is read through its own route
#: is kept before it is asked again (the store may hold it by then).
API_KEEP_S = 900.0
#: Connections to the store kept alive: one per fetcher.
LINK_CONNECTIONS = 32
ASK_WAIT_S = 60.0


class Gone(OSError):
    """The server no longer holds this version: its reads fail, they never
    turn into another version's bytes."""

    def __init__(self) -> None:
        super().__init__(errno.ESTALE, "this version of the file is no longer held")


class Source:
    def __init__(self, client: DriveClient, counts: Counter[str]) -> None:
        self.client = client
        self.counts = counts
        self._links = Connections(keep=LINK_CONNECTIONS)
        self._mu = threading.Lock()
        #: (space ref, sha256) to (answer, until when it is used).
        self._answers: dict[tuple[str, str], tuple[dict, float]] = {}
        self._asking: dict[tuple[str, str], threading.Event] = {}

    def where(self, space: Space, sha: str, *, fresh: bool = False) -> dict:
        """The server's ``source`` answer for ``sha``, kept until a link
        nears its end; one request for it however many readers ask at once.
        Raises ``Gone`` when the space no longer holds the version."""
        key = (space.ref, sha)
        with self._mu:
            # The answer a fresh one must replace: one another reader's
            # refresh failed to replace is still the refused link.
            refused = (self._answers.get(key) or (None,))[0] if fresh else None
        while True:
            now = time.monotonic()
            with self._mu:
                held = self._answers.get(key)
                if held is not None and held[1] > now and (refused is None or held[0] is not refused):
                    return held[0]
                asking = self._asking.get(key)
                if asking is None:
                    asking = self._asking[key] = threading.Event()
                    break
            asking.wait(ASK_WAIT_S)
        try:
            try:
                answer = self.client.source(space.ref, sha)
            except Refused as exc:
                if exc.errno == errno.ENOENT:
                    with self._mu:
                        self._answers.pop(key, None)
                    raise Gone() from None
                raise
            self.counts["source"] += 1
            if answer.get("via") == "link" and answer.get("url"):
                until = now + max(float(answer.get("expires_in") or 0) - LINK_MARGIN_S, 1.0)
            else:
                answer = {**answer, "via": "api"}
                until = now + API_KEEP_S
            with self._mu:
                self._answers[key] = (answer, until)
                if len(self._answers) > 4096:
                    self._answers.pop(next(iter(self._answers)))
            return answer
        finally:
            with self._mu:
                self._asking.pop(key, None)
            asking.set()

    def request_max(self, space: Space, sha: str) -> int:
        return API_REQUEST_MAX if self.where(space, sha)["via"] == "api" else REQUEST_MAX

    def fetch(self, space: Space, sha: str, start: int, end: int,
              sink: Callable[[int, memoryview], None], buf: memoryview) -> None:
        """Bytes ``start`` to ``end`` handed to ``sink(offset, chunk)``. A
        link the store refuses (run out, 403 by the contract and 400 from
        some stores, a passing 5xx, or 404 once it no longer has the bytes)
        is asked for again and the range read again from its start; the
        server then names it anew or answers that the version is gone."""
        for attempt in range(3):
            answer = self.where(space, sha, fresh=attempt > 0)
            at = start

            def take(chunk: memoryview) -> None:
                nonlocal at
                sink(at, chunk)
                at += len(chunk)

            if answer["via"] == "link":
                refused = get_range(self._links, answer["url"], {}, start, end, take, buf)
                if refused is None:
                    self.counts["link_bytes"] += end - start
                    return
                if refused.status < 300:
                    raise fail(errno.EIO)  # the range ignored: not the bytes asked for
                self.counts[f"link_{refused.status}"] += 1
                continue
            try:
                self.client.content_into(space.ref, sha, start, end, take, buf)
            except Refused as exc:
                if exc.errno == errno.ENOENT:
                    raise Gone() from None
                raise
            self.counts["api_bytes"] += end - start
            return
        raise fail(errno.EIO)
