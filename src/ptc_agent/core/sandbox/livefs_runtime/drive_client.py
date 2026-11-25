"""The server's Drive mount endpoint, as the daemon calls it.

Rides the livefs ``Remote``: the same token file (re-read when it is
rewritten), pacing, kept-alive connections and the rule that a change is
never sent twice. Each method answers parsed JSON or raises ``OSError`` with
the errno the program should see.
"""

from __future__ import annotations

import errno
import http.client
import io
import json
import time
from collections.abc import Callable, Iterator

from .drive_protocol import (
    ACTION_PREFIX,
    CODES,
    FEED_WAIT_S,
    PROCESS_HEADER,
    PROCESS_PREFIX,
    WireEntry,
    code_of,
    errno_for,
    read_frames,
)
from .drive_ranges import Connections, get_range
from .remote import REQUEST_TIMEOUT_S, THROTTLE_BUDGET_S, Remote, fail

#: A save of the largest file over a slow link, or a ``texts`` page.
BULK_TIMEOUT_S = 180

_JSON = {"Content-Type": "application/json"}

#: The daemon's own pace, all requests together, under the Drive's rate per
#: computer on the server (40 a second, bursts of 400): deletes are one
#: request a file, and ``rm -r`` of a thousand files should not take a minute.
#: The margin under the server's numbers covers the two clocks starting apart.
RATE_PER_S = 38.0
BURST = 380
#: Idle connections kept for ranged reads: one per block fetcher.
RANGE_POOL = 32


def _who(call: str | None) -> tuple[str | None, dict]:
    """A call id for ``X-Livefs-Call``, or a process tag for its own header."""
    if call and call.startswith(PROCESS_PREFIX):
        return None, {PROCESS_HEADER: call[len(PROCESS_PREFIX):]}
    return call, {}


class Refused(OSError):
    """A request the server answered with a refusal."""

    def __init__(self, status: int, body: bytes, retry_after: float | None = None) -> None:
        code = errno_for(status, body)
        super().__init__(code, _message(body) or f"the server answered {status}")
        self.status = status
        self.body = body
        #: The refusal's own code; None for a body no Drive handler wrote
        #: (a proxy's page, a crash, another route's 404).
        code = code_of(body)
        self.code = code if code in CODES else None
        #: When a server too busy for it asked for the request again.
        self.retry_after = retry_after


def _retry_after(resp) -> float | None:
    try:
        return max(float(resp.getheader("Retry-After")), 0.0)
    except (TypeError, ValueError):
        return None


def _message(body: bytes) -> str | None:
    try:
        answer = json.loads(body)
    except ValueError:
        return None
    return answer.get("message") if isinstance(answer, dict) else None


class DriveClient:
    def __init__(self, config_path: str, *, quick_timeout: float = REQUEST_TIMEOUT_S) -> None:
        # One pace for the daemon as a whole, under the Drive's own rate per
        # computer: three clients only so a long request never holds up a
        # short one's timeout.
        self.quick = Remote(config_path, quick_timeout, rate=RATE_PER_S, burst=BURST)
        self.bulk = Remote(config_path, BULK_TIMEOUT_S, pacer=self.quick)
        self.feed = Remote(config_path, FEED_WAIT_S + 20, pacer=self.quick)
        #: Ranges of large files through the server (``content_into``), as
        #: many at once as the block cache fetches, each kept alive.
        self.ranges = Connections(BULK_TIMEOUT_S, RANGE_POOL)

    @staticmethod
    def _answer(status: int, data: bytes, ok: tuple[int, ...] = (200,)) -> dict:
        if status not in ok:
            raise Refused(status, data)
        try:
            answer = json.loads(data)
        except ValueError:
            raise fail(errno.EIO) from None
        if not isinstance(answer, dict):
            raise fail(errno.EIO)  # no answer of the Drive's (a proxy's, say)
        return answer

    def _get(self, action: str, params: dict, *, remote: Remote | None = None) -> dict:
        status, _, data = (remote or self.quick).request("GET", f"{ACTION_PREFIX}/{action}", params)
        return self._answer(status, data)

    def _post(self, action: str, params: dict, call: str | None) -> dict:
        call, headers = _who(call)
        status, _, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/{action}", params, body=b"", call=call, headers=headers
        )
        return self._answer(status, data)

    # --- reads -----------------------------------------------------------

    def spaces(self) -> list[dict]:
        spaces = self._get("spaces", {}).get("spaces")
        if not isinstance(spaces, list):
            raise fail(errno.EIO)
        return spaces

    def manifest(self, space: str) -> dict:
        return self._get("manifest", {"space": space}, remote=self.bulk)

    def texts(self, space: str, ids: list[str]) -> Iterator[tuple[str, str, bytes]]:
        """The text of each id the server holds as text, as (id, sha256, bytes).
        A read, so sent again once when it fails short of an answer."""
        body = json.dumps({"ids": ids}).encode()
        for attempt in (1, 2):
            try:
                status, _, data = self.bulk.request(
                    "POST", f"{ACTION_PREFIX}/texts", {"space": space}, body=body, headers=_JSON
                )
                break
            except OSError:
                if attempt == 2:
                    raise
        if status != 200:
            raise Refused(status, data)
        return read_frames(io.BytesIO(data))

    def all_texts(self, space: str) -> Iterator[tuple[str, str, bytes]]:
        """Every text the space holds, as (id, sha256, bytes), in one
        streamed answer read a frame at a time: a mount starting empty takes
        its text in one request rather than a page per thousand files. Raises
        ``OSError`` when it cannot start; a stream cut short just ends, and
        the caller asks for what is still missing by page."""
        with self.bulk.stream("POST", f"{ACTION_PREFIX}/texts", {"space": space, "all": 1}) as resp:
            if resp.status != 200:
                raise Refused(resp.status, resp.read())
            try:
                yield from read_frames(resp)
            except (OSError, ValueError, http.client.HTTPException):
                return

    def content(self, space: str, sha: str, start: int | None = None, end: int | None = None) -> bytes:
        """The bytes of ``sha`` in the space, or ``start`` to ``end`` (exclusive)."""
        headers = {} if start is None else {"Range": f"bytes={start}-{(end or 0) - 1}"}
        status, _, data = self.bulk.request(
            "GET", f"{ACTION_PREFIX}/content", {"space": space, "sha256": sha}, headers=headers
        )
        if status not in (200, 206):
            raise Refused(status, data)
        if start is not None and status == 200:
            data = data[start:end]
        return data

    def source(self, space: str, sha: str) -> dict:
        """Where the bytes of version ``sha`` are read by range: ``{"via":
        "link", "url", "expires_in", "size"}`` (a signed GET on the store) or
        ``{"via": "api", "size"}`` (``content_into``). ENOENT once the space
        no longer holds that version."""
        return self._get("source", {"space": space, "sha256": sha})

    def content_into(
        self, space: str, sha: str, start: int, end: int, sink: Callable[[memoryview], None], buf: memoryview
    ) -> None:
        """Bytes ``start`` to ``end`` (exclusive) of ``sha``, handed to ``sink``
        as they arrive. A 401 reads the token file again and a 429 waits, as
        every other request does: neither reached a handler."""
        reloaded, throttled = False, 0.0
        while True:
            url, headers = self.quick.prepared(f"{ACTION_PREFIX}/content", {"space": space, "sha256": sha})
            answer = get_range(self.ranges, url, headers, start, end, sink, buf)
            if answer is None:
                return
            if answer.status == 401 and not reloaded:
                reloaded = True
                self.quick.reload()
                continue
            if answer.status == 429 and throttled < THROTTLE_BUDGET_S:
                wait = Remote.throttle_wait(answer.retry_after)
                time.sleep(wait)
                throttled += wait
                continue
            raise Refused(answer.status, answer.body)

    def entry(self, space: str, path: str) -> WireEntry | None:
        try:
            return self._get("entry", {"space": space, "path": path})["entry"]
        except Refused as exc:
            if exc.errno == errno.ENOENT:
                return None
            raise

    def changes(self, space: str, since: int, wait: int = FEED_WAIT_S) -> dict:
        return self._get("changes", {"space": space, "since": since, "wait": wait}, remote=self.feed)

    # --- changes ---------------------------------------------------------

    def write(
        self, space: str, path: str, body: bytes, base: str | None, call: str | None
    ) -> dict:
        """Save ``body`` over the head ``base`` (None: nothing may be there).
        Sent once: a save the server was too busy for is refused with a
        ``retry_after`` before anything is applied, and ``drive_saves``
        sends it again then."""
        call, who = _who(call)
        headers = {"If-Match": f'"{base}"'} if base else {"If-None-Match": "*"}
        headers = {**headers, **who, "Content-Type": "application/octet-stream"}
        status, resp, data = self.bulk.request(
            "PUT", f"{ACTION_PREFIX}/write", {"space": space, "path": path}, body=body, headers=headers, call=call
        )
        if status == 503:
            raise Refused(status, data, _retry_after(resp))
        return self._answer(status, data)

    # --- saves in parts -------------------------------------------------

    def upload_begin(self, space: str, path: str, size: int, sha: str, base: str | None,
                     call: str | None) -> dict:
        """Begin a save in parts over the head ``base`` (None: nothing may be
        there). A refusal here is the save's outcome, filed by the server; a
        busy server's (503) carries ``retry_after`` and filed nothing."""
        call, who = _who(call)
        headers = {"If-Match": f'"{base}"'} if base else {"If-None-Match": "*"}
        headers = {**headers, **who, **_JSON}
        body = json.dumps({"size": size, "sha256": sha}).encode()
        status, resp, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/uploads", {"space": space, "path": path},
            body=body, headers=headers, call=call,
        )
        if status == 503:
            raise Refused(status, data, _retry_after(resp))
        return self._answer(status, data)

    def upload_sign(self, space: str, upload_id: str, parts: list[dict]) -> dict:
        status, resp, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/uploads/{upload_id}/parts", {"space": space},
            body=json.dumps({"parts": parts}).encode(), headers=_JSON,
        )
        if status == 503:
            raise Refused(status, data, _retry_after(resp))
        return self._answer(status, data)

    def upload_part(self, space: str, upload_id: str, number: int, body: bytes, md5: str) -> dict:
        """One part through the server (relay), checked against its MD5."""
        status, resp, data = self.bulk.request(
            "PUT", f"{ACTION_PREFIX}/uploads/{upload_id}/parts/{number}", {"space": space},
            body=body, headers={"Content-MD5": md5, "Content-Type": "application/octet-stream"},
        )
        if status == 503:
            raise Refused(status, data, _retry_after(resp))
        return self._answer(status, data)

    def upload_complete(self, space: str, upload_id: str) -> dict:
        status, resp, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/uploads/{upload_id}/complete", {"space": space}, body=b"",
        )
        if status == 503:
            raise Refused(status, data, _retry_after(resp))
        return self._answer(status, data, ok=(200, 202))

    def upload_status(self, space: str, upload_id: str) -> dict:
        return self._get(f"uploads/{upload_id}", {"space": space})

    def upload_cancel(self, space: str, upload_id: str) -> bool:
        """Whether the upload ended with nothing recorded: aborted now, even
        while the server reads it back, or ended so earlier. False once it
        ended ``done``: that save stands."""
        status, _, data = self.quick.request(
            "DELETE", f"{ACTION_PREFIX}/uploads/{upload_id}", {"space": space},
        )
        if status in (200, 204):
            return True
        refused = Refused(status, data)
        if refused.code == "not_found":
            return True
        if refused.code == "upload_ended":
            try:
                answer = json.loads(data)
            except ValueError:
                answer = None
            if not isinstance(answer, dict):
                return False
            state = answer.get("state") or (answer.get("detail") or {}).get("state")
            return state in ("failed", "aborted")
        raise refused

    def notices(self, notes: list[dict]) -> dict:
        """Hand the agent's notices to the server (``drive_notices``). Each
        carries an id the server files once, so sending one again is safe."""
        body = json.dumps({"notices": notes}).encode()
        status, _, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/notices", {}, body=body, headers=_JSON
        )
        return self._answer(status, data)

    def mkdir(self, space: str, path: str, call: str | None) -> dict:
        return self._post("mkdir", {"space": space, "path": path}, call)

    def move(self, space: str, src: str, dst: str, replace: bool, call: str | None) -> dict:
        return self._post(
            "move", {"space": space, "from": src, "to": dst, "replace": int(replace)}, call
        )

    def trash(self, space: str, path: str, kind: str, call: str | None, if_match: str | None = None) -> dict:
        """``if_match``: only while the file's head is this sha256 (412
        ``changed`` otherwise)."""
        params = {"space": space, "path": path, "kind": kind}
        if kind == "folder":
            params["empty"] = 1
        if if_match is None:
            return self._post("trash", params, call)
        call, headers = _who(call)
        status, _, data = self.quick.request(
            "POST", f"{ACTION_PREFIX}/trash", params, body=b"", call=call,
            headers={**headers, "If-Match": f'"{if_match}"'},
        )
        return self._answer(status, data)
