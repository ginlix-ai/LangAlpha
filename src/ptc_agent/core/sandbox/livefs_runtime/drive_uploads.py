"""Saving a file larger than one request carries (``limits.file_bytes``): in
parts, from its journal record on disk, a part at a time in memory.

The server's uploads routes take the file's size and sha256 up front, then
its parts, numbered from 1, each ``part_size`` bytes but the last: signed
straight to the store (``direct``) or through the server (``relay``). A
begin with the same path, size and sha256 as an open upload resumes it with
the parts the store already holds, so a save cut off part way, or sent again
after a daemon restart, sends only the rest. The server files the outcome
for the command when the upload ends; the upload is read until then only so
this daemon learns it too.

Such a save takes minutes, so the file's ``saving`` is held only across its
single requests (the begin, a one-request write, the complete), as a small
save holds it across its one request, and a rename or removal of the file
waits for at most one of those. It abandons the save (``Upload``): parts
stop before the next one goes and the upload is cancelled; one the server is
already reading back cannot be, and ``Saves`` takes back what it lands.
Bytes written meanwhile would make the stored file differ from the sha256
the begin named, so the save is given up (``Changed``) and sent again once
the file goes quiet.
"""

from __future__ import annotations

import base64
import concurrent.futures
import errno
import hashlib
import json
import os
import random
import threading
import time
from collections import Counter
from collections.abc import Callable
from typing import TypeVar

from .drive_client import DriveClient, Refused
from .drive_disk import pread_exact, sha256_fd
from .drive_ranges import Connections, put
from .drive_writes import Pending
from .remote import fail

#: Parts under way at once for one save; each is one part in memory.
PART_THREADS = 4
#: Tries per part before the save is given up for now (sent again later,
#: when only the parts still missing go).
PART_TRIES = 5
#: How often the upload's end is asked for while the server reads it back.
POLL_S = 1.5
#: Signed part addresses asked for at once (the server's most).
SIGN_BATCH = 32
#: Completes in a row that find parts missing before the save is given up.
COMPLETE_TRIES = 3
#: While the upload's status cannot be read: how often it is asked again,
#: and for how long before the save is given up for now.
POLL_FAILING_S = 10.0
POLL_GIVE_UP_S = 900.0
PUT_TIMEOUT_S = 120.0

T = TypeVar("T")


class Changed(Exception):
    """The file was written while its save was under way: saved later."""


class Abandoned(Exception):
    """The file was renamed or removed while its save was under way."""


class Restart(Exception):
    """The upload ended under the save (it ran out of time, or was ended
    elsewhere) with nothing recorded: the save begins again."""


class Stuck(OSError):
    """The save cannot go on now, for ``reason``, and nothing it sent was
    refused by the Drive: the bytes are kept and sent again later."""

    def __init__(self, reason: str) -> None:
        super().__init__(errno.EIO, reason)
        self.reason = reason


class Failed(Refused):
    """The upload ended ``failed``: the server refused the file and filed
    that for the command."""


class Upload:
    """One save in parts under way, as its file knows it (``Pending.upload``):
    what a rename or removal sets to stop it, and the upload's id once begun."""

    __slots__ = ("abandoned", "id")

    def __init__(self) -> None:
        self.abandoned = threading.Event()
        self.id: str | None = None


def current(p: Pending, rev: int, upload: Upload) -> None:
    """The one test that a save still stands, under the file's lock."""
    if upload.abandoned.is_set():
        raise Abandoned
    if not p.open or p.rev != rev:
        raise Changed


def _midway(exc: Refused) -> BaseException:
    """What a refusal after the begin means for the save. Only the begin and
    the upload's end are the save's outcome, which the server files; a part,
    a signature, a complete or a status refused on the way is not, so the
    bytes are never dropped for one: an upload that ended under the save
    begins again, and anything else is kept and sent again later."""
    if exc.code in ("upload_ended", "not_found"):
        return Restart()
    what = exc.code or f"an answer {exc.status} from no Drive handler"
    return Stuck(f"the Drive's server refused a step of the save in parts ({what})")


class Uploads:
    def __init__(self, client: DriveClient, counts: Counter[str]) -> None:
        self.client = client
        self.counts = counts
        self._pool = concurrent.futures.ThreadPoolExecutor(PART_THREADS, thread_name_prefix="drive-part")
        #: Connections to the store for parts sent straight there.
        self._store = Connections(PUT_TIMEOUT_S, PART_THREADS)

    def _bytes(self, p: Pending, rev: int, upload: Upload, size: int, offset: int) -> bytes:
        with p.lock:
            current(p, rev, upload)
            return pread_exact(p.fd, size, offset)

    def digest(self, p: Pending, rev: int, upload: Upload, size: int) -> str:
        """The file's sha256 at ``rev``; given up as soon as it changes."""
        def still() -> None:
            with p.lock:
                current(p, rev, upload)
        # A descriptor of its own: a let-go closes ``p.fd`` between chunks,
        # and its number may then name another file.
        with p.lock:
            current(p, rev, upload)
            fd = os.dup(p.fd)
        try:
            return sha256_fd(fd, size, still)
        finally:
            os.close(fd)

    def send_large(self, p: Pending, rev: int, upload: Upload, size: int, sha: str, call: str | None,
                   request: Callable[[Callable[[], T]], T]) -> dict:
        """Save ``p`` as it was at ``rev``. ``request`` runs one request under
        the file's ``saving`` once ``current`` still holds. Answers the
        server's ``done`` status (``entry`` absent when the file was removed
        while the server read it back) or a ``held`` begin; raises
        ``Refused`` from the begin and ``Failed`` (the server refused it and
        filed that), ``Restart``, ``Changed`` or ``Abandoned`` (the upload
        ended where it still can be), or ``OSError`` (sent again later)."""
        space = p.space.ref
        begun = request(lambda: self.client.upload_begin(space, p.rel, size, sha, p.base, call))
        mode = begun["mode"]
        if mode == "held":
            # The store holds these bytes already: the begin saved the file.
            return begun
        if mode == "single":
            body = self._bytes(p, rev, upload, size, 0)
            return request(lambda: self.client.write(space, p.rel, body, p.base, call))
        upload_id = upload.id = begun["upload_id"]
        held = {int(h["part_number"]) for h in begun.get("parts_held") or ()}
        count = begun["part_count"]
        todo = [n for n in range(1, count + 1) if n not in held]
        self.counts["upload_parts_held"] += len(held)
        parts = _Parts(self, p, rev, upload, begun)
        # A begin that found this very save being read back already (sent
        # before with no answer) joins it: nothing to send.
        status: dict | None = begun if begun.get("state") == "verifying" else None
        if status is not None:
            self.counts["upload_joined"] += 1
        try:
            for _ in range(COMPLETE_TRIES):
                if status is None:
                    parts.send(todo)
                    try:
                        status = request(lambda: self.client.upload_complete(space, upload_id))
                    except Refused as exc:
                        if exc.code != "parts_missing":
                            raise
                        todo = [int(n) for n in json.loads(exc.body or b"{}").get("missing_parts") or ()]
                        continue
                    self.counts["upload_completed"] += 1
                status = self._ended(space, upload, status)
                if status.get("state") != "uploading":
                    return status
                # Open again (a part the store lost): send what it lacks.
                held = {int(h["part_number"]) for h in status.get("parts_held") or ()}
                todo = [n for n in range(1, count + 1) if n not in held]
                status = None
                self.counts["upload_reopened"] += 1
            raise Stuck("the Drive's server kept finding parts of it missing")
        except (Changed, Abandoned):
            try:
                self.client.upload_cancel(space, upload_id)
            except OSError:
                pass  # the server ends an upload nothing touches
            raise
        except Failed:
            raise
        except Refused as exc:
            raise _midway(exc) from exc

    def _ended(self, space: str, upload: Upload, status: dict) -> dict:
        """The upload's end, read until the server has it, or ``uploading``
        when it is open again. A status read that fails is read again, for
        minutes: while this save waits on it, a rename or removal of the file
        can still end the upload (``Abandoned``), which a save given up
        could not."""
        failing_since: float | None = None
        ended_asked = False
        while status.get("state") == "verifying":
            if upload.abandoned.is_set() and not ended_asked:
                try:
                    if self.client.upload_cancel(space, upload.id):
                        raise Abandoned
                    # Ended ``done`` already: ``Saves`` takes back what it made.
                    ended_asked = True
                except OSError:
                    pass  # not answered: asked again next time round
            wait = POLL_S
            if failing_since is not None:
                if time.monotonic() - failing_since > POLL_GIVE_UP_S:
                    raise Stuck("the Drive's server did not say how its read-back of the file ended")
                wait = POLL_FAILING_S
            if upload.abandoned.is_set():
                time.sleep(wait)
            else:
                upload.abandoned.wait(wait)  # a removal ends it at once
            try:
                status = self.client.upload_status(space, upload.id)
            except Refused as exc:
                if exc.code is not None:
                    raise
                failing_since = failing_since or time.monotonic()
                continue
            except OSError:
                failing_since = failing_since or time.monotonic()
                continue
            failing_since = None
        state = status.get("state")
        if state in ("done", "uploading"):
            self.counts[f"upload_{state}"] += 1
            return status
        if state == "failed":
            error = status.get("error") or {}
            self.counts["upload_failed"] += 1
            code = error.get("code") or error.get("type") or "unavailable"
            raise Failed(409, json.dumps({"code": code, "message": error.get("message") or "not saved"}).encode())
        if state == "aborted":
            raise Restart
        raise Stuck(f"the Drive's server answered an upload state it does not document ({state})")


class _Parts:
    """The parts of one save, ``PART_THREADS`` under way at once: straight to
    the store on signed addresses where the begin offered that (``direct``),
    through the server otherwise or once the store will not take a part
    (``relay``)."""

    def __init__(self, uploads: Uploads, p: Pending, rev: int, upload: Upload, begun: dict) -> None:
        self.uploads, self.client, self.counts = uploads, uploads.client, uploads.counts
        self.p, self.rev, self.upload = p, rev, upload
        self.id = begun["upload_id"]
        self.size, self.part_size = begun["size"], begun["part_size"]
        self.direct = begun["mode"] == "direct"
        #: Signed ahead in batches, by length only (the server checks the
        #: whole file's sha256 at the end); a part signs again when its PUT
        #: fails.
        self.signed: dict[int, dict] = {}

    def send(self, todo: list[int]) -> None:
        for at in range(0, len(todo), SIGN_BATCH):
            batch = todo[at : at + SIGN_BATCH]
            if self.direct:
                try:
                    answer = self.client.upload_sign(self.p.space.ref, self.id, [{"part_number": n} for n in batch])
                    self.signed.update((int(link["part_number"]), link) for link in answer["parts"])
                except Refused as exc:
                    # Signing down for now: each part signs on its own, then
                    # goes through the server (``_one``), as in ``_direct``.
                    if exc.status != 503 and exc.code is not None:
                        raise
            futures = [self.uploads._pool.submit(self._one, n) for n in batch]
            error: BaseException | None = None
            for f in concurrent.futures.as_completed(futures):
                if not f.cancelled() and f.exception() is not None and error is None:
                    error = f.exception()
                    for other in futures:
                        other.cancel()
            if error is not None:
                raise error

    def _one(self, n: int) -> None:
        offset = (n - 1) * self.part_size
        body = self.uploads._bytes(self.p, self.rev, self.upload, min(self.part_size, self.size - offset), offset)
        direct = self.direct
        for attempt in range(PART_TRIES):
            if attempt:
                time.sleep(min(2.0 ** attempt, 20.0) * random.uniform(0.8, 1.2))
                with self.p.lock:
                    current(self.p, self.rev, self.upload)
            if direct:
                if self._direct(n, body, sign=attempt > 0):
                    return
                if attempt >= 1:
                    direct = False  # the store will not take it from here
            elif self._relay(n, body):
                return
        raise fail(errno.EIO)

    def _direct(self, n: int, body: bytes, *, sign: bool) -> bool:
        link = self.signed.get(n)
        if link is None or sign:
            try:
                link = self.client.upload_sign(self.p.space.ref, self.id, [{"part_number": n}])["parts"][0]
            except Refused as exc:
                if exc.status != 503 and exc.code is not None:
                    raise
                return False
            self.signed[n] = link
        status = put(self.uploads._store, link["url"], link.get("headers") or {}, body)
        if 200 <= status < 300:
            self.counts["upload_direct_bytes"] += len(body)
            return True
        self.counts[f"upload_put_{status}"] += 1
        return False

    def _relay(self, n: int, body: bytes) -> bool:
        md5 = base64.b64encode(hashlib.md5(body, usedforsecurity=False).digest()).decode()
        try:
            self.client.upload_part(self.p.space.ref, self.id, n, body, md5)
        except Refused as exc:
            if exc.status == 503 or exc.code in (None, "bad_digest"):
                return False
            raise
        except OSError:
            return False
        self.counts["upload_relay_bytes"] += len(body)
        return True
