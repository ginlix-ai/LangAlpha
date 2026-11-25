"""What the Drive daemon has to tell the agent about saves the server never
answered, kept on the computer's disk until it reaches the server.

A save the server refused is filed by the server itself, under the call id
the save carried. One that never got an answer (the connection dropped, a
proxy's error page, a token the server would not take) leaves the server
nothing to file, so the daemon notes it here: the file is not saved yet, and
later that it landed. The daemon sends the list to the server as soon as it
can (``POST notices``), which files each note under its call as the server's
own outcomes are; while the server cannot be reached, the host takes the list
itself after a command (``drive-notices``). Each note has an id, so one that
reaches the server both ways is reported once.

The file is shared by the daemon and that host command, so every change
holds a lock on it; nothing holds the lock across a request.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import secrets
import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .drive_client import DriveClient

#: Notes kept at most; past it the oldest are folded into one ``more``
#: note that counts them, so the agent is told how many it does not see.
KEPT = 500

#: How often the daemon looks for notices nothing woke it for, and the most
#: it waits between tries while the server does not take them.
SEND_EVERY_S = 2.0
SEND_BACKOFF_MAX_S = 30.0
#: The most one ``notices`` request carries, under the server's 1 MiB cap
#: on a JSON body.
BODY_MAX = 512 * 1024

#: Kinds of note.
UNSAVED = "unsaved"
SAVED = "saved"
LOST = "lost"
MORE = "more"


class Notices:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock_path = path + ".lock"

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _read(self) -> list[dict]:
        """Every note; none when there is no queue yet. Any other failure
        raises, so no caller writes back a queue read only in part."""
        out = []
        try:
            f = open(self.path)
        except FileNotFoundError:
            return out
        with f:
            for line in f:
                with contextlib.suppress(ValueError):
                    note = json.loads(line)
                    if isinstance(note, dict) and note.get("id"):
                        out.append(note)
        return out

    def _write(self, notes: list[dict]) -> None:
        if len(notes) > KEPT:
            over, notes = notes[: len(notes) - KEPT + 1], notes[len(notes) - KEPT + 1:]
            count = sum(int(n.get("count") or 1) if n.get("kind") == MORE else 1 for n in over)
            notes.insert(0, {"id": secrets.token_hex(8), "kind": MORE, "call": None, "space": "",
                             "path": "", "count": count, "at": time.time()})
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            for note in notes:
                f.write(json.dumps(note) + "\n")
        os.replace(tmp, self.path)

    def add(self, kind: str, call: str | None, space: str, rel: str, reason: str = "") -> dict:
        note = {
            "id": secrets.token_hex(8),
            "kind": kind,
            "call": call,
            "space": space,
            "path": rel,
            "reason": reason,
            "at": time.time(),
        }
        with self._locked():
            notes = self._read()
            notes.append(note)
            self._write(notes)
        return note

    def peek(self) -> list[dict]:
        with self._locked():
            return self._read()

    def remove(self, ids: set[str]) -> None:
        if not ids:
            return
        with self._locked():
            notes = self._read()
            kept = [n for n in notes if n["id"] not in ids]
            if len(kept) != len(notes):
                self._write(kept)

    def take(self) -> list[dict]:
        """Every note, leaving none: the host collecting them."""
        with self._locked():
            notes = self._read()
            if notes:
                self._write([])
        return notes

    def __len__(self) -> int:
        return len(self.peek())


def _under_body_cap(notes: list[dict]) -> list[dict]:
    """The first notes whose request body stays under the server's cap on
    a JSON body; the rest go in the next request. Never none: one note is
    far smaller than the cap."""
    size, batch = 0, []
    for note in notes:
        size += len(json.dumps(note)) + 2
        if batch and size > BODY_MAX:
            break
        batch.append(note)
    return batch


def deliver(
    notices: Notices, client: DriveClient, wake: threading.Event, stop: threading.Event, counts: dict
) -> None:
    """The daemon's loop handing the notices to the server, which files each
    under its call; the host takes what is left while the server cannot be
    reached (``drive-notices``). ``counts`` gets ``sent`` and ``errors``."""
    pause = 1.0
    wake.set()  # whatever an earlier daemon left
    while not stop.is_set():
        wake.wait(SEND_EVERY_S)
        wake.clear()
        if stop.is_set():
            return
        try:
            waiting = notices.peek()
        except OSError:
            continue
        if not waiting:
            continue
        batch = _under_body_cap(waiting)
        try:
            client.notices(batch)
        except OSError:
            counts["errors"] += 1
            stop.wait(pause)
            pause = min(pause * 2, SEND_BACKOFF_MAX_S)
            wake.set()
            continue
        pause = 1.0
        counts["sent"] += len(batch)
        with contextlib.suppress(OSError):
            notices.remove({n["id"] for n in batch})
        if len(batch) < len(waiting):
            wake.set()
