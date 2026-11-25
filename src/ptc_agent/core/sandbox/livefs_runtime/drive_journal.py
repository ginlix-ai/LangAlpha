"""Files being written, kept on disk until they are saved.

Saves go out after a file goes quiet, not at its close (``drive_saves``), so
between a program's last write and the save landing the computer holds the
only copy. That copy lives here (root's private directory, never a workspace
folder) rather than in an unlinked temp file, so a daemon that dies or is
replaced leaves it for the next one, which takes it up and saves it. A
record is two files under one id: ``<id>.data``, the bytes the operations
write into directly, and ``<id>.json``, where they go and what they were
edited from, rewritten as that changes.

Each record has one owner at a time: the daemon writing it holds an
exclusive ``flock`` on its data for as long as the file is being written.
A replaced daemon keeps serving the descriptors still open on its old mount
and keeps saving them, so a new daemon takes up only records no live daemon
holds (``claim``), and looks again later for the rest. A data file is made
under a temporary name, locked, and only then given its name; and a record
is removed while still locked. So a record is never seen unlocked while its
owner lives.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import secrets
import time
from dataclasses import dataclass


@dataclass
class Record:
    id: str
    space: str
    rel: str
    base: str | None
    unanswered: list[str]
    call: str | None


class Journal:
    def __init__(self, root: str) -> None:
        self.root = root
        os.makedirs(root, mode=0o700, exist_ok=True)

    @staticmethod
    def new_id() -> str:
        return secrets.token_hex(8)

    def _path(self, record_id: str, suffix: str) -> str:
        return os.path.join(self.root, record_id + suffix)

    def data_path(self, record_id: str) -> str:
        return self._path(record_id, ".data")

    def open(self, record_id: str) -> int:
        """A new, empty data file for ``record_id``, open for reading and
        writing and locked by this daemon until the descriptor closes."""
        new = self._path(record_id, ".data.new")
        fd = os.open(new, os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.rename(new, self._path(record_id, ".data"))
        except BaseException:
            os.close(fd)
            with contextlib.suppress(OSError):
                os.unlink(new)
            raise
        return fd

    def describe(self, record_id: str, space: str, rel: str, base: str | None,
                 unanswered: list[str], call: str | None) -> None:
        """Where ``record_id``'s bytes go and what they were edited from."""
        meta = self._path(record_id, ".json")
        with open(meta + ".tmp", "w") as out:
            json.dump({"space": space, "rel": rel, "base": base,
                       "unanswered": unanswered, "call": call}, out)
        os.replace(meta + ".tmp", meta)

    def drop(self, record_id: str) -> None:
        """Remove a record; its owner calls this before closing the data."""
        for suffix in (".json", ".data", ".json.tmp"):
            with contextlib.suppress(OSError):
                os.unlink(self._path(record_id, suffix))

    def claim(self, skip: set[str] | frozenset = frozenset()) -> list[tuple[Record, int]]:
        """Every record no live daemon holds, with its data open and locked
        for the caller. Records in ``skip`` (the caller's own) are left
        alone, as is any another daemon still holds. A record cut short
        before it was described, and nobody's, is removed."""
        out: list[tuple[Record, int]] = []
        try:
            names = sorted(os.listdir(self.root))
        except OSError:
            return out
        for name in names:
            if name.endswith(".data.new"):
                self._sweep_new(name)
                continue
            if not name.endswith(".data"):
                continue
            record_id = name[: -len(".data")]
            if record_id in skip:
                continue
            claimed = self._claim(record_id)
            if claimed is not None:
                out.append(claimed)
        return out

    def _claim(self, record_id: str) -> tuple[Record, int] | None:
        path = self._path(record_id, ".data")
        try:
            fd = os.open(path, os.O_RDWR | os.O_CLOEXEC)
        except OSError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Removed by its owner between the open and the lock.
            if os.stat(path).st_ino != os.fstat(fd).st_ino:
                os.close(fd)
                return None
        except OSError:
            os.close(fd)
            return None
        try:
            with open(self._path(record_id, ".json")) as f:
                meta = json.load(f)
            record = Record(record_id, meta["space"], meta["rel"], meta.get("base"),
                            list(meta.get("unanswered") or ()), meta.get("call"))
        except (FileNotFoundError, ValueError, KeyError, TypeError):
            # Never described, or not a description: nothing to save it by.
            self.drop(record_id)
            os.close(fd)
            return None
        except OSError:
            # Unreadable for now: its data may be the only copy of an edit.
            os.close(fd)
            return None
        return record, fd

    def _sweep_new(self, name: str) -> None:
        """A data file a daemon died making before it named it (old enough
        that no daemon is between its create and its lock)."""
        path = os.path.join(self.root, name)
        try:
            if time.time() - os.stat(path).st_mtime < 60:
                return
        except OSError:
            return
        try:
            fd = os.open(path, os.O_RDWR | os.O_CLOEXEC)
        except OSError:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.unlink(path)
        except OSError:
            pass
        finally:
            os.close(fd)
