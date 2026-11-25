#!/usr/bin/env python3
"""A stand-in for the server's Drive mount endpoint, for the mount harness.

Speaks the "Mount protocol" of the Drive contract under
``/api/v1/livefs/drive/``, holding one personal space in memory (seeded from
a directory) with its bytes on disk. ``/admin/...`` makes changes as someone
elsewhere would (the app, another computer) and reports what the daemon
asked for and what each call's saves came to, the way the real server files
them for the tool result.

Stdlib only; runs inside the harness container.

    python3 fake_server.py --port 8790 --token T --data DIR [--seed CORPUS]
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import socket
import sys
import threading
import time
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

# The server's own path rules and error codes (stdlib only, so the harness
# imports them): from this checkout, or where the container mounts its source.
for _repo in (os.environ.get("DRIVE_REPO"), os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."),
              "/opt/drive/repo"):
    if _repo and os.path.isfile(os.path.join(_repo, "src", "server", "database", "drive", "keys.py")):
        sys.path.insert(0, os.path.abspath(_repo))
        break
from src.server.database.drive import keys  # noqa: E402
from src.server.database.drive.records import DriveError as ServerError  # noqa: E402

PREFIX = "/api/v1/livefs/drive/"
FIELDS = ("id", "path", "kind", "size", "sha256", "is_text", "modified_ms", "seq")
TEXT_MAX = 1024 * 1024
TEXTS_MAX_BYTES = 16 * 1024 * 1024
#: The HTTP status the endpoint answers each refusal with.
STATUS = {
    "not_found": 404, "exists": 409, "is_folder": 409, "not_folder": 409, "not_empty": 409,
    "changed": 412, "too_large": 413, "invalid_range": 416, "invalid_path": 422, "over_quota": 507,
    "too_many_entries": 507, "unavailable": 503, "forbidden": 403, "parts_missing": 409,
    "upload_ended": 409, "bad_part": 422, "mismatch": 409, "too_many_uploads": 409,
}

key = keys.path_key


class DriveError(ServerError):
    """The server's refusal, with the path it names."""

    def __init__(self, code: str, message: str, path: str = "") -> None:
        super().__init__(code, message)  # type: ignore[arg-type]
        self.path = path


def norm(path: str) -> str:
    try:
        return keys.normalize(path)
    except ServerError as exc:
        raise DriveError(exc.code, exc.message, path) from None


def is_text(data: bytes) -> bool:
    if len(data) > TEXT_MAX or b"\x00" in data:
        return False
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


#: What ``/admin/limits`` may set on a ``Drive``, with how it reads each value.
KNOBS = {
    "quota": int, "file_cap": int, "busy_writes": int, "drop_writes": int, "drop_after": int,
    "status_writes": list, "stall_writes": float, "stall_content": float, "answer_delay": float,
    "feed_hold": bool, "notices_down": bool, "blob_base": str, "link_life": int, "link_told": int,
    "part_size": int, "verify_s": float, "upload_cap": int, "abort_verifying": bool, "status_fails": int,
    "reopen_part": int, "fail_ends": list, "open_cap": int,
}


class Upload:
    """A save in parts: its parts land in one file at their offsets."""

    def __init__(self, drive: Drive, path: str, size: int, sha: str | None, if_match: str | None,
                 if_none_match: bool, call: str | None) -> None:
        self.id = str(uuid.uuid4())
        self.path, self.size, self.sha = path, size, sha
        self.if_match, self.if_none_match, self.call = if_match, if_none_match, call
        self.part_size = drive.part_size
        self.count = max(-(-size // self.part_size), 1)
        self.parts: dict[int, int] = {}
        self.file = os.path.join(drive.blobs, f"upload-{self.id}")
        with open(self.file, "wb") as f:
            f.truncate(size)
        self.state = "uploading"
        self.error: dict | None = None
        self.entry: dict | None = None
        self.ends_at = 0.0

    def status(self) -> dict:
        out = {"upload_id": self.id, "state": self.state, "mode": "relay", "path": self.path, "size": self.size,
               "part_size": self.part_size, "part_count": self.count,
               "parts_held": [{"part_number": n, "size": z} for n, z in sorted(self.parts.items())]}
        if self.state == "done":
            # As the server: the file's entry as it is now, by id (a newer
            # head, or null once it went to the trash).
            out["entry"] = None if self.entry is None or self.entry.get("deleted") else wire(self.entry)
        if self.error is not None:
            out["error"] = self.error
        return out


class Drive:
    def __init__(self, data_dir: str) -> None:
        self.space_id = str(uuid.uuid4())
        self.blobs = os.path.join(data_dir, "blobs")
        os.makedirs(self.blobs, exist_ok=True)
        self.lock = threading.Condition()
        self.seq = 0
        self.entries: dict[str, dict] = {}
        self.live: dict[str, str] = {}  # lower(path) -> id
        #: sha256 -> (file on disk, size)
        self.contents: dict[str, tuple[str, int]] = {}
        self.file_cap = 50 * 1024 * 1024
        self.quota = 5 * 1024 * 1024 * 1024
        #: The largest file a save in parts carries (``limits.upload_bytes``);
        #: 0 is the allowance, as the server's default.
        self.upload_cap = 0
        self.outcomes: dict[str, list[dict]] = {}
        self.requests: Counter[str] = Counter()
        self.trash: list[dict] = []
        #: Saves still to answer as the server with no room for them yet.
        self.busy_writes = 0
        #: Failure knobs (``/admin/limits``): saves to cut off unanswered
        #: before or after they apply, saves to answer with a status of a
        #: proxy or the token check (``[status, count]``), seconds a save or
        #: a content read stalls, seconds a save's answer waits after it
        #: applied, and the feed held (answering nothing new).
        self.drop_writes = 0
        self.drop_after = 0
        self.status_writes: list = [0, 0]
        self.stall_writes = 0.0
        self.stall_content = 0.0
        self.answer_delay = 0.0
        self.feed_hold = False
        self.notices_down = False
        #: Where signed links for content point (``fake_blob.py``); empty
        #: answers every ``source`` through the server's own route.
        self.blob_base = ""
        #: Seconds a link from ``source`` lasts (the server's is 900), and
        #: the life its answer names (0: the same): a store that refuses a
        #: link sooner than it was told is what a reader waiting long meets.
        self.link_life = 900
        self.link_told = 0
        #: Bytes the ``content`` route sent.
        self.content_bytes = 0
        #: Notices the daemon sent, in order.
        self.notices: list[dict] = []
        #: Saves in parts, by id; parts are this large, and an upload takes
        #: this long to read back once complete.
        self.uploads: dict[str, Upload] = {}
        self.part_size = 16 * 1024 * 1024
        self.verify_s = 0.2
        #: Whether a cancel stops an upload being read back (the server's
        #: abort), status reads still to answer as a proxy's 502, and a part
        #: the store loses once at the read-back, opening the upload again.
        self.abort_verifying = True
        self.status_fails = 0
        self.reopen_part = 0
        #: Uploads still to end ``failed`` with this kind at the read-back
        #: (``[kind, count]``: a store that failed, bytes damaged on the
        #: way), and the open uploads a person may have, as the server's 8.
        self.fail_ends: list = ["", 0]
        self.open_cap = 8

    # --- seeding ---------------------------------------------------------

    def seed(self, root: str) -> None:
        index_path = root.rstrip("/") + ".index.json"
        index = None
        if os.path.exists(index_path):
            with open(index_path) as f:
                index = json.load(f)
        if index is None:
            index = {"dirs": [], "files": []}
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames.sort()
                rel = os.path.relpath(dirpath, root)
                if rel != ".":
                    index["dirs"].append(rel)
                for name in sorted(filenames):
                    full = os.path.join(dirpath, name)
                    with open(full, "rb") as fh:
                        data = fh.read()
                    index["files"].append(
                        [os.path.relpath(full, root), hashlib.sha256(data).hexdigest(), len(data),
                         is_text(data), int(os.stat(full).st_mtime * 1000)]
                    )
            with open(index_path, "w") as f:
                json.dump(index, f)
        now = int(time.time() * 1000)
        for rel in index["dirs"]:
            self._new(rel, "folder", None, 0, False, now)
        for rel, sha, size, text, mtime in index["files"]:
            self.contents[sha] = (os.path.join(root, rel), size)
            self._new(rel, "file", sha, size, text, mtime)

    def _new(self, path: str, kind: str, sha, size: int, text: bool, mtime: int) -> dict:
        self.seq += 1
        entry = {"id": str(uuid.uuid4()), "path": path, "kind": kind, "size": size,
                 "sha256": sha, "is_text": text, "modified_ms": mtime, "seq": self.seq}
        self.entries[entry["id"]] = entry
        self.live[key(path)] = entry["id"]
        return entry

    # --- reads -----------------------------------------------------------

    def get(self, path: str) -> dict | None:
        found = self.live.get(key(path))
        return self.entries[found] if found else None

    def file_of(self, sha: str) -> tuple[str, int]:
        found = self.contents.get(sha)
        if found is None:
            raise DriveError("not_found", "no such content")
        return found

    def import_file(self, path: str, src: str) -> dict:
        """A file already on this disk, entered as-is (the large bench's data)."""
        side = src + ".sha256"
        with open(side) as f:
            sha = f.read().strip()
        size = os.path.getsize(src)
        parent = path.rpartition("/")[0]
        with self.lock:
            if parent and self.get(parent) is None:
                self._new(parent, "folder", None, 0, False, int(time.time() * 1000))
            self.contents[sha] = (src, size)
            found = self.get(path)
            if found is not None and found["kind"] == "file":
                # A new version of the file, as a save over it makes: same id.
                found.update(sha256=sha, size=size, modified_ms=int(time.time() * 1000))
                self._bump(found)
                entry = found
            else:
                entry = self._new(path, "file", sha, size, False, int(time.time() * 1000))
            self.lock.notify_all()
        return entry

    def purge(self, sha: str) -> None:
        """A replaced version's bytes past the server's grace: gone."""
        with self.lock:
            self.contents.pop(sha, None)

    def bytes_of(self, sha: str) -> bytes:
        found = self.contents.get(sha)
        if found is None:
            raise DriveError("not_found", "no such content")
        with open(found[0], "rb") as f:
            return f.read()

    def usage(self) -> dict:
        # Every row the space keeps counts, the trash and delete marks too.
        return {"bytes": sum(s for _, s in self.contents.values()), "entries": len(self.entries),
                "max_bytes": self.quota, "max_entries": 100000, "max_file_bytes": self.file_cap}

    def manifest(self) -> dict:
        with self.lock:
            rows = [[e[k] for k in FIELDS] for e in (self.entries[i] for i in self.live.values())]
            return {"space": {"ref": "personal", "id": self.space_id}, "cursor": self.seq,
                    "limits": {"file_bytes": self.file_cap, "upload_bytes": self.upload_cap or self.quota}, "usage": self.usage(),
                    "fields": list(FIELDS), "entries": rows}

    def changes(self, since: int, wait: float) -> dict:
        deadline = time.monotonic() + wait
        if self.feed_hold:
            time.sleep(min(wait, 1.0))
            return {"entries": [], "cursor": since, "resync": False}
        with self.lock:
            while self.seq == since:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                self.lock.wait(left)
            if since > self.seq:
                return {"entries": [], "cursor": self.seq, "resync": True}
            found = [dict(e) for e in self.entries.values() if e["seq"] > since]
            found.sort(key=lambda e: (e["seq"], e["path"]))
            return {"entries": found, "cursor": self.seq, "resync": False, "usage": self.usage()}

    def recreate(self) -> int:
        """As the tables made again: every live entry under a new id, the
        trash gone, the sequence counted from nothing."""
        with self.lock:
            live = [e for e in self.entries.values() if not e.get("deleted")]
            self.entries, self.live, self.trash, self.seq = {}, {}, [], 0
            for old in sorted(live, key=lambda e: (e["kind"] != "folder", e["path"].count("/"))):
                self.seq += 1
                entry = {**old, "id": str(uuid.uuid4()), "seq": self.seq}
                self.entries[entry["id"]] = entry
                self.live[key(entry["path"])] = entry["id"]
            self.lock.notify_all()
            return self.seq

    # --- changes ---------------------------------------------------------

    def _bump(self, entry: dict) -> None:
        self.seq += 1
        entry["seq"] = self.seq

    def _parents(self, path: str) -> None:
        parts = path.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            folder = "/".join(parts[:i])
            found = self.get(folder)
            if found is None:
                self._new(folder, "folder", None, 0, False, int(time.time() * 1000))
            elif found["kind"] != "folder":
                raise DriveError("not_folder", f"{folder} is a file", folder)

    def write(self, path: str, data: bytes, if_match: str | None, if_none_match: bool) -> dict:
        path = norm(path)
        if len(data) > self.file_cap:
            raise DriveError("too_large", f"A file is at most {self.file_cap} bytes", path)
        sha = hashlib.sha256(data).hexdigest()
        with self.lock:
            found = self.get(path)
            if found is not None and found["kind"] == "folder":
                raise DriveError("is_folder", f"{path} is a folder", path)
            if if_none_match and found is not None:
                raise DriveError("exists", f"{path} exists", path)
            if if_match is not None and (found is None or found["sha256"] != if_match):
                raise DriveError("changed", f"{path} changed since it was read", path)
            if sha not in self.contents and self.usage()["bytes"] + len(data) > self.quota:
                raise DriveError("over_quota", "The Drive is full; empty the trash or remove files", path)
            if found is not None and found["sha256"] == sha:
                return found
            self._parents(path)
            if sha not in self.contents:
                blob = os.path.join(self.blobs, sha)
                with open(blob, "wb") as f:
                    f.write(data)
                self.contents[sha] = (blob, len(data))
            now = int(time.time() * 1000)
            if found is None:
                found = self._new(path, "file", sha, len(data), is_text(data), now)
            else:
                found.update(sha256=sha, size=len(data), is_text=is_text(data), modified_ms=now)
                self._bump(found)
            self.lock.notify_all()
            return found

    def mkdir(self, path: str) -> dict:
        path = norm(path)
        with self.lock:
            if self.get(path) is not None:
                raise DriveError("exists", f"{path} exists", path)
            self._parents(path)
            entry = self._new(path, "folder", None, 0, False, int(time.time() * 1000))
            self.lock.notify_all()
            return entry

    def _under(self, path: str) -> list[dict]:
        prefix = key(path) + "/"
        return [self.entries[i] for p, i in self.live.items() if p.startswith(prefix)]

    def trash_entry(self, path: str, kind: str | None, only_empty: bool, if_match: str | None = None) -> str:
        path = norm(path)
        with self.lock:
            found = self.get(path)
            if found is None:
                raise DriveError("not_found", f"Nothing at {path}", path)
            if if_match is not None and found.get("sha256") != if_match:
                raise DriveError("changed", f"{path} changed", path)
            if kind == "file" and found["kind"] == "folder":
                raise DriveError("is_folder", f"{path} is a folder", path)
            if kind == "folder" and found["kind"] != "folder":
                raise DriveError("not_folder", f"{path} is a file", path)
            under = self._under(path) if found["kind"] == "folder" else []
            if only_empty and under:
                raise DriveError("not_empty", f"{path} is not empty", path)
            trash_id = str(uuid.uuid4())
            for entry in [found, *under]:
                del self.live[key(entry["path"])]
                entry["deleted"] = True
                self._bump(entry)
                self.trash.append({"trash_id": trash_id, "path": entry["path"]})
            self.lock.notify_all()
            return trash_id

    def move(self, src: str, dst: str, replace: bool) -> tuple[dict, list[str]]:
        src, dst = norm(src), norm(dst)
        with self.lock:
            found = self.get(src)
            if found is None:
                raise DriveError("not_found", f"Nothing at {src}", src)
            target = self.get(dst)
            removed: list[str] = []
            if target is not None and target["id"] != found["id"]:
                if not replace:
                    raise DriveError("exists", f"{dst} exists", dst)
                if target["kind"] == "folder" or found["kind"] == "folder":
                    raise DriveError("is_folder" if target["kind"] == "folder" else "not_folder",
                                     f"cannot replace {dst}", dst)
                # The target keeps its id and history; the source's bytes
                # become its new version, and the source entry ends.
                target.update(sha256=found["sha256"], size=found["size"], is_text=found["is_text"],
                              modified_ms=int(time.time() * 1000))
                self._bump(target)
                del self.live[key(found["path"])]
                found["deleted"] = True
                self._bump(found)
                removed.append(found["id"])
                self.lock.notify_all()
                return target, removed
            self._parents(dst)
            under = self._under(src) if found["kind"] == "folder" else []
            for entry in [found, *under]:
                del self.live[key(entry["path"])]
            for entry in [found, *under]:
                entry["path"] = dst + entry["path"][len(src):]
                self.live[key(entry["path"])] = entry["id"]
                self._bump(entry)
            self.lock.notify_all()
            return found, removed

    # --- saves in parts --------------------------------------------------

    def _precondition(self, path: str, if_match: str | None, if_none_match: bool) -> dict | None:
        found = self.get(path)
        if found is not None and found["kind"] == "folder":
            raise DriveError("is_folder", f"{path} is a folder", path)
        if if_none_match and found is not None:
            raise DriveError("exists", f"{path} exists", path)
        if if_match is not None and (found is None or found["sha256"] != if_match):
            raise DriveError("changed", f"{path} changed since it was read", path)
        return found

    def _record(self, path: str, sha: str, blob: str, size: int) -> dict:
        """A version recorded at ``path`` from bytes already on disk. Under ``lock``."""
        self._parents(path)
        self.contents.setdefault(sha, (blob, size))
        found = self.get(path)
        now = int(time.time() * 1000)
        if found is None:
            found = self._new(path, "file", sha, size, False, now)
        elif found["sha256"] != sha:
            found.update(sha256=sha, size=size, is_text=False, modified_ms=now)
            self._bump(found)
        self.lock.notify_all()
        return found

    def upload_begin(self, path: str, size: int, sha: str | None, if_match: str | None,
                     if_none_match: bool, call: str | None) -> tuple[dict, dict | None]:
        """The begin's answer, and the entry recorded when it was held."""
        path = norm(path)
        with self.lock:
            found = self._precondition(path, if_match, if_none_match)
            if size > (self.upload_cap or self.quota):
                raise DriveError("too_large", f"A file is at most {self.upload_cap or self.quota} bytes", path)
            if sha in self.contents and self.contents[sha][1] == size:
                entry = self._record(path, sha, self.contents[sha][0], size)
                return {"mode": "held", "entry": wire(entry), "cursor": self.seq}, entry
            held = found["size"] if found is not None else 0
            if self.usage()["bytes"] - held + size > self.quota:
                raise DriveError("over_quota", "The Drive is full; delete files or clear a file's earlier versions",
                                 path)
            if size <= self.file_cap:
                return {"mode": "single", "max_bytes": self.file_cap}, None
            for up in self.uploads.values():
                # As the server: the same path, size, sha256 and conditions
                # resume it, or join it while it is read back.
                if (up.state in ("uploading", "verifying") and up.path == path and up.size == size and up.sha == sha
                        and sha and up.if_match == if_match and up.if_none_match == if_none_match):
                    up.call = call
                    return {**up.status(), "resumed": True}, None
            if sum(u.state in ("uploading", "verifying") for u in self.uploads.values()) >= self.open_cap:
                raise DriveError("too_many_uploads", "As many uploads are under way as the Drive allows", path)
            up = Upload(self, path, size, sha, if_match, if_none_match, call)
            self.uploads[up.id] = up
            return {**up.status(), "resumed": False}, None

    def _upload(self, upload_id: str) -> Upload:
        up = self.uploads.get(upload_id)
        if up is None:
            raise DriveError("not_found", "no such upload")
        return up

    def upload_part(self, upload_id: str, n: int, data: bytes) -> dict:
        with self.lock:
            up = self._upload(upload_id)
            if up.state != "uploading":
                raise DriveError("upload_ended", "the upload has ended")
            want = min(up.part_size, up.size - (n - 1) * up.part_size)
            if not 1 <= n <= up.count or len(data) != want:
                raise DriveError("bad_part", f"part {n} is {want} bytes")
        with open(up.file, "r+b") as f:
            f.seek((n - 1) * up.part_size)
            f.write(data)
        with self.lock:
            up.parts[n] = len(data)
        return {"part_number": n, "size": len(data), "md5": hashlib.md5(data).hexdigest()}

    def upload_complete(self, upload_id: str) -> dict:
        with self.lock:
            up = self._upload(upload_id)
            if up.state == "uploading":
                missing = [n for n in range(1, up.count + 1) if n not in up.parts]
                if missing:
                    exc = DriveError("parts_missing", "some parts are missing")
                    exc.missing = missing  # type: ignore[attr-defined]
                    raise exc
                up.state = "verifying"
                up.ends_at = time.monotonic() + self.verify_s
                # The server ends it, and files its outcome, on its own.
                threading.Timer(self.verify_s, self.upload_end, (up,)).start()
            return up.status()

    def upload_end(self, up: Upload) -> None:
        """Read the stored file back and record it, once: the outcome is
        filed for the command here, whoever asks for the status."""
        with self.lock:
            if up.state != "verifying":
                return
            if self.reopen_part and self.reopen_part in up.parts:
                del up.parts[self.reopen_part]
                self.reopen_part = 0
                up.state = "uploading"
                return
        h = hashlib.sha256()
        with open(up.file, "rb") as f:
            while chunk := f.read(8 << 20):
                h.update(chunk)
        sha = h.hexdigest()
        with self.lock:
            if up.state != "verifying":
                return
            try:
                kind, left = self.fail_ends
                if left:
                    self.fail_ends = [kind, left - 1]
                    raise DriveError(kind, "the store failed", up.path)
                if up.sha and sha != up.sha:
                    raise DriveError("mismatch", "the stored bytes are not the file named", up.path)
                found = self._precondition(up.path, up.if_match, up.if_none_match)
                held = found["size"] if found is not None else 0
                if sha not in self.contents and self.usage()["bytes"] - held + up.size > self.quota:
                    raise DriveError("over_quota", "The Drive is full", up.path)
                blob = os.path.join(self.blobs, sha)
                if sha not in self.contents:
                    os.replace(up.file, blob)
                up.entry = self._record(up.path, sha, blob, up.size)
                up.state = "done"
                outcome = {"op": "write", "path": up.path, "ok": True}
            except DriveError as exc:
                up.state, up.error = "failed", {"code": exc.code, "message": exc.message}
                outcome = {"op": "write", "path": up.path, "ok": False, "code": exc.code, "error": exc.message}
            if up.call:
                self.outcomes.setdefault(up.call, []).append(outcome)
        with contextlib.suppress(OSError):
            os.unlink(up.file)

    def upload_status(self, upload_id: str) -> dict:
        with self.lock:
            up = self._upload(upload_id)
            due = up.state == "verifying" and time.monotonic() >= up.ends_at
        if due:
            self.upload_end(up)
        with self.lock:
            return up.status()

    def upload_cancel(self, upload_id: str) -> dict:
        with self.lock:
            up = self._upload(upload_id)
            if up.state in ("done", "failed", "aborted") or (up.state == "verifying" and not self.abort_verifying):
                exc = DriveError("upload_ended", "the upload has ended")
                exc.state = "verifying" if up.state == "verifying" else up.state  # type: ignore[attr-defined]
                raise exc
            up.state = "aborted"
        with contextlib.suppress(OSError):
            os.unlink(up.file)
        return up.status()


def wire(entry: dict) -> dict:
    return {k: entry[k] for k in FIELDS} | ({"deleted": True} if entry.get("deleted") else {})


class Endpoint:
    """The mount endpoint's answers over a ``Drive``, whatever carries them
    (HTTP here, a call in process in ``local.py``): which texts a batch
    holds, and what a change answers and files for its call."""

    def __init__(self, drive: Drive) -> None:
        self.drive = drive

    def texts(self, ids: list[str] | None) -> list[tuple[str, str, bytes]]:
        """(id, sha256, bytes) per live text among ``ids`` (None: every one).
        A batch asked by id stops once it reaches ``TEXTS_MAX_BYTES``."""
        drive, out, sent = self.drive, [], 0
        for entry_id in sorted(drive.entries) if ids is None else ids:
            entry = drive.entries.get(entry_id)
            if entry is None or entry.get("deleted") or not entry["is_text"]:
                continue
            data = drive.bytes_of(entry["sha256"])
            out.append((entry_id, entry["sha256"], data))
            sent += len(data)
            if ids is not None and sent >= TEXTS_MAX_BYTES:
                break
        return out

    def _filed(self, call: str | None, op: str, path: str, error: DriveError | None = None) -> None:
        if not call:
            return
        outcome = {"op": op, "path": path, "ok": error is None}
        if error is not None:
            outcome.update(code=error.code, error=error.message)
        with self.drive.lock:
            self.drive.outcomes.setdefault(call, []).append(outcome)

    def _change(self, op: str, call: str | None, path: str, apply) -> dict:
        try:
            answer, shown = apply()
        except DriveError as exc:
            self._filed(call, op, exc.path or path, exc)
            raise
        self._filed(call, op, shown)
        return answer

    def write(self, path: str, body: bytes, if_match: str | None, if_none_match: bool, call: str | None) -> dict:
        def apply():
            entry = self.drive.write(path, body, if_match, if_none_match)
            return {"entry": wire(entry), "cursor": self.drive.seq}, entry["path"]
        return self._change("write", call, path, apply)

    def upload_begin(self, path: str, size: int, sha: str | None, if_match: str | None,
                     if_none_match: bool, call: str | None) -> dict:
        """A refusal is the save's outcome, filed for the call; so is a
        begin answered ``held``, which records the version at once."""
        try:
            answer, entry = self.drive.upload_begin(path, size, sha, if_match, if_none_match, call)
        except DriveError as exc:
            self._filed(call, "write", exc.path or path, exc)
            raise
        if entry is not None:
            self._filed(call, "write", entry["path"])
        return answer

    def mkdir(self, path: str, call: str | None) -> dict:
        def apply():
            entry = self.drive.mkdir(path)
            return {"entry": wire(entry), "cursor": self.drive.seq}, entry["path"]
        return self._change("mkdir", call, path, apply)

    def move(self, src: str, dst: str, replace: bool, call: str | None) -> dict:
        def apply():
            entry, removed = self.drive.move(src, dst, replace)
            return {"entry": wire(entry), "removed": removed, "cursor": self.drive.seq}, entry["path"]
        return self._change("move", call, src, apply)

    def trash(self, path: str, kind: str | None, empty: bool, call: str | None, if_match: str | None = None) -> dict:
        def apply():
            trash_id = self.drive.trash_entry(path, kind, empty, if_match)
            return {"trash_id": trash_id, "cursor": self.drive.seq}, path
        return self._change("trash", call, path, apply)


class Handler(BaseHTTPRequestHandler):
    """HTTP framing, headers and connection faults over the ``Endpoint``."""

    protocol_version = "HTTP/1.1"
    drive: Drive
    endpoint: Endpoint
    token: str

    def log_message(self, fmt, *args):  # quiet
        pass

    def _send(self, status: int, body: bytes, ctype: str = "application/json", headers: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj) -> None:
        self._send(status, json.dumps(obj).encode())

    def _cut(self) -> None:
        """End the connection with no answer, as a reset or timeout does."""
        self.close_connection = True
        with contextlib.suppress(OSError):
            self.connection.shutdown(socket.SHUT_RDWR)

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_PUT(self):
        self._route("PUT")

    def do_DELETE(self):
        self._route("DELETE")

    def _route(self, method: str) -> None:
        url = urlsplit(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
        drive = self.drive
        if url.path.startswith("/admin/"):
            return self._admin(method, url.path[len("/admin/"):], q)
        if not url.path.startswith(PREFIX):
            return self._json(404, {"code": "not_found", "message": "no route"})
        if self.headers.get("Authorization") != f"Bearer {self.token}":
            self._body()
            return self._json(401, {"detail": "Invalid or expired mount token"})
        action = url.path[len(PREFIX):]
        drive.requests[action] += 1
        endpoint, call = self.endpoint, self.headers.get("X-Livefs-Call")
        try:
            if action == "spaces" and method == "GET":
                return self._json(200, {"spaces": [{"ref": "personal", "name": "personal", "id": drive.space_id}]})
            if action == "manifest" and method == "GET":
                return self._json(200, drive.manifest())
            if action in ("texts", "content") and drive.stall_content:
                time.sleep(drive.stall_content)
            if action == "notices" and method == "POST":
                if drive.notices_down:
                    self._body()
                    return self._cut()
                with drive.lock:
                    drive.notices.extend(json.loads(self._body() or b"{}").get("notices") or [])
                return self._json(200, {"filed": True})
            if action == "texts" and method == "POST":
                body = self._body()
                ids = None if q.get("all") == "1" else json.loads(body or b"{}").get("ids") or []
                frames = (b"%s %s %d\n%s\n" % (i.encode(), sha.encode(), len(data), data)
                          for i, sha, data in endpoint.texts(ids))
                return self._send(200, b"".join(frames), "application/octet-stream")
            if action == "content" and method == "GET":
                src, size = drive.file_of(q["sha256"])
                rng = self.headers.get("Range")
                start, end, status = 0, size, 200
                if rng and rng.startswith("bytes="):
                    a, _, b = rng[6:].partition("-")
                    start, end, status = int(a), min(int(b) + 1 if b else size, size), 206
                self.send_response(status)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(end - start))
                self.send_header("ETag", f'"{q["sha256"]}"')
                if status == 206:
                    self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
                self.end_headers()
                with open(src, "rb") as f:
                    self.wfile.flush()
                    self.connection.sendfile(f, start, end - start)
                with drive.lock:
                    drive.content_bytes += end - start
                return None
            if action == "source" and method == "GET":
                src, size = drive.file_of(q["sha256"])
                if not drive.blob_base:
                    # Bytes the computer does not reach the store for.
                    return self._json(200, {"via": "api", "sha256": q["sha256"], "size": size})
                life = drive.link_life
                expires = int(time.time()) + life
                query = urlencode({"f": src, "exp": expires, "sig": hashlib.sha256(f"{src}{expires}".encode()).hexdigest()[:16]})
                return self._json(200, {"via": "link", "sha256": q["sha256"], "size": size,
                                        "url": f"{drive.blob_base}/o/{q['sha256']}?{query}",
                                        "expires_in": drive.link_told or life})
            if action == "entry" and method == "GET":
                entry = drive.get(norm(q.get("path", "")))
                if entry is None:
                    raise DriveError("not_found", "nothing there")
                return self._json(200, {"entry": wire(entry)})
            if action == "changes" and method == "GET":
                return self._json(200, drive.changes(int(q.get("since", 0)), min(float(q.get("wait", 0)), 30)))
            if action == "write" and method == "PUT":
                body = self._body()
                if drive.stall_writes:
                    time.sleep(drive.stall_writes)
                with drive.lock:
                    busy, drive.busy_writes = drive.busy_writes > 0, max(0, drive.busy_writes - 1)
                    drop, drive.drop_writes = drive.drop_writes > 0, max(0, drive.drop_writes - 1)
                    drop_after, drive.drop_after = drive.drop_after > 0, max(0, drive.drop_after - 1)
                    code, left = drive.status_writes
                    if left:
                        drive.status_writes = [code, left - 1]
                if drop:
                    return self._cut()
                if left:
                    # Answered before any handler, as a proxy or the token
                    # check does: nothing is filed.
                    return self._send(code, b"<html>bad gateway</html>", "text/html")
                if busy:
                    # As the real route: never filed, the daemon sends it again.
                    error = DriveError("unavailable", "The Drive is busy; try again shortly", q["path"])
                    return self._send(503, json.dumps({"code": error.code, "message": error.message,
                                                       "path": q["path"]}).encode(),
                                      headers={"Retry-After": "1"})
                match = self.headers.get("If-Match")
                answer = endpoint.write(q["path"], body, match.strip('"') if match else None,
                                        self.headers.get("If-None-Match") == "*", call)
                if drop_after:
                    return self._cut()
                if drive.answer_delay:
                    time.sleep(drive.answer_delay)
                return self._json(200, answer)
            if action.startswith("uploads"):
                return self._uploads(method, action, q, call)
            if action == "mkdir" and method == "POST":
                self._body()
                return self._json(200, endpoint.mkdir(q["path"], call))
            if action == "move" and method == "POST":
                self._body()
                return self._json(200, endpoint.move(q["from"], q["to"], q.get("replace") == "1", call))
            if action == "trash" and method == "POST":
                self._body()
                match = self.headers.get("If-Match")
                return self._json(200, endpoint.trash(q["path"], q.get("kind"), q.get("empty") == "1", call,
                                                      match.strip('"') if match else None))
            return self._json(404, {"code": "not_found", "message": f"no action {action}"})
        except ServerError as exc:
            path = getattr(exc, "path", "")
            body = {"code": exc.code, "message": exc.message, "path": path}
            if getattr(exc, "missing", None):
                body["missing_parts"] = exc.missing  # type: ignore[attr-defined]
            if getattr(exc, "state", None):
                body["state"] = exc.state  # type: ignore[attr-defined]
            return self._json(STATUS.get(exc.code, 400), body)

    def _uploads(self, method: str, action: str, q: dict, call: str | None) -> None:
        """Saves in parts, through the server only (relay)."""
        drive, parts = self.drive, action.split("/")
        if len(parts) == 1 and method == "POST":
            body = json.loads(self._body() or b"{}")
            match = self.headers.get("If-Match")
            return self._json(200, self.endpoint.upload_begin(
                q["path"], int(body["size"]), body.get("sha256"), match.strip('"') if match else None,
                self.headers.get("If-None-Match") == "*", call))
        upload_id = parts[1]
        if len(parts) == 4 and parts[2] == "parts" and method == "PUT":
            return self._json(200, drive.upload_part(upload_id, int(parts[3]), self._body()))
        if len(parts) == 3 and parts[2] == "complete" and method == "POST":
            self._body()
            return self._json(202, drive.upload_complete(upload_id))
        if len(parts) == 2 and method == "GET":
            with drive.lock:
                failing, drive.status_fails = drive.status_fails > 0, max(0, drive.status_fails - 1)
            if failing:
                return self._send(502, b"<html>bad gateway</html>", "text/html")
            return self._json(200, drive.upload_status(upload_id))
        if len(parts) == 2 and method == "DELETE":
            return self._json(200, drive.upload_cancel(upload_id))
        return self._json(404, {"code": "not_found", "message": f"no action {action}"})

    def _admin(self, method: str, action: str, q: dict) -> None:
        drive = self.drive
        try:
            if action == "write":
                entry = drive.write(q["path"], self._body(), None, False)
                return self._json(200, wire(entry))
            if action == "mkdir":
                return self._json(200, wire(drive.mkdir(q["path"])))
            if action == "move":
                entry, _ = drive.move(q["from"], q["to"], q.get("replace") == "1")
                return self._json(200, wire(entry))
            if action == "trash":
                return self._json(200, {"trash_id": drive.trash_entry(q["path"], None, False)})
            if action == "entry":
                entry = drive.get(norm(q["path"]))
                return self._json(200, {"entry": wire(entry) if entry else None})
            if action == "content":
                entry = drive.get(norm(q["path"]))
                if entry is None:
                    return self._json(404, {"code": "not_found"})
                return self._send(200, drive.bytes_of(entry["sha256"]), "application/octet-stream")
            if action == "limits":
                body = json.loads(self._body() or b"{}")
                unknown = set(body) - set(KNOBS)
                if unknown:
                    return self._json(400, {"code": "unknown_knob", "message": ", ".join(sorted(unknown))})
                # Answers what each knob was, for the caller to put back.
                before = {name: getattr(drive, name) for name in body}
                for name, value in body.items():
                    setattr(drive, name, KNOBS[name](value))
                return self._json(200, before)
            if action == "outcomes":
                with drive.lock:
                    return self._json(200, drive.outcomes.pop(q["call"], []))
            if action == "stats":
                return self._json(200, {"requests": dict(drive.requests), "trash": len(drive.trash),
                                        "seq": drive.seq, "usage": drive.usage(),
                                        "content_bytes": drive.content_bytes})
            if action == "purge":
                drive.purge(q["sha256"])
                return self._json(200, {})
            if action == "import":
                return self._json(200, wire(drive.import_file(q["path"], q["file"])))
            if action == "notices":
                with drive.lock:
                    return self._json(200, list(drive.notices))
            if action == "recreate":
                return self._json(200, {"seq": drive.recreate()})
            if action == "trash_list":
                return self._json(200, drive.trash)
            return self._json(404, {"code": "not_found"})
        except ServerError as exc:
            return self._json(STATUS.get(exc.code, 400), {"code": exc.code, "message": exc.message})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8790)
    ap.add_argument("--token", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--seed")
    a = ap.parse_args()
    drive = Drive(a.data)
    t0 = time.monotonic()
    if a.seed:
        drive.seed(a.seed)
    Handler.drive = drive
    Handler.endpoint = Endpoint(drive)
    Handler.token = a.token
    server = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    server.daemon_threads = True
    print(f"fake drive: {len(drive.live)} entries in {time.monotonic() - t0:.1f}s on :{a.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
