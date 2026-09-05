"""Workspace file transfer runtime, uploaded into the sandbox verbatim.

The backend moves metadata only. This script walks the workspace, hashes
files, and moves bytes straight between the sandbox disk and object storage
through presigned URLs. It runs on the sandbox's bare python3, so it is
standard library only and imports nothing from the host repo.

CLI: ``python3 wsfiles_transfer.py <op> (--spec-b64 <base64 json> | <in.json>)``
where ``op`` is scan, push, pull, pack or unlink. The result is the last
stdout line, behind ``RESULT_MARKER``, and the process exits 0 even on partial
failure; exit 2 is reserved for unreadable or invalid input.
"""

import base64
import codecs
import hashlib
import http.client
import json
import os
import shutil
import socket
import ssl
import stat
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any

_CHUNK = 1024 * 1024
# Defaults for a spec that names no concurrency. The server always sends its
# own (transfer.py's PUSH_CONCURRENCY / PULL_CONCURRENCY); these are the same
# measured knees, so a runtime driven by hand behaves like the server's.
_PUSH_CONCURRENCY = 16
_PULL_CONCURRENCY = 32
_PACK_DIR = "_internal/packs"
_PACK_STALE_S = 3600.0
# Every transient file this runtime or the server writes into the workspace
# sits at the root under this prefix, and the scan skips it there and only
# there: a user's own ``sub/.wsfiles-notes`` is a file like any other.
_TEMP_PREFIX = ".wsfiles-"
_RELAY_STAGING_PREFIX = ".wsfiles-relay-"
_MAX_ATTEMPTS = 3
_BACKOFF_S = (0.5, 2.0)
_ERROR_BODY_LIMIT = 64 * 1024


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------


def _hash_file(path: str) -> tuple[str, bool, int]:
    """Hash and classify the file in one read; returns (sha256, is_binary, size).

    The size is the byte count the digest covers, not a separate stat: a file
    that grows between the two would otherwise be reported with a digest of
    the new bytes and the length of the old.

    Binary means a NUL anywhere, or bytes anywhere that are not strict UTF-8.
    Neither is sampled from a leading window: text is stored in a column that
    cannot hold a NUL, so one further in is dropped on the way to the row and
    the stored bytes stop hashing to the row's own digest, and a blob row is
    never re-read on the way out, so a bad sequence after the first pages
    would be served as text with replacement marks. The decoder is dropped at
    the first NUL or failure, so the rest of a binary costs only the hash.
    """
    h = hashlib.sha256()
    size = 0
    decoder = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            h.update(chunk)
            if decoder is not None:
                if b"\0" in chunk:
                    decoder = None
                else:
                    try:
                        decoder.decode(chunk)
                    except UnicodeDecodeError:
                        decoder = None
            size += len(chunk)
    if decoder is not None:
        try:
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            decoder = None
    return h.hexdigest(), decoder is None, size


def _resolve_under_root(root: str, rel: str) -> str | None:
    """Return the absolute final path, or None when ``rel`` escapes ``root``.

    The check is lexical on purpose: a symlink already inside the workspace
    pointing outside is the user's own arrangement, but a JSON item must never
    name a location outside the root it was given.
    """
    if not rel or rel.startswith("/") or os.path.isabs(rel):
        return None
    norm = os.path.normpath(rel)
    if norm == "." or os.path.isabs(norm):
        return None
    parts = norm.split(os.sep)
    # Only a whole ".." component escapes; a name that merely starts with two
    # dots ("..notes") is an ordinary file.
    if ".." in parts:
        return None
    return os.path.join(root, norm)


def _result(status: str, http_status: int | None = None, error: str | None = None) -> dict[str, Any]:
    return {"status": status, "http": http_status, "error": error}


def _read_error_body(resp: Any) -> str:
    try:
        return resp.read(_ERROR_BODY_LIMIT).decode("utf-8", "replace")
    except Exception:
        return ""


def _is_connection_error(exc: BaseException) -> bool:
    if isinstance(exc, (urllib.error.HTTPError, _HttpStatusError)):
        return False
    return isinstance(
        exc,
        (
            urllib.error.URLError,
            socket.timeout,
            TimeoutError,
            ConnectionError,
            OSError,
            http.client.HTTPException,
        ),
    )


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


def scan(spec: dict[str, Any]) -> dict[str, Any]:
    root = os.path.abspath(spec["root"])
    exclude_dir_names = set(spec.get("exclude_dir_names") or ())
    exclude_rel_dirs = {p.strip("/") for p in (spec.get("exclude_rel_dirs") or ())}
    exclude_rel_dir_prefixes = tuple(
        p.strip("/") for p in (spec.get("exclude_rel_dir_prefixes") or ())
    )
    exclude_basenames = set(spec.get("exclude_basenames") or ())
    exclude_rel_files = {p.strip("/") for p in (spec.get("exclude_rel_files") or ())}
    exclude_root_basenames = set(spec.get("exclude_root_basenames") or ())
    exclude_root_prefixes = tuple(spec.get("exclude_root_basename_prefixes") or ())
    exclude_suffixes = tuple(spec.get("exclude_suffixes") or ())
    max_file_bytes = spec.get("max_file_bytes")
    prior = spec.get("prior") or {}

    entries: list[dict[str, Any]] = []
    oversized: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    counts = {"hashed": 0, "reused": 0}

    def excluded_dir(name: str, rel: str) -> bool:
        return (
            name in exclude_dir_names
            or rel in exclude_rel_dirs
            or rel.startswith(exclude_rel_dir_prefixes)
        )

    def excluded_name(name: str) -> bool:
        return name in exclude_dir_names or name in exclude_basenames

    def file_entry(abs_path: str, rel: str) -> None:
        st = os.stat(abs_path, follow_symlinks=False)
        size = st.st_size
        if max_file_bytes is not None and size > max_file_bytes:
            oversized.append({"path": rel, "size": size})
            return
        known = prior.get(rel)
        # The backend stores mtimes at microsecond precision, so the reuse
        # check compares at that granularity; an exact ns compare would rehash
        # every file on every scan.
        if (
            known
            and len(known) >= 3
            and known[0] == size
            and known[2]
            and int(known[1]) // 1000 == st.st_mtime_ns // 1000
        ):
            digest, is_binary = known[2], None
            counts["reused"] += 1
        else:
            digest, is_binary, size = _hash_file(abs_path)
            counts["hashed"] += 1
        entries.append(
            {
                "path": rel,
                "kind": "file",
                "size": size,
                "mtime_ns": st.st_mtime_ns,
                "mode": stat.S_IMODE(st.st_mode),
                "sha256": digest,
                "symlink_target": None,
                "is_binary": is_binary,
            }
        )

    def symlink_entry(abs_path: str, rel: str) -> None:
        st = os.lstat(abs_path)
        entries.append(
            {
                "path": rel,
                "kind": "symlink",
                "size": 0,
                "mtime_ns": st.st_mtime_ns,
                "mode": 0,
                "sha256": None,
                "symlink_target": os.readlink(abs_path),
            }
        )

    def walk(abs_dir: str, rel_dir: str) -> None:
        """Post-order walk: a directory's own row follows its children's."""
        try:
            with os.scandir(abs_dir) as it:
                children = sorted(it, key=lambda e: e.name)
        except OSError as exc:
            errors.append({"path": rel_dir or ".", "error": str(exc), "errno": exc.errno})
            return
        for child in children:
            rel = f"{rel_dir}/{child.name}" if rel_dir else child.name
            # Reserved root names are skipped whatever the entry is: the
            # pull op's sweep removes any root ``.wsfiles-`` entry no item
            # claims, directories included, so a row for one would only
            # promise what the next restore deletes.
            if not rel_dir and (
                child.name in exclude_root_basenames
                or child.name.startswith(exclude_root_prefixes)
            ):
                continue
            try:
                if child.is_symlink():
                    if excluded_name(child.name) or rel in exclude_rel_files:
                        continue
                    symlink_entry(child.path, rel)
                elif child.is_dir(follow_symlinks=False):
                    if excluded_dir(child.name, rel):
                        continue
                    walk(child.path, rel)
                    # Every directory gets a row, not only empty leaves: a
                    # directory's mode is user data too (a read-only tree
                    # has to come back read-only).
                    st = child.stat(follow_symlinks=False)
                    entries.append(
                        {
                            "path": rel,
                            "kind": "dir",
                            "size": 0,
                            "mtime_ns": st.st_mtime_ns,
                            "mode": stat.S_IMODE(st.st_mode),
                            "sha256": None,
                            "symlink_target": None,
                        }
                    )
                elif child.is_file(follow_symlinks=False):
                    if (
                        child.name in exclude_basenames
                        or rel in exclude_rel_files
                        or child.name.endswith(exclude_suffixes)
                    ):
                        continue
                    file_entry(child.path, rel)
            except OSError as exc:
                errors.append({"path": rel, "error": str(exc), "errno": exc.errno})

    walk(root, "")
    entries.sort(key=lambda e: e["path"])
    return {
        "entries": entries,
        "oversized": oversized,
        "errors": errors,
        "hashed": counts["hashed"],
        "reused": counts["reused"],
    }


# ---------------------------------------------------------------------------
# HTTP with per-thread connection reuse
# ---------------------------------------------------------------------------
#
# The sandbox's environment routes HTTPS through an egress proxy that exists
# to substitute platform secrets into headers. A presigned URL carries its
# own credentials, so the store needs none of that, and the proxy is shared
# by every sandbox on the node: under load it stalls a CONNECT for tens of
# seconds or refuses it outright. Each worker thread therefore connects to
# the store directly, uses the proxy only where direct egress is blocked,
# and keeps one connection per host so a restore is not a handshake per
# object.

_local = threading.local()
_CONNECT_TIMEOUT_S = 5.0
# Opening every worker's connection at the same instant drops SYNs somewhere
# between the sandbox and the store, and a dropped SYN costs its retransmit
# backoff (1 s, then 3 s, then 7 s). Handshakes go through this gate a few
# at a time; established connections are pooled, so the gate is paid once.
# Width 8 was measured and is worse: the sandbox runs on a one-CPU quota,
# and handshakes are the most CPU-expensive thing a worker does.
_CONNECT_GATE = threading.BoundedSemaphore(4)
# One TLS context for the process so a session ticket from the first
# handshake to a host resumes every later one: the store's certificate is
# verified once per process instead of once per worker thread. On a
# one-CPU sandbox the saving is CPU, not round trips.
_SSL_CTX = ssl.create_default_context()
_SESSIONS: dict[str, Any] = {}
_HANDSHAKES = {"full": 0, "resumed": 0}
_HANDSHAKES_LOCK = threading.Lock()


class _HttpsConnection(http.client.HTTPSConnection):
    """HTTPSConnection that offers the host's cached TLS session on connect."""

    def connect(self) -> None:
        http.client.HTTPConnection.connect(self)
        server_hostname = self._tunnel_host or self.host
        session = _SESSIONS.get(server_hostname)
        self.sock = _SSL_CTX.wrap_socket(self.sock, server_hostname=server_hostname, session=session)
        with _HANDSHAKES_LOCK:
            _HANDSHAKES["resumed" if self.sock.session_reused else "full"] += 1


def _remember_session(conn: Any) -> None:
    """Cache the session once the server has sent its ticket.

    With TLS 1.3 the ticket follows the handshake, so it is only there after
    the first response has been read from the connection.
    """
    sock = getattr(conn, "sock", None)
    session = getattr(sock, "session", None)
    if session is not None:
        _SESSIONS[conn._tunnel_host or conn.host] = session


class _HttpStatusError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body


def _proxy_for(scheme: str, host: str) -> tuple[str, int] | None:
    try:
        if urllib.request.proxy_bypass(host):
            return None
        raw = urllib.request.getproxies().get(scheme)
    except Exception:
        return None
    if not raw:
        return None
    u = urllib.parse.urlsplit(raw if "://" in raw else f"http://{raw}")
    if not u.hostname:
        return None
    return u.hostname, u.port or 80


def _open(scheme: str, host: str, port: int, timeout_s: float) -> Any:
    cls = _HttpsConnection if scheme == "https" else http.client.HTTPConnection
    routes = getattr(_local, "routes", None)
    if routes is None:
        routes = _local.routes = {}
    proxy = _proxy_for(scheme, host)
    with _CONNECT_GATE:
        if proxy is not None and routes.get(host) != "direct" and routes.get(host) != "proxy":
            conn = cls(host, port, timeout=_CONNECT_TIMEOUT_S)
            try:
                conn.connect()
                routes[host] = "direct"
                conn.timeout = timeout_s
                conn.sock.settimeout(timeout_s)
                conn.wsfiles_absolute_target = False
                return conn
            except OSError:
                routes[host] = "proxy"
                conn.close()
        if proxy is None or routes.get(host) == "direct":
            conn = cls(host, port, timeout=timeout_s)
            conn.wsfiles_absolute_target = False
        else:
            conn = cls(proxy[0], proxy[1], timeout=timeout_s)
            if scheme == "https":
                conn.set_tunnel(host, port)
            conn.wsfiles_absolute_target = scheme == "http"
        conn.connect()
    return conn


def _connection(scheme: str, host: str, port: int, timeout_s: float) -> Any:
    key = (scheme, host, port)
    conns = getattr(_local, "conns", None)
    if conns is None:
        conns = _local.conns = {}
    conn = conns.get(key)
    if conn is None:
        conn = conns[key] = _open(scheme, host, port, timeout_s)
    return key, conn


def _drop_connection(key: tuple) -> None:
    conns = getattr(_local, "conns", None)
    conn = conns.pop(key, None) if conns else None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def _request(method: str, url: str, timeout_s: float, body: Any = None, headers: dict | None = None) -> Any:
    """Send one request on the thread's pooled connection; returns the response.

    The caller must read the response to the end before issuing another
    request on the same thread. Anything but a 2xx raises ``_HttpStatusError``
    with the body already consumed: a redirect is not followed, because a PUT
    answered with one stored nothing and the signature would not survive the
    new location. Any transport failure closes the connection so the retry
    starts clean.
    """
    u = urllib.parse.urlsplit(url)
    scheme = u.scheme or "https"
    host = u.hostname or ""
    port = u.port or (443 if scheme == "https" else 80)
    key, conn = _connection(scheme, host, port, timeout_s)
    target = url if conn.wsfiles_absolute_target else (u.path or "/") + (f"?{u.query}" if u.query else "")
    try:
        conn.request(method, target, body=body, headers=headers or {})
        resp = conn.getresponse()
    except BaseException:
        _drop_connection(key)
        raise
    if scheme == "https":
        _remember_session(conn)
    if not 200 <= resp.status < 300:
        text = _read_error_body(resp)
        _drop_connection(key)
        raise _HttpStatusError(resp.status, text)
    resp.wsfiles_conn_key = key
    return resp


def _drop_response_connection(resp: Any) -> None:
    """Discard the connection behind a response whose body read failed.

    Left in the pool, its unread body makes the next request on the thread
    fail with ``CannotSendRequest``, which reads as unreachable and burns an
    attempt on a transient read error.
    """
    key = getattr(resp, "wsfiles_conn_key", None)
    if key is not None:
        _drop_connection(key)


def _timed(fn: Any, *args: Any) -> dict[str, Any]:
    t0 = time.monotonic()
    res = fn(*args)
    res["ms"] = int((time.monotonic() - t0) * 1000)
    return res


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------


class _BoundedBody:
    """A file wrapper that stops at ``limit`` and remembers if more was there.

    ``http.client`` streams a file object to EOF whatever Content-Length it
    was handed, so a file appended to during its own PUT puts the tail on the
    wire behind the declared body: the store keeps the prefix, which still
    matches the signed digest, and the tail arrives at the head of the next
    request on that pooled connection.
    """

    def __init__(self, fh: Any, limit: int) -> None:
        self._fh = fh
        self._left = limit
        self.overrun = False

    def read(self, size: int = -1) -> bytes:
        if self._left <= 0:
            # One byte past the declared body decides; it is never sent.
            self.overrun = self.overrun or bool(self._fh.read(1))
            return b""
        want = self._left if size is None or size < 0 else min(size, self._left)
        data = self._fh.read(want)
        self._left -= len(data)
        return data


def _size_of(path: str) -> int:
    """The file's current size, or -1 when it can no longer be stat'd."""
    try:
        return os.stat(path).st_size
    except OSError:
        return -1


def _push_one(root: str, item: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    final = _resolve_under_root(root, item.get("path", ""))
    if final is None:
        return _result("failed", error="path escapes root")
    expected_size = int(item["size"])
    try:
        if os.stat(final).st_size != expected_size:
            return _result("changed", error="size changed before upload")
    except OSError as exc:
        return _result("failed", error=str(exc))

    headers = dict(item.get("headers") or {})
    headers["Content-Length"] = str(expected_size)
    last: dict[str, Any] | None = None
    for attempt in range(_MAX_ATTEMPTS):
        if attempt:
            time.sleep(_BACKOFF_S[min(attempt - 1, len(_BACKOFF_S) - 1)])
        try:
            with open(final, "rb") as fh:
                body = _BoundedBody(fh, expected_size)
                resp = _request("PUT", item["url"], timeout_s, body=body, headers=headers)
                try:
                    resp.read()
                except BaseException:
                    _drop_response_connection(resp)
                    raise
                # The store verifies the bytes it was framed to read, so a
                # file that grew during its own PUT is stored, and matches,
                # as the prefix it was scanned as. Only the sandbox can see
                # that the file is no longer those bytes.
                if body.overrun or _size_of(final) != expected_size:
                    return _result(
                        "changed", resp.status, "size changed during upload"
                    )
                return _result("ok", resp.status)
        except _HttpStatusError as exc:
            if exc.status == 400 and "BadDigest" in exc.body:
                return _result("changed", exc.status, "BadDigest")
            if exc.status == 403 and "SignatureDoesNotMatch" in exc.body:
                return _result("changed", exc.status, "SignatureDoesNotMatch")
            last = _result("failed", exc.status, f"HTTP {exc.status}")
            if exc.status < 500:
                return last
        except Exception as exc:
            if not _is_connection_error(exc):
                return _result("failed", error=f"{type(exc).__name__}: {exc}")
            last = _result("unreachable", error=f"{type(exc).__name__}: {exc}")
    return last or _result("failed", error="exhausted retries")


def push(spec: dict[str, Any]) -> dict[str, Any]:
    root = os.path.abspath(spec["root"])
    timeout_s = float(spec.get("timeout_s") or 300)
    items = spec.get("items") or []
    concurrency = max(1, int(spec.get("concurrency") or _PUSH_CONCURRENCY))
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for item, res in zip(items, pool.map(lambda i: _timed(_push_one, root, i, timeout_s), items)):
            results[item["sha256"]] = res
            # A pack chunk is a one-shot artifact, but only the store having it
            # makes the local copy expendable: an unreachable store sends the
            # server down the relay path, which reads the chunk back out of the
            # sandbox. What neither path took is left where it is, and the next
            # pack op's stale sweep removes it from a directory the scan
            # excludes, so it never becomes a user's file.
            if item.get("unlink") and res.get("status") == "ok":
                final = _resolve_under_root(root, item.get("path", ""))
                if final:
                    _unlink_quiet(final)
    return {"results": results, "handshakes": dict(_HANDSHAKES)}


# ---------------------------------------------------------------------------
# pull
# ---------------------------------------------------------------------------


def _download_to_temp(url: str, parent: str, timeout_s: float) -> tuple[str, str, int, int]:
    """GET ``url`` into a temp file beside the target; returns (temp, sha256, bytes, http)."""
    tmp = tempfile.NamedTemporaryFile(dir=parent, prefix=_TEMP_PREFIX, delete=False)
    try:
        h = hashlib.sha256()
        n = 0
        resp = _request("GET", url, timeout_s)
        try:
            for chunk in iter(lambda: resp.read(_CHUNK), b""):
                h.update(chunk)
                n += len(chunk)
                tmp.write(chunk)
        except BaseException:
            _drop_response_connection(resp)
            raise
        status = resp.status
        tmp.close()
        return tmp.name, h.hexdigest(), n, status
    except BaseException:
        tmp.close()
        _unlink_quiet(tmp.name)
        raise


def _unlink_quiet(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _populated_directory(final: str) -> bool:
    """True when a populated directory holds the path a file or symlink needs.

    An empty one yields, but only at placement (``_yield_empty_directory``):
    every fresh sandbox seeds ``data``, ``results`` and ``work`` before the
    restore runs, and a manifest that names one of them as a symlink or file
    must win over the seed, or it fails on every recreation and the next
    backup records the seed as the user's choice. Removing it before the
    bytes are verified would leave nothing at the path when the transfer
    fails, and nothing recreates a seed mid-session."""
    if not os.path.isdir(final) or os.path.islink(final):
        return False
    return bool(os.listdir(final))


def _yield_empty_directory(final: str) -> None:
    """Remove the empty directory at ``final`` right before the verified bytes land."""
    if os.path.isdir(final) and not os.path.islink(final):
        os.rmdir(final)


def _pull_file(root: str, item: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    final = _resolve_under_root(root, item.get("path", ""))
    if final is None:
        return _result("failed", error="path escapes root")
    if _populated_directory(final):
        return _result("failed", error="target is a populated directory")
    url = item.get("url")
    expected_sha = item.get("sha256")
    expected_size = item.get("size")
    if item.get("file"):
        return _place_staged(root, final, item)
    if not url:
        return _result("failed", error="missing url")

    last: dict[str, Any] | None = None
    for attempt in range(_MAX_ATTEMPTS):
        if attempt:
            time.sleep(_BACKOFF_S[min(attempt - 1, len(_BACKOFF_S) - 1)])
        try:
            tmp, digest, n, status = _download_to_temp(url, root, timeout_s)
        except _HttpStatusError as exc:
            last = _result("failed", exc.status, f"HTTP {exc.status}")
            if exc.status < 500:
                return last
            continue
        except Exception as exc:
            if not _is_connection_error(exc):
                return _result("failed", error=f"{type(exc).__name__}: {exc}")
            last = _result("unreachable", error=f"{type(exc).__name__}: {exc}")
            continue

        if (expected_sha is not None and digest != expected_sha) or (
            expected_size is not None and n != int(expected_size)
        ):
            _unlink_quiet(tmp)
            return _result("mismatch", status, f"got sha256={digest} bytes={n}")
        try:
            _yield_empty_directory(final)
            os.replace(tmp, final)
            mode = item.get("mode")
            if mode is not None:
                os.chmod(final, int(mode))
            mtime_ns = item.get("mtime_ns")
            if mtime_ns is not None:
                os.utime(final, ns=(int(mtime_ns), int(mtime_ns)))
        except OSError as exc:
            _unlink_quiet(tmp)
            return _result("failed", status, str(exc))
        return _result("ok", status)
    return last or _result("failed", error="exhausted retries")


def _sweep_orphan_staging(root: str, items: list[dict[str, Any]]) -> None:
    """Remove root-level transient entries no item of this op claims.

    A relay upload the file API cut short, or a download the runtime died
    in, leaves its partial bytes under the prefix, and the scan skips it
    there, so nothing else would ever see them. Restores are serialized per
    workspace, so any entry this op does not claim is a leftover of an
    earlier one.
    """
    claimed = {os.path.basename(i["file"]) for i in items if i.get("file")}
    for name in os.listdir(root):
        if not name.startswith(_TEMP_PREFIX) or name in claimed:
            continue
        path = os.path.join(root, name)
        if os.path.isdir(path) and not os.path.islink(path):
            _rmtree_quiet(path)
        else:
            _unlink_quiet(path)


def _place_staged(root: str, final: str, item: dict[str, Any]) -> dict[str, Any]:
    """Move a file the server uploaded to a staging name into place.

    The staged copy is verified against the manifest before it becomes the
    file: an upload cut short by a full disk would otherwise be the file's
    next content at the next backup. The staged copy is removed either way.
    """
    staged = _resolve_under_root(root, item["file"])
    if staged is None:
        return _result("failed", error="file escapes root")
    try:
        digest, _, n = _hash_file(staged)
    except OSError as exc:
        return _result("failed", error=str(exc))
    expected_sha, expected_size = item.get("sha256"), item.get("size")
    if (expected_sha is not None and digest != expected_sha) or (
        expected_size is not None and n != int(expected_size)
    ):
        _unlink_quiet(staged)
        return _result("mismatch", None, f"got sha256={digest} bytes={n}")
    try:
        _yield_empty_directory(final)
        os.replace(staged, final)
        mode = item.get("mode")
        if mode is not None:
            os.chmod(final, int(mode))
        mtime_ns = item.get("mtime_ns")
        if mtime_ns is not None:
            os.utime(final, ns=(int(mtime_ns), int(mtime_ns)))
    except OSError as exc:
        _unlink_quiet(staged)
        return _result("failed", error=str(exc))
    return _result("ok")


def _reopen_dir(path: str) -> None:
    """Give the owner write and search on a directory a previous restore closed.

    Directory modes are the last thing a restore applies, so a retry after a
    partial one finds its parents already read-only; step 4 closes them again.
    """
    st = os.stat(path)
    if st.st_mode & 0o300 != 0o300:
        os.chmod(path, stat.S_IMODE(st.st_mode) | 0o300)


def _pull_symlink(root: str, item: dict[str, Any]) -> dict[str, Any]:
    final = _resolve_under_root(root, item.get("path", ""))
    if final is None:
        return _result("failed", error="path escapes root")
    target = item.get("symlink_target")
    if not target:
        return _result("failed", error="missing symlink_target")
    try:
        if os.path.islink(final) or os.path.isfile(final):
            os.unlink(final)
        elif _populated_directory(final):
            return _result("failed", error="target is a populated directory")
        else:
            _yield_empty_directory(final)
        os.symlink(target, final)
    except OSError as exc:
        return _result("failed", error=str(exc))
    mtime_ns = item.get("mtime_ns")
    if mtime_ns is not None:
        try:
            os.utime(final, ns=(int(mtime_ns), int(mtime_ns)), follow_symlinks=False)
        except (NotImplementedError, OSError):
            pass
    return _result("ok")


def _extract_member(root: str, chunk: Any, member: dict[str, Any], http_status: int | None) -> dict[str, Any]:
    """Slice one member out of an open pack chunk and place it like a pulled file."""
    final = _resolve_under_root(root, member.get("path", ""))
    if final is None:
        return _result("failed", error="path escapes root")
    if _populated_directory(final):
        return _result("failed", error="target is a populated directory")
    size = int(member.get("size") or 0)
    try:
        os.makedirs(os.path.dirname(final), exist_ok=True)
        chunk.seek(int(member.get("offset") or 0))
        data = chunk.read(size)
    except OSError as exc:
        return _result("failed", http_status, str(exc))
    if len(data) != size:
        return _result("mismatch", http_status, f"short read from pack: got bytes={len(data)}")
    expected_sha = member.get("sha256")
    if expected_sha is not None:
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_sha:
            return _result("mismatch", http_status, f"got sha256={digest}")
    tmp = tempfile.NamedTemporaryFile(dir=root, prefix=_TEMP_PREFIX, delete=False)
    try:
        tmp.write(data)
        tmp.close()
        _yield_empty_directory(final)
        os.replace(tmp.name, final)
        mode = member.get("mode")
        if mode is not None:
            os.chmod(final, int(mode))
        mtime_ns = member.get("mtime_ns")
        if mtime_ns is not None:
            os.utime(final, ns=(int(mtime_ns), int(mtime_ns)))
    except OSError as exc:
        tmp.close()
        _unlink_quiet(tmp.name)
        return _result("failed", http_status, str(exc))
    return _result("ok", http_status)


def _pull_pack(root: str, item: dict[str, Any], timeout_s: float) -> dict[str, dict[str, Any]]:
    """Download one pack chunk, then slice every member out of it.

    Members fail together when the chunk cannot be fetched and one at a time
    when their own bytes do not verify; the caller only ever sees member paths.
    A chunk already in the sandbox (``file``, relative to the root) is
    verified and consumed in place: the relay path uploads it whole so the
    members still get this extractor's names, modes and mtimes.
    """
    members = item.get("members") or []
    url = item.get("url")
    expected_sha = item.get("sha256")

    def fail_all(res: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {m.get("path", ""): dict(res) for m in members}

    local = item.get("file")
    if local:
        tmp = _resolve_under_root(root, local)
        if tmp is None:
            return fail_all(_result("failed", error="file escapes root"))
        try:
            digest, _, n = _hash_file(tmp)
        except OSError as exc:
            return fail_all(_result("failed", error=str(exc)))
        if expected_sha is not None and digest != expected_sha:
            _unlink_quiet(tmp)
            return fail_all(_result("mismatch", None, f"got sha256={digest} bytes={n}"))
        return _extract_all(root, tmp, members, None)
    if not url:
        return fail_all(_result("failed", error="missing url"))
    # The chunk lands under the pack directory, which the scan excludes, so a
    # restore that dies mid-download can never leave a partial chunk where
    # the next backup would record it as a user's file.
    scratch = os.path.join(root, _PACK_DIR)
    try:
        os.makedirs(scratch, exist_ok=True)
    except OSError as exc:
        return fail_all(_result("failed", error=str(exc)))
    last: dict[str, Any] | None = None
    tmp = None
    status: int | None = None
    for attempt in range(_MAX_ATTEMPTS):
        if attempt:
            time.sleep(_BACKOFF_S[min(attempt - 1, len(_BACKOFF_S) - 1)])
        try:
            tmp, digest, n, status = _download_to_temp(url, scratch, timeout_s)
        except _HttpStatusError as exc:
            last = _result("failed", exc.status, f"HTTP {exc.status}")
            if exc.status < 500:
                return fail_all(last)
            continue
        except Exception as exc:
            if not _is_connection_error(exc):
                return fail_all(_result("failed", error=f"{type(exc).__name__}: {exc}"))
            last = _result("unreachable", error=f"{type(exc).__name__}: {exc}")
            continue
        if expected_sha is not None and digest != expected_sha:
            _unlink_quiet(tmp)
            return fail_all(_result("mismatch", status, f"got sha256={digest} bytes={n}"))
        break
    else:
        return fail_all(last or _result("failed", error="exhausted retries"))

    return _extract_all(root, tmp, members, status)


def _extract_all(
    root: str, chunk_path: str, members: list[dict[str, Any]], status: int | None
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    try:
        with open(chunk_path, "rb") as chunk:
            for member in members:
                out[member.get("path", "")] = _extract_member(root, chunk, member, status)
    finally:
        _unlink_quiet(chunk_path)
    return out


def _timed_pack(root: str, item: dict[str, Any], timeout_s: float) -> dict[str, dict[str, Any]]:
    t0 = time.monotonic()
    out = _pull_pack(root, item, timeout_s)
    ms = int((time.monotonic() - t0) * 1000)
    for res in out.values():
        res["ms"] = ms
    return out


def pull(spec: dict[str, Any]) -> dict[str, Any]:
    """Materialize items; with ``defer_dir_modes`` the directories stay open.

    The server sets that when more files follow in a later op, so directory
    modes and mtimes are applied exactly once, by the op that places the last
    file; a read-only directory closed early would reject its own children.
    """
    root = os.path.abspath(spec["root"])
    timeout_s = float(spec.get("timeout_s") or 300)
    items = spec.get("items") or []
    concurrency = max(1, int(spec.get("concurrency") or _PULL_CONCURRENCY))
    defer_dir_modes = bool(spec.get("defer_dir_modes"))
    results: dict[str, dict[str, Any]] = {}
    _sweep_orphan_staging(root, items)

    files: list[dict[str, Any]] = []
    packs: list[dict[str, Any]] = []
    symlinks: list[dict[str, Any]] = []
    dirs: list[tuple[dict[str, Any], str]] = []

    # Step 1: parents first, then dir items, so a file lands in a directory
    # that already exists. Dir modes wait for step 4: a read-only directory
    # restored read-only first would reject its own children.
    for item in items:
        if item.get("kind") == "pack":
            packs.append(item)
            continue
        path = item.get("path", "")
        final = _resolve_under_root(root, path)
        if final is None:
            results[path] = _result("failed", error="path escapes root")
            continue
        kind = item.get("kind", "file")
        try:
            os.makedirs(os.path.dirname(final), exist_ok=True)
            if kind == "dir":
                os.makedirs(final, exist_ok=True)
                _reopen_dir(final)
                dirs.append((item, final))
            elif kind == "symlink":
                symlinks.append(item)
            elif kind == "file":
                files.append(item)
            else:
                results[path] = _result("failed", error=f"unknown kind {kind!r}")
        except OSError as exc:
            results[path] = _result("failed", error=str(exc))

    # Step 2: packs and files, in parallel. Packs are submitted first: each
    # is one download that fans out into many members, so it is the tail.
    if files or packs:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            pack_futures = [pool.submit(_timed_pack, root, p, timeout_s) for p in packs]
            for item, res in zip(files, pool.map(lambda i: _timed(_pull_file, root, i, timeout_s), files)):
                results[item["path"]] = res
            for fut in pack_futures:
                results.update(fut.result())

    # Step 3: symlinks after files, so a link never shadows a file being written.
    for item in symlinks:
        results[item["path"]] = _pull_symlink(root, item)

    # Step 4: dir modes and mtimes last, deepest first; writing children
    # above would disturb the mtimes, and a parent's mode may forbid writes.
    for item, final in sorted(dirs, key=lambda d: d[1], reverse=True):
        if defer_dir_modes:
            results[item["path"]] = _result("ok")
            continue
        mode = item.get("mode")
        mtime_ns = item.get("mtime_ns")
        try:
            if mode is not None:
                os.chmod(final, int(mode))
            if mtime_ns is not None:
                os.utime(final, ns=(int(mtime_ns), int(mtime_ns)))
            results[item["path"]] = _result("ok")
        except OSError as exc:
            results[item["path"]] = _result("failed", error=str(exc))

    return {"results": results, "handshakes": dict(_HANDSHAKES)}


# ---------------------------------------------------------------------------
# pack
# ---------------------------------------------------------------------------


def pack(spec: dict[str, Any]) -> dict[str, Any]:
    """Concatenate ``members`` (path, sha256, size) into chunk files under ``out_dir``.

    Deterministic: members are taken in sorted path order and a chunk closes
    only at a member boundary once the next member would exceed ``max_bytes``,
    so the same member set always yields the same chunk bytes and digests. A
    member whose bytes no longer match what the scan reported is dropped and
    listed under ``changed`` rather than written wrong; the previous chunk
    files are wiped first so a stale one can never be pushed.
    """
    root = os.path.abspath(spec["root"])
    base = _resolve_under_root(root, spec.get("out_dir") or _PACK_DIR)
    if base is None:
        raise ValueError("out_dir escapes root")
    max_bytes = int(spec.get("max_bytes") or 32 * 1024 * 1024)
    members = sorted(spec.get("members") or [], key=lambda m: m["path"])

    # Two syncs of one workspace can overlap (a manual backup during a
    # scheduled one), so each op owns a directory of its own and only ever
    # removes what nothing can still be pushing: anything untouched for
    # longer than a transfer is allowed to take.
    os.makedirs(base, exist_ok=True)
    _sweep_stale(base, _PACK_STALE_S)
    out_dir = tempfile.mkdtemp(prefix="op-", dir=base)

    chunks: list[dict[str, Any]] = []
    changed: list[str] = []
    current: dict[str, Any] | None = None

    def open_chunk() -> dict[str, Any]:
        tmp = tempfile.NamedTemporaryFile(dir=out_dir, prefix="chunk-tmp-", delete=False)
        return {"file": tmp, "hash": hashlib.sha256(), "size": 0, "members": []}

    def close_chunk() -> None:
        # A chunk is opened only immediately before its first member is
        # written, so one that exists always has at least one.
        nonlocal current
        if current is None:
            return
        current["file"].close()
        digest = current["hash"].hexdigest()
        final = os.path.join(out_dir, f"chunk-{digest}")
        os.replace(current["file"].name, final)
        chunks.append(
            {
                "path": os.path.relpath(final, root),
                "sha256": digest,
                "size": current["size"],
                "members": current["members"],
            }
        )
        current = None

    for member in members:
        rel = member["path"]
        expected_size = int(member["size"])
        expected_sha = member.get("sha256")
        abs_path = _resolve_under_root(root, rel)
        if abs_path is None:
            changed.append(rel)
            continue
        # Small by contract, so the whole member is read before anything is
        # written: a member that fails to verify must leave no bytes behind.
        try:
            with open(abs_path, "rb") as f:
                data = f.read(expected_size + 1)
        except OSError:
            changed.append(rel)
            continue
        if len(data) != expected_size or (expected_sha is not None and hashlib.sha256(data).hexdigest() != expected_sha):
            changed.append(rel)
            continue
        if current is not None and current["members"] and current["size"] + expected_size > max_bytes:
            close_chunk()
        if current is None:
            current = open_chunk()
        current["file"].write(data)
        current["hash"].update(data)
        current["members"].append(
            {
                "path": rel,
                "offset": current["size"],
                "size": expected_size,
                "sha256": expected_sha or hashlib.sha256(data).hexdigest(),
            }
        )
        current["size"] += expected_size
    close_chunk()
    if not chunks:
        _rmtree_quiet(out_dir)
    return {"chunks": chunks, "changed": changed}


def _sweep_stale(base: str, max_age_s: float) -> None:
    cutoff = time.time() - max_age_s
    for name in os.listdir(base):
        path = os.path.join(base, name)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if st.st_mtime > cutoff:
            continue
        if stat.S_ISDIR(st.st_mode):
            _rmtree_quiet(path)
        else:
            _unlink_quiet(path)


def _rmtree_quiet(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


def unlink(spec: dict[str, Any]) -> dict[str, Any]:
    """Remove files under root; used to drop chunks the server relayed itself."""
    root = os.path.abspath(spec["root"])
    removed = 0
    for rel in spec.get("paths") or []:
        path = _resolve_under_root(root, rel)
        if path is None or not os.path.isfile(path):
            continue
        _unlink_quiet(path)
        removed += 1
    return {"removed": removed}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_OPS = {"scan": scan, "push": push, "pull": pull, "pack": pack, "unlink": unlink}


RESULT_MARKER = "WSFILES_RESULT "
_USAGE = "usage: wsfiles_transfer.py <scan|push|pull|pack|unlink> (--spec-b64 <base64 json> | <in.json>)\n"


def _load_spec(argv: list[str]) -> dict[str, Any]:
    if argv[2] == "--spec-b64":
        raw = base64.b64decode(argv[3])
    else:
        with open(argv[2], "rb") as f:
            raw = f.read()
        # The spec file is a one-shot exchange; nothing else reads it.
        _unlink_quiet(argv[2])
    spec = json.loads(raw.decode("utf-8"))
    if not isinstance(spec, dict) or not isinstance(spec.get("root"), str):
        raise ValueError("input must be a JSON object with a string 'root'")
    return spec


def _main(argv: list[str]) -> int:
    """Run one op and print its result as the last stdout line.

    The result travels back on stdout behind ``RESULT_MARKER`` so the caller
    needs no second round trip to collect it; the spec arrives inline when
    it fits an argument and as a file otherwise.
    """
    # The flag and its value are one unit, so a bare ``--spec-b64`` is a
    # truncated four-argument form rather than a valid three-argument one.
    inline = argv[2:3] == ["--spec-b64"]
    if len(argv) not in (3, 4) or argv[1] not in _OPS or inline != (len(argv) == 4):
        sys.stderr.write(_USAGE)
        return 2
    try:
        spec = _load_spec(argv)
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"invalid input: {exc}\n")
        return 2
    try:
        out = _OPS[argv[1]](spec)
    except Exception as exc:
        out = {"error": f"{type(exc).__name__}: {exc}"}
    sys.stdout.write("\n" + RESULT_MARKER + json.dumps(out, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
