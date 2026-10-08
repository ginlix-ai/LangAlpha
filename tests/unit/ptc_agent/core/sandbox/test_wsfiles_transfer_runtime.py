"""Exercise the sandbox-side transfer runtime against a loopback fake bucket.

The runtime is stdlib-only and uploaded verbatim into sandboxes, so it is
imported as a plain module here. Network tests carry ``enable_socket`` for
the unit-suite tripwire and only ever talk to 127.0.0.1.
"""

import base64
import errno
import hashlib
import os
import socket
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ptc_agent.core.sandbox import wsfiles_transfer_runtime as rt


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _b64sha(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


# ---------------------------------------------------------------------------
# fake bucket
# ---------------------------------------------------------------------------


class _Bucket(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.objects: dict[str, bytes] = {}
        self.fail_500: set[str] = set()
        self.redirects: set[str] = set()
        self.hits: dict[str, int] = {}


class _Handler(BaseHTTPRequestHandler):
    server: _Bucket

    def log_message(self, *_):
        pass

    def _count(self):
        self.server.hits[self.path] = self.server.hits.get(self.path, 0) + 1

    def _redirect(self) -> bool:
        if self.path not in self.server.redirects:
            return False
        self.send_response(307)
        self.send_header("Location", f"{self.server.base}/elsewhere{self.path}")
        self.send_header("Content-Length", "0")
        self.end_headers()
        return True

    def do_PUT(self):
        self._count()
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n)
        if self._redirect():
            return
        want = self.headers.get("x-amz-checksum-sha256")
        if want is not None and want != _b64sha(body):
            payload = b"<Error><Code>BadDigest</Code></Error>"
            self.send_response(400)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.server.objects[self.path] = body
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._count()
        if self._redirect():
            return
        if self.path in self.server.fail_500:
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = self.server.objects.get(self.path)
        if body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def bucket():
    srv = _Bucket(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    srv.base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(rt, "_BACKOFF_S", (0.0, 0.0))


def _closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------
# path resolution
# ---------------------------------------------------------------------------


def test_resolve_under_root_accepts_a_name_that_merely_starts_with_dots():
    """Only a whole ".." component escapes. "..notes" is an ordinary file name,
    and rejecting it silently dropped such files from every transfer."""
    assert rt._resolve_under_root("/root", "..notes") == "/root/..notes"
    assert rt._resolve_under_root("/root", "d/..hidden.txt") == "/root/d/..hidden.txt"
    assert rt._resolve_under_root("/root", "...") == "/root/..."


def test_resolve_under_root_still_rejects_traversal_and_absolutes():
    for rel in ("../x", "..", "a/../../x", "/etc/passwd", "", "."):
        assert rt._resolve_under_root("/root", rel) is None


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


def _write(root, rel: str, data: bytes = b"x") -> str:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return str(p)


def _sans_ms(results):
    return {k: {f: v for f, v in r.items() if f != "ms"} for k, r in results.items()}


def _scan(root, **over):
    spec = {
        "root": str(root),
        "exclude_dir_names": ["node_modules", ".git", "_internal"],
        "exclude_rel_dirs": [".agents/threads", ".agents/skills/.staging"],
        "exclude_rel_dir_prefixes": [".agents/skills/.trash-"],
        "exclude_rel_files": [".agents/skills/.skills-sync.flock"],
        "exclude_basenames": [".DS_Store", "__init__.py"],
        "exclude_suffixes": [".pyc"],
        "max_file_bytes": 100,
        "prior": {},
    }
    spec.update(over)
    return rt.scan(spec)


def test_scan_reserves_a_file_by_relative_path_not_by_name(tmp_path):
    _write(tmp_path, ".agents/skills/.skills-sync.flock", b"")
    _write(tmp_path, "results/.skills-sync.flock", b"mine")
    os.makedirs(tmp_path / "x")
    os.symlink("../results/.skills-sync.flock", tmp_path / "x/.skills-sync.flock")
    paths = {e["path"] for e in _scan(tmp_path)["entries"]}
    assert ".agents/skills/.skills-sync.flock" not in paths
    assert "results/.skills-sync.flock" in paths
    assert "x/.skills-sync.flock" in paths


def test_scan_exclusions_and_pruning(tmp_path):
    _write(tmp_path, "a.txt", b"hello")
    _write(tmp_path, "node_modules/x.js")
    _write(tmp_path, "deep/node_modules/y.js")
    _write(tmp_path, ".agents/threads/t.md")
    _write(tmp_path, ".agents/user/u.md")
    _write(tmp_path, "pkg/__init__.py")
    _write(tmp_path, "pkg/mod.pyc")
    _write(tmp_path, "pkg/mod.py")
    _write(tmp_path, ".DS_Store")
    _write(tmp_path, ".agents/skills/.trash-123/junk")
    _write(tmp_path, ".agents/skills/.staging/junk")
    # The reconciler's names are reserved under its own directory only.
    _write(tmp_path, "pkg/.staging/mine.txt")
    _write(tmp_path, "pkg/.trash-abc/mine.txt")
    os.symlink("a.txt", tmp_path / "node_modules_link")

    out = _scan(tmp_path)
    paths = [e["path"] for e in out["entries"]]
    # Pruned trees leave no trace beneath their parent; every kept dir has a row.
    assert paths == [
        ".agents", ".agents/skills", ".agents/user", ".agents/user/u.md", "a.txt", "deep",
        "node_modules_link", "pkg", "pkg/.staging", "pkg/.staging/mine.txt",
        "pkg/.trash-abc", "pkg/.trash-abc/mine.txt", "pkg/mod.py",
    ]
    assert paths == sorted(paths)
    assert next(e for e in out["entries"] if e["path"] == "deep")["kind"] == "dir"
    a = next(e for e in out["entries"] if e["path"] == "a.txt")
    assert a["kind"] == "file" and a["size"] == 5 and a["sha256"] == _sha(b"hello")
    assert a["is_binary"] is False
    assert a["mode"] == stat.S_IMODE(os.stat(tmp_path / "a.txt").st_mode)
    assert out["errors"] == [] and out["oversized"] == []


def test_scan_symlink_named_like_excluded_dir_is_skipped(tmp_path):
    _write(tmp_path, "a.txt")
    os.symlink("a.txt", tmp_path / ".git")
    os.symlink("a.txt", tmp_path / ".DS_Store")
    out = _scan(tmp_path)
    assert [e["path"] for e in out["entries"]] == ["a.txt"]


def test_scan_prior_reuse_vs_rehash(tmp_path):
    p = _write(tmp_path, "f.bin", b"abc")
    st = os.stat(p)
    prior = {"f.bin": [st.st_size, st.st_mtime_ns, "deadbeef"]}
    out = _scan(tmp_path, prior=prior)
    assert out["reused"] == 1 and out["hashed"] == 0
    assert out["entries"][0]["sha256"] == "deadbeef"
    assert out["entries"][0]["is_binary"] is None
    assert out["entries"][0]["mtime_ns"] == st.st_mtime_ns

    # The prior is stored at microsecond precision: same microsecond reuses.
    us_truncated = {"f.bin": [st.st_size, (st.st_mtime_ns // 1000) * 1000, "deadbeef"]}
    out = _scan(tmp_path, prior=us_truncated)
    assert out["reused"] == 1 and out["hashed"] == 0

    stale = {"f.bin": [st.st_size, st.st_mtime_ns + 1000, "deadbeef"]}
    out = _scan(tmp_path, prior=stale)
    assert out["reused"] == 0 and out["hashed"] == 1
    assert out["entries"][0]["sha256"] == _sha(b"abc")
    assert out["entries"][0]["is_binary"] is False

    wrong_size = {"f.bin": [st.st_size + 1, st.st_mtime_ns, "deadbeef"]}
    out = _scan(tmp_path, prior=wrong_size)
    assert out["reused"] == 0 and out["hashed"] == 1



def test_scan_without_hashing_reads_no_file(tmp_path, monkeypatch):
    _write(tmp_path, "big.bin", b"abc")
    monkeypatch.setattr(rt, "_hash_file", lambda _p: pytest.fail("a listing hashed a file"))
    out = rt.scan({"root": str(tmp_path), "hash": False})
    entry = out["entries"][0]
    assert (entry["path"], entry["size"], entry["sha256"]) == ("big.bin", 3, None)
    assert out["hashed"] == 0

def test_scan_size_describes_the_hashed_bytes(tmp_path, monkeypatch):
    """A file that grows between the stat and the hash is reported with the
    length the digest covers; the stale stat size would fail every later
    length check against the same digest."""
    path = _write(tmp_path, "grows.txt", b"short")
    real = rt._hash_file

    def grow_then_hash(p):
        with open(p, "ab") as f:
            f.write(b" and then longer")
        return real(p)

    monkeypatch.setattr(rt, "_hash_file", grow_then_hash)
    (entry,) = _scan(tmp_path)["entries"]
    assert entry["path"] == "grows.txt"
    assert entry["size"] == len(b"short and then longer")
    assert entry["sha256"] == _sha(b"short and then longer")
    assert os.path.getsize(path) == entry["size"]


def test_scan_is_binary_classification(tmp_path):
    """``late_nul.bin`` is the one that matters: a NUL past any sampling window.

    Text rows are stored in a column that cannot hold a NUL, so calling this
    file text writes bytes that no longer hash to the row's own digest, and a
    restore that verifies that digest then refuses to place it for good."""
    _write(tmp_path, "text.md", "plain text, with ünïcödé\n".encode())
    _write(tmp_path, "nul.bin", b"looks like text" + b"\0" + b"until here")
    _write(tmp_path, "late_nul.bin", b"a" * 70000 + b"\0")
    _write(tmp_path, "latin1.txt", b"caf\xe9")
    _write(tmp_path, "empty", b"")
    out = _scan(tmp_path, max_file_bytes=1_000_000)
    by = {e["path"]: e["is_binary"] for e in out["entries"]}
    assert by == {
        "text.md": False,
        "nul.bin": True,
        "late_nul.bin": True,
        "latin1.txt": True,
        "empty": False,
    }


def test_scan_multibyte_char_split_at_sniff_window_is_text(tmp_path):
    _write(tmp_path, "split.txt", b"a" * 8191 + "\u00e9tail\n".encode())
    _write(tmp_path, "truncated.txt", b"a" * 8191 + b"\xc3")
    out = _scan(tmp_path, max_file_bytes=10_000)
    by = {e["path"]: e for e in out["entries"]}
    assert by["split.txt"]["is_binary"] is False
    assert by["truncated.txt"]["is_binary"] is True


def test_scan_emits_every_dir_with_its_mode(tmp_path):
    (tmp_path / "empty").mkdir()
    (tmp_path / "outer/inner").mkdir(parents=True)
    (tmp_path / "has_file").mkdir()
    _write(tmp_path, "has_file/x")
    (tmp_path / "only_excluded").mkdir()
    _write(tmp_path, "only_excluded/.DS_Store")
    os.chmod(tmp_path / "has_file", 0o700)
    out = _scan(tmp_path)
    by = {e["path"]: e for e in out["entries"]}
    assert set(by) == {"empty", "outer", "outer/inner", "has_file", "has_file/x", "only_excluded"}
    assert by["empty"]["kind"] == "dir" and by["empty"]["sha256"] is None
    assert by["empty"]["mode"] == stat.S_IMODE(os.stat(tmp_path / "empty").st_mode)
    assert by["has_file"]["kind"] == "dir" and by["has_file"]["mode"] == 0o700
    assert [e["path"] for e in out["entries"]] == sorted(by)


def test_pull_applies_dir_mode_after_children(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    out = rt.pull(
        {
            "root": str(root),
            "items": [
                {"path": "ro", "kind": "dir", "mode": 0o555, "mtime_ns": 1_000_000_000},
                {"path": "ro/inner", "kind": "dir", "mode": 0o555, "mtime_ns": 1_000_000_000},
                {"path": "ro/inner/f.txt", "kind": "file", "url": None, "size": 0, "sha256": None,
                 "mode": 0o444, "mtime_ns": 1_000_000_000},
            ],
        },
    )
    assert out["results"]["ro"]["status"] == "ok"
    assert out["results"]["ro/inner"]["status"] == "ok"
    assert stat.S_IMODE(os.stat(root / "ro").st_mode) == 0o555
    assert stat.S_IMODE(os.stat(root / "ro/inner").st_mode) == 0o555
    assert os.stat(root / "ro").st_mtime_ns == 1_000_000_000
    os.chmod(root / "ro", 0o755)
    os.chmod(root / "ro/inner", 0o755)


def test_scan_records_symlinks_without_following(tmp_path):
    _write(tmp_path, "real.txt", b"data")
    os.symlink("real.txt", tmp_path / "link")
    os.symlink("/nowhere/at/all", tmp_path / "dangling")
    (tmp_path / "d").mkdir()
    os.symlink("..", tmp_path / "d/loop")
    out = _scan(tmp_path)
    by = {e["path"]: e for e in out["entries"]}
    assert by["link"] == {
        "path": "link",
        "kind": "symlink",
        "size": 0,
        "mtime_ns": os.lstat(tmp_path / "link").st_mtime_ns,
        "mode": 0,
        "sha256": None,
        "symlink_target": "real.txt",
    }
    assert by["dangling"]["symlink_target"] == "/nowhere/at/all"
    assert by["d/loop"]["symlink_target"] == ".."
    assert set(by) == {"real.txt", "link", "dangling", "d", "d/loop"}
    assert out["errors"] == []


def test_scan_oversized_reported_not_emitted(tmp_path):
    _write(tmp_path, "big.bin", b"z" * 101)
    _write(tmp_path, "ok.bin", b"z" * 100)
    out = _scan(tmp_path)
    assert [e["path"] for e in out["entries"]] == ["ok.bin"]
    assert out["oversized"] == [{"path": "big.bin", "size": 101}]
    assert out["hashed"] == 1


def test_scan_unreadable_file_goes_to_errors(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("root reads anything")
    p = _write(tmp_path, "secret", b"s")
    os.chmod(p, 0)
    try:
        out = _scan(tmp_path)
    finally:
        os.chmod(p, 0o600)
    assert out["entries"] == []
    assert out["errors"] and out["errors"][0]["path"] == "secret"


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------


def _push_item(bucket, path: str, data: bytes, key: str | None = None, size: int | None = None):
    return {
        "path": path,
        "sha256": _sha(data),
        "size": len(data) if size is None else size,
        "url": f"{bucket.base}/{key or _sha(data)}",
        "headers": {"x-amz-checksum-sha256": _b64sha(data), "Content-Type": "application/octet-stream"},
    }


@pytest.mark.enable_socket
def test_push_ok_streams_bytes(tmp_path, bucket):
    data = os.urandom(3 * 1024 * 1024 + 17)
    _write(tmp_path, "dir/a.bin", data)
    item = _push_item(bucket, "dir/a.bin", data)
    out = rt.push({"root": str(tmp_path), "concurrency": 2, "timeout_s": 10, "items": [item]})
    assert _sans_ms(out["results"]) == {_sha(data): {"status": "ok", "http": 200, "error": None}}
    assert bucket.objects[f"/{_sha(data)}"] == data


@pytest.mark.enable_socket
def test_push_redirect_is_a_failure_that_keeps_the_chunk(tmp_path, bucket):
    """A 3xx on a presigned PUT stored nothing; reporting it as ok would register
    a digest with no object behind it and unlink the only copy of the chunk."""
    data = b"chunk bytes"
    _write(tmp_path, "_internal/packs/c", data)
    item = _push_item(bucket, "_internal/packs/c", data)
    item["unlink"] = True
    bucket.redirects.add(f"/{_sha(data)}")
    out = rt.push({"root": str(tmp_path), "timeout_s": 10, "items": [item]})
    assert _sans_ms(out["results"]) == {_sha(data): {"status": "failed", "http": 307, "error": "HTTP 307"}}
    assert f"/{_sha(data)}" not in bucket.objects
    assert bucket.hits[f"/{_sha(data)}"] == 1
    assert (tmp_path / "_internal/packs/c").read_bytes() == data

    get = _file_item(bucket, "g.bin", b"served")
    bucket.redirects.add(get["url"][len(bucket.base):])
    out = rt.pull({"root": str(tmp_path), "timeout_s": 10, "items": [get]})
    assert _sans_ms(out["results"]) == {"g.bin": {"status": "failed", "http": 307, "error": "HTTP 307"}}
    assert not (tmp_path / "g.bin").exists()


@pytest.mark.enable_socket
def test_push_bad_digest_maps_to_changed(tmp_path, bucket):
    signed = b"original"
    _write(tmp_path, "a.bin", b"modified")
    item = _push_item(bucket, "a.bin", signed)
    out = rt.push({"root": str(tmp_path), "items": [item]})
    res = out["results"][_sha(signed)]
    assert res["status"] == "changed" and res["http"] == 400
    assert bucket.hits[f"/{_sha(signed)}"] == 1, "4xx must not retry"


def test_push_size_changed_before_upload_skips_http(tmp_path):
    data = b"abc"
    _write(tmp_path, "a.bin", b"abcd")
    item = _push_item(bucket=type("B", (), {"base": "http://127.0.0.1:9"})(), path="a.bin", data=data)
    out = rt.push({"root": str(tmp_path), "items": [item]})
    assert out["results"][_sha(data)]["status"] == "changed"



def _parts(bucket, data: bytes, part_size: int):
    return [
        {"part_number": i + 1, "offset": off, "size": min(part_size, len(data) - off),
         "url": f"{bucket.base}/part-{i + 1}"}
        for i, off in enumerate(range(0, len(data), part_size))
    ]


@pytest.mark.enable_socket
def test_multipart_digest_counts_a_retried_part_once(tmp_path, bucket, monkeypatch):
    """The server assembles only when ``sent_sha256`` names the object's digest,
    so a part retried after sending half its bytes must not hash them twice."""
    data = os.urandom(300_000)
    final = _write(tmp_path, "big.bin", data)
    real_request = rt._request
    failed = []

    def _drop_part_two_once(method, url, timeout_s, body=None, headers=None):
        if url.endswith("/part-2") and not failed:
            failed.append(True)
            body.read(4096)
            raise ConnectionResetError("dropped mid-part")
        return real_request(method, url, timeout_s, body=body, headers=headers)

    monkeypatch.setattr(rt, "_request", _drop_part_two_once)
    out = rt._push_parts(final, _parts(bucket, data, 100_000), len(data), 10, _sha(data))
    assert out["status"] == "ok" and failed
    assert out["sent_sha256"] == _sha(data)
    assert b"".join(bucket.objects[f"/part-{n}"] for n in (1, 2, 3)) == data


@pytest.mark.enable_socket
def test_multipart_same_size_rewrite_is_changed(tmp_path, bucket):
    """Parts are signed for length only; the store would take these bytes."""
    scanned = os.urandom(300_000)
    final = _write(tmp_path, "big.bin", os.urandom(len(scanned)))
    out = rt._push_parts(final, _parts(bucket, scanned, 100_000), len(scanned), 10, _sha(scanned))
    assert out["status"] == "changed"
    assert "sent_sha256" not in out

def test_bounded_body_stops_at_the_declared_length(tmp_path):
    """http.client streams a file to EOF whatever Content-Length says."""
    p = tmp_path / "a.bin"
    p.write_bytes(b"0123456789")
    with open(p, "rb") as fh:
        body = rt._BoundedBody(fh, 4)
        assert b"".join(iter(lambda: body.read(3), b"")) == b"0123"
    with open(p, "rb") as fh:
        body = rt._BoundedBody(fh, 10)
        assert b"".join(iter(lambda: body.read(4), b"")) == b"0123456789"


@pytest.mark.enable_socket
def test_a_file_that_grows_during_its_own_put_is_reported_changed(tmp_path, bucket, monkeypatch):
    """The store verifies the bytes it was framed to read, so the prefix
    matches the signed digest and it answers 200. Only the sandbox can see
    that the file is no longer the bytes the manifest would claim, and a row
    written from them would be the file's whole content after a strict stop."""
    data = b"scanned bytes\n" * 100
    _write(tmp_path, "a.bin", data)
    item = _push_item(bucket, "a.bin", data)

    real_request = rt._request

    def _grow_then_request(*args, **kwargs):
        with open(tmp_path / "a.bin", "ab") as f:
            f.write(b"appended tail")
        return real_request(*args, **kwargs)

    monkeypatch.setattr(rt, "_request", _grow_then_request)

    out = rt.push({"root": str(tmp_path), "timeout_s": 10, "items": [item]})
    res = out["results"][_sha(data)]
    assert res["status"] == "changed" and res["http"] == 200
    # The tail never reached the wire, so the object is the scanned prefix and
    # the pooled connection is not left holding bytes the next request would
    # read as its own status line.
    assert bucket.objects[f"/{_sha(data)}"] == data


@pytest.mark.enable_socket
def test_a_file_that_shrinks_during_its_own_put_is_changed_not_unreachable(tmp_path, bucket, monkeypatch):
    """A short body would leave the store waiting out the timeout, read as an
    unreachable link, and send the next sync down the relay for nothing."""
    data = b"scanned bytes\n" * 100
    _write(tmp_path, "a.bin", data)
    item = _push_item(bucket, "a.bin", data)
    real_request = rt._request

    def _truncate_then_request(*args, **kwargs):
        with open(tmp_path / "a.bin", "r+b") as f:
            f.truncate(10)
        return real_request(*args, **kwargs)

    monkeypatch.setattr(rt, "_request", _truncate_then_request)
    out = rt.push({"root": str(tmp_path), "timeout_s": 10, "items": [item]})
    res = out["results"][_sha(data)]
    assert res["status"] == "changed" and "shrank" in res["error"]
    assert f"/{_sha(data)}" not in bucket.objects


@pytest.mark.enable_socket
def test_push_unreachable_after_retries(tmp_path):
    data = b"abc"
    _write(tmp_path, "a.bin", data)
    fake = type("B", (), {"base": f"http://127.0.0.1:{_closed_port()}"})()
    item = _push_item(fake, "a.bin", data)
    out = rt.push({"root": str(tmp_path), "items": [item], "timeout_s": 2})
    res = out["results"][_sha(data)]
    assert res["status"] == "unreachable" and res["http"] is None and res["error"]


def test_push_rejects_path_escape(tmp_path):
    item = {"path": "../x", "sha256": "0" * 64, "size": 1, "url": "http://127.0.0.1:9/x", "headers": {}}
    out = rt.push({"root": str(tmp_path), "items": [item]})
    assert out["results"]["0" * 64]["status"] == "failed"


# ---------------------------------------------------------------------------
# pull
# ---------------------------------------------------------------------------


def _file_item(bucket, path: str, data: bytes, mode: int = 0o640, mtime_ns: int = 1_700_000_000_123_456_789, **over):
    key = f"/{_sha(data)}"
    bucket.objects[key] = data
    item = {
        "path": path,
        "kind": "file",
        "sha256": _sha(data),
        "size": len(data),
        "url": f"{bucket.base}{key}",
        "mode": mode,
        "mtime_ns": mtime_ns,
        "symlink_target": None,
    }
    item.update(over)
    return item


@pytest.mark.enable_socket
def test_pull_ok_applies_mode_and_mtime(tmp_path, bucket):
    data = os.urandom(2 * 1024 * 1024 + 5)
    item = _file_item(bucket, "nested/deep/a.bin", data, mode=0o600, mtime_ns=1_600_000_000_000_000_000)
    out = rt.pull({"root": str(tmp_path), "concurrency": 4, "timeout_s": 10, "items": [item]})
    assert _sans_ms(out["results"]) == {"nested/deep/a.bin": {"status": "ok", "http": 200, "error": None}}
    final = tmp_path / "nested/deep/a.bin"
    assert final.read_bytes() == data
    st = os.stat(final)
    assert stat.S_IMODE(st.st_mode) == 0o600
    assert st.st_mtime_ns == 1_600_000_000_000_000_000
    assert not [p for p in os.listdir(final.parent) if p.startswith(".wsfiles-")]


@pytest.mark.enable_socket
def test_pull_onto_the_same_bytes_fetches_nothing(tmp_path, bucket):
    data = os.urandom(64 * 1024)
    _write(tmp_path, "kept.bin", data)
    _write(tmp_path, "stale.bin", os.urandom(len(data)))
    kept = _file_item(bucket, "kept.bin", data, mode=0o600, mtime_ns=1_600_000_000_000_000_000)
    stale_bytes = os.urandom(len(data))
    stale = _file_item(bucket, "stale.bin", stale_bytes)
    out = rt.pull({"root": str(tmp_path), "timeout_s": 10, "items": [kept, stale]})
    assert out["results"]["kept.bin"]["status"] == "ok"
    assert out["results"]["stale.bin"]["status"] == "ok"
    assert bucket.hits.get(f"/{_sha(data)}") is None
    assert bucket.hits[f"/{_sha(stale_bytes)}"] == 1
    st = os.stat(tmp_path / "kept.bin")
    assert stat.S_IMODE(st.st_mode) == 0o600
    assert st.st_mtime_ns == 1_600_000_000_000_000_000
    assert (tmp_path / "stale.bin").read_bytes() == stale_bytes


@pytest.mark.enable_socket
def test_pull_mismatch_leaves_no_file(tmp_path, bucket):
    item = _file_item(bucket, "a.bin", b"served", sha256=_sha(b"expected"))
    out = rt.pull({"root": str(tmp_path), "items": [item]})
    assert out["results"]["a.bin"]["status"] == "mismatch"
    assert not (tmp_path / "a.bin").exists()
    assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]


@pytest.mark.enable_socket
def test_pull_404_failed_no_retry_and_500_retries(tmp_path, bucket):
    missing = _file_item(bucket, "m.bin", b"m")
    del bucket.objects[f"/{_sha(b'm')}"]
    broken = _file_item(bucket, "b.bin", b"b")
    bucket.fail_500.add(f"/{_sha(b'b')}")
    out = rt.pull({"root": str(tmp_path), "items": [missing, broken]})
    assert _sans_ms(out["results"])["m.bin"] == {"status": "failed", "http": 404, "error": "HTTP 404"}
    assert bucket.hits[f"/{_sha(b'm')}"] == 1
    assert out["results"]["b.bin"]["status"] == "failed" and out["results"]["b.bin"]["http"] == 500
    assert bucket.hits[f"/{_sha(b'b')}"] == 3


@pytest.mark.enable_socket
def test_pull_unreachable(tmp_path):
    item = {
        "path": "a.bin",
        "kind": "file",
        "sha256": "0" * 64,
        "size": 1,
        "url": f"http://127.0.0.1:{_closed_port()}/x",
        "mode": 0o644,
        "mtime_ns": 0,
        "symlink_target": None,
    }
    out = rt.pull({"root": str(tmp_path), "items": [item], "timeout_s": 2})
    assert out["results"]["a.bin"]["status"] == "unreachable"


def test_pull_symlink_replaces_file_link_or_empty_dir_but_not_a_populated_one(tmp_path):
    """A fresh sandbox seeds ``data``, ``results`` and ``work`` before the
    restore runs; a manifest that names one as a symlink has to win over the
    empty seed, or the restore fails on every recreation and the next backup
    records the seed in the symlink's place."""
    _write(tmp_path, "was_file", b"f")
    os.symlink("old", tmp_path / "was_link")
    (tmp_path / "was_dir").mkdir()
    (tmp_path / "full_dir").mkdir()
    _write(tmp_path, "full_dir/child", b"c")
    items = [
        {"path": p, "kind": "symlink", "sha256": None, "size": 0, "url": None, "mode": 0, "mtime_ns": 1_500_000_000_000_000_000, "symlink_target": "target"}
        for p in ("was_file", "was_link", "was_dir", "full_dir", "fresh/new")
    ]
    out = rt.pull({"root": str(tmp_path), "items": items})
    r = out["results"]
    assert r["was_file"]["status"] == "ok" and os.readlink(tmp_path / "was_file") == "target"
    assert r["was_link"]["status"] == "ok" and os.readlink(tmp_path / "was_link") == "target"
    assert r["fresh/new"]["status"] == "ok" and os.readlink(tmp_path / "fresh/new") == "target"
    assert r["was_dir"]["status"] == "ok" and os.readlink(tmp_path / "was_dir") == "target"
    assert r["full_dir"]["status"] == "failed" and (tmp_path / "full_dir/child").is_file()


def test_pull_symlink_keep_existing_leaves_whatever_is_there(tmp_path):
    """A deferred link is placed only where nothing is: the turn may have
    written that path since the look, as a file or a link of its own."""
    _write(tmp_path, "was_file", b"f")
    os.symlink("mine", tmp_path / "was_link")
    items = [
        {"path": p, "kind": "symlink", "sha256": None, "size": 0, "url": None, "mode": 0, "mtime_ns": 1_500_000_000_000_000_000, "symlink_target": "target", "keep_existing": True}
        for p in ("was_file", "was_link", "fresh")
    ]
    out = rt.pull({"root": str(tmp_path), "items": items})
    r = out["results"]
    assert all(r[p]["status"] == "ok" for p in ("was_file", "was_link", "fresh"))
    assert (tmp_path / "was_file").read_bytes() == b"f"
    assert os.readlink(tmp_path / "was_link") == "mine"
    assert os.readlink(tmp_path / "fresh") == "target"


def test_pull_dir_keep_existing_leaves_a_standing_dir_as_it_is(tmp_path):
    """A deferred dir the turn made since the look keeps its own mode and
    mtime, even when another item's parent is made first; a missing one gets
    the backup's, open to its owner."""
    os.makedirs(tmp_path / "mine")
    os.chmod(tmp_path / "mine", 0o755)
    before = os.stat(tmp_path / "mine").st_mtime_ns
    items = [
        {"path": p, "kind": "dir", "sha256": None, "size": 0, "url": None, "mode": 0o500, "mtime_ns": 1_400_000_000_000_000_000, "symlink_target": None, "keep_existing": True}
        for p in ("fresh/inner", "mine", "fresh")
    ]
    out = rt.pull({"root": str(tmp_path), "items": items})
    assert all(r["status"] == "ok" for r in out["results"].values())
    mine = os.stat(tmp_path / "mine")
    assert stat.S_IMODE(mine.st_mode) == 0o755 and mine.st_mtime_ns == before
    fresh = os.stat(tmp_path / "fresh")
    assert stat.S_IMODE(fresh.st_mode) == 0o700
    assert fresh.st_mtime_ns == 1_400_000_000_000_000_000


def test_pull_dir_keep_existing_leaves_what_the_turn_puts_there_during_the_op(tmp_path, monkeypatch):
    """The turn can make the directory, or a file of the same name, between
    any look and the restore's own mkdir: either is the turn's, as a file
    written there keeps its bytes."""
    mkdir, mine = os.mkdir, str(tmp_path / "mine")

    def turn_makes_it_first(path, *args, **kwargs):
        if path == mine and not os.path.lexists(mine):
            mkdir(mine)
            os.chmod(mine, 0o700)
            os.utime(mine, ns=(1, 1))
        return mkdir(path, *args, **kwargs)

    monkeypatch.setattr(rt.os, "mkdir", turn_makes_it_first)
    _write(tmp_path, "was_file", b"f")
    items = [
        {"path": p, "kind": "dir", "mode": 0o755, "mtime_ns": 1_400_000_000_000_000_000, "keep_existing": True}
        for p in ("mine", "was_file")
    ]
    out = rt.pull({"root": str(tmp_path), "items": items})
    assert all(r["status"] == "ok" and not r.get("made") for r in out["results"].values())
    st = os.stat(mine)
    assert stat.S_IMODE(st.st_mode) == 0o700 and st.st_mtime_ns == 1
    assert (tmp_path / "was_file").read_bytes() == b"f"


def test_pull_a_deferred_dir_an_earlier_op_made_is_stamped_after_its_files(tmp_path):
    """A relayed batch makes its dirs in one op and places their files in the
    next; by then a deferred dir is standing, so only the mark the first op
    reported tells the second it is the restore's to stamp."""
    item = {"path": "d", "kind": "dir", "mode": 0o750, "mtime_ns": 1_400_000_000_000_000_000, "keep_existing": True}
    first = rt.pull({"root": str(tmp_path), "items": [item], "defer_dir_modes": True})
    assert first["results"]["d"] == {"status": "ok", "http": None, "error": None, "made": True}
    child = _staged(tmp_path, "d/f.md", b"f", keep_existing=True)
    out = rt.pull({"root": str(tmp_path), "items": [child, dict(item, made=True)]})
    assert out["results"]["d"]["status"] == "ok" and out["results"]["d/f.md"]["status"] == "ok"
    st = os.stat(tmp_path / "d")
    assert stat.S_IMODE(st.st_mode) == 0o750 | 0o300
    assert st.st_mtime_ns == 1_400_000_000_000_000_000


def test_pull_never_makes_again_a_dir_an_earlier_op_made_and_the_turn_removed(tmp_path):
    item = {"path": "gone/d", "kind": "dir", "mode": 0o750, "mtime_ns": 1, "keep_existing": True, "made": True}
    out = rt.pull({"root": str(tmp_path), "items": [item]})
    assert out["results"]["gone/d"]["status"] == "ok"
    assert not (tmp_path / "gone").exists()


def test_pull_a_deferred_dir_takes_what_later_batches_place_beneath_it(tmp_path):
    """The deferred pass places a dir in its first batch and the files under
    it in later ones, beside a turn that writes there too: a backup's
    read-only mode would refuse them all."""
    ro = {"path": "ro", "kind": "dir", "mode": 0o500, "mtime_ns": 1, "keep_existing": True}
    rt.pull({"root": str(tmp_path), "items": [ro]})
    later = _staged(tmp_path, "ro/late.md", b"late", keep_existing=True)
    out = rt.pull({"root": str(tmp_path), "items": [later]})
    assert out["results"]["ro/late.md"]["status"] == "ok"
    assert (tmp_path / "ro/late.md").read_bytes() == b"late"


@pytest.mark.enable_socket
def test_pull_dir_items_get_mode_and_mtime_after_children(tmp_path, bucket):
    dir_item = {
        "path": "d",
        "kind": "dir",
        "sha256": None,
        "size": 0,
        "url": None,
        "mode": 0o750,
        "mtime_ns": 1_400_000_000_000_000_000,
        "symlink_target": None,
    }
    child = _file_item(bucket, "d/child", b"c")
    out = rt.pull({"root": str(tmp_path), "items": [dir_item, child]})
    assert out["results"]["d"]["status"] == "ok" and out["results"]["d/child"]["status"] == "ok"
    st = os.stat(tmp_path / "d")
    assert stat.S_IMODE(st.st_mode) == 0o750
    assert st.st_mtime_ns == 1_400_000_000_000_000_000


def test_pull_rejects_path_escape_and_absolute(tmp_path):
    items = [
        {"path": p, "kind": "file", "sha256": "0" * 64, "size": 1, "url": "http://127.0.0.1:9/x", "mode": 0o644, "mtime_ns": 0, "symlink_target": None}
        for p in ("../outside", "a/../../outside", "/etc/passwd")
    ]
    out = rt.pull({"root": str(tmp_path), "items": items})
    assert {k: v["status"] for k, v in out["results"].items()} == {
        "../outside": "failed",
        "a/../../outside": "failed",
        "/etc/passwd": "failed",
    }
    assert not (tmp_path.parent / "outside").exists()


def test_pull_file_over_a_populated_dir_fails_and_an_empty_one_yields(tmp_path):
    (tmp_path / "d").mkdir()
    _write(tmp_path, "d/child", b"c")
    (tmp_path / "e").mkdir()
    (tmp_path / ".wsfiles-relay-e").write_bytes(b"seeded over")
    full = {"path": "d", "kind": "file", "sha256": "0" * 64, "size": 1, "url": "http://127.0.0.1:9/x", "mode": 0o644, "mtime_ns": 0, "symlink_target": None}
    empty = {"path": "e", "kind": "file", "file": ".wsfiles-relay-e", "sha256": _sha(b"seeded over"), "size": 11, "mode": 0o644, "mtime_ns": 1}
    out = rt.pull({"root": str(tmp_path), "items": [full, empty]})
    assert out["results"]["d"]["status"] == "failed" and (tmp_path / "d/child").is_file()
    assert out["results"]["e"]["status"] == "ok" and (tmp_path / "e").read_bytes() == b"seeded over"


@pytest.mark.enable_socket
def test_a_seed_dir_survives_a_transfer_that_fails_after_the_check(tmp_path, _no_backoff):
    """The empty seed yields only once the bytes are verified: a failed pull or
    a mismatched staged copy leaves ``data`` standing, so the path still exists
    for the rest of the session and the next restore gets the same chance."""
    (tmp_path / "data").mkdir()
    (tmp_path / "work").mkdir()
    (tmp_path / ".wsfiles-relay-work").write_bytes(b"not what the manifest says")
    dead = {"path": "data", "kind": "file", "sha256": "0" * 64, "size": 1, "url": "http://127.0.0.1:9/x", "mode": 0o644, "mtime_ns": 0, "symlink_target": None}
    bad = {"path": "work", "kind": "file", "file": ".wsfiles-relay-work", "sha256": "0" * 64, "size": 1, "mode": 0o644, "mtime_ns": 1}
    out = rt.pull({"root": str(tmp_path), "items": [dead, bad]})
    assert out["results"]["data"]["status"] == "unreachable"
    assert out["results"]["work"]["status"] == "mismatch"
    assert (tmp_path / "data").is_dir() and (tmp_path / "work").is_dir()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_prints_result_line_and_exit_codes(tmp_path, capsys):
    import base64
    import json

    def _result():
        out = capsys.readouterr()
        lines = [l for l in out.out.splitlines() if l.startswith(rt.RESULT_MARKER)]
        return json.loads(lines[-1][len(rt.RESULT_MARKER):]) if lines else None, out.err

    _write(tmp_path, "ws/a.txt", b"a")
    spec = {"root": str(tmp_path / "ws")}
    inline = base64.b64encode(json.dumps(spec).encode()).decode()
    assert rt._main(["x", "scan", "--spec-b64", inline]) == 0
    res, _ = _result()
    assert res["entries"][0]["path"] == "a.txt"

    spec_file = tmp_path / "in.json"
    spec_file.write_text(json.dumps(spec))
    assert rt._main(["x", "scan", str(spec_file)]) == 0
    res, _ = _result()
    assert res["entries"][0]["path"] == "a.txt"
    assert not spec_file.exists(), "the spec file is consumed"

    assert rt._main(["x", "scan", str(tmp_path / "missing.json")]) == 2
    res, err = _result()
    assert res is None and "invalid input" in err
    assert rt._main(["x", "bogus", "--spec-b64", inline]) == 2
    assert rt._main(["x", "scan"]) == 2


def test_pull_file_places_a_staged_relay_upload(tmp_path):
    """Bytes the server uploaded to a staging name are verified, moved into
    place and stamped; a truncated or foreign staged copy never becomes the file."""
    (tmp_path / ".wsfiles-relay-1").write_bytes(b"relayed")
    item = {"path": "d/r.txt", "kind": "file", "file": ".wsfiles-relay-1", "sha256": _sha(b"relayed"),
            "size": 7, "mode": 0o600, "mtime_ns": 1_500_000_000_000_000_000}
    (tmp_path / ".wsfiles-relay-2").write_bytes(b"x")
    short = {"path": "s.txt", "kind": "file", "file": ".wsfiles-relay-2", "sha256": _sha(b"xyz"),
             "size": 3, "mode": 0o644, "mtime_ns": 1}
    (tmp_path / ".wsfiles-relay-3").write_bytes(b"abc")
    wrong = {"path": "w.txt", "kind": "file", "file": ".wsfiles-relay-3", "sha256": _sha(b"xyz"),
             "size": 3, "mode": 0o644, "mtime_ns": 1}
    gone = {"path": "gone.txt", "kind": "file", "file": ".wsfiles-relay-4", "sha256": _sha(b"a"), "size": 1}
    no_url = {"path": "n.txt", "kind": "file", "url": None, "size": 1}
    out = rt.pull({"root": str(tmp_path), "items": [item, short, wrong, gone, no_url]})
    assert out["results"]["d/r.txt"]["status"] == "ok"
    st = os.stat(tmp_path / "d/r.txt")
    assert stat.S_IMODE(st.st_mode) == 0o600 and st.st_mtime_ns == 1_500_000_000_000_000_000
    assert (tmp_path / "d/r.txt").read_bytes() == b"relayed"
    assert out["results"]["s.txt"]["status"] == "mismatch"
    assert out["results"]["w.txt"]["status"] == "mismatch"
    assert out["results"]["gone.txt"]["status"] == "failed"
    assert out["results"]["n.txt"] == {"status": "failed", "http": None, "error": "missing url", "ms": out["results"]["n.txt"]["ms"]}
    assert not (tmp_path / "s.txt").exists() and not (tmp_path / "w.txt").exists()
    assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]


def _staged(root, rel: str, data: bytes, **over):
    name = f".wsfiles-relay-{len(os.listdir(root))}"
    (root / name).write_bytes(data)
    return {"path": rel, "kind": "file", "file": name, "sha256": _sha(data), "size": len(data),
            "mode": 0o644, "mtime_ns": 1, **over}


def test_pull_keep_existing_never_replaces_a_file_written_after_the_look(tmp_path, monkeypatch):
    """A deferred file the turn writes while its backup copy is on the way is
    the newer one: placement keeps it, where a plain item is still replaced."""
    place = rt._place

    def turn_writes_first(tmp, final, item, http=None):
        with open(final, "wb") as f:
            f.write(b"turn")
        return place(tmp, final, item, http)

    monkeypatch.setattr(rt, "_place", turn_writes_first)
    kept = _staged(tmp_path, "note.md", b"backup", keep_existing=True)
    plain = _staged(tmp_path, "plain.md", b"backup")
    out = rt.pull({"root": str(tmp_path), "items": [kept, plain]})
    assert {p: r["status"] for p, r in out["results"].items()} == {"note.md": "ok", "plain.md": "ok"}
    assert (tmp_path / "note.md").read_bytes() == b"turn"
    assert (tmp_path / "plain.md").read_bytes() == b"backup"
    assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]


def test_pull_stamps_the_bytes_before_they_are_placed(tmp_path, monkeypatch):
    """A file written at the path just after the placement must keep its own
    mtime: stamped with the backup's, a scan that trusts a size and mtime it
    has seen would never read the new bytes."""
    seen = {}

    def watch(real):
        def call(src, dst, *args, **kwargs):
            seen[os.path.basename(dst)] = os.stat(src).st_mtime_ns
            return real(src, dst, *args, **kwargs)
        return call

    monkeypatch.setattr(rt.os, "link", watch(os.link))
    monkeypatch.setattr(rt.os, "replace", watch(os.replace))
    when = 1_400_000_000_000_000_000
    items = [_staged(tmp_path, "kept.md", b"backup", keep_existing=True, mtime_ns=when),
             _staged(tmp_path, "plain.md", b"backup", mtime_ns=when)]
    out = rt.pull({"root": str(tmp_path), "items": items})
    assert {p: r["status"] for p, r in out["results"].items()} == {"kept.md": "ok", "plain.md": "ok"}
    assert seen["kept.md"] == when and seen["plain.md"] == when


def test_pull_keep_existing_places_a_missing_file_and_keeps_a_present_one(tmp_path, monkeypatch):
    """Without hard links the placement falls back to a look then a rename."""
    (tmp_path / "there.md").write_bytes(b"turn")
    for no_links in (False, True):
        if no_links:
            def refuse(src, dst):
                raise OSError(errno.EXDEV, "cross-device link")
            monkeypatch.setattr(rt.os, "link", refuse)
            (tmp_path / "missing.md").unlink()
        items = [_staged(tmp_path, "there.md", b"backup", keep_existing=True),
                 _staged(tmp_path, "missing.md", b"backup", keep_existing=True, mode=0o600)]
        out = rt.pull({"root": str(tmp_path), "items": items})
        assert {p: r["status"] for p, r in out["results"].items()} == {"there.md": "ok", "missing.md": "ok"}
        assert (tmp_path / "there.md").read_bytes() == b"turn"
        assert (tmp_path / "missing.md").read_bytes() == b"backup"
        assert stat.S_IMODE(os.stat(tmp_path / "missing.md").st_mode) == 0o600
        assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]


def test_pull_pack_keep_existing_member_leaves_a_present_file(tmp_path):
    members = {"a.txt": b"aaa", "b.txt": b"bb"}
    data = b"".join(members[p] for p in sorted(members))
    (tmp_path / ".wsfiles-relay-x").write_bytes(data)
    (tmp_path / "a.txt").write_bytes(b"turn")
    entries, off = [], 0
    for p in sorted(members):
        entries.append({"path": p, "offset": off, "size": len(members[p]), "sha256": _sha(members[p]),
                        "mode": 0o644, "mtime_ns": 1, "keep_existing": True})
        off += len(members[p])
    out = _pull(tmp_path, {"kind": "pack", "file": ".wsfiles-relay-x", "sha256": _sha(data), "size": len(data), "members": entries})
    assert {p: r["status"] for p, r in out["results"].items()} == {"a.txt": "ok", "b.txt": "ok"}
    assert (tmp_path / "a.txt").read_bytes() == b"turn"
    assert (tmp_path / "b.txt").read_bytes() == b"bb"


def test_pull_removes_staging_files_no_item_claims(tmp_path):
    """A relay upload the file API cut short leaves its partial bytes under
    the staging name with no item pointing at it; the scan skips the prefix,
    so the next pull op is the only thing that will ever remove it."""
    (tmp_path / ".wsfiles-relay-orphan").write_bytes(b"partial")
    (tmp_path / ".wsfiles-relay-1").write_bytes(b"relayed")
    (tmp_path / ".wsfiles-tmpdead").write_bytes(b"a download the runtime died in")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub/.wsfiles-other").write_bytes(b"the user's own file")
    item = {"path": "r.txt", "kind": "file", "file": ".wsfiles-relay-1", "sha256": _sha(b"relayed"),
            "size": 7, "mode": 0o644, "mtime_ns": 1}
    out = rt.pull({"root": str(tmp_path), "items": [item]})
    assert out["results"]["r.txt"]["status"] == "ok"
    assert not (tmp_path / ".wsfiles-relay-orphan").exists()
    assert not (tmp_path / ".wsfiles-tmpdead").exists()
    assert (tmp_path / "sub/.wsfiles-other").exists()
    # A pull with nothing to stage (the direct pass) sweeps too.
    (tmp_path / ".wsfiles-relay-orphan2").write_bytes(b"partial")
    rt.pull({"root": str(tmp_path), "items": []})
    assert not (tmp_path / ".wsfiles-relay-orphan2").exists()


def test_pull_defers_dir_modes_and_reopens_a_closed_dir(tmp_path):
    """With ``defer_dir_modes`` the directories stay writable for a later op;
    that op reopens a directory an earlier attempt already closed, places the
    file, and closes it again."""
    dirs = [
        {"path": "ro", "kind": "dir", "mode": 0o555, "mtime_ns": 1_000_000_000},
        {"path": "ro/inner", "kind": "dir", "mode": 0o555, "mtime_ns": 1_000_000_000},
    ]
    out = rt.pull({"root": str(tmp_path), "items": dirs, "defer_dir_modes": True})
    assert {p: r["status"] for p, r in out["results"].items()} == {"ro": "ok", "ro/inner": "ok"}
    assert stat.S_IMODE(os.stat(tmp_path / "ro/inner").st_mode) & 0o300 == 0o300

    os.chmod(tmp_path / "ro/inner", 0o555)
    os.chmod(tmp_path / "ro", 0o555)
    (tmp_path / ".wsfiles-relay-f").write_bytes(b"late")
    staged = {"path": "ro/inner/f.txt", "kind": "file", "file": ".wsfiles-relay-f", "sha256": _sha(b"late"),
              "size": 4, "mode": 0o444, "mtime_ns": 1_000_000_000}
    out = rt.pull({"root": str(tmp_path), "items": [staged] + dirs})
    assert {p: r["status"] for p, r in out["results"].items()} == {"ro": "ok", "ro/inner": "ok", "ro/inner/f.txt": "ok"}
    assert (tmp_path / "ro/inner/f.txt").read_bytes() == b"late"
    assert stat.S_IMODE(os.stat(tmp_path / "ro").st_mode) == 0o555
    assert stat.S_IMODE(os.stat(tmp_path / "ro/inner").st_mode) == 0o555
    assert os.stat(tmp_path / "ro/inner").st_mtime_ns == 1_000_000_000
    os.chmod(tmp_path / "ro", 0o755)
    os.chmod(tmp_path / "ro/inner", 0o755)


@pytest.mark.enable_socket
def test_pull_prefers_direct_egress_over_a_dead_proxy(tmp_path, bucket, monkeypatch):
    """A proxy in the environment is not the route to the store: the runtime
    connects directly and only falls back to the proxy when that fails."""
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:1")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    rt._local.__dict__.clear()
    item = _file_item(bucket, "direct.bin", b"direct")
    out = rt.pull({"root": str(tmp_path), "items": [item], "concurrency": 1})
    assert out["results"]["direct.bin"]["status"] == "ok"
    assert (tmp_path / "direct.bin").read_bytes() == b"direct"
    rt._local.__dict__.clear()


@pytest.mark.enable_socket
def test_a_body_read_that_fails_mid_stream_drops_the_pooled_connection(tmp_path, bucket, monkeypatch):
    """A half-read response poisons the connection it came in on: the next
    request on that thread fails with ``CannotSendRequest``, which the caller
    reads as unreachable and spends a retry attempt on. The response carries
    the pool key so the failing read can discard exactly its own connection.
    """
    rt._local.__dict__.clear()
    monkeypatch.setattr(rt, "_CHUNK", 8)
    key = ("http", "127.0.0.1", bucket.server_address[1])
    item = _file_item(bucket, "half.bin", b"z" * 64)

    stamped = []
    real_request = rt._request
    fail_next = [True]

    class _ReadFailsMidBody:
        """One chunk, then the peer goes away with the body unfinished."""

        def __init__(self, resp):
            self.status = resp.status
            self.wsfiles_conn_key = resp.wsfiles_conn_key
            stamped.append(resp.wsfiles_conn_key)
            self._resp = resp
            self._served = False

        def read(self, amt=None):
            if self._served:
                raise ConnectionResetError("peer reset mid-body")
            self._served = True
            return self._resp.read(amt)

    def _flaky_request(*args, **kwargs):
        resp = real_request(*args, **kwargs)
        if not fail_next[0]:
            return resp
        fail_next[0] = False
        return _ReadFailsMidBody(resp)

    monkeypatch.setattr(rt, "_request", _flaky_request)

    with pytest.raises(ConnectionResetError):
        rt._download_to_temp(item["url"], str(tmp_path), 10)
    assert stamped == [key]
    assert key not in rt._local.conns
    assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]

    # The control: a body read that runs to the end keeps its connection.
    name, _, _, _ = rt._download_to_temp(item["url"], str(tmp_path), 10)
    os.unlink(name)
    assert key in rt._local.conns
    rt._local.__dict__.clear()


# ---------------------------------------------------------------------------
# pack
# ---------------------------------------------------------------------------


def _members(root, files: dict[str, bytes]):
    for rel, data in files.items():
        _write(root, rel, data)
    return [{"path": rel, "sha256": _sha(data), "size": len(data)} for rel, data in files.items()]


def _pack(root, members, max_bytes=32 * 1024 * 1024):
    return rt.pack({"root": str(root), "out_dir": "_internal/packs", "max_bytes": max_bytes, "members": members})


def test_pack_is_deterministic_in_path_order(tmp_path):
    members = _members(tmp_path, {"b.txt": b"bee", "a/z.txt": b"zed", "a/y.txt": b"why"})
    out = _pack(tmp_path, members)
    assert out["changed"] == [] and len(out["chunks"]) == 1
    chunk = out["chunks"][0]
    data = b"whyzedbee"
    assert [m["path"] for m in chunk["members"]] == ["a/y.txt", "a/z.txt", "b.txt"]
    assert [(m["offset"], m["size"]) for m in chunk["members"]] == [(0, 3), (3, 3), (6, 3)]
    assert chunk["sha256"] == _sha(data) and chunk["size"] == len(data)
    assert chunk["path"].startswith("_internal/packs/op-") and chunk["path"].endswith(f"/chunk-{_sha(data)}")
    assert (tmp_path / chunk["path"]).read_bytes() == data
    # The same set handed over in another order yields the same chunk.
    assert _pack(tmp_path, list(reversed(members)))["chunks"][0]["sha256"] == chunk["sha256"]


def test_pack_splits_at_member_boundaries(tmp_path):
    members = _members(tmp_path, {f"f{i}.bin": bytes([i]) * 4 for i in range(5)})
    first = _pack(tmp_path, members, max_bytes=10)
    assert [len(c["members"]) for c in first["chunks"]] == [2, 2, 1]
    assert all(c["size"] <= 10 for c in first["chunks"])
    # A member over the cap still gets a chunk of its own.
    second = _pack(tmp_path, members + _members(tmp_path, {"big.bin": b"x" * 25}), max_bytes=10)
    assert [c["size"] for c in second["chunks"]] == [25, 8, 8, 4]


def test_overlapping_pack_ops_never_touch_each_other(tmp_path):
    """Two syncs of one workspace can overlap; each op owns its directory,
    and only directories nothing can still be pushing from are swept."""
    members = _members(tmp_path, {"a.txt": b"aaa"})
    first = _pack(tmp_path, members)
    first_chunk = tmp_path / first["chunks"][0]["path"]
    second = _pack(tmp_path, members)
    assert first_chunk.exists() and (tmp_path / second["chunks"][0]["path"]).exists()
    assert first["chunks"][0]["sha256"] == second["chunks"][0]["sha256"]
    # Age the first op's directory past the sweep threshold: the next op removes it.
    old = 1_000_000_000
    os.utime(first_chunk.parent, ns=(old * 10**9, old * 10**9))
    third = _pack(tmp_path, members)
    assert not first_chunk.parent.exists()
    assert (tmp_path / second["chunks"][0]["path"]).exists()
    assert (tmp_path / third["chunks"][0]["path"]).exists()


def test_unlink_removes_only_files_under_root(tmp_path):
    _write(tmp_path, "_internal/packs/op-1/chunk-x", b"c")
    (tmp_path / "d").mkdir()
    out = rt.unlink({"root": str(tmp_path), "paths": ["_internal/packs/op-1/chunk-x", "d", "../escape", "missing"]})
    assert out == {"removed": 1}
    assert not (tmp_path / "_internal/packs/op-1/chunk-x").exists() and (tmp_path / "d").is_dir()


def test_scan_skips_the_root_temp_namespace_only(tmp_path):
    """Every transient file lives at the root under the prefix; a user's own
    ``dir/.wsfiles-notes`` is a file like any other, or the next sync would
    prune its row and a strict stop would lose it."""
    _write(tmp_path, "keep.txt", b"k")
    _write(tmp_path, ".wsfiles-abc123", b"partial")
    _write(tmp_path, "dir/.wsfiles-notes", b"mine")
    out = _scan(tmp_path, exclude_root_basename_prefixes=[".wsfiles-"])
    assert sorted(e["path"] for e in out["entries"] if e["kind"] == "file") == ["dir/.wsfiles-notes", "keep.txt"]


def test_scan_skips_the_sync_marker_at_the_root_only(tmp_path):
    """The marker is a root file; the same name deeper down is the user's."""
    _write(tmp_path, ".file_sync_marker", b"2026")
    _write(tmp_path, ".file_sync_marker.bak", b"mine too")
    _write(tmp_path, "results/.file_sync_marker", b"mine")
    out = _scan(tmp_path, exclude_root_basenames=[".file_sync_marker"])
    assert sorted(e["path"] for e in out["entries"] if e["kind"] == "file") == [
        ".file_sync_marker.bak", "results/.file_sync_marker",
    ]


def test_scan_skips_reserved_root_names_for_every_entry_type(tmp_path):
    """The pull op's sweep removes any unclaimed root ``.wsfiles-`` entry,
    directories included, so a row for one would only promise what the next
    restore deletes; the same names one level down are the user's."""
    _write(tmp_path, ".wsfiles-relay-1/part", b"x")
    _write(tmp_path, "keep.txt", b"k")
    os.symlink("keep.txt", tmp_path / ".wsfiles-link")
    os.symlink("keep.txt", tmp_path / ".file_sync_marker")
    _write(tmp_path, "sub/.wsfiles-mine/part", b"y")
    os.symlink("../keep.txt", tmp_path / "sub" / ".file_sync_marker")
    out = _scan(
        tmp_path,
        exclude_root_basenames=[".file_sync_marker"],
        exclude_root_basename_prefixes=[".wsfiles-"],
    )
    assert [e["path"] for e in out["entries"]] == [
        "keep.txt", "sub", "sub/.file_sync_marker", "sub/.wsfiles-mine", "sub/.wsfiles-mine/part",
    ]


def test_pull_writes_its_temp_files_at_the_root(tmp_path):
    """A temp beside the target would need the scan to skip the prefix at
    every depth, which is what made a user's ``dir/.wsfiles-notes`` vanish."""
    seen: list[str] = []
    orig = rt.tempfile.NamedTemporaryFile

    def spy(*a, **kw):
        seen.append(kw["dir"])
        return orig(*a, **kw)

    rt.tempfile.NamedTemporaryFile = spy
    try:
        (tmp_path / ".wsfiles-relay-1").write_bytes(b"relayed")
        item = {"path": "d/e/r.txt", "kind": "file", "file": ".wsfiles-relay-1", "sha256": _sha(b"relayed"), "size": 7}
        rt.pull({"root": str(tmp_path), "items": [item]})
        chunk = b"member"
        (tmp_path / ".wsfiles-relay-c").write_bytes(chunk)
        pack = {"kind": "pack", "file": ".wsfiles-relay-c", "sha256": _sha(chunk), "size": len(chunk),
                "members": [{"path": "d/e/m.txt", "offset": 0, "size": 6, "sha256": _sha(b"member")}]}
        out = rt.pull({"root": str(tmp_path), "items": [pack]})
    finally:
        rt.tempfile.NamedTemporaryFile = orig
    assert out["results"]["d/e/m.txt"]["status"] == "ok"
    assert seen and set(seen) == {str(tmp_path)}


def test_hash_file_reads_a_late_bad_sequence_as_binary(tmp_path):
    """A blob row is never re-read on the way out, so the whole file has to
    decide: a bad sequence after the first pages would otherwise be served
    as text with replacement marks."""
    late = b"x" * (64 * 1024 + 1) + b"\xff\xfe"
    (tmp_path / "late.txt").write_bytes(late)
    big_text = ("caf\u00e9 " * 40_000).encode("utf-8")
    (tmp_path / "big.txt").write_bytes(big_text)
    (tmp_path / "nul.bin").write_bytes(b"a" * 10 + b"\0" + b"b" * 10)
    assert rt._hash_file(str(tmp_path / "late.txt")) == (_sha(late), True, len(late))
    assert rt._hash_file(str(tmp_path / "big.txt")) == (_sha(big_text), False, len(big_text))
    assert rt._hash_file(str(tmp_path / "nul.bin"))[1] is True


def test_pack_drops_a_member_that_changed_or_vanished(tmp_path):
    members = _members(tmp_path, {"same.txt": b"same", "edited.txt": b"old!", "gone.txt": b"gone"})
    (tmp_path / "edited.txt").write_bytes(b"new!")  # same size, different bytes
    (tmp_path / "gone.txt").unlink()
    out = _pack(tmp_path, members)
    assert sorted(out["changed"]) == ["edited.txt", "gone.txt"]
    assert [m["path"] for m in out["chunks"][0]["members"]] == ["same.txt"]
    assert out["chunks"][0]["sha256"] == _sha(b"same")


def test_pack_with_nothing_to_pack_writes_no_chunk(tmp_path):
    assert _pack(tmp_path, []) == {"chunks": [], "changed": []}
    assert os.listdir(tmp_path / "_internal/packs") == []


def test_pack_releases_the_previous_runs_chunks_before_writing(tmp_path):
    first = _pack(tmp_path, _members(tmp_path, {"a.txt": b"aaa"}))["chunks"]
    members = _members(tmp_path, {"b.txt": b"bbb"})
    second = rt.pack({
        "root": str(tmp_path), "out_dir": "_internal/packs", "members": members,
        "release": [c["path"] for c in first],
    })["chunks"]
    assert not (tmp_path / first[0]["path"]).exists()
    assert (tmp_path / second[0]["path"]).is_file()


def test_pack_refuses_to_write_beside_a_chunk_it_could_not_release(tmp_path):
    """The removals before it are quiet; this is where one that missed is caught."""
    (tmp_path / "_internal/packs/op-prev/chunk-x").mkdir(parents=True)
    members = _members(tmp_path, {"b.txt": b"bbb"})
    with pytest.raises(OSError, match="could not be removed"):
        rt.pack({
            "root": str(tmp_path), "out_dir": "_internal/packs", "members": members,
            "release": ["_internal/packs/op-prev/chunk-x"],
        })
    assert os.listdir(tmp_path / "_internal/packs") == ["op-prev"]


def test_a_pack_that_fails_part_way_removes_what_it_wrote(tmp_path, monkeypatch):
    """A full disk failed the pack after a chunk was down. Left in place, the
    chunks held the room every later backup needed until the age sweep."""
    members = _members(tmp_path, {f"f{i}.bin": bytes([i]) * 4 for i in range(5)})
    real_replace, closed = os.replace, []

    def replace(src, dst):
        closed.append(dst)
        if len(closed) == 2:
            raise OSError(errno.ENOSPC, "No space left on device")
        real_replace(src, dst)

    monkeypatch.setattr(rt.os, "replace", replace)
    with pytest.raises(OSError):
        _pack(tmp_path, members, max_bytes=10)
    assert os.listdir(tmp_path / "_internal/packs") == []


@pytest.mark.enable_socket
def test_push_unlink_removes_the_file_after_upload(tmp_path, bucket):
    data = b"chunk bytes"
    _write(tmp_path, "_internal/packs/chunk-x", data)
    item = {**_push_item(bucket, "_internal/packs/chunk-x", data), "unlink": True}
    out = rt.push({"root": str(tmp_path), "concurrency": 2, "timeout_s": 10, "items": [item]})
    assert out["results"][_sha(data)]["status"] == "ok"
    assert bucket.objects[f"/{_sha(data)}"] == data
    assert not (tmp_path / "_internal/packs/chunk-x").exists()


@pytest.mark.enable_socket
def test_push_unlink_is_withheld_when_the_store_is_unreachable(tmp_path):
    """The server relays a chunk the sandbox could not upload, by reading it
    back out of the sandbox. Unlinking on a failed push loses those bytes."""
    data = b"chunk bytes"
    _write(tmp_path, "_internal/packs/chunk-x", data)
    fake = type("B", (), {"base": f"http://127.0.0.1:{_closed_port()}"})()
    item = {**_push_item(fake, "_internal/packs/chunk-x", data), "unlink": True}
    out = rt.push({"root": str(tmp_path), "items": [item], "timeout_s": 2})
    assert out["results"][_sha(data)]["status"] == "unreachable"
    assert (tmp_path / "_internal/packs/chunk-x").read_bytes() == data


@pytest.mark.enable_socket
def test_push_unlink_is_withheld_when_the_store_rejected_the_chunk(tmp_path, bucket):
    signed = b"original"
    _write(tmp_path, "_internal/packs/chunk-x", b"modified")
    item = {**_push_item(bucket, "_internal/packs/chunk-x", signed), "unlink": True}
    out = rt.push({"root": str(tmp_path), "items": [item]})
    assert out["results"][_sha(signed)]["status"] == "changed"
    assert (tmp_path / "_internal/packs/chunk-x").exists()


def _pack_item(bucket, members: dict[str, bytes], **over):
    order = sorted(members)
    data = b"".join(members[p] for p in order)
    key = f"/{_sha(data)}"
    bucket.objects[key] = data
    entries, offset = [], 0
    for p in order:
        entries.append(
            {"path": p, "offset": offset, "size": len(members[p]), "sha256": _sha(members[p]),
             "mode": 0o640, "mtime_ns": 1_700_000_000_000_000_000}
        )
        offset += len(members[p])
    item = {"kind": "pack", "sha256": _sha(data), "size": len(data), "url": f"{bucket.base}{key}", "members": entries}
    item.update(over)
    return item


def _pull(root, *items):
    return rt.pull({"root": str(root), "concurrency": 4, "timeout_s": 10, "items": list(items)})


@pytest.mark.enable_socket
def test_pull_pack_extracts_every_member_with_one_get(tmp_path, bucket):
    item = _pack_item(bucket, {"a/one.txt": b"one", "two.txt": b"two two", "empty.txt": b""})
    out = _pull(tmp_path, item)
    assert _sans_ms(out["results"]) == {
        p: {"status": "ok", "http": 200, "error": None} for p in ("a/one.txt", "two.txt", "empty.txt")
    }
    assert (tmp_path / "a/one.txt").read_bytes() == b"one"
    assert (tmp_path / "two.txt").read_bytes() == b"two two"
    assert (tmp_path / "empty.txt").read_bytes() == b""
    st = os.stat(tmp_path / "two.txt")
    assert stat.S_IMODE(st.st_mode) == 0o640 and st.st_mtime_ns == 1_700_000_000_000_000_000
    assert bucket.hits[item["url"][len(bucket.base):]] == 1
    assert not [p for p in os.listdir(tmp_path) if p.startswith(".wsfiles-")]
    assert os.listdir(tmp_path / "_internal/packs") == []


@pytest.mark.enable_socket
def test_pull_never_writes_through_a_link_out_of_its_root(tmp_path, bucket):
    """A workspace folder links the shared skills from the computer root; a row
    recorded before the link must not land in every sibling's copy."""
    shared = tmp_path / "shared/pdf"
    shared.mkdir(parents=True)
    (shared / "SKILL.md").write_bytes(b"current")
    before = os.stat(shared)
    root = tmp_path / "folder"
    (root / ".agents/skills").mkdir(parents=True)
    os.symlink("../../../shared/pdf", root / ".agents/skills/pdf")
    link = {"sha256": None, "size": 0, "url": None, "mode": 0, "mtime_ns": 1, "symlink_target": "x"}
    out = _pull(
        root,
        {"path": ".agents/skills/pdf", "kind": "dir", "mode": 0o700, "mtime_ns": 1},
        _file_item(bucket, ".agents/skills/pdf/SKILL.md", b"stale"),
        {"path": ".agents/skills/pdf/ref", "kind": "symlink", **link},
        _pack_item(bucket, {".agents/skills/pdf/lib/a.py": b"stale", "notes.md": b"mine"}),
    )
    leaves = ("failed", "path leaves root through a link")
    assert {p: (r["status"], r["error"]) for p, r in out["results"].items()} == {
        ".agents/skills/pdf": leaves,
        ".agents/skills/pdf/SKILL.md": leaves,
        ".agents/skills/pdf/ref": leaves,
        ".agents/skills/pdf/lib/a.py": leaves,
        "notes.md": ("ok", None),
    }
    assert os.listdir(shared) == ["SKILL.md"]
    assert (shared / "SKILL.md").read_bytes() == b"current"
    after = os.stat(shared)
    assert (after.st_mode, after.st_mtime_ns) == (before.st_mode, before.st_mtime_ns)
    assert (root / "notes.md").read_bytes() == b"mine"


def test_pull_never_places_a_link_through_one_it_just_placed(tmp_path):
    """Step 1 checks every parent before any link exists, so a stale row under
    a linked name passes it; step 3 places the ancestor link first."""
    shared = tmp_path / "shared/pdf"
    shared.mkdir(parents=True)
    (shared / "helper").write_bytes(b"current")
    root = tmp_path / "folder"
    (root / ".agents/skills").mkdir(parents=True)
    link = {"sha256": None, "size": 0, "url": None, "mode": 0, "mtime_ns": 1}
    out = _pull(
        root,
        {"path": ".agents/skills/pdf", "kind": "symlink", "symlink_target": "../../../shared/pdf", **link},
        {"path": ".agents/skills/pdf/helper", "kind": "symlink", "symlink_target": "x", **link},
    )
    assert {p: (r["status"], r["error"]) for p, r in out["results"].items()} == {
        ".agents/skills/pdf": ("ok", None),
        ".agents/skills/pdf/helper": ("failed", "path leaves root through a link"),
    }
    assert not os.path.islink(shared / "helper")
    assert (shared / "helper").read_bytes() == b"current"


def test_pull_pack_consumes_a_chunk_already_in_the_sandbox(tmp_path):
    """The relay path uploads the chunk itself; the runtime verifies, slices and removes it."""
    members = {"names/trailing. ": b"sp", "names/new\nline.txt": b"nl", "c.txt": b"ccc"}
    data = b"".join(members[p] for p in sorted(members))
    (tmp_path / ".wsfiles-relay-x").write_bytes(data)
    entries, off = [], 0
    for p in sorted(members):
        entries.append({"path": p, "offset": off, "size": len(members[p]), "sha256": _sha(members[p]), "mode": 0o600, "mtime_ns": 1_700_000_000_000_000_000})
        off += len(members[p])
    out = _pull(tmp_path, {"kind": "pack", "file": ".wsfiles-relay-x", "sha256": _sha(data), "size": len(data), "members": entries})
    assert {p: r["status"] for p, r in out["results"].items()} == {p: "ok" for p in members}
    assert (tmp_path / "names/trailing. ").read_bytes() == b"sp"
    assert (tmp_path / "names/new\nline.txt").read_bytes() == b"nl"
    assert stat.S_IMODE(os.stat(tmp_path / "c.txt").st_mode) == 0o600
    assert not (tmp_path / ".wsfiles-relay-x").exists()


def test_pull_pack_local_chunk_mismatch_fails_all_and_removes_the_chunk(tmp_path):
    (tmp_path / ".wsfiles-relay-x").write_bytes(b"corrupt")
    item = {"kind": "pack", "file": ".wsfiles-relay-x", "sha256": _sha(b"aaa"), "size": 3,
            "members": [{"path": "a.txt", "offset": 0, "size": 3, "sha256": _sha(b"aaa"), "mode": 0o600, "mtime_ns": 0}]}
    out = _pull(tmp_path, item)
    assert out["results"]["a.txt"]["status"] == "mismatch"
    assert not (tmp_path / "a.txt").exists()
    assert not (tmp_path / ".wsfiles-relay-x").exists()
    out = _pull(tmp_path, dict(item, file="../outside"))
    assert out["results"]["a.txt"] == {"status": "failed", "http": None, "error": "file escapes root", "ms": out["results"]["a.txt"]["ms"]}


@pytest.mark.enable_socket
def test_pull_pack_chunk_mismatch_fails_every_member_and_writes_nothing(tmp_path, bucket):
    item = _pack_item(bucket, {"a.txt": b"aaa", "b.txt": b"bbb"}, sha256=_sha(b"other"))
    out = _pull(tmp_path, item)
    assert {p: r["status"] for p, r in out["results"].items()} == {"a.txt": "mismatch", "b.txt": "mismatch"}
    assert not (tmp_path / "a.txt").exists() and not (tmp_path / "b.txt").exists()


@pytest.mark.enable_socket
def test_pull_pack_member_mismatch_is_confined_to_that_member(tmp_path, bucket):
    item = _pack_item(bucket, {"a.txt": b"aaa", "b.txt": b"bbb"})
    item["members"][1]["sha256"] = _sha(b"xxx")
    out = _pull(tmp_path, item)
    assert {p: r["status"] for p, r in out["results"].items()} == {"a.txt": "ok", "b.txt": "mismatch"}
    assert (tmp_path / "a.txt").read_bytes() == b"aaa" and not (tmp_path / "b.txt").exists()


@pytest.mark.enable_socket
def test_pull_pack_restores_a_chunk_its_manifest_names_only_part_of(tmp_path, bucket):
    """A member that changed while its chunk was repacked keeps its row on the
    old chunk while its sibling moves on, and restore weighs the item by the
    rows that remain. The chunk is still whole and its digest still matches."""
    item = _pack_item(bucket, {"a.txt": b"aaa", "b.txt": b"bbb"})
    item["members"] = item["members"][1:]
    item["size"] = 3
    out = _pull(tmp_path, item)
    assert {p: r["status"] for p, r in out["results"].items()} == {"b.txt": "ok"}
    assert (tmp_path / "b.txt").read_bytes() == b"bbb" and not (tmp_path / "a.txt").exists()


@pytest.mark.enable_socket
def test_pull_pack_404_fails_all_and_500_retries(tmp_path, bucket):
    item = _pack_item(bucket, {"a.txt": b"aaa"})
    key = item["url"][len(bucket.base):]
    bucket.fail_500.add(key)
    out = _pull(tmp_path, item)
    assert _sans_ms(out["results"]) == {"a.txt": {"status": "failed", "http": 500, "error": "HTTP 500"}}
    assert bucket.hits[key] == rt._MAX_ATTEMPTS
    missing = _pack_item(bucket, {"m.txt": b"m"})
    del bucket.objects[missing["url"][len(bucket.base):]]
    out = _pull(tmp_path, missing)
    assert _sans_ms(out["results"]) == {"m.txt": {"status": "failed", "http": 404, "error": "HTTP 404"}}


@pytest.mark.enable_socket
def test_pull_packs_and_files_share_one_pass(tmp_path, bucket):
    pack = _pack_item(bucket, {"small.txt": b"small"})
    big = _file_item(bucket, "big.bin", os.urandom(4096))
    out = _pull(tmp_path, big, pack)
    assert {p: r["status"] for p, r in out["results"].items()} == {"small.txt": "ok", "big.bin": "ok"}


@pytest.mark.parametrize(
    "argv",
    [
        ["p"],
        ["p", "scan"],
        ["p", "nope", "spec.json"],
        ["p", "scan", "--spec-b64"],
        ["p", "scan", "spec.json", "extra"],
        ["p", "scan", "spec.json", "--spec-b64"],
    ],
)
def test_main_answers_every_malformed_argv_with_usage(argv, capsys):
    assert rt._main(argv) == 2
    assert capsys.readouterr().err == rt._USAGE


def test_main_accepts_both_spec_forms(tmp_path, monkeypatch):
    monkeypatch.setitem(rt._OPS, "scan", lambda spec: {"seen": spec})
    b64 = base64.b64encode(b'{"root":"/x"}').decode()
    assert rt._main(["p", "scan", "--spec-b64", b64]) == 0

    path = tmp_path / "spec.json"
    path.write_bytes(b'{"root":"/y"}')
    assert rt._main(["p", "scan", str(path)]) == 0
    # The file form consumes its spec, so a retry cannot read a stale one.
    assert not path.exists()


# ---------------------------------------------------------------------------
# one computer, several workspace folders
# ---------------------------------------------------------------------------


def test_pack_writes_chunks_at_the_computer_root_when_pack_root_is_given(tmp_path):
    """The walk root is one workspace's folder; the chunks are the machine's
    scratch and stay under the computer's ``_internal``, which is the one
    directory no workspace's scan walks."""
    computer = tmp_path
    work = tmp_path / "alpha-1111"
    work.mkdir()
    members = _members(work, {"a.txt": b"aaa"})
    out = rt.pack(
        {
            "root": str(work),
            "pack_root": str(computer),
            "out_dir": "_internal/packs",
            "max_bytes": 1024,
            "members": members,
        }
    )
    chunk = out["chunks"][0]
    # Outside the walk root, so it cannot be named relative to it.
    assert os.path.isabs(chunk["path"])
    assert chunk["path"].startswith(str(computer / "_internal" / "packs") + os.sep)
    assert not (work / "_internal").exists()
    assert open(chunk["path"], "rb").read() == b"aaa"


def test_pack_without_pack_root_still_names_chunks_relative_to_the_walk_root(tmp_path):
    """A spec written before the split reads exactly as it did."""
    members = _members(tmp_path, {"a.txt": b"aaa"})
    chunk = _pack(tmp_path, members)["chunks"][0]
    assert not os.path.isabs(chunk["path"])
    assert chunk["path"].startswith("_internal/packs/op-")


@pytest.mark.enable_socket
def test_push_accepts_an_absolute_chunk_path_only_under_the_pack_directory(
    tmp_path, bucket
):
    computer = tmp_path
    work = tmp_path / "alpha-1111"
    work.mkdir()
    chunk = _write(computer, "_internal/packs/op-1/chunk-x", b"chunk")
    outside = _write(computer, "beta-2222/secret.txt", b"nope")
    items = [
        {**_push_item(bucket, chunk, b"chunk", key="chunk"), "unlink": True},
        _push_item(bucket, outside, b"nope", key="nope"),
    ]
    out = rt.push(
        {"root": str(work), "pack_root": str(computer), "items": items, "timeout_s": 5}
    )
    results = _sans_ms(out["results"])
    assert results[_sha(b"chunk")]["status"] == "ok"
    # A path outside the pack directory is a workspace-relative name or nothing.
    assert results[_sha(b"nope")] == {
        "status": "failed",
        "http": None,
        "error": "path escapes root",
    }
    assert not os.path.exists(chunk)
    assert os.path.exists(outside)


def test_unlink_removes_a_chunk_outside_the_walk_root_and_nothing_else(tmp_path):
    computer = tmp_path
    work = tmp_path / "alpha-1111"
    work.mkdir()
    chunk = _write(computer, "_internal/packs/op-1/chunk-x", b"c")
    sibling = _write(computer, "beta-2222/secret.txt", b"s")
    out = rt.unlink(
        {
            "root": str(work),
            "pack_root": str(computer),
            "paths": [chunk, sibling, f"{computer}/../escape"],
        }
    )
    assert out == {"removed": 1}
    assert not os.path.exists(chunk) and os.path.exists(sibling)


def test_a_scan_of_one_folder_never_sees_its_siblings(tmp_path):
    computer = tmp_path
    work = computer / "alpha-1111"
    _write(work, "report.html", b"mine")
    _write(computer, "beta-2222/secret.txt", b"theirs")
    _write(computer, "_internal/packs/op-1/chunk-x", b"scratch")
    out = _scan(work)
    assert [e["path"] for e in out["entries"] if e["kind"] == "file"] == ["report.html"]


def test_a_symlink_standing_where_an_excluded_directory_would_be_is_skipped(tmp_path):
    """A restore places directories by path, so a symlink at an excluded path
    is that directory as far as the manifest is concerned."""
    (tmp_path / ".agents" / "skills").mkdir(parents=True)
    (tmp_path / ".agents" / "skills" / ".staging").symlink_to(tmp_path)
    (tmp_path / ".agents" / "skills" / ".trash-1").symlink_to(tmp_path)
    (tmp_path / ".agents" / "skills" / "mine").symlink_to(tmp_path)
    out = _scan(tmp_path)
    assert [e["path"] for e in out["entries"] if e["kind"] == "symlink"] == [
        ".agents/skills/mine"
    ]
