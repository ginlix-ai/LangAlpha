"""CopyFile through object storage, run against two local folders and a
loopback bucket.

The transfer runtime runs as the python3 subprocess a computer runs, so the
scan, the packing, both transfers and the placing script are the real ones;
only the store's signing and the blob registry are stand-ins.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import socket
import stat
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.agent.tools import copy_file as cf
from ptc_agent.agent.tools.computer_reach import ComputerTarget, ComputerUnreachable
from ptc_agent.core.paths import lexical_path
from ptc_agent.core.project_context import ProjectContext
from src.server.services.persistence import blobs, ship, transfer
from src.server.services.persistence.ship import StoreUnreachable, ship_via_store

USER = "user-ship"


class _Bucket(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.objects: dict[str, bytes] = {}
        self.puts = 0


class _Handler(BaseHTTPRequestHandler):
    server: _Bucket

    def log_message(self, *_):
        pass

    def _answer(self, status: int, body: bytes = b"") -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        want = self.headers.get("x-amz-checksum-sha256")
        if want != base64.b64encode(hashlib.sha256(body).digest()).decode():
            self._answer(400, b"<Error><Code>BadDigest</Code></Error>")
            return
        self.server.puts += 1
        self.server.objects[self.path] = body
        self._answer(200)

    def do_GET(self):
        body = self.server.objects.get(self.path)
        self._answer(200, body) if body is not None else self._answer(404)


@pytest.fixture
def bucket():
    srv = _Bucket(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    srv.base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.fixture
def store(bucket, monkeypatch):
    """Signing against the loopback bucket and an in-memory registry."""
    registered: set[str] = set()

    def signed_put(key, *, sha256_hex, content_length, content_type, expires_in=900):
        checksum = base64.b64encode(bytes.fromhex(sha256_hex)).decode()
        return f"{bucket.base}/{key}", {"x-amz-checksum-sha256": checksum}

    async def known(user_id, shas):
        return {s for s in shas if s in registered}

    async def register(user_id, entries):
        registered.update(sha for sha, _ in entries)

    monkeypatch.setattr("src.utils.storage.BLOB_TRANSFER_MODE", "auto")
    monkeypatch.setattr("src.utils.storage.STORAGE_PROVIDER", "s3")
    monkeypatch.setattr(blobs, "get_signed_upload_url", signed_put)
    monkeypatch.setattr(blobs, "registered_blobs", known)
    monkeypatch.setattr(blobs, "register_blobs", register)
    monkeypatch.setattr(ship, "registered_blobs", known)
    monkeypatch.setattr(ship, "get_signed_url", lambda key, expires: f"{bucket.base}/{key}")
    bucket.registered = registered
    return bucket


class Machine:
    """A computer's sandbox calls, served from a local folder, the transfer
    runtime run as the python3 subprocess a computer runs."""

    def __init__(self, root: Path) -> None:
        self.root = self.working_dir = str(root)
        self.sandbox_id = f"sb-{root.name}"
        self.config = SimpleNamespace(
            sandbox=SimpleNamespace(provider="daytona"),
            filesystem=SimpleNamespace(enable_path_validation=True, allowed_directories=[self.root]),
        )
        self.runtime = SimpleNamespace(exec=self._exec, upload_file=self._upload)

    async def _runtime_call(self, fn, *args, retry_policy=None):
        return await fn(*args)

    async def _exec(self, command, timeout):
        done = await asyncio.to_thread(
            subprocess.run, ["bash", "-c", command], capture_output=True, text=True, timeout=timeout
        )
        return SimpleNamespace(stdout=done.stdout, exit_code=done.returncode)

    async def _upload(self, content: bytes, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(content)

    def normalize_path(self, path: str, project=None) -> str:
        path = path.strip()
        if path == self.root or path.startswith(f"{self.root}/"):
            return lexical_path(path)
        return lexical_path(f"{self.root}/{path.lstrip('/')}")

    def validate_path(self, path: str, project=None) -> bool:
        whole = self.normalize_path(path)
        return not whole.startswith(f"{self.root}/_internal")

    async def astream_file_bytes(self, path: str):
        if not os.path.isfile(path):
            return None

        async def chunks():
            with open(path, "rb") as f:
                while chunk := f.read(64 * 1024):
                    yield chunk

        return chunks()

    async def aupload_file_bytes(self, path: str, content: bytes, verify: bool = False) -> bool:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(content)
        return True

    async def execute_bash_command(self, command, working_dir=None, timeout=60):
        done = await asyncio.to_thread(
            subprocess.run,
            ["bash", "-c", command],
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "success": done.returncode == 0,
            "stdout": done.stdout,
            "stderr": done.stderr,
            "exit_code": done.returncode,
        }


def _computer(computer_id: str, name: str, root: Path) -> ComputerTarget:
    root.mkdir(parents=True, exist_ok=True)
    return ComputerTarget(
        computer_id,
        name,
        SimpleNamespace(sandbox=Machine(root)),
        ProjectContext(workspace_id="", dir_name=None),
        str(root),
    )


async def _ship(source, shipment, destination):
    return await ship_via_store(
        USER,
        source.sandbox,
        source.root,
        destination.sandbox,
        destination.root,
        shipment,
        source_name=source.name,
        destination_name=destination.name,
    )


@pytest.fixture
def machines(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    return _computer("a", "Mac", a), _computer("b", "Cloud", b), a, b


def _tree(root: Path) -> dict[str, tuple]:
    """Every entry under ``root``: its kind, mode and bytes or link target."""
    out = {}
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root))
        st = p.lstat()
        if stat.S_ISLNK(st.st_mode):
            out[rel] = ("link", os.readlink(p))
        elif stat.S_ISDIR(st.st_mode):
            out[rel] = ("dir", oct(stat.S_IMODE(st.st_mode)))
        else:
            out[rel] = ("file", oct(stat.S_IMODE(st.st_mode)), hashlib.sha256(p.read_bytes()).hexdigest())
    return out


def _leftovers(*roots: Path) -> list[Path]:
    # Less the source folder's own entry named like one, which _project makes.
    return [
        p
        for root in roots
        for p in root.rglob("*")
        if cf._COPY_TAG in p.name and p.name != ".x.langalpha-copy-deadbeef"
    ]


def _project(a: Path) -> Path:
    """A folder with every kind of entry a copy carries and what it leaves out."""
    top = a / "proj"
    (top / "deep/er").mkdir(parents=True)
    (top / "empty").mkdir()
    (top / "big.bin").write_bytes(os.urandom(600 * 1024))
    (top / "deep/er/small.txt").write_text("small\n")
    (top / "notes.md").write_text("notes\n")
    (top / "secret").write_text("s\n")
    (top / "secret").chmod(0o600)
    (top / "run.sh").write_text("#!/bin/sh\n")
    (top / "run.sh").chmod(0o755)
    (top / "link").symlink_to("notes.md")
    (top / ".agents/threads").mkdir(parents=True)
    (top / ".agents/threads/log").write_text("left out\n")
    (top / ".x.langalpha-copy-deadbeef").mkdir()
    (top / "deep").chmod(0o750)
    return top


def _carried(tree: dict[str, tuple]) -> dict[str, tuple]:
    return {
        k: v
        for k, v in tree.items()
        if not k.startswith(".agents/threads") and cf._COPY_TAG not in k
    }


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_folder_goes_through_the_store_as_it_is(machines, store):
    mac, cloud, a, b = machines
    top = _project(a)
    result = await cf.copy_file(mac, "proj", cloud, "ws/proj", ship=_ship)
    assert _tree(b / "ws/proj") == _carried(_tree(top))
    assert ("dir", oct(stat.S_IMODE(top.stat().st_mode))) == _tree(b / "ws")["proj"]
    assert result.startswith(f'Copied the folder {top} on computer "Mac" (5 files,')
    assert not _leftovers(a, b)
    # Pack chunks are the source computer's scratch and go once pushed.
    assert not [p for p in (a / "_internal").rglob("*") if p.is_file() and "packs" in str(p)]


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_what_the_store_holds_is_not_uploaded_again(machines, store):
    mac, cloud, a, b = machines
    _project(a)
    await cf.copy_file(mac, "proj", cloud, "one", ship=_ship)
    sent = store.puts
    assert sent == 2  # the large file, and one chunk of the small ones
    await cf.copy_file(mac, "proj", cloud, "two", ship=_ship)
    assert store.puts == sent
    (a / "proj/notes.md").write_text("changed\n")
    await cf.copy_file(mac, "proj", cloud, "three", ship=_ship)
    # Only the chunk holding the changed small file goes again.
    assert store.puts == sent + 1
    assert (b / "three/notes.md").read_text() == "changed\n"


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_file_goes_through_the_store_keeping_its_mode(machines, store):
    mac, cloud, a, b = machines
    data = os.urandom(300 * 1024)
    (a / "data.bin").write_bytes(data)
    (a / "data.bin").chmod(0o640)
    result = await cf.copy_file(mac, "data.bin", cloud, "ws/", ship=_ship)
    assert (b / "ws/data.bin").read_bytes() == data
    assert stat.S_IMODE((b / "ws/data.bin").stat().st_mode) == 0o640
    assert result == f'Copied 300.0 KB from {a}/data.bin on computer "Mac" to {b}/ws/data.bin on computer "Cloud".'
    assert not _leftovers(a, b)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_merge_through_the_store_keeps_the_folder_it_merges_into(machines, store):
    mac, cloud, a, b = machines
    top = _project(a)
    there = b / "ws/proj"
    there.mkdir(parents=True)
    there.chmod(0o700)
    (there / "mine.txt").write_text("mine\n")
    (there / "notes.md").write_text("old\n")
    # A link in the way is replaced, not written through.
    (b / "outside.txt").write_text("outside\n")
    (there / "run.sh").symlink_to(b / "outside.txt")
    result = await cf.copy_file(mac, "proj", cloud, "ws/proj", overwrite=True, ship=_ship)
    assert result.startswith("Merged the folder")
    assert (there / "mine.txt").read_text() == "mine\n"
    assert (there / "notes.md").read_text() == "notes\n"
    assert not (there / "run.sh").is_symlink()
    assert (b / "outside.txt").read_text() == "outside\n"
    assert stat.S_IMODE(there.stat().st_mode) == 0o700
    merged = {k: v for k, v in _tree(there).items() if k != "mine.txt"}
    assert merged == _carried(_tree(top))
    assert not _leftovers(a, b)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_folder_over_the_limit_is_refused_before_it_is_read(machines, store, monkeypatch):
    mac, cloud, a, b = machines
    _project(a)
    monkeypatch.setattr(cf, "COPY_MAX_BYTES_VIA_STORE", 1000)
    with pytest.raises(ComputerUnreachable, match="carries up to 1000 bytes between these computers"):
        await cf.copy_file(mac, "proj", cloud, "ws/proj", ship=_ship)
    assert store.puts == 0
    assert not _leftovers(a, b)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_destination_without_the_store_leaves_the_route(machines, store, monkeypatch):
    mac, cloud, a, b = machines
    _project(a)
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    closed = s.getsockname()[1]
    s.close()
    monkeypatch.setattr(ship, "get_signed_url", lambda key, expires: f"http://127.0.0.1:{closed}/{key}")
    staging = b / ".stage"
    staging.mkdir()
    shipment = cf.Shipment(
        path=str(a / "proj"), folder=True, size=0, staging=str(staging), max_bytes=10**9
    )
    with pytest.raises(StoreUnreachable):
        await _ship(mac, shipment, cloud)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_blob_the_store_no_longer_holds_leaves_the_route(machines, store, monkeypatch):
    """The registry can name a blob the collector already took: the GET
    answers 404, and the server still holds a route for the copy."""
    mac, cloud, a, b = machines
    _project(a)
    monkeypatch.setattr(ship, "get_signed_url", lambda key, expires: f"{store.base}/gone/{key}")
    staging = b / ".stage"
    staging.mkdir()
    shipment = cf.Shipment(
        path=str(a / "proj"), folder=True, size=0, staging=str(staging), max_bytes=10**9
    )
    with pytest.raises(StoreUnreachable):
        await _ship(mac, shipment, cloud)


@pytest.mark.asyncio
async def test_no_store_route_carries_through_the_server(machines):
    mac, cloud, a, b = machines
    top = _project(a)

    async def no_route(source, shipment, destination):
        assert Path(shipment.staging).is_dir()
        return None

    await cf.copy_file(mac, "proj", cloud, "ws/proj", ship=no_route)
    assert _tree(b / "ws/proj") == _carried(_tree(top))
    assert not _leftovers(a, b)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_read_only_folder_lands_read_only_and_leaves_nothing(machines, store):
    mac, cloud, a, b = machines
    top = a / "locked"
    (top / "inner").mkdir(parents=True)
    (top / "inner/f.txt").write_text("f\n")
    (top / "inner").chmod(0o555)
    try:
        await cf.copy_file(mac, "locked", cloud, "ws/locked", ship=_ship)
        assert _tree(b / "ws/locked") == _tree(top)
        assert not _leftovers(a, b)
    finally:
        (top / "inner").chmod(0o755)
        if (b / "ws/locked/inner").exists():
            (b / "ws/locked/inner").chmod(0o755)


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_the_computers_own_folder_is_not_read(machines, store):
    mac, cloud, a, b = machines
    (a / "_internal").mkdir()
    (a / "_internal/credentials").write_text("secret\n")
    with pytest.raises(ComputerUnreachable):
        await cf.copy_file(mac, f"{a}/_internal", cloud, "ws/x", ship=_ship)
    shipment = cf.Shipment(
        path=str(a / "_internal"),
        folder=True,
        size=0,
        staging=str(b),
        max_bytes=10**9,
        same_as=(str(a / "_internal"),),
    )
    with pytest.raises(ComputerUnreachable, match="one of the computer's own folders"):
        await _ship(mac, shipment, cloud)
    assert store.puts == 0


#: A runtime from before a copy's rules: its scan ignores them and reports
#: nothing of the folder itself.
_STALE_RUNTIME = """
import importlib.util, sys
found = importlib.util.spec_from_file_location("rt", {real!r})
rt = importlib.util.module_from_spec(found)
found.loader.exec_module(rt)
def scan(spec):
    for key in ("tail_base", "exclude_tails", "exclude_name_patterns", "exclude_same_as"):
        spec.pop(key, None)
    out = rt.scan(spec)
    for key in ("root_mode", "root_mtime_ns", "root_left_out"):
        out.pop(key, None)
    return out
rt._OPS["scan"] = scan
sys.exit(rt._main(sys.argv))
"""


@pytest.mark.enable_socket
@pytest.mark.asyncio
async def test_a_runtime_from_before_a_copys_rules_is_replaced_before_it_scans(machines, store):
    mac, cloud, a, b = machines
    top = _project(a)
    script = Path(transfer._script_path(mac.sandbox))
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(_STALE_RUNTIME.format(real=str(transfer._TRANSFER_RUNTIME_SOURCE)))
    await cf.copy_file(mac, "proj", cloud, "ws/proj", ship=_ship)
    assert _tree(b / "ws/proj") == _carried(_tree(top))
    assert script.read_bytes() == transfer._TRANSFER_RUNTIME_SOURCE.read_bytes()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raised, crosses",
    [
        (RuntimeError("the registry is down"), False),
        (StoreUnreachable("no store"), False),
        (ComputerUnreachable("it changed while it was copied"), True),
        (cf.ShipmentTooLarge(5), True),
    ],
)
async def test_only_a_refusal_for_the_agent_leaves_the_store_route(
    machines, monkeypatch, raised, crosses
):
    from src.server.services import computer_reach as reach

    mac, cloud, _, _ = machines

    async def backed_up(computer_id):
        return {"files_backup": True}

    async def failing(*args, **kwargs):
        raise raised

    monkeypatch.setattr(reach, "get_computer", backed_up)
    monkeypatch.setattr(reach, "transfer_mode", lambda sandbox: "direct")
    monkeypatch.setattr(reach, "ship_via_store", failing)
    turn = reach.TurnComputers(USER, None, None)
    shipment = cf.Shipment(path="/x", folder=True, size=0, staging="/s", max_bytes=1)
    if crosses:
        with pytest.raises(type(raised)):
            await turn.ship(mac, shipment, cloud)
    else:
        assert await turn.ship(mac, shipment, cloud) is None
