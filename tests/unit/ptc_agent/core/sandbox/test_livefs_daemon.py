"""What the file mount's daemon promises, pinned without FUSE, root or a server.

The daemon runs as root in the sandbox, so its link handling decides what
happens to files the agent already had there, and its save rules decide
which refusals reach the tool result. A dict-backed server stands in for the
endpoint and a fake kernel for the FUSE context and ``/proc``; ``start`` and
``link`` run against ``tmp_path`` with the checks that need root or a mount
table answering as a healthy sandbox would.
"""

from __future__ import annotations

import errno
import hashlib
import http.server
import itertools
import json
import os
import shutil
import stat
import sys
import threading
import time
from types import SimpleNamespace

import pytest

# The sandbox runs this package as ``livefs``; its siblings are relative
# imports, so the host path loads the same code.
from ptc_agent.core.sandbox.livefs_runtime import (
    boot,
    daemon,
    lifecycle,
    links,
    ops,
    remote,
    views,
)
from ptc_agent.core.sandbox.livefs_runtime.protocol import (
    CALL_ENV,
    CONFIG_ENV,
    MAX_FILE_BYTES,
    PROVISIONAL_HEADER,
    MountError,
    etag_version,
)
from tests.unit.restored_overlay import archive, refuse_here

NOTES = "user/memory/notes.md"
PREFS = "user/profile/prefs.json"


def _error(code: str) -> bytes:
    return json.dumps({"code": code, "message": code}).encode()


def _version(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _json_only(path: str, body: bytes) -> bool:
    try:
        json.loads(body)
    except ValueError:
        return False
    return True


class _Server:
    """The livefs endpoint over a dict of mount-relative paths to bytes.
    ``store`` is what a save keeps of the bytes sent (None: the save deleted
    the file, as an automation's ``"status": "deleted"`` does), ``version`` what names
    a file's bytes, ``structural`` the directories it makes up,
    ``inline`` whether a listing carries each file's text, ``trees`` the
    directories whose listing carries every one below it, ``read_only``
    the files no save may change, and ``etag`` the tag a read's version
    reaches the daemon as (None: dropped on the way)."""

    def __init__(
        self,
        files,
        *,
        sealed=(),
        accept=lambda path, body: True,
        store=lambda path, body: body,
        version=_version,
        structural=(),
        inline=False,
        trees=(),
        read_only=(),
        etag=lambda version: f'"{version}"',
    ) -> None:
        self.files = dict(files)
        self.sealed = set(sealed)
        self.accept = accept
        self.store = store
        self.version = version
        self.structural = set(structural)
        self.inline = inline
        self.trees = set(trees)
        self.read_only = set(read_only)
        self.etag = etag
        self.writes: list[tuple[str, bytes, dict]] = []
        self.reads: list[str] = []
        self.lists: list[str] = []

    def request(self, method, action, params, *, body=None, headers=None, call=None, attempts=2):
        return getattr(self, f"_{action}")(params.get("path", ""), params, body, headers or {})

    def _entries(self, path):
        prefix = f"{path}/" if path else ""
        entries = {}
        for key, data in self.files.items():
            if key.startswith(prefix):
                name, nested, _ = key[len(prefix):].partition("/")
                entries[name] = {"name": name, "type": "dir"} if nested else {
                    "name": name,
                    "type": "file",
                    "size": len(data),
                    "version": self.version(data),
                    "writable": key not in self.read_only,
                    **({"content": data.decode()} if self.inline else {}),
                }
        return list(entries.values())

    def _list(self, path, params, body, headers):
        self.lists.append(path)
        entries = self._entries(path)
        if path and not entries:
            return 404, None, _error("not_found")
        listing = {
            "entries": entries,
            "writable": path not in self.sealed,
            "structural": path in self.structural,
        }
        if path in self.trees:
            below = {
                "/".join(parts[:depth])
                for key in self.files
                if key.startswith(f"{path}/")
                for parts in [key[len(path) + 1 :].split("/")[:-1]]
                for depth in range(1, len(parts) + 1)
            }
            listing["below"] = {
                folder: {"entries": self._entries(f"{path}/{folder}"), "writable": False}
                for folder in below
            }
        return 200, None, json.dumps(listing).encode()

    def _read(self, path, params, body, headers):
        self.reads.append(path)
        if path not in self.files:
            return 404, None, _error("not_found")
        data = self.files[path]
        tag = self.etag(self.version(data))
        headers = {} if tag is None else {"ETag": tag}
        resp = SimpleNamespace(getheader=lambda name, default=None: headers.get(name, default))
        return 200, resp, data

    def _write(self, path, params, body, headers):
        self.writes.append((path, body, headers))
        if not self.accept(path, body):
            return 422, None, _error("invalid")
        stored = self.store(path, body)
        if stored is None:
            self.files.pop(path, None)
            return 200, None, json.dumps({"removed": True}).encode()
        self.files[path] = stored
        saved = {"version": self.version(stored), "size": len(stored), "as_sent": stored == body}
        return 200, None, json.dumps(saved).encode()

    def _delete(self, path, params, body, headers):
        self.files.pop(path)
        return 204, None, b""

    def _rename(self, path, params, body, headers):
        if any(key.startswith(f"{path}/") for key in self.files):
            return 409, None, _error("is_directory")
        self.files[params["to"]] = self.files.pop(path)
        return 204, None, b""


class _Kernel:
    """The pid FUSE names as a request's caller, and the call each pid
    started under."""

    def __init__(self) -> None:
        self.pid = 1
        self.calls: dict[int, str] = {}

    def run(self, pid: int, call: str) -> None:
        self.calls[pid] = call
        self.pid = pid


@pytest.fixture
def kernel():
    return _Kernel()


def _ops(server: _Server, kernel: _Kernel, **options) -> ops.LiveFS:
    return ops.LiveFS(
        server,
        os.getuid(),
        os.getgid(),
        caller=lambda: kernel.pid,
        call_of=kernel.calls.get,
        **options,
    )


def _walk(fs: ops.LiveFS, path: str) -> dict:
    """The lookups the kernel sends for a path, one per component."""
    parts = path.strip("/").split("/")
    for depth in range(1, len(parts)):
        fs.getattr("/" + "/".join(parts[:depth]))
    return fs.getattr(path)


def _read(fs: ops.LiveFS, path: str) -> bytes:
    fh = fs.open(path, os.O_RDONLY)
    try:
        return fs.read(path, 1 << 20, 0, fh)
    finally:
        fs.release(path, fh)


def _sent(server: _Server) -> list[tuple[bytes, bool]]:
    return [(body, PROVISIONAL_HEADER in headers) for _, body, headers in server.writes]


@pytest.fixture
def box(tmp_path, monkeypatch):
    """A sandbox in ``tmp_path`` whose daemon is already serving, holding a
    token from an earlier ``start``."""
    paths = lifecycle.Paths(
        mount=str(tmp_path / "mnt" / "livefs"),
        generations=str(tmp_path / "mnt" / ".livefs"),
        private=str(tmp_path / "var" / "lib" / "livefs"),
    )
    private = tmp_path / "var" / "lib" / "livefs"
    private.mkdir(parents=True)
    old = {"base_url": "http://server", "token": "lfs1.old"}
    (private / "config.json").write_text(json.dumps(old))
    mount = tmp_path / "mnt" / "livefs"
    mount.parent.mkdir()
    root = tmp_path / "home" / "workspace"
    root.mkdir(parents=True)
    monkeypatch.setattr(daemon, "unsupported", lambda: None)
    monkeypatch.setattr(daemon, "has_libfuse", lambda: True)
    monkeypatch.setattr(daemon, "healthy", lambda path: True)
    monkeypatch.setattr(daemon, "_probe", lambda paths: None)
    monkeypatch.setattr(daemon, "start", lambda args, state, paths, gate=None: (False, None))
    return SimpleNamespace(tmp=tmp_path, private=private, mount=mount, root=root, paths=paths)


_NEW_CONFIG = json.dumps({"base_url": "http://server", "token": "lfs1.new"})


def _start(box, *, config=None, base_url=None) -> dict:
    return daemon.start_mount(
        SimpleNamespace(root=str(box.root), config=config, base_url=base_url), box.paths
    )


def _link(box, *links) -> dict:
    return daemon.link_folders(
        SimpleNamespace(
            root=str(box.root),
            link=[f"{source}:{box.root / target}" for source, target in links],
        ),
        box.paths,
    )


def _installed_token(box) -> str:
    return json.loads((box.private / "config.json").read_text())["token"]


# -- new token -----------------------------------------------------------------


def test_a_new_token_is_kept_privately(box):
    assert _start(box, config=_NEW_CONFIG)["ok"]

    assert _installed_token(box) == "lfs1.new"
    assert stat.S_IMODE((box.private / "config.json").stat().st_mode) == 0o600


def test_the_token_is_taken_out_of_the_environment_the_daemon_inherits(
    box, monkeypatch
):
    seen = []
    monkeypatch.setenv(CONFIG_ENV, _NEW_CONFIG)
    monkeypatch.setattr(sys, "argv", ["livefs", "start", "--root", str(box.root)])
    monkeypatch.setattr(daemon, "Paths", lambda: box.paths)
    monkeypatch.setattr(
        daemon,
        "start_mount",
        lambda args, paths: seen.append((args.config, os.environ.get(CONFIG_ENV)))
        or {"ok": True},
    )

    assert daemon.main() == 0
    assert seen == [(_NEW_CONFIG, None)]


def test_a_moved_server_is_written_into_the_config_and_the_token_kept(box, monkeypatch):
    probes = []
    monkeypatch.setattr(daemon, "_probe", lambda paths: probes.append(1))
    config = box.private / "config.json"
    before = json.loads(config.read_text())

    assert _start(box, base_url="http://moved")["ok"]
    assert json.loads(config.read_text()) == {**before, "base_url": "http://moved"}
    assert probes == [1]

    assert _start(box, base_url="http://moved")["ok"]
    assert probes == [1]


def test_a_server_found_unreachable_is_asked_again_until_it_answers(box, monkeypatch):
    """Nothing about a serving daemon changes when its server stops
    answering, so the sandbox keeps the failed answer: whichever host asks
    next, the probe runs again."""
    answers = ["no answer from http://server", "no answer from http://server", None]
    probes = []

    def probe(paths):
        probes.append(1)
        return answers.pop(0)

    monkeypatch.setattr(daemon, "_probe", probe)
    assert _start(box, config=_NEW_CONFIG)["error"] == "unreachable"
    assert _start(box)["error"] == "unreachable"
    assert _start(box)["ok"]
    assert _start(box)["ok"]

    assert len(probes) == 3


def test_a_start_that_waited_on_one_finding_the_server_unreachable_asks_it_too(
    box, monkeypatch
):
    """Two starts can overlap (an exec retried on the transport): the second
    may find nothing changed before the first recorded its failed probe, so
    the failure is read again under the lock."""
    probes = []

    def probe(paths):
        probes.append(1)
        return None

    monkeypatch.setattr(daemon, "_probe", probe)
    args = SimpleNamespace(root=str(box.root), config=None, base_url=None)
    refused, early = daemon._ready(args, box.paths)
    assert refused is None and early is None
    # The first start records its failure while the second waits on the lock.
    lifecycle.write_state(box.paths, {"unreachable": "no answer from http://server"})

    state = lifecycle.read_state(box.paths)
    started, refused = daemon._serving(args, box.paths, state, early)

    assert refused is None and probes == [1]
    assert "unreachable" not in state


@pytest.mark.parametrize(
    "config", [json.dumps({"base_url": "http://server"}), "not json"], ids=["no-token", "not-json"]
)
def test_a_config_that_is_not_a_mount_config_answers_bad_config(box, config):
    assert _start(box, config=config)["error"] == "bad_config"
    assert _installed_token(box) == "lfs1.old"


# -- start and link apart -----------------------------------------------------


def test_start_serves_and_leaves_every_link_alone(box):
    _link(box, ("user", ".agents/user"))
    link = box.root / ".agents" / "user"
    before = os.readlink(link)

    assert _start(box) == {
        "ok": True,
        "error": None,
        "started": None,
    }
    assert os.readlink(link) == before


def test_link_lays_the_links_without_the_daemon_or_its_token(box, monkeypatch):
    """The links point through the mount's own link, so they are right
    whatever the daemon is doing, and laying them asks nothing of it."""

    def untouched(*args, **kwargs):
        raise AssertionError("link reached the daemon")

    monkeypatch.setattr(daemon, "start", untouched)
    monkeypatch.setattr(daemon, "_probe", untouched)
    config = (box.private / "config.json").read_text()

    assert _link(box, ("user", ".agents/user")) == {
        "ok": True,
        "error": None,
        "failed": None,
        "set_aside": None,
    }
    assert os.readlink(box.root / ".agents" / "user") == f"{box.mount}/user"
    assert (box.private / "config.json").read_text() == config


def _shipped(tmp_path):
    """A shipped package as the asset sync leaves it, and the host's manifest."""
    shipped = tmp_path / "src" / "livefs"
    shutil.copytree(lifecycle._PACKAGE_DIR, shipped, ignore=shutil.ignore_patterns("__pycache__"))
    return shipped, lifecycle.code_manifest()


def test_root_runs_a_private_copy_of_the_code_the_host_ships(tmp_path):
    shipped, manifest = _shipped(tmp_path)
    private = tmp_path / "private"
    code = lifecycle.code_version()

    home = boot.home(str(private), str(shipped), code, manifest)

    assert home == str(private / "code" / code)
    assert sorted(os.listdir(os.path.join(home, "livefs"))) == sorted(manifest)
    assert stat.S_IMODE(os.stat(private / "code").st_mode) == 0o700
    assert boot.home(str(private), str(shipped), code, manifest) == home


@pytest.mark.parametrize("change", ["rewrite", "link"])
def test_shipped_code_the_host_did_not_ship_is_never_run(tmp_path, change):
    """The sandbox's user can rewrite the shipped package, and a sync
    rewrites it file by file, so root runs nothing that reads otherwise."""
    shipped, manifest = _shipped(tmp_path)
    ops = shipped / "ops.py"
    if change == "rewrite":
        ops.write_text("import os\n")
    else:
        (tmp_path / "ops.py").write_bytes(ops.read_bytes())
        ops.unlink()
        ops.symlink_to(tmp_path / "ops.py")
    private = tmp_path / "private"

    assert boot.home(str(private), str(shipped), lifecycle.code_version(), manifest) is None
    assert not (private / "code" / lifecycle.code_version()).exists()


def test_boot_answers_stale_code_as_the_daemon_names_it():
    source = open(boot.__file__).read()

    assert f'"error": "{MountError.STALE_CODE}"' in source


def test_installing_libfuse_waits_out_the_users_apt_and_keeps_their_lists(
    tmp_path, monkeypatch
):
    """The install runs on the user's machine at any time, maybe beside an
    apt-get of their own, so it waits for the lock and wipes nothing."""
    launched: list[str] = []
    monkeypatch.setattr(
        lifecycle.subprocess, "Popen", lambda argv, **kwargs: launched.append(argv[-1])
    )
    paths = lifecycle.Paths(private=str(tmp_path))

    lifecycle.install_libfuse(paths)
    lifecycle.install_libfuse(paths)

    [script] = launched
    calls = script.split("&&")
    assert len(calls) == 2
    assert all("apt-get -o DPkg::Lock::Timeout=120 " in call for call in calls)
    assert "rm " not in script and "/var/lib/apt/lists" not in script


# -- links ---------------------------------------------------------------------


def test_a_restart_repoints_the_mount_path_and_leaves_every_link_alone(box):
    generations = box.tmp / "mnt" / ".livefs"
    for name, text in (("1", "old daemon"), ("2", "new daemon")):
        (generations / name / "user").mkdir(parents=True)
        (generations / name / "user" / "notes.md").write_text(text)
    lifecycle.point(str(generations / "1"), box.paths)
    _link(box, ("user", ".agents/user"))
    link = box.root / ".agents" / "user"
    assert (link / "notes.md").read_text() == "old daemon"

    lifecycle.point(str(generations / "2"), box.paths)

    assert os.readlink(link) == f"{box.mount}/user"
    assert (link / "notes.md").read_text() == "new daemon"
    assert sorted(os.listdir(box.tmp / "mnt")) == [".livefs", "livefs"]


def test_link_removes_its_links_no_longer_asked_for_but_not_what_replaced_one(box):
    _link(
        box,
        ("user", ".agents/user"),
        ("workspaces/a/memory", "research/.agents/memory"),
        ("workspaces/b/memory", "archive/.agents/memory"),
    )
    # The agent repointed one link at a folder of its own.
    mine = box.root / "my-notes"
    mine.mkdir()
    repointed = box.root / "archive" / ".agents" / "memory"
    repointed.unlink()
    repointed.symlink_to(mine)

    assert _link(box, ("user", ".agents/user"))["ok"]

    assert os.path.islink(box.root / ".agents" / "user")
    assert not os.path.lexists(box.root / "research" / ".agents" / "memory")
    assert os.readlink(repointed) == str(mine)


def test_anything_already_at_a_link_path_is_set_aside_under_a_free_name(box):
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    (memory.parent / "memory.local").mkdir()  # an earlier set-aside
    index = box.root / ".agents" / "threads.jsonl"
    index.parent.mkdir()
    index.write_text("{}\n")

    answer = _link(
        box,
        ("workspaces/a/memory", "research/.agents/memory"),
        ("computer/threads.jsonl", ".agents/threads.jsonl"),
    )

    assert answer["ok"]
    assert answer["set_aside"] == {
        str(memory): f"{memory}.local.1",
        str(index): f"{index}.local",
    }
    assert (memory.parent / "memory.local.1" / "notes.md").read_text() == "kept"
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"


def test_a_folder_a_restored_sandbox_cannot_rename_is_copied_aside(box, monkeypatch):
    """Restored from its archive, the sandbox refuses to rename the folder
    (EXDEV), so it is copied beside the link and deleted where it is."""
    memory = box.root / "research" / ".agents" / "memory"
    (memory / "topics").mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    (memory / "topics" / "a.md").write_text("also kept")
    # The agent's own, under the name an earlier build copied through.
    (memory.parent / "memory.local.copying").mkdir()
    (memory.parent / "memory.local.copying" / "mine.md").write_text("mine")
    refuse_here(monkeypatch, archive(box.root))

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["ok"]
    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    assert (memory.parent / "memory.local" / "topics" / "a.md").read_text() == "also kept"
    assert (memory.parent / "memory.local.copying" / "mine.md").read_text() == "mine"
    assert not [p for p in memory.parent.iterdir() if p.name.startswith(".memory.local")]
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"


def test_a_read_only_folder_in_one_a_restored_sandbox_cannot_rename_is_deleted(box, monkeypatch):
    """A folder inside the agent made read-only refuses the delete after the
    copy until it is opened up; left there, every later link copied it again."""
    memory = box.root / "research" / ".agents" / "memory"
    (memory / "topics").mkdir(parents=True)
    (memory / "topics" / "a.md").write_text("kept")
    (memory / "topics").chmod(0o500)
    refuse_here(monkeypatch, archive(box.root))

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["ok"]
    assert (memory.parent / "memory.local" / "topics" / "a.md").read_text() == "kept"
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory", "memory.local"]
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"


def test_a_named_pipe_in_one_a_restored_sandbox_cannot_rename_is_made_anew(box, monkeypatch):
    """copytree refuses a pipe, which failed the copy and the link on every
    pass; the copy makes it anew, as the folder script's does."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    os.mkfifo(memory / "feed")
    # More than the umask lets mkfifo make.
    (memory / "feed").chmod(0o666)
    refuse_here(monkeypatch, archive(box.root))

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["ok"]
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    feed = os.lstat(memory.parent / "memory.local" / "feed")
    assert stat.S_ISFIFO(feed.st_mode) and stat.S_IMODE(feed.st_mode) == 0o666
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"


def test_a_file_with_two_names_in_one_a_restored_sandbox_cannot_rename_stays_one(
    box, monkeypatch
):
    """A rename keeps the two names one file; a copy of each would take its
    size twice and let an edit through one name miss the other."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    os.link(memory / "notes.md", memory / "again.md")
    refuse_here(monkeypatch, archive(box.root))

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["ok"]
    aside = memory.parent / "memory.local"
    notes, again = (aside / "notes.md").lstat(), (aside / "again.md").lstat()
    assert (notes.st_ino, notes.st_nlink) == (again.st_ino, 2)
    assert (aside / "again.md").read_text() == "kept"


def test_a_copy_a_killed_pass_left_goes_before_the_next(box, monkeypatch):
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    (memory.parent / ".memory.local.copying-ab12cd_9" / "half").mkdir(parents=True)
    refuse_here(monkeypatch, archive(box.root))

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["ok"]
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory", "memory.local"]


def test_an_empty_directory_at_a_link_path_is_replaced_not_set_aside(box):
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] is None
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"
    assert not os.path.lexists(f"{memory}.local")


def test_a_folder_copied_aside_whose_delete_fails_is_not_copied_again(box, monkeypatch):
    """The link is retried every few minutes. Copied again on each, what the
    delete cannot remove would take its size again every time."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    remove, refuse = links.tree_runtime.remove_tree, [True]

    def stuck(top, deadline=None):
        if refuse[0] and top == str(memory):
            raise PermissionError(errno.EACCES, "Permission denied", top)
        return remove(top, deadline)

    monkeypatch.setattr(links.tree_runtime, "remove_tree", stuck)
    for _ in range(2):
        assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory", "memory.local"]

    refuse[0] = False
    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"
    assert json.loads((box.private / "state.json").read_text())["copied"] == {}


@pytest.mark.parametrize("written_since", [False, True])
def test_a_failed_delete_looks_inside_a_folder_shut_to_its_owner(box, monkeypatch, written_since):
    """Whether the copy holds the folder is asked inside one its owner made
    unreadable, as the copy and the delete both go inside it: unasked, a file
    written there since would be deleted, and taken for one, a folder holding
    nothing new would be copied aside again."""
    memory = box.root / "research" / ".agents" / "memory"
    locked = memory / "locked"
    locked.mkdir(parents=True)
    (locked / "secret.md").write_text("shut")
    locked.chmod(0)
    refuse_here(monkeypatch, archive(box.root))
    remove, refuse = links.tree_runtime.remove_tree, [True]

    def stuck(top, deadline=None):
        if refuse[0] and top == str(memory):
            raise PermissionError(errno.EACCES, "Permission denied", top)
        return remove(top, deadline)

    monkeypatch.setattr(links.tree_runtime, "remove_tree", stuck)
    assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    if written_since:
        locked.chmod(0o700)
        (locked / "since.md").write_text("written since")
        locked.chmod(0)
    refuse[0] = False

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    aside = memory.parent / ("memory.local.1" if written_since else "memory.local")
    assert answer["set_aside"] == {str(memory): str(aside)}
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"
    assert stat.S_IMODE((aside / "locked").lstat().st_mode) == 0
    for copy in memory.parent.glob("memory.local*/locked"):
        copy.chmod(0o700)
    assert (aside / "locked" / "secret.md").read_text() == "shut"
    if written_since:
        assert (aside / "locked" / "since.md").read_text() == "written since"


def test_what_is_written_to_a_folder_whose_delete_failed_is_set_aside_not_deleted(
    box, monkeypatch
):
    """The link stays out until the delete is done, so the folder at its path
    stays in use. The copy no longer holds what was written there since, which
    the next link sets aside anew rather than deleting."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    remove, refuse = links.tree_runtime.remove_tree, [True]

    def stuck(top, deadline=None):
        if refuse[0] and top == str(memory):
            raise PermissionError(errno.EACCES, "Permission denied", top)
        return remove(top, deadline)

    monkeypatch.setattr(links.tree_runtime, "remove_tree", stuck)
    assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    (memory / "since.md").write_text("written since")
    refuse[0] = False

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local.1"}
    assert (memory.parent / "memory.local.1" / "since.md").read_text() == "written since"
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"
    assert json.loads((box.private / "state.json").read_text())["copied"] == {}


@pytest.mark.parametrize("change", ["folder made", "mode changed"])
def test_a_folder_changed_where_the_delete_failed_is_set_aside_not_deleted(
    box, monkeypatch, change
):
    """A folder made there since the copy, or one whose mode changed, is as
    much what was written there as a file is, and the copy holds neither."""
    memory = box.root / "research" / ".agents" / "memory"
    (memory / "topics").mkdir(parents=True)
    (memory / "topics" / "a.md").write_text("kept")
    (memory / "topics").chmod(0o755)
    refuse_here(monkeypatch, archive(box.root))
    remove, refuse = links.tree_runtime.remove_tree, [True]

    def stuck(top, deadline=None):
        if refuse[0] and top == str(memory):
            raise PermissionError(errno.EACCES, "Permission denied", top)
        return remove(top, deadline)

    monkeypatch.setattr(links.tree_runtime, "remove_tree", stuck)
    assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    if change == "folder made":
        (memory / "drafts").mkdir()
    else:
        (memory / "topics").chmod(0o700)
    refuse[0] = False

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    aside = memory.parent / "memory.local.1"
    assert answer["set_aside"] == {str(memory): str(aside)}
    if change == "folder made":
        assert (aside / "drafts").is_dir()
    else:
        assert stat.S_IMODE((aside / "topics").lstat().st_mode) == 0o700
    assert (memory.parent / "memory.local" / "topics" / "a.md").read_text() == "kept"
    assert os.readlink(memory) == f"{box.mount}/workspaces/a/memory"


@pytest.mark.skipif(os.geteuid() == 0, reason="root enters a folder shut to its owner as it is")
def test_a_delete_cut_short_in_a_folder_shut_to_its_owner_is_not_copied_again(
    box, monkeypatch
):
    """The delete lets its owner into such a folder, and cut short there it
    stays so: taken for a change to the folder, that would copy it aside
    again on every link."""
    memory = box.root / "research" / ".agents" / "memory"
    locked = memory / "locked"
    locked.mkdir(parents=True)
    (locked / "secret.md").write_text("shut")
    locked.chmod(0)
    refuse_here(monkeypatch, archive(box.root))
    unlink, refuse = os.unlink, [True]

    def stuck(name, *args, **kwargs):
        if refuse[0] and name == "secret.md":
            raise PermissionError(errno.EACCES, "Permission denied", name)
        return unlink(name, *args, **kwargs)

    monkeypatch.setattr(links.tree_runtime.os, "unlink", stuck)
    assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    assert stat.S_IMODE(locked.lstat().st_mode) == 0o700
    refuse[0] = False

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory", "memory.local"]
    aside = memory.parent / "memory.local" / "locked"
    assert stat.S_IMODE(aside.lstat().st_mode) == 0
    aside.chmod(0o700)
    assert (aside / "secret.md").read_text() == "shut"


def test_a_copy_aside_whose_delete_is_killed_is_not_copied_again(box, monkeypatch):
    """Killed partway through the delete after the copy, the next link finds
    the copy recorded and finishes the delete, rather than copying what is
    left into another folder beside it."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    remove = links.tree_runtime.remove_tree

    def killed(top, deadline=None):
        if top == str(memory):
            raise SystemExit("killed")
        return remove(top, deadline)

    monkeypatch.setattr(links.tree_runtime, "remove_tree", killed)
    with pytest.raises(SystemExit):
        _link(box, ("workspaces/a/memory", "research/.agents/memory"))
    monkeypatch.setattr(links.tree_runtime, "remove_tree", remove)

    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory", "memory.local"]
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    assert json.loads((box.private / "state.json").read_text())["copied"] == {}


def test_a_copy_aside_whose_delete_is_killed_is_finished_once_its_link_is_dropped(
    box, monkeypatch
):
    """No link comes to the path again to finish the delete, so the pass that
    drops it does, and keeps the record until it has: else the folder half
    deleted would stay beside its whole copy for good."""
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    remove = links.tree_runtime.remove_tree

    def failing(error):
        def remove_tree(top, deadline=None):
            if top == str(memory):
                raise error
            return remove(top, deadline)

        return remove_tree

    monkeypatch.setattr(links.tree_runtime, "remove_tree", failing(SystemExit("killed")))
    with pytest.raises(SystemExit):
        _link(box, ("workspaces/a/memory", "research/.agents/memory"))
    monkeypatch.setattr(
        links.tree_runtime, "remove_tree", failing(PermissionError(13, "denied"))
    )
    assert _link(box)["ok"]
    assert str(memory) in json.loads((box.private / "state.json").read_text())["copied"]
    monkeypatch.setattr(links.tree_runtime, "remove_tree", remove)

    assert _link(box)["ok"]

    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory.local"]
    assert (memory.parent / "memory.local" / "notes.md").read_text() == "kept"
    assert json.loads((box.private / "state.json").read_text())["copied"] == {}


def test_a_file_written_where_a_copy_aside_already_passed_is_in_it(box, monkeypatch):
    """A process at work in the folder can write where the copy has been, and
    the folder is deleted once its copy is whole."""
    memory = box.root / "research" / ".agents" / "memory"
    (memory / "topic").mkdir(parents=True)
    (memory / "topic" / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    sync_entry, made = links.tree_runtime._sync_entry, []

    def write_behind(entry, target, deadline, linked):
        # The top folder's listing is done before any folder below it.
        if os.path.dirname(entry.path) != str(memory) and not made:
            made.append((memory / "late.md").write_text("late"))
        return sync_entry(entry, target, deadline, linked)

    monkeypatch.setattr(links.tree_runtime, "_sync_entry", write_behind)
    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert (memory.parent / "memory.local" / "late.md").read_text() == "late"
    assert (memory.parent / "memory.local" / "topic" / "notes.md").read_text() == "kept"


def test_a_folder_still_changing_as_it_is_copied_aside_waits_for_the_next_link(
    box, monkeypatch
):
    memory = box.root / "research" / ".agents" / "memory"
    memory.mkdir(parents=True)
    (memory / "notes.md").write_text("kept")
    refuse_here(monkeypatch, archive(box.root))
    sync_entry, writes = links.tree_runtime._sync_entry, itertools.count()

    def busy(entry, target, deadline, linked):
        (memory / f"log-{next(writes)}.txt").write_text("more")
        return sync_entry(entry, target, deadline, linked)

    monkeypatch.setattr(links.tree_runtime, "_sync_entry", busy)
    assert not _link(box, ("workspaces/a/memory", "research/.agents/memory"))["ok"]
    assert sorted(p.name for p in memory.parent.iterdir()) == ["memory"]
    assert (memory / "notes.md").read_text() == "kept"

    monkeypatch.setattr(links.tree_runtime, "_sync_entry", sync_entry)
    answer = _link(box, ("workspaces/a/memory", "research/.agents/memory"))

    assert answer["set_aside"] == {str(memory): f"{memory}.local"}
    assert len(list((memory.parent / "memory.local").iterdir())) == next(writes) + 1


def test_root_lays_the_links_as_the_folders_owner(box, monkeypatch):
    """Root following a symlink the user planted could write anywhere."""
    calls, made = [], []
    monkeypatch.setattr(daemon.os, "geteuid", lambda: 0)
    monkeypatch.setattr(daemon.os, "getegid", lambda: 0)
    monkeypatch.setattr(daemon.os, "getgroups", lambda: [0])
    monkeypatch.setattr(daemon.os, "setgroups", lambda g: calls.append(("groups", g)))
    monkeypatch.setattr(daemon.os, "setegid", lambda g: calls.append(("egid", g)))
    monkeypatch.setattr(daemon.os, "seteuid", lambda u: calls.append(("euid", u)))
    monkeypatch.setattr(
        daemon, "link", lambda value, target, copied, keep: made.append(list(calls))
    )
    owner = box.root.stat()

    _link(box, ("user", ".agents/user"))

    as_owner = [("groups", []), ("egid", owner.st_gid), ("euid", owner.st_uid)]
    assert made == [as_owner]
    assert calls == as_owner + [("euid", 0), ("egid", 0), ("groups", [0])]


# -- server answers ------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (404, _error("not_found"), errno.ENOENT),
        (409, _error("is_directory"), errno.EISDIR),
        (409, _error("not_directory"), errno.ENOTDIR),
        (412, _error("exists"), errno.EEXIST),
        (412, _error("changed"), errno.ESTALE),
        (428, _error("precondition_required"), errno.EINVAL),
        (403, _error("read_only"), errno.EACCES),
        (422, _error("invalid"), errno.EINVAL),
        (413, _error("too_large"), errno.EFBIG),
        (503, _error("unavailable"), errno.EIO),
        (502, b"<html>bad gateway</html>", errno.EIO),
    ],
)
def test_a_server_error_reaches_the_caller_as_its_errno(status, body, expected):
    assert remote.error_for(status, body).errno == expected


@pytest.mark.parametrize(
    ("tag", "version"),
    [('"v1"', "v1"), ('W/"v1"', "v1"), ("v1", "v1"), (None, None), ("", None), ('W/""', None)],
)
def test_a_tag_names_its_version_whether_or_not_a_proxy_marked_it_weak(tag, version):
    assert etag_version(tag) == version


@pytest.mark.parametrize("tag", ['"{}"', 'W/"{}"'])
def test_an_edit_saves_over_the_version_its_read_named_even_marked_weak(kernel, tag):
    # Regression: a CDN that decompressed the read marked its ETag weak, the
    # save named that as the version, and every edit was refused as changed.
    server = _Server({NOTES: b"first"}, etag=tag.format)
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_APPEND)
    fs.write(f"/{NOTES}", b" second", 0, fh)
    fs.release(f"/{NOTES}", fh)

    assert [h.get("If-Match") for _, _, h in server.writes] == [f'"{_version(b"first")}"']
    assert server.files[NOTES] == b"first second"


def test_a_read_whose_version_was_dropped_fails_and_no_edit_makes_the_file_anew(kernel):
    # Regression: read with no version, an edit was saved as a new file and
    # refused as one that exists.
    server = _Server({NOTES: b"first"}, etag=lambda version: None)
    fs = _ops(server, kernel)

    for flags in (os.O_RDONLY, os.O_WRONLY | os.O_APPEND):
        with pytest.raises(OSError) as exc:
            fs.open(f"/{NOTES}", flags)
        assert exc.value.errno == errno.EIO
    assert server.writes == []


def test_renaming_a_directory_answers_exdev_so_mv_copies_it_file_by_file(kernel):
    server = _Server({"user/memory/notes/a.md": b"a"})

    with pytest.raises(OSError) as exc:
        _ops(server, kernel).rename("/user/memory/notes", "/user/memory/archive")

    assert exc.value.errno == errno.EXDEV
    assert server.files == {"user/memory/notes/a.md": b"a"}


@pytest.mark.parametrize(
    "make",
    [lambda fs, path: fs.create(path, 0o644), lambda fs, path: fs.mkdir(path, 0o755)],
    ids=["create", "mkdir"],
)
def test_a_directory_that_takes_no_new_files_refuses_create_and_mkdir_up_front(kernel, make):
    server = _Server({"user/memos/q3.md": b"memo", NOTES: b"n"}, sealed={"user/memos"})
    fs = _ops(server, kernel)
    make(fs, "/user/memory/fresh")

    with pytest.raises(OSError) as exc:
        make(fs, "/user/memos/fresh")

    assert exc.value.errno == errno.EACCES
    assert server.writes == []


def test_a_delete_leaves_the_directories_above_it_under_the_servers_rules(kernel):
    # Regression: every ancestor of a deleted file used to be kept as a local
    # directory, which lifted the create refusal and made renaming one a
    # silent local no-op.
    server = _Server(
        {"user/memory/notes/a.md": b"a", "user/memory/notes/b.md": b"b"},
        sealed={"user"},
    )
    fs = _ops(server, kernel)
    fs.unlink("/user/memory/notes/a.md")

    with pytest.raises(OSError) as create:
        fs.create("/user/new.md", 0o644)
    with pytest.raises(OSError) as rename:
        fs.rename("/user/memory/notes", "/user/memory/archive")

    assert create.value.errno == errno.EACCES
    assert rename.value.errno == errno.EXDEV
    assert server.files == {"user/memory/notes/b.md": b"b"}


def test_a_directory_its_last_delete_emptied_stays_for_the_rmdir_after_it(kernel):
    server = _Server({"user/memory/notes/a.md": b"a", NOTES: b"n"})
    fs = _ops(server, kernel)
    fs.unlink("/user/memory/notes/a.md")

    assert fs.getattr("/user/memory/notes")["st_mode"] & stat.S_IFDIR
    fs.rmdir("/user/memory/notes")
    with pytest.raises(OSError) as gone:
        fs.getattr("/user/memory/notes")
    assert gone.value.errno == errno.ENOENT


def test_a_directory_made_here_moves_with_the_ones_made_inside_it(kernel):
    fs = _ops(_Server({NOTES: b"n"}), kernel)
    fs.mkdir("/user/memory/draft", 0o755)
    fs.mkdir("/user/memory/draft/q3", 0o755)

    fs.rename("/user/memory/draft", "/user/memory/final")

    assert fs.getattr("/user/memory/final/q3")["st_mode"] & stat.S_IFDIR
    with pytest.raises(OSError) as gone:
        fs.getattr("/user/memory/draft")
    assert gone.value.errno == errno.ENOENT


def test_a_temp_file_replaced_into_place_before_its_close_lands_there(kernel):
    # Regression: the bytes stayed on the handle, which then saved to the
    # temp path the rename had emptied, so neither path held them.
    tmp = "/user/memory/.notes.md.tmp"
    server = _Server({NOTES: b"old"})
    fs = _ops(server, kernel)
    fh = fs.create(tmp, 0o644)
    fs.write(tmp, b"new", 0, fh)

    fs.rename(tmp, f"/{NOTES}")
    fs.write(f"/{NOTES}", b" and more", 3, fh)
    fs.flush(f"/{NOTES}", fh)
    fs.release(f"/{NOTES}", fh)

    assert server.files == {NOTES: b"new and more"}
    assert _read(fs, f"/{NOTES}") == b"new and more"


def test_a_command_keeps_the_view_its_call_id_first_saw(kernel):
    server = _Server({NOTES: b"v1"})
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    assert _read(fs, f"/{NOTES}") == b"v1"
    server.files[NOTES] = b"v2 from the app"

    kernel.run(102, "call-a")  # another process of the same command
    assert _read(fs, f"/{NOTES}") == b"v1"
    kernel.run(201, "call-b")
    assert _read(fs, f"/{NOTES}") == b"v2 from the app"


def test_a_directory_its_last_delete_emptied_is_gone_for_the_next_command(kernel):
    server = _Server({"user/memory/notes/a.md": b"a", NOTES: b"n"})
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    fs.unlink("/user/memory/notes/a.md")

    kernel.run(201, "call-b")
    with pytest.raises(OSError) as gone:
        fs.getattr("/user/memory/notes")

    assert gone.value.errno == errno.ENOENT


def test_the_call_is_read_from_the_environment_the_process_started_with(tmp_path):
    for pid, environ in {
        7: b"PATH=/usr/bin\0" + f"{CALL_ENV}=call-a".encode() + b"\0",
        8: b"PATH=/usr/bin\0" + f"{CALL_ENV}=".encode() + b"\0",
        9: b"PATH=/usr/bin\0",
    }.items():
        (tmp_path / str(pid)).mkdir()
        (tmp_path / str(pid) / "environ").write_bytes(environ)

    calls = [daemon.call_in_environ(pid, str(tmp_path)) for pid in (7, 8, 9, 10)]

    assert calls == ["call-a", None, None, None]


# -- saves ---------------------------------------------------------------------


def test_a_redirect_makes_one_save_of_the_bytes_its_command_wrote(kernel):
    # Regression: the shell's close of its own copy of the fd, before the
    # command writes, used to send an empty save ahead of the real one.
    server = _Server({PREFS: b'{"theme": "dark"}'}, accept=_json_only)
    fs = _ops(server, kernel)
    fh = fs.open(f"/{PREFS}", os.O_WRONLY | os.O_TRUNC)
    fs.flush(f"/{PREFS}", fh)
    assert fs.getattr(f"/{PREFS}")["st_size"] == 0

    fs.write(f"/{PREFS}", b'{"theme": "light"}', 0, fh)
    fs.flush(f"/{PREFS}", fh)
    fs.release(f"/{PREFS}", fh)

    assert _sent(server) == [(b'{"theme": "light"}', False)]
    assert server.files[PREFS] == b'{"theme": "light"}'


def test_a_redirect_nothing_followed_empties_the_file_at_release_as_a_real_save(kernel):
    server = _Server({PREFS: b'{"theme": "dark"}'}, accept=_json_only)
    fs = _ops(server, kernel)
    fh = fs.open(f"/{PREFS}", os.O_WRONLY | os.O_TRUNC)
    fs.flush(f"/{PREFS}", fh)
    fs.flush(f"/{PREFS}", fh)

    with pytest.raises(OSError):
        fs.release(f"/{PREFS}", fh)

    assert _sent(server) == [(b"", False)]


def test_a_file_made_and_removed_before_its_release_stays_removed(kernel):
    # The release that makes an unwritten file comes after close returns, so
    # a removal can reach the daemon first.
    server = _Server({NOTES: b"n"})
    fs = _ops(server, kernel)
    fh = fs.create("/user/memory/lock.md", 0o644)
    fs.flush("/user/memory/lock.md", fh)

    fs.unlink("/user/memory/lock.md")
    fs.release("/user/memory/lock.md", fh)

    assert "user/memory/lock.md" not in server.files


def test_a_save_that_deletes_its_file_leaves_it_out_of_every_view(kernel):
    # An automation saved with "status": "deleted" is gone once the save is.
    # Answered as a save, it stayed listed, and reading it failed.
    brief = "user/automations/brief.json"
    server = _Server(
        {brief: b'{"status": "active"}', "user/automations/README.md": b"doc"},
        store=lambda path, body: None if b'"deleted"' in body else body,
    )
    fs = _ops(server, kernel)
    kernel.run(201, "call-b")
    assert "brief.json" in fs.readdir("/user/automations", 0)
    kernel.run(101, "call-a")
    assert "brief.json" in fs.readdir("/user/automations", 0)
    fh = fs.open(f"/{brief}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{brief}", b'{"status": "deleted"}', 0, fh)

    fs.release(f"/{brief}", fh)

    assert brief not in server.files
    for pid, call in ((101, "call-a"), (201, "call-b")):
        kernel.run(pid, call)
        assert "brief.json" not in fs.readdir("/user/automations", 0)
        with pytest.raises(OSError) as gone:
            fs.getattr(f"/{brief}")
        assert gone.value.errno == errno.ENOENT


def test_a_refused_save_of_written_bytes_fails_the_close_and_is_not_retried(kernel):
    server = _Server({PREFS: b'{"theme": "dark"}'}, accept=_json_only)
    fs = _ops(server, kernel)
    fh = fs.open(f"/{PREFS}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{PREFS}", b"{not json", 0, fh)

    with pytest.raises(OSError) as exc:
        fs.flush(f"/{PREFS}", fh)
    fs.release(f"/{PREFS}", fh)

    assert exc.value.errno == errno.EINVAL
    assert _sent(server) == [(b"{not json", False)]


@pytest.mark.parametrize("grow", ["write", "truncate"])
def test_a_file_grown_past_the_size_limit_saves_nothing_its_handle_wrote(kernel, grow):
    # Regression: the bytes written before the refused write were saved at
    # close, so a file too large to save replaced the old one, cut short.
    server = _Server({NOTES: b"old"})
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{NOTES}", b"x" * MAX_FILE_BYTES, 0, fh)

    with pytest.raises(OSError) as refused:
        if grow == "write":
            fs.write(f"/{NOTES}", b"x", MAX_FILE_BYTES, fh)
        else:
            fs.truncate(f"/{NOTES}", MAX_FILE_BYTES + 1, fh)
    with pytest.raises(OSError) as closed:
        fs.flush(f"/{NOTES}", fh)
    fs.release(f"/{NOTES}", fh)

    assert refused.value.errno == closed.value.errno == errno.EFBIG
    assert server.writes == []
    assert server.files[NOTES] == b"old"


def test_ftruncate_under_libfuse_3_cuts_the_open_file_not_a_second_handle(kernel):
    # Regression: the truncate went through a handle of its own, and the open
    # one saved its old bytes over it, so the file kept the old tail.
    server = _Server({NOTES: b"old contents"})
    fs = _ops(server, kernel)
    fuse = SimpleNamespace(operations=fs, raw_fi=False, encoding="utf-8", errors="strict")
    fh = fs.open(f"/{NOTES}", os.O_RDWR)

    open_file = SimpleNamespace(contents=SimpleNamespace(fh=fh))
    daemon._truncate_fuse_3(fuse, f"/{NOTES}".encode(), 0, open_file)
    fs.write(f"/{NOTES}", b"new", 0, fh)
    fs.flush(f"/{NOTES}", fh)
    fs.release(f"/{NOTES}", fh)

    assert server.files[NOTES] == b"new"


def test_a_save_stored_as_sent_is_read_back_without_asking_the_server(kernel):
    server = _Server({NOTES: b"v1"})
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{NOTES}", b"v2", 0, fh)
    fs.release(f"/{NOTES}", fh)

    assert _read(fs, f"/{NOTES}") == b"v2"
    assert server.reads == []


def test_a_save_the_server_stored_otherwise_is_read_back_as_stored(kernel):
    server = _Server({PREFS: b"{}"}, store=lambda path, body: body + b"\n")
    fs = _ops(server, kernel)
    fh = fs.open(f"/{PREFS}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{PREFS}", b'{"theme": "light"}', 0, fh)
    fs.release(f"/{PREFS}", fh)

    assert _read(fs, f"/{PREFS}") == b'{"theme": "light"}\n'
    assert server.reads == [PREFS]


def test_a_save_whose_answer_was_lost_is_found_landed_and_not_sent_again(kernel):
    # Regression: sent again, a save that had landed met its own precondition
    # and reported a conflict for bytes the server held.
    server = _Server({NOTES: b"v1"})
    answered = server._write

    def lost(*args):
        answered(*args)
        raise OSError(errno.EIO, "no answer")

    server._write = lost
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{NOTES}", b"v2", 0, fh)
    with pytest.raises(OSError):
        fs.flush(f"/{NOTES}", fh)

    server._write = answered
    fs.release(f"/{NOTES}", fh)

    assert _sent(server) == [(b"v2", False)]
    assert server.files[NOTES] == b"v2"


@pytest.mark.parametrize("tag", ['"{}"', 'W/"{}"'])
def test_bytes_written_after_a_save_whose_answer_was_lost_are_saved_over_it(kernel, tag):
    # Regression: the next save compared the server's file with the bytes
    # written since, not the ones the lost save sent, and its old
    # precondition then met that landed save as a conflict.
    server = _Server({NOTES: b"v1"}, etag=tag.format)
    answered = server._write

    def checked(path, params, body, headers):
        if headers.get("If-Match") != f'"{_version(server.files[path])}"':
            return 412, None, _error("changed")
        return answered(path, params, body, headers)

    def lost(*args):
        checked(*args)
        raise OSError(errno.EIO, "no answer")

    server._write = lost
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{NOTES}", b"v2", 0, fh)
    with pytest.raises(OSError):
        fs.flush(f"/{NOTES}", fh)

    server._write = checked
    fs.write(f"/{NOTES}", b"v3", 0, fh)
    fs.release(f"/{NOTES}", fh)

    assert _sent(server) == [(b"v2", False), (b"v3", False)]
    assert server.files[NOTES] == b"v3"


@pytest.mark.parametrize("landed", [False, True], ids=["lost-on-the-way", "answer-lost"])
@pytest.mark.parametrize("opened", ["made", "emptied"])
def test_a_save_at_release_that_got_no_answer_is_settled_before_its_handle_goes(
    kernel, opened, landed
):
    # Regression: a file only made or emptied has its one save at release,
    # and the handle went with that save unanswered, so one lost on the way
    # silently left the file as it was, with nothing reported.
    path = "/user/memory/lock.md" if opened == "made" else f"/{NOTES}"
    server = _Server({NOTES: b"n"})
    answered = server._write
    sent = []

    def first_unanswered(*args):
        sent.append(args[2])
        if len(sent) > 1:
            return answered(*args)
        if landed:
            answered(*args)
        raise OSError(errno.EIO, "no answer")

    server._write = first_unanswered
    fs = _ops(server, kernel)
    if opened == "made":
        fh = fs.create(path, 0o644)
    else:
        fh = fs.open(path, os.O_WRONLY | os.O_TRUNC)
    fs.flush(path, fh)
    fs.release(path, fh)

    # One that landed is found there, not sent again to meet its own precondition.
    assert sent == ([b""] if landed else [b"", b""])
    assert server.files[path.lstrip("/")] == b""
    assert _read(fs, path) == b""


def test_bytes_written_while_an_earlier_save_is_out_are_saved_too(kernel):
    # Regression: another writer's open sent this handle's emptied file, the
    # command wrote while that save was out, and its answer marked the handle
    # clean, so the written bytes were never sent.
    server = _Server({NOTES: b"v1"})
    answered = server._write

    def meanwhile(path, params, body, headers):
        if PROVISIONAL_HEADER in headers:
            fs.write(f"/{NOTES}", b"v2", 0, fh)
        return answered(path, params, body, headers)

    server._write = meanwhile
    fs = _ops(server, kernel)
    fh = fs.open(f"/{NOTES}", os.O_WRONLY | os.O_TRUNC)
    other = fs.open(f"/{NOTES}", os.O_WRONLY)
    fs.release(f"/{NOTES}", other)
    fs.release(f"/{NOTES}", fh)

    assert _sent(server) == [(b"", True), (b"v2", False)]
    assert server.files[NOTES] == b"v2"


def test_a_file_saved_in_a_directory_its_command_made_is_found_there(kernel):
    # Regression: the listing above, taken before the mkdir, went on lacking
    # the directory once the save made the server hold it.
    fs = _ops(_Server({NOTES: b"n"}), kernel)
    with pytest.raises(OSError):
        fs.getattr("/user/memory/archive")
    fs.mkdir("/user/memory/archive", 0o755)
    fh = fs.create("/user/memory/archive/a.md", 0o644)
    fs.write("/user/memory/archive/a.md", b"x", 0, fh)
    fs.release("/user/memory/archive/a.md", fh)

    assert fs.getattr("/user/memory/archive")["st_mode"] & stat.S_IFDIR
    assert "archive" in fs.readdir("/user/memory", 0)
    assert fs.getattr("/user/memory/archive/a.md")["st_size"] == 1


def test_an_open_read_handle_answers_its_files_own_mode_and_mtime(kernel):
    # Regression: any open handle answered writable, as of the moment asked,
    # so fstat called a read-only transcript writable and moved its mtime on
    # every call.
    transcript = "workspaces/w1/transcripts/a1/turn-0001.jsonl"
    fs = _ops(_Server({transcript: b"one"}, read_only={transcript}), kernel)
    fh = fs.open(f"/{transcript}", os.O_RDONLY)

    by_handle = [fs.getattr(f"/{transcript}", fh) for _ in range(2)]

    assert stat.S_IMODE(by_handle[0]["st_mode"]) == 0o444
    assert by_handle[0]["st_mtime"] == by_handle[1]["st_mtime"]
    assert by_handle[0]["st_mtime"] == fs.getattr(f"/{transcript}")["st_mtime"]



def test_an_old_read_handle_keeps_its_time_apart_from_the_paths(kernel):
    # Regression: a read handle on an older version asked the path's time
    # cache for it, so stat and fstat moved each other's mtime in turn.
    fs = _ops(_Server({"user/memory/a.md": b"one"}), kernel)
    old = fs.open("/user/memory/a.md", os.O_RDONLY)
    fh = fs.open("/user/memory/a.md", os.O_WRONLY | os.O_TRUNC)
    fs.write("/user/memory/a.md", b"two", 0, fh)
    fs.release("/user/memory/a.md", fh)

    times = [
        (fs.getattr("/user/memory/a.md")["st_mtime"], fs.getattr("/user/memory/a.md", old)["st_mtime"])
        for _ in range(2)
    ]

    assert times[0] == times[1]
    assert times[0][0] != times[0][1]

# -- what outlives a command ---------------------------------------------------


def test_bytes_named_by_their_digest_are_fetched_once_for_every_command(kernel):
    server = _Server({NOTES: b"notes"})
    fs = _ops(server, kernel)
    for pid, call in ((101, "call-a"), (201, "call-b")):
        kernel.run(pid, call)
        assert _read(fs, f"/{NOTES}") == b"notes"

    assert server.reads == [NOTES]


def test_bytes_whose_version_is_not_their_digest_are_not_shown_to_another_command(kernel):
    # The automations file keeps its version while its run state moves.
    server = _Server({PREFS: b"{}"}, version=lambda data: "rev-1")
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    assert _read(fs, f"/{PREFS}") == b"{}"
    server.files[PREFS] = b'{"ran": 1}'

    kernel.run(201, "call-b")
    assert _read(fs, f"/{PREFS}") == b'{"ran": 1}'


def test_a_command_keeps_its_bytes_of_a_file_whose_version_is_not_their_digest(kernel):
    # Regression: another command's read of the same version replaced them,
    # and its open handle then stood in for them.
    server = _Server({PREFS: b"{}"}, version=lambda data: "rev-1")
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    assert _read(fs, f"/{PREFS}") == b"{}"
    server.files[PREFS] = b'{"ran": 1}'
    kernel.run(201, "call-b")
    fh = fs.open(f"/{PREFS}", os.O_RDONLY)
    assert fs.read(f"/{PREFS}", 100, 0, fh) == b'{"ran": 1}'

    kernel.run(102, "call-a")
    assert _read(fs, f"/{PREFS}") == b"{}"
    fs.release(f"/{PREFS}", fh)


def test_a_save_stored_otherwise_is_read_back_as_stored_under_a_version_kept_before(
    kernel,
):
    # Regression: an automation file keeps its version across a save it
    # rewrites, and the bytes read before the save were shown again.
    server = _Server(
        {PREFS: b"{}"}, version=lambda data: "rev-1", store=lambda path, body: body + b"\n"
    )
    fs = _ops(server, kernel)
    assert _read(fs, f"/{PREFS}") == b"{}"
    fh = fs.open(f"/{PREFS}", os.O_WRONLY | os.O_TRUNC)
    fs.write(f"/{PREFS}", b'{"theme": "light"}', 0, fh)
    fs.release(f"/{PREFS}", fh)

    assert _read(fs, f"/{PREFS}") == b'{"theme": "light"}\n'


@pytest.mark.parametrize("let_go", ["evicted", "replaced"])
def test_bytes_kept_for_one_command_go_with_its_view(kernel, monkeypatch, let_go):
    # Regression: they held the whole view, listings and all, once the view
    # itself was let go, where no command could use them again.
    server = _Server({PREFS: b"{}"}, version=lambda data: "rev-1")
    fs = _ops(server, kernel)
    if let_go == "evicted":
        for n in range(views.VIEW_LIMIT + 8):
            kernel.run(100 + n, f"call-{n}")
            _read(fs, f"/{PREFS}")
    else:
        monkeypatch.setattr(views, "UNTAGGED_TTL_S", -1.0)  # untagged views are replaced at once
        for _ in range(2):
            _read(fs, f"/{PREFS}")

    kept_for = {owner for _, _, owner in fs._views._bytes._items if owner is not None}
    assert kept_for <= set(fs._views._views.values())


def test_a_file_its_listing_carries_is_read_with_no_request_of_its_own(kernel):
    server = _Server({NOTES: b"notes"}, inline=True)
    fs = _ops(server, kernel)

    assert _read(fs, f"/{NOTES}") == b"notes"
    assert server.reads == []


TRANSCRIPTS = "workspaces/w1/transcripts"
THREAD_FILES = {
    f"{TRANSCRIPTS}/a1/turn-0001.jsonl": b"one",
    f"{TRANSCRIPTS}/a1/tasks/k1/run-0001.jsonl": b"two",
    f"{TRANSCRIPTS}/b2/turn-0001.jsonl": b"three",
}


def test_a_listing_of_the_folders_below_answers_a_search_under_it(kernel):
    # A search lists one folder and reads every file under it, each a round
    # trip of its own unless that first listing answers for all of them.
    server = _Server(THREAD_FILES, inline=True, trees={TRANSCRIPTS})
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    _walk(fs, f"/{TRANSCRIPTS}")
    server.lists.clear()

    assert fs.readdir(f"/{TRANSCRIPTS}", 0) == [".", "..", "a1", "b2"]
    assert fs.readdir(f"/{TRANSCRIPTS}/a1/tasks", 0) == [".", "..", "k1"]
    assert {path: _read(fs, f"/{path}") for path in THREAD_FILES} == THREAD_FILES
    assert server.lists == [TRANSCRIPTS]
    assert server.reads == []


def test_a_folder_the_command_listed_already_stays_as_it_listed_it(kernel):
    server = _Server(THREAD_FILES, trees={TRANSCRIPTS})
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    assert fs.readdir(f"/{TRANSCRIPTS}/a1", 0) == [".", "..", "tasks", "turn-0001.jsonl"]

    server.files[f"{TRANSCRIPTS}/a1/turn-0002.jsonl"] = b"four"
    fs.readdir(f"/{TRANSCRIPTS}", 0)
    assert fs.readdir(f"/{TRANSCRIPTS}/a1", 0) == [".", "..", "tasks", "turn-0001.jsonl"]


def test_the_made_up_directories_answer_every_command_but_never_hide_a_new_one(kernel):
    server = _Server(
        {NOTES: b"n", "workspaces/w1/memory/a.md": b"a"},
        structural={"", "user", "workspaces"},
    )
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    _walk(fs, f"/{NOTES}")
    _walk(fs, "/workspaces/w1/memory/a.md")

    kernel.run(201, "call-b")
    server.lists.clear()
    _walk(fs, f"/{NOTES}")
    assert server.lists == ["user/memory"]

    server.files["workspaces/w2/memory/b.md"] = b"b"
    kernel.run(301, "call-c")
    assert _walk(fs, "/workspaces/w2/memory/b.md")["st_size"] == 1


def test_a_name_the_made_up_directories_lack_is_asked_for_once(kernel):
    # rg looks for its ignore files in every directory above the one it
    # searches, so each search would list every made-up one again.
    server = _Server({"workspaces/w1/memory/a.md": b"a"}, structural={"", "workspaces"})
    fs = _ops(server, kernel)
    kernel.run(101, "call-a")
    _walk(fs, "/workspaces/w1/memory/a.md")
    with pytest.raises(OSError):
        fs.getattr("/workspaces/.gitignore")

    kernel.run(201, "call-b")
    server.lists.clear()
    with pytest.raises(OSError):
        fs.getattr("/workspaces/.gitignore")
    assert server.lists == []


def test_the_made_up_directories_are_listed_again_once_link_lays_another_layout(
    kernel, tmp_path
):
    state = tmp_path / "state.json"

    def link_wrote(state_data: dict) -> None:
        (tmp_path / "state.tmp").write_text(json.dumps(state_data))
        os.replace(tmp_path / "state.tmp", state)

    link_wrote({"sources": ["workspaces/w1/memory", "workspaces/w2/memory"]})
    server = _Server(
        {"workspaces/w1/memory/a.md": b"a", "workspaces/w2/memory/b.md": b"b"},
        structural={"", "workspaces"},
    )
    fs = _ops(server, kernel, layout=str(state))
    kernel.run(101, "call-a")
    assert fs.readdir("/workspaces", 0) == [".", "..", "w1", "w2"]

    link_wrote({"sources": ["workspaces/w1/memory", "workspaces/w2/memory"], "token": 2})
    kernel.run(201, "call-b")
    fs.readdir("/workspaces", 0)
    assert server.lists.count("workspaces") == 1

    del server.files["workspaces/w2/memory/b.md"]
    link_wrote({"sources": ["workspaces/w1/memory"]})
    kernel.run(301, "call-c")
    assert fs.readdir("/workspaces", 0) == [".", "..", "w1"]


# -- the connection to the server ----------------------------------------------


class _Endpoint(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _answer(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self.server.seen.append((self.command, self.client_address[1]))
        self.server.user_agents.append(self.headers.get("User-Agent"))
        status, headers, delay = self.server.answer(self.command, len(self.server.seen))
        time.sleep(delay)
        if status is None:  # taken, and the connection lost before the answer
            self.close_connection = True
            return
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")
        # Closed with no word to the client, as a server going away does.
        self.close_connection = self.server.hang_up

    do_GET = do_PUT = do_POST = _answer

    def log_message(self, *args) -> None:
        pass


class _Loopback(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def handle_error(self, request, client_address) -> None:
        pass  # the client gave up on an answer it timed out waiting for


@pytest.fixture
def endpoint(tmp_path):
    """A server on loopback: ``answer(method, nth)`` gives each request's
    status (None: no answer), headers and delay, and ``seen`` each one's
    method and client port."""
    server = _Loopback(("127.0.0.1", 0), _Endpoint)
    server.seen = []
    server.user_agents = []
    server.answer = lambda method, nth: (200, {}, 0)
    server.hang_up = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"base_url": f"http://127.0.0.1:{server.server_port}", "token": "t"})
    )
    yield SimpleNamespace(server=server, config=str(config))
    server.shutdown()
    server.server_close()


@pytest.mark.enable_socket
def test_every_request_names_the_sandbox_as_its_user_agent(endpoint):
    # http.client sends none, and edge bot mitigation can challenge a request
    # without one, which the daemon would only see as the mount going away.
    remote.Remote(endpoint.config).request("GET", "list", {"path": ""})

    assert endpoint.server.user_agents == ["langalpha-sandbox/1.0"]


@pytest.mark.enable_socket
@pytest.mark.parametrize("method", ["PUT", "GET"])
def test_a_request_that_timed_out_is_not_sent_again(endpoint, method):
    # Regression: a save sent again after a timeout landed twice, and the
    # second met the first's precondition as a conflict.
    endpoint.server.answer = lambda method, nth: (200, {}, 1.0)
    with pytest.raises(OSError) as exc:
        remote.Remote(endpoint.config, timeout=0.2).request(
            method, "write", {"path": NOTES}, body=b"v2" if method == "PUT" else None
        )

    assert exc.value.errno == errno.EIO
    # The server records a request on its own thread, which a loaded runner
    # may not have reached when the client gave up.
    deadline = time.monotonic() + 5
    while not endpoint.server.seen and time.monotonic() < deadline:
        time.sleep(0.01)
    assert [seen for seen, _ in endpoint.server.seen] == [method]


@pytest.mark.enable_socket
def test_a_read_on_a_connection_the_server_had_closed_is_sent_again_on_a_new_one(
    endpoint, monkeypatch
):
    # A close the idle check did not see yet is met by the next request.
    monkeypatch.setattr(remote._Conn, "dropped", lambda self: False)
    endpoint.server.hang_up = True
    server = remote.Remote(endpoint.config)
    server.request("GET", "list", {"path": ""})

    status, _, _ = server.request("GET", "list", {"path": ""})

    assert status == 200
    assert [seen for seen, _ in endpoint.server.seen] == ["GET", "GET"]
    assert len({port for _, port in endpoint.server.seen}) == 2


@pytest.mark.enable_socket
def test_a_save_the_server_took_but_never_answered_is_not_sent_again(endpoint):
    # Regression: a kept-alive connection reset after the server took the
    # save sent it again, and the copy met the first's precondition.
    endpoint.server.answer = lambda method, nth: (None if method == "PUT" else 200, {}, 0)
    server = remote.Remote(endpoint.config)
    server.request("GET", "list", {"path": ""})

    with pytest.raises(OSError) as exc:
        server.request("PUT", "write", {"path": NOTES}, body=b"v2")

    assert exc.value.errno == errno.EIO
    assert [seen for seen, _ in endpoint.server.seen] == ["GET", "PUT"]


@pytest.mark.enable_socket
def test_a_save_takes_a_kept_alive_connection_only_when_it_answered_moments_ago(
    endpoint, monkeypatch
):
    server = remote.Remote(endpoint.config)
    server.request("GET", "list", {"path": ""})
    server.request("PUT", "write", {"path": NOTES}, body=b"v2")
    monkeypatch.setattr(remote, "WRITE_REUSE_S", -1.0)  # every connection too old
    server.request("PUT", "write", {"path": NOTES}, body=b"v3")

    ports = [port for _, port in endpoint.server.seen]
    assert ports[0] == ports[1] != ports[2]


@pytest.mark.enable_socket
def test_requests_from_threads_that_each_make_one_share_a_connection(endpoint):
    # Regression: connections were kept per thread, and libfuse calls in on a
    # new Python thread state each time, so every request opened its own.
    server = remote.Remote(endpoint.config)
    for _ in range(5):
        thread = threading.Thread(target=server.request, args=("GET", "list", {"path": ""}))
        thread.start()
        thread.join()

    assert len(endpoint.server.seen) == 5
    assert len({port for _, port in endpoint.server.seen}) == 1


@pytest.mark.enable_socket
def test_a_throttled_save_waits_as_told_and_is_sent_again(endpoint):
    endpoint.server.answer = lambda method, nth: (
        (429, {"Retry-After": "0"}, 0) if nth == 1 else (200, {}, 0)
    )
    status, _, _ = remote.Remote(endpoint.config).request(
        "PUT", "write", {"path": NOTES}, body=b"v2"
    )

    assert status == 200
    assert [seen for seen, _ in endpoint.server.seen] == ["PUT", "PUT"]
