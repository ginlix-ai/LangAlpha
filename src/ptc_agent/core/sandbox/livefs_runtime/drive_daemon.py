"""Mount the user's Drive on the computer, beside the livefs mount.

A second daemon from the same package, so it ships and is checked by
``boot`` the same way, and shares livefs's lifecycle (a tmpfs generation per
start, a link swapped over), token file and HTTP client. Its settings are its
own: kernel caching on for a day, no open calls, and a local copy of every
text file while the disk has room (see ``drive_ops`` and ``drive_sync``).

    python3 -m livefs drive-start --root R --base-url U   (a token in LIVEFS_CONFIG, optional)
    python3 -m livefs drive-link --root R [--target DST ...]
    python3 -m livefs drive-stop --root R
    python3 -m livefs drive-status
    python3 -m livefs drive-notices    (takes the agent's notices the server has not had)
    python3 -m livefs drive-settle --call C [--wait S]   (pushes C's saves, then takes notices)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import socket
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass

from . import lifecycle
from .daemon import call_in_environ, link_folders
from .drive_cache import Cache
from .drive_client import DriveClient
from .drive_journal import Journal
from .drive_effects import Signals
from .drive_libfuse import Kernel, fuse_class, keep_listings
from .drive_notices import Notices, deliver
from .drive_ops import DriveFS
from .drive_protocol import FSNAME, GENERATIONS, MOUNT, PRIVATE, PROCESS_PREFIX, SETTLE_WAIT_S
from .drive_sync import WARM_DEFAULT_BYTES, Sync, load_spaces
from .lifecycle import (
    Paths,
    answering,
    locked,
    prepare,
    probing,
    read_state,
    start_probed,
    sweep,
    use_libfuse3,
    write_state,
)
from .protocol import CONFIG_ENV

#: How long the kernel keeps what it was told. Nothing here relies on it
#: running out: the feed invalidates what changed.
CACHE_S = 24 * 3600.0

PROBE_TIMEOUT_S = 5

#: How often the daemon writes its status to disk, for a host that finds the
#: control socket not answering.
STATUS_EVERY_S = 2.0

#: Binaries kept on disk at most, before the least recently read go.
BINARY_BUDGET = 2 * 1024 * 1024 * 1024

#: Requests libfuse runs at once at most. Saves run off the loop, so what
#: can hold a request on the server is a fetch of bytes not on disk, a
#: delete, a rename or an fsync. Two kinds of work pull apart here. Cold
#: metadata (20,000 files, kernel caches dropped) only passes Python's one
#: lock between more threads: 8 simultaneous searches end in about 2.2 s
#: with 2 and 4.5 to 4.9 s with 8 or 32. A large file read by block waits on
#: the store in a loop thread, and a columnar reader (DuckDB) keeps one read
#: per thread of its own in flight: a cold one-column aggregate of a 5 GB
#: Parquet at 30 ms and 100 MB/s took 36 s with 2, 22 s with 4 and 7 to
#: 11 s with 8 or 16. Large reads win: a cold search doubles, a cold scan
#: of a large file takes a third to a fifth of the time. Warm searches reach
#: the daemon only for ``opendir`` and cost the same at any count.
MAX_THREADS = 32

#: Test knobs, as JSON in the daemon's environment: ``warm``,
#: ``warm_bytes``, ``binary_budget``, ``store_reserve`` (free bytes the cache
#: leaves on its disk), ``max_threads`` and ``debug``.
SETTINGS_ENV = "DRIVE_MOUNT_SETTINGS"


@dataclass(frozen=True)
class DrivePaths(Paths):
    mount: str = MOUNT
    generations: str = GENERATIONS
    private: str = PRIVATE
    #: livefs's private directory, whose token this daemon uses too.
    token_dir: str = Paths().private

    @property
    def config(self) -> str:
        return os.path.join(self.token_dir, "config.json")

    @property
    def token(self) -> Paths:
        return Paths(private=self.token_dir)

    @property
    def store(self) -> str:
        return os.path.join(self.private, "store")

    @property
    def status(self) -> str:
        return os.path.join(self.private, "status.json")

    @property
    def journal(self) -> str:
        return os.path.join(self.private, "unsaved")

    @property
    def notices(self) -> str:
        return os.path.join(self.private, "notices.jsonl")

    @property
    def control(self) -> str:
        return os.path.join(self.private, "control.sock")


def _settings() -> dict:
    try:
        found = json.loads(os.environ.get(SETTINGS_ENV) or "{}")
    except ValueError:
        return {}
    return found if isinstance(found, dict) else {}


def process_of(pid: int, proc: str = "/proc") -> str | None:
    """A tag for the session ``pid`` runs in (``<sid>-<start time>``), for a
    process no tool call started: every process of one ``rm -r`` or script
    shares it. The start time tells a reused session id apart."""
    try:
        with open(f"{proc}/{pid}/stat") as f:
            sid = int(f.read().rpartition(")")[2].split()[3])
        with open(f"{proc}/{sid}/stat") as f:
            start = f.read().rpartition(")")[2].split()[19]
    except (OSError, ValueError, IndexError):
        return None
    return f"{sid}-{start}"


def _call_or_process(pid: int) -> str | None:
    call = call_in_environ(pid)
    if call:
        return call
    tag = process_of(pid)
    return PROCESS_PREFIX + tag if tag else None


def _memory_limit(cgroup: str = "/sys/fs/cgroup") -> int | None:
    for name in ("memory.max", "memory/memory.limit_in_bytes"):
        with contextlib.suppress(OSError), open(os.path.join(cgroup, name)) as f:
            value = f.read().strip()
            return int(value) if value.isdigit() else None
    return None


# --- start ---------------------------------------------------------------


def _probe(paths: DrivePaths) -> str | None:
    client = DriveClient(paths.config, quick_timeout=PROBE_TIMEOUT_S)
    try:
        client.spaces()
    except OSError as exc:
        return f"the Drive endpoint did not answer: {exc}"
    return None


def start_drive(args, paths: DrivePaths) -> dict:
    """Make the Drive mount serve current code, installing a new token first
    when one came. Idempotent, as livefs's ``start`` is. Probes whenever it
    may start a daemon: one not serving current code."""
    refused, _ = prepare(args, paths, paths.token)
    if refused:
        return refused
    with locked(paths):
        state = read_state(paths)
        current = answering(paths.mount) and state.get("code") == lifecycle.code_version()
        probe = None if current else probing(lambda: _probe(paths))
        started, refused = start_probed(args, state, paths, probe, serve="drive-serve", alive=answering)
        write_state(paths, state)
    return refused or {"ok": True, "error": None, "started": started or None}


def link_drive(args, paths: DrivePaths) -> dict:
    """Link exactly ``--target`` to the mount, removing the links an earlier
    call laid that this one leaves out, as livefs's ``link`` does. Each points
    at the mount's own link (``/mnt/drive/``: the trailing slash is how the
    shared code tells its links from anything else at the path), so it stays
    right across a restart."""
    args.link = [f":{target}" for target in args.target or ()]
    return link_folders(args, paths)


def stop_drive(args, paths: DrivePaths) -> dict:
    """Remove the links, unmount every generation and remove the mount's
    link: the feature went off. The store stays, so turning it on again
    fetches only what changed."""
    args.target = []
    unlinked = link_drive(args, paths)
    with locked(paths):
        sweep("", paths)
        with contextlib.suppress(OSError):
            if os.path.islink(paths.mount):
                os.unlink(paths.mount)
        state = read_state(paths)
        state.pop("code", None)
        write_state(paths, state)
    return {**unlinked, "ok": True}


def _ask(paths: DrivePaths, question: dict, timeout: float) -> dict | None:
    """One question to the serving daemon over its control socket."""
    if not answering(paths.mount):
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(paths.control)
            conn.sendall(json.dumps(question).encode() + b"\n")
            answer = json.loads(conn.makefile("rb").readline(1 << 22))
    except (OSError, ValueError):
        return None
    return answer if isinstance(answer, dict) else None


def drive_status(args, paths: DrivePaths) -> dict:
    """The daemon's own account of itself: asked fresh, or as it last wrote
    it when it does not answer."""
    status = (_ask(paths, {"op": "status"}, 3.0) or {}).get("status")
    if status is None:
        try:
            with open(paths.status) as f:
                status = json.load(f)
        except (OSError, ValueError):
            status = None
    return {"ok": status is not None, "error": None, "serving": answering(paths.mount), "status": status}


def drive_notices(args, paths: DrivePaths) -> dict:
    """The agent's notices the daemon could not hand the server, taken (the
    host files them itself, while the server cannot be reached from here)."""
    if not os.path.isdir(paths.private):
        return {"ok": True, "error": None, "notices": []}
    return {"ok": True, "error": None, "notices": Notices(paths.notices).take()}


def drive_settle(args, paths: DrivePaths) -> dict:
    """After a command ``--call``: have the daemon send that call's saves
    still waiting and wait up to ``--wait`` for their answers, then take the
    notices (a refusal the server filed for the call itself). ``left`` is
    how many are still unanswered, told as not saved yet."""
    wait_s = float(args.wait if args.wait is not None else SETTLE_WAIT_S)
    left = None
    if args.call:
        answer = _ask(paths, {"op": "settle", "call": args.call, "wait": wait_s}, wait_s + 3.0)
        left = (answer or {}).get("left")
    taken = drive_notices(args, paths)
    return {**taken, "left": left}


# --- serve ---------------------------------------------------------------


#: The kernel's read-ahead on the mount, KiB. At its 128 KiB default a
#: columnar reader's ~0.5 to 1.5 MiB reads reach the daemon as tens of 4 KiB
#: requests each; at 4 MiB they arrive whole (measured with DuckDB).
READ_AHEAD_KB = 4096


def _set_read_ahead(mount: str, host: Host) -> None:
    """Set how far the kernel reads ahead on the mount (``read_ahead_kb``),
    and record what it is. The kernel offers 128 KiB at ``init`` and lets a
    daemon only lower it there, so a larger window is set on the mount's
    backing device once the kernel has taken the ``init`` answer (set
    before, it would take the smaller of the two): run beside ``init``, a
    ``stat`` of the mount returns only after that. Where ``/sys`` is
    read-only it stays as it is."""
    with contextlib.suppress(OSError):
        os.stat(mount)
    target = os.path.realpath(mount)
    dev = None
    with contextlib.suppress(OSError), open("/proc/self/mountinfo") as f:
        for line in f:
            fields = line.split()
            if len(fields) > 4 and fields[4] == target:
                dev = fields[2]
    if dev is None:
        return
    path = f"/sys/class/bdi/{dev}/read_ahead_kb"
    try:
        with open(path, "w") as f:
            f.write(str(READ_AHEAD_KB))
    except OSError as exc:
        host.extra["read_ahead_kb_unset"] = exc.strerror or str(exc)
    with contextlib.suppress(OSError, ValueError), open(path) as f:
        host.extra["read_ahead_kb"] = int(f.read())


class Host:
    """What the host asks the serving daemon over its control socket, and the
    status it also finds on disk: one JSON line in, one out.
    ``{"op": "settle", "call", "wait"}`` sends that call's saves and answers
    ``left``, the ones still unanswered; ``{"op": "status"}`` answers the
    daemon's own account of itself."""

    def __init__(self, ops: DriveFS, sync: Sync, sent: dict, paths: DrivePaths) -> None:
        self.ops, self.sync, self.sent, self.paths = ops, sync, sent, paths
        #: What the start measured, shown after the loops' own state.
        self.extra: dict = {}
        self._started = time.monotonic()

    def status(self) -> dict:
        rss = hwm = None
        with contextlib.suppress(OSError), open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) * 1024
                elif line.startswith("VmHWM:"):
                    hwm = int(line.split()[1]) * 1024
        ops, cache = self.ops, self.ops.reads.cache
        held = ops.names.counts()
        return {
            **self.sync.state,
            "notices": self.sent,
            **self.extra,
            "uptime_s": round(time.monotonic() - self._started, 3),
            "ops": dict(ops.counts),
            "spaces": held["spaces"],
            "pending": held["pending"],
            "unsaved": ops.saves.unsaved(),
            "store": {
                "files": cache.files(),
                "bytes": cache.bytes_held(),
                "binaries": cache.binaries,
                "claimed": cache.claimed,
                "budget": cache.budget,
                "missing_text": cache.missing_text,
                "refused": cache.refused,
            },
            "memory": {"rss": rss, "peak": hwm},
        }

    def write_status(self) -> None:
        # One temp file per thread: the warm-up and the status loop both
        # write it, and a shared one could be published half written.
        tmp = f"{self.paths.status}.{threading.get_ident()}.tmp"
        with contextlib.suppress(OSError):
            with open(tmp, "w") as f:
                json.dump(self.status(), f)
            os.replace(tmp, self.paths.status)

    def keep_status(self, stop: threading.Event) -> None:
        while True:
            self.write_status()
            if stop.wait(STATUS_EVERY_S):
                return

    def serve_control(self, stop: threading.Event) -> None:
        """In root's private directory, so only the host's commands reach it."""
        path = self.paths.control
        with contextlib.suppress(OSError):
            os.unlink(path)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(path)
            os.chmod(path, 0o600)
            server.listen(16)
        except OSError as exc:
            print(f"drive: control socket not served: {exc}", file=sys.stderr, flush=True)
            server.close()
            return
        server.settimeout(1.0)
        with server:
            while not stop.is_set():
                try:
                    conn, _ = server.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if stop.wait(0.1):
                        return
                    continue
                threading.Thread(target=self._answer, args=(conn,), daemon=True,
                                 name="drive-control").start()

    def _answer(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(5.0)
            try:
                asked = json.loads(conn.makefile("rb").readline(4096))
                op = asked["op"]
                if op == "settle":
                    call, wait_s = str(asked["call"]), min(float(asked["wait"]), 60.0)
            except (OSError, ValueError, KeyError, TypeError):
                return
            if op == "settle":
                answer = {"left": self.ops.saves.settle(call, wait_s)}
            elif op == "status":
                answer = {"status": self.status()}
            else:
                return
            with contextlib.suppress(OSError):
                conn.sendall(json.dumps(answer).encode() + b"\n")


def serve(args, paths: DrivePaths) -> None:
    use_libfuse3()
    from . import mfusepy as fuse

    settings = _settings()
    owner = os.stat(args.root)
    os.makedirs(paths.store, mode=0o700, exist_ok=True)

    client = DriveClient(paths.config)
    t0 = time.monotonic()
    spaces = load_spaces(client)
    loaded_s = time.monotonic() - t0
    free = shutil.disk_usage(paths.store).free
    reserve = settings.get("store_reserve")
    counts: Counter[str] = Counter()
    cache = Cache(
        paths.store,
        int(settings.get("binary_budget") or min(BINARY_BUDGET, free // 5)),
        int(reserve) if reserve is not None else None,
        counts=counts,
    )
    cache.scan()
    journal = Journal(paths.journal)
    notices = Notices(paths.notices)
    limit = _memory_limit()
    warm_bytes = int(
        settings.get("warm_bytes")
        or min(WARM_DEFAULT_BYTES, (limit // 4) if limit else WARM_DEFAULT_BYTES)
    )
    signals = Signals()
    stop = threading.Event()
    sent = {"sent": 0, "errors": 0}
    host: Host | None = None

    def on_init() -> None:
        """The mount answers from here on: bind the kernel's calls and start
        every loop beside the FUSE one."""
        assert host is not None
        fuse_ptr = fuse._libfuse.fuse_get_context().contents.fuse
        host.extra["listings_kept"] = keep_listings(fuse, fuse_ptr)
        sync.kernel = Kernel(fuse._libfuse, fuse_ptr, args.mount)

        def texts_then_warm() -> None:
            sync.texts_then_warm()
            host.write_status()

        for name, target in (
            ("read-ahead", lambda: _set_read_ahead(args.mount, host)),
            ("texts", texts_then_warm),
            ("feed", sync.follow_feed),
            ("forget", sync.forget_asked),
            ("status", lambda: host.keep_status(stop)),
            ("saver", lambda: ops.saves.run(stop)),
            ("notices", lambda: deliver(notices, client, signals.notice, stop, sent)),
            ("control", lambda: host.serve_control(stop)),
        ):
            threading.Thread(target=target, daemon=True, name=f"drive-{name}").start()

    ops = DriveFS(
        spaces,
        cache,
        client,
        owner.st_uid,
        owner.st_gid,
        journal=journal,
        notices=notices,
        caller=lambda: fuse.fuse_get_context()[2],
        call_of=_call_or_process,
        effects=signals,
        on_init=on_init,
        counts=counts,
        root=os.path.realpath(args.mount),
    )
    # Records a replaced daemon still serving holds stay its own; the saver
    # takes them up once it lets go (``Saves.adopt``).
    restored = ops.writes.restore(journal.claim())
    sync = Sync(ops, client, signals, args.mount, stop, warm_bytes=warm_bytes, warm=settings.get("warm", True))
    host = Host(ops, sync, sent, paths)
    host.extra["manifest_s"] = round(loaded_s, 3)
    sync.collect()
    print(
        f"drive: {sum(len(s.by_id) for s in spaces)} entries in {loaded_s:.2f}s,"
        f" {cache.files()} files on disk, {restored} unsaved taken up",
        file=sys.stderr,
        flush=True,
    )
    if getattr(args, "gated", False) and sys.stdin.readline().strip() != "go":
        return

    try:
        fuse_class(fuse, ops.counts)(
            ops,
            args.mount,
            foreground=True,
            max_threads=int(settings.get("max_threads") or MAX_THREADS),
            max_idle_threads=int(settings.get("max_threads") or MAX_THREADS),
            allow_other=True,
            default_permissions=True,
            attr_timeout=CACHE_S,
            entry_timeout=CACHE_S,
            negative_timeout=CACHE_S,
            hard_remove=True,
            fsname=FSNAME,
            # libfuse's own trace of every request, to the daemon's log.
            debug=bool(settings.get("debug")),
        )
    finally:
        stop.set()
        signals.notice.set()


def main() -> int:
    parser = argparse.ArgumentParser(prog="livefs")
    parser.add_argument(
        "command",
        choices=(
            "drive-serve", "drive-start", "drive-link", "drive-stop", "drive-status", "drive-notices",
            "drive-settle",
        ),
    )
    parser.add_argument("--root", default="/")
    parser.add_argument("--mount")
    parser.add_argument("--base-url")
    parser.add_argument("--gated", action="store_true")
    parser.add_argument("--target", action="append")
    parser.add_argument("--call")
    parser.add_argument("--wait", type=float)
    args = parser.parse_args()
    args.config = os.environ.pop(CONFIG_ENV, None)
    paths = DrivePaths()
    if args.command == "drive-serve":
        serve(args, paths)
        return 0
    commands = {
        "drive-start": start_drive,
        "drive-link": link_drive,
        "drive-stop": stop_drive,
        "drive-status": drive_status,
        "drive-notices": drive_notices,
        "drive-settle": drive_settle,
    }
    result = commands[args.command](args, paths)
    print(json.dumps(result))
    return 0 if result.get("ok") else 1
