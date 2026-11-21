"""How the browser gives way to the computer's other work: it goes first when
memory runs out, opens no more of itself when memory is short, and leaves
nothing running once its daemon is gone."""

from __future__ import annotations

import fcntl
import os
import shutil
import signal
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from . import daemon, hold, launch, signin
from .protocol import CHROME_BIN, ENGINE_BIN, ENGINE_PROFILE_PREFIX, STREAM_PORT, state_file

#: Share of the computer's memory in use past which no more browser is opened: a tab
#: of a heavy page holds several hundred MB, and the rest is for the computer's work.
MEMORY_FULL_SHARE = 0.6

_CGROUP = Path("/sys/fs/cgroup")
_MEMINFO = Path("/proc/meminfo")
_PROC = Path("/proc")
#: What a killed daemon takes to let go of its stream port.
_DAEMON_GONE_S = 1.0
#: What a killed Chrome takes to leave the process table.
_BROWSER_GONE_S = 1.0
#: Held by the gate while it admits a command (``gate._admit``), which may be the
#: one to start the next browser.
ADMISSION_LOCK = "admitting.lock"


def tend(up: bool, *, admitting: bool = False) -> None:
    """While the browser is up it is first for the OOM killer, and a daemon whose
    Chrome died is ended; once it is not up, a Chrome its dead daemon left
    behind is reaped."""
    if up:
        end_browserless(admitting=admitting)
        _browser_dies_first()
    elif admitting:
        _reap_left_behind()
    else:
        with _between_commands() as clear:
            if clear:
                _reap_left_behind()


@contextmanager
def admitting() -> Iterator[None]:
    """Hold the gate's lock, so no command is admitted until the block ends."""
    path = state_file(ADMISSION_LOCK)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


@contextmanager
def _between_commands() -> Iterator[bool]:
    """Whether no command is being admitted, held to the block's end, so the
    hold and launch a reap forgets are never those a command is recording."""
    try:
        path = state_file(ADMISSION_LOCK)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = open(path, "a")
    except OSError:
        yield True
        return
    with lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True


def end_browserless(*, admitting: bool = False) -> bool:
    """End the engine's daemon whose Chrome is gone, as when the OOM killer took
    it, and say whether one was: the browser is closed.

    The daemon would keep its stream listening on the last frame, and the
    closed browser's hold and launch, which last while the stream does, would
    carry over to the next one, so they go with it. Not while a browser command
    other than the caller's runs, which may be launching its Chrome. A caller
    *admitting* a command holds the gate's lock, under which another of our
    commands is not one: it waits for the lock or has become the engine's.
    """
    processes = _processes()
    if _engine_busy(processes, exclude=os.getpid(), wrappers=not admitting):
        return False
    running = {p.ppid for p in _engine_browsers(processes)}
    ended = False
    for p in _engine_daemons(processes):
        if p.pid in running:
            continue
        try:
            os.kill(p.pid, signal.SIGKILL)
            ended = True
        except OSError:
            continue
    if ended:
        hold.forget()
        launch.forget()
        deadline = time.monotonic() + _DAEMON_GONE_S
        while daemon.listening(STREAM_PORT) and time.monotonic() < deadline:
            time.sleep(0.05)
    return ended


def memory_full() -> bool:
    """Whether what cannot be reclaimed fills the computer's share of memory.

    Measured against the computer's limit, so it holds on every resource tier. A
    machine with no limit of its own, such as a microVM, is measured against all its
    memory. One that says neither never counts as full.
    """
    measured = _cgroup_memory()
    if measured:
        limit, used = measured
    else:
        try:
            info = {
                name: int(rest.split()[0])
                for name, rest in (
                    line.split(":", 1) for line in _MEMINFO.read_text().splitlines()
                )
            }
            limit = info["MemTotal"]
            used = info["AnonPages"] + info["Shmem"]
        except (OSError, ValueError, KeyError, IndexError):
            return False
    return used >= limit * MEMORY_FULL_SHARE


#: Where cgroup v2, then v1, keep the limit and what is charged against it, and
#: the counters of what cannot be reclaimed. v1 counts the whole subtree in ``total_``.
_CGROUP_MEMORY = (
    ("memory.max", "memory.stat", ("anon", "shmem")),
    ("memory/memory.limit_in_bytes", "memory/memory.stat", ("total_rss", "total_shmem")),
)
#: v1 has no word for no limit, only a number no machine has.
_NO_LIMIT = 1 << 62


def _cgroup_memory() -> tuple[int, int] | None:
    """The computer's memory limit and what of it cannot be reclaimed, or None
    without a limit of its own."""
    for limit_file, stat_file, counters in _CGROUP_MEMORY:
        try:
            limit = int((_CGROUP / limit_file).read_text())
            stat = dict(line.split() for line in (_CGROUP / stat_file).read_text().splitlines())
            used = sum(int(stat[name]) for name in counters)
        except (OSError, ValueError, KeyError):
            continue
        return (limit, used) if limit < _NO_LIMIT else None
    return None


@dataclass(frozen=True)
class _Process:
    pid: int
    ppid: int
    uid: int
    argv: list[str]


def _processes() -> list[_Process]:
    """Every process the machine shows, read once."""
    found = []
    try:
        entries = [entry for entry in _PROC.iterdir() if entry.name.isdigit()]
    except OSError:
        return []
    for entry in entries:
        try:
            # The name in the second field can hold anything, so read from after it.
            ppid = int((entry / "stat").read_text().rpartition(")")[2].split()[1])
            uid = entry.stat().st_uid
        except (OSError, ValueError, IndexError):
            continue
        try:
            argv = (entry / "cmdline").read_bytes().decode(errors="replace").split("\0")
        except OSError:
            # Still a parent or a child in the browser's tree.
            argv = []
        found.append(_Process(int(entry.name), ppid, uid, argv))
    return found


def _engine_browsers(processes: list[_Process], profile: str | None = None) -> list[_Process]:
    """Each Chrome the engine started: by the engine's name for it, on the
    engine's throwaway profile or the kept one (``signin``). Both count, so a
    Chrome the agent's own code runs, even from the same path, is left alone.
    With *profile*, only those on it."""
    uid = os.getuid()
    kept = signin.profile()
    return [
        p
        for p in processes
        if p.argv[:1] == [CHROME_BIN]
        and p.uid == uid
        and (on := _profile(p)) is not None
        and (_throwaway(on) or on == kept)
        and (profile is None or on == profile)
    ]


def _profile(browser: _Process) -> str | None:
    for arg in browser.argv:
        if arg.startswith("--user-data-dir="):
            return arg.split("=", 1)[1].rstrip("/")
    return None


def _throwaway(profile: str) -> bool:
    """Whether *profile* is one the engine made for a single browser."""
    return os.path.basename(profile).startswith(ENGINE_PROFILE_PREFIX)


def unlock_profile(profile: str) -> None:
    """Clear the lock a Chrome killed on *profile* left, with none running on it.

    Chrome takes over its own stale lock only when the lock names this host,
    and a computer restored or moved comes back under another name.
    """
    if _engine_browsers(_processes(), profile):
        return
    for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        try:
            os.unlink(os.path.join(profile, name))
        except OSError:
            continue


def end_on_profile(profile: str) -> None:
    """Kill every Chrome of the engine's on *profile*, and the daemon that ran
    it, waiting until they are gone. For a profile about to be erased, which a
    Chrome closing in its own time would write back."""
    browsers = _engine_browsers(_processes(), profile)
    for p in browsers:
        try:
            os.kill(p.pid, signal.SIGKILL)
        except OSError:
            continue
    deadline = time.monotonic() + _BROWSER_GONE_S
    while time.monotonic() < deadline and _any_left(browsers):
        time.sleep(0.05)
    end_browserless(admitting=True)


def browser_up() -> bool:
    """Whether a Chrome the engine's daemon runs is up. One its dead daemon left
    behind is not: no command reaches it, and the next command reaps it."""
    return any(_run_by_engine(p) for p in _engine_browsers(_processes()))


def _run_by_engine(browser: _Process) -> bool:
    try:
        return os.readlink(_PROC / str(browser.ppid) / "exe") == ENGINE_BIN
    except OSError:
        return False


def engine_busy() -> bool:
    """Whether a browser command is running, ours or the engine's own: either
    may be starting a browser that is not yet listening.

    The engine's daemon is the same binary with no arguments, and it outlives
    a Chrome the OOM killer took by up to its idle timeout, so counting it
    would keep a person's start from opening anything for that long.
    """
    return _engine_busy(_processes())


def engine_gone(*, admitting: bool = False) -> bool:
    """Whether nothing of the engine runs: no daemon, so no browser, and no
    command that may be starting one. A caller *admitting* a command holds the
    gate's lock, so another of our commands is not one, as in ``end_browserless``."""
    processes = _processes()
    return not _engine_daemons(processes) and not _engine_busy(
        processes, exclude=os.getpid(), wrappers=not admitting
    )


def _engine_busy(processes: list[_Process], exclude: int | None = None, wrappers: bool = True) -> bool:
    wrapper = str(Path(__file__).resolve().parent)
    uid = os.getuid()
    for p in processes:
        if p.uid != uid or p.pid == exclude:
            continue
        try:
            # The wrapper on PATH names this package through its own folder.
            if wrappers and p.argv[1:2] == ["-I"] and p.argv[3:4] == ["agent-browser"] and (
                os.path.realpath(os.path.join(os.readlink(_PROC / str(p.pid) / "cwd"), p.argv[2]))
                == wrapper
            ):
                return True
            if any(p.argv[1:]) and os.readlink(_PROC / str(p.pid) / "exe") == ENGINE_BIN:
                return True
        except OSError:
            continue
    return False


def _engine_daemons(processes: list[_Process]) -> list[_Process]:
    """The engine's daemons: its binary run with no arguments."""
    uid = os.getuid()
    found = []
    for p in processes:
        if p.uid != uid or any(p.argv[1:]):
            continue
        try:
            if os.readlink(_PROC / str(p.pid) / "exe") == ENGINE_BIN:
                found.append(p)
        except OSError:
            continue
    return found


def _reap_left_behind() -> None:
    """A daemon killed outright leaves its Chrome running, holding memory, and neither
    the next command nor ``close`` can reach it.

    The person's hold on it and what it was launched with go too, or the next
    browser would start held, or on its user agent. Only once it is gone, so the
    next look at the processes cannot find it again and forget what a command
    records for the browser after it.
    """
    reaped = []
    for p in _engine_browsers(_processes()):
        if _run_by_engine(p):
            continue
        try:
            os.kill(p.pid, signal.SIGKILL)
        except OSError:
            continue
        reaped.append(p)
    if not reaped:
        return
    deadline = time.monotonic() + _BROWSER_GONE_S
    while time.monotonic() < deadline and _any_left(reaped):
        time.sleep(0.05)
    hold.forget()
    launch.forget()
    for p in reaped:
        # The kept profile stays, sign-ins and all, for the next browser.
        profile = _profile(p)
        if profile and _throwaway(profile):
            shutil.rmtree(profile, ignore_errors=True)


def _any_left(browsers: list[_Process]) -> bool:
    left = {p.pid for p in _engine_browsers(_processes())}
    return any(p.pid in left for p in browsers)


def _browser_dies_first() -> None:
    """Put the whole browser ahead of the computer's other work for the OOM killer.

    Chrome lowers its renderers from what they inherit, and a large job outranks
    them once the top of the tree is gone but before their memory is back, so the
    job is killed anyway. The next command brings a browser back; analysis cannot be.
    """
    processes = _processes()
    children: dict[int, list[int]] = {}
    for p in processes:
        children.setdefault(p.ppid, []).append(p.pid)
    pending = [p.pid for p in _engine_browsers(processes)]
    while pending:
        pid = pending.pop()
        pending.extend(children.get(pid, ()))
        try:
            (_PROC / str(pid) / "oom_score_adj").write_text("1000")
        except OSError:
            continue
