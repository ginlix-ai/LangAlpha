"""What the runtime reads of the machine, where Linux and a Mac keep it
differently: its processes, its memory and CPU limits, and whether memory is
short on a machine the person also uses."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import struct
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

_PROC = Path("/proc")
_MEMINFO = Path("/proc/meminfo")
_CGROUP = Path("/sys/fs/cgroup")
#: Where cgroup v2, then v1, keep the memory limit and what is charged against
#: it, and the counters of what cannot be reclaimed. v1 counts the whole
#: subtree in ``total_``.
_CGROUP_MEMORY = (
    ("memory.max", "memory.stat", ("anon", "shmem")),
    ("memory/memory.limit_in_bytes", "memory/memory.stat", ("total_rss", "total_shmem")),
)
#: v1 has no word for no memory limit, only a number no machine has.
_NO_LIMIT = 1 << 62
#: Share of a Linux machine's memory that must stay available for the browser
#: to open more of itself on a machine the person also uses.
_AVAILABLE_SHARE = 0.1
#: The Mac's memory pressure past its warning, which a Mac in everyday use
#: sits at: critical, at which it starts ending apps (1 normal, 2 warn, 4 critical).
_MAC_PRESSURE_CRITICAL = 4
# libproc's record of a process (``proc_bsdinfo``): where its status sits, and
# its parent's pid, followed by its owner's uid.
_PROC_PIDTBSDINFO = 3
_BSDINFO_SIZE = 136
_BSDINFO_STATUS = 4
_BSDINFO_PPID = 16
#: A process that has ended, its files closed, and that only waits for its parent
#: to collect it: not one of the machine's, for anything here that asks.
_MAC_ZOMBIE = 5
_LINUX_ENDED = frozenset("ZX")
#: What Linux adds to a process's file once that file is removed, as a tool
#: update does with a version two behind while a browser still runs it.
_LINUX_DELETED = " (deleted)"
_PATH_MAX = 4 * 1024
# sysctl's names for the arguments a process was started with.
_CTL_KERN = 1
_KERN_ARGMAX = 8
_KERN_PROCARGS2 = 49


@dataclass(frozen=True)
class Process:
    """``exe`` is the file the process runs, read only for this user's own,
    and named by its path even once that file is removed."""

    pid: int
    ppid: int
    uid: int
    argv: list[str]
    exe: str | None = None


def processes(wanted: Callable[[str], bool] | None = None) -> list[Process]:
    """Every process the machine shows that has not ended, read once. On a Mac
    only those of this user's whose file *wanted* takes have their arguments
    read; the rest have none, as a process whose arguments cannot be read has
    none on Linux."""
    return _mac_processes(wanted) if sys.platform == "darwin" else _linux_processes()


def _linux_processes() -> list[Process]:
    found = []
    me = os.getuid()
    try:
        entries = [entry for entry in _PROC.iterdir() if entry.name.isdigit()]
    except OSError:
        return []
    for entry in entries:
        try:
            # The name in the second field can hold anything, so read from after it.
            state, ppid_field = (entry / "stat").read_text().rpartition(")")[2].split()[:2]
            ppid = int(ppid_field)
            uid = entry.stat().st_uid
        except (OSError, ValueError):
            continue
        if state in _LINUX_ENDED:
            continue
        try:
            argv = (entry / "cmdline").read_bytes().decode(errors="replace").split("\0")
        except OSError:
            # Still a parent or a child in the browser's tree.
            argv = []
        exe = file_named(str(entry / "exe")) if uid == me else None
        found.append(Process(int(entry.name), ppid, uid, argv, exe))
    return found


def process_file(pid: int) -> str:
    """Linux's link to the file *pid* runs, which runs that very file while
    the process lives, even once the path it started from names another."""
    return str(_PROC / str(pid) / "exe")


def file_named(link: str) -> str | None:
    """The path a process's file was run from, read from its link
    (``process_file``), and named so even once that file is removed."""
    try:
        named = os.readlink(link)
    except OSError:
        return None
    return named[: -len(_LINUX_DELETED)] if named.endswith(_LINUX_DELETED) else named


def _mac_processes(wanted: Callable[[str], bool] | None) -> list[Process]:
    """From the kernel, a call for each process rather than ``ps``, which takes
    half a second on a Mac with thousands of them. The arguments come whole,
    where ``ps`` joins them with spaces and Chrome's own path has one."""
    libc = _library("c")
    libproc = _library("proc")
    if libc is None or libproc is None:
        return []
    count = libproc.proc_listallpids(None, 0)
    if count <= 0:
        return []
    # Room for processes that start between the two calls.
    pids = (ctypes.c_int * (count + 256))()
    count = libproc.proc_listallpids(pids, ctypes.sizeof(pids))
    me = os.getuid()
    info = ctypes.create_string_buffer(_BSDINFO_SIZE)
    path = ctypes.create_string_buffer(_PATH_MAX)
    arguments = None
    found = []
    for pid in pids[: max(count, 0)]:
        if libproc.proc_pidinfo(pid, _PROC_PIDTBSDINFO, 0, info, _BSDINFO_SIZE) != _BSDINFO_SIZE:
            continue
        (status,) = struct.unpack_from("=I", info.raw, _BSDINFO_STATUS)
        if status == _MAC_ZOMBIE:
            continue
        ppid, uid = struct.unpack_from("=II", info.raw, _BSDINFO_PPID)
        argv: list[str] = []
        exe = None
        if uid == me and libproc.proc_pidpath(pid, path, _PATH_MAX) > 0:
            exe = os.fsdecode(path.value)
            if wanted is None or wanted(exe):
                if arguments is None:
                    arguments = ctypes.create_string_buffer(_argmax(libc))
                argv = _mac_arguments(libc, arguments, pid)
        found.append(Process(pid, ppid, uid, argv, exe))
    return found


def _library(name: str) -> ctypes.CDLL | None:
    try:
        return ctypes.CDLL(ctypes.util.find_library(name), use_errno=True)
    except OSError:
        return None


def _argmax(libc: ctypes.CDLL) -> int:
    value = ctypes.c_int(0)
    size = ctypes.c_size_t(ctypes.sizeof(value))
    mib = (ctypes.c_int * 2)(_CTL_KERN, _KERN_ARGMAX)
    if libc.sysctl(mib, 2, ctypes.byref(value), ctypes.byref(size), None, 0) != 0 or value.value <= 0:
        return 1 << 20
    return value.value


def _mac_arguments(libc: ctypes.CDLL, buffer: ctypes.Array, pid: int) -> list[str]:
    """The process's arguments: a count, the path it was started from,
    padding, then the arguments, each ended by a NUL."""
    size = ctypes.c_size_t(len(buffer))
    mib = (ctypes.c_int * 3)(_CTL_KERN, _KERN_PROCARGS2, pid)
    if libc.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) != 0 or size.value < 4:
        return []
    data = ctypes.string_at(buffer, size.value)
    count = int.from_bytes(data[:4], sys.byteorder)
    rest = data[4:].partition(b"\0")[2]
    return [os.fsdecode(word) for word in rest.lstrip(b"\0").split(b"\0")[:count]]


def memory_pressed() -> bool:
    """Whether memory is short on a machine the person also uses, by what the
    machine itself reports rather than a share of it: their own apps hold much
    of it, and the browser goes before them, not after."""
    if sys.platform == "darwin":
        level = _mac_sysctl_int(b"kern.memorystatus_vm_pressure_level")
        return level is not None and level >= _MAC_PRESSURE_CRITICAL
    info = _meminfo()
    try:
        return info is not None and info["MemAvailable"] < info["MemTotal"] * _AVAILABLE_SHARE
    except KeyError:
        return False


def memory_held() -> tuple[int, int] | None:
    """The computer's memory and what of it cannot be reclaimed, or None where
    the machine says neither: its own limit where it has one, as a container
    does, else all the memory of a machine with none, such as a microVM."""
    measured = _cgroup_memory()
    if measured is not None:
        return measured
    info = _meminfo()
    try:
        return (info["MemTotal"], info["AnonPages"] + info["Shmem"]) if info is not None else None
    except KeyError:
        return None


def _cgroup_memory() -> tuple[int, int] | None:
    for limit_file, stat_file, counters in _CGROUP_MEMORY:
        try:
            limit = int((_CGROUP / limit_file).read_text())
            stat = dict(line.split() for line in (_CGROUP / stat_file).read_text().splitlines())
            used = sum(int(stat[name]) for name in counters)
        except (OSError, ValueError, KeyError):
            continue
        return (limit, used) if limit < _NO_LIMIT else None
    return None


def _meminfo() -> dict[str, int] | None:
    """Linux's ``/proc/meminfo``, in kB, or None where there is none to read."""
    try:
        return {
            name: int(rest.split()[0])
            for name, rest in (line.split(":", 1) for line in _MEMINFO.read_text().splitlines())
        }
    except (OSError, ValueError, IndexError):
        return None


def cpu_quota() -> float | None:
    """The computer's CPU limit under cgroup v2, else v1, or None without one."""
    try:
        quota, period = (_CGROUP / "cpu.max").read_text().split()
    except (OSError, ValueError):
        try:
            quota = (_CGROUP / "cpu/cpu.cfs_quota_us").read_text().strip()
            period = (_CGROUP / "cpu/cpu.cfs_period_us").read_text().strip()
        except OSError:
            return None
    try:
        # v2 says ``max`` for no limit, v1 ``-1``.
        return int(quota) / int(period) if quota not in ("max", "-1") else None
    except (ValueError, ZeroDivisionError):
        return None


def has_oom_killer() -> bool:
    """Whether the machine has Linux's OOM killer, whose scores ``/proc`` keeps."""
    return _PROC.is_dir()


def set_oom_score(pid: int, score: int) -> None:
    """Rank *pid* for the OOM killer, from -1000 never to 1000 first. A process
    gone or not this user's is left as it is."""
    try:
        (_PROC / str(pid) / "oom_score_adj").write_text(str(score))
    except OSError:
        pass


def _mac_sysctl_int(name: bytes) -> int | None:
    libc = _library("c")
    if libc is None:
        return None
    value = ctypes.c_int(0)
    size = ctypes.c_size_t(ctypes.sizeof(value))
    if libc.sysctlbyname(name, ctypes.byref(value), ctypes.byref(size), None, 0) != 0:
        return None
    return value.value
