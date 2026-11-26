"""Bring a machine's toolset to the table, one unit at a time.

Each version installs into its own folder beside the last and is swapped in
by one link, so a command already running keeps its file, one starting after
gets the new one, and nothing is ever half there. Nothing running is ended.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import glob
import hashlib
import http.client
import json
import os
import platform
import shutil
import stat
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Iterator

from . import units
from .units import Unit

STATE = "state.json"
LOCK = ".lock"
#: The pid of the converge holding the lock, and when that process started, for
#: readers who may not take it.
CONVERGING = "converging"
#: What each unit's answer is when it did not fail.
CURRENT, INSTALLED, ADOPTED = "current", "installed", "adopted"
_CURRENT_LINK = "current"
_TEMP_PREFIX = ".tmp-"
_DOWNLOAD_TIMEOUT_S = 120
_CHUNK = 1 << 20
#: The most a release the table pins no size for may download: far past any
#: engine, so only a host sending more than a release can fill the disk.
_MAX_UNSIZED = 512 << 20
#: How long a converge waits for one already holding the lock: longer than a
#: run's every download, shorter than the server waits on the call, so one stuck
#: holder costs later runs an answer, not a process each left waiting on it.
_LOCK_WAIT_S = 480
_LOCK_POLL_S = 0.5
#: The state file's and the marker's: root writes them, anyone may read them.
_READABLE = 0o644


class Failed(Exception):
    """A unit left as it was, for ``reason``."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason


@dataclass
class Answer:
    """What one converge prints for the server, which reads it as JSON."""

    target: str
    version: str | None
    units: dict[str, str] = field(default_factory=dict)
    #: Units that failed, by reason; each kept its old version.
    failures: dict[str, str] = field(default_factory=dict)
    #: Set when the machine took a newer table, and then nothing was touched.
    refused: str | None = None

    @property
    def failed(self) -> bool:
        """What ``--strict`` exits 1 on."""
        return bool(self.failures) or self.refused is not None

    def to_json(self) -> str:
        outcomes = dict(self.units)
        outcomes.update((name, f"failed:{reason}") for name, reason in self.failures.items())
        answer: dict[str, object] = {"version": self.version, "target": self.target, "units": outcomes}
        if self.refused is not None:
            answer["refused"] = self.refused
        return json.dumps(answer, sort_keys=True)


def platform_name() -> str | None:
    """This machine's platform, as a binary unit's releases are keyed."""
    system = {"darwin": "darwin", "linux": "linux"}.get(sys.platform)
    machine = {"x86_64": "x64", "amd64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
        platform.machine().lower()
    )
    return f"{system}-{machine}" if system and machine else None


def converge(root: str, table: units.Table) -> Answer:
    """Install every unit that is not current, under the root's lock."""
    # Every link written points at a path under it, which a relative root
    # would make relative to the link's own folder.
    root = os.path.abspath(root)
    with contextlib.ExitStack() as held:
        try:
            _make_dirs(root)
            held.enter_context(_locked(root))
        except TimeoutError:
            print(f"toolset: {root}: another converge held the lock for {_LOCK_WAIT_S} s", file=sys.stderr)
            return Answer(table.version, None, failures={u["name"]: "busy" for u in table.units})
        except OSError as exc:
            print(f"toolset: {root}: {exc}", file=sys.stderr)
            return Answer(table.version, None, failures={u["name"]: "root" for u in table.units})
        try:
            return _converge(root, table)
        except OSError as exc:
            # The record could not be written: what was swapped in stays, and
            # the next run, finding it in place, records it.
            print(f"toolset: {root}: cannot write {STATE}: {exc}", file=sys.stderr)
            return Answer(table.version, None, failures={u["name"]: "state" for u in table.units})


def _converge(root: str, table: units.Table) -> Answer:
    try:
        state = read_state(root, strict=True)
    except OSError as exc:
        # The record holds the revision this machine took, so a run that
        # cannot read it cannot know which tables to refuse.
        print(f"toolset: {root}: cannot read {STATE}: {exc}", file=sys.stderr)
        return Answer(table.version, None, failures={u["name"]: "state" for u in table.units})
    if state["revision"] is not None and table.revision < state["revision"]:
        print(
            f"toolset: table revision {table.revision} is older than {state['revision']}, which this machine has taken",
            file=sys.stderr,
        )
        return Answer(table.version, state["version"], refused="older")
    record = {
        "revision": table.revision,
        "version": state["version"],
        "units": dict(state["units"]),
        "exposed": dict(state["exposed"]),
    }
    if record["revision"] != state["revision"]:
        # Taken before any unit moves, so a run cut short after its first swap
        # still refuses the older table that would swap it back.
        write_state(root, record)
    answer = Answer(table.version, state["version"])
    for unit in table.units:
        name = unit["name"]
        prior = record["exposed"].get(name, [])
        try:
            _outside(root, unit)
            both = list(dict.fromkeys([*prior, *unit["expose"]]))
            if both != prior:
                # Recorded before any name is made, so a run cut short still
                # knows every name a later table may drop.
                record["exposed"][name] = both
                write_state(root, record)
            outcome = _converge_unit(root, unit, record["units"].get(name))
        except Failed as exc:
            answer.failures[name] = exc.reason
            print(f"toolset: {name}: {exc}", file=sys.stderr)
            continue
        answer.units[name] = outcome
        still = {_identity(p) for p in unit["expose"]}
        left = _retire(root, name, [p for p in prior if _identity(p) not in still])
        record["exposed"][name] = left + list(unit["expose"])
        if left:
            answer.failures[name] = "retire"
            print(f"toolset: {name}: cannot remove {', '.join(left)}", file=sys.stderr)
        if outcome != CURRENT:
            record["units"][name] = units.pin(unit)
            # Kept as each unit lands, so a run cut short leaves a record of
            # what it did swap in.
            write_state(root, record)
    named = {u["name"] for u in table.units}
    for name in sorted(set(record["exposed"]) - named):
        # A unit the table no longer has is taken off the machine, so a tool
        # dropped from the table, a vulnerable one say, is no longer run.
        left = _retire(root, name, record["exposed"].pop(name))
        if left:
            record["exposed"][name] = left
            answer.failures[name] = "retire"
            print(f"toolset: {name}: cannot remove {', '.join(left)}", file=sys.stderr)
    # Every folder of a unit the table does not have and whose names are gone,
    # not only one dropped now: a version a process ran from when its unit was
    # dropped is removed by a later run, once nothing runs from it.
    for entry in sorted(os.listdir(root)):
        if (
            units.plain_name(entry)
            and entry not in named
            and entry not in record["exposed"]
            and entry not in (STATE, LOCK, CONVERGING)
        ):
            _drop_unit(os.path.join(root, entry))
    record["units"] = {name: v for name, v in record["units"].items() if name in named}
    # A set's version says every unit is at it; with one behind, it stays
    # where it was, so the next run retries.
    if not answer.failures:
        record["version"] = table.version
    if record != state:
        write_state(root, record)
    answer.version = record["version"]
    return answer


def status(root: str, table: units.Table) -> dict[str, object]:
    state = read_state(root)
    return {
        "revision": state["revision"],
        "version": state["version"],
        "target": table.version,
        "converging": converging(root),
        "units": state["units"],
    }


def _converge_unit(root: str, unit: Unit, held: str | None) -> str:
    """The one place that decides between current, adopted and installed."""
    kind, slot = _KINDS[unit["kind"]](unit), _Slot(root, unit)
    is_held = held == units.pin(unit)
    if is_held and slot.in_place():
        return CURRENT
    # What an image put in place before the toolset existed is taken as it is
    # when it is exactly this version: an older browser runtime knows its
    # engine only by that path, so a link there would lose it its running
    # browser, and nothing needs downloading. Nothing records the adoption,
    # since these paths answer it again on every run. A path already pointing
    # through ``current`` is the toolset's own, which a link's realpath would
    # otherwise pass for any version whose target has not moved.
    if (
        unit["expose"]
        and not slot.exposes_any()
        and all(kind.holds(path) for path in unit["expose"])
    ):
        return CURRENT if is_held else ADOPTED
    try:
        if not kind.holds(slot.file):
            slot.install(kind.write)
        was = slot.swap_in()
    except OSError as exc:
        raise Failed("io", str(exc)) from exc
    _drop_older(slot.dir, {slot.version, was})
    return INSTALLED


class _Kind:
    """How one kind of unit is checked and written; nothing else differs
    between kinds."""

    def __init__(self, unit: Unit) -> None:
        self.unit = unit

    def holds(self, path: str) -> bool:
        """Whether *path* carries exactly this version: asked of the expose
        paths to adopt them, and of a version folder's file before reusing it,
        since a crash can leave that folder renamed in with its file cut short."""
        try:
            return self._holds(path)
        except OSError:
            return False

    def _holds(self, path: str) -> bool:
        raise NotImplementedError

    def write(self, path: str) -> None:
        """Put this version at *path*, raising ``Failed`` when it cannot."""
        raise NotImplementedError


class _Binary(_Kind):
    def _holds(self, path: str) -> bool:
        release = _release(self.unit)
        return (
            release is not None
            and _regular(path)
            # The release's bytes are no engine where they cannot run.
            and _has_mode(path, self.unit)
            and _is_release(path, release)
        )

    def write(self, path: str) -> None:
        _fetch(self.unit, path)
        os.chmod(path, units.mode(self.unit))


class _Link(_Kind):
    def _holds(self, path: str) -> bool:
        found = glob.glob(self.unit["target_glob"])
        return (
            len(found) == 1
            and os.path.isfile(path)
            and os.path.realpath(path) == os.path.realpath(found[0])
        )

    def write(self, path: str) -> None:
        os.symlink(_link_target(self.unit["target_glob"]), path)


class _File(_Kind):
    def _holds(self, path: str) -> bool:
        content = units.content_bytes(self.unit)
        return (
            _regular(path)
            and _has_mode(path, self.unit)
            and os.path.getsize(path) == len(content)
            and _read(path) == content
        )

    def write(self, path: str) -> None:
        with open(path, "wb") as out:
            out.write(units.content_bytes(self.unit))
            _sync(out)
        os.chmod(path, units.mode(self.unit))


_KINDS = {"binary": _Binary, "link": _Link, "file": _File}


class _Slot:
    """A unit's place under the root: ``<name>/<pin>/<file>`` for each version,
    ``<name>/current`` naming one, and each expose path a link through
    ``current``. A folder is named by the pin rather than the version, so a
    release pinned anew under the same version installs beside the one a
    running browser started on instead of over it, and the browser sees it
    replaced."""

    def __init__(self, root: str, unit: Unit) -> None:
        self.version = units.pin(unit)
        self.expose = unit["expose"]
        self.dir = os.path.join(root, unit["name"])
        self.version_dir = os.path.join(self.dir, self.version)
        self.file = os.path.join(self.version_dir, unit["file"])
        self.current = os.path.join(self.dir, _CURRENT_LINK)
        self.exposed = os.path.join(self.current, unit["file"])

    def in_place(self) -> bool:
        """Whether this version is swapped in and every name points through
        it, asked of a version the record says is there, so nothing is read."""
        return (
            _readlink(self.current) == self.version
            and os.path.isfile(self.file)
            and all(_readlink(path) == self.exposed for path in self.expose)
        )

    def exposes_any(self) -> bool:
        """Whether a name already points into this unit's folder, through
        ``current`` or straight at a version, so it is the toolset's and not an
        image's to adopt."""
        inside = os.path.join(self.dir, "")
        return any((_readlink(path) or "").startswith(inside) for path in self.expose)

    def install(self, write: Callable[[str], None]) -> None:
        """Write the version's folder beside the others and rename it in."""
        _make_dirs(self.dir)
        staging = tempfile.mkdtemp(prefix=_TEMP_PREFIX, dir=self.dir)
        try:
            write(os.path.join(staging, os.path.basename(self.file)))
            # mkdtemp's folder is the owner's alone, and these are run by everyone.
            os.chmod(staging, 0o755)
            if os.path.lexists(self.version_dir):
                # A folder that is not whole may be the one ``current`` names,
                # so it is moved aside rather than removed first: the name is
                # without a folder only between the two renames.
                aside = tempfile.mkdtemp(prefix=_TEMP_PREFIX, dir=self.dir)
                old = os.path.join(aside, "old")
                os.rename(self.version_dir, old)
                try:
                    os.rename(staging, self.version_dir)
                except BaseException:
                    os.rename(old, self.version_dir)
                    raise
                _remove(aside)
            else:
                os.rename(staging, self.version_dir)
        except BaseException:
            _remove(staging)
            raise

    def swap_in(self) -> str | None:
        """Point ``current`` and the names at this version; the version
        ``current`` named before, if any."""
        # Every name's place is made ready before any name moves, so one that
        # cannot be made fails the unit with every name as it was.
        for path in self.expose:
            if os.path.isdir(path) and not os.path.islink(path):
                raise Failed("expose", f"{path} is a folder")
            _make_dirs(os.path.dirname(path))
        was = _readlink(self.current)
        if was is None:
            swap_link(self.version, self.current)
            self._expose(self.expose)
            return was
        if os.path.exists(self.exposed):
            # Every name points through ``current``, so the names are made
            # first and the swap of ``current`` is the one step that moves the
            # version: a name that cannot be made leaves the old version in use.
            self._expose(self.expose)
            swap_link(self.version, self.current)
            return was
        # The file is renamed, so a name through ``current`` to this version's
        # file would find nothing there until ``current`` moves: each name
        # points straight at this version's file first, then through
        # ``current`` once it has moved, and works at every step. A run cut
        # short leaves the names working, and the next points them through
        # ``current``.
        moving = [path for path in self.expose if _readlink(path) != self.exposed]
        swap_links([(self.file, path) for path in moving])
        swap_link(self.version, self.current)
        self._expose(moving)
        return was

    def _expose(self, paths: list[str]) -> None:
        swap_links([(self.exposed, path) for path in paths if _readlink(path) != self.exposed])


def _make_dirs(path: str) -> None:
    """Make *path* and any folder missing above it, each one every user can
    open whatever the umask: commands reach the engine and read the record
    as the computer's user."""
    missing = []
    while not os.path.isdir(path):
        if os.path.lexists(path):
            raise NotADirectoryError(f"{path} is in the way of a folder")
        missing.append(path)
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    for folder in reversed(missing):
        with contextlib.suppress(FileExistsError):
            os.mkdir(folder)
        os.chmod(folder, 0o755)


def _has_mode(path: str, unit: Unit) -> bool:
    return stat.S_IMODE(os.stat(path).st_mode) == units.mode(unit)


def _link_target(pattern: str) -> str:
    found = glob.glob(pattern)
    if not found:
        raise Failed("no-target", f"nothing matches {pattern}")
    if len(found) > 1:
        raise Failed("several-targets", f"{len(found)} paths match {pattern}")
    if not os.path.isfile(found[0]):
        raise Failed("no-target", f"{found[0]} is not a file")
    return found[0]


def _release(unit: Unit) -> dict | None:
    name = platform_name()
    return unit["platforms"].get(name) if name else None


def _regular(path: str) -> bool:
    return os.path.isfile(path) and not os.path.islink(path)


def _is_release(path: str, release: dict) -> bool:
    """Whether *path* holds *release*'s bytes, by size first when the table
    pins one, so a file cut short is told apart without reading it."""
    size = release.get("size")
    if size is not None and os.path.getsize(path) != size:
        return False
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest() == release["sha256"]


def _read(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _sync(f) -> None:
    """Flush *f* to disk, so a crash after the rename that follows cannot
    leave its name on a file cut short."""
    f.flush()
    os.fsync(f.fileno())


def _fetch(unit: Unit, path: str) -> None:
    """Download this platform's release to *path*, checked as it streams."""
    release = _release(unit)
    if release is None:
        raise Failed("platform", f"no release for {platform_name() or platform.machine()}")
    url, pinned = release["url"], release.get("size")
    digest = hashlib.sha256()
    size = 0
    deadline = time.monotonic() + _DOWNLOAD_TIMEOUT_S
    try:
        with urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT_S) as response, open(path, "wb") as out:
            while True:
                # Whatever has arrived, so a connection that trickles still
                # meets the deadline between reads.
                chunk = response.read1(_CHUNK)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                if size > (_MAX_UNSIZED if pinned is None else pinned):
                    # Stopped before it is written, so a mirror sending more
                    # than the release cannot fill the computer's disk.
                    raise Failed("size", f"{url} is more than {pinned or _MAX_UNSIZED} bytes")
                out.write(chunk)
                if time.monotonic() > deadline:
                    raise Failed("download", f"{url} took longer than {_DOWNLOAD_TIMEOUT_S} s")
            _sync(out)
    except (OSError, ValueError, http.client.HTTPException) as exc:
        raise Failed("download", f"{url}: {exc}") from exc
    if pinned is not None and size != pinned:
        raise Failed("size", f"{url} is {size} bytes, not {pinned}")
    if digest.hexdigest() != release["sha256"]:
        raise Failed("digest", f"{url} is not the release the table pins")


def swap_link(value: str, path: str) -> None:
    """Make ``path`` a symlink to ``value`` in one step, over whatever file
    or link is there. One name needs no second name to go back to: a replace
    that fails leaves it as it was."""
    tmp = path + STAGED_SUFFIX
    with contextlib.suppress(FileNotFoundError):
        os.unlink(tmp)
    os.symlink(value, tmp)
    try:
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


#: Beside each name a swap replaces: its new link, and a second name for what
#: it replaces, kept until every name has moved. ``units`` refuses to expose
#: a name ending in either.
STAGED_SUFFIX, KEPT_SUFFIX = units.STAGED_SUFFIX, units.KEPT_SUFFIX


def swap_links(links: list[tuple[str, str]]) -> None:
    """Make each ``(value, path)`` a symlink, all or none of them.

    Every new link is made beside its path, and what each path holds is given
    a second name, before any path is replaced, so one that cannot be made (no
    room, something in the way) fails with every path as it was; then each is
    replaced in one step, and a replace that fails puts back the ones before it.
    A second name that could not be put back is left beside its path, which
    holds the new link and works. A second name left over is never put back,
    since one a finished swap could not remove is stale: the next swap of its
    path drops it.
    """
    staged: list[tuple[str, str, str | None]] = []

    def drop_staged(keep: frozenset = frozenset()) -> None:
        for tmp, _, kept in staged:
            for name in (tmp, kept):
                if name is not None and name not in keep:
                    with contextlib.suppress(OSError):
                        os.unlink(name)

    try:
        for value, path in links:
            tmp, kept = path + STAGED_SUFFIX, path + KEPT_SUFFIX
            for name in (tmp, kept):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(name)
            os.symlink(value, tmp)
            staged.append((tmp, path, None))
            if os.path.lexists(path):
                os.link(path, kept, follow_symlinks=False)
                staged[-1] = (tmp, path, kept)
    except BaseException:
        drop_staged()
        raise
    done: list[tuple[str, str | None]] = []
    for tmp, path, kept in staged:
        try:
            os.replace(tmp, path)
        except OSError:
            stuck = set()
            for moved, was in reversed(done):
                try:
                    if was is None:
                        os.unlink(moved)
                    else:
                        os.replace(was, moved)
                except OSError:
                    if was is not None:
                        stuck.add(was)
            drop_staged(frozenset(stuck))
            raise
        done.append((path, kept))
    for _, _, kept in staged:
        if kept is not None:
            with contextlib.suppress(OSError):
                os.unlink(kept)


def _identity(path: str) -> str:
    """The entry *path* names, its folder resolved through any link: two
    spellings of one entry, ``/bin/x`` and ``/usr/bin/x`` on a merged ``/usr``,
    are one name."""
    return os.path.join(os.path.realpath(os.path.dirname(path)), os.path.basename(path))


def _outside(root: str, unit: Unit) -> None:
    """Fail *unit* if a name it exposes is inside the root, as resolved through
    any link on the way, where it would stand in for the record or a unit's
    folder; before anything of the unit is recorded or made."""
    inside = os.path.join(os.path.realpath(root), "")
    for path in unit["expose"]:
        if os.path.join(_identity(path), "").startswith(inside):
            raise Failed("expose", f"{path} is inside {root}")


def _retire(root: str, name: str, paths: list[str]) -> list[str]:
    """Unlink each of *paths* that is a link into unit *name*'s folder, a name
    the table no longer gives it; the ones that could not be, which the record
    keeps so a later run tries them again. A path that is no such link was put
    there by an image or a person, and stays."""
    inside = os.path.join(os.path.realpath(os.path.join(root, name)), "")
    left = []
    for path in paths:
        try:
            target = os.readlink(path)
        except (FileNotFoundError, NotADirectoryError):
            continue
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                # Not a link at all.
                continue
            left.append(path)
            continue
        # Its folder resolved, not the file it names, which a link unit keeps
        # outside the root.
        if not os.path.join(os.path.realpath(os.path.dirname(target)), "").startswith(inside):
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            left.append(path)
    return left


def _drop_unit(unit_dir: str) -> None:
    """Remove a unit the table dropped, all but a version a process still runs
    from, as ``_drop_older`` keeps one."""
    if not os.path.isdir(unit_dir):
        return
    _drop_older(unit_dir, set())
    with contextlib.suppress(OSError):
        if os.listdir(unit_dir) == [_CURRENT_LINK]:
            os.unlink(os.path.join(unit_dir, _CURRENT_LINK))
            os.rmdir(unit_dir)


def _drop_older(unit_dir: str, keep: set) -> None:
    """Keep the version swapped in and the one before it, to go back to, and
    any version a process still runs from: a browser keeps the engine it
    started on until it closes, which can outlast two updates."""
    running = _running_from(unit_dir)
    for entry in os.listdir(unit_dir):
        if entry != _CURRENT_LINK and entry not in keep and entry not in running:
            with contextlib.suppress(OSError):
                _remove(os.path.join(unit_dir, entry))


def _running_from(unit_dir: str) -> set:
    """The versions under *unit_dir* a live process executes, read from
    ``/proc``; none where there is no ``/proc``."""
    prefix = os.path.realpath(unit_dir) + os.sep
    found = set()
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return found
    for pid in pids:
        exe = _readlink(f"/proc/{pid}/exe")
        if exe is None:
            continue
        # Linux names a file replaced under a running process "<path> (deleted)".
        if exe.endswith(" (deleted)"):
            exe = exe[: -len(" (deleted)")]
        if exe.startswith(prefix):
            found.add(exe[len(prefix):].split(os.sep, 1)[0])
    return found


def _remove(path: str) -> None:
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path, ignore_errors=True)
    else:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


def _readlink(path: str) -> str | None:
    try:
        return os.readlink(path)
    except OSError:
        return None


def read_state(root: str, *, strict: bool = False) -> dict:
    """What the last run recorded; nothing, if there is none or what is there
    is not a record. *strict* raises the ``OSError`` of a record that is there
    and could not be read, rather than read it as none. An ``adopted`` list
    earlier runs kept is ignored, as adoption is read from the expose paths
    themselves. ``exposed`` holds the names each unit was given, so a name a
    later table drops, or a whole unit, can be taken off."""
    empty: dict = {"revision": None, "version": None, "units": {}, "exposed": {}}
    try:
        with open(os.path.join(root, STATE), encoding="utf-8") as f:
            state = json.load(f)
    except FileNotFoundError:
        return empty
    except OSError:
        if strict:
            raise
        return empty
    except ValueError:
        return empty
    if not isinstance(state, dict):
        return empty
    revision, version, held = state.get("revision"), state.get("version"), state.get("units")
    if not (revision is None or (type(revision) is int and revision > 0)):
        return empty
    if not (version is None or isinstance(version, str)):
        return empty
    if not (isinstance(held, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in held.items())):
        return empty
    exposed = state.get("exposed", {})
    if not (
        isinstance(exposed, dict)
        and all(
            units.plain_name(k) and isinstance(v, list) and all(isinstance(p, str) and os.path.isabs(p) for p in v)
            for k, v in exposed.items()
        )
    ):
        exposed = {}
    return {"revision": revision, "version": version, "units": held, "exposed": exposed}


def write_state(root: str, state: dict) -> None:
    """Write *state* (``revision``, ``version``, ``units``, ``exposed``) in one rename."""
    record = {
        "revision": state["revision"],
        "version": state["version"],
        "units": state["units"],
        "exposed": state.get("exposed", {}),
    }
    _write_readable(os.path.join(root, STATE), json.dumps(record, sort_keys=True).encode())


def _write_readable(path: str, data: bytes) -> None:
    """Replace *path* with *data* in one rename, synced first so the rename
    never names a file cut short, and readable by everyone: the browser's
    ``status`` reads these as the computer's user. Only root writes them."""
    fd, tmp = tempfile.mkstemp(prefix=f".{os.path.basename(path)}-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            _sync(f)
        os.chmod(tmp, _READABLE)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise


@contextlib.contextmanager
def _locked(root: str) -> Iterator[None]:
    """Hold the root's lock for the block, announced in ``CONVERGING``; waits
    for a run already holding it, which leaves this one little or nothing to do,
    and raises ``TimeoutError`` past ``_LOCK_WAIT_S``.

    Root's alone: anyone who could open it could hold it and stall every
    converge, so who else needs to know one is under way reads the marker. The
    marker goes while the lock is still held, so it is never the next run's."""
    fd = os.open(os.path.join(root, LOCK), os.O_RDWR | os.O_CREAT, 0o600)
    marker = os.path.join(root, CONVERGING)
    try:
        # Whatever an earlier run's umask left.
        with contextlib.suppress(OSError):
            os.fchmod(fd, 0o600)
        deadline = time.monotonic() + _LOCK_WAIT_S
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise TimeoutError(root) from None
                time.sleep(_LOCK_POLL_S)
        pid = os.getpid()
        started = _started(pid)
        _write_readable(marker, (f"{pid} {started}" if started else str(pid)).encode())
        try:
            yield
        finally:
            with contextlib.suppress(OSError):
                os.unlink(marker)
    finally:
        os.close(fd)


def converging(root: str) -> bool:
    """Whether a converge is under way, asked without waiting and by any
    user: the process it names is alive and is the one that started then,
    since one killed leaves its marker and a container hands its pid on
    quickly. One that died left it for the next run to replace."""
    try:
        with open(os.path.join(root, CONVERGING)) as f:
            pid_text, _, started = f.read().strip().partition(" ")
        pid = int(pid_text)
    except (OSError, ValueError):
        return False
    if started:
        return _started(pid) == started
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive, and root's, as a converge is when the computer's user asks.
        pass
    return True


def _started(pid: int) -> str | None:
    """When *pid* started, in clock ticks since boot, as Linux shows it to any
    user; None where it shows nothing, or once the process is gone."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rpartition(")")[2].split()
    except OSError:
        return None
    # Field 22 of the line, the 20th after the name's closing parenthesis.
    return fields[19] if len(fields) > 19 else None
