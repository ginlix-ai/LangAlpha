"""The runtime's ``sweep`` op against a real directory tree.

A sweep decides whether a project is synced at all, so its one unforgivable
answer is "unchanged" for a project that did change: that change then waits
for a sibling's turn or a teardown pass, and a lost sandbox in between loses
it. These pin each way a user can change a tree to "changed", and pin parity
with the scan's exclusions so churn the backup never carries (dependency
trees, thread logs) does not force a resync on every turn.

``boot_id`` comes from ``/proc``, which the host running the suite may lack,
so it is patched to a fixed value; the clock offset is patched too, so a test
can step the wall clock without touching the host's.
"""

import errno
import os
import time

import pytest

from ptc_agent.core.sandbox import wsfiles_transfer_runtime as rt
from src.server.services.persistence.transfer import exclusion_spec

BOOT = "boot-a"
OFFSET = 1_700_000_000_000_000_000
# Past any coarse change-time granularity, so an edit after the mark cannot
# land on the same tick as the mark itself.
_TICK_S = 0.05


@pytest.fixture(autouse=True)
def _boot(monkeypatch):
    monkeypatch.setattr(rt, "_boot_id", lambda: BOOT)
    monkeypatch.setattr(rt, "_clock_offset_ns", lambda: OFFSET)


def _write(root, rel, data=b"x"):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p


def _mark(boot_id=BOOT, offset_ns=OFFSET):
    time.sleep(_TICK_S)
    mark = {"ns": time.time_ns(), "boot_id": boot_id, "offset_ns": offset_ns}
    time.sleep(_TICK_S)
    return mark


def _sweep(*projects):
    # The server's own exclusion spec, so parity is with what ships.
    spec = exclusion_spec(None)
    spec["root"] = "/"
    spec["projects"] = [
        {"key": key, "root": str(root), "mark": mark} for key, root, mark in projects
    ]
    return rt.sweep(spec)


def _verdict(root, mark):
    out = _sweep(("p", root, mark))
    for bucket in ("changed", "unchanged", "missing"):
        if out[bucket] == ["p"]:
            return bucket
    raise AssertionError(out)


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "proj"
    _write(root, "top.txt")
    _write(root, "sub/a.txt")
    _write(root, "sub/deep/f.txt", b"before")
    return root


def test_untouched_project_is_unchanged(project):
    mark = _mark()
    out = _sweep(("p", project, mark))
    assert out["unchanged"] == ["p"] and out["changed"] == [] and out["missing"] == []
    assert out["boot_id"] == BOOT
    # An unchanged project is walked in full: every entry was looked at.
    assert out["visited"] == 5


def test_in_place_write_deep_in_the_tree_is_changed(project):
    mark = _mark()
    # Rewriting an existing file moves only that file's change time, not
    # any directory's: the walk has to reach it.
    (project / "sub/deep/f.txt").write_bytes(b"after")
    assert _verdict(project, mark) == "changed"


def test_new_file_is_changed(project):
    mark = _mark()
    _write(project, "sub/deep/new.txt")
    assert _verdict(project, mark) == "changed"


def test_deletion_is_changed(project):
    mark = _mark()
    # The file is gone, so nothing of it is left to stat; only its parent's
    # change time records the deletion.
    (project / "sub/deep/f.txt").unlink()
    assert _verdict(project, mark) == "changed"


def test_rename_is_changed(project):
    mark = _mark()
    os.rename(project / "sub/a.txt", project / "sub/b.txt")
    assert _verdict(project, mark) == "changed"


def test_chmod_is_changed(project):
    mark = _mark()
    os.chmod(project / "sub/deep/f.txt", 0o600)
    assert _verdict(project, mark) == "changed"


def test_mtime_set_into_the_past_is_still_changed(project):
    """Tools that preserve timestamps (``cp -p``, ``tar x``, ``rsync -t``) set
    mtime to the source's; change time is the one a program cannot set back."""
    mark = _mark()
    old = time.time() - 30 * 86400
    f = project / "sub/deep/f.txt"
    f.write_bytes(b"restored from an archive")
    os.utime(f, (old, old))
    assert f.stat().st_mtime_ns < mark["ns"]
    assert _verdict(project, mark) == "changed"
    new = _write(project, "sub/deep/extracted.txt")
    os.utime(new, (old, old))
    assert _verdict(project, mark) == "changed"


def test_no_mark_is_changed_without_a_walk(project):
    out = _sweep(("p", project, None))
    assert out["changed"] == ["p"] and out["visited"] == 0


def test_mark_from_another_boot_is_changed(project):
    out = _sweep(("p", project, _mark(boot_id="boot-before-restart")))
    assert out["changed"] == ["p"] and out["visited"] == 0


def test_mark_without_a_numeric_time_is_changed(project):
    assert _verdict(project, {"ns": None, "boot_id": BOOT, "offset_ns": OFFSET}) == "changed"
    assert _verdict(project, {"ns": "123", "boot_id": BOOT, "offset_ns": OFFSET}) == "changed"


# --- the wall clock stepped back since the mark --------------------------


def test_mark_the_wall_clock_has_since_stepped_back_past_is_changed(project, monkeypatch):
    """A step back lets a change made after the mark carry a change time
    older than it, so no walk can vouch for the project."""
    mark = _mark()
    monkeypatch.setattr(rt, "_clock_offset_ns", lambda: OFFSET - 2 * rt._CLOCK_STEP_NS)
    out = _sweep(("p", project, mark))
    assert out["changed"] == ["p"] and out["visited"] == 0


def test_mark_from_a_higher_offset_is_changed_without_a_walk(project):
    out = _sweep(("p", project, _mark(offset_ns=OFFSET + 2 * rt._CLOCK_STEP_NS)))
    assert out["changed"] == ["p"] and out["visited"] == 0


@pytest.mark.parametrize(
    "drift", [-rt._CLOCK_STEP_NS, -rt._CLOCK_STEP_NS // 2, 0, 5 * rt._CLOCK_STEP_NS]
)
def test_slew_within_the_tolerance_or_a_step_forward_is_walked(project, drift):
    """NTP slew stays under the tolerance, and a step forward only makes
    later change times newer, which the walk already catches."""
    mark = _mark()
    out = _sweep(("p", project, {**mark, "offset_ns": OFFSET - drift}))
    assert out["unchanged"] == ["p"] and out["visited"] == 5


@pytest.mark.parametrize("offset_ns", [None, "1700", 1.5])
def test_mark_without_an_integer_offset_is_changed(project, offset_ns):
    """A mark written before marks carried an offset cannot prove the clock
    held still."""
    mark = _mark()
    if offset_ns is None:
        del mark["offset_ns"]
    else:
        mark["offset_ns"] = offset_ns
    out = _sweep(("p", project, mark))
    assert out["changed"] == ["p"] and out["visited"] == 0


def test_clock_offset_is_wall_minus_boot_clock(monkeypatch):
    monkeypatch.undo()
    boot_clock = getattr(time, "CLOCK_BOOTTIME", time.CLOCK_MONOTONIC)
    expected = time.time_ns() - time.clock_gettime_ns(boot_clock)
    assert abs(rt._clock_offset_ns() - expected) < rt._CLOCK_STEP_NS // 10


def test_sandbox_without_a_boot_id_trusts_no_mark(project, monkeypatch):
    mark = _mark()
    monkeypatch.setattr(rt, "_boot_id", lambda: None)
    out = _sweep(("p", project, mark))
    assert out["changed"] == ["p"] and out["boot_id"] is None


def test_missing_root_is_missing_not_changed(tmp_path):
    out = _sweep(("gone", tmp_path / "nope", _mark()))
    assert out["missing"] == ["gone"] and out["changed"] == []


def test_projects_are_judged_independently_in_one_call(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    for root in (a, b, c):
        _write(root, "sub/f.txt")
    mark = _mark()
    _write(b, "sub/f.txt", b"edited")
    out = _sweep(("a", a, mark), ("b", b, mark), ("c", c, None), ("d", tmp_path / "d", mark))
    assert out["unchanged"] == ["a"]
    assert out["changed"] == ["b", "c"]
    assert out["missing"] == ["d"]


def test_a_changed_project_stops_at_its_first_newer_entry(tmp_path):
    root = tmp_path / "p"
    for i in range(50):
        _write(root, f"d{i:02d}/f.txt")
    mark = _mark()
    for i in range(50):
        (root / f"d{i:02d}/f.txt").write_bytes(b"edited")
    out = _sweep(("p", root, mark))
    assert out["changed"] == ["p"]
    assert out["visited"] < 100


# --- parity with the scan's exclusions -----------------------------------


def _excluded_tree(root):
    # Directories and files the backup never carries, all present before the
    # mark so their creation does not move a kept parent's change time.
    _write(root, "keep.txt")
    _write(root, "node_modules/pkg/index.js")
    _write(root, "src/node_modules/dep.js")
    _write(root, ".agents/threads/t1.md")
    _write(root, ".agents/skills/.staging/s.md")
    _write(root, ".agents/skills/.trash-123/old.md")
    _write(root, ".agents/skills/.skills-sync.flock")
    _write(root, "src/mod.pyc")
    _write(root, "src/.DS_Store")
    _write(root, ".file_sync_marker")
    (root / ".wsfiles-relay-abc").mkdir()
    _write(root, ".wsfiles-relay-abc/blob")


def test_excluded_entries_are_what_the_scan_excludes(tmp_path):
    """Same predicate for both ops: whatever the scan lists is exactly what a
    sweep looks at, so neither can see a change the other ignores."""
    root = tmp_path / "p"
    _excluded_tree(root)
    spec = exclusion_spec(None)
    spec["root"] = str(root)
    scanned = {e["path"] for e in rt.scan(spec)["entries"]}
    assert scanned == {"keep.txt", "src", ".agents", ".agents/skills"}
    out = _sweep(("p", root, _mark()))
    assert out["unchanged"] == ["p"]
    assert out["visited"] == len(scanned)


def test_churn_inside_excluded_entries_leaves_the_project_unchanged(tmp_path):
    root = tmp_path / "p"
    _excluded_tree(root)
    mark = _mark()
    _write(root, "node_modules/pkg/index.js", b"reinstalled")
    _write(root, "node_modules/pkg/new.js")
    _write(root, "src/node_modules/dep.js", b"v2")
    _write(root, ".agents/threads/t1.md", b"appended")
    _write(root, ".agents/threads/t2.md")
    _write(root, ".agents/skills/.staging/s.md", b"v2")
    _write(root, ".agents/skills/.trash-123/old.md", b"v2")
    _write(root, ".agents/skills/.skills-sync.flock", b"locked")
    _write(root, "src/mod.pyc", b"recompiled")
    _write(root, "src/.DS_Store", b"finder")
    _write(root, ".file_sync_marker", b"stamped")
    _write(root, ".wsfiles-relay-abc/blob", b"v2")
    assert _verdict(root, mark) == "unchanged"


def test_reserved_names_are_reserved_at_the_project_root_only(tmp_path):
    """Below the root ``.file_sync_marker`` is the user's file and the scan
    keeps it, so an edit to it must count."""
    root = tmp_path / "p"
    _write(root, "results/.file_sync_marker")
    mark = _mark()
    _write(root, "results/.file_sync_marker", b"mine")
    assert _verdict(root, mark) == "changed"


# --- unreadable entries --------------------------------------------------


class _Entry:
    """A ``DirEntry`` whose stat fails, since a real one past PATH_MAX is
    impractical to build on every host that runs the suite."""

    def __init__(self, real, err):
        self._real, self._err = real, err
        self.name, self.path = real.name, real.path

    def stat(self, *, follow_symlinks=True):
        raise OSError(self._err, os.strerror(self._err), self.path)

    def __getattr__(self, attr):
        return getattr(self._real, attr)


class _Scandir:
    def __init__(self, it, name, err):
        self._it, self._name, self._err = it, name, err

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._it.close()

    def __iter__(self):
        for e in self._it:
            yield _Entry(e, self._err) if e.name == self._name else e


def _failing_stat(monkeypatch, name, err):
    real = os.scandir
    monkeypatch.setattr(rt.os, "scandir", lambda p: _Scandir(real(p), name, err))


def test_a_path_too_long_to_open_does_not_mark_the_project_changed(project, monkeypatch):
    """The scan already reports it as unsaved for good; counting it changed
    would resync the project on every sweep for nothing."""
    mark = _mark()
    _failing_stat(monkeypatch, "deep", errno.ENAMETOOLONG)
    assert _verdict(project, mark) == "unchanged"


def test_any_other_unreadable_entry_marks_the_project_changed(project, monkeypatch):
    """Only a scan can tell whether an unreadable entry hides a file the
    backup is missing, so the sweep hands the question to one."""
    mark = _mark()
    _failing_stat(monkeypatch, "deep", errno.EACCES)
    assert _verdict(project, mark) == "changed"


# --- scan provides the mark's raw material ------------------------------


def test_scan_reports_its_start_time_boot_and_clock_offset(tmp_path):
    _write(tmp_path, "a.txt")
    before = time.time_ns()
    out = rt.scan({"root": str(tmp_path)})
    after = time.time_ns()
    assert before <= out["started_ns"] <= after
    assert out["boot_id"] == BOOT
    assert out["clock_offset_ns"] == OFFSET


def _no_proc(*a, **kw):
    raise FileNotFoundError("/proc/sys/kernel/random/boot_id")


def test_boot_id_is_none_without_proc_or_sysctl(monkeypatch):
    monkeypatch.undo()

    def _no_sysctl(*a, **kw):
        raise FileNotFoundError("sysctl")

    monkeypatch.setattr("builtins.open", _no_proc)
    monkeypatch.setattr(rt.subprocess, "run", _no_sysctl)
    assert rt._boot_id() is None


def test_boot_id_falls_back_to_the_macos_boot_session(monkeypatch):
    monkeypatch.undo()
    asked = []

    def _sysctl(argv, **kw):
        asked.append(argv)
        return rt.subprocess.CompletedProcess(argv, 0, stdout="8663-macos\n")

    monkeypatch.setattr("builtins.open", _no_proc)
    monkeypatch.setattr(rt.subprocess, "run", _sysctl)
    assert rt._boot_id() == "8663-macos"
    assert asked == [["sysctl", "-n", "kern.bootsessionuuid"]]
