"""The in-sandbox move script, run for real against a scratch disk.

It is the one piece that renames user files, so each case runs the actual
script in a subprocess over a temporary root rather than asserting on its
text: the moves land, the tool ledger follows them, and nothing is deleted
but a deleted workspace's folder that a rename could not move aside under
``_internal``.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from src.server.services.computer_manager._folders import _SCRIPT, _SCRIPT_LIMIT_S
from tests.unit.restored_overlay import archive, pinned, prefix

LEFTOVERS = "_internal/leftovers"
MOVING = "_internal/moving"
MARKER = ".file_sync_marker"


def _args(
    root: Path,
    *,
    moves=(),
    tombstones=(),
    deferred=(),
    copying=(),
    ledger: dict | None = None,
    prune: bool = True,
) -> Path:
    ledger_path = root / "_internal" / "ledger.json"
    if ledger is not None:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(json.dumps(ledger))
    # A staged row's ``landings`` are what the database recorded earlier
    # passes planning for it: the only folders its journal entry may name.
    moves = [{"staged": False, "landings": [], **m} for m in moves]
    tombstones = [{"staged": False, "source": t["dir"], "landings": [], **t} for t in tombstones]
    args = {
        "root": str(root),
        "moving": MOVING,
        "leftovers": LEFTOVERS,
        "ledger": str(ledger_path),
        "lock": str(root / "_internal" / "ledger.lock"),
        "moves": moves,
        "tombstones": tombstones,
        "deferred": list(deferred),
        "copying": list(copying),
        "marker": MARKER,
        "prune": prune,
    }
    args_path = root.parent / "folders_args.json"
    args_path.write_text(json.dumps(args))
    return args_path


def _run(root: Path, *, before: str = "", limit: int = _SCRIPT_LIMIT_S, **plan) -> dict:
    args_path = _args(root, **plan)
    done = subprocess.run(
        [sys.executable, "-c", before + _SCRIPT, str(args_path), str(limit)],
        capture_output=True, text=True, check=True, timeout=limit + 15,
    )
    assert not args_path.exists()
    return json.loads(done.stdout.strip().splitlines()[-1])


def _folder(root: Path, name: str, marker: str) -> None:
    (root / name).mkdir(parents=True)
    (root / name / "owner.txt").write_text(marker)


def _owner(root: Path, name: str) -> str:
    return (root / name / "owner.txt").read_text()


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "workspace"


def test_a_rename_moves_the_folder_and_the_ledger_follows(root):
    _folder(root, "Research", "A")
    report = _run(
        root,
        moves=[{"id": "A", "source": "Research", "target": "Macro"}],
        ledger={"dirs": {"A": "Research", "B": "Other"}},
    )
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    assert not (root / "Research").exists()
    ledger = json.loads((root / "_internal" / "ledger.json").read_text())
    assert ledger["dirs"] == {"A": "Macro", "B": "Other"}


def test_two_workspaces_trade_names(root):
    _folder(root, "X", "A")
    _folder(root, "Y", "B")
    report = _run(
        root,
        moves=[
            {"id": "A", "source": "X", "target": "Y"},
            {"id": "B", "source": "Y", "target": "X"},
        ],
    )
    assert report["errors"] == []
    assert (_owner(root, "Y"), _owner(root, "X")) == ("A", "B")


def test_a_deleted_workspace_is_moved_aside_before_its_name_is_reused(root):
    _folder(root, "Research", "dead")
    _folder(root, "Research-1a2b", "A")
    report = _run(
        root,
        tombstones=[{"id": "D", "dir": "Research"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
    )
    assert report["tombstones"] == {"D": f"{LEFTOVERS}/D"}
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert _owner(root, "Research") == "A"


def _hard_to_copy(root: Path, name: str, marker: str) -> None:
    """A folder holding what a copy has to carry over: a nested dir, a
    symlink, a read-only file, and a folder its owner made read-only."""
    _folder(root, name, marker)
    top = root / name
    (top / "pkg" / "lib").mkdir(parents=True)
    (top / "pkg" / "lib" / "a.bin").write_bytes(os.urandom(5000))
    (top / "alias").symlink_to("pkg")
    (top / "ro.txt").write_text("ro")
    (top / "ro.txt").chmod(0o444)
    (top / "frozen").mkdir()
    (top / "frozen" / "keep.txt").write_text("kept")
    (top / "frozen").chmod(0o555)


def _tree(top: Path) -> dict:
    """Each entry under ``top`` by its kind and mode, a file by its bytes and
    mtime, a link by its target."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(top):
        for name in dirnames + filenames:
            entry = Path(dirpath, name)
            st = entry.lstat()
            if stat.S_ISLNK(st.st_mode):
                body = os.readlink(entry)
            elif stat.S_ISREG(st.st_mode):
                body = (entry.read_bytes(), st.st_mtime_ns)
            else:
                body = None
            out[str(entry.relative_to(top))] = (stat.S_IFMT(st.st_mode), stat.S_IMODE(st.st_mode), body)
    return out


# A Daytona sandbox restored from its archive refuses to rename any folder it
# held then (EXDEV), which failed every rename, delete and move on it.


def test_on_a_restored_disk_a_rename_copies_the_folder_into_place(root):
    _hard_to_copy(root, "Research", "A")
    want = _tree(root / "Research")
    report = _run(
        root,
        before=prefix(archive(root)),
        moves=[{"id": "A", "source": "Research", "target": "Macro"}],
        copying=["A"],
        ledger={"dirs": {"A": "Research"}},
    )
    assert report["moves"] == {"A": "Macro"}
    assert report["errors"] == []
    assert _tree(root / "Macro") == want
    assert not (root / "Research").exists()
    assert os.listdir(root / MOVING / "copies") == []
    ledger = json.loads((root / "_internal" / "ledger.json").read_text())
    assert ledger["dirs"] == {"A": "Macro"}


def test_on_a_restored_disk_a_deleted_workspace_is_deleted_where_it_is(root):
    _hard_to_copy(root, "Research", "dead")
    _folder(root, "Research-1a2b", "A")
    report = _run(
        root,
        before=prefix(archive(root)),
        tombstones=[{"id": "D", "dir": "Research", "stuck": "delete"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
        copying=["A"],
    )
    assert report["errors"] == []
    assert report["tombstones"] == {"D": None}
    assert report["deleted"] == ["D"]
    assert not (root / LEFTOVERS / "D").exists()
    assert _owner(root, "Research") == "A"


def test_on_a_restored_disk_a_moved_workspace_is_copied_under_leftovers(root):
    _hard_to_copy(root, "Research", "moved")
    want = _tree(root / "Research")
    report = _run(
        root,
        before=prefix(archive(root)),
        tombstones=[{"id": "move-1", "dir": "Research", "stuck": "copy"}],
        prune=False,
    )
    assert report["tombstones"] == {"move-1": f"{LEFTOVERS}/move-1"}
    assert _tree(root / LEFTOVERS / "move-1") == want
    assert not (root / "Research").exists()


def test_a_rename_with_no_room_for_its_copy_keeps_the_old_folder(root):
    _hard_to_copy(root, "Research", "A")
    report = _run(
        root,
        before=prefix(archive(root), full=True),
        moves=[{"id": "A", "source": "Research", "target": "Macro"}],
        copying=["A"],
    )
    assert report["moves"] == {"A": "Research"}
    assert list(report["blocked"]) == ["A"]
    assert report["errors"] == []
    assert _owner(root, "Research") == "A"
    assert not (root / "Macro").exists()
    assert not (root / MOVING / "copies" / "A").exists()


def test_a_moved_workspace_with_no_room_for_its_copy_is_left_for_the_host(root):
    _hard_to_copy(root, "Research", "moved")
    report = _run(
        root,
        before=prefix(archive(root), full=True),
        tombstones=[{"id": "move-1", "dir": "Research", "stuck": "copy"}],
        prune=False,
    )
    assert list(report["blocked"]) == ["move-1"]
    assert "move-1" not in report["tombstones"]
    assert _owner(root, "Research") == "moved"


def test_a_copy_the_deadline_cuts_short_lands_on_a_later_pass(root):
    _hard_to_copy(root, "Research", "A")
    want = _tree(root / "Research")
    restored = archive(root)
    plan = {"moves": [{"id": "A", "source": "Research", "target": "Macro"}], "copying": ["A"]}
    # A limit that leaves no time past the landings: the copy stops at once.
    first = _run(root, before=prefix(restored), limit=10, **plan)
    assert first["moves"] == {"A": "Research"}
    assert first["errors"] == []
    assert [p for p in first["progress"] if "copying Research" in p]
    assert (root / MOVING / "copies" / "A").is_dir()

    second = _run(root, before=prefix(restored), **plan)

    assert second["moves"] == {"A": "Macro"}
    assert _tree(root / "Macro") == want
    assert not (root / "Research").exists()


def test_an_unowned_folder_on_the_target_is_kept_aside_not_merged(root):
    _folder(root, "Macro", "stray")
    _folder(root, "Research", "A")
    report = _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    [(folder, away)] = report["evicted"]
    assert folder == "Macro" and away.startswith(f"{LEFTOVERS}/Macro-")
    assert _owner(root, away) == "stray"
    assert _owner(root, "Macro") == "A"


def test_on_a_restored_disk_an_unowned_folder_is_copied_aside(root):
    _hard_to_copy(root, "Macro", "stray")
    want = _tree(root / "Macro")
    _folder(root, "Research", "A")
    report = _run(
        root,
        before=prefix(archive(root)),
        moves=[{"id": "A", "source": "Research", "target": "Macro"}],
        copying=["A"],
    )
    [(folder, away)] = report["evicted"]
    assert folder == "Macro" and away.startswith(f"{LEFTOVERS}/Macro-")
    assert _tree(root / away) == want
    assert _owner(root, "Macro") == "A"


def _late_once_copied_aside() -> str:
    """Source, after ``prefix``, that runs the clock past every deadline once
    a copy aside is renamed into place, so the delete after it stops at once."""
    return (
        "import time as _time\n"
        "_rename_then, _now, _late = _os.rename, _time.time, []\n"
        "def _renamed(src, dst, *, src_dir_fd=None, dst_dir_fd=None):\n"
        "    _rename_then(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)\n"
        "    if str(src).endswith('.copying'):\n"
        "        _late.append(dst)\n"
        "_os.rename = _renamed\n"
        "_time.time = lambda: _now() + (3600 if _late else 0)\n"
    )


def test_an_unowned_folder_copied_aside_whose_delete_is_cut_short_is_not_blocked(root):
    """Copied whole, it is reported where it went, and the row goes back
    under its old name until the next pass, rather than taking the backoff a
    copy with no room gets."""
    _hard_to_copy(root, "Macro", "stray")
    want = _tree(root / "Macro")
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"id": "A", "source": "Research", "target": "Macro"}

    first = _run(
        root, before=prefix(restored) + _late_once_copied_aside(), moves=[plan], copying=["A"]
    )

    [(folder, away)] = first["evicted"]
    assert folder == "Macro" and _tree(root / away) == want
    assert first["blocked"] == {}
    assert [p for p in first["progress"] if "deleting Macro where it is" in p]
    assert first["moves"] == {"A": "Research"}
    assert _owner(root, "Research") == "A"

    second = _run(root, before=prefix(restored), moves=[plan], copying=["A"])

    assert second["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    assert _tree(root / away) == want
    # The delete went on where it stopped, with no second copy of what it left.
    assert os.listdir(root / LEFTOVERS) == [os.path.basename(away)]


def _late_after_copying(files: int) -> str:
    """Source, after ``prefix``, that runs the clock past every deadline once
    a copy aside has copied ``files`` files, so it stops partway."""
    return (
        "import stat as _stat, time as _time\n"
        "_utime_then, _now, _stamped = _os.utime, _time.time, []\n"
        "def _utime(path, *args, **kwargs):\n"
        "    _utime_then(path, *args, **kwargs)\n"
        "    if '.copying/' in str(path) and _stat.S_ISREG(_os.lstat(path).st_mode):\n"
        "        _stamped.append(path)\n"
        "_os.utime = _utime\n"
        f"_time.time = lambda: _now() + (3600 if len(_stamped) >= {files} else 0)\n"
    )


def _copied_so_far(root: Path) -> dict:
    """Each file a copy aside cut short has made, by its inode."""
    [partial] = [p for p in (root / LEFTOVERS).iterdir() if p.name.endswith(".copying")]
    return {
        str(p.relative_to(partial)): p.lstat().st_ino
        for p in partial.rglob("*")
        if p.is_file() and not p.is_symlink()
    }


def _fills_after(checks: int) -> str:
    """Source, after ``prefix``, that fills the disk once a copy has checked
    for room ``checks`` times, and runs the clock past every deadline then,
    so deleting what the copy wrote is cut short."""
    return (
        "import time as _time\n"
        "_statvfs_then, _now, _checks = _os.statvfs, _time.time, []\n"
        "def _filling(path):\n"
        "    st = _statvfs_then(path)\n"
        "    _checks.append(path)\n"
        f"    if len(_checks) <= {checks}:\n"
        "        return st\n"
        "    return _os.statvfs_result((st.f_bsize, st.f_frsize, st.f_blocks, 0, 0,\n"
        "        st.f_files, st.f_ffree, st.f_favail, st.f_flag, st.f_namemax))\n"
        "_os.statvfs = _filling\n"
        f"_time.time = lambda: _now() + (3600 if len(_checks) > {checks} else 0)\n"
    )


def _journaled(root: Path) -> dict:
    return json.loads((root / MOVING / "landed.json").read_text())


def test_a_copy_with_no_room_is_deleted_before_its_row_is_blocked(root):
    """A blocked row runs no pass until its block ends, so the disk the copy
    took would stay taken all that time. A pass the delete outlasts leaves the
    row unblocked, and the next deletes the rest rather than copying on."""
    _hard_to_copy(root, "Research", "A")
    restored = archive(root)
    plan = {"moves": [{"id": "A", "source": "Research", "target": "Macro"}], "copying": ["A"]}

    first = _run(root, before=prefix(restored) + _fills_after(2), **plan)

    assert first["moves"] == {"A": "Research"}
    assert first["blocked"] == {}
    assert [p for p in first["progress"] if "no room to copy Research, deleting" in p]
    assert (root / MOVING / "copies" / "A").is_dir()

    second = _run(root, before=prefix(restored), **plan)

    assert second["moves"] == {"A": "Research"}
    assert list(second["blocked"]) == ["A"]
    assert not (root / MOVING / "copies" / "A").exists()
    assert not (root / "Macro").exists()
    assert "n:A" not in _journaled(root)
    assert _owner(root, "Research") == "A"


def test_a_copy_aside_with_no_room_is_deleted_before_its_row_is_blocked(root):
    _hard_to_copy(root, "Macro", "stray")
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"moves": [{"id": "A", "source": "Research", "target": "Macro"}], "copying": ["A"]}

    # The copy's folder, then two of the files and the link at its top.
    first = _run(root, before=prefix(restored) + _fills_after(3), **plan)

    assert first["moves"] == {"A": "Research"}
    assert first["blocked"] == {}
    assert 0 < len(_copied_so_far(root)) < 4

    second = _run(root, before=prefix(restored), **plan)

    assert second["moves"] == {"A": "Research"}
    assert list(second["blocked"]) == ["A"]
    assert os.listdir(root / LEFTOVERS) == []
    assert not [k for k in _journaled(root) if k in ("n:A", "e:Macro")]
    assert (_owner(root, "Research"), _owner(root, "Macro")) == ("A", "stray")


def test_a_copy_aside_the_deadline_cuts_short_goes_on_from_there(root):
    """The row goes back under its old name with no backoff, and the next
    pass copies only what the first did not, then lands."""
    _hard_to_copy(root, "Macro", "stray")
    want = _tree(root / "Macro")
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"id": "A", "source": "Research", "target": "Macro"}

    first = _run(
        root, before=prefix(restored) + _late_after_copying(2), moves=[plan], copying=["A"]
    )

    assert first["moves"] == {"A": "Research"}
    assert first["blocked"] == {}
    assert first["evicted"] == []
    assert [p for p in first["progress"] if "copying Macro aside" in p]
    assert (_owner(root, "Research"), _owner(root, "Macro")) == ("A", "stray")
    copied = _copied_so_far(root)
    assert len(copied) == 2

    second = _run(root, before=prefix(restored), moves=[plan], copying=["A"])

    assert second["moves"] == {"A": "Macro"}
    [(folder, away)] = second["evicted"]
    assert folder == "Macro" and _tree(root / away) == want
    assert _owner(root, "Macro") == "A"
    assert os.listdir(root / LEFTOVERS) == [os.path.basename(away)]
    # The files the first pass copied are the very ones the copy holds.
    assert {rel: (root / away / rel).lstat().st_ino for rel in copied} == copied


def test_a_copy_aside_cut_short_is_kept_only_while_its_row_still_has_to_move(root):
    _hard_to_copy(root, "Macro", "stray")
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"id": "A", "source": "Research", "target": "Macro"}
    _run(root, before=prefix(restored) + _late_after_copying(2), moves=[plan], copying=["A"])
    copied = _copied_so_far(root)

    # A pass that leaves the row, busy, where it is.
    _run(root, before=prefix(restored), copying=["A"])
    assert _copied_so_far(root) == copied

    # Renamed back, it no longer moves at all.
    _run(root, before=prefix(restored), copying=[])
    assert os.listdir(root / LEFTOVERS) == []
    journal = json.loads((root / MOVING / "landed.json").read_text())
    assert "e:Macro" not in journal
    assert _owner(root, "Macro") == "stray"


def test_a_copy_aside_cut_short_goes_once_its_row_is_renamed_elsewhere(root):
    """Renamed again, the row clears only its new target: the old one's part
    copy goes in the same pass, before the move it would crowd."""
    _hard_to_copy(root, "Macro", "stray")
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"id": "A", "source": "Research", "target": "Macro"}
    _run(root, before=prefix(restored) + _late_after_copying(2), moves=[plan], copying=["A"])
    assert len(_copied_so_far(root)) == 2

    report = _run(
        root,
        before=prefix(restored),
        moves=[{"id": "A", "source": "Research", "target": "Equities"}],
        copying=["A"],
    )

    assert report["moves"] == {"A": "Equities"}
    assert _owner(root, "Equities") == "A"
    assert os.listdir(root / LEFTOVERS) == []
    assert "e:Macro" not in _journaled(root)
    assert _owner(root, "Macro") == "stray"


def test_an_unowned_folder_whose_delete_fails_after_its_copy_is_not_copied_again(root):
    """The delete stops on a dir something holds: the row goes back under its
    old name, rather than stay in staging, where it takes no turns, for as
    long as the error lasts, and the next pass deletes what is left rather
    than copying it into a second folder."""
    _hard_to_copy(root, "Macro", "stray")
    (root / "Macro" / "pinned").mkdir()
    restored = archive(root)
    _folder(root, "Research", "A")
    plan = {"id": "A", "source": "Research", "target": "Macro"}

    first = _run(root, before=prefix(restored) + pinned("pinned"), moves=[plan], copying=["A"])

    [(_, away)] = first["evicted"]
    assert first["moves"] == {"A": "Research"}
    assert [e for e in first["errors"] if "land A" in e]
    assert _owner(root, "Research") == "A"
    assert (root / "Macro").exists()

    second = _run(root, before=prefix(restored), moves=[plan], copying=["A"])

    assert second["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    assert os.listdir(root / LEFTOVERS) == [os.path.basename(away)]


def test_a_row_whose_target_cannot_be_cleared_goes_back_to_its_folder(root):
    """An unowned restored folder on the target with no room to copy it
    aside: the row goes back under its old name, not left in staging where it
    takes no turns, and the target is blocked so the next pass does not try
    the same copy."""
    _folder(root, "Macro", "stray")
    restored = archive(root)
    _folder(root, "Research", "A")
    report = _run(
        root,
        before=prefix(restored, full=True),
        moves=[{"id": "A", "source": "Research", "target": "Macro"}],
        copying=["A"],
    )
    assert report["moves"] == {"A": "Research"}
    assert list(report["blocked"]) == ["A"]
    assert (_owner(root, "Research"), _owner(root, "Macro")) == ("A", "stray")
    assert not (root / MOVING / "A").exists()


def test_a_row_staged_across_an_archive_lands_by_copy(root):
    """Its staging folder went into the restored layer with everything else,
    so it cannot be renamed onto the target either."""
    _hard_to_copy(root, f"{MOVING}/A", "A")
    want = _tree(root / MOVING / "A")
    report = _run(
        root,
        before=prefix(archive(root)),
        moves=[{
            "id": "A", "source": "Research", "target": "Macro",
            "staged": True, "landings": ["Macro"],
        }],
        copying=["A"],
    )
    assert report["moves"] == {"A": "Macro"}
    assert report["errors"] == []
    assert _tree(root / "Macro") == want
    assert not (root / MOVING / "A").exists()


def test_a_row_copied_onto_its_target_stays_there_while_staging_takes_passes_to_go(root):
    """Landed by a copy, with its staging still to delete over two passes:
    the entry naming the target is the one record of where the content went,
    so the pass that finishes the delete lands the row there rather than
    taking its folder for an unowned one."""
    _hard_to_copy(root, f"{MOVING}/A", "A")
    (root / MOVING / "A" / "pinned").mkdir()
    want = _tree(root / MOVING / "A")
    restored = archive(root)
    plan = {
        "id": "A", "source": "Research", "target": "Macro",
        "staged": True, "landings": ["Macro"],
    }
    for _ in range(2):
        report = _run(root, before=prefix(restored) + pinned("pinned"), moves=[plan], copying=["A"])
        assert report["moves"] == {"A": f"{MOVING}/A"}

    last = _run(root, before=prefix(restored), moves=[plan], copying=["A"])

    assert last["moves"] == {"A": "Macro"}
    assert last["evicted"] == []
    assert _tree(root / "Macro") == want
    assert not (root / MOVING / "A").exists()


def test_a_delete_cut_short_after_the_copy_loses_the_sync_marker_first(root):
    """Content in staging and the old folder part deleted: the marker goes
    first, so what is left reads as a folder that lost files. The row stays
    staged, and the next pass finishes the delete and lands it."""
    _hard_to_copy(root, "Research", "A")
    (root / "Research" / MARKER).write_text("synced")
    (root / "Research" / "pinned").mkdir()
    want = _tree(root / "Research")
    restored = archive(root)
    plan = {"target": "Macro", "source": "Research", "id": "A"}

    # Past the copy's commit, the delete in place stops on one dir.
    first = _run(root, before=prefix(restored) + pinned("pinned"), moves=[plan], copying=["A"])

    assert first["moves"] == {"A": f"{MOVING}/A"}
    assert [e for e in first["errors"] if "deleting Research where it is" in e]
    assert (root / "Research").exists()
    assert not (root / "Research" / MARKER).exists()
    assert _tree(root / MOVING / "A") == want

    second = _run(
        root,
        before=prefix(restored),
        moves=[{**plan, "staged": True, "landings": ["Macro"]}],
        copying=["A"],
    )

    assert second["moves"] == {"A": "Macro"}
    assert not (root / "Research").exists()
    assert _tree(root / "Macro") == want


def test_a_move_never_lands_on_a_deleted_workspace_it_could_not_clear(root):
    """The tombstone's row still points at the folder, so its cleanup would
    remove whatever landed there: the mover stays where it is instead, not
    staged, where it would take no turns until the folder clears."""
    _folder(root, "Research", "dead")
    _folder(root, "Research-1a2b", "A")
    # A file where the leftovers folder should be makes the tombstone's move fail.
    (root / "_internal").mkdir(parents=True)
    (root / LEFTOVERS).write_text("")
    report = _run(
        root,
        tombstones=[{"id": "D", "dir": "Research"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
    )
    assert "D" not in report["tombstones"]
    assert report["moves"] == {"A": "Research-1a2b"}
    assert _owner(root, "Research") == "dead"
    assert _owner(root, "Research-1a2b") == "A"
    assert not (root / MOVING / "A").exists()


def test_a_move_interrupted_after_staging_finishes_from_staging(root):
    _folder(root, f"{MOVING}/A", "A")
    report = _run(
        root, moves=[{"id": "A", "source": "Research", "target": "Macro", "staged": True}]
    )
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"


def test_a_workspace_whose_folder_was_never_made_takes_its_name(root):
    root.mkdir(parents=True)
    report = _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    assert report["moves"] == {"A": "Macro"}
    assert report["errors"] == []


def test_a_workspace_with_no_folder_does_not_take_an_unowned_one(root):
    """Files left at its name by no row are not this workspace's."""
    _folder(root, "Macro", "stray")
    report = _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    assert report["moves"] == {"A": "Macro"}
    [(folder, away)] = report["evicted"]
    assert folder == "Macro" and _owner(root, away) == "stray"
    assert not (root / "Macro").exists()


def test_a_landing_the_database_never_recorded_is_found_after_a_rename(root):
    """The row stays staged when its landing fails to record, and a rename
    before the next pass changes its target: the journal says where it went."""
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    assert _owner(root, "Macro") == "A"
    report = _run(
        root,
        moves=[{
            "id": "A", "source": "Research", "target": "Final", "staged": True,
            "landings": ["Macro"],
        }],
    )
    assert report["moves"] == {"A": "Final"}
    assert _owner(root, "Final") == "A"
    assert not (root / "Macro").exists()


def test_the_journal_only_answers_for_a_row_still_staged(root):
    """A row staged this pass moves from its source; an entry left from an
    earlier move names a folder that may be someone else's by now."""
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    _folder(root, "Notes", "A again")
    report = _run(root, moves=[{"id": "A", "source": "Notes", "target": "Notes 2"}])
    assert report["moves"] == {"A": "Notes 2"}
    assert _owner(root, "Notes 2") == "A again"
    assert _owner(root, "Macro") == "A"


def test_a_recorded_landing_drops_out_of_the_journal(root):
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    _folder(root, "X", "B")
    _run(root, moves=[{"id": "B", "source": "X", "target": "Y"}])
    journal = json.loads((root / MOVING / "landed.json").read_text())
    assert journal == {"B": "Y"}


def test_a_workspace_deleted_mid_move_clears_what_its_last_landing_left(root):
    """Its row points at staging, but the landing reached the disk: the
    folder it landed in is its, not a free name for the next workspace."""
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    report = _run(
        root,
        tombstones=[{"id": "A", "dir": f"{MOVING}/A", "staged": True, "landings": ["Macro"]}],
    )
    assert report["tombstones"] == {"A": f"{LEFTOVERS}/A"}
    assert _owner(root, f"{LEFTOVERS}/A") == "A"
    assert not (root / "Macro").exists()


def test_a_workspace_deleted_while_staged_clears_its_staging(root):
    _folder(root, f"{MOVING}/A", "A")
    report = _run(root, tombstones=[{"id": "A", "dir": f"{MOVING}/A", "staged": True}])
    assert report["tombstones"] == {"A": f"{LEFTOVERS}/A"}
    assert not (root / MOVING / "A").exists()


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root renames out of a read-only directory",
)
def test_a_move_that_cannot_stage_keeps_its_folder(root):
    """Reporting the target would point the row at an empty folder while its
    files stay behind, owned by no row."""
    _folder(root, "Research", "A")
    (root / MOVING).mkdir(parents=True)
    # A root nothing can be renamed out of; the journal and lock still write.
    root.chmod(0o555)
    try:
        report = _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    finally:
        root.chmod(0o755)
    assert report["moves"] == {"A": "Research"}
    assert _owner(root, "Research") == "A"
    assert not (root / "Macro").exists()


def test_a_retried_pass_does_not_clear_a_landed_move_as_a_tombstone(root):
    """The first pass cleared the deleted folder and landed the mover on its
    name, then never recorded either: the tombstone row still names the folder
    the mover now holds."""
    _folder(root, "Research", "dead")
    _folder(root, "Research-1a2b", "A")
    first = dict(
        tombstones=[{"id": "D", "dir": "Research"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
    )
    _run(root, **first)
    report = _run(
        root,
        tombstones=[{"id": "D", "dir": "Research"}],
        moves=[{
            "id": "A", "source": "Research-1a2b", "target": "Research", "staged": True,
            "landings": ["Research"],
        }],
    )
    assert report["tombstones"] == {"D": f"{LEFTOVERS}/D"}
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert _owner(root, "Research") == "A"


def test_a_landing_on_a_deferred_tombstones_cleared_folder_is_still_found(root):
    """As above, but the retry leaves the tombstone (past the pass cap, or a
    file change holds it), so its row still names the folder: its journal
    entry says the dead content left, and what is there is the mover's."""
    _folder(root, "Research", "dead")
    _folder(root, "Research-1a2b", "A")
    _run(
        root,
        tombstones=[{"id": "D", "dir": "Research"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
    )
    report = _run(
        root,
        moves=[{
            "id": "A", "source": "Research-1a2b", "target": "Research-1a2b", "staged": True,
            "landings": ["Research"],
        }],
        deferred=["D"],
    )
    assert report["moves"] == {"A": "Research-1a2b"}
    assert _owner(root, "Research-1a2b") == "A"
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert not (root / "Research").exists()


def test_a_tombstone_whose_pass_died_before_the_rename_is_cleared_on_retry(root):
    """The journal entry is written before the rename; a pass that died
    between them leaves the folder in place, and a new workspace taking the
    name would inherit it."""
    _folder(root, "Research", "dead")
    (root / MOVING).mkdir(parents=True)
    (root / MOVING / "landed.json").write_text(json.dumps({"t:D": f"{LEFTOVERS}/D"}))
    report = _run(root, tombstones=[{"id": "D", "dir": "Research"}])
    assert report["tombstones"] == {"D": f"{LEFTOVERS}/D"}
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert not (root / "Research").exists()


def test_a_tombstone_left_past_the_pass_cap_keeps_its_journal_entry(root):
    """Its folder left but the database never heard, and the entry is the one
    record of that: without it, whatever sits at the old name by the next pass
    is taken for the deleted workspace's and cleared with it."""
    _folder(root, "Research", "dead")
    _run(root, tombstones=[{"id": "D", "dir": "Research"}])
    _run(root, deferred=["D"])
    _folder(root, "Research", "stray")
    report = _run(root, tombstones=[{"id": "D", "dir": "Research"}])
    assert report["tombstones"] == {"D": f"{LEFTOVERS}/D"}
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert _owner(root, "Research") == "stray"
    # Recorded now, so no pass clears or leaves it: the entry goes.
    _run(root)
    assert json.loads((root / MOVING / "landed.json").read_text()) == {}


def test_a_staging_folder_made_after_the_landing_is_set_aside(root):
    """An acquisition that wrote to the row's staged path after the landing
    made a folder there; the landing, not that folder, is the workspace."""
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    _folder(root, f"{MOVING}/A", "stray")
    report = _run(
        root,
        moves=[{
            "id": "A", "source": "Research", "target": "Macro", "staged": True,
            "landings": ["Macro"],
        }],
    )
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    [(folder, away)] = report["evicted"]
    assert folder == f"{MOVING}/A" and _owner(root, away) == "stray"


def test_a_stray_staging_folder_is_not_taken_for_a_row_staged_now(root):
    _folder(root, f"{MOVING}/A", "stray")
    _folder(root, "Research", "A")
    report = _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    [(_folder_name, away)] = report["evicted"]
    assert _owner(root, away) == "stray"


def test_a_workspace_deleted_before_its_staging_ran_clears_its_old_folder(root):
    _folder(root, "Research", "A")
    report = _run(
        root,
        tombstones=[{"id": "A", "dir": f"{MOVING}/A", "source": "Research", "staged": True}],
    )
    assert report["tombstones"] == {"A": f"{LEFTOVERS}/A"}
    assert _owner(root, f"{LEFTOVERS}/A") == "A"


def _journal(root: Path, entries: dict) -> None:
    (root / MOVING).mkdir(parents=True, exist_ok=True)
    (root / MOVING / "landed.json").write_text(json.dumps(entries))


def test_a_copy_committed_before_an_archive_still_finishes_its_delete(root):
    """The archive gave the copy a new inode, so the journal knows the commit
    ran by the copy being gone from where it was built: what is left of the
    old folder goes, rather than staying under its name for the next
    workspace called so."""
    _hard_to_copy(root, f"{MOVING}/W", "W")
    want = _tree(root / MOVING / "W")
    _folder(root, "Research", "left")
    _journal(root, {"c:W": ["Research", 1, f"{MOVING}/W"]})
    report = _run(
        root,
        before=prefix(archive(root)),
        moves=[{
            "id": "W", "source": "Research", "target": "Macro",
            "staged": True, "landings": ["Macro"],
        }],
        copying=["W"],
    )
    assert report["moves"] == {"W": "Macro"}
    assert report["errors"] == []
    assert not (root / "Research").exists()
    assert _tree(root / "Macro") == want


def test_a_copy_whose_commit_is_journaled_is_kept_by_the_prune(root):
    """Still where it was built, it says the commit never ran: pruned, the
    folder under the journal's name would read as the copy, and its source
    be deleted."""
    _folder(root, "Research", "moved")
    _folder(root, f"{MOVING}/copies/move-1", "moved")
    _journal(root, {"c:move-1": ["Research", 1, f"{LEFTOVERS}/move-1"]})
    _run(root, moves=[], copying=[])
    assert (root / MOVING / "copies" / "move-1").is_dir()
    assert _owner(root, "Research") == "moved"


def test_a_copy_entry_naming_no_folder_is_ignored(root):
    """A staged row with no source has None among its folders, so an entry
    naming none passed for its own, and its delete failed every pass."""
    _folder(root, f"{MOVING}/W", "W")
    _folder(root, "Macro", "other")
    _journal(root, {"c:W": [None, 1, "Macro"]})
    report = _run(root, moves=[{
        "id": "W", "source": None, "target": "Research",
        "staged": True, "landings": ["Research"],
    }])
    assert [e for e in report["errors"] if e.startswith("journal c:W: ignored")]
    assert report["moves"] == {"W": "Research"}
    assert (_owner(root, "Research"), _owner(root, "Macro")) == ("W", "other")


def test_a_journal_entry_naming_another_rows_folder_is_ignored(root):
    """Code in the sandbox can write the journal. Followed, an entry naming a
    sibling's folder moves the sibling into this row, and the report still
    reads as the planned landing."""
    _folder(root, f"{MOVING}/A", "A")
    _folder(root, "Sibling", "B")
    _journal(root, {"A": "Sibling"})
    report = _run(
        root,
        moves=[{
            "id": "A", "source": "Research", "target": "Macro", "staged": True,
            "landings": ["Macro"],
        }],
    )
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"
    assert _owner(root, "Sibling") == "B"
    assert any(error.startswith("journal A:") for error in report["errors"])


def test_a_deleted_workspaces_journal_entry_never_clears_a_sibling(root):
    """Its clearing ends in an rm, so following an entry that names a
    sibling's folder would delete the sibling."""
    _folder(root, f"{MOVING}/T", "T")
    _folder(root, "Sibling", "B")
    _journal(root, {"T": "Sibling"})
    report = _run(
        root,
        tombstones=[{"id": "T", "dir": f"{MOVING}/T", "source": "Old", "staged": True}],
    )
    assert report["tombstones"] == {"T": f"{LEFTOVERS}/T"}
    assert _owner(root, f"{LEFTOVERS}/T") == "T"
    assert _owner(root, "Sibling") == "B"


def test_a_tombstone_entry_outside_its_own_leftovers_is_ignored(root):
    """A pass only moves a deleted workspace's folder to its leftovers. An
    entry naming anything else reads as already cleared, and the dead folder
    stays where the next workspace to take the name inherits it."""
    _folder(root, "Research", "dead")
    _folder(root, "Sibling", "B")
    _journal(root, {"t:D": "Sibling"})
    report = _run(root, tombstones=[{"id": "D", "dir": "Research"}])
    assert report["tombstones"] == {"D": f"{LEFTOVERS}/D"}
    assert _owner(root, f"{LEFTOVERS}/D") == "dead"
    assert _owner(root, "Sibling") == "B"


def test_a_tampered_entry_never_takes_another_staged_rows_landing(root):
    """Both rows landed and neither reached the database, then A's entry is
    pointed at B's landing. No pass planned that folder for A, so B takes its
    own content back, and A's, which lost the entry naming it, is kept aside."""
    _folder(root, "X", "A")
    _folder(root, "Z", "B")
    _run(root, moves=[
        {"id": "A", "source": "X", "target": "Y"},
        {"id": "B", "source": "Z", "target": "W"},
    ])
    _journal(root, {"A": "W", "B": "W"})
    report = _run(root, moves=[
        {"id": "A", "source": "X", "target": "Y", "staged": True, "landings": ["Y"]},
        {"id": "B", "source": "Z", "target": "W", "staged": True, "landings": ["W"]},
    ])
    assert "journal A: ignored 'W'" in report["errors"]
    assert report["moves"] == {"A": "Y", "B": "W"}
    assert _owner(root, "W") == "B"
    [(folder, away)] = report["evicted"]
    assert (folder, _owner(root, away)) == ("Y", "A")


def test_a_tampered_entry_never_hands_a_row_the_landing_on_its_old_folder(root):
    """As above, but the two traded names, so B's landing is A's old folder:
    A falling back to it would move B's content into A just the same."""
    _folder(root, "X", "A")
    _folder(root, "Y", "B")
    _run(root, moves=[
        {"id": "A", "source": "X", "target": "Y"},
        {"id": "B", "source": "Y", "target": "X"},
    ])
    _journal(root, {"A": "X", "B": "X"})
    report = _run(root, moves=[
        {"id": "A", "source": "X", "target": "Y", "staged": True, "landings": ["Y"]},
        {"id": "B", "source": "Y", "target": "X", "staged": True, "landings": ["X"]},
    ])
    assert report["moves"] == {"A": "Y", "B": "X"}
    assert _owner(root, "X") == "B"
    [(folder, away)] = report["evicted"]
    assert (folder, _owner(root, away)) == ("Y", "A")


def _run_locked_out(root: Path, **plan) -> dict:
    """A pass that never gets the ledger lock, its 30 s wait cut short."""
    import fcntl

    args_path = _args(root, **plan)
    (root / "_internal").mkdir(parents=True, exist_ok=True)
    # Each wait between tries passes a minute.
    later = (
        "import time\n"
        "_now, _skew = time.time, [0]\n"
        "def _sleep(_s):\n"
        "    _skew[0] += 60\n"
        "time.time = lambda: _now() + _skew[0]\n"
        "time.sleep = _sleep\n"
    )
    with open(root / "_internal" / "ledger.lock", "a+") as ledger_lock:
        fcntl.flock(ledger_lock, fcntl.LOCK_EX)
        done = subprocess.run(
            [sys.executable, "-c", later + _SCRIPT, str(args_path), str(_SCRIPT_LIMIT_S)],
            capture_output=True, text=True, check=True, timeout=30,
        )
    return json.loads(done.stdout.strip().splitlines()[-1])


def _run_failing_rename(root: Path, src: str, dst: str, **plan) -> dict:
    """A pass where one rename fails, as a busy mount or a full directory makes it."""
    args_path = _args(root, **plan)
    fail = (
        "import os\n_rename = os.rename\n"
        "def _failing(a, b):\n"
        f"    if a.endswith({'/' + src!r}) and b.endswith({'/' + dst!r}):\n"
        "        raise OSError(16, 'busy')\n"
        "    return _rename(a, b)\n"
        "os.rename = _failing\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", fail + _SCRIPT, str(args_path), str(_SCRIPT_LIMIT_S)],
        capture_output=True, text=True, check=True, timeout=_SCRIPT_LIMIT_S + 15,
    )
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_a_staged_row_that_cannot_stage_again_keeps_its_old_folder(root):
    """A pass staged both rows in the database and never reached the disk, so
    A's content is still in its old folder, which B is renamed to. Left
    unheld, B would set A's content aside as unowned and take the folder; it
    stays in its own instead."""
    _folder(root, "X", "A")
    _folder(root, "W", "B")
    report = _run_failing_rename(root, "X", f"{MOVING}/A", moves=[
        {"id": "A", "source": "X", "target": "Z", "staged": True, "landings": ["Z"]},
        {"id": "B", "source": "W", "target": "X", "staged": True, "landings": ["X"]},
    ])
    assert report["moves"] == {"A": "X", "B": "W"}
    assert report["evicted"] == []
    assert (_owner(root, "X"), _owner(root, "W")) == ("A", "B")


def test_a_landing_outlives_a_later_pass_that_planned_elsewhere_and_moved_nothing(root):
    """Pass 1 landed A on its name and never recorded it. Pass 2 planned its
    old folder instead, as a pass does when the target is held, and timed out
    on the ledger lock. Pass 3 still finds the first landing, which the row
    records beside the second."""
    _folder(root, "Research", "A")
    _run(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    staged = {"id": "A", "source": "Research", "target": "Research", "staged": True}
    timed_out = _run_locked_out(root, moves=[{**staged, "landings": ["Macro"]}])
    assert (timed_out["errors"], timed_out["moves"]) == (["ledger flock timeout"], {})
    report = _run(root, moves=[{**staged, "landings": ["Macro", "Research"]}])
    assert report["moves"] == {"A": "Research"}
    assert _owner(root, "Research") == "A"
    assert not (root / "Macro").exists()


def test_a_landing_taken_back_to_staging_leaves_the_journal(root):
    """A's unrecorded landing, on its old folder while its name was taken, goes
    back to staging; A cannot land this pass, and B, renamed to A's old name,
    lands on the folder A left. Neither records. An entry still naming it
    would claim B's content for A."""
    _folder(root, "Research", "A")
    _journal(root, {"A": "Research"})
    _folder(root, "Macro", "unowned")
    _folder(root, "Notes", "B")
    # The unowned folder on A's name cannot be set aside, so A cannot land there.
    (root / LEFTOVERS).write_text("")
    a = {"id": "A", "source": "Research", "target": "Macro", "staged": True}
    b = {"id": "B", "source": "Notes", "target": "Research"}
    blocked = _run(root, moves=[{**a, "landings": ["Research"]}, b])
    assert blocked["moves"] == {"A": f"{MOVING}/A", "B": "Research"}
    (root / LEFTOVERS).unlink()
    report = _run(root, moves=[
        {**a, "landings": ["Research", "Macro"]},
        {**b, "staged": True, "landings": ["Research"]},
    ])
    assert report["moves"] == {"A": "Macro", "B": "Research"}
    assert (_owner(root, "Macro"), _owner(root, "Research")) == ("A", "B")


def test_two_entries_naming_one_folder_are_both_ignored(root):
    """No pass lands two rows on one folder, so one entry is false, and
    nothing says which: the folder goes to neither."""
    _folder(root, f"{MOVING}/A", "A")
    _folder(root, "Shared", "B")
    _journal(root, {"A": "Shared", "B": "Shared"})
    report = _run(root, moves=[
        {"id": "A", "source": "Old A", "target": "Mine", "staged": True, "landings": ["Shared"]},
        {"id": "B", "source": "Old B", "target": "Theirs", "staged": True, "landings": ["Shared"]},
    ])
    assert {"journal A: ignored 'Shared'", "journal B: ignored 'Shared'"} <= set(report["errors"])
    assert _owner(root, "Mine") == "A"
    assert _owner(root, "Shared") == "B"
    assert not (root / "Theirs").exists()


def _land_on_a_folder_never_made(root: Path) -> None:
    """A's folder was never made, and B, renamed to A's old name, landed there;
    the pass never reached the database."""
    _folder(root, "Notes", "B")
    _run(root, moves=[
        {"id": "A", "source": "Old", "target": "New"},
        {"id": "B", "source": "Notes", "target": "Old"},
    ])
    assert _owner(root, "Old") == "B"


def test_a_row_falls_back_to_its_old_folder_only_when_no_landing_claims_it(root):
    """A staged row with nothing journaled falls back to its old folder, where
    B's content now is until B takes it back."""
    _land_on_a_folder_never_made(root)
    report = _run(root, moves=[
        {"id": "A", "source": "Old", "target": "New", "staged": True, "landings": ["New"]},
        {"id": "B", "source": "Notes", "target": "Old", "staged": True, "landings": ["Old"]},
    ])
    assert report["errors"] == []
    assert report["moves"] == {"A": "New", "B": "Old"}
    assert _owner(root, "Old") == "B"
    assert not (root / "New").exists()


def test_a_deleted_workspace_never_clears_a_landing_on_its_old_folder(root):
    """As above with A deleted before the retry: its clearing ends in an rm."""
    _land_on_a_folder_never_made(root)
    report = _run(
        root,
        tombstones=[{
            "id": "A", "dir": f"{MOVING}/A", "source": "Old", "staged": True, "landings": ["New"],
        }],
        moves=[{
            "id": "B", "source": "Notes", "target": "Old", "staged": True, "landings": ["Old"],
        }],
    )
    assert report["tombstones"] == {"A": None}
    assert report["moves"] == {"B": "Old"}
    assert _owner(root, "Old") == "B"


def test_a_pass_the_host_lost_ends_itself_at_its_limit(root):
    """A timed-out exec is not killed (Docker only stops reading), and the host
    lets the folder locks go once the limit has passed: whatever the script is
    doing then, here waiting on a ledger lock a sibling's sync holds, it ends.
    The shell ignores the alarm first, as an ignore survives exec."""
    import fcntl
    import signal
    import time

    _folder(root, "Research", "A")
    args_path = _args(root, moves=[{"id": "A", "source": "Research", "target": "Macro"}])
    (root / "_internal").mkdir()
    with open(root / "_internal" / "ledger.lock", "a+") as ledger_lock:
        fcntl.flock(ledger_lock, fcntl.LOCK_EX)
        started = time.monotonic()
        done = subprocess.run(
            ["/bin/sh", "-c", 'trap "" ALRM; exec "$0" -c "$1" "$2" "$3"',
             sys.executable, _SCRIPT, str(args_path), "1"],
            capture_output=True, text=True, timeout=60,
        )
        elapsed = time.monotonic() - started
    assert done.returncode == -signal.SIGALRM, done
    assert done.stdout == ""
    # Well inside the script's 30 s wait for the ledger lock.
    assert elapsed < 15
    assert _owner(root, "Research") == "A"


class _SubprocessRuntime:
    """Runs the command the manager builds, as the sandbox shell would."""

    def __init__(self, cwd=None):
        self.commands = []
        self.cwd = cwd

    async def upload_file(self, content, dest_path):
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(dest_path).write_bytes(content)

    async def exec(self, command, timeout=None):
        self.commands.append(command)
        done = subprocess.run(
            ["/bin/sh", "-c", command.replace("python3 ", f"{sys.executable} ", 1)],
            capture_output=True, text=True, timeout=timeout, cwd=self.cwd,
        )
        return type("Result", (), {
            "exit_code": done.returncode, "stdout": done.stdout, "stderr": done.stderr,
        })()


@pytest.mark.asyncio
async def test_clearing_a_deleted_workspace_runs_the_real_script(root, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.services.computer_manager import _folders

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    retarget = AsyncMock()
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "retarget_folder_cleanup", retarget)
    monkeypatch.setattr(_folders, "get_workspace_dir_name", AsyncMock(return_value="Research"))
    monkeypatch.setattr(_folders, "computer_run_in_progress", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, "computer_command_running", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, "hold_workspace_folder", AsyncMock(return_value=True))
    _folder(root, "Research", "dead")
    moved = await _folders.FolderSettleMixin._clear_tombstone_folder(
        None, "c", _SubprocessRuntime(), str(root), "D", "Research"
    )
    assert moved == f"{LEFTOVERS}/D"
    assert _owner(root, moved) == "dead"
    retarget.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_deleted_workspace_a_file_change_holds_is_not_cleared_yet(root, monkeypatch):
    """The change read the folder before the delete; the caller defers the
    cleanup and a later pass clears it once the change is done."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.services.computer_manager import _folders

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "get_workspace_dir_name", AsyncMock(return_value="Research"))
    monkeypatch.setattr(_folders, "computer_run_in_progress", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, "computer_command_running", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, "hold_workspace_folder", AsyncMock(return_value=False))
    _folder(root, "Research", "dead")
    runtime = _SubprocessRuntime()
    with pytest.raises(RuntimeError, match="file change"):
        await _folders.FolderSettleMixin._clear_tombstone_folder(
            None, "c", runtime, str(root), "D", "Research"
        )
    assert _owner(root, "Research") == "dead"
    assert runtime.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize("busy", ["computer_run_in_progress", "computer_command_running"])
async def test_a_deleted_workspace_is_not_cleared_while_a_sibling_runs(root, monkeypatch, busy):
    """A sibling's code may still write ../Research, from a run or a command
    handed off past its turn; moving it now would let that write recreate the
    name for the next workspace to take it."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.services.computer_manager import _folders

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    hold = AsyncMock(return_value=True)
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "get_workspace_dir_name", AsyncMock(return_value="Research"))
    monkeypatch.setattr(_folders, "computer_run_in_progress", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, "computer_command_running", AsyncMock(return_value=False))
    monkeypatch.setattr(_folders, busy, AsyncMock(return_value=True))
    monkeypatch.setattr(_folders, "hold_workspace_folder", hold)
    _folder(root, "Research", "dead")
    runtime = _SubprocessRuntime()
    with pytest.raises(RuntimeError, match="run on the computer"):
        await _folders.FolderSettleMixin._clear_tombstone_folder(
            None, "c", runtime, str(root), "D", "Research"
        )
    assert _owner(root, "Research") == "dead"
    assert runtime.commands == []
    hold.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_tombstone_a_settle_cleared_first_leaves_the_names_new_owner_alone(
    root, monkeypatch
):
    """The caller read the row before the lock; a settle since then moved the
    folder aside and a live workspace took the name."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.services.computer_manager import _folders

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    retarget = AsyncMock()
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "retarget_folder_cleanup", retarget)
    monkeypatch.setattr(
        _folders, "get_workspace_dir_name", AsyncMock(return_value=f"{LEFTOVERS}/D")
    )
    _folder(root, "Research", "live")
    runtime = _SubprocessRuntime()
    moved = await _folders.FolderSettleMixin._clear_tombstone_folder(
        None, "c", runtime, str(root), "D", "Research"
    )
    assert moved == f"{LEFTOVERS}/D"
    assert _owner(root, "Research") == "live"
    assert runtime.commands == []
    retarget.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_plan_longer_than_one_shell_argument_still_runs(root):
    """A shell command is one argument string, capped at 128 KiB on Linux;
    200 deleted workspaces with long CJK names serialize past that."""
    from src.server.services.computer_manager import _folders

    names = [f"{i:03d}" + "研" * 77 for i in range(200)]
    tombstones = [
        {"id": f"T{i}", "dir": name, "source": name, "staged": False}
        for i, name in enumerate(names)
    ]
    assert len(json.dumps(tombstones)) > 128 * 1024
    runtime = _SubprocessRuntime()
    report = await _folders._run_folder_script(
        runtime, str(root), moves=[], tombstones=tombstones, prune=True
    )
    assert report["tombstones"] == {t["id"]: None for t in tombstones}
    # The script itself, compressed and base64'd, is about 19 KiB; the plan adds nothing.
    assert max(map(len, runtime.commands)) < 64 * 1024
    assert list((root / "_internal").glob(".folders_args.*")) == []


@pytest.mark.asyncio
async def test_a_workspace_named_like_a_module_the_script_imports_does_not_break_it(root):
    """The sandbox runs the script from the computer root, beside every
    workspace folder: a package there named ``json`` would be imported in place
    of the standard library's, failing every retry with the rows staged."""
    from src.server.services.computer_manager import _folders

    for module in ("base64", "json"):
        _folder(root, module, module)
        (root / module / "__init__.py").write_text("")
    _folder(root, "Research", "A")
    report = await _folders._run_folder_script(
        _SubprocessRuntime(cwd=root),
        str(root),
        moves=[{"id": "A", "source": "Research", "target": "Macro", "staged": False}],
        tombstones=[],
        prune=True,
    )
    assert report["moves"] == {"A": "Macro"}
    assert _owner(root, "Macro") == "A"


def test_a_long_name_over_an_unowned_folder_still_lands(root):
    """The folder moved aside keeps its name plus a suffix; at 255 bytes the
    suffix does not fit unless the name is cut, and the move would fail on
    every pass."""
    # A CJK name reaches this length in bytes, the sandbox's limit; ASCII
    # reaches it in characters too, so the case fails on any disk.
    long_name = "a" * 240
    _folder(root, "Old", "A")
    _folder(root, long_name, "stray")

    out = _run(root, moves=[{"id": "A", "source": "Old", "target": long_name}])

    assert out["moves"] == {"A": long_name}
    assert _owner(root, long_name) == "A"
    [(_, away)] = out["evicted"]
    assert _owner(root, away) == "stray"

@pytest.mark.asyncio
async def test_a_settle_sends_the_script_every_field_it_reads(root, monkeypatch):
    """The manager's payload and the script's reads drift apart silently: the
    one thing a settle cannot survive is the script dying on a missing key."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.database.workspace_folders import (
        FOLDER_LAYOUT_VERSION, FolderRow, moving_path, plan_folder_moves,
    )
    from src.server.services.computer_manager import _folders

    rows = [
        FolderRow("A", "Macro", "Research", False),
        # Landed on its name by a pass that never recorded it.
        FolderRow("B", "Notes", moving_path("B"), False, ("Drafts",), ("Notes",)),
        FolderRow("D", "Old", "Old", True),
    ]

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    record = AsyncMock(return_value={"A"})
    monkeypatch.setattr(_folders, "read_folder_rows", AsyncMock(return_value=(FOLDER_LAYOUT_VERSION, rows)))
    monkeypatch.setattr(_folders, "busy_workspace_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "stage_folder_moves", AsyncMock(return_value=plan_folder_moves(rows)))
    monkeypatch.setattr(_folders, "record_folder_landings", record)
    _folder(root, "Research", "A")
    _folder(root, "Notes", "B")
    _journal(root, {"B": "Notes"})
    _folder(root, "Old", "dead")

    assert await _folders.FolderSettleMixin._settle_folders(
        None, "c", _SubprocessRuntime(), root=str(root)
    )
    assert record.await_args.kwargs == {
        "moves": {"A": "Macro", "B": "Notes"}, "tombstones": {"D": f"{LEFTOVERS}/D"},
    }
    assert (_owner(root, "Macro"), _owner(root, "Notes")) == ("A", "B")


@pytest.mark.asyncio
async def test_a_folder_its_owner_kept_is_set_aside_only_for_its_names_new_owner(
    root, monkeypatch
):
    """Deleted on the owner's machine without asking for the files to go, a
    folder stays where it is, and goes under leftovers, whole, only when a new
    workspace takes its name. No pass deletes it, even where a rename fails."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.database.workspace_folders import (
        FOLDER_LAYOUT_VERSION, FolderRow, plan_folder_moves,
    )
    from src.server.services.computer_manager import _folders

    rows = [
        FolderRow("K", "Research", "Research", True, kept=True),
        FolderRow("L", "Old", "Old", True, kept=True),
        FolderRow("A", "Research", "Research-1a2b", False),
    ]

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    sent = []
    real_run = _folders._run_folder_script

    async def run(*args, **kwargs):
        sent.append(kwargs)
        return await real_run(*args, **kwargs)

    record = AsyncMock(return_value={"A"})
    monkeypatch.setattr(_folders, "read_folder_rows", AsyncMock(return_value=(FOLDER_LAYOUT_VERSION, rows)))
    monkeypatch.setattr(_folders, "busy_workspace_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "stage_folder_moves", AsyncMock(return_value=plan_folder_moves(rows)))
    monkeypatch.setattr(_folders, "record_folder_landings", record)
    monkeypatch.setattr(_folders, "_run_folder_script", run)
    _hard_to_copy(root, "Research", "kept")
    want = _tree(root / "Research")
    _folder(root, "Old", "also kept")
    _folder(root, "Research-1a2b", "A")

    assert await _folders.FolderSettleMixin._settle_folders(
        None, "c", _SubprocessRuntime(), root=str(root)
    )
    [payload] = sent
    assert [(t["id"], t["stuck"]) for t in payload["tombstones"]] == [("K", "copy")]
    assert record.await_args.kwargs == {
        "moves": {"A": "Research"}, "tombstones": {"K": f"{LEFTOVERS}/K"},
    }
    assert _owner(root, "Research") == "A"
    assert _tree(root / LEFTOVERS / "K") == want
    assert _owner(root, "Old") == "also kept"


def test_on_a_restored_disk_a_kept_folder_is_copied_aside_not_deleted(root):
    """A failed rename deletes an ordinary deleted workspace's folder where it
    is; one its owner kept is copied under leftovers instead."""
    _hard_to_copy(root, "Research", "kept")
    want = _tree(root / "Research")
    _folder(root, "Research-1a2b", "A")
    report = _run(
        root,
        before=prefix(archive(root)),
        tombstones=[{"id": "K", "dir": "Research", "stuck": "copy"}],
        moves=[{"id": "A", "source": "Research-1a2b", "target": "Research"}],
        copying=["A"],
    )
    assert report["errors"] == []
    assert report["tombstones"] == {"K": f"{LEFTOVERS}/K"}
    assert "K" not in report.get("deleted", [])
    assert _tree(root / LEFTOVERS / "K") == want
    assert _owner(root, "Research") == "A"


@pytest.mark.asyncio
async def test_a_settle_whose_script_never_started_puts_its_rows_back(root, monkeypatch):
    """Staged rows are refused by every acquisition until they land; when the
    plan never reached the disk, nothing moved, so the row can go back now."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.database.workspace_folders import (
        FOLDER_LAYOUT_VERSION, FolderRow, moving_path, plan_folder_moves,
    )
    from src.server.services.computer_manager import _folders

    rows = [
        FolderRow("A", "Macro", "Research", False),
        FolderRow("B", "Notes", moving_path("B"), False, ("Drafts",)),
    ]

    @asynccontextmanager
    async def lock(_computer_id):
        yield object()

    class _NoUpload(_SubprocessRuntime):
        async def upload_file(self, content, dest_path):
            raise OSError("disk full")

    runtime = _NoUpload()
    record = AsyncMock(return_value={"A"})
    monkeypatch.setattr(_folders, "read_folder_rows", AsyncMock(return_value=(FOLDER_LAYOUT_VERSION, rows)))
    monkeypatch.setattr(_folders, "busy_workspace_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "stage_folder_moves", AsyncMock(return_value=plan_folder_moves(rows)))
    monkeypatch.setattr(_folders, "record_folder_landings", record)

    assert await _folders.FolderSettleMixin._settle_folders(None, "c", runtime, root=str(root))
    # B was staged by an earlier pass, so its content may be in staging.
    assert record.await_args.kwargs == {"moves": {"A": "Research"}, "tombstones": {}}
    assert not runtime.commands


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["Sibling", "Stray", "../outside", ".agents"])
async def test_a_journal_entry_naming_no_landing_the_row_records_is_ignored(
    root, tmp_path, entry
):
    """Only a folder the database recorded a pass planning for this row is a
    landing. Followed, the entry would pull a sibling, an unowned folder spelled
    like a name, a folder from outside the root, or the computer's own into the
    workspace."""
    from src.server.services.computer_manager import _folders

    _folder(root, f"{MOVING}/A", "A")
    _folder(root, "Sibling", "B")
    _folder(root, "Stray", "stray")
    _folder(tmp_path, "outside", "outside")
    _folder(root, ".agents", "computer")
    _journal(root, {"A": entry})
    report = await _folders._run_folder_script(
        _SubprocessRuntime(),
        str(root),
        moves=[{
            "id": "A", "source": "Research", "target": "Macro", "staged": True,
            "landings": ["Macro"],
        }],
        tombstones=[],
        prune=True,
    )
    assert report["moves"] == {"A": "Macro"}
    assert f"journal A: ignored {entry!r}" in report["errors"]
    assert _owner(root, "Macro") == "A"
    for folder, owner in (("Sibling", "B"), ("Stray", "stray"), (".agents", "computer")):
        assert _owner(root, folder) == owner
    assert _owner(tmp_path, "outside") == "outside"


class _LostExec(_SubprocessRuntime):
    """An exec whose script the host cannot account for, or one that ended."""

    def __init__(self, outcome):
        super().__init__()
        self.outcome = outcome

    async def exec(self, command, timeout=None):
        import asyncio

        self.commands.append(command)
        if self.outcome == "timed out":
            # DockerRuntime.exec on its timeout: the process is never signalled.
            return type("Result", (), {"exit_code": -1, "stdout": "", "stderr": "timeout"})()
        if self.outcome == "failed in transit":
            raise ConnectionError("toolbox unreachable")
        if self.outcome == "cancelled":
            raise asyncio.CancelledError
        if self.outcome == "hangs":
            await asyncio.Event().wait()
        # Died at its own limit: 128 + SIGALRM, an exit status the host saw.
        return type("Result", (), {"exit_code": 142, "stdout": "", "stderr": ""})()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome, waits",
    [("timed out", True), ("failed in transit", True), ("cancelled", True), ("ended", False)],
)
async def test_a_lost_script_keeps_the_folder_locks_until_its_limit_has_passed(
    root, monkeypatch, outcome, waits
):
    """Released sooner, the locks let a file change or another settle use
    folders the script may still be moving. One whose exit the host saw is
    over, and holds nothing up."""
    import asyncio
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from src.server.database.workspace_folders import FOLDER_LAYOUT_VERSION, FolderRow, plan_folder_moves
    from src.server.services.computer_manager import _folders

    rows = [FolderRow("A", "Macro", "Research", False)]
    events = []

    @asynccontextmanager
    async def lock(_computer_id):
        try:
            yield object()
        finally:
            events.append("released")

    async def sleep(seconds):
        events.append(("slept", seconds))

    record = AsyncMock(return_value=set())
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(_folders, "read_folder_rows", AsyncMock(return_value=(FOLDER_LAYOUT_VERSION, rows)))
    monkeypatch.setattr(_folders, "busy_workspace_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "stage_folder_moves", AsyncMock(return_value=plan_folder_moves(rows)))
    monkeypatch.setattr(_folders, "record_folder_landings", record)

    settle = _folders.FolderSettleMixin._settle_folders(None, "c", _LostExec(outcome), root=str(root))
    if outcome == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await settle
    else:
        assert await settle
    limit = _folders._SCRIPT_LIMIT_S + _folders._SCRIPT_START_S
    assert events == ([("slept", limit)] if waits else []) + ["released"]
    # The rows stay staged; the next settle finds the content.
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_settle_cancelled_through_anyio_keeps_the_folder_locks_until_its_limit(
    root, monkeypatch
):
    """A server cancels a request through an AnyIO scope, which cancels again
    at every await: the wait for a lost script has to outlast every delivery."""
    import time
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    import anyio

    from src.server.database.workspace_folders import FOLDER_LAYOUT_VERSION, FolderRow, plan_folder_moves
    from src.server.services.computer_manager import _folders

    rows = [FolderRow("A", "Macro", "Research", False)]
    released = []

    @asynccontextmanager
    async def lock(_computer_id):
        try:
            yield object()
        finally:
            released.append(time.monotonic())

    monkeypatch.setattr(_folders, "_SCRIPT_LIMIT_S", 0.3)
    monkeypatch.setattr(_folders, "_SCRIPT_START_S", 0)
    monkeypatch.setattr(_folders, "read_folder_rows", AsyncMock(return_value=(FOLDER_LAYOUT_VERSION, rows)))
    monkeypatch.setattr(_folders, "busy_workspace_ids", AsyncMock(return_value=set()))
    monkeypatch.setattr(_folders, "workspace_folders_lock", lock)
    monkeypatch.setattr(_folders, "stage_folder_moves", AsyncMock(return_value=plan_folder_moves(rows)))
    monkeypatch.setattr(_folders, "record_folder_landings", AsyncMock(return_value=set()))

    start = time.monotonic()
    with anyio.move_on_after(0.05) as scope:
        await _folders.FolderSettleMixin._settle_folders(None, "c", _LostExec("hangs"), root=str(root))
    assert scope.cancelled_caught
    assert released and released[0] - start >= 0.3
