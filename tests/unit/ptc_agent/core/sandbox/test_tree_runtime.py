"""The copy that moves a folder a rename cannot, run in process.

A folder too large for one script pass is copied over several, so a copy cut
short has to resume rather than start over, and one made while the folder
stayed in use has to catch up on what changed since.
"""

from __future__ import annotations

import itertools
import os
import shutil
import socket
import stat
import time
from pathlib import Path

import pytest

from ptc_agent.core.sandbox.livefs_runtime import tree_runtime


def _source(top: Path, n: int = 20) -> None:
    for i in range(n):
        (top / f"d{i:02d}").mkdir(parents=True)
        (top / f"d{i:02d}" / "f.bin").write_bytes(os.urandom(200))
    (top / "alias").symlink_to("d00")


def _tree(top: Path) -> dict:
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
            out[str(entry.relative_to(top))] = (stat.S_IMODE(st.st_mode), body)
    return out


def test_a_copy_the_deadline_cuts_short_resumes_where_it_stopped(tmp_path, monkeypatch):
    src, dst = tmp_path / "src", tmp_path / "dst"
    _source(src)
    whole = tree_runtime.sync_tree(str(src), str(tmp_path / "whole"))
    reads, now = itertools.count(), time.time
    # The clock runs past the deadline a few entries in.
    monkeypatch.setattr(tree_runtime.time, "time", lambda: now() + (3600 if next(reads) > 12 else 0))

    assert tree_runtime.sync_tree(str(src), str(dst), deadline=now() + 60) is None

    monkeypatch.undo()
    entries, written = tree_runtime.sync_tree(str(src), str(dst))
    assert _tree(dst) == _tree(src)
    assert entries == whole[0]
    assert 0 < written < whole[1]
    assert tree_runtime.sync_tree(str(src), str(dst)) == (entries, 0)


def test_a_copy_made_again_writes_only_what_changed_since(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    _source(src)
    tree_runtime.sync_tree(str(src), str(dst))
    (src / "d00" / "f.bin").write_bytes(b"changed")
    (src / "d01" / "f.bin").unlink()
    (src / "new.txt").write_text("new")

    _, written = tree_runtime.sync_tree(str(src), str(dst))

    assert written == 3
    assert _tree(dst) == _tree(src)


def test_a_file_too_large_for_one_pass_is_copied_over_several(tmp_path, monkeypatch):
    """Each pass copies a few chunks of the one file; started over each time,
    it would never be whole."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "big.bin").write_bytes(os.urandom(10 * 1024))
    monkeypatch.setattr(tree_runtime, "_CHUNK", 1024)
    budget = itertools.count()
    monkeypatch.setattr(tree_runtime, "_past", lambda deadline: next(budget) >= 4)

    passes = 0
    while tree_runtime.sync_tree(str(src), str(dst), deadline=0) is None:
        passes += 1
        assert passes < 10
        budget = itertools.count()
    assert passes > 1
    assert _tree(dst) == _tree(src)


def test_a_copy_that_does_not_fit_raises_no_room(tmp_path, monkeypatch):
    """Checked file by file, and a disk that fills mid-write says the same."""
    src = tmp_path / "src"
    _source(src)
    with monkeypatch.context() as m:
        m.setattr(tree_runtime, "room_for", lambda need, at: False)
        with pytest.raises(tree_runtime.NoRoom):
            tree_runtime.sync_tree(str(src), str(tmp_path / "a"))

    def full(*args):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(tree_runtime, "_copy_file", full)
    with pytest.raises(tree_runtime.NoRoom):
        tree_runtime.sync_tree(str(src), str(tmp_path / "b"))


def test_a_stale_copy_frees_its_room_before_the_new_one_is_measured(tmp_path, monkeypatch):
    """A file changed since a pass copied it whole: its old copy goes first,
    so a disk with room for one copy of it does not refuse the new one."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "big.bin").write_bytes(os.urandom(100_000))
    tree_runtime.sync_tree(str(src), str(dst))
    (src / "big.bin").write_bytes(os.urandom(100_000))
    os.utime(src / "big.bin", ns=(10**18, 10**18))

    def room_for(need, at):
        used = sum(p.stat().st_size for p in dst.rglob("*") if p.is_file())
        return need + used <= 150_000

    monkeypatch.setattr(tree_runtime, "room_for", room_for)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert _tree(dst) == _tree(src)


def test_names_linked_to_one_file_stay_one_file_in_the_copy(tmp_path):
    """As a rename keeps them: a write through one name still shows through
    the other, and the bytes are copied once."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "objects").mkdir(parents=True)
    (src / "objects" / "pack").write_bytes(os.urandom(4000))
    os.link(src / "objects" / "pack", src / "pack.again")

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None

    one, two = (dst / "objects" / "pack").stat(), (dst / "pack.again").stat()
    assert (one.st_ino, one.st_nlink) == (two.st_ino, 2)
    assert _tree(dst) == _tree(src)
    assert tree_runtime.sync_tree(str(src), str(dst))[1] == 0


def test_names_linked_to_one_file_count_toward_the_reserve(tmp_path, monkeypatch):
    """Each takes an entry in its folder as any file does, though no bytes."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "a").write_bytes(b"")
    for name in ("b", "c"):
        os.link(src / "a", src / name)

    def room_for(need, at):
        # Room for the first name and no more.
        return need != tree_runtime._ENTRY_ROOM or not any(
            os.path.lexists(dst / name) for name in "abc"
        )

    monkeypatch.setattr(tree_runtime, "room_for", room_for)

    with pytest.raises(tree_runtime.NoRoom):
        tree_runtime.sync_tree(str(src), str(dst))


def test_a_file_written_while_it_is_read_is_read_again(tmp_path, monkeypatch):
    """Bytes read before a write and after it make a version the file never
    had, and the pass that copied it can commit it and delete the source."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "big.bin").write_bytes(b"a" * 4096)
    os.utime(src / "big.bin", ns=(10**18, 10**18))
    monkeypatch.setattr(tree_runtime, "_CHUNK", 1024)
    checks, past = itertools.count(), tree_runtime._past

    def write_midway(deadline):
        if next(checks) == 2:
            (src / "big.bin").write_bytes(b"b" * 4096)
        return past(deadline)

    monkeypatch.setattr(tree_runtime, "_past", write_midway)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert (dst / "big.bin").read_bytes() == b"b" * 4096
    assert _tree(dst) == _tree(src)


def test_a_file_that_grows_while_it_is_read_is_measured_again(tmp_path, monkeypatch):
    """Room is found for the length the walk saw, and the copy goes no
    further: grown past it, the file is measured again before it is copied
    again, so a copy never takes what the reserve keeps free."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "log.bin").write_bytes(b"a" * 4096)
    monkeypatch.setattr(tree_runtime, "_CHUNK", 1024)
    checks, past = itertools.count(), tree_runtime._past

    def grow_midway(deadline):
        if next(checks) == 1:
            with open(src / "log.bin", "ab") as fh:
                fh.write(b"b" * 100_000)
        return past(deadline)

    measured = []

    def room_for(need, at):
        measured.append(need)
        return need <= 50_000

    monkeypatch.setattr(tree_runtime, "_past", grow_midway)
    monkeypatch.setattr(tree_runtime, "room_for", room_for)

    with pytest.raises(tree_runtime.NoRoom):
        tree_runtime.sync_tree(str(src), str(dst))
    assert measured == [tree_runtime._ENTRY_ROOM, 4096, 104_096]
    assert not (dst / "log.bin").exists()


def test_a_file_the_deadline_cuts_off_after_its_last_chunk_is_finished(tmp_path, monkeypatch):
    """Whole by its length, and stamped with the source's mtime, which the
    next pass takes as done: its owner, attributes and mode are set now."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "f.bin").write_bytes(os.urandom(2048))
    monkeypatch.setattr(tree_runtime, "_CHUNK", 1024)
    finished, copy_xattrs = [], tree_runtime._copy_xattrs

    def finishing(s, d):
        finished.append(os.path.basename(d))
        copy_xattrs(s, d)

    monkeypatch.setattr(tree_runtime, "_copy_xattrs", finishing)
    budget = itertools.count()
    # Past it once both chunks are written, before the read that finds the end.
    monkeypatch.setattr(tree_runtime, "_past", lambda deadline: deadline is not None and next(budget) >= 3)

    assert tree_runtime.sync_tree(str(src), str(dst), deadline=0) is None
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert "f.bin" in finished
    assert _tree(dst) == _tree(src)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads what its owner cannot")
def test_what_its_owner_shut_itself_out_of_is_copied_and_shut_again(tmp_path):
    """A rename reads nothing, so a file and a folder of mode 000 move; the
    copy lets its owner in for the read and puts the modes back."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "sealed").mkdir(parents=True)
    (src / "sealed" / "inside.txt").write_text("inside")
    (src / "secret.bin").write_bytes(b"secret")
    sealed = [src / "secret.bin", src / "sealed"]
    for path in sealed:
        os.chmod(path, 0)
    try:
        entries, _ = tree_runtime.sync_tree(str(src), str(dst))
        assert tree_runtime.sync_tree(str(src), str(dst)) == (entries, 0)
        modes = {
            str(p.relative_to(tmp_path)): stat.S_IMODE(p.lstat().st_mode)
            for p in sealed + [dst / "secret.bin", dst / "sealed"]
        }
        assert set(modes.values()) == {0}
        for path in (dst / "secret.bin", dst / "sealed"):
            os.chmod(path, 0o700)
        assert (dst / "secret.bin").read_bytes() == b"secret"
        assert (dst / "sealed" / "inside.txt").read_text() == "inside"
    finally:
        for path in sealed + [dst / "secret.bin", dst / "sealed"]:
            if os.path.lexists(path):
                os.chmod(path, 0o700)


def test_an_entry_made_where_the_copy_already_passed_is_in_it(tmp_path, monkeypatch):
    """A process still at work in the folder: the copy is whole only once a
    walk finds nothing more to write, as its source is deleted once it is."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    _source(src, n=2)
    sync_entry, made = tree_runtime._sync_entry, []

    def write_behind(entry, target, deadline, linked):
        # The top folder's listing is done before any folder below it.
        if os.path.dirname(entry.path) != str(src) and not made:
            made.append((src / "late.txt").write_text("late"))
        return sync_entry(entry, target, deadline, linked)

    monkeypatch.setattr(tree_runtime, "_sync_entry", write_behind)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert (dst / "late.txt").read_text() == "late"
    assert _tree(dst) == _tree(src)


@pytest.mark.parametrize("when", ["listed", "opened", "read"])
def test_a_file_deleted_while_the_copy_walks_goes_from_it(tmp_path, monkeypatch, when):
    """A process still at work in the folder deletes what it made: after its
    folder was listed, before the file was opened, or while it was read. The
    walk goes on without it, and takes away what an earlier pass copied."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "keep.txt").write_text("keep")
    (src / "tmp.txt").write_text("tmp")
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    (src / "tmp.txt").write_text("tmp, written again")
    doomed = str(src / "tmp.txt")

    def delete(path):
        if path == doomed and os.path.lexists(path):
            os.unlink(path)

    if when == "listed":
        sync_entry = tree_runtime._sync_entry

        def hook(entry, target, deadline, linked):
            delete(entry.path)
            return sync_entry(entry, target, deadline, linked)

        monkeypatch.setattr(tree_runtime, "_sync_entry", hook)
    elif when == "opened":

        def hook(path, *args):
            delete(path)
            return open(path, *args)

        monkeypatch.setattr(tree_runtime, "open", hook, raising=False)
    else:
        lstat = tree_runtime._lstat

        def hook(path):
            delete(path)
            return lstat(path)

        monkeypatch.setattr(tree_runtime, "_lstat", hook)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert not os.path.lexists(dst / "tmp.txt")
    assert _tree(dst) == _tree(src)


@pytest.mark.parametrize(
    "when",
    [
        "listed",
        pytest.param(
            "walked",
            marks=pytest.mark.skipif(
                not hasattr(os, "listxattr"), reason="no extended attributes on this OS"
            ),
        ),
    ],
)
def test_a_folder_deleted_while_the_copy_walks_goes_from_it(tmp_path, monkeypatch, when):
    """As a file: after the folder above it was listed, or once the walk had
    passed it, before its attributes were read for the copy."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    for name in ("p", "q"):
        (src / name).mkdir(parents=True)
        (src / name / "f.txt").write_text(name)
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    deleted = []

    if when == "listed":
        # Both are listed before either is walked, so the first walked takes
        # the other away.
        sync_entry = tree_runtime._sync_entry

        def hook(entry, target, deadline, linked):
            if not deleted:
                deleted.append("q" if Path(entry.path).parent.name == "p" else "p")
                shutil.rmtree(src / deleted[0])
            return sync_entry(entry, target, deadline, linked)

        monkeypatch.setattr(tree_runtime, "_sync_entry", hook)
    else:
        copy_xattrs = tree_runtime._copy_xattrs

        def hook(a, b):
            if a == str(src / "q") and not deleted:
                deleted.append("q")
                shutil.rmtree(a)
            return copy_xattrs(a, b)

        monkeypatch.setattr(tree_runtime, "_copy_xattrs", hook)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert deleted and not os.path.lexists(dst / deleted[0])
    assert _tree(dst) == _tree(src)


def test_a_linked_name_moved_away_before_its_copy_leaves_the_rest_one_file(
    tmp_path, monkeypatch
):
    """The names after the first link to its copy, which goes with it."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "a").write_bytes(os.urandom(100))
    os.link(src / "a", src / "b")
    moved = []

    def hook(path, *args):
        if not moved and os.path.dirname(path) == str(src):
            moved.append(os.path.basename(path))
            os.rename(path, src / "c")
        return open(path, *args)

    monkeypatch.setattr(tree_runtime, "open", hook, raising=False)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert _tree(dst) == _tree(src)
    left = ({"a", "b"} - set(moved)).pop()
    one, two = (dst / left).stat(), (dst / "c").stat()
    assert (one.st_ino, one.st_nlink) == (two.st_ino, 2)


def test_a_linked_name_replaced_while_it_is_copied_leaves_the_others_their_own_file(
    tmp_path, monkeypatch
):
    """The first name's copy holds the file put in its place, which the old
    file's other names must not be linked to: one of the same size and mtime
    passes for them in every later walk."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "sub").mkdir(parents=True)
    (src / "a").write_bytes(b"old" * 1000)
    os.utime(src / "a", ns=(10**18, 10**18))
    os.link(src / "a", src / "sub" / "b")
    os.link(src / "a", src / "sub" / "c")
    monkeypatch.setattr(tree_runtime, "_CHUNK", 1024)
    replaced, past = [], tree_runtime._past

    def replace_midway(deadline):
        if not replaced and (dst / "a").exists():
            replaced.append(True)
            (src / "a.new").write_bytes(b"new" * 1000)
            os.utime(src / "a.new", ns=(10**18, 10**18))
            os.rename(src / "a.new", src / "a")
        return past(deadline)

    monkeypatch.setattr(tree_runtime, "_past", replace_midway)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert replaced
    assert (dst / "a").read_bytes() == b"new" * 1000
    assert (dst / "sub" / "b").read_bytes() == b"old" * 1000
    one, two = (dst / "sub" / "b").stat(), (dst / "sub" / "c").stat()
    assert (one.st_ino, one.st_nlink) == (two.st_ino, 2)
    assert _tree(dst) == _tree(src)


def test_a_folder_changed_after_the_walk_listed_it_takes_another_walk(tmp_path, monkeypatch):
    """Its stamp takes what the folder was when listed, and a walk that wrote
    nothing else would pass that copy as whole, as for a file."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "f.txt").write_text("x")
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    sync_entry, changed = tree_runtime._sync_entry, []

    def chmod_midway(entry, target, deadline, linked):
        if not changed and entry.name == "f.txt":
            changed.append(True)
            os.chmod(src / "sub", 0o700)
        return sync_entry(entry, target, deadline, linked)

    monkeypatch.setattr(tree_runtime, "_sync_entry", chmod_midway)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert changed
    assert stat.S_IMODE((dst / "sub").stat().st_mode) == 0o700


def test_folders_count_toward_the_reserve(tmp_path, monkeypatch):
    """A tree of empty folders has no file to measure, and each takes a block
    the reserve keeps free."""
    src = tmp_path / "src"
    for i in range(3):
        (src / f"d{i}").mkdir(parents=True)
    monkeypatch.setattr(tree_runtime, "room_for", lambda need, at: False)

    with pytest.raises(tree_runtime.NoRoom):
        tree_runtime.sync_tree(str(src), str(tmp_path / "dst"))


def test_a_folder_swapped_for_a_link_while_it_is_deleted_leads_the_delete_nowhere(
    tmp_path, monkeypatch
):
    """Code at work in the sandbox can swap a folder for a link between the
    delete's stat of it and its listing. Followed, the link takes the delete
    out of the tree, as root."""
    top, outside = tmp_path / "top", tmp_path / "outside"
    (top / "sub").mkdir(parents=True)
    (top / "sub" / "keep.txt").write_text("in the tree")
    outside.mkdir()
    (outside / "keep.txt").write_text("not in the tree")
    swapped = []

    class Racing:
        def __getattr__(self, name):
            return getattr(os, name)

        def lstat(self, path, *args, **kwargs):
            return self._then_swap(path, os.lstat(path, *args, **kwargs))

        def stat(self, path, *args, **kwargs):
            return self._then_swap(path, os.stat(path, *args, **kwargs))

        def _then_swap(self, path, st):
            if not swapped and os.path.basename(path) == "sub":
                swapped.append(True)
                os.rename(top / "sub", tmp_path / "moved")
                os.symlink(outside, top / "sub")
            return st

    monkeypatch.setattr(tree_runtime, "os", Racing())

    assert tree_runtime.remove_tree(str(top))
    assert swapped
    assert not os.path.lexists(top)
    assert (outside / "keep.txt").read_text() == "not in the tree"


def test_a_named_pipe_keeps_its_mode_past_the_umask(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    os.mkfifo(src / "feed")
    (src / "feed").chmod(0o666)
    umask = os.umask(0o022)
    try:
        assert tree_runtime.sync_tree(str(src), str(dst)) is not None
        assert stat.S_IMODE((dst / "feed").lstat().st_mode) == 0o666
        # One an earlier copy made with the umask's mode is made again.
        (dst / "feed").chmod(0o644)
        assert tree_runtime.sync_tree(str(src), str(dst))[1] == 1
    finally:
        os.umask(umask)
    assert stat.S_IMODE((dst / "feed").lstat().st_mode) == 0o666


@pytest.mark.skipif(not hasattr(os, "setxattr"), reason="no extended attributes on this OS")
def test_a_copy_keeps_extended_attributes_a_rename_would(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "f.txt").write_text("x")
    try:
        os.setxattr(src / "sub" / "f.txt", "user.origin", b"https://example.com/f")
        os.setxattr(src / "sub", "user.tag", b"kept")
    except OSError:
        pytest.skip("this filesystem takes no user attributes")
    # Read-only, so the attributes have to go on before the mode does.
    (src / "sub" / "f.txt").chmod(0o444)
    (src / "sub").chmod(0o555)
    try:
        assert tree_runtime.sync_tree(str(src), str(dst)) is not None
        assert os.getxattr(dst / "sub" / "f.txt", "user.origin") == b"https://example.com/f"
        assert os.getxattr(dst / "sub", "user.tag") == b"kept"
    finally:
        for top in (src, dst):
            if (top / "sub").exists():
                (top / "sub").chmod(0o755)


@pytest.mark.skipif(not hasattr(os, "setxattr"), reason="no extended attributes on this OS")
def test_an_attribute_the_source_lost_since_an_earlier_copy_goes(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "sub").mkdir(parents=True)
    try:
        os.setxattr(src / "sub", "user.tag", b"revoked")
    except OSError:
        pytest.skip("this filesystem takes no user attributes")
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert "user.tag" in os.listxattr(dst / "sub")

    os.removexattr(src / "sub", "user.tag")
    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert "user.tag" not in os.listxattr(dst / "sub")


@pytest.mark.skipif(not hasattr(os, "setxattr"), reason="no extended attributes on this OS")
def test_an_attribute_changed_as_the_copy_takes_them_is_taken_again(tmp_path, monkeypatch):
    """A change to one moves only the source's ctime, which the copy's own
    writes after it would put behind the copy's: a later pass would take the
    copy for one made since."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "f.txt").write_text("x")
    try:
        os.setxattr(src / "f.txt", "user.tag", b"old")
    except OSError:
        pytest.skip("this filesystem takes no user attributes")
    copy_xattrs, calls = tree_runtime._copy_xattrs, itertools.count()

    def changed_after(s, d):
        copy_xattrs(s, d)
        if next(calls) == 0:
            # Past a coarse clock's tick, so the ctime moves.
            time.sleep(0.05)
            os.setxattr(s, "user.tag", b"new")

    monkeypatch.setattr(tree_runtime, "_copy_xattrs", changed_after)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert os.getxattr(dst / "f.txt", "user.tag") == b"new"


def test_a_link_and_a_pipe_keep_their_times(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "alias").symlink_to("nowhere")
    os.mkfifo(src / "feed")
    then = 1_600_000_000 * 10**9
    for name in ("alias", "feed"):
        os.utime(src / name, ns=(then, then), follow_symlinks=False)

    def times(top: Path) -> dict:
        return {name: (top / name).lstat().st_mtime_ns for name in ("alias", "feed")}

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert times(dst) == times(src) == {"alias": then, "feed": then}
    # One an earlier copy stamped with the time it was made is made again.
    os.utime(dst / "alias", ns=(then + 1, then + 1), follow_symlinks=False)
    assert tree_runtime.sync_tree(str(src), str(dst))[1] == 1
    assert times(dst) == times(src)


def test_a_link_and_a_pipe_given_another_owner_since_the_copy_are_made_again(tmp_path, monkeypatch):
    """A chown moves nothing a link or a pipe has but its ctime; copied as
    root, the old copy kept the owner it was made with."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "alias").symlink_to("nowhere")
    os.mkfifo(src / "feed")
    owned = []
    monkeypatch.setattr(tree_runtime.os, "geteuid", lambda: 0)
    monkeypatch.setattr(tree_runtime.os, "lchown", lambda path, uid, gid: owned.append(os.path.basename(path)))

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert tree_runtime.sync_tree(str(src), str(dst))[1] == 0
    owned.clear()
    time.sleep(0.05)
    for name in ("alias", "feed"):
        os.chown(src / name, os.getuid(), os.getgid(), follow_symlinks=False)
    assert tree_runtime.sync_tree(str(src), str(dst))[1] == 2
    assert sorted(name for name in owned if name != "dst") == ["alias", "feed"]


def test_a_copy_made_as_root_keeps_setuid_and_setgid(tmp_path, monkeypatch):
    """A chown clears both, on Linux for root too, so the mode goes on after
    the owner; set before, the copy lost them and was copied again each pass."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "tool").write_bytes(b"#!/bin/sh\n")
    (src / "tool").chmod(0o6755)

    def lchown(path, uid, gid):
        mode = stat.S_IMODE(os.lstat(path).st_mode)
        os.chmod(path, mode & ~(stat.S_ISUID | stat.S_ISGID))

    monkeypatch.setattr(tree_runtime.os, "geteuid", lambda: 0)
    monkeypatch.setattr(tree_runtime.os, "lchown", lchown)

    assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert stat.S_IMODE((dst / "tool").lstat().st_mode) == 0o6755
    assert tree_runtime.sync_tree(str(src), str(dst))[1] == 0


def test_a_file_that_became_a_socket_leaves_no_copy_behind(tmp_path, monkeypatch):
    """A socket is not copied, and neither is the file an earlier pass
    copied under its name kept."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    (src / "agent.sock").write_text("stale")
    tree_runtime.sync_tree(str(src), str(dst))
    (src / "agent.sock").unlink()
    # By a relative name: a socket path has a short length limit.
    monkeypatch.chdir(src)
    with socket.socket(socket.AF_UNIX) as sock:
        sock.bind("agent.sock")
        assert tree_runtime.sync_tree(str(src), str(dst)) is not None
    assert not os.path.lexists(dst / "agent.sock")
