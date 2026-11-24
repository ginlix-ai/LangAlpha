"""Races the kernel cannot hit on cue, driven in this process: the daemon's
operations over the fake server's ``Drive`` (``local.py``), called as the
kernel calls them, with the server paused or failed at the step that
matters. Needs no root and no ``/dev/fuse``."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import sys
import tempfile
import threading
import time

from checks import Results, case
from fake_server import Upload
from local import Daemon, Runtime, server

W = os.O_CREAT | os.O_WRONLY


def _err(fn, *args) -> str:
    """What the call answered: "ok", or its errno's name."""
    try:
        fn(*args)
        return "ok"
    except OSError as exc:
        return errno.errorcode.get(exc.errno, str(exc.errno))


def _new(fs, path: str, data: bytes, flags: int = W) -> None:
    fs.create(path, 0o644, flags)
    fs.write(path, data, 0, 0)


@case("races")
def folder_renamed_while_a_file_in_it_saves(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    fs = d.fs
    fs.mkdir("/personal/tmp", 0o755)
    path = "/personal/tmp/x.bin"
    _new(fs, path, b"payload")
    go, inflight = threading.Event(), threading.Event()
    s.on("write", "tmp/x.bin", lambda body: (inflight.set(), go.wait(2)))
    t = threading.Thread(target=d.save, args=(path,))
    t.start()
    if not inflight.wait(2):
        go.set()
        raise AssertionError("the save never reached the server")
    mv = threading.Thread(target=fs.rename, args=("/personal/tmp", "/personal/final"))
    mv.start()
    time.sleep(0.2)
    go.set()
    t.join()
    mv.join()
    s.clear()
    d.age()
    d.tick()
    time.sleep(0.3)
    d.save_all()
    r.pin("a folder renamed while a file in it is being saved: the file is on the server under the new "
          "folder, nothing left under the old",
          s.bytes("final/x.bin") == b"payload" and not s.has("tmp/x.bin") and not s.has("tmp"),
          f"refusals {s.refusals}")


@case("races")
def a_second_daemon_on_the_same_journal(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("doc.md", b"v1")
    root = tempfile.mkdtemp(dir=rt.tmp)
    one = Daemon(rt, s, root)
    path = "/personal/doc.md"
    one.fs.truncate(path, 0)
    one.fs.write(path, b"part1-", 0, 0)
    two = Daemon(rt, s, root)
    taken = two.restore()
    one.fs.write(path, b"part2", 6, 0)  # the writer keeps going through the old mount
    two.save_all()
    _err(one.save, path)
    r.pin("a new daemon leaves a record the old one still writes; the old one saves all of it",
          s.bytes("doc.md") == b"part1-part2" and not s.refusals,
          f"taken {taken}, server {s.bytes('doc.md')!r}, refusals {s.refusals}")
    # The old daemon goes with a save unanswered: the new one takes it up.
    s = server(rt)
    root = tempfile.mkdtemp(dir=rt.tmp)
    one = Daemon(rt, s, root)
    _new(one.fs, "/personal/late.txt", b"late")
    two = Daemon(rt, s, root)
    early = two.restore()
    one.die()
    two.adopt()
    two.save_all()
    r.pin("once the old daemon is gone, its record is taken up and saved",
          s.bytes("late.txt") == b"late", f"taken before it went {early}")


@case("races")
def a_write_holding_the_file_while_its_save_lands(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    path = "/personal/log.txt"
    _new(d.fs, path, b"AAAA")
    out = []
    with d.write_paused_after_lookup(path) as found:
        w = threading.Thread(target=lambda: out.append(d.fs.write(path, b"BBBB", 4, 0)))
        w.start()
        if not found.wait(2):
            raise AssertionError("the write never reached its lookup")
        d.save(path)
        w.join()
    if d.pending(path):
        d.save(path)
    r.pin("a write that held the file while its save landed is not thrown away",
          s.bytes("log.txt") == b"AAAABBBB", f"write returned {out}, server {s.bytes('log.txt')!r}")
    # The idle sweep against a write already holding the file.
    s = server(rt)
    s.put("idle.txt", b"base")
    d = Daemon(rt, s)
    path = "/personal/idle.txt"
    d.fs.create(path, 0o644, os.O_WRONLY)  # opened, not changed
    d.age(path, first=False)

    sweeper = threading.Thread(target=d.tick)

    def sweep_meanwhile() -> None:
        sweeper.start()
        time.sleep(0.2)

    d.while_writing(path, sweep_meanwhile, b"EDIT")
    sweeper.join()
    if d.pending(path):
        d.save(path)
    r.pin("the idle sweep does not close a file a write is changing",
          s.bytes("idle.txt") == b"EDIT", repr(s.bytes("idle.txt")))


@case("races")
def a_refused_save_stops_the_writer(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("notes.md", b"ORIGINAL-LINE-1\nORIGINAL-LINE-2\n")
    d = Daemon(rt, s)
    path = "/personal/notes.md"
    d.fs.truncate(path, 0)
    d.fs.write(path, b"agent-part-1\n", 0, 0)
    theirs = b"OTHER-EDIT-FROM-WEB-xxxxxxxxxxxxxxxxx\n"
    s.put("notes.md", theirs)
    d.deliver("notes.md")
    _err(d.save, path)

    def second() -> None:
        d.fs.write(path, b"agent-part-2\n", 13, 0)
        if d.pending(path):
            d.save(path)

    got = _err(second)
    r.pin("after a refused save, the writer's next bytes fail rather than splice onto the other version",
          s.bytes("notes.md") == theirs and got != "ok", f"next write {got}, server {s.bytes('notes.md')!r}")
    s = server(rt)
    d = Daemon(rt, s)
    path = "/personal/new.bin"
    _new(d.fs, path, b"12345")
    s.on("write", "new.bin", lambda body: s.refuse("over_quota"))
    _err(d.save, path)
    s.clear()

    def second_new() -> None:
        d.fs.write(path, b"6789", 5, 0)
        if d.pending(path):
            d.save(path)

    got = _err(second_new)
    r.pin("after a refused new file, the writer's next bytes fail rather than save behind a hole",
          not s.has("new.bin") and got != "ok", f"next write {got}, server {s.bytes('new.bin')!r}")


@case("races")
def a_record_whose_save_landed_before_the_daemon_died(r: Results, rt: Runtime) -> None:
    s = server(rt)
    root = tempfile.mkdtemp(dir=rt.tmp)
    one = Daemon(rt, s, root)
    _new(one.fs, "/personal/new.txt", b"hello")
    s.put("new.txt", b"hello")  # committed; the daemon died before its answer
    one.die()
    two = Daemon(rt, s, root)
    two.restore()
    two.save_all()
    r.pin("a record whose save landed before the daemon died is taken as saved, not refused",
          not s.refusals and not two.pending_paths() and not two.forgot,
          f"refusals {s.refusals}, pending {two.pending_paths()}")


@case("races")
def a_file_still_being_written_after_a_save(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    path = "/personal/big.csv"
    _new(d.fs, path, b"x" * 10)
    d.age(path, 31, changed=False)
    s.on("write", "big.csv", lambda body: d.fs.write(path, b"y", 10, 0))
    d.save(path)
    s.clear()
    d.since_change(path, 0.1)  # still being written: its last write 0.1 s ago
    r.pin("a file still being written is not saved again on the next tick after a save",
          not d.due(), f"dirty {d.dirty(path)}")


@case("races")
def fsync_during_a_refused_background_save(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    path = "/personal/q.txt"
    _new(d.fs, path, b"data")

    def refuse(body):
        time.sleep(0.2)
        s.refuse("over_quota")

    s.on("write", "q.txt", refuse)
    t = d.save_in_background(path)
    time.sleep(0.05)
    try:
        answer = d.fs.fsync(path, 0, 0)
    except OSError as exc:
        answer = errno.errorcode.get(exc.errno, str(exc.errno))
    t.join()
    r.pin("fsync during a background save the server refuses answers that refusal",
          answer not in (0, None), f"fsync {answer!r}")


@case("races")
def a_file_past_the_cap_is_not_saved_cut_short(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("data.bin", b"GOOD-DATA")
    s.drive.file_cap = s.drive.upload_cap = 10
    root = tempfile.mkdtemp(dir=rt.tmp)
    one = Daemon(rt, s, root)
    path = "/personal/data.bin"
    one.fs.truncate(path, 0)
    one.fs.write(path, b"12345678", 0, 0)
    met = _err(one.fs.write, path, b"9abcdef", 8, 0)
    r.pin("the write past the cap answers EFBIG", met == "EFBIG", met)
    one.die()  # within the debounce
    two = Daemon(rt, s, root)
    two.restore()
    two.save_all()
    r.pin("a file that met EFBIG is not saved cut short by the next daemon",
          s.bytes("data.bin") == b"GOOD-DATA", repr(s.bytes("data.bin")))


@case("races")
def a_temp_file_renamed_while_its_save_is_unanswered(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("repo/.git/HEAD", b"ref")
    d = Daemon(rt, s)
    path = "/personal/repo/.git/index.lock"
    _new(d.fs, path, b"INDEX", W | os.O_EXCL)
    committed = threading.Event()

    def commit_then_drop(body):
        s.put("repo/.git/index.lock", body)
        committed.set()
        time.sleep(0.3)
        raise OSError(errno.ECONNRESET, "connection reset")

    s.on("write", "repo/.git/index.lock", commit_then_drop)
    t = d.save_in_background(path)
    committed.wait(2)
    s.clear()
    renamed = _err(d.fs.rename, path, "/personal/repo/.git/index")
    t.join()
    d.save_all()
    r.pin("a lock file renamed while its save went unanswered is not left on the server",
          not s.has("repo/.git/index.lock") and s.bytes("repo/.git/index") == b"INDEX", f"rename {renamed}")


@case("races")
def a_save_that_committed_after_its_answer_was_lost(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    path = "/personal/late.txt"
    _new(d.fs, path, b"one")
    calls = []

    def hook(body):
        calls.append(body)
        if len(calls) == 1:
            raise OSError(errno.ECONNRESET, "dropped")  # not committed, yet
        if len(calls) == 2:
            s.put("late.txt", calls[0])  # the first commits late

    s.on("write", "late.txt", hook)
    _err(d.save, path)
    d.fs.write(path, b"two", 0, 0)
    _err(d.save, path)
    s.clear()
    r.pin("a save that committed after its answer was lost does not make the next one a refusal",
          s.bytes("late.txt") == b"two" and not d.forgot and not d.pending_paths(),
          f"server {s.bytes('late.txt')!r}, dropped {d.forgot}")


@case("races")
def a_folder_renamed_over_one_holding_unsaved_files(r: Results, rt: Runtime) -> None:
    d = Daemon(rt, server(rt))
    d.fs.mkdir("/personal/a", 0o755)
    d.fs.mkdir("/personal/b", 0o755)
    _new(d.fs, "/personal/b/f.txt", b"unsaved")
    got = _err(d.fs.rename, "/personal/a", "/personal/b")
    r.pin("a folder renamed over one holding only unsaved files is refused with ENOTEMPTY",
          got == "ENOTEMPTY", got)


@case("races")
def a_rename_whose_move_failed(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("src.txt", b"source")
    d = Daemon(rt, s)
    _new(d.fs, "/personal/dst.txt", b"precious")
    s.on("move", "src.txt", lambda: s.refuse("changed"))
    _err(d.fs.rename, "/personal/src.txt", "/personal/dst.txt")
    s.clear()
    data = d.pending_bytes("/personal/dst.txt")
    r.pin("a rename whose move failed keeps the destination's unsaved bytes", data == b"precious", repr(data))


@case("races")
def a_settle_of_many_files(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s)
    for i in range(40):
        _new(d.fs, f"/personal/s{i}.txt", b"x")
        s.on("write", f"s{i}.txt", lambda body: time.sleep(0.05))
    d.settle("call-one", 10)
    saved = sum(s.has(f"s{i}.txt") for i in range(40))
    r.pin("a settle sends at most 8 saves at once", s.peak <= 8 and saved == 40,
          f"peak {s.peak}, saved {saved}")


@case("races")
def notices_past_what_is_kept(r: Results, rt: Runtime) -> None:
    notices = rt.notices.Notices(os.path.join(tempfile.mkdtemp(dir=rt.tmp), "notices.jsonl"))
    for i in range(520):
        notices.add("unsaved", "c", "personal", f"f{i}", "x")
    notes = notices.take()
    more = [n for n in notes if n.get("kind") == "more"]
    r.pin("notices past the 500 kept say how many more there were",
          len(notes) <= 500 and more and more[0].get("count") == 21, f"{len(notes)} kept, more {more[:1]}")


@case("races")
def saved_now_goes_to_the_call_told_not_saved(r: Results, rt: Runtime) -> None:
    s = server(rt)
    d = Daemon(rt, s, call=["call-told"])
    path = "/personal/t.txt"
    _new(d.fs, path, b"one")

    def drop(body):
        raise OSError(errno.ECONNRESET, "dropped")

    s.on("write", "t.txt", drop)
    _err(d.save, path)
    s.clear()
    d.set_call("call-later")
    d.fs.write(path, b"two", 0, 0)
    d.save(path)
    saved = [n for n in d.notices.take() if n["kind"] == "saved"]
    r.pin("'Saved now' is filed under the call that was told 'not saved yet'",
          saved and saved[0]["call"] == "call-told", str(saved))


@case("races")
def a_create_over_a_name_the_kernel_holds_absent(r: Results, rt: Runtime) -> None:
    s = server(rt)
    s.put("faults/r6.log", b"line1\n")
    d = Daemon(rt, s)
    path = "/personal/faults/r6.log"
    d.fs.create(path, 0o644, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    size = d.fs.getattr(path)["st_size"]
    d.fs.write(path, b"line2\n", size, None)
    d.fs.fsync(path, 0, None)
    appended = s.bytes("faults/r6.log")
    excl = _err(d.fs.create, path, 0o644, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    r.pin("a create over an existing file honours O_APPEND and O_EXCL",
          appended == b"line1\nline2\n" and excl == "EEXIST", f"{appended!r}, O_EXCL {excl}")


@case("races")
def shared_maps_walked_while_others_change_them(r: Results, rt: Runtime) -> None:
    """Each race many times over, with threads switching as often as they can."""
    s = server(rt)
    s.put("faults/seed.md", b"seed\n")
    d = Daemon(rt, s)
    switch = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        r.pin("the generated .ignore built while the feed changes the manifest never fails",
              *_ignore_while_feed_changes(d))
        r.pin("rmdir while the saver ends pending files never fails", *_rmdir_while_pending_churns(d, s))
        r.pin("text stored a moment before the manifest names it is not collected meanwhile",
              *_collect_while_text_arrives(d))
    finally:
        sys.setswitchinterval(switch)


def _wait(pred, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)
    return True


@case("races")
def a_rename_or_delete_never_waits_on_a_save_in_parts(r: Results, rt: Runtime) -> None:
    """Files saved in parts, each read back by the server for seconds: an
    unlink of one and a rename of another answer at once, and the server
    ends with what a disk would hold (the one gone, the other at its new
    name), whatever landed meanwhile."""
    s = server(rt)
    s.drive.file_cap, s.drive.part_size, s.drive.verify_s = 1 << 20, 256 << 10, 4.0
    d = Daemon(rt, s)
    fs = d.fs
    gone, moved = os.urandom(3 << 20), os.urandom(3 << 20)
    _new(fs, "/personal/gone.bin", gone)
    _new(fs, "/personal/moved.bin", moved)
    saves = [d.save_in_background("/personal/gone.bin"), d.save_in_background("/personal/moved.bin")]
    verifying = _wait(lambda: sum(u.state == "verifying" for u in s.drive.uploads.values()) == 2, 20)
    took: dict = {}

    def timed(name, fn, *args) -> None:
        t0 = time.monotonic()
        took[name] = (_err(fn, *args), time.monotonic() - t0)

    ops = [threading.Thread(target=timed, args=("unlink", fs.unlink, "/personal/gone.bin")),
           threading.Thread(target=timed, args=("rename", fs.rename, "/personal/moved.bin", "/personal/there.bin"))]
    for t in ops:
        t.start()
    for t in ops:
        t.join(30)
    (unlinked, t_unlink), (renamed, t_rename) = took["unlink"], took["rename"]
    r.pin("an unlink of a file whose save in parts is being read back answers at once",
          verifying and unlinked == "ok" and t_unlink < 1.0, f"{unlinked} in {t_unlink:.2f}s")
    r.pin("so does a rename of one", verifying and renamed == "ok" and t_rename < 1.0, f"{renamed} in {t_rename:.2f}s")
    for t in saves:
        t.join(30)

    def settled() -> bool:
        d.tick()
        return not s.has("gone.bin") and not s.has("moved.bin") and s.bytes("there.bin") == moved

    r.pin("the server ends as a disk would: one gone, the other at its new name", _wait(settled, 30),
          f"gone {s.has('gone.bin')}, moved {s.has('moved.bin')}, there {s.has('there.bin')}")


def _large_server(rt: Runtime, verify_s: float = 1.0):
    s = server(rt)
    s.drive.file_cap, s.drive.part_size, s.drive.verify_s = 1 << 20, 256 << 10, verify_s
    return s


def _settle_saves(d: Daemon, until, timeout: float = 20) -> bool:
    def done() -> bool:
        d.age()
        d.tick()
        return until()
    return _wait(done, timeout)


@case("races")
def a_take_back_never_trashes_a_newer_edit(r: Results, rt: Runtime) -> None:
    """A new file saved in parts is renamed while the server reads it back,
    which this server cannot stop, and the person edits the old name before
    the daemon learns the upload landed: the take-back goes only for the
    bytes its own upload sent, so the edit stays."""
    s = _large_server(rt, verify_s=1.0)
    s.drive.abort_verifying = False
    d = Daemon(rt, s)
    fs = d.fs
    big, edit = os.urandom(3 << 20), b"the person's edit\n"
    _new(fs, "/personal/a.bin", big)
    edited = threading.Event()
    s.on("upload_status", "*", lambda: edited.wait(20))
    save = d.save_in_background("/personal/a.bin")
    verifying = _wait(lambda: any(u.state == "verifying" for u in s.drive.uploads.values()), 20)
    renamed = _err(fs.rename, "/personal/a.bin", "/personal/b.bin")
    landed = _wait(lambda: any(u.state == "done" for u in s.drive.uploads.values()), 20)
    s.put("a.bin", edit)
    edited.set()
    save.join(30)
    s.clear()
    saved = _settle_saves(d, lambda: s.bytes("b.bin") == big)
    r.pin("the rename answers while the upload is read back", verifying and renamed == "ok", renamed)
    r.pin("the person's edit at the old name survives the take-back", landed and s.bytes("a.bin") == edit,
          f"a.bin holds {len(s.bytes('a.bin') or b'')} bytes")
    r.pin("the file is saved at its new name", saved)


@case("races")
def a_removal_after_a_failed_status_read_still_ends_the_upload(r: Results, rt: Runtime) -> None:
    """The complete answered and the next status read failed; the program
    then removes the file. The save is still the upload's, so the removal
    ends it and the file does not come back."""
    s = _large_server(rt, verify_s=2.0)
    d = Daemon(rt, s)
    fs = d.fs
    _new(fs, "/personal/gone.bin", os.urandom(3 << 20))
    s.drive.status_fails = 1
    save = d.save_in_background("/personal/gone.bin")
    verifying = _wait(lambda: any(u.state == "verifying" for u in s.drive.uploads.values()), 20)
    _wait(lambda: s.drive.status_fails == 0, 10)
    removed = _err(fs.unlink, "/personal/gone.bin")
    save.join(40)
    time.sleep(2.5)
    r.pin("the removal answers", verifying and removed == "ok", removed)
    r.pin("the deleted file does not come back", not s.has("gone.bin"),
          str({u.state for u in s.drive.uploads.values()}))


@case("races")
def a_rename_of_a_large_file_whose_save_went_unanswered(r: Results, rt: Runtime) -> None:
    """A save in parts got no answer to its complete, and the file is then
    renamed: the move succeeds and the file is saved under its new name."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    fs = d.fs
    big = os.urandom(3 << 20)
    _new(fs, "/personal/old.bin", big)

    def lost(*_):
        raise OSError(errno.ECONNRESET, "the answer was lost")

    s.on("upload_complete", "*", lost)
    try:
        d.save("/personal/old.bin")
    except OSError:
        pass
    s.clear()
    renamed = _err(fs.rename, "/personal/old.bin", "/personal/new.bin")
    saved = _settle_saves(d, lambda: s.bytes("new.bin") == big)
    r.pin("the rename succeeds", renamed == "ok", renamed)
    r.pin("the file is saved under its new name and not its old", saved and not s.has("old.bin"),
          f"new {s.has('new.bin')}, old {s.has('old.bin')}")


@case("races")
def a_rename_of_a_large_file_whose_complete_landed_but_its_answer_was_lost(r: Results, rt: Runtime) -> None:
    """The complete reached the server and was applied, and only its answer
    was lost; the file is then renamed: the move succeeds and the file is
    saved under its new name."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    fs = d.fs
    big = os.urandom(3 << 20)
    _new(fs, "/personal/old.bin", big)

    def lost(*_):
        for u in list(s.drive.uploads):
            s.drive.upload_complete(u)
        raise OSError(errno.ECONNRESET, "the answer was lost")

    s.on("upload_complete", "*", lost)
    try:
        d.save("/personal/old.bin")
    except OSError:
        pass
    s.clear()
    renamed = _err(fs.rename, "/personal/old.bin", "/personal/new.bin")
    saved = _settle_saves(d, lambda: s.bytes("new.bin") == big)
    r.pin("the rename succeeds", renamed == "ok", renamed)
    r.pin("the file is saved under its new name and not its old", saved and not s.has("old.bin"),
          f"new {s.has('new.bin')}, old {s.has('old.bin')}")


@case("races")
def a_save_answered_by_no_drive_handler_is_kept_and_told(r: Results, rt: Runtime) -> None:
    """A save answered 404 with no Drive code (a proxy, a route not there):
    the bytes are kept and sent again, and the command is told NOT SAVED YET
    with the reason, never dropped as refused."""
    s = server(rt)
    d = Daemon(rt, s)
    fs = d.fs
    d.set_call("call-codeless")
    _new(fs, "/personal/kept.txt", b"keep me\n")

    def codeless(_body):
        raise s.Refused(404, b"<html>not found</html>")

    s.on("write", "kept.txt", codeless)
    try:
        d.save("/personal/kept.txt")
    except OSError:
        pass
    told = [n for n in d.notices.peek() if n["kind"] == "unsaved" and n.get("call") == "call-codeless"]
    kept = d.pending("/personal/kept.txt") and d.pending_bytes("/personal/kept.txt") == b"keep me\n"
    s.clear()
    saved = _settle_saves(d, lambda: s.bytes("kept.txt") == b"keep me\n")
    r.pin("the bytes are kept", kept)
    r.pin("the command is told NOT SAVED YET, with the reason", bool(told) and "404" in told[0].get("reason", ""),
          str(told))
    r.pin("once the server answers, it is saved", saved)


@case("races")
def a_reader_gets_the_file_it_opened_or_an_error(r: Results, rt: Runtime) -> None:
    """A reader partway through a large file: the file replaced by a small
    one, or deleted and made again at the same path, still reads as what it
    began on."""
    for how in ("small", "recreated"):
        s = server(rt)
        v1 = os.urandom(6 << 20)
        s.put("big.bin", v1)
        d = Daemon(rt, s)
        fs = d.fs
        first = fs.read("/personal/big.bin", 1 << 20, 0, 0)
        if how == "small":
            s.put("big.bin", b"small now\n")
        else:
            gone = s.drive.get("big.bin")["id"]
            s.drive.trash_entry("big.bin", "file", False)
            fs.names.commit_drop(d.space, gone, s.drive.seq)
            s.put("big.bin", b"another file\n")
        d.deliver("big.bin")
        try:
            more = fs.read("/personal/big.bin", 1 << 20, 1 << 20, 0)
        except OSError as exc:
            more = exc.errno
        r.pin(f"{how}: the reader goes on with the bytes it began on",
              first == v1[: 1 << 20] and more == v1[1 << 20 : 2 << 20],
              f"got {more if isinstance(more, int) else len(more)}")


@case("races")
def a_save_whose_file_went_while_it_was_read_back(r: Results, rt: Runtime) -> None:
    """The upload lands and the file is trashed before the daemon reads its
    end, so the status names no entry: the save ends cleanly and is not sent
    again over the deletion."""
    s = _large_server(rt, verify_s=0.5)
    d = Daemon(rt, s)
    fs = d.fs
    _new(fs, "/personal/went.bin", os.urandom(3 << 20))
    trashed = threading.Event()
    s.on("upload_status", "*", lambda: trashed.wait(20))
    save = d.save_in_background("/personal/went.bin")
    landed = _wait(lambda: any(u.state == "done" for u in s.drive.uploads.values()), 20)
    s.drive.trash_entry("went.bin", "file", False)
    trashed.set()
    save.join(30)
    s.clear()
    d.age()
    d.tick()
    time.sleep(0.5)
    r.pin("the save ends and lets the file go", landed and not d.pending("/personal/went.bin"))
    r.pin("it is not sent again over the deletion", not s.has("went.bin"))


@case("races")
def an_upload_open_again_at_its_end_is_finished(r: Results, rt: Runtime) -> None:
    """The store loses a part at the read-back and the upload is open again:
    the daemon sends that part and completes again."""
    s = _large_server(rt, verify_s=0.2)
    s.drive.reopen_part = 2
    d = Daemon(rt, s)
    big = os.urandom(3 << 20)
    _new(d.fs, "/personal/again.bin", big)
    save = d.save_in_background("/personal/again.bin")
    save.join(30)
    r.pin("the file is saved whole", not save.is_alive() and s.bytes("again.bin") == big,
          f"alive {save.is_alive()}, {len(s.bytes('again.bin') or b'')} bytes")


@case("races")
def an_upload_ended_under_a_save_begins_again(r: Results, rt: Runtime) -> None:
    """A part answered ``upload_ended`` (the upload ran out of time): the
    save begins again rather than dropping the file."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    big = os.urandom(3 << 20)
    _new(d.fs, "/personal/ended.bin", big)
    ended = {"n": 0}

    def once(_body):
        if not ended["n"]:
            ended["n"] += 1
            for up in s.drive.uploads.values():
                up.state = "aborted"
            s.refuse("upload_ended")

    s.on("upload_part", "*", once)
    try:
        d.save("/personal/ended.bin")
    except OSError:
        pass
    s.clear()
    saved = _settle_saves(d, lambda: s.bytes("ended.bin") == big)
    r.pin("the file is saved, not dropped", saved, f"pending {d.pending('/personal/ended.bin')}")


@case("races")
def a_save_whose_store_failed_at_the_end_is_kept(r: Results, rt: Runtime) -> None:
    """The server ends an upload ``failed`` because the store failed, or the
    bytes came damaged: not the Drive refusing them. They are kept, sent
    again from the begin, and land; the command is told it is not saved
    yet, then that it is."""
    for kind in ("unavailable", "mismatch"):
        s = _large_server(rt, verify_s=0.1)
        s.drive.fail_ends = [kind, 1]
        d = Daemon(rt, s)
        d.set_call(f"call-end-{kind}")
        big = os.urandom(3 << 20)
        _new(d.fs, f"/personal/{kind}.bin", big)
        try:
            d.save(f"/personal/{kind}.bin")
        except OSError:
            pass
        kept = d.pending(f"/personal/{kind}.bin")
        for p in d.names.writing():
            p.retry_at = 0.0
        call = f"call-end-{kind}"
        saved = _settle_saves(d, lambda: s.bytes(f"{kind}.bin") == big
                              and any(n["kind"] == "saved" for n in d.notices.peek() if n.get("call") == call))
        told = [n["kind"] for n in d.notices.peek() if n.get("call") == call]
        r.pin(f"{kind} at the end: the bytes are kept and land", kept and saved, f"kept {kept}, told {told}")
        r.pin(f"{kind} at the end: told not saved yet, then saved", told == ["unsaved", "saved"], str(told))


@case("races")
def nine_large_saves_at_once_all_land(r: Results, rt: Runtime) -> None:
    """Nine large files saved at once, first beside three of the person's
    eight uploads held by the browser, then beside six: none is dropped, the
    daemon keeps to its share, and saves the server had no upload for wait
    and land as uploads end."""
    for browser in (3, 6):
        s = _large_server(rt, verify_s=0.3)
        for i in range(browser):
            up = Upload(s.drive, f"browser/{i}.bin", 3 << 20, None, None, True, None)
            s.drive.uploads[up.id] = up
        d = Daemon(rt, s)
        files = {f"/personal/n{i}.bin": os.urandom((2 << 20) + i) for i in range(9)}
        for path, data in files.items():
            _new(d.fs, path, data)
        peak = {"n": 0}

        def all_landed() -> bool:
            peak["n"] = max(peak["n"], sum(u.state in ("uploading", "verifying") and not u.path.startswith("browser")
                                           for u in s.drive.uploads.values()))
            d.age()
            d.tick()
            return all(s.bytes(path[len("/personal/"):]) == data for path, data in files.items())

        landed = _wait(all_landed, 120)
        dropped = [p for p in files if not d.pending(p) and s.bytes(p[len("/personal/"):]) != files[p]]
        r.pin(f"beside {browser} browser uploads: all nine land, none dropped", landed and not dropped,
              f"dropped {dropped}; refusals {sorted(set(s.refusals))}")
        r.pin(f"beside {browser} browser uploads: the daemon keeps to its share", peak["n"] <= 4,
              f"peak {peak['n']} under way")


@case("races")
def a_save_that_never_lands_fails_for_good_and_its_bytes_are_kept(r: Results, rt: Runtime) -> None:
    """Saves answered by no Drive handler for the whole give-up time: tries
    stop, the command is told where its bytes are kept, the file still reads
    them, a large file is not hashed again for each try, and once kept long
    enough the bytes go, told."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    d.set_call("call-never")
    big = os.urandom(3 << 20)
    _new(d.fs, "/personal/never.bin", big)
    hashed = {"n": 0}
    real = d.fs.saves.uploads.digest

    def counted(*a):
        hashed["n"] += 1
        return real(*a)

    d.fs.saves.uploads.digest = counted

    def codeless(*_):
        raise s.Refused(502, b"<html>bad gateway</html>")

    s.on("upload_begin", "*", codeless)
    p = d.names.pending("/personal/never.bin")
    for _ in range(3):
        p.retry_at = 0.0
        try:
            d.fs.saves.save(p, None)
        except OSError:
            pass
    r.pin("a file unchanged is hashed once across its tries", hashed["n"] == 1, f"{hashed['n']} hashes")
    p.failing_since -= rt.writes.GIVE_UP_S
    p.retry_at = 0.0
    try:
        d.fs.saves.save(p, None)
    except OSError:
        pass
    lost = [n for n in d.notices.peek() if n["kind"] == "lost" and n.get("call") == "call-never"]
    reads = d.fs.read("/personal/never.bin", 1 << 20, 0, 0)
    r.pin("tries stop and the command is told where its bytes are kept", p.gave_up and bool(lost),
          lost[0]["reason"] if lost else "")
    r.pin("the file still reads its bytes", reads == big[: 1 << 20])
    d.age()
    d.tick()
    r.pin("no try is sent before its late one", d.pending("/personal/never.bin") and p.gave_up)
    p.retry_at = 0.0
    if hasattr(type(p), "kept_until"):
        p.kept_until = 0.0
    d.tick()
    _wait(lambda: not d.pending("/personal/never.bin"), 10)
    gone = [n for n in d.notices.peek() if n["kind"] == "lost" and "are gone" in n.get("reason", "")]
    r.pin("once kept long enough the bytes go, told", not d.pending("/personal/never.bin") and bool(gone))


def _fail_for_good(rt: Runtime, d: Daemon, path: str) -> None:
    """Try ``path``'s save twice, the second past the give-up time."""
    p = d.names.pending(path)
    for back in (0.0, rt.writes.GIVE_UP_S):
        p.failing_since -= back
        p.retry_at = 0.0
        try:
            d.fs.saves.save(p, None)
        except OSError:
            pass


@case("races")
def a_save_failed_for_good_keeps_trying_and_ends_told_right(r: Results, rt: Runtime) -> None:
    """A save that failed for good is still tried about hourly while its
    bytes are kept: a server back after the give-up time saves it, told.
    One that in fact landed is told saved when its kept time ends, not
    gone. Removing a file failed for good frees its journal bytes at once."""
    s = _large_server(rt)
    d = Daemon(rt, s)

    def codeless(*_):
        raise s.Refused(502, b"<html>bad gateway</html>")

    d.set_call("call-late")
    _new(d.fs, "/personal/late.txt", b"late but saved\n")
    s.on("write", "late.txt", codeless)
    _fail_for_good(rt, d, "/personal/late.txt")
    p = d.names.pending("/personal/late.txt")
    soon = p.retry_at - time.monotonic()
    r.pin("failed for good, its next try is about an hour away", p.gave_up and 0 < soon <= 1.3 * 3600,
          f"gave up {p.gave_up}, next try in {soon:.0f} s")
    s.clear()
    p.retry_at = 0.0
    saved = _settle_saves(d, lambda: s.bytes("late.txt") == b"late but saved\n"
                          and not d.pending("/personal/late.txt"), 20)
    told = [n["kind"] for n in d.notices.peek() if n.get("call") == "call-late"]
    r.pin("the server back after the give-up time: a late try saves it, told", saved and "saved" in told,
          f"server {s.bytes('late.txt')!r}; told {told}")

    d.set_call("call-landed")
    body = b"landed, answer lost\n"
    _new(d.fs, "/personal/landed.txt", body)

    def lands_then_lost(_body):
        s.put("landed.txt", body)
        raise s.Refused(502, b"<html>bad gateway</html>")

    s.on("write", "landed.txt", lands_then_lost)
    _fail_for_good(rt, d, "/personal/landed.txt")
    p = d.names.pending("/personal/landed.txt")
    p.retry_at = 0.0
    if hasattr(type(p), "kept_until"):
        p.kept_until = 0.0
    d.tick()
    _wait(lambda: not d.pending("/personal/landed.txt"), 10)
    told = [(n["kind"], n.get("reason", "")) for n in d.notices.peek() if n.get("call") == "call-landed"]
    r.pin("kept time over for a save that had landed: told saved, not gone",
          not d.pending("/personal/landed.txt") and told and told[-1][0] == "saved"
          and not any("are gone" in why for _, why in told), json.dumps(told)[:300])
    s.clear()

    d.set_call("call-newer")
    _new(d.fs, "/personal/newer.txt", b"version a\n")

    def a_lands_then_lost(_body):
        s.put("newer.txt", b"version a\n")
        raise s.Refused(502, b"<html>bad gateway</html>")

    s.on("write", "newer.txt", a_lands_then_lost)
    p = d.names.pending("/personal/newer.txt")
    p.retry_at = 0.0
    try:
        d.fs.saves.save(p, None)
    except OSError:
        pass
    s.on("write", "newer.txt", codeless)
    d.fs.truncate("/personal/newer.txt", 0)
    d.fs.write("/personal/newer.txt", b"version b, never sent\n", 0, 0)
    _fail_for_good(rt, d, "/personal/newer.txt")
    p = d.names.pending("/personal/newer.txt")
    p.retry_at = 0.0
    if hasattr(type(p), "kept_until"):
        p.kept_until = 0.0
    d.tick()
    _wait(lambda: not d.pending("/personal/newer.txt"), 10)
    told = [(n["kind"], n.get("reason", "")) for n in d.notices.peek() if n.get("call") == "call-newer"]
    r.pin("kept time over after an older save landed: the newer edit is told gone, not saved",
          told and told[-1][0] == "lost", json.dumps(told)[-300:])
    s.clear()

    d.set_call("call-rm")
    _new(d.fs, "/personal/rm.bin", os.urandom(3 << 20))
    s.on("upload_begin", "*", codeless)
    _fail_for_good(rt, d, "/personal/rm.bin")
    records = lambda: [n for n in os.listdir(d.journal.root) if not n.startswith(".")]  # noqa: E731
    held = records()
    try:
        d.fs.unlink("/personal/rm.bin")
    except OSError:
        pass
    r.pin("removing a file failed for good frees its journal bytes at once",
          bool(held) and not records() and not d.pending("/personal/rm.bin"), f"before {held}, after {records()}")
    s.clear()


@case("races")
def a_refused_delete_keeps_the_edit_not_yet_saved(r: Results, rt: Runtime) -> None:
    """A file stored on the server, edited here and not yet saved, then
    removed while the server refuses the delete: the remove fails, and the
    edit is still the file's, to be saved."""
    s = server(rt)
    s.put("kept.txt", b"stored\n")
    d = Daemon(rt, s)
    d.fs.truncate("/personal/kept.txt", 0)
    d.fs.write("/personal/kept.txt", b"edited, not saved\n", 0, 0)
    s.on("trash", "kept.txt", lambda: s.refuse("forbidden"))
    removed = _err(d.fs.unlink, "/personal/kept.txt")
    s.clear()
    shown = d.pending_bytes("/personal/kept.txt")
    r.pin("the refused delete fails", removed != "ok", str(removed))
    r.pin("the edit is still the file's", shown == b"edited, not saved\n", repr(shown))
    d.age()
    _settle_saves(d, lambda: s.bytes("kept.txt") == b"edited, not saved\n", 10)
    r.pin("and is saved", s.bytes("kept.txt") == b"edited, not saved\n", repr(s.bytes("kept.txt")))


@case("races")
def a_large_save_hands_the_cache_only_the_bytes_it_saved(r: Results, rt: Runtime) -> None:
    """A large save lands while the file is written again, which is then let
    go (replaced) before the saver looks:
    the cache must not take the journal's bytes, by then the newer ones,
    under the saved version's digest."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    first = os.urandom(3 << 20)
    _new(d.fs, "/personal/swap.bin", first)
    sha = hashlib.sha256(first).hexdigest()
    saves = d.fs.saves
    real = saves._commit

    seen = {}

    def commit_then_written_and_let_go(p, *a, **k):
        d.fs.write("/personal/swap.bin", b"N" * 4096, 0, 0)  # written while the save landed
        real(p, *a, **k)
        seen["open"] = p.open
        d.names.let_go(p)  # then let go (replaced), before the save looks

    saves._commit = commit_then_written_and_let_go
    p = d.names.pending("/personal/swap.bin")
    p.retry_at = 0.0
    try:
        saves.save(p, None)
    except OSError:
        pass
    saves._commit = real
    held = None
    if d.store.has(sha):
        with open(d.store._path(sha), "rb") as f:
            held = hashlib.sha256(f.read()).hexdigest()
    r.pin("the cache holds no other bytes under the saved digest", held in (None, sha), f"held {held}; {seen}")


@case("races")
def cleanup_with_the_server_stalled_never_holds_up_saves(r: Results, rt: Runtime) -> None:
    """An upload end tried again while the server stalls, and a file made by
    a renamed save whose take-back failed once: the saver's turn still
    queues a due save at once, and the take-back is tried again until the
    file goes."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    saves = d.fs.saves
    ending = rt.saves._Ending(d.space, "stalled-upload", "x.bin", "0" * 64, False)

    def stalls():
        time.sleep(3)
        raise OSError(errno.ETIMEDOUT, "stalled")

    s.on("upload_cancel", "stalled-upload", stalls)
    saves._ending.append(ending)
    saves._adopted_at = 0.0
    _new(d.fs, "/personal/due.txt", b"due now\n")
    d.age()
    started = time.monotonic()
    d.tick()
    took = time.monotonic() - started
    r.pin("a stalled upload end does not hold up the saver's turn", took < 1.0, f"turn took {took:.2f}s")
    landed = _wait(lambda: s.bytes("due.txt") == b"due now\n", 5)
    r.pin("and the due save goes", landed)
    s.clear()

    s.put("left.bin", b"made by a renamed save\n")
    made = s.entry("personal", "left.bin")
    failed = {"n": 0}

    def cut_once():
        if not failed["n"]:
            failed["n"] += 1
            raise OSError(errno.ECONNRESET, "cut")

    s.on("trash", "left.bin", cut_once)
    saves._take_back(d.space, "left.bin", made["sha256"], {"entry": {"id": made["id"]}})

    def gone() -> bool:
        saves._adopted_at = 0.0
        d.tick()
        return not s.has("left.bin")

    r.pin("a take-back that failed once is tried again until the file goes", _wait(gone, 10) and failed["n"] == 1,
          f"still there: {s.has('left.bin')}")
    s.clear()


@case("races")
def an_unsaved_notice_goes_to_the_command_that_wrote_last(r: Results, rt: Runtime) -> None:
    """Command A writes a file; while its save is on the way, command B
    writes it too, and the save gets no answer: B is told, and once the
    file is saved, both are told it is."""
    s = server(rt)
    d = Daemon(rt, s)
    d.set_call("call-a")
    _new(d.fs, "/personal/two.txt", b"from a\n")

    def b_writes_then_lost(_body):
        d.set_call("call-b")
        d.fs.write("/personal/two.txt", b"from b\n", 0, 0)
        raise OSError(errno.ECONNRESET, "lost")

    s.on("write", "two.txt", b_writes_then_lost)
    try:
        d.save("/personal/two.txt")
    except OSError:
        pass
    s.clear()
    told = [(n["kind"], n.get("call")) for n in d.notices.peek()]
    saved = _settle_saves(d, lambda: s.bytes("two.txt") == b"from b\n")
    after = [(n["kind"], n.get("call")) for n in d.notices.peek()]
    r.pin("the command that wrote last is told it is not saved yet", ("unsaved", "call-b") in told, str(told))
    r.pin("each command told is told it is saved", saved and {c for k, c in after if k == "saved"}
          >= {c for k, c in after if k == "unsaved"}, str(after))


@case("races")
def a_rename_whose_upload_end_went_unanswered_still_ends_it(r: Results, rt: Runtime) -> None:
    """A large file's save went unanswered and the file is renamed; the
    cancel of its upload gets no answer either. The daemon asks again until
    the upload is ended, and nothing lands at the old name."""
    s = _large_server(rt)
    d = Daemon(rt, s)
    big = os.urandom(3 << 20)
    _new(d.fs, "/personal/was.bin", big)

    def lost(*_):
        raise OSError(errno.ECONNRESET, "the answer was lost")

    s.on("upload_complete", "*", lost)
    try:
        d.save("/personal/was.bin")
    except OSError:
        pass
    s.clear()
    s.on("upload_cancel", "*", lost)
    renamed = _err(d.fs.rename, "/personal/was.bin", "/personal/now.bin")
    open_after = [u.state for u in s.drive.uploads.values() if u.path == "was.bin"]
    s.clear()
    d.fs.saves._adopted_at = 0.0
    d.tick()
    ended = [u.state for u in s.drive.uploads.values() if u.path == "was.bin"]
    saved = _settle_saves(d, lambda: s.bytes("now.bin") == big)
    r.pin("the rename succeeds", renamed == "ok", renamed)
    r.pin("the upload its cancel did not reach is ended later", open_after == ["uploading"] and ended == ["aborted"],
          f"{open_after} -> {ended}")
    r.pin("nothing lands at the old name, and the file is at its new one", saved and not s.has("was.bin"))


@case("races")
def an_append_while_a_reader_holds_the_old_version(r: Results, rt: Runtime) -> None:
    """A large file replaced elsewhere while something here reads it: the
    path still shows the old version, so an append made meanwhile is an
    edit of that one. It never overwrites the newer version with the old
    bytes; the server refuses it as changed, and the command is told."""
    s = server(rt)
    v1, v2 = os.urandom(6 << 20), os.urandom(7 << 20)
    s.put("held.bin", v1)
    d = Daemon(rt, s)
    fs = d.fs
    first = fs.read("/personal/held.bin", 1 << 20, 0, 0)
    s.put("held.bin", v2)
    d.deliver("held.bin")
    shown = fs.getattr("/personal/held.bin")["st_size"]
    d.set_call("call-append-held")
    fs.write("/personal/held.bin", b"tail\n", shown, 0)
    try:
        d.save("/personal/held.bin")
    except OSError:
        pass
    filed = s.drive.outcomes.get("call-append-held") or []
    r.pin("a reader partway on the old version still sees it", first == v1[: 1 << 20] and shown == len(v1),
          f"shown {shown}")
    r.pin("the newer version on the server is never overwritten from the old one", s.bytes("held.bin") == v2,
          f"server holds {len(s.bytes('held.bin') or b'')} bytes")
    r.pin("the append is refused as changed and filed for its command",
          any(not o["ok"] and o.get("code") == "changed" for o in filed), str(filed))


def _for_seconds(seconds: float, during, background) -> None:
    stop = threading.Event()

    err: list[BaseException] = []
    runs = [0]

    def loop() -> None:
        i = 0
        while not stop.is_set():
            try:
                background(i)
            except BaseException as exc:
                err.append(exc)
                return
            i += 1
            runs[0] = i

    worker = threading.Thread(target=loop)
    worker.start()
    try:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            during()
    finally:
        stop.set()
        worker.join()
    if err:
        raise err[0]
    if not runs[0]:
        raise AssertionError("background worker never ran")


def _ignore_while_feed_changes(d: Daemon) -> tuple[bool, str]:
    broke: list[str] = []

    def read() -> None:
        try:
            d.fs.getattr("/personal/.ignore")
        except RuntimeError as exc:
            broke.append(str(exc))

    _for_seconds(3, read, lambda i: d.put_entry(
        d.entry(f"x11-{i}", f"faults/x11/{i}.dat", "file", 1, "0" * 64, False, 0, 10**9 + i)))
    return not broke, f"{len(broke)} failures: {broke[:1]}"


def _rmdir_while_pending_churns(d: Daemon, s) -> tuple[bool, str]:
    broke: list[str] = []
    s.on("trash", "faults/e11", lambda: {"cursor": 0})
    folder = d.entry("e11", "faults/e11", "folder", 0, None, False, 0, 1)

    def rmdir() -> None:
        d.put_entry(folder)
        try:
            d.fs.rmdir("/personal/faults/e11")
        except RuntimeError as exc:
            broke.append(str(exc))
        except OSError:
            pass

    _for_seconds(3, rmdir, lambda i: d.churn_pending(f"/personal/zz11/{i}"))
    s.clear()
    return not broke, f"{len(broke)} failures: {broke[:1]}"


def _collect_while_text_arrives(d: Daemon) -> tuple[bool, str]:
    sync = d.sync()
    lost = 0
    stop = threading.Event()

    err: list[BaseException] = []
    passes = [0]

    def collect() -> None:
        while not stop.is_set():
            try:
                sync.collect()
            except BaseException as exc:
                err.append(exc)
                return
            passes[0] += 1

    worker = threading.Thread(target=collect)
    worker.start()
    try:
        for i in range(2000):
            data = f"fresh text {i}\n".encode()
            sha = hashlib.sha256(data).hexdigest()
            d.store.put(sha, data)
            d.put_entry(d.entry(f"c11-{i}", f"faults/c11/{i}.md", "file", len(data), sha, True, 0, 2 * 10**9 + i))
            lost += not d.store.has(sha)
    finally:
        stop.set()
        worker.join()
    if err:
        raise err[0]
    if not passes[0]:
        raise AssertionError("collect never ran")
    return lost == 0, f"{lost} of 2000 collected"
