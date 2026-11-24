"""A write onto a disk that fills part way: the program is told the bytes
that went down, then the disk's own error, never a count it did not get.
Notices batched under the server's cap on a body, and names no Drive path
can hold refused before any request."""

from __future__ import annotations

import errno
import os
import threading
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.livefs_runtime import drive_writes
from ptc_agent.core.sandbox.livefs_runtime.drive_writes import Pending, Writes


def _full_after(monkeypatch, room: int) -> None:
    """A disk with ``room`` bytes left: a write past it lands short, then fails."""
    real = os.pwrite
    left = {"n": room}

    def pwrite(fd, data, offset):
        if not left["n"]:
            raise OSError(errno.ENOSPC, "No space left on device")
        n = real(fd, bytes(data[: left["n"]]), offset)
        left["n"] -= n
        return n

    monkeypatch.setattr(drive_writes.os, "pwrite", pwrite)


def test_a_short_write_reports_what_landed_then_the_disk_error(tmp_path, monkeypatch):
    fd = os.open(tmp_path / "f", os.O_RDWR | os.O_CREAT, 0o600)
    p = Pending(SimpleNamespace(ref="personal", name="personal"), "a.bin", fd, 0, None, "c", None, "r1")
    writes = Writes(None, None, None, None)
    _full_after(monkeypatch, 10)
    assert writes.write(p, b"x" * 64, 0, "c", 1 << 30) == 10
    assert p.size == 10
    with pytest.raises(OSError) as raised:
        writes.write(p, b"x" * 54, 10, "c", 1 << 30)
    assert raised.value.errno == errno.ENOSPC
    assert p.size == 10


def test_notices_go_in_bodies_under_the_servers_cap(tmp_path, monkeypatch):
    """Many long notices are sent in several requests, each under the cap on
    a JSON body, and every one reaches the server."""
    import json

    from ptc_agent.core.sandbox.livefs_runtime import drive_notices

    notices = drive_notices.Notices(str(tmp_path / "notices.jsonl"))
    for i in range(400):
        notices.add("unsaved", "c", "personal", f"dir/{'x' * 3000}/{i}", "the Drive's server did not answer")
    sent: list[int] = []
    stop = threading.Event()

    class Client:
        def notices(self, batch):
            sent.append(len(json.dumps({"notices": batch})))
            if len(notices) == len(batch):
                stop.set()
            return {}

    monkeypatch.setattr(drive_notices, "SEND_EVERY_S", 0.01)
    drive_notices.deliver(notices, Client(), threading.Event(), stop, {"sent": 0, "errors": 0})
    assert len(sent) > 1 and max(sent) < 1 << 20
    assert len(notices) == 0


def test_a_name_that_is_not_utf8_is_refused_before_the_server(tmp_path):
    from ptc_agent.core.sandbox.livefs_runtime.drive_names import Names
    from ptc_agent.core.sandbox.livefs_runtime.drive_tree import Space

    names = Names([Space("personal", "personal", "s1")], threading.Lock())
    with pytest.raises(OSError) as raised:
        names.resolve("/personal/bad-\udcff.txt")
    assert raised.value.errno == errno.EINVAL
    assert names.resolve("/personal/日本.txt")[1] == "日本.txt"


def _pending(tmp_path) -> Pending:
    fd = os.open(tmp_path / "g", os.O_RDWR | os.O_CREAT, 0o600)
    return Pending(SimpleNamespace(ref="personal", name="personal"), "a.bin", fd, 0, None, "call-a", None, "r1")


def test_each_command_that_wrote_is_told_and_its_call_is_not_taken_back(tmp_path):
    """A save sent for command A gets no answer after command B wrote: B is
    told, the save's older call does not replace B's, and once saved both
    are told."""
    p = _pending(tmp_path)
    p.changed_now(5, "call-a")
    assert p.tell_unsaved().call == "call-a"
    p.changed_now(9, "call-b")
    p.unanswered(None, None, "call-a")
    assert p.call == "call-b"
    assert p.tell_unsaved().call == "call-b"
    assert p.tell_unsaved() is None
    assert sorted(t.call for t in p.accepted("s", p.rev, "call-b")) == ["call-a", "call-b"]


def test_tries_back_off_to_a_quarter_hour_then_go_hourly_while_kept(tmp_path, monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_writes.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(drive_writes.random, "uniform", lambda a, b: 1.0)
    p = _pending(tmp_path)
    p.changed_now(5, "call-a")
    for _ in range(30):
        p.unanswered(None, None, None)
    assert p.retry_at - clock["t"] == drive_writes.RETRY_MAX_S and not p.gave_up
    clock["t"] += drive_writes.GIVE_UP_S
    assert p.unanswered(None, None, None), "this try failed it for good"
    kept = 1000.0 + drive_writes.GIVE_UP_S + drive_writes.KEPT_S
    assert p.gave_up and p.kept_until == kept
    assert p.retry_at == clock["t"] + drive_writes.LATE_TRY_S, "still tried, hourly"
    clock["t"] = kept - 60
    assert not p.unanswered(None, None, None), "told once"
    assert p.retry_at == kept, "never past the time its bytes go"
    p.changed_now(6, "call-c")
    assert not p.gave_up and p.retry_at == 0.0 and p.kept_until == 0.0, "a write tries again"


def test_a_record_whose_description_cannot_be_read_now_is_kept(tmp_path):
    """Taking up a dead daemon's record, a description that fails to read
    for a reason other than its absence leaves the record for a later look:
    its data may be the only copy of an edit."""
    from ptc_agent.core.sandbox.livefs_runtime.drive_journal import Journal

    journal = Journal(str(tmp_path / "journal"))
    record = journal.new_id()
    fd = journal.open(record)
    os.pwrite(fd, b"unsaved edit", 0)
    journal.describe(record, "personal", "a.txt", None, [], "c")
    os.close(fd)  # its daemon died
    meta = os.path.join(journal.root, record + ".json")
    os.replace(meta, meta + ".kept")
    os.mkdir(meta)  # reading it fails, and not for its absence
    assert journal.claim() == []
    assert os.path.exists(os.path.join(journal.root, record + ".data"))
    os.rmdir(meta)
    os.replace(meta + ".kept", meta)
    ((found, fd2),) = journal.claim()
    assert found.rel == "a.txt" and os.pread(fd2, 64, 0) == b"unsaved edit"
    os.close(fd2)


def _half_writes(monkeypatch, module) -> None:
    """A disk that takes at most half of each write, as a pwrite may."""
    real = os.pwrite

    def pwrite(fd, data, offset):
        data = bytes(data)
        return real(fd, data[: max(1, len(data) // 2)], offset)

    monkeypatch.setattr(module.os, "pwrite", pwrite)


def test_a_large_head_copied_short_is_copied_whole(tmp_path, monkeypatch):
    from ptc_agent.core.sandbox.livefs_runtime import drive_disk, drive_reads

    data = os.urandom(3 * drive_reads.COPY_CHUNK + 5)
    fake = SimpleNamespace(read_version=lambda space, entry, n, at: data[at : at + n])
    entry = SimpleNamespace(size=len(data))
    fd = os.open(tmp_path / "f", os.O_RDWR | os.O_CREAT, 0o600)
    _half_writes(monkeypatch, drive_disk)
    assert drive_reads.Reads.copy_into(fake, None, entry, fd, lambda n: 0) == len(data)
    assert os.pread(fd, len(data) + 1, 0) == data


def test_a_small_head_copied_short_is_copied_whole(tmp_path, monkeypatch):
    from ptc_agent.core.sandbox.livefs_runtime import drive_disk
    from ptc_agent.core.sandbox.livefs_runtime.drive_journal import Journal

    journal = Journal(str(tmp_path / "journal"))
    names = SimpleNamespace(start_writing=lambda path, p: None)
    writes = Writes(names, journal, None, None)
    head = os.urandom(100_000)
    _half_writes(monkeypatch, drive_disk)
    p = writes.opened("/personal/a.bin", SimpleNamespace(ref="personal", name="personal"), "a.bin", "s", head, "c")
    assert os.pread(p.fd, len(head) + 1, 0) == head


def test_a_write_that_fits_is_taken_though_its_padding_does_not(tmp_path):
    """Room is asked a step ahead of the write; where the disk has room for
    the write but not the step, the write still goes and the edit stays."""
    fd = os.open(tmp_path / "f", os.O_RDWR | os.O_CREAT, 0o600)
    p = Pending(SimpleNamespace(ref="personal", name="personal"), "a.bin", fd, 0, None, "c", None, "r1")
    free = 8 << 20
    writes = Writes(None, None, None, None, shortfall=lambda n: max(0, n - free))
    data = os.urandom(1 << 20)
    assert writes.write(p, data, 0, "c", 1 << 30) == len(data)
    assert p.open and os.pread(fd, len(data) + 1, 0) == data
    assert p.room_to <= free, "room counted that the disk never gave"


def test_a_notice_queue_that_fails_part_way_through_a_read_is_left_whole(tmp_path, monkeypatch):
    from ptc_agent.core.sandbox.livefs_runtime import drive_notices

    notices = drive_notices.Notices(str(tmp_path / "notices.jsonl"))
    notices.add("unsaved", "c1", "personal", "a.txt")
    notices.add("lost", "c2", "personal", "b.txt")
    real = open

    class FailsAfterOneLine:
        def __init__(self, f):
            self.f = f

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.f.close()

        def __iter__(self):
            yield next(iter(self.f))
            raise OSError(errno.EIO, "I/O error")

    def flaky(path, mode="r", *a, **k):
        f = real(path, mode, *a, **k)
        return FailsAfterOneLine(f) if mode == "r" and path == notices.path else f

    monkeypatch.setattr(drive_notices, "open", flaky, raising=False)
    with pytest.raises(OSError):
        notices.add("saved", "c3", "personal", "c.txt")
    monkeypatch.undo()
    assert [n["call"] for n in notices.peek()] == ["c1", "c2"]
