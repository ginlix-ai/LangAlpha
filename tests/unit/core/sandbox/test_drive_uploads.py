"""A save in parts: parts sent again after a transient failure, parts the
server reports missing, a refusal read from the upload's end, and a file
that changes, or is renamed, while its parts go out."""

from __future__ import annotations

import errno
import hashlib
import json
import os
from collections import Counter
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.livefs_runtime import drive_saves, drive_uploads
from ptc_agent.core.sandbox.livefs_runtime.drive_client import Refused
from ptc_agent.core.sandbox.livefs_runtime.drive_uploads import Abandoned, Changed, Restart, Stuck, Upload, Uploads
from ptc_agent.core.sandbox.livefs_runtime.drive_writes import Pending

PART = 1024


class FakeClient:
    def __init__(self, data: bytes, mode: str = "relay") -> None:
        self.data = data
        self.mode = mode
        self.parts: dict[int, bytes] = {}
        self.fail_part: dict[int, int] = {}
        self.missing_once: list[int] = []
        self.end: dict = {"state": "done", "entry": {"sha256": hashlib.sha256(data).hexdigest()}, "cursor": 9}
        self.cancelled = False
        self.statuses = 0

        self.held: list[int] = []
        #: Status answers (or errors) before ``end``; what a cancel answers.
        self.answers: list = []
        self.cancel_ends = True

    def upload_begin(self, space, path, size, sha, base, call) -> dict:
        count = -(-len(self.data) // PART)
        return {"mode": self.mode, "upload_id": "u1", "size": len(self.data), "part_size": PART,
                "part_count": count, "parts_held": [{"part_number": n} for n in self.held]}

    def upload_part(self, space, upload, n, body, md5) -> dict:
        if self.fail_part.get(n):
            self.fail_part[n] -= 1
            raise OSError(errno.ECONNRESET, "reset")
        self.parts[n] = body
        return {"part_number": n}

    def upload_sign(self, space, upload, parts) -> dict:
        return {"parts": [{"part_number": p["part_number"], "url": f"http://store/{p['part_number']}",
                           "headers": {}} for p in parts]}

    def upload_complete(self, space, upload) -> dict:
        if self.missing_once:
            missing, self.missing_once = self.missing_once, []
            for n in missing:
                self.parts.pop(n, None)
            raise Refused(409, json.dumps({"code": "parts_missing", "missing_parts": missing}).encode())
        return {"state": "verifying"}

    def upload_status(self, space, upload) -> dict:
        self.statuses += 1
        if self.answers:
            answer = self.answers.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return answer
        return self.end

    def upload_cancel(self, space, upload) -> bool:
        self.cancelled = True
        return self.cancel_ends

    def assembled(self) -> bytes:
        return b"".join(self.parts[n] for n in sorted(self.parts))


def _pending(tmp_path, data: bytes) -> Pending:
    path = tmp_path / "f"
    path.write_bytes(data)
    fd = os.open(path, os.O_RDONLY)
    space = SimpleNamespace(ref="personal", name="personal")
    return Pending(space, "big.bin", fd, len(data), None, "call-1", None, "r1")


def _send(client: FakeClient, p: Pending, counts: Counter | None = None, upload: Upload | None = None) -> dict:
    up = Uploads(client, counts if counts is not None else Counter())
    sha = hashlib.sha256(client.data).hexdigest()
    return up.send_large(p, p.rev, upload or Upload(), p.size, sha, "call-1", lambda send: send())


@pytest.fixture(autouse=True)
def _quick(monkeypatch):
    monkeypatch.setattr(drive_uploads.time, "sleep", lambda s: None)
    monkeypatch.setattr(drive_uploads, "POLL_S", 0.0)
    monkeypatch.setattr(drive_uploads, "POLL_FAILING_S", 0.0)


def test_parts_sent_again_after_failures(tmp_path):
    data = os.urandom(PART * 5 + 17)
    client = FakeClient(data)
    client.fail_part = {2: 2, 4: 1}
    client.missing_once = [3]
    p = _pending(tmp_path, data)
    out = _send(client, p)
    assert out["state"] == "done"
    assert client.assembled() == data
    assert client.statuses == 1


def test_held_parts_are_not_sent(tmp_path):
    data = os.urandom(PART * 4)
    client = FakeClient(data)
    client.parts = {1: data[:PART], 2: data[PART : 2 * PART]}
    p = _pending(tmp_path, data)
    sent = []
    real = client.upload_part
    client.upload_part = lambda *a: sent.append(a[2]) or real(*a)
    client.held = [1, 2]
    _send(client, p)
    assert sorted(sent) == [3, 4]
    assert client.assembled() == data


def test_direct_part_the_store_refuses_goes_through_the_server(tmp_path, monkeypatch):
    data = os.urandom(PART * 3)
    client = FakeClient(data, mode="direct")
    puts = Counter()

    def put(conns, url, headers, body):
        n = int(url.rsplit("/", 1)[1])
        puts[n] += 1
        if n == 2:
            return 0  # the store never answers for this one
        client.parts[n] = body
        return 200

    monkeypatch.setattr(drive_uploads, "put", put)
    counts = Counter()
    _send(client, _pending(tmp_path, data), counts)
    assert client.assembled() == data
    assert puts[2] == 2 and counts["upload_relay_bytes"] == PART


def test_a_refusal_at_the_end_is_the_save_refused(tmp_path):
    data = os.urandom(PART * 2)
    client = FakeClient(data)
    client.end = {"state": "failed", "error": {"code": "over_quota", "message": "full"}}
    with pytest.raises(Refused) as raised:
        _send(client, _pending(tmp_path, data))
    assert raised.value.code == "over_quota"


def test_a_file_changed_midway_cancels_the_upload(tmp_path):
    data = os.urandom(PART * 6)
    client = FakeClient(data)
    p = _pending(tmp_path, data)
    real = client.upload_part

    def part(*a):
        p.rev += 1  # written meanwhile
        return real(*a)

    client.upload_part = part
    with pytest.raises(Changed):
        _send(client, p)
    assert client.cancelled


def test_a_save_abandoned_midway_stops_and_cancels(tmp_path):
    data = os.urandom(PART * 40)
    client = FakeClient(data)
    p = _pending(tmp_path, data)
    upload = Upload()
    real = client.upload_part

    def part(*a):
        upload.abandoned.set()  # renamed or removed meanwhile
        return real(*a)

    client.upload_part = part
    with pytest.raises(Abandoned):
        _send(client, p, upload=upload)
    assert client.cancelled and len(client.parts) < 40


def _refused(status: int, code: str | None) -> Refused:
    body = json.dumps({"code": code, "message": code}).encode() if code else b"<html>bad gateway</html>"
    return Refused(status, body)


def test_an_upload_open_again_at_its_end_is_sent_what_it_lacks(tmp_path):
    """The server finds a part gone while it reads the file back and opens
    the upload again: the part goes again and the upload completes."""
    data = os.urandom(PART * 4)
    client = FakeClient(data)
    real = client.upload_status

    def status(space, upload):
        if client.statuses == 0:
            client.statuses += 1
            client.parts.pop(3)
            return {"state": "uploading", "parts_held": [{"part_number": n} for n in sorted(client.parts)]}
        return real(space, upload)

    client.upload_status = status
    counts = Counter()
    assert _send(client, _pending(tmp_path, data), counts)["state"] == "done"
    assert client.assembled() == data and counts["upload_reopened"] == 1


@pytest.mark.parametrize("code, raised", [("upload_ended", Restart), ("not_found", Restart), ("bad_part", Stuck),
                                          (None, OSError)])
def test_a_refusal_midway_never_drops_the_save(tmp_path, code, raised):
    """A part refused after the begin is not the save's outcome: an upload
    that ended begins again, anything else is kept and sent again."""
    data = os.urandom(PART * 3)
    client = FakeClient(data)

    def part(*a):
        raise _refused(409 if code else 404, code)

    client.upload_part = part
    with pytest.raises(raised) as caught:
        _send(client, _pending(tmp_path, data))
    if raised is not Restart:
        assert not isinstance(caught.value, Refused)
        assert drive_saves._unanswered(caught.value), "kept and sent again, never dropped"


def test_a_status_that_fails_is_read_again_and_a_removal_still_ends_the_upload(tmp_path):
    """A status read fails while the server reads the file back, and the
    file is removed meanwhile: the save still holds the upload, and ends it."""
    data = os.urandom(PART * 2)
    client = FakeClient(data)
    upload = Upload()
    client.answers = [OSError(errno.ECONNRESET, "reset"), _refused(502, None)]
    real = client.upload_status

    def status(space, up):
        if client.statuses == 1:
            upload.abandoned.set()  # rm while the status cannot be read
        return real(space, up)

    client.upload_status = status
    with pytest.raises(Abandoned):
        _send(client, _pending(tmp_path, data), upload=upload)
    assert client.cancelled


def test_refusals_are_only_the_drive_refusing_the_bytes():
    """Only the codes in the table drop the bytes; a codeless answer, a code
    not listed, a busy store and a full set of uploads are tried again."""
    assert drive_saves._unanswered(_refused(404, None))
    assert drive_saves._unanswered(_refused(400, "no_such_code"))
    assert drive_saves._unanswered(_refused(429, None))
    assert drive_saves._unanswered(_refused(503, "unavailable"))
    assert drive_saves._unanswered(_refused(409, "too_many_uploads"))
    assert drive_saves._unanswered(_refused(409, "changed")) is None
    for code in drive_saves.REFUSED:
        assert drive_saves._unanswered(_refused(409, code)) is None


@pytest.mark.parametrize("kind", ["unavailable", "mismatch"])
def test_an_upload_that_ended_failing_to_store_is_kept(tmp_path, kind):
    """The server ends the upload ``failed`` because the store failed, or
    the stored bytes were damaged on the way: not a refusal of the bytes."""
    data = os.urandom(PART * 2)
    client = FakeClient(data)
    client.end = {"state": "failed", "error": {"code": kind, "message": "store"}}
    with pytest.raises(Refused) as raised:
        _send(client, _pending(tmp_path, data))
    assert drive_saves._unanswered(raised.value), "kept and sent again"


def test_one_dropped_put_stays_straight_to_the_store(tmp_path, monkeypatch):
    """A PUT to the store dropped once: that part is signed and sent there
    again, and no part goes through the server."""
    data = os.urandom(PART * 6)
    client = FakeClient(data, mode="direct")
    dropped = []

    def put(conns, url, headers, body):
        n = int(url.rsplit("/", 1)[1])
        if n == 3 and not dropped:
            dropped.append(n)
            return 0
        client.parts[n] = body
        return 200

    monkeypatch.setattr(drive_uploads, "put", put)
    counts = Counter()
    _send(client, _pending(tmp_path, data), counts)
    assert client.assembled() == data
    assert counts["upload_relay_bytes"] == 0 and counts["upload_direct_bytes"] == len(data)


def test_a_begin_that_finds_its_upload_read_back_sends_nothing(tmp_path):
    """The same save sent before with no answer is being read back: the
    begin joins it, and the save polls its end without sending a part."""
    data = os.urandom(PART * 3)
    client = FakeClient(data)
    real = client.upload_begin
    client.upload_begin = lambda *a: {**real(*a), "state": "verifying", "resumed": True}
    client.upload_part = lambda *a: pytest.fail("a part was sent")
    client.upload_complete = lambda *a: pytest.fail("a joined upload was completed again")
    counts = Counter()
    assert _send(client, _pending(tmp_path, data), counts)["state"] == "done"
    assert counts["upload_joined"] == 1 and client.statuses == 1


@pytest.mark.parametrize("body", [b'{"code": []}', b'{"code": [1]}', b'{"code": {"a": 1}}', b'{"code": 7}'])
def test_a_code_that_is_not_a_string_is_no_drive_code(body):
    """A proxy's JSON with a non-string ``code`` is no handler's answer: an
    unanswered save, sent again, never a crash past the saver."""
    from ptc_agent.core.sandbox.livefs_runtime import drive_protocol

    refused = Refused(502, body)
    assert refused.code is None and drive_protocol.errno_for(502, body) == errno.EIO


def test_signing_that_is_down_sends_the_parts_through_the_server(tmp_path, monkeypatch):
    """Signing answered 503 (or by no Drive handler): the parts still go,
    through the server, as one the store will not take does."""
    data = os.urandom(PART * 3)
    client = FakeClient(data, mode="direct")

    def down(space, upload, parts):
        raise Refused(503, b'{"code": "unavailable"}')

    client.upload_sign = down
    monkeypatch.setattr(drive_uploads, "put", lambda *a: pytest.fail("nothing was signed"))
    counts = Counter()
    _send(client, _pending(tmp_path, data), counts)
    assert client.assembled() == data and counts["upload_relay_bytes"] == len(data)


@pytest.mark.parametrize("head", [b"e1 aa -5\n", b"e1 aa 99999999999\n", b"e1 aa 3\nabcX"])
def test_a_frame_with_a_bad_length_or_end_ends_the_stream(head):
    import io

    from ptc_agent.core.sandbox.livefs_runtime import drive_protocol

    good = drive_protocol.encode_frame("e2", "bb", b"later")
    assert list(drive_protocol.read_frames(io.BytesIO(head + good))) == []


def test_a_bulk_text_stream_cut_mid_chunk_just_ends(monkeypatch):
    import contextlib
    import http.client

    from ptc_agent.core.sandbox.livefs_runtime import drive_client

    def frames(resp):
        yield "e1", "aa", b"first"
        raise http.client.IncompleteRead(b"", 10)

    monkeypatch.setattr(drive_client, "read_frames", frames)
    client = object.__new__(drive_client.DriveClient)
    client.bulk = SimpleNamespace(stream=lambda *a, **k: contextlib.nullcontext(SimpleNamespace(status=200)))
    assert list(client.all_texts("personal")) == [("e1", "aa", b"first")]


@pytest.mark.parametrize("body", [b"[]", b"{}", b'{"spaces": 3}'])
def test_a_reply_not_shaped_as_one_is_an_io_error(body):
    from ptc_agent.core.sandbox.livefs_runtime import drive_client

    client = object.__new__(drive_client.DriveClient)
    client.quick = SimpleNamespace(request=lambda *a, **k: (200, {}, body))
    with pytest.raises(OSError):
        client.spaces()
