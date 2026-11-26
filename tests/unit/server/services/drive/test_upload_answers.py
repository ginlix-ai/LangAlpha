"""What the Drive answers where a client could misread it: an upload's
status, its refusals' statuses, the mount's bodies and its feature gate, a
path no encoding can hold, and a part too large to relay."""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.server.app import drive_mount
from src.server.services.drive.errors import http_status
from src.server.dependencies.principal import Principal
from src.server.database.drive.keys import normalize
from src.server.models.drive import UploadIn
from src.server.services import drive
from src.server.services.drive import DriveError, uploads
from src.server.services.livefs.tokens import LivefsIdentity


def test_a_path_with_a_lone_surrogate_is_refused_as_a_path():
    with pytest.raises(DriveError) as refused:
        normalize("notes/\ud800.md")
    assert refused.value.code == "invalid_path"


def test_too_many_uploads_is_not_read_as_throttling():
    assert http_status("too_many_uploads") == 409


def _row(state: str, entry_id: str | None) -> dict:
    return {
        "upload_id": str(uuid.uuid4()),
        "space_id": "s",
        "state": state,
        "mode": "direct",
        "path": "big.bin",
        "size": 1,
        "part_size": 1,
        "part_count": 1,
        "expires_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "entry_id": entry_id,
        "error": None,
    }


@pytest.mark.asyncio
async def test_a_done_upload_whose_file_is_gone_says_so(monkeypatch):
    monkeypatch.setattr(uploads.reads, "live_entry", AsyncMock(return_value=None))
    out = await uploads.status(_row("done", str(uuid.uuid4())))
    assert "entry" in out and out["entry"] is None


@pytest.mark.asyncio
async def test_an_upload_past_its_deadline_reads_as_ended_before_the_sweep(monkeypatch):
    held = AsyncMock(return_value=[])
    monkeypatch.setattr(uploads, "_parts_held", held)
    late = _row("uploading", None)
    out = await uploads.status(late)
    assert out["state"] == "aborted" and "parts_held" not in out
    held.assert_not_called()

    open_ = {**late, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
    out = await uploads.status(open_)
    assert out["state"] == "uploading" and out["parts_held"] == []


@pytest.fixture
def relay(monkeypatch):
    """A begin that reaches the point of opening an upload in parts."""
    from src.server.database.drive.records import Limits

    space = drive.Space(
        space_id=str(uuid.uuid4()),
        kind="personal",
        user_id="u",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    big = Limits(
        max_space_bytes=None,
        max_entries=10,
        max_trash_bytes=None,
        max_file_bytes=1 << 50,
        max_request_bytes=1,
    )
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=big))
    monkeypatch.setattr(drive, "room_for", AsyncMock())
    monkeypatch.setattr(uploads, "is_storage_enabled", lambda: True)
    monkeypatch.setattr(uploads, "multipart_enabled", lambda: True)
    opened = AsyncMock(side_effect=AssertionError("no upload is opened"))
    monkeypatch.setattr(uploads, "opened_upload", opened)
    return space


@pytest.mark.asyncio
async def test_a_file_too_large_to_relay_is_refused_at_begin(relay):
    actor = drive.Actor(kind="person", user_id="u", via="app")
    size = uploads.RELAY_PART_MAX_BYTES * 10_000 + 1
    with pytest.raises(DriveError) as refused:
        await uploads.begin(relay, actor, UploadIn(path="huge.bin", size=size), direct=False)
    assert refused.value.code == "too_large"


@pytest.fixture
def mount(monkeypatch):
    app = FastAPI()
    app.include_router(drive_mount.router)
    identity = LivefsIdentity(computer_id="c", user_id="u", root_dir="/root")
    app.dependency_overrides[drive_mount._caller] = lambda: identity
    return TestClient(app)


def test_notices_need_the_drive_on(mount, monkeypatch):
    monkeypatch.setattr(drive, "enabled", AsyncMock(return_value=False))
    filed = AsyncMock()
    monkeypatch.setattr(drive_mount.drive_notices, "file", filed)
    answer = mount.post("/api/v1/livefs/drive/notices", json={"notices": []})
    assert answer.status_code == 404
    filed.assert_not_called()


@pytest.mark.parametrize(
    ("since", "cursor", "sent"),
    [(7, 7, False), (7, 9, True)],
    ids=["a quiet poll", "a cursor that moved with no entry behind it"],
)
def test_the_mounts_feed_sends_usage_whenever_the_cursor_moved(mount, monkeypatch, since, cursor, sent):
    # A purge or a sweep's trim changes the usage ``statfs`` reads and no entry.
    usage = drive.Usage(
        bytes=1, trash_bytes=0, entries=1, max_bytes=None, max_entries=10,
        max_file_bytes=1, max_upload_bytes=1,
    )
    monkeypatch.setattr(drive_mount.drive_notices, "mark_seen", AsyncMock())
    monkeypatch.setattr(drive, "open", AsyncMock(return_value=SimpleNamespace(space_id="s")))
    monkeypatch.setattr(
        drive, "changes", AsyncMock(return_value=drive.Changes(entries=[], cursor=cursor, resync=False))
    )
    monkeypatch.setattr(drive, "usage", AsyncMock(return_value=usage))
    answer = mount.get(f"/api/v1/livefs/drive/changes?space=personal&since={since}").json()
    assert answer["cursor"] == cursor
    assert ("usage" in answer) is sent


# Fixed, not uuid4(): it feeds a parametrize id, which must match across the
# workers of a parallel run.
UPLOAD_ID = "7cda4efa-38b9-4118-b91f-c24b1952d9d0"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/api/v1/livefs/drive/notices"),
        ("post", "/api/v1/livefs/drive/texts?space=s"),
        ("post", "/api/v1/livefs/drive/uploads?space=s&path=a.bin"),
        ("post", f"/api/v1/livefs/drive/uploads/{UPLOAD_ID}/parts?space=s"),
    ],
)
def test_a_body_past_the_cap_is_refused_unread(mount, monkeypatch, method, path):
    monkeypatch.setattr(drive, "enabled", AsyncMock(return_value=True))
    monkeypatch.setattr(drive, "open", AsyncMock())
    monkeypatch.setattr(drive_mount.outcomes, "call_context", AsyncMock(return_value=None))
    monkeypatch.setattr(drive_mount.outcomes, "record", AsyncMock())
    body = b'{"x": "' + b"a" * (drive_mount._JSON_BODY_MAX + 1) + b'"}'
    answer = getattr(mount, method)(path, content=body)
    assert answer.status_code == 413
    assert answer.json()["code"] == "too_large"


@pytest.mark.parametrize(
    ("principal", "status"),
    [
        (Principal("u", "computer", scopes=("drive:write",), computer_id="c-1"), 403),
        (Principal("u", "session"), 200),
        (Principal("u", "cli", scopes=("drive:write",)), 200),
    ],
)
def test_only_the_person_clears_a_files_history(monkeypatch, principal, status):
    """Refused for a computer's token, not asked: an approval kind an older
    app does not know would break it."""
    from src.server.app import drive as routes

    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[routes.WritePrincipal.__metadata__[0].dependency] = lambda: principal
    entry = SimpleNamespace(entry_id=str(uuid.uuid4()), kind="file", path="f.bin")
    monkeypatch.setattr(drive, "open", AsyncMock())
    monkeypatch.setattr(drive, "stat", AsyncMock(return_value=entry))
    cleared = AsyncMock(return_value=(2, 10))
    monkeypatch.setattr(drive, "clear_history", cleared)
    asked = AsyncMock()
    monkeypatch.setattr(routes.cli_approval, "gate", asked)

    answer = TestClient(app).post("/api/v1/drive/s/versions/clear", json={"path": "f.bin"})

    assert answer.status_code == status
    asked.assert_not_called()
    if status == 403:
        assert answer.json()["detail"] == {"type": "forbidden", "message": routes.CLEAR_REFUSED}
        cleared.assert_not_called()


@pytest.mark.asyncio
async def test_the_heartbeat_outlives_a_failed_renewal(monkeypatch):
    monkeypatch.setattr(uploads, "_HEARTBEAT_S", 0.01)
    answers = iter([RuntimeError("database"), True, True])
    renewed_after_the_failure = asyncio.Event()

    async def renew(upload_id, token, lease):
        answer = next(answers, None)
        if answer is None:
            renewed_after_the_failure.set()
            return True
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(uploads.rows, "renew", renew)
    lease = uploads._Lease({"upload_id": "u", "lease_token": "t"})
    async with lease.kept():
        await asyncio.wait_for(renewed_after_the_failure.wait(), timeout=5)
        assert not lease.lost.is_set()


@pytest.mark.asyncio
async def test_a_lease_it_cannot_prove_it_holds_is_lost(monkeypatch):
    from datetime import timedelta

    monkeypatch.setattr(uploads, "_HEARTBEAT_S", 0.01)
    monkeypatch.setattr(uploads, "VERIFY_LEASE", timedelta(seconds=0.05))
    monkeypatch.setattr(uploads.rows, "renew", AsyncMock(side_effect=RuntimeError("database")))
    lease = uploads._Lease({"upload_id": "u", "lease_token": "t"})
    async with lease.kept():
        await asyncio.wait_for(lease.lost.wait(), timeout=2)


@pytest.mark.asyncio
async def test_a_held_parts_md5_is_given_only_where_the_store_says_it_is_one(monkeypatch):
    md5 = "9e107d9d372bb6826bd81d3542a419d6"
    held = [
        {"part_number": 1, "size": 1, "etag": f'"{md5}"'},
        {"part_number": 2, "size": 1, "etag": '"b54357faf0632cce46e942fa68356b38-2"'},
        {"part_number": 3, "size": 1, "etag": '"4c1fc0a3a4f0e4c4e0e8d84fa9a0f5f3e1b2a9d0"'},
        {"part_number": 4, "size": 1, "etag": f'"{md5.upper()}"'},
    ]
    monkeypatch.setattr(uploads, "list_uploaded_parts", lambda key, upload_id: held)
    parts = await uploads._parts_held({"object_key": "k", "store_upload_id": "u"})
    assert [p["md5"] for p in parts] == [md5, None, None, md5]


@pytest.mark.asyncio
async def test_a_relayed_part_keeps_its_bytes_held_until_the_store_call_returns(monkeypatch):
    """CR151: a cancelled request stops waiting, but the thread still holds the part."""
    import contextlib
    import threading

    held = {"n": 0}

    @contextlib.asynccontextmanager
    async def holding(size):
        held["n"] += size
        try:
            yield
        finally:
            held["n"] -= size

    entered, release = threading.Event(), threading.Event()

    def upload_part(key, store_id, *, part_number, data, content_md5):
        entered.set()
        release.wait(5)
        return "etag"

    row = {**_row("uploading", None), "object_key": "k", "store_upload_id": "s"}
    monkeypatch.setattr(uploads, "_open_row", AsyncMock(return_value=row))
    monkeypatch.setattr(uploads.drive, "holding", holding)
    monkeypatch.setattr(uploads, "upload_part", upload_part)

    async def read(cap):
        return b"x"

    sent = asyncio.create_task(uploads.relay_part(SimpleNamespace(), row["upload_id"], 1, None, read))
    assert await asyncio.to_thread(entered.wait, 5)
    sent.cancel()
    await asyncio.sleep(0.05)
    assert held["n"] == 1
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await sent
    assert held["n"] == 0


@pytest.mark.asyncio
async def test_a_read_back_whose_lease_is_lost_keeps_its_slot_until_the_read_stops(monkeypatch):
    """CR150: the verification slot is the thread's until it returns."""
    import threading

    stopped = threading.Event()

    def sha256_object(key, deadline_for=None, stop=None):
        (stop or threading.Event()).wait(2)
        stopped.set()
        return None

    monkeypatch.setattr(uploads, "_VERIFIES", asyncio.Semaphore(1))
    monkeypatch.setattr(uploads, "_HEARTBEAT_S", 0.01)
    monkeypatch.setattr(uploads, "_assemble", AsyncMock(return_value=True))
    monkeypatch.setattr(uploads.rows, "renew", AsyncMock(return_value=False))
    monkeypatch.setattr(uploads, "sha256_object", sha256_object)
    row = {**_row("verifying", None), "object_key": "k", "lease_token": "t", "assembled": True}

    await uploads._finish(row)
    assert stopped.is_set()
    assert not uploads._VERIFIES.locked()


@pytest.mark.parametrize(
    ("error", "resent"),
    [
        (drive.DriveBusy(), True),
        (DriveError("unavailable", "The file could not be stored now"), True),
        (DriveError("changed", "changed"), False),
        (DriveError("over_quota", "full"), False),
        (RuntimeError("boom"), True),
    ],
    ids=["busy", "store-unavailable", "changed", "over-quota", "crash"],
)
def test_a_save_the_daemon_sends_again_is_not_filed_as_refused(error, resent):
    """CR032: the daemon retries every ``unavailable``; only its refusals end a save."""
    assert (drive_mount._save_resent(error) is not None) is resent


class _NoteIds:
    """Redis as ``drive_notices`` uses it for the ids it has filed."""

    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True

    async def delete(self, *keys):
        self.keys.difference_update(keys)


def _notice(note_id: str, **fields) -> dict:
    return {"id": note_id, "kind": "saved", "space": "personal", "path": "a.txt", "call": "c1", **fields}


@pytest.mark.asyncio
async def test_a_notice_of_no_known_kind_is_skipped_and_the_rest_are_filed(monkeypatch):
    """CR040: an unhashable kind must not end the batch."""
    from src.server.services.livefs import drive_notices

    record = AsyncMock(return_value=True)
    monkeypatch.setattr(drive_notices.cache, "client", lambda: _NoteIds())
    monkeypatch.setattr(drive_notices.outcomes, "record", record)
    await drive_notices.file("comp", "/root", [_notice("n1", kind=[]), _notice("n2")])
    assert record.await_count == 1


@pytest.mark.asyncio
async def test_a_notice_whose_outcome_was_not_recorded_can_be_filed_again(monkeypatch):
    """CR041: the id is let go when the outcome did not land."""
    from src.server.services.livefs import drive_notices

    ids = _NoteIds()
    record = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(drive_notices.cache, "client", lambda: ids)
    monkeypatch.setattr(drive_notices.outcomes, "record", record)
    await drive_notices.file("comp", "/root", [_notice("n1")])
    await drive_notices.file("comp", "/root", [_notice("n1")])
    assert record.await_count == 2


def test_each_version_says_whether_it_is_text(monkeypatch):
    """A past version is shown by its own kind, not the head's."""
    from src.server.app import drive as routes
    from src.server.database.drive.records import Actor, Version

    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[routes.ReadPrincipal.__metadata__[0].dependency] = lambda: Principal("u", "session")
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    by = Actor(kind="person", user_id="u", via="app")

    def version(seq: int, is_text: bool) -> Version:
        return Version(seq, "ab" * 32, 3, "text/plain", when, None, None, by, is_text=is_text)

    monkeypatch.setattr(drive, "open", AsyncMock())
    entry = SimpleNamespace(entry_id="e", name="f", memo=None)
    listed = (entry, [version(2, False), version(1, True)], False, 0, 0)
    monkeypatch.setattr(drive, "versions", AsyncMock(return_value=listed))

    answer = TestClient(app).get("/api/v1/drive/s/versions", params={"path": "f"})

    assert answer.status_code == 200
    assert [v["is_text"] for v in answer.json()["versions"]] == [False, True]


@pytest.mark.parametrize("line", ["Check memo.md out and say why", None], ids=["a memo", "any other file"])
def test_a_save_of_held_bytes_is_filed_with_what_its_upload_would_have_said(mount, monkeypatch, line):
    """Bytes the space holds are saved at begin, with no upload to file the
    save's end, so the missing check-out is reported from here."""
    from src.server.database.drive.records import Entry

    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    entry = Entry(
        entry_id="e", space_id="s", path="memo.md", parent="", name="memo.md", kind="file",
        head_seq=2, sha256="ab" * 32, size=3, content_type="text/markdown", is_text=True,
        created_at=when, modified_at=when, changed_seq=5, deleted_at=None, trash_id=None,
        deleted_by=None, modified_by=None,
    )
    monkeypatch.setattr(drive, "open", AsyncMock(return_value=SimpleNamespace(space_id="s")))
    monkeypatch.setattr(drive, "cursor", AsyncMock(return_value=5))
    monkeypatch.setattr(drive_mount, "_reaches_store", AsyncMock(return_value=False))
    monkeypatch.setattr(drive_mount.outcomes, "call_context", AsyncMock(return_value=None))
    record = AsyncMock()
    monkeypatch.setattr(drive_mount.outcomes, "record", record)
    monkeypatch.setattr(uploads, "begin", AsyncMock(return_value={"mode": "held", "entry": entry}))
    unchecked = AsyncMock(return_value=line)
    monkeypatch.setattr(drive_mount.memo, "unchecked", unchecked)

    answer = mount.post(
        "/api/v1/livefs/drive/uploads?space=personal&path=memo.md",
        json={"size": 3, "sha256": "ab" * 32},
    )

    assert answer.status_code == 200 and answer.json()["mode"] == "held"
    shown = drive_mount.drive_notices.shown("/root", "personal", "memo.md")
    assert unchecked.await_args.args[:2] == (entry, shown)
    outcome = record.await_args.args[2]
    assert outcome == {"op": "write", "ok": True, "path": shown, "size": 3} | ({"report": line} if line else {})


@pytest.mark.parametrize(
    ("name", "held_text", "size", "reused"),
    [
        ("notes.md", True, 10, True),
        ("data.csv", True, 10, False),
        ("data.csv", False, 10, True),
        ("notes.md", False, 10, False),
        ("notes.md", False, 2 * 1024 * 1024, True),
    ],
    ids=["text-as-text", "text-as-data", "data-as-data", "unknown-small", "large-binary"],
)
@pytest.mark.asyncio
async def test_held_bytes_are_reused_only_under_a_name_that_keeps_their_kind(
    monkeypatch, name, held_text, size, reused
):
    """CR045: a write of the same bytes under ``name`` would keep the same
    text flag; otherwise the bytes are sent again."""
    from src.server.database.drive.records import Content, Limits

    limits = Limits(max_space_bytes=None, max_entries=10, max_trash_bytes=None,
                    max_file_bytes=1 << 40, max_request_bytes=50 << 20)
    held = Content(space_id="s", sha256="ab" * 32, size=size, is_text=held_text,
                   content_type="text/plain", blob_user_id=None)
    saved = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=limits))
    monkeypatch.setattr(uploads.reads, "content_by_sha", AsyncMock(return_value=held))
    monkeypatch.setattr(uploads, "is_storage_enabled", lambda: True)
    monkeypatch.setattr(uploads, "multipart_enabled", lambda: False)
    monkeypatch.setattr(drive, "save", saved)
    monkeypatch.setattr(drive, "room_for", AsyncMock())

    req = UploadIn(path=name, size=size, sha256="ab" * 32)
    out = await uploads.begin(SimpleNamespace(space_id="s", user_id="u"), SimpleNamespace(user_id="u"), req, direct=True)

    assert (out["mode"] == "held") is reused
    assert saved.await_count == (1 if reused else 0)


@pytest.mark.parametrize(("dropped", "woken"), [(1, True), (0, False)], ids=["cleared", "nothing"])
@pytest.mark.asyncio
async def test_a_clear_wakes_the_feed_only_when_it_dropped_a_version(monkeypatch, dropped, woken):
    import contextlib

    from src.server.services.drive import feed

    @contextlib.asynccontextmanager
    async def upkeep(space_id, limits):
        file = SimpleNamespace(kind="file", path="f", has_record=False)
        yield SimpleNamespace(file=AsyncMock(return_value=file))

    announced = AsyncMock()
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=None))
    monkeypatch.setattr(drive.writes, "upkeep", upkeep)
    monkeypatch.setattr(drive.retention, "clear", AsyncMock(return_value=(dropped, 0)))
    monkeypatch.setattr(feed, "announce", announced)
    space = SimpleNamespace(space_id="s")
    person = drive.Actor(kind="person", user_id="u", via="app")
    assert await drive.clear_history(space, str(uuid.UUID(int=1)), actor=person) == (dropped, 0)
    assert announced.await_count == (1 if woken else 0)


@pytest.mark.parametrize(
    ("pinned_now", "shown", "refused"),
    [(5, 5, False), (None, None, False), (None, 5, True), (6, 5, True), (5, None, True)],
    ids=["same", "none", "unpinned since", "moved since", "pinned since"],
)
@pytest.mark.asyncio
async def test_a_clear_is_bound_to_the_pinned_final_its_caller_was_shown(
    monkeypatch, pinned_now, shown, refused
):
    """The person was told that version stays: once its pin has moved the
    clear would take it, so it drops nothing."""
    import contextlib

    from src.server.services.drive import feed

    @contextlib.asynccontextmanager
    async def upkeep(space_id, limits):
        file = SimpleNamespace(
            kind="file", path="f", has_record=False, memo=SimpleNamespace(final_seq=pinned_now)
        )
        yield SimpleNamespace(file=AsyncMock(return_value=file))

    cleared = AsyncMock(return_value=(2, 0))
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=None))
    monkeypatch.setattr(drive.writes, "upkeep", upkeep)
    monkeypatch.setattr(drive.retention, "clear", cleared)
    monkeypatch.setattr(feed, "announce", AsyncMock())
    space = SimpleNamespace(space_id="s")
    person = drive.Actor(kind="person", user_id="u", via="app")
    call = drive.clear_history(space, str(uuid.UUID(int=1)), before=7, pinned=shown, actor=person)
    if refused:
        with pytest.raises(drive.DriveError) as err:
            await call
        assert err.value.code == "changed" and cleared.await_count == 0
    else:
        assert await call == (2, 0)
