"""Contracts for sync/restore driven by the sandbox-side transfer runtime.

The persistence service no longer touches file bytes on the blob path: the
sandbox scans and hashes itself, the registry says which digests already have
an object, and only the missing ones move, straight from the sandbox to the
store under a content-bound presigned PUT. These pin the decision table that
was verified live (direct, relay fallback, store rejection, storage off).
"""

from __future__ import annotations

import hashlib

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.workspace_file import datetime_to_micros, micros_to_datetime
from src.server.services.persistence import backup, blobs, resolve, restore
from src.server.services.persistence._rows import (
    _detect_is_binary,
    _mode_int,
    _ns_to_datetime,
    _row_base,
)
from src.server.services.persistence.transfer import ScanEntry, ScanResult


@pytest.fixture(autouse=True)
def _transfer_mode_from_provider():
    """The mode is read from the environment at import; pin it so a developer's
    .env cannot reroute these through the other path."""
    with (
        patch("src.utils.storage.BLOB_TRANSFER_MODE", "auto"),
        patch("src.utils.storage.STORAGE_PROVIDER", "s3"),
    ):
        yield


@pytest.fixture(autouse=True)
def _sync_lock():
    """Sync and restore serialize on a Postgres advisory lock; there is no DB here."""

    @asynccontextmanager
    async def _lock(_workspace_id):
        yield None

    with (
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(restore, "workspace_sync_lock", _lock),
    ):
        yield


WS = "ws-direct"
USER = "user-direct"
NS = 1_700_000_000_123_456_789
A = "a" * 64
B = "b" * 64


def _entry(path, sha=A, size=3, mode=0o644, kind="file", target=None, is_binary=False, mtime_ns=NS):
    return ScanEntry(path, kind, size, mtime_ns, mode, sha if kind == "file" else None, target, is_binary)


def _scan(*entries):
    return ScanResult(list(entries), [], [], hashed=len(entries), reused=0)


def _sandbox(provider="daytona"):
    sb = MagicMock()
    sb.working_dir = "/workspace"
    sb.config.sandbox.provider = provider
    sb.adownload_file_bytes = AsyncMock(return_value=b"abc")
    return sb


CLOCK = datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    """Every database and store seam, so each test states only what differs."""
    with (
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=CLOCK)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value={})) as meta,
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "delete_removed_files", new=AsyncMock(return_value=0)) as deleter,
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch.object(backup, "bulk_upsert_files", new=AsyncMock(side_effect=lambda ws, rows, conn=None: len(rows))) as upsert,
        patch.object(backup, "bulk_update_file_stamps", new=AsyncMock()) as mtimes,
        patch.object(backup, "is_storage_enabled", return_value=True),
        patch.object(backup, "workspace_owner", new=AsyncMock(return_value=USER)),
        # These pin the per-object path; the pack set has its own tests.
        patch.object(backup, "PACK_CUTOFF", -1),
        patch.object(blobs, "registered_blobs", new=AsyncMock(return_value=set())) as registered,
        patch.object(blobs, "register_blobs", new=AsyncMock()) as register,
        patch.object(blobs, "get_signed_upload_url", return_value=("https://put", {"h": "v"})) as presign,
        patch.object(blobs, "push_direct", new=AsyncMock(return_value={})) as push,
        patch.object(blobs, "store_blob", new=AsyncMock()) as store,
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=_scan())) as scan,
    ):
        yield {
            "meta": meta, "deleter": deleter, "upsert": upsert, "mtimes": mtimes,
            "registered": registered, "register": register, "presign": presign,
            "push": push, "store": store, "scan": scan,
        }


def _rows(db):
    return {r["file_path"]: r for r in db["upsert"].await_args.args[1]}


# --- timestamps ------------------------------------------------------------


def test_micros_round_trip_is_exact():
    """A float epoch loses the last microsecond on some values; integers do not."""
    dt = datetime(2026, 9, 3, 12, 41, 7, 536614, tzinfo=timezone.utc)
    micros = datetime_to_micros(dt)
    assert micros_to_datetime(micros) == dt
    assert _ns_to_datetime(micros * 1000 + 781) == dt


# --- sync: direct path -----------------------------------------------------


@pytest.mark.asyncio
async def test_new_files_go_direct_and_register_only_what_the_store_took(db):
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.bin", B, size=5, is_binary=True), _entry("dup.txt", A))
    db["push"].return_value = {A: {"status": "ok", "http": 200}, B: {"status": "ok", "http": 200}}

    result = await backup.sync_to_db(WS, _sandbox())

    # One presign and one push item per distinct digest, never per path.
    assert db["presign"].call_count == 2
    items = db["push"].await_args.args[1]
    assert sorted(i["sha256"] for i in items) == [A, B]
    assert all(i["url"] == "https://put" and i["headers"] == {"h": "v"} for i in items)
    assert sorted(db["register"].await_args.args[1]) == [(A, 3), (B, 5)]
    db["store"].assert_not_awaited()  # no bytes through this process
    rows = _rows(db)
    assert set(rows) == {"a.txt", "b.bin", "dup.txt"}
    assert rows["a.txt"]["blob_sha256"] == A and rows["a.txt"]["content_text"] is None
    assert rows["b.bin"]["is_binary"] is True and rows["a.txt"]["is_binary"] is False
    assert rows["a.txt"]["permissions"] == "0644"
    assert rows["a.txt"]["sandbox_modified_at"] == micros_to_datetime(NS // 1000)
    assert result == {"synced": 3, "skipped": 0, "deleted": 0, "errors": 0, "oversized": 0, "total_size": 0}


@pytest.mark.asyncio
async def test_registered_digest_skips_upload_entirely(db):
    """The registry is the proof of an object; a hit costs no bytes and no HTTP."""
    db["scan"].return_value = _scan(_entry("a.txt", A))
    db["registered"].return_value = {A}

    await backup.sync_to_db(WS, _sandbox())

    db["presign"].assert_not_called()
    db["push"].assert_not_awaited()
    db["register"].assert_not_awaited()
    assert _rows(db)["a.txt"]["blob_sha256"] == A


@pytest.mark.asyncio
async def test_store_rejection_withholds_the_row_and_counts_an_error(db):
    """A reached store that said no is final this pass; the old row survives."""
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    db["push"].return_value = {A: {"status": "failed", "http": 403}, B: {"status": "changed", "http": 400}}

    result = await backup.sync_to_db(WS, _sandbox())

    db["store"].assert_not_awaited()  # no relay after a rejection
    db["upsert"].assert_not_awaited()
    assert result["errors"] == 2 and result["synced"] == 0
    db["deleter"].assert_awaited_once()
    assert db["deleter"].await_args.args[1] == {"a.txt", "b.txt"}  # paths still active: never pruned


@pytest.mark.asyncio
async def test_unreachable_store_falls_back_to_relay_in_the_same_pass(db):
    db["scan"].return_value = _scan(_entry("a.txt", A))
    db["push"].return_value = {A: {"status": "unreachable"}}
    sb = _sandbox()
    sb.adownload_file_bytes = AsyncMock(return_value=b"\x00" * 3)  # sha256 differs from A

    with patch.object(blobs.hashlib, "sha256") as sha:
        sha.return_value.hexdigest.return_value = A
        result = await backup.sync_to_db(WS, sb)

    sb.adownload_file_bytes.assert_awaited_once_with("/workspace/a.txt")
    db["store"].assert_awaited_once_with(USER, A, b"\x00" * 3)
    assert _rows(db)["a.txt"]["blob_sha256"] == A
    assert result["errors"] == 0


@pytest.mark.asyncio
async def test_relay_mode_and_unpresignable_store_never_push(db):
    db["scan"].return_value = _scan(_entry("a.txt", A))
    for setup in ({"provider": "docker"}, {"presign": None}):
        db["push"].reset_mock()
        db["store"].reset_mock()
        db["presign"].return_value = ("https://put", {}) if "presign" not in setup else None
        sb = _sandbox(setup.get("provider", "daytona"))
        with patch.object(blobs.hashlib, "sha256") as sha:
            sha.return_value.hexdigest.return_value = A
            await backup.sync_to_db(WS, sb)
        db["push"].assert_not_awaited()
        db["store"].assert_awaited_once()


# --- sync: classification --------------------------------------------------


@pytest.mark.asyncio
async def test_unchanged_pointer_row_with_new_mode_is_refreshed_without_bytes(db):
    db["scan"].return_value = _scan(_entry("a.txt", A, mode=0o600))
    db["meta"].return_value = {
        "a.txt": {"kind": "file", "file_size": 3, "content_hash": A, "mtime_ns": NS, "permissions": "0644",
                  "blob_sha256": A, "is_binary": False, "mime_type": "text/plain"},
    }

    result = await backup.sync_to_db(WS, _sandbox())

    db["registered"].assert_not_awaited()
    db["push"].assert_not_awaited()
    row = _rows(db)["a.txt"]
    assert row["permissions"] == "0600" and row["blob_sha256"] == A
    assert result["skipped"] == 1 and result["synced"] == 1


@pytest.mark.asyncio
async def test_unchanged_inline_row_only_gets_its_mtime_refreshed(db):
    """An inline row cannot be upserted without the bytes it carries."""
    db["scan"].return_value = _scan(_entry("a.txt", A, mtime_ns=NS + 5_000_000))
    db["meta"].return_value = {
        "a.txt": {"kind": "file", "file_size": 3, "content_hash": A, "mtime_ns": NS, "permissions": "0644",
                  "blob_sha256": None},
    }

    await backup.sync_to_db(WS, _sandbox())

    db["upsert"].assert_not_awaited()
    db["mtimes"].assert_awaited_once()
    (path, when, perms), = db["mtimes"].await_args.args[1]
    assert path == "a.txt" and when == micros_to_datetime((NS + 5_000_000) // 1000)
    assert perms == "0644"


@pytest.mark.asyncio
async def test_unchanged_inline_row_persists_a_mode_only_change(db):
    """chmod +x on a file whose bytes did not move: the row has no pointer to
    refresh through an upsert, so the stamps update carries the new mode, or
    the file comes back from a recreated sandbox with its old one."""
    db["scan"].return_value = _scan(_entry("a.txt", A, mode=0o755))
    db["meta"].return_value = {
        "a.txt": {"kind": "file", "file_size": 3, "content_hash": A, "mtime_ns": NS, "permissions": "0644",
                  "blob_sha256": None},
    }

    await backup.sync_to_db(WS, _sandbox())

    db["upsert"].assert_not_awaited()
    (path, when, perms), = db["mtimes"].await_args.args[1]
    assert path == "a.txt" and when == micros_to_datetime(NS // 1000) and perms == "0755"


def test_a_zero_mode_is_recorded_and_restored_as_zero():
    """0000 is a real mode (a file made unreadable on purpose); only a symlink,
    whose mode is never applied, records none. Absent is what restore reads
    as the 0644 default, so zero must never collapse into it."""
    assert _row_base(_entry("locked", mode=0))["permissions"] == "0000"
    assert _row_base(_entry("l", kind="symlink", target="a", mode=0))["permissions"] is None
    assert _mode_int("0000", "file") == 0
    assert _mode_int(None, "file") == 0o644


def test_a_multibyte_character_split_at_the_scan_window_is_still_text():
    """The probe reads 8KiB of a longer file, so its end can land inside a
    character. Decoding that window on its own made the truncation look like
    invalid UTF-8, and ordinary CJK or accented prose was stored as bytes and
    dropped out of text search. A sequence that is invalid wherever it sits
    still reads as binary.
    """
    # The window ends one byte into the euro sign, which is three bytes long.
    straddles = b"a" * 8191 + "€".encode() + b"b" * 100
    assert _detect_is_binary("notes.txt", straddles) is False
    assert _detect_is_binary("notes.txt", b"a" * 100 + b"\xff\xfe" + b"b" * 9000) is True
    # The whole content decides, not the first window: a blob row is never
    # re-read on the way out.
    assert _detect_is_binary("notes.txt", b"a" * 9000 + b"\xff\xfe") is True
    assert _detect_is_binary("notes.txt", ("caf\u00e9 " * 400_000).encode("utf-8")) is False
    # A NUL is not sampled either: text goes to a column that cannot hold one.
    assert _detect_is_binary("notes.txt", b"a" * 70000 + b"\x00") is True


@pytest.mark.asyncio
async def test_same_microsecond_stamp_is_a_pure_skip(db):
    db["scan"].return_value = _scan(_entry("a.txt", A, mtime_ns=NS + 100))  # same microsecond
    db["meta"].return_value = {
        "a.txt": {"kind": "file", "file_size": 3, "content_hash": A, "mtime_ns": NS, "permissions": "0644",
                  "blob_sha256": A},
    }
    result = await backup.sync_to_db(WS, _sandbox())
    db["upsert"].assert_not_awaited()
    db["mtimes"].assert_not_awaited()
    assert result["skipped"] == 1


@pytest.mark.asyncio
async def test_directories_and_symlinks_become_rows_without_content(db):
    db["scan"].return_value = _scan(
        _entry("empty", kind="dir", mode=0o755),
        _entry("link", kind="symlink", mode=0, target="../t.txt"),
    )
    await backup.sync_to_db(WS, _sandbox())
    db["registered"].assert_not_awaited()
    rows = _rows(db)
    assert rows["empty"]["kind"] == "dir" and rows["empty"]["permissions"] == "0755"
    assert rows["empty"]["content_hash"] is None and rows["empty"]["blob_sha256"] is None
    assert rows["link"]["kind"] == "symlink" and rows["link"]["symlink_target"] == "../t.txt"
    assert rows["link"]["file_size"] == 0


@pytest.mark.asyncio
async def test_scan_read_errors_count_against_a_strict_backup(db):
    db["scan"].return_value = ScanResult([_entry("ok.txt", A)], [], [{"path": "bad", "error": "EACCES"}], 1, 0)
    db["push"].return_value = {A: {"status": "ok"}}
    result = await backup.sync_to_db(WS, _sandbox())
    assert result["errors"] == 1 and result["synced"] == 1


@pytest.mark.asyncio
async def test_storage_off_keeps_bytes_inline(db):
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.bin", B, is_binary=True))
    sb = _sandbox()
    sb.adownload_file_bytes = AsyncMock(side_effect=[b"text", b"\x00\x01"])
    with patch.object(backup, "is_storage_enabled", return_value=False):
        await backup.sync_to_db(WS, sb)
    db["registered"].assert_not_awaited()
    rows = _rows(db)
    assert rows["a.txt"]["content_text"] == "text" and rows["a.txt"]["blob_sha256"] is None
    assert rows["b.bin"]["content_binary"] == b"\x00\x01" and rows["b.bin"]["is_binary"] is True


@pytest.mark.asyncio
async def test_a_late_nul_is_stored_whole_with_no_store_to_fall_back_on(db):
    """The inline row has to reproduce its own digest, and a text column cannot.

    With no object store the row is the only copy, and the text column drops a
    NUL on the way in. A file whose NUL sits past any leading sniff would then
    be stored short of its digest, and restore, which verifies it, would refuse
    to place the file on every start from then on."""
    content = b"a" * 70000 + b"\x00"
    digest = hashlib.sha256(content).hexdigest()
    db["scan"].return_value = _scan(_entry("log.txt", digest, size=len(content)))
    sb = _sandbox()
    sb.adownload_file_bytes = AsyncMock(return_value=content)
    with patch.object(backup, "is_storage_enabled", return_value=False):
        await backup.sync_to_db(WS, sb)
    row = _rows(db)["log.txt"]
    assert row["is_binary"] is True
    assert row["content_text"] is None and row["content_binary"] == content
    assert row["content_hash"] == digest and row["file_size"] == len(content)


# --- restore ---------------------------------------------------------------


def _row(path, *, kind="file", blob=None, text=None, perms="0644", target=None, size=3):
    return {
        "file_path": path, "kind": kind, "blob_sha256": blob, "content_text": text, "content_binary": None,
        "is_binary": False, "permissions": perms, "symlink_target": target, "file_size": size,
        "content_hash": blob or (hashlib.sha256(text.encode()).hexdigest() if text else None),
        "sandbox_modified_at": micros_to_datetime(NS // 1000),
    }


@pytest.fixture
def restore_db():
    with (
        patch.object(restore, "get_files_for_workspace", new=AsyncMock(return_value=[])) as rows,
        patch.object(restore, "set_files_restore_incomplete", new=AsyncMock()) as flag,
        patch.object(restore, "workspace_owner", new=AsyncMock(return_value=USER)),
        patch.object(restore, "get_signed_url", return_value="https://get") as sign,
        patch.object(restore, "pull_direct", new=AsyncMock(return_value={})) as pull,
    ):
        yield {"rows": rows, "flag": flag, "sign": sign, "pull": pull}


@pytest.mark.asyncio
async def test_restore_pulls_pointer_rows_and_structure_in_one_runtime_call(restore_db):
    restore_db["rows"].return_value = [
        _row("a.txt", blob=A, perms="0600"),
        _row("d", kind="dir", perms="0755"),
        _row("l", kind="symlink", target="a.txt"),
        _row("old.txt", text="inline"),
    ]
    restore_db["pull"].side_effect = [
        {"a.txt": {"status": "ok"}, "d": {"status": "ok"}, "l": {"status": "ok"}},
        {"old.txt": {"status": "ok"}},  # the stamp after the relay upload
    ]
    sb = _sandbox()
    sb.acreate_directories = AsyncMock(return_value=True)
    sb.aupload_file_bytes = AsyncMock(return_value=True)

    result = await restore.restore_to_sandbox(WS, sb)

    first = restore_db["pull"].await_args_list[0]
    by_path = {i["path"]: i for i in first.args[1]}
    assert set(by_path) == {"a.txt", "d", "l"}
    assert by_path["a.txt"]["url"] == "https://get" and by_path["a.txt"]["mode"] == 0o600
    assert by_path["a.txt"]["mtime_ns"] == (NS // 1000) * 1000
    assert by_path["d"]["url"] is None and by_path["l"]["symlink_target"] == "a.txt"
    # A relay pass follows, so the first op leaves the directories open.
    assert first.kwargs == {"defer_dir_modes": True}
    # The legacy inline row went through the server to a staging name; the
    # placement op verifies and moves it, and closes the directory after it.
    uploads = [c.args[0] for c in sb.aupload_file_bytes.await_args_list]
    assert "/workspace/old.txt" not in uploads
    (staged,) = [p for p in uploads if p.startswith("/workspace/.wsfiles-relay-")]
    placement = restore_db["pull"].await_args_list[1].args[1]
    old = restore._pull_item(_row("old.txt", text="inline"), url=None)
    # Size and digest describe the bytes relayed, not the row: this fixture's
    # own file_size is stale, the shape a row written before file_size came
    # from the content itself is in.
    old.update({
        "file": staged.removeprefix("/workspace/"),
        "sha256": hashlib.sha256(b"inline").hexdigest(),
        "size": len(b"inline"),
    })
    assert placement == [old, restore._pull_item(_row("d", kind="dir", perms="0755"), url=None)]
    assert result == {"restored": 4, "errors": 0}
    assert [c.args for c in restore_db["flag"].await_args_list] == [(WS, True), (WS, False)]
    assert any(p.endswith(".file_sync_marker") for p in uploads)


@pytest.mark.asyncio
async def test_restore_unreachable_store_relays_and_a_mismatch_is_an_error(restore_db):
    restore_db["rows"].return_value = [_row("a.txt", blob=A), _row("b.txt", blob=B)]
    restore_db["pull"].side_effect = [
        {"a.txt": {"status": "unreachable"}, "b.txt": {"status": "unreachable"}},
        {"a.txt": {"status": "ok"}, "b.txt": {"status": "ok"}},
    ]
    sb = _sandbox()
    sb.acreate_directories = AsyncMock(return_value=True)
    sb.aupload_file_bytes = AsyncMock(return_value=True)
    with patch.object(resolve, "fetch_blob", new=AsyncMock(return_value=b"abc")) as fetch:
        result = await restore.restore_to_sandbox(WS, sb)
    assert fetch.await_count == 2
    assert result == {"restored": 2, "errors": 0}

    restore_db["pull"].side_effect = [{"a.txt": {"status": "mismatch"}, "b.txt": {"status": "ok"}}]
    result = await restore.restore_to_sandbox(WS, sb)
    assert result == {"restored": 1, "errors": 1}
    assert restore_db["flag"].await_args.args == (WS, True)


@pytest.mark.asyncio
async def test_restore_relay_mode_never_presigns(restore_db):
    restore_db["rows"].return_value = [_row("a.txt", blob=A)]
    restore_db["pull"].return_value = {"a.txt": {"status": "ok"}}
    sb = _sandbox("docker")
    sb.acreate_directories = AsyncMock(return_value=True)
    sb.aupload_file_bytes = AsyncMock(return_value=True)
    with patch.object(resolve, "fetch_blob", new=AsyncMock(return_value=b"abc")):
        result = await restore.restore_to_sandbox(WS, sb)
    restore_db["sign"].assert_not_called()
    assert result == {"restored": 1, "errors": 0}


# --- the sandbox exchange -----------------------------------------------------


def _exec_sandbox(*responses):
    """A sandbox whose runtime.exec yields ``responses`` in order."""
    sandbox = MagicMock()
    sandbox.working_dir = "/home/workspace"
    sandbox.runtime.exec = AsyncMock(
        side_effect=[MagicMock(stdout=out, exit_code=code) for out, code in responses]
    )
    sandbox.runtime.upload_file = AsyncMock()

    async def _call(func, *args, retry_policy=None, **kw):
        return await func(*args)

    sandbox._runtime_call = AsyncMock(side_effect=_call)
    return sandbox


@pytest.mark.asyncio
async def test_transfer_op_is_one_exec_with_the_result_on_stdout():
    from src.server.services.persistence import transfer

    marker = transfer.RESULT_MARKER
    sandbox = _exec_sandbox((f"noise\n{marker}" + '{"results":{"a":{"status":"ok"}}}\n', 0))
    out = await transfer.run_transfer_op(sandbox, "pull", {"root": "/home/workspace", "items": []}, timeout_s=30)
    assert out == {"results": {"a": {"status": "ok"}}}
    assert sandbox.runtime.exec.await_count == 1
    sandbox.runtime.upload_file.assert_not_awaited()
    cmd = sandbox.runtime.exec.await_args.args[0]
    assert " pull --spec-b64 " in cmd


@pytest.mark.asyncio
async def test_transfer_op_reships_a_missing_or_stale_runtime_once():
    from src.server.services.persistence import transfer

    marker = transfer.RESULT_MARKER
    sandbox = _exec_sandbox(
        ("python3: can't open file: No such file or directory\n", 2),
        (f"{marker}" + '{"entries":[],"errors":[],"oversized":[],"hashed":0,"reused":0}\n', 0),
    )
    out = await transfer.run_transfer_op(sandbox, "scan", {"root": "/home/workspace"}, timeout_s=30)
    assert out["entries"] == []
    assert sandbox.runtime.exec.await_count == 2
    sandbox.runtime.upload_file.assert_awaited_once()
    assert sandbox.runtime.upload_file.await_args.args[1].endswith("/_internal/src/wsfiles_transfer.py")


@pytest.mark.asyncio
async def test_transfer_op_large_spec_is_uploaded_then_run():
    from src.server.services.persistence import transfer

    marker = transfer.RESULT_MARKER
    sandbox = _exec_sandbox((f"{marker}" + '{"results":{}}\n', 0))
    items = [{"path": f"f{i}", "url": "https://s/" + "x" * 600} for i in range(400)]
    await transfer.run_transfer_op(sandbox, "pull", {"root": "/home/workspace", "items": items}, timeout_s=30)
    sandbox.runtime.upload_file.assert_awaited_once()
    assert "/_internal/.wsfiles/pull-" in sandbox.runtime.upload_file.await_args.args[1]
    assert "--spec-b64" not in sandbox.runtime.exec.await_args.args[0]


@pytest.mark.asyncio
async def test_transfer_op_without_a_result_line_is_a_runtime_error():
    from src.server.services.persistence import transfer

    sandbox = _exec_sandbox(("Traceback: boom\n", 1))
    with pytest.raises(transfer.TransferRuntimeError):
        await transfer.run_transfer_op(sandbox, "scan", {"root": "/home/workspace"}, timeout_s=30)
