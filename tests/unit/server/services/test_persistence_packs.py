"""Contracts for the pack set: small files stored as members of shared chunks.

Verified live first (2168 files into one 4.3 MB chunk; edit, restore, and
serve while stopped) against a real sandbox and bucket. These pin the
decisions: which files pack, when the set is rewritten, what a rejected
chunk does to the manifest, and how a restore or a read addresses a member.
"""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.persistence import backup, blobs, resolve, restore
from src.server.services.persistence.resolve import resolve_file_bytes
from src.server.services.persistence.transfer import PACK_CUTOFF, ScanEntry, ScanResult
from src.server.database.workspace_file import micros_to_datetime


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
    """``sync_to_db`` serializes on a Postgres advisory lock; there is no DB here."""

    @asynccontextmanager
    async def _lock(_workspace_id):
        yield None

    with patch.object(backup, "workspace_sync_lock", _lock):
        yield


WS = "ws-packs"
USER = "user-packs"
NS = 1_700_000_000_123_456_000
CLOCK = datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


A, B, BIG = b"aaa", b"bbbbb", b"x" * (PACK_CUTOFF + 1)
CHUNK = _sha(A + B)


def _entry(path, data, mode=0o644, mtime_ns=NS):
    return ScanEntry(path, "file", len(data), mtime_ns, mode, _sha(data), None, False)


def _scan(*entries):
    return ScanResult(entries=list(entries), oversized=[], errors=[], hashed=len(entries), reused=0)


def _sandbox(provider="daytona"):
    sb = MagicMock()
    sb.working_dir = "/workspace"
    sb.config.sandbox.provider = provider
    sb.adownload_file_bytes = AsyncMock(return_value=A + B)
    return sb


def _chunk(members):
    """What the pack op reports for one chunk holding ``members`` in path order."""
    out, offset = [], 0
    for path, data in sorted(members):
        out.append({"path": path, "offset": offset, "size": len(data), "sha256": _sha(data)})
        offset += len(data)
    data = b"".join(d for _, d in sorted(members))
    return {"path": f"_internal/packs/chunk-{_sha(data)}", "sha256": _sha(data), "size": len(data), "members": out}


def _packed_meta(path, data, chunk=CHUNK, offset=0, mode="0644"):
    return {
        "kind": "file", "file_size": len(data), "content_hash": _sha(data), "mtime_ns": NS,
        "permissions": mode, "pack_sha256": chunk, "pack_offset": offset, "blob_sha256": None, "is_binary": False,
    }


@pytest.fixture
def db():
    def _ok(sandbox, items):
        return {i["sha256"]: {"status": "ok"} for i in items}

    with (
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=CLOCK)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value={})) as meta,
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "delete_removed_files", new=AsyncMock(return_value=0)),
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch.object(backup, "bulk_upsert_files", new=AsyncMock(side_effect=lambda ws, rows, conn=None: len(rows))) as upsert,
        patch.object(backup, "bulk_update_file_stamps", new=AsyncMock()),
        patch.object(backup, "is_storage_enabled", return_value=True),
        patch.object(backup, "workspace_owner", new=AsyncMock(return_value=USER)),
        patch.object(blobs, "registered_blobs", new=AsyncMock(return_value=set())) as registered,
        patch.object(blobs, "register_blobs", new=AsyncMock()) as register,
        patch.object(blobs, "get_signed_upload_url", return_value=("https://put", {})),
        patch.object(blobs, "push_direct", new=AsyncMock(side_effect=_ok)) as push,
        patch.object(blobs, "pack_direct", new=AsyncMock(return_value={"chunks": [_chunk([("a.txt", A), ("b.txt", B)])], "changed": []})) as pack,
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=_scan(_entry("a.txt", A), _entry("b.txt", B), _entry("big.bin", BIG)))) as scan,
    ):
        yield {"meta": meta, "upsert": upsert, "registered": registered, "register": register, "push": push, "pack": pack, "scan": scan}


def _rows(db):
    return {r["file_path"]: r for r in db["upsert"].await_args.args[1]}


# --- sync ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_files_at_or_below_the_cutoff_pack_and_larger_ones_go_per_object(db):
    result = await backup.sync_to_db(WS, _sandbox())

    db["pack"].assert_awaited_once()
    members = db["pack"].await_args.args[1]
    assert members == [{"path": "a.txt", "sha256": _sha(A), "size": 3}, {"path": "b.txt", "sha256": _sha(B), "size": 5}]
    pushed = {i["path"]: i for c in db["push"].await_args_list for i in c.args[1]}
    assert pushed[f"_internal/packs/chunk-{CHUNK}"]["unlink"] is True
    assert pushed["big.bin"]["unlink"] is False
    rows = _rows(db)
    assert rows["a.txt"]["pack_sha256"] == CHUNK and rows["a.txt"]["pack_offset"] == 0
    assert rows["b.txt"]["pack_sha256"] == CHUNK and rows["b.txt"]["pack_offset"] == 3
    assert rows["a.txt"]["blob_sha256"] is None and rows["a.txt"]["content_text"] is None
    assert rows["a.txt"]["content_hash"] == _sha(A) and rows["a.txt"]["file_size"] == 3
    assert rows["big.bin"]["blob_sha256"] == _sha(BIG) and rows["big.bin"]["pack_sha256"] is None
    registered = {sha for c in db["register"].await_args_list for sha, _ in c.args[1]}
    assert registered == {CHUNK, _sha(BIG)}
    assert result["synced"] == 3 and result["errors"] == 0


@pytest.mark.asyncio
async def test_an_unchanged_pack_set_is_a_skip_without_the_pack_op(db):
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    result = await backup.sync_to_db(WS, _sandbox())
    db["pack"].assert_not_awaited()
    db["push"].assert_not_awaited()
    assert result["skipped"] == 2 and result["synced"] == 0


@pytest.mark.asyncio
async def test_a_moved_stamp_on_an_unchanged_member_refreshes_the_row_without_bytes(db):
    db["scan"].return_value = _scan(_entry("a.txt", A, mode=0o600), _entry("b.txt", B))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    result = await backup.sync_to_db(WS, _sandbox())
    db["pack"].assert_not_awaited()
    rows = _rows(db)
    assert set(rows) == {"a.txt"}
    assert rows["a.txt"]["permissions"] == "0600" and rows["a.txt"]["pack_sha256"] == CHUNK
    assert result["skipped"] == 2


@pytest.mark.asyncio
async def test_a_moved_stamp_is_left_unrecorded_while_pruning_is_withheld(db):
    """A restore that placed the file but failed its chmod leaves a moved
    stamp; recording it would make the wrong mode the one the retry restores."""
    backup.files_restore_incomplete.return_value = True
    db["scan"].return_value = _scan(_entry("a.txt", A, mode=0o600), _entry("b.txt", B))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    result = await backup.sync_to_db(WS, _sandbox())
    db["pack"].assert_not_awaited()
    db["upsert"].assert_not_awaited()
    assert result["skipped"] == 2


@pytest.mark.asyncio
async def test_one_changed_member_rewrites_the_whole_set(db):
    a2 = b"AAA"
    db["scan"].return_value = _scan(_entry("a.txt", a2), _entry("b.txt", B))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    db["pack"].return_value = {"chunks": [_chunk([("a.txt", a2), ("b.txt", B)])], "changed": []}
    await backup.sync_to_db(WS, _sandbox())
    assert [m["path"] for m in db["pack"].await_args.args[1]] == ["a.txt", "b.txt"]
    rows = _rows(db)
    assert rows["a.txt"]["pack_sha256"] == rows["b.txt"]["pack_sha256"] == _sha(a2 + B)


@pytest.mark.asyncio
async def test_a_member_that_left_rewrites_the_set(db):
    """The old chunk stays referenced by nothing, so the set must not keep pointing at it."""
    db["scan"].return_value = _scan(_entry("a.txt", A))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    db["pack"].return_value = {"chunks": [_chunk([("a.txt", A)])], "changed": []}
    await backup.sync_to_db(WS, _sandbox())
    assert [m["path"] for m in db["pack"].await_args.args[1]] == ["a.txt"]
    assert _rows(db)["a.txt"]["pack_sha256"] == _sha(A)


@pytest.mark.asyncio
async def test_a_member_absent_while_pruning_is_withheld_does_not_rewrite_the_set(db):
    """The flag keeps the absent file's row, and the file is expected back on
    the next restore. Repacking around it would repeat on every sync."""
    backup.files_restore_incomplete.return_value = True
    db["scan"].return_value = _scan(_entry("a.txt", A))
    db["meta"].return_value = {"a.txt": _packed_meta("a.txt", A), "b.txt": _packed_meta("b.txt", B, offset=3)}
    result = await backup.sync_to_db(WS, _sandbox())
    db["pack"].assert_not_awaited()
    db["push"].assert_not_awaited()
    assert result["skipped"] == 1 and result["synced"] == 0 and result["deleted"] == 0


@pytest.mark.asyncio
async def test_a_per_object_row_below_the_cutoff_joins_the_pack(db):
    """Rows stored per object before packs existed migrate on their next pass."""
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    db["meta"].return_value = {
        "a.txt": {**_packed_meta("a.txt", A), "pack_sha256": None, "pack_offset": None, "blob_sha256": _sha(A)},
        "b.txt": {**_packed_meta("b.txt", B), "pack_sha256": None, "pack_offset": None, "blob_sha256": _sha(B)},
    }
    await backup.sync_to_db(WS, _sandbox())
    db["pack"].assert_awaited_once()
    rows = _rows(db)
    assert rows["a.txt"]["pack_sha256"] == CHUNK and rows["a.txt"]["blob_sha256"] is None


@pytest.mark.asyncio
async def test_a_chunk_the_store_rejected_withholds_its_members_rows(db):
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B), _entry("big.bin", BIG))
    db["push"].side_effect = lambda sb, items: {
        i["sha256"]: {"status": "failed" if i["sha256"] == CHUNK else "ok"} for i in items
    }
    result = await backup.sync_to_db(WS, _sandbox())
    rows = _rows(db)
    assert set(rows) == {"big.bin"}
    assert result["errors"] == 2 and result["synced"] == 1


@pytest.mark.asyncio
async def test_members_that_changed_during_packing_count_as_errors(db):
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    db["pack"].return_value = {"chunks": [_chunk([("a.txt", A)])], "changed": ["b.txt"]}
    result = await backup.sync_to_db(WS, _sandbox())
    assert set(_rows(db)) == {"a.txt"} and result["errors"] == 1


@pytest.mark.asyncio
async def test_storage_off_never_packs(db):
    db["scan"].return_value = _scan(_entry("a.txt", A))
    sb = _sandbox()
    sb.adownload_file_bytes = AsyncMock(return_value=A)
    with patch.object(backup, "is_storage_enabled", return_value=False):
        await backup.sync_to_db(WS, sb)
    db["pack"].assert_not_awaited()
    rows = _rows(db)
    assert rows["a.txt"]["content_text"] == "aaa" and rows["a.txt"]["pack_sha256"] is None


# --- restore ------------------------------------------------------------------


def _row(path, data, *, pack=None, offset=None, blob=None, perms="0644"):
    return {
        "file_path": path, "kind": "file", "blob_sha256": blob, "pack_sha256": pack, "pack_offset": offset,
        "content_text": None, "content_binary": None, "is_binary": False, "permissions": perms,
        "symlink_target": None, "file_size": len(data), "content_hash": _sha(data),
        "sandbox_modified_at": micros_to_datetime(NS // 1000),
    }


@asynccontextmanager
async def _no_lock(_workspace_id):
    yield None


@pytest.fixture
def restore_db():
    with (
        patch.object(restore, "get_files_for_workspace", new=AsyncMock(return_value=[
            _row("a.txt", A, pack=CHUNK, offset=0, perms="0600"),
            _row("b.txt", B, pack=CHUNK, offset=3),
            _row("big.bin", BIG, blob=_sha(BIG)),
        ])) as rows,
        patch.object(restore, "set_files_restore_incomplete", new=AsyncMock()) as flag,
        patch.object(restore, "workspace_owner", new=AsyncMock(return_value=USER)),
        patch.object(restore, "get_signed_url", side_effect=lambda key, exp: f"https://get/{key}") as sign,
        patch.object(restore, "pull_direct", new=AsyncMock(return_value={})) as pull,
        patch.object(restore, "workspace_sync_lock", _no_lock),
    ):
        yield {"rows": rows, "flag": flag, "sign": sign, "pull": pull}


def _restore_sandbox(provider="daytona"):
    sb = _sandbox(provider)
    sb.acreate_directories = AsyncMock(return_value=True)
    sb.aupload_file_bytes = AsyncMock(return_value=True)
    return sb


@pytest.mark.asyncio
async def test_restore_pulls_a_pack_as_one_item_with_its_members(restore_db):
    restore_db["pull"].return_value = {"a.txt": {"status": "ok"}, "b.txt": {"status": "ok"}, "big.bin": {"status": "ok"}}
    result = await restore.restore_to_sandbox(WS, _restore_sandbox())
    items = restore_db["pull"].await_args.args[1]
    packs = [i for i in items if i.get("kind") == "pack"]
    assert len(packs) == 1 and len(items) == 2
    pack = packs[0]
    assert pack["sha256"] == CHUNK and pack["url"] == f"https://get/blobs/{USER}/{CHUNK}" and pack["size"] == 8
    by_path = {m["path"]: m for m in pack["members"]}
    assert by_path["a.txt"] == {"path": "a.txt", "offset": 0, "size": 3, "sha256": _sha(A), "mode": 0o600, "mtime_ns": (NS // 1000) * 1000}
    assert by_path["b.txt"]["offset"] == 3
    assert restore_db["sign"].call_count == 2  # one per chunk, one per object
    assert result == {"restored": 3, "errors": 0}
    # Raised before the first transfer, cleared only after a clean finish.
    assert [c.args for c in restore_db["flag"].await_args_list] == [(WS, True), (WS, False)]


RELAY_CHUNK = f"/workspace/.wsfiles-relay-{CHUNK}"


def _pack_calls(pull):
    return [c.args[1] for c in pull.await_args_list if any(i.get("file") for i in c.args[1])]


@pytest.mark.asyncio
async def test_restore_relays_an_unreachable_pack_as_one_upload(restore_db):
    """Direct pull unreachable: the chunk is uploaded whole, the per-object row
    goes to a staging name, and one placement op slices and moves both."""
    restore_db["pull"].side_effect = [
        {"a.txt": {"status": "unreachable"}, "b.txt": {"status": "unreachable"}, "big.bin": {"status": "unreachable"}},
        {"a.txt": {"status": "ok"}, "b.txt": {"status": "ok"}, "big.bin": {"status": "ok"}},
    ]
    sb = _restore_sandbox()
    fetched = {CHUNK: A + B, _sha(BIG): BIG}
    fetch = AsyncMock(side_effect=lambda uid, sha: fetched[sha])
    with (
        patch.object(restore, "fetch_blob", new=fetch),
        patch.object(resolve, "fetch_blob", new=fetch),
    ):
        result = await restore.restore_to_sandbox(WS, sb)
    assert sorted(c.args[1] for c in fetch.await_args_list) == sorted([CHUNK, _sha(BIG)])
    uploads = {c.args[0]: c.args[1] for c in sb.aupload_file_bytes.await_args_list if not c.args[0].endswith(".file_sync_marker")}
    (staged,) = [p for p in uploads if p != RELAY_CHUNK]
    assert staged.startswith("/workspace/.wsfiles-relay-") and uploads[staged] == BIG
    assert uploads[RELAY_CHUNK] == A + B
    (items,) = _pack_calls(restore_db["pull"])
    big = restore._pull_item(_row("big.bin", BIG, blob=_sha(BIG)), url=None)
    big.update({"file": staged.removeprefix("/workspace/"), "sha256": _sha(BIG)})
    assert items == [big, {
        "kind": "pack", "file": f".wsfiles-relay-{CHUNK}", "sha256": CHUNK, "size": 8,
        "members": [
            {"path": "a.txt", "offset": 0, "size": 3, "sha256": _sha(A), "mode": 0o600, "mtime_ns": (NS // 1000) * 1000},
            {"path": "b.txt", "offset": 3, "size": 5, "sha256": _sha(B), "mode": 0o644, "mtime_ns": (NS // 1000) * 1000},
        ],
    }]
    assert result == {"restored": 3, "errors": 0}


@pytest.mark.asyncio
async def test_relay_mode_never_uploads_members_one_by_one(restore_db):
    restore_db["pull"].return_value = {"a.txt": {"status": "ok"}, "b.txt": {"status": "ok"}, "big.bin": {"status": "ok"}}
    sb = _restore_sandbox("docker")
    fetched = {CHUNK: A + B, _sha(BIG): BIG}
    fetch = AsyncMock(side_effect=lambda uid, sha: fetched[sha])
    with (
        patch.object(restore, "fetch_blob", new=fetch),
        patch.object(resolve, "fetch_blob", new=fetch),
        patch.object(resolve, "fetch_blob_range", new=AsyncMock()) as ranged,
    ):
        result = await restore.restore_to_sandbox(WS, sb)
    restore_db["sign"].assert_not_called()
    ranged.assert_not_awaited()
    assert fetch.await_count == 2
    uploads = {c.args[0] for c in sb.aupload_file_bytes.await_args_list}
    assert RELAY_CHUNK in uploads and "/workspace/.file_sync_marker" in uploads
    assert "/workspace/big.bin" not in uploads and len(uploads) == 3
    assert result == {"restored": 3, "errors": 0}


@pytest.mark.asyncio
async def test_relay_mode_counts_a_member_the_runtime_rejects(restore_db):
    restore_db["rows"].return_value = [_row("a.txt", A, pack=CHUNK, offset=0), _row("b.txt", B, pack=CHUNK, offset=3)]
    restore_db["pull"].return_value = {"a.txt": {"status": "mismatch", "error": "got sha256=x"}, "b.txt": {"status": "ok"}}
    sb = _restore_sandbox("docker")
    with patch.object(restore, "fetch_blob", new=AsyncMock(return_value=A + B)):
        result = await restore.restore_to_sandbox(WS, sb)
    assert result == {"restored": 1, "errors": 1}
    # Raised up front and never cleared: the sandbox is a partial mirror.
    assert [c.args for c in restore_db["flag"].await_args_list] == [(WS, True)]


@pytest.mark.asyncio
async def test_relay_mode_fails_every_member_when_the_chunk_upload_fails(restore_db):
    restore_db["rows"].return_value = [_row("a.txt", A, pack=CHUNK, offset=0), _row("b.txt", B, pack=CHUNK, offset=3)]
    sb = _restore_sandbox("docker")
    sb.aupload_file_bytes = AsyncMock(return_value=False)
    with patch.object(restore, "fetch_blob", new=AsyncMock(return_value=A + B)):
        result = await restore.restore_to_sandbox(WS, sb)
    restore_db["pull"].assert_not_awaited()
    assert result == {"restored": 0, "errors": 2}


# --- reads --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolver_reads_a_packed_row_by_range():
    with patch.object(resolve, "fetch_blob_range", new=AsyncMock(return_value=B)) as ranged:
        assert await resolve_file_bytes(_row("b.txt", B, pack=CHUNK, offset=3), user_id=USER) == B
    ranged.assert_awaited_once_with(USER, CHUNK, 3, 5, expected_sha256=_sha(B))


@pytest.mark.asyncio
async def test_resolver_serves_a_zero_length_member_without_a_request():
    """An empty range is not a valid HTTP range; the store layer answers it locally."""
    from src.utils.storage import s3_compatible

    with patch.object(s3_compatible, "_get_range_client", side_effect=AssertionError("no request")):
        assert s3_compatible.get_bytes_range("blobs/x", 0, 0) == b""


@pytest.mark.asyncio
async def test_a_chunk_the_sandbox_could_not_upload_is_relayed_out_of_the_sandbox(db):
    """The relay's only source for a chunk's bytes is the sandbox's own copy, so
    the runtime has to keep a chunk whose push failed. Deleting it on every push
    left the fallback downloading a file that was no longer there."""
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B), _entry("big.bin", BIG))
    db["push"].side_effect = lambda sb, items: {
        i["sha256"]: {"status": "unreachable" if i["sha256"] == CHUNK else "ok"} for i in items
    }
    sb = _sandbox()
    with (
        patch.object(blobs, "store_blob", new=AsyncMock()) as store,
        patch.object(blobs, "unlink_direct", new=AsyncMock(return_value=1)) as unlink,
    ):
        result = await backup.sync_to_db(WS, sb)
    chunk_path = f"_internal/packs/chunk-{CHUNK}"
    sb.adownload_file_bytes.assert_awaited_once_with(f"/workspace/{chunk_path}")
    store.assert_awaited_once_with(USER, CHUNK, A + B)
    # What the runtime kept, the server removes once it has the bytes.
    unlink.assert_awaited_once_with(sb, [chunk_path])
    assert _rows(db)["a.txt"]["pack_sha256"] == CHUNK
    assert result["errors"] == 0


@pytest.mark.asyncio
async def test_relay_rejects_bytes_whose_length_disagrees_with_the_scan(db):
    """The scan stats before it hashes, so a file that grew in between carries
    the new digest with the old length. Relay checks both; a row published with
    a length its blob does not have would fail every later restore of it."""
    db["scan"].return_value = _scan(_entry("big.bin", BIG))
    sb = _sandbox("docker")
    sb.adownload_file_bytes = AsyncMock(return_value=BIG + b"!")
    with patch.object(blobs, "store_blob", new=AsyncMock()) as store:
        result = await backup.sync_to_db(WS, sb)
    store.assert_not_awaited()
    db["upsert"].assert_not_awaited()
    assert result["errors"] == 1


@pytest.mark.asyncio
async def test_a_chunk_the_registry_already_holds_is_removed_without_a_push(db):
    """Nothing pushes a dedup hit, so nothing downstream would unlink it."""
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    db["registered"].return_value = {CHUNK}
    sb = _sandbox()
    with patch.object(blobs, "unlink_direct", new=AsyncMock(return_value=1)) as unlink:
        await backup.sync_to_db(WS, sb)
    db["push"].assert_not_awaited()
    unlink.assert_awaited_once_with(sb, [f"_internal/packs/chunk-{CHUNK}"])
    assert _rows(db)["a.txt"]["pack_sha256"] == CHUNK


@pytest.mark.asyncio
async def test_relayed_chunks_are_removed_from_the_sandbox(db):
    """Only the direct push unlinks in the runtime; a relayed chunk needs an explicit removal."""
    db["scan"].return_value = _scan(_entry("a.txt", A), _entry("b.txt", B))
    sb = _sandbox("docker")
    sb.adownload_file_bytes = AsyncMock(return_value=A + B)
    with (
        patch.object(blobs, "store_blob", new=AsyncMock()) as store,
        patch.object(blobs, "unlink_direct", new=AsyncMock(return_value=1)) as unlink,
    ):
        await backup.sync_to_db(WS, sb)
    db["push"].assert_not_awaited()
    store.assert_awaited_once_with(USER, CHUNK, A + B)
    unlink.assert_awaited_once_with(sb, [f"_internal/packs/chunk-{CHUNK}"])
    assert _rows(db)["a.txt"]["pack_sha256"] == CHUNK
