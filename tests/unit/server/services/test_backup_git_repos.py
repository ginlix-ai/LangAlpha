"""A Git repository is backed up whole or not at all, and a name the
manifest cannot key is reported rather than failing the pass.

Verified live first on a Docker computer: a repository with an unpushed
branch, a stash and an uncommitted edit came back from a rebuild with all of
them and a clean ``git fsck --full``; a lock git left held the last complete
history; a file over the relay cap inside ``.git`` reported the history as too
large. These pin the decisions.
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import wsfiles_transfer_runtime as rt
from src.server.services.persistence import backup, repos, transfer
from src.server.services.persistence.repos import hold_repos, repo_of
from src.server.services.persistence.sync_result import SyncResult, UnsavedFile
from src.server.services.persistence.transfer import ScanEntry, ScanResult
from tests.unit.server.services._no_db import NoDbConn

WS = "ws-git"
MTIME_NS = 1_700_000_000_000_000_000
LAYOUT = SandboxLayout.for_root("/workspace").for_workspace("git-ab12")
CLOCK = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)


def _file(path: str, size: int = 10, sha: str = "new", mtime_ns: int = MTIME_NS) -> ScanEntry:
    return ScanEntry(path, "file", size, mtime_ns, 0o644, sha, None, False)


def _dir(path: str) -> ScanEntry:
    return ScanEntry(path, "dir", 0, MTIME_NS, 0o755, None, None, None)


def _row(sha: str = "old") -> dict:
    return {"kind": "file", "file_size": 10, "mtime_ns": MTIME_NS, "content_hash": sha, "permissions": "0644"}


# --- which repositories a pass holds ---------------------------------------


def test_the_first_git_on_a_path_owns_it():
    """A submodule's history sits under its parent's ``.git/modules/``."""
    assert repo_of("thesis/.git/modules/data/HEAD") == "thesis/.git"
    assert repo_of(".git") == ".git"
    assert repo_of("thesis/ch1.md") is None


def test_a_lock_git_holds_means_the_walk_may_have_caught_two_states():
    hold = hold_repos([], [_dir("t/.git"), _file("t/.git/HEAD"), _file("t/.git/index.lock")])
    assert hold.repos == {"t/.git"}
    assert hold.unsaved == [UnsavedFile("t/.git", "changed")]


def test_a_lock_a_killed_git_left_behind_does_not_hold_forever():
    """A command timeout or a stop mid-clone leaves the lock for good; counted
    forever, it held the history and refused every teardown. It is not stored
    either: restored, it made the rebuilt computer's git refuse to write."""
    hour_later = MTIME_NS + 60 * 60 * 1_000_000_000
    stale = hold_repos([], [_dir("t/.git"), _file("t/.git/HEAD"), _file("t/.git/index.lock")], now_ns=hour_later)
    assert not stale.repos
    assert stale.stores("t/.git/HEAD") and not stale.stores("t/.git/index.lock")
    live = hold_repos([], [_dir("t/.git"), _file("t/.git/index.lock")], now_ns=MTIME_NS + 1)
    assert live.repos == {"t/.git"}


def test_a_branch_named_like_a_temp_file_is_a_ref_not_git_writing():
    """``tmp_`` is git's only in the object store; a branch ``tmp_fix`` held
    its repository on every pass."""
    hold = hold_repos([], [_dir("t/.git"), _file("t/.git/refs/heads/tmp_fix"), _file("t/.git/logs/refs/heads/tmp_fix")])
    assert not hold.repos
    assert not hold_repos([], [_dir("t/.git"), _file("t/.git/refs/heads/objects/ab/tmp_x")]).repos
    for temp in (
        "objects/pack/tmp_pack_x",
        "objects/pack/.tmp-12-pack-ab.pack",
        "modules/data/objects/ab/tmp_obj_x",
        "modules/refs/objects/pack/tmp_pack_x",
    ):
        assert hold_repos([], [_dir("t/.git"), _file(f"t/.git/{temp}")]).repos == {"t/.git"}, temp


def test_a_history_the_scan_sized_past_the_limit_keeps_nothing():
    """It was never stored, so its old rows go too: a restore brings back no
    history rather than an old one beside a newer working tree."""
    hold = hold_repos([UnsavedFile("t/.git", "history_too_large", 2000)], [])
    assert hold.unsaved == [UnsavedFile("t/.git", "history_too_large", 2000)]
    assert hold.repos == {"t/.git"}
    assert hold.rows_of(["t/.git", "t/.git/HEAD"]) == set()


def test_a_file_over_the_transfer_cap_makes_its_history_too_large_not_at_risk():
    """On the relay path a pack over 256 MiB cannot travel. Reported as a
    file whose only copy is at risk, it refused every teardown, which leaving
    ``.git`` out never did."""
    other = UnsavedFile("a.csv", "too_large", 9)
    hold = hold_repos(
        [UnsavedFile("t/.git/objects/pack/big.pack", "too_large", 300), other],
        [_dir("t/.git"), _file("t/.git/HEAD")],
    )
    assert hold.unsaved == [other, UnsavedFile("t/.git", "history_too_large", 310)]
    assert SyncResult(unsaved=hold.unsaved).at_risk == [other]


def test_git_writing_wins_over_too_large():
    """Mid-repack the old packs and the new one are all on disk. Read as too
    large, the history lost its last complete rows and a teardown went on."""
    hold = hold_repos(
        [UnsavedFile("t/.git", "history_too_large", 2000)],
        [_file("t/.git/objects/pack/tmp_pack_x", mtime_ns=MTIME_NS)],
        now_ns=MTIME_NS + 1,
    )
    assert hold.unsaved == [UnsavedFile("t/.git", "changed")]
    assert hold.kept == {"t/.git"}


def test_a_file_git_removed_mid_scan_keeps_a_history_read_as_too_large():
    """Mid-repack the scan sized the old packs and the new, then found an old
    index gone: the history is moving, not too large for good."""
    hold = hold_repos(
        [UnsavedFile("t/.git", "history_too_large", 2000), UnsavedFile("t/.git/objects/pack/old.idx", "changed")],
        [],
    )
    assert hold.kept == {"t/.git"}
    assert UnsavedFile("t/.git", "changed") in hold.unsaved
    assert all(f.reason != "history_too_large" for f in hold.unsaved)


def test_a_history_listed_past_the_limit_is_too_large():
    """Sized under the limit before the walk, it can grow past it during."""
    hold = hold_repos(
        [], [_dir("t/.git"), _file("t/.git/objects/pack/a.pack"), _file("t/.git/objects/pack/b.pack")], max_bytes=15
    )
    assert hold.unsaved == [UnsavedFile("t/.git", "history_too_large", 20)]
    assert hold.kept == frozenset()


def test_a_repository_a_restore_has_yet_to_finish_is_held():
    """Its kept rows and this pass's new ones would be two states as one."""
    hold = hold_repos([], [_dir("t/.git"), _file("t/.git/packed-refs")], pending={"t/.git"})
    assert hold.repos == hold.kept == {"t/.git"} and hold.unsaved == []


@pytest.mark.asyncio
async def test_a_repository_in_a_scratchpad_waits_for_the_deferred_restore():
    """The deferred pass brings its old loose ref back later, where it would
    outrank the new ``packed-refs``."""
    repo = f"{transfer.WorkspaceLayout.SCRATCHPAD_DIR}/ab12cd34/clone/.git"
    listing = ScanResult(
        entries=[_dir(repo), _file(f"{repo}/packed-refs"), _file("a.csv")],
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    _, inline, _, prune = await _sync(listing, {f"{repo}/refs/heads/main": _row()})
    assert [e.path for e in inline.await_args.args[2]] == ["a.csv"]
    assert f"{repo}/refs/heads/main" in prune.await_args.args[1]


def test_without_object_storage_a_history_gets_one_files_cap():
    """There it is rows in Postgres, written in one transaction."""
    assert transfer.history_cap_bytes(blobs_on=True) == transfer.GIT_HISTORY_MAX_BYTES
    assert transfer.history_cap_bytes(blobs_on=False) == transfer.INLINE_MAX_BYTES
    inline = transfer.ScanRules.of(transfer.INLINE_MAX_BYTES, transfer.INLINE_MAX_BYTES)
    assert inline.fingerprint != transfer.ScanRules.of(transfer.INLINE_MAX_BYTES).fingerprint


def test_too_large_wins_over_held_and_the_other_files_keep_their_lines():
    """A history that cannot travel is not held at an old copy; an unreadable
    file in it still says why the pass could not prune."""
    unreadable = UnsavedFile("t/.git/objects/ab/cd", "unreadable")
    hold = hold_repos(
        [unreadable, UnsavedFile("t/.git/objects/pack/big.pack", "too_large", 300)],
        [_dir("t/.git")],
    )
    assert hold.unsaved == [unreadable, UnsavedFile("t/.git", "history_too_large", 300)]
    assert hold.kept == frozenset()


def test_an_unsaved_file_holds_its_repository_without_holding_the_scan_mark():
    """The file's own line decides whether a later pass can save it."""
    unreadable = UnsavedFile("t/.git/objects/ab/cd", "unreadable")
    hold = hold_repos([unreadable], [_dir("t/.git")])
    assert hold.unsaved == [unreadable, UnsavedFile("t/.git", "history_held")]
    assert SyncResult(unsaved=hold.unsaved[1:]).errors == 0


def test_a_pass_that_cannot_prune_holds_every_repository():
    """New files written beside the rows of the ones git deleted would store
    two states as one (a finished merge's MERGE_HEAD, a ref git packed)."""
    hold = hold_repos([], [_dir("t/.git"), _file("t/.git/HEAD"), _dir("u/.git")], hold_all=True)
    assert hold.repos == hold.kept == {"t/.git", "u/.git"}
    assert hold.unsaved == []


def test_a_git_file_is_a_pointer_not_a_repository():
    assert not hold_repos([], [_file("wt/.git"), _file("wt/x.lock")]).repos


def test_the_held_rows_are_the_repositorys_alone():
    hold = hold_repos([], [_dir("t/.git"), _file("t/.git/index.lock")])
    assert hold.rows_of(["t/.git", "t/.git/HEAD", "t/ch1.md", "u/.git/HEAD"]) == {"t/.git", "t/.git/HEAD"}


# --- what a pass stores for a held repository ------------------------------


@pytest.fixture(autouse=True)
def _no_db():
    @asynccontextmanager
    async def _lock(_workspace_id):
        yield NoDbConn()

    with (
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=CLOCK)),
    ):
        yield


async def _sync(listing: ScanResult, existing: dict, *, incomplete: bool = False):
    sandbox = MagicMock()
    sandbox.working_dir = "/workspace"
    with (
        patch.object(backup, "PACK_CUTOFF", -1),
        patch.object(backup, "is_storage_enabled", return_value=False),
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=listing)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value=existing)),
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=incomplete)),
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch.object(backup, "_persist_inline", new=AsyncMock(return_value=(0, []))) as inline,
        patch.object(backup, "bulk_upsert_files", new=AsyncMock(return_value=0)) as upsert,
        patch.object(backup, "bulk_update_file_stamps", new=AsyncMock()),
        patch.object(backup, "delete_file_rows", new=AsyncMock(return_value=0)),
        patch.object(backup, "delete_gone_rows", new=AsyncMock(side_effect=lambda ws, paths, **_: len(paths))),
        patch.object(backup, "delete_removed_files", new=AsyncMock(return_value=0)) as prune,
    ):
        result = await backup.sync_to_db(WS, sandbox, layout=LAYOUT)
    return result, inline, upsert, prune


@pytest.mark.asyncio
async def test_a_held_repository_keeps_its_last_complete_rows():
    """Nothing of this pass's reaches the manifest, and the prune keeps every
    row it had, the last history a restore can read whole."""
    listing = ScanResult(
        entries=[
            _dir("t/.git"),
            _file("t/.git/HEAD"),
            _file("t/.git/index.lock"),
            _file("t/.git/objects/ab/new"),
            _file("t/ch1.md"),
        ],
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    existing = {"t/.git/HEAD": _row(), "t/.git/objects/ab/gone": _row(), "t/ch1.md": _row()}

    result, inline, upsert, prune = await _sync(listing, existing)

    sent = [e.path for call in inline.await_args_list for e in call.args[2]]
    assert sent == ["t/ch1.md"]
    upserted = [r["file_path"] for call in upsert.await_args_list for r in call.args[1]]
    assert not any(p.startswith("t/.git") for p in upserted)
    kept = prune.await_args.args[1]
    assert {"t/.git/HEAD", "t/.git/objects/ab/gone"} <= kept
    assert UnsavedFile("t/.git", "changed") in result.unsaved


@pytest.mark.asyncio
async def test_a_history_too_large_is_left_to_the_prune():
    listing = ScanResult(
        entries=[_file("t/ch1.md")],
        whole_too_large=[{"path": "t/.git", "size": 2000}],
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    existing = {"t/.git": {"kind": "dir"}, "t/.git/HEAD": _row(), "t/ch1.md": _row()}

    result, _, _, prune = await _sync(listing, existing)

    assert not {"t/.git", "t/.git/HEAD"} & prune.await_args.args[1]
    assert result.unsaved == [UnsavedFile("t/.git", "history_too_large", 2000)]
    assert result.at_risk == []


@pytest.mark.asyncio
async def test_a_file_vanishing_inside_a_repository_holds_it_for_a_retry():
    """It is git at work, not a deletion: read as one, a ref ``pack-refs``
    moved could lose its row."""
    listing = ScanResult(
        entries=[_dir("t/.git"), _file("t/.git/packed-refs"), _file("t/ch1.md")],
        oversized=[],
        errors=[{"path": "t/.git/refs/heads/main", "errno": 2}],
        hashed=0,
        reused=0,
    )
    existing = {"t/.git/refs/heads/main": _row(), "t/ch1.md": _row()}

    result, _, _, prune = await _sync(listing, existing)

    assert "t/.git/refs/heads/main" in prune.await_args.args[1]
    assert result.errors > 0


@pytest.mark.asyncio
async def test_a_restore_still_incomplete_stores_no_repository():
    listing = ScanResult(
        entries=[_dir("t/.git"), _file("t/.git/HEAD"), _file("t/ch1.md")],
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    _, inline, upsert, _ = await _sync(listing, {}, incomplete=True)
    sent = [e.path for call in inline.await_args_list for e in call.args[2]]
    upserted = [r["file_path"] for call in upsert.await_args_list for r in call.args[1]]
    assert sent == ["t/ch1.md"]
    assert not any(p.startswith("t/.git") for p in upserted)


class _Conn:
    """A connection whose transaction records whether it rolled back."""

    def __init__(self) -> None:
        self.rolled_back = 0

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except Exception:
            self.rolled_back += 1
            raise


@pytest.mark.asyncio
async def test_inline_rows_of_a_repository_are_all_or_none(monkeypatch):
    """Inline rows are written as they are read; a repository that failed
    halfway kept a newer ref beside the old objects. Its removed rows go in
    the same transaction: a pass cut short after the new ``packed-refs``
    kept the loose refs that outrank it."""
    calls = []
    removed = []

    async def delete(workspace_id, paths, *, walked_dir_name, untouched_since, conn):
        assert (walked_dir_name, untouched_since) == (LAYOUT.dir_name, CLOCK)
        removed.extend(paths)
        return len(paths)

    monkeypatch.setattr(backup, "delete_gone_rows", delete)

    async def persist(workspace_id, sandbox, entries, *, layout, conn, exact=False):
        calls.append(([e.path for e in entries], exact))
        missed = [UnsavedFile(e.path, "changed") for e in entries if e.path.endswith("cd")]
        return len(entries) - len(missed), missed

    monkeypatch.setattr(backup, "_persist_inline", persist)
    conn = _Conn()
    entries = [_file("a.csv"), _file("t/.git/refs/heads/main"), _file("t/.git/objects/ab/cd"), _file("u/.git/HEAD")]

    gone = {"t/.git": ["t/.git/refs/heads/old"], "u/.git": ["u/.git/refs/heads/old"]}
    synced, deleted, unsaved = await backup._persist_inline_by_repo(
        WS, MagicMock(), entries, layout=LAYOUT, conn=conn, gone=gone, untouched_since=CLOCK
    )

    assert calls[0] == (["a.csv"], False)
    assert all(exact for _, exact in calls[1:])
    assert conn.rolled_back == 1
    assert synced == 2  # a.csv and u/.git/HEAD; t/.git's ref rolled back with it
    assert unsaved == [UnsavedFile("t/.git/objects/ab/cd", "changed")]
    assert removed == ["u/.git/refs/heads/old"] and deleted == 1


@pytest.mark.asyncio
async def test_a_link_whose_target_is_not_utf8_keeps_its_last_row():
    """Its own name still keys a row, and the link is still there; the
    prune read it as deleted and dropped the only copy."""
    link = ScanEntry("link", "symlink", 0, MTIME_NS, 0o777, None, "\udcff", None)
    shown = _dir("bad\ufffd")  # ``bad\xff`` as shown: it must not stand for a real ``bad\ufffd``
    listing = ScanResult(
        entries=[_file("a.csv")],
        unnamed=[link, shown],
        unnamed_links={"link"},
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    result, _, _, prune = await _sync(listing, {"link": {**_row(), "kind": "symlink"}, "bad\ufffd": _row()})
    keep = prune.await_args.args[1]
    assert "link" in keep and "bad\ufffd" not in keep
    assert result.unsaved == [UnsavedFile("link", "bad_name"), UnsavedFile("bad\ufffd", "bad_name")]


@pytest.mark.asyncio
async def test_a_name_that_is_not_utf8_is_reported_and_the_pass_goes_on():
    """One such name failed the whole batch insert, so nothing else in the
    workspace was backed up again, and a routine pass only logged it."""
    listing = ScanResult(
        entries=[_file("a.csv")],
        unnamed=[_file("�걨.txt", 3)],
        oversized=[],
        errors=[],
        hashed=0,
        reused=0,
    )
    result, inline, _, _ = await _sync(listing, {})
    assert [e.path for e in inline.await_args.args[2]] == ["a.csv"]
    assert result.unsaved == [UnsavedFile("�걨.txt", "bad_name", 3)]
    assert result.at_risk == result.unsaved


@pytest.mark.asyncio
async def test_a_row_the_prune_fences_keep_holds_its_repository(monkeypatch):
    """Written since the scan began, or in a folder the workspace has since
    left: deleting it was not this pass's to do, and the new rows would stand
    beside it."""
    monkeypatch.setattr(backup, "delete_gone_rows", AsyncMock(return_value=0))
    monkeypatch.setattr(backup, "_persist_inline", AsyncMock(side_effect=lambda ws, sb, entries, **_: (len(entries), [])))
    conn = _Conn()
    synced, deleted, unsaved = await backup._persist_inline_by_repo(
        WS,
        MagicMock(),
        [_file("t/.git/packed-refs")],
        layout=LAYOUT,
        conn=conn,
        gone={"t/.git": ["t/.git/refs/heads/main"]},
        untouched_since=CLOCK,
    )
    assert (synced, deleted, conn.rolled_back) == (0, 0, 1)
    assert unsaved == [UnsavedFile("t/.git", "changed")]


# --- the scan's side ---------------------------------------------------------


@pytest.mark.asyncio
async def test_the_scan_sets_aside_names_the_manifest_cannot_key():
    """Only the topmost of a misnamed tree is listed, shown with replacement
    marks a response can carry; a link whose target is misnamed is too."""
    wire = {
        "entries": [
            {"path": "ok.txt", "kind": "file", "size": 1},
            {"path": "bad\udcc4", "kind": "dir"},
            {"path": "bad\udcc4/inner.txt", "kind": "file", "size": 2},
            {"path": "bad\udcc4-old", "kind": "dir"},
            {"path": "link", "kind": "symlink", "symlink_target": "\udcc4"},
        ],
        "oversized": [{"path": "big\udcc4.bin", "size": 9}, {"path": "bad\udcc4/.git/big.pack", "size": 9}],
        "whole_too_large": [{"path": "bad\udcc4/.git", "size": 9, "transient": []}],
        "errors": [],
    }
    with patch.object(transfer, "run_transfer_op", new=AsyncMock(return_value=wire)):
        scan = await transfer.scan_workspace(MagicMock(), {}, max_file_bytes=None, layout=LAYOUT)
    assert [e.path for e in scan.entries] == ["ok.txt"]
    # A misnamed file too large to carry is one line, as a name to fix; shown
    # in ``oversized`` it could pass for a real ``bad�/.git``'s pack.
    assert [e.path for e in scan.unnamed] == ["bad�", "bad�-old", "big�.bin", "link"]
    assert scan.unnamed_links == {"link"}
    assert scan.oversized == []
    # Shown as ``bad�/.git`` it could pass for a real folder's history.
    assert scan.whole_too_large == []


def _tree(root: Path, *rels: str) -> None:
    for rel in rels:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text("x")


def _hash_order(rt_mod, monkeypatch, root: Path, spec: dict | None = None) -> list[str]:
    order: list[str] = []
    real = rt_mod._hash_file
    monkeypatch.setattr(rt_mod, "_hash_file", lambda p: (order.append(p), real(p))[1])
    rt_mod.scan({"root": str(root), **(spec or {})})
    return [str(Path(p).relative_to(root)) for p in order]


def test_the_walk_reads_head_then_loose_refs_then_packed_refs_then_objects(tmp_path: Path, monkeypatch):
    """Git writes an object before any ref naming it, a branch before
    ``HEAD`` names it, and ``pack-refs`` writes the packed file before
    removing the loose refs. Read in name order, a commit landing mid-walk
    left a ref to an object the backup never saw: 23 of 60 scans during a
    commit loop, none with refs first. The same holds in a submodule's
    history under ``.git/modules/``."""
    _tree(
        tmp_path,
        ".git/HEAD",
        ".git/packed-refs",
        ".git/objects/ab/cd",
        ".git/refs/heads/main",
        ".git/modules/data/objects/ef/gh",
        ".git/modules/data/refs/heads/main",
    )
    rel = _hash_order(rt, monkeypatch, tmp_path)
    assert (
        rel.index(".git/HEAD")
        < rel.index(".git/refs/heads/main")
        < rel.index(".git/packed-refs")
        < rel.index(".git/objects/ab/cd")
    )
    assert rel.index(".git/modules/data/refs/heads/main") < rel.index(".git/modules/data/objects/ef/gh")


def test_a_packed_refs_written_while_the_refs_are_read_is_read(tmp_path: Path, monkeypatch):
    """``pack-refs`` writes ``packed-refs`` and then removes the loose refs.
    In a repository that had none, the listing taken before missed the file
    while the walk missed the refs."""
    _tree(tmp_path, ".git/HEAD", ".git/refs/heads/main", ".git/objects/ab/cd")
    real = rt._hash_file

    def hash_and_pack(path):
        if path.endswith("refs/heads/main"):
            (tmp_path / ".git/packed-refs").write_text("x refs/heads/main")
        return real(path)

    monkeypatch.setattr(rt, "_hash_file", hash_and_pack)
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"]})
    assert ".git/packed-refs" in {e["path"] for e in out["entries"]}


def test_a_git_folder_that_cannot_be_listed_again_is_a_read_error(tmp_path: Path, monkeypatch):
    """The second listing may be the only one with a new ``packed-refs``."""
    _tree(tmp_path, ".git/HEAD", ".git/refs/heads/main")
    git = str(tmp_path / ".git")
    real, calls = os.scandir, []

    def scandir(path="."):
        calls.append(str(path))
        if str(path) == git and calls.count(git) == 2:
            raise OSError(5, "Input/output error")
        return real(path)

    monkeypatch.setattr(rt.os, "scandir", scandir)
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"]})
    assert [(e["path"], e["errno"]) for e in out["errors"]] == [(".git", 5)]


def test_a_git_link_is_left_out_and_a_git_file_kept(tmp_path: Path):
    """A ``.git`` symlink points at a history outside the folder, which a
    restore cannot bring back. A ``.git`` file is a submodule's or a linked
    worktree's pointer, whose history the walk finds where it lives."""
    _tree(tmp_path, "linked/a.txt", "sub/.git", "elsewhere/HEAD")
    os.symlink(tmp_path / "elsewhere", tmp_path / "linked/.git")
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"]})
    paths = {e["path"] for e in out["entries"]}
    assert "linked/.git" not in paths and {"linked/a.txt", "sub/.git"} <= paths


def test_a_ref_named_like_a_dependency_is_kept(tmp_path: Path):
    """The name rules are about a working tree; inside a ``.git`` the same
    names are refs, whose loss strands a commit."""
    _tree(tmp_path, ".git/refs/heads/node_modules/topic", ".git/refs/tags/release.o", "node_modules/x.js", "a.o")
    spec = transfer.exclusion_spec(None)
    out = rt.scan({**spec, "root": str(tmp_path)})
    paths = {e["path"] for e in out["entries"]}
    assert {".git/refs/heads/node_modules/topic", ".git/refs/tags/release.o"} <= paths
    assert not {"node_modules/x.js", "a.o"} & paths


@pytest.mark.parametrize(
    ("rel", "writing"),
    [
        ("index.lock", True),
        ("refs/heads/main.lock", True),
        ("gc.pid", True),
        ("objects/incoming-ab12/pack/x.pack", True),
        ("objects/ab/tmp_obj_x", True),
        ("objects/pack/tmp_pack_x", True),
        ("objects/pack/.tmp-12-pack-ab.pack", True),
        ("modules/data/objects/ab/tmp_obj_x", True),
        ("modules/refs/objects/pack/tmp_pack_x", True),
        ("refs/heads/tmp_fix", False),
        ("logs/refs/heads/tmp_topic", False),
        ("refs/heads/objects/pack/tmp_x", False),
        ("logs/refs/heads/objects/pack/tmp_large", False),
        ("objects/pack/pack-ab.pack", False),
        ("HEAD", False),
    ],
)
def test_the_sandbox_and_the_server_agree_on_what_git_is_writing(rel: str, writing: bool):
    """The runtime ships stdlib-only and carries the rule by hand: it sizes a
    history by it, and the server holds and stores by it."""
    assert repos._git_is_writing(rel) is writing
    assert rt._git_transient(rel) is writing


def test_a_history_past_the_limit_is_sized_and_never_read(tmp_path: Path, monkeypatch):
    """Hashed and thrown away, a large clone cost its size in reads on every
    changed pass. A pack being written is named, not counted: a repack holds
    the old packs and the new one at once."""
    _tree(tmp_path, ".git/objects/pack/a.pack", ".git/objects/pack/tmp_pack_b", "a.txt")
    (tmp_path / ".git/objects/pack/a.pack").write_bytes(b"x" * 100)
    (tmp_path / ".git/objects/pack/tmp_pack_b").write_bytes(b"x" * 100)
    order: list[str] = []
    real = rt._hash_file
    monkeypatch.setattr(rt, "_hash_file", lambda p: (order.append(p), real(p))[1])
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"], "max_whole_dir_bytes": 50})
    [history] = out["whole_too_large"]
    assert (history["path"], history["size"]) == (".git", 100)
    assert [t["path"] for t in history["transient"]] == [".git/objects/pack/tmp_pack_b"]
    assert [e["path"] for e in out["entries"]] == ["a.txt"]
    assert all(".git" not in p for p in order)


def test_a_history_past_the_limit_still_reports_what_it_could_not_read(tmp_path: Path, monkeypatch):
    """Read as too large alone, its last complete rows went with the prune."""
    _tree(tmp_path, ".git/objects/pack/a.pack", ".git/objects/ab/cd")
    (tmp_path / ".git/objects/pack/a.pack").write_bytes(b"x" * 100)
    bad = str(tmp_path / ".git/objects/ab")
    real = os.scandir

    def scandir(path="."):
        if str(path) == bad:
            raise OSError(5, "Input/output error")
        return real(path)

    monkeypatch.setattr(rt.os, "scandir", scandir)
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"], "max_whole_dir_bytes": 50})
    assert [h["path"] for h in out["whole_too_large"]] == [".git"]
    assert [(e["path"], e["errno"]) for e in out["errors"]] == [(".git/objects/ab", 5)]


def test_a_reflog_named_like_a_temp_file_counts_toward_the_size(tmp_path: Path):
    """Left out as git writing, it let a history over the limit be read in
    full before the server turned it away."""
    _tree(tmp_path, ".git/logs/refs/heads/tmp_topic", ".git/objects/pack/tmp_pack_x")
    (tmp_path / ".git/logs/refs/heads/tmp_topic").write_bytes(b"x" * 100)
    out = rt.scan({"root": str(tmp_path), "whole_dir_names": [".git"], "max_whole_dir_bytes": 50})
    [history] = out["whole_too_large"]
    assert history["size"] == 100
    assert [t["path"] for t in history["transient"]] == [".git/objects/pack/tmp_pack_x"]


def test_git_status_alone_does_not_make_a_project_look_changed(tmp_path: Path):
    """Each ``git status`` makes and removes ``.git/index.lock``, which moves
    the folder's change time; a sweep that compared it rescanned every
    project with a repository on every turn."""
    _tree(tmp_path, ".git/HEAD", ".git/objects/ab/cd", "a.txt")
    time.sleep(0.01)
    mark = {"ns": time.time_ns(), "boot_id": rt._boot_id(), "offset_ns": rt._clock_offset_ns()}
    time.sleep(0.01)
    lock = tmp_path / ".git/index.lock"
    lock.write_text("x")
    os.unlink(lock)
    spec = {"whole_dir_names": [".git"], "projects": [{"key": "p", "root": str(tmp_path), "mark": mark}]}
    assert rt.sweep(spec)["unchanged"] == ["p"]
    _tree(tmp_path, ".git/objects/ef/gh")
    assert rt.sweep(spec)["changed"] == ["p"]


def test_a_ref_git_deleted_makes_a_project_look_changed(tmp_path: Path):
    """A deletion shows only in its folder's change time, so only the git
    folder's root, where ``index.lock`` comes and goes, may go unread."""
    _tree(tmp_path, ".git/HEAD", ".git/refs/heads/topic", ".git/modules/libs/data/HEAD", "a.txt")
    time.sleep(0.01)
    mark = {"ns": time.time_ns(), "boot_id": rt._boot_id(), "offset_ns": rt._clock_offset_ns()}
    time.sleep(0.01)
    for root in (".git", ".git/modules/libs/data"):
        lock = tmp_path / root / "index.lock"
        lock.write_text("x")
        os.unlink(lock)
    spec = {"whole_dir_names": [".git"], "projects": [{"key": "p", "root": str(tmp_path), "mark": mark}]}
    assert rt.sweep(spec)["unchanged"] == ["p"]
    os.unlink(tmp_path / ".git/refs/heads/topic")
    assert rt.sweep(spec)["changed"] == ["p"]
