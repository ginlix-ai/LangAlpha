"""The Drive against real PostgreSQL: what only the database can show.

The lease on an upload being read back, the conditions a begin resumes
under, a cancel racing the version's commit, the expiry sweep racing a
renewal, the counts the space keeps as its rows change, what a replace or
the trash's bound displaces, a history clear, and a trash
item holding two generations of one path. Memos, undo and the record are
in test_drive_memos_db.py.
"""

# The ``space`` fixture comes from drive_helpers; each test takes it by name.
# ruff: noqa: F811

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from src.server.database.drive import reads, retention, writes
from src.server.database.drive import trash as trash_rows
from src.server.database.drive import uploads as rows
from src.server.database.drive.keys import path_key
from src.server.database.drive.records import Actor, DriveError
from src.server.database.drive.writes import ContentRef
from tests.integration.drive_helpers import (  # noqa: F401
    LIMITS,
    MIB,
    counts_of,
    make_due,
    mark,
    person_of,
    ref_of,
    save_text,
    space,
    waiting_on_a_lock,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# --- the counts (S10) ---------------------------------------------------------


async def test_the_counts_follow_every_change(space, db_conn):
    actor = person_of(space)
    await save_text(space, "a/one.bin", "one")
    await save_text(space, "a/two.bin", "two")
    await save_text(space, "a/two.bin", "two-b")  # a second version
    await save_text(space, "b.bin", "one")  # bytes shared with a/one.bin
    await save_text(space, "c.bin", "c", sitting="s1")
    await save_text(space, "c.bin", "c-folded", sitting="s1")  # folded into one version
    async with writes.locked(space.space_id, LIMITS) as tx:
        folder = await trash_rows.trash(tx, "a", actor, None, False)
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.restore(tx, folder)
        await tx.rename("b.bin", "c.bin", actor, replace=True)
        await retention.clear(tx, await tx.live("a/two.bin"), None)
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "a/one.bin", actor, None, False)
        await trash_rows.purge(tx, None, None)
        await retention.free_released(tx)
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted
    live, trash, entries = await reads.usage(space.space_id)
    assert entries == counted["entry_count"]
    assert live == counted["held_bytes"] - counted["released_bytes"] - counted["gone_bytes"]


async def test_a_drift_in_the_counts_is_healed_and_reported(space, db_conn):
    await save_text(space, "a.bin", "one")
    await save_text(space, "b.bin", "two")
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_spaces SET held_bytes = held_bytes + 7, entry_count = 0 WHERE space_id = %s",
            (space.space_id,),
        )
        await cur.execute(
            "UPDATE drive_contents SET live_refs = 0 WHERE space_id = %s AND sha256 = 'sha-one'",
            (space.space_id,),
        )
    async with writes.locked(space.space_id, LIMITS) as tx:
        drift = await retention.reconcile(tx)
    assert drift["entry_count"] == -2 and drift["content_refs"] == 1
    # The usage a client read was wrong: the healing is a change it is told of.
    assert tx.changed
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert await retention.reconcile(tx) == {}
    assert not tx.changed


async def test_upkeep_is_never_refused_for_room(space, db_conn, monkeypatch):
    """A space over its allowance, counted low by a drift: the recount
    raises its live bytes, which a change would be refused for; upkeep adds
    nothing, so it commits, the drift healed."""
    from src.server.services import drive

    await save_text(space, "a.bin", "x" * 100)
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_spaces SET held_bytes = held_bytes - 90 WHERE space_id = %s",
            (space.space_id,),
        )
    tight = dataclasses.replace(LIMITS, max_space_bytes=50)
    monkeypatch.setattr(drive, "_space_limits", AsyncMock(return_value=tight))
    await drive.maintain(space.space_id)
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted


async def test_the_sweep_finds_a_space_by_its_due_undo_copies(space, db_conn):
    await save_text(space, "u.bin", "1")
    await save_text(space, "u.bin", "2")
    assert space.space_id not in await reads.spaces_to_maintain(14, 3600, 500)
    await make_due(db_conn, "u.bin", space)
    assert space.space_id in await reads.spaces_to_maintain(14, 3600, 500)


async def test_the_sweep_takes_first_the_spaces_due_longest(space, db_conn):
    """More spaces due than a pass takes: the one whose undo copy has
    waited longest is in it, so none is passed over pass after pass."""
    await save_text(space, "u.bin", "1")
    await save_text(space, "u.bin", "2")
    await make_due(db_conn, "u.bin", space)
    async with db_conn.cursor() as cur:
        await cur.execute("INSERT INTO drive_spaces (kind) VALUES ('team') RETURNING space_id::text AS id")
        other = (await cur.fetchone())["id"]
        try:
            await cur.execute(
                "INSERT INTO drive_entries (space_id, kind, name, parent, path, path_key,"
                " changed_seq, undo_until) VALUES (%s, 'file', 'o.bin', '', 'o.bin', 'o.bin', 1,"
                " now() - interval '1 hour')",
                (other,),
            )
            assert await reads.spaces_to_maintain(14, 3600, 1) == [other]
            await cur.execute(
                "UPDATE drive_entries SET undo_until = now() - interval '2 hours'"
                " WHERE space_id = %s AND path = 'u.bin'",
                (space.space_id,),
            )
            assert await reads.spaces_to_maintain(14, 3600, 1) == [space.space_id]
        finally:
            await cur.execute("DELETE FROM drive_spaces WHERE space_id = %s", (other,))


# --- replace and the bound (S5, A6) -------------------------------------------------


async def test_a_replaced_file_with_history_goes_to_the_trash(space):
    actor = person_of(space)
    await save_text(space, "draft.txt", "v1")
    await save_text(space, "draft.txt", "v2")
    await save_text(space, "final.txt", "old")
    async with writes.locked(space.space_id, LIMITS) as tx:
        gone = (await tx.rename("draft.txt", "final.txt", actor, replace=True)).ended_id
    trash = await reads.trashed(space.space_id, None)
    assert [t.entry.entry_id for t in trash] == [gone]
    # Its earlier bytes are counted as the trash's.
    _, trashed, _ = await reads.usage(space.space_id)
    assert trashed == len("v1")


async def test_a_replaced_file_with_no_history_leaves_only_a_notice(space):
    actor = person_of(space)
    await save_text(space, "tmp.txt", "new")
    await save_text(space, "final.txt", "old")
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.rename("tmp.txt", "final.txt", actor, replace=True)
    assert await reads.trashed(space.space_id, None) == []
    _, trashed, _ = await reads.usage(space.space_id)
    assert trashed == 0


async def test_the_bound_purges_only_trash_that_holds_bytes_of_its_own(space):
    actor = person_of(space)
    await save_text(space, "shared.bin", "x" * 100)
    await save_text(space, "copy.bin", "x" * 100)  # the same bytes, still live elsewhere
    await save_text(space, "own.bin", "y" * 100)
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "copy.bin", actor, None, False)  # the oldest, holding nothing of its own
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "own.bin", actor, None, False)
    tight = dataclasses.replace(LIMITS, max_trash_bytes=50)
    async with writes.locked(space.space_id, tight) as tx:
        await trash_rows.bound(tx)
    left = [t.entry.path for t in await reads.trashed(space.space_id, None)]
    assert left == ["copy.bin"]


async def test_the_bound_takes_a_memo_in_the_trash_only_after_every_other_file(space):
    actor = person_of(space)
    await save_text(space, "memo.md", "m" * 100)
    await mark(space, "memo.md")
    await save_text(space, "big.bin", "b" * 100)
    for path in ("memo.md", "big.bin"):  # the memo is the oldest in the trash
        async with writes.locked(space.space_id, LIMITS) as tx:
            await trash_rows.trash(tx, path, actor, None, False)
    tight = dataclasses.replace(LIMITS, max_trash_bytes=150)
    async with writes.locked(space.space_id, tight) as tx:
        await trash_rows.bound(tx)
    assert [t.entry.path for t in await reads.trashed(space.space_id, None)] == ["memo.md"]


async def test_the_bound_leaves_a_folder_deleted_with_a_memo_in_it_until_last(space):
    """A purge takes the whole trash item, so an ordinary file deleted with
    a memo waits as the memo does."""
    actor = person_of(space)
    await save_text(space, "d/old.bin", "o" * 100)
    await save_text(space, "d/memo.md", "m" * 100)
    await mark(space, "d/memo.md")
    await save_text(space, "new.bin", "n" * 100)
    for path in ("d", "new.bin"):  # the folder is the oldest in the trash
        async with writes.locked(space.space_id, LIMITS) as tx:
            await trash_rows.trash(tx, path, actor, None, False)
    tight = dataclasses.replace(LIMITS, max_trash_bytes=250)
    async with writes.locked(space.space_id, tight) as tx:
        await trash_rows.bound(tx)
    assert [t.entry.path for t in await reads.trashed(space.space_id, None)] == ["d"]


# --- clearing history (S3) ----------------------------------------------------------


async def test_a_clear_drops_the_old_versions_there_when_it_takes_the_lock(space):
    await save_text(space, "f.bin", "1")
    await mark(space, "f.bin")
    for text in ("2", "3"):
        await save_text(space, "f.bin", text)
    entry = await reads.live_entry(space.space_id, path="f.bin")
    async with writes.locked(space.space_id, LIMITS) as tx:
        dropped, _ = await retention.clear(tx, entry, 2)
    assert dropped == 1
    async with writes.locked(space.space_id, LIMITS) as tx:
        dropped, _ = await retention.clear(tx, entry, None)
    assert dropped == 1
    left = [v.seq for v in (await reads.versions(entry.entry_id, LIMITS.history))[0]]
    assert left == [3]


async def test_a_head_too_large_to_keep_still_lists_its_older_versions(space):
    """CR147: the SQL-only flag must leave every row, not only the head's."""
    from src.server.database.drive.records import HistoryRule

    for text in ("1", "22", "333333"):
        await save_text(space, "large.bin", text)
    entry = await reads.live_entry(space.space_id, path="large.bin")
    found, large = await reads.versions(entry.entry_id, HistoryRule(max_bytes=4))
    assert [v.seq for v in found] == [3, 2]
    assert large is True


async def test_a_text_page_reads_its_bytes_from_the_snapshot_it_was_sized_in(space, monkeypatch):
    """CR146: a head replaced between sizing and reading must not grow the page."""

    async def save_text(path: str, text: str):
        data = text.encode()
        ref = ContentRef(f"sha-{text}", len(data), True, "text/plain")
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.save(path, ref, person_of(space), data=data)

    await save_text("page/a.txt", "a" * 10)
    await save_text("page/b.txt", "b" * 10)
    real, calls = reads.fetch, []

    async def between(sql, params, conn=None):
        found = await real(sql, params, conn)
        calls.append(sql)
        if len(calls) == 1:
            last = max(r["entry_id"] for r in found)
            entry = await reads.live_entry(space.space_id, entry_id=last)
            await save_text(entry.path, "c" * 1000)
        return found

    monkeypatch.setattr(reads, "fetch", between)
    page = await reads.texts(space.space_id, None, None, 10, 25, under="page")
    assert sum(len(data) for _, data in page) <= 25


# --- the stored spelling of a parent (CR149, CR039) ------------------------------------

#: Four 250-byte names: with "ss/" before them, 1006 bytes; with "ẞ/", 1007.
_DEEP = "/".join(c * 250 for c in "wxyz")


async def test_a_save_under_a_longer_stored_spelling_is_held_to_the_path_limit(space):
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("ẞ")
    asked = f"ss/{_DEEP}/" + "n" * 17
    assert len(asked.encode()) == 1024
    with pytest.raises(DriveError) as refused:
        await save_text(space, asked, "one")
    assert refused.value.code == "invalid_path"
    assert await reads.live_entry(space.space_id, path=asked) is None


async def test_a_folder_move_is_measured_against_the_stored_destination(space):
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("ẞ")
    rest = "/".join(c * 250 for c in "wxy") + "/" + "n" * 247
    await save_text(space, f"src/{rest}", "one")
    moved_to = "ss/" + "a" * 20
    assert len(f"{moved_to}/{rest}".encode()) == 1024
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.move("src", moved_to)
    assert refused.value.code == "invalid_path"
    assert await reads.live_entry(space.space_id, path="src") is not None


# --- one digest under names of two kinds -------------------------------------------------

_SAME = b"a,b\n1,2\n"
_SAME_SHA = "5a" * 32


def _as(text: bool) -> ContentRef:
    return ContentRef(_SAME_SHA, len(_SAME), text, "text/plain" if text else "text/csv")


async def _kinds(space) -> dict[str, tuple[bool, bool]]:
    """Each file's kind as its version says it and as its content says it."""
    out = {}
    for path in ("same/notes.md", "same/data.csv"):
        entry = await reads.live_entry(space.space_id, path=path)
        found = await reads.content_of(space.space_id, entry.entry_id, entry.head_seq)
        out[path] = (entry.is_text, found.is_text)
    return out


async def _searched(space) -> list[str]:
    page = await reads.texts(space.space_id, None, None, 10, 1 << 20, under="same")
    return sorted(entry.path for entry, _ in page)


async def test_held_text_saved_under_a_data_name_keeps_the_data_kind(space):
    """Text bytes first, then the same bytes as a data file the store would
    hold: the data file is not read or searched as text."""
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.save("same/notes.md", _as(True), person_of(space), data=_SAME)
        await tx.save("same/data.csv", _as(False), person_of(space), blob_user_id=space.user_id)
    assert await _kinds(space) == {"same/notes.md": (True, True), "same/data.csv": (False, False)}
    assert await _searched(space) == ["same/notes.md"]
    entry = await reads.live_entry(space.space_id, path="same/data.csv")
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert (await tx.stored(entry.entry_id, entry.head_seq)).is_text is False


async def test_held_data_saved_under_a_text_name_is_kept_as_text(space):
    """The data file's bytes first, in the store, then the same bytes as
    text: the text file is searched, its bytes now held as text."""
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.save("same/data.csv", _as(False), person_of(space), blob_user_id=space.user_id)
        await tx.save("same/notes.md", _as(True), person_of(space), data=_SAME)
    assert await _kinds(space) == {"same/notes.md": (True, True), "same/data.csv": (False, False)}
    assert await _searched(space) == ["same/notes.md"]
    found = await reads.content_by_sha(space.space_id, _SAME_SHA)
    assert found.blob_user_id is None
    page = await reads.texts(space.space_id, None, None, 10, 1 << 20, under="same")
    assert [data for _, data in page] == [_SAME]


async def test_a_clear_reaches_the_feed_as_the_file_unchanged(space):
    """Other clients read a file's versions and the usage again on a change
    to it; a clear changes both, so it is one, with the head as it was."""
    for text in ("1", "2"):
        await save_text(space, "fed.bin", text)
    entry = await reads.live_entry(space.space_id, path="fed.bin")
    before = await reads.cursor(space.space_id)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.clear(tx, entry, None))[0] == 1
    found = await reads.changes(space.space_id, before)
    assert [(e.entry_id, e.sha256, e.head_seq, e.modified_at) for e in found.entries] == [
        (entry.entry_id, entry.sha256, entry.head_seq, entry.modified_at)
    ]
    # Nothing left to clear: no change.
    after = await reads.cursor(space.space_id)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.clear(tx, entry, None))[0] == 0
    assert await reads.cursor(space.space_id) == after


# --- upkeep a client can see moves the cursor ---------------------------------------------


@pytest.fixture
def announced(monkeypatch):
    """The service's upkeep, with this space's limits and the feed's wake counted."""
    from src.server.services import drive

    woken = AsyncMock()
    monkeypatch.setattr(drive, "_space_limits", AsyncMock(return_value=LIMITS))
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=LIMITS))
    monkeypatch.setattr(drive.feed, "announce", woken)
    return woken


async def test_a_purge_moves_the_cursor_with_nothing_to_list(space, announced):
    from src.server.services import drive

    await save_text(space, "gone.bin", "x")
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "gone.bin", person_of(space), None, False)
    before = await reads.cursor(space.space_id)
    assert await drive.purge(space) == 1
    found = await reads.changes(space.space_id, before)
    assert (found.entries, found.resync) == ([], False)
    assert found.cursor > before
    announced.assert_awaited_once()


async def test_a_sweep_that_drops_an_undo_copy_moves_the_cursor(space, announced, db_conn):
    from src.server.services import drive

    for text in ("1", "2"):
        await save_text(space, "trim.bin", text)
    await make_due(db_conn, "trim.bin", space)
    before = await reads.cursor(space.space_id)
    await drive.maintain(space.space_id)
    found = await reads.changes(space.space_id, before)
    assert [e.path for e in found.entries] == ["trim.bin"]
    assert found.entries[0].undo_until is None
    entry = await reads.live_entry(space.space_id, path="trim.bin")
    assert [v.seq for v in (await reads.versions(entry.entry_id, LIMITS.history))[0]] == [2]
    announced.assert_awaited_once()


async def test_a_sweep_that_changes_nothing_a_client_sees_keeps_the_cursor(
    space, announced, monkeypatch
):
    """Freeing released copies moves no count a client reads."""
    from src.server.services import drive

    for text in ("1", "2"):
        await save_text(space, "freed.bin", text)
    entry = await reads.live_entry(space.space_id, path="freed.bin")
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        await retention.clear(tx, entry, None)
    usage = await reads.usage(space.space_id)
    before = await reads.cursor(space.space_id)
    graceless = dataclasses.replace(LIMITS.history, grace_seconds=0)
    monkeypatch.setattr(
        drive, "_space_limits", AsyncMock(return_value=dataclasses.replace(LIMITS, history=graceless))
    )
    await drive.maintain(space.space_id)
    assert await reads.cursor(space.space_id) == before
    assert await reads.usage(space.space_id) == usage
    announced.assert_not_awaited()


# --- restore (S8) ----------------------------------------------------------------------


async def test_a_trash_item_with_two_generations_of_a_path_restores(space):
    actor = person_of(space, sitting="burst")
    await save_text(space, "d/x.txt", "first")
    async with writes.locked(space.space_id, LIMITS) as tx:
        first = await trash_rows.trash(tx, "d/x.txt", actor, None, False)
    await save_text(space, "d/x.txt", "second")
    async with writes.locked(space.space_id, LIMITS) as tx:
        second = await trash_rows.trash(tx, "d", actor, None, False)
    assert first == second  # one burst, one item
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await trash_rows.restore(tx, first)
    assert sorted(e.path for e in back) == ["d", "d/x (1).txt", "d/x.txt"]


async def _burst(space, paths: list[str]) -> str:
    """``rm -r`` as a computer's mount sends it: one delete a path, deepest
    first, one sitting, so the trash joins them into one item."""
    actor = person_of(space, sitting="rm-r")
    ids = set()
    for path in paths:
        async with writes.locked(space.space_id, LIMITS) as tx:
            ids.add(await trash_rows.trash(tx, path, actor, None, False))
    assert len(ids) == 1
    return ids.pop()


async def _restored(space, trash_id: str) -> list[str]:
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await trash_rows.restore(tx, trash_id)
    return sorted(e.path for e in back)


async def test_a_burst_restored_beside_its_replacement_keeps_its_children(space):
    await save_text(space, "d/x.txt", "x")
    item = await _burst(space, ["d/x.txt", "d"])
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("d")
    before = await reads.cursor(space.space_id)
    assert await _restored(space, item) == ["d (1)", "d (1)/x.txt"]
    assert await reads.live_entry(space.space_id, path="d/x.txt") is None
    fed = await reads.changes(space.space_id, before)
    assert sorted(e.path for e in fed.entries) == ["d (1)", "d (1)/x.txt"]


async def test_a_deeper_burst_follows_each_renamed_ancestor(space):
    await save_text(space, "d/e/x.txt", "x")
    item = await _burst(space, ["d/e/x.txt", "d/e", "d"])
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("d")
    assert await _restored(space, item) == ["d (1)", "d (1)/e", "d (1)/e/x.txt"]


async def test_a_burst_under_a_renamed_middle_folder_lands_in_it(space):
    await save_text(space, "d/e/x.txt", "x")
    item = await _burst(space, ["d/e/x.txt", "d/e"])
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("d/e")
    assert await _restored(space, item) == ["d/e (1)", "d/e (1)/x.txt"]


async def test_a_child_that_collides_under_its_renamed_parent_goes_beside(space):
    """Two generations of ``d/x.txt`` in one burst: the later comes back with
    ``d``, the earlier beside it, both under the renamed ``d``."""
    await save_text(space, "d/x.txt", "first")
    actor = person_of(space, sitting="rm-r")
    async with writes.locked(space.space_id, LIMITS) as tx:
        item = await trash_rows.trash(tx, "d/x.txt", actor, None, False)
    await save_text(space, "d/x.txt", "second")
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert await trash_rows.trash(tx, "d", actor, None, False) == item
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.mkdir("d")
    assert await _restored(space, item) == ["d (1)", "d (1)/x (1).txt", "d (1)/x.txt"]


# --- uploads (S1, S2, S7, A1, A3, A4, A7) --------------------------------------------------


async def _upload(space, path="big.bin", *, sha="ab" * 32, if_match=None, if_none_match=False, actor=None):
    upload_id = str(uuid.uuid4())
    return await rows.create(
        {
            "upload_id": upload_id,
            "space_id": space.space_id,
            "user_id": space.user_id,
            "path": path,
            "path_key": path_key(path),
            "size": 64 * MIB,
            "part_size": 16 * MIB,
            "part_count": 4,
            "sha256": sha,
            "content_type": "application/octet-stream",
            "note": None,
            "if_match": if_match,
            "if_none_match": if_none_match,
            "actor": dataclasses.asdict(actor or person_of(space)),
            "mode": "direct",
            "object_key": f"uploads/{space.user_id}/{upload_id}",
            "store_upload_id": "store-id",
            "report": None,
        },
        max_open=8,
    )


def _finish_without_a_store(monkeypatch, space):
    """The steps after the read-back, with the store's part answered here."""
    from src.server.services import drive
    from src.server.services.drive import uploads

    monkeypatch.setattr(uploads._bytes, "registered", AsyncMock(return_value=True))
    monkeypatch.setattr(drive, "limits", AsyncMock(return_value=LIMITS))
    monkeypatch.setattr(uploads, "_clean", AsyncMock())
    monkeypatch.setattr(uploads, "_report", AsyncMock())
    return uploads


async def _lapse(upload_id: str) -> None:
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE drive_uploads SET lease_until = now() - interval '1 second' WHERE upload_id = %s",
            (upload_id,),
        )


async def test_a_worker_that_lost_its_lease_records_nothing(space, monkeypatch):
    uploads = _finish_without_a_store(monkeypatch, space)
    row = await _upload(space)
    mine = await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    await _lapse(row["upload_id"])
    theirs = await rows.reclaim(row["upload_id"], timedelta(minutes=2))
    assert theirs["lease_token"] != mine["lease_token"]

    with pytest.raises(uploads._LeaseLost):
        await uploads._record(uploads._Lease(mine), space, "ab" * 32, [])
    assert await reads.live_entry(space.space_id, path="big.bin") is None
    assert (await rows.get(row["upload_id"]))["state"] == "verifying"

    await uploads._record(uploads._Lease(theirs), space, "ab" * 32, [])
    done = await rows.get(row["upload_id"])
    assert done["state"] == "done" and done["entry_id"] is not None


async def test_a_cancel_while_reading_back_stops_the_version(space, monkeypatch):
    from src.server.services.drive import uploads as service

    uploads = _finish_without_a_store(monkeypatch, space)
    row = await _upload(space)
    mine = await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    out = await service.cancel(space, row["upload_id"])
    assert out["state"] == "aborted"
    with pytest.raises(uploads._LeaseLost):
        await uploads._record(uploads._Lease(mine), space, "ab" * 32, [])
    assert await reads.live_entry(space.space_id, path="big.bin") is None


async def test_a_cancel_after_the_save_says_it_came_too_late(space, monkeypatch):
    from src.server.services.drive import uploads as service

    uploads = _finish_without_a_store(monkeypatch, space)
    row = await _upload(space)
    mine = await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    await uploads._record(uploads._Lease(mine), space, "ab" * 32, [])
    with pytest.raises(DriveError) as refused:
        await service.cancel(space, row["upload_id"])
    assert refused.value.code == "upload_ended"
    assert refused.value.details == {"state": "done"}


async def _find(
    space,
    *,
    if_match=None,
    if_none_match=False,
    actor=None,
    report=None,
    relay=False,
    content_type="application/octet-stream",
):
    return await rows.find_open(
        space.space_id,
        path_key("big.bin"),
        64 * MIB,
        "ab" * 32,
        space.user_id,
        content_type=content_type,
        if_match=if_match,
        if_none_match=if_none_match,
        actor=dataclasses.asdict(actor or person_of(space)),
        note=None,
        report=report,
        relay=relay,
    )


async def test_a_begin_that_must_relay_does_not_resume_a_direct_upload(space):
    """CR152: and leaves the direct one as it was."""
    row = await _upload(space)
    assert await _find(space, relay=True, report={"call_id": "relay"}) is None
    assert (await rows.get(row["upload_id"]))["report"] is None
    assert (await _find(space))["upload_id"] == row["upload_id"]


async def test_a_begin_under_other_conditions_does_not_resume(space):
    await _upload(space, if_match=None)
    # CR036: the type asked for is the version's, so another one starts afresh.
    assert await _find(space, content_type="text/csv") is None
    assert await _find(space, if_none_match=True) is None
    assert await _find(space, if_match="cd" * 32) is None
    assert await _find(space) is not None


async def test_whoever_resumes_owns_the_upload(space):
    row = await _upload(space)
    agent = Actor(kind="agent", user_id=space.user_id, via="mount", sitting="call:2")
    found = await _find(space, actor=agent, report={"computer_id": "c", "call_id": "2", "path": "p"})
    assert found["upload_id"] == row["upload_id"]
    assert rows.actor_of(found)["kind"] == "agent"
    assert rows.report_of(found)["call_id"] == "2"


async def test_a_begin_does_not_resume_an_upload_that_ended_while_it_waited(space, test_db_pool):
    """CR148: the row the lookup chose must still be resumable once its lock is had."""
    row = await _upload(space)
    await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    async with test_db_pool.connection() as finisher:
        async with finisher.transaction():
            await finisher.execute(
                "UPDATE drive_uploads SET state = 'done' WHERE upload_id = %s",
                (row["upload_id"],),
            )
            found = asyncio.create_task(_find(space, report={"call_id": "late"}))
            await waiting_on_a_lock(test_db_pool, finisher)
        assert await found is None
    ended = await rows.get(row["upload_id"])
    assert ended["state"] == "done" and ended["report"] is None


async def test_an_upload_read_back_past_its_deadline_still_counts_as_open(space, test_db_pool):
    """CR035: a ``verifying`` row ends done or failed, never by its deadline."""
    row = await _upload(space)
    await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE drive_uploads SET expires_at = now() - interval '1 second' WHERE upload_id = %s",
            (row["upload_id"],),
        )
    with pytest.raises(rows.TooManyUploads):
        other = str(uuid.uuid4())
        await rows.create(
            {
                **row,
                "upload_id": other,
                "path_key": path_key(row["path"]),
                "object_key": f"uploads/{space.user_id}/{other}",
            },
            max_open=1,
        )


async def test_a_begin_meets_an_upload_being_read_back(space):
    row = await _upload(space)
    await rows.claim_verify(row["upload_id"], timedelta(minutes=2))
    found = await _find(space)
    assert found is not None
    assert (found["upload_id"], found["state"]) == (row["upload_id"], "verifying")


async def test_the_expiry_sweep_spares_an_upload_renewed_meanwhile(space, test_db_pool):
    row = await _upload(space)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE drive_uploads SET expires_at = now() - interval '1 second' WHERE upload_id = %s",
            (row["upload_id"],),
        )
    async with test_db_pool.connection() as renewer:
        async with renewer.transaction():
            await renewer.execute(
                "UPDATE drive_uploads SET expires_at = now() + interval '1 day' WHERE upload_id = %s",
                (row["upload_id"],),
            )
            sweep = asyncio.create_task(rows.expired(10))
            await waiting_on_a_lock(test_db_pool, renewer)
        swept = await sweep
    assert swept == []
    assert (await rows.get(row["upload_id"]))["state"] == "uploading"


# --- the check before bytes move (A5, S4, S17) ---------------------------------------------


async def _room(space, path, ref, actor, limits):
    from src.server.services import drive

    await drive.room_for(
        space, path, ref, actor, if_match=None, if_none_match=False, bounds=limits
    )


async def test_the_check_before_a_transfer_credits_bytes_the_space_holds(space):
    await save_text(space, "a.bin", "z" * 100)
    full = dataclasses.replace(LIMITS, max_space_bytes=150)
    shared, _ = ref_of("z" * 100)
    await _room(space, "b.bin", shared, person_of(space), full)  # nothing new is kept
    other, _ = ref_of("w" * 100)
    with pytest.raises(DriveError) as refused:
        await _room(space, "b.bin", other, person_of(space), full)
    assert refused.value.code == "over_quota"


async def test_the_check_before_a_transfer_folds_a_sitting_as_the_save_will(space):
    await save_text(space, "a.bin", "z" * 100, sitting="s")
    full = dataclasses.replace(LIMITS, max_space_bytes=150)
    again, _ = ref_of("y" * 100)
    await _room(space, "a.bin", again, person_of(space, "s"), full)  # replaces the head's copy
    with pytest.raises(DriveError):
        await _room(space, "a.bin", again, person_of(space, "other"), full)


async def test_the_check_before_a_transfer_rolls_back(space, db_conn):
    ref, _ = ref_of("q" * 10)
    await _room(space, "new.bin", ref, person_of(space), LIMITS)
    assert await reads.live_entry(space.space_id, path="new.bin") is None
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted and kept["held_bytes"] == 0


async def test_content_freed_since_it_was_found_is_asked_for_again(space):
    await save_text(space, "a.bin", "gone")
    actor = person_of(space)
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "a.bin", actor, None, False)
        await trash_rows.purge(tx, None, None)
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert await retention.free_released(tx) == len("gone")
    ref, _ = ref_of("gone")
    with pytest.raises(writes.ContentGone):
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.save("b.bin", ref, actor)


async def _registered(owner: str, sha: str) -> int | None:
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT byte_len FROM workspace_file_blobs WHERE user_id = %s AND sha256 = %s",
            (owner, sha),
        )
        found = await cur.fetchone()
    return None if found is None else found["byte_len"]


async def _condemned(owner: str, sha: str):
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT condemned_at FROM workspace_file_blobs WHERE user_id = %s AND sha256 = %s",
            (owner, sha),
        )
        return (await cur.fetchone())["condemned_at"]


async def test_a_registration_nothing_used_is_left_to_the_sweep_not_deleted(space, monkeypatch):
    """Another writer may be putting the same bytes under the same key right
    now (workspace sync uploads before it registers), so letting a
    registration go deletes nothing: it condemns the row, and the sweep's
    claim and grace decide."""
    from src.server.database import workspace_file_blobs as blobs
    from src.server.database.blob_keys import REGISTER_SQL
    from src.server.database.pool import get_db_connection

    deleted = []
    monkeypatch.setattr(blobs, "_storage_delete_object", lambda key: deleted.append(key) or True)
    monkeypatch.setattr(blobs, "_storage_copy_object", lambda src, dst, size: True)
    owner = space.user_id
    stamp = await blobs.store_blob_by_copy(owner, "cd" * 32, "uploads/x", 10)
    assert await blobs.release_blob(owner, "cd" * 32, stamp) is True
    assert deleted == []
    assert await _condemned(owner, "cd" * 32) is not None
    async with get_db_connection() as conn:  # the other writer's upload lands and registers
        await conn.execute(REGISTER_SQL, (owner, "cd" * 32, 10))
    assert await _condemned(owner, "cd" * 32) is None

    stamp = await blobs.store_blob_by_copy(owner, "ef" * 32, "uploads/y", 10)
    async with get_db_connection() as conn:  # another writer's touch: it wants the blob since
        await conn.execute(blobs._TOUCH_SQL, (owner, ["ef" * 32]))
    assert await blobs.release_blob(owner, "ef" * 32, stamp) is False
    assert await _condemned(owner, "ef" * 32) is None
    assert deleted == []


# --- a takeover (S6, A2) --------------------------------------------------------------------


async def test_a_takeover_assembles_what_its_lapsed_worker_never_did(space, monkeypatch):
    from src.server.services import store_multipart
    from src.server.services.drive import uploads

    _finish_without_a_store(monkeypatch, space)
    assembled = []
    monkeypatch.setattr(
        uploads,
        "assemble_parts",
        lambda upload: assembled.append(upload.key) or store_multipart.Assembly(ok=True),
    )
    monkeypatch.setattr(uploads, "sha256_object", lambda key, deadline, stop=None: "ab" * 32)
    row = await _upload(space)
    await rows.claim_verify(row["upload_id"], timedelta(minutes=2))  # a worker that then died
    await _lapse(row["upload_id"])
    taken = await rows.reclaim(row["upload_id"], timedelta(minutes=2))
    await uploads._finish(taken)
    assert assembled == [row["object_key"]]
    done = await rows.get(row["upload_id"])
    assert done["state"] == "done" and done["assembled"]


# --- the blob registry keeps the Drive's bytes (S18) ------------------------------------------


async def test_a_sweep_that_does_not_know_the_drive_cannot_take_its_bytes(space, db_conn):
    """The previous release's condemn and reap, as statements: one update
    over every candidate, then a delete per row."""
    drive_sha, other_sha = "a" * 64, "b" * 64
    async with db_conn.cursor() as cur:
        for sha in (drive_sha, other_sha):
            await cur.execute(
                "INSERT INTO workspace_file_blobs (user_id, sha256, byte_len, last_referenced_at)"
                " VALUES (%s, %s, 1, now() - interval '10 days')",
                (space.user_id, sha),
            )
        await cur.execute(
            "INSERT INTO drive_contents (space_id, sha256, byte_len, is_text, blob_user_id)"
            " VALUES (%s, %s, 1, false, %s)",
            (space.space_id, drive_sha, space.user_id),
        )
        await cur.execute(
            "UPDATE workspace_file_blobs SET condemned_at = now()"
            " WHERE condemned_at IS NULL AND last_referenced_at < now() - interval '7 days'"
        )
        assert cur.rowcount == 1
        await cur.execute(
            "DELETE FROM workspace_file_blobs WHERE user_id = %s", (space.user_id,)
        )
        assert cur.rowcount == 1
        await cur.execute(
            "SELECT sha256, condemned_at FROM workspace_file_blobs WHERE user_id = %s",
            (space.user_id,),
        )
        assert [(r["sha256"], r["condemned_at"]) for r in await cur.fetchall()] == [(drive_sha, None)]
