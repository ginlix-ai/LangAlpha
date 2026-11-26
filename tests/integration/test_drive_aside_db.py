"""The record stays with the name, against real PostgreSQL: renames through a
computer's mount of memos and ordinary files (``drive/aside.py``)."""

# The ``space`` fixture comes from drive_helpers; each test takes it by name.
# ruff: noqa: F811

from __future__ import annotations

import dataclasses

import pytest

from src.server.database.drive import changes, reads, retention, writes
from src.server.database.drive import trash as trash_rows
from src.server.database.drive.records import DriveError
from tests.integration.drive_helpers import (  # noqa: F401
    LIMITS,
    agent_of,
    kept_seqs,
    mark,
    person_of,
    ref_of,
    save_text,
    space,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _mount(space, command="call:sed-1", sitting="turn"):
    return dataclasses.replace(agent_of(space, "mount", sitting), command=command)


async def _write(space, path, text, actor, **conditions):
    ref, data = ref_of(text)
    async with writes.locked(space.space_id, LIMITS) as tx:
        return await tx.save(path, ref, actor, data=data, **conditions)


async def _rename(space, src, dst, actor, replace=False):
    async with writes.locked(space.space_id, LIMITS) as tx:
        return await tx.rename(src, dst, actor, replace=replace)


async def _trash(space, path, actor):
    async with writes.locked(space.space_id, LIMITS) as tx:
        return await trash_rows.trash(tx, path, actor, None, False)


async def _memo(space, path, text="old"):
    await save_text(space, path, text)
    await mark(space, path)
    return await reads.live_entry(space.space_id, path=path)


async def _at(space, path):
    return await reads.live_entry(space.space_id, path=path)


async def test_a_memo_renamed_aside_and_written_again_is_still_the_memo(space, db_conn):
    memo = await _memo(space, "n.md")
    sed = _mount(space)
    await _rename(space, "n.md", "n.md.bak", sed)
    entry = await _write(space, "n.md", "new", sed, if_none_match=True)
    assert entry.entry_id == memo.entry_id and entry.is_memo and entry.sha256 == "sha-new"
    assert await kept_seqs(space, "n.md") == [2, 1]
    backup = await _at(space, "n.md.bak")
    assert backup.entry_id != memo.entry_id and not backup.has_record
    assert backup.sha256 == "sha-old" and await kept_seqs(space, "n.md.bak") == [1]
    # The note is taken up: a later write at the name in the command is a save.
    async with db_conn.cursor() as cur:
        await cur.execute(
            "SELECT aside_key, aside_command, aside_at FROM drive_memos WHERE entry_id = %s",
            (memo.entry_id,),
        )
        assert set((await cur.fetchone()).values()) == {None}


async def test_the_backup_removed_first_is_an_ordinary_file_in_that_delete(space):
    memo = await _memo(space, "r.md")
    sed = _mount(space)
    await _rename(space, "r.md", "r.md.bak", sed)
    trash_id = await _trash(space, "r.md.bak", sed)
    entry = await _write(space, "r.md", "new", sed)
    assert entry.entry_id == memo.entry_id and entry.is_memo and entry.head_seq == 2
    item = [t for t in await reads.trashed(space.space_id, None) if t.trash_id == trash_id]
    assert len(item) == 1 and item[0].entry.path == "r.md.bak"
    assert item[0].entry.entry_id != memo.entry_id and not item[0].entry.has_record
    assert item[0].entry.sha256 == "sha-old"


async def test_a_memo_the_person_put_in_the_trash_meanwhile_stays_there(space):
    memo = await _memo(space, "u.md")
    sed = _mount(space)
    await _rename(space, "u.md", "u.md.bak", sed)
    trash_id = await _trash(space, "u.md.bak", person_of(space))
    entry = await _write(space, "u.md", "new", sed, if_none_match=True)
    assert entry.entry_id != memo.entry_id and not entry.has_record
    item = [t for t in await reads.trashed(space.space_id, None) if t.trash_id == trash_id]
    assert len(item) == 1 and item[0].entry.entry_id == memo.entry_id


async def test_the_backup_in_the_trash_is_restored_apart_from_a_file_trashed_by_the_same_save(space):
    """The backup removed, then a file with earlier versions renamed onto
    the name: both are in one trash item, each its own delete."""
    memo = await _memo(space, "k.md")
    sed = _mount(space)
    await _write(space, "new.md", "draft", sed)
    await _write(space, "new.md", "final", _mount(space, sitting="later"))
    await _rename(space, "k.md", "k.md.bak", sed)
    trash_id = await _trash(space, "k.md.bak", sed)
    r = await _rename(space, "new.md", "k.md", sed)
    assert r.entry.entry_id == memo.entry_id and r.entry.sha256 == "sha-final"
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await trash_rows.restore(tx, trash_id)
    assert sorted((e.path, e.sha256) for e in back) == [("k.md.bak", "sha-old"), ("new.md", "sha-final")]


async def test_a_temp_file_renamed_onto_the_name_lands_as_the_memos_version(space):
    memo = await _memo(space, "t.md")
    sed = _mount(space)
    await _rename(space, "t.md", "t.md.bak", sed)
    temp = await _write(space, "sedTEMP", "new", sed)
    r = await _rename(space, "sedTEMP", "t.md", sed)
    assert (r.entry.entry_id, r.ended_id, r.carried) == (memo.entry_id, temp.entry_id, False)
    assert r.entry.sha256 == "sha-new" and await kept_seqs(space, "t.md") == [2, 1]
    assert (await _at(space, "t.md.bak")).sha256 == "sha-old"
    assert await _at(space, "sedTEMP") is None


async def test_a_folder_renamed_onto_the_name_moves_there_and_the_memo_stays_aside(space):
    """A folder is never a memo's new bytes, so it takes the free name."""
    memo = await _memo(space, "f.md")
    await save_text(space, "staged/part.md", "x")
    mv = _mount(space)
    await _rename(space, "f.md", "f.md.bak", mv)
    r = await _rename(space, "staged", "f.md", mv)
    assert r.entry.kind == "folder" and r.carried
    assert (await _at(space, "f.md/part.md")).sha256 == "sha-x"
    assert (await _at(space, "f.md.bak")).entry_id == memo.entry_id


async def test_under_an_open_check_out_the_write_back_is_under_it(space):
    await _memo(space, "c.md")
    async with writes.locked(space.space_id, LIMITS) as tx:
        found = await tx.live("c.md")
        await changes.checkout(tx, found, agent_of(space, "cli"), "Raise the target")
    sed = _mount(space)
    await _rename(space, "c.md", "c.md.bak", sed)
    entry = await _write(space, "c.md", "new", sed)
    assert (entry.head_note, entry.head_change_id is not None) == ("Raise the target", True)


async def test_renamed_twice_by_one_command_it_still_returns_to_the_name_it_first_left(space):
    memo = await _memo(space, "a2.md")
    command = _mount(space)
    await _rename(space, "a2.md", "a2.md.tmp", command)
    await _rename(space, "a2.md.tmp", "A2.md~", command)
    entry = await _write(space, "a2.md", "new", command, if_none_match=True)
    assert entry.entry_id == memo.entry_id and entry.is_memo
    assert not (await _at(space, "A2.md~")).has_record


async def test_renamed_aside_and_back_by_one_command_it_is_a_plain_move(space, db_conn):
    """``mv q.md q.md.bak && mv q.md.bak q.md``: the memo moves back as
    itself, with no file left at the other name, even with no room for one."""
    memo = await _memo(space, "back.md")
    command = _mount(space)
    await _rename(space, "back.md", "back.md.bak", command)
    async with writes.locked(space.space_id, LIMITS) as tx:
        full = dataclasses.replace(LIMITS, max_entries=(await retention.counts(tx))["entries"])
    async with writes.locked(space.space_id, full) as tx:
        r = await tx.rename("back.md.bak", "back.md", command, replace=False)
    assert (r.entry.entry_id, r.ended_id, r.carried) == (memo.entry_id, None, True)
    assert r.entry.is_memo and await kept_seqs(space, "back.md") == [1]
    async with db_conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) AS n FROM drive_entries WHERE space_id = %s AND path = 'back.md.bak'",
            (space.space_id,),
        )
        assert (await cur.fetchone())["n"] == 0
        await cur.execute(
            "SELECT aside_key FROM drive_memos WHERE entry_id = %s", (memo.entry_id,)
        )
        assert (await cur.fetchone())["aside_key"] is None
    assert await reads.trashed(space.space_id, None) == []
    # Back at its name, it is no longer aside: the command's next file at
    # the backup's name is that file, not the memo.
    other = await _write(space, "back.md.bak", "other", command)
    assert other.entry_id != memo.entry_id
    assert (await _at(space, "back.md")).sha256 == "sha-old"


async def test_moved_on_purpose_then_saved_with_a_backup_it_returns_to_the_name_it_was_moved_to(space):
    """``mv q3.md archive/ && sed -i.bak archive/q3.md`` in one command: the
    backup is of the name the memo was moved to."""
    memo = await _memo(space, "m3.md")
    await save_text(space, "archive/keep.md", "x")
    command = _mount(space)
    await _rename(space, "m3.md", "archive/m3.md", command)
    await _rename(space, "archive/m3.md", "archive/m3.md.bak", command)
    entry = await _write(space, "archive/m3.md", "new", command, if_none_match=True)
    assert entry.entry_id == memo.entry_id and entry.is_memo
    assert await kept_seqs(space, "archive/m3.md") == [2, 1]
    assert not (await _at(space, "archive/m3.md.bak")).has_record


@pytest.mark.parametrize("moved", ["archive/q-final.md", "archive/q.md.bak", "old.md"])
async def test_a_memo_moved_elsewhere_keeps_its_record_there(space, moved):
    """Not a backup beside the name: the command moved the memo on purpose,
    and what it then writes at the old name is a new ordinary file."""
    memo = await _memo(space, "q.md")
    await save_text(space, "archive/keep.md", "x")
    command = _mount(space)
    await _rename(space, "q.md", moved, command)
    entry = await _write(space, "q.md", "template", command, if_none_match=True)
    assert entry.entry_id != memo.entry_id and not entry.has_record
    kept = await _at(space, moved)
    assert kept.entry_id == memo.entry_id and kept.is_memo
    assert await kept_seqs(space, moved) == [1]


@pytest.mark.parametrize("other", ["call:other", None])
async def test_a_rename_another_command_or_none_writes_back_stands(space, other):
    memo = await _memo(space, "d.md")
    await _rename(space, "d.md", "d.md.bak", _mount(space))
    entry = await _write(space, "d.md", "new", _mount(space, command=other))
    assert entry.entry_id != memo.entry_id and not entry.has_record
    assert (await _at(space, "d.md.bak")).entry_id == memo.entry_id


async def _move(space, src, dst):
    async with writes.locked(space.space_id, LIMITS) as tx:
        return await tx.move(src, dst)


@pytest.mark.parametrize("by", ["app", "unnamed mount"])
@pytest.mark.parametrize("via_name", [False, True])
async def test_a_memo_moved_by_anyone_but_the_command_stays_where_that_move_put_it(
    space, by, via_name
):
    """The backup moved by a person (straight to ``orig_v.md``, or back to
    ``v.md`` first) is where they chose: the command's later write at the
    name is a new file, though ``orig_v.md`` reads as a backup."""
    memo = await _memo(space, "v.md")
    sed = _mount(space)
    await _rename(space, "v.md", "v.md.bak", sed)
    hops = ["v.md.bak", "v.md", "orig_v.md"] if via_name else ["v.md.bak", "orig_v.md"]
    for src, dst in zip(hops, hops[1:]):
        if by == "app":
            await _move(space, src, dst)
        else:
            await _rename(space, src, dst, _mount(space, command=None))
    entry = await _write(space, "v.md", "new", sed, if_none_match=True)
    assert entry.entry_id != memo.entry_id and not entry.has_record
    kept = await _at(space, "orig_v.md")
    assert kept.entry_id == memo.entry_id and kept.is_memo and kept.sha256 == "sha-old"


async def test_a_memo_moved_onto_an_ordinary_file_keeps_its_record_there(space):
    memo = await _memo(space, "m.md")
    await save_text(space, "m.md", "two")
    plain = await save_text(space, "plain.md", "plain")
    r = await _rename(space, "m.md", "plain.md", _mount(space), replace=True)
    assert (r.entry.entry_id, r.ended_id, r.carried) == (memo.entry_id, plain.entry_id, True)
    assert r.entry.sha256 == "sha-two" and await kept_seqs(space, "plain.md") == [2, 1]
    assert r.trashed_memo is None
    trashed = {t.entry.entry_id for t in await reads.trashed(space.space_id, None)}
    assert plain.entry_id in trashed and memo.entry_id not in trashed


async def test_an_ordinary_file_moved_onto_a_memo_is_its_next_version(space):
    memo = await _memo(space, "o.md")
    plain = await save_text(space, "p.md", "plain")
    r = await _rename(space, "p.md", "o.md", _mount(space), replace=True)
    assert (r.entry.entry_id, r.ended_id, r.carried) == (memo.entry_id, plain.entry_id, False)
    assert r.entry.sha256 == "sha-plain" and await kept_seqs(space, "o.md") == [2, 1]
    assert r.trashed_memo is None


async def test_a_memo_moved_onto_a_memo_goes_to_the_trash_named(space):
    target = await _memo(space, "b.md", "b")
    source = await _memo(space, "a.md", "a")
    r = await _rename(space, "a.md", "b.md", _mount(space), replace=True)
    assert (r.entry.entry_id, r.ended_id, r.carried) == (target.entry_id, source.entry_id, False)
    assert r.entry.sha256 == "sha-a" and await kept_seqs(space, "b.md") == [2, 1]
    trashed = await reads.trashed(space.space_id, None)
    assert r.trashed_memo in {t.trash_id for t in trashed if t.entry.entry_id == source.entry_id}


async def test_ordinary_files_renamed_and_written_again_are_two_files(space):
    plain = await save_text(space, "x.md", "old")
    sed = _mount(space)
    await _rename(space, "x.md", "x.md.bak", sed)
    entry = await _write(space, "x.md", "new", sed, if_none_match=True)
    assert entry.entry_id != plain.entry_id
    assert (await _at(space, "x.md.bak")).entry_id == plain.entry_id
    with pytest.raises(DriveError) as err:
        await _rename(space, "x.md", "x.md.bak", sed)
    assert err.value.code == "exists"
