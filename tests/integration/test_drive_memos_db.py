"""Memos, the one undo copy and a file's record against real PostgreSQL:
which versions a file keeps as its mark and saves change, the sweep that
ends an undo window, the events a mark makes and the record's paging."""

# The ``space`` fixture comes from drive_helpers; each test takes it by name.
# ruff: noqa: F811

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from datetime import timedelta

import psycopg
import pytest

from src.server.database.drive import changes, memos, reads, retention, writes
from src.server.database.drive import trash as trash_rows
from src.server.database.drive import texts as cache
from src.server.database.drive.records import DriveError
from tests.integration.drive_helpers import (  # noqa: F401
    LIMITS,
    agent_of,
    agent_save,
    counts_of,
    kept_seqs,
    make_due,
    mark,
    person_of,
    save_text,
    space,
    waiting_on_a_lock,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_an_ordinary_file_keeps_only_the_copy_before_its_last_save(space):
    for text in ("1", "2", "3", "4"):
        entry = await save_text(space, "o.bin", text)
    assert await kept_seqs(space, "o.bin") == [4, 3]
    assert entry.undo_until is not None
    assert timedelta(days=6) < entry.undo_until - entry.modified_at <= timedelta(days=7)


async def test_a_copy_too_large_to_keep_leaves_no_undo(space):
    small = dataclasses.replace(LIMITS, history=dataclasses.replace(LIMITS.history, max_bytes=3))
    await save_text(space, "big.bin", "1234", limits=small)
    entry = await save_text(space, "big.bin", "5", limits=small)
    assert await kept_seqs(space, "big.bin") == [2]
    assert entry.undo_until is None


async def test_a_sittings_undo_copy_is_what_came_before_it(space):
    await save_text(space, "s.bin", "before")
    await save_text(space, "s.bin", "edit-1", sitting="turn")
    await save_text(space, "s.bin", "edit-2", sitting="turn")
    entry = await reads.live_entry(space.space_id, path="s.bin")
    assert await kept_seqs(space, "s.bin") == [2, 1]
    found = await reads.content_of(space.space_id, entry.entry_id, 1)
    assert found.sha256 == "sha-before"


async def test_a_later_save_in_the_same_sitting_starts_the_days_again(space, db_conn):
    """An editor left open keeps one sitting: its days run from its last
    save, not its first, so the copy from before it is not swept mid-edit."""
    await save_text(space, "long.bin", "before")
    await save_text(space, "long.bin", "edit-1", sitting="tab")
    await make_due(db_conn, "long.bin", space)
    entry = await save_text(space, "long.bin", "edit-2", sitting="tab")
    assert entry.head_seq == 2 and await kept_seqs(space, "long.bin") == [2, 1]
    assert timedelta(days=6) < entry.undo_until - entry.modified_at <= timedelta(days=7)


async def test_a_memo_keeps_every_version_and_an_unmark_keeps_them_as_the_floor(space, db_conn):
    await save_text(space, "m.bin", "1")
    assert await mark(space, "m.bin")
    for text in ("2", "3", "4"):
        entry = await save_text(space, "m.bin", text)
    assert await kept_seqs(space, "m.bin") == [4, 3, 2, 1]
    assert entry.undo_until is None
    assert await mark(space, "m.bin", memo=False)
    for text in ("5", "6", "7"):
        await save_text(space, "m.bin", text)
    assert await kept_seqs(space, "m.bin") == [7, 6, 4, 3, 2, 1]
    # The sweep drops the undo copy above the floor and never what is below it.
    await make_due(db_conn, "m.bin", space)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.expire(tx))[0] == 1
    assert await kept_seqs(space, "m.bin") == [7, 4, 3, 2, 1]
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted


async def test_the_database_keeps_a_memos_record_against_any_delete_but_a_clear(space, db_conn):
    """The code from before memos trims versions by age and count with a
    plain delete, and still runs while a deploy drains."""

    async def plain_delete() -> int:
        async with db_conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM drive_versions v USING drive_entries e"
                " WHERE e.entry_id = v.entry_id AND e.space_id = %s AND e.path = 'g.bin'"
                " AND v.seq <> e.head_seq",
                (space.space_id,),
            )
            return cur.rowcount

    for text in ("1", "2", "3"):
        await save_text(space, "g.bin", text)
    await mark(space, "g.bin")
    for text in ("4", "5"):
        await save_text(space, "g.bin", text)
    assert await plain_delete() == 0
    assert await kept_seqs(space, "g.bin") == [5, 4, 3, 2]
    # A former memo: its record through the un-mark stays, the copy above it goes.
    await mark(space, "g.bin", memo=False)
    for text in ("6", "7"):
        await save_text(space, "g.bin", text)
    assert await plain_delete() == 1
    assert await kept_seqs(space, "g.bin") == [7, 5, 4, 3, 2]
    # A clear says what it is, and a purge takes the file with everything.
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert (await retention.clear(tx, await tx.live("g.bin"), None))[0] == 4
    assert await kept_seqs(space, "g.bin") == [7]
    # The clear's say-so ends with it: the next delete is held again.
    await mark(space, "g.bin")
    await save_text(space, "g.bin", "8")
    assert await plain_delete() == 0


async def _fold_in_place(
    db_conn, space, path: str, seq: int, of: int,
    sets: str = "sha256 = o.sha256, byte_len = o.byte_len",
) -> int:
    """The fold of the code from before memos: version ``seq``'s row given
    version ``of``'s bytes in place."""
    async with db_conn.cursor() as cur:
        await cur.execute(
            f"UPDATE drive_versions v SET {sets} FROM drive_versions o, drive_entries e"
            " WHERE e.space_id = %s AND e.path = %s AND v.entry_id = e.entry_id"
            " AND o.entry_id = e.entry_id AND o.seq = %s AND v.seq = %s",
            (space.space_id, path, of, seq),
        )
        return cur.rowcount


async def test_the_database_refuses_new_bytes_for_a_version_the_record_holds_as_written(space, db_conn):
    """The code from before memos folds a save into the head's row in place,
    whatever the head is, and still runs while a deploy drains."""

    async def fold_in_place(seq: int, of: int, **kw) -> int:
        return await _fold_in_place(db_conn, space, "h.md", seq, of, **kw)

    for text in ("1", "2"):
        await save_text(space, "h.md", text)
    await mark(space, "h.md")
    with pytest.raises(psycopg.errors.CheckViolation):
        await fold_in_place(2, of=1)  # the version the mark was put on
    # Above it and not pinned, the head still takes a fold, as the agent's own does.
    await save_text(space, "h.md", "3")
    assert await fold_in_place(3, of=1) == 1
    await mark(space, "h.md", status="final")
    with pytest.raises(psycopg.errors.CheckViolation):
        await fold_in_place(3, of=2)
    assert await fold_in_place(3, of=2, sets="note = 'reworded'") == 1


@pytest.mark.parametrize("ended_by", ["a new check-out", "a check-in"])
async def test_the_database_refuses_new_bytes_for_a_version_under_an_ended_check_out(
    space, db_conn, ended_by
):
    """Old code folds the same sitting's next save into the head whatever
    change made it, which would give a version another change's bytes under
    its own reason."""
    await save_text(space, "x.md", "1")
    await mark(space, "x.md")
    await _check(space, "x.md", "Tighten the thesis")
    await agent_save(space, "x.md", "2", sitting="turn")
    if ended_by == "a check-in":
        await _check(space, "x.md", check_in=True)
    else:
        await _check(space, "x.md", "Add the risks")
    with pytest.raises(psycopg.errors.CheckViolation):
        await _fold_in_place(db_conn, space, "x.md", 2, of=1)
    assert await _fold_in_place(db_conn, space, "x.md", 2, of=1, sets="note = v.note") == 1
    if ended_by == "a new check-out":
        # New code makes the same sitting's save under the new change a version.
        entry = await agent_save(space, "x.md", "3", sitting="turn")
        assert (entry.head_seq, entry.head_note) == (3, "Add the risks")


async def test_the_head_of_an_open_check_out_still_takes_a_fold(space, db_conn):
    await save_text(space, "o.md", "1")
    await mark(space, "o.md")
    opened = await _check(space, "o.md", "Tighten the thesis")
    await agent_save(space, "o.md", "2", sitting="turn")
    assert await _fold_in_place(db_conn, space, "o.md", 2, of=1) == 1
    entry = await agent_save(space, "o.md", "3", sitting="turn")
    assert (entry.head_seq, entry.head_change_id) == (2, opened.change.change_id)
    assert entry.sha256 == "sha-3"


async def test_a_continued_check_out_folds_the_same_sittings_save(space):
    await save_text(space, "u.md", "1")
    await mark(space, "u.md")
    opened = await _check(space, "u.md", "Tighten the thesis")
    await agent_save(space, "u.md", "2", sitting="turn")
    await _check(space, "u.md", check_in=True)
    assert (await _check(space, "u.md")).continued
    entry = await agent_save(space, "u.md", "3", sitting="turn")
    assert (entry.head_seq, entry.head_change_id) == (2, opened.change.change_id)
    assert entry.sha256 == "sha-3"


async def test_a_clear_below_the_undo_copy_leaves_its_ended_window_to_the_sweep(space, db_conn):
    """A former memo's clear that drops record versions and not the undo
    copy, after the window ended and before the sweep came: the sweep still
    drops that copy."""
    await save_text(space, "c.bin", "1")
    assert await mark(space, "c.bin")
    for text in ("2", "3"):
        await save_text(space, "c.bin", text)
    assert await mark(space, "c.bin", memo=False)
    for text in ("4", "5"):
        await save_text(space, "c.bin", text)
    assert await kept_seqs(space, "c.bin") == [5, 4, 3, 2, 1]
    await make_due(db_conn, "c.bin", space)
    async with writes.locked(space.space_id, LIMITS) as tx:
        found = await tx.live("c.bin")
        assert (await retention.clear(tx, found, 3))[0] == 2
    assert await kept_seqs(space, "c.bin") == [5, 4, 3]
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.expire(tx))[0] == 1
    assert await kept_seqs(space, "c.bin") == [5, 3]
    entry = await reads.live_entry(space.space_id, path="c.bin")
    assert entry.undo_until is None


async def test_marking_within_the_window_keeps_the_history_the_sweep_would_drop(space, db_conn):
    await save_text(space, "w.bin", "1")
    await save_text(space, "w.bin", "2")
    await make_due(db_conn, "w.bin", space)
    await mark(space, "w.bin")
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert await retention.expire(tx) == (0, 0)
    assert await kept_seqs(space, "w.bin") == [2, 1]


async def test_each_change_to_the_mark_is_one_event_and_no_change_is_none(space):
    await save_text(space, "e.md", "1")
    assert await mark(space, "e.md")
    assert not await mark(space, "e.md")
    assert await mark(space, "e.md", memo=None, status="final")
    assert not await mark(space, "e.md", memo=None, status="final")
    assert await mark(space, "e.md", memo=None, tickers=["AAPL"])
    assert await mark(space, "e.md", memo=False)
    with pytest.raises(DriveError) as refused:
        await mark(space, "e.md", memo=False)
    assert refused.value.code == "not_memo"
    entry = await reads.live_entry(space.space_id, path="e.md")
    page = await memos.record_page(entry.entry_id, before=None, limit=50)
    kinds = [getattr(i, "kind", "version") for i in page.items]
    assert kinds == ["unmark", "update", "update", "mark", "version"]
    assert page.items[2].final_seq == 1
    assert entry.memo.final_seq == 1 and entry.memo.final_at is not None
    # A change to the mark is not a save: the content's last save stands.
    assert (entry.modified_at, entry.modified_by.kind) == (page.items[4].version.created_at, "person")
    with pytest.raises(DriveError) as refused:
        await mark(space, "e.md", memo=None, status="draft")
    assert refused.value.code == "not_memo"


async def test_a_save_never_folds_into_a_pinned_version(space):
    await save_text(space, "p.md", "1", sitting="s")
    await mark(space, "p.md", status="final")
    entry = await save_text(space, "p.md", "2", sitting="s")
    assert entry.head_seq == 2
    entry = await save_text(space, "p.md", "3", sitting="s")
    assert entry.head_seq == 2


async def test_a_save_never_folds_into_the_version_a_file_was_marked_at(space):
    """Made, marked and edited in one turn: the edit is its own version, so
    the memo as marked stays and the edit can still be given its reason."""
    await agent_save(space, "k.md", "1", via="mount", sitting="t")
    await mark(space, "k.md")
    entry = await agent_save(space, "k.md", "2", via="mount", sitting="t")
    assert entry.head_seq == 2
    assert (await _check(space, "k.md", "Tightened the summary")).adopted_seq == 2
    notes = await _notes(space, "k.md")
    assert (notes[1][0], notes[2][0]) == (None, "Tightened the summary")


async def _check(space, path, reason=None, *, thread=None, check_in=False):
    async with writes.locked(space.space_id, LIMITS) as tx:
        found = await tx.live(path)
        actor = agent_of(space, "cli", thread=thread)
        if check_in:
            return await changes.checkin(tx, found, actor, reason)
        return await changes.checkout(tx, found, actor, reason)


async def _notes(space, path) -> dict[int, tuple[str | None, str | None]]:
    entry = await reads.live_entry(space.space_id, path=path)
    found = (await reads.versions(entry.entry_id, LIMITS.history))[0]
    return {v.seq: (v.note, v.change_id) for v in found}


@pytest.mark.parametrize(
    ("via", "note", "refused"),
    [("cli", None, True), ("tool", None, True), ("mount", None, False), ("cli", "why", False)],
)
async def test_the_agent_changes_a_memo_only_inside_a_check_out(space, via, note, refused):
    await save_text(space, "r.md", "1")
    await mark(space, "r.md")
    if refused:
        with pytest.raises(DriveError) as err:
            await agent_save(space, "r.md", "2", via=via, sitting="t1")
        assert err.value.code == "checkout_required"
        assert err.value.details == {"path": "r.md", "last_change": None}
        assert await kept_seqs(space, "r.md") == [1]
        return
    entry = await agent_save(space, "r.md", "2", via=via, sitting="t1", note=note)
    # A note starts a change of its own; the mount keeps a save with none.
    assert entry.head_note == note
    assert (entry.head_change_id is not None) == (note is not None)


async def test_a_check_out_carries_its_reason_and_folds_only_within_it(space):
    await save_text(space, "c.md", "1")
    await mark(space, "c.md")
    first = await _check(space, "c.md", "Tighten the thesis")
    assert (first.change.versions, first.adopted_seq, first.continued) == (0, None, False)
    entry = await agent_save(space, "c.md", "2", via="tool", sitting="turn")
    assert (entry.head_seq, entry.head_note) == (2, "Tighten the thesis")
    entry = await agent_save(space, "c.md", "3", via="mount", sitting="turn")
    assert entry.head_seq == 2
    second = await _check(space, "c.md", "Add the risks")
    entry = await agent_save(space, "c.md", "4", via="tool", sitting="turn")
    assert (entry.head_seq, entry.head_note) == (3, "Add the risks")
    assert await _notes(space, "c.md") == {
        3: ("Add the risks", second.change.change_id),
        2: ("Tighten the thesis", first.change.change_id),
        1: (None, None),
    }
    # A person's save in between neither needs nor joins a change.
    person = await save_text(space, "c.md", "5", sitting="turn")
    assert (person.head_seq, person.head_note, person.head_change_id) == (4, None, None)


async def test_check_in_rewords_every_version_and_continue_reopens(space):
    await save_text(space, "w.md", "1")
    await mark(space, "w.md")
    opened = await _check(space, "w.md", "Draft")
    await agent_save(space, "w.md", "2", sitting="a")
    await agent_save(space, "w.md", "3", sitting="b")
    done = await _check(space, "w.md", "Rewrote the summary", check_in=True)
    assert (done.change.versions, done.change.first_seq, done.change.last_seq) == (2, 2, 3)
    assert done.change.ended_at is not None and done.entry.head_note == "Rewrote the summary"
    notes = await _notes(space, "w.md")
    assert notes[2][0] == notes[3][0] == "Rewrote the summary"
    with pytest.raises(DriveError) as err:
        await agent_save(space, "w.md", "4", sitting="c")
    assert err.value.details["last_change"]["reason"] == "Rewrote the summary"
    assert err.value.details["last_change"]["versions"] == 2
    again = await _check(space, "w.md")
    assert again.continued and again.change.change_id == opened.change.change_id
    entry = await agent_save(space, "w.md", "4", sitting="c")
    assert (entry.head_seq, entry.head_change_id) == (4, opened.change.change_id)


async def test_a_check_out_adopts_the_chats_reasonless_mount_save(space):
    await save_text(space, "a.md", "1")
    await mark(space, "a.md")
    await agent_save(space, "a.md", "2", via="mount", sitting="t")
    checked = await _check(space, "a.md", "Fixed the numbers")
    assert (checked.adopted_seq, checked.change.versions) == (2, 1)
    assert checked.entry.head_note == "Fixed the numbers"
    # A head someone else made is never adopted.
    await save_text(space, "a.md", "3")
    assert (await _check(space, "a.md", "Again")).adopted_seq is None


async def test_with_no_chat_a_change_ends_an_hour_after_it_was_touched(space, db_conn):
    await save_text(space, "h.md", "1")
    await mark(space, "h.md")
    await _check(space, "h.md", "Quick fix")
    await agent_save(space, "h.md", "2", sitting="x")
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_memo_changes SET touched_at = now() - interval '61 minutes'"
            " WHERE reason = 'Quick fix'"
        )
    with pytest.raises(DriveError) as err:
        await agent_save(space, "h.md", "3", sitting="x")
    assert err.value.code == "checkout_required"


async def _turn(db_conn, thread: str, index: int, started: int, ended: int | None) -> None:
    """A turn of ``thread`` that started ``started`` minutes ago and ended
    ``ended`` minutes ago, or runs now; born running, as every turn is."""
    async with db_conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO conversation_responses (conversation_thread_id, turn_index, status,"
            " created_at) VALUES (%s, %s, 'in_progress', now() - make_interval(mins => %s))",
            (thread, index, started),
        )
        if ended is not None:
            await cur.execute(
                "UPDATE conversation_responses SET status = 'completed',"
                " usage_settled_at = now() - make_interval(mins => %s)"
                " WHERE conversation_thread_id = %s AND turn_index = %s",
                (ended, thread, index),
            )


async def _chat(seed_workspace) -> str:
    from src.server.database.conversation.threads_write import create_thread

    thread = str(uuid.uuid4())
    await create_thread(thread, seed_workspace["workspace_id"], "completed")
    return thread


async def _checked_out_at(space, db_conn, path, thread, ago: int) -> None:
    await save_text(space, path, "1")
    await mark(space, path)
    await _check(space, path, "Earlier reason", thread=thread)
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_memo_changes SET opened_at = now() - make_interval(mins => %s),"
            " touched_at = now() - make_interval(mins => %s) WHERE thread_id = %s",
            (ago, ago, thread),
        )


async def test_a_turn_ten_minutes_after_the_last_saves_under_the_change(
    space, seed_workspace, db_conn
):
    thread = await _chat(seed_workspace)
    # The last touch is over an hour old, so only the recorded turns keep it open.
    await _turn(db_conn, thread, 0, 125, 115)
    await _checked_out_at(space, db_conn, "t1.md", thread, 120)
    await _turn(db_conn, thread, 1, 105, None)
    entry = await agent_save(space, "t1.md", "2", via="tool", sitting="r2", thread=thread)
    assert entry.head_note == "Earlier reason"


async def test_a_turn_two_hours_after_the_last_is_refused(space, seed_workspace, db_conn):
    thread = await _chat(seed_workspace)
    await _turn(db_conn, thread, 0, 180, 170)
    await _checked_out_at(space, db_conn, "t2.md", thread, 175)
    await _turn(db_conn, thread, 1, 50, None)
    with pytest.raises(DriveError) as err:
        await agent_save(space, "t2.md", "2", via="tool", sitting="r2", thread=thread)
    assert err.value.code == "checkout_required"
    last = err.value.details["last_change"]
    assert (last["reason"], last["versions"], last["checked_in"]) == ("Earlier reason", 0, False)
    # Carried on, it holds for this turn.
    await _check(space, "t2.md", thread=thread)
    entry = await agent_save(space, "t2.md", "2", via="tool", sitting="r2", thread=thread)
    assert entry.head_note == "Earlier reason"
    # Another chat's change is not this one's.
    with pytest.raises(DriveError) as err:
        await agent_save(space, "t2.md", "3", sitting="r3")
    assert err.value.details["last_change"] is None


async def test_an_hour_quiet_between_two_later_turns_ends_the_change(
    space, seed_workspace, db_conn
):
    thread = await _chat(seed_workspace)
    await _turn(db_conn, thread, 0, 240, 230)
    await _checked_out_at(space, db_conn, "t4.md", thread, 235)
    # The first later turn follows within minutes; the hour of quiet is after it.
    await _turn(db_conn, thread, 1, 225, 215)
    await _turn(db_conn, thread, 2, 95, None)
    with pytest.raises(DriveError) as err:
        await agent_save(space, "t4.md", "2", via="tool", sitting="r3", thread=thread)
    assert err.value.code == "checkout_required"


async def _task(db_conn, thread: str, started: int, ended: int | None) -> None:
    """A background task of ``thread`` that started ``started`` minutes ago
    and ended ``ended`` minutes ago, or runs now."""
    async with db_conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO subagent_tasks (thread_id, task_id) VALUES (%s, 't1')", (thread,)
        )
        await cur.execute(
            "INSERT INTO subagent_runs (thread_id, task_id, started_at)"
            " VALUES (%s, 't1', now() - make_interval(mins => %s))",
            (thread, started),
        )
        if ended is not None:
            await cur.execute(
                "UPDATE subagent_runs SET status = 'completed',"
                " finalized_at = now() - make_interval(mins => %s) WHERE thread_id = %s",
                (ended, thread),
            )


@pytest.mark.parametrize("ended", [20, None])
async def test_a_background_task_that_ran_through_the_gap_keeps_the_change_open(
    space, seed_workspace, db_conn, ended
):
    """The turn that launched it ended two hours ago and the next began just
    now; the task worked in between, finished or still running."""
    thread, path = await _chat(seed_workspace), f"t5-{ended}.md"
    await _turn(db_conn, thread, 0, 180, 170)
    await _checked_out_at(space, db_conn, path, thread, 175)
    await _task(db_conn, thread, 172, ended)
    await _turn(db_conn, thread, 1, 15, None)
    entry = await agent_save(space, path, "2", via="tool", sitting="r2", thread=thread)
    assert entry.head_note == "Earlier reason"


async def test_a_background_task_that_ended_long_before_the_next_turn_does_not(
    space, seed_workspace, db_conn
):
    thread = await _chat(seed_workspace)
    await _turn(db_conn, thread, 0, 240, 230)
    await _checked_out_at(space, db_conn, "t6.md", thread, 235)
    await _task(db_conn, thread, 232, 150)
    await _turn(db_conn, thread, 1, 15, None)
    with pytest.raises(DriveError) as err:
        await agent_save(space, "t6.md", "2", via="tool", sitting="r2", thread=thread)
    assert err.value.code == "checkout_required"


async def test_a_check_out_adopts_only_a_mount_save_made_while_a_memo(space):
    # Made with ``la drive put`` and then marked: its first version stays as made.
    await agent_save(space, "p.md", "1", via="cli")
    await mark(space, "p.md")
    assert (await _check(space, "p.md", "Edit")).adopted_seq is None
    # Saved through the mount before it was marked: not the save the mount asked about.
    await agent_save(space, "q.md", "1", via="mount", sitting="m")
    await mark(space, "q.md")
    assert (await _check(space, "q.md", "Edit")).adopted_seq is None
    assert await _notes(space, "p.md") == {1: (None, None)}
    assert await _notes(space, "q.md") == {1: (None, None)}


async def test_a_check_out_leaves_a_reasonless_save_from_before_the_chat_went_quiet(
    space, seed_workspace, db_conn
):
    """A reason written hours later is for another change, not for that save."""
    thread = await _chat(seed_workspace)
    await save_text(space, "late.md", "1")
    await mark(space, "late.md")
    await _turn(db_conn, thread, 0, 180, 170)
    await agent_save(space, "late.md", "2", via="mount", sitting="t", thread=thread)
    async with db_conn.cursor() as cur:
        await cur.execute(
            "UPDATE drive_entries SET modified_at = now() - interval '175 minutes'"
            " WHERE space_id = %s AND path = 'late.md'",
            (space.space_id,),
        )
    await _turn(db_conn, thread, 1, 5, None)
    checked = await _check(space, "late.md", "Add Q3 numbers", thread=thread)
    assert checked.adopted_seq is None
    assert (await _notes(space, "late.md"))[2] == (None, None)


async def _explain(space, path, note, actor, seq=None):
    async with writes.locked(space.space_id, LIMITS) as tx:
        return await memos.explain(tx, await tx.live(path), seq, note, actor)


async def test_a_person_says_why_afterwards_only_for_their_own_version_with_none(space):
    await save_text(space, "y.md", "1")
    await mark(space, "y.md")
    said = await _explain(space, "y.md", "First draft", person_of(space))
    assert (said.seq, said.note) == (1, "First draft")
    assert (await reads.live_entry(space.space_id, path="y.md")).head_note == "First draft"
    with pytest.raises(DriveError) as err:
        await _explain(space, "y.md", "Again", person_of(space))
    assert err.value.code == "exists"
    # A version made under a check-out says why already: its change's reason.
    await _check(space, "y.md", "Add the risks")
    await agent_save(space, "y.md", "2", sitting="t")
    with pytest.raises(DriveError) as err:
        await _explain(space, "y.md", "Mine", person_of(space))
    assert (err.value.code, err.value.message) == ("exists", "Version 2 of y.md already says why.")
    # A mount save with no check-out has none, and only its maker may say why.
    await _check(space, "y.md", check_in=True)
    await agent_save(space, "y.md", "3", via="mount", sitting="u")
    with pytest.raises(DriveError) as err:
        await _explain(space, "y.md", "Mine", person_of(space))
    assert err.value.code == "forbidden"
    assert await _notes(space, "y.md") == {
        3: (None, None),
        2: ("Add the risks", (await _notes(space, "y.md"))[2][1]),
        1: ("First draft", None),
    }


async def _fed(space, since: int) -> list[str]:
    return [e.path for e in (await reads.changes(space.space_id, since)).entries]


async def test_a_why_under_an_earlier_version_moves_the_file_in_the_feed(space):
    """A client shows it in the record, which it reads again on the feed."""
    await save_text(space, "old.md", "1")
    await mark(space, "old.md")
    await save_text(space, "old.md", "2", sitting="later")
    before = await reads.cursor(space.space_id)
    await _explain(space, "old.md", "First draft", person_of(space), seq=1)
    assert await _fed(space, before) == ["old.md"]
    # So does a check-in that rewords a change another save has since followed.
    await _check(space, "old.md", "Draft")
    await agent_save(space, "old.md", "3", sitting="a")
    await save_text(space, "old.md", "4", sitting="person")
    before = await reads.cursor(space.space_id)
    await _check(space, "old.md", "Rewrote the summary", check_in=True)
    assert await _fed(space, before) == ["old.md"]
    assert (await _notes(space, "old.md"))[3][0] == "Rewrote the summary"


async def test_the_record_pages_by_its_cursor_in_one_order(space):
    await save_text(space, "rec.md", "1")
    await mark(space, "rec.md")
    for i in range(2, 6):
        await save_text(space, "rec.md", str(i))
        await mark(space, "rec.md", memo=None, tickers=[f"T{i}"])
    entry = await reads.live_entry(space.space_id, path="rec.md")
    whole = (await memos.record_page(entry.entry_id, before=None, limit=200)).items
    stamps = [i.version.created_at if hasattr(i, "version") else i.created_at for i in whole]
    assert stamps == sorted(stamps, reverse=True)
    assert len(whole) == 5 + 5
    paged, before, asked = [], None, set()
    while True:
        page = await memos.record_page(entry.entry_id, before=before, limit=3)
        paged += page.items
        if page.next is None:
            break
        assert page.next not in asked, "the cursor came back a second time"
        asked.add(page.next)
        before = page.next
    assert paged == whole


async def test_a_purge_takes_the_mark_and_its_events(space, db_conn):
    await save_text(space, "gone.md", "1")
    await mark(space, "gone.md", status="draft")
    entry = await reads.live_entry(space.space_id, path="gone.md")
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "gone.md", person_of(space), None, False)
        await trash_rows.purge(tx, None, None)
    async with db_conn.cursor() as cur:
        await cur.execute(
            "SELECT (SELECT count(*) FROM drive_memos WHERE entry_id = %(id)s)"
            " + (SELECT count(*) FROM drive_memo_events WHERE entry_id = %(id)s) AS n",
            {"id": entry.entry_id},
        )
        assert (await cur.fetchone())["n"] == 0


async def test_undo_then_undo_again_redoes(space):
    await save_text(space, "u.md", "one")
    await save_text(space, "u.md", "two")
    entry = await reads.live_entry(space.space_id, path="u.md")
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await tx.undo(entry.entry_id, person_of(space), entry.sha256, None)
    assert (back.head_seq, back.sha256) == (3, "sha-one")
    async with writes.locked(space.space_id, LIMITS) as tx:
        again = await tx.undo(entry.entry_id, person_of(space), None, None)
    assert (again.head_seq, again.sha256) == (4, "sha-two")
    assert await kept_seqs(space, "u.md") == [4, 3]


async def test_a_memo_moved_over_an_ordinary_file_keeps_its_record_there(space):
    """The record stays with the name: the memo is what was moved, and the
    file it replaced goes to the trash (more in test_drive_aside_db.py)."""
    await save_text(space, "thesis.md", "body")
    await mark(space, "thesis.md", status="draft")
    other = await save_text(space, "other.md", "old")
    memo = await reads.live_entry(space.space_id, path="thesis.md")
    async with writes.locked(space.space_id, LIMITS) as tx:
        renamed = await tx.rename("thesis.md", "other.md", person_of(space), replace=True)
    target, gone = renamed.entry, renamed.ended_id
    assert (target.entry_id, gone, target.sha256) == (memo.entry_id, other.entry_id, "sha-body")
    assert target.memo.status == "draft"
    assert [t.entry.entry_id for t in await reads.trashed(space.space_id, None)] == [gone]


async def test_the_first_save_after_an_unmark_can_be_undone(space):
    await save_text(space, "was.md", "1")
    await mark(space, "was.md")
    await mark(space, "was.md", memo=False)
    entry = await save_text(space, "was.md", "2")
    assert entry.undo_until is not None
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await tx.undo(entry.entry_id, person_of(space), entry.sha256, None)
    assert back.sha256 == "sha-1"


async def test_a_former_memo_whose_undo_copy_was_swept_has_nothing_to_undo(space, db_conn):
    """The record below stays, but it is not the state before the last save."""
    for text in ("1", "2", "3"):
        await save_text(space, "rec.md", text)
        if text == "1":
            await mark(space, "rec.md")
    await mark(space, "rec.md", memo=False)
    await save_text(space, "rec.md", "4")
    entry = await save_text(space, "rec.md", "5")
    await make_due(db_conn, "rec.md", space)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        await retention.expire(tx)
    assert await kept_seqs(space, "rec.md") == [5, 3, 2, 1]
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(entry.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_a_window_past_its_end_has_nothing_to_undo_before_the_sweep_runs(space, db_conn):
    await save_text(space, "late.md", "one")
    entry = await save_text(space, "late.md", "two")
    await make_due(db_conn, "late.md", space)
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(entry.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"
    after = await reads.live_entry(space.space_id, path="late.md")
    assert (after.head_seq, after.sha256) == (entry.head_seq, "sha-two")
    assert await kept_seqs(space, "late.md") == [2, 1]


async def test_an_unmark_leaves_nothing_to_undo_until_the_next_save(space):
    await save_text(space, "off.md", "1")
    await mark(space, "off.md")
    await save_text(space, "off.md", "2")
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await tx.undo(
            (await reads.live_entry(space.space_id, path="off.md")).entry_id,
            person_of(space), None, None,
        )
    assert back.sha256 == "sha-1"
    await mark(space, "off.md", memo=False)
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(back.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_a_sitting_that_saves_the_old_bytes_back_leaves_nothing_to_undo(space):
    await save_text(space, "same.md", "one")
    edited = await save_text(space, "same.md", "two", sitting="turn")
    assert edited.undo_until is not None
    back = await save_text(space, "same.md", "one", sitting="turn")
    assert (back.head_seq, back.undo_until) == (2, None)
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(back.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_a_sitting_that_passes_back_through_the_old_bytes_can_still_be_undone(space):
    """An autosaving editor: the person types, undoes back to the original
    and types something else, all in one sitting. The copy from before the
    sitting outlives the moment the head matched it."""
    await save_text(space, "via.md", "one")
    await save_text(space, "via.md", "two", sitting="tab")
    await save_text(space, "via.md", "one", sitting="tab")
    moved = await save_text(space, "via.md", "three", sitting="tab")
    assert moved.head_seq == 2 and await kept_seqs(space, "via.md") == [2, 1]
    assert timedelta(days=6) < moved.undo_until - moved.modified_at <= timedelta(days=7)
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await tx.undo(moved.entry_id, person_of(space), moved.sha256, None)
    assert back.sha256 == "sha-one"


async def test_a_former_memo_whose_last_copy_was_too_large_has_nothing_to_undo(space):
    """The record below is older than the save being undone, so it is not
    what Undo puts back."""
    small = dataclasses.replace(LIMITS, history=dataclasses.replace(LIMITS.history, max_bytes=3))
    await save_text(space, "large.md", "1")
    await mark(space, "large.md")
    await mark(space, "large.md", memo=False)
    await save_text(space, "large.md", "1234", limits=small)
    entry = await save_text(space, "large.md", "5", limits=small)
    assert await kept_seqs(space, "large.md") == [3, 1]
    assert entry.undo_until is None
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, small) as tx:
            await tx.undo(entry.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_a_sitting_back_at_its_old_bytes_never_undoes_to_an_older_record(space):
    await save_text(space, "older.md", "x")
    await mark(space, "older.md")
    await save_text(space, "older.md", "a")
    await mark(space, "older.md", memo=False)
    await save_text(space, "older.md", "b", sitting="turn")
    back = await save_text(space, "older.md", "a", sitting="turn")
    assert (back.head_seq, back.undo_until) == (3, None)
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(back.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_a_memo_whose_version_below_the_head_was_cleared_has_nothing_to_undo(space):
    """The pinned final it keeps is older than the save being undone."""
    await save_text(space, "live.md", "1")
    await mark(space, "live.md", status="final")
    await save_text(space, "live.md", "2")
    entry = await save_text(space, "live.md", "3")
    async with writes.locked(space.space_id, LIMITS) as tx:
        back = await tx.undo(entry.entry_id, person_of(space), None, None)
    assert back.sha256 == "sha-2"
    async with writes.locked(space.space_id, LIMITS) as tx:
        await retention.clear(tx, back, None)
    assert await kept_seqs(space, "live.md") == [4, 1]
    with pytest.raises(DriveError) as refused:
        async with writes.locked(space.space_id, LIMITS) as tx:
            await tx.undo(entry.entry_id, person_of(space), None, None)
    assert refused.value.code == "not_found"


async def test_the_sweep_takes_due_undo_copies_a_batch_at_a_time(space, db_conn, monkeypatch):
    monkeypatch.setattr(retention, "EXPIRE_BATCH", 2)
    for name in ("a.bin", "b.bin", "c.bin"):
        await save_text(space, name, "1")
        await save_text(space, name, "2")
        await make_due(db_conn, name, space)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.expire(tx))[0] == 2
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert (await retention.expire(tx))[0] == 1
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert await retention.expire(tx) == (0, 0)


async def test_a_mark_call_that_sets_nothing_does_not_mark(space):
    await save_text(space, "plain.md", "1")
    assert not await mark(space, "plain.md", memo=None)
    assert (await reads.live_entry(space.space_id, path="plain.md")).memo is None


@pytest.mark.parametrize(
    "mark",
    [["x", "version", 1], ["2026-11-25T00:00:00+00:00", "other", 1], ["2026-11-25", "event", "1"], 7],
)
async def test_a_record_cursor_this_list_never_gave_is_refused(space, mark):
    await save_text(space, "cur.md", "1")
    entry = await reads.live_entry(space.space_id, path="cur.md")
    with pytest.raises(DriveError) as refused:
        await memos.record_page(entry.entry_id, before=memos._token(mark), limit=5)
    assert refused.value.code == "invalid"
    with pytest.raises(DriveError) as refused:
        await memos.memo_page(
            space.space_id, sort="revised", descending=True, status=None, ticker=None,
            limit=5, cursor=memos._token(2**63),
        )
    assert refused.value.code == "invalid"


# --- what a clear would drop (``clearable``) ---------------------------------------


async def _clearable(space, path: str) -> int:
    entry = await reads.live_entry(space.space_id, path=path)
    return (await reads.clearable(space.space_id, entry.entry_id, LIMITS.history))[0]


async def test_clearable_counts_what_the_clear_drops(space):
    # A live memo with a pinned final: every version but the head and the final.
    for text in ("1", "2"):
        await save_text(space, "pinned.md", text)
    await mark(space, "pinned.md", status="final")
    for text in ("3", "4", "5"):
        await save_text(space, "pinned.md", text)
    assert await _clearable(space, "pinned.md") == 3
    # A plain file with its undo copy: that copy.
    for text in ("1", "2"):
        await save_text(space, "plain.bin", text)
    assert await _clearable(space, "plain.bin") == 1
    # Only a head: nothing.
    await save_text(space, "alone.bin", "1")
    assert await _clearable(space, "alone.bin") == 0
    # And the clear drops exactly that many.
    for path, n in (("pinned.md", 3), ("plain.bin", 1), ("alone.bin", 0)):
        async with writes.locked(space.space_id, LIMITS) as tx:
            found = await tx.file((await tx.live(path)).entry_id)
            assert (await retention.clear(tx, found, None))[0] == n


async def test_clearable_below_a_head_counts_what_a_clear_bound_to_it_drops(space):
    """The count a person is shown is of what lies below the head listed
    with it, so a save landing since is in neither the count nor the clear."""
    for text in ("1", "2"):
        await save_text(space, "bound.md", text)
    await mark(space, "bound.md", status="final")
    for text in ("3", "4", "5"):
        await save_text(space, "bound.md", text)
    entry = await reads.live_entry(space.space_id, path="bound.md")
    assert (await reads.clearable(space.space_id, entry.entry_id, LIMITS.history, 4))[0] == 2
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert (await retention.clear(tx, await tx.file(entry.entry_id), 4))[0] == 2
    assert await kept_seqs(space, "bound.md") == [5, 4, 2]


async def test_a_bound_clear_of_a_file_from_before_memos_keeps_its_spares_at_the_bound(
    space, db_conn
):
    """A file saved under the rule before migration 154 holds spares below
    its undo copy until its next save, under the window the migration gave
    it: a clear bound below one leaves it, and the window."""
    await save_text(space, "stage1.bin", "1")
    await mark(space, "stage1.bin")
    for text in ("2", "3", "4"):
        await save_text(space, "stage1.bin", text)
    async with db_conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM drive_memos m USING drive_entries e WHERE e.entry_id = m.entry_id"
            " AND e.space_id = %s AND e.path = 'stage1.bin'",
            (space.space_id,),
        )
        await cur.execute(
            "UPDATE drive_entries SET undo_until = now() + interval '7 days'"
            " WHERE space_id = %s AND path = 'stage1.bin'",
            (space.space_id,),
        )
    entry = await reads.live_entry(space.space_id, path="stage1.bin")
    assert entry.undo_until is not None
    assert (await reads.clearable(space.space_id, entry.entry_id, LIMITS.history, 2))[0] == 1
    async with writes.locked(space.space_id, LIMITS) as tx:
        assert (await retention.clear(tx, await tx.file(entry.entry_id), 2))[0] == 1
    assert await kept_seqs(space, "stage1.bin") == [4, 3, 2]
    after = await reads.live_entry(space.space_id, path="stage1.bin")
    assert after.undo_until == entry.undo_until
    kept, counted = await counts_of(db_conn, space.space_id)
    assert kept == counted


async def test_the_versions_listing_is_one_snapshot(space, monkeypatch):
    """The pin, the head and the count a clear is bound to are read
    together: a pin and a save landing between the reads are in none."""
    from src.server.services import drive

    await save_text(space, "snap.md", "1")
    await mark(space, "snap.md")
    await save_text(space, "snap.md", "2")
    await mark(space, "snap.md", status="final")  # pins v2
    await save_text(space, "snap.md", "3")
    listed = reads.versions

    async def changed_between(*args, **kwargs):
        # Below the listed head v3, the snapshot's pin v2 leaves v1 to clear;
        # a count that read the new pin v3 would clear v1 and v2.
        await mark(space, "snap.md", status="final")  # repins to the head, v3
        await save_text(space, "snap.md", "4")
        return await listed(*args, **kwargs)

    monkeypatch.setattr(reads, "versions", changed_between)
    entry, found, _, dropped, _ = await drive.versions(space, "snap.md")
    assert entry.memo.final_seq == 2
    assert [v.seq for v in found] == [3, 2, 1]
    assert dropped == 1
    now = await reads.live_entry(space.space_id, path="snap.md")
    assert (now.head_seq, now.memo.final_seq) == (4, 3)


# --- the attribution cache against a version change (F1) ---------------------------


async def test_a_clear_of_several_versions_and_a_cache_write_never_deadlock(space, test_db_pool):
    """A delete that holds the epoch row (from its first version) and then
    reaches a second version, while a cache write waits on that epoch row:
    the write holds no version, so the delete goes through, and the write,
    let in after, finds the epoch moved and keeps nothing."""
    await save_text(space, "race.md", "1")
    await mark(space, "race.md")  # keeps every version
    for text in ("2", "3"):
        await save_text(space, "race.md", text)
    entry = await reads.live_entry(space.space_id, path="race.md")
    found = await cache.chain(entry.entry_id)
    writing = None
    try:
        async with test_db_pool.connection() as conn:
            async with conn.transaction():
                # As a clear says it is one: nothing else deletes a memo's record.
                await conn.execute("SELECT set_config('drive.clearing', 'on', true)")
                await conn.execute(
                    "DELETE FROM drive_versions WHERE entry_id = %s AND seq = 1",
                    (entry.entry_id,),
                )
                writing = asyncio.create_task(
                    cache.put_attribution(entry.entry_id, 2, found.epoch, [2])
                )
                await waiting_on_a_lock(test_db_pool, conn)
                await asyncio.wait_for(
                    conn.execute(
                        "DELETE FROM drive_versions WHERE entry_id = %s AND seq = 2",
                        (entry.entry_id,),
                    ),
                    10,
                )
        assert await asyncio.wait_for(writing, 10) is False
    finally:
        # A failure above leaves the writer running: stop it and collect it.
        if writing is not None:
            if not writing.done():
                writing.cancel()
            await asyncio.gather(writing, return_exceptions=True)
    after = await cache.chain(entry.entry_id)
    assert after.epoch > found.epoch and after.attributed == set()


async def test_the_chain_of_a_file_purged_since_it_was_read_is_not_found(space):
    """A purge between the route's read of the file and the chain leaves no
    entry for the epoch row to name."""
    with pytest.raises(DriveError) as refused:
        await cache.chain(str(uuid.uuid4()))
    assert refused.value.code == "not_found"


async def test_the_chain_of_a_file_purged_after_its_epoch_row_is_not_found(
    space, db_conn, monkeypatch
):
    """The other timing: the purge lands after the epoch row is made and
    takes it and the versions with the entry."""
    await save_text(space, "gone.md", "1")
    entry = await reads.live_entry(space.space_id, path="gone.md")
    fetch, calls = cache.fetch, 0

    async def purged_after_the_insert(sql, params, conn=None):
        nonlocal calls
        calls += 1
        out = await fetch(sql, params, conn)
        if calls == 1:
            async with db_conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM drive_entries WHERE entry_id = %s", (entry.entry_id,)
                )
        return out

    monkeypatch.setattr(cache, "fetch", purged_after_the_insert)
    with pytest.raises(DriveError) as refused:
        await cache.chain(entry.entry_id)
    assert (refused.value.code, calls) == ("not_found", 2)


async def test_a_version_change_deletes_the_files_cached_attributions(space):
    await save_text(space, "kept.md", "1")
    await mark(space, "kept.md")
    await save_text(space, "kept.md", "2")
    entry = await reads.live_entry(space.space_id, path="kept.md")
    found = await cache.chain(entry.entry_id)
    assert await cache.put_attribution(entry.entry_id, 2, found.epoch, [1, 2])
    assert await cache.blame(entry.entry_id, 2, found.epoch) == [1, 2]
    async with writes.locked(space.space_id, LIMITS) as tx:
        await retention.clear(tx, await tx.file(entry.entry_id), None)
    assert await cache.blame(entry.entry_id, 2, found.epoch) is None
    # A write under the epoch it read before the change keeps nothing.
    assert not await cache.put_attribution(entry.entry_id, 2, found.epoch, [1, 2])
