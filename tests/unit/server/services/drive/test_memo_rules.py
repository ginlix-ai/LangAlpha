"""The memo rules that need no database: when the agent's save needs a
check-out and when one is open, the pinned version a save never folds into,
tickers, the next mark, and what the agent is told. The SQL side is in
tests/integration/test_drive_memos_db.py."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest

from src.server.database.drive import memos as rows
from src.server.database.drive.memos import checked, next_mark
from src.server.database.drive.policy import (
    Join,
    Start,
    Unreasoned,
    checkout_refusal,
    folds,
    is_open,
    owes_checkout,
    under,
    unchecked,
)
from src.server.database.drive.records import Actor, DriveError, Entry, MemoChange, MemoMark

USER = "user-1"
NOW = datetime(2026, 11, 25, tzinfo=UTC)
MARKED = MemoMark(marked=True, status=None, tickers=(), final_seq=None)


def _actor(**overrides) -> Actor:
    fields = {"kind": "agent", "user_id": USER, "via": "tool", "agent": "main", "sitting": "run-1"}
    return Actor(**(fields | overrides))


def _entry(memo=MARKED, head_note=None, head=_actor(), head_seq=3, change=None) -> Entry:
    return Entry(
        entry_id="e", space_id="s", path="memos/thesis.md", parent="memos", name="thesis.md",
        kind="file", head_seq=head_seq, sha256="a" * 64, size=1, content_type="text/markdown",
        is_text=True, created_at=NOW, modified_at=NOW, changed_seq=1, deleted_at=None,
        trash_id=None, deleted_by=None, modified_by=head, memo=memo, head_note=head_note,
        head_change_id=change,
    )


def _change(ended=None, touched=NOW, thread="t-1", versions=2) -> MemoChange:
    return MemoChange(
        change_id="c-1", entry_id="e", thread_id=thread, computer_id="pc", reason="Add the risks",
        opened_at=NOW, touched_at=touched, ended_at=ended, versions=versions,
    )


@pytest.mark.parametrize("via", ["tool", "cli", "api"])
def test_the_agent_is_refused_a_memo_change_outside_a_check_out(via):
    with pytest.raises(DriveError) as refused:
        under(_entry(), _actor(via=via), None, None, False)
    assert refused.value.code == "checkout_required"
    assert refused.value.details == {"path": "memos/thesis.md", "last_change": None}
    # An ended change is quoted back.
    with pytest.raises(DriveError) as refused:
        under(_entry(), _actor(via=via), None, _change(ended=NOW), False)
    assert refused.value.details["last_change"] == {
        "change_id": "c-1", "reason": "Add the risks", "date": NOW.isoformat(), "versions": 2,
        "checked_in": True,
    }


def test_where_a_save_goes():
    open_change = _change()
    # A note sent with the save starts a change of its own.
    assert under(_entry(), _actor(), "Fix the date", open_change, True) == Start("Fix the date")
    # Else the open change, whose reason is the version's note.
    assert under(_entry(), _actor(), None, open_change, True) == Join("c-1", "Add the risks")
    # Through the mount with none open: kept, with no reason.
    assert under(_entry(), _actor(via="mount"), None, open_change, False) == Unreasoned()


def test_only_the_agents_change_to_a_memo_owes_a_check_out():
    person = _actor(kind="person", agent=None, via="app")
    unmarked = dataclasses.replace(MARKED, marked=False)
    assert owes_checkout(_entry(), _actor())
    assert not owes_checkout(_entry(), person)
    assert not owes_checkout(_entry(memo=None), _actor())
    assert not owes_checkout(_entry(memo=unmarked), _actor())


def _at(minutes: float) -> datetime:
    return NOW + timedelta(minutes=minutes)


@pytest.mark.parametrize(
    ("touched", "busy", "now", "open_"),
    [
        # 1. Turn 2 starts 10 minutes after turn 1 ended.
        (5, [(0, 30), (40, None)], 50, True),
        # 2. Turn 2 starts 2 hours after turn 1 ended, and runs now.
        (5, [(0, 30), (150, None)], 160, False),
        # 3. One turn of 3 hours, checked out at minute 5.
        (5, [(0, None)], 180, True),
        # 4. Turn 2, 2 hours on, leaves the memo; turn 3 starts 5 minutes after it.
        (5, [(0, 30), (150, 160), (165, None)], 170, False),
        # 5. A continue in turn 2 of case 2, then turn 3 ten minutes after turn 2.
        (155, [(150, None)], 156, True),
        (155, [(150, 160), (170, None)], 175, True),
        # A background task still running covers the stretch after its turn.
        (5, [(0, 10), (3, None)], 90, True),
        (5, [(0, 10)], 90, False),
        # Quiet now for under an hour, and for an hour.
        (5, [(0, 30)], 89, True),
        (5, [(0, 30)], 90, False),
    ],
)
def test_a_change_lapses_once_its_chat_was_quiet_for_an_hour(touched, busy, now, open_):
    spans = [(_at(start), None if end is None else _at(end)) for start, end in busy]
    assert is_open(_change(touched=_at(touched)), spans, _at(now)) is open_


def test_a_checked_in_change_is_closed_and_one_with_no_chat_lasts_an_hour_from_its_touch():
    assert not is_open(_change(ended=NOW), [(NOW, None)], NOW)
    # 6. No chat: under an hour since the last touch is open, an hour is not.
    assert is_open(_change(thread=None), None, _at(59))
    assert not is_open(_change(thread=None), None, _at(60))


def test_a_save_folds_only_in_the_same_sitting_and_the_same_change():
    pinned = dataclasses.replace(MARKED, status="final", final_seq=3)
    assert folds(_entry(), _actor(), None)
    assert not folds(_entry(memo=pinned), _actor(), None)
    assert folds(_entry(memo=dataclasses.replace(pinned, final_seq=2)), _actor(), None)
    assert folds(_entry(change="c-1"), _actor(), "c-1")
    assert not folds(_entry(change="c-1"), _actor(), "c-2")
    assert not folds(_entry(), _actor(), "c-2")
    # Restores carry no sitting, so they never fold.
    assert not folds(_entry(), _actor(sitting=None), None)


def test_the_refusal_wording():
    shown = ".agents/drive/personal/memos/q3 thesis.md"
    assert checkout_refusal(shown, "memos/q3 thesis.md", None) == (
        ".agents/drive/personal/memos/q3 thesis.md was not saved: it is a memo, which you check "
        "out before changing. Run la drive checkout 'memos/q3 thesis.md' '<what you are changing "
        "and why>', then make this call again."
    )
    # Late on Nov 26 in New York is Nov 27 in UTC: the date is the call's.
    last = {"reason": "Add the risks", "date": "2026-11-27T02:00:00+00:00", "versions": 0,
            "checked_in": False}
    assert checkout_refusal("m.md", "m.md", last, "America/New_York") == (
        'm.md was not saved: it is a memo and your check-out of Nov 26, "Add the risks", '
        "ended after the conversation was quiet for an hour, with no change made. To carry on with "
        "that change: la drive checkout m.md --continue. For a different change: la drive "
        "checkout m.md '<what you are changing and why>'. Then make this call again."
    )
    assert "Nov 27, \"Add the risks\", ended after the conversation was quiet for an hour. To" in (
        checkout_refusal("m.md", "m.md", last | {"versions": 1}, "Not/AZone")
    )
    assert checkout_refusal("m.md", "m.md", last | {"versions": 2, "checked_in": True}) == (
        'm.md was not saved: it is a memo and you checked in your change of Nov 27, "Add the '
        'risks". To carry on with that change: la drive checkout m.md --continue. For a different '
        "change: la drive checkout m.md '<what you are changing and why>'. Then make this call "
        "again."
    )
    assert 'Nov 27, "Add the risks", with no change made. To carry on' in (
        checkout_refusal("m.md", "m.md", last | {"checked_in": True})
    )


def test_an_agent_without_bash_is_told_to_hand_the_check_out_back():
    assert checkout_refusal("m.md", "memos/m.md", None, bash=False) == (
        "m.md was not saved: it is a memo, which is checked out with a Bash command before it is "
        "changed, and you have no Bash. Report this to the agent that gave you the task: once it "
        "runs la drive checkout memos/m.md '<what is being changed and why>', this edit goes "
        "through; until then a retry is refused."
    )


def test_tickers_are_trimmed_upper_cased_and_kept_once_in_order():
    assert rows._tickers([" aapl", "MSFT ", "AAPL", "", "brk.b", "600519.SH"]) == (
        "AAPL", "MSFT", "BRK.B", "600519.SH",
    )


@pytest.mark.parametrize(
    "bad", [["two words"], ["-AAPL"], ["A" * 25], [f"T{i}" for i in range(21)], "AAPL"]
)
def test_a_bad_ticker_list_is_refused(bad):
    with pytest.raises(DriveError) as refused:
        rows._tickers(bad)
    assert refused.value.code == "invalid"


@pytest.mark.parametrize(
    "change",
    [
        {"memo": False, "status": "draft"},
        {"memo": False, "tickers": []},
        {"status": "done"},
        {"tickers": ["two words"]},
        {"note": "why"},
    ],
)
def test_a_bad_change_is_refused_invalid(change):
    with pytest.raises(DriveError) as refused:
        checked(change)
    assert refused.value.code == "invalid"


def test_next_mark():
    path = "memos/thesis.md"
    # Marking a file never marked.
    made = next_mark(None, checked({"memo": True, "tickers": ["aapl"]}), 3, NOW, path)
    # The version it was marked at stays as written from then on.
    assert made == MemoMark(
        marked=True, status=None, tickers=("AAPL",), final_seq=None, kept_through_seq=3
    )
    # Final pins the head; final again on the same head changes nothing,
    # and on a later head pins that one.
    final = next_mark(made, {"status": "final"}, 3, NOW, path)
    assert (final.final_seq, final.final_at) == (3, NOW)
    later = datetime(2026, 11, 26, tzinfo=UTC)
    assert next_mark(final, {"status": "final"}, 3, later, path) == final
    repinned = next_mark(final, {"status": "final"}, 5, later, path)
    assert (repinned.final_seq, repinned.final_at) == (5, later)
    # Leaving final unpins.
    draft = next_mark(final, {"status": "draft"}, 5, later, path)
    assert (draft.status, draft.final_seq, draft.final_at) == ("draft", None, None)
    # A change that changes nothing comes back equal.
    assert next_mark(made, {"tickers": ("AAPL",)}, 3, NOW, path) == made
    # Un-marking keeps the rest and the record so far.
    off = next_mark(final, {"memo": False}, 4, later, path)
    assert off == dataclasses.replace(final, marked=False, kept_through_seq=4)


@pytest.mark.parametrize(
    ("old", "change"),
    [
        (None, {"memo": False}),
        (dataclasses.replace(MARKED, marked=False), {"memo": False}),
        (None, {"status": "draft"}),
        (dataclasses.replace(MARKED, marked=False), {"tickers": ("AAPL",)}),
    ],
)
def test_a_file_that_is_not_a_live_memo_answers_not_memo(old, change):
    with pytest.raises(DriveError) as refused:
        next_mark(old, change, 3, NOW, "a.md")
    assert refused.value.code == "not_memo"



def test_a_mount_save_with_no_check_out_reports_the_draft_line():
    mount = _actor(via="mount")
    shown = "/home/workspace/.agents/drive/personal/memos/thesis.md"
    line = unchecked(_entry(head=mount), shown, mount, None)
    assert line == (
        f"{shown} was saved. It is a memo and this save has no reason yet: "
        "la drive checkout memos/thesis.md '<what changed and why>' gives it one."
    )
    assert unchecked(_entry(head=mount), shown, mount, _change(ended=NOW)) == (
        line[:-1] + ', or la drive checkout memos/thesis.md --continue if it is part of "Add the risks".'
    )
    assert unchecked(_entry(head=mount, head_note="why"), "x", mount, None) is None
    assert unchecked(_entry(head=mount, change="c-1"), "x", mount, None) is None
    assert unchecked(_entry(head=mount, memo=None), "x", mount, None) is None
    unmarked = dataclasses.replace(MARKED, marked=False)
    assert unchecked(_entry(head=mount, memo=unmarked), "x", mount, None) is None
    # Only through the mount, and only where the command was shown a path.
    cli = _actor(via="cli")
    assert unchecked(_entry(head=cli), "x", cli, None) is None
    assert unchecked(_entry(head=mount), None, mount, None) is None


@pytest.mark.parametrize(
    "head",
    [
        _actor(kind="person", via="app", agent=None, sitting=None),
        _actor(via="mount", sitting="run-0"),
        None,
    ],
)
def test_a_command_that_changed_nothing_is_not_asked_to_explain_anothers_version(head):
    """`sed -i` that matches nothing saves the same bytes: the head is still
    whoever made it, and this command has nothing to give a reason for."""
    assert unchecked(_entry(head=head), "x", _actor(via="mount"), None) is None


def test_a_same_bytes_save_over_the_version_marked_is_not_asked_for_a_check_out():
    """The sitting's mount save before the mark is still the head; a check-out
    never takes a version at or below the mark, so it cannot give it a reason."""
    mount = _actor(via="mount")
    at_mark = dataclasses.replace(MARKED, kept_through_seq=3)
    assert unchecked(_entry(head=mount, memo=at_mark), "x", mount, None) is None
    after = dataclasses.replace(MARKED, kept_through_seq=2)
    assert unchecked(_entry(head=mount, memo=after), "x", mount, None) is not None


def test_an_agent_asking_to_say_why_afterwards_is_pointed_at_the_check_out():
    from src.server.services.drive.memo import explain_refused

    assert explain_refused("memos/q3 thesis.md") == (
        "A version's reason comes from your check-out: "
        "la drive checkout 'memos/q3 thesis.md' '<what you are changing and why>'"
    )


def test_a_check_out_command_with_no_change_to_act_on_says_what_to_run():
    from src.server.database.drive.policy import nothing_to_check_in, nothing_to_continue

    assert nothing_to_continue("memos/q3 thesis.md") == (
        "This conversation has no earlier change to memos/q3 thesis.md to carry on with. "
        "Start one: la drive checkout 'memos/q3 thesis.md' '<what you are changing and why>'"
    )
    assert nothing_to_check_in("m.md") == "This conversation has no change to m.md to check in."


@pytest.mark.asyncio
async def test_a_line_that_cannot_be_read_never_fails_the_save_it_speaks_of(monkeypatch):
    from src.server.services.drive import memo

    asked = []

    async def down(entry_id, actor):
        asked.append(entry_id)
        raise ConnectionError("the pool lost its connection")

    monkeypatch.setattr(memo.checkouts, "latest", down)
    mount = _actor(via="mount")
    # A head just written, so the line is due and the lookup is reached.
    saved = dataclasses.replace(_entry(head=mount), modified_at=datetime.now(UTC))
    assert await memo.unchecked(saved, "/w/.agents/drive/personal/a.md", mount) is None
    assert asked == [saved.entry_id]


@pytest.mark.asyncio
async def test_titles_that_cannot_be_read_never_fail_the_answer(monkeypatch):
    from src.server.app import drive as routes
    from src.server.models.drive import ModifiedByOut

    async def down(user_id, thread_ids):
        raise ConnectionError("the pool lost its connection")

    monkeypatch.setattr(routes.shared_access, "thread_titles", down)
    by = ModifiedByOut(kind="agent", user_id="u", agent="main", via="cli", thread_id="t-1")
    await routes._titled("u", [by, None])
    assert by.thread_title is None



@pytest.mark.asyncio
async def test_a_head_the_chat_went_quiet_after_gets_no_check_out_line(monkeypatch):
    """A save of the same bytes leaves an old head in place; ``_adopt``
    leaves such a head, so the line that promises it is not written."""
    from contextlib import asynccontextmanager

    from src.server.database.drive import changes
    from src.server.services.drive import memo

    mount = _actor(kind="agent", via="mount", sitting="s-1", thread_id="t-1")
    now = datetime.now(UTC)
    reads = []

    async def busy(conn, thread_id, since):
        reads.append(thread_id)
        return spans

    @asynccontextmanager
    async def connection():
        yield None

    monkeypatch.setattr(changes, "thread_busy", busy)
    monkeypatch.setattr(changes, "get_db_connection", connection)
    monkeypatch.setattr(changes, "latest", _none)

    fresh = dataclasses.replace(_entry(head=mount), modified_at=now)
    spans = []
    assert await memo.unchecked(fresh, "x", mount) is not None
    assert reads == []  # a head just made is never read for

    old = dataclasses.replace(_entry(head=mount), modified_at=now - timedelta(hours=3))
    assert await memo.unchecked(old, "x", mount) is None
    spans = [(now - timedelta(hours=3), None)]  # one turn running since
    assert await memo.unchecked(old, "x", mount) is not None
    assert reads == ["t-1", "t-1"]


async def _none(*args, **kwargs):
    return None
