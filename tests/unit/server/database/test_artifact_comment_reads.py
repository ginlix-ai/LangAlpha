"""The read-cursor contract of artifact comments: a message takes its seq under
the comment's row lock, the owner's own words never count as unread, and the
cursor only moves forward, capped at the latest message, without writing the
comment row."""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import comments as comments_db

_A = "00000000-0000-0000-0000-00000000000a"
_C = "00000000-0000-0000-0000-00000000000c"
_T = "00000000-0000-0000-0000-00000000000d"
_U = "00000000-0000-0000-0000-00000000000e"
_W = "00000000-0000-0000-0000-00000000000f"
_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _db(artifacts_db):
    return artifacts_db


def _comment_row() -> dict:
    """The comment as a write reads it back after its change."""
    return {
        "comment_id": _C,
        "artifact_id": _A,
        "number": 1,
        "path": "report.html",
        "file_sha256": None,
        "title": None,
        "anchor": {},
        "status": "open",
        "delivery": "note",
        "thread_id": None,
        "thread_title": None,
        "thread_archived": False,
        "created_at": _NOW,
        "updated_at": _NOW,
        "resolved_at": None,
        "read_through": 1,
        "unread": 0,
    }


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


def _calls(mock_cursor) -> list[tuple[str, tuple]]:
    return [
        (_flat(c.args[0]), c.args[1] if len(c.args) > 1 else ())
        for c in mock_cursor.execute.call_args_list
    ]


def _index(calls, needle: str) -> int:
    return next(i for i, (sql, _) in enumerate(calls) if needle in sql)


def _upserts(calls):
    return [(s, p) for s, p in calls if "INSERT INTO artifact_comment_reads" in s]


def test_the_cursor_never_moves_back():
    sql = _flat(comments_db._ADVANCE_CURSOR_SQL)
    assert "GREATEST(r.read_through, EXCLUDED.read_through)" in sql
    assert "WHERE r.read_through < EXCLUDED.read_through RETURNING" in sql


@pytest.mark.asyncio
async def test_advancing_reports_whether_the_cursor_moved(mock_cursor):
    mock_cursor.fetchone.side_effect = [{"read_through": 3}, None]

    assert await comments_db._advance_cursor(
        mock_cursor, comment_id=_C, user_id=_U, through=3
    )
    assert not await comments_db._advance_cursor(
        mock_cursor, comment_id=_C, user_id=_U, through=2
    )
    assert mock_cursor.execute.await_args.args[1] == (_C, _U, 2)


@pytest.mark.asyncio
async def test_advancing_to_nothing_touches_nothing(mock_cursor):
    assert not await comments_db._advance_cursor(
        mock_cursor, comment_id=_C, user_id=_U, through=0
    )
    mock_cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_owner_message_takes_its_seq_under_the_lock_and_moves_the_cursor(
    mock_cursor,
):
    mock_cursor.fetchone.side_effect = [
        {"delivery": "note", "status": "open", "thread_id": None, "workspace_id": _W},
        {"seq": 7},
        {"read_through": 7},
        _comment_row(),
    ]

    await artifacts.add_owner_message(
        user_id=_U, artifact_id=_A, comment_id=_C, body="hi", delivery=None
    )

    calls = _calls(mock_cursor)
    lock = _index(calls, "FOR UPDATE OF c")
    insert = _index(calls, "INSERT INTO artifact_comment_messages")
    upsert = _index(calls, "INSERT INTO artifact_comment_reads")
    assert lock < insert < upsert
    assert "SELECT COALESCE(MAX(seq), 0) + 1" in calls[insert][0]
    assert calls[upsert][1] == (_C, _U, 7)


@pytest.mark.asyncio
async def test_a_new_comment_starts_the_cursor_at_its_first_message(mock_cursor):
    mock_cursor.fetchone.side_effect = [
        {"last_comment_number": 1, "path": "report.html", "workspace_id": _W},
        {"comment_id": _C},
        {"seq": 1},
        {"read_through": 1},
        _comment_row(),
    ]

    await artifacts.create_comment(
        user_id=_U, artifact_id=_A, anchor={}, body="hi", delivery="note"
    )

    calls = _calls(mock_cursor)
    number = _index(calls, "SET last_comment_number = l.last_comment_number + 1")
    assert (
        number
        < _index(calls, "INSERT INTO artifact_comments ")
        < _index(calls, "INSERT INTO artifact_comment_messages")
    )
    assert [p for _, p in _upserts(calls)] == [(_C, _U, 1)]


@pytest.mark.asyncio
async def test_an_agent_reply_is_locked_first_and_leaves_the_cursor_alone(mock_cursor):
    mock_cursor.fetchone.side_effect = [
        {
            "artifact_id": _A,
            "number": 1,
            "delivery": "sent",
            "status": "open",
            "path": "report.html",
            "workspace_id": _W,
        },
        {"seq": 2},
    ]

    await artifacts.add_agent_reply(
        user_id=_U, thread_id=_T, comment_id=_C, body="ok", resolve=False
    )

    calls = _calls(mock_cursor)
    insert = _index(calls, "INSERT INTO artifact_comment_messages")
    assert _index(calls, "FOR UPDATE OF c") < insert
    assert calls[insert][1][1] == "agent"
    assert not any("artifact_comment_reads" in sql for sql, _ in calls)


@pytest.mark.asyncio
async def test_a_read_past_the_latest_message_is_capped_at_it(mock_cursor):
    mock_cursor.fetchone.side_effect = [
        {"latest": 4},
        {"read_through": 4},
        _comment_row(),
    ]

    _, moved = await artifacts.mark_comment_read(
        user_id=_U, artifact_id=_A, comment_id=_C, through=99
    )

    calls = _calls(mock_cursor)
    assert moved
    assert [p for _, p in _upserts(calls)] == [(_C, _U, 4)]
    assert not any("UPDATE artifact_comments" in sql for sql, _ in calls)


@pytest.mark.asyncio
async def test_a_read_behind_the_cursor_moves_nothing(mock_cursor):
    mock_cursor.fetchone.side_effect = [{"latest": 4}, None, _comment_row()]

    _, moved = await artifacts.mark_comment_read(
        user_id=_U, artifact_id=_A, comment_id=_C, through=1
    )

    assert not moved


@pytest.mark.asyncio
async def test_reading_a_comment_that_is_not_the_owners_is_not_found(mock_cursor):
    mock_cursor.fetchone.side_effect = [None]

    with pytest.raises(artifacts.CommentNotFound):
        await artifacts.mark_comment_read(
            user_id=_U, artifact_id=_A, comment_id=_C, through=1
        )

    assert len(_upserts(_calls(mock_cursor))) == 0


@pytest.mark.asyncio
async def test_reading_an_artifact_locks_in_id_order_and_reports_what_moved(
    mock_cursor,
):
    mock_cursor.fetchall.side_effect = [
        [{"comment_id": "a"}, {"comment_id": "b"}],
        [{"comment_id": "a"}],
        [],
    ]

    comments, moved = await artifacts.mark_artifact_read(user_id=_U, artifact_id=_A)

    calls = _calls(mock_cursor)
    assert "ORDER BY comment_id FOR KEY SHARE" in calls[0][0]
    (upsert, params), *_ = _upserts(calls)
    assert "MAX(m.seq)" in upsert
    assert "GREATEST(r.read_through, EXCLUDED.read_through)" in upsert
    assert "WHERE r.read_through < EXCLUDED.read_through" in upsert
    assert params == (_U, ["a", "b"])
    assert moved == ["a"]
    assert comments == []
    assert not any("UPDATE artifact_comments" in sql for sql, _ in calls)


@pytest.mark.asyncio
async def test_reading_an_artifact_without_comments_writes_no_cursor(mock_cursor):
    mock_cursor.fetchall.side_effect = [[], []]

    _, moved = await artifacts.mark_artifact_read(user_id=_U, artifact_id=_A)

    assert moved == []
    assert _upserts(_calls(mock_cursor)) == []


def test_only_agent_messages_past_the_cursor_are_unread():
    sql = _flat(comments_db._UNREAD_SQL)
    assert "um.author = 'agent'" in sql
    assert "um.seq > COALESCE(r.read_through, 0)" in sql


@pytest.mark.asyncio
async def test_an_artifact_counts_its_unread_comments_not_their_messages(mock_cursor):
    mock_cursor.fetchone.return_value = None
    assert await artifacts.comment_counts(_A, user_id=_U) == artifacts.CommentCounts(
        open=0, unread=0
    )

    mock_cursor.fetchone.return_value = {"open": 3, "unread": 2}
    counts = await artifacts.comment_counts(_A, user_id=_U)
    assert (counts.open, counts.unread) == (3, 2)
    sql = _calls(mock_cursor)[-1][0]
    assert "COUNT(*) FILTER (WHERE c.status = 'open')::int AS open" in sql
    assert "SUM(" not in sql
    unread = _flat(comments_db._UNREAD_SQL).strip()
    assert f"COUNT(*) FILTER (WHERE {unread} > 0)::int AS unread" in sql
