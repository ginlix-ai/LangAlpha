"""Editing the owner's words before a send takes them: one statement locks
the comment row as a send's claim does and changes the message only while
neither the comment nor the message has been taken, so an edit and a claim
never both win."""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import CommentConflict, CommentNotFound

_A = "00000000-0000-0000-0000-00000000000a"
_C = "00000000-0000-0000-0000-00000000000c"
_M = "00000000-0000-0000-0000-00000000000b"
_U = "00000000-0000-0000-0000-00000000000e"
_NOW = datetime(2026, 11, 27, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _db(artifacts_db):
    return artifacts_db


def _comment_row() -> dict:
    return {
        "comment_id": _C, "artifact_id": _A, "kind": "drive", "number": 1, "path": "notes.md",
        "file_sha256": None, "title": None, "anchor": {"kind": "element"}, "status": "open",
        "delivery": "queued", "thread_id": None, "thread_title": None, "thread_archived": False,
        "created_at": _NOW, "updated_at": _NOW, "resolved_at": None, "read_through": 1, "unread": 0,
    }


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


async def _edit(**over):
    return await artifacts.edit_owner_message(
        **{"user_id": _U, "artifact_id": _A, "comment_id": _C, "message_id": _M,
           "body": "new words", "caller": _U, **over}
    )


@pytest.mark.asyncio
async def test_an_edit_is_decided_by_the_rows_in_one_statement(mock_cursor):
    mock_cursor.fetchone.side_effect = [{"message_id": _M}, _comment_row()]
    mock_cursor.fetchall.return_value = []

    comment = await _edit()

    sql, params = mock_cursor.execute.await_args_list[0].args
    flat = _flat(sql)
    # The comment row is taken first, under the lock a claim takes, and only
    # while no send has it.
    assert flat.index("UPDATE artifact_comments") < flat.index("UPDATE artifact_comment_messages")
    assert "AND delivery <> 'sent'" in flat
    # Only an owner's message no send carried, written by the caller.
    assert "m.author = 'user'" in flat
    assert "m.sent_at IS NULL AND m.request_key IS NULL" in flat
    assert "m.written_by IS NOT DISTINCT FROM %(writer)s" in flat
    # The anchor is not touched.
    assert "anchor" not in flat
    assert params["body"] == "new words" and params["writer"] is None
    assert comment.comment_id == _C


@pytest.mark.asyncio
async def test_words_a_send_took_are_refused_as_reached(mock_cursor):
    mock_cursor.fetchone.side_effect = [None, {"author": "user", "written_by": None}]

    with pytest.raises(CommentConflict) as refused:
        await _edit()

    assert refused.value.code == "comment_sent"
    sql, params = mock_cursor.execute.await_args_list[1].args
    assert "LEFT JOIN artifact_comment_messages m" in _flat(sql)
    assert params == (_M, _C, _A, _U)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("found", "caller"),
    [
        # Another member's words in a shared workspace.
        ({"author": "user", "written_by": "lee"}, "kim"),
        # A member's words, edited by the holder.
        ({"author": "user", "written_by": "lee"}, _U),
        # The agent's words.
        ({"author": "agent", "written_by": None}, _U),
        # No message by that id on the comment.
        ({"author": None, "written_by": None}, _U),
    ],
)
async def test_words_the_caller_did_not_write_are_not_found(mock_cursor, found, caller):
    """Nothing was sent, so ``comment_sent`` would tell the caller something
    false; words not theirs are out of reach, as another user's comment is."""
    mock_cursor.fetchone.side_effect = [None, found]

    with pytest.raises(CommentNotFound):
        await _edit(caller=caller)


@pytest.mark.asyncio
async def test_another_users_comment_is_not_found(mock_cursor):
    mock_cursor.fetchone.side_effect = [None, None]

    with pytest.raises(CommentNotFound):
        await _edit()


@pytest.mark.asyncio
async def test_a_member_edits_only_their_own_words(mock_cursor):
    mock_cursor.fetchone.side_effect = [{"message_id": _M}, _comment_row()]
    mock_cursor.fetchall.return_value = []

    await _edit(user_id="shared-holder", caller="lee")

    assert mock_cursor.execute.await_args_list[0].args[1]["writer"] == "lee"


@pytest.mark.asyncio
async def test_the_route_edits_tells_the_queue_and_refuses_words_a_send_took(monkeypatch):
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from src.server.app.artifacts import comments as comment_routes
    from src.server.app.artifacts.models import EditCommentMessageRequest
    from src.server.database.artifacts import Artifact
    from src.server.database.artifacts.comments import Comment
    from src.server.services.shared_workspaces.access import Access

    record = Artifact(
        artifact_id=_A, code="c0de", workspace_id="home", kind="drive", path="e1",
        created_thread_id=None, bound_thread_id=None, keep_updated=False, latest_seq=0,
        shared_at=None, files=None, revision=0, created_at=_NOW,
    )
    monkeypatch.setattr(comment_routes, "reached", AsyncMock(return_value=(record, Access(_U))))
    edited = AsyncMock(return_value=Comment(**{**_comment_row(), "thread_id": "t-1"}))
    monkeypatch.setattr(comment_routes.artifacts_db, "edit_owner_message", edited)
    told = AsyncMock()
    monkeypatch.setattr(comment_routes, "publish_comment_change", told)

    resp = await comment_routes.edit_comment_message(
        _A, _C, _M, EditCommentMessageRequest(body="  new words "), _U
    )

    assert resp.comment_id == _C
    assert edited.await_args.kwargs["body"] == "new words"
    assert told.await_args.kwargs["thread_id"] == "t-1"

    edited.side_effect = CommentConflict("comment_sent", "reached")
    with pytest.raises(HTTPException) as refused:
        await comment_routes.edit_comment_message(
            _A, _C, _M, EditCommentMessageRequest(body="again"), _U
        )
    assert refused.value.status_code == 409


def test_an_empty_edit_is_refused_as_create_refuses_one():
    from pydantic import ValidationError

    from src.server.app.artifacts.models import EditCommentMessageRequest

    with pytest.raises(ValidationError):
        EditCommentMessageRequest(body="   ")
    with pytest.raises(ValidationError):
        EditCommentMessageRequest(body="x" * 8001)
