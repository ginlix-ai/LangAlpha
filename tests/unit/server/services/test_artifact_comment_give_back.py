"""What a send marked and no agent read waits again, whichever way it ended: a
stop handing a steer back, the server dropping it, a run that never began, a
worker that died before admission. Marked ``sent``, a comment never goes out
again, and the reader is shown the note, never the block the agent reads."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.runs import outbox
from src.server.services.artifacts import comments

_SHOWN = {"note": "my note", "reading": "chat", "comments": [{"comment_id": "c1"}]}


def _steer(**over):
    return {
        "payload": {
            "content": "my note\n\n<file-comments>...</file-comments>",
            "request_key": "k",
            "timestamp": 1.0,
            "user_id": "u",
            "artifact_comments": _SHOWN,
            **over,
        },
    }


def test_a_returned_steer_reads_as_the_note_its_sender_typed():
    entry = outbox._returned_entry(_steer())

    assert entry["content"] == "my note"
    assert entry["artifact_comments"] == _SHOWN


def test_a_returned_steer_with_no_note_reads_as_its_comments_alone():
    shown = {**_SHOWN, "note": ""}
    entry = outbox._returned_entry(_steer(artifact_comments=shown))

    assert entry["content"] == ""
    assert entry["artifact_comments"] == shown


def test_a_plain_steer_reads_as_it_was_sent():
    entry = outbox._returned_entry({"payload": {"content": "hello", "request_key": "k"}})

    assert entry == {"content": "hello", "request_key": "k", "timestamp": None}


@pytest.mark.asyncio
async def test_a_stop_gives_back_the_comments_of_the_steers_it_returns(monkeypatch):
    from src.server.database.artifacts import delivery

    release = AsyncMock(return_value=[])
    monkeypatch.setattr(delivery, "release_unread", release)
    plain = {"payload": {"content": "hello", "request_key": "plain"}}

    await outbox._give_back_comments("conn", [_steer(), plain])

    release.assert_awaited_once_with("conn", user_id="u", request_keys=["k"])


@pytest.mark.asyncio
async def test_steers_carrying_no_comments_touch_no_comment(monkeypatch):
    from src.server.database.artifacts import delivery

    release = AsyncMock()
    monkeypatch.setattr(delivery, "release_unread", release)

    await outbox._give_back_comments("conn", [{"payload": {"content": "hello"}}])

    release.assert_not_awaited()


@pytest.fixture
def db(monkeypatch):
    fake = SimpleNamespace(
        release_unread_send=AsyncMock(return_value=[("a1", "c1")]),
        release_claim=AsyncMock(return_value=[("a1", "c1")]),
        list_abandoned_claims=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(comments, "artifacts_db", fake)
    monkeypatch.setattr(comments, "_publish_changes", AsyncMock())
    return fake


@pytest.mark.asyncio
async def test_a_run_that_never_began_gives_back_what_its_send_claimed(db):
    scope = comments.CommentSendScope(
        thread_id="t", user_id="u", chief_of_staff=False, timezone=None
    )
    scope._claimed = [SimpleNamespace()]
    scope._request_key = "k"

    await scope.run_never_began()

    db.release_unread_send.assert_awaited_once_with(user_id="u", request_key="k")
    comments._publish_changes.assert_awaited_once_with("u", "t", {"a1": ["c1"]})


@pytest.mark.asyncio
async def test_a_send_that_claimed_nothing_has_nothing_to_give_back(db):
    scope = comments.CommentSendScope(
        thread_id="t", user_id="u", chief_of_staff=False, timezone=None
    )

    await scope.run_never_began()

    db.release_unread_send.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_give_back_never_raises_into_the_stop(db):
    db.release_unread_send.side_effect = RuntimeError("down")

    await comments.give_back_unread(user_id="u", thread_id="t", request_key="k")

    comments._publish_changes.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_sweep_waits_out_a_send_held_behind_a_compaction(db):
    await comments.release_abandoned_claims()

    kwargs = db.list_abandoned_claims.await_args.kwargs
    assert kwargs["older_than_seconds"] >= 10 * 60
    assert kwargs["within_seconds"] > kwargs["older_than_seconds"]


@pytest.mark.asyncio
async def test_a_lone_workers_startup_takes_them_at_once(db):
    await comments.release_abandoned_claims(assume_dead=True)

    assert db.list_abandoned_claims.await_args.kwargs["older_than_seconds"] == 0


@pytest.mark.asyncio
async def test_the_sweep_asks_again_under_the_comments_locks(db):
    """``release_claim`` is what refuses a send a run took since the listing."""
    db.list_abandoned_claims.return_value = [
        SimpleNamespace(user_id="u", request_key="k", thread_id="t")
    ]

    assert await comments.release_abandoned_claims() == 1

    db.release_claim.assert_awaited_once_with(user_id="u", request_key="k")
    comments._publish_changes.assert_awaited_once_with("u", "t", {"a1": ["c1"]})


@pytest.mark.asyncio
async def test_a_stop_tells_other_tabs_which_comments_it_gave_back(monkeypatch):
    published = AsyncMock()
    monkeypatch.setattr(comments, "publish_comment_change", published)
    meta = AsyncMock(return_value={"user_id": "u"})
    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_auth_meta", meta
    )
    chip = {"artifact_id": "a1", "workspace_id": "w", "path": "r.html"}

    await comments.publish_returned_comments(
        "t",
        [
            {
                "artifact_comments": {
                    "comments": [
                        {**chip, "comment_id": "c1"},
                        {**chip, "comment_id": "c2"},
                    ]
                }
            },
            {"content": "a steer with no comments"},
        ],
    )

    published.assert_awaited_once_with(
        "u",
        artifact_id="a1",
        workspace_id="w",
        path="r.html",
        thread_id="t",
        comment_ids=["c1", "c2"],
    )


@pytest.mark.asyncio
async def test_a_stop_that_gave_back_no_comments_tells_no_one(monkeypatch):
    meta = AsyncMock()
    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_auth_meta", meta
    )

    await comments.publish_returned_comments("t", [{"content": "a plain steer"}])

    meta.assert_not_awaited()
