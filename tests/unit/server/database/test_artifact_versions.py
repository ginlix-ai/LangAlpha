"""Version rules under the record's lock: a keep-updated cut lands only on
the version its read began from, a cut prunes kept-updated versions past the
newest ones and nothing else, and a refused send gives its comments back only
when, after their locks, no copy of it reached a run."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import delivery, versions

_A = "00000000-0000-0000-0000-00000000000a"
_U = "00000000-0000-0000-0000-00000000000e"
_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _db(artifacts_db):
    return artifacts_db


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


def _sql(mock_cursor) -> list[str]:
    return [_flat(c.args[0]) for c in mock_cursor.execute.call_args_list]


def _record(latest_seq: int) -> artifacts.Artifact:
    return artifacts.Artifact(
        artifact_id=_A,
        code="c0de",
        workspace_id="w",
        path="r.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=True,
        latest_seq=latest_seq,
        shared_at=_NOW,
        files=("r.html",),
        revision=1,
        created_at=_NOW,
    )


def _file(text: str) -> artifacts.VersionFile:
    data = text.encode()
    return artifacts.VersionFile(
        "r.html", hashlib.sha256(data).hexdigest(), len(data), "text/html", data
    )


@pytest.fixture
def locked(monkeypatch):
    lock = AsyncMock(return_value=_record(latest_seq=4))
    monkeypatch.setattr(versions, "lock_owned", lock)
    return lock


@pytest.mark.asyncio
async def test_a_keep_updated_read_older_than_the_latest_cuts_nothing(
    locked, mock_cursor
):
    result = await artifacts.publish_version(
        user_id=_U,
        artifact_id=_A,
        origin="keep_updated",
        files=[_file("stale")],
        based_on=3,
    )

    assert (result.cut, result.seq) == (False, 4)
    assert not any("INSERT" in s or "DELETE" in s for s in _sql(mock_cursor))


@pytest.mark.asyncio
async def test_a_cut_prunes_kept_updated_versions_past_the_newest(locked, mock_cursor):
    mock_cursor.fetchall.return_value = [{"path": "r.html", "sha256": "0" * 64}]
    mock_cursor.fetchone.return_value = None

    result = await artifacts.publish_version(
        user_id=_U,
        artifact_id=_A,
        origin="keep_updated",
        files=[_file("fresh")],
        based_on=4,
    )

    assert (result.cut, result.seq) == (True, 5)
    calls = mock_cursor.execute.call_args_list
    (prune,) = [c for c in calls if "DELETE FROM artifact_versions" in c.args[0]]
    sql = _flat(prune.args[0])
    assert "v.origin = 'keep_updated'" in sql
    assert "v.seq <> %(latest)s" in sql
    assert "ORDER BY seq DESC LIMIT %(keep)s" in sql
    assert prune.args[1] == {
        "artifact_id": _A,
        "latest": 5,
        "keep": artifacts.KEEP_UPDATED_VERSIONS,
    }
    assert artifacts.KEEP_UPDATED_VERSIONS == 10
    insert = next(
        i for i, c in enumerate(calls) if "INSERT INTO artifact_versions" in c.args[0]
    )
    assert insert < calls.index(prune)


@pytest.mark.asyncio
async def test_a_publish_clears_a_legacy_refusal(locked, mock_cursor):
    mock_cursor.fetchone.return_value = None

    await artifacts.publish_version(
        user_id=_U, artifact_id=_A, origin="owner", files=[_file("x")]
    )

    (update,) = [
        s for s in _sql(mock_cursor) if s.lstrip().startswith("UPDATE share_links")
    ]
    assert "legacy_refused = NULL" in update


@pytest.mark.asyncio
async def test_a_release_reads_for_a_run_after_the_comment_locks(
    mock_cursor, monkeypatch
):
    from src.server.database.runs import lifecycle, outbox

    order: list[str] = []
    mock_cursor.fetchall.return_value = [("c1", _A)]

    async def has_run(conn, key):
        order.append("run")
        return True

    monkeypatch.setattr(lifecycle, "request_key_has_run", has_run)
    monkeypatch.setattr(outbox, "steer_key_taken", AsyncMock(return_value=False))

    assert await artifacts.release_claim(user_id=_U, request_key="k") == []

    sql = _sql(mock_cursor)
    assert "FOR UPDATE" in sql[0]
    assert order == ["run"]
    assert not any("UPDATE artifact_comment_messages" in s for s in sql)


@pytest.mark.asyncio
async def test_a_release_with_no_run_or_steer_gives_back(mock_cursor, monkeypatch):
    from src.server.database.runs import lifecycle, outbox

    mock_cursor.fetchall.return_value = [("c1", _A)]
    monkeypatch.setattr(lifecycle, "request_key_has_run", AsyncMock(return_value=False))
    monkeypatch.setattr(outbox, "steer_key_taken", AsyncMock(return_value=False))

    assert await artifacts.release_claim(user_id=_U, request_key="k") == [(_A, "c1")]
    assert any("UPDATE artifact_comment_messages" in s for s in _sql(mock_cursor))


@pytest.mark.asyncio
async def test_an_owners_publish_does_not_undo_a_stop_that_landed_during_its_read(
    locked, mock_cursor
):
    with pytest.raises(artifacts.SharingChanged):
        await artifacts.publish_version(
            user_id=_U,
            artifact_id=_A,
            origin="owner",
            files=[_file("x")],
            # What the record said when the read began; a stop has moved it.
            revision=0,
        )

    assert not any("UPDATE share_links" in s or "INSERT" in s for s in _sql(mock_cursor))


@pytest.mark.asyncio
async def test_an_owners_publish_does_not_replace_a_version_cut_during_its_read(
    locked, mock_cursor
):
    with pytest.raises(artifacts.VersionMoved):
        await artifacts.publish_version(
            user_id=_U,
            artifact_id=_A,
            origin="owner",
            files=[_file("x")],
            # The latest when the read began; version 4 has landed since.
            based_on=3,
            revision=1,
        )

    assert not any("UPDATE share_links" in s or "INSERT" in s for s in _sql(mock_cursor))


@pytest.mark.asyncio
async def test_an_owners_publish_at_the_revision_it_read_goes_through(
    locked, mock_cursor
):
    mock_cursor.fetchone.return_value = None

    await artifacts.publish_version(
        user_id=_U, artifact_id=_A, origin="owner", files=[_file("x")], revision=1
    )

    assert any("UPDATE share_links" in s for s in _sql(mock_cursor))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "keep_updated, changed", [(False, True), (True, False), (None, False)]
)
async def test_a_publish_of_the_same_files_changes_the_record_only_with_the_setting(
    locked, mock_cursor, monkeypatch, keep_updated, changed
):
    """Other views show the setting, so moving it alone is still a change."""
    now = _file("x")
    monkeypatch.setattr(
        versions, "_hashes", AsyncMock(return_value={now.path: now.sha256})
    )
    mock_cursor.fetchone.return_value = None

    result = await artifacts.publish_version(
        user_id=_U, artifact_id=_A, origin="owner", files=[now], keep_updated=keep_updated
    )

    assert (result.cut, result.shared_now, result.changed) == (False, False, changed)


@pytest.mark.asyncio
async def test_a_deleted_workspace_ends_an_owners_grant_however_long_it_has_left(
    mock_cursor,
):
    mock_cursor.fetchone.return_value = None

    assert await artifacts.get_version_file(_A, 1, "r.html") is None

    (sql,) = _sql(mock_cursor)
    assert "w.status <> 'deleted'" in sql


@pytest.mark.asyncio
async def test_a_deleted_workspaces_comments_stop_waiting_for_a_send(mock_cursor):
    """A send cannot claim them, so the composer must not offer them."""
    mock_cursor.fetchall.return_value = []

    await artifacts.list_queued_comments(user_id=_U, thread_id="t")

    assert "w.status <> 'deleted'" in _sql(mock_cursor)[0]


@pytest.mark.asyncio
async def test_a_links_first_cut_leaves_it_following_the_file(
    monkeypatch, mock_cursor
):
    """It was live until now, whichever build shared it."""
    record = _record(latest_seq=0)
    monkeypatch.setattr(versions, "lock_owned", AsyncMock(return_value=record))
    mock_cursor.fetchone.return_value = None

    await artifacts.publish_version(
        user_id=_U, artifact_id=_A, origin="legacy", files=[_file("x")]
    )

    (params,) = [
        c.args[1]
        for c in mock_cursor.execute.call_args_list
        if "UPDATE share_links" in c.args[0]
    ]
    assert params["keep_updated"] is True


def _said(*rows):
    return {
        "c1": [
            SimpleNamespace(
                message_id=m,
                author=author,
                body=m,
                sent_at=_NOW if key else None,
                request_key=key,
            )
            for m, author, key in rows
        ]
    }


_CLAIMABLE = {
    "comment_id": "c1",
    "artifact_id": _A,
    "number": 1,
    "title": None,
    "anchor": {},
    "path": "r.html",
    "workspace_id": "w",
    "recorded_at": _NOW,
}


@pytest.mark.asyncio
async def test_a_retransmit_carries_what_its_first_copy_did_and_not_words_added_since(
    mock_cursor, monkeypatch
):
    """A copy that lost the race would mark them sent and never deliver them."""
    mock_cursor.fetchall.return_value = [_CLAIMABLE]
    monkeypatch.setattr(
        delivery,
        "messages_by_comment",
        AsyncMock(return_value=_said(("m1", "user", "k"), ("m2", "user", None))),
    )

    (claimed,) = await artifacts.claim_comments(
        user_id=_U, thread_id="t", comment_ids=["c1"], request_key="k"
    )

    assert claimed.message_ids == ["m1"]
    assert claimed.delivered == ["m1"]
    assert claimed.earlier == []


@pytest.mark.asyncio
async def test_a_new_send_takes_every_unsent_word(mock_cursor, monkeypatch):
    mock_cursor.fetchall.return_value = [_CLAIMABLE]
    monkeypatch.setattr(
        delivery,
        "messages_by_comment",
        AsyncMock(
            return_value=_said(
                ("m1", "user", "older"), ("r1", "agent", None), ("m2", "user", None)
            )
        ),
    )

    (claimed,) = await artifacts.claim_comments(
        user_id=_U, thread_id="t", comment_ids=["c1"], request_key="k"
    )

    assert claimed.message_ids == ["m2"]
    assert claimed.earlier == [("user", "m1"), ("agent", "r1")]


@pytest.mark.asyncio
async def test_what_no_agent_read_goes_back_though_its_key_is_spent(mock_cursor):
    """A run or a steer holds the key, which is why this path asks neither."""
    mock_cursor.fetchall.return_value = [("c1", _A)]

    released = await artifacts.release_unread_send(user_id=_U, request_key="k")

    assert released == [(_A, "c1")]
    sql = _sql(mock_cursor)
    assert "FOR UPDATE" in sql[0]
    assert any("sent_at = NULL" in s for s in sql)
