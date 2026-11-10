"""Version rules under the record's lock: what a read was based on must
still hold when its version lands, visitors move only to a version of the list
someone reviewed, a cut prunes kept-updated versions past the newest ones but
never the latest or the shared one, and a refused send gives its comments back
only when, after their locks, no copy of it reached a run."""

from __future__ import annotations

import dataclasses
import hashlib
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import delivery, versions
from src.server.database.artifacts.records import FOLLOWS_TURNS

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


def _record(latest_seq: int, **changes) -> artifacts.Artifact:
    """Shared, kept updated, its visitors on the latest version."""
    record = artifacts.Artifact(
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
        shared_seq=latest_seq,
    )
    return dataclasses.replace(record, **changes)


def _kept(**changes) -> artifacts.Artifact:
    """Kept and shared, kept updated, its Dashboard two versions past the share."""
    fields = {
        "latest_seq": 4,
        "kept_at": _NOW,
        "shared_seq": 2,
        "files": ("r.html", "d.json"),
    }
    return _record(**{**fields, **changes})


def _file(text: str, path: str = "r.html") -> artifacts.VersionFile:
    data = text.encode()
    return artifacts.VersionFile(
        path, hashlib.sha256(data).hexdigest(), len(data), "text/html", data
    )


# --- do visitors move on a turn's cut -------------------------------------------


@pytest.mark.parametrize(
    "record, paths, left_out, moves",
    [
        # A kept walk that found exactly the shared list.
        (_kept(), ("r.html", "d.json"), (), True),
        # A file the page started reading, which nobody reviewed.
        (_kept(), ("r.html", "d.json", "new.json"), (), False),
        # A reviewed file the walk no longer found.
        (_kept(), ("r.html",), (), False),
        # Shared without Keep updated: visitors wait for the owner's Update.
        (_kept(keep_updated=False), ("r.html", "d.json"), (), False),
        # Kept, never shared.
        (_kept(shared_at=None, shared_seq=0, files=None), ("r.html", "d.json"), (), False),
        # A keep stopped during a walk: the record now looks like a shared list
        # kept updated, but the walk found a file nobody reviewed.
        (_kept(kept_at=None), ("r.html", "new.json"), (), False),
        # A shared list, not kept, short by a file the owner left out since.
        (_kept(kept_at=None, left_out_files=("d.json",)), ("r.html",), (), True),
        # The same record kept: the walk drops the left-out file, and visitors
        # still move. They used to stop for good here.
        (_kept(left_out_files=("d.json",)), ("r.html",), ("d.json",), True),
        (_kept(left_out_files=("d.json",)), ("r.html",), (), True),
        # Another path to a left-out file, which the read left out with it.
        (_kept(left_out_files=("x.json",)), ("r.html",), ("d.json",), True),
    ],
)
def test_visitors_follow_a_turn_only_to_a_version_of_their_list(
    record, paths, left_out, moves
):
    assert versions.visitors_follow_turn(record, paths, left_out) is moves


# --- what a read was based on -----------------------------------------------------


@pytest.mark.parametrize(
    "locked, raised",
    [
        (_record(4, revision=2), artifacts.SharingChanged),
        (_record(5), artifacts.VersionMoved),
        (_record(4, left_out_files=("b.html",)), artifacts.LeftOutChanged),
    ],
)
def test_a_read_that_raced_the_record_never_lands(locked, raised):
    with pytest.raises(raised):
        artifacts.ReadBasis.of(_record(4)).check(locked)


def test_a_read_lands_on_the_left_out_list_it_read_in_any_order():
    read = artifacts.ReadBasis.of(_record(4, left_out_files=("b", "c")))

    read.check(_record(4, left_out_files=("c", "b")))


# --- which records a turn's end refreshes -----------------------------------------


@pytest.mark.parametrize(
    "record, follows",
    [
        (_kept(keep_updated=False), True),
        (_kept(shared_at=None, shared_seq=0, files=None, keep_updated=False), True),
        (_record(4), True),
        (_record(4, keep_updated=False), False),
        (_record(4, shared_at=None, shared_seq=0), False),
        # Shared before versions and not kept: its first view cuts v1.
        (_record(0), False),
        (_record(3, shared_seq=0), False),
        # Kept is never without a version; the rule still says so.
        (_kept(latest_seq=0, shared_seq=0), False),
    ],
)
def test_turns_refresh_kept_records_and_shared_ones_kept_updated(record, follows):
    assert record.follows_turns is follows


@pytest.mark.asyncio
async def test_the_listing_of_records_a_turn_refreshes_is_the_same_rule(mock_cursor):
    await artifacts.list_kept_updated("w")

    (sql,) = _sql(mock_cursor)
    assert _flat(FOLLOWS_TURNS) in sql
    assert "l.latest_seq > 0" in _flat(FOLLOWS_TURNS)
    assert "l.shared_seq > 0" in _flat(FOLLOWS_TURNS)


# --- the cut under the lock -------------------------------------------------------


@pytest.fixture
def locked(monkeypatch, mock_cursor):
    """The record under its lock; the UPDATE answers with it unchanged unless
    a test says otherwise."""
    lock = AsyncMock(return_value=_record(latest_seq=4))
    monkeypatch.setattr(versions, "lock_owned", lock)
    mock_cursor.fetchone.side_effect = lambda: dataclasses.asdict(lock.return_value)
    return lock


def _update(mock_cursor) -> tuple[str, dict]:
    (update,) = [
        c
        for c in mock_cursor.execute.call_args_list
        if _flat(c.args[0]).lstrip().startswith("UPDATE share_links")
    ]
    # What it sets, not what it reads back.
    return _flat(update.args[0]).split("RETURNING")[0], update.args[1]


def _wrote(mock_cursor) -> bool:
    return any("UPDATE share_links" in s or "INSERT" in s for s in _sql(mock_cursor))


def _same_as_latest(monkeypatch, *files: artifacts.VersionFile) -> None:
    monkeypatch.setattr(
        versions, "_hashes", AsyncMock(return_value={f.path: f.sha256 for f in files})
    )


async def _publish(read: artifacts.ReadBasis, files, **over):
    return await artifacts.publish_version(
        **{
            "user_id": _U,
            "artifact_id": _A,
            "read": read,
            "files": files,
            "title": None,
            "label": None,
            "keep_updated": None,
            "loaded_files": [],
            "left_out_files": [],
            **over,
        }
    )


async def _turn(read: artifacts.ReadBasis, files, left_out=()):
    return await artifacts.turn_version(
        user_id=_U, artifact_id=_A, read=read, files=files, title=None, left_out=left_out
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read, raised",
    [
        # A stop landed during the read: the publish must not undo it.
        (_record(4, revision=0), artifacts.SharingChanged),
        # Version 4 landed since: these bytes may be older.
        (_record(3), artifacts.VersionMoved),
        # A file taken off since, which the read would carry.
        (_record(4, left_out_files=("b.html",)), artifacts.LeftOutChanged),
    ],
)
async def test_an_owners_publish_raced_by_the_record_writes_nothing(
    locked, mock_cursor, read, raised
):
    with pytest.raises(raised):
        await _publish(artifacts.ReadBasis.of(read), [_file("x")])

    assert not _wrote(mock_cursor)


@pytest.mark.asyncio
async def test_an_owners_publish_equal_to_a_private_latest_still_shares_it(
    locked, mock_cursor, monkeypatch
):
    """The latest may be a version cut for the Dashboard alone: nothing is cut,
    but visitors move to it with its list, and a legacy refusal clears."""
    locked.return_value = _kept()
    now = _file("x")
    _same_as_latest(monkeypatch, now)

    result = await _publish(
        artifacts.ReadBasis.of(_kept()),
        [now],
        loaded_files=["d.csv"],
        left_out_files=["b.html"],
    )

    sql, params = _update(mock_cursor)
    assert (result.cut, result.seq) == (False, 4)
    assert (params["shared_seq"], params["files"]) == (4, ["r.html"])
    assert "legacy_refused = NULL" in sql
    assert (params["loaded_files"], params["left_out_files"]) == (["d.csv"], ["b.html"])


@pytest.mark.asyncio
@pytest.mark.parametrize("moved", [False, True])
async def test_other_views_hear_of_a_publish_only_when_what_they_show_moved(
    locked, mock_cursor, monkeypatch, moved
):
    """Other views show the setting, so moving it alone is still a change."""
    now = _file("x")
    _same_as_latest(monkeypatch, now)
    after = _record(4, keep_updated=not moved)
    mock_cursor.fetchone.side_effect = lambda: dataclasses.asdict(after)

    result = await _publish(
        artifacts.ReadBasis.of(_record(4)), [now], keep_updated=after.keep_updated
    )

    assert (result.cut, result.shared_now, result.changed) == (False, False, moved)


@pytest.mark.asyncio
async def test_keeping_marks_the_record_in_the_write_that_cuts_and_moves_no_visitor(
    locked, mock_cursor
):
    """A keep whose read failed never gets here, so a record is never kept
    with no version to show."""
    locked.return_value = _kept(kept_at=None)

    result = await artifacts.keep_version(
        user_id=_U,
        artifact_id=_A,
        read=artifacts.ReadBasis.of(_kept(kept_at=None)),
        files=[_file("x"), _file("{}", "d.json")],
        title=None,
        loaded_files=["d.json"],
    )

    sql, params = _update(mock_cursor)
    assert (result.cut, params["seq"], params["shared_seq"]) == (True, 5, 2)
    assert "kept_at = NOW()" in sql
    assert "loaded_files = %(loaded_files)s::text[]" in sql
    assert params["loaded_files"] == ["d.json"]
    # The visitors' list stays as it is.
    assert not re.search(r"(?<![\w.])files =", sql) and "legacy_refused" not in sql


@pytest.mark.asyncio
async def test_a_turns_cut_of_a_kept_record_short_by_a_left_out_file_moves_visitors(
    locked, mock_cursor
):
    """Leaving a file out after the publish changes no ``files``, and the walk
    drops it; visitors still follow, as they do for the record not kept."""
    record = _kept(left_out_files=("d.json",))
    locked.return_value = record

    result = await _turn(artifacts.ReadBasis.of(record), [_file("x")], ("d.json",))

    sql, params = _update(mock_cursor)
    assert (result.cut, params["seq"], params["shared_seq"]) == (True, 5, 5)
    assert params["files"] == ["r.html"]
    # Only an owner's publish changes what the page is known to load or what
    # it leaves out.
    assert "loaded_files" not in sql and "left_out_files" not in sql


@pytest.mark.asyncio
async def test_a_turns_cut_with_a_path_nobody_reviewed_moves_only_the_owner(
    locked, mock_cursor
):
    locked.return_value = _kept()

    await _turn(artifacts.ReadBasis.of(_kept()), [_file("x"), _file("{}", "new.json")])

    sql, params = _update(mock_cursor)
    assert (params["seq"], params["shared_seq"]) == (5, 2)
    assert "files =" not in sql and "legacy_refused" not in sql


@pytest.mark.asyncio
async def test_a_turns_cut_raced_by_a_newer_version_writes_nothing(locked, mock_cursor):
    with pytest.raises(artifacts.VersionMoved):
        await _turn(artifacts.ReadBasis.of(_record(3)), [_file("stale")])

    assert not _wrote(mock_cursor)


@pytest.mark.asyncio
async def test_a_turns_cut_of_a_record_that_stopped_following_turns_does_nothing(
    locked, mock_cursor
):
    locked.return_value = _record(4, keep_updated=False)

    result = await _turn(artifacts.ReadBasis.of(_record(4)), [_file("x")])

    assert (result.cut, result.seq, result.changed) == (False, 4, False)
    assert not _wrote(mock_cursor)


@pytest.mark.asyncio
async def test_a_cut_prunes_kept_updated_versions_past_the_newest_but_never_the_shared(
    locked, mock_cursor
):
    locked.return_value = _kept()

    await _turn(artifacts.ReadBasis.of(_kept()), [_file("x"), _file("{}", "new.json")])

    calls = mock_cursor.execute.call_args_list
    (prune,) = [c for c in calls if "DELETE FROM artifact_versions" in c.args[0]]
    sql = _flat(prune.args[0])
    assert "v.origin = 'keep_updated'" in sql
    assert "v.seq NOT IN (%(latest)s, %(shared)s)" in sql
    assert "ORDER BY seq DESC LIMIT %(keep)s" in sql
    assert prune.args[1] == {
        "artifact_id": _A,
        "latest": 5,
        "shared": 2,
        "keep": artifacts.KEEP_UPDATED_VERSIONS,
    }
    assert artifacts.KEEP_UPDATED_VERSIONS == 10
    insert = next(
        i for i, c in enumerate(calls) if "INSERT INTO artifact_versions" in c.args[0]
    )
    assert insert < calls.index(prune)


async def _legacy(read: artifacts.Artifact):
    return await artifacts.legacy_version(
        user_id=_U,
        artifact_id=_A,
        read=artifacts.ReadBasis.of(read),
        files=[_file("x")],
        title=None,
    )


@pytest.mark.asyncio
async def test_a_legacy_link_kept_first_gets_its_first_shared_version_following_the_file(
    locked, mock_cursor
):
    """No version yet means none shared: a keep may have cut private ones. It
    was live until now, whichever build shared it."""
    record = _kept(shared_seq=0, latest_seq=3)
    locked.return_value = record

    result = await _legacy(record)

    sql, params = _update(mock_cursor)
    assert (result.cut, result.seq) == (True, 4)
    assert (params["seq"], params["shared_seq"]) == (4, 4)
    assert "keep_updated = TRUE" in sql
    assert "legacy_refused = NULL" in sql


@pytest.mark.asyncio
async def test_a_legacy_cut_after_the_first_shared_version_does_nothing(
    locked, mock_cursor
):
    """Of two workers snapshotting one link, only the first cuts it, and the
    second does not fail its visitor."""
    locked.return_value = _kept()

    result = await _legacy(_kept(shared_seq=0, latest_seq=3))

    assert (result.cut, result.seq) == (False, 4)
    assert not _wrote(mock_cursor)


# --- the record's other writes ----------------------------------------------------


@pytest.mark.asyncio
async def test_unsharing_drops_the_shared_version(mock_cursor):
    mock_cursor.fetchone.return_value = None

    with pytest.raises(artifacts.ArtifactNotFound):
        await artifacts.unshare(user_id=_U, artifact_id=_A)

    assert "shared_seq = 0" in _sql(mock_cursor)[0]


@pytest.mark.asyncio
async def test_stopping_keeping_only_stops_keeping(mock_cursor):
    mock_cursor.fetchone.return_value = None

    with pytest.raises(artifacts.ArtifactNotFound):
        await artifacts.stop_keeping(user_id=_U, artifact_id=_A)

    assert "SET kept_at = NULL FROM" in _sql(mock_cursor)[0]


@pytest.mark.asyncio
async def test_letting_go_of_records_off_the_list_waits_on_the_keep_and_the_list(
    mock_cursor,
):
    """Both stamps are read in the statement, so a pin's keep or an undo's
    listing that lands after the list was read still holds the record."""
    mock_cursor.fetchall.return_value = []

    await artifacts.release_unlisted(
        user_id=_U, workspace_id="w", listed={"b", "a"}, grace=timedelta(minutes=10)
    )

    sql = _sql(mock_cursor)[0]
    params = mock_cursor.execute.call_args.args[1]
    assert "SET kept_at = NULL" in sql
    assert "l.kept_at < NOW() - %(grace)s" in sql
    assert "NOT (l.artifact_id::text = ANY(%(listed)s::text[]))" in sql
    assert "p.updated_at < NOW() - %(grace)s" in sql
    assert params["listed"] == ["a", "b"]


@pytest.mark.asyncio
async def test_a_legacy_refusal_waits_on_the_shared_version_not_the_latest(
    mock_cursor,
):
    await versions.refuse_legacy(artifact_id=_A, code="too_many_files")

    (sql,) = _sql(mock_cursor)
    assert "l.shared_seq = 0" in sql
    assert "l.latest_seq = 0" not in sql


@pytest.mark.asyncio
async def test_a_visitor_is_served_only_the_shared_version(mock_cursor):
    """Never a newer version kept for the owner alone."""
    await artifacts.get_shared_version_file(
        code="c0de", artifact_id=_A, revision=1, seq=2, path="r.html"
    )

    (sql,) = _sql(mock_cursor)
    assert "f.seq = l.shared_seq" in sql
    assert "l.shared_seq = %(seq)s" in sql


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
