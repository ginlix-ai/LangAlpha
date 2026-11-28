"""Where a comment goes by default: a bound thread first, whatever the owner's
preference; then the thread beside or the creator, in the order the
preference names; else a new thread. A thread that cannot take comments
falls through, and a stored preference no writer should have left reads as
the default. A chart's comments go only to its own workspace's threads."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import (
    Destinations,
    DestinationThread,
    delivery,
    routing,
)

_A = "00000000-0000-0000-0000-00000000000a"
_U = "user-1"
_W = "00000000-0000-0000-0000-00000000000f"
_BOUND = "00000000-0000-0000-0000-0000000000b0"
_BESIDE = "00000000-0000-0000-0000-0000000000b1"
_CREATOR = "00000000-0000-0000-0000-0000000000c0"
_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)


def _row(**overrides) -> dict:
    row = {
        "artifact_id": _A,
        "code": "abc",
        "workspace_id": _W,
        "kind": "file",
        "path": "report/index.html",
        "created_thread_id": _CREATOR,
        "bound_thread_id": None,
        "keep_updated": False,
        "latest_seq": 0,
        "shared_at": None,
        "files": None,
        "revision": 0,
        "created_at": _NOW,
        "legacy_refused": None,
        "loaded_files": [],
        "left_out_files": [],
        "comment_destination": None,
    }
    return {**row, **overrides}


def _thread(thread_id: str, *, usable: bool = True) -> DestinationThread:
    return DestinationThread(
        thread_id=thread_id, title=None, workspace_id=_W, usable=usable, home=False
    )


def _destinations(
    preference,
    *,
    bound: DestinationThread | None = None,
    beside: DestinationThread | None = _thread(_BESIDE),
    creator: DestinationThread | None = _thread(_CREATOR),
) -> Destinations:
    return Destinations(
        artifact=artifacts.artifact_from_row(_row()),
        bound=bound,
        beside=beside,
        creator=creator,
        preference=preference,
    )


def _picked(found: Destinations) -> tuple[str | None, str]:
    thread, rule = found.default
    return (thread.thread_id if thread else None), rule


@pytest.mark.parametrize("preference", ["beside", "creator", "new"])
def test_a_bound_thread_wins_whatever_the_preference(preference):
    found = _destinations(preference, bound=_thread(_BOUND))
    assert _picked(found) == (_BOUND, "bound")


@pytest.mark.parametrize(
    ("preference", "expected"),
    [
        ("beside", (_BESIDE, "beside")),
        ("creator", (_CREATOR, "creator")),
        ("new", (None, "new")),
    ],
)
def test_an_unbound_file_follows_the_preference(preference, expected):
    assert _picked(_destinations(preference)) == expected


def test_beside_falls_back_to_the_creator_and_creator_to_beside():
    assert _picked(_destinations("beside", beside=None)) == (_CREATOR, "creator")
    assert _picked(_destinations("creator", creator=None)) == (_BESIDE, "beside")


@pytest.mark.parametrize("preference", ["beside", "creator"])
def test_unusable_threads_fall_through(preference):
    archived = _destinations(
        preference,
        bound=_thread(_BOUND, usable=False),
        beside=_thread(_BESIDE, usable=False),
        creator=_thread(_CREATOR, usable=False),
    )
    assert _picked(archived) == (None, "new")
    bound_archived = _destinations(preference, bound=_thread(_BOUND, usable=False))
    assert _picked(bound_archived)[1] == preference


def test_new_with_an_archived_binding_starts_a_thread():
    found = _destinations("new", bound=_thread(_BOUND, usable=False))
    assert _picked(found) == (None, "new")


@pytest.mark.parametrize("stored", [None, "", "BESIDE", "bound", "elsewhere", 3])
def test_a_preference_no_writer_should_have_left_reads_as_beside(stored):
    assert routing._comment_destination(stored) == "beside"


@pytest.fixture
def _db(artifacts_db, monkeypatch):
    monkeypatch.setattr(routing, "get_home_workspace_id", lambda user_id: _W)


@pytest.mark.asyncio
async def test_destinations_read_the_owners_preference_alone(_db, mock_cursor):
    mock_cursor.fetchone.return_value = _row(comment_destination="creator")
    mock_cursor.fetchall.return_value = [
        {
            "thread_id": tid,
            "title": None,
            "workspace_id": _W,
            "usable": True,
            "home": False,
        }
        for tid in (_BESIDE, _CREATOR)
    ]

    found = await artifacts.destinations(
        user_id=_U, artifact_id=_A, beside_thread_id=_BESIDE, caller=_U
    )

    sql, params = mock_cursor.execute.await_args_list[0].args
    assert "other_preference ->> 'artifact_comment_destination'" in sql
    assert params["user_id"] == _U
    assert found.preference == "creator"
    assert _picked(found) == (_CREATOR, "creator")


_HOME = "00000000-0000-0000-0000-0000000000e0"


@pytest.fixture
def _home(artifacts_db, monkeypatch):
    for module in (routing, delivery):
        monkeypatch.setattr(module, "get_home_workspace_id", lambda user_id: _HOME)


def test_a_chart_comment_goes_only_to_its_own_workspace():
    """A file's comments may go to the owner's Home, whose Chief of Staff
    reaches every workspace's files; a chart's only to its own workspace,
    the one place its id names it; a Drive file's to any of the owner's own
    workspaces, the Drive being at one path in each, but no shared one and
    not Home's stand-in."""
    rule = " ".join(routing.destination_sql("l", "t", "tw").split())
    assert rule == (
        "(t.archived_at IS NULL AND (t.workspace_id = l.workspace_id"
        " OR (l.kind IN ('drive', 'file') AND t.workspace_id = %(home_id)s)"
        " OR (l.kind = 'drive' AND NOT COALESCE(tw.config, '{}'::jsonb) ? 'home_stand_in'"
        " AND NOT EXISTS ( SELECT 1 FROM shared_workspaces s"
        " WHERE s.workspace_id = tw.workspace_id))))"
    )


def _says_the_rule(sql: str) -> bool:
    return routing.destination_sql("l", "t", "tw") in sql


@pytest.mark.asyncio
async def test_a_queue_or_a_binding_checks_the_rule_on_the_record(_home, mock_cursor):
    mock_cursor.fetchone.return_value = {"?column?": 1}
    await routing.require_destination(
        mock_cursor, user_id=_U, artifact_id=_A, thread_id=_BESIDE, caller=_U
    )
    sql, params = mock_cursor.execute.await_args.args
    assert _says_the_rule(sql)
    assert "JOIN share_links l ON l.artifact_id = %(artifact_id)s" in sql
    assert (params["artifact_id"], params["home_id"]) == (_A, _HOME)

    mock_cursor.fetchone.return_value = None
    with pytest.raises(routing.DestinationUnavailable):
        await routing.require_destination(
            mock_cursor, user_id=_U, artifact_id=_A, thread_id=_BESIDE, caller=_U
        )


@pytest.mark.parametrize("kind", ["file", "chart"])
@pytest.mark.asyncio
async def test_the_menu_marks_usable_by_the_same_rule(_home, mock_cursor, kind):
    mock_cursor.fetchone.return_value = _row(kind=kind, path="NVDA:1day")
    mock_cursor.fetchall.return_value = []

    found = await artifacts.destinations(
        user_id=_U, artifact_id=_A, beside_thread_id=_BESIDE, caller=_U
    )

    record_sql = mock_cursor.execute.await_args_list[0].args[0]
    assert "l.kind IN ('file', 'chart', 'drive')" in record_sql
    sql, params = mock_cursor.execute.await_args_list[1].args
    assert f"{routing.destination_sql('l', 't', 'tw')} AS usable" in sql
    assert params["artifact_id"] == _A
    # Home is still named as Home, so the menu can say why it is not usable.
    assert params["home_id"] == _HOME
    assert found.artifact.kind == kind


@pytest.mark.asyncio
async def test_a_send_carries_comments_by_the_same_rule(_home, mock_cursor):
    mock_cursor.fetchall.return_value = []
    await artifacts.claim_comments(
        user_id=_U, thread_id=_BESIDE, comment_ids=["c1"], request_key="k", caller=_U,
        drive_on=True,
    )
    assert _says_the_rule(mock_cursor.execute.await_args.args[0])


_ENTRY = "00000000-0000-0000-0000-0000000000d0"
_WORKED = "00000000-0000-0000-0000-0000000000d1"
_SAVED = "00000000-0000-0000-0000-0000000000d2"


def _drive(
    preference, *, bound=None, worked=_thread(_WORKED), beside=None
) -> Destinations:
    return Destinations(
        artifact=artifacts.artifact_from_row(_row(kind="drive", path=_ENTRY)),
        bound=bound,
        beside=beside,
        creator=None,
        preference=preference,
        worked=worked,
    )


@pytest.mark.parametrize("preference", ["beside", "creator"])
def test_a_drive_file_on_its_page_goes_to_the_thread_that_last_worked_on_it(preference):
    """The Drive page has no thread beside it, and a Drive file no creator."""
    assert _picked(_drive(preference)) == (_WORKED, "worked")
    assert _picked(_drive(preference, worked=None)) == (None, "new")
    assert _picked(_drive(preference, worked=_thread(_WORKED, usable=False))) == (
        None,
        "new",
    )


def test_a_drive_file_beside_a_chat_follows_the_preference_as_a_file_does():
    """In a chat's Drive tab the thread that last worked on the file stands
    where a file's creator does."""
    beside = _thread(_BESIDE)
    assert _picked(_drive("beside", beside=beside)) == (_BESIDE, "beside")
    assert _picked(_drive("creator", beside=beside)) == (_WORKED, "worked")
    assert _picked(_drive("beside", beside=beside, worked=None)) == (_BESIDE, "beside")
    assert _picked(_drive("creator", beside=beside, worked=None)) == (_BESIDE, "beside")
    assert _picked(_drive("new", beside=beside)) == (None, "new")
    # A chat in a shared workspace takes no Drive comment: the next rule does.
    shared = _thread(_BESIDE, usable=False)
    assert _picked(_drive("beside", beside=shared)) == (_WORKED, "worked")
    assert _picked(_drive("beside", beside=shared, worked=None)) == (None, "new")
    assert _picked(_drive("beside", beside=beside, bound=_thread(_BOUND))) == (_BOUND, "bound")


def test_a_drive_file_follows_its_binding_and_the_new_preference():
    assert _picked(_drive("new", bound=_thread(_BOUND))) == (_BOUND, "bound")
    assert _picked(_drive("beside", bound=_thread(_BOUND))) == (_BOUND, "bound")
    assert _picked(_drive("new")) == (None, "new")


def _usable(thread_id: str, usable: bool = True) -> dict:
    return {
        "thread_id": thread_id,
        "title": None,
        "workspace_id": _W,
        "usable": usable,
        "home": False,
    }


@pytest.mark.parametrize(
    ("candidates", "usable", "expected"),
    [
        # Most recent first, as the read orders them; the newest usable wins.
        ([_WORKED, _SAVED], {_WORKED, _SAVED}, _WORKED),
        ([_SAVED, _WORKED], {_WORKED, _SAVED}, _SAVED),
        ([_WORKED, _SAVED], {_SAVED}, _SAVED),
        ([_WORKED, _SAVED], set(), None),
    ],
)
@pytest.mark.asyncio
async def test_worked_is_the_most_recent_usable_candidate(
    _home, mock_cursor, candidates, usable, expected
):
    mock_cursor.fetchone.return_value = _row(kind="drive", path=_ENTRY, created_thread_id=None)
    mock_cursor.fetchall.side_effect = [
        [{"thread_id": t} for t in candidates],
        [_usable(t, t in usable) for t in candidates],
    ]

    found = await artifacts.destinations(
        user_id=_U, artifact_id=_A, beside_thread_id=None, caller=_U
    )

    worked_sql, worked_params = mock_cursor.execute.await_args_list[1].args
    assert "FROM artifact_comments c" in worked_sql
    assert "c.delivery IN ('queued', 'sent')" in worked_sql
    assert "FROM drive_versions v" in worked_sql
    assert "v.actor_kind = 'agent'" in worked_sql
    assert "ORDER BY x.at DESC" in worked_sql
    assert worked_params == {"artifact_id": _A, "entry_id": _ENTRY}
    assert (found.worked.thread_id if found.worked else None) == expected
    assert _picked(found) == (expected, "worked" if expected else "new")


@pytest.mark.asyncio
async def test_a_version_thread_that_is_no_id_is_no_candidate(_home, mock_cursor):
    mock_cursor.fetchone.return_value = _row(kind="drive", path=_ENTRY, created_thread_id=None)
    mock_cursor.fetchall.side_effect = [[{"thread_id": "not-a-thread"}]]

    found = await artifacts.destinations(
        user_id=_U, artifact_id=_A, beside_thread_id=None, caller=_U
    )

    # No ids, so no thread lookup at all.
    assert len(mock_cursor.execute.await_args_list) == 2
    assert found.worked is None


@pytest.mark.asyncio
async def test_a_file_reads_no_worked_thread(_home, mock_cursor):
    mock_cursor.fetchone.return_value = _row()
    mock_cursor.fetchall.return_value = []

    found = await artifacts.destinations(
        user_id=_U, artifact_id=_A, beside_thread_id=None, caller=_U
    )

    assert all(
        "drive_versions" not in call.args[0]
        for call in mock_cursor.execute.await_args_list
    )
    assert found.worked is None


@pytest.mark.asyncio
async def test_the_picker_lists_by_the_same_rule(_home, mock_cursor):
    mock_cursor.fetchall.return_value = []

    await artifacts.destination_threads(
        user_id=_U, artifact_id=_A, caller=_U, query="Q3_%", limit=20
    )

    sql, params = mock_cursor.execute.await_args.args
    assert _says_the_rule(sql)
    assert "ORDER BY t.updated_at DESC" in sql
    assert "t.title ILIKE" in sql
    # The text is matched as typed, its LIKE wildcards escaped.
    assert params["pattern"] == "%Q3\\_\\%%"
    assert (params["limit"], params["home_id"]) == (20, _HOME)

    await artifacts.destination_threads(
        user_id=_U, artifact_id=_A, caller=_U, query=None, limit=5
    )
    assert mock_cursor.execute.await_args.args[1]["pattern"] is None


@pytest.mark.asyncio
async def test_the_picker_answers_for_one_thread_a_draft_kept(_home, mock_cursor):
    """A draft kept with a chat picked through "Another chat" asks whether
    that chat still takes the comments: by the same rule, that thread only."""
    mock_cursor.fetchall.return_value = []
    thread = "00000000-0000-0000-0000-0000000000a7"

    await artifacts.destination_threads(
        user_id=_U, artifact_id=_A, caller=_U, query=None, limit=1, thread_id=thread
    )

    sql, params = mock_cursor.execute.await_args.args
    assert _says_the_rule(sql)
    assert "t.conversation_thread_id = %(thread_id)s::uuid" in sql
    assert params["thread_id"] == thread

    await artifacts.destination_threads(
        user_id=_U, artifact_id=_A, caller=_U, query=None, limit=20
    )
    assert mock_cursor.execute.await_args.args[1]["thread_id"] is None
