"""Where a comment goes by default: a bound thread first, whatever the owner's
preference; then the thread beside or the creator, in the order the
preference names; else a new thread. A thread that cannot take comments
falls through, and a stored preference no writer should have left reads as
the default."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.server.database import artifacts
from src.server.database.artifacts import Destinations, DestinationThread, routing

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
        user_id=_U, artifact_id=_A, beside_thread_id=_BESIDE
    )

    sql, params = mock_cursor.execute.await_args_list[0].args
    assert "other_preference ->> 'artifact_comment_destination'" in sql
    assert params["user_id"] == _U
    assert found.preference == "creator"
    assert _picked(found) == (_CREATOR, "creator")
