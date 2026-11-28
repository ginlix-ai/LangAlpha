"""A Drive file's comment record: found and made only on the caller's own
Home, for a live file of their personal Drive, with the Drive on for them;
every route on it answers 404 otherwise, and a member of a shared workspace
never reaches one. A box on it names no picture, so deleting a comment on
it has nothing to take off a computer."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport, AsyncClient

from src.server.app.artifacts import records, workspace_router
from src.server.app.artifacts import comments as comment_routes
from src.server.app.artifacts.models import CreateCommentRequest
from src.server.database.artifacts import Artifact
from src.server.database.home_workspace import get_home_workspace_id
from src.server.services import drive
from src.server.services.drive import DriveError
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

_OWNER = "owner"
_HOME = get_home_workspace_id(_OWNER)
_ENTRY = "00000000-0000-0000-0000-0000000000d0"
_RECORD = "00000000-0000-0000-0000-00000000000a"
_AT = datetime(2026, 11, 27, tzinfo=timezone.utc)


def _drive_record(**over) -> Artifact:
    fields = dict(
        artifact_id=_RECORD,
        code="c0de",
        workspace_id=_HOME,
        kind="drive",
        path=_ENTRY,
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=0,
        shared_at=None,
        files=None,
        revision=0,
        created_at=_AT,
    )
    fields.update(over)
    return Artifact(**fields)


def _entry(kind: str = "file"):
    return SimpleNamespace(entry_id=_ENTRY, kind=kind, path="notes")


@pytest_asyncio.fixture
async def owner():
    app = create_test_app(workspace_router)
    app.dependency_overrides[get_current_user_id] = lambda: _OWNER
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


@pytest.fixture
def the_drive(monkeypatch):
    """The owner's Drive on, holding the file, and the record's reads."""
    space = SimpleNamespace(space_id="s1")
    state = SimpleNamespace(
        opened=AsyncMock(return_value=space),
        entry=AsyncMock(return_value=_entry()),
        ensure=AsyncMock(return_value=_drive_record()),
        find=AsyncMock(return_value=None),
        home=AsyncMock(return_value={"workspace_id": _HOME}),
    )
    monkeypatch.setattr(drive, "open", state.opened)
    monkeypatch.setattr("src.server.database.drive.reads.live_entry", state.entry)
    monkeypatch.setattr(records.artifacts_db, "ensure_record", state.ensure)
    monkeypatch.setattr(records.artifacts_db, "find_record", state.find)
    monkeypatch.setattr(
        records.artifacts_db,
        "comment_counts",
        AsyncMock(return_value=SimpleNamespace(open=0, unread=0)),
    )
    monkeypatch.setattr(
        "src.server.database.workspace.get_or_create_home_workspace", state.home
    )
    return state


@pytest.mark.asyncio
async def test_a_drive_file_s_record_is_made_on_the_owner_s_home(owner, the_drive):
    made = await owner.post(
        f"/api/v1/workspaces/{_HOME}/artifacts", json={"drive": _ENTRY.upper()}
    )
    found = await owner.get(
        f"/api/v1/workspaces/{_HOME}/artifacts", params={"drive": _ENTRY}
    )

    assert made.status_code == 200
    assert (made.json()["kind"], made.json()["path"]) == ("drive", _ENTRY)
    the_drive.ensure.assert_awaited_once_with(_HOME, "drive", _ENTRY)
    the_drive.opened.assert_awaited_with(_OWNER, "personal")
    # Home's row is what the record hangs off, made with the record alone.
    the_drive.home.assert_awaited_once_with(_OWNER)
    assert found.json() == {"artifact": None}
    the_drive.find.assert_awaited_once_with(_HOME, "drive", _ENTRY)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["another workspace", "drive off", "no such file", "a folder", "not an id"]
)
async def test_anything_but_a_live_file_on_the_caller_s_home_is_not_found(
    owner, the_drive, case
):
    workspace, entry = _HOME, _ENTRY
    if case == "another workspace":
        workspace = "00000000-0000-0000-0000-00000000000f"
    elif case == "drive off":
        the_drive.opened.side_effect = DriveError("not_found", "No such space")
    elif case == "no such file":
        the_drive.entry.return_value = None
    elif case == "a folder":
        the_drive.entry.return_value = _entry("folder")
    else:
        entry = "notes/a.md"

    made = await owner.post(
        f"/api/v1/workspaces/{workspace}/artifacts", json={"drive": entry}
    )
    found = await owner.get(
        f"/api/v1/workspaces/{workspace}/artifacts", params={"drive": entry}
    )

    assert (made.status_code, found.status_code) == (404, 404)
    the_drive.ensure.assert_not_awaited()
    the_drive.home.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_record_names_one_of_a_path_a_chart_or_a_drive_file(owner):
    both = {"path": "a.html", "drive": _ENTRY}
    made = await owner.post(f"/api/v1/workspaces/{_HOME}/artifacts", json=both)
    assert made.status_code == 422


@pytest.mark.asyncio
async def test_a_drive_record_is_not_found_while_the_drive_is_off(monkeypatch):
    monkeypatch.setattr(
        records.artifacts_db, "get_artifact", AsyncMock(return_value=_drive_record())
    )
    monkeypatch.setattr(drive, "enabled", AsyncMock(return_value=False))
    with pytest.raises(HTTPException) as refused:
        await records.reached(_RECORD, _OWNER)
    assert refused.value.status_code == 404

    monkeypatch.setattr(drive, "enabled", AsyncMock(return_value=True))
    assert (await records.reached(_RECORD, _OWNER))[0].kind == "drive"


@pytest.mark.asyncio
async def test_no_one_but_its_owner_reaches_a_drive_record(monkeypatch):
    """Home is never shared, so another caller's look at it is refused by
    the workspace; and should one ever reach it, the record still is not."""
    from src.server.services.shared_workspaces.access import Access

    record = _drive_record()

    async def as_owner(artifact_id, *, user_id):
        return record if user_id == _OWNER else None

    monkeypatch.setattr(records.artifacts_db, "get_artifact", as_owner)
    monkeypatch.setattr(
        records.artifacts_db, "artifact_workspace_id", AsyncMock(return_value=_HOME)
    )
    monkeypatch.setattr(
        "src.server.services.membership.teams_door_open", lambda: True
    )
    monkeypatch.setattr(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"workspace_id": _HOME, "user_id": _OWNER}),
    )

    with pytest.raises(HTTPException) as refused:
        await records.reached(_RECORD, "member")
    assert refused.value.status_code == 404

    # A Home somehow reached as a shared workspace's member still yields nothing.
    monkeypatch.setattr(
        records, "workspace_access", AsyncMock(return_value=Access(_OWNER, object()))
    )
    with pytest.raises(HTTPException) as refused:
        await records.reached(_RECORD, "member")
    assert refused.value.status_code == 404


@pytest.mark.asyncio
async def test_a_drive_record_is_no_record_to_a_route_only_a_file_takes(monkeypatch):
    monkeypatch.setattr(
        records.artifacts_db, "get_artifact", AsyncMock(return_value=_drive_record())
    )
    monkeypatch.setattr(drive, "enabled", AsyncMock(return_value=True))
    with pytest.raises(HTTPException) as refused:
        await records.reached_file(_RECORD, _OWNER)
    assert refused.value.status_code == 404


def _create(anchor: dict) -> CreateCommentRequest:
    return CreateCommentRequest(anchor=anchor, body="here")


_PICTURED = {
    "kind": "region",
    "boxes": [{"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2, "picture": ".agents/comments/a.png"}],
}


def test_a_box_on_a_drive_file_names_no_picture():
    with pytest.raises(RequestValidationError) as refused:
        _create(_PICTURED).check_against("drive")
    (error,) = refused.value.errors()
    assert error["loc"] == ("body", "anchor", "boxes", 0, "picture")

    _create(_PICTURED).check_against("file")
    unpictured = {"kind": "region", "boxes": [{"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}]}
    _create(unpictured).check_against("drive")


def test_a_drive_file_takes_no_chart_anchor():
    chart = {"kind": "chart", "marks": [{"type": "point", "time": "2026-10-03", "price": 1}]}
    with pytest.raises(RequestValidationError):
        _create(chart).check_against("drive")


@pytest.mark.asyncio
@pytest.mark.parametrize("on", [True, False])
async def test_a_thread_s_queue_holds_drive_comments_only_with_the_drive_on(
    monkeypatch, on
):
    """What the queue shows is what a send would carry: with the Drive off,
    the agent could open none of its files. A queued Drive comment is shown
    where its file is now, with its entry."""
    from src.server.database.artifacts.comments import Comment
    from src.server.services.shared_workspaces.access import Access

    monkeypatch.setattr(
        comment_routes, "thread_access", AsyncMock(return_value=Access(_OWNER))
    )
    monkeypatch.setattr(comment_routes.drive, "sends_comments", AsyncMock(return_value=on))
    queued = Comment(
        comment_id="c1", artifact_id=_RECORD, kind="drive", number=1,
        path="notes/then.md", file_sha256=None, title=None,
        anchor={"kind": "block", "lines": "1"}, status="open", delivery="queued",
        thread_id="t", thread_title=None, thread_archived=False,
        created_at=_AT, updated_at=_AT, resolved_at=None, read_through=0,
        unread=0, workspace_id=_HOME, place="notes/now.md", entry_id=_ENTRY,
    )
    listed = AsyncMock(return_value=[queued])
    monkeypatch.setattr(comment_routes.artifacts_db, "list_queued_comments", listed)

    found = await comment_routes.list_thread_queued_comments(
        "00000000-0000-0000-0000-0000000000c2", _OWNER
    )

    assert listed.await_args.kwargs["drive_on"] is on
    (shown,) = found.model_dump(mode="json")["comments"]
    assert (shown["path"], shown["entry_id"], shown["workspace_id"]) == (
        "notes/now.md", _ENTRY, _HOME,
    )


@pytest.mark.asyncio
async def test_a_failed_read_of_the_drive_keeps_its_comments_waiting(monkeypatch):
    """The queue and a send read one answer, and a failed read is off for
    both: running what the owner turned off costs more than a wait."""
    monkeypatch.setattr(
        "src.server.services.features.user_feature_enabled",
        AsyncMock(side_effect=RuntimeError("flags down")),
    )
    assert await drive.sends_comments(_OWNER) is False


def _send_request(content: str):
    from src.server.models.chat import ChatMessage, ChatRequest

    return ChatRequest(
        messages=[ChatMessage(role="user", content=content)],
        artifact_comment_ids=["00000000-0000-0000-0000-0000000000c1"],
        request_key="00000000-0000-0000-0000-0000000000e1",
        reading="chat",
    )


def _scope():
    from src.server.services.artifacts import comments as sending

    return sending.CommentSendScope(
        thread_id="t", user_id=_OWNER, owner_id=_OWNER,
        chief_of_staff=True, timezone=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("on", [True, False])
async def test_a_send_reads_the_drive_once_and_claims_by_it(monkeypatch, on):
    from src.server.database.artifacts import Claim
    from src.server.services.artifacts import comments as sending

    read = AsyncMock(return_value=on)
    monkeypatch.setattr(drive, "sends_comments", read)
    claim = AsyncMock(return_value=Claim([]))
    monkeypatch.setattr(sending.artifacts_db, "claim_comments", claim)

    await _scope().take(
        _send_request("see these"), "see these",
        make_thread=AsyncMock(return_value=False),
    )

    read.assert_awaited_once_with(_OWNER)
    assert claim.await_args.kwargs["drive_on"] is on


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("held", "code"),
    [
        ({"in_trash": True}, "file_in_trash"),
        ({"drive_off": True}, "drive_off"),
        ({"drive_off": True, "in_trash": True}, "drive_off"),
        ({}, "comments_unavailable"),
    ],
)
async def test_a_send_of_only_held_drive_comments_says_why(monkeypatch, held, code):
    """A send that took nothing names the trash or the Drive being off when
    that is why, rather than telling the owner the comments went or were
    deleted. With the Drive off a restore alone would not send them, so that
    reason comes first."""
    from src.server.database.artifacts import Claim
    from src.server.services.artifacts import comments as sending

    monkeypatch.setattr(drive, "sends_comments", AsyncMock(return_value=True))
    monkeypatch.setattr(
        sending.artifacts_db,
        "claim_comments",
        AsyncMock(return_value=Claim([], **held)),
    )

    with pytest.raises(HTTPException) as refused:
        await _scope().take(
            _send_request(""), "", make_thread=AsyncMock(return_value=False)
        )

    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == code


@pytest.mark.asyncio
async def test_a_file_purged_while_its_record_is_made_is_not_found(owner, the_drive):
    """The live-file check passed, then a purge took the entry before the
    insert: the record's foreign key refuses it, answered as a file gone."""
    from src.server.database.artifacts import ArtifactNotFound

    the_drive.ensure.side_effect = ArtifactNotFound(_ENTRY)

    made = await owner.post(
        f"/api/v1/workspaces/{_HOME}/artifacts", json={"drive": _ENTRY}
    )

    assert made.status_code == 404
