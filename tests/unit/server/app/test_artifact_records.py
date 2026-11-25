"""The workspace record routes answer 404 for a workspace that is not the
caller's, as every artifact route does, never a 403 that confirms it exists."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.app.artifacts import workspace_router
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

_DBWS = "src.server.services.workspace_files.db_get_workspace"
_WS = "00000000-0000-0000-0000-00000000000f"


@pytest_asyncio.fixture
async def stranger():
    app = create_test_app(workspace_router)
    app.dependency_overrides[get_current_user_id] = lambda: "someone-else"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [{"workspace_id": _WS, "user_id": "owner"}, None])
async def test_a_workspace_not_the_callers_is_not_found(stranger, workspace):
    with patch(_DBWS, AsyncMock(return_value=workspace)):
        found = await stranger.get(
            f"/api/v1/workspaces/{_WS}/artifacts", params={"path": "a.html"}
        )
        made = await stranger.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"path": "a.html"}
        )
    assert (found.status_code, made.status_code) == (404, 404)


@pytest.mark.asyncio
async def test_a_deleted_workspace_is_not_found(stranger):
    deleted = {"workspace_id": _WS, "user_id": "someone-else", "status": "deleted"}
    with patch(_DBWS, AsyncMock(return_value=deleted)):
        resp = await stranger.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"path": "a.html"}
        )
    assert resp.status_code == 404


def _record(**over):
    from datetime import datetime, timezone

    from src.server.database.artifacts import Artifact

    at = datetime(2026, 11, 10, tzinfo=timezone.utc)
    fields = dict(
        artifact_id="00000000-0000-0000-0000-00000000000a",
        code="c0de",
        workspace_id=_WS,
        kind="file",
        path="dash.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=True,
        latest_seq=5,
        shared_at=at,
        files=("dash.html",),
        revision=1,
        created_at=at,
        kept_at=at,
        shared_seq=3,
    )
    fields.update(over)
    return Artifact(**fields)


@pytest.mark.asyncio
async def test_the_owner_sees_the_dashboards_version_and_the_one_visitors_see():
    from types import SimpleNamespace

    from src.server.app.artifacts import records

    counts = AsyncMock(return_value=SimpleNamespace(open=0, unread=0))
    with patch.object(records.artifacts_db, "comment_counts", counts):
        answer = await records._response(_record(), "owner", "owner")
    assert (answer.latest_seq, answer.shared_seq) == (5, 3)


@pytest.mark.asyncio
async def test_the_owners_tabs_hear_both_pointers_move():
    from src.server.services.artifacts import events

    with patch.object(events, "publish_user_event", AsyncMock()) as sent:
        await events.publish_artifact_change("owner", _record())
    event = sent.await_args.args[1]
    assert (event["latest_seq"], event["shared_seq"]) == (5, 3)


@pytest_asyncio.fixture
async def owner():
    app = create_test_app(workspace_router)
    app.dependency_overrides[get_current_user_id] = lambda: "owner"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


_GETWS = "src.server.database.workspace.get_workspace"


@pytest.mark.asyncio
async def test_a_chart_has_one_record_whatever_the_spelling_and_needs_no_computer(
    owner,
):
    from src.server.app.artifacts import records

    # Home before it is on a computer: a chart reads no files.
    home = {"workspace_id": _WS, "user_id": "owner", "status": "unbound"}
    chart = _record(
        path="NVDA:1day", kind="chart", keep_updated=False, latest_seq=0,
        shared_at=None, files=None, kept_at=None, shared_seq=0,
    )
    ensure, find = AsyncMock(return_value=chart), AsyncMock(return_value=None)
    counts = AsyncMock(return_value=type("C", (), {"open": 0, "unread": 0})())
    with (
        patch(_GETWS, AsyncMock(return_value=home)),
        patch.object(records.artifacts_db, "ensure_record", ensure),
        patch.object(records.artifacts_db, "find_record", find),
        patch.object(records.artifacts_db, "comment_counts", counts),
    ):
        made = await owner.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"chart": " nvda:1day "}
        )
        found = await owner.get(
            f"/api/v1/workspaces/{_WS}/artifacts", params={"chart": "nvda:1day"}
        )
    assert made.status_code == 200
    assert (made.json()["kind"], made.json()["path"]) == ("chart", "NVDA:1day")
    ensure.assert_awaited_once_with(_WS, "chart", "NVDA:1day")
    assert found.json() == {"artifact": None}
    find.assert_awaited_once_with(_WS, "chart", "NVDA:1day")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"chart": "NVDA"},
        {"chart": "NVDA:1d"},
        {"chart": ":1day"},
        {"chart": "../x:1day"},
        {"chart": "a b:1day"},
        # An invisible format character would make a look-alike record.
        {"chart": "NVDA\u200b:1day"},
        {"chart": "NV\u202eDA:1day"},
    ],
)
async def test_a_chart_id_that_names_no_chart_is_refused(owner, body):
    home = {"workspace_id": _WS, "user_id": "owner", "status": "running"}
    with patch(_GETWS, AsyncMock(return_value=home)):
        resp = await owner.post(f"/api/v1/workspaces/{_WS}/artifacts", json=body)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_a_record_is_named_by_a_path_or_a_chart_not_both(owner):
    both = {"path": "a.html", "chart": "NVDA:1day"}
    made = await owner.post(f"/api/v1/workspaces/{_WS}/artifacts", json=both)
    found = await owner.get(f"/api/v1/workspaces/{_WS}/artifacts", params=both)
    neither = await owner.get(f"/api/v1/workspaces/{_WS}/artifacts")
    assert (made.status_code, found.status_code, neither.status_code) == (422, 422, 422)


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [{"workspace_id": _WS, "user_id": "x"}, None])
async def test_a_chart_in_a_workspace_not_the_callers_is_not_found(owner, workspace):
    with patch(_GETWS, AsyncMock(return_value=workspace)):
        resp = await owner.post(
            f"/api/v1/workspaces/{_WS}/artifacts", json={"chart": "NVDA:1day"}
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_a_chart_is_no_record_to_a_route_only_a_file_takes():
    """Publishing, sharing, keeping and versions are a file's alone, so a
    chart's id is not found there, while its comments' routes reach it."""
    from fastapi import HTTPException

    from src.server.app.artifacts import records

    chart = _record(
        path="NVDA:1day", kind="chart", keep_updated=False, latest_seq=0,
        shared_at=None, files=None, kept_at=None, shared_seq=0,
    )
    with patch.object(
        records.artifacts_db, "get_artifact", AsyncMock(return_value=chart)
    ):
        assert (await records.reached(chart.artifact_id, "owner"))[0] is chart
        with pytest.raises(HTTPException) as refused:
            await records.reached_file(chart.artifact_id, "owner")
    assert refused.value.status_code == 404
