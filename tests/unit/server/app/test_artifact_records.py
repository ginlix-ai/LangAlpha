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
        answer = await records._response(_record(), "owner")
    assert (answer.latest_seq, answer.shared_seq) == (5, 3)


@pytest.mark.asyncio
async def test_the_owners_tabs_hear_both_pointers_move():
    from src.server.services.artifacts import events

    with patch.object(events, "publish_user_event", AsyncMock()) as sent:
        await events.publish_artifact_change("owner", _record())
    event = sent.await_args.args[1]
    assert (event["latest_seq"], event["shared_seq"]) == (5, 3)
