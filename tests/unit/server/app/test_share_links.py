"""Unit tests for the owner's share-link routes (``share_links.py``).

The router is mounted on a bare test app with the DB patched where
``share_links`` imports it. What is locked here is the HTTP contract: a file
is shared only by publishing, so a share asked for here is a 409 naming the
artifact; a stop ends a file's share through its artifact record; a file
link names its artifact; app links and hidden paths are refused; and the
owner check, including the lookup by code that answers for any of the
owner's workspaces and for no one else's.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.app.share_links import owner_router as share_link_owner_router
from src.server.app.share_links import router as share_links_router
from src.server.database.artifacts import Artifact, ArtifactNotFound
from src.server.database.share_links import ShareLink
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_OWNER = "test-user-123"
_WS_ID = "ws-fake-0001"
_CODE = "k3Vq9ZtR2mXa"
_ARTIFACT = "11111111-2222-4333-8444-555555555555"
_BASE = f"/api/v1/workspaces/{_WS_ID}/share-links"

_M = "src.server.app.share_links"
_DBWS = f"{_M}.db_get_workspace"
# What a workspace's own share routes read the row through, Home's guard
# included.
_OWNED_DBWS = "src.server.services.workspace_files.db_get_workspace"
_GET_LINK = f"{_M}.get_link"
_CREATE = f"{_M}.artifacts_db.ensure_artifact"
_UNSHARE = f"{_M}.artifacts_db.unshare"
_CHANGED = f"{_M}.publish_artifact_change"
_SET_PRIVATE = f"{_M}.set_private"
_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _workspace(**overrides) -> dict:
    ws = {
        "workspace_id": _WS_ID,
        "user_id": _OWNER,
        "status": "stopped",
        "computer_root_dir": "/home/workspace",
        "dir_name": "proj",
    }
    ws.update(overrides)
    return ws


def _link(**overrides) -> ShareLink:
    row = {
        "code": _CODE,
        "workspace_id": _WS_ID,
        "kind": "file",
        "path": "report.html",
        "port": None,
        "title": None,
        "files": None,
        "shared_at": None,
        "created_at": _AT,
        "artifact": _artifact(),
    }
    row.update(overrides)
    return ShareLink(**row)


def _app_link(**overrides) -> ShareLink:
    return _link(kind="app", path=None, port=8080, artifact=None, **overrides)


def _artifact() -> Artifact:
    return Artifact(
        artifact_id=_ARTIFACT,
        code=_CODE,
        workspace_id=_WS_ID,
        path="report.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=0,
        shared_at=None,
        files=None,
        revision=1,
        created_at=_AT,
    )


@pytest_asyncio.fixture
async def client():
    app = create_test_app(share_links_router, share_link_owner_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def stranger_client():
    app = create_test_app(share_links_router, share_link_owner_router)
    app.dependency_overrides[get_current_user_id] = lambda: "someone-else"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# --- PATCH: stop; a file is shared by publishing ---------------------------


async def test_patch_share_on_a_file_link_points_to_publish(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_UNSHARE, AsyncMock()) as unshare,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": True})
    assert resp.status_code == 409
    assert resp.json()["detail"] == {
        "code": "publish_required",
        "artifact_id": _ARTIFACT,
    }
    unshare.assert_not_awaited()


async def test_patch_stop_on_a_file_link_unshares_its_artifact(client):
    shared = _link(files=("report.html",), shared_at=_AT)
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(side_effect=[shared, _link()])),
        patch(_UNSHARE, AsyncMock(return_value=_artifact())) as unshare,
        patch(_CHANGED, AsyncMock()) as changed,
        patch(_SET_PRIVATE, AsyncMock()) as set_private,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 200
    unshare.assert_awaited_once_with(user_id=_OWNER, artifact_id=_ARTIFACT)
    changed.assert_awaited_once_with(_OWNER, _artifact())
    set_private.assert_not_awaited()
    body = resp.json()
    assert body["shared"] is False
    assert body["shared_files"] is None
    assert body["artifact_id"] == _ARTIFACT


async def test_patch_stop_on_a_record_gone_since_is_404(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_UNSHARE, AsyncMock(side_effect=ArtifactNotFound(_ARTIFACT))),
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 404


# --- app links cannot be shared and name no artifact ------------------------


async def test_patch_share_on_an_app_link_is_400(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_app_link())),
        patch(_SET_PRIVATE, AsyncMock()) as set_private,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": True})
    assert resp.status_code == 400
    set_private.assert_not_awaited()


async def test_the_file_list_route_is_gone(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
    ):
        resp = await client.get(f"{_BASE}/{_CODE}/files")
    assert resp.status_code in (404, 405)


async def test_a_page_link_cannot_be_made(client):
    with patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())):
        resp = await client.post(
            _BASE, json={"kind": "page", "page_id": _ARTIFACT}
        )
    assert resp.status_code == 422


# --- POST: create ---------------------------------------------------------


async def test_create_refuses_a_hidden_path(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock()) as create,
    ):
        resp = await client.post(_BASE, json={"kind": "file", "path": ".agents/x.md"})
    assert resp.status_code == 400
    create.assert_not_awaited()


async def test_create_makes_the_files_record_and_returns_its_link(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock(return_value=_artifact())),
        patch(_GET_LINK, AsyncMock()) as get_link,
    ):
        resp = await client.post(_BASE, json={"kind": "file", "path": "report.html"})
    assert resp.status_code == 200
    # The record is the link: no second read.
    get_link.assert_not_awaited()
    body = resp.json()
    assert body["url"] == f"/a/{_CODE}"
    assert body["shared"] is False
    assert body["artifact_id"] == _ARTIFACT


async def test_create_folds_a_sandbox_absolute_path(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock(return_value=_artifact())) as create,
    ):
        resp = await client.post(
            _BASE, json={"kind": "file", "path": "/home/workspace/proj/report.html"}
        )
    assert resp.status_code == 200
    create.assert_awaited_once_with(_WS_ID, "report.html")


# --- ownership ------------------------------------------------------------


async def test_non_owner_cannot_create(stranger_client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock()) as create,
    ):
        resp = await stranger_client.post(
            _BASE, json={"kind": "file", "path": "report.html"}
        )
    assert resp.status_code == 403
    create.assert_not_awaited()


async def test_non_owner_cannot_stop(stranger_client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())) as get_link,
        patch(_UNSHARE, AsyncMock()) as unshare,
    ):
        resp = await stranger_client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 403
    get_link.assert_not_awaited()
    unshare.assert_not_awaited()


async def test_link_of_another_workspace_is_404(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link(workspace_id="ws-fake-0002"))),
        patch(_UNSHARE, AsyncMock()) as unshare,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 404
    unshare.assert_not_awaited()


# --- GET by code: any of the owner's workspaces --------------------------


async def test_lookup_answers_for_a_sibling_workspace_with_its_id(client):
    sibling = _workspace(workspace_id="ws-fake-0002")
    with (
        patch(_GET_LINK, AsyncMock(return_value=_app_link(workspace_id="ws-fake-0002"))),
        patch(_DBWS, AsyncMock(return_value=sibling)) as get_ws,
    ):
        resp = await client.get(f"/api/v1/share-links/{_CODE}")
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws-fake-0002"
    assert resp.json()["port"] == 8080
    assert resp.json()["artifact_id"] is None
    get_ws.assert_awaited_once_with("ws-fake-0002")


async def test_lookup_of_another_users_link_is_404(stranger_client):
    with (
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_DBWS, AsyncMock(return_value=_workspace())),
    ):
        resp = await stranger_client.get(f"/api/v1/share-links/{_CODE}")
    assert resp.status_code == 404


async def test_lookup_of_an_unknown_code_is_404(client):
    with (
        patch(_GET_LINK, AsyncMock(return_value=None)),
        patch(_DBWS, AsyncMock()) as get_ws,
    ):
        resp = await client.get(f"/api/v1/share-links/{_CODE}")
    assert resp.status_code == 404
    get_ws.assert_not_awaited()
