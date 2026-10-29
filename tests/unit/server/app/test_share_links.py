"""Unit tests for the owner's share-link routes (``share_links.py``).

The router is mounted on a bare test app with the DB and the manifest walk
patched where ``share_links`` imports them. What is locked here is the HTTP
contract the dialog relies on: the 409s that refuse a silently widened share
and a share that would undo a Stop, the order-insensitive equality that
accepts a reviewed list, the refusals for app links and hidden paths, and the
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
from src.server.app.share_manifest import (
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
)
from src.server.database.share_links import ShareLink
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_OWNER = "test-user-123"
_WS_ID = "ws-fake-0001"
_CODE = "k3Vq9ZtR2mXa"
_BASE = f"/api/v1/workspaces/{_WS_ID}/share-links"

_M = "src.server.app.share_links"
_DBWS = f"{_M}.db_get_workspace"
# What a workspace's own share routes read the row through, Home's guard
# included.
_OWNED_DBWS = "src.server.services.workspace_files.db_get_workspace"
_GET_LINK = f"{_M}.get_link"
_CREATE = f"{_M}.ensure_file_link"
_SET_SHARED = f"{_M}.set_shared"
_SET_PRIVATE = f"{_M}.set_private"
_MANIFEST = f"{_M}.build_manifest"


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
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return ShareLink(**row)


def _app_link(**overrides) -> ShareLink:
    return _link(kind="app", path=None, port=8080, **overrides)


_ENTRIES = [
    ManifestEntry("report.html", 10, "entry"),
    ManifestEntry("style.css", 5, "style"),
]


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


# --- PATCH: share with a reviewed list, or stop --------------------------


async def test_patch_refuses_a_list_that_differs_from_the_recomputed_one(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_MANIFEST, AsyncMock(return_value=_ENTRIES)),
        patch(_SET_SHARED, AsyncMock()) as set_shared,
    ):
        resp = await client.patch(
            f"{_BASE}/{_CODE}", json={"shared": True, "files": ["report.html"]}
        )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "files_changed"
    assert detail["files"] == [e.as_dict() for e in _ENTRIES]
    set_shared.assert_not_awaited()


async def test_patch_accepts_the_same_set_in_another_order(client):
    stored = _link(files=("report.html", "style.css"), shared_at=datetime.now(timezone.utc))
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_MANIFEST, AsyncMock(return_value=_ENTRIES)),
        patch(_SET_SHARED, AsyncMock(return_value=stored)) as set_shared,
    ):
        resp = await client.patch(
            f"{_BASE}/{_CODE}",
            json={"shared": True, "files": ["style.css", "report.html"]},
        )
    assert resp.status_code == 200
    set_shared.assert_awaited_once_with(
        _CODE, ["report.html", "style.css"], expected_revision=0
    )
    body = resp.json()
    assert body["shared"] is True
    assert body["shared_files"] == ["report.html", "style.css"]
    assert body["title"] == "report.html"


async def test_patch_that_lost_a_race_with_a_stop_is_409(client):
    """The share was read before the manifest walk; a Stop that landed since holds."""
    was_shared = _link(
        files=("report.html",),
        shared_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        revision=5,
    )
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=was_shared)),
        patch(_MANIFEST, AsyncMock(return_value=_ENTRIES)),
        patch(_SET_SHARED, AsyncMock(return_value=None)) as set_shared,
    ):
        resp = await client.patch(
            f"{_BASE}/{_CODE}",
            json={"shared": True, "files": ["report.html", "style.css"]},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"] == {"code": "link_changed"}
    assert set_shared.await_args.kwargs["expected_revision"] == was_shared.revision


async def test_patch_share_without_a_list_is_422(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_MANIFEST, AsyncMock(return_value=_ENTRIES)) as manifest,
        patch(_SET_SHARED, AsyncMock()) as set_shared,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": True})
    assert resp.status_code == 422
    manifest.assert_not_awaited()
    set_shared.assert_not_awaited()


async def test_patch_unshare_makes_the_link_private(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link(files=("report.html",)))),
        patch(_SET_PRIVATE, AsyncMock(return_value=_link())) as set_private,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 200
    set_private.assert_awaited_once_with(_CODE)
    body = resp.json()
    assert body["shared"] is False
    assert body["shared_files"] is None


# --- app links have no file list and cannot be shared --------------------


async def test_patch_share_on_an_app_link_is_400(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_app_link())),
        patch(_SET_SHARED, AsyncMock()) as set_shared,
    ):
        resp = await client.patch(
            f"{_BASE}/{_CODE}", json={"shared": True, "files": ["index.html"]}
        )
    assert resp.status_code == 400
    set_shared.assert_not_awaited()


async def test_files_on_an_app_link_is_400(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_app_link())),
        patch(_MANIFEST, AsyncMock()) as manifest,
    ):
        resp = await client.get(f"{_BASE}/{_CODE}/files")
    assert resp.status_code == 400
    manifest.assert_not_awaited()


# --- POST: create ---------------------------------------------------------


async def test_create_refuses_a_hidden_path(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock()) as create,
    ):
        resp = await client.post(_BASE, json={"kind": "file", "path": ".agents/x.md"})
    assert resp.status_code == 400
    create.assert_not_awaited()


async def test_create_returns_the_short_url(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock(return_value=_link())),
    ):
        resp = await client.post(_BASE, json={"kind": "file", "path": "report.html"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["url"] == f"/a/{_CODE}"
    assert body["shared"] is False


async def test_create_folds_a_sandbox_absolute_path(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CREATE, AsyncMock(return_value=_link())) as create,
    ):
        resp = await client.post(
            _BASE, json={"kind": "file", "path": "/home/workspace/proj/report.html"}
        )
    assert resp.status_code == 200
    create.assert_awaited_once_with(_WS_ID, "report.html")


# --- GET files: manifest caps map to HTTP --------------------------------


async def test_files_too_large_is_422_with_the_cap(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_MANIFEST, AsyncMock(side_effect=ManifestTooLarge("too_many_files", 200))),
    ):
        resp = await client.get(f"{_BASE}/{_CODE}/files")
    assert resp.status_code == 422
    assert resp.json()["detail"] == {"code": "too_many_files", "limit": 200}


async def test_files_missing_entry_is_404(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())),
        patch(_MANIFEST, AsyncMock(side_effect=ManifestEntryMissing("report.html"))),
    ):
        resp = await client.get(f"{_BASE}/{_CODE}/files")
    assert resp.status_code == 404


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


async def test_non_owner_cannot_share(stranger_client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link())) as get_link,
        patch(_SET_SHARED, AsyncMock()) as set_shared,
    ):
        resp = await stranger_client.patch(
            f"{_BASE}/{_CODE}", json={"shared": True, "files": ["report.html"]}
        )
    assert resp.status_code == 403
    get_link.assert_not_awaited()
    set_shared.assert_not_awaited()


async def test_link_of_another_workspace_is_404(client):
    with (
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_GET_LINK, AsyncMock(return_value=_link(workspace_id="ws-fake-0002"))),
        patch(_SET_PRIVATE, AsyncMock()) as set_private,
    ):
        resp = await client.patch(f"{_BASE}/{_CODE}", json={"shared": False})
    assert resp.status_code == 404
    set_private.assert_not_awaited()


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
