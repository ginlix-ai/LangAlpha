"""What an ``/s/`` or ``/a/`` code opens on the public router.

A file share link answers exactly one file route, the serve route, and only
for the files its owner confirmed; every other public route treats the code
as an unknown thread token. The metadata route dispatches on the link's kind
and the viewer's identity: a private link exists for its owner alone.

Patch targets name the module holding the reference, not the one defining it:
``share_access`` decides link access for both routes, so its ``get_link`` and
``db_get_workspace`` are the ones patched.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.app.public import router as public_router
from src.server.app.share_access import (
    SharedFileTarget,
    resolve_serve_target,
)
from src.server.database.share_links import ShareLink
from src.server.services import file_grants
from src.server.utils.api import Viewer, get_viewer
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

CODE = "k3Vq9ZtR2mXa"
WS_ID = "ws-fake-0001"
OWNER = "owner-fake-1"
STRANGER = "user-fake-2"
WORK_DIR = "/home/workspace/proj"
SHARED_FILES = ["report.html", "charts/a.png"]

_ACCESS_LINK = "src.server.app.share_access.get_link"
_ACCESS_WS = "src.server.app.share_access.db_get_workspace"
_ACCESS_THREAD = "src.server.app.share_access.get_thread_by_share_token"
_PUBLIC_THREAD = "src.server.app.public.get_shared_thread"
_PUBLIC_PREVIEW_URL = "src.server.app.public.owner_preview_url"
_PUBLIC_URL_EXPIRY = "src.server.app.public.signed_url_expires_at"
_SERVE_WS = "src.server.app.workspace_files.serve.db_get_workspace"
_SERVE_FP = "src.server.app.workspace_files.serve.FilePersistenceService"
_SERVE_VAULT = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"
_SERVE_WSMGR = "src.server.app.workspace_files.serve.WorkspaceManager"
_SHARE_FILES_FP = "src.server.app.share_files.FilePersistenceService.get_file_content"
_SHARE_FILES_VAULT = "src.server.app.share_files.get_vault_secrets_for_redaction"


@pytest.fixture(autouse=True)
def _pinned_grant_key(monkeypatch):
    """The owner's frame_base is a minted grant; the key is read from Postgres once."""
    monkeypatch.setattr(file_grants, "_key", b"k" * 32)


@pytest.fixture(autouse=True)
def _no_vault_secrets():
    with (
        patch(_SERVE_VAULT, AsyncMock(return_value={})),
        patch(_SHARE_FILES_VAULT, AsyncMock(return_value={})),
    ):
        yield


def _workspace(**overrides) -> dict:
    ws = {
        "workspace_id": WS_ID,
        "user_id": OWNER,
        "status": "stopped",
        "computer_root_dir": "/home/workspace",
        "dir_name": "proj",
        "sandbox_id": None,
        "config": None,
    }
    ws.update(overrides)
    return ws


def _link(kind: str = "file", *, shared: bool = True, **overrides) -> ShareLink:
    row = {
        "code": CODE,
        "workspace_id": WS_ID,
        "kind": kind,
        "path": "report.html" if kind == "file" else None,
        "port": None if kind == "file" else 8080,
        "title": None,
        "files": tuple(SHARED_FILES) if (kind == "file" and shared) else None,
        "shared_at": datetime(2026, 1, 1, tzinfo=timezone.utc) if shared else None,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return ShareLink(**row)


def _html_record(text: str) -> dict:
    return {
        "file_name": "report.html",
        "content_text": text,
        "content_binary": None,
        "is_binary": False,
        "mime_type": "text/html",
    }


def _client(viewer: str | None = None, *, override_viewer: bool = True) -> AsyncClient:
    app = create_test_app(public_router)
    if override_viewer:
        app.dependency_overrides[get_viewer] = lambda: Viewer(viewer)
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Accept": "application/json"},
    )


def _serving(link: ShareLink | None, workspace: dict | None = _workspace()):
    """Everything the serve route reads, with a stopped workspace served from the mirror."""
    fp = MagicMock()
    fp.get_file_content = AsyncMock(
        return_value=_html_record("<html><body>shared</body></html>")
    )
    return (
        patch(_ACCESS_LINK, AsyncMock(return_value=link)),
        patch(_ACCESS_WS, AsyncMock(return_value=workspace)),
        patch(_ACCESS_THREAD, AsyncMock(return_value=None)),
        patch(_SERVE_WS, AsyncMock(return_value=workspace)),
        patch(_SERVE_FP, fp),
        patch(_SERVE_WSMGR),
    )


# --- resolve_serve_target ---------------------------------------------------


async def test_shared_file_link_resolves_to_its_frozen_file_list():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        target = await resolve_serve_target(CODE)
    assert isinstance(target, SharedFileTarget)
    assert target.files == frozenset(SHARED_FILES)
    assert target.scope.workspace_id == WS_ID
    assert target.scope.root_path == ""
    assert target.work_dir == WORK_DIR
    assert target.visible("charts/a.png")
    assert not target.visible("charts/b.png")


async def test_private_file_link_resolves_to_404():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(shared=False))),
        patch(_ACCESS_WS, AsyncMock()) as ws,
    ):
        with pytest.raises(HTTPException) as exc:
            await resolve_serve_target(CODE)
    assert exc.value.status_code == 404
    ws.assert_not_awaited()


@pytest.mark.parametrize("shared", [True, False])
async def test_app_link_never_resolves_for_serving(shared):
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link("app", shared=shared))),
        patch(_ACCESS_WS, AsyncMock()) as ws,
    ):
        with pytest.raises(HTTPException) as exc:
            await resolve_serve_target(CODE)
    assert exc.value.status_code == 404
    ws.assert_not_awaited()


async def test_non_code_token_falls_through_to_thread_rules():
    """A thread token is never looked up as a link and still needs allow_files."""
    thread = {
        "conversation_thread_id": "thread-fake-1",
        "workspace_id": WS_ID,
        "share_permissions": {"allow_files": False},
    }
    with (
        patch(_ACCESS_LINK, AsyncMock()) as link,
        patch(_ACCESS_THREAD, AsyncMock(return_value=thread)),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        with pytest.raises(HTTPException) as exc:
            await resolve_serve_target("share_abc123")
    assert exc.value.status_code == 403
    link.assert_not_awaited()


# --- serve route -------------------------------------------------------------


async def test_serve_answers_a_listed_path():
    link_p, ws_p, thread_p, sws_p, fp_p, mgr_p = _serving(_link())
    with link_p, ws_p, thread_p, sws_p, fp_p, mgr_p:
        async with _client() as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}/files/serve/report.html"
            )
    assert resp.status_code == 200
    assert b"shared" in resp.content
    assert resp.headers["content-type"].startswith("text/html")


@pytest.mark.parametrize("path", ["charts/b.png", "notes.md", "charts/../report2.html"])
async def test_serve_refuses_an_unlisted_path(path):
    """A sibling in a listed folder is still unlisted: the list is exact."""
    link_p, ws_p, thread_p, sws_p, fp_p, mgr_p = _serving(_link())
    with link_p, ws_p, thread_p, sws_p, fp_p as fp, mgr_p:
        async with _client() as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}/files/serve/{path}")
    assert resp.status_code == 404
    fp.get_file_content.assert_not_awaited()


async def test_serve_refuses_a_private_file_link():
    link_p, ws_p, thread_p, sws_p, fp_p, mgr_p = _serving(_link(shared=False))
    with link_p, ws_p, thread_p, sws_p, fp_p as fp, mgr_p:
        async with _client() as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}/files/serve/report.html"
            )
    assert resp.status_code == 404
    fp.get_file_content.assert_not_awaited()


@pytest.mark.parametrize("shared", [True, False])
async def test_serve_refuses_every_app_link(shared):
    link_p, ws_p, thread_p, sws_p, fp_p, mgr_p = _serving(
        _link("app", shared=shared)
    )
    with link_p, ws_p, thread_p, sws_p, fp_p as fp, mgr_p:
        async with _client() as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}/files/serve/report.html"
            )
    assert resp.status_code == 404
    fp.get_file_content.assert_not_awaited()


# --- route sweep -------------------------------------------------------------


_SERVE_ROUTE = "/api/v1/public/shared/{share_token}/files/serve/{path:path}"
_METADATA_ROUTE = "/api/v1/public/shared/{share_token}"
_KNOWN_OTHER_ROUTES = {
    "/api/v1/public/shared/{share_token}/replay",
    "/api/v1/public/shared/{share_token}/files",
    "/api/v1/public/shared/{share_token}/files/read",
    "/api/v1/public/shared/{share_token}/files/download",
    "/api/v1/public/shared/{share_token}/files/resolve",
}


def _other_token_routes() -> list[tuple[str, str]]:
    """Every (method, path) on the public router a file link must not answer."""
    found = []
    for route in public_router.routes:
        path = getattr(route, "path", "")
        if "{share_token}" not in path or path in (_SERVE_ROUTE, _METADATA_ROUTE):
            continue
        for method in sorted(getattr(route, "methods", None) or ()):
            if method in ("HEAD", "OPTIONS"):
                continue
            found.append((method, path))
    return found


async def test_route_sweep_covers_the_known_routes():
    swept = {path for _, path in _other_token_routes()}
    assert _KNOWN_OTHER_ROUTES <= swept, swept


@pytest.mark.parametrize("method,path", _other_token_routes())
async def test_file_link_code_is_unknown_on_every_other_route(method, path):
    """A shared file link opens the serve route and nothing else."""
    url = path.replace("{share_token}", CODE)
    kwargs: dict = {}
    if path.endswith(("/files/read", "/files/download")):
        kwargs["params"] = {"path": "report.html"}
    if method == "POST":
        kwargs["json"] = {"candidates": ["report.html"]}
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ACCESS_THREAD, AsyncMock(return_value=None)),
        patch(
            "src.server.app.public.get_shared_thread",
            AsyncMock(side_effect=HTTPException(status_code=404, detail="x")),
        ),
        patch(_SHARE_FILES_FP, AsyncMock()) as fp,
    ):
        async with _client() as client:
            resp = await client.request(method, url, **kwargs)
    assert resp.status_code == 404, (method, path, resp.text)
    fp.assert_not_awaited()


# --- metadata ----------------------------------------------------------------


async def test_metadata_thread_token_answers_the_thread():
    thread = {
        "conversation_thread_id": "thread-fake-1",
        "workspace_id": WS_ID,
        "share_permissions": {"allow_files": True},
        "title": "T",
        "root_dir": "/home/dev/langalpha",
        # A turn from before a move spells its paths under the root it left.
        "previous_root_dirs": ["/home/old/langalpha"],
        "dir_name": "NVDA",
    }
    with (
        patch(_ACCESS_LINK, AsyncMock()) as link,
        patch(_PUBLIC_THREAD, AsyncMock(return_value=thread)),
    ):
        async with _client() as client:
            resp = await client.get("/api/v1/public/shared/share_abc123")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "thread"
    assert body["thread_id"] == "thread-fake-1"
    assert body["permissions"] == {"allow_files": True, "allow_download": False}
    assert body["root_dir"] == "/home/dev/langalpha"
    assert body["previous_root_dirs"] == ["/home/old/langalpha"]
    assert body["dir_name"] == "NVDA"
    assert "previous_dir_names" not in body
    link.assert_not_awaited()


async def test_metadata_without_files_leaves_out_the_roots_a_move_left():
    """They only open files, and the thread may never spell them."""
    thread = {
        "conversation_thread_id": "thread-fake-1",
        "workspace_id": WS_ID,
        "share_permissions": {"allow_files": False},
        "root_dir": "/home/dev/langalpha",
        "previous_root_dirs": ["/home/old/langalpha"],
        "dir_name": "NVDA",
    }
    with (
        patch(_ACCESS_LINK, AsyncMock()),
        patch(_PUBLIC_THREAD, AsyncMock(return_value=thread)),
    ):
        async with _client() as client:
            resp = await client.get("/api/v1/public/shared/share_abc123")
    body = resp.json()
    assert body["previous_root_dirs"] == []
    assert body["root_dir"] == "/home/dev/langalpha"


async def test_metadata_shared_file_is_public_to_anyone():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(path="out/report.html"))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(None) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    assert resp.json() == {
        "kind": "file",
        "name": "report.html",
        "path": "out/report.html",
        "access": "public",
        "frame_base": f"/api/v1/public/shared/{CODE}/files/serve/",
    }


@pytest.mark.parametrize("viewer", [None, STRANGER])
async def test_metadata_private_file_is_404_for_everyone_but_the_owner(viewer):
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(shared=False))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 404


async def test_metadata_private_file_opens_for_the_owner_under_a_grant():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(shared=False))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    # The grant is the owner's: no cache on the way may replay it to a visitor.
    assert resp.headers["cache-control"] == "no-store"
    body = resp.json()
    assert body["access"] == "owner"
    assert body["frame_base"].startswith(f"/api/v1/wsfiles/g/v1.{WS_ID}.")
    assert body["frame_base"].endswith("/")
    # A lifetime, not a timestamp: the page anchors it on its own clock.
    grant_exp = int(body["frame_base"].split(".")[2])
    assert body["expires_in"] == pytest.approx(grant_exp - time.time(), abs=5)


async def test_metadata_owner_as_visitor_sees_the_public_view():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}", params={"as": "visitor"}
            )
    assert resp.status_code == 200
    body = resp.json()
    assert body["access"] == "public"
    assert body["frame_base"] == f"/api/v1/public/shared/{CODE}/files/serve/"


async def test_metadata_owner_as_visitor_on_a_private_file_is_404():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(shared=False))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}", params={"as": "visitor"}
            )
    assert resp.status_code == 404


async def test_metadata_app_opens_for_the_owner_only():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link("app", path="dash.html"))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(
            _PUBLIC_PREVIEW_URL,
            AsyncMock(return_value="https://preview.example.com/signed?token=abc"),
        ) as preview,
        patch(
            _PUBLIC_URL_EXPIRY, AsyncMock(return_value=int(time.time()) + 3600)
        ) as expiry,
    ):
        async with _client(OWNER) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    body = resp.json()
    assert body.pop("expires_in") == pytest.approx(3600, abs=5)
    assert body == {
        "kind": "app",
        "title": "App on port 8080",
        "url": "https://preview.example.com/signed/dash.html?token=abc",
    }
    preview.assert_awaited_once_with(WS_ID, OWNER, 8080)
    # The expiry is the minted URL's, not the one with the entry path spliced in.
    expiry.assert_awaited_once_with("https://preview.example.com/signed?token=abc")


async def test_metadata_app_opens_at_the_page_an_old_preview_url_named():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link("app", path="dash.html"))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(
            _PUBLIC_PREVIEW_URL,
            AsyncMock(return_value="https://preview.example.com/signed?token=abc"),
        ),
        patch(_PUBLIC_URL_EXPIRY, AsyncMock(return_value=1_767_229_200)),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}", params={"path": "reports/q3.html"}
            )
    assert resp.json()["url"] == "https://preview.example.com/signed/reports/q3.html?token=abc"


@pytest.mark.parametrize("viewer", [None, STRANGER])
async def test_metadata_app_is_404_for_anyone_else(viewer):
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link("app"))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_PUBLIC_PREVIEW_URL, AsyncMock()) as preview,
    ):
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 404
    preview.assert_not_awaited()


async def test_metadata_missing_workspace_is_404():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=None)),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 404


async def test_metadata_stale_bearer_still_gets_the_public_answer(monkeypatch):
    """A visitor whose session expired reads as anonymous, never as a 401."""
    monkeypatch.setattr("src.server.utils.api.HOST_MODE", "platform")
    with (
        patch(
            "src.server.utils.api._decode_token",
            AsyncMock(side_effect=HTTPException(status_code=401, detail="Invalid token")),
        ),
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(override_viewer=False) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}",
                headers={"Authorization": "Bearer stale"},
            )
    assert resp.status_code == 200
    assert resp.json()["access"] == "public"


@pytest.mark.parametrize(
    ("link", "status"),
    [
        (_link(), 200),
        (_link(shared=False), 503),
        (None, 503),
    ],
    ids=["shared", "private", "unknown"],
)
async def test_metadata_while_the_keys_are_unreachable_never_says_not_found(
    monkeypatch, link, status
):
    """The bearer may be the owner's: a shared link still answers publicly, but
    a private link and an unknown code both wait for the keys, alike."""
    monkeypatch.setattr("src.server.utils.api.HOST_MODE", "platform")
    with (
        patch(
            "src.server.utils.api._decode_token",
            AsyncMock(side_effect=HTTPException(status_code=503, detail="Auth unavailable")),
        ),
        patch(_ACCESS_LINK, AsyncMock(return_value=link)),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ACCESS_THREAD, AsyncMock(return_value=None)),
    ):
        async with _client(override_viewer=False) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}",
                headers={"Authorization": "Bearer owner"},
            )
    assert resp.status_code == status
    if status == 200:
        assert resp.json()["access"] == "public"
