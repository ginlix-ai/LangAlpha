"""What an ``/s/`` or ``/a/`` code opens on the public router.

A file share link answers no file route: its visitors read the version they
see through the artifact grant the metadata route hands them, so every file
route treats the code as an unknown thread token. The metadata route
dispatches on the link's kind and the viewer's identity: a private link
exists for its owner alone, who sees the live file, and anyone else sees the
latest version.

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
from src.server.app.share_access import resolve_shared_files
from src.server.database.artifacts import Artifact, Version
from src.server.database.share_links import ShareLink
from src.server.services import file_grants
from src.server.services.artifacts import grants as artifact_grants
from src.server.services.artifacts.grants import verify_artifact_share_grant
from src.server.services.artifacts.snapshot import SnapshotUnavailable
from src.server.services.preview_base import PreviewBase
from src.server.utils.api import Viewer, get_viewer
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

CODE = "k3Vq9ZtR2mXa"
ARTIFACT = "11111111-2222-4333-8444-555555555555"
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
_SERVE_VAULT = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"
_SHARE_FILES_FP = "src.server.app.share_files.FilePersistenceService.get_file_content"
_SHARE_FILES_VAULT = "src.server.app.share_files.get_vault_secrets_for_redaction"
_ARTIFACTS_DB = "src.server.app.public.artifacts_db"
_LEGACY = "src.server.app.public.legacy_snapshot"
_PUBLIC_BASE = "src.server.app.public.preview_base"


@pytest.fixture(autouse=True)
def _pinned_grant_key(monkeypatch):
    """Every frame_base is a minted grant; the keys are read from Postgres once."""
    monkeypatch.setattr(file_grants._CODEC, "_key", b"k" * 32)
    monkeypatch.setattr(artifact_grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(artifact_grants._VISITOR, "_key", b"s" * 32)


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


_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _artifact(*, latest_seq: int = 2, shared: bool = True) -> Artifact:
    return Artifact(
        artifact_id=ARTIFACT,
        code=CODE,
        workspace_id=WS_ID,
        path="report.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=latest_seq,
        shared_at=_AT if shared else None,
        files=tuple(SHARED_FILES),
        revision=4,
        created_at=_AT,
    )


def _link(
    kind: str = "file",
    *,
    shared: bool = True,
    artifact: Artifact | None = None,
    **overrides,
) -> ShareLink:
    if kind == "file" and artifact is None:
        artifact = _artifact(latest_seq=2 if shared else 0, shared=shared)
    row = {
        "code": CODE,
        "workspace_id": WS_ID,
        "kind": kind,
        "path": "report.html" if kind == "file" else None,
        "port": None if kind == "file" else 8080,
        "title": None,
        "files": tuple(SHARED_FILES) if (kind == "file" and shared) else None,
        "shared_at": _AT if shared else None,
        "created_at": _AT,
        "revision": 4,
        "artifact": artifact,
    }
    row.update(overrides)
    return ShareLink(**row)


def _version(seq: int = 2) -> Version:
    return Version(
        seq=seq,
        entry_path="report.html",
        origin="owner",
        title="Q3 report",
        label=None,
        byte_len=100,
        published_at=_AT,
    )


def _versions(*, latest: Version | None = None) -> MagicMock:
    """The version rows the metadata route reads, standing in for the module."""
    db = MagicMock()
    db.get_version = AsyncMock(return_value=latest or _version())
    return db


def _client(viewer: str | None = None, *, override_viewer: bool = True) -> AsyncClient:
    app = create_test_app(public_router)
    if override_viewer:
        app.dependency_overrides[get_viewer] = lambda: Viewer(viewer)
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Accept": "application/json"},
    )


# --- serve route -------------------------------------------------------------


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
        async with _client() as client:
            resp = await client.get(
                "/api/v1/public/shared/share_abc123/files/serve/report.html"
            )
    assert resp.status_code == 403
    link.assert_not_awaited()


async def test_a_thread_token_still_opens_its_subtree():
    thread = {
        "conversation_thread_id": "thread-fake-1",
        "workspace_id": WS_ID,
        "share_permissions": {"allow_files": True, "root_path": "out"},
    }
    with (
        patch(_ACCESS_THREAD, AsyncMock(return_value=thread)),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        target = await resolve_shared_files("share_abc123", require_files=True)
    assert target.visible("out/report.html")
    assert not target.visible("report.html")


async def test_a_link_shared_before_versions_is_snapshotted_first():
    versions = _versions(latest=_version(1))
    unversioned = _link(artifact=_artifact(latest_seq=0))
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=unversioned)),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ARTIFACTS_DB, versions),
        patch(_LEGACY, AsyncMock(return_value=_artifact(latest_seq=1))) as legacy,
        patch(_PUBLIC_BASE, return_value=None),
    ):
        async with _client() as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    assert resp.json()["seq"] == 1
    legacy.assert_awaited_once()
    versions.get_version.assert_awaited_once_with(ARTIFACT, 1)


async def test_a_first_version_that_cannot_be_read_is_503():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(artifact=_artifact(latest_seq=0)))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ARTIFACTS_DB, _versions()),
        patch(_LEGACY, AsyncMock(side_effect=SnapshotUnavailable("stopped"))),
    ):
        async with _client() as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 503


# --- route sweep -------------------------------------------------------------


_METADATA_ROUTE = "/api/v1/public/shared/{share_token}"
_KNOWN_OTHER_ROUTES = {
    "/api/v1/public/shared/{share_token}/files/serve/{path:path}",
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
        if "{share_token}" not in path or path == _METADATA_ROUTE:
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
    """A shared file link opens no file route: its files go through a grant."""
    url = path.replace("{share_token}", CODE).replace("{path:path}", "report.html")
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


async def test_metadata_shared_file_is_its_latest_version_to_anyone():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(path="out/report.html"))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ARTIFACTS_DB, _versions()),
        patch(_PUBLIC_BASE, return_value=None),
    ):
        async with _client(None) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 200
    body = resp.json()
    frame_base = body.pop("frame_base")
    assert body.pop("expires_in") > 0
    assert body == {
        "kind": "file",
        "access": "public",
        "artifact_id": ARTIFACT,
        "name": "report.html",
        "path": "report.html",
        "title": "Q3 report",
        "seq": 2,
        "isolated": False,
    }
    assert frame_base.startswith("/api/v1/artifacts/g/s1.") and frame_base.endswith("/")
    grant = await verify_artifact_share_grant(frame_base.split("/")[5])
    assert (grant.code, grant.artifact_id, grant.revision, grant.seq) == (
        CODE,
        ARTIFACT,
        4,
        2,
    )


async def test_metadata_frames_a_version_on_its_artifact_host_under_a_base():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ARTIFACTS_DB, _versions()),
        patch(_PUBLIC_BASE, return_value=PreviewBase("https", "preview.example.com")),
    ):
        async with _client(None) as client:
            body = (await client.get(f"/api/v1/public/shared/{CODE}")).json()
    assert body["isolated"] is True
    assert body["frame_base"].startswith(
        f"https://artifact-{ARTIFACT}.preview.example.com/g/s1."
    )


@pytest.mark.parametrize("viewer", [None, STRANGER])
async def test_metadata_private_file_is_404_for_everyone_but_the_owner(viewer):
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link(shared=False))),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
    ):
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == 404


async def test_metadata_private_file_opens_live_for_the_owner_under_a_grant():
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
    assert {k: body[k] for k in body if k not in ("frame_base", "expires_in")} == {
        "kind": "file",
        "access": "owner",
        "artifact_id": ARTIFACT,
        "workspace_id": WS_ID,
        "name": "report.html",
        "path": "report.html",
        "shared": False,
        "latest_seq": 0,
    }
    assert body["frame_base"].startswith(f"/api/v1/wsfiles/g/v1.{WS_ID}.")
    assert body["frame_base"].endswith("/")
    # A lifetime, not a timestamp: the page anchors it on its own clock.
    grant_exp = int(body["frame_base"].split(".")[2])
    assert body["expires_in"] == pytest.approx(grant_exp - time.time(), abs=5)


async def test_metadata_owner_as_visitor_sees_the_public_view():
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_link())),
        patch(_ACCESS_WS, AsyncMock(return_value=_workspace())),
        patch(_ARTIFACTS_DB, _versions()),
        patch(_PUBLIC_BASE, return_value=None),
    ):
        async with _client(OWNER) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}", params={"as": "visitor"}
            )
    assert resp.status_code == 200
    body = resp.json()
    assert body["access"] == "public"
    assert body["seq"] == 2
    assert body["frame_base"].startswith("/api/v1/artifacts/g/s1.")


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
        patch(_ARTIFACTS_DB, _versions()),
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
        patch(_ARTIFACTS_DB, _versions()),
    ):
        async with _client(override_viewer=False) as client:
            resp = await client.get(
                f"/api/v1/public/shared/{CODE}",
                headers={"Authorization": "Bearer owner"},
            )
    assert resp.status_code == status
    if status == 200:
        assert resp.json()["access"] == "public"
