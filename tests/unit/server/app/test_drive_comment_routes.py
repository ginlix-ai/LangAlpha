"""The Drive routes the comments on its files use: the list markers' counts,
and an HTML file's text with the page bridge for the app's frame. Both are
the Drive's, so with the Drive off they are not found."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.app import drive as drive_routes
from src.server.database.artifacts.comments import CommentCounts
from src.server.preview_proxy.page_bridge import bridge_for
from src.server.services import drive
from src.server.services.drive import Content, DriveError
from src.server.utils.api import get_current_user_id
from tests.conftest import create_test_app

_AT = datetime(2026, 11, 27, tzinfo=timezone.utc)

_ENTRY = "00000000-0000-0000-0000-0000000000d0"
_PAGE = b"<!doctype html><html><head><title>t</title></head><body>hi</body></html>"


@pytest_asyncio.fixture
async def client():
    app = create_test_app(drive_routes.router)
    app.dependency_overrides[get_current_user_id] = lambda: "owner"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


@pytest.fixture
def space(monkeypatch):
    opened = AsyncMock(return_value=SimpleNamespace(space_id="s1"))
    monkeypatch.setattr(drive, "open", opened)
    return opened


@pytest.mark.asyncio
async def test_counts_are_each_live_file_s_open_and_unread(client, space, monkeypatch):
    counted = AsyncMock(return_value={_ENTRY: CommentCounts(open=2, unread=1)})
    monkeypatch.setattr(drive_routes.artifacts_db, "drive_comment_counts", counted)

    resp = await client.get("/api/v1/drive/personal/comment-counts")

    assert resp.status_code == 200
    assert resp.json() == {"entries": {_ENTRY: {"open": 2, "unread": 1}}}
    counted.assert_awaited_once_with(user_id="owner", space_id="s1")


@pytest.mark.asyncio
async def test_counts_are_not_found_with_the_drive_off(client, space, monkeypatch):
    space.side_effect = DriveError("not_found", "No such space")
    counted = AsyncMock()
    monkeypatch.setattr(drive_routes.artifacts_db, "drive_comment_counts", counted)

    resp = await client.get("/api/v1/drive/personal/comment-counts")

    assert resp.status_code == 404
    counted.assert_not_awaited()


def _serving(
    monkeypatch, body: bytes, content_type: str, *, name: str = "page.html", is_text: bool = True
) -> None:
    found = Content(
        space_id="s1",
        sha256="ab" * 32,
        size=len(body),
        is_text=is_text,
        content_type=content_type,
        blob_user_id=None,
    )
    entry = SimpleNamespace(entry_id=_ENTRY, name=name)
    monkeypatch.setattr(drive, "content", AsyncMock(return_value=(entry, found)))

    async def stream(_found, start=0, end=None):
        yield body[start:end]

    monkeypatch.setattr(drive, "stream", stream)


@pytest.mark.asyncio
@pytest.mark.parametrize("inject", ["comments", "view"])
async def test_an_html_file_comes_with_the_bridge_first_in_its_head(
    client, space, monkeypatch, inject
):
    _serving(monkeypatch, _PAGE, "text/html")

    resp = await client.get(
        "/api/v1/drive/personal/content", params={"id": _ENTRY, "inject": inject}
    )

    assert resp.status_code == 200
    bridge = bridge_for(inject)
    head = resp.content.index(b"<head>") + len(b"<head>")
    assert resp.content.index(bridge.script) > head
    assert resp.content.index(bridge.script) < resp.content.index(b"<title>")
    # The framed copy is a different document from the bytes as they are.
    assert resp.headers["etag"] == f'"{"ab" * 32}.{bridge.digest}"'
    assert resp.headers["content-security-policy"] == "sandbox; default-src 'none'"


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["text/plain", "application/xhtml+xml"])
async def test_a_page_by_its_name_takes_the_bridge_whatever_type_it_was_stored_under(
    client, space, monkeypatch, content_type
):
    """The app frames a file named as a page from its text, which a browser
    parses as HTML: a note renamed to ``.html`` is framed, so it is bridged."""
    _serving(monkeypatch, _PAGE, content_type, name="Report.HTML")

    resp = await client.get(
        "/api/v1/drive/personal/content", params={"id": _ENTRY, "inject": "comments"}
    )

    assert bridge_for("comments").script in resp.content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "content_type", "is_text"),
    [
        ("notes.md", "text/markdown", True),
        # Stored as a page, but the app opens it by its name, as text.
        ("page.txt", "text/html", True),
        # Bytes that are not text are never framed.
        ("page.html", "text/html", False),
    ],
)
async def test_a_file_the_app_does_not_frame_comes_as_it_is(
    client, space, monkeypatch, name, content_type, is_text
):
    _serving(monkeypatch, b"# notes\n", content_type, name=name, is_text=is_text)

    resp = await client.get(
        "/api/v1/drive/personal/content", params={"id": _ENTRY, "inject": "comments"}
    )

    assert resp.content == b"# notes\n"
    assert resp.headers["etag"] == f'"{"ab" * 32}"'


@pytest.mark.asyncio
async def test_a_page_in_another_encoding_says_it_takes_no_bridge(client, space, monkeypatch):
    """The app then frames the markup it already holds, with no comments
    over it, rather than wait on a bridge that is not there."""
    _serving(monkeypatch, "<html><body>\u4e2d\u6587</body></html>".encode("gbk"), "text/html")

    resp = await client.get(
        "/api/v1/drive/personal/content", params={"id": _ENTRY, "inject": "comments"}
    )

    assert resp.status_code == 409
    assert resp.json()["detail"]["type"] == "not_text"


@pytest.mark.asyncio
async def test_a_page_the_store_cannot_read_now_says_to_retry(client, space, monkeypatch):
    """The bridged page is read whole before it is answered, so a store
    failure there must reach the app as the Drive's own error."""
    _serving(monkeypatch, _PAGE, "text/html")

    async def stream(_found, start=0, end=None):
        raise DriveError("unavailable", "The file's bytes cannot be read now")
        yield b""

    monkeypatch.setattr(drive, "stream", stream)

    resp = await client.get(
        "/api/v1/drive/personal/content", params={"id": _ENTRY, "inject": "comments"}
    )

    assert resp.status_code == 503
    assert resp.json()["detail"]["type"] == "unavailable"
    assert resp.headers["retry-after"] == "2"


@pytest.mark.asyncio
async def test_html_without_inject_comes_as_it_is(client, space, monkeypatch):
    _serving(monkeypatch, _PAGE, "text/html")

    resp = await client.get("/api/v1/drive/personal/content", params={"id": _ENTRY})

    assert resp.content == _PAGE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("found", "kind"),
    [
        (SimpleNamespace(deleted_at=_AT, trash_id="t1"), "in_trash"),
        # Ended by a rename over it: nothing can restore it, so it is gone.
        (SimpleNamespace(deleted_at=_AT, trash_id=None), "not_found"),
        (None, "not_found"),
    ],
)
async def test_an_entry_asked_for_by_id_says_when_it_is_in_the_trash(
    client, space, monkeypatch, found, kind
):
    """A comment's link finds its file by id: one in the trash says so, so the
    chat can tell the owner where it is rather than that it is gone."""
    read = AsyncMock(return_value=found)
    monkeypatch.setattr(drive.reads, "entry_by_id", read)

    resp = await client.get("/api/v1/drive/personal/entry", params={"id": _ENTRY})

    assert resp.status_code == 404
    assert resp.json()["detail"]["type"] == kind
    read.assert_awaited_once_with("s1", _ENTRY)


@pytest.mark.asyncio
async def test_an_entry_asked_for_by_path_never_asks_the_trash(client, space, monkeypatch):
    monkeypatch.setattr(drive.reads, "live_entry", AsyncMock(return_value=None))
    read = AsyncMock()
    monkeypatch.setattr(drive.reads, "entry_by_id", read)

    resp = await client.get("/api/v1/drive/personal/entry", params={"path": "a.md"})

    assert resp.json()["detail"]["type"] == "not_found"
    read.assert_not_awaited()
