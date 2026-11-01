"""What a visitor's grant opens of a shared artifact, fetch by fetch.

A visitor's grant names one version and the link's revision. Its signature
only proves the server minted it; whether the link still opens that version
is read from the link's row on every fetch, so an unshare or an Update holds
on the next request on every worker, and an older version is never served
through a share, whatever the URL names. An owner's grant, which opens every
version, is never read as a visitor's, nor the other way round.
"""

from __future__ import annotations

import inspect
import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import StoredVersionFile
from src.server.database.artifacts import versions as artifact_versions
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.preview_proxy.page_bridge import bridge_for, with_bridge
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import (
    ArtifactGrantError,
    mint_artifact_grant,
    mint_artifact_share_grant,
    verify_artifact_grant,
    verify_artifact_share_grant,
)

CODE = "k3Vq9ZtR2mXa"
ARTIFACT = "11111111-2222-4333-8444-555555555555"
OTHER_ARTIFACT = "99999999-2222-4333-8444-555555555555"
HTML = b"<!doctype html><html><head><title>t</title></head><body>v11</body></html>"
_STORED = StoredVersionFile(
    user_id="owner-1",
    sha256="a" * 64,
    byte_len=len(HTML),
    content_type="text/html",
    content=HTML,
)


@pytest.fixture(autouse=True)
def _keys(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(grants._VISITOR, "_key", b"s" * 32)


@pytest.fixture
def shared_file(monkeypatch):
    """The link row's answer, as ``get_shared_version_file`` reads it."""
    lookup = AsyncMock(return_value=_STORED)
    monkeypatch.setattr(artifact_proxy, "get_shared_version_file", lookup)
    owner_lookup = AsyncMock(side_effect=AssertionError("read as an owner's grant"))
    monkeypatch.setattr(artifact_proxy, "get_version_file", owner_lookup)
    return lookup


async def _visitor_grant(*, seq: int = 11, revision: int = 3) -> str:
    grant = await mint_artifact_share_grant(CODE, ARTIFACT, revision=revision, seq=seq)
    return grant.token


async def _load(token: str, seq: str = "11", path: str = "index.html", **kwargs):
    kwargs.setdefault("host_artifact_id", ARTIFACT)
    return await artifact_proxy.load_artifact_file(token, seq, path, **kwargs)


# --- The revoke re-check -----------------------------------------------------


@pytest.mark.asyncio
async def test_every_fetch_asks_the_link_row_again(shared_file):
    token = await _visitor_grant()

    first = await _load(token)
    shared_file.return_value = None
    second = await _load(token)

    assert first is not None and first.status == 200
    assert second is None
    assert shared_file.await_count == 2
    for call in shared_file.await_args_list:
        assert call.kwargs == {
            "code": CODE,
            "artifact_id": ARTIFACT,
            "revision": 3,
            "seq": 11,
            "path": "index.html",
        }


@pytest.mark.asyncio
async def test_an_ended_grant_answers_no_304_for_a_file_the_browser_kept(shared_file):
    token = await _visitor_grant()
    served = await _load(token)
    etag = dict(served.headers)["etag"]

    shared_file.return_value = None
    again = await _load(token, if_none_match=etag)

    assert again is None


@pytest.mark.asyncio
async def test_a_visitor_file_is_never_held_without_asking(shared_file):
    token = await _visitor_grant()
    for path in ("index.html", "data.json"):
        served = await _load(token, path=path)
        assert dict(served.headers)["cache-control"] == "private, no-cache"


def test_the_row_is_read_as_one_statement_that_holds_every_condition():
    source = re.sub(r"\s+", " ", inspect.getsource(artifacts_db.get_shared_version_file))
    for condition in (
        "w.status <> 'deleted'",
        "f.artifact_id = l.artifact_id AND f.seq = l.latest_seq",
        "l.code = %(code)s",
        "l.kind = 'file'",
        "l.artifact_id = %(artifact_id)s",
        "l.shared_at IS NOT NULL",
        "l.revision = %(revision)s",
        "l.latest_seq = %(seq)s",
    ):
        assert condition in source, condition


@pytest.mark.asyncio
async def test_the_row_query_gets_what_the_grant_claims(
    mock_connection, mock_cursor, monkeypatch
):
    @asynccontextmanager
    async def _conn(conn=None):
        yield mock_connection

    monkeypatch.setattr(artifact_versions, "get_db_connection", _conn)
    mock_cursor.fetchone.return_value = None

    found = await artifacts_db.get_shared_version_file(
        code=CODE, artifact_id=ARTIFACT, revision=3, seq=11, path="index.html"
    )

    assert found is None
    params = mock_cursor.execute.await_args.args[1]
    assert params == {
        "code": CODE,
        "artifact_id": ARTIFACT,
        "revision": 3,
        "seq": 11,
        "path": "index.html",
    }


# --- Older versions, other artifacts, the owner's grant -----------------------


@pytest.mark.asyncio
async def test_a_visitor_grant_opens_no_other_version(shared_file):
    token = await _visitor_grant(seq=11)

    for seq in ("9", "10", "12"):
        assert await _load(token, seq=seq) is None
    shared_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_grant_edited_to_name_an_older_version_does_not_verify(shared_file):
    parts = (await _visitor_grant(seq=11)).split(".")
    parts[4] = "9"
    forged = ".".join(parts)

    with pytest.raises(ArtifactGrantError):
        await verify_artifact_share_grant(forged)
    assert await _load(forged, seq="9") is None
    shared_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_visitor_grant_opens_no_other_artifact_host(shared_file):
    token = await _visitor_grant()

    assert await _load(token, host_artifact_id=OTHER_ARTIFACT) is None
    shared_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_two_grants_are_never_read_as_each_other():
    owner = (await mint_artifact_grant(ARTIFACT)).token
    visitor = await _visitor_grant()

    with pytest.raises(ArtifactGrantError):
        await verify_artifact_share_grant(owner)
    with pytest.raises(ArtifactGrantError):
        await verify_artifact_grant(visitor)


@pytest.mark.asyncio
async def test_a_visitor_grant_signed_with_the_owners_key_does_not_verify(monkeypatch):
    monkeypatch.setattr(grants._VISITOR, "_key", b"o" * 32)
    signed_as_owner = await _visitor_grant()
    monkeypatch.setattr(grants._VISITOR, "_key", b"s" * 32)

    with pytest.raises(ArtifactGrantError):
        await verify_artifact_share_grant(signed_as_owner)


@pytest.mark.asyncio
async def test_every_version_is_read_only_and_the_live_view_takes_comments(
    shared_file, monkeypatch
):
    visitor = await _load(await _visitor_grant())
    monkeypatch.setattr(
        artifact_proxy, "get_version_file", AsyncMock(return_value=_STORED)
    )
    owner = await _load((await mint_artifact_grant(ARTIFACT)).token, seq="9")
    view, comments = bridge_for("view"), bridge_for("comments")
    live = with_bridge(HTML, comments)

    for version in (visitor, owner):
        assert view.script in version.body
        assert b"page:comment-mode" not in version.body
        # The bridge is the head's first script, ahead of the document's title.
        assert version.body.index(view.script) < version.body.index(b"<title>")
    assert comments.script in live
    assert b"page:comment-mode" in live
