"""Who a members-only link opens to, on the public routes.

A link's audience is ``organization`` or public. A members-only link opens
for an active member of the organization its owner's account is in, judged
on every request from the owner's account as the auth service names it, a
suspended or removed owner included. Its metadata route hands a member a
grant bound to the link and to them, which every bearerless route takes in
the token's place.

The organizations are modelled at ``member_of``, the one read the rules make,
so ``admits_member`` and the routes run as shipped.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.app.public import router as public_router
from src.server.app.share_access import get_shared_thread, resolve_link
from src.server.database.artifacts import Artifact, Version
from src.server.database.share_links import ShareLink
from src.server.services import member_grants
from src.server.services.account import AccountUnknown
from src.server.services.account_contract import Org
from src.server.services.artifacts import grants as artifact_grants
from src.server.services.share_audience import admits_member
from src.server.utils.api import Viewer, get_viewer
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

TOKEN = "share_abc123xyz"
CODE = "k3Vq9ZtR2mXa"
ARTIFACT = "11111111-2222-4333-8444-555555555555"
WS_ID = "ws-fake-0001"

OWNER = "owner-1"
MEMBER = "member-2"
SUSPENDED = "member-3"
OTHER_ORG = "outsider-4"
PERSONAL = "personal-5"
GONE_OWNER = "owner-6"

ORG = "org-a"
ACCOUNTS: dict[str, Org | None] = {
    OWNER: Org(ORG, "Acme"),
    MEMBER: Org(ORG, "Acme"),
    SUSPENDED: Org(ORG, "Acme", suspended=True),
    OTHER_ORG: Org("org-b", "Beta"),
    PERSONAL: None,
    GONE_OWNER: Org(ORG, "Acme", suspended=True),
}

_AUD = "src.server.services.share_audience"
_ACCESS_THREAD = "src.server.app.share_access.get_thread_by_share_token"
_PUBLIC_THREAD = "src.server.app.public.get_thread_by_share_token"
_ACCESS_LINK = "src.server.app.share_access.get_link"
_ACCESS_WS = "src.server.app.share_access.db_get_workspace"
_ACCESS_PUBLIC_LINKS = "src.server.app.share_access.serves_public_links"
_PUBLIC_PUBLIC_LINKS = "src.server.app.public.serves_public_links"
_ARTIFACTS_DB = "src.server.app.public.artifacts_db"
_PUBLIC_BASE = "src.server.app.public.artifact_base"
_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _pinned_keys(monkeypatch):
    monkeypatch.setattr(member_grants._THREAD, "_key", b"t" * 32)
    monkeypatch.setattr(member_grants._FILE, "_key", b"f" * 32)
    monkeypatch.setattr(artifact_grants._VISITOR, "_key", b"s" * 32)


async def _member_of(user_id: str) -> Org | None:
    if user_id not in ACCOUNTS:
        raise AccountUnknown()
    return ACCOUNTS[user_id]


@pytest.fixture
def door(monkeypatch):
    """The ``teams`` door, open unless a test shuts it; the organizations as
    ``ACCOUNTS`` names them."""
    state = {"open": True}
    monkeypatch.setattr(f"{_AUD}.teams_door_open", lambda: state["open"])
    monkeypatch.setattr(f"{_AUD}.member_of", AsyncMock(side_effect=_member_of))
    monkeypatch.setattr(
        "src.server.services.account.member_of", AsyncMock(side_effect=_member_of)
    )
    return state


@pytest.fixture
def public_links(monkeypatch):
    """The ``public_links`` rule, allowed unless a test turns it off."""
    rule = AsyncMock(return_value=True)
    monkeypatch.setattr(_ACCESS_PUBLIC_LINKS, rule)
    monkeypatch.setattr(_PUBLIC_PUBLIC_LINKS, rule)
    return rule


def _thread(audience: str | None = "organization", owner: str = OWNER) -> dict:
    perms = {"allow_files": True}
    if audience is not None:
        perms["audience"] = audience
    return {
        "conversation_thread_id": "thread-fake-1",
        "workspace_id": WS_ID,
        "share_permissions": perms,
        "owner_id": owner,
        "title": "Q3 notes",
    }


def _file_link(audience: str | None = "organization") -> ShareLink:
    artifact = Artifact(
        artifact_id=ARTIFACT,
        code=CODE,
        workspace_id=WS_ID,
        path="report.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=2,
        shared_at=_AT,
        files=("report.html",),
        revision=4,
        created_at=_AT,
        shared_seq=2,
        audience=audience,
    )
    return ShareLink(
        code=CODE,
        workspace_id=WS_ID,
        kind="file",
        path="report.html",
        port=None,
        title=None,
        files=("report.html",),
        shared_at=_AT,
        created_at=_AT,
        revision=4,
        artifact=artifact,
    )


def _client(viewer: Viewer) -> AsyncClient:
    app = create_test_app(public_router)
    app.dependency_overrides[get_viewer] = lambda: viewer
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# --- whose organization, read on each request --------------------------------


@pytest.mark.parametrize(
    ("viewer", "owner", "admitted"),
    [
        (MEMBER, OWNER, True),
        (OWNER, OWNER, True),
        # A suspended or removed owner's work stays the organization's.
        (MEMBER, GONE_OWNER, True),
        (SUSPENDED, OWNER, False),
        (OTHER_ORG, OWNER, False),
        (PERSONAL, OWNER, False),
        # An owner in no organization has no members.
        (MEMBER, PERSONAL, False),
        (None, OWNER, False),
    ],
)
async def test_an_active_member_of_the_owners_organization_is_admitted(door, viewer, owner, admitted):
    assert await admits_member(viewer, owner) is admitted


async def test_with_the_door_shut_nobody_is_admitted_and_nothing_is_read(door):
    door["open"] = False
    assert await admits_member(MEMBER, OWNER) is False


@pytest.mark.parametrize(("viewer", "owner"), [(MEMBER, "unknown-owner"), ("unknown-viewer", OWNER)])
async def test_an_account_that_cannot_be_read_is_unavailable_not_guessed(door, viewer, owner):
    with pytest.raises(HTTPException) as e:
        await admits_member(viewer, owner)
    assert e.value.status_code == 503


# --- the metadata route: what each viewer is answered ------------------------


@pytest.mark.parametrize(
    ("viewer", "status"),
    [
        (Viewer(None), 401),
        (Viewer(MEMBER), 200),
        (Viewer(OTHER_ORG), 404),
        (Viewer(PERSONAL), 404),
        # A suspended account reads as signed out; signing in opens nothing.
        (Viewer(None, suspended=True), 404),
        (Viewer(SUSPENDED), 404),
        # A credential the keys could not check may be a member's.
        (Viewer(None, unconfirmed=HTTPException(status_code=503, detail="x")), 503),
    ],
)
async def test_a_members_only_thread_link_answers_each_viewer(door, public_links, viewer, status):
    with patch(_PUBLIC_THREAD, AsyncMock(return_value=_thread())):
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{TOKEN}")
    assert resp.status_code == status
    if status == 401:
        assert resp.json()["detail"]["type"] == "members_only"
    if status == 200:
        body = resp.json()
        assert body["audience"] == "organization"
        assert member_grants.is_thread_grant(body["grant"])
        assert 0 < body["expires_in"] <= member_grants.TTL_SECONDS


@pytest.mark.parametrize(
    ("viewer", "status"),
    [
        (Viewer(None), 401),
        (Viewer(MEMBER), 200),
        (Viewer(OTHER_ORG), 404),
        (Viewer(None, suspended=True), 404),
        (Viewer(SUSPENDED), 404),
    ],
)
async def test_a_members_only_file_link_answers_each_viewer(door, public_links, viewer, status):
    versions = MagicMock()
    versions.get_version = AsyncMock(
        return_value=Version(
            seq=2, entry_path="report.html", origin="owner", title="Q3",
            label=None, byte_len=100, published_at=_AT,
        )
    )
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_file_link())),
        patch(_ACCESS_WS, AsyncMock(return_value={"workspace_id": WS_ID, "user_id": OWNER})),
        patch(_ARTIFACTS_DB, versions),
        patch(_PUBLIC_BASE, return_value=None),
    ):
        async with _client(viewer) as client:
            resp = await client.get(f"/api/v1/public/shared/{CODE}")
    assert resp.status_code == status
    if status == 200:
        body = resp.json()
        assert body["audience"] == "organization"
        assert any(member_grants.is_file_grant(part) for part in body["frame_base"].split("/"))


async def test_with_the_door_shut_a_members_link_is_not_found_even_signed_out(door, public_links):
    door["open"] = False
    with patch(_PUBLIC_THREAD, AsyncMock(return_value=_thread())):
        async with _client(Viewer(None)) as client:
            resp = await client.get(f"/api/v1/public/shared/{TOKEN}")
    assert resp.status_code == 404


# --- the grant, judged again on every bearerless request ---------------------


async def _grant(viewer: str = MEMBER, token: str = TOKEN) -> str:
    return (await member_grants.mint_thread_grant(token, viewer)).token


async def test_a_members_grant_opens_the_thread_and_the_token_alone_does_not(door, public_links):
    with patch(_ACCESS_THREAD, AsyncMock(return_value=_thread())) as read:
        assert (await get_shared_thread(await _grant()))["owner_id"] == OWNER
        read.assert_awaited_with(TOKEN)
        with pytest.raises(HTTPException) as e:
            await get_shared_thread(TOKEN)
    assert e.value.status_code == 404


async def test_a_grant_for_another_viewer_or_a_forged_one_opens_nothing(door, public_links):
    outsider = await _grant(OTHER_ORG)
    forged = (await _grant())[:-2] + "AA"
    with patch(_ACCESS_THREAD, AsyncMock(return_value=_thread())):
        for grant in (outsider, forged):
            with pytest.raises(HTTPException) as e:
                await get_shared_thread(grant)
            assert e.value.status_code == 404


async def test_a_grant_ends_once_the_member_is_suspended(door, public_links, monkeypatch):
    grant = await _grant()
    monkeypatch.setitem(ACCOUNTS, MEMBER, Org(ORG, "Acme", suspended=True))
    with patch(_ACCESS_THREAD, AsyncMock(return_value=_thread())):
        with pytest.raises(HTTPException) as e:
            await get_shared_thread(grant)
    assert e.value.status_code == 404


@pytest.mark.parametrize("change", ["public", "stopped"])
async def test_flipping_the_audience_or_stopping_the_share_ends_the_grant(door, public_links, change):
    grant = await _grant()
    thread = None if change == "stopped" else _thread(audience="public")
    with patch(_ACCESS_THREAD, AsyncMock(return_value=thread)):
        with pytest.raises(HTTPException) as e:
            await get_shared_thread(grant)
    assert e.value.status_code == 404


# --- public_links governs only the public audience ---------------------------


async def test_public_links_off_refuses_a_public_link_and_serves_a_members_one(door, public_links):
    public_links.return_value = False
    with patch(_ACCESS_THREAD, AsyncMock(return_value=_thread(audience=None))):
        with pytest.raises(HTTPException) as e:
            await get_shared_thread(TOKEN)
    assert e.value.status_code == 404
    with patch(_ACCESS_THREAD, AsyncMock(return_value=_thread())):
        assert (await get_shared_thread(await _grant()))["owner_id"] == OWNER

    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_file_link())),
        patch(_ACCESS_WS, AsyncMock(return_value={"workspace_id": WS_ID, "user_id": OWNER})),
    ):
        access = await resolve_link(CODE, user_id=MEMBER)
    assert access is not None and access.member
    with (
        patch(_ACCESS_LINK, AsyncMock(return_value=_file_link(audience=None))),
        patch(_ACCESS_WS, AsyncMock(return_value={"workspace_id": WS_ID, "user_id": OWNER})),
    ):
        with pytest.raises(HTTPException) as e:
            await resolve_link(CODE, user_id=MEMBER)
    assert e.value.status_code == 404


# --- a file's frames under a member's grant ----------------------------------


_PROXY_FILE = "src.server.preview_proxy.artifacts.get_shared_version_file"


async def _file_grant(viewer: str = MEMBER, *, seq: int = 2) -> str:
    grant = await member_grants.mint_file_grant(
        CODE, ARTIFACT, revision=4, seq=seq, viewer_id=viewer
    )
    return grant.token


@pytest.mark.parametrize(
    ("viewer", "seq", "host", "served"),
    [
        (MEMBER, 2, ARTIFACT, True),
        (OTHER_ORG, 2, ARTIFACT, False),
        (SUSPENDED, 2, ARTIFACT, False),
        # A grant names one artifact and one version, and opens no other.
        (MEMBER, 1, ARTIFACT, False),
        (MEMBER, 2, "99999999-2222-4333-8444-555555555555", False),
    ],
)
async def test_a_members_file_grant_opens_its_own_version_for_a_member(door, viewer, seq, host, served):
    from src.server.database.artifacts.versions import StoredVersionFile
    from src.server.preview_proxy.artifacts import _member_file

    stored = StoredVersionFile(OWNER, "0" * 64, 1, "text/html", b"x", owner_id=OWNER)
    with patch(_PROXY_FILE, AsyncMock(return_value=stored)) as read:
        got = await _member_file(await _file_grant(viewer), seq, "report.html", host)
    assert (got is stored) is served
    if served:
        # Read as a members-only link, so a link set back to public stops it.
        assert read.await_args.kwargs["members"] is True
