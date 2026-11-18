"""A shared workspace member's artifact grant ends when they stop reaching it.

The grant a member is minted names them, and every fetch reads their access
to the workspace again: a removal or a suspension holds on the next request,
on every version, however long the grant had left.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.artifacts import StoredVersionFile
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.services import account
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import (
    ArtifactGrantError,
    mint_artifact_grant,
    mint_artifact_member_grant,
    verify_artifact_grant,
    verify_artifact_member_grant,
)
from src.server.services.shared_workspaces.access import Access, NoAccess

ARTIFACT = "11111111-2222-4333-8444-555555555555"
OTHER_ARTIFACT = "99999999-2222-4333-8444-555555555555"
WORKSPACE = "aaaaaaaa-2222-4333-8444-555555555555"
HOLDER = "holder-1"
MEMBER = "member-1"
_STORED = StoredVersionFile(
    user_id=HOLDER,
    sha256="a" * 64,
    byte_len=4,
    content_type="application/json",
    content=b"{}\n\n",
)


@pytest.fixture(autouse=True)
def _keys(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(grants._MEMBER, "_key", b"o" * 32)


@pytest.fixture
def reach(monkeypatch):
    """The member's access, as ``workspace_access`` answers it; set
    ``state["raise"]`` to answer a removal or an unreadable account."""
    state: dict = {"raise": None, "calls": []}

    async def access(workspace, caller):
        state["calls"].append((workspace["workspace_id"], caller))
        if state["raise"] is not None:
            raise state["raise"]
        return Access(HOLDER, SimpleNamespace())

    monkeypatch.setattr(artifact_proxy, "workspace_access", access)
    monkeypatch.setattr(
        artifact_proxy, "artifact_workspace_id", AsyncMock(return_value=WORKSPACE)
    )
    monkeypatch.setattr(
        artifact_proxy,
        "get_workspace",
        AsyncMock(return_value={"workspace_id": WORKSPACE, "user_id": HOLDER}),
    )
    monkeypatch.setattr(
        artifact_proxy, "get_version_file", AsyncMock(return_value=_STORED)
    )
    return state


async def _load(token: str, seq: str = "3", **kwargs):
    kwargs.setdefault("host_artifact_id", ARTIFACT)
    return await artifact_proxy.load_artifact_file(token, seq, "data.json", **kwargs)


async def _member_token() -> str:
    return (await mint_artifact_member_grant(ARTIFACT, MEMBER)).token


@pytest.mark.asyncio
async def test_a_member_opens_every_version_while_they_reach_the_workspace(reach):
    token = await _member_token()

    for seq in ("1", "3", "7"):
        served = await _load(token, seq=seq)
        assert served is not None and served.status == 200
    assert reach["calls"] == [(WORKSPACE, MEMBER)] * 3


@pytest.mark.parametrize(
    "ended",
    [
        NoAccess(status_code=404, detail="Workspace not found"),
        account.AccountSuspended(status_code=403, detail={"message": "suspended"}),
    ],
    ids=["removed", "suspended"],
)
@pytest.mark.asyncio
async def test_a_removed_or_suspended_member_is_refused_on_the_next_fetch(reach, ended):
    token = await _member_token()
    assert await _load(token) is not None

    reach["raise"] = ended

    assert await _load(token) is None
    assert await _load(token, seq="9") is None


@pytest.mark.asyncio
async def test_unreadable_access_is_unavailable_never_guessed(reach):
    reach["raise"] = account.AccountUnknown()

    with pytest.raises(artifact_proxy.ArtifactFileUnavailable):
        await _load(await _member_token())


@pytest.mark.asyncio
async def test_a_member_file_is_asked_for_again_each_time(reach):
    served = await _load(await _member_token())

    assert dict(served.headers)["cache-control"] == "private, no-cache"


@pytest.mark.asyncio
async def test_a_member_grant_opens_no_other_artifact_host(reach):
    assert await _load(await _member_token(), host_artifact_id=OTHER_ARTIFACT) is None
    assert reach["calls"] == []


@pytest.mark.asyncio
async def test_the_owner_and_member_grants_are_never_read_as_each_other():
    owner = (await mint_artifact_grant(ARTIFACT)).token
    member = await _member_token()

    with pytest.raises(ArtifactGrantError):
        await verify_artifact_member_grant(owner)
    with pytest.raises(ArtifactGrantError):
        await verify_artifact_grant(member)
    # The viewer cannot be swapped: it is signed.
    parts = member.split(".")
    parts[2] = "someone-else"
    with pytest.raises(ArtifactGrantError):
        await verify_artifact_member_grant(".".join(parts))


@pytest.mark.asyncio
async def test_a_member_is_minted_a_grant_bound_to_them(monkeypatch):
    from src.server.app.artifacts import records

    artifact = SimpleNamespace(artifact_id=ARTIFACT)
    monkeypatch.setattr(
        records,
        "reached",
        AsyncMock(return_value=(artifact, Access(HOLDER, SimpleNamespace()))),
    )
    monkeypatch.setattr(records, "artifact_base", lambda: None)

    answer = await records.mint_grant(ARTIFACT, MEMBER)

    token = answer.base.removeprefix(grants.ARTIFACT_GRANT_ROUTE).rstrip("/")
    assert await verify_artifact_member_grant(token) == (ARTIFACT, MEMBER)
    assert answer.expires_in <= grants.MEMBER_TTL_SECONDS


@pytest.mark.asyncio
async def test_the_owner_is_minted_the_owners_grant(monkeypatch):
    from src.server.app.artifacts import records

    artifact = SimpleNamespace(artifact_id=ARTIFACT)
    monkeypatch.setattr(
        records, "reached", AsyncMock(return_value=(artifact, Access(HOLDER)))
    )
    monkeypatch.setattr(records, "artifact_base", lambda: None)

    answer = await records.mint_grant(ARTIFACT, HOLDER)

    token = answer.base.removeprefix(grants.ARTIFACT_GRANT_ROUTE).rstrip("/")
    assert await verify_artifact_grant(token) == ARTIFACT

