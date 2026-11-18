"""A member's file grant and preview capability are bound to that member.

Several people reach a shared workspace's files, so what one of them is
handed names them and is judged against their access on every request. An
owner's own grant and capability keep the shape they had.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.services import file_grants, preview_capabilities
from src.server.services.account import AccountUnknown
from src.server.services.file_grants import FileGrantError
from src.server.services.shared_workspaces.access import Access, NoAccess

WS = "9b358bb2-f75c-45a4-b7e5-20123de3f8aa"
B = "b7000000-0000-4000-8000-000000000005"
X = "c7000000-0000-4000-8000-000000000001"


@pytest.fixture(autouse=True)
def _keys():
    with patch(
        "src.server.services.signed_grants.get_server_key",
        AsyncMock(side_effect=lambda name: name.encode() * 4),
    ):
        for codec in (file_grants._CODEC, file_grants._SHARED):
            codec._key = None
        yield
        for codec in (file_grants._CODEC, file_grants._SHARED):
            codec._key = None


@pytest.mark.asyncio
async def test_an_owners_grant_keeps_its_shape_and_names_no_viewer():
    grant = await file_grants.mint_file_grant(WS)
    assert grant.token.startswith(f"v1.{WS}.") and grant.token.count(".") == 3
    assert await file_grants.verify_grant_viewer(grant.token) == (WS, None)
    assert await file_grants.verify_file_grant(grant.token) == WS


@pytest.mark.asyncio
async def test_a_members_grant_names_them_and_lasts_an_hour():
    grant = await file_grants.mint_shared_file_grant(WS, B, now=1_000)
    assert grant.token.startswith(f"w1.{WS}.{B}.")
    assert grant.expires_at == 1_000 + 3600
    assert await file_grants.verify_grant_viewer(grant.token, now=1_001) == (WS, B)


@pytest.mark.asyncio
async def test_a_members_grant_rewritten_for_someone_else_opens_nothing():
    parts = (await file_grants.mint_shared_file_grant(WS, B)).token.split(".")
    parts[2] = X
    with pytest.raises(FileGrantError):
        await file_grants.verify_grant_viewer(".".join(parts))


@pytest.mark.asyncio
async def test_a_narrowed_grant_keeps_its_viewer():
    grant = await file_grants.mint_shared_file_grant(WS, B)
    narrowed = await file_grants.narrowed_file_grant(grant.token, ttl=60)
    assert narrowed.viewer_id == B and narrowed.token.startswith("w1.")
    assert narrowed.expires_at <= grant.expires_at


async def _serve_check(access):
    from src.server.services.shared_workspaces.access import member_or_404

    return await member_or_404(access({"workspace_id": WS, "user_id": "shared-1"}, B))


@pytest.mark.asyncio
async def test_a_members_grant_serves_only_while_they_may_use_the_workspace():
    ok = AsyncMock(return_value=Access("shared-1", shared={"workspace_id": WS}))
    assert (await _serve_check(ok)).shared["workspace_id"] == WS
    for refused in (403, 404):
        gone = AsyncMock(side_effect=NoAccess(status_code=refused))
        with pytest.raises(HTTPException) as e:
            await _serve_check(gone)
        assert e.value.status_code == 404
    # Unknown stays unknown, never a 404 that reads as gone.
    with pytest.raises(HTTPException) as e:
        await _serve_check(AsyncMock(side_effect=AccountUnknown()))
    assert e.value.status_code == 503
    # A grant bound to a viewer never opens a personal workspace.
    personal = AsyncMock(return_value=Access(B))
    with pytest.raises(HTTPException) as e:
        await _serve_check(personal)
    assert e.value.status_code == 404


def _capability(viewer_id=None):
    return preview_capabilities.Capability(
        sandbox_id="sb-1", port=3000, upstream=None, workspace_id=WS,
        generation=7, expires_at=2_000_000_000, owner_id=None, viewer_id=viewer_id,
    )


def test_an_owners_capability_record_is_unchanged():
    record = _capability().to_record()
    assert "viewer_id" not in record
    assert preview_capabilities.Capability.from_record(record) == _capability()


def test_a_members_capability_record_keeps_the_viewer():
    record = _capability(B).to_record()
    assert record["viewer_id"] == B
    assert preview_capabilities.Capability.from_record(record).viewer_id == B


@pytest.mark.asyncio
async def test_a_members_capability_serves_only_while_they_may_use_the_workspace():
    cap = _capability(B)
    with (
        patch.object(preview_capabilities, "is_current", AsyncMock(return_value=True)),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value={})),
    ):
        with patch(
            "src.server.services.preview_capabilities.workspace_access",
            AsyncMock(return_value=Access("shared-1", shared={})),
        ):
            assert await preview_capabilities.serves(cap) is True
        with patch(
            "src.server.services.preview_capabilities.workspace_access",
            AsyncMock(side_effect=NoAccess(status_code=403)),
        ):
            assert await preview_capabilities.serves(cap) is False
        with patch(
            "src.server.services.preview_capabilities.workspace_access",
            AsyncMock(side_effect=AccountUnknown()),
        ), pytest.raises(preview_capabilities.PreviewProxyUnavailable):
            await preview_capabilities.serves(cap)


def test_a_members_signed_url_is_cached_for_them_alone():
    from src.server.services.preview_links import _preview_cache_key

    assert _preview_cache_key("sb", 3000) == "preview:signed_url:sb:3000"
    assert _preview_cache_key("sb", 3000, B) == f"preview:signed_url:sb:3000:{B}"


def test_a_personal_thread_listing_has_no_shared_block():
    from datetime import datetime, timezone

    from src.server.models.conversation import (
        SharedThreadInfo,
        ThreadSender,
        WorkspaceThreadListItem,
    )

    at = datetime(2026, 11, 15, tzinfo=timezone.utc)
    base = dict(
        thread_id="t", workspace_id=WS, thread_index=0, current_status="completed",
        created_at=at, updated_at=at,
    )
    personal = WorkspaceThreadListItem(**base).model_dump()
    assert "shared" not in personal
    sender = ThreadSender(user_id=B, name="Lee")
    shared = WorkspaceThreadListItem(
        **base, shared=SharedThreadInfo(created_by=sender, last_sender=sender)
    ).model_dump()
    assert shared["shared"]["created_by"]["user_id"] == B
    assert {k: v for k, v in shared.items() if k != "shared"} == personal
