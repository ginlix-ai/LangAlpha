"""The Drive's one permission check, and the routes that lean on it."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.server.services import drive
from src.server.services.drive import DriveError, Space
from tests.conftest import create_test_app

USER = "user-1"
NOW = datetime.now(timezone.utc)
MINE = Space("11111111-1111-4111-8111-111111111111", "personal", USER, NOW)
THEIRS = Space("22222222-2222-4222-8222-222222222222", "personal", "user-2", NOW)
READS = "src.server.services.drive.reads"


@pytest.mark.asyncio
async def test_personal_names_the_callers_own_space():
    with patch(f"{READS}.personal_space", AsyncMock(return_value=MINE)) as made:
        assert await drive.reach(USER, "personal") == MINE
    made.assert_awaited_once_with(USER)


@pytest.mark.asyncio
async def test_the_callers_space_by_id():
    with patch(f"{READS}.get_space", AsyncMock(return_value=MINE)):
        assert await drive.reach(USER, MINE.space_id) == MINE


@pytest.mark.parametrize(
    ("ref", "found"),
    [
        (THEIRS.space_id, THEIRS),
        ("33333333-3333-4333-8333-333333333333", None),
        ("not-a-uuid", None),
    ],
)
@pytest.mark.asyncio
async def test_a_space_the_caller_cannot_reach_is_not_found(ref, found):
    """Someone else's space and one that does not exist answer alike."""
    with patch(f"{READS}.get_space", AsyncMock(return_value=found)):
        with pytest.raises(DriveError) as caught:
            await drive.reach(USER, ref)
    assert caught.value.code == "not_found"


def _client(principal):
    from src.server.app.drive import router
    from src.server.dependencies.principal import get_principal

    app = create_test_app(router)
    app.dependency_overrides[get_principal] = lambda: principal
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_every_route_is_not_found_while_the_feature_is_off():
    from src.server.dependencies.principal import Principal

    with (
        patch("src.server.services.drive.enabled", AsyncMock(return_value=False)),
        patch("src.server.services.drive.reach", AsyncMock(return_value=MINE)) as reach,
    ):
        async with _client(Principal(USER, "session")) as c:
            spaces = await c.get("/api/v1/drive/spaces")
            entries = await c.get("/api/v1/drive/personal/entries")
            put = await c.put("/api/v1/drive/personal/content?path=a.md", content=b"x")
    assert [spaces.status_code, entries.status_code, put.status_code] == [404, 404, 404]
    assert entries.json()["detail"]["type"] == "not_found"
    reach.assert_not_awaited()


@pytest.mark.parametrize("auth", ["pat", "oauth"])
@pytest.mark.asyncio
async def test_only_a_signed_in_person_empties_the_trash(auth):
    from src.server.dependencies.principal import Principal

    principal = Principal(USER, auth, scopes=("drive:read", "drive:write"))
    with patch("src.server.services.drive.purge", AsyncMock(return_value=1)) as purge:
        async with _client(principal) as c:
            resp = await c.delete("/api/v1/drive/personal/trash")
    assert resp.status_code == 403
    purge.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_token_without_the_drive_scopes_is_refused():
    from src.server.dependencies.principal import Principal

    principal = Principal(USER, "pat", scopes=("agent:read",))
    async with _client(principal) as c:
        resp = await c.get("/api/v1/drive/personal/entries")
    assert resp.status_code == 403
    assert resp.json()["detail"]["type"] == "insufficient_scope"
