"""The onboarding progress record's routes (``/api/v1/users/me/onboarding``).

The database module is replaced: these hold the request contract (which keys
reach the write, and how), while the SQL's own rules (a first stamp stands)
are pinned in ``tests/integration/test_onboarding_progress_db.py``.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

USER = "test-user-123"
_ROUTES = "src.server.app.users"
_EMPTY = {
    "models": None,
    "runs_on": None,
    "plan_step_done_at": None,
    "this_mac": None,
    "setup_on": None,
    "setup_step": None,
    "setup_completed_at": None,
    "created_at": None,
    "updated_at": None,
    "account_setup_completed_at": None,
}


@pytest_asyncio.fixture
async def client():
    from src.server.app.users import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
def preferences():
    with patch(f"{_ROUTES}.db_get_user_preferences", AsyncMock(return_value=None)) as read:
        yield read


@pytest.fixture
def write():
    now = datetime(2026, 11, 15, tzinfo=timezone.utc)
    row = {**_EMPTY, "models": "claude_code", "created_at": now, "updated_at": now}
    with patch(f"{_ROUTES}.patch_onboarding_progress", AsyncMock(return_value=row)) as written:
        yield written


@pytest.mark.asyncio
async def test_a_user_who_has_not_started_reads_every_field_null(client):
    with patch(f"{_ROUTES}.get_onboarding_progress", AsyncMock(return_value=None)):
        resp = await client.get("/api/v1/users/me/onboarding")

    assert resp.status_code == 200
    assert resp.json() == _EMPTY


@pytest.mark.asyncio
async def test_a_patch_writes_only_the_keys_it_sends(client, write):
    resp = await client.patch(
        "/api/v1/users/me/onboarding", json={"models": "claude_code", "runs_on": "cloud"}
    )

    assert resp.status_code == 200
    assert resp.json()["models"] == "claude_code"
    write.assert_awaited_once_with(USER, {"models": "claude_code", "runs_on": "cloud"})


@pytest.mark.asyncio
async def test_an_explicit_null_reaches_the_write_to_clear_the_field(client, write):
    """"Change my answers" clears them: a null is a write, an absent key is not."""
    resp = await client.patch(
        "/api/v1/users/me/onboarding", json={"models": None, "setup_completed": True}
    )

    assert resp.status_code == 200
    write.assert_awaited_once_with(USER, {"models": None, "setup_completed": True})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"models": "gpt"},
        {"runs_on": "laptop"},
        {"this_mac": "yes"},
        {"setup_on": "phone"},
        {"setup_step": "welcome"},
        {"plan_step_done_at": "2026-11-15T00:00:00Z"},
        {"tier": "x"},
        {"setup_completed": "yes"},
    ],
)
async def test_an_unknown_key_or_value_is_a_422_and_writes_nothing(client, write, body):
    resp = await client.patch("/api/v1/users/me/onboarding", json=body)

    assert resp.status_code == 422
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_user_with_no_row_in_users_is_a_404(client):
    with patch(f"{_ROUTES}.patch_onboarding_progress", AsyncMock(return_value=None)):
        resp = await client.patch("/api/v1/users/me/onboarding", json={"runs_on": "cloud"})

    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stamp, read",
    [
        ("2026-11-15T09:30:00Z", "2026-11-15T09:30:00Z"),
        ("2026-11-15T09:30:00", "2026-11-15T09:30:00Z"),
        ("not a time", None),
        (12, None),
    ],
    ids=["utc", "offset-less", "garbage", "not-a-string"],
)
async def test_the_platforms_account_setup_stamp_is_read_past_the_cache(
    client, preferences, write, stamp, read
):
    """The preferences cache would hide a fresh stamp from the gate waiting on
    it; a PATCH answers it too, since the client keeps the PATCH's answer as
    the record."""
    preferences.return_value = {"other_preference": {"account_setup_completed_at": stamp}}
    with patch(f"{_ROUTES}.get_onboarding_progress", AsyncMock(return_value=None)):
        got = await client.get("/api/v1/users/me/onboarding")
    patched = await client.patch("/api/v1/users/me/onboarding", json={"models": "claude_code"})

    for resp in (got, patched):
        at = resp.json()["account_setup_completed_at"]
        assert (at and at.replace("+00:00", "Z")) == read
    preferences.assert_awaited_with(USER, cached=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["2026-11-15T09:30:00Z", None], ids=["set", "clear"])
async def test_a_preferences_write_cannot_set_or_clear_the_account_setup_stamp(client, value):
    """Read back as the time the hosted onboarding finished, so the user's own
    preferences write would let an unfinished account pass as done, or undo
    one that is."""
    with (
        patch(f"{_ROUTES}.db_get_user", AsyncMock(return_value={"user_id": USER})),
        patch(f"{_ROUTES}.upsert_user_preferences", AsyncMock()) as upsert,
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"account_setup_completed_at": value}},
        )

    assert resp.status_code == 422
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_preferences_reset_keeps_the_account_setup_stamp():
    """A reset clears what the user chose; the onboarding record survives it,
    and so must the stamp it is read beside."""
    from contextlib import asynccontextmanager

    from src.server.database.user import delete_user_preferences

    cursor = AsyncMock()
    cursor.rowcount = 1
    cursor.fetchone = AsyncMock(
        return_value={
            "other_preference": {
                "theme": "dark",
                "account_setup_completed_at": "2026-11-15T09:30:00Z",
                "feature_overrides": {"claude_oauth": True, "market_watch": True},
            }
        }
    )
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor(**_):
        yield cursor

    @asynccontextmanager
    async def _transaction():
        yield

    @asynccontextmanager
    async def _connection():
        yield conn

    conn.cursor, conn.transaction = _cursor, _transaction
    with patch("src.server.database.user.get_db_connection", _connection):
        assert await delete_user_preferences(USER, keep_overrides=["claude_oauth"]) is True

    sql, params = cursor.execute.await_args.args
    assert "INSERT INTO user_preferences" in sql
    assert params[1].obj == {
        "feature_overrides": {"claude_oauth": True},
        "account_setup_completed_at": "2026-11-15T09:30:00Z",
    }
