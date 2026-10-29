"""
Tests for the Users API router (src/server/app/users.py).

Covers user CRUD, preferences CRUD, delete-preferences (reset onboarding),
and platform access tier from the platform service.
The auth-sync endpoint is NOT tested here because it depends on
get_current_auth_info (a different dependency not overridden in create_test_app).
"""

import uuid
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
PREF_ID = str(uuid.uuid4())


def _user(user_id="test-user-123", **overrides):
    data = {
        "user_id": user_id,
        "email": "test@example.com",
        "name": "Test User",
        "avatar_url": None,
        "timezone": "America/New_York",
        "locale": "en-US",
        "onboarding_completed": False,
        "personalization_completed": False,
        "has_api_key": False,
        "has_oauth_token": False,
        "auth_provider": "google",
        "created_at": NOW,
        "updated_at": NOW,
        "last_login_at": None,
    }
    data.update(overrides)
    return data


def _prefs(user_id="test-user-123"):
    return {
        "user_preference_id": PREF_ID,
        "user_id": user_id,
        "risk_preference": {"risk_tolerance": "moderate"},
        "investment_preference": {},
        "agent_preference": {},
        "other_preference": {},
        "created_at": NOW,
        "updated_at": NOW,
    }


@pytest_asyncio.fixture
async def client():
    from src.server.app.users import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


DB = "src.server.app.users"


@pytest.fixture(autouse=True)
def _preferences_transaction():
    """The preferences PUT writes in one transaction; each test stubs the
    statements in it, so the connection only has to open and close."""
    conn = SimpleNamespace(transaction=nullcontext)
    with (
        patch(f"{DB}.get_db_connection", lambda: nullcontext(conn)),
        patch(f"{DB}.db_lock_user_preferences", new_callable=AsyncMock, return_value=None),
    ):
        yield


# ---------------------------------------------------------------------------
# POST /api/v1/users — create user
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_user(client):
    user = _user()
    with patch(
        f"{DB}.db_create_user",
        new_callable=AsyncMock,
        return_value=user,
    ):
        resp = await client.post(
            "/api/v1/users",
            json={"email": "test@example.com", "name": "Test User"},
        )

    assert resp.status_code == 201
    assert resp.json()["user_id"] == "test-user-123"


@pytest.mark.asyncio
async def test_create_user_duplicate_409(client):
    with patch(
        f"{DB}.db_create_user",
        new_callable=AsyncMock,
        side_effect=ValueError("User already exists"),
    ):
        resp = await client.post(
            "/api/v1/users",
            json={"email": "test@example.com"},
        )

    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# GET /api/v1/users/me — get current user
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_current_user(client):
    result = {"user": _user(), "preferences": _prefs()}
    # The profile, not the plan: an .env in platform mode would ask the platform.
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch("src.server.dependencies.usage_limits.HOST_MODE", "oss"),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    body = resp.json()
    assert body["user"]["user_id"] == "test-user-123"
    assert body["preferences"] is not None


@pytest.mark.asyncio
async def test_get_current_user_no_preferences(client):
    result = {"user": _user(), "preferences": None}
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch("src.server.dependencies.usage_limits.HOST_MODE", "oss"),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    assert resp.json()["preferences"] is None


@pytest.mark.asyncio
async def test_get_current_user_not_found(client):
    with patch(
        f"{DB}.get_user_with_preferences",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me — update current user
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_current_user(client):
    user = _user()
    updated = {**user, "name": "New Name"}
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.db_update_user",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(
            f"{DB}.db_get_user_preferences",
            new_callable=AsyncMock,
            return_value=_prefs(),
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me",
            json={"name": "New Name"},
        )

    assert resp.status_code == 200
    assert resp.json()["user"]["name"] == "New Name"


@pytest.mark.asyncio
async def test_update_current_user_not_found(client):
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.put(
            "/api/v1/users/me",
            json={"name": "X"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_current_user_db_returns_none(client):
    user = _user()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.db_update_user",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me",
            json={"name": "X"},
        )

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/preferences
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_preferences(client):
    user = _user()
    prefs = _prefs()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.db_get_user_preferences",
            new_callable=AsyncMock,
            return_value=prefs,
        ),
    ):
        resp = await client.get("/api/v1/users/me/preferences")

    assert resp.status_code == 200
    assert resp.json()["user_id"] == "test-user-123"


@pytest.mark.asyncio
async def test_get_preferences_user_not_found(client):
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get("/api/v1/users/me/preferences")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_preferences_prefs_not_found(client):
    user = _user()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.db_get_user_preferences",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.get("/api/v1/users/me/preferences")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/preferences
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_preferences(client):
    user = _user()
    prefs = _prefs()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.upsert_user_preferences",
            new_callable=AsyncMock,
            return_value=prefs,
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "risk_preference": {"risk_tolerance": "moderate"},
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["user_id"] == "test-user-123"


@pytest.mark.asyncio
async def test_update_preferences_user_not_found(client):
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_value", ["not-an-engine", {"engine": "serper"}, 123])
async def test_update_preferences_invalid_search_provider_rejected(client, bad_value):
    """Unknown engine strings and non-string values both get a clean 400."""
    user = _user()
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=user,
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"search_provider": bad_value}},
        )

    assert resp.status_code == 400
    assert "search_provider" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["serper", None])
async def test_update_preferences_search_provider_valid_or_none_accepted(client, value):
    """A valid engine is accepted; None (key deletion) passes validation."""
    user = _user()
    prefs = _prefs()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.upsert_user_preferences",
            new_callable=AsyncMock,
            return_value=prefs,
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"search_provider": value}},
        )

    assert resp.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_value", ["not-a-depth", {"level": "deep"}, 123])
async def test_update_preferences_invalid_search_depth_rejected(client, bad_value):
    """Unknown level names and non-string values both get a clean 400."""
    user = _user()
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=user,
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"search_depth": bad_value}},
        )

    assert resp.status_code == 400
    assert "search_depth" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_preferences_depth_must_belong_to_payload_provider(client):
    """When the payload also sets a provider, the depth must be one of THAT
    provider's levels — 'deep' is a tavily level, not a serper one."""
    user = _user()
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=user,
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"search_provider": "serper", "search_depth": "deep"}},
        )

    assert resp.status_code == 400
    assert "search_depth" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        # Provider + matching depth.
        {"search_provider": "tavily", "search_depth": "deep"},
        # No provider in payload → any provider's level passes shape validation
        # (the resolve-time gate checks it against the effective provider).
        {"search_depth": "deep"},
        # None = key deletion.
        {"search_depth": None},
    ],
)
async def test_update_preferences_valid_search_depth_accepted(client, payload):
    user = _user()
    prefs = _prefs()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.upsert_user_preferences",
            new_callable=AsyncMock,
            return_value=prefs,
        ),
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": payload},
        )

    assert resp.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["auto_approve_handoffs", "auto_approve_workspace_creation"])
@pytest.mark.parametrize("bad_value", ["true", 1, 0, {"on": True}])
async def test_update_preferences_non_boolean_auto_approve_rejected(client, key, bad_value):
    """A setting that skips approval cards is refused unless it is a real
    boolean, rather than guessed from "true", 1 or 0."""
    with patch(
        f"{DB}.upsert_user_preferences",
        new_callable=AsyncMock,
    ) as mock_upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {key: bad_value}},
        )

    assert resp.status_code == 422
    locs = [error["loc"] for error in resp.json()["detail"]]
    assert any(key in loc for loc in locs)
    mock_upsert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["auto_approve_handoffs", "auto_approve_workspace_creation"])
@pytest.mark.parametrize("value", [True, False, None])
async def test_update_preferences_auto_approve_boolean_or_none_accepted(client, key, value):
    """``None`` must reach the merge as ``{key: None}``, which deletes the
    key: turning a setting off goes back to asking."""
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=_user(),
        ),
        patch(
            f"{DB}.upsert_user_preferences",
            new_callable=AsyncMock,
            return_value=_prefs(),
        ) as mock_upsert,
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {key: value}},
        )

    assert resp.status_code == 200
    assert mock_upsert.await_args.kwargs["other_preference"] == {key: value}


@pytest.mark.asyncio
async def test_update_preferences_unrelated_other_preference_keys_untouched(client):
    """A patch that never mentions the auto-approve keys must not leak them in
    as ``None`` alongside the key the client actually sent, or it would delete
    an auto-approve setting the client never touched."""
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=_user(),
        ),
        patch(
            f"{DB}.upsert_user_preferences",
            new_callable=AsyncMock,
            return_value=_prefs(),
        ) as mock_upsert,
        patch(
            f"{DB}.maybe_complete_onboarding",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"voice_input_enabled": True}},
        )

    assert resp.status_code == 200
    assert mock_upsert.await_args.kwargs["other_preference"] == {"voice_input_enabled": True}


# ---------------------------------------------------------------------------
# DELETE /api/v1/users/me/preferences
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_preferences(client):
    user = _user()
    with (
        patch(
            f"{DB}.db_get_user",
            new_callable=AsyncMock,
            return_value=user,
        ),
        patch(
            f"{DB}.db_delete_user_preferences",
            new_callable=AsyncMock,
        ),
        patch(
            f"{DB}.db_update_user",
            new_callable=AsyncMock,
        ),
    ):
        resp = await client.delete("/api/v1/users/me/preferences")

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True


@pytest.mark.asyncio
async def test_delete_preferences_user_not_found(client):
    with patch(
        f"{DB}.db_get_user",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.delete("/api/v1/users/me/preferences")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/users/me — access_tier from platform
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_user_access_tier_has_access(client):
    """When platform returns access_tier=0, user response reflects it."""
    result = {"user": _user(), "preferences": _prefs()}
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch(
            "src.server.dependencies.usage_limits._fetch_platform_membership",
            new_callable=AsyncMock,
            return_value={"access_tier": 0, "plan_display_name": "Pro"},
        ),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    assert resp.json()["user"]["access_tier"] == 0
    assert resp.json()["user"]["plan_display_name"] == "Pro"


@pytest.mark.asyncio
async def test_get_user_access_tier_no_access(client):
    """When AUTH_SERVICE_URL is unset, access_tier defaults to -1."""
    result = {"user": _user(), "preferences": _prefs()}
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch(
            "src.server.dependencies.usage_limits._fetch_platform_membership",
            new_callable=AsyncMock,
            return_value={"access_tier": -1, "plan_display_name": None},
        ),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    assert resp.json()["user"]["access_tier"] == -1
    assert resp.json()["user"]["plan_display_name"] is None


@pytest.mark.asyncio
async def test_get_user_unknown_membership_is_503_not_no_access(client):
    """The web sends a user whose tier reads -1 to the setup wizard, so a tier
    nobody could read answers 503 with the credit gate's shape instead."""
    result = {"user": _user(), "preferences": _prefs()}
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch(
            "src.server.dependencies.usage_limits._fetch_platform_membership",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "15"
    assert resp.json()["detail"]["type"] == "service_unavailable"


@pytest.mark.asyncio
async def test_get_user_refresh_tier_asks_past_the_cache(client):
    result = {"user": _user(), "preferences": _prefs()}
    fetch = AsyncMock(return_value={"access_tier": 1, "plan_display_name": "Plus"})
    with (
        patch(
            f"{DB}.get_user_with_preferences",
            new_callable=AsyncMock,
            return_value=result,
        ),
        patch("src.server.dependencies.usage_limits._fetch_platform_membership", new=fetch),
    ):
        resp = await client.get("/api/v1/users/me?refresh_tier=true")

    assert resp.status_code == 200
    assert fetch.await_args.kwargs == {"refresh": True}


# ---------------------------------------------------------------------------
# _fetch_platform_membership / _fetch_platform_tier — direct unit tests
# ---------------------------------------------------------------------------


def _mock_cache(cached_value=None, *, last_known=None, paused=None):
    """A cache holding the 5-minute entry, the last-known membership and the
    unreachable pause, in the order the membership read asks for them."""
    cache = AsyncMock()
    cache.mget = AsyncMock(return_value=[cached_value, last_known, paused])
    cache.set = AsyncMock(return_value=True)
    cache.set_many = AsyncMock(return_value=True)
    return cache


LIMITS = "src.server.dependencies.usage_limits"


@pytest.mark.asyncio
async def test_fetch_platform_tier_no_service_url():
    """Returns -1 immediately when AUTH_SERVICE_URL is unset."""
    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", ""),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == -1


@pytest.mark.asyncio
async def test_fetch_platform_membership_oss_mode_short_circuits():
    """HOST_MODE=oss skips the platform call even if AUTH_SERVICE_URL is set."""
    mock_client = AsyncMock()
    with (
        patch(f"{LIMITS}.HOST_MODE", "oss"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{LIMITS}.get_http_client", return_value=mock_client),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        result = await _fetch_platform_membership("user-123")

    assert result == {"access_tier": -1, "plan_display_name": None}
    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_platform_tier_returns_tier():
    """Returns tier when platform responds with access_tier."""
    mock_response = httpx.Response(
        200, json={"valid": True, "access_tier": 0}
    )
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    cache = _mock_cache(cached_value=None)  # cache miss

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}.get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="service-token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == 0

    (fresh_key, _, fresh_ttl), (known_key, _, known_ttl) = cache.set_many.await_args.args[0]
    assert fresh_key == "platform_membership:user-123"
    assert fresh_ttl == 300
    assert known_key == "platform_membership:last_known:user-123"
    assert known_ttl > fresh_ttl
    cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_platform_tier_cache_hit():
    """Returns cached value without calling platform."""
    cache = _mock_cache(cached_value={"access_tier": 1, "plan_display_name": "Pro"})
    mock_client = AsyncMock()

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}.get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == 1
    mock_client.post.assert_not_called()  # no HTTP call on cache hit


@pytest.mark.asyncio
async def test_fetch_platform_membership_caches_tier_and_plan_display_name_together():
    """Both fields share one cache entry — only one HTTP round-trip per 5 min."""
    mock_response = httpx.Response(
        200, json={"valid": True, "access_tier": 2, "plan_display_name": "Premium"}
    )
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{LIMITS}.get_http_client", return_value=mock_client),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        result = await _fetch_platform_membership("user-123")

    assert result == {"access_tier": 2, "plan_display_name": "Premium"}
    # The cached value carries both fields so a follow-up tier-only read is free.
    cached_value = cache.set_many.await_args.args[0][0][1]
    assert cached_value["access_tier"] == 2
    assert cached_value["plan_display_name"] == "Premium"


@pytest.mark.asyncio
async def test_fetch_platform_tier_platform_error():
    """A 5xx is no answer: the tier is unknown, not -1, and nothing is cached
    in the membership's place."""
    mock_response = httpx.Response(500, text="Internal Server Error")
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}.get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result is None
    cache.set_many.assert_not_awaited()
    cache.set.assert_awaited_once_with(
        "platform_membership:unreachable:user-123", True, ttl=15
    )


@pytest.mark.asyncio
async def test_fetch_platform_tier_network_error():
    """A refused connection is the platform restarting, not the user's plan."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}.get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result is None
    cache.set_many.assert_not_awaited()
    cache.set.assert_awaited_once_with(
        "platform_membership:unreachable:user-123", True, ttl=15
    )


def _platform(mock_client, cache):
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(patch(f"{LIMITS}.HOST_MODE", "platform"))
    stack.enter_context(patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"))
    stack.enter_context(patch(f"{LIMITS}.get_http_client", return_value=mock_client))
    stack.enter_context(
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache)
    )
    stack.enter_context(patch("os.getenv", return_value="token"))
    return stack


PRO = {"access_tier": 2, "plan_display_name": "Pro"}


@pytest.mark.asyncio
async def test_unreachable_platform_stands_in_the_last_known_membership():
    """The 5-minute entry running out mid-outage must not cost a paying user
    their plan: the last answer the platform gave stands in."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    cache = _mock_cache(None, last_known=PRO)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        assert await _fetch_platform_membership("user-123") == PRO

    cache.set_many.assert_not_awaited()
    cache.set.assert_awaited_once_with(
        "platform_membership:unreachable:user-123", True, ttl=15
    )


@pytest.mark.asyncio
async def test_a_paused_user_is_not_asked_again():
    """A down or hanging platform is asked once per user per pause, not on
    every request, and the pause does not change what the user is told."""
    mock_client = AsyncMock()
    cache = _mock_cache(None, last_known=PRO, paused=True)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        assert await _fetch_platform_membership("user-123") == PRO

    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_a_paused_user_with_nothing_on_record_is_unknown():
    mock_client = AsyncMock()
    cache = _mock_cache(None, paused=True)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import (
            _fetch_platform_tier,
            get_platform_access_tier,
        )

        assert await _fetch_platform_tier("user-123") is None
        # Plan gates read unknown as no plan: a feature lost for the outage.
        assert await get_platform_access_tier("user-123") is None

    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_asks_past_the_cache_and_the_pause():
    """Redeeming an invitation has to show the new plan at once."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        return_value=httpx.Response(200, json={"valid": True, "access_tier": 3})
    )
    cache = _mock_cache(PRO, last_known=PRO, paused=True)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        result = await _fetch_platform_membership("user-123", refresh=True)

    assert result["access_tier"] == 3
    mock_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_failed_refresh_does_not_keep_the_plan_it_replaces():
    """The 5-minute entry goes even when the ask fails, so once the pause runs
    out the next request asks again rather than reading the old plan back."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    cache = _mock_cache(PRO, last_known=PRO)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        assert await _fetch_platform_membership("user-123", refresh=True) == PRO

    cache.delete.assert_awaited_once_with("platform_membership:user-123")


@pytest.mark.asyncio
async def test_a_refusal_asks_past_the_cache_and_leaves_it_in_place():
    """A plan bought minutes ago must count, and the cached plan must stay for
    the user's other reads while the ask is out, or each would ask too."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        return_value=httpx.Response(200, json={"valid": True, "access_tier": 3})
    )
    cache = _mock_cache(PRO, last_known=PRO)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        assert await _fetch_platform_tier("user-123", recheck=True) == 3

    mock_client.post.assert_awaited_once()
    cache.delete.assert_not_awaited()
    cache.set_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_refusal_during_the_outage_pause_takes_the_last_known_plan():
    """A client retrying a refused model must not reach a platform that has
    just failed to answer: the pause holds for a refusal too."""
    mock_client = AsyncMock()
    cache = _mock_cache(PRO, last_known=PRO, paused=True)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        assert await _fetch_platform_tier("user-123", recheck=True) == 2

    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_an_entry_without_a_last_known_record_seeds_one():
    """An older worker's entry carries no last-known record; without one, its
    expiry during an outage would leave the user with no plan on record."""
    mock_client = AsyncMock()
    cache = _mock_cache(PRO)

    with _platform(mock_client, cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        assert await _fetch_platform_membership("user-123") == PRO

    cache.set.assert_awaited_once_with(
        "platform_membership:last_known:user-123", PRO, ttl=30 * 24 * 3600
    )
    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_a_cached_no_plan_seeds_nothing():
    """An older worker caches -1 for a failed ask too, which is no answer."""
    cache = _mock_cache({"access_tier": -1, "plan_display_name": None})

    with _platform(AsyncMock(), cache):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        await _fetch_platform_membership("user-123")

    cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_platform_tier_missing_field():
    """Returns -1 when response lacks access_tier field."""
    mock_response = httpx.Response(200, json={"valid": True})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}.get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == -1


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/preferences — model_preference validation
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _prefs_endpoint(stored_model_preference=None):
    """The preferences PUT with its DB reads stubbed, yielding the upsert mock."""
    upsert = AsyncMock(return_value=_prefs())
    with (
        patch(f"{DB}.db_get_user", new_callable=AsyncMock, return_value=_user()),
        patch(
            f"{DB}.db_get_user_preferences",
            new_callable=AsyncMock,
            return_value={"model_preference": stored_model_preference or {}},
        ),
        patch(f"{DB}.upsert_user_preferences", new=upsert),
        patch(f"{DB}.maybe_complete_onboarding", new_callable=AsyncMock),
    ):
        yield upsert


@pytest.mark.parametrize(
    "model_preference",
    [
        {"fast_mode": "yes"},
        {"prompt_guidance": "verbose"},
        {"compaction_profile": "enormous"},
        {"profiles": {"some-model": {"fast_mode": "yes"}}},
        {"native_compaction": "on"},
        {"profiles": {"some-model": {"native_compaction": "on"}}},
    ],
)
@pytest.mark.asyncio
async def test_bad_tuning_is_rejected_on_either_level(client, model_preference):
    """The account level is a real write path, not just a fallback for profiles.

    It used to skip validation entirely whenever ``profiles`` was absent, so a
    string landed in a column the resolver reads as a boolean.
    """
    async with _prefs_endpoint() as upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences", json={"model_preference": model_preference}
        )

    assert resp.status_code == 400
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_tuning_is_accepted_on_the_account_level(client):
    async with _prefs_endpoint() as upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"fast_mode": True, "prompt_guidance": "lean"}},
        )

    assert resp.status_code == 200
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_deleting_a_provider_a_stored_model_still_needs_is_rejected(client):
    """The invariant holds on the merged row, not on the request.

    A patch that only clears ``custom_providers`` names no model at all, so
    checking the request body alone let it through and left the stored model
    pointing at a provider that no longer existed.
    """
    stored = {
        "custom_providers": [{"name": "my-gw", "parent_provider": "openai"}],
        "custom_models": [
            {"name": "my-model", "model_id": "gpt-x", "provider": "my-gw"}
        ],
    }
    async with _prefs_endpoint(stored) as upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"custom_providers": None}},
        )

    assert resp.status_code == 400
    assert "my-gw" in resp.json()["detail"]
    upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# PUT /users/me/preferences: a changed default model and existing threads
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _default_change(moved=0):
    """The PUT with the reassignment stubbed; the before and after views come
    from the row read under the profile lock and the row the upsert returns."""
    carry = AsyncMock(return_value=moved)
    async with _prefs_endpoint() as upsert:
        upsert.return_value = {
            **_prefs(),
            "model_preference": {"preferred_model": "m-b"},
        }
        with (
            patch(
                f"{DB}.db_lock_user_preferences",
                new_callable=AsyncMock,
                return_value={"model_preference": {"preferred_model": "m-a"}},
            ),
            patch(f"{DB}.carry_default_change", new=carry),
        ):
            yield upsert, carry


@pytest.mark.asyncio
async def test_a_default_change_reports_the_threads_it_moved(client):
    async with _default_change(moved=3) as (upsert, carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "model_preference": {"preferred_model": "m-b"},
                "apply_default_to": "existing_threads",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["threads_reassigned"] == 3
    carry.assert_awaited_once_with(
        "test-user-123",
        before={"preferred_model": "m-a"},
        after={"preferred_model": "m-b"},
        apply_to="existing_threads",
        conn=ANY,
    )
    # A one-shot answer, never stored.
    assert "apply_default_to" not in upsert.await_args.kwargs
    assert "apply_default_to" not in upsert.await_args.kwargs["model_preference"]


@pytest.mark.asyncio
async def test_a_write_naming_no_default_moves_no_threads(client):
    async with _default_change() as (_upsert, carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "model_preference": {"default_model_scope": "existing_threads"},
                "apply_default_to": "existing_threads",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["threads_reassigned"] == 0
    carry.assert_not_awaited()


@pytest.mark.parametrize("scope", ["ask", "new_threads", "existing_threads", None])
@pytest.mark.asyncio
async def test_a_known_default_model_scope_is_saved(client, scope):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"default_model_scope": scope}},
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["model_preference"] == {"default_model_scope": scope}


@pytest.mark.parametrize("scope", ["always", 1, ["existing_threads"]])
@pytest.mark.asyncio
async def test_an_unknown_default_model_scope_is_rejected(client, scope):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"default_model_scope": scope}},
        )

    assert resp.status_code == 400
    assert "default_model_scope" in resp.json()["detail"]
    upsert.assert_not_awaited()


@pytest.mark.parametrize("follows", ["primary", "deployment", None])
@pytest.mark.asyncio
async def test_a_known_background_follows_is_saved(client, follows):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"background_follows": follows}},
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["model_preference"] == {"background_follows": follows}


@pytest.mark.parametrize("follows", ["auto", 1, ["deployment"]])
@pytest.mark.asyncio
async def test_an_unknown_background_follows_is_rejected(client, follows):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"model_preference": {"background_follows": follows}},
        )

    assert resp.status_code == 400
    assert resp.json()["detail"].startswith("background_follows must be one of")
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_choosing_auto_for_the_background_model_is_a_default_change(client):
    """Auto moves what an unset Background model default runs, so threads on
    the old one move with it like any other default change."""
    async with _default_change(moved=2) as (_upsert, carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "model_preference": {"background_follows": "deployment"},
                "apply_default_to": "existing_threads",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["threads_reassigned"] == 2
    carry.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unknown_apply_default_to_is_422(client):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "model_preference": {"preferred_model": "m-b"},
                "apply_default_to": "ask",
            },
        )

    assert resp.status_code == 422
    upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# /users/me/preferences: the Background model's keys before their rename
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bag", ["model_preference", "other_preference"])
@pytest.mark.parametrize(
    "old, new, value",
    [
        ("preferred_flash_model", "preferred_background_model", "m-bg"),
        # A deletion is refused too: no reader looks under the old name.
        ("preferred_flash_model", "preferred_background_model", None),
        ("flash_follows", "background_follows", "deployment"),
    ],
)
@pytest.mark.asyncio
async def test_an_old_model_key_is_refused_naming_the_new_one(client, bag, old, new, value):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={bag: {old: value}},
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == f"{old} was renamed to {new}"
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_old_model_key_is_refused_beside_its_new_name(client):
    async with _default_change() as (upsert, _carry):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "model_preference": {
                    "preferred_flash_model": "m-old",
                    "preferred_background_model": "m-new",
                }
            },
        )

    assert resp.status_code == 400
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_preferences_answer_under_the_stored_names_only(client):
    stored = {
        "preferred_model": "m-a",
        "preferred_background_model": "m-bg",
        "background_follows": "deployment",
    }
    with (
        patch(f"{DB}.db_get_user", new_callable=AsyncMock, return_value=_user()),
        patch(
            f"{DB}.db_get_user_preferences",
            new_callable=AsyncMock,
            return_value={**_prefs(), "model_preference": dict(stored)},
        ),
    ):
        resp = await client.get("/api/v1/users/me/preferences")

    assert resp.status_code == 200
    assert resp.json()["model_preference"] == stored


# ---------------------------------------------------------------------------
# PUT /users/me/preferences: other_preference.subagents_default
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [True, False, None])
@pytest.mark.asyncio
async def test_a_boolean_subagents_default_is_saved(client, value):
    """true and false are stored; null reaches the merge, which deletes the key."""
    async with _prefs_endpoint() as upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"subagents_default": value}},
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["other_preference"] == {"subagents_default": value}


@pytest.mark.parametrize("value", ["false", "nope", 0, 1, {"on": False}, [False]])
@pytest.mark.asyncio
async def test_a_non_boolean_subagents_default_is_rejected(client, value):
    """The thread gate reads only a stored JSON false as off, so anything else
    would look like a choice in Settings and change nothing."""
    async with _prefs_endpoint() as upsert:
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"subagents_default": value}},
        )

    assert resp.status_code == 400
    assert "subagents_default" in resp.json()["detail"]
    upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# PUT /users/me/preferences: the structured profile
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _profile_endpoint(stored=None, read_before_lock=None):
    """The preferences PUT with ``stored`` as the row read under the profile
    lock (the one the structured keys are checked against) and
    ``read_before_lock`` as the earlier unlocked read, yielding the upsert,
    the completion check, and the unlocked read."""
    upsert = AsyncMock(return_value=_prefs())
    read = AsyncMock(return_value=read_before_lock)
    with (
        patch(f"{DB}.db_get_user", new_callable=AsyncMock, return_value=_user()),
        patch(f"{DB}.db_get_user_preferences", new=read),
        patch(f"{DB}.db_lock_user_preferences", new_callable=AsyncMock, return_value=stored),
        patch(f"{DB}.upsert_user_preferences", new=upsert),
        patch(f"{DB}.maybe_complete_onboarding", new_callable=AsyncMock) as complete,
    ):
        yield upsert, complete, read


@pytest.mark.asyncio
async def test_the_profile_form_is_saved_in_its_stored_spelling_and_completes_onboarding(client):
    """What is saved is what the check hands back, not what was sent. The
    rules themselves are pinned in ``tests/unit/server/models/
    test_structured_preferences.py``."""
    async with _profile_endpoint() as (upsert, complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "risk_preference": {"risk_level": "Moderate"},
                "investment_preference": {"markets": ["us", "HK", "us"], "sectors": ["Real Estate"]},
                "agent_preference": {"research_depth": "Light", "output_format": ["Visual", "text", "visual"], "tone": "terse"},
            },
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["risk_preference"] == {"risk_level": "moderate"}
    assert upsert.await_args.kwargs["investment_preference"] == {
        "markets": ["us", "hk"],
        "sectors": ["real_estate"],
    }
    assert upsert.await_args.kwargs["agent_preference"] == {
        "research_depth": "light",
        "output_format": ["visual", "text"],
        "tone": "terse",
    }
    complete.assert_awaited_once_with("test-user-123")


@pytest.mark.asyncio
async def test_every_bad_value_is_named_in_one_422(client):
    async with _profile_endpoint() as (upsert, complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "risk_preference": {"risk_level": "yolo"},
                "investment_preference": {"markets": ["uk"], "horizon": "decades"},
                "agent_preference": {"proactivity": "always"},
            },
        )

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail.startswith("risk_preference.risk_level: ")
    for path in (
        "risk_preference.risk_level",
        "investment_preference.markets[0]",
        "investment_preference.horizon",
        "agent_preference.proactivity",
    ):
        assert path in detail
    upsert.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_null_reaches_the_merge_as_a_deletion(client):
    async with _profile_endpoint() as (upsert, _complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "risk_preference": {"risk_level": None},
                "investment_preference": {"markets": None, "sectors": None},
                "agent_preference": {"research_depth": None, "proactivity": None},
            },
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["risk_preference"] == {"risk_level": None}
    assert upsert.await_args.kwargs["investment_preference"] == {"markets": None, "sectors": None}
    assert upsert.await_args.kwargs["agent_preference"] == {"research_depth": None, "proactivity": None}


@pytest.mark.asyncio
async def test_output_format_is_checked_with_the_structured_keys(client):
    """Settings sends a ranking, or null to let the agent decide; a format
    outside the ranking's vocabulary is a 422 like every structured key's."""
    async with _profile_endpoint() as (upsert, _complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"agent_preference": {"research_depth": "thorough", "output_format": ["visual", "pdf"]}},
        )

    assert resp.status_code == 422
    assert resp.json()["detail"].startswith("agent_preference.output_format[1]: 'pdf' is not one of")
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_value_the_row_already_holds_passes_whatever_it_is(client):
    """``horizon`` was free text before it had a vocabulary; a form that sends
    the stored value back unchanged must not be refused for it."""
    stored = {"investment_preference": {"horizon": "long_term", "sectors": ["Tech & AI"]}}
    async with _profile_endpoint(stored) as (upsert, _complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "investment_preference": {
                    "horizon": "long_term",
                    "sectors": ["Tech & AI"],
                    "markets": ["us"],
                }
            },
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["investment_preference"] == {
        "horizon": "long_term",
        "sectors": ["Tech & AI"],
        "markets": ["us"],
    }


@pytest.mark.asyncio
async def test_a_value_the_row_held_only_before_the_lock_is_checked_again(client):
    """Another write can replace the row between the unlocked read and the
    lock. The unchanged-value exemption holds against the locked row only, so
    the stale ``long_term`` must not be written back over a newer valid one."""
    before_lock = {"investment_preference": {"horizon": "long_term"}}
    under_lock = {"investment_preference": {"horizon": "years"}}
    async with _profile_endpoint(under_lock, read_before_lock=before_lock) as (upsert, complete, _read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={"investment_preference": {"horizon": "long_term"}},
        )

    assert resp.status_code == 422
    assert resp.json()["detail"].startswith("investment_preference.horizon: ")
    upsert.assert_not_awaited()
    complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_free_text_keys_pass_untouched_without_a_read(client):
    async with _profile_endpoint() as (upsert, _complete, read):
        resp = await client.put(
            "/api/v1/users/me/preferences",
            json={
                "risk_preference": {"risk_level": None, "risk_tolerance": "Whatever I like", "tolerance": 7},
                "investment_preference": {"holding_period": "a few years"},
                "agent_preference": {"output_format": None, "tone": "terse"},
            },
        )

    assert resp.status_code == 200
    assert upsert.await_args.kwargs["risk_preference"] == {
        "risk_level": None,
        "risk_tolerance": "Whatever I like",
        "tolerance": 7,
    }
    assert upsert.await_args.kwargs["agent_preference"] == {"output_format": None, "tone": "terse"}
    # A null sets no structured key, so there is nothing to check it against.
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_users_me_reports_when_the_account_was_created(client):
    """The web's new-user rule reads it."""
    created = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    result = {"user": _user(created_at=created), "preferences": None}
    with (
        patch(f"{DB}.get_user_with_preferences", new_callable=AsyncMock, return_value=result),
        patch(
            "src.server.dependencies.usage_limits._fetch_platform_membership",
            new_callable=AsyncMock,
            return_value={},
        ),
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    assert resp.json()["user"]["created_at"] == "2026-10-01T12:00:00Z"
