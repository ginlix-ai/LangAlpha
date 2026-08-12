"""
Tests for the Users API router (src/server/app/users.py).

Covers user CRUD, preferences CRUD, delete-preferences (reset onboarding),
and platform access tier from the platform service.
The auth-sync endpoint is NOT tested here because it depends on
get_current_auth_info (a different dependency not overridden in create_test_app).
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

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
    with patch(
        f"{DB}.get_user_with_preferences",
        new_callable=AsyncMock,
        return_value=result,
    ):
        resp = await client.get("/api/v1/users/me")

    assert resp.status_code == 200
    body = resp.json()
    assert body["user"]["user_id"] == "test-user-123"
    assert body["preferences"] is not None


@pytest.mark.asyncio
async def test_get_current_user_no_preferences(client):
    result = {"user": _user(), "preferences": None}
    with patch(
        f"{DB}.get_user_with_preferences",
        new_callable=AsyncMock,
        return_value=result,
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
async def test_get_user_access_tier_platform_unreachable(client):
    """When platform is unreachable, access_tier defaults to -1 (fail-open)."""
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


# ---------------------------------------------------------------------------
# _fetch_platform_membership / _fetch_platform_tier — direct unit tests
# ---------------------------------------------------------------------------


def _mock_cache(cached_value=None):
    """Return a mock cache that returns cached_value on get, no-ops on set."""
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=cached_value)
    cache.set = AsyncMock(return_value=True)
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
        patch(f"{LIMITS}._get_http_client", return_value=mock_client),
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
            f"{LIMITS}._get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="service-token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == 0

    # Verify result was cached
    cache.set.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_platform_tier_cache_hit():
    """Returns cached value without calling platform."""
    cache = _mock_cache(cached_value={"access_tier": 1, "plan_display_name": "Pro"})
    mock_client = AsyncMock()

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}._get_http_client",
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
        patch(f"{LIMITS}._get_http_client", return_value=mock_client),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_membership

        result = await _fetch_platform_membership("user-123")

    assert result == {"access_tier": 2, "plan_display_name": "Premium"}
    cache.set.assert_called_once()
    # The cached value carries both fields so a follow-up tier-only read is free.
    cached_value = cache.set.call_args[0][1]
    assert cached_value["access_tier"] == 2
    assert cached_value["plan_display_name"] == "Premium"


@pytest.mark.asyncio
async def test_fetch_platform_tier_platform_error():
    """Returns -1 on non-200 response (fail-open)."""
    mock_response = httpx.Response(500, text="Internal Server Error")
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}._get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == -1


@pytest.mark.asyncio
async def test_fetch_platform_tier_network_error():
    """Returns -1 on network errors (fail-open)."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    cache = _mock_cache(cached_value=None)

    with (
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(
            f"{LIMITS}._get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == -1


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
            f"{LIMITS}._get_http_client",
            return_value=mock_client,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _fetch_platform_tier

        result = await _fetch_platform_tier("user-123")

    assert result == -1
