"""Personal access tokens, which the platform checks per call.

The platform is the only authority, so every failure to reach or read it
fails closed as a 503 (never a 401, which would read as a revoked key), and
the one request made carries the key alone.
"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.server.auth import pats

PLATFORM = "https://platform.example.test"
TOKEN = "la_pat_unit-test-secret"
USER = "user-pat-1"


class FakeCache:
    def __init__(self, held: dict | None = None) -> None:
        self.held = dict(held or {})
        self.sets: list[tuple] = []

    async def get(self, key):
        return self.held.get(key)

    async def set(self, key, value, ttl=None):
        self.sets.append((key, value, ttl))
        self.held[key] = value
        return True


class FakeResponse:
    def __init__(self, status_code=200, body=None, unreadable=False) -> None:
        self.status_code = status_code
        self._body = body
        self._unreadable = unreadable

    def json(self):
        if self._unreadable:
            raise ValueError("not json")
        return self._body


class Platform:
    """The shared HTTP client, answering ``response`` or raising ``error``."""

    def __init__(self) -> None:
        self.response = FakeResponse(200, {"valid": True, "user_id": USER, "scopes": ["agent:read"]})
        self.error: Exception | None = None
        self.post = AsyncMock(side_effect=self._post)

    async def _post(self, url, **kwargs):
        if self.error is not None:
            raise self.error
        return self.response


@pytest.fixture
def cache(monkeypatch) -> FakeCache:
    fake = FakeCache()
    monkeypatch.setattr(pats, "get_cache_client", lambda: fake)
    return fake


@pytest.fixture
def platform(monkeypatch, cache) -> Platform:
    fake = Platform()
    monkeypatch.setattr(pats, "AUTH_SERVICE_URL", PLATFORM)
    monkeypatch.setattr(pats, "get_http_client", AsyncMock(return_value=fake))
    return fake


def _cache_key(token: str = TOKEN) -> str:
    return f"pat_validation:{hashlib.sha256(token.encode()).hexdigest()}"


async def _refusal(token: str = TOKEN) -> HTTPException:
    with pytest.raises(HTTPException) as caught:
        await pats.verify_pat(token)
    return caught.value


def test_only_the_pat_prefix_is_a_personal_access_token():
    assert pats.is_pat("la_pat_abc")
    assert not pats.is_pat("lac1.abc")
    assert not pats.is_pat("eyJhbGciOi")
    assert not pats.is_pat("")


# -- a server with no platform ----------------------------------------------------


@pytest.mark.asyncio
async def test_a_server_with_no_platform_refuses_every_pat_without_asking_anyone(
    monkeypatch, cache
):
    monkeypatch.setattr(pats, "AUTH_SERVICE_URL", "")
    client = AsyncMock()
    monkeypatch.setattr(pats, "get_http_client", client)

    refusal = await _refusal()

    assert refusal.status_code == 401
    assert refusal.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'
    client.assert_not_awaited()
    assert cache.sets == []


# -- a live key -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_live_key_is_its_owner_with_the_scopes_the_platform_answered(platform):
    platform.response = FakeResponse(
        200, {"valid": True, "user_id": USER, "scopes": ["agent:read", "data:read"]}
    )

    principal = await pats.verify_pat(TOKEN)

    assert principal == pats.PatPrincipal(USER, ("agent:read", "data:read"))


@pytest.mark.asyncio
async def test_the_platform_is_sent_the_key_alone_and_no_service_headers(platform):
    """``/validate`` checks a service token before the key and would answer for
    the service instead."""
    await pats.verify_pat(TOKEN)

    platform.post.assert_awaited_once_with(
        f"{PLATFORM}/api/auth/validate", headers={"X-Api-Key": TOKEN}
    )


@pytest.mark.asyncio
async def test_a_trailing_slash_on_the_platform_address_is_trimmed(platform, monkeypatch):
    monkeypatch.setattr(pats, "AUTH_SERVICE_URL", f"{PLATFORM}/")

    await pats.verify_pat(TOKEN)

    assert platform.post.await_args.args == (f"{PLATFORM}/api/auth/validate",)


@pytest.mark.asyncio
async def test_the_answer_is_cached_for_thirty_seconds_under_a_digest_of_the_key(
    platform, cache
):
    await pats.verify_pat(TOKEN)

    assert cache.sets == [
        (_cache_key(), {"valid": True, "user_id": USER, "scopes": ["agent:read"]}, 30)
    ]
    assert TOKEN not in cache.sets[0][0]


@pytest.mark.asyncio
async def test_a_cached_answer_skips_the_platform(platform, cache):
    cache.held[_cache_key()] = {"valid": True, "user_id": USER, "scopes": ["agent:run"]}

    principal = await pats.verify_pat(TOKEN)

    assert principal == pats.PatPrincipal(USER, ("agent:run",))
    platform.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_second_call_within_the_window_asks_the_platform_once(platform):
    await pats.verify_pat(TOKEN)
    await pats.verify_pat(TOKEN)

    assert platform.post.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cached",
    [
        "garbage",
        {"valid": True},
        {"valid": "yes", "scopes": []},
        {"valid": True, "scopes": "agent:read"},
        [],
    ],
)
async def test_a_cached_value_that_is_not_an_answer_is_asked_again(platform, cache, cached):
    cache.held[_cache_key()] = cached

    principal = await pats.verify_pat(TOKEN)

    assert principal.user_id == USER
    platform.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_scopes_that_are_not_strings_are_dropped(platform):
    platform.response = FakeResponse(
        200, {"valid": True, "user_id": USER, "scopes": ["agent:read", 7, None, "data:read"]}
    )

    principal = await pats.verify_pat(TOKEN)

    assert principal.scopes == ("agent:read", "data:read")


@pytest.mark.asyncio
async def test_a_live_key_with_no_scopes_list_holds_none(platform):
    platform.response = FakeResponse(200, {"valid": True, "user_id": USER})

    principal = await pats.verify_pat(TOKEN)

    assert principal.scopes == ()


# -- a key the platform refuses ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_key_the_platform_calls_invalid_is_a_401_with_www_authenticate(platform):
    platform.response = FakeResponse(200, {"valid": False})

    refusal = await _refusal()

    assert refusal.status_code == 401
    assert refusal.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'


@pytest.mark.asyncio
async def test_a_refusal_is_cached_too_so_a_bad_key_does_not_hammer_the_platform(
    platform, cache
):
    platform.response = FakeResponse(200, {"valid": False})
    await _refusal()

    await _refusal()

    assert platform.post.await_count == 1
    assert cache.sets[0][1]["valid"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("user_id", [None, "", 42])
async def test_a_valid_answer_naming_no_owner_is_refused(platform, user_id):
    platform.response = FakeResponse(200, {"valid": True, "user_id": user_id, "scopes": []})

    refusal = await _refusal()

    assert refusal.status_code == 401


# -- a platform that cannot answer ------------------------------------------------


def _assert_unavailable(refusal: HTTPException) -> None:
    assert refusal.status_code == 503
    assert refusal.headers["Retry-After"] == "15"
    assert refusal.detail["type"] == "service_unavailable"


@pytest.mark.asyncio
async def test_an_unreachable_platform_is_a_503_not_a_401(platform, cache):
    platform.error = ConnectionError("no route")

    _assert_unavailable(await _refusal())

    assert cache.sets == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500, 502])
async def test_a_platform_that_does_not_answer_200_is_a_503(platform, cache, status):
    platform.response = FakeResponse(status, {"valid": True, "user_id": USER, "scopes": []})

    _assert_unavailable(await _refusal())

    assert cache.sets == []


@pytest.mark.asyncio
async def test_an_unreadable_200_is_a_503(platform, cache):
    platform.response = FakeResponse(200, unreadable=True)

    _assert_unavailable(await _refusal())

    assert cache.sets == []


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [[], "ok", None, {}, {"valid": "true"}, {"user_id": USER}])
async def test_a_200_that_is_not_an_answer_is_a_503(platform, cache, body):
    platform.response = FakeResponse(200, body)

    _assert_unavailable(await _refusal())

    assert cache.sets == []


@pytest.mark.asyncio
async def test_a_platform_failure_is_logged_without_the_key(platform, caplog):
    platform.error = ConnectionError(f"could not reach with {TOKEN}")

    await _refusal()

    assert "ConnectionError" in caplog.text
    assert TOKEN not in caplog.text
