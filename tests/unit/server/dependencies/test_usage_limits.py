"""
Tests for usage_limits dependency — service-to-service auth headers,
credit limit enforcement (platform + BYOK paths), and burst guard.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException


MODULE = "src.server.dependencies.usage_limits"


# ===================================================================
# Burst guard tests (_check_burst_guard + release_burst_slot)
# ===================================================================


def _mock_redis_cache(enabled=True, pipeline_results=None, decr_result=0):
    """Return a mock Redis cache for burst guard tests."""
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = MagicMock() if enabled else None

    if enabled and cache.client:
        pipe = AsyncMock()
        pipe.incr = MagicMock(return_value=pipe)
        pipe.expire = MagicMock(return_value=pipe)
        pipe.execute = AsyncMock(return_value=pipeline_results or [1])
        cache.client.pipeline = MagicMock(return_value=pipe)
        cache.client.decr = AsyncMock(return_value=decr_result)
        cache.client.set = AsyncMock()

    return cache


class TestCheckBurstGuard:
    """Tests for _check_burst_guard Redis INCR/DECR logic."""

    @pytest.mark.asyncio
    async def test_under_limit_allowed(self):
        """Request under the limit returns allowed=True with correct count."""
        cache = _mock_redis_cache(pipeline_results=[3])

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert result["current"] == 3
        assert result["limit"] == 10

    @pytest.mark.asyncio
    async def test_at_limit_allowed(self):
        """Request at exactly max_concurrent is still allowed."""
        cache = _mock_redis_cache(pipeline_results=[10])

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert result["current"] == 10

    @pytest.mark.asyncio
    async def test_over_limit_rollback(self):
        """Request over limit triggers DECR rollback and returns allowed=False."""
        cache = _mock_redis_cache(pipeline_results=[11])

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is False
        assert result["current"] == 10
        assert result["limit"] == 10
        cache.client.decr.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_redis_disabled_fail_open(self):
        """When Redis is disabled, burst guard allows the request."""
        cache = _mock_redis_cache(enabled=False)
        cache.client = None

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert "current" not in result

    @pytest.mark.asyncio
    async def test_redis_error_fail_open(self):
        """When Redis raises an exception, burst guard allows the request."""
        cache = _mock_redis_cache(pipeline_results=[1])
        pipe = cache.client.pipeline()
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True


class TestReleaseBurstSlot:
    """Tests for release_burst_slot Redis DECR logic."""

    @pytest.mark.asyncio
    async def test_decr_to_positive(self):
        """Normal release: DECR to a positive value, no clamping."""
        cache = _mock_redis_cache(decr_result=2)

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            await release_burst_slot("user-1")

        cache.client.decr.assert_awaited_once()
        cache.client.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_decr_to_negative_clamps_to_zero(self):
        """When DECR goes negative, clamp the key to 0."""
        cache = _mock_redis_cache(decr_result=-1)

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            await release_burst_slot("user-1")

        cache.client.decr.assert_awaited_once()
        cache.client.set.assert_awaited_once()
        # Verify it sets to 0
        set_args = cache.client.set.call_args
        assert set_args[0][1] == 0

    @pytest.mark.asyncio
    async def test_redis_error_swallowed(self):
        """Redis errors during release are swallowed (no exception raised)."""
        cache = _mock_redis_cache()
        cache.client.decr = AsyncMock(side_effect=ConnectionError("Redis down"))

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            # Should not raise
            await release_burst_slot("user-1")


def _mock_cache_miss():
    """Return a mock Redis cache that always misses (get→None, set→no-op)."""
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    return cache


@pytest.mark.asyncio
async def test_call_validate_for_user_uses_x_service_token_header():
    """_call_validate_for_user sends X-Service-Token, not Authorization: Bearer."""
    mock_response = httpx.Response(
        200,
        json={"valid": True, "quota": {"allowed": True}},
    )
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with (
        patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{MODULE}._get_http_client", return_value=mock_client),
        patch("os.getenv", return_value="my-secret-token"),
    ):
        from src.server.dependencies.usage_limits import _call_validate_for_user

        result = await _call_validate_for_user("user-123", check_quota="chat")

    assert result is not None
    assert result["valid"] is True

    # Verify the actual headers sent
    call_kwargs = mock_client.post.call_args
    headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")

    assert "X-Service-Token" in headers
    assert headers["X-Service-Token"] == "my-secret-token"
    assert "Authorization" not in headers
    assert headers["X-User-Id"] == "user-123"


@pytest.mark.asyncio
async def test_call_validate_for_user_no_token_omits_service_header():
    """When INTERNAL_SERVICE_TOKEN is empty, X-Service-Token is not sent."""
    mock_response = httpx.Response(200, json={"valid": True})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with (
        patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{MODULE}._get_http_client", return_value=mock_client),
        patch("os.getenv", return_value=""),
    ):
        from src.server.dependencies.usage_limits import _call_validate_for_user

        await _call_validate_for_user("user-456")

    call_kwargs = mock_client.post.call_args
    headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")

    assert "X-Service-Token" not in headers
    assert "Authorization" not in headers
    assert headers["X-User-Id"] == "user-456"


@pytest.mark.asyncio
async def test_call_validate_for_user_returns_none_when_no_auth_url():
    """When AUTH_SERVICE_URL is unset, _call_validate_for_user returns None immediately."""
    with patch(f"{MODULE}.AUTH_SERVICE_URL", ""):
        from src.server.dependencies.usage_limits import _call_validate_for_user

        result = await _call_validate_for_user("user-789")

    assert result is None


@pytest.mark.asyncio
async def test_call_validate_for_user_sends_check_quota_in_body():
    """check_quota and byok flags are included in the request body."""
    mock_response = httpx.Response(200, json={"valid": True})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with (
        patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{MODULE}._get_http_client", return_value=mock_client),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _call_validate_for_user

        await _call_validate_for_user("user-123", check_quota="workspace", byok=True)

    call_kwargs = mock_client.post.call_args
    body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")

    assert body["check_quota"] == "workspace"
    assert body["byok"] is True


@pytest.mark.asyncio
async def test_call_validate_for_user_fails_open_on_exception():
    """Network errors return None (fail-open)."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with (
        patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
        patch(f"{MODULE}._get_http_client", return_value=mock_client),
        patch("os.getenv", return_value="token"),
    ):
        from src.server.dependencies.usage_limits import _call_validate_for_user

        result = await _call_validate_for_user("user-123")

    assert result is None


# ===================================================================
# Test 4: enforce_credit_limit byok parameter tests
# ===================================================================


class TestEnforceCreditLimitByok:
    """Verify enforce_credit_limit behaviour under byok=True.

    BYOK path goes through _enforce_byok_negative_balance which uses
    Redis cache. Tests mock the cache as a miss so the HTTP call
    to _call_validate_for_user is exercised.
    """

    @pytest.mark.asyncio
    async def test_byok_negative_balance_raises_429(self):
        """byok=True with negative remaining_credits raises 429 with type=negative_balance."""
        quota_response = {
            "quota": {
                "allowed": True,
                "remaining_credits": -5.0,
                "used_credits": 105.0,
                "credit_limit": 100.0,
                "retry_after": 30,
            }
        }

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=True)

            assert exc_info.value.status_code == 429
            assert exc_info.value.detail["type"] == "negative_balance"

    @pytest.mark.asyncio
    async def test_byok_positive_balance_passes(self):
        """byok=True with positive remaining_credits should not raise, even if allowed=False."""
        quota_response = {
            "quota": {
                "allowed": False,
                "remaining_credits": 50.0,
                "used_credits": 50.0,
                "credit_limit": 100.0,
            }
        }

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)

    @pytest.mark.asyncio
    async def test_byok_zero_balance_passes(self):
        """byok=True with remaining_credits=0 should not raise (zero is not negative)."""
        quota_response = {
            "quota": {
                "allowed": False,
                "remaining_credits": 0,
                "used_credits": 100.0,
                "credit_limit": 100.0,
            }
        }

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)

    @pytest.mark.asyncio
    async def test_byok_none_remaining_passes(self):
        """byok=True with remaining_credits=None should not raise (missing field = no block)."""
        quota_response = {
            "quota": {
                "allowed": False,
                "used_credits": 100.0,
                "credit_limit": 100.0,
            }
        }

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)

    @pytest.mark.asyncio
    async def test_byok_cache_hit_negative_raises_without_http(self):
        """When cache says 'negative', skip HTTP call entirely and raise 429."""
        cache = _mock_cache_miss()
        cache.get = AsyncMock(return_value="negative")  # cache hit
        mock_validate = AsyncMock()

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", mock_validate),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=True)

            assert exc_info.value.status_code == 429
            mock_validate.assert_not_called()  # no HTTP call

    @pytest.mark.asyncio
    async def test_byok_cache_hit_ok_passes_without_http(self):
        """When cache says 'ok', skip HTTP call entirely and allow."""
        cache = _mock_cache_miss()
        cache.get = AsyncMock(return_value="ok")  # cache hit
        mock_validate = AsyncMock()

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", mock_validate),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)
            mock_validate.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_byok_allowed_false_raises_429(self):
        """byok=False with allowed=False raises 429 (existing behaviour preserved)."""
        quota_response = {
            "quota": {
                "allowed": False,
                "limit_type": "credit_limit",
                "remaining_credits": 0,
                "used_credits": 100.0,
                "credit_limit": 100.0,
                "retry_after": 30,
            }
        }

        with (
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=False)

            assert exc_info.value.status_code == 429
            assert exc_info.value.detail["type"] == "credit_limit"

    @pytest.mark.asyncio
    async def test_no_auth_service_url_returns_immediately(self):
        """When AUTH_SERVICE_URL is unset, enforce_credit_limit is a no-op."""
        with patch(f"{MODULE}.AUTH_SERVICE_URL", ""):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)
            await enforce_credit_limit("user-1", byok=False)
