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


def _mock_redis_cache(enabled=True, zcard=1):
    """Return a mock Redis cache for the ZSET burst guard tests.

    Pipeline result order mirrors the implementation:
    [zremrangebyscore, zadd, zcard, expire].
    """
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = MagicMock() if enabled else None

    if enabled and cache.client:
        pipe = AsyncMock()
        pipe.zremrangebyscore = MagicMock(return_value=pipe)
        pipe.zadd = MagicMock(return_value=pipe)
        pipe.zcard = MagicMock(return_value=pipe)
        pipe.expire = MagicMock(return_value=pipe)
        pipe.execute = AsyncMock(return_value=[0, 1, zcard, True])
        cache.client.pipeline = MagicMock(return_value=pipe)
        cache.client.zrem = AsyncMock(return_value=1)

    return cache


class TestCheckBurstGuard:
    """_check_burst_guard: ZSET-membership admission (slot per request)."""

    @pytest.mark.asyncio
    async def test_under_limit_allowed_returns_slot(self):
        """Admission under the limit returns the held slot's id — the caller
        must thread it through to release_burst_slot."""
        cache = _mock_redis_cache(zcard=3)

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert result["current"] == 3
        assert result["limit"] == 10
        assert result["slot_id"]
        # The slot registered in the ZSET is the one handed back.
        pipe = cache.client.pipeline()
        zadd_members = pipe.zadd.call_args[0][1]
        assert list(zadd_members.keys()) == [result["slot_id"]]

    @pytest.mark.asyncio
    async def test_at_limit_allowed(self):
        """Request landing exactly at max_concurrent is still allowed."""
        cache = _mock_redis_cache(zcard=10)

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert result["current"] == 10
        assert result["slot_id"]

    @pytest.mark.asyncio
    async def test_over_limit_removes_own_member_only(self):
        """Denial rolls back the request's OWN ZSET member — never another
        in-flight request's slot (the old counter DECR could)."""
        cache = _mock_redis_cache(zcard=11)

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is False
        assert result["current"] == 10
        assert result["limit"] == 10
        assert "slot_id" not in result
        pipe = cache.client.pipeline()
        own_slot = list(pipe.zadd.call_args[0][1].keys())[0]
        cache.client.zrem.assert_awaited_once()
        assert cache.client.zrem.call_args[0][1] == own_slot

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
        cache = _mock_redis_cache()
        pipe = cache.client.pipeline()
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))

        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            from src.server.dependencies.usage_limits import _check_burst_guard

            result = await _check_burst_guard("user-1", max_concurrent=10)

        assert result["allowed"] is True
        assert "slot_id" not in result


class TestReleaseBurstSlot:
    """release_burst_slot: idempotent ZREM of the held slot member."""

    @pytest.mark.asyncio
    async def test_release_zrems_the_held_slot(self):
        cache = _mock_redis_cache()

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            await release_burst_slot("user-1", "slot-abc")

        cache.client.zrem.assert_awaited_once()
        assert cache.client.zrem.call_args[0][1] == "slot-abc"

    @pytest.mark.asyncio
    async def test_release_without_slot_is_noop(self):
        """No slot id means fail-open admission held no member (or a legacy
        pre-cutover request): nothing to remove, never touch the ZSET."""
        cache = _mock_redis_cache()

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            await release_burst_slot("user-1")

        cache.client.zrem.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_redis_error_swallowed(self):
        """Redis errors during release are swallowed (no exception raised);
        the ZSET reap-by-score self-heals the leaked slot."""
        cache = _mock_redis_cache()
        cache.client.zrem = AsyncMock(side_effect=ConnectionError("Redis down"))

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            # Should not raise
            await release_burst_slot("user-1", "slot-abc")

    @pytest.mark.asyncio
    async def test_strict_reraises_redis_error(self):
        """strict=True (hook-outbox executor path) re-raises so the drainer
        nacks and retries — a swallowed ZREM would ack a still-held slot."""
        cache = _mock_redis_cache()
        cache.client.zrem = AsyncMock(side_effect=ConnectionError("Redis down"))

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            with pytest.raises(ConnectionError):
                await release_burst_slot("user-1", "slot-abc", strict=True)

    @pytest.mark.asyncio
    async def test_strict_raises_when_cache_unavailable(self):
        """`enabled` flips off on a failed startup connect: strict release
        must raise (drainer nack), never return as if the slot were freed."""
        cache = _mock_redis_cache(enabled=False)
        cache.client = None

        with (
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
            patch(f"{MODULE}.HOST_MODE", "platform"),
        ):
            from src.server.dependencies.usage_limits import release_burst_slot

            with pytest.raises(RuntimeError):
                await release_burst_slot("user-1", "slot-abc", strict=True)
            # Non-strict callers keep the quiet no-op (reap self-heals).
            await release_burst_slot("user-1", "slot-abc")


class TestBurstReapHorizon:
    """The leak-reap horizon must cover the longest legitimate run — a
    shorter horizon reaps live slots, so long turns stop counting."""

    def test_horizon_covers_workflow_timeout(self, monkeypatch):
        monkeypatch.delenv("BURST_COUNTER_TTL", raising=False)
        from src.config.settings import get_workflow_timeout
        from src.server.dependencies.usage_limits import _burst_reap_horizon

        assert _burst_reap_horizon() >= get_workflow_timeout()

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("BURST_COUNTER_TTL", "42")
        from src.server.dependencies.usage_limits import _burst_reap_horizon

        assert _burst_reap_horizon() == 42


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
        patch(f"{MODULE}.HOST_MODE", "platform"),
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
        patch(f"{MODULE}.HOST_MODE", "platform"),
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
        patch(f"{MODULE}.HOST_MODE", "platform"),
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
        patch(f"{MODULE}.HOST_MODE", "platform"),
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
    async def test_byok_outstanding_debt_raises_429(self):
        """byok=True with outstanding_debt > 0 raises 429 with type=negative_balance."""
        quota_response = {
            "quota": {
                "allowed": True,
                "outstanding_debt": 100,
                "retry_after": 30,
            }
        }

        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=True)

            assert exc_info.value.status_code == 429
            assert exc_info.value.detail["type"] == "negative_balance"
            assert exc_info.value.detail["outstanding_debt"] == 100

    @pytest.mark.asyncio
    async def test_byok_unlimited_sentinel_does_not_block(self):
        """Regression: remaining_credits=-1 (unlimited sentinel) MUST NOT block.

        Pre-fix bug: langalpha treated remaining_credits<0 as outstanding debt,
        but ginlix-platform uses -1 for unlimited tiers. This caused BYOK users
        on unlimited plans (or daily-unlimited plans) to be permanently blocked.
        """
        quota_response = {
            "quota": {
                "allowed": True,
                "remaining_credits": -1,  # unlimited sentinel
                "outstanding_debt": 0,
            }
        }

        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)

    @pytest.mark.asyncio
    async def test_byok_zero_debt_passes(self):
        """byok=True with outstanding_debt=0 should not raise, even if quota.allowed=False."""
        quota_response = {
            "quota": {
                "allowed": False,
                "remaining_credits": 0,
                "outstanding_debt": 0,
            }
        }

        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=_mock_cache_miss()),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)

    @pytest.mark.asyncio
    async def test_byok_missing_debt_field_passes(self):
        """Wire-compat: older platform builds without outstanding_debt → no block."""
        quota_response = {
            "quota": {
                "allowed": False,
                "remaining_credits": -1,  # would have blocked under old code
            }
        }

        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
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
            patch(f"{MODULE}.HOST_MODE", "platform"),
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
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", mock_validate),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)
            mock_validate.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_byok_allowed_false_raises_429(self):
        """byok=False with allowed=False raises 429."""
        quota_response = {
            "quota": {
                "allowed": False,
                "limit_type": "credit_limit",
                "message": "Daily credit limit reached",
                "remaining_credits": 0,
                "used_credits": 100.0,
                "credit_limit": 100.0,
                "retry_after": 30,
            }
        }

        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=False)

            assert exc_info.value.status_code == 429
            assert exc_info.value.detail["type"] == "credit_limit"

    @pytest.mark.asyncio
    async def test_non_byok_forwards_platform_message_verbatim(self):
        """Platform message + unknown limit_type pass through unchanged."""
        quota_response = {
            "quota": {
                "allowed": False,
                "limit_type": "some_future_limit",
                "message": "A future limit string from platform",
                "retry_after": 30,
            }
        }
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import enforce_credit_limit
            with pytest.raises(HTTPException) as exc_info:
                await enforce_credit_limit("user-1", byok=False)
            assert exc_info.value.detail["type"] == "some_future_limit"
            assert exc_info.value.detail["message"] == "A future limit string from platform"

    @pytest.mark.asyncio
    async def test_no_auth_service_url_returns_immediately(self):
        """When AUTH_SERVICE_URL is unset, enforce_credit_limit is a no-op."""
        with patch(f"{MODULE}.AUTH_SERVICE_URL", ""):
            from src.server.dependencies.usage_limits import enforce_credit_limit

            await enforce_credit_limit("user-1", byok=True)
            await enforce_credit_limit("user-1", byok=False)


# ===================================================================
# get_capacity_status (read-only count-quota status for the UI)
# ===================================================================


class TestGetCapacityStatus:
    """Verify the display-only capacity reader: parses counts, never raises."""

    @pytest.mark.asyncio
    async def test_oss_mode_returns_none(self):
        """OSS mode has no quotas — returns None without calling the platform."""
        with patch(f"{MODULE}.HOST_MODE", "oss"):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_performance") is None

    @pytest.mark.asyncio
    async def test_no_auth_service_url_returns_none(self):
        """No AUTH_SERVICE_URL configured — returns None immediately."""
        with patch(f"{MODULE}.AUTH_SERVICE_URL", ""):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_max") is None

    @pytest.mark.asyncio
    async def test_parses_capacity_used_and_limit(self):
        """Platform count quota maps to {used, limit}."""
        quota_response = {"quota": {"capacity_used": 1, "capacity_limit": 3}}
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_performance") == {"used": 1, "limit": 3}

    @pytest.mark.asyncio
    async def test_unlimited_sentinel_preserved(self):
        """limit == -1 (unlimited) is passed through, not clamped."""
        quota_response = {"quota": {"capacity_used": 5, "capacity_limit": -1}}
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "always_on") == {"used": 5, "limit": -1}

    @pytest.mark.asyncio
    async def test_unlimited_with_omitted_used(self):
        """Platform omits capacity_used on unlimited tiers — still report limit -1.

        Regression: ginlix-platform's capacity counter returns
        ``QuotaInfo(allowed=True, capacity_limit=-1)`` with no ``capacity_used`` for
        unlimited plans, so requiring ``used`` would hide the "Unlimited" hint.
        """
        quota_response = {"quota": {"allowed": True, "capacity_limit": -1}}
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_performance") == {"used": 0, "limit": -1}

    @pytest.mark.asyncio
    async def test_legacy_field_names_fallback(self):
        """Falls back to active/limit when the platform omits capacity_* aliases."""
        quota_response = {"quota": {"active": 2, "limit": 4}}
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_performance") == {"used": 2, "limit": 4}

    @pytest.mark.asyncio
    async def test_missing_quota_object_returns_none(self):
        """No quota object in the response — degrade to None, no display."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value={"access_tier": 1}),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_performance") is None

    @pytest.mark.asyncio
    async def test_partial_counts_returns_none(self):
        """Quota object present but missing a count field — None rather than a half value."""
        quota_response = {"quota": {"capacity_used": 1}}  # no limit
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "spec_max") is None

    @pytest.mark.asyncio
    async def test_unreachable_platform_returns_none(self):
        """Validate returns None (platform down) — fail soft to None."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=None),
        ):
            from src.server.dependencies.usage_limits import get_capacity_status

            assert await get_capacity_status("user-1", "always_on") is None


# ===================================================================
# always_on_entitlement_lost — the idle-cleanup reconciler's check.
# Fail-safe: only a definitive "no always-on scope" returns True; OSS,
# unreachable, and ambiguous responses keep always-on (return False).
# ===================================================================


class TestAlwaysOnEntitlementLost:
    @pytest.mark.asyncio
    async def test_oss_mode_keeps_always_on(self):
        """OSS mode never reconciles — returns False without calling validate."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is False
            mock_v.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scope_present_keeps_always_on(self):
        """200 with the scope present → still entitled → False."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"scopes": ["workspace:always_on", "workspace:spec:max"]},
            ),
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is False

    @pytest.mark.asyncio
    async def test_scope_absent_reports_lost(self):
        """200 with a real scope list lacking always-on → entitlement lost → True."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"scopes": ["workspace:spec:performance"]},
            ),
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is True

    @pytest.mark.asyncio
    async def test_empty_scope_list_reports_lost(self):
        """200 with an explicit empty list (e.g. free tier) → lost → True. The
        list is present, so this is a definitive answer, not an outage."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"scopes": []},
            ),
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is True

    @pytest.mark.asyncio
    async def test_unreachable_keeps_always_on(self):
        """validate None (platform down / non-200) → fail-safe keep → False.
        An outage must never mass-disable always-on."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=None),
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is False

    @pytest.mark.asyncio
    async def test_ambiguous_response_keeps_always_on(self):
        """200 but scopes missing/None (not a list) → ambiguous → fail-safe keep."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"valid": True},
            ),
        ):
            from src.server.dependencies.usage_limits import always_on_entitlement_lost

            assert await always_on_entitlement_lost("user-1") is False


# ===================================================================
# enforce_capacity — the count-quota gate (429). OSS no-op; fail-open
# on unreachable / missing quota; 429 only on an explicit allowed:False.
# ===================================================================


class TestEnforceCapacity:
    @pytest.mark.asyncio
    async def test_oss_mode_is_noop(self):
        """OSS mode never counts — returns without calling the platform."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            await enforce_capacity("user-1", "spec_max")
            mock_v.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_auth_service_url_is_noop(self):
        """No AUTH_SERVICE_URL (partial deploy) — fail-open, no platform call."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", ""),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            await enforce_capacity("user-1", "spec_performance")
            mock_v.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exhausted_quota_raises_429_with_platform_fields(self):
        """allowed:False → 429 forwarding the platform's limit_type / counts."""
        quota_response = {
            "quota": {
                "allowed": False,
                "capacity_used": 3,
                "capacity_limit": 3,
                "limit_type": "max_limit",
                "message": "You've used all your max workspaces",
            }
        }
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=quota_response),
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            with pytest.raises(HTTPException) as exc:
                await enforce_capacity("user-1", "spec_max")

            assert exc.value.status_code == 429
            assert exc.value.detail["type"] == "max_limit"
            assert exc.value.detail["current"] == 3
            assert exc.value.detail["limit"] == 3
            assert exc.value.detail["remaining"] == 0
            assert exc.value.headers["X-RateLimit-Remaining"] == "0"

    @pytest.mark.asyncio
    async def test_allowed_quota_passes(self):
        """allowed:True → no raise."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"quota": {"allowed": True}},
            ),
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            await enforce_capacity("user-1", "always_on")  # no exception

    @pytest.mark.asyncio
    async def test_unreachable_platform_fails_open(self):
        """validate None (platform down) → fail-open, never block the user."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=None),
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            await enforce_capacity("user-1", "spec_performance")  # no exception

    @pytest.mark.asyncio
    async def test_missing_quota_object_fails_open(self):
        """200 without a quota object → fail-open (no count to enforce)."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"access_tier": 1},
            ),
        ):
            from src.server.dependencies.usage_limits import enforce_capacity

            await enforce_capacity("user-1", "spec_max")  # no exception


# ===================================================================
# assert_spec_allowed — hybrid spec gate (scope 403 + count 429).
# standard is never gated; the count check is skipped when already at
# the target tier (no new slot consumed).
# ===================================================================


class TestAssertSpecAllowed:
    @pytest.mark.asyncio
    async def test_standard_is_noop(self):
        """standard tier is never gated — no scope or count check."""
        with (
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes,
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "standard")
            mock_scopes.assert_not_awaited()
            mock_cap.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_performance_missing_scope_raises_403(self):
        """Non-empty scope list lacking the performance scope → 403, no count check."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:max"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            with pytest.raises(HTTPException) as exc:
                await assert_spec_allowed("user-1", "performance", current_tier="standard")

            assert exc.value.status_code == 403
            mock_cap.assert_not_awaited()  # scope failed first

    @pytest.mark.asyncio
    async def test_performance_scope_present_checks_count(self):
        """Scope held + tier change → count quota enforced for spec_performance."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:performance"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "performance", current_tier="standard")
            mock_cap.assert_awaited_once_with("user-1", "spec_performance")

    @pytest.mark.asyncio
    async def test_already_at_tier_skips_count(self):
        """current_tier == tier → scope checked but no new slot counted."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:max"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "max", current_tier="max")
            mock_cap.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_downgrade_skips_count(self):
        """max → performance is a downgrade: scope checked, no new slot counted."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:performance"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "performance", current_tier="max")
            mock_cap.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_max_checks_max_scope_and_count(self):
        """max tier gates on the spec:max scope + spec_max count."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:max"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "max", current_tier="standard")
            mock_cap.assert_awaited_once_with("user-1", "spec_max")

    @pytest.mark.asyncio
    async def test_oss_mode_is_noop(self):
        """OSS mode: scope + count gates both fail open, so any tier is allowed."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes,
        ):
            from src.server.dependencies.usage_limits import assert_spec_allowed

            await assert_spec_allowed("user-1", "max", current_tier="standard")
            mock_v.assert_not_awaited()
            mock_scopes.assert_not_awaited()


# ===================================================================
# assert_always_on_allowed — always-on gate (scope 403 + count 429).
# ===================================================================


class TestAssertAlwaysOnAllowed:
    @pytest.mark.asyncio
    async def test_missing_scope_raises_403(self):
        """Non-empty scope list lacking always-on → 403, count not reached."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:spec:max"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_always_on_allowed

            with pytest.raises(HTTPException) as exc:
                await assert_always_on_allowed("user-1")

            assert exc.value.status_code == 403
            mock_cap.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scope_present_checks_count(self):
        """always-on scope held → count quota enforced for always_on."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:always_on"],
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import assert_always_on_allowed

            await assert_always_on_allowed("user-1")
            mock_cap.assert_awaited_once_with("user-1", "always_on")

    @pytest.mark.asyncio
    async def test_oss_mode_is_noop(self):
        """OSS mode: both gates fail open — always-on never blocked."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes,
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import assert_always_on_allowed

            await assert_always_on_allowed("user-1")
            mock_scopes.assert_not_awaited()
            mock_v.assert_not_awaited()


# ===================================================================
# require_workspace_scope — the scope-403 half of the hybrid gate.
# Fail-open (allow) only on OSS mode or an unreachable/omitted scope
# response (None); a definitive list — including an empty one — is
# enforced so a no-scope user can't slip through as if the service were
# down.
# ===================================================================


class TestRequireWorkspaceScope:
    @pytest.mark.asyncio
    async def test_oss_mode_allows(self):
        """OSS mode: gate inactive → returns without fetching scopes."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes,
        ):
            from src.server.dependencies.usage_limits import require_workspace_scope

            await require_workspace_scope("user-1", "workspace:always_on")
            mock_scopes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_none_scopes_fails_open(self):
        """Unreachable platform (None) → fail-open, no 403."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes", new_callable=AsyncMock, return_value=None
            ),
        ):
            from src.server.dependencies.usage_limits import require_workspace_scope

            await require_workspace_scope("user-1", "workspace:always_on")  # no raise

    @pytest.mark.asyncio
    async def test_definitive_empty_list_denies(self):
        """A platform-confirmed empty list lacks every scope → 403."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock, return_value=[]),
        ):
            from src.server.dependencies.usage_limits import require_workspace_scope

            with pytest.raises(HTTPException) as exc:
                await require_workspace_scope("user-1", "workspace:always_on")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_present_scope_allows(self):
        """Definitive list containing the scope → no raise."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes",
                new_callable=AsyncMock,
                return_value=["workspace:always_on"],
            ),
        ):
            from src.server.dependencies.usage_limits import require_workspace_scope

            await require_workspace_scope("user-1", "workspace:always_on")  # no raise


# ===================================================================
# _get_user_scopes — None (unreachable/omitted) vs a definitive list.
# ===================================================================


class TestGetUserScopes:
    @pytest.mark.asyncio
    async def test_unreachable_returns_none(self):
        """validate None → None (the fail-open signal), not []."""
        with patch(
            f"{MODULE}._call_validate_for_user",
            new_callable=AsyncMock,
            return_value=None,
        ):
            from src.server.dependencies.usage_limits import (
                _get_user_scopes,
                _scope_cache,
            )

            _scope_cache.pop("user-1", None)
            assert await _get_user_scopes("user-1") is None

    @pytest.mark.asyncio
    async def test_omitted_scopes_key_returns_none(self):
        """A 200 without a scopes key → None (can't confirm), not []."""
        with patch(
            f"{MODULE}._call_validate_for_user",
            new_callable=AsyncMock,
            return_value={"valid": True},
        ):
            from src.server.dependencies.usage_limits import (
                _get_user_scopes,
                _scope_cache,
            )

            _scope_cache.pop("user-2", None)
            assert await _get_user_scopes("user-2") is None

    @pytest.mark.asyncio
    async def test_definitive_empty_list_preserved(self):
        """A 200 with scopes: [] → [] (definitive), distinct from None."""
        with patch(
            f"{MODULE}._call_validate_for_user",
            new_callable=AsyncMock,
            return_value={"scopes": []},
        ):
            from src.server.dependencies.usage_limits import (
                _get_user_scopes,
                _scope_cache,
            )

            _scope_cache.pop("user-3", None)
            assert await _get_user_scopes("user-3") == []

    @pytest.mark.asyncio
    async def test_fail_open_result_gets_short_negative_ttl(self):
        """None (fail-open) is cached ~15 s; a definitive answer gets the full TTL."""
        import time

        from src.server.dependencies.usage_limits import (
            _SCOPE_CACHE_TTL,
            _get_user_scopes,
            _scope_cache,
        )

        with patch(
            f"{MODULE}._call_validate_for_user",
            new_callable=AsyncMock,
            return_value=None,
        ):
            _scope_cache.pop("user-4", None)
            await _get_user_scopes("user-4")
        assert _scope_cache["user-4"][1] - time.time() <= 15.5

        with patch(
            f"{MODULE}._call_validate_for_user",
            new_callable=AsyncMock,
            return_value={"scopes": ["workspace.spec.performance"]},
        ):
            _scope_cache.pop("user-4", None)
            await _get_user_scopes("user-4")
        assert _scope_cache["user-4"][1] - time.time() > _SCOPE_CACHE_TTL - 5
        _scope_cache.pop("user-4", None)


# ===================================================================
# spec_grantable — non-raising form of the full spec gate (scope 403 +
# count 429). True unless assert_spec_allowed would reject; fail-open in
# OSS mode. Confines HTTPException to this dependency layer so service
# callers can branch on a bool.
# ===================================================================


class TestSpecGrantable:
    @pytest.mark.asyncio
    async def test_standard_is_grantable(self):
        """standard is never gated → True without touching scopes/count."""
        with patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes:
            from src.server.dependencies.usage_limits import spec_grantable

            assert await spec_grantable("user-1", "standard") is True
            mock_scopes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_oss_mode_fails_open_true(self):
        """OSS mode: gate inactive → grantable, no scope fetch."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock) as mock_scopes,
        ):
            from src.server.dependencies.usage_limits import spec_grantable

            assert await spec_grantable("user-1", "max") is True
            mock_scopes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_grantable_when_gate_passes_and_forwards_current_tier(self):
        """Scope + count satisfied → True; current_tier forwarded verbatim."""
        with patch(
            f"{MODULE}.assert_spec_allowed", new_callable=AsyncMock
        ) as mock_assert:
            from src.server.dependencies.usage_limits import spec_grantable

            assert (
                await spec_grantable(
                    "user-1", "performance", current_tier="standard"
                )
                is True
            )
            mock_assert.assert_awaited_once_with(
                "user-1", "performance", current_tier="standard"
            )

    @pytest.mark.asyncio
    async def test_not_grantable_on_missing_scope_403(self):
        """assert raising 403 (missing scope) → False, not propagated."""
        with patch(
            f"{MODULE}.assert_spec_allowed",
            new=AsyncMock(
                side_effect=HTTPException(status_code=403, detail="not on plan")
            ),
        ):
            from src.server.dependencies.usage_limits import spec_grantable

            assert await spec_grantable("user-1", "max") is False

    @pytest.mark.asyncio
    async def test_not_grantable_on_count_429(self):
        """assert raising 429 (over per-tier count) → False.

        This is the count half a scope-only predicate would miss — a duplicate
        must not carry an elevated tier past the per-tier quota.
        """
        with patch(
            f"{MODULE}.assert_spec_allowed",
            new=AsyncMock(
                side_effect=HTTPException(status_code=429, detail="limit reached")
            ),
        ):
            from src.server.dependencies.usage_limits import spec_grantable

            assert (
                await spec_grantable(
                    "user-1", "performance", current_tier="standard"
                )
                is False
            )

    @pytest.mark.asyncio
    async def test_none_scopes_fails_open_true(self):
        """Unreachable scopes (None) + count no-op → fail-open True (never block)."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._get_user_scopes", new_callable=AsyncMock, return_value=None
            ),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock),
        ):
            from src.server.dependencies.usage_limits import spec_grantable

            assert (
                await spec_grantable("user-1", "max", current_tier="standard") is True
            )

    @pytest.mark.asyncio
    async def test_definitive_empty_scopes_not_grantable(self):
        """A platform-confirmed empty scope list is enforced, not treated as
        fail-open: the scope gate rejects, so spec_grantable is False."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._get_user_scopes", new_callable=AsyncMock, return_value=[]),
            patch(f"{MODULE}.enforce_capacity", new_callable=AsyncMock) as mock_cap,
        ):
            from src.server.dependencies.usage_limits import spec_grantable

            assert (
                await spec_grantable("user-1", "max", current_tier="standard") is False
            )
            mock_cap.assert_not_awaited()  # scope gate rejected first


# ===================================================================
# spec_entitlement_lost — lazy spec reclaim's check at (re)provision time.
# Shares _scope_entitlement_lost's fail-safe semantics: only a definitive
# "no spec scope" returns True; OSS, unreachable, and ambiguous responses
# keep the tier (return False). standard/unknown tiers are never reclaimed.
# ===================================================================


class TestSpecEntitlementLost:
    @pytest.mark.asyncio
    async def test_oss_mode_keeps_tier(self):
        """OSS mode never reconciles — returns False without calling validate."""
        with (
            patch(f"{MODULE}.HOST_MODE", "oss"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "max") is False
            mock_v.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unreachable_keeps_tier(self):
        """validate None (platform down / non-200) → fail-safe keep → False.
        An outage must never mass-reclaim elevated tiers."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock, return_value=None),
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "max") is False

    @pytest.mark.asyncio
    async def test_ambiguous_response_keeps_tier(self):
        """200 but scopes missing/None (not a list) → ambiguous → fail-safe keep."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"valid": True},
            ),
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "performance") is False

    @pytest.mark.asyncio
    async def test_scope_absent_reports_lost(self):
        """200 with a real scope list lacking the max scope → tier lost → True."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"scopes": ["workspace:spec:performance"]},
            ),
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "max") is True

    @pytest.mark.asyncio
    async def test_scope_present_keeps_tier(self):
        """200 with the performance scope present → still entitled → False."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(
                f"{MODULE}._call_validate_for_user",
                new_callable=AsyncMock,
                return_value={"scopes": ["workspace:spec:performance", "workspace:always_on"]},
            ),
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "performance") is False

    @pytest.mark.asyncio
    async def test_standard_and_unknown_tiers_never_reclaimed(self):
        """Ungated tiers short-circuit to False before any platform call."""
        with (
            patch(f"{MODULE}.HOST_MODE", "platform"),
            patch(f"{MODULE}.AUTH_SERVICE_URL", "http://localhost:8003"),
            patch(f"{MODULE}._call_validate_for_user", new_callable=AsyncMock) as mock_v,
        ):
            from src.server.dependencies.usage_limits import spec_entitlement_lost

            assert await spec_entitlement_lost("user-1", "standard") is False
            assert await spec_entitlement_lost("user-1", "titanium") is False
            mock_v.assert_not_awaited()
