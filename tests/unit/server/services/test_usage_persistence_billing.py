"""
Tests for UsagePersistenceService billing/credit computation.

Verifies that:
- track_llm_usage derives _token_credits from platform_cost, not total_cost
- _has_platform_calls flag is set correctly
- persist_usage computes effective_is_byok from per-call billing data
"""

from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

CORE_MODULE = "src.utils.tracking.core"
DB_MODULE = "src.server.database.conversation"


def _make_service(**kwargs):
    from src.server.services.persistence.usage import UsagePersistenceService
    defaults = {
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "user_id": "user-1",
    }
    defaults.update(kwargs)
    return UsagePersistenceService(**defaults)


# ===================================================================
# Test 3: UsagePersistenceService credit computation
# ===================================================================


class TestTrackLlmUsageBilling:
    """track_llm_usage sets credits from platform_cost, not total_cost."""

    @pytest.mark.asyncio
    async def test_token_credits_from_platform_cost(self):
        svc = _make_service()

        mock_result = {
            "by_model": {"m": {}},
            "total_cost": 0.10,       # $0.10 total
            "platform_cost": 0.04,    # $0.04 platform only
            "cost_breakdown": {},
            "per_call_costs": [],
        }

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            return_value=mock_result,
        ):
            await svc.track_llm_usage([{"dummy": "record"}])

        # Credits = platform_cost * 1000 (default rate)
        expected = Decimal("0.04") * Decimal("1000")
        assert svc._token_credits == expected

    @pytest.mark.asyncio
    async def test_has_platform_calls_true_when_platform_cost_positive(self):
        svc = _make_service()

        mock_result = {
            "by_model": {},
            "total_cost": 0.10,
            "platform_cost": 0.06,
            "cost_breakdown": {},
            "per_call_costs": [],
        }

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            return_value=mock_result,
        ):
            await svc.track_llm_usage([{"dummy": "record"}])

        assert svc._has_platform_calls is True

    @pytest.mark.asyncio
    async def test_has_platform_calls_false_when_no_platform_cost(self):
        svc = _make_service()

        mock_result = {
            "by_model": {},
            "total_cost": 0.10,
            "platform_cost": 0.0,   # all BYOK/OAuth
            "cost_breakdown": {},
            "per_call_costs": [],
        }

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            return_value=mock_result,
        ):
            await svc.track_llm_usage([{"dummy": "record"}])

        assert svc._has_platform_calls is False

    @pytest.mark.asyncio
    async def test_token_credits_zero_when_all_byok(self):
        svc = _make_service()

        mock_result = {
            "by_model": {},
            "total_cost": 0.50,
            "platform_cost": 0.0,
            "cost_breakdown": {},
            "per_call_costs": [],
        }

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            return_value=mock_result,
        ):
            await svc.track_llm_usage([{"dummy": "record"}])

        assert svc._token_credits == Decimal("0.0")


class TestPersistUsageEffectiveByok:
    """persist_usage overrides is_byok based on _has_platform_calls."""

    async def _persist_and_capture(self, svc, *, is_byok=False, has_token_usage=True):
        """Run persist_usage and return the usage_data dict passed to create_usage_record."""
        if has_token_usage:
            svc._token_usage = {"by_model": {}, "total_cost": 0.0}
        else:
            svc._token_usage = None

        mock_create = AsyncMock()

        @asynccontextmanager
        async def mock_get_conn():
            yield MagicMock()

        with patch(
            f"{DB_MODULE}.create_usage_record", mock_create
        ), patch(
            "src.server.database.pool.get_db_connection", mock_get_conn
        ):
            await svc.persist_usage(response_id="resp-1", is_byok=is_byok)

        assert mock_create.called, "create_usage_record should have been called"
        usage_data = mock_create.call_args[0][0]
        return usage_data

    @pytest.mark.asyncio
    async def test_effective_byok_true_when_no_platform_calls(self):
        """Even if caller says is_byok=False, effective_is_byok=True when no platform calls."""
        svc = _make_service()
        svc._has_platform_calls = False

        data = await self._persist_and_capture(svc, is_byok=False)
        assert data["is_byok"] is True

    @pytest.mark.asyncio
    async def test_effective_byok_false_when_platform_calls_exist(self):
        """Even if caller says is_byok=True, effective_is_byok=False when platform calls exist."""
        svc = _make_service()
        svc._has_platform_calls = True

        data = await self._persist_and_capture(svc, is_byok=True)
        assert data["is_byok"] is False

    @pytest.mark.asyncio
    async def test_caller_byok_used_when_no_token_usage(self):
        """When _token_usage is None (no LLM calls), fall back to caller's is_byok hint."""
        svc = _make_service()
        svc._has_platform_calls = False  # irrelevant -- no token_usage

        data = await self._persist_and_capture(svc, is_byok=True, has_token_usage=False)
        assert data["is_byok"] is True

        svc2 = _make_service()
        svc2._has_platform_calls = True

        data2 = await self._persist_and_capture(svc2, is_byok=False, has_token_usage=False)
        assert data2["is_byok"] is False


class TestTrackLlmUsageErrorPath:
    """Error path in track_llm_usage must not corrupt is_byok derivation."""

    @pytest.mark.asyncio
    async def test_error_path_leaves_token_usage_none(self):
        """When calculate_cost throws, _token_usage stays None so persist_usage
        falls back to the caller's is_byok hint instead of _has_platform_calls."""
        svc = _make_service()

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            side_effect=ValueError("boom"),
        ):
            result = await svc.track_llm_usage([{"dummy": "record"}])

        # Caller gets valid return value
        assert result["total_cost"] == 0.0
        # _token_usage stays None — prevents truthy check from overriding is_byok
        assert svc._token_usage is None
        assert svc._token_credits == Decimal("0.0")

    @pytest.mark.asyncio
    async def test_error_path_persist_uses_caller_hint(self):
        """After track_llm_usage error, persist_usage uses caller's is_byok, not _has_platform_calls."""
        svc = _make_service()

        with patch(
            f"{CORE_MODULE}.calculate_cost_from_per_call_records",
            side_effect=ValueError("boom"),
        ):
            await svc.track_llm_usage([{"dummy": "record"}])

        # Platform user: is_byok=False should be preserved
        mock_create = AsyncMock()

        @asynccontextmanager
        async def mock_get_conn():
            yield MagicMock()

        with patch(
            f"{DB_MODULE}.create_usage_record", mock_create
        ), patch(
            "src.server.database.pool.get_db_connection", mock_get_conn
        ):
            await svc.persist_usage(response_id="resp-1", is_byok=False)

        usage_data = mock_create.call_args[0][0]
        # Should be False (caller hint), not True (_has_platform_calls=False)
        assert usage_data["is_byok"] is False


class TestResetClearsState:
    """reset() must clear all billing state including _has_platform_calls."""

    def test_reset_clears_has_platform_calls(self):
        svc = _make_service()
        svc._has_platform_calls = True
        svc._token_credits = Decimal("42.0")
        svc._token_usage = {"by_model": {}}

        svc.reset()

        assert svc._has_platform_calls is False
        assert svc._token_credits == Decimal("0.0")
        assert svc._token_usage is None
