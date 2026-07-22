"""
Tests for AutomationExecutor credential resolution and credit gating.

Covers:
- BYOK-only user: is_byok=True passed to astream_*, byok=True to gate
- OAuth-only user: treated as has_cred=True (not mis-gated as platform)
- Neither BYOK nor OAuth: byok=False to gate (platform daily-credit path)
- Zero-credit platform user: 429 from enforce_credit_limit gates before
  any workflow invocation and records the execution as failed
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.automation_executor import AutomationExecutor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USER_ID = "user-test-placeholder"
_AUTO_ID = "auto-test-placeholder"
_EXEC_ID = "exec-test-placeholder"
_WS_ID = "ws-test-placeholder"


def _make_automation(agent_mode="flash", **overrides):
    data = {
        "automation_id": _AUTO_ID,
        "user_id": _USER_ID,
        "agent_mode": agent_mode,
        "instruction": "Summarize today's market",
        "trigger_type": "cron",
        "workspace_id": _WS_ID if agent_mode == "ptc" else None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "llm_model": None,
        "additional_context": None,
        "trigger_config": None,
    }
    data.update(overrides)
    return data


async def _empty_async_gen(*args, **kwargs):
    """Empty async generator — stands in for astream_*_workflow."""
    return
    yield  # make it an async generator


# ---------------------------------------------------------------------------
# Base patch targets
# ---------------------------------------------------------------------------

_PATCHES = {
    "is_byok_active": "src.server.services.automation_executor.is_byok_active",
    "has_any_oauth": "src.server.services.automation_executor.has_any_oauth_token",
    "enforce_credit": "src.server.services.automation_executor.enforce_credit_limit",
    "auto_db": "src.server.services.automation_executor.auto_db",
    "flash_ws": "src.server.services.automation_executor.get_or_create_flash_workspace",
    "btm": "src.server.services.runs.executor.LocalRunExecutor",
    "fire_webhook": "src.server.services.automation_executor.AutomationExecutor._fire_webhook",
}


def _patch_all(
    is_byok=False,
    has_oauth=False,
    credit_raises=None,
):
    """Return a dict of patches to apply with context managers."""
    return {
        "is_byok_active": patch(
            _PATCHES["is_byok_active"], new=AsyncMock(return_value=is_byok)
        ),
        "has_any_oauth": patch(
            _PATCHES["has_any_oauth"], new=AsyncMock(return_value=has_oauth)
        ),
        "enforce_credit": patch(
            _PATCHES["enforce_credit"],
            new=AsyncMock(side_effect=credit_raises),
        ),
        "auto_db": patch(_PATCHES["auto_db"]),
        "flash_ws": patch(
            _PATCHES["flash_ws"],
            new=AsyncMock(return_value={"workspace_id": _WS_ID}),
        ),
        "fire_webhook": patch(
            _PATCHES["fire_webhook"], new=AsyncMock(return_value=None)
        ),
        "btm": patch(_PATCHES["btm"]),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCredentialGate:
    """AutomationExecutor passes correct is_byok and credit gate to workflows."""

    @pytest.mark.asyncio
    async def test_byok_only_user_flash(self):
        """BYOK-only user: enforce_credit called with byok=True, workflow gets is_byok=True."""
        patches = _patch_all(is_byok=True, has_oauth=False)

        with (
            patches["is_byok_active"] as mock_byok,
            patches["has_any_oauth"],
            patches["enforce_credit"] as mock_credit,
            patches["auto_db"] as mock_adb,
            patches["flash_ws"],
            patches["fire_webhook"],
            patches["btm"] as mock_btm_cls,
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            _setup_btm(mock_btm_cls)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="flash")
            await executor.execute(automation, _EXEC_ID)

        mock_byok.assert_awaited_once_with(_USER_ID)
        mock_credit.assert_awaited_once_with(_USER_ID, byok=True)

        _assert_astream_called_with_byok(mock_astream, expected_byok=True)

    @pytest.mark.asyncio
    async def test_oauth_only_user_flash(self):
        """OAuth-only user: credit gate sees has_cred=True (not mis-gated as a
        platform user), but the workflow gets is_byok=False — the BYOK ladder is
        not attempted for a user with no BYOK keys."""
        patches = _patch_all(is_byok=False, has_oauth=True)

        with (
            patches["is_byok_active"],
            patches["has_any_oauth"] as mock_oauth,
            patches["enforce_credit"] as mock_credit,
            patches["auto_db"] as mock_adb,
            patches["flash_ws"],
            patches["fire_webhook"],
            patches["btm"] as mock_btm_cls,
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            _setup_btm(mock_btm_cls)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="flash")
            await executor.execute(automation, _EXEC_ID)

        mock_oauth.assert_awaited_once_with(_USER_ID)
        # Credit gate keys off has_cred (BYOK or OAuth); workflow is_byok keys
        # off has_byok alone — OAuth-only ⟹ gate byok=True, workflow is_byok=False.
        mock_credit.assert_awaited_once_with(_USER_ID, byok=True)

        _assert_astream_called_with_byok(mock_astream, expected_byok=False)

    @pytest.mark.asyncio
    async def test_no_cred_user_flash(self):
        """Neither BYOK nor OAuth: enforce_credit called with byok=False (platform path)."""
        patches = _patch_all(is_byok=False, has_oauth=False)

        with (
            patches["is_byok_active"],
            patches["has_any_oauth"],
            patches["enforce_credit"] as mock_credit,
            patches["auto_db"] as mock_adb,
            patches["flash_ws"],
            patches["fire_webhook"],
            patches["btm"] as mock_btm_cls,
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            _setup_btm(mock_btm_cls)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="flash")
            await executor.execute(automation, _EXEC_ID)

        mock_credit.assert_awaited_once_with(_USER_ID, byok=False)

        _assert_astream_called_with_byok(mock_astream, expected_byok=False)

    @pytest.mark.asyncio
    async def test_byok_only_user_ptc(self):
        """BYOK-only user running PTC automation: workflow gets is_byok=True."""
        patches = _patch_all(is_byok=True, has_oauth=False)

        with (
            patches["is_byok_active"],
            patches["has_any_oauth"],
            patches["enforce_credit"] as mock_credit,
            patches["auto_db"] as mock_adb,
            patches["flash_ws"],
            patches["fire_webhook"],
            patches["btm"] as mock_btm_cls,
            patch(
                "src.server.handlers.chat.astream_ptc_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            _setup_btm(mock_btm_cls)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="ptc")
            await executor.execute(automation, _EXEC_ID)

        mock_credit.assert_awaited_once_with(_USER_ID, byok=True)
        _assert_astream_called_with_byok(mock_astream, expected_byok=True)

    @pytest.mark.asyncio
    async def test_zero_credit_platform_user_is_gated(self):
        """429 from enforce_credit_limit blocks the workflow and records failure."""
        exc = HTTPException(status_code=429, detail={"message": "daily credit limit", "type": "credit_limit"})
        patches = _patch_all(is_byok=False, has_oauth=False, credit_raises=exc)

        with (
            patches["is_byok_active"],
            patches["has_any_oauth"],
            patches["enforce_credit"],
            patches["auto_db"] as mock_adb,
            patches["flash_ws"],
            patches["fire_webhook"],
            patches["btm"] as mock_btm_cls,
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            _setup_btm(mock_btm_cls)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="flash")
            await executor.execute(automation, _EXEC_ID)

        # Workflow must NOT have been invoked
        mock_astream.assert_not_called()

        # Execution must be recorded as failed
        _assert_execution_marked_failed(mock_adb)

        # A credit-gate 429 is intentionally counted toward the failure count,
        # so a persistently zero-credit automation eventually auto-disables.
        mock_adb.increment_failure_count.assert_awaited_once()


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def _assert_astream_called_with_byok(mock_astream, *, expected_byok: bool):
    """Assert the astream mock was called exactly once with is_byok=expected_byok."""
    mock_astream.assert_called_once()
    _, kwargs = mock_astream.call_args
    assert "is_byok" in kwargs, "astream call missing is_byok kwarg"
    assert kwargs["is_byok"] is expected_byok, (
        f"expected is_byok={expected_byok}, got {kwargs['is_byok']}"
    )


def _assert_execution_marked_failed(mock_adb):
    """Assert update_execution_status was called with 'failed'."""
    calls = mock_adb.update_execution_status.call_args_list
    statuses = [c.args[1] if c.args else c.kwargs.get("status") for c in calls]
    assert "failed" in statuses, (
        f"Expected execution to be marked 'failed', got statuses: {statuses}"
    )


def _setup_auto_db(mock_adb):
    """Wire up common auto_db mock return values."""
    mock_adb.update_execution_status = AsyncMock()
    mock_adb.reset_failure_count = AsyncMock()
    mock_adb.increment_failure_count = AsyncMock()
    mock_adb.update_automation_next_run = AsyncMock()
    mock_adb.update_automation = AsyncMock()
    mock_adb.restore_executing_to_active = AsyncMock()


def _setup_btm(mock_btm_cls):
    """Wire up LocalRunExecutor mock."""
    mock_btm = MagicMock()
    mock_btm.wait_for_persistence = AsyncMock()
    mock_btm_cls.get_instance = MagicMock(return_value=mock_btm)
