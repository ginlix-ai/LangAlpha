"""
Tests for AutomationExecutor credential resolution and credit gating.

Covers:
- BYOK-only user: is_byok=True passed to astream_*, byok=True to gate
- OAuth-only user: treated as has_cred=True (not mis-gated as platform)
- Neither BYOK nor OAuth: byok=False to gate (platform daily-credit path)
- Zero-credit platform user: a usage-limit 429 from enforce_credit_limit
  gates before any workflow invocation and settles the firing limited, with
  the quota service's message verbatim and no strike
- Our own capacity (a burst 429, a 503) settles failed with no strike
- A turn the ledger admitted is announced once, carries the firing on its
  run row, and is left to that run to settle
"""

from contextlib import contextmanager
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


async def _one_event_gen(*args, **kwargs):
    """A turn that streams one event after its run was admitted."""
    yield "event: message_chunk\ndata: {}\n\n"


# ---------------------------------------------------------------------------
# Base patch targets
# ---------------------------------------------------------------------------

_PATCHES = {
    "is_byok_active": "src.server.services.automation_executor.is_byok_active",
    "has_any_oauth": "src.server.services.automation_executor.has_any_oauth_token",
    "enforce_credit": "src.server.services.automation_executor.enforce_credit_limit",
    "auto_db": "src.server.services.automation_executor.auto_db",
    "settlement_db": "src.server.services.automation_settlement.auto_db",
    "flash_ws": "src.server.services.automation_executor.get_or_create_flash_workspace",
    "get_run": "src.server.database.runs.lifecycle.get_run",
}


@contextmanager
def _webhook_of(module):
    """Stub the ``WebhookClient`` a module sends through; yields its
    ``fire_event``."""
    fire = AsyncMock(return_value=None)
    with patch(f"{module}.WebhookClient", return_value=MagicMock(fire_event=fire)):
        yield fire


def _patch_all(
    is_byok=False,
    has_oauth=False,
    credit_raises=None,
):
    """Return a dict of patches to apply with context managers.

    The executor and the settlement each bind the DB module; both names get
    one mock, so every write lands in one place.
    """
    db = MagicMock()
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
        "auto_db": patch(_PATCHES["auto_db"], new=db),
        "settlement_db": patch(_PATCHES["settlement_db"], new=db),
        "flash_ws": patch(
            _PATCHES["flash_ws"],
            new=AsyncMock(return_value={"workspace_id": _WS_ID}),
        ),
        "webhook": _webhook_of("src.server.services.automation_executor"),
        "settlement_webhook": _webhook_of("src.server.services.automation_settlement"),
        "get_run": patch(_PATCHES["get_run"], new=AsyncMock(side_effect=_run_row)),
    }


def _run_row(run_id):
    return {
        "conversation_response_id": run_id,
        "status": "completed",
        "metadata": {},
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
            patches["settlement_db"],
            patches["flash_ws"],
            patches["webhook"],
            patches["settlement_webhook"],
            patches["get_run"],
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)

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
            patches["settlement_db"],
            patches["flash_ws"],
            patches["webhook"],
            patches["settlement_webhook"],
            patches["get_run"],
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)

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
            patches["settlement_db"],
            patches["flash_ws"],
            patches["webhook"],
            patches["settlement_webhook"],
            patches["get_run"],
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)

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
            patches["settlement_db"],
            patches["flash_ws"],
            patches["webhook"],
            patches["settlement_webhook"],
            patches["get_run"],
            patch(
                "src.server.handlers.chat.astream_ptc_workflow",
                side_effect=_empty_async_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)

            executor = AutomationExecutor()
            automation = _make_automation(agent_mode="ptc")
            await executor.execute(automation, _EXEC_ID)

        mock_credit.assert_awaited_once_with(_USER_ID, byok=True)
        _assert_astream_called_with_byok(mock_astream, expected_byok=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "detail, message",
        [
            ({"message": "daily credit limit", "type": "credit_limit"}, "daily credit limit"),
            ({"message": "Weekly limit reached.", "type": "weekly_limit"}, "Weekly limit reached."),
            # A denial with no words gets the credit gate's own.
            ({"type": "credit_limit"}, "Stopped by the credit gate."),
        ],
    )
    async def test_zero_credit_platform_user_is_gated(self, detail, message):
        """A usage limit blocks the workflow and settles the firing limited.

        The user's to act on, so it never counts toward auto-disable, and the
        quota service's words are relayed as they are.
        """
        mock_adb, mock_astream, mock_settled = await _gated(
            HTTPException(status_code=429, detail=detail)
        )

        mock_astream.assert_not_called()
        _assert_execution_marked_failed(mock_adb)
        settled = mock_adb.settle_execution.await_args.kwargs
        assert settled["strike"] is None
        assert settled["failure_reason"] == "usage_limit"
        assert settled["error_message"] == message
        assert mock_settled.await_args.kwargs["error"] == message
        assert mock_settled.await_args.kwargs["failure_reason"] == "usage_limit"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code, kind",
        [(503, "service_unavailable"), (429, "service_unavailable"), (429, "burst_limit")],
    )
    async def test_our_own_outage_does_not_burn_a_strike(self, status_code, kind):
        """The credit gate fails closed on 503, and that must not auto-disable.

        The quota service restarts on every deploy. If a restart landing on a
        schedule window counted as this automation's failure, a few unlucky
        coincidences would quietly switch off something the user built, for a
        reason that was never theirs. A burst 429 is our capacity the same way.
        """
        mock_adb, mock_astream, _ = await _gated(
            HTTPException(
                status_code=status_code,
                detail={"message": "Service temporarily unavailable.", "type": kind},
            )
        )

        mock_astream.assert_not_called()
        # The run still didn't happen, so the execution row is honest about it.
        _assert_execution_marked_failed(mock_adb)
        settled = mock_adb.settle_execution.await_args.kwargs
        # But the automation itself is not held responsible.
        assert settled["strike"] is None
        assert settled["failure_reason"] == "server_error"
        assert settled["error_message"].startswith("HTTPException: ")


async def _gated(exc):
    """Fire an automation whose credit gate raises ``exc``."""
    patches = _patch_all(is_byok=False, has_oauth=False, credit_raises=exc)
    with (
        patches["is_byok_active"],
        patches["has_any_oauth"],
        patches["enforce_credit"],
        patches["auto_db"] as mock_adb,
        patches["settlement_db"],
        patches["flash_ws"],
        patches["webhook"],
        patches["settlement_webhook"] as mock_settled,
        patches["get_run"],
        patch(
            "src.server.handlers.chat.astream_flash_workflow",
            side_effect=_empty_async_gen,
        ) as mock_astream,
    ):
        _setup_auto_db(mock_adb)
        await AutomationExecutor().execute(_make_automation(agent_mode="flash"), _EXEC_ID)
    return mock_adb, mock_astream, mock_settled


class TestAdmitted:
    """A turn the ledger admitted."""

    @pytest.mark.asyncio
    async def test_an_admitted_run_is_announced_and_left_to_its_run(self):
        patches = _patch_all()

        with (
            patches["is_byok_active"],
            patches["has_any_oauth"],
            patches["enforce_credit"],
            patches["auto_db"] as mock_adb,
            patches["settlement_db"],
            patches["flash_ws"],
            patches["webhook"] as mock_started,
            patches["settlement_webhook"] as mock_settled,
            patches["get_run"] as mock_get_run,
            patch(
                "src.server.handlers.chat.astream_flash_workflow",
                side_effect=_one_event_gen,
            ) as mock_astream,
        ):
            _setup_auto_db(mock_adb)
            mock_get_run.side_effect = lambda run_id: {
                **_run_row(run_id), "status": "in_progress"
            }
            await AutomationExecutor().execute(_make_automation(), _EXEC_ID)

        run_id = mock_astream.call_args.kwargs["run_id"]
        # The reader's turn is never steered into: an automation's turn is
        # its own.
        assert mock_astream.call_args.kwargs["steerable"] is False
        # What the run's finalize settles the firing by.
        assert mock_astream.call_args.kwargs["run_metadata"] == {
            "automation_execution_id": _EXEC_ID,
            "automation_id": _AUTO_ID,
        }
        mock_get_run.assert_any_await(run_id)

        # Admission recorded the run and announced the start with it.
        recorded = [
            c.kwargs for c in mock_adb.transition_execution.call_args_list
            if c.kwargs.get("conversation_response_id")
        ]
        assert recorded == [
            {"from_statuses": ("running",), "to": "running", "conversation_response_id": run_id}
        ]
        assert mock_started.await_args.args[0] == "automation.started"
        assert mock_started.await_args.kwargs["run_id"] == run_id

        mock_adb.settle_execution.assert_not_awaited()
        mock_settled.assert_not_awaited()


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
    """Assert the firing settled as 'failed'."""
    mock_adb.settle_execution.assert_awaited_once()
    assert mock_adb.settle_execution.await_args.kwargs["to"] == "failed"


def _setup_auto_db(mock_adb):
    """Wire up common auto_db mock return values."""
    mock_adb.transition_execution = AsyncMock(
        return_value={"conversation_thread_id": None}
    )
    mock_adb.settle_execution = AsyncMock(
        return_value={
            "conversation_thread_id": None,
            "conversation_response_id": None,
            "settled_from": "running",
        }
    )
    mock_adb.record_delivery = AsyncMock()
    mock_adb.update_automation = AsyncMock()
