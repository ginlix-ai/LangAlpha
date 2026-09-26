"""
Tests for src/server/handlers/chat/admission_gate.py.

Covers:
- wait_or_steer: ready, steered, reclaimed, and 409 cases
- admission_conflict_detail: the single wording source for admission 409s
"""

from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# wait_or_steer
# ---------------------------------------------------------------------------


_LIVE_ROW = {"conversation_response_id": "run-live", "cancel_requested_at": None}
_STOPPING_ROW = {
    "conversation_response_id": "run-live",
    "cancel_requested_at": "2026-07-14T00:00:00+00:00",
}


class TestWaitOrSteer:
    @pytest.fixture(autouse=True)
    def ledger_slot(self):
        """The ledger slot read behind wait_or_steer's accept-after-exit
        reclaim (v4 2.4c — worker-agnostic, replaces the local task check).
        Defaults to a live row (no reclaim); exit-race tests set it None."""
        with patch(
            "src.server.database.runs.lifecycle.get_active_run",
            new_callable=AsyncMock,
            return_value=_LIVE_ROW,
        ) as slot:
            yield slot

    @pytest.mark.asyncio
    async def test_fresh_returns_true(self):
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("fresh", None))

        ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")
        assert ready is True
        assert event is None

    @pytest.mark.asyncio
    async def test_steer_only_fresh_raises_409_not_running(self):
        """A steer_only probe must never be admitted as a fresh turn — the
        probe's SSE reader ignores turn events, so a turn admitted on that
        connection streams its output (interrupts included) into a void.
        409 'not_running' routes the gateway to its resubmit path instead."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("fresh", None))

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                manager, "t-1", "hello", "u-1", steer_only=True
            )

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "not_running"
        assert "t-1" not in detail["message"]

    @pytest.mark.asyncio
    async def test_steer_only_still_steers_running(self):
        """steer_only forbids only the fresh-admission fallback; a
        genuinely-running turn steers exactly as before."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        # Live slot (fixture default) → no accept-after-exit reclaim.

        with patch(
            "src.server.handlers.chat.steering.steer_thread",
            new_callable=AsyncMock,
            return_value={"position": 2, "payload": "QUEUED_JSON"},
        ):
            ready, event = await wait_or_steer(
                manager, "t-1", "hello", "u-1", steer_only=True
            )

        assert ready is False
        assert "steering_accepted" in event

    @pytest.mark.asyncio
    async def test_steer_accept_racing_exit_reclaims_and_admits_fresh(self, ledger_slot):
        """The admission snapshot said "running" but the workflow exited before
        the Redis push landed: the message must be reclaimed and the POST
        routed as a fresh turn — never a false steering_accepted for a
        message nothing will consume."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None  # slot emptied between snapshot and push

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=True,
            ) as mock_unsteer,
        ):
            ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert ready is True
        assert event is None
        mock_unsteer.assert_awaited_once_with("t-1", "QUEUED_JSON")

    @pytest.mark.asyncio
    async def test_steer_accept_racing_exit_steer_only_raises_not_running(
        self, ledger_slot
    ):
        """Same race under steer_only: a reclaimed accept surfaces as
        not_running so the gateway resubmits — not as a fresh admission."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await wait_or_steer(
                    manager, "t-1", "hello", "u-1", steer_only=True
                )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "not_running"

    @pytest.mark.asyncio
    async def test_steer_accept_drained_by_exit_still_reports_accepted(
        self, ledger_slot
    ):
        """If the reclaim misses, the exiting workflow's final drain got the
        message first and steering_returned already carried it back on the
        turn stream — the POST keeps the queue contract and reports
        accepted."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert ready is False
        assert "steering_accepted" in event

    @pytest.mark.asyncio
    async def test_running_steers_immediately_with_no_wait(self):
        """CRITICAL regression: a genuinely-running turn is steered immediately
        — admission returns "running" and steer_thread is called without any
        wait on the running task."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        # Live slot (fixture default) → no accept-after-exit reclaim.

        with patch(
            "src.server.handlers.chat.steering.steer_thread",
            new_callable=AsyncMock,
            return_value={"position": 1, "payload": "QUEUED_JSON"},
        ) as mock_steer:
            ready, event = await wait_or_steer(
                manager, "t-1", "hello", "u-1"
            )

        assert ready is False
        assert event is not None
        assert "steering_accepted" in event
        assert '"position": 1' in event
        # Stamped with the live run so the middleware's own-run filter
        # delivers it (v4 2.4c).
        mock_steer.assert_awaited_once_with("t-1", "hello", "u-1", run_id="run-live")
        # No wait helper exists anymore — admission decided immediately.
        manager.wait_for_admission.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stopping_raises_409_without_steering(self):
        """An explicitly-cancelled turn still winding down → 409 'stopping',
        never steered (a second checkpoint writer would corrupt state)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("stopping", _STOPPING_ROW))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so handle_workflow_error can tag the SSE error code.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "stopping"
        assert "t-1" not in detail["message"]
        mock_steer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raises_409_when_steering_fails(self):
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value=None,
            ),
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "running"
        assert "t-1" not in detail["message"]

    @pytest.mark.asyncio
    async def test_compacting_raises_409_without_steering(self):
        """A thread mid-compaction whose wait timed out → 409 'compacting',
        never steered (steering mid-summarize corrupts the context rewrite)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("compacting", None))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so the client can recognize the transient state and
        # re-queue; no thread_id leaked into the user-facing message.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "compacting"
        assert "t-1" not in detail["message"]
        mock_steer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cannot_steer_running_is_a_conflict(self):
        """Dispatched flows (can_steer=False) never steer: a running peer is a
        409, not a steering_accepted — a second astream on a thread-keyed
        checkpointer would collide."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hi", "u-1", can_steer=False)

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "running"
        mock_steer.assert_not_awaited()


# ---------------------------------------------------------------------------
# admission_conflict_detail (single 409 wording source)
# ---------------------------------------------------------------------------


class TestAdmissionConflictDetail:
    """The single wording source for every in-generator admission 409 — the
    ``stopping``/``compacting``/``running`` conflicts and the ``steer_only``
    probe's ``not_running`` refusal. Same mapping for PTC and Flash, foreground
    and dispatched."""

    def test_compacting_state_names_compaction(self):
        from src.server.handlers.chat.admission_gate import admission_conflict_detail

        detail = admission_conflict_detail("compacting")
        # Structured detail (code + message) so handle_workflow_error can tag the
        # SSE error with a code and the client can recognize a transient state.
        assert isinstance(detail, dict)
        assert detail["code"] == "compacting"
        assert "compacting" in detail["message"].lower()
        assert "retry" in detail["message"].lower() or "resend" in detail["message"].lower()

    def test_stopping_state_names_stopping(self):
        from src.server.handlers.chat.admission_gate import admission_conflict_detail

        detail = admission_conflict_detail("stopping")
        assert isinstance(detail, dict)
        assert detail["code"] == "stopping"
        assert "stopping" in detail["message"].lower()

    def test_not_running_names_the_steer_refusal(self):
        from src.server.handlers.chat.admission_gate import admission_conflict_detail

        # The steer_only probe against an idle thread: not a wait_for_admission
        # state, but routed through the same mapper so the wording lives here too.
        detail = admission_conflict_detail("not_running")
        assert isinstance(detail, dict)
        assert detail["code"] == "not_running"
        assert "running" in detail["message"].lower()

    def test_running_state_is_the_generic_fallback(self):
        from src.server.handlers.chat.admission_gate import admission_conflict_detail

        # Any other non-fresh state (e.g. "running") falls through to the
        # "still running" code.
        detail = admission_conflict_detail("running")
        assert isinstance(detail, dict)
        assert detail["code"] == "running"
        assert "still running" in detail["message"].lower()
        # The actionable hint (reconnect / cancel) is part of the contract —
        # the web UI renders this message verbatim, so keep it pinned.
        assert "reconnect" in detail["message"].lower()
        assert "cancel" in detail["message"].lower()

    def test_unknown_state_falls_back_to_generic(self):
        from src.server.handlers.chat.admission_gate import admission_conflict_detail

        detail = admission_conflict_detail("wedged")
        assert detail["code"] == "running"
        assert "still running" in detail["message"].lower()
