"""
Tests for src/server/handlers/chat/admission_gate.py.

Covers:
- wait_or_steer: ready, steered, re-admitted, duplicate and 409 cases
- admission_conflict_detail: the single wording source for admission 409s
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# wait_or_steer
# ---------------------------------------------------------------------------


_LIVE_ROW = {"conversation_response_id": "run-live", "cancel_requested_at": None}
_NEXT_ROW = {"conversation_response_id": "run-next", "cancel_requested_at": None}
_STOPPING_ROW = {
    "conversation_response_id": "run-live",
    "cancel_requested_at": "2026-07-14T00:00:00+00:00",
}
_ADMIT = "src.server.database.runs.outbox.admit_steer"
_GET_JOB = "src.server.database.runs.outbox.get_steer_job"
_FIND_KEY = "src.server.database.runs.lifecycle.find_run_by_request_key"
_GET_RUN = "src.server.database.runs.lifecycle.get_run"
_KEY = "8c1d2e3f-4a5b-4c6d-8e7f-90a1b2c3d4e5"
_LIVE_RUN = {**_LIVE_ROW, "conversation_thread_id": "t-1", "status": "in_progress"}


def _admitting(*states):
    """A manager whose admission reads return ``states`` in order."""
    rows = {"running": _LIVE_ROW, "running-next": _NEXT_ROW, "stopping": _STOPPING_ROW}
    manager = AsyncMock()
    manager.wait_for_admission = AsyncMock(
        side_effect=[
            ("running" if s == "running-next" else s, rows.get(s)) for s in states
        ]
    )
    return manager


def _accepted(position=1):
    return {"state": "accepted", "job_id": "job-1", "position": position}


class TestWaitOrSteer:
    @pytest.fixture(autouse=True)
    def ledger(self):
        with (
            patch(_ADMIT, new_callable=AsyncMock, create=True) as admit,
            patch(_GET_JOB, new_callable=AsyncMock, return_value=None, create=True) as job,
            patch(_FIND_KEY, new_callable=AsyncMock, return_value=None) as find,
            patch(_GET_RUN, new_callable=AsyncMock, return_value=_LIVE_RUN) as get_run,
        ):
            admit.return_value = _accepted()
            yield SimpleNamespace(admit=admit, job=job, find=find, get_run=get_run)

    @pytest.mark.asyncio
    async def test_fresh_returns_true(self, ledger):
        from src.server.handlers.chat.admission_gate import wait_or_steer

        ready, event = await wait_or_steer(_admitting("fresh"), "t-1", "hello", "u-1")
        assert ready is True
        assert event is None
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_steer_only_fresh_raises_409_not_running(self):
        """A steer_only probe must never be admitted as a fresh turn — the
        probe's SSE reader ignores turn events, so a turn admitted on that
        connection streams its output (interrupts included) into a void.
        409 'not_running' routes the gateway to its resubmit path instead."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                _admitting("fresh"), "t-1", "hello", "u-1", steer_only=True
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

        ready, event = await wait_or_steer(
            _admitting("running"), "t-1", "hello", "u-1", steer_only=True
        )

        assert ready is False
        assert "steering_accepted" in event

    @pytest.mark.asyncio
    async def test_running_steers_immediately_with_no_wait(self, ledger):
        """A genuinely-running turn is steered at once, as a job naming that
        run, keyed by the send's request_key."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        manager = _admitting("running")
        ready, event = await wait_or_steer(
            manager,
            "t-1",
            "hello",
            "u-1",
            platform="telegram",
            request_key=_KEY,
            workspace_id="ws-1",
        )

        assert ready is False
        assert "steering_accepted" in event
        assert '"position": 1' in event
        kwargs = ledger.admit.await_args.kwargs
        assert (kwargs["thread_id"], kwargs["run_id"]) == ("t-1", "run-live")
        assert kwargs["idempotency_key"] == f"steer:{_KEY}"
        payload = kwargs["payload"]
        assert (
            payload["target"],
            payload["content"],
            payload["user_id"],
            payload["workspace_id"],
            payload["platform"],
            payload["request_key"],
        ) == ("thread", "hello", "u-1", "ws-1", "telegram", _KEY)
        manager.wait_for_admission.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unkeyed_steer_still_gets_a_unique_key(self, ledger):
        from src.server.handlers.chat.admission_gate import wait_or_steer

        await wait_or_steer(_admitting("running"), "t-1", "a", "u-1")
        await wait_or_steer(_admitting("running"), "t-1", "b", "u-1")

        first, second = (c.kwargs["idempotency_key"] for c in ledger.admit.await_args_list)
        assert first.startswith("steer:") and second.startswith("steer:")
        assert first != second

    @pytest.mark.asyncio
    async def test_a_steer_meeting_a_run_that_ended_admits_fresh(self, ledger):
        """The admission read said "running" but the run ended before the
        insert: nothing was written, and the next read finds the thread idle."""
        from src.server.handlers.chat.admission_gate import wait_or_steer

        ledger.admit.return_value = {"state": "not_running"}

        assert await wait_or_steer(
            _admitting("running", "fresh"), "t-1", "hello", "u-1"
        ) == (True, None)
        ledger.admit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_steer_meeting_a_run_that_ended_steer_only_raises_not_running(
        self, ledger
    ):
        """Same race under steer_only: the gateway resubmits it."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        ledger.admit.return_value = {"state": "not_running"}

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                _admitting("running", "fresh"), "t-1", "hello", "u-1", steer_only=True
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "not_running"

    @pytest.mark.asyncio
    async def test_a_steer_meeting_a_run_that_ended_steers_the_next_one(self, ledger):
        from src.server.handlers.chat.admission_gate import wait_or_steer

        ledger.admit.side_effect = [{"state": "not_running"}, _accepted(position=1)]

        ready, event = await wait_or_steer(
            _admitting("running", "running-next"), "t-1", "hello", "u-1"
        )

        assert ready is False and "steering_accepted" in event
        assert [c.kwargs["run_id"] for c in ledger.admit.await_args_list] == [
            "run-live",
            "run-next",
        ]

    @pytest.mark.asyncio
    async def test_a_steer_that_keeps_missing_runs_is_a_conflict(self, ledger):
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        ledger.admit.return_value = {"state": "not_running"}

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                _admitting("running", "running", "running"), "t-1", "hello", "u-1"
            )

        assert exc_info.value.detail["code"] == "running"

    @pytest.mark.asyncio
    async def test_a_run_another_owner_holds_is_a_conflict(self, ledger):
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        ledger.admit.return_value = {"state": "owned_elsewhere"}

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(_admitting("running"), "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert detail["code"] == "running"
        assert "t-1" not in detail["message"]

    @pytest.mark.asyncio
    async def test_stopping_raises_409_without_steering(self, ledger):
        """An explicitly-cancelled turn still winding down → 409 'stopping',
        never steered (a second checkpoint writer would corrupt state)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(_admitting("stopping"), "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so handle_workflow_error can tag the SSE error code.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "stopping"
        assert "t-1" not in detail["message"]
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_compacting_raises_409_without_steering(self, ledger):
        """A thread mid-compaction whose wait timed out → 409 'compacting',
        never steered (steering mid-summarize corrupts the context rewrite)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(_admitting("compacting"), "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so the client can recognize the transient state and
        # re-queue; no thread_id leaked into the user-facing message.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "compacting"
        assert "t-1" not in detail["message"]
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cannot_steer_running_is_a_conflict(self, ledger):
        """Dispatched flows (can_steer=False) never steer: a running peer is a
        409, not a steering_accepted — a second astream on a thread-keyed
        checkpointer would collide."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import wait_or_steer

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                _admitting("running"), "t-1", "hi", "u-1", can_steer=False
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "running"
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "messages",
        [[{"role": "user", "content": "the edited question"}], []],
        ids=["edit", "regenerate"],
    )
    async def test_a_fork_meeting_a_running_turn_is_a_conflict(self, ledger, messages):
        """An edit or a regenerate replaces a turn; steered into the running
        one it would add a message instead and truncate nothing."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission_gate import (
            steer_allowed,
            wait_or_steer,
        )
        from src.server.models.chat import ChatRequest

        request = ChatRequest(messages=messages, checkpoint_id="cp-3", fork_from_turn=3)

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                _admitting("running"),
                "t-1",
                "the edited question",
                "u-1",
                can_steer=steer_allowed(request, dispatched=False, steerable=True),
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "running"
        ledger.admit.assert_not_awaited()


class TestKeyedSendSteersOnce:
    """A keyed send reaches the conversation once, whichever way its first
    copy went in: as a turn of its own or as a steer into a running one."""

    @pytest.fixture(autouse=True)
    def ledger(self):
        with (
            patch(_ADMIT, new_callable=AsyncMock, create=True) as admit,
            patch(_GET_JOB, new_callable=AsyncMock, return_value=None, create=True) as job,
            patch(_FIND_KEY, new_callable=AsyncMock, return_value=None) as find,
            patch(_GET_RUN, new_callable=AsyncMock, return_value=_LIVE_RUN),
        ):
            admit.return_value = _accepted()
            yield SimpleNamespace(admit=admit, job=job, find=find)

    @staticmethod
    async def _send(steer_job_id=None):
        from src.server.handlers.chat.admission_gate import wait_or_steer

        return await wait_or_steer(
            _admitting("running"),
            "t-1",
            "hello",
            "u-1",
            request_key=_KEY,
            steer_job_id=steer_job_id,
        )

    @pytest.mark.asyncio
    async def test_a_copy_whose_first_copy_started_past_its_probe_is_not_steered(
        self, ledger
    ):
        """The first copy's START committed between this copy's duplicate
        probe and its admission read, so the run it sees running is its own."""
        from src.server.database.runs.lifecycle import DuplicateRequestError

        first_copy = {**_LIVE_ROW, "conversation_thread_id": "t-1"}
        ledger.find.return_value = first_copy

        with pytest.raises(DuplicateRequestError) as dup:
            await self._send()

        assert dup.value.existing_run is first_copy
        assert dup.value.steered is False
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_resend_of_a_steered_send_names_the_turn_that_took_it(
        self, ledger
    ):
        from src.server.database.runs.lifecycle import DuplicateRequestError

        ledger.job.return_value = {
            "id": "job-1",
            "status": "pending",
            "run_id": "run-live",
            "payload": {"request_key": _KEY, "steered_run_id": "run-live"},
        }

        with pytest.raises(DuplicateRequestError) as dup:
            await self._send()

        assert dup.value.existing_run is _LIVE_RUN
        assert dup.value.steered is True
        ledger.admit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_turn_a_parked_steer_starts_is_not_its_own_duplicate(
        self, ledger
    ):
        ledger.job.return_value = {
            "id": "job-1",
            "status": "pending",
            "run_id": None,
            "payload": {"request_key": _KEY},
        }

        ready, event = await self._send(steer_job_id="job-1")

        assert (ready, "steering_accepted" in event) == (False, True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("payload", "steered"),
        [
            ({"steered_run_id": "run-live"}, True),
            ({"dispatched_run_id": "run-live"}, False),
        ],
        ids=["steered", "dispatched-as-its-turn"],
    )
    async def test_a_copy_that_lost_the_insert_names_the_winner(
        self, ledger, payload, steered
    ):
        """Another copy admitted the key between the recheck and the insert.
        A steer dispatched as a turn of its own is that turn's input, not a
        message added to another turn."""
        from src.server.database.runs.lifecycle import DuplicateRequestError

        ledger.admit.return_value = {
            "state": "duplicate",
            "job": {"id": "job-1", "run_id": None, "payload": payload},
        }

        with pytest.raises(DuplicateRequestError) as dup:
            await self._send()

        assert dup.value.existing_run is _LIVE_RUN
        assert dup.value.steered is steered


class TestHandOffSteer:
    """A Chief of Staff hand-off that meets a running turn hands the dispatch
    primer the run it went into and its steer's row, which names the run
    that takes it in if that one ends first."""

    @pytest.fixture(autouse=True)
    def ledger(self):
        with (
            patch(_ADMIT, new_callable=AsyncMock, create=True) as admit,
            patch(_GET_JOB, new_callable=AsyncMock, return_value=None, create=True),
            patch(_FIND_KEY, new_callable=AsyncMock, return_value=None),
            patch(_GET_RUN, new_callable=AsyncMock, return_value=_LIVE_RUN),
        ):
            admit.return_value = _accepted()
            yield SimpleNamespace(admit=admit)

    @staticmethod
    async def _hand_off():
        from src.server.handlers.chat.admission_gate import AgentSteer, wait_or_steer

        agent = AgentSteer(
            sender="chief_of_staff",
            origin={"type": "agent", "agent": "chief_of_staff", "id": "cos-1"},
            idempotency_key="steer:dispatch:g-1",
            origin_stamp=None,
        )
        return await wait_or_steer(
            _admitting("running"), "t-1", "size the market", "u-1", agent=agent
        )

    @staticmethod
    def _copy_of(payload):
        """Another copy of the hand-off admitted its key first."""
        return {
            "state": "duplicate",
            "job": {"id": "job-1", "run_id": "run-live", "payload": payload},
        }

    @pytest.mark.parametrize(
        "copy",
        [None, {"steered_run_id": "run-live"}, {}],
        ids=["accepted", "copy-held-by-the-run", "copy-parked"],
    )
    @pytest.mark.asyncio
    async def test_a_hand_off_into_a_running_turn_names_the_run_and_the_steer(
        self, ledger, copy
    ):
        from src.server.handlers.chat.request_prep import HandOffSteered

        if copy is not None:
            ledger.admit.return_value = self._copy_of(copy)

        assert await self._hand_off() == (False, HandOffSteered("run-live", "job-1"))

    @pytest.mark.asyncio
    async def test_a_copy_whose_steer_became_its_own_turn_names_that_turn(
        self, ledger
    ):
        from src.server.database.runs.lifecycle import DuplicateRequestError

        ledger.admit.return_value = self._copy_of({"dispatched_run_id": "run-live"})

        with pytest.raises(DuplicateRequestError) as dup:
            await self._hand_off()

        assert dup.value.existing_run is _LIVE_RUN
        assert dup.value.steered is False


# ---------------------------------------------------------------------------
# admission_conflict_detail (single 409 wording source)
# ---------------------------------------------------------------------------


class TestAdmissionConflictDetail:
    """The single wording source for every in-generator admission 409 — the
    ``stopping``/``compacting``/``running`` conflicts and the ``steer_only``
    probe's ``not_running`` refusal. Same mapping for every turn, foreground
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
