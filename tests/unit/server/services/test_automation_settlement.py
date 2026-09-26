"""What ending a firing does, per outcome and trigger type.

Locks the settlement contract: the row status each outcome records, which
outcomes strike, disable at once, reset, close the schedule or only re-arm a
price alert (all inside the one settling transaction), the failure reason the
user acts on, which webhook goes out and with which run, who clears the wait
notice, the metric label, and that a writer who loses the row does none of it.
Then how a run's ledger row decides the outcome and what it says, how the
run's finalize job settles the firing it ran for, once, and how the sweep
settles a firing whose process went away.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.automation_settlement import (
    INTERRUPTED_ERROR,
    RUN_FAILURES,
    STOPPED_ERROR,
    Outcome,
    _settle_finished_run,
    drain_tails,
    ledger_outcome,
    run_failure_message,
    settle,
    settle_abandoned,
)

_MOD = "src.server.services.automation_settlement"
_AID = "auto-1"
_EID = "exec-1"
_THREAD = "thread-1"
_RUN = "run-1"


def _price(mode):
    return {"symbol": "AAPL", "conditions": [{"type": "price_above", "value": 1}], "retrigger": {"mode": mode}}


TRIGGERS = {
    "cron": {"trigger_type": "cron"},
    "once": {"trigger_type": "once"},
    "price_one_shot": {"trigger_type": "price", "trigger_config": _price("one_shot")},
    "price_recurring": {"trigger_type": "price", "trigger_config": _price("recurring")},
}

# The schedule action the settling transaction takes.
CLOSES = {"cron": None, "once": "close_once", "price_one_shot": "close_price", "price_recurring": "rearm_price"}
REARMS = {"cron": None, "once": None, "price_one_shot": "rearm_price", "price_recurring": "rearm_price"}
# A usage limit ends a one-shot alert, which would otherwise fire into the
# same limit again; a one-time automation keeps its schedule to be retimed.
CLOSES_ALERT = {**REARMS, "price_one_shot": "close_price"}

_ANY = ("pending", "waiting", "running")

# outcome: row status, settles from, strike, schedule, webhook, skip reason,
# failure reason, metric
EXPECTED = {
    Outcome.COMPLETED: ("completed", ("running",), "reset", CLOSES, "automation.completed", None, None, "success"),
    Outcome.FAILED: ("failed", _ANY, "count", REARMS, "automation.failed", None, None, "failure"),
    Outcome.KEY_REJECTED: ("failed", _ANY, "fuse", REARMS, "automation.failed", None, "provider_auth", "failure"),
    Outcome.LIMITED: ("failed", _ANY, None, CLOSES_ALERT, "automation.failed", None, "usage_limit", "limited"),
    Outcome.FAILED_OURS: ("failed", _ANY, None, REARMS, "automation.failed", None, "server_error", "failure"),
    Outcome.INTERRUPTED: ("failed", ("pending", "running"), None, REARMS, "automation.failed", None, "interrupted", "interrupted"),
    Outcome.STOPPED: ("skipped", ("running",), None, CLOSES, "automation.failed", "user", None, "stopped"),
    Outcome.SKIPPED: ("skipped", ("waiting",), None, CLOSES, None, None, None, "skipped"),
}


def _automation(trigger="price_recurring"):
    return {"automation_id": _AID, "user_id": "user-1", **TRIGGERS[trigger]}


@contextmanager
def _settlement(row):
    fx = SimpleNamespace(
        db=MagicMock(
            settle_execution=AsyncMock(return_value=row),
            record_delivery=AsyncMock(),
        ),
        fire=AsyncMock(return_value=[{"method": "slack", "success": True}]),
        announce=AsyncMock(),
        metric=MagicMock(),
    )
    with (
        patch(f"{_MOD}.auto_db", new=fx.db),
        patch(f"{_MOD}.fire_webhook", new=fx.fire),
        patch(f"{_MOD}.announce_wait", new=fx.announce),
        patch(f"{_MOD}.safe_add", new=fx.metric),
    ):
        yield fx


def _row(
    settled_from="running", run=_RUN, previous_failure_reason=None,
    previous_delivery_result=None,
):
    return {
        "conversation_thread_id": _THREAD,
        "conversation_response_id": run,
        "settled_from": settled_from,
        "previous_failure_reason": previous_failure_reason,
        "previous_delivery_result": previous_delivery_result,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", TRIGGERS)
@pytest.mark.parametrize("outcome", list(Outcome))
async def test_outcome_effects(outcome, trigger):
    (
        status, from_statuses, strike, schedule, webhook, skip_reason,
        failure_reason, metric,
    ) = EXPECTED[outcome]
    error = "boom" if outcome in RUN_FAILURES else None
    with _settlement(_row()) as fx:
        assert await settle(_automation(trigger), _EID, outcome, error=error)

    written = fx.db.settle_execution.await_args.kwargs
    assert fx.db.settle_execution.await_args.args == (_EID,)
    assert written["automation_id"] == _AID
    assert written["to"] == status
    assert written["from_statuses"] == from_statuses
    assert written["strike"] == strike
    assert written["schedule"] == schedule[trigger]
    assert written["skip_reason"] == skip_reason
    assert written["failure_reason"] == failure_reason
    assert written["quiet_for"] is None
    if outcome is Outcome.INTERRUPTED:
        assert written["error_message"] == INTERRUPTED_ERROR
    if outcome is Outcome.STOPPED:
        assert written["error_message"] == STOPPED_ERROR

    if webhook:
        assert fx.fire.await_args.args[0] == webhook
        assert fx.fire.await_args.kwargs["error"] == written["error_message"]
        # The run the row records, when the caller named none.
        assert fx.fire.await_args.kwargs["run_id"] == _RUN
        assert fx.fire.await_args.kwargs["failure_reason"] == failure_reason
        fx.db.record_delivery.assert_awaited_once()
    else:
        fx.fire.assert_not_awaited()
        fx.db.record_delivery.assert_not_awaited()
    fx.announce.assert_not_awaited()
    assert fx.metric.call_args.args[2]["status"] == metric


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [Outcome.SKIPPED, Outcome.FAILED])
async def test_leaving_the_line_clears_the_wait_notice(outcome):
    with _settlement(_row(settled_from="waiting")) as fx:
        assert await settle(_automation(), _EID, outcome)
    fx.announce.assert_awaited_once_with("user-1", _THREAD, _EID, waiting=False)


@pytest.mark.asyncio
async def test_the_webhook_names_the_run_it_settled():
    with _settlement(_row(run=None)) as fx:
        await settle(_automation(), _EID, Outcome.COMPLETED, run_id="run-2")
    assert fx.fire.await_args.kwargs["run_id"] == "run-2"
    assert fx.db.settle_execution.await_args.kwargs["conversation_response_id"] == "run-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome, reason",
    [(Outcome.LIMITED, "usage_limit"), (Outcome.FAILED_OURS, "server_error")],
)
@pytest.mark.parametrize(
    "run, previous, delivery, notified",
    [
        # Refused before its turn, as the firing before it was: the channel
        # already heard, or that firing was itself a repeat.
        (None, "same", [{"method": "slack", "success": True}], False),
        (None, "same", None, False),
        (None, None, None, True),
        (None, "provider_auth", None, True),
        # The notice before this one never landed.
        (None, "same", [{"method": "slack", "success": False}], True),
        # Admitted: its started notice needs the terminal event.
        (_RUN, "same", None, True),
    ],
)
async def test_a_repeated_refusal_before_admission_is_not_announced_again(
    outcome, reason, run, previous, delivery, notified
):
    previous = reason if previous == "same" else previous
    row = _row(run=run, previous_failure_reason=previous, previous_delivery_result=delivery)
    with _settlement(row) as fx:
        assert await settle(_automation(), _EID, outcome, error="Refused.")
    assert fx.db.settle_execution.await_args.kwargs["failure_reason"] == reason
    assert fx.db.settle_execution.await_args.kwargs["error_message"] == "Refused."
    assert fx.fire.await_count == int(notified)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome, previous",
    [
        # Each streak is its own: our outage after a limit is news.
        (Outcome.FAILED_OURS, "usage_limit"),
        (Outcome.LIMITED, "server_error"),
        # Only a limit or our outage comes in streaks.
        (Outcome.FAILED, "usage_limit"),
        (Outcome.INTERRUPTED, "interrupted"),
    ],
)
async def test_only_a_repeat_of_the_same_streak_is_quieted(outcome, previous):
    with _settlement(_row(run=None, previous_failure_reason=previous)) as fx:
        assert await settle(_automation(), _EID, outcome, error="boom")
    fx.fire.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unreadable_price_trigger_rearms():
    automation = {**_automation(), "trigger_config": {"symbol": ""}}
    with _settlement(_row()) as fx:
        await settle(automation, _EID, Outcome.COMPLETED)
    assert fx.db.settle_execution.await_args.kwargs["schedule"] == "rearm_price"


@pytest.mark.asyncio
async def test_what_follows_a_settle_never_raises():
    with _settlement(_row()) as fx:
        fx.db.record_delivery.side_effect = RuntimeError("db down")
        assert await settle(_automation(), _EID, Outcome.FAILED)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", list(Outcome))
async def test_a_lost_settle_does_nothing_more(outcome):
    with _settlement(None) as fx:
        assert not await settle(_automation(), _EID, outcome)

    fx.db.record_delivery.assert_not_awaited()
    fx.fire.assert_not_awaited()
    fx.announce.assert_not_awaited()
    fx.metric.assert_not_called()


async def _settling_mid_webhook(fx):
    """A settle whose webhook is still out, and the event that lets it land."""
    landed = asyncio.Event()

    async def fire(*args, **kwargs):
        await landed.wait()
        return [{"method": "slack", "success": True}]

    fx.fire.side_effect = fire
    settling = asyncio.create_task(settle(_automation(), _EID, Outcome.FAILED))
    while not fx.fire.called:
        await asyncio.sleep(0)
    return settling, landed


@pytest.mark.asyncio
async def test_the_drain_waits_for_a_webhook_a_cancel_left_running():
    # The row is settled before the webhook goes out, so one shutdown cut off
    # would never be sent: no sweep finds a settled row.
    with _settlement(_row()) as fx:
        settling, landed = await _settling_mid_webhook(fx)
        settling.cancel()  # as shutdown cancels a running settler
        with pytest.raises(asyncio.CancelledError):
            await settling

        draining = asyncio.create_task(drain_tails())
        await asyncio.sleep(0.05)
        assert not draining.done()
        landed.set()
        await draining

    fx.db.record_delivery.assert_awaited_once()


@pytest.mark.asyncio
async def test_the_drain_gives_up_at_its_bound_and_names_what_is_left(caplog):
    with _settlement(_row()) as fx:
        settling, landed = await _settling_mid_webhook(fx)
        with caplog.at_level("WARNING"):
            await drain_tails(timeout=0.01)
        landed.set()
        assert await settling

    assert _EID in caplog.text


def _failed_call(status_code, *, owned, **metadata):
    return {
        "status": "error",
        "metadata": {
            "error_status_code": status_code,
            "error_credential_owned": owned,
            **metadata,
        },
    }


@pytest.mark.parametrize(
    "run, outcome",
    [
        (None, Outcome.INTERRUPTED),
        ({"status": "in_progress"}, None),
        ({"status": "completed"}, Outcome.COMPLETED),
        ({"status": "cancelled", "metadata": {"cancelled_by_user": True}}, Outcome.STOPPED),
        ({"status": "cancelled", "metadata": {"cancelled_by_user": False}}, Outcome.INTERRUPTED),
        ({"status": "cancelled", "metadata": None}, Outcome.INTERRUPTED),
        ({"status": "error", "metadata": {"recovery": "scanner"}}, Outcome.INTERRUPTED),
        ({"status": "error", "metadata": {}}, Outcome.FAILED),
        ({"status": "interrupted", "metadata": {}}, Outcome.FAILED),
        ({"status": "interrupted", "metadata": {}, "interrupt_reason": "hitl"}, Outcome.FAILED),
        ({"status": "interrupted", "metadata": {}, "interrupt_reason": "credit_pause"}, Outcome.LIMITED),
        # A model call refused the credential: the user's key (only a 401
        # rejects it), or ours.
        (_failed_call(401, owned=True), Outcome.KEY_REJECTED),
        (_failed_call(403, owned=True), Outcome.FAILED),
        (_failed_call(401, owned=False), Outcome.FAILED_OURS),
        (_failed_call(403, owned=None), Outcome.FAILED_OURS),
        (_failed_call(429, owned=True), Outcome.FAILED),
        (_failed_call(500, owned=True), Outcome.FAILED),
        # A lost worker is ours whatever its last call said.
        (_failed_call(401, owned=True, recovery="scanner"), Outcome.INTERRUPTED),
    ],
)
def test_the_ledger_decides_the_outcome(run, outcome):
    assert ledger_outcome(run) is outcome


@pytest.mark.parametrize(
    "host_mode, source, status, outcome",
    [
        # Hosted: only the user's own credential is theirs to fix.
        ("platform", "byok", 401, Outcome.KEY_REJECTED),
        ("platform", "oauth", 401, Outcome.KEY_REJECTED),
        ("platform", "platform", 401, Outcome.FAILED_OURS),
        ("platform", None, 401, Outcome.FAILED_OURS),
        ("platform", "byok", 403, Outcome.FAILED),
        # Self-hosted: the operator's key is the user's own.
        ("oss", "platform", 401, Outcome.KEY_REJECTED),
    ],
)
def test_what_a_failed_model_call_records_decides_the_outcome(
    host_mode, source, status, outcome, monkeypatch
):
    """The ledger metadata the run writes is what settlement reads, so the
    two halves are pinned together rather than each against a copy."""
    from src.server.services.runs import sse_producer

    monkeypatch.setattr(sse_producer.app_settings, "HOST_MODE", host_mode)
    exc = RuntimeError("refused")
    exc.__model_resilience__ = {"attempted_models": [{"status_code": status}]}

    metadata = sse_producer.model_call_failure(exc, source)
    assert ledger_outcome({"status": "error", "metadata": metadata}) is outcome


def _paused(*events):
    return {
        "status": "interrupted",
        "interrupt_reason": "credit_pause",
        "conversation_response_id": _RUN,
        "errors": None,
        "sse_events": list(events),
    }


def _pause_event(message, kind="credit_pause"):
    return {"event": "interrupt", "data": {"action_requests": [{"type": kind, "message": message}]}}


def test_a_run_says_why_in_its_own_last_error():
    run = {
        "status": "error",
        "conversation_response_id": _RUN,
        "errors": ["first", "AuthenticationError: bad key sk-ant-api03-" + "a" * 40],
    }
    message = run_failure_message(run)
    assert message.startswith("AuthenticationError: bad key ")
    assert "a" * 40 not in message


def test_a_long_error_is_cut_after_it_is_scrubbed():
    run = {"status": "error", "conversation_response_id": _RUN, "errors": ["x" * 2000]}
    assert len(run_failure_message(run)) == 500


def test_a_run_with_no_error_is_named():
    run = {"status": "error", "conversation_response_id": _RUN, "errors": []}
    assert _RUN in run_failure_message(run)


def test_a_credit_pause_relays_the_services_words():
    run = _paused(
        {"event": "message_chunk", "data": {}},
        _pause_event("Ask something else", kind="question"),
        _pause_event("You have used this month's credits."),
    )
    assert run_failure_message(run) == "You have used this month's credits."


@pytest.mark.parametrize(
    "events",
    [(), ({"event": "interrupt", "data": None},), ("not an event",), (_pause_event(None),)],
)
def test_a_credit_pause_without_its_words_says_nothing(events):
    assert run_failure_message(_paused(*events)) is None


# ─── The run's finalize job ───────────────────────────────────────────


def _finalize_job():
    return {
        "hook_outbox_id": 1,
        "run_id": _RUN,
        "conversation_thread_id": _THREAD,
        "hook_type": "automation_settle",
        "payload": {"execution_id": _EID, "automation_id": _AID, "user_id": "user-1", "workspace_id": "ws-1"},
        "attempts": 0,
    }


@contextmanager
def _finished(run, automation=None):
    with _settlement(_row()) as fx:
        fx.db.get_automation = AsyncMock(return_value=automation)
        fx.db.get_settling_run = AsyncMock(return_value=run)
        fx.excerpt = AsyncMock(return_value="Markets rose")
        with patch(f"{_MOD}.read_run_excerpt", new=fx.excerpt):
            yield fx


def _ended(status, errors=None, **metadata):
    return {"conversation_response_id": _RUN, "status": status, "metadata": metadata, "errors": errors}


_REJECTED = ["AuthenticationError: invalid x-api-key"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "run, to, strike, failure_reason, error",
    [
        (_ended("completed"), "completed", "reset", None, None),
        (_ended("cancelled", cancelled_by_user=True), "skipped", None, None, STOPPED_ERROR),
        (_ended("cancelled"), "failed", None, "interrupted", INTERRUPTED_ERROR),
        (_ended("error", ["ValueError: the table has no rows"]), "failed", "count", None, "ValueError: the table has no rows"),
        (_ended("error", _REJECTED, error_status_code=401, error_credential_owned=True), "failed", "fuse", "provider_auth", _REJECTED[0]),
        (_ended("error", _REJECTED, error_status_code=401, error_credential_owned=False), "failed", None, "server_error", _REJECTED[0]),
        (_paused(_pause_event("Daily limit reached.")), "failed", None, "usage_limit", "Daily limit reached."),
    ],
    ids=["completed", "user_stop", "shutdown", "failed", "users_key", "platform_key", "credit_pause"],
)
async def test_a_finished_run_settles_the_firing_it_ran_for(run, to, strike, failure_reason, error):
    with _finished(run, _automation("cron")) as fx:
        await _settle_finished_run(_finalize_job())

    written = fx.db.settle_execution.await_args.kwargs
    assert fx.db.settle_execution.await_args.args == (_EID,)
    assert written["to"] == to
    assert written["strike"] == strike
    assert written["failure_reason"] == failure_reason
    assert written["error_message"] == error
    assert written["conversation_thread_id"] == _THREAD
    assert written["conversation_response_id"] == _RUN
    # The answer is read from the run's own checkpoint, and only a completed
    # run has one to deliver.
    assert written["result_excerpt"] == ("Markets rose" if to == "completed" else None)
    webhook = fx.fire.await_args
    assert webhook.args[0] == ("automation.completed" if to == "completed" else "automation.failed")
    assert webhook.args[3:] == (_THREAD, "ws-1")
    assert webhook.kwargs["run_id"] == _RUN
    assert webhook.kwargs["error"] == error
    assert webhook.kwargs["failure_reason"] == failure_reason


@pytest.mark.asyncio
async def test_a_finished_run_settles_its_firing_once():
    """A retried job, a reclaimed lease or the sweep getting there first
    finds the firing settled and sends nothing again."""
    with _finished(_ended("completed"), _automation("cron")) as fx:
        fx.db.settle_execution.side_effect = [_row(), None]
        await _settle_finished_run(_finalize_job())
        await _settle_finished_run(_finalize_job())

    assert fx.db.settle_execution.await_count == 2
    fx.fire.assert_awaited_once()
    fx.db.record_delivery.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_deleted_automation_owes_nothing():
    with _finished(_ended("completed")) as fx:
        await _settle_finished_run(_finalize_job())

    fx.db.get_automation.assert_awaited_once_with(_AID, "user-1")
    fx.db.settle_execution.assert_not_awaited()
    fx.fire.assert_not_awaited()


# ─── The sweep ────────────────────────────────────────────────────────


def _abandoned(status="running", run=_RUN):
    return {
        "automation_execution_id": _EID,
        "automation_id": _AID,
        "status": status,
        "conversation_thread_id": _THREAD,
        "conversation_response_id": run,
        "user_id": "user-1",
        "workspace_id": "ws-1",
    }


@contextmanager
def _sweep(run_row):
    fx = SimpleNamespace(
        settle=AsyncMock(return_value=True),
        get_run=AsyncMock(return_value=run_row),
        excerpt=AsyncMock(return_value="The answer"),
    )
    with (
        patch(f"{_MOD}.settle", new=fx.settle),
        patch(f"{_MOD}.auto_db.get_settling_run", new=fx.get_run),
        patch(f"{_MOD}.read_run_excerpt", new=fx.excerpt),
    ):
        yield fx


@pytest.mark.asyncio
async def test_sweep_skips_an_abandoned_wait():
    with _sweep(None) as fx:
        assert await settle_abandoned(_automation(), _abandoned("waiting"), 300) is Outcome.SKIPPED
    kwargs = fx.settle.await_args.kwargs
    assert kwargs["skip_reason"] == "interrupted"
    assert kwargs["quiet_for"] == 300
    fx.get_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_interrupts_a_firing_never_admitted():
    with _sweep(None) as fx:
        assert await settle_abandoned(_automation(), _abandoned(run=None), 300) is Outcome.INTERRUPTED
    fx.get_run.assert_not_awaited()
    assert fx.settle.await_args.kwargs["workspace_id"] == "ws-1"


@pytest.mark.asyncio
async def test_sweep_leaves_a_run_still_going():
    with _sweep({"status": "in_progress"}) as fx:
        assert await settle_abandoned(_automation(), _abandoned(), 300) is None
    fx.settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_completes_by_the_ledger():
    with _sweep({"status": "completed"}) as fx:
        assert await settle_abandoned(_automation(), _abandoned(), 300) is Outcome.COMPLETED
    fx.excerpt.assert_awaited_once_with(_THREAD, _RUN)
    args, kwargs = fx.settle.await_args
    assert args[2] is Outcome.COMPLETED
    assert kwargs["run_id"] == _RUN
    assert kwargs["excerpt"] == "The answer"


@pytest.mark.asyncio
async def test_sweep_fails_a_run_that_failed():
    run = {"status": "error", "metadata": {}, "conversation_response_id": _RUN}
    with _sweep(run) as fx:
        assert await settle_abandoned(_automation(), _abandoned(), 300) is Outcome.FAILED
    assert _RUN in fx.settle.await_args.kwargs["error"]
    fx.excerpt.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_settles_a_rejected_key():
    run = {**_failed_call(401, owned=True), "conversation_response_id": _RUN, "errors": ["AuthenticationError: invalid x-api-key"]}
    with _sweep(run) as fx:
        assert await settle_abandoned(_automation(), _abandoned(), 300) is Outcome.KEY_REJECTED
    assert fx.settle.await_args.kwargs["error"] == "AuthenticationError: invalid x-api-key"


@pytest.mark.asyncio
async def test_sweep_settles_a_paused_run_as_limited():
    with _sweep(_paused(_pause_event("Daily limit reached."))) as fx:
        assert await settle_abandoned(_automation(), _abandoned(), 300) is Outcome.LIMITED
    assert fx.settle.await_args.args[2] is Outcome.LIMITED
    assert fx.settle.await_args.kwargs["error"] == "Daily limit reached."


@pytest.mark.asyncio
async def test_sweep_that_loses_the_row_reports_nothing():
    with _sweep({"status": "completed"}) as fx:
        fx.settle.return_value = False
        assert await settle_abandoned(_automation(), _abandoned(), 300) is None
