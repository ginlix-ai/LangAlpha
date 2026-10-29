"""A market target's alert taken in by a live turn, through the report inbox.

The alert rides the one steering message ahead of the user's words, as a
report does, and its ``steering_delivered`` entry carries what the reader's
alert row is drawn from. A run waiting on its background tasks counts an
alert waiting for the thread as a reason to stop waiting, when its steering
takes reports in, its owner has market watch on, and a re-run can follow,
and its trigger then does not claim anyone wrote anything.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent import utils
from ptc_agent.agent.middleware.background_subagent.orchestrator import (
    BackgroundSubagentOrchestrator,
)
from ptc_agent.agent.middleware.steering import SteeredReport, SteeringMiddleware
from ptc_agent.agent.transcript.classify import (
    MARKET_TARGET_TRIGGER,
    STEERING_TRIGGER,
    is_steering_trigger,
)

_OUTBOX = "src.server.database.runs.outbox"

_NOTICE = {"target_id": "tg-1", "notice": "hit", "symbol": "AAPL"}
_ALERT = SteeredReport(
    job_id="job-alert",
    content="<market-alert>\nAAPL crossed 230.00\n</market-alert>",
    shown={"market_alert": _NOTICE},
)
_REPORT = SteeredReport(
    job_id="job-report",
    content="REPORT",
    shown={"report_source": {"kind": "task", "task_id": "a"}},
)
_STEER = SteeredReport(
    job_id="job-steer",
    content="also check MSFT",
    kind="steer",
    sender="user",
    user_id="u-1",
    timestamp=1.0,
)


def _config(run_id: str | None = "run-A") -> dict:
    cfg: dict = {"configurable": {"thread_id": "t-1"}}
    if run_id is not None:
        cfg["metadata"] = {"run_id": run_id}
    return cfg


def _inbox(*taken: SteeredReport) -> MagicMock:
    inbox = MagicMock()
    inbox.take = AsyncMock(return_value=list(taken))
    inbox.confirm = AsyncMock()
    inbox.take_resumes = AsyncMock(return_value=[])
    return inbox


@pytest.mark.asyncio
async def test_alert_rides_the_steering_message_ahead_of_the_users_words():
    runtime = MagicMock()

    result = await SteeringMiddleware(
        _inbox(_REPORT, _ALERT, _STEER)
    ).abefore_model({}, runtime, config=_config())

    (msg,) = result["messages"]
    assert msg.additional_kwargs["lc_source"] == "steering"
    assert msg.content.index("<market-alert>") < msg.content.index(
        "also check MSFT"
    )
    delivered = msg.additional_kwargs["steering_delivered"]
    # The user's messages only: a chat gateway retires this many.
    assert delivered["count"] == 1
    report, alert, user = delivered["messages"]
    assert report["report_source"] == _REPORT.shown["report_source"]
    assert "market_alert" not in report
    assert alert["content"] == "" and alert["market_alert"] == _NOTICE
    assert "report_source" not in alert
    assert user["content"] == "also check MSFT"
    held = msg.additional_kwargs["held_jobs"]
    assert held["run_id"] == "run-A"
    assert held["job_ids"] == ["job-report", "job-alert", "job-steer"]
    (event,) = [
        c.args[0]
        for c in runtime.stream_writer.call_args_list
        if c.args[0].get("type") == "steering_delivered"
    ]
    assert event["messages"][1]["market_alert"] == _NOTICE


def _waiting(alert: bool) -> AsyncMock:
    return AsyncMock(return_value={utils.MARKET_ALERT_SENDER} if alert else set())


_MARKET = frozenset({utils.MARKET_ALERT_SENDER})


async def _senders(*, steers: list[str], alert: bool, market_alerts: bool):
    has_alert = _waiting(alert)
    with (
        patch(f"{_OUTBOX}.pending_steer_senders", new=AsyncMock(return_value=steers)),
        patch(f"{_OUTBOX}.steerable_alert_senders", new=has_alert),
    ):
        checker = await utils.build_message_checker(
            "t-1",
            own_run_id="run-A",
            alert_senders=_MARKET if market_alerts else frozenset(),
        )
        return await checker(), has_alert


@pytest.mark.asyncio
async def test_checker_names_the_alert_only_when_no_steer_waits():
    senders, has_alert = await _senders(steers=[], alert=True, market_alerts=True)
    assert senders == [utils.MARKET_ALERT_SENDER]
    has_alert.assert_awaited_once_with(
        "t-1", {utils.MARKET_ALERT_SENDER: "market_target"}
    )

    senders, has_alert = await _senders(
        steers=["user"], alert=True, market_alerts=True
    )
    assert senders == ["user"]
    has_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_run_that_takes_no_reports_does_not_count_alerts():
    senders, has_alert = await _senders(steers=[], alert=True, market_alerts=False)

    assert senders == []
    has_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failing_alert_check_counts_as_nothing_waiting():
    with (
        patch(f"{_OUTBOX}.pending_steer_senders", new=AsyncMock(return_value=[])),
        patch(
            f"{_OUTBOX}.steerable_alert_senders",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ),
    ):
        checker = await utils.build_message_checker("t-1", alert_senders=_MARKET)
        assert await checker() == []


def _orchestrator(
    *, enabled: bool = True, market_alerts: bool = True, **kwargs
) -> BackgroundSubagentOrchestrator:
    mw = MagicMock()
    mw.enabled = enabled
    mw.registry = MagicMock()
    mw.registry.has_pending_tasks.return_value = False
    mw.registry._tasks = {}
    agent = MagicMock()
    agent.aupdate_state = AsyncMock()
    agent.aget_state = AsyncMock(return_value=MagicMock(interrupts=()))
    return BackgroundSubagentOrchestrator(
        agent, mw, alert_senders=_MARKET if market_alerts else frozenset(), **kwargs
    )


async def _reinvoke(orch, *, steers: list[str], alert: bool) -> bool:
    with (
        patch(f"{_OUTBOX}.pending_steer_senders", new=AsyncMock(return_value=steers)),
        patch(f"{_OUTBOX}.steerable_alert_senders", new=_waiting(alert)),
    ):
        return await orch._reinvoke_for_steering(_config(), iteration=1)


@pytest.mark.asyncio
async def test_a_waiting_alert_reinvokes_with_the_alert_trigger():
    orch = _orchestrator()

    assert await _reinvoke(orch, steers=[], alert=True) is True

    (msg,) = orch.agent.aupdate_state.call_args[0][1]["messages"]
    assert msg.content == MARKET_TARGET_TRIGGER
    # Compaction and the transcript treat it as the trigger it is.
    assert is_steering_trigger(msg.content)


@pytest.mark.asyncio
async def test_the_users_steering_keeps_the_users_trigger_beside_an_alert():
    orch = _orchestrator()

    assert await _reinvoke(orch, steers=["user"], alert=True) is True

    (msg,) = orch.agent.aupdate_state.call_args[0][1]["messages"]
    assert msg.content == STEERING_TRIGGER


@pytest.mark.asyncio
async def test_a_run_without_subagents_is_not_reinvoked_for_an_alert():
    """Its steering takes no reports in, so the re-run would read nothing."""
    orch = _orchestrator(enabled=False)

    assert await _reinvoke(orch, steers=[], alert=True) is False

    orch.agent.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_run_whose_owner_has_market_watch_off_is_not_reinvoked_for_an_alert():
    """Its steering closes the alert unread, so the re-run would read nothing."""
    orch = _orchestrator(market_alerts=False)

    assert await _reinvoke(orch, steers=[], alert=True) is False

    orch.agent.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "steers, reinvoked", [([], False), (["user"], True)], ids=["alert", "steer"]
)
async def test_a_run_stopped_on_a_question_is_reinvoked_only_for_a_steer(
    steers, reinvoked
):
    """Re-invoking supersedes the question, which an alert nobody sent must
    not do: it stays parked for the turn that resumes with the answer."""
    orch = _orchestrator()
    orch.agent.aget_state.return_value = MagicMock(interrupts=(MagicMock(),))

    assert await _reinvoke(orch, steers=steers, alert=True) is reinvoked


async def _no_events(*_args, **_kwargs):
    return
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("max_iterations, counted", [(1, False), (2, True)])
async def test_only_a_wait_a_rerun_can_follow_ends_on_an_alert(max_iterations, counted):
    """On the last pass an alert ending the wait would end the turn with its
    tasks running, and each result would come back as a turn of its own."""
    orch = _orchestrator(auto_wait=True, max_iterations=max_iterations)
    orch.middleware.registry.has_pending_tasks.return_value = True
    orch.middleware.registry.wait_for_all = AsyncMock()
    orch.agent.astream = _no_events
    build = AsyncMock(return_value=None)

    with patch.object(utils, "build_message_checker", new=build):
        async for _ in orch.astream({"messages": []}, _config()):
            pass

    orch.middleware.registry.wait_for_all.assert_awaited_once()
    assert build.await_args_list[0].kwargs["alert_senders"] == (
        _MARKET if counted else frozenset()
    )
