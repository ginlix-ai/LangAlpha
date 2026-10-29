"""A market target's notice on the report inbox: the offer a live turn is
handed, and the delivery stamp its confirm leaves before closing the job, so
a later claim of the same notice finds nothing owed."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.middleware.steering import SteeredReport
from src.server.services.market_targets import delivery, inline
from src.server.services.report_back import steer
from src.server.services.report_back.notify_turn import NotOwed

_MT_DB = "src.server.database.market_targets"
_OUTBOX = "src.server.database.runs.outbox"
_BY_KEY = "src.server.database.runs.lifecycle.find_run_by_request_key"


def _target(**overrides) -> dict:
    row = {
        "target_id": "tg-1",
        "conversation_thread_id": "t-1",
        "user_id": "u-1",
        "symbol": "AAPL",
        "market": "stock",
        "kind": "above",
        "value": 230.0,
        "pct_from": None,
        "armed_price": 222.1,
        "status": "fired",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=3),
        "fired_at": datetime(2026, 10, 12, 14, 31, tzinfo=timezone.utc),
        "fired_price": 230.45,
        "hit_delivered_at": None,
        "warn_delivered_at": None,
    }
    row.update(overrides)
    return row


def _job(notice: str = "hit") -> dict:
    return {
        "hook_outbox_id": "job-1",
        "hook_type": "market_target",
        "payload": {"target_id": "tg-1", "notice": notice, "user_id": "u-1"},
    }


def _held(job_id: str, target_id: str, notice: str) -> dict:
    """A held alert job as the confirm reads it back."""
    return {
        "hook_outbox_id": job_id,
        "hook_type": "market_target",
        "payload": {"target_id": target_id, "notice": notice},
    }


async def _offer(
    job: dict,
    target: dict | None,
    *,
    admitted=None,
    quote=None,
    feature_on=True,
    mark: AsyncMock | None = None,
):
    last_quote = AsyncMock(return_value=quote)
    with (
        patch(f"{_MT_DB}.get_target", new=AsyncMock(return_value=target)),
        patch(_BY_KEY, new=AsyncMock(return_value=admitted)),
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=feature_on),
        ),
        patch.object(delivery, "last_quote", new=last_quote),
        patch(f"{_MT_DB}.mark_delivered", new=mark or AsyncMock(return_value=True)),
    ):
        return await inline.market_alert_offer(job), last_quote


@pytest.mark.asyncio
async def test_an_owed_hit_is_offered_with_its_alert_row():
    offer, last_quote = await _offer(_job(), _target())

    assert isinstance(offer, SteeredReport)
    assert offer.job_id == "job-1"
    assert offer.content.startswith("<market-alert>")
    assert offer.shown["market_alert"]["target_id"] == "tg-1"
    assert offer.shown["market_alert"]["notice"] == "hit"
    assert "report_source" not in offer.shown
    last_quote.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_expiring_notice_asks_for_the_price_briefly():
    target = _target(status="armed", fired_at=None, fired_price=None)

    offer, last_quote = await _offer(_job("expiring"), target)

    assert isinstance(offer, SteeredReport)
    assert offer.shown["market_alert"]["notice"] == "expiring"
    last_quote.assert_awaited_once()
    assert last_quote.call_args.kwargs["timeout"] < delivery._QUOTE_TIMEOUT


@pytest.mark.asyncio
async def test_a_delivered_notice_is_closed_as_owed_nothing():
    offer, _ = await _offer(
        _job(), _target(hit_delivered_at=datetime.now(timezone.utc))
    )

    assert isinstance(offer, NotOwed)


@pytest.mark.parametrize("already", [False, True], ids=["unstamped", "stamped-meanwhile"])
@pytest.mark.asyncio
async def test_a_notice_whose_wake_was_admitted_is_stamped_to_it_and_closed(already):
    """The wake turn's message is the notice: stamped to that run and closed,
    not offered again, nor left open for every live turn to pass over (a
    held job waits on infinity, and a run waiting on its tasks re-reads it
    between polls)."""
    mark = AsyncMock(return_value=not already)
    offer, _ = await _offer(
        _job(),
        _target(),
        admitted={"conversation_response_id": "wake-1", "conversation_thread_id": "t-1"},
        mark=mark,
    )

    assert isinstance(offer, NotOwed)
    mark.assert_awaited_once_with("tg-1", "hit", "wake-1")


@pytest.mark.asyncio
async def test_a_failed_stamp_for_an_admitted_wake_leaves_the_job_open():
    """The read fails, so the inbox leaves the job to its executor, whose
    next claim adopts the wake run."""
    with (
        patch(f"{_MT_DB}.get_target", new=AsyncMock(return_value=_target())),
        patch(
            _BY_KEY,
            new=AsyncMock(
                return_value={
                    "conversation_response_id": "wake-1",
                    "conversation_thread_id": "t-1",
                }
            ),
        ),
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=True),
        ),
        patch(f"{_MT_DB}.mark_delivered", new=AsyncMock(side_effect=RuntimeError("db"))),
    ):
        assert await steer._decide(_job(), "t-1") is None


@pytest.mark.asyncio
async def test_a_run_holding_the_key_on_another_thread_is_not_its_wake():
    offer, _ = await _offer(
        _job(),
        _target(),
        admitted={"conversation_response_id": "run-x", "conversation_thread_id": "t-other"},
    )

    assert isinstance(offer, SteeredReport)


@pytest.mark.asyncio
async def test_a_notice_whose_owner_has_market_watch_off_is_closed():
    offer, _ = await _offer(_job(), _target(), feature_on=False)

    assert isinstance(offer, NotOwed)


@pytest.mark.asyncio
async def test_a_malformed_job_is_left_to_its_executor():
    job = _job()
    job["payload"] = {"notice": "hit"}

    offer, _ = await _offer(job, _target())

    assert offer is None


@pytest.mark.asyncio
async def test_take_routes_alert_jobs_to_the_market_offer():
    report = SteeredReport(
        job_id="job-1", content="alert", shown={"market_alert": {"n": 1}}
    )
    market_offer = AsyncMock(return_value=report)
    task_offer = AsyncMock()
    with (
        patch(
            f"{_OUTBOX}.list_steerable_report_jobs",
            new=AsyncMock(return_value=[_job()]),
        ),
        patch(
            f"{_OUTBOX}.hold_steered_report_jobs",
            new=AsyncMock(return_value=["job-1"]),
        ),
        patch(f"{_OUTBOX}.close_unowed_report_jobs", new=AsyncMock(return_value=[])),
        patch(f"{_OUTBOX}.close_unowed_held_alerts", new=AsyncMock(return_value=[])),
        patch.object(inline, "market_alert_offer", new=market_offer),
        patch.object(steer, "_task_offer", new=task_offer),
        patch.object(steer, "_analyst_offer", new=task_offer),
        patch(
            "src.server.services.report_back.subagent.publish_cleared_wake",
            new=AsyncMock(),
        ),
    ):
        reports = await steer.SteeredReportInbox().take(
            "t-1", "run-A", kinds=("report",)
        )

    assert reports == [report]
    task_offer.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirm_stamps_held_alerts_before_closing_their_jobs():
    order: list[str] = []
    mark = AsyncMock(side_effect=lambda *a: order.append(f"stamp {a[0]}") or True)
    close = AsyncMock(side_effect=lambda *a, **k: order.append("close") or a[0])
    held = [_held("job-1", "tg-1", "hit"), _held("job-2", "tg-2", "expiring")]
    with (
        patch(f"{_OUTBOX}.held_notice_jobs", new=AsyncMock(return_value=held)) as read,
        patch(f"{_MT_DB}.mark_delivered", new=mark),
        patch(f"{_OUTBOX}.confirm_jobs", new=close),
    ):
        await steer.SteeredReportInbox().confirm("run-A", ["job-1", "job-2", "job-3"])

    read.assert_awaited_once_with(["job-1", "job-2", "job-3"], "run-A")
    assert mark.await_args_list[0].args == ("tg-1", "hit", "run-A")
    assert mark.await_args_list[1].args == ("tg-2", "expiring", "run-A")
    assert order == ["stamp tg-1", "stamp tg-2", "close"]


@pytest.mark.asyncio
async def test_a_failed_stamp_still_closes_the_job():
    """The notice is in the checkpointed message; an open job would show it
    again as a wake."""
    held = [_held("job-1", "tg-1", "hit"), _held("job-2", "tg-2", "hit")]
    mark = AsyncMock(side_effect=[RuntimeError("db down"), True])
    close = AsyncMock(return_value=["job-1", "job-2"])
    with (
        patch(f"{_OUTBOX}.held_notice_jobs", new=AsyncMock(return_value=held)),
        patch(f"{_MT_DB}.mark_delivered", new=mark),
        patch(f"{_OUTBOX}.confirm_jobs", new=close),
    ):
        await steer.SteeredReportInbox().confirm("run-A", ["job-1", "job-2"])

    assert mark.await_count == 2
    close.assert_awaited_once_with(["job-1", "job-2"], owner=("run", "run-A"))


async def _confirm_found(owner: tuple[str, str]):
    held = [_held("job-1", "tg-1", "hit")]
    read = AsyncMock(return_value=held)
    mark = AsyncMock(return_value=True)
    close = AsyncMock(return_value=["job-1"])
    with (
        patch(
            "ptc_agent.agent.middleware.steering.checkpointed_holds",
            return_value={"job-1"},
        ),
        patch(f"{_OUTBOX}.held_notice_jobs", new=read),
        patch(f"{_MT_DB}.mark_delivered", new=mark),
        patch(f"{_OUTBOX}.confirm_jobs", new=close),
    ):
        closed = await steer._confirm_found(
            ["job-1", "job-2"], SimpleNamespace(values={"messages": []}), owner
        )
    return closed, read, mark, close


@pytest.mark.asyncio
async def test_a_checkpointed_hold_is_stamped_before_its_close():
    """A run that stopped or lost its worker after reading the alert still
    owes nothing: the notice is in its checkpoint."""
    closed, read, mark, close = await _confirm_found(("run", "run-A"))

    assert closed == ["job-1"]
    read.assert_awaited_once_with(["job-1"], "run-A")
    assert mark.await_args.args == ("tg-1", "hit", "run-A")
    close.assert_awaited_once_with(["job-1"], owner=("run", "run-A"))


@pytest.mark.asyncio
async def test_a_task_run_hold_reads_no_alerts():
    _, read, mark, _ = await _confirm_found(("task", "task-run-A"))

    read.assert_not_awaited()
    mark.assert_not_awaited()
