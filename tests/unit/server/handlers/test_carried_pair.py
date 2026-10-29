"""The pair a turn takes over from an interrupted report-back summary.

An interrupted summary releases nothing, and the turn that answers it is a
public request stripped of the pair. ``carried_pair`` stamps that turn with
the summary's pair from the thread's latest attempt, so its finalize
releases the pair under the summary's generation.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.runs.hook_jobs import build_finalize_jobs_from_run_row
from src.server.services.report_back.handoff.carry import carried_pair

_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"


def _request(**fields):
    base = {"report_back_ptc_thread_id": None, "origin_dispatch_gen": None}
    base.update(fields)
    return SimpleNamespace(**base)


def _attempt(status, **meta):
    return {"status": status, "metadata": meta}


_SUMMARY_PAIR = {"report_back_ptc_thread_id": "ptc-9", "origin_dispatch_gen": "gen-3"}


@pytest.mark.asyncio
async def test_the_turn_after_an_interrupted_summary_takes_its_pair():
    """Whatever follows the interrupt, a resume or a new message, owes the
    release. A carried turn interrupted again stamps the same pair, so the
    next one carries it on."""
    latest = AsyncMock(return_value=_attempt("interrupted", **_SUMMARY_PAIR))
    with patch(_LATEST, latest):
        assert await carried_pair(_request(), "cos-1") == _SUMMARY_PAIR
    latest.assert_awaited_once_with("cos-1")


@pytest.mark.asyncio
async def test_a_summary_names_its_own_pair_and_reads_nothing():
    latest = AsyncMock(return_value=_attempt("interrupted", **_SUMMARY_PAIR))
    with patch(_LATEST, latest):
        req = _request(report_back_ptc_thread_id="ptc-2", origin_dispatch_gen="gen-5")
        assert await carried_pair(req, "cos-1") == {}
    latest.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "latest",
    [
        None,
        _attempt("completed", **_SUMMARY_PAIR),
        _attempt("error", **_SUMMARY_PAIR),
        _attempt("interrupted"),
    ],
    ids=["first_turn", "summary_released", "summary_failed", "pairless_interrupt"],
)
async def test_nothing_is_carried_past_a_settled_or_pairless_turn(latest):
    """A summary that ended released its own pair, and an interrupt outside a
    report-back holds none."""
    with patch(_LATEST, AsyncMock(return_value=latest)):
        assert await carried_pair(_request(), "cos-1") == {}


@pytest.mark.asyncio
async def test_a_failed_read_fails_the_turn_start():
    with patch(_LATEST, AsyncMock(side_effect=ConnectionError("db down"))):
        with pytest.raises(ConnectionError):
            await carried_pair(_request(), "cos-1")


@pytest.mark.asyncio
async def test_the_carrying_turn_releases_the_pair_under_its_generation():
    """What the turn stamps is what its finalize reads: a release fenced to
    the summary's generation, ordered on the watching thread, so a pair
    dispatched again since survives it. Interrupted, it releases nothing."""
    with patch(
        _LATEST, AsyncMock(return_value=_attempt("interrupted", **_SUMMARY_PAIR))
    ):
        carried = await carried_pair(_request(), "home-1")
    row = {
        "conversation_response_id": "run-8",
        "conversation_thread_id": "home-1",
        "metadata": {"msg_type": "ptc", "user_id": "u-1", **carried},
    }

    jobs = build_finalize_jobs_from_run_row(row)("completed")
    assert sorted(j.hook_type for j in jobs) == ["user_feed", "watch_clear"]
    (wc,) = [j for j in jobs if j.hook_type == "watch_clear"]
    assert wc.payload == {
        "ptc_thread_id": "ptc-9",
        "user_id": "u-1",
        "error_wake": False,
        "dispatch_gen": "gen-3",
    }
    assert wc.ordering_key == "home-1"
    assert [j.hook_type for j in build_finalize_jobs_from_run_row(row)("interrupted")] == [
        "user_feed"
    ]
