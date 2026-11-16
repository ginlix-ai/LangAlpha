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
from src.server.services.report_back.handoff.carry import (
    PriorAttempt,
    carried_pair,
    prior_attempt,
)

_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"
_GET_RUN = "src.server.database.runs.lifecycle.get_run"


def _request(**fields):
    base = {"report_back_ptc_thread_id": None, "origin_dispatch_gen": None}
    base.update(fields)
    return SimpleNamespace(**base)


def _attempt(status, **meta):
    return {"status": status, "metadata": meta}


def _after(latest) -> PriorAttempt:
    return PriorAttempt(latest=latest, retried=None)


_SUMMARY_PAIR = {"report_back_ptc_thread_id": "ptc-9", "origin_dispatch_gen": "gen-3"}


def test_the_turn_after_an_interrupted_summary_takes_its_pair():
    """Whatever follows the interrupt, a resume or a new message, owes the
    release. A carried turn interrupted again stamps the same pair, so the
    next one carries it on."""
    prior = _after(_attempt("interrupted", **_SUMMARY_PAIR))
    assert carried_pair(_request(), prior) == _SUMMARY_PAIR


def test_the_turn_after_a_summary_a_restart_stopped_takes_its_pair():
    """The restart left the summary unfinished; the turn that resumes it owes
    the release. A user's stop, which leaves no restart stamp, ended it, and
    it released its own."""
    stopped = _attempt("cancelled", interrupted_by_restart=True, **_SUMMARY_PAIR)
    assert carried_pair(_request(), _after(stopped)) == _SUMMARY_PAIR
    user_stop = _attempt("cancelled", cancelled_by_user=True, **_SUMMARY_PAIR)
    assert carried_pair(_request(), _after(user_stop)) == {}


def test_a_summary_names_its_own_pair_and_takes_none():
    req = _request(report_back_ptc_thread_id="ptc-2", origin_dispatch_gen="gen-5")
    assert carried_pair(req, _after(_attempt("interrupted", **_SUMMARY_PAIR))) == {}


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
def test_nothing_is_carried_past_a_settled_or_pairless_turn(latest):
    """A summary that ended released its own pair, and an interrupt outside a
    report-back holds none."""
    assert carried_pair(_request(), _after(latest)) == {}


@pytest.mark.asyncio
async def test_the_prior_attempt_is_read_once_and_the_retried_run_only_for_a_retry():
    latest = AsyncMock(return_value=_attempt("interrupted"))
    get_run = AsyncMock(return_value=_attempt("error"))
    with patch(_LATEST, latest), patch(_GET_RUN, get_run):
        prior = await prior_attempt(_request(retry_of_run_id=None), "cos-1")
        retry = await prior_attempt(_request(retry_of_run_id="run-0"), "cos-1")
    assert prior == PriorAttempt(latest=_attempt("interrupted"), retried=None)
    assert retry.retried == _attempt("error")
    assert latest.await_count == 2
    get_run.assert_awaited_once_with("run-0")


@pytest.mark.asyncio
async def test_a_failed_read_fails_the_turn_start():
    with patch(_LATEST, AsyncMock(side_effect=ConnectionError("db down"))):
        with pytest.raises(ConnectionError):
            await prior_attempt(_request(), "cos-1")


def test_the_carrying_turn_releases_the_pair_under_its_generation():
    """What the turn stamps is what its finalize reads: a release fenced to
    the summary's generation, ordered on the watching thread, so a pair
    dispatched again since survives it. Interrupted, it releases nothing."""
    carried = carried_pair(_request(), _after(_attempt("interrupted", **_SUMMARY_PAIR)))
    row = {
        "conversation_response_id": "run-8",
        "conversation_thread_id": "home-1",
        "metadata": {"msg_type": "ptc", "user_id": "u-1", **carried},
    }

    jobs = build_finalize_jobs_from_run_row(row)("completed")
    assert sorted(j.hook_type for j in jobs) == ["user_feed", "user_notification", "watch_clear"]
    (wc,) = [j for j in jobs if j.hook_type == "watch_clear"]
    assert wc.payload == {
        "ptc_thread_id": "ptc-9",
        "user_id": "u-1",
        "error_wake": False,
        "dispatch_gen": "gen-3",
    }
    assert wc.ordering_key == "home-1"
    assert [j.hook_type for j in build_finalize_jobs_from_run_row(row)("interrupted")] == [
        "user_feed",
        "user_notification",
    ]
