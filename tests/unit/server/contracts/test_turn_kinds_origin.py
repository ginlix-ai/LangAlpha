"""Where a run came from: classified once at START, read back at settle."""

import pytest

from src.server.contracts.turn_kinds import RUN_ORIGIN_META, classify_origin, run_origin


@pytest.mark.parametrize(
    "meta, query_type, origin",
    [
        ({}, "follow_up", "turn"),
        ({}, None, "turn"),
        ({"automation_id": "a-1"}, "follow_up", "automation"),
        ({"report_back_ptc_thread_id": "ptc-1"}, None, "report"),
        ({"report_back_ptc_thread_id": "ptc-1"}, "system", "report"),
        # A message, or an answer, past an interrupted summary carries its
        # pair (``carried_pair``) but is the user's own turn.
        ({"report_back_ptc_thread_id": "ptc-1"}, "initial", "turn"),
        ({"report_back_ptc_thread_id": "ptc-1"}, "resume_feedback", "turn"),
        ({}, "system", "report"),
        ({}, "wakeup", "wakeup"),
        ({}, "market_target", "market_target"),
    ],
)
def test_start_classifies_by_metadata_and_query_type(meta, query_type, origin):
    assert classify_origin(meta, query_type) == origin


@pytest.mark.parametrize("query_type", ["wakeup", "market_target", "system"])
def test_a_retry_of_a_wake_is_the_users_own_turn_at_settle(query_type):
    """A ``/retry`` writes no query row: START classifies it with none and
    stamps ``turn``, and the settle's join, which finds the turn's old wake
    row, must not relabel it."""
    stamped = {RUN_ORIGIN_META: classify_origin({}, None)}
    assert run_origin(stamped, query_type) == "turn"


def test_a_stamped_wake_stays_a_wake_and_an_unstamped_row_is_classified():
    assert run_origin({RUN_ORIGIN_META: "wakeup"}, "wakeup") == "wakeup"
    assert run_origin({}, "wakeup") == "wakeup"
    assert run_origin({RUN_ORIGIN_META: "bogus"}, "market_target") == "market_target"
