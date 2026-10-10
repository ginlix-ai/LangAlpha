"""Coverage for the secretary's completed-thread DB reader, run labels and length cap.

``_runs_to_read`` bounds the read to the requested window (``turns``) and
returns one row per run; it used to concatenate every turn's text into one
blob. ``_run_block`` puts each run's text under a line naming the run: without
it an earlier run's text read as the newest one's, a run that wrote nothing
vanished, and a report-back read as another attempt at the work.

``_join_recent_turns`` then caps the joined output at ``MAX_OUTPUT_CHARS`` on
real turn boundaries (taken from the list, never rediscovered by scanning the
text) so a turn whose own markdown contains ``---`` is not mistaken for a
turn separator.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.tools.secretary.utils import (
    MAX_OUTPUT_CHARS,
    _EMPTY_LATEST_FALLBACK_TURNS,
    _MAX_HISTORY_TURNS,
    _TURN_SEPARATOR,
    _join_recent_turns,
    _run_block,
    _runs_to_read,
    _text_from_response,
    _truncate_single,
    user_zone,
)

_RECENT = "src.server.database.conversation.responses.get_recent_responses_for_thread"
_UTC = user_zone("UTC")


def _chunk(text: str) -> dict:
    return {"event": "message_chunk", "data": {"content_type": "text", "content": text}}


# Each run's stored events by response id, as ``_response`` built them.
_STORED: dict[str, list[dict]] = {}


def _response(turn_index: int, *texts: str, **fields) -> dict:
    events = _STORED[f"r-{turn_index}"] = [_chunk(t) for t in texts]
    return {
        "conversation_response_id": f"r-{turn_index}",
        "conversation_thread_id": "t-1",
        "turn_index": turn_index,
        "status": "completed",
        "sse_events": events,
        **fields,
    }


@pytest.fixture(autouse=True)
def _stored_events():
    """The reader's rows come without their stored events, so a run's are
    read by id; here, from what ``_response`` built."""
    _STORED.clear()

    async def read(response_ids):
        return {i: {"sse_events": _STORED[i]} for i in response_ids if i in _STORED}

    events = AsyncMock(side_effect=read)
    with patch(
        "src.server.database.conversation.replay_rows.get_replay_responses", events
    ):
        yield events


def _ids(runs: list[dict]) -> list[str]:
    return [r["conversation_response_id"] for r in runs]


# --- _runs_to_read: which runs a read shows ----------------------------------


@pytest.mark.asyncio
async def test_turns_default_fetches_only_latest_run():
    """turns=1 asks the DB for one row, so older runs cannot leak in."""
    recent = AsyncMock(return_value=[_response(9, "latest answer")])

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1", turns=1)

    assert _ids(runs) == ["r-9"]
    recent.assert_awaited_once_with("t-1", limit=1)


@pytest.mark.asyncio
async def test_turns_n_keeps_runs_without_text():
    """A window keeps every run, a text-less one included, so a failure that
    wrote nothing still shows up between the runs around it."""
    window = [_response(1, "kept"), _response(2, status="error"), _response(3, "also")]
    recent = AsyncMock(return_value=window)

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1", turns=3)

    assert _ids(runs) == ["r-1", "r-2", "r-3"]
    recent.assert_awaited_once_with("t-1", limit=3)


@pytest.mark.asyncio
async def test_turns_zero_requests_recent_history_clamped():
    recent = AsyncMock(return_value=[_response(0, "a")])

    with patch(_RECENT, recent):
        await _runs_to_read("t-1", turns=0)

    recent.assert_awaited_once_with("t-1", limit=_MAX_HISTORY_TURNS)


@pytest.mark.asyncio
async def test_turns_large_n_is_clamped_to_ceiling():
    recent = AsyncMock(return_value=[_response(0, "x")])

    with patch(_RECENT, recent):
        await _runs_to_read("t-1", turns=10_000)

    recent.assert_awaited_once_with("t-1", limit=_MAX_HISTORY_TURNS)


@pytest.mark.asyncio
async def test_no_runs_returns_empty_without_a_second_read():
    recent = AsyncMock(return_value=[])

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1")

    assert runs == []
    recent.assert_awaited_once_with("t-1", limit=1)


@pytest.mark.asyncio
async def test_text_less_latest_brings_the_newest_run_with_text_beside_it():
    """A text-less newest run is shown with the newest earlier run that has
    text, not replaced by it: the old text must not pass as the new run's."""
    latest_only = [_response(9, status="error")]
    window = [_response(7, "real answer"), _response(8), _response(9, status="error")]
    recent = AsyncMock(side_effect=[latest_only, window])

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1", turns=1)

    assert _ids(runs) == ["r-7", "r-9"]
    assert recent.await_args_list[0].kwargs == {"limit": 1}
    assert recent.await_args_list[1].kwargs == {"limit": _EMPTY_LATEST_FALLBACK_TURNS}


@pytest.mark.asyncio
async def test_text_less_window_returns_only_the_newest_run():
    recent = AsyncMock(side_effect=[[_response(9)], [_response(8), _response(9)]])

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1", turns=1)

    assert _ids(runs) == ["r-9"]


@pytest.mark.asyncio
async def test_a_run_that_settled_between_the_reads_stands_alone():
    recent = AsyncMock(side_effect=[[_response(9)], [_response(9), _response(10, "new")]])

    with patch(_RECENT, recent):
        runs = await _runs_to_read("t-1", turns=1)

    assert _ids(runs) == ["r-10"]


@pytest.mark.asyncio
async def test_db_failure_propagates():
    """A read failure propagates (so the tool layer can surface an error)
    rather than being swallowed into an empty, success-looking result."""
    recent = AsyncMock(side_effect=RuntimeError("db down"))

    with patch(_RECENT, recent):
        with pytest.raises(RuntimeError):
            await _runs_to_read("t-1")


# --- _text_from_response: one run's text --------------------------------------


def test_concatenates_chunks_within_a_run():
    assert _text_from_response(_response(5, "Hello ", "world", "!")) == "Hello world!"


def test_filters_non_text_events():
    resp = {
        "sse_events": [
            {"event": "tool_call", "data": {}},
            _chunk("only this"),
            {"event": "message_chunk", "data": {"content_type": "image", "content": "x"}},
        ],
    }
    assert _text_from_response(resp) == "only this"


# --- _run_block: the line that names a run -----------------------------------

_STARTED = datetime(2026, 10, 5, 18, 40, tzinfo=timezone.utc)


def test_a_run_reads_under_its_start_time_in_the_users_zone():
    block = _run_block(
        _response(1, created_at=_STARTED), "SEED_OK", user_zone("America/New_York")
    )

    assert block == "[Run started 2026-10-05 14:40 EDT, completed]\nSEED_OK"


def test_a_failed_run_without_text_says_so():
    run = _response(2, status="error", created_at=_STARTED)

    assert _run_block(run, "", _UTC) == "[Run started 2026-10-05 18:40 UTC, failed, no text]"


def test_a_report_back_names_the_analyst_thread_it_reports_on():
    run = _response(
        3, created_at=_STARTED, metadata={"report_back_ptc_thread_id": "a-thread"}
    )

    assert _run_block(run, "It failed.", _UTC).startswith(
        "[Run started 2026-10-05 18:40 UTC, completed, a report-back on analyst thread a-thread]"
    )


def test_a_retried_run_carries_its_attempt():
    run = _response(4, created_at=_STARTED, attempt_no=2, status="cancelled")

    assert _run_block(run, "", _UTC) == (
        "[Run started 2026-10-05 18:40 UTC, stopped, attempt 2 of the same request, no text]"
    )


def test_a_live_run_without_text_has_none_yet():
    run = _response(5, created_at=_STARTED, status="in_progress")

    assert _run_block(run, "", _UTC).endswith("still running, no text yet]")


def test_a_naive_start_time_is_read_as_utc():
    run = _response(6, created_at=_STARTED.replace(tzinfo=None))

    assert _run_block(run, "x", _UTC).startswith("[Run started 2026-10-05 18:40 UTC,")


def test_an_unknown_zone_reads_as_utc():
    assert user_zone("Not/AZone").key == "UTC"


# --- _truncate_single: one turn, head-truncated -----------------------------


def test_truncate_single_under_limit_is_unchanged():
    assert _truncate_single("short output") == "short output"


def test_truncate_single_at_exactly_cap_is_unchanged():
    text = "y" * MAX_OUTPUT_CHARS
    assert _truncate_single(text) == text


def test_truncate_single_keeps_head():
    text = "A" * (MAX_OUTPUT_CHARS + 500)
    out = _truncate_single(text)

    assert out.startswith("A")
    assert out.endswith("[truncated — full output available in workspace]")
    assert "earlier turns truncated" not in out


# --- _join_recent_turns: list-aware length cap ------------------------------


def test_join_under_limit_joins_with_separator():
    assert _join_recent_turns(["a", "b"]) == f"a{_TURN_SEPARATOR}b"


def test_join_empty_returns_empty():
    assert _join_recent_turns([]) == ""
    assert _join_recent_turns(["", ""]) == ""


def test_single_turn_with_markdown_divider_is_not_a_turn_boundary():
    """Regression: a long SINGLE turn containing a markdown '---' rule keeps its
    head and is never relabeled as multiple truncated turns. The divider used
    to be read as a turn separator, gutting the answer to a fragment under a
    false '[earlier turns truncated]' banner.
    """
    # One turn whose body has a markdown horizontal rule well past the cap.
    text = "LEAD " + "x" * MAX_OUTPUT_CHARS + _TURN_SEPARATOR + "footer"
    out = _join_recent_turns([text])

    assert out.startswith("LEAD ")
    assert "earlier turns truncated" not in out
    assert out.endswith("[truncated — full output available in workspace]")


def test_join_multi_turn_drops_oldest_keeps_newest():
    """Over the cap, whole older turns are dropped from the front."""
    oldest = "O" * MAX_OUTPUT_CHARS  # alone nearly fills the cap
    out = _join_recent_turns([oldest, "middle", "NEWEST"])

    assert out.endswith("NEWEST")
    assert out.startswith("[earlier turns truncated")
    assert "OOO" not in out  # the oldest turn is gone entirely


def test_join_multi_turn_huge_newest_keeps_newest_head():
    """When the newest turn alone exceeds the cap, keep its head (the start of
    the most-recent answer) and drop older turns — not the tail of the newest.
    """
    newest = "NEWSTART " + "z" * (MAX_OUTPUT_CHARS + 100)
    out = _join_recent_turns(["OLD answer", newest])

    assert out.startswith("[earlier turns truncated")
    assert "NEWSTART " in out  # newest turn's head survives
    assert "OLD answer" not in out  # older turn dropped
    assert out.endswith("[truncated — full output available in workspace]")


# --- main_text: where a run's text comes from --------------------------------

_COMMITTED = "src.server.services.history.committed.committed_texts"


@pytest.mark.asyncio
async def test_a_run_reads_the_text_its_turn_committed():
    committed = AsyncMock(return_value={"r-1": "committed text"})
    with patch(_RECENT, AsyncMock(return_value=[_response(1, "stored text")])), patch(
        _COMMITTED, committed
    ):
        runs = await _runs_to_read("t-1")
    assert runs[0]["main_text"] == "committed text"
    committed.assert_awaited_once_with("t-1", {"r-1": 1})


@pytest.mark.asyncio
async def test_a_run_without_committed_text_reads_its_stored_main_lane():
    sub = {
        "event": "message_chunk",
        "data": {"content_type": "text", "content": "sub", "agent": "task:abc"},
    }
    run = _response(1, "main")
    run["sse_events"].append(sub)
    with patch(_RECENT, AsyncMock(return_value=[run])):
        runs = await _runs_to_read("t-1")
    assert runs[0]["main_text"] == "main"


@pytest.mark.asyncio
async def test_only_runs_without_committed_text_read_their_stored_events(
    _stored_events,
):
    with patch(
        _RECENT, AsyncMock(return_value=[_response(1), _response(2, "stored")])
    ), patch(_COMMITTED, AsyncMock(return_value={"r-1": "committed"})):
        runs = await _runs_to_read("t-1", turns=2)
    assert [r["main_text"] for r in runs] == ["committed", "stored"]
    _stored_events.assert_awaited_once_with(["r-2"])
