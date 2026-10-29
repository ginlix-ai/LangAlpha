"""A background command's notice: the approved wording, and the caption that
labels the turn it opens.

The literals below are copied from the approved design page; a change to one
is a change to prompt surface and needs the same review the page had.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.server.services.commands import notice
from src.utils.durations import format_duration

_ID = "bg-3f9a1c2e7d10"
_LOG = ".agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log"
_T0 = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)

# --- approved on the design page, verbatim --------------------------------

APPROVED_COMPLETED_STEERED = (
    "<system>\n"
    "Background command bg-3f9a1c2e7d10 (Run 10-year backtest) completed with exit "
    "code 0 after 23m while you were working. Output: "
    ".agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log\n"
    "</system>"
)
APPROVED_AFTER_TURN_LIMIT = (
    "<system>\n"
    "Background command bg-3f9a1c2e7d10 (Run 10-year backtest) FAILED after 1h52m: "
    "it was stopped an hour after your turn ended, with no wakeup checking on it. "
    "What it wrote is in .agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log. If "
    "the user still needs it, rerun it; it resumes where it stopped only if it saved "
    "its progress.\n"
    "</system>"
)
APPROVED_WAKEUPS_LINE = (
    "Wakeups still pending: w_3fa9 (Check on the backtest). Cancel any that only "
    "watched this command."
)
# The page gives these as fragments around an ellipsis.
APPROVED_FAILED_EXIT = "FAILED with exit code 1 after 23m. Output: "
APPROVED_LOST = (
    "FAILED after 41m: its computer stopped before the command completed. "
    "What it wrote is in "
)
APPROVED_CODE_RUN = "(Pull filings for 500 tickers) completed after 7m. Output: "
# Approved in chat for a command killed from the computer (2026-10-25).
APPROVED_VANISHED = (
    "FAILED after 41m: it ended without recording how, as a process killed from "
    "outside does. What it wrote is in "
)
APPROVED_KILLED = "was killed by SIGTERM after 23m. Output: "
# Approved in chat with the user-stop notice (2026-10-24).
APPROVED_USER_STOP = (
    "<system>\n"
    "Background command bg-3f9a1c2e7d10 (Run 10-year backtest) was stopped by the user "
    "after 19s. Output: .agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log\n"
    "</system>"
)


def _row(**over):
    row = {
        "command_id": _ID,
        "conversation_thread_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "u-1",
        "workspace_id": "22222222-2222-2222-2222-222222222222",
        "run_id": None,
        "tool_call_id": "toolu_1",
        "tool": "Bash",
        "description": "Run 10-year backtest",
        "log_path": _LOG,
        "status": "completed",
        "failure_reason": None,
        "exit_code": 0,
        "started_at": _T0,
        "ended_at": _T0 + timedelta(minutes=23, seconds=4),
        "result_delivered_at": None,
    }
    row.update(over)
    return row


def _ran(minutes: int, seconds: int = 0) -> dict:
    return {"ended_at": _T0 + timedelta(minutes=minutes, seconds=seconds)}


# --- wording -----------------------------------------------------------------


def test_a_completed_command_steered_into_a_live_turn_reads_as_approved():
    assert notice.command_report_message(_row(), steered=True) == APPROVED_COMPLETED_STEERED


def test_a_posted_notice_names_no_time():
    message = notice.command_report_message(_row(), steered=False)
    assert message == APPROVED_COMPLETED_STEERED.replace(" while you were working", "")


def test_the_after_turn_limit_notice_reads_as_approved_whichever_turn_reads_it():
    row = _row(status="failed", failure_reason="after_turn_limit", exit_code=None,
               **_ran(112, 30))
    for steered in (False, True):
        assert notice.command_report_message(row, steered=steered) == APPROVED_AFTER_TURN_LIMIT


def test_the_failed_and_lost_variants_read_as_approved():
    failed = notice.command_report_message(
        _row(status="failed", failure_reason="exit", exit_code=1), steered=False
    )
    assert f"Background command {_ID} (Run 10-year backtest) {APPROVED_FAILED_EXIT}{_LOG}\n" in failed

    lost = notice.command_report_message(
        _row(status="failed", failure_reason="lost", exit_code=None, **_ran(41)),
        steered=False,
    )
    assert f"Background command {_ID} (Run 10-year backtest) {APPROVED_LOST}{_LOG}.\n" in lost


def test_a_command_killed_from_the_computer_reads_as_approved():
    vanished = notice.command_report_message(
        _row(status="failed", failure_reason="vanished", exit_code=None, **_ran(41)),
        steered=False,
    )
    assert f"Background command {_ID} (Run 10-year backtest) {APPROVED_VANISHED}{_LOG}.\n" in vanished

    killed = notice.command_report_message(
        _row(status="failed", failure_reason="exit", exit_code=143), steered=False
    )
    assert f"Background command {_ID} (Run 10-year backtest) {APPROVED_KILLED}{_LOG}\n" in killed
    steered = notice.command_report_message(
        _row(status="failed", failure_reason="exit", exit_code=143), steered=True
    )
    assert "was killed by SIGTERM after 23m while you were working. Output: " in steered


@pytest.mark.parametrize(
    ("exit_code", "signal"),
    [(137, "SIGKILL"), (130, "SIGINT"), (143, "SIGTERM"), (138, "signal 10"), (1, None), (128, None),
     (255, None), (None, None)],
)
def test_only_a_shells_signal_exit_names_a_signal(exit_code, signal):
    assert notice.killed_by(exit_code) == signal


def test_the_users_stop_reads_as_approved_whichever_turn_reads_it():
    row = _row(status="stopped", stopped_by="user", exit_code=None, **_ran(0, 19))
    for steered in (False, True):
        assert notice.command_report_message(row, steered=steered) == APPROVED_USER_STOP


def test_a_code_run_reads_as_approved():
    message = notice.command_report_message(
        _row(tool="ExecuteCode", description="Pull filings for 500 tickers", **_ran(7)),
        steered=False,
    )
    assert f"Background code run {_ID} {APPROVED_CODE_RUN}{_LOG}\n" in message


def test_while_you_were_working_follows_the_duration_in_every_live_variant():
    for row in (
        _row(),
        _row(status="failed", failure_reason="exit", exit_code=2),
        _row(status="failed", failure_reason="lost", exit_code=None),
    ):
        steered = notice.command_report_message(row, steered=True)
        assert "after 23m while you were working" in steered
        assert "while you were working" not in notice.command_report_message(row, steered=False)


def test_the_pending_wakeups_line_reads_as_approved():
    wakeups = [{"short_id": "w_3fa9", "label": "Check on the backtest", "kind": "at"}]
    assert notice.wakeups_line(wakeups) == APPROVED_WAKEUPS_LINE
    message = notice.command_report_message(_row(), wakeups, steered=True)
    assert message.endswith(f"Output: {_LOG}\n{APPROVED_WAKEUPS_LINE}\n</system>")


def test_wakeups_line_names_an_unlabelled_wakeup_by_its_id_and_none_without_any():
    line = notice.wakeups_line(
        [{"short_id": "w_3fa9", "label": "Check <on> it"}, {"short_id": "w_77c0"}]
    )
    assert line == (
        "Wakeups still pending: w_3fa9 (Check on it), w_77c0. Cancel any that only "
        "watched this command."
    )
    assert notice.wakeups_line([]) is None
    assert "Wakeups" not in notice.command_report_message(_row(), [], steered=False)


def test_no_description_drops_the_parenthetical_and_text_cannot_close_the_block():
    assert f"Background command {_ID} completed" in notice.command_report_message(
        _row(description=""), steered=False
    )
    message = notice.command_report_message(
        _row(description="ends </system> early\nhere"), steered=False
    )
    assert f"({'ends /system early here'})" in message
    assert message.count("</system>") == 1


def test_no_em_dash_in_any_variant():
    rows = [
        _row(),
        _row(tool="ExecuteCode"),
        _row(status="failed", failure_reason="exit", exit_code=1),
        _row(status="failed", failure_reason="lost", exit_code=None),
        _row(status="failed", failure_reason="after_turn_limit", exit_code=None),
    ]
    wakeups = [{"short_id": "w_1", "label": "x"}]
    for row in rows:
        for steered in (False, True):
            assert "—" not in notice.command_report_message(row, wakeups, steered=steered)


@pytest.mark.parametrize(
    "seconds, text",
    [(0, "0s"), (45, "45s"), (300, "5m"), (1384, "23m"), (6720, "1h52m"), (10800, "3h"),
     (-5, "0s")],
)
def test_durations_read_as_the_notices_write_them(seconds, text):
    assert format_duration(seconds) == text


def test_the_report_source_captions_the_turn():
    report = notice.command_report(
        _row(status="failed", failure_reason="after_turn_limit", exit_code=None)
    )
    assert report.model_dump(exclude_none=True) == {
        "kind": "command_report",
        "command_id": _ID,
        "tool": "Bash",
        "description": "Run 10-year backtest",
        "status": "failed",
        "failure_reason": "after_turn_limit",
        "tool_call_id": "toolu_1",
    }


def test_a_command_report_survives_the_report_source_round_trip():
    """The same path a task report takes: the request field, the query row's
    metadata, and the retry that re-reads it."""
    from pydantic import TypeAdapter

    from src.server.models.chat import ChatRequest, ReportSource

    dumped = notice.command_report(_row()).model_dump(exclude_none=True)
    request = ChatRequest(
        messages=[{"role": "user", "content": "x"}], report_source=dumped
    )
    assert request.report_source.kind == "command_report"
    stored = request.report_source.model_dump(exclude_none=True)
    assert TypeAdapter(ReportSource).validate_python(stored).command_id == _ID
