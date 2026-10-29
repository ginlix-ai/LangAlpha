"""ThreadStatus's wording: the listing's layout and each item's state.

The layout is the approved one the agent reads, so it is pinned whole.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.tools.thread_status import listing
from src.tools.thread_status.listing import Item, render_listing

NOW = datetime(2026, 10, 23, 15, 0, tzinfo=UTC)  # a Friday; 11:00 in New York
TZ = "America/New_York"
LOG = ".agents/threads/3f9a1c2e/commands/{}.log"


def _command(command_id: str, status: str = "running", **row) -> dict:
    return {
        "command_id": command_id,
        "tool": "Bash",
        "description": "Run 10-year backtest",
        "status": status,
        "started_at": NOW - timedelta(minutes=17),
        "log_path": LOG.format(command_id),
        **row,
    }


def _ended(command_id: str, minutes_ago: int) -> Item:
    row = _command(
        command_id,
        "completed",
        exit_code=0,
        started_at=NOW - timedelta(minutes=minutes_ago + 1),
        ended_at=NOW - timedelta(minutes=minutes_ago),
        reported=True,
    )
    return listing.command_item(row, NOW)


class TestTheListing:
    def test_the_full_view_reads_as_approved(self, monkeypatch):
        monkeypatch.setattr(listing, "LISTED_ENDED_MAX", 1)
        items = [
            listing.command_item(_command("bg-3f9a1c2e7d10"), NOW),
            listing.task_item(
                {
                    "task_id": "k7Xm2p",
                    "subagent_type": "research",
                    "description": "Compare momentum papers",
                    "status": "in_progress",
                    "started_at": NOW - timedelta(minutes=6),
                },
                NOW,
                listing.task_progress(14, "WebFetch"),
            ),
            listing.wakeup_item(
                {
                    "short_id": "w_3fa9",
                    "kind": "at",
                    "status": "armed",
                    "label": "Check the backtest and post the tearsheet",
                    "schedule": [datetime(2026, 10, 23, 20, 0, tzinfo=UTC)],
                },
                NOW,
                TZ,
            ),
            listing.automation_item(
                {
                    "file_name": "morning-brief.json",
                    "name": "Morning brief",
                    "trigger_type": "cron",
                    "cron_expression": "30 8 * * 1-5",
                    "timezone": TZ,
                    "status": "active",
                    "next_run_at": datetime(2026, 10, 26, 12, 30, tzinfo=UTC),
                    "last_execution": {"status": "completed"},
                },
                NOW,
            ),
            listing.command_item(
                _command(
                    "bg-77c0e41a9b2e",
                    "failed",
                    tool="ExecuteCode",
                    description="Pull filings for 500 tickers",
                    exit_code=1,
                    failure_reason="exit",
                    started_at=NOW - timedelta(minutes=10),
                    ended_at=NOW - timedelta(minutes=3),
                    log_path=LOG.format("bg-77c0e41a9b2e"),
                    reported=True,
                ),
                NOW,
            ),
            *(_ended(f"bg-00000000000{n}", 30 + n) for n in range(3)),
        ]

        shown = render_listing(items)

        assert shown.text == (
            "Running\n"
            "  bg-3f9a1c2e7d10     command · Bash · Run 10-year backtest · 17m\n"
            "                      log .agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log\n"
            "  Task-k7Xm2p         task · research · Compare momentum papers · 6m · 14 tool calls, now WebFetch\n"
            "Pending\n"
            "  w_3fa9              wakeup · Check the backtest and post the tearsheet · at 16:00 today\n"
            "  morning-brief.json  automation · Morning brief · weekdays 08:30 · next run Mon 08:30 · last run completed\n"
            "Completed, failed or stopped in the last 24 hours\n"
            "  bg-77c0e41a9b2e     command · ExecuteCode · Pull filings for 500 tickers · failed, exit 1 after 7m · reported\n"
            "                      log .agents/threads/3f9a1c2e/commands/bg-77c0e41a9b2e.log\n"
            "  +3 more. Filter by status (completed, failed or stopped) to see them."
        )
        assert shown.running
        assert [i.id for i in shown.shown_ended] == ["bg-77c0e41a9b2e"]

    def test_ended_items_list_newest_first_up_to_ten(self):
        items = [_ended(f"bg-0000000000{n:02d}", n) for n in range(12)]

        shown = render_listing(items)

        assert [i.id for i in shown.shown_ended] == [f"bg-0000000000{n:02d}" for n in range(10)]
        assert shown.text.endswith(
            "  +2 more. Filter by status (completed, failed or stopped) to see them."
        )
        assert not shown.running

    def test_a_view_filtered_to_an_ended_status_names_it(self, monkeypatch):
        monkeypatch.setattr(listing, "FILTERED_ENDED_MAX", 1)
        items = [_ended(f"bg-00000000000{n}", n) for n in range(3)]

        shown = render_listing(items, status="completed")

        assert shown.text.splitlines()[0] == "Completed in the last 24 hours"
        assert shown.text.endswith("\n  +2 more.")

    def test_a_filter_keeps_only_its_status(self):
        items = [
            listing.command_item(_command("bg-3f9a1c2e7d10"), NOW),
            _ended("bg-000000000001", 5),
        ]

        shown = render_listing(items, status="running")

        assert "bg-000000000001" not in shown.text
        assert shown.shown_ended == ()

    @pytest.mark.parametrize(
        ("status", "text"),
        [
            (
                None,
                "Nothing is running or set to report back to this conversation, and "
                "nothing ended in the last 24 hours.",
            ),
            ("running", "Nothing is running."),
            ("pending", "Nothing is pending."),
            ("failed", "Nothing failed in the last 24 hours."),
        ],
    )
    def test_an_empty_view_says_so(self, status, text):
        assert render_listing([], status=status).text == text


class TestCommandState:
    @pytest.mark.parametrize(
        ("row", "state"),
        [
            ({"status": "running"}, "17m"),
            ({"status": "running", "stopped_by": "agent"}, "17m, stopping"),
            ({"status": "completed", "exit_code": 0}, "completed after 7m"),
            ({"status": "stopped", "stopped_by": "agent"}, "stopped by you after 7m"),
            ({"status": "stopped", "stopped_by": "user"}, "stopped by the user after 7m"),
            ({"status": "stopped", "stopped_by": "archive"}, "stopped after 7m"),
            ({"status": "failed", "exit_code": 2, "failure_reason": "exit"}, "failed, exit 2 after 7m"),
            ({"status": "failed", "failure_reason": "lost"}, "failed after 7m: its computer stopped"),
            (
                {"status": "failed", "failure_reason": "vanished"},
                "failed after 7m: it ended without recording how",
            ),
            ({"status": "failed", "exit_code": 143, "failure_reason": "exit"}, "killed by SIGTERM after 7m"),
            ({"status": "failed", "exit_code": 255, "failure_reason": "exit"}, "failed, exit 255 after 7m"),
            (
                {"status": "failed", "failure_reason": "after_turn_limit"},
                "failed after 7m: stopped an hour after your turn ended",
            ),
        ],
    )
    def test_each_state(self, row, state):
        row = {
            "started_at": NOW - timedelta(minutes=17),
            "ended_at": NOW - timedelta(minutes=10),
            **row,
        }
        assert listing.command_state(row, NOW) == state

    def test_the_detail_names_the_log_and_the_end_of_it(self):
        row = _command(
            "bg-3f9a1c2e7d10",
            "completed",
            exit_code=0,
            ended_at=NOW - timedelta(minutes=16),
            command_preview="python backtest.py --years 10",
            output_tail="\n".join(f"line {n}" for n in range(30)),
        )

        detail = listing.command_detail(row, NOW)

        lines = detail.splitlines()
        assert lines[:5] == [
            "bg-3f9a1c2e7d10  command · Bash · Run 10-year backtest",
            "Status: completed after 1m",
            "Log: .agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log",
            "Command: python backtest.py --years 10",
            "Last lines of the log:",
        ]
        assert lines[5:] == [f"line {n}" for n in range(10, 30)]

    def test_a_running_detail_shows_no_log_lines(self):
        row = _command("bg-3f9a1c2e7d10", output_tail="partial")

        detail = listing.command_detail(row, NOW)

        assert "Status: running for 17m" in detail
        assert "partial" not in detail


class TestOtherItems:
    @pytest.mark.parametrize(
        ("raw", "status", "state"),
        [
            ("completed", "completed", "completed after 4m"),
            ("error", "failed", "failed after 4m"),
            ("cancelled", "stopped", "stopped after 4m"),
            ("interrupted", "stopped", "stopped at an interrupt after 4m"),
        ],
    )
    def test_a_task_in_the_one_vocabulary(self, raw, status, state):
        row = {
            "task_id": "k7Xm2p",
            "status": raw,
            "started_at": NOW - timedelta(minutes=5),
            "finalized_at": NOW - timedelta(minutes=1),
        }
        item = listing.task_item(row, NOW, "3 tool calls")

        assert item.status == status
        assert item.text == f"subagent · {state}"

    @pytest.mark.parametrize(
        ("row", "status", "text"),
        [
            ({"status": "fired", "last_fired_at": NOW - timedelta(hours=1)}, "completed", "Look again · fired 10:00 today"),
            ({"status": "cancelled"}, "stopped", "Look again · cancelled"),
            ({"status": "armed", "expires_at": NOW - timedelta(minutes=1)}, "completed", "Look again · expired"),
        ],
    )
    def test_an_ended_wakeup(self, row, status, text):
        row = {
            "short_id": "w_3fa9",
            "kind": "at",
            "label": "Look again",
            "schedule": [NOW - timedelta(hours=1)],
            "updated_at": NOW,
            **row,
        }
        item = listing.wakeup_item(row, NOW, TZ)

        assert (item.status, item.text) == (status, text)

    @pytest.mark.parametrize(
        ("expr", "words"),
        [
            ("30 8 * * 1-5", "weekdays 08:30"),
            ("0 9 * * *", "daily 09:00"),
            ("0 18 * * 5", "Fridays 18:00"),
            ("0 18 * * 0", "Sundays 18:00"),
            ("*/5 * * * *", None),
            ("0 9 1 * *", None),
        ],
    )
    def test_cron_words(self, expr, words):
        assert listing.cron_words(expr) == words

    def test_an_automation_that_neither_runs_nor_ran_lately_is_left_out(self):
        row = {
            "file_name": "old.json",
            "name": "Old",
            "trigger_type": "once",
            "status": "completed",
            "last_execution": {"status": "completed", "completed_at": NOW - timedelta(days=3)},
        }
        assert listing.automation_item(row, NOW) is None
