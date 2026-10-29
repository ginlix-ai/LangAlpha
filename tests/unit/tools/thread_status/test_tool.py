"""ThreadStatus reads, stamps and stops against one thread's records.

Showing a command's end is its report, so a look stamps the command delivered
and its notice never opens a turn. A stop reaches only this thread's commands.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.limits import is_pending_read
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.services.commands.control import StopResult
from src.tools.thread_status.tool import build_thread_status_tool

THREAD = "00000000-0000-4000-8000-000000000001"
OTHER = "00000000-0000-4000-8000-000000000002"
CONFIG = {"configurable": {"timezone": "UTC"}}
LOG = ".agents/threads/00000000/commands/{}.log"


def _row(command_id: str, status: str, *, thread: str = THREAD, **row: Any) -> dict:
    now = datetime.now(UTC)
    return {
        "command_id": command_id,
        "conversation_thread_id": thread,
        "tool": "Bash",
        "description": "Run the backtest",
        "status": status,
        "started_at": now - timedelta(minutes=5),
        "ended_at": None if status == "running" else now - timedelta(minutes=1),
        "log_path": LOG.format(command_id),
        "reported": False,
        **row,
    }


@pytest.fixture
def db(monkeypatch):
    """Every source ThreadStatus reads, empty unless a test fills it. The
    ledger stamps every id it is handed unless a test says otherwise."""
    from src.server.database import automation
    from src.server.database.runs import command_runs, subagent_runs
    from src.server.database.wakeups import read as wakeups_read
    from src.server.services.commands import control
    from src.server.services.report_back.handoff import status

    fake = SimpleNamespace(
        list_for_thread=AsyncMock(return_value=[]),
        get_thread_command=AsyncMock(return_value=None),
        stamp_delivered=AsyncMock(side_effect=lambda ids: list(ids)),
        list_reporting_tasks=AsyncMock(return_value=[]),
        list_thread_wakeups=AsyncMock(return_value=[]),
        get_thread_wakeup=AsyncMock(return_value=None),
        list_all_automations=AsyncMock(return_value=[]),
        get_automation_file=AsyncMock(return_value=None),
        stop_thread_command=AsyncMock(return_value=StopResult("not_found")),
        read_owed_analysts=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(command_runs, "list_for_thread", fake.list_for_thread)
    monkeypatch.setattr(command_runs, "get_thread_command", fake.get_thread_command)
    monkeypatch.setattr(command_runs, "stamp_delivered", fake.stamp_delivered)
    monkeypatch.setattr(subagent_runs, "list_reporting_tasks", fake.list_reporting_tasks)
    monkeypatch.setattr(wakeups_read, "list_thread_wakeups", fake.list_thread_wakeups)
    monkeypatch.setattr(wakeups_read, "get_thread_wakeup", fake.get_thread_wakeup)
    monkeypatch.setattr(automation, "list_all_automations", fake.list_all_automations)
    monkeypatch.setattr(automation, "get_automation_file", fake.get_automation_file)
    monkeypatch.setattr(control, "stop_thread_command", fake.stop_thread_command)
    monkeypatch.setattr(status, "read_owed_analysts", fake.read_owed_analysts)
    return fake


def _tool(**kwargs: Any):
    return build_thread_status_tool(thread_id=THREAD, user_id="user-1", **kwargs)


async def _call(tool=None, **args: Any):
    tool = tool or _tool()
    call = {"type": "tool_call", "id": "call-1", "name": "ThreadStatus", "args": args}
    return await tool.ainvoke(call, CONFIG)


class TestTheListing:
    @pytest.mark.asyncio
    async def test_showing_command_ends_stamps_them_in_one_write(self, db):
        """Which of them owe a notice is the ledger's to say: a stopped one
        never does, and a delivered one is done (``stamp_delivered``)."""
        db.list_for_thread.return_value = [
            _row("bg-000000000001", "completed", exit_code=0),
            _row("bg-000000000002", "failed", exit_code=1),
            _row("bg-000000000003", "stopped", stopped_by="user"),
            _row("bg-000000000005", "running"),
        ]

        await _call()

        [(ids,)] = [c.args for c in db.stamp_delivered.await_args_list]
        assert set(ids) == {"bg-000000000001", "bg-000000000002", "bg-000000000003"}

    @pytest.mark.asyncio
    async def test_the_commands_it_stamped_ride_the_artifact_for_their_sources(self, db):
        """Their notices are dropped, and the provenance middleware emits
        what those would have brought; a stamp another path made first
        brought them already."""
        db.list_for_thread.return_value = [
            _row("bg-000000000001", "completed", exit_code=0),
            _row("bg-000000000002", "failed", exit_code=1),
        ]
        db.stamp_delivered.side_effect = lambda ids: ["bg-000000000001"]

        message = await _call()

        assert message.artifact == {"reported_commands": ["bg-000000000001"]}

    @pytest.mark.asyncio
    async def test_a_look_that_stamps_nothing_carries_no_commands(self, db):
        db.list_for_thread.return_value = [_row("bg-000000000001", "completed", reported=True)]
        db.stamp_delivered.side_effect = lambda ids: []
        message = await _call()
        assert not (message.artifact or {}).get("reported_commands")

    @pytest.mark.asyncio
    async def test_a_command_past_the_shown_ten_stays_owed(self, db):
        db.list_for_thread.return_value = [
            _row(
                f"bg-0000000000{n:02d}",
                "completed",
                ended_at=datetime.now(UTC) - timedelta(minutes=n),
            )
            for n in range(11)
        ]

        await _call()

        [(ids,)] = [c.args for c in db.stamp_delivered.await_args_list]
        assert "bg-000000000010" not in ids and len(ids) == 10

    @pytest.mark.asyncio
    async def test_a_running_item_marks_the_read_a_poll(self, db):
        db.list_for_thread.return_value = [_row("bg-000000000001", "running")]

        message = await _call()

        assert is_pending_read(message)
        assert "bg-000000000001  command · Bash · Run the backtest · 5m" in message.content

    @pytest.mark.asyncio
    async def test_nothing_running_is_not_a_poll(self, db):
        db.list_for_thread.return_value = [_row("bg-000000000001", "completed")]

        message = await _call()

        assert not is_pending_read(message)

    @pytest.mark.asyncio
    async def test_a_reported_command_says_so(self, db):
        db.list_for_thread.return_value = [_row("bg-000000000001", "completed", reported=True)]

        message = await _call(type="command")

        assert "completed after 4m · reported" in message.content

    @pytest.mark.asyncio
    async def test_a_source_that_fails_is_named_and_the_rest_listed(self, db):
        db.list_for_thread.return_value = [_row("bg-000000000001", "running")]
        db.list_reporting_tasks.side_effect = RuntimeError("db down")

        message = await _call()

        assert "bg-000000000001" in message.content
        assert message.content.endswith("\nCould not read tasks just now.")

    @pytest.mark.asyncio
    async def test_analysts_are_listed_on_a_chief_of_staff_turn_only(self, db):
        message = await _call(type="analyst")

        assert message.content == "Analysts are listed on a Chief of Staff turn only."

    @pytest.mark.asyncio
    async def test_an_analyst_listed_by_its_thread_id_has_a_detail(self, db):
        analyst = "00000000-0000-4000-8000-0000000000a1"
        db.read_owed_analysts.return_value = [
            {"thread_id": analyst, "workspace_name": "Semis", "title": "NVDA margins"}
        ]
        tool = _tool(analysts=True)

        listed = await _call(tool)
        assert analyst in listed.content

        message = await _call(tool, id=analyst.upper())
        assert message.content == f"{analyst}  analyst · Semis · NVDA margins\nStatus: working"
        assert is_pending_read(message)

        done = await _call(tool, id="00000000-0000-4000-8000-0000000000a2")
        assert done.content.startswith("No 00000000-0000-4000-8000-0000000000a2 in this conversation.")

    @pytest.mark.asyncio
    async def test_an_analyst_detail_whose_registry_cannot_be_read_says_so(self, db):
        db.read_owed_analysts.side_effect = RuntimeError("redis down")

        message = await _call(_tool(analysts=True), id="00000000-0000-4000-8000-0000000000a1")

        assert message.content == "Could not read analysts just now."
        assert not is_pending_read(message)

    @pytest.mark.asyncio
    async def test_automations_of_other_threads_are_left_out(self, db):
        now = datetime.now(UTC)
        automation = {
            "name": "Morning brief",
            "trigger_type": "cron",
            "cron_expression": "30 8 * * 1-5",
            "timezone": "UTC",
            "status": "active",
            "next_run_at": now + timedelta(hours=3),
        }
        db.list_all_automations.return_value = [
            {**automation, "file_name": "here.json", "conversation_thread_id": THREAD},
            {**automation, "file_name": "there.json", "conversation_thread_id": OTHER},
        ]

        message = await _call(type="automation")

        assert "here.json" in message.content
        assert "there.json" not in message.content


class TestOneItem:
    @pytest.mark.asyncio
    async def test_a_command_detail_stamps_an_owed_end(self, db):
        db.get_thread_command.return_value = _row(
            "bg-000000000001", "completed", output_tail="done"
        )

        message = await _call(id="bg-000000000001")

        assert message.content.startswith("bg-000000000001  command · Bash · Run the backtest")
        db.stamp_delivered.assert_awaited_once_with(["bg-000000000001"])
        assert message.artifact == {"reported_commands": ["bg-000000000001"]}

    @pytest.mark.asyncio
    async def test_a_running_command_detail_stamps_nothing_and_reads_as_a_poll(self, db):
        db.get_thread_command.return_value = _row("bg-000000000001", "running")

        message = await _call(id="bg-000000000001")

        db.stamp_delivered.assert_not_awaited()
        assert is_pending_read(message)

    @pytest.mark.asyncio
    async def test_an_ended_command_detail_is_not_a_poll(self, db):
        db.get_thread_command.return_value = _row("bg-000000000001", "completed", exit_code=0)
        assert not is_pending_read(await _call(id="bg-000000000001"))

    @pytest.mark.asyncio
    async def test_a_running_task_or_automation_detail_reads_as_a_poll(self, db):
        _task(db, "abc123")
        assert is_pending_read(await _call(id="Task-abc123"))

        db.get_automation_file.return_value = {
            "file_name": "daily.json",
            "name": "Daily",
            "conversation_thread_id": THREAD,
            "status": "active",
            "last_execution": {"status": "running", "started_at": datetime.now(UTC)},
        }
        assert is_pending_read(await _call(id="daily"))
        db.get_automation_file.assert_awaited_with("user-1", "daily")

    @pytest.mark.asyncio
    async def test_another_threads_automation_is_unknown(self, db):
        db.get_automation_file.return_value = {
            "file_name": "daily.json",
            "name": "Daily",
            "conversation_thread_id": OTHER,
            "status": "active",
        }

        message = await _call(id="daily.json")

        assert message.content == "No daily.json in this conversation. ThreadStatus lists the ids."

    @pytest.mark.asyncio
    async def test_another_threads_command_is_unknown(self, db):
        message = await _call(id="bg-000000000001")

        db.get_thread_command.assert_awaited_once_with(THREAD, "bg-000000000001")
        assert message.content == (
            "No bg-000000000001 in this conversation. ThreadStatus lists the ids."
        )
        db.stamp_delivered.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_detail_that_cannot_be_read_says_so(self, db):
        db.get_thread_command.side_effect = RuntimeError("pool closed")

        message = await _call(id="bg-000000000001")

        assert message.content == "Could not read commands just now."

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("item_id", "answer"),
        [
            ("bg-000000000001", "No bg-000000000001 in this conversation. ThreadStatus lists the ids."),
            ("Task-zzzzzz", "No Task-zzzzzz in this conversation. ThreadStatus lists the ids."),
            # A bare id a task missed may still be a file's name.
            ("zzzzzz", "Could not read automations just now."),
        ],
    )
    async def test_an_id_its_own_type_misses_is_unknown_whatever_the_automations(
        self, db, item_id, answer
    ):
        """The automations claim any id, so their read failing says nothing
        about an id in another type's own form that type read as not here."""
        db.get_automation_file.side_effect = RuntimeError("pool closed")

        message = await _call(id=item_id)

        assert message.content == answer


def _task(db, task_id: str) -> None:
    db.list_reporting_tasks.return_value = [
        {
            "task_id": task_id,
            "description": "Compare momentum papers",
            "subagent_type": "research",
            "status": "in_progress",
            "started_at": datetime.now(UTC) - timedelta(minutes=2),
        }
    ]


class TestATaskById:
    @pytest.mark.parametrize("given", ["Task-L_EAWw", "L_EAWw"])
    @pytest.mark.asyncio
    async def test_either_name_of_a_task_finds_it(self, db, given):
        # Minted ids are url-safe, so a bare one can hold _ or -.
        _task(db, "L_EAWw")

        message = await _call(id=given)

        db.list_reporting_tasks.assert_awaited_once_with(THREAD, task_id="L_EAWw")
        assert message.content.startswith("Task-L_EAWw  task · research")
        assert message.content.endswith('TaskOutput(task_id="L_EAWw") reads its result.')

    @pytest.mark.asyncio
    async def test_an_unknown_task_says_so(self, db):
        message = await _call(id="Task-zzzzzz")

        assert message.content == "No Task-zzzzzz in this conversation. ThreadStatus lists the ids."

    @pytest.mark.asyncio
    async def test_a_wakeup_id_that_is_no_task_finds_the_wakeup(self, db):
        db.get_thread_wakeup.return_value = {
            "short_id": "w_3fa9",
            "kind": "at",
            "status": "fired",
            "label": "Look again",
            "schedule": [datetime.now(UTC) - timedelta(hours=1)],
            "last_fired_at": datetime.now(UTC) - timedelta(hours=1),
        }

        message = await _call(id="w_3fa9")

        db.get_thread_wakeup.assert_awaited_once_with(THREAD, "w_3fa9")
        assert message.content.startswith("w_3fa9  wakeup · Look again · fired")

    @pytest.mark.asyncio
    async def test_a_running_task_shows_this_workers_progress_once_started(self, db):
        """A task registered but not started has no progress of its own yet
        (a resume keeps the earlier round's counts until it starts)."""
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        task = await registry.register(
            tool_call_id="tc-1",
            description="Compare momentum papers",
            prompt="p",
            subagent_type="research",
            asyncio_task=None,
            run_id="run-1",
        )
        task.total_tool_calls, task.current_tool = 14, "WebFetch"
        _task(db, task.task_id)
        tool = _tool(registry=registry)

        assert "tool calls" not in (await _call(tool, type="task")).content

        task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
        try:
            listed = await _call(tool, type="task")
        finally:
            task.asyncio_task.cancel()
        assert listed.content.endswith(" · 14 tool calls, now WebFetch")


class TestStop:
    @pytest.mark.asyncio
    async def test_a_running_command_is_stopped_by_the_agent(self, db):
        db.stop_thread_command.return_value = StopResult(
            "stopped", _row("bg-000000000001", "stopped", stopped_by="agent")
        )

        message = await _call(stop="bg-000000000001")

        db.stop_thread_command.assert_awaited_once_with(THREAD, "bg-000000000001", by="agent")
        assert message.content == (
            "Stopped bg-000000000001. It does not report back. What it wrote is in "
            ".agents/threads/00000000/commands/bg-000000000001.log."
        )

    @pytest.mark.asyncio
    async def test_a_stop_names_its_command_for_a_view_that_missed_the_call(self, db):
        """A view reconnected since the call began never saw it, and re-reads
        the thread's commands on the command a result's artifact names."""
        db.stop_thread_command.return_value = StopResult(
            "stopped", _row("bg-000000000001", "stopped", stopped_by="agent")
        )

        message = await _call(stop="bg-000000000001")

        assert message.artifact == {"command_id": "bg-000000000001"}

    @pytest.mark.asyncio
    async def test_a_stop_its_computer_did_not_answer_is_under_way(self, db):
        db.stop_thread_command.return_value = StopResult(
            "stopping", _row("bg-000000000001", "running", stopped_by="agent")
        )

        message = await _call(stop="bg-000000000001")

        assert message.content.startswith(
            "Stopping bg-000000000001: its computer did not answer in time"
        )

    @pytest.mark.asyncio
    async def test_a_stop_that_raises_says_it_may_still_run(self, db):
        db.stop_thread_command.side_effect = RuntimeError("no route")

        message = await _call(stop="bg-000000000001")

        assert message.content == (
            "ERROR: Could not stop bg-000000000001, so it may still be running: no route"
        )

    @pytest.mark.asyncio
    async def test_an_ended_command_is_not_stopped_and_its_end_is_seen(self, db):
        db.stop_thread_command.return_value = StopResult(
            "ended", _row("bg-000000000001", "completed")
        )

        message = await _call(stop="bg-000000000001")

        assert message.content.startswith("bg-000000000001 is not running: completed after 4m.")
        db.stamp_delivered.assert_awaited_once_with(["bg-000000000001"])

    @pytest.mark.asyncio
    async def test_a_command_the_thread_does_not_have_is_unknown(self, db):
        message = await _call(stop="bg-000000000001")

        assert message.content.startswith("No bg-000000000001 in this conversation.")

    @pytest.mark.asyncio
    async def test_only_a_command_can_be_stopped(self, db):
        message = await _call(stop="Task-k7Xm2p")

        db.stop_thread_command.assert_not_awaited()
        assert message.content.startswith("ERROR: Task-k7Xm2p is not a background command.")
