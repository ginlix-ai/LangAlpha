"""Messages into a running background task, on the steer outbox.

Both producers (the user's steer POST and the main agent's
``Task(action="update")``) admit a ``steer`` row onto the task's chain, fenced
to the run they were sent to. The task run holds them before its next model
call and joins everything held in one step into a single instruction; the
model call that reads them confirms them, because the step that injected them
is checkpointed by then. Whatever the run never reads is settled by its
ending, in the ledger, so the run keeps no queue of its own to sweep.

The delivery names each entry it took, and who sent it, in the captured event
and in the stamp checkpoint replay re-emits, so a client settles its own
instruction by id however the text was joined, and labels each by sender.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.background_subagent import (
    middleware as bg_middleware,
)
from ptc_agent.agent.middleware.background_subagent import (
    redis_stream,
    run_executor,
)
from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.steering import (
    SubagentSteeringMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.task_actions import (
    handle_task_action,
)
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.report_back.steer import SteeredReportInbox

_OUTBOX = "src.server.database.runs.outbox"
_CALL = "call-1"
_RUN = "run-A"
_TASK = "k7Xm2p"


class _Inbox:
    def __init__(self, taken: list[dict]) -> None:
        self.taken = taken
        self.calls: list[tuple] = []
        self.confirmed: list[tuple] = []

    async def take_task(self, thread_id: str, task_id: str, task_run_id: str):
        self.calls.append((thread_id, task_id, task_run_id))
        return self.taken

    async def confirm_task(self, task_run_id: str, job_ids: list[str]) -> None:
        self.confirmed.append((task_run_id, job_ids))


class _Registry:
    def __init__(self, inbox, task_run_id: str | None = _RUN) -> None:
        self._tasks = {
            _CALL: SimpleNamespace(task_id=_TASK, task_run_id=task_run_id)
        }
        self.events: list[dict] = []
        self.thread_id = "t-1"
        self.report_inbox = inbox

    async def append_captured_event(self, tool_call_id: str, event: dict) -> None:
        self.events.append(event)


async def _drain(registry):
    token = current_background_tool_call_id.set(_CALL)
    try:
        return await SubagentSteeringMiddleware(registry).abefore_model(
            {}, MagicMock()
        )
    finally:
        current_background_tool_call_id.reset(token)


ENTRIES = [
    {"input_id": "u-1", "content": "Focus on margins", "sender": "user"},
    {"input_id": "m-1", "content": "Also cover 2024 guidance", "sender": "agent"},
]


@pytest.fixture
def mixed_queue() -> _Registry:
    """The user's instruction, then the main agent's follow-up, same run."""
    return _Registry(
        _Inbox([{"job_id": f"j-{i}", **e} for i, e in enumerate(ENTRIES)])
    )


class TestTake:
    @pytest.mark.asyncio
    async def test_delivery_names_each_entry_it_took(self, mixed_queue):
        registry = mixed_queue

        result = await _drain(registry)

        (msg,) = result["messages"]
        assert msg.content == (
            "The user sent a new message while you were working:"
            "\nFocus on margins"
            "\n\nThe parent agent sent a new message while you were working:"
            "\nAlso cover 2024 guidance"
            "\n\nAddress the messages above as you continue this turn."
        )
        assert msg.additional_kwargs["steering_delivered"]["entries"] == ENTRIES
        # Held for this run, for the model call that reads them to confirm.
        assert registry.report_inbox.calls == [("t-1", _TASK, _RUN)]
        assert msg.additional_kwargs["held_jobs"] == {
            "task_run_id": _RUN,
            "job_ids": ["j-0", "j-1"],
        }

    @pytest.mark.asyncio
    async def test_the_captured_event_matches_the_stamp(self, mixed_queue):
        """Live and checkpoint replay must show the same delivery."""
        result = await _drain(mixed_queue)

        (event,) = mixed_queue.events
        assert event["event"] == "steering_delivered"
        delivered = result["messages"][0].additional_kwargs["steering_delivered"]
        assert event["data"] == {"agent": f"task:{_TASK}", **delivered}
        assert delivered == {
            "content": "Focus on margins\nAlso cover 2024 guidance",
            "count": 2,
            "input_ids": ["u-1", "m-1"],
            "entries": ENTRIES,
        }

    @pytest.mark.asyncio
    async def test_checkpoint_replay_carries_the_entries(self, mixed_queue):
        """A thread reloaded from the checkpoint, with no captured stream,
        still names each entry."""
        result = await _drain(mixed_queue)

        items = history_events_to_sse(
            messages_to_history_events(result["messages"], agent=f"task:{_TASK}"),
            thread_id="t-1",
        )

        (item,) = items
        assert item["event"] == "steering_delivered"
        assert item["data"]["agent"] == f"task:{_TASK}"
        assert item["data"]["entries"] == ENTRIES

    @pytest.mark.asyncio
    async def test_nothing_waiting_injects_nothing(self):
        registry = _Registry(_Inbox([]))

        assert await _drain(registry) is None
        assert registry.events == []

    @pytest.mark.asyncio
    async def test_a_task_without_a_run_takes_nothing(self):
        """Steers are fenced to a run: a task with no run id has none to take,
        and must not take another run's."""
        inbox = _Inbox([{"job_id": "j-0", **ENTRIES[0]}])
        registry = _Registry(inbox, task_run_id=None)

        assert await _drain(registry) is None
        assert inbox.calls == []

    @pytest.mark.asyncio
    async def test_a_failed_take_leaves_the_step_alone(self):
        inbox = _Inbox([])
        inbox.take_task = AsyncMock(side_effect=RuntimeError("db down"))

        assert await _drain(_Registry(inbox)) is None

    @pytest.mark.asyncio
    async def test_the_take_never_reads_redis(self, mixed_queue):
        with patch(
            "src.utils.cache.redis_cache.get_cache_client"
        ) as get_cache_client:
            assert await _drain(mixed_queue) is not None
        get_cache_client.assert_not_called()


def test_the_redis_queue_and_its_sweep_are_gone():
    """The ledger settles a run's unread steers when it ends; a run-scoped
    Redis queue alongside it would hand the same input in twice."""
    assert not hasattr(run_executor, "_return_unconsumed_steering")
    assert not hasattr(redis_stream, "steering_queue_key")
    assert not hasattr(redis_stream, "parse_steering_payload")
    assert not hasattr(bg_middleware.BackgroundSubagentMiddleware, "_queue_followup_to_redis")


class TestConfirm:
    @staticmethod
    async def _call(registry, messages, handler=None):
        handler = handler or AsyncMock(return_value="response")
        result = await SubagentSteeringMiddleware(registry).awrap_model_call(
            SimpleNamespace(state={"messages": messages}), handler
        )
        return result, handler

    @staticmethod
    def _held(run: str, ids: list[str]) -> HumanMessage:
        return HumanMessage(
            content="steer",
            additional_kwargs={"held_jobs": {"task_run_id": run, "job_ids": ids}},
        )

    @pytest.mark.asyncio
    async def test_the_model_call_confirms_what_was_injected_since_its_last_reply(
        self,
    ):
        inbox = _Inbox([])
        messages = [
            self._held(_RUN, ["j-old"]),
            AIMessage(content="ok"),
            self._held(_RUN, ["j-1"]),
            ToolMessage(content="r", tool_call_id="x"),
            self._held(_RUN, ["j-2", "j-3"]),
        ]

        result, handler = await self._call(_Registry(inbox), messages)

        assert result == "response"
        handler.assert_awaited_once()
        # Rows before the last reply were confirmed by the call that read them.
        assert inbox.confirmed == [(_RUN, ["j-2", "j-3"]), (_RUN, ["j-1"])]

    @pytest.mark.asyncio
    async def test_a_failed_confirm_still_calls_the_model(self):
        """An unconfirmed row is settled when the run ends; losing the model
        call over it would stall the task."""
        inbox = _Inbox([])
        inbox.confirm_task = AsyncMock(side_effect=RuntimeError("db down"))

        result, handler = await self._call(
            _Registry(inbox), [self._held(_RUN, ["j-1"])]
        )

        assert result == "response"
        handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_nothing_held_confirms_nothing(self):
        inbox = _Inbox([])

        await self._call(_Registry(inbox), [HumanMessage(content="go")])

        assert inbox.confirmed == []


class TestResumeOpener:
    """A resume for messages the task's last run never read hands their rows
    to the run it admits. That run's opener carries them as a steer it took
    itself would, so its first model call confirms them, after the opener
    is checkpointed; confirming them any earlier lost them with a worker
    that died before that."""

    _HELD = {"task_run_id": _RUN, "job_ids": ["j-1", "j-2"]}
    _DELIVERED = {
        "content": "narrow it\nand this",
        "count": 2,
        "input_ids": ["in-1", "in-2"],
        "entries": [
            {"input_id": "in-1", "content": "narrow it"},
            {"input_id": "in-2", "content": "and this"},
        ],
    }

    def _opener(self, resume: dict | None, task_id: str = _TASK) -> HumanMessage:
        from ptc_agent.agent.middleware.background_subagent.context import (
            current_steer_resume,
        )
        from ptc_agent.agent.middleware.background_subagent.subagent import (
            _run_opener,
        )

        token = current_steer_resume.set(resume)
        try:
            return _run_opener("narrow it\nand this", task_id)
        finally:
            current_steer_resume.reset(token)

    @pytest.mark.asyncio
    async def test_the_opener_holds_the_handed_rows_for_the_first_model_call(self):
        inbox = _Inbox([])
        opener = self._opener(
            {"task_id": _TASK, "delivered": self._DELIVERED, "held_jobs": self._HELD}
        )

        assert opener.additional_kwargs == {
            "steering_delivered": self._DELIVERED,
            "held_jobs": self._HELD,
        }
        # Still a run's opener, not a steer injected mid-run.
        assert "lc_source" not in opener.additional_kwargs
        # Building the opener confirms nothing: the run has not checkpointed it.
        assert inbox.confirmed == []

        await TestConfirm._call(
            _Registry(inbox), [AIMessage(content="the last run's answer"), opener]
        )

        assert inbox.confirmed == [(_RUN, ["j-1", "j-2"])]

    @pytest.mark.asyncio
    async def test_after_its_first_reply_the_opener_is_confirmed_no_more(self):
        inbox = _Inbox([])
        opener = self._opener(
            {"task_id": _TASK, "delivered": self._DELIVERED, "held_jobs": self._HELD}
        )

        await TestConfirm._call(
            _Registry(inbox), [opener, AIMessage(content="on it")]
        )

        assert inbox.confirmed == []

    def test_a_plain_resume_or_another_tasks_opens_with_its_prompt_alone(self):
        assert self._opener(None).additional_kwargs == {}
        other = {"task_id": "other1", "delivered": self._DELIVERED, "held_jobs": self._HELD}
        assert self._opener(other).additional_kwargs == {}

    def test_the_launching_turn_no_longer_confirms_for_the_run(self):
        from ptc_agent.agent.middleware.background_subagent import context

        assert not hasattr(context, "confirm_steer_resume")


class TestServerPort:
    """``SteeredReportInbox``'s task side: the hold is owned by the task run,
    so another run of the task (or the main turn) cannot take the same rows."""

    @staticmethod
    def _job(n: int, sender: str = "user") -> dict:
        return {
            "hook_outbox_id": f"j-{n}",
            "payload": {"content": f"note {n}", "input_id": f"i-{n}", "sender": sender},
        }

    @pytest.mark.asyncio
    async def test_take_holds_the_task_chain_for_its_run(self):
        jobs = [self._job(1), self._job(2, "agent"), self._job(3)]
        listing = AsyncMock(return_value=jobs)
        # j-2 was taken by someone else between the read and the hold.
        hold = AsyncMock(return_value=["j-1", "j-3"])
        with (
            patch(f"{_OUTBOX}.list_takeable_jobs", new=listing),
            patch(f"{_OUTBOX}.hold_jobs", new=hold),
        ):
            taken = await SteeredReportInbox().take_task("t-1", _TASK, _RUN)

        listing.assert_awaited_once_with(
            "t-1", ["steer"], target="task", task_id=_TASK
        )
        hold.assert_awaited_once_with(["j-1", "j-2", "j-3"], owner=("task", _RUN))
        assert taken == [
            {"job_id": "j-1", "content": "note 1", "input_id": "i-1", "sender": "user"},
            {"job_id": "j-3", "content": "note 3", "input_id": "i-3", "sender": "user"},
        ]

    @pytest.mark.asyncio
    async def test_a_failed_take_takes_nothing(self):
        with patch(
            f"{_OUTBOX}.list_takeable_jobs",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ):
            assert await SteeredReportInbox().take_task("t-1", _TASK, _RUN) == []

    @pytest.mark.asyncio
    async def test_confirm_closes_under_the_runs_hold(self):
        confirm = AsyncMock(return_value=["j-1"])
        with patch(f"{_OUTBOX}.confirm_jobs", new=confirm):
            await SteeredReportInbox().confirm_task(_RUN, ["j-1"])

        confirm.assert_awaited_once_with(["j-1"], owner=("task", _RUN))


# ---------------------------------------------------------------------------
# The producer: Task(action="update")
# ---------------------------------------------------------------------------


def _task(task_run_id: str | None = _RUN) -> BackgroundTask:
    return BackgroundTask(
        tool_call_id="tc-9",
        task_id=_TASK,
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        task_run_id=task_run_id,
    )


def _producer(ledger_status: str | None = None, *, ledger: bool = True):
    registry = BackgroundTaskRegistry(thread_id="t-1")
    registry.report_inbox = SteeredReportInbox()
    if ledger:
        registry.run_ledger = SimpleNamespace(
            get_latest_run=AsyncMock(
                return_value={"status": ledger_status} if ledger_status else None
            )
        )
    task = _task()
    registry._tasks[task.tool_call_id] = task
    registry._task_id_to_tool_call_id[task.task_id] = task.tool_call_id
    return BackgroundSubagentMiddleware(registry=registry, enabled=True, subagent_types={"general-purpose"}), task


async def _update(
    mw, prompt: str = "Also cover 2024 guidance", messages: list | None = None
) -> ToolMessage:
    request = SimpleNamespace(
        state={"messages": messages or []},
        tool_call={
            "name": "Task",
            "id": "tc-update",
            "args": {
                "action": "update",
                "task_id": _TASK,
                "description": "more",
                "prompt": prompt,
            },
        },
        runtime=SimpleNamespace(
            config={"configurable": {"thread_id": "t-1"}, "metadata": {"run_id": "r"}}
        ),
    )
    return await handle_task_action(mw, request, AsyncMock())


def _admit(result: dict) -> AsyncMock:
    return AsyncMock(return_value=result)


class TestUpdateProducer:
    @pytest.mark.asyncio
    async def test_an_update_admits_an_agent_steer_into_the_live_run(self):
        mw, task = _producer()
        admit = _admit({"state": "accepted", "job_id": "j-1", "position": 1})
        with (
            patch(f"{_OUTBOX}.admit_task_steer", new=admit),
            patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock()) as lookup,
        ):
            msg = await _update(mw)

        kwargs = admit.await_args.kwargs
        input_id = kwargs["payload"]["input_id"]
        assert kwargs["thread_id"] == "t-1"
        assert kwargs["task_id"] == _TASK
        assert kwargs["task_run_id"] == _RUN
        assert kwargs["idempotency_key"] == f"steer:t-1:{_TASK}:{input_id}"
        assert kwargs["payload"]["sender"] == "agent"
        assert kwargs["payload"]["content"] == "Also cover 2024 guidance"
        assert kwargs["payload"]["task_run_id"] == _RUN
        # A fresh id has no earlier acceptance to look up.
        lookup.assert_not_awaited()

        assert msg.status != "error"
        artifact = msg.additional_kwargs["task_artifact"]
        assert artifact["input_id"] == input_id
        assert artifact["task_run_id"] == _RUN
        assert artifact["action"] == "update"

    @pytest.mark.asyncio
    async def test_a_saved_call_run_again_keeps_its_first_key(self):
        """A Retry resumes from the turn's last checkpoint, so a Task update
        whose admission committed before its tool step was saved runs again:
        it must reuse the key, and a reused call id in a later turn must not."""
        call = {"name": "Task", "id": "tc-update", "args": {}}
        first = AIMessage(content="", id="ai-1", tool_calls=[call])
        later = AIMessage(content="", id="ai-2", tool_calls=[call])
        keys = []
        for messages in ([first], [first], [first, later]):
            mw, _ = _producer()
            admit = _admit({"state": "accepted", "job_id": "j-1", "position": 1})
            with (
                patch(f"{_OUTBOX}.admit_task_steer", new=admit),
                patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=None)),
            ):
                await _update(mw, messages=messages)
            keys.append(admit.await_args.kwargs["idempotency_key"])
        assert keys[0] == keys[1]
        assert keys[2] != keys[0]

    @pytest.mark.asyncio
    async def test_a_duplicate_reads_as_the_original_acceptance(self):
        mw, task = _producer()
        job = {"id": "j-0", "payload": {"input_id": "orig-1"}}
        with (
            patch(
                f"{_OUTBOX}.admit_task_steer",
                new=_admit({"state": "duplicate", "job": job}),
            ),
            patch(f"{_OUTBOX}.task_steer_position", new=AsyncMock(return_value=1)),
        ):
            assert await mw._admit_followup(task, "again") == ("accepted", "orig-1")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ledger_status", "reported"),
        [
            ("completed", "completed"),
            ("error", "error"),
            # Still in progress yet refused: its stop is stamped.
            ("in_progress", "cancelled"),
        ],
    )
    async def test_a_run_that_ended_first_names_its_status(
        self, ledger_status, reported
    ):
        mw, task = _producer(ledger_status)
        with patch(
            f"{_OUTBOX}.admit_task_steer", new=_admit({"state": "not_running"})
        ):
            assert await mw._admit_followup(task, "late") == ("not_running", reported)
            msg = await _update(mw)

        assert msg.status == "error"
        assert f"finished ({reported})" in msg.content
        assert "action='resume'" in msg.content

    @pytest.mark.asyncio
    async def test_a_refusal_with_no_ledger_status_reads_as_undelivered(self):
        mw, task = _producer(ledger=False)
        with patch(
            f"{_OUTBOX}.admit_task_steer", new=_admit({"state": "not_running"})
        ):
            assert await mw._admit_followup(task, "late") == ("not_running", None)
            msg = await _update(mw)

        assert msg.status == "error"
        assert "Could not deliver follow-up" in msg.content

    @pytest.mark.asyncio
    async def test_an_outbox_failure_is_unavailable(self):
        mw, task = _producer()
        with patch(
            f"{_OUTBOX}.admit_task_steer",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ):
            assert await mw._admit_followup(task, "x") == ("unavailable", None)
            msg = await _update(mw)

        assert msg.status == "error"
        assert "Could not deliver follow-up" in msg.content

    @pytest.mark.asyncio
    async def test_a_task_with_no_run_id_is_never_admitted(self):
        mw, _ = _producer()
        admit = AsyncMock()
        with patch(f"{_OUTBOX}.admit_task_steer", new=admit):
            assert await mw._admit_followup(_task(None), "x") == ("unavailable", None)

        admit.assert_not_awaited()
