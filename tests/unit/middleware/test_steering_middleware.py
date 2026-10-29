"""SteeringMiddleware delivery from the outbox inbox.

Everything a live turn takes in (steers from any sender, owed reports) comes
through ``report_inbox.take``, held for the run until the model call that
reads it confirms it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.steering import (
    SteeredReport,
    SteeringMiddleware,
    steers_ahead_message,
)


def _config(run_id: str | None = "run-A", input_id: str | None = None) -> dict:
    cfg: dict = {"configurable": {"thread_id": "t-1"}}
    if input_id:
        cfg["configurable"]["steer_input_message_id"] = input_id
    if run_id is not None:
        cfg["metadata"] = {"run_id": run_id}
    return cfg


async def _run(config: dict, inbox=None, state=None, **kwargs):
    return await SteeringMiddleware(inbox, **kwargs).abefore_model(
        state or {}, MagicMock(), config=config
    )


def _inbox(*taken: SteeredReport) -> MagicMock:
    inbox = MagicMock()
    inbox.take = AsyncMock(return_value=list(taken))
    inbox.confirm = AsyncMock()
    return inbox


def _steer(job_id: str, content: str, sender: str = "user", **kw) -> SteeredReport:
    return SteeredReport(
        job_id=job_id, content=content, kind="steer", sender=sender, **kw
    )


_R1 = SteeredReport(
    job_id="job-1",
    content="REPORT ONE",
    shown={"report_source": {"kind": "task", "task_id": "a"}},
)
_R2 = SteeredReport(
    job_id="job-2",
    content="REPORT TWO",
    shown={"report_source": {"kind": "task", "task_id": "b"}},
)


@pytest.mark.asyncio
async def test_delivered_entry_keeps_its_chat_app():
    """The label reads the platform off the delivered entry; the model reads
    only the content."""
    inbox = _inbox(
        _steer("s1", "from the phone", platform="telegram"),
        _steer("s2", "from the web", request_key="rk-2"),
    )

    (msg,) = (await _run(_config(), inbox))["messages"]

    assert "telegram" not in msg.content
    phone, web = msg.additional_kwargs["steering_delivered"]["messages"]
    assert phone["platform"] == "telegram"
    assert "platform" not in web
    assert web["request_key"] == "rk-2"
    assert msg.content == (
        "The user sent new messages while you were working:"
        "\n1. from the phone\n2. from the web"
        "\n\nAddress the messages above as you continue this turn."
    )


@pytest.mark.asyncio
async def test_senders_are_labelled_in_the_order_sent_and_only_the_users_are_counted():
    """A block per run of one sender's messages, never grouped across the
    take: a later message may take back an earlier one from another sender."""
    inbox = _inbox(
        _steer("s1", "do x", "chief_of_staff", origin={
            "type": "agent", "agent": "chief_of_staff",
            "origin_home_thread_id": "home-1", "origin_dispatch_gen": "g1",
        }),
        _steer("s2", "stop that"),
        _steer("s3", "then z", "chief_of_staff"),
    )

    (msg,) = (await _run(_config(), inbox))["messages"]

    assert msg.content == (
        "The Chief of Staff sent a new message on the user's behalf"
        " while you were working:\ndo x"
        "\n\nThe user sent a new message while you were working:\nstop that"
        "\n\nThe Chief of Staff sent a new message on the user's behalf"
        " while you were working:\nthen z"
        "\n\nAddress the messages above as you continue this turn."
    )
    delivered = msg.additional_kwargs["steering_delivered"]
    # A chat gateway retires this many of the messages it queued, and it
    # never queued an agent's.
    assert delivered["count"] == 1
    assert [m["content"] for m in delivered["messages"]] == [
        "do x", "stop that", "then z",
    ]
    assert [m.get("sender") for m in delivered["messages"]] == [
        "chief_of_staff", None, "chief_of_staff",
    ]
    # The reader sees who sent it, never the Home thread it came from.
    assert delivered["messages"][0]["origin"] == {
        "type": "agent",
        "agent": "chief_of_staff",
    }


@pytest.mark.asyncio
async def test_each_injection_is_a_message_of_its_own():
    """The reducer replaces a message whose id it holds, so a shared id would
    overwrite an earlier steer where it stands."""
    from ptc_agent.agent.state import messages_delta_reducer

    injected = []
    for _ in range(2):
        (msg,) = (await _run(_config(), _inbox(_steer("s1", "again"))))["messages"]
        injected.append(msg)

    state = messages_delta_reducer([HumanMessage("question", id="q")], [[injected[0]]])
    state = messages_delta_reducer(state, [[AIMessage("answer", id="a")]])
    state = messages_delta_reducer(state, [[injected[1]]])

    assert [m.id for m in state] == ["q", injected[0].id, "a", injected[1].id]


@pytest.mark.asyncio
async def test_reports_alone_inject_one_message_without_user_header():
    inbox = _inbox(_R1, _R2)

    (msg,) = (await _run(_config(), inbox))["messages"]

    assert msg.content == "REPORT ONE\n\nREPORT TWO"
    assert "sent a new message" not in msg.content
    assert msg.additional_kwargs["steers_at"] == len(msg.content)
    assert msg.additional_kwargs["lc_source"] == "steering"
    delivered = msg.additional_kwargs["steering_delivered"]
    # count is the user's messages, which a chat gateway retires from its queue.
    assert delivered["count"] == 0
    assert [(m["content"], m["report_source"]) for m in delivered["messages"]] == [
        ("", _R1.shown["report_source"]),
        ("", _R2.shown["report_source"]),
    ]
    inbox.take.assert_awaited_once_with("t-1", "run-A", kinds=("report", "steer"))
    # The run holds them until the model call that reads them confirms them.
    assert msg.additional_kwargs["held_jobs"] == {
        "run_id": "run-A",
        "job_ids": ["job-1", "job-2"],
    }


@pytest.mark.asyncio
async def test_reports_come_before_steering():
    inbox = _inbox(_R1, _steer("s1", "do it differently"))

    (msg,) = (await _run(_config(), inbox))["messages"]

    assert msg.content == (
        "REPORT ONE\n\n"
        "The user sent a new message while you were working:\ndo it differently"
        "\n\nAddress the message above as you continue this turn."
    )
    assert msg.content[msg.additional_kwargs["steers_at"] :].startswith(
        "The user sent a new message"
    )
    delivered = msg.additional_kwargs["steering_delivered"]
    assert delivered["count"] == 1
    report, user = delivered["messages"]
    assert report["report_source"] == _R1.shown["report_source"]
    assert user["content"] == "do it differently"
    assert msg.additional_kwargs["held_jobs"]["job_ids"] == ["job-1", "s1"]


@pytest.mark.asyncio
async def test_kinds_reach_the_inbox():
    inbox = _inbox()

    assert await _run(_config(), inbox, kinds=("steer",)) is None
    inbox.take.assert_awaited_once_with("t-1", "run-A", kinds=("steer",))


@pytest.mark.asyncio
async def test_a_run_without_an_id_takes_nothing():
    """A taken row is held for the run that took it, so a run with no id of
    its own leaves them in the outbox."""
    inbox = _inbox(_R1)

    assert await _run(_config(run_id=None), inbox) is None
    inbox.take.assert_not_awaited()


@pytest.mark.asyncio
async def test_failing_inbox_injects_nothing():
    inbox = _inbox()
    inbox.take.side_effect = RuntimeError("db down")

    assert await _run(_config(), inbox) is None


# ---------------------------------------------------------------------------
# A steer sent before the turn started opens the turn, ahead of its input
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_turn_opens_with_the_steers_sent_before_it_in_send_order():
    """The server puts them in the turn's input, so its input checkpoint,
    where a regenerate starts over, holds them."""
    ahead = steers_ahead_message(
        [
            _steer("s-1", "first"),
            _steer("s-2", "second", "chief_of_staff"),
            _steer("s-3", "third"),
        ],
        "run-A",
    )

    assert ahead.content == (
        "The user sent a new message while you were working:\nfirst"
        "\n\nThe Chief of Staff sent a new message on the user's behalf"
        " while you were working:\nsecond"
        "\n\nThe user sent a new message while you were working:\nthird"
        "\n\nAddress the messages above as you continue this turn."
    )
    assert ahead.additional_kwargs["held_jobs"] == {
        "run_id": "run-A",
        "job_ids": ["s-1", "s-2", "s-3"],
    }
    assert ahead.additional_kwargs["steering_delivered"]["count"] == 2


@pytest.mark.asyncio
async def test_the_first_call_shows_what_the_turn_opened_with_then_what_it_took():
    """Each message streams its own steering_delivered, the opening one
    first, so the live view places it ahead of the input as replay does."""
    ahead = steers_ahead_message([_steer("s-early", "sent before")], "run-A")
    question = HumanMessage("question", id="in-1")
    state = {"messages": [HumanMessage("earlier", id="h0"), AIMessage("a", id="a0"), ahead, question]}
    runtime = MagicMock()
    inbox = _inbox(_R1, _steer("s-late", "sent just now"))

    result = await SteeringMiddleware(inbox).abefore_model(
        state, runtime, config=_config(input_id="in-1")
    )

    (late,) = result["messages"]
    assert "REPORT ONE" in late.content and "sent just now" in late.content
    assert late.additional_kwargs["held_jobs"]["job_ids"] == ["job-1", "s-late"]
    streamed = [c.args[0] for c in runtime.stream_writer.call_args_list]
    assert [e["type"] for e in streamed] == ["steering_delivered"] * 2
    assert [m["content"] for m in streamed[0]["messages"]] == ["sent before"]
    assert all(e["thread_id"] == "t-1" for e in streamed)


@pytest.mark.asyncio
async def test_what_the_turn_opened_with_is_shown_once():
    ahead = steers_ahead_message([_steer("s-early", "sent before")], "run-A")
    state = {"messages": [ahead, HumanMessage("question", id="in-1"), AIMessage("a", id="a1")]}
    runtime = MagicMock()

    await SteeringMiddleware(_inbox()).abefore_model(
        state, runtime, config=_config(input_id="in-1")
    )

    runtime.stream_writer.assert_not_called()


@pytest.mark.asyncio
async def test_a_retry_does_not_show_what_another_run_opened_with():
    """A retry starts over from the failed run's input, which already showed
    the steers it opened with."""
    ahead = steers_ahead_message([_steer("s-early", "sent before")], "run-A")
    state = {"messages": [ahead, HumanMessage("question", id="in-1")]}
    runtime = MagicMock()

    await SteeringMiddleware(_inbox()).abefore_model(
        state, runtime, config=_config(run_id="run-B")
    )

    runtime.stream_writer.assert_not_called()


@pytest.mark.asyncio
async def test_every_steer_the_middleware_takes_is_appended():
    """One the turn did not open with (its take at the start failed) reads
    where the call takes it: after what the thread holds."""
    state = {"messages": [HumanMessage("question", id="in-1")]}
    inbox = _inbox(_steer("s-early", "old"), _steer("s-late", "new"))

    (msg,) = (await _run(_config(input_id="in-1"), inbox, state=state))["messages"]

    assert msg.content == (
        "The user sent new messages while you were working:\n1. old\n2. new"
        "\n\nAddress the messages above as you continue this turn."
    )
    assert msg.additional_kwargs["held_jobs"]["job_ids"] == ["s-early", "s-late"]


# ---------------------------------------------------------------------------
# The model call confirms the rows injected since the last model response
# ---------------------------------------------------------------------------


async def _model_call(messages, inbox):
    request = MagicMock()
    request.state = {"messages": messages}
    handler = AsyncMock(return_value=AIMessage(content="ok"))
    response = await SteeringMiddleware(inbox).awrap_model_call(request, handler)
    handler.assert_awaited_once_with(request)
    return response


def _held(*job_ids: str, run_id: str = "run-A", key: str = "held_jobs"):
    return HumanMessage(
        content="REPORT",
        additional_kwargs={key: {"run_id": run_id, "job_ids": list(job_ids)}},
    )


@pytest.mark.asyncio
async def test_model_call_confirms_rows_injected_since_the_last_response():
    from langchain_core.messages import ToolMessage

    inbox = _inbox()
    messages = [
        HumanMessage(content="go"),
        _held("job-old"),
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "t", "args": {}}]),
        ToolMessage(content="r", tool_call_id="c1"),
        _held("job-1", "job-2"),
    ]

    await _model_call(messages, inbox)

    inbox.confirm.assert_awaited_once_with("run-A", ["job-1", "job-2"])


@pytest.mark.asyncio
async def test_a_message_checkpointed_under_the_old_name_still_confirms():
    inbox = _inbox()

    await _model_call([_held("job-1", key="held_reports")], inbox)

    inbox.confirm.assert_awaited_once_with("run-A", ["job-1"])


@pytest.mark.asyncio
async def test_model_call_without_held_rows_confirms_nothing():
    inbox = _inbox()

    await _model_call([_held("job-old"), AIMessage(content="x"), HumanMessage("hi")], inbox)

    inbox.confirm.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_confirm_still_makes_the_model_call():
    inbox = _inbox()
    inbox.confirm.side_effect = RuntimeError("db down")

    response = await _model_call([_held("job-1")], inbox)

    assert response.content == "ok"


@pytest.mark.asyncio
async def test_taking_holds_and_only_the_model_call_confirms():
    """The injecting step is checkpointed before the model call starts, so a
    row confirmed there can no longer be lost with the turn; confirming at
    take time could."""
    from ptc_agent.agent.state import messages_delta_reducer

    ahead = steers_ahead_message([_steer("s-early", "sent before")], "run-A")
    state = {"messages": [ahead, HumanMessage("question", id="in-1")]}
    inbox = _inbox(_R1, _steer("s-late", "sent during"))

    update = await _run(_config(input_id="in-1"), inbox, state=state)

    inbox.take.assert_awaited_once()
    inbox.confirm.assert_not_awaited()

    merged = messages_delta_reducer(state["messages"], [update["messages"]])
    await _model_call(merged, inbox)

    confirmed = [c.args for c in inbox.confirm.await_args_list]
    assert sorted(confirmed) == [
        ("run-A", ["job-1", "s-late"]),
        ("run-A", ["s-early"]),
    ]


@pytest.mark.asyncio
async def test_a_resume_the_middleware_called_does_not_end_the_confirm_walk():
    """Its AIMessage is no model response, so what was injected before it is
    still unconfirmed when the next model call wraps."""
    from langchain_core.messages import ToolMessage

    inbox = _inbox()
    messages = [
        _held("job-1"),
        AIMessage(
            content="",
            tool_calls=[{"id": "c1", "name": "Task", "args": {}}],
            additional_kwargs={"lc_source": "steering"},
        ),
        ToolMessage(content="resumed", tool_call_id="c1"),
    ]

    await _model_call(messages, inbox)

    inbox.confirm.assert_awaited_once_with("run-A", ["job-1"])


def test_checkpointed_holds_reads_every_steering_message_of_its_owner():
    """At a run's ending: every stamp the run's checkpoint carries, a run's or
    a task run's, not only those since its last model response."""
    from ptc_agent.agent.middleware.steering import checkpointed_holds

    messages = [
        HumanMessage(
            content="a", additional_kwargs={"held_jobs": {"run_id": "run-A", "job_ids": ["j-1"]}}
        ),
        AIMessage(content="answer"),
        HumanMessage(
            content="b", additional_kwargs={"held_reports": {"run_id": "run-A", "job_ids": ["j-2"]}}
        ),
        HumanMessage(
            content="c", additional_kwargs={"held_jobs": {"run_id": "run-B", "job_ids": ["j-3"]}}
        ),
        HumanMessage(
            content="d",
            additional_kwargs={"held_jobs": {"task_run_id": "tr-1", "job_ids": ["j-4"]}},
        ),
        HumanMessage(content="plain"),
    ]

    assert checkpointed_holds(messages, "run-A") == {"j-1", "j-2"}
    assert checkpointed_holds(messages, "tr-1") == {"j-4"}
    assert checkpointed_holds([], "run-A") == set()


# ---------------------------------------------------------------------------
# A resume this middleware called hands its rows to the run it resumes
#
# Confirming them in the launching turn closed them before the resumed run
# had checkpointed the message carrying them, so a worker lost in between
# lost them. The resume hands them to the task run it admits instead, whose
# first model call confirms them, and whose ending settles them if it never
# gets there.
# ---------------------------------------------------------------------------


def _resume_request():
    from types import SimpleNamespace

    from ptc_agent.agent.middleware.steering import TaskResume, _resume_call

    call = _resume_call(
        [
            TaskResume(
                task_id="k7Xm2p",
                job_ids=["job-1", "job-2"],
                entries=[
                    {"input_id": "in-1", "content": "narrow it"},
                    {"input_id": "in-2", "content": "and this"},
                ],
            )
        ],
        "run-A",
    )
    return SimpleNamespace(
        tool_call=call.tool_calls[0],
        state={"messages": [call]},
        runtime=SimpleNamespace(config={"configurable": {"thread_id": "t-1"}}),
    )


def _resume_inbox(moved: list[str]) -> MagicMock:
    inbox = MagicMock()
    inbox.hand_off_resumes = AsyncMock(return_value=moved)
    inbox.confirm_resumes = AsyncMock()
    inbox.release_resumes = AsyncMock()
    return inbox


def _resumed(task_run_id: str = "tr-2"):
    from langchain_core.messages import ToolMessage

    return ToolMessage(
        content="Resumed",
        tool_call_id="c",
        name="Task",
        additional_kwargs={
            "task_artifact": {"action": "resume", "task_run_id": task_run_id}
        },
    )


@pytest.mark.asyncio
async def test_a_resume_hands_its_rows_to_the_resumed_run_and_confirms_nothing():
    from ptc_agent.agent.middleware.background_subagent.context import (
        current_steer_resume,
        hand_off_steer_resume,
        steer_resume_held,
    )

    inbox = _resume_inbox(["job-1", "job-2"])
    seen: dict = {}

    async def handler(_request):
        seen["resume"] = dict(current_steer_resume.get() or {})
        seen["handed"] = await hand_off_steer_resume("k7Xm2p", "tr-2")
        seen["held"] = steer_resume_held("k7Xm2p")
        return _resumed()

    await SteeringMiddleware(inbox).awrap_tool_call(_resume_request(), handler)

    assert callable(seen["resume"]["hand_off"])
    assert "confirm" not in seen["resume"]
    assert seen["handed"] is True
    inbox.hand_off_resumes.assert_awaited_once_with(
        "t-1", "run-A", "k7Xm2p", "tr-2", ["job-1", "job-2"]
    )
    # What the resumed run's opener carries for its first model call.
    assert seen["held"] == {"task_run_id": "tr-2", "job_ids": ["job-1", "job-2"]}
    # The rows are the task run's now: the launching turn settles nothing.
    inbox.confirm_resumes.assert_not_awaited()
    inbox.release_resumes.assert_not_awaited()
    assert current_steer_resume.get() is None


@pytest.mark.asyncio
async def test_a_hand_off_that_moved_nothing_refuses_and_gives_the_rows_back():
    from langchain_core.messages import ToolMessage

    from ptc_agent.agent.middleware.background_subagent.context import (
        hand_off_steer_resume,
        steer_resume_held,
    )

    inbox = _resume_inbox([])
    seen: dict = {}

    async def handler(_request):
        seen["handed"] = await hand_off_steer_resume("k7Xm2p", "tr-2")
        seen["held"] = steer_resume_held("k7Xm2p")
        return ToolMessage(
            content="Error: could not resume", tool_call_id="c", status="error"
        )

    await SteeringMiddleware(inbox).awrap_tool_call(_resume_request(), handler)

    assert seen == {"handed": False, "held": None}
    inbox.confirm_resumes.assert_not_awaited()
    inbox.release_resumes.assert_awaited_once_with(
        "t-1", "run-A", "k7Xm2p", ["job-1", "job-2"]
    )


@pytest.mark.asyncio
async def test_a_resume_with_no_run_to_hand_to_confirms_as_before():
    """No run ledger, so no run of its own to hold them: the launching turn
    still confirms them once the resume started."""
    inbox = _resume_inbox([])

    await SteeringMiddleware(inbox).awrap_tool_call(
        _resume_request(), AsyncMock(return_value=_resumed())
    )

    inbox.hand_off_resumes.assert_not_awaited()
    inbox.confirm_resumes.assert_awaited_once_with("run-A", ["job-1", "job-2"])
    inbox.release_resumes.assert_not_awaited()


def test_sender_runs_keep_the_order_sent():
    """The task's steers render through the same runs: a user's correction
    after the main agent's follow-up stays after it."""
    from ptc_agent.agent.transcript.steer_text import sender_runs

    sent = [("user", "stop"), ("user", "wait"), ("agent", "continue"), ("user", "no")]
    assert sender_runs(sent) == [
        ("user", ["stop", "wait"]),
        ("agent", ["continue"]),
        ("user", ["no"]),
    ]


def test_a_resumed_task_reads_who_sent_each_message():
    """A task resumed for messages its last run never read takes them in the
    blocks a running task reads them in; replay keeps the words as sent."""
    from ptc_agent.agent.middleware.steering import TaskResume, _resume_call

    call = _resume_call(
        [
            TaskResume(
                task_id="k7Xm2p",
                job_ids=["job-1", "job-2"],
                entries=[
                    {"input_id": "in-1", "content": "narrow it", "sender": "user"},
                    {"input_id": "in-2", "content": "skip Q3", "sender": "agent"},
                ],
            )
        ],
        "run-A",
    )

    prompt = call.tool_calls[0]["args"]["prompt"]
    assert prompt.startswith("The user sent a new message while you were working:\nnarrow it")
    assert "The parent agent sent a new message while you were working:\nskip Q3" in prompt
    held = call.additional_kwargs["steer_resumes"][call.tool_calls[0]["id"]]
    assert held["delivered"]["content"] == "narrow it\nskip Q3"
