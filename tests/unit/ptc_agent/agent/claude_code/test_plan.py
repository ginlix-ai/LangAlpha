"""The Claude plan as a run's CLI reports it, and the stop at its limit (``plan.PlanWatch``)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from claude_agent_sdk import ResultMessage
from langchain_core.messages import AIMessage

from ptc_agent.agent.claude_code.cli import RuntimeSpec, _Gated, turn_failure
from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.mid_turn import AfterTool
from ptc_agent.agent.claude_code.plan import PlanWatch
from ptc_agent.agent.claude_code.runtime import ClaudeTurn
from ptc_agent.agent.claude_code.tools_bridge import ToolResults
from src.llms.claude_code import PLAN_NOTICE_KEY, ClaudeTurnFailed, PlanLimit, PlanReading, PlanStopped, binding_limit

NOW = datetime(2026, 11, 6, 19, 5, tzinfo=UTC)
LATER = datetime.fromtimestamp(int((datetime.now(UTC) + timedelta(hours=1)).timestamp()), UTC)
BILLED = {"status": "allowed_warning", "isUsingOverage": True, "overageStatus": "allowed"}
REJECTED = {"status": "rejected", "rateLimitType": "five_hour", "resetsAt": LATER.timestamp()}
HAIKU = "claude-haiku-5-5"


class _Log:
    """The user's plan state: one row's limit, and their choice."""

    def __init__(self, limit: PlanLimit | None = None, *, out: str = "stop", fails: bool = False) -> None:
        self.row, self.out_, self.fails = limit, out, fails
        self.records: list[tuple[str, PlanReading]] = []
        self.reads: list[tuple[str, str]] = []
        self.order: list[str] = []

    async def open(self):
        if self.fails:
            raise RuntimeError("no pool")

    async def out(self):
        return self.out_

    def record(self, signed_in, reading):
        self.records.append((signed_in, reading))

    async def limit(self, signed_in, model):
        self.reads.append((signed_in, model))
        if self.fails:
            raise RuntimeError("no pool")
        return binding_limit([self.row] if self.row else [], model)

    async def written(self):
        self.order.append("written")


def _source(log: _Log, signed_in: str | None = "saved"):
    return SimpleNamespace(plan=log, signed_in=signed_in)


def _event(**raw):
    return SimpleNamespace(rate_limit_info=SimpleNamespace(raw=raw))


def _watch(*events, stop=True) -> PlanWatch:
    watch = PlanWatch(_source(_Log()), HAIKU, stop_at_limit=stop)
    for raw in events:
        watch.seen(_event(**raw))
    return watch


def test_a_report_keeps_only_the_windows_it_carried_and_credits_only_where_it_knew_them():
    reading = PlanReading.of({
        "status": "allowed",
        "unifiedWindows": {"five_hour": {"utilization": 0.4, "resetsAt": 100}, "seven_day": {"utilization": None, "resetsAt": 200}},
        "overageStatus": "rejected", "overageDisabledReason": "fetch_error",
    }, NOW, HAIKU)
    assert reading.windows == {"five_hour": (0.4, datetime.fromtimestamp(100, UTC))}
    assert (reading.limit, reading.allowed, reading.credits, reading.billed) == (None, True, None, False)
    off = PlanReading.of({**REJECTED, "overageStatus": "rejected", "overageDisabledReason": "out_of_credits"}, NOW, HAIKU)
    assert (off.limit, off.allowed, off.credits) == (PlanLimit("five_hour", LATER), False, "off")
    assert (PlanReading.of(BILLED, NOW, HAIKU).credits, PlanReading.of(BILLED, NOW, HAIKU).billed) == ("on", True)
    billed = PlanReading.of({"status": "allowed", "overageInUse": True}, NOW, HAIKU)
    assert billed.billed and not billed.allowed, "a billed response says the plan is used up, whatever its status"


def test_the_weekly_fable_limit_is_read_and_holds_only_fable():
    reading = PlanReading.of({"status": "rejected", "rateLimitType": "seven_day_overage_included", "resetsAt": 100}, NOW, "x")
    assert reading.limit == PlanLimit("seven_day_overage_included", datetime.fromtimestamp(100, UTC))
    assert reading.limit.binds("claude-fable-5-1") and not reading.limit.binds("claude-sonnet-5-5")


def test_a_model_is_held_by_the_binding_limit_that_lasts_longest():
    week = PlanLimit("seven_day_opus", NOW + timedelta(days=3))
    hour = PlanLimit("five_hour", NOW + timedelta(hours=2))
    assert binding_limit([week, hour], "claude-opus-5-5", NOW) == week
    assert binding_limit([week, hour], "claude-sonnet-5-5", NOW) == hour
    assert binding_limit([week, hour], "claude-sonnet-5-5", NOW + timedelta(hours=3)) is None


@pytest.mark.asyncio
async def test_under_stop_a_response_billed_to_usage_credits_stops_the_run_at_its_batch_end():
    log = _Log()
    watch = await PlanWatch.open(_source(log), HAIKU, enforce=True)
    assert not await watch.gate()
    watch.seen(_event(**BILLED))
    assert watch.over_plan and await watch.gate()
    assert [signed_in for signed_in, _ in log.records] == ["saved"]
    assert log.records[0][1].model == HAIKU


@pytest.mark.asyncio
async def test_under_usage_credits_or_on_a_tokens_check_nothing_stops_the_run():
    watch = await PlanWatch.open(_source(_Log(PlanLimit("five_hour", LATER), out="credits")), HAIKU, enforce=True)
    watch.seen(_event(**BILLED))
    assert not watch.over_plan and not await watch.gate()
    check = await PlanWatch.open(_source(_Log(PlanLimit("five_hour", LATER))), HAIKU, enforce=False)
    check.seen(_event(**REJECTED))
    assert not await check.gate()


@pytest.mark.asyncio
async def test_a_switch_to_stop_mid_turn_stops_the_turn_at_its_next_gate():
    log = _Log(out="credits")
    watch = await PlanWatch.open(_source(log), HAIKU, enforce=True)
    watch.seen(_event(**BILLED, rateLimitType="five_hour", resetsAt=LATER.timestamp()))
    assert not await watch.gate()
    log.out_ = "stop"
    assert await watch.gate(), "the switch holds from the turn's next request"
    assert watch.failure(None).window == "five_hour"


@pytest.mark.asyncio
async def test_a_limit_another_run_kept_on_the_row_stops_this_one_and_names_its_window():
    log = _Log(PlanLimit("five_hour", LATER))
    watch = await PlanWatch.open(_source(log, "own"), HAIKU, enforce=True)
    assert await watch.gate()
    assert log.reads == [("own", HAIKU)]
    stopped = watch.failure(None)
    assert isinstance(stopped, PlanStopped)
    assert (stopped.window, stopped.until, stopped.credits_billed) == ("five_hour", LATER, False)


@pytest.mark.asyncio
@pytest.mark.parametrize(("limit", "model", "stops"), [
    (PlanLimit("seven_day_opus", LATER), "claude-sonnet-5-5", False),
    (PlanLimit("seven_day_opus", LATER), "claude-opus-5-5", True),
    (PlanLimit("seven_day", LATER), "claude-sonnet-5-5", True),
    (PlanLimit("five_hour", NOW - timedelta(days=1)), "claude-sonnet-5-5", False),
])
async def test_the_row_stops_a_run_only_while_its_limit_lasts_and_on_the_family_it_binds(limit, model, stops):
    assert await (await PlanWatch.open(_source(_Log(limit)), model, enforce=True)).gate() is stops


@pytest.mark.asyncio
async def test_a_run_whose_credential_is_not_known_reads_no_row():
    log = _Log(PlanLimit("five_hour", LATER))
    assert not await (await PlanWatch.open(_source(log, None), HAIKU, enforce=True)).gate()
    assert log.reads == []


@pytest.mark.asyncio
async def test_an_unreadable_row_stops_nothing_and_an_unreadable_setting_reads_as_stop():
    watch = await PlanWatch.open(_source(_Log(PlanLimit("five_hour", LATER), fails=True)), HAIKU, enforce=True)
    assert watch.stop_at_limit and not await watch.gate()
    watch.seen(_event(**REJECTED))
    assert await watch.gate()


def test_a_limit_whose_reset_passed_no_longer_holds_the_turn():
    passed = {**REJECTED, "resetsAt": (NOW - timedelta(minutes=1)).timestamp()}
    assert not _watch(passed).over_plan
    assert _watch(BILLED).over_plan, "a billed response with no reset known holds until the turn ends"


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [BILLED, {"status": "rejected", "rateLimitType": "five_hour"}])
async def test_a_bill_or_refusal_with_no_reset_after_a_limit_passed_holds_the_turn(unknown):
    watch = _watch({**REJECTED, "resetsAt": (NOW - timedelta(minutes=1)).timestamp()})
    assert not await watch.gate()
    watch.seen(_event(**unknown))
    assert watch.limit.known is False, "a reset that passed never stands for a new one"
    assert await watch.gate()


def test_a_bill_claude_said_no_reset_for_keeps_a_reset_still_ahead():
    assert _watch(REJECTED, BILLED).limit == PlanLimit("five_hour", LATER)
    assert _watch(BILLED).limit.known is False


@pytest.mark.asyncio
async def test_a_reply_billed_where_claude_said_no_reset_is_stamped_with_a_notice_naming_no_time():
    kept = await _watch(BILLED).stamp(AIMessage("x"))
    assert kept.response_metadata == {PLAN_NOTICE_KEY: {"reason": "billed_credits"}}


def test_a_refusal_is_the_querys_own_and_a_bill_the_turns():
    watch = _watch(BILLED, REJECTED)
    watch.reopen()
    assert (watch.rejected, watch.billed, watch.limit) == (False, True, PlanLimit("five_hour", LATER))


def test_a_used_up_plans_refusal_names_its_window_and_what_was_billed_first():
    watch = _watch(BILLED, REJECTED)
    assert watch.failure(None) is None, "a query that did not fail did its turn"
    failed = ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1, is_error=True, num_turns=1,
                           session_id="s", result="limit", terminal_reason="api_error")
    stopped = turn_failure(failed, [], watch)
    assert isinstance(stopped, PlanStopped)
    assert (stopped.window, stopped.until, stopped.credits_billed) == ("five_hour", LATER, True)


@pytest.mark.asyncio
async def test_the_cli_stops_at_the_batch_end_the_plan_stops_at():
    async def take():
        return None

    watch = _watch()
    hook = AfterTool(ToolResults(), send=AsyncMock(), plan=watch)
    hook.open(["t1", "t2"], take)
    assert "continue" not in await hook({"hook_event_name": "PostToolUse"}, "t1", None)
    watch.seen(_event(**BILLED))
    assert (await hook({"hook_event_name": "PostToolUse"}, "t2", None))["continue"] is False


def _turn() -> ClaudeTurn:
    return ClaudeTurn(RuntimeSpec(system_prompt="s", model=HAIKU, tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t")


@pytest.mark.asyncio
@pytest.mark.parametrize("plan", [
    pytest.param(lambda: _watch(BILLED), id="its-own-response-billed"),
    pytest.param(lambda: PlanWatch(_source(_Log(PlanLimit("five_hour", LATER))), HAIKU, stop_at_limit=True), id="a-siblings-row"),
])
async def test_no_steer_is_sent_on_once_the_plan_is_used_up_under_stop(plan):
    turn = _turn()
    turn.steering = SimpleNamespace(waiting=AsyncMock(return_value=["u"]), take=AsyncMock(return_value=None))
    assert await turn.again({"messages": []}, {}, SimpleNamespace(queries=1, plan=plan())) == []
    turn.steering.take.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_turn_however_it_ended_closes_once_what_its_responses_said_of_the_plan_is_kept():
    log = _Log()
    turn = _turn()
    turn._plan = PlanWatch(_source(log), HAIKU, stop_at_limit=False)

    async def close():
        log.order.append("CLI ended")

    turn._run = SimpleNamespace(close=close)
    await turn.close()
    assert log.order == ["CLI ended", "written"], "the run's closing frame reads the plan the turn left"


@pytest.mark.asyncio
async def test_a_turn_opens_one_watch_for_all_its_launches(monkeypatch):
    handed = []

    async def compact(spec, pin, launcher, *, plan, **kwargs):
        handed.append(plan)
        return []

    monkeypatch.setattr("ptc_agent.agent.claude_code.runtime.compact", compact)
    turn = _turn()
    await turn._compact({"messages": [], "claude_session": {"session_id": "s", "entries": []}})
    assert handed == [await turn.watch()] and await turn.watch() is handed[0]


@pytest.mark.asyncio
async def test_a_reply_the_plan_ran_out_during_ends_the_turn_stamped_with_the_notice():
    reply = AIMessage("done", id="a1", response_metadata={"claude_message_id": "m1"})
    ended = (await _turn().ended({"messages": [reply]}, SimpleNamespace(plan=_watch(BILLED, REJECTED))))["messages"][0]
    assert (ended.id, ended.content, ended.response_metadata["claude_message_id"]) == ("a1", "done", "m1")
    assert ended.response_metadata[PLAN_NOTICE_KEY] == {
        "reason": "billed_credits", "window": "five_hour", "until": LATER.isoformat(),
    }
    assert await _turn().ended({"messages": [reply]}, SimpleNamespace(plan=_watch(BILLED, REJECTED, stop=False))) == {}
    assert await _turn().ended({"messages": [reply]}, SimpleNamespace(plan=_watch(REJECTED))) == {}


@pytest.mark.asyncio
async def test_the_stamp_keeps_the_reply_and_adds_the_notice():
    kept = await _watch(BILLED, REJECTED).stamp(AIMessage("x", response_metadata={"a": 1}))
    notice = {"reason": "billed_credits", "window": "five_hour", "until": LATER.isoformat()}
    assert kept.response_metadata == {"a": 1, PLAN_NOTICE_KEY: notice}


@pytest.mark.asyncio
async def test_the_billed_notice_goes_out_once_the_reports_before_it_are_kept(monkeypatch):
    log = _Log()
    watch = PlanWatch(_source(log), HAIKU, stop_at_limit=True)
    for raw in (BILLED, REJECTED):
        watch.seen(_event(**raw))
    monkeypatch.setattr("ptc_agent.agent.claude_code.plan.get_stream_writer", lambda: lambda event: log.order.append(event["type"]))
    assert await watch.stamp(AIMessage("x")) is not None
    assert log.order == ["written", "claude_code_plan"], "a client reading the plan on the notice reads the limit"


@pytest.mark.asyncio
async def test_a_turn_cut_off_while_the_launch_gate_reads_closes_the_cli_it_started():
    reading = asyncio.Event()

    class _Slow(_Log):
        async def limit(self, signed_in, model):
            reading.set()
            await asyncio.Event().wait()

    transport = SimpleNamespace(connect=AsyncMock(), close=AsyncMock())
    connect = asyncio.create_task(_Gated(transport, PlanWatch(_source(_Slow()), HAIKU, stop_at_limit=True)).connect())
    await reading.wait()
    connect.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connect
    transport.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_launch_the_gate_lets_through_starts_a_query_of_its_own():
    watch = _watch(REJECTED, stop=False)
    await _Gated(SimpleNamespace(connect=AsyncMock(), close=AsyncMock()), watch).connect()
    assert not watch.rejected, "an earlier query's refusal is not read as this one's"


def test_a_stop_the_plan_made_reads_as_one_where_its_window_is_unknown():
    watch = _watch({"status": "rejected", "rateLimitType": "five_hour"})
    watch.stopped = True
    interrupted = ClaudeTurnFailed("interrupted", "aborted_streaming")
    stopped = watch.failure(interrupted)
    assert isinstance(stopped, PlanStopped), "a halted reply is the plan's stop, not an interrupt"
    assert (stopped.window, stopped.until) == (None, None)


@pytest.mark.asyncio
async def test_a_switch_to_stop_during_the_last_reply_stamps_its_bill():
    log = _Log(out="credits")
    watch = PlanWatch(_source(log), HAIKU, stop_at_limit=False, enforce=True)
    watch.seen(_event(**BILLED, rateLimitType="five_hour", resetsAt=LATER.timestamp()))
    assert await watch.stamp(AIMessage("x")) is None
    log.out_ = "stop"
    stamped = await watch.stamp(AIMessage("x"))
    assert stamped is not None and stamped.response_metadata[PLAN_NOTICE_KEY]["reason"] == "billed_credits"
