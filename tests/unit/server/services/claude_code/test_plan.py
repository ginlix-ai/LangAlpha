"""The Claude plan as runs keep it and Settings, admission and a stopped turn's refusal read it."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from src.server.services.claude_code.hosted import Hosted, OnCloud, OnOwn

from src.llms.claude_code import PlanLimit, PlanReading, PlanStopped, binding_limit
from ptc_agent.agent.claude_code.machine_token import MachineState
from src.server.database import claude_code_plan as store
from src.server.services.claude_code import checks, saved
from src.server.services.claude_code.checks import check_would_bill
from src.server.services.claude_code.machine import MachineAnswer, MachineHome, check_machine
from src.server.services.claude_code.plan import PlanState, UserPlan, credits_unknown, plan_out, plan_view
from src.server.services.llm import claude_code as admission
from src.server.services.llm.claude_code import claude_code_refusal, require_plan
from tests.unit.server.services.claude_code.fakes import Cache, serving

NOW = datetime(2026, 11, 6, 19, 5, tzinfo=UTC)
UNTIL = datetime(2026, 11, 6, 20, 40, tzinfo=UTC)
SONNET, OPUS = "claude-sonnet-5-5", "claude-opus-5-5"


def _reading(at: datetime, model: str = SONNET, *, limit: PlanLimit | None = None, billed: bool = False,
             windows: dict | None = None, credits: str | None = None) -> PlanReading:
    return PlanReading(at, model, windows or {}, limit, refused=limit is not None and not billed, billed=billed,
                       credits=credits)


def merged(state: dict, reading: PlanReading) -> dict:
    return PlanState.of(state).merge(reading).to_json()


def _state(*readings: PlanReading) -> dict:
    state: dict = {}
    for reading in readings:
        state = merged(state, reading)
    return state


def _limits(state: dict) -> dict[str, str]:
    return {w: v["until"] for w, v in state["limits"].items()}


def test_a_limit_on_one_window_never_replaces_anothers():
    week, hour = NOW + timedelta(days=3), NOW + timedelta(hours=2)
    state = _state(_reading(NOW, OPUS, limit=PlanLimit("seven_day_opus", week)),
                   _reading(NOW + timedelta(minutes=1), limit=PlanLimit("five_hour", hour)))
    assert _limits(state) == {"seven_day_opus": week.isoformat(), "five_hour": hour.isoformat()}


def test_a_request_that_ran_on_the_plan_clears_only_the_limits_holding_its_model():
    week, hour = NOW + timedelta(days=3), NOW + timedelta(hours=2)
    held = _state(_reading(NOW, OPUS, limit=PlanLimit("seven_day_opus", week)), _reading(NOW, limit=PlanLimit("five_hour", hour)))
    later = NOW + timedelta(hours=1)
    assert _limits(merged(held, _reading(later))) == {"seven_day_opus": week.isoformat()}
    assert _limits(merged(held, _reading(later, OPUS))) == {}
    assert _limits(merged(held, _reading(later, billed=True))) == _limits(held), \
        "a billed response says the plan is used up"


def test_a_late_report_never_undoes_a_newer_one():
    hour = NOW + timedelta(hours=2)
    newer = _state(_reading(NOW + timedelta(minutes=5), limit=PlanLimit("five_hour", hour), credits="off",
                            windows={"five_hour": (1.0, hour)}))
    late = merged(newer, _reading(NOW, windows={"five_hour": (0.2, hour)}, credits="on",
                                  limit=PlanLimit("five_hour", hour + timedelta(hours=1))))
    assert (late["windows"]["five_hour"]["used"], late["credits"]["value"], _limits(late)) == (1.0, "off", _limits(newer))


def test_a_refusal_written_after_a_newer_request_ran_on_the_plan_brings_no_limit_back():
    hour = NOW + timedelta(hours=2)
    refused = _reading(NOW, limit=PlanLimit("five_hour", hour))
    ran = merged({}, _reading(NOW + timedelta(minutes=1)))
    assert _limits(merged(ran, refused)) == {}, "the request that ran after it says the limit is over"
    opus_ran = merged({}, _reading(NOW + timedelta(minutes=1), OPUS))
    assert _limits(merged(opus_ran, _reading(NOW, limit=PlanLimit("seven_day_sonnet", hour)))) != {}, \
        "an Opus request says nothing of the weekly Sonnet limit"
    assert _limits(merged(ran, _reading(NOW + timedelta(minutes=2), limit=PlanLimit("five_hour", hour)))) != {}


def test_credits_move_with_the_newest_report_that_knew_them():
    state = _state(_reading(NOW, credits="on"), _reading(NOW + timedelta(minutes=2)),
                   _reading(NOW + timedelta(minutes=1), credits="off"))
    assert state["credits"]["value"] == "off", "a newer report that did not know them keeps them movable"
    assert merged(state, _reading(NOW, credits="on"))["credits"]["value"] == "off"


def test_usage_credits_once_unknown_come_back_only_with_a_newer_report():
    off = _state(_reading(NOW, credits="off"))
    unknown = PlanState.of(off).credits_unknown_at(NOW + timedelta(minutes=1))
    assert unknown.credits_value is None
    assert merged(unknown.to_json(), _reading(NOW, credits="off"))["credits"]["value"] is None, "a late report"
    assert merged(unknown.to_json(), _reading(NOW + timedelta(minutes=2), credits="on"))["credits"]["value"] == "on"


def test_a_limit_whose_reset_passed_goes():
    state = _state(_reading(NOW, limit=PlanLimit("five_hour", NOW + timedelta(minutes=1))))
    assert merged(state, _reading(NOW + timedelta(minutes=2), OPUS, billed=True))["limits"] == {}


def _row(state: dict, **kw) -> dict:
    return {"state": state, "generation": 0, "updated_at": NOW, **kw}


def test_settings_reads_every_limit_still_ahead_and_no_window_past_its_reset():
    state = _state(_reading(NOW, windows={"five_hour": (1.0, UNTIL), "seven_day": (0.6, NOW - timedelta(minutes=1))},
                            limit=PlanLimit("five_hour", UNTIL), credits="on"),
                   _reading(NOW, OPUS, limit=PlanLimit("seven_day_opus", UNTIL + timedelta(days=1))))
    view = plan_view(_row(state), NOW)
    assert (view.five_hour.used, view.seven_day, view.credits) == (1.0, None, "on")
    assert [(h.window, h.until, h.family) for h in view.limits] == [
        ("five_hour", UNTIL, None), ("seven_day_opus", UNTIL + timedelta(days=1), "opus"),
    ]
    assert [h.window for h in plan_view(_row(state), UNTIL).limits] == ["seven_day_opus"]
    assert plan_view(None) is None and plan_view(_row({})) is None, "a replaced token's row reported nothing yet"


@pytest.fixture
def rows(monkeypatch):
    """The user's rows by credential, as the store reads and writes them."""
    state = {"rows": {}, "out": "stop", "fails": False, "reads": [], "writes": []}

    async def get_plans(user_id, credentials=None):
        if state["fails"]:
            raise RuntimeError("no pool")
        state["reads"].append(credentials)
        return {c: r for c, r in state["rows"].items() if credentials is None or c in credentials}

    async def update_plan(user_id, credential, generation, seen_at, change):
        state["writes"].append((credential, generation))
        return True

    async def plan_out(user_id):
        return state["out"]

    monkeypatch.setattr(store, "get_plans", get_plans)
    monkeypatch.setattr(store, "update_plan", update_plan)
    monkeypatch.setattr(admission, "plan_out", plan_out)
    monkeypatch.setattr("src.server.services.claude_code.plan.plan_out", plan_out)
    monkeypatch.setattr("src.server.services.claude_code.checks.plan_out", plan_out)
    monkeypatch.setattr("src.server.database.automation.get_user_timezone", AsyncMock(return_value=None))
    return state


@pytest.mark.asyncio
async def test_a_runs_reports_go_to_the_generation_it_opened_on(rows):
    rows["rows"] = {"saved": _row({}, generation=3)}
    plan = UserPlan("u", "c-1")
    plan.record("saved", _reading(NOW))
    await plan.open()
    plan.record("saved", _reading(NOW))
    plan.record("machine", _reading(NOW))
    await plan.written()
    assert rows["writes"] == [("saved", 3), ("computer:c-1", 0)], "nothing is written before the run opened"


@pytest.mark.asyncio
@pytest.mark.parametrize(("on_row", "kept"), [("old", True), (None, True), ("new", False)])
async def test_a_check_of_a_token_read_before_it_opened_keeps_nothing_once_another_is_saved(rows, on_row, kept):
    rows["rows"] = {"saved": _row({}, generation=2, token_digest=on_row)}
    plan = UserPlan("u", token_digest="old")
    await plan.open()
    plan.record("saved", _reading(NOW))
    await plan.written()
    assert rows["writes"] == ([("saved", 2)] if kept else [])


@pytest.mark.asyncio
async def test_a_report_the_run_had_nowhere_to_write_is_not_called_written(rows, monkeypatch):
    plan = UserPlan("u", "c-1")
    monkeypatch.setattr(store, "get_plans", AsyncMock(side_effect=RuntimeError("db down")))
    with pytest.raises(RuntimeError):
        await plan.open()
    plan.record("saved", _reading(NOW))
    assert await plan.written() is False
    assert rows["writes"] == []


@pytest.mark.asyncio
async def test_a_run_reads_the_row_of_the_credential_it_signed_in_with(rows):
    rows["rows"] = {"computer:c-1": _row(_state(_reading(NOW, limit=PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)))))}
    assert (await UserPlan("u", "c-1").limit("machine", SONNET)).window == "five_hour"
    assert await UserPlan("u", "c-1").limit("saved", SONNET) is None
    assert await UserPlan("u").limit("machine", SONNET) is None


async def _refused(model: str, credential: str = "saved") -> dict | None:
    try:
        await require_plan(model, "u", credential)
    except HTTPException as e:
        return e.detail
    return None


def _held(*limits: PlanLimit, credits: str | None = None) -> dict:
    return _row(_state(*(_reading(NOW, limit=limit) for limit in limits), *([_reading(NOW, credits=credits)] if credits else [])))


@pytest.mark.asyncio
async def test_a_turn_on_a_used_up_plan_is_refused_with_the_limit_that_lasts_longest(rows):
    hour, week = datetime.now(UTC) + timedelta(hours=1), datetime.now(UTC) + timedelta(days=2)
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", hour), PlanLimit("seven_day_opus", week))}
    detail = await _refused("claude-opus-5-5-code")
    assert {k: detail[k] for k in ("type", "reason", "window", "until")} == {
        "type": "claude_code_off", "reason": "plan_limited", "window": "seven_day_opus", "until": week.isoformat(),
    }
    assert "credits_billed" not in detail and "error_kind" not in detail
    assert (await _refused("claude-sonnet-5-5-code"))["window"] == "five_hour"


@pytest.mark.asyncio
@pytest.mark.parametrize(("window", "model", "refused"), [
    ("seven_day_opus", "claude-sonnet-5-5-code", False),
    ("seven_day_opus", "claude-opus-5-5-code", True),
    ("seven_day_sonnet", "claude-haiku-5-5-code", False),
    ("seven_day", "claude-haiku-5-5-code", True),
    ("seven_day_overage_included", "claude-fable-5-2-code", True),
    ("seven_day_overage_included", "claude-opus-5-5-code", False),
])
async def test_a_weekly_family_limit_refuses_only_that_familys_rows(rows, window, model, refused):
    rows["rows"] = {"saved": _held(PlanLimit(window, datetime.now(UTC) + timedelta(days=1)))}
    assert (await _refused(model) is not None) is refused


@pytest.mark.asyncio
@pytest.mark.parametrize(("out", "credits", "refused"), [
    ("stop", "on", True), ("credits", "on", False), ("credits", None, False), ("credits", "off", True),
])
async def test_under_usage_credits_a_turn_is_refused_only_where_they_are_off(rows, out, credits, refused):
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)), credits=credits)}
    rows["out"] = out
    assert (await _refused("claude-haiku-5-5-code") is not None) is refused


@pytest.mark.asyncio
async def test_under_usage_credits_once_their_status_is_unknown_a_turn_goes_through(rows):
    held = _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)), credits="off")
    rows["rows"] = {"saved": {**held, "state": PlanState.of(held["state"]).credits_unknown_at(NOW).to_json()}}
    rows["out"] = "credits"
    assert await _refused("claude-haiku-5-5-code") is None, "its first request says whether they pay"


@pytest.mark.asyncio
async def test_a_choice_that_cannot_be_read_refuses_as_stop_does(rows, monkeypatch):
    async def no_pool(user_id):
        raise RuntimeError("no pool")

    rows["rows"] = {"saved": _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)), credits="on")}
    monkeypatch.setattr(admission, "plan_out", no_pool)
    assert (await _refused("claude-haiku-5-5-code"))["reason"] == "plan_limited", "as the run's gate reads it"


@pytest.mark.asyncio
async def test_a_passed_limit_or_an_unreadable_row_refuses_nothing(rows):
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", datetime.now(UTC) - timedelta(seconds=1)))}
    assert await _refused("claude-haiku-5-5-code") is None
    rows["fails"] = True
    assert await _refused("claude-haiku-5-5-code") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("answer", "saved", "read"), [
    (None, "saved_token", None),
    (SimpleNamespace(relays=True, fresh=True), "saved_token", ["saved"]),
    (SimpleNamespace(relays=False, fresh=True), None, ["computer:c-1"]),
    (SimpleNamespace(relays=True, fresh=False), "saved_token", None),
    (SimpleNamespace(relays=False, fresh=False), None, None),
])
async def test_admission_on_an_own_computer_reads_the_row_its_turn_signs_in_on(rows, monkeypatch, answer, saved, read):
    async def placement(*_args, **_kwargs):
        return OnOwn({"computer_id": "c-1"})

    async def read_answer(computer_id):
        return answer

    async def signed_in(model, user_id, given):
        assert given is answer
        return saved

    monkeypatch.setattr(admission, "cli_placement", placement)
    monkeypatch.setattr(admission, "read_answer", read_answer)
    monkeypatch.setattr(admission, "require_computer_signed_in", signed_in)
    monkeypatch.setattr(admission, "rows_available", lambda: True)
    config = SimpleNamespace(feature_enabled=lambda feature: True)
    await admission.require_runnable(config, "claude-haiku-5-5-code", user_id="u", workspace_id="w")
    assert rows["reads"] == ([read] if read else [])



@pytest.mark.asyncio
@pytest.mark.parametrize("own", [False, True])
async def test_a_turn_on_the_stored_token_at_its_plans_limit_is_refused_for_the_plan(rows, monkeypatch, own):
    # Whoever started it: a wake is held for the reset, and an automation
    # told when it resets.
    async def placement(*_args, **_kwargs):
        if own:
            return OnOwn({"computer_id": "c-1"})
        return OnCloud(Hosted("stored_token"))

    async def read_answer(computer_id):
        return SimpleNamespace(relays=True, fresh=True)

    async def signed_in(model, user_id, given):
        return "stored_token"

    monkeypatch.setattr(admission, "cli_placement", placement)
    monkeypatch.setattr(admission, "read_answer", read_answer)
    monkeypatch.setattr(admission, "require_computer_signed_in", signed_in)
    monkeypatch.setattr(admission, "rows_available", lambda: True)
    rows["rows"] = {c: _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1))) for c in ("saved", "computer:c-1")}
    config = SimpleNamespace(feature_enabled=lambda feature: True)

    with pytest.raises(HTTPException) as refused:
        await admission.require_runnable(config, "claude-haiku-5-5-code", user_id="u", workspace_id="w")
    assert refused.value.detail["reason"] == "plan_limited"


def test_a_turn_the_plan_stopped_ends_on_a_refusal_naming_its_window_reset_and_billing():
    assert claude_code_refusal(RuntimeError("another failure"), "m") is None
    stopped = PlanStopped("seven_day_opus", UNTIL, credits_billed=True)
    try:
        raise RuntimeError("the run failed") from stopped
    except RuntimeError as e:
        detail = claude_code_refusal(e, "claude-opus-5-5-code")
    assert detail == {
        "type": "claude_code_off", "model": "claude-opus-5-5-code", "reason": "plan_stopped",
        "window": "seven_day_opus", "until": UNTIL.isoformat(), "credits_billed": True,
        "message": (
            "Your Claude plan's weekly Opus limit is used up, so this turn stopped. It resets at Nov 6, 20:40 UTC. "
            "Nothing ran on another model. Requests already under way were billed to your usage credits."
        ),
    }
    plain = claude_code_refusal(PlanStopped("seven_day_overage_included", UNTIL), "m")
    assert plain["credits_billed"] is False and plain["message"].startswith("Your Claude plan's weekly Fable limit")


HOLD = timedelta(hours=5)


def _no_reset(at: datetime, model: str = SONNET, window: str | None = None) -> PlanReading:
    return _reading(at, model, limit=PlanLimit(window, at + HOLD, known=False), billed=True)


def test_a_bill_claude_said_no_reset_for_holds_every_model_until_its_bound():
    reading = PlanReading.of({"status": "rejected", "overageStatus": "allowed", "isUsingOverage": True}, NOW, SONNET)
    assert reading.limit == PlanLimit(None, NOW + HOLD, known=False)
    named = PlanReading.of({"status": "rejected", "rateLimitType": "seven_day_opus"}, NOW, OPUS)
    assert named.limit == PlanLimit("seven_day_opus", NOW + HOLD, known=False)
    state = PlanState.of(_state(reading))
    assert state.held(NOW) == [PlanLimit(None, NOW + HOLD, known=False)]
    assert binding_limit(state.held(NOW), OPUS, NOW) is not None
    assert state.held(NOW + HOLD) == [], "a missing reset holds no longer than its bound"


LATER_ON = NOW + timedelta(minutes=1)


@pytest.mark.parametrize(("then", "held"), [
    (_reading(LATER_ON, OPUS), []),
    (_reading(LATER_ON, limit=PlanLimit("five_hour", UNTIL)), [PlanLimit("five_hour", UNTIL)]),
    (_reading(NOW - timedelta(minutes=1), OPUS), [PlanLimit(None, NOW + HOLD, known=False)]),
])
def test_a_limit_claude_said_no_reset_for_gives_way_to_a_later_report_that_ran_or_named_a_reset(then, held):
    assert PlanState.of(merged(_state(_no_reset(NOW)), then)).held(NOW) == held


def test_a_window_claude_said_no_reset_for_holds_only_its_family_and_clears_with_it():
    state = _state(_no_reset(NOW, OPUS, "seven_day_opus"))
    assert binding_limit(PlanState.of(state).held(NOW), SONNET, NOW) is None
    assert PlanState.of(merged(state, _reading(LATER_ON, SONNET))).held(NOW) != []
    assert PlanState.of(merged(state, _reading(LATER_ON, OPUS))).held(NOW) == []


def test_a_bill_claude_said_no_reset_for_keeps_a_limit_with_a_reset_that_holds_its_model():
    state = _state(_reading(NOW, limit=PlanLimit("five_hour", UNTIL)), _no_reset(LATER_ON))
    assert PlanState.of(state).held(NOW) == [PlanLimit("five_hour", UNTIL)]


def test_settings_reads_a_limit_claude_said_no_reset_for_as_one_with_no_reset():
    view = plan_view(_row(_state(_no_reset(NOW))), NOW)
    assert [(h.window, h.family, h.reset_known) for h in view.limits] == [(None, None, False)]


@pytest.mark.asyncio
async def test_a_turn_after_a_bill_claude_said_no_reset_for_is_refused_naming_no_time(rows, monkeypatch):
    rows["rows"] = {"saved": _row(_state(_no_reset(datetime.now(UTC))))}
    detail = await _refused("claude-haiku-5-5-code")
    assert (detail["reason"], "window" in detail, "until" in detail) == ("plan_limited", False, False)
    assert detail["message"] == (
        "Your Claude plan's limit is used up, and Claude didn't say when it resets. Pick another model, or send this "
        "again later."
    )
    rows["rows"] = {"saved": _row(_state(_no_reset(datetime.now(UTC), OPUS, "seven_day_opus")))}
    named = await _refused("claude-opus-5-5-code")
    assert ("window" in named, "until" in named) == (False, False), "a window with no reset is told with no time"
    rows["rows"] = {"saved": _row(_state(_no_reset(datetime.now(UTC))))}
    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    assert await check_would_bill("u", "saved"), "a token check holds at it as admission does"
    rows["out"] = "credits"
    assert await _refused("claude-haiku-5-5-code") is None


def test_a_turn_stopped_where_claude_said_no_reset_ends_on_a_refusal_naming_no_time():
    detail = claude_code_refusal(PlanStopped(credits_billed=True), "m")
    assert ("window" in detail, "until" in detail, detail["credits_billed"]) == (False, False, True)
    assert detail["message"] == (
        "Your Claude plan's limit is used up, so this turn stopped. Nothing ran on another model. Requests already "
        "under way were billed to your usage credits."
    )

@pytest.mark.asyncio
@pytest.mark.parametrize(("window", "credits", "out", "bills"), [
    ("five_hour", "on", "stop", True),
    ("five_hour", None, "stop", True),
    ("seven_day", "on", "stop", True),
    ("five_hour", "off", "stop", False),
    ("five_hour", "on", "credits", False),
    ("seven_day_opus", "on", "stop", False),
    (None, "on", "stop", False),
])
async def test_a_token_check_would_bill_only_under_stop_on_a_limit_binding_its_model_with_credits_not_off(
    rows, monkeypatch, window, credits, out, bills,
):
    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    limits = [PlanLimit(window, datetime.now(UTC) + timedelta(hours=1))] if window else []
    rows["rows"], rows["out"] = {"saved": _held(*limits, credits=credits)}, out
    assert await check_would_bill("u", "saved") is bills


@pytest.mark.asyncio
async def test_an_unreadable_row_bills_nothing_and_an_unreadable_choice_reads_as_stop(rows, monkeypatch):
    async def no_pool(user_id):
        raise RuntimeError("no pool")

    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)), credits="on")}
    monkeypatch.setattr("src.server.services.claude_code.checks.plan_out", no_pool)
    assert await check_would_bill("u", "saved") is True
    rows["fails"] = True
    assert await check_would_bill("u", "saved") is False


@pytest.mark.asyncio
@pytest.mark.parametrize(("flagged", "kept"), [(False, "works"), (True, "refused")])
async def test_a_saved_tokens_check_that_would_bill_sends_nothing_and_keeps_the_last_answer(
    rows, monkeypatch, flagged, kept,
):
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)), credits="on")}
    works, keep = AsyncMock(return_value=True), AsyncMock()
    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    monkeypatch.setattr("ptc_agent.agent.claude_code.runtime.token_works", works)
    monkeypatch.setattr(saved, "keep_check", keep)
    row = {"access_token": "sk-ant-oat01-x", "updated_at": NOW, "needs_reauth_at": NOW if flagged else None}
    await saved.check_token("u", row, "key", AsyncMock(return_value=True))
    works.assert_not_awaited()
    keep.assert_awaited_once_with("key", kept)


@pytest.mark.asyncio
@pytest.mark.parametrize(("last", "kept"), [(None, "works"), ("works", "works"), ("refused", "refused"), ("unknown", "works")])
async def test_a_machines_check_that_would_bill_sends_nothing_and_keeps_the_last_answer(rows, monkeypatch, last, kept):
    rows["rows"] = {"computer:c-1": _held(PlanLimit("five_hour", datetime.now(UTC) + timedelta(hours=1)))}
    works = AsyncMock(return_value=True)
    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    monkeypatch.setattr("ptc_agent.agent.claude_code.runtime.token_works", works)
    monkeypatch.setattr("ptc_agent.agent.claude_code.machine_token.machine_state",
                        AsyncMock(return_value=MachineState("file", True, "~/.claude")))
    before = MachineAnswer(last, email="me@example.com", checked_at=1.0) if last else None
    answer = await check_machine(SimpleNamespace(link=AsyncMock()), "u", "c-1", before)
    works.assert_not_awaited()
    assert (answer.file, answer.email) == (kept, "me@example.com" if last else None)


KNOWN = PlanLimit("five_hour", NOW + timedelta(hours=1))


@pytest.mark.parametrize("order", ["reported", "reversed"])
def test_a_reset_claude_gave_stands_for_a_hold_with_none_in_either_order_of_writes(order):
    known, unknown = _reading(NOW, limit=KNOWN), _no_reset(LATER_ON)
    first, then = (known, unknown) if order == "reported" else (unknown, known)
    assert PlanState.of(merged(_state(first), then)).held(NOW) == [KNOWN]


def test_a_reset_written_after_a_later_hold_for_its_window_takes_its_place():
    held = PlanState.of(merged(_state(_no_reset(LATER_ON, window="five_hour")), _reading(NOW, limit=KNOWN))).held(NOW)
    assert held == [KNOWN]


def test_a_reset_seen_before_the_plan_ran_again_leaves_a_hold_seen_after():
    state = merged(_state(_reading(NOW + timedelta(minutes=1)), _no_reset(NOW + timedelta(minutes=2))),
                   _reading(NOW, limit=KNOWN))
    assert PlanState.of(state).held(NOW) == [PlanLimit(None, NOW + timedelta(minutes=2) + HOLD, known=False)]


def test_a_family_reset_leaves_a_hold_that_named_no_window_on_every_other_model():
    state = merged(_state(_no_reset(NOW)), _reading(LATER_ON, OPUS, limit=PlanLimit("seven_day_opus", NOW + timedelta(days=2))))
    assert binding_limit(PlanState.of(state).held(NOW), SONNET, NOW) == PlanLimit(None, NOW + HOLD, known=False)


@pytest.mark.asyncio
async def test_a_hold_with_no_reset_refuses_nothing_while_usage_credits_are_off(rows):
    at = datetime.now(UTC)
    rows["rows"] = {"saved": _row(_state(_no_reset(at), _reading(at, credits="off")))}
    assert PlanState.of(rows["rows"]["saved"]["state"]).held() == []
    assert await _refused("claude-haiku-5-5-code") is None, "a request at the limit is refused at no cost"
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", at + timedelta(hours=1)), credits="off")}
    assert await _refused("claude-haiku-5-5-code") is not None


def test_a_report_naming_no_plan_window_takes_the_window_it_shows_used_up():
    soon, later = NOW + timedelta(minutes=20), NOW + timedelta(days=20)
    reading = PlanReading.of({
        "status": "allowed", "isUsingOverage": True, "rateLimitType": "overage", "resetsAt": later.timestamp(),
        "unifiedWindows": {"five_hour": {"utilization": 1.0, "resetsAt": soon.timestamp()},
                           "seven_day": {"utilization": 0.4, "resetsAt": later.timestamp()}},
    }, NOW, SONNET)
    assert reading.limit == PlanLimit("five_hour", soon)


@pytest.mark.asyncio
async def test_choosing_usage_credits_fences_every_row_from_an_older_report(monkeypatch):
    changed = {}

    async def change_plans(user_id, change, *, conn=None):
        changed["state"] = change({})

    monkeypatch.setattr(store, "change_plans", change_plans)
    await credits_unknown("u")
    late = merged(changed["state"], _reading(NOW, credits="off"))
    assert PlanState.of(late).credits_value is None, "a report seen before the choice never says again"


@pytest.mark.asyncio
async def test_the_plan_choice_is_read_from_the_row_not_the_cache(monkeypatch):
    reads = AsyncMock(return_value={"other_preference": {"claude_code_plan_out": "credits"}})
    monkeypatch.setattr("src.server.services.claude_code.plan.get_user_preferences", reads)
    assert await plan_out("u") == "credits"
    reads.assert_awaited_once_with("u", cached=False)


@pytest.mark.asyncio
async def test_a_token_saved_after_its_check_met_the_limit_keeps_the_limit_for_the_next_turn(rows, monkeypatch):
    @asynccontextmanager
    async def locked(*_):
        yield object()

    token = "sk-ant-oat01-new"
    rows["rows"] = {"saved": _row({}, generation=2, token_digest=saved.token_hash(token))}
    for name, value in (("stored_row", AsyncMock(return_value=None)), ("keep_check", AsyncMock()),
                        ("connection_change", locked), ("saved_token_runs", AsyncMock(return_value=True)),
                        ("upsert_oauth_tokens", AsyncMock())):
        monkeypatch.setattr(saved, name, value)
    monkeypatch.setattr(store, "forget_plan", AsyncMock())
    with serving(Cache()):
        home = saved.SavedHome("u")
        async with home.placed(token, held=AsyncMock(return_value=True), retryable=False) as launcher:
            assert launcher.plan is home.checked
            launcher.plan.record("saved", _no_reset(NOW))
        await home.keep(token, held=AsyncMock(return_value=True), email=None, expires_at=NOW, token_uuid="")
    assert rows["writes"] == [("saved", 2)]


@pytest.mark.asyncio
async def test_a_token_placed_after_its_check_met_the_limit_keeps_the_limit_on_the_computers_row(rows, monkeypatch):
    rows["rows"] = {"computer:c-1": _row({}, generation=3)}
    monkeypatch.setattr("src.server.services.claude_code.machine.keep_answer", AsyncMock(side_effect=lambda _c, a: a))
    monkeypatch.setattr(store, "forget_plan", AsyncMock())
    with serving(Cache()):
        home = MachineHome("u", {"computer_id": "c-1"})
        home.checked.record("machine", _no_reset(NOW))
        await home.keep("sk-ant-oat01-new", held=AsyncMock(return_value=True), email=None, expires_at=NOW, token_uuid="")
    assert rows["writes"] == [("computer:c-1", 3)]


def test_a_credit_report_seen_after_the_choice_stands_when_written_before_it():
    state = PlanState.of(_state(_reading(LATER_ON, credits="off")))
    assert state.credits_unknown_at(NOW).credits_value == "off"
    assert state.credits_unknown_at(LATER_ON + timedelta(minutes=1)).credits_value is None


def test_a_window_claude_said_no_reset_for_is_held_beside_another_windows_reset():
    weekly = _no_reset(LATER_ON, OPUS, window="seven_day_opus")
    held = PlanState.of(merged(_state(_reading(NOW, limit=KNOWN)), weekly)).held(NOW)
    assert held == [KNOWN, PlanLimit("seven_day_opus", LATER_ON + HOLD, known=False)]


@pytest.mark.asyncio
async def test_a_turn_whose_saved_token_was_replaced_before_it_read_it_keeps_its_reports(rows):
    rows["rows"] = {"saved": _row({}, generation=2, token_digest="old")}
    plan = UserPlan("u")
    await plan.open()
    rows["rows"] = {"saved": _row({}, generation=3, token_digest=saved.token_hash("sk-ant-oat01-new"))}
    await plan.took("sk-ant-oat01-old")
    plan.record("saved", _reading(NOW))
    await plan.written()
    await plan.took("sk-ant-oat01-new")
    plan.record("saved", _reading(NOW))
    await plan.written()
    assert rows["writes"] == [("saved", 2), ("saved", 3)], "a token read before the replacement stays fenced"


@pytest.mark.asyncio
async def test_a_turn_that_launches_after_a_token_was_placed_on_its_computer_keeps_its_reports(rows):
    rows["rows"] = {"computer:c-1": _row({}, generation=3)}
    plan = UserPlan("u", "c-1")
    await plan.open()
    rows["rows"] = {"computer:c-1": _row({}, generation=4)}
    await plan.launching()
    plan.record("machine", _reading(NOW))
    await plan.written()
    assert rows["writes"] == [("computer:c-1", 4)]


# --- unattended turns: automations, wakeups and subagents at the limit ---------


@pytest.mark.asyncio
async def test_a_refused_turn_names_when_the_hold_lifts_with_a_reset_or_without(rows):
    """A wake held for the reset waits until ``held_until``: the reset Claude
    gave, or the hold's own bound where it gave none."""
    hour = datetime.now(UTC) + timedelta(hours=1)
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", hour))}
    detail = await _refused("claude-haiku-5-5-code")
    assert (detail["held_until"], detail["held_known"]) == (hour.isoformat(), True)
    seen = datetime.now(UTC)
    rows["rows"] = {"saved": _row(_state(_no_reset(seen)))}
    detail = await _refused("claude-haiku-5-5-code")
    assert ("until" in detail, detail["held_until"], detail["held_known"]) == (
        False, (seen + timedelta(hours=5)).isoformat(), False,
    )


@pytest.mark.asyncio
async def test_a_refused_turn_names_the_credential_and_model_the_plan_held(rows):
    hour = datetime.now(UTC) + timedelta(hours=1)
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", hour))}
    detail = await _refused("claude-haiku-5-5-code")
    assert detail["held_credential"] == "saved"
    assert detail["held_model"] and "haiku" in detail["held_model"]


@pytest.mark.asyncio
async def test_the_plan_still_holds_a_model_until_its_limit_lifts_whatever_the_credits_choice(rows):
    from src.server.services.llm.claude_code import plan_still_holds

    hour = datetime.now(UTC) + timedelta(hours=1)
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", hour))}
    assert await plan_still_holds("claude-haiku-5-5", "u", "saved")
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", hour), credits="on")}
    assert await plan_still_holds("claude-haiku-5-5", "u", "saved")
    rows["rows"] = {"saved": _row({})}
    assert not await plan_still_holds("claude-haiku-5-5", "u", "saved")


def test_an_automations_report_words_the_reset_on_its_owners_clock():
    from src.server.services.llm.claude_code import plan_words, reword_plan

    until = datetime(2030, 1, 3, 20, 40, tzinfo=UTC)
    assert plan_words("plan_limited", "five_hour", until, zone="America/New_York") == (
        "Your Claude plan's 5-hour limit is used up until Thu Jan 3, 3:40 PM EST. Pick another model, or send this "
        "again after it resets."
    )
    stopped = claude_code_refusal(PlanStopped("seven_day", until, credits_billed=True), "m")
    assert reword_plan(stopped, "Asia/Shanghai") == (
        "Your Claude plan's weekly limit is used up, so this turn stopped. It resets at Fri Jan 4, 4:40 AM CST. "
        "Nothing ran on another model. Requests already under way were billed to your usage credits."
    )
    assert reword_plan(claude_code_refusal(PlanStopped(), "m"), "UTC") == (
        "Your Claude plan's limit is used up, so this turn stopped. Nothing ran on another model."
    )


_OWN = {"computer_id": "c-1", "user_id": "u", "kind": "ssh"}
_CLOUD = {"computer_id": "c-2", "user_id": "u", "kind": "docker"}


@pytest.mark.asyncio
@pytest.mark.parametrize(("computer", "fresh", "relays", "saved", "credential"), [
    (_OWN, True, True, True, "saved"),
    (_OWN, True, True, False, "computer:c-1"),
    # A token of the machine's own, found since the turn began, is the one its CLI runs.
    (_OWN, True, False, True, "computer:c-1"),
    (_OWN, False, True, True, None),
    (_OWN, None, True, False, None),
    (_CLOUD, None, True, True, "saved"),
    (_CLOUD, None, True, False, None),
])
async def test_a_claude_code_subagent_reads_the_row_its_cli_signs_in_with(
    rows, monkeypatch, computer, fresh, relays, saved, credential,
):
    async def read_answer(computer_id):
        return None if fresh is None else SimpleNamespace(fresh=fresh, relays=relays)

    monkeypatch.setattr(admission, "read_answer", read_answer)
    hour = datetime.now(UTC) + timedelta(hours=1)
    rows["rows"] = {c: _held(PlanLimit("five_hour", hour)) for c in ("saved", "computer:c-1")}
    refusal = await admission.subagent_plan_refusal(computer, SONNET, saved)
    assert rows["reads"] == ([[credential]] if credential else [])
    assert (refusal is not None) is (credential is not None)


@pytest.mark.asyncio
async def test_a_claude_code_subagent_at_the_limit_is_refused_saying_until_when(rows, monkeypatch):
    from src.server.database import automation as automation_db

    zone = AsyncMock(return_value="America/New_York")
    monkeypatch.setattr(automation_db, "get_user_timezone", zone)
    until = datetime(2030, 1, 3, 20, 40, tzinfo=UTC)
    rows["rows"] = {"saved": _held(PlanLimit("five_hour", until))}
    assert await admission.subagent_plan_refusal(_CLOUD, SONNET, True) == (
        "Not started: the Claude plan's 5-hour limit is used up until Thu Jan 3, 3:40 PM EST, so Claude Code "
        "subagents can't run before then."
    )
    zone.assert_awaited_once_with("u")
    zone.return_value = None
    assert await admission.subagent_plan_refusal(_CLOUD, SONNET, True) == (
        "Not started: the Claude plan's 5-hour limit is used up until Jan 3, 20:40 UTC, so Claude Code subagents "
        "can't run before then."
    )
    rows["rows"] = {"saved": _held(PlanLimit("seven_day_opus", until))}
    assert await admission.subagent_plan_refusal(_CLOUD, SONNET, True) is None, "a family limit holds only its family"
    rows["rows"] = {"saved": _row(_state(_no_reset(datetime.now(UTC))))}
    assert await admission.subagent_plan_refusal(_CLOUD, SONNET, True) == (
        "Not started: the Claude plan's limit is used up, and Claude didn't say when it resets, so Claude Code "
        "subagents can't run before then."
    )
    rows["out"] = "credits"
    assert await admission.subagent_plan_refusal(_CLOUD, SONNET, True) is None, "under usage credits they run"

